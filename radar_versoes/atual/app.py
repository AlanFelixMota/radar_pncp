#!/usr/bin/env python3
"""Interface web local do Radar PNCP V1.0.0."""

from __future__ import annotations

import os
import json
import re
import sqlite3
import time
from datetime import date, datetime, timedelta, time as dt_time
from pathlib import Path

import pandas as pd
import filtros_precos as fprecos
import requests
import streamlit as st
from interface_radar_pncp import VERSION, PAGINAS, ROTULOS_NAV, AJUDAS_NAV, AREAS_SISTEMA, AREAS_CONFIG, resolver_rota, estado_coleta, progresso_resultados

import job_manager as jm
from proveniencia import garantir_proveniencia
import fts_manager as fm
import tic_manager as tm
import radar_theme as ui
import radar_stats as rs
import pncp_health as ph
import coleta_scheduler as rsch
import comprasgov_resultados_sync as crsync
import itens_incremental as ii
import painel_fila as pf
import pesquisa_atualizacao as pa
import cobertura_base as cb
import mutirao_persistente as mp
import mutirao_scheduler as msch
import resultados_pncp as rpncp
from radar_paths import DATA_DIR, DB_COMPRAS, DB_JOBS, DB_FTS, DB_TIC, DB_STATS, LOG_DIR, TEST_MODE, garantir_diretorios

garantir_diretorios()

from radar_db import (
    FiltrosBusca,
    buscar_contratacoes,
    buscar_itens_correspondentes,
    estatisticas_painel,
    iso_para_data,
    obter_contratacao,
    obter_itens_contratacao,
    obter_metadados,
    validar_banco,
)


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = DB_COMPRAS
JOBS_DB_PATH = DB_JOBS

st.set_page_config(
    page_title="Radar PNCP",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)


def _assinatura_arquivo(caminho: Path) -> tuple[int, int]:
    try:
        stt = caminho.stat()
        return (int(stt.st_size), int(stt.st_mtime_ns))
    except OSError:
        return (0, 0)


@st.cache_data(ttl=300, show_spinner=False)
def validar_banco_cache(caminho: str) -> tuple[bool, str]:
    # Não usa o WAL na chave: alterações normais do job não devem invalidar a UI.
    return validar_banco(Path(caminho))


@st.cache_data(ttl=60, show_spinner=False)
def carregar_snapshot_stats(caminho: str, _assinatura: tuple[int, int]) -> dict | None:
    return rs.carregar_estatisticas(Path(caminho))




@st.cache_data(ttl=10, show_spinner=False)
def status_agendamento_cache(modo: str, base_dir: str) -> dict:
    return rsch.obter_status(modo, Path(base_dir))


@st.cache_data(ttl=30, show_spinner=False)
def status_mutirao_persistente_cache(base_dir: str) -> dict:
    # Consultar o Agendador do Windows envolve abrir PowerShell. Trinta segundos
    # bastam para o painel operacional e evitam subprocessos a cada rerun.
    return msch.obter_status(Path(base_dir))


@st.cache_data(ttl=30, show_spinner=False)
def resumo_fila_itens_cache(caminho: str) -> dict:
    """Resumo dinâmico indexado; não depende do mtime do banco.

    O compras.db muda continuamente durante os jobs. Na 0.6.6.0 a fila era
    recalculada com varreduras globais a cada expiração curta do cache, tornando
    a Administração lenta. ``ii.resumo_fila`` agora usa contagens indexadas.
    """
    con = sqlite3.connect(caminho, timeout=10)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON")
    con.execute("PRAGMA busy_timeout=10000")
    try:
        return ii.resumo_fila(con)
    finally:
        con.close()


@st.cache_data(ttl=30, show_spinner=False)
def resumo_hibrido_cache(caminho: str) -> dict:
    """Lê somente contagens seletivas da camada híbrida.

    Evita o ``SUM(CASE ...)`` que varria toda a tabela de contratações.
    """
    con = sqlite3.connect(caminho, timeout=10)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout=10000")
    try:
        cols = {str(r[1]) for r in con.execute("PRAGMA table_info(contratacoes)")}
        if "itens_validacao_comprasgov_status" not in cols:
            return {"validados_pendentes":0,"fallbacks":0,"transitorios":0,"ultima_execucao":None,"novos_itens":0}

        def contar(status: str) -> int:
            return int(con.execute(
                "SELECT COUNT(*) FROM contratacoes WHERE itens_validacao_comprasgov_status=? AND itens_coletados=0",
                (status,),
            ).fetchone()[0] or 0)

        validados = contar("VALIDADO_COMPRASGOV")
        fallbacks = contar("FALLBACK_PNCP")
        transitorios = contar("ERRO_TRANSITORIO_COMPRASGOV")
        novos = int(con.execute(
            """SELECT COALESCE(SUM(itens_validacao_comprasgov_novos),0)
                 FROM contratacoes
                WHERE itens_validacao_comprasgov_status='VALIDADO_COMPRASGOV' AND itens_coletados=0"""
        ).fetchone()[0] or 0)
        ultima = None
        tem_log = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='itens_validacao_comprasgov_log'"
        ).fetchone()
        if tem_log:
            row = con.execute(
                "SELECT executado_em FROM itens_validacao_comprasgov_log ORDER BY id DESC LIMIT 1"
            ).fetchone()
            ultima = row[0] if row else None
        return {
            "validados_pendentes": validados, "fallbacks": fallbacks,
            "transitorios": transitorios, "ultima_execucao": ultima, "novos_itens": novos,
        }
    finally:
        con.close()


@st.cache_data(ttl=60, show_spinner=False)
def status_resultados_comprasgov_cache(caminho: str, referencia: str) -> dict:
    return crsync.obter_status_local(Path(caminho), referencia)


@st.cache_data(ttl=60, show_spinner=False)
def cobertura_base_cache(
    db_compras: str, db_jobs: str, inicio: str, fim: str,
    _sig_compras: tuple[int, int], _sig_jobs: tuple[int, int],
) -> dict:
    return cb.analisar_cobertura(Path(db_compras), Path(db_jobs), inicio, fim)


def _hora_para_obj(valor: str | None, padrao: tuple[int, int]) -> dt_time:
    try:
        if valor:
            hh, mm = str(valor).split(":", 1)
            return dt_time(int(hh), int(mm))
    except Exception:
        pass
    return dt_time(*padrao)


def _fmt_agendamento_iso(valor: str | None) -> str:
    if not valor:
        return "—"
    try:
        # Exibe só o necessário e preserva o horário local informado pelo Windows.
        v = str(valor)
        if "T" in v:
            d, resto = v.split("T", 1)
            hhmm = resto[:5]
            return f"{d[8:10]}/{d[5:7]}/{d[:4]} {hhmm}"
    except Exception:
        pass
    return str(valor)

def obter_snapshot_interface() -> dict:
    assinatura = _assinatura_arquivo(DB_STATS)
    snap = carregar_snapshot_stats(str(DB_STATS), assinatura)
    if snap is not None:
        return snap

    # Primeira abertura da V0.7.6: gera uma única vez. Depois, os jobs atualizam
    # o snapshot em segundo plano e a UI só lê radar_stats.db.
    with st.spinner("Preparando estatísticas rápidas do Radar PNCP (feito apenas na primeira abertura)..."):
        snap = rs.reconstruir_estatisticas(DB_PATH, DB_STATS)
    carregar_snapshot_stats.clear()
    return snap


@st.cache_data(ttl=60, show_spinner=False)
def cobertura_comprasgov_2026_cache(caminho: str) -> str | None:
    try:
        con = sqlite3.connect(caminho, timeout=5)
        tem = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='comprasgov_cobertura'"
        ).fetchone()
        row = con.execute(
            "SELECT valor FROM comprasgov_cobertura WHERE chave='comprasgov_2026_anual_data_max'"
        ).fetchone() if tem else None
        con.close()
        return str(row[0]) if row and row[0] else None
    except Exception:
        return None


@st.cache_data(ttl=30, show_spinner=False)
def status_reconciliacao_cache(caminho: str) -> dict:
    return _status_reconciliacao_db_sem_cache(Path(caminho))


@st.cache_data(ttl=30, show_spinner=False)
def status_fila_reconciliacao_cache(caminho: str) -> dict:
    out = {"fila_pendentes_api": 0, "fila_404": 0, "fila_410": 0, "fila_erros": 0, "fila_resolvidas": 0}
    try:
        con = sqlite3.connect(caminho, timeout=5)
        tem = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='pncp_reconciliacao_staging'"
        ).fetchone()
        if tem:
            filas = {str(r[0]): int(r[1]) for r in con.execute(
                "SELECT status,COUNT(*) FROM pncp_reconciliacao_staging GROUP BY status"
            ).fetchall()}
            out["fila_pendentes_api"] = sum(filas.get(k, 0) for k in (
                "PENDENTE", "ERRO_TRANSITORIO", "ERRO", "AGUARDANDO_RATE_LIMIT"
            ))
            out["fila_404"] = filas.get("NAO_ENCONTRADA", 0)
            out["fila_410"] = filas.get("REMOVIDA_PNCP", 0)
            out["fila_erros"] = filas.get("ERRO", 0) + filas.get("ERRO_TRANSITORIO", 0)
            out["fila_resolvidas"] = sum(filas.get(k, 0) for k in (
                "RESOLVIDA", "JA_EXISTIA", "PAI_IMPORTADO"
            ))
        con.close()
    except Exception:
        pass
    return out


def status_reconciliacao_interface() -> dict:
    """Lê métricas operacionais do snapshot; a fila de retomada tem fallback
    barato no compras.db para funcionar já na primeira abertura da V0.7.6.
    """
    snap = obter_snapshot_interface()
    op = snap.get("operacional") if isinstance(snap, dict) else None
    if isinstance(op, dict):
        fila = (
            {
                "fila_pendentes_api": int(op.get("fila_pendentes_api") or 0),
                "fila_404": int(op.get("fila_404") or 0),
                "fila_410": int(op.get("fila_410") or 0),
                "fila_erros": int(op.get("fila_erros") or 0),
                "fila_resolvidas": int(op.get("fila_resolvidas") or 0),
            }
            if "fila_pendentes_api" in op
            else status_fila_reconciliacao_cache(str(DB_PATH))
        )
        return {
            "staging": int(op.get("staging") or 0),
            "pais": int(op.get("pais_staging") or 0),
            "quarentena": int(op.get("quarentena_itens") or 0),
            "itens_removidos_pncp": int(op.get("itens_removidos_pncp") or 0),
            "quarentena_contratacoes": int(op.get("quarentena_contratacoes") or 0),
            "pendentes_completude": int(op.get("pendentes_completude") or 0),
            **fila,
        }
    return status_reconciliacao_cache(str(DB_PATH))


@st.cache_data(ttl=60, show_spinner=False)
def status_fts_cache(db: str, fts: str, _assinatura: tuple[int, int]) -> dict:
    return fm.status_indice(Path(db), Path(fts))


@st.cache_data(ttl=60, show_spinner=False)
def status_tic_cache(db: str, tic: str, _assinatura: tuple[int, int]) -> dict:
    return tm.status_classificacao(Path(db), Path(tic))


def _url_teste_reconciliacao_pncp() -> tuple[str | None, str | None]:
    """Obtém uma contratação pendente e monta a mesma rota usada pela reconciliação."""
    try:
        con = sqlite3.connect(str(DB_PATH), timeout=5)
        row = None
        tem_fila = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='pncp_reconciliacao_staging'"
        ).fetchone()
        if tem_fila:
            row = con.execute(
                """SELECT numero_controle_pncp
                     FROM pncp_reconciliacao_staging
                    WHERE status IN ('PENDENTE','ERRO_TRANSITORIO','ERRO','AGUARDANDO_RATE_LIMIT')
                    ORDER BY qtd_itens DESC, numero_controle_pncp
                    LIMIT 1"""
            ).fetchone()
        # Reconciliação já encerrada: ainda precisamos conseguir testar /api/consulta.
        if not row:
            row = con.execute(
                """SELECT numero_controle_pncp
                     FROM contratacoes
                    WHERE COALESCE(pncp_confirmado,0)=1
                    ORDER BY COALESCE(data_publicacao_pncp,'') DESC
                    LIMIT 1"""
            ).fetchone()
        con.close()
    except Exception:
        return None, None
    if not row:
        return None, None
    pid = str(row[0])
    m = re.fullmatch(r"(\d{14})-\d+-(\d{6})/(\d{4})", pid)
    if not m:
        return pid, None
    cnpj, seq, ano = m.groups()
    return pid, f"https://pncp.gov.br/api/consulta/v1/orgaos/{cnpj}/compras/{ano}/{seq}"


def _url_teste_itens_pncp() -> tuple[str | None, str | None]:
    """Escolhe um número PNCP válido e monta a rota oficial da API de itens."""
    try:
        con = sqlite3.connect(str(DB_PATH), timeout=5)
        row = con.execute(
            """SELECT numero_controle_pncp
                 FROM contratacoes
                WHERE numero_controle_pncp GLOB '[0-9]*-[0-9]-[0-9]*/[0-9]*'
                ORDER BY CASE WHEN COALESCE(itens_coletados,0)=0 THEN 0 ELSE 1 END,
                         COALESCE(data_publicacao_pncp,'') DESC
                LIMIT 1"""
        ).fetchone()
        con.close()
    except Exception:
        return None, None
    if not row:
        return None, None
    pid = str(row[0])
    m = re.fullmatch(r"(\d{14})-\d+-(\d{6})/(\d{4})", pid)
    if not m:
        return pid, None
    cnpj, seq, ano = m.groups()
    return pid, f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{int(seq)}/itens"


def _render_resultado_health(nome: str, resultado: dict) -> None:
    estado = str(resultado.get("estado") or "ERRO")
    detalhe = str(resultado.get("detalhe") or "Sem detalhe")
    seg = float(resultado.get("segundos") or 0)
    msg = f"**{nome}: {estado}** · {detalhe} · {seg:.2f} s"
    if estado in ("OK", "RESPONDENDO"):
        st.success(msg)
    elif estado == "LIMITADO":
        st.warning(msg)
    else:
        st.error(msg)


def _testar_api_pncp(url: str) -> tuple[int | None, float, str | None]:
    inicio = time.perf_counter()
    try:
        resp = requests.get(
            url,
            timeout=(10, 30),
            headers={
                "Accept": "application/json",
                "Accept-Language": "pt-BR,pt;q=0.9",
                "User-Agent": "RADAR_PNCP/0.6.6.4 health-check",
            },
        )
        return int(resp.status_code), time.perf_counter() - inicio, None
    except Exception as exc:
        return None, time.perf_counter() - inicio, f"{type(exc).__name__}: {exc}"


def brl(valor) -> str:
    if valor is None:
        return "—"
    try:
        return "R$ " + f"{float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except (TypeError, ValueError):
        return "—"


def data_br(valor: str | None) -> str:
    if not valor:
        return "—"
    d = iso_para_data(valor)
    return d.strftime("%d/%m/%Y") if d else str(valor)


def texto_curto(texto: str | None, limite: int = 260) -> str:
    texto = " ".join((texto or "").split())
    if len(texto) <= limite:
        return texto or "—"
    return texto[: limite - 1].rstrip() + "…"


def termos_pesquisa(texto: str, modo: str) -> list[str]:
    texto = " ".join((texto or "").strip().split())
    if not texto:
        return []
    return [texto.casefold()] if modo == "frase" else [x.casefold() for x in texto.split()]


def campos_correspondentes(registro: dict, itens_hits: list[dict], texto: str, modo: str) -> list[str]:
    termos = termos_pesquisa(texto, modo)
    if not termos:
        return []

    candidatos = [
        ("Objeto", registro.get("objeto")),
        ("Informações complementares", registro.get("informacao_complementar")),
        ("Órgão", registro.get("orgao_razao_social")),
        ("Unidade", registro.get("unidade_nome")),
        ("Processo", registro.get("processo")),
        ("Número da compra", registro.get("numero_compra")),
        ("Número de controle PNCP", registro.get("numero_controle_pncp")),
    ]
    encontrados: list[str] = []
    for nome, valor in candidatos:
        v = (str(valor) if valor is not None else "").casefold()
        if any(t in v for t in termos):
            encontrados.append(nome)
    if itens_hits:
        nums = ", ".join(str(i.get("numero_item")) for i in itens_hits[:3])
        encontrados.append(f"Item(ns) {nums}")
    return encontrados


def status_legivel(valor: str | None) -> str:
    mapa = {
        "OK": "Itens coletados",
        "PENDENTE": "Pendente",
        "ERRO_TEMPORARIO": "Erro temporário / retry",
        "AGUARDANDO_RATE_LIMIT": "Aguardando limite do PNCP",
        "INDISPONIVEL_PNCP": "Indisponível no PNCP",
    }
    return mapa.get(valor or "", valor or "—")


def fechar_detalhe() -> None:
    st.session_state.pop('detalhe_id',None)


@st.dialog('Detalhes da contratação',width='large',on_dismiss=fechar_detalhe)
def mostrar_detalhes(numero_controle: str) -> None:
    registro = obter_contratacao(DB_PATH, numero_controle)
    if not registro:
        st.error("A contratação selecionada não foi encontrada no banco.")
        return

    itens = obter_itens_contratacao(DB_PATH, numero_controle)

    topo = st.container()
    with topo:
        c1, c2 = st.columns([8, 1])
        with c1:
            st.caption(numero_controle)
        with c2:
            if st.button("Fechar", key="fechar_detalhe", use_container_width=True):
                st.session_state.pop("detalhe_id", None)
                st.rerun()

        st.markdown(f"**{registro.get('orgao_razao_social') or 'Órgão não informado'}**")
        st.write(registro.get("objeto") or "Objeto não informado")

        with st.container(border=True, key="radar_pncp_detalhes_resumo"):
            m1, m2, m3, m4 = st.columns([1.1, 1.5, 1.5, .8])
            m1.metric("Publicação", data_br(registro.get("data_publicacao_pncp")))
            m2.metric("Valor estimado", brl(registro.get("valor_total_estimado")))
            with m3:
                ui.dado_resumo("Modalidade", registro.get("modalidade_nome") or "—")
            m4.metric("Itens na base", len(itens))

        aba_contratacao, aba_itens, aba_resultados = st.tabs(["Contratação", "Itens", "Resultados homologados"])
        with aba_contratacao:
            a, b = st.columns(2)
            with a:
                st.markdown("**Identificação**")
                st.write(f"Número da compra: {registro.get('numero_compra') or '—'}")
                st.write(f"Ano: {registro.get('ano_compra') or '—'}")
                st.write(f"Processo: {registro.get('processo') or '—'}")
                st.write(f"Situação: {registro.get('situacao_nome') or '—'}")
                st.write(f"Status dos itens: {status_legivel(registro.get('itens_status_exibicao'))}")
                if registro.get("fonte_ultima"):
                    fonte = "PNCP (API)" if registro.get("fonte_ultima") == "PNCP_API" else "Compras.gov (CSV)"
                    st.write(f"Fonte mais recente: {fonte}")
                if registro.get("itens_fonte"):
                    fonte_itens = "PNCP (API)" if registro.get("itens_fonte") == "PNCP_API" else "Compras.gov (CSV)"
                    st.write(f"Fonte dos itens: {fonte_itens}")
            with b:
                st.markdown("**Local / unidade**")
                st.write(f"Unidade: {registro.get('unidade_nome') or '—'}")
                st.write(f"Município: {registro.get('municipio_nome') or '—'}")
                st.write(f"UF: {registro.get('uf_sigla') or '—'}")
                st.write(f"CNPJ do órgão: {registro.get('orgao_cnpj') or '—'}")

            st.markdown("**Datas**")
            d1, d2, d3 = st.columns(3)
            d1.write(f"Abertura: {data_br(registro.get('data_abertura_proposta'))}")
            d2.write(f"Encerramento: {data_br(registro.get('data_encerramento_proposta'))}")
            d3.write(f"Atualização PNCP: {data_br(registro.get('data_atualizacao'))}")

            if registro.get("informacao_complementar"):
                st.markdown("**Informações complementares**")
                st.write(registro["informacao_complementar"])

            link_cols = st.columns(2)
            if registro.get("url_pncp"):
                link_cols[0].link_button("Abrir no PNCP", registro["url_pncp"], use_container_width=True)
            if registro.get("link_sistema_origem"):
                link_cols[1].link_button(
                    "Abrir no sistema de origem",
                    registro["link_sistema_origem"],
                    use_container_width=True,
                )

        with aba_resultados:
            import painel_fornecedores
            painel_fornecedores.mostrar_da_contratacao(DB_PATH, numero_controle)

        with aba_itens:
            status = registro.get("itens_status_exibicao")
            if status == "INDISPONIVEL_PNCP":
                st.warning(
                    "O endpoint de itens desta publicação está indisponível no PNCP. "
                    "O registro histórico foi preservado pelo Radar PNCP."
                )
            if not itens:
                st.info("Nenhum item armazenado para esta contratação.")
            else:
                df = pd.DataFrame(itens)
                renomear = {
                    "numero_item": "Item",
                    "descricao": "Descrição",
                    "informacao_complementar": "Informação complementar",
                    "material_ou_servico_nome": "Material/serviço",
                    "quantidade": "Quantidade",
                    "unidade_medida": "Unidade",
                    "valor_unitario_estimado": "Valor unitário estimado",
                    "valor_total": "Valor total",
                    "situacao_item_nome": "Situação",
                    "criterio_julgamento_nome": "Critério de julgamento",
                    "catalogo_codigo_item": "Código catálogo",
                    "catalogo_nome": "Catálogo",
                    "categoria_catalogo_nome": "Categoria catálogo",
                    "ncm_nbs_codigo": "NCM/NBS",
                    "ncm_nbs_descricao": "Descrição NCM/NBS",
                    "tem_resultado": "Tem resultado",
                    "data_inclusao": "Inclusão",
                    "data_atualizacao": "Atualização",
                }
                df = df.rename(columns=renomear)
                import cestas
                from painel_cestas import selecionar
                selecionar([cestas.referencia_item(registro,i,cestas.agora()) for i in itens],
                           'estimados_'+numero_controle)
                with st.expander('Mais informações dos itens'):
                    st.dataframe(df,use_container_width=True,hide_index=True,height=360)
                st.caption('Referências desta aba usam valores estimados. Para selecionar preços homologados e fornecedores, abra Resultados homologados.')



def formatar_status_job(status: str | None) -> str:
    mapa = {
        "AGUARDANDO": "Aguardando início",
        "EXECUTANDO": "Executando",
        "PARADA_SOLICITADA": "Parada solicitada",
        "CONCLUIDA": "Concluída",
        "CONCLUIDA_COM_PENDENCIAS_EXTERNAS": "Concluída com pendências externas",
        "PARCIAL": "Parcial",
        "INTERROMPIDA": "Interrompida inesperadamente",
        "INTERROMPIDA_USUARIO": "Interrompida pelo usuário",
        "ERRO": "Erro",
    }
    return mapa.get(status or "", status or "—")


def _data_hora_br(valor: str | None) -> str:
    if not valor:
        return "—"
    try:
        dt = pd.to_datetime(valor)
        return dt.strftime("%d/%m/%Y %H:%M:%S")
    except Exception:
        return str(valor)


def _mes_br(valor: str | None) -> str:
    if not valor:
        return "—"
    try:
        y, m = str(valor)[:7].split("-")
        return f"{m}/{y}"
    except Exception:
        return str(valor)


def _periodo_job(j: dict) -> str:
    if j.get("tipo") == jm.TIPO_CARGA_HISTORICA:
        return f"{data_br(j.get('data_inicio'))} → {data_br(j.get('data_fim'))}"
    if j.get("tipo") == jm.TIPO_CARGA_CSV_COMPRASGOV:
        return f"{_mes_br(j.get('data_inicio'))} → {_mes_br(j.get('data_fim'))}"
    if j.get("tipo") == jm.TIPO_CARGA_BASE_CSV_COMPRASGOV:
        return "2021 → 2024 + deltas 2025 existentes"
    if j.get("tipo") == jm.TIPO_CARGA_2026_COMPRASGOV:
        return "2026 · contratações + itens + resultados"
    if j.get("tipo") in (jm.TIPO_API_COMPRASGOV_INCREMENTAL, jm.TIPO_COLETA_MULTIFONTE_INCREMENTAL):
        return f"{data_br(j.get('data_inicio'))} → {data_br(j.get('data_fim'))}"
    if j.get("tipo") == jm.TIPO_ATUALIZACAO_DIARIA:
        op = str(j.get("operacao") or "DIARIO_ATUALIZACAO_3D")
        prefixo = "Revisão 30d" if op.startswith("REVISAO_") else "Retomada" if op.startswith("RETOMADA_") else "Diária"
        return f"{prefixo} · {data_br(j.get('data_inicio'))} → {data_br(j.get('data_fim'))}"
    if j.get("tipo") == jm.TIPO_RECONCILIAR_STAGING_PNCP:
        return "Staging Compras.gov → PNCP"
    if j.get("tipo") == jm.TIPO_VALIDAR_COMPLETUDE_PNCP:
        limite = j.get("operacao") or "2000"
        return "Sem limite" if str(limite) == "0" else f"Até {int(limite):,} contratações".replace(",", ".")
    if j.get("tipo") == jm.TIPO_ITENS_INCREMENTAL:
        partes = str(j.get("operacao") or "1000:2000:0").split(":")
        return f"Até {int(partes[0]):,} contratos / {int(partes[1]):,} páginas".replace(",", ".")
    if j.get("tipo") == jm.TIPO_ITENS_HIBRIDO:
        partes = str(j.get("operacao") or "5000:6:95:").split(":")
        return f"Híbrido · até {int(partes[0] or 5000):,} contratos · {int(partes[2] or 95) if len(partes)>2 else 95} req/min".replace(",", ".")
    if j.get("tipo") == jm.TIPO_ITENS_HIBRIDO_PILOTO:
        partes = str(j.get("operacao") or "500:6").split(":")
        return f"Piloto · até {int(partes[0] or 500):,} contratos · {int(partes[1] or 6) if len(partes)>1 else 6} workers".replace(",", ".")
    if j.get("tipo") == jm.TIPO_ITENS_MUTIRAO:
        partes = str(j.get("operacao") or "2000:4000:0030:60:0").split(":")
        hc = str(partes[2] if len(partes) > 2 else "0030").zfill(4)
        sem_limite = bool(int(partes[4] or 0)) if len(partes) > 4 else False
        deadline_compacto = str(partes[5] or "") if len(partes) > 5 else ""
        if sem_limite:
            janela = "até esvaziar"
        elif deadline_compacto:
            try:
                dd = datetime.strptime(deadline_compacto, "%Y%m%dT%H%M%S")
                janela = f"até {dd:%d/%m %H:%M} (COLETA prioritária)"
            except ValueError:
                janela = f"até janela da COLETA {hc[:2]}:{hc[2:4]}"
        else:
            janela = f"até janela da COLETA {hc[:2]}:{hc[2:4]}"
        return f"Mutirão · lotes {int(partes[0]):,}/{int(partes[1]):,} · {janela}".replace(",", ".")
    if j.get("tipo") in (jm.TIPO_INDICE_FTS, jm.TIPO_CLASSIFICACAO_TIC):
        return "Construção" if j.get("operacao") == "build" else "Atualização"
    return data_br(j.get("data_referencia"))


def _tipo_job_legivel(tipo: str) -> str:
    return {
        jm.TIPO_COLETA_DIARIA: "Coleta diária",
        jm.TIPO_CARGA_HISTORICA: "Carga histórica PNCP",
        jm.TIPO_CARGA_CSV_COMPRASGOV: "Deltas mensais Compras.gov",
        jm.TIPO_CARGA_BASE_CSV_COMPRASGOV: "Carga-base Compras.gov",
        jm.TIPO_CARGA_2026_COMPRASGOV: "Carga massiva Compras.gov 2026",
        jm.TIPO_API_COMPRASGOV_INCREMENTAL: "COLETA incremental Compras.gov API (legado)",
        jm.TIPO_COLETA_MULTIFONTE_INCREMENTAL: "Recuperação de período",
        jm.TIPO_ATUALIZACAO_DIARIA: "Atualização diária ou revisão · COLETA",
        jm.TIPO_RECONCILIAR_STAGING_PNCP: "Reconciliação PNCP",
        jm.TIPO_VALIDAR_COMPLETUDE_PNCP: "Validação de completude PNCP",
        jm.TIPO_ITENS_INCREMENTAL: "Fila incremental de itens",
        jm.TIPO_ITENS_MUTIRAO: "Fallback PNCP de itens",
        jm.TIPO_ITENS_HIBRIDO: "Coleta de itens em segundo plano",
        jm.TIPO_ITENS_HIBRIDO_PILOTO: "Piloto híbrido de itens",
        jm.TIPO_RESULTADOS_PNCP: "Coleta de resultados por item",
        jm.TIPO_INDICE_FTS: "Índice de pesquisa",
        jm.TIPO_CLASSIFICACAO_TIC: "Classificação TIC",
    }.get(tipo, tipo)


def _render_job_diario(job: dict) -> None:
    progresso = jm.obter_progresso_data(
        Path(job["db_compras"]),
        job["data_referencia"],
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Status", formatar_status_job(job["status"]))
    c2.metric("Data", data_br(job["data_referencia"]))
    c3.metric("Modalidades", f"{progresso['modalidades_concluidas']}/14")
    resolvidos = progresso["com_itens"] + progresso["indisponiveis"]
    c4.metric("Contratações resolvidas", f"{resolvidos}/{progresso['contratacoes']}")

    st.progress(
        min(1.0, progresso["modalidades_concluidas"] / 14.0),
        text=(
            "Coleta das modalidades: "
            f"{progresso['modalidades_concluidas']} de 14 concluídas"
        ),
    )

    total = progresso["contratacoes"]
    frac_itens = min(1.0, resolvidos / total) if total else 0.0
    st.progress(
        frac_itens,
        text=(
            f"Coleta dos itens: {resolvidos} de {total} contratações resolvidas"
            if total
            else "Coleta dos itens: aguardando contratações"
        ),
    )

    p1, p2, p3, p4 = st.columns(4)
    p1.metric("Contratações", f"{total:,}".replace(",", "."))
    p2.metric("Com itens", f"{progresso['com_itens']:,}".replace(",", "."))
    p3.metric("Indisponíveis PNCP", f"{progresso['indisponiveis']:,}".replace(",", "."))
    p4.metric("Itens armazenados", f"{progresso['itens_armazenados']:,}".replace(",", "."))


def _render_job_historico(job: dict) -> None:
    resumo = jm.obter_resumo_job_historico(job["id"], JOBS_DB_PATH)
    total = resumo["total"]
    resolvidos = resumo["resolvidos"]

    status_tipo = (
        "ok" if job["status"] in ("CONCLUIDA", "CONCLUIDA_COM_PENDENCIAS_EXTERNAS")
        else "info" if job["status"] in ("AGUARDANDO", "EXECUTANDO")
        else "warn" if job["status"] in ("PARCIAL", "PARADA_SOLICITADA")
        else "error"
    )
    st.markdown(
        ui.status_badge(formatar_status_job(job["status"]), status_tipo),
        unsafe_allow_html=True,
    )
    st.caption(
        f"Período: {data_br(job['data_inicio'])} → {data_br(job['data_fim'])}"
    )
    if job.get("etapa"):
        st.caption(f"Etapa: {job['etapa']}")

    c1, c2, c3 = st.columns(3)
    c1.metric("Dias concluídos", f"{resolvidos}/{total}")
    c2.metric("Dias incompletos", resumo["parciais"])
    c3.metric("Dias restantes", max(0, total - resolvidos))

    frac = min(1.0, resolvidos / total) if total else 0.0
    st.progress(
        frac,
        text=f"Período: {resolvidos} de {total} dia(s) concluído(s)",
    )

    atual = job.get("data_atual")
    if atual:
        st.markdown(f"#### Dia atual · {data_br(atual)}")
        progresso = jm.obter_progresso_data(Path(job["db_compras"]), atual)
        resolvidos_dia = progresso["com_itens"] + progresso["indisponiveis"]

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Modalidades do dia", f"{progresso['modalidades_concluidas']}/14")
        c2.metric("Contratações", f"{progresso['contratacoes']:,}".replace(",", "."))
        c3.metric("Com itens", f"{progresso['com_itens']:,}".replace(",", "."))
        c4.metric("Itens armazenados", f"{progresso['itens_armazenados']:,}".replace(",", "."))

        st.progress(
            min(1.0, progresso["modalidades_concluidas"] / 14.0),
            text=(
                f"{data_br(atual)} — modalidades: "
                f"{progresso['modalidades_concluidas']} de 14"
            ),
        )
        total_dia = progresso["contratacoes"]
        frac_dia = min(1.0, resolvidos_dia / total_dia) if total_dia else 0.0
        st.progress(
            frac_dia,
            text=(
                f"{data_br(atual)} — itens: "
                f"{resolvidos_dia} de {total_dia} contratações resolvidas"
                if total_dia
                else f"{data_br(atual)} — aguardando contratações"
            ),
        )
    else:
        st.caption("Aguardando o worker selecionar o próximo dia do período.")



def _render_job_csv(job: dict) -> None:
    resumo = jm.obter_resumo_job_csv(job["id"], JOBS_DB_PATH)
    total = resumo["total"]
    concluidos = resumo["concluidos"]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Status", formatar_status_job(job["status"]))
    c2.metric("Período", _periodo_job(job))
    c3.metric("Meses concluídos", f"{concluidos}/{total}")
    c4.metric("Tamanho da base", _fmt_bytes(_tamanho_total_sqlite(DB_PATH)))
    etapas_atual = int(job.get("progresso_atual") or 0)
    etapas_total = int(job.get("progresso_total") or 0)
    if etapas_total:
        st.progress(
            min(1.0, etapas_atual / etapas_total),
            text=(
                f"{job.get('etapa') or 'Carga em duas fases'}: "
                f"{etapas_atual} de {etapas_total} etapas mensais"
            ),
        )
    else:
        st.progress(min(1.0, concluidos / total) if total else 0.0,
                    text=f"Carga mensal: {concluidos} de {total} mês(es) concluído(s)")

    atual = job.get("data_atual")
    if atual:
        linha = next((r for r in resumo["meses"] if r["ano_mes"] == atual), None)
        if linha:
            st.markdown(f"#### Mês atual · {_mes_br(atual)}")
            unidade = linha.get("unidade_progresso")
            a = int(linha.get("progresso_atual") or 0)
            t = int(linha.get("progresso_total") or 0)
            frac = min(1.0, a / t) if t else 0.0
            if unidade == "bytes":
                texto = f"{linha.get('etapa') or 'Download'}: {_fmt_bytes(a)} de {_fmt_bytes(t)}" if t else (linha.get('etapa') or 'Download')
            elif unidade == "linhas":
                texto = f"{linha.get('etapa') or 'Importação'}: {a:,} de {t:,} linhas".replace(",", ".") if t else (linha.get('etapa') or 'Importação')
            elif unidade == "contratacoes":
                texto = f"{linha.get('etapa') or 'Consolidação'}: {a:,} de {t:,} contratações".replace(",", ".") if t else (linha.get('etapa') or 'Consolidação')
            else:
                texto = linha.get("etapa") or "Preparando"
            st.progress(frac, text=texto)
            if linha.get("mensagem"):
                st.caption(linha["mensagem"])

    if resumo["meses"]:
        recentes = resumo["meses"][-12:]
        tabela = []
        for r in recentes:
            tabela.append({
                "Mês": _mes_br(r["ano_mes"]),
                "Status": formatar_status_job(r["status"]),
                "Etapa": r.get("etapa") or "—",
                "Mensagem": r.get("mensagem") or "",
            })
        with st.expander("Situação dos meses", expanded=False):
            st.dataframe(pd.DataFrame(tabela), use_container_width=True, hide_index=True)


def _segundos_job(job: dict, fallback: float = 0.0) -> float:
    try:
        ini = datetime.fromisoformat(str(job.get("iniciado_em") or ""))
        return max(float(fallback or 0.0), (datetime.now() - ini).total_seconds())
    except Exception:
        return max(0.0, float(fallback or 0.0))


def _eta_por_fracao(elapsed: float, frac: float) -> float | None:
    if elapsed <= 0 or frac <= 0.01 or frac >= 1.0:
        return 0.0 if frac >= 1.0 else None
    return max(0.0, elapsed * (1.0 - frac) / frac)


def _render_job_itens(job: dict) -> None:
    """Painel operacional para lote manual e mutirão de itens.

    O heartbeat é gravado na própria linha do job, portanto a interface não
    precisa varrer compras.db nem interpretar milhares de linhas do log.
    """
    p = ii.decodificar_progresso(job.get("mensagem")) or {}
    eh_mutirao = job.get("tipo") == jm.TIPO_ITENS_MUTIRAO or p.get("kind") == "itens_mutirao"

    if eh_mutirao:
        partes = str(job.get("operacao") or "5000:8000:0030:60:0").split(":")
        lim_c = int(p.get("lote_contratos_limite") or partes[0] or 5000)
        lim_p = int(p.get("lote_paginas_limite") or partes[1] or 8000)
        atual_c = int(p.get("lote_contratos_processados") or 0)
        alvo_c = int(p.get("lote_contratos_selecionados") or lim_c)
        atual_p = int(p.get("lote_paginas_processadas") or 0)
        lote_num = int(p.get("lote_num") or 0)
        sessao_conf = int(p.get("sessao_conjuntos_confirmados") or 0)
        sessao_pag = int(p.get("sessao_paginas") or 0)
        sessao_itens = int(p.get("sessao_itens") or 0)
        sessao_erros = int(p.get("sessao_erros") or 0)
        backlog_ini = int(p.get("backlog_inicial") or job.get("progresso_total") or 0)
        backlog_rest = int(p.get("backlog_restante_estimado") if p.get("backlog_restante_estimado") is not None else max(0, backlog_ini - sessao_conf))
        elapsed = _segundos_job(job, float(p.get("elapsed_seconds") or 0.0))
        lote_elapsed = float(p.get("lote_elapsed_seconds") or 0.0)

        c1,c2,c3,c4,c5 = st.columns(5)
        c1.metric("Status", formatar_status_job(job.get("status")))
        c2.metric("Lote atual", f"#{lote_num}" if lote_num else "Preparando")
        c3.metric("Sessão · conjuntos", f"{sessao_conf:,}".replace(",", "."))
        c4.metric("Backlog estimado", f"{backlog_rest:,}".replace(",", "."))
        c5.metric("Tempo da sessão", _fmt_duracao(elapsed))

        if backlog_ini:
            frac_sessao = min(1.0, max(0.0, sessao_conf / backlog_ini))
            st.progress(frac_sessao, text=f"Mutirão nesta sessão: {sessao_conf:,} de {backlog_ini:,} conjuntos ({100*frac_sessao:.2f}%)".replace(",", "."))

        frac_c = min(1.0, atual_c / max(1, alvo_c))
        frac_p = min(1.0, atual_p / max(1, lim_p))
        st.progress(frac_c, text=f"Lote #{lote_num} · contratações: {atual_c:,} de {alvo_c:,} ({100*frac_c:.1f}%)".replace(",", "."))
        st.progress(frac_p, text=f"Lote #{lote_num} · páginas: {atual_p:,} de {lim_p:,} ({100*frac_p:.1f}%)".replace(",", "."))

        frac_fim = max(frac_c, frac_p)
        eta = _eta_por_fracao(lote_elapsed, frac_fim)
        velocidade = atual_c / (lote_elapsed / 60.0) if lote_elapsed >= 10 and atual_c else 0.0
        d1,d2,d3,d4,d5 = st.columns(5)
        d1.metric("Itens nesta sessão", f"{sessao_itens:,}".replace(",", "."))
        d2.metric("Páginas nesta sessão", f"{sessao_pag:,}".replace(",", "."))
        d3.metric("Erros recuperáveis", f"{sessao_erros:,}".replace(",", "."))
        d4.metric("Velocidade do lote", f"{velocidade:.1f} contr./min".replace(".", ",") if velocidade else "—")
        d5.metric("ETA do lote", _fmt_duracao(eta))
        limitante = "contratações" if frac_c >= frac_p else "páginas"
        deadline = p.get("deadline")
        detalhe = f"Provável limite de encerramento do lote: **{limitante}**."
        if deadline:
            try:
                dt = datetime.fromisoformat(str(deadline))
                detalhe += f" Pausa de segurança prevista até **{dt:%d/%m/%Y %H:%M}**."
            except Exception:
                pass
        st.caption(detalhe)
        if p.get("contrato_atual"):
            st.caption(f"Contrato atual: `{p.get('contrato_atual')}`" + (f" · página {p.get('pagina_atual')}" if p.get("pagina_atual") else ""))
        return

    # Lote manual. Se ainda não houve heartbeat estruturado, os limites vêm da operação.
    partes = str(job.get("operacao") or "1000:2000:0").split(":")
    lim_c = int(p.get("contratos_limite") or partes[0] or 1000)
    lim_p = int(p.get("paginas_limite") or partes[1] or 2000)
    atual_c = int(p.get("contratos_processados") or job.get("progresso_atual") or 0)
    alvo_c = int(p.get("contratos_selecionados") or job.get("progresso_total") or lim_c)
    if alvo_c <= 1 and atual_c == 0:
        alvo_c = lim_c
    atual_p = int(p.get("paginas_processadas") or 0)
    conjuntos = int(p.get("conjuntos_confirmados") or 0)
    itens = int(p.get("itens_confirmados") or 0)
    erros = int(p.get("erros_recuperaveis") or 0)
    elapsed = _segundos_job(job, float(p.get("elapsed_seconds") or 0.0))
    frac_c = min(1.0, atual_c / max(1, alvo_c))
    frac_p = min(1.0, atual_p / max(1, lim_p))
    frac_fim = max(frac_c, frac_p)
    eta = _eta_por_fracao(elapsed, frac_fim)
    velocidade = atual_c / (elapsed / 60.0) if elapsed >= 10 and atual_c else 0.0

    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Status", formatar_status_job(job.get("status")))
    c2.metric("Contratações", f"{atual_c:,}/{alvo_c:,}".replace(",", "."))
    c3.metric("Páginas PNCP", f"{atual_p:,}/{lim_p:,}".replace(",", "."))
    c4.metric("Tamanho da base", _fmt_bytes(_tamanho_total_sqlite(DB_PATH)))
    st.progress(frac_c, text=f"Contratações: {atual_c:,} de {alvo_c:,} ({100*frac_c:.1f}%)".replace(",", "."))
    st.progress(frac_p, text=f"Páginas: {atual_p:,} de {lim_p:,} ({100*frac_p:.1f}%)".replace(",", "."))

    d1,d2,d3,d4,d5 = st.columns(5)
    d1.metric("Conjuntos confirmados", f"{conjuntos:,}".replace(",", "."))
    d2.metric("Itens PNCP", f"{itens:,}".replace(",", "."))
    d3.metric("Erros recuperáveis", f"{erros:,}".replace(",", "."))
    d4.metric("Velocidade", f"{velocidade:.1f} contr./min".replace(".", ",") if velocidade else "—")
    d5.metric("Estimativa restante", _fmt_duracao(eta))
    limitante = "contratações" if frac_c >= frac_p else "páginas"
    backlog_ini = int(p.get("backlog_inicial") or 0)
    backlog_est = max(0, backlog_ini - conjuntos) if backlog_ini else None
    detalhe = f"Provável limite de encerramento: **{limitante}** · tempo decorrido: **{_fmt_duracao(elapsed)}**."
    if backlog_est is not None:
        detalhe += f" Backlog estimado após confirmações deste lote: **{backlog_est:,}**.".replace(",", ".")
    st.caption(detalhe)
    if p.get("contrato_atual"):
        st.caption(f"Contrato atual: `{p.get('contrato_atual')}`" + (f" · página {p.get('pagina_atual')}" if p.get("pagina_atual") else ""))


def _render_job_generico(job: dict) -> None:
    """Renderiza jobs que usam apenas o progresso genérico da tabela jobs.

    É usado por jobs novos que não possuem data_referencia diária. Mantém a
    interface resiliente: um tipo de job desconhecido não deve cair no renderer
    de coleta diária e tentar converter None com date.fromisoformat().
    """
    atual = int(job.get("progresso_atual") or 0)
    total = int(job.get("progresso_total") or 0)
    frac = min(1.0, atual / total) if total else 0.0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Status", formatar_status_job(job.get("status")))
    c2.metric("Escopo", _periodo_job(job))
    c3.metric("Etapa", job.get("etapa") or "Preparando")
    c4.metric("Tamanho da base", _fmt_bytes(_tamanho_total_sqlite(DB_PATH)))

    if job.get("tipo") == jm.TIPO_ATUALIZACAO_DIARIA:
        texto = job.get("mensagem") or job.get("etapa") or "COLETA em execução"
        resultados = progresso_resultados(texto)
        if resultados:
            st.progress(resultados["fracao"], text=f"Resultados: {resultados['atual']} de {resultados['total']} dias · {data_br(resultados['dia'])}")
            st.caption("Progresso desta etapa. A atualização de contratações e a coleta de resultados podem consultar períodos diferentes.")
        else:
            st.info(texto)
            st.caption("A execução passa por contratações, resultados e finalização. A duração depende de cada etapa.")
    else:
        texto = job.get("etapa") or _tipo_job_legivel(job.get("tipo")) or "Processando"
        if total:
            texto += f": {atual:,} de {total:,}".replace(",", ".")
        st.progress(frac, text=texto)
        if job.get("mensagem"):
            st.caption(job["mensagem"])


def _render_job_base_csv(job: dict) -> None:
    atual = int(job.get("progresso_atual") or 0)
    total = int(job.get("progresso_total") or 0)
    frac = min(1.0, atual / total) if total else 0.0
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Status", formatar_status_job(job["status"]))
    c2.metric("Base", "2021–2024")
    c3.metric("Etapa", job.get("etapa") or "Preparando")
    c4.metric("Tamanho da base", _fmt_bytes(_tamanho_total_sqlite(DB_PATH)))
    st.progress(frac, text=(job.get("etapa") or "Carga-base") + (f": {atual:,} de {total:,}".replace(",", ".") if total else ""))
    if job.get("mensagem"):
        st.caption(job["mensagem"])


def _fmt_duracao(segundos) -> str:
    if segundos is None:
        return "—"
    segundos = max(0, int(segundos))
    dias, resto = divmod(segundos, 86400)
    horas, resto = divmod(resto, 3600)
    minutos, _ = divmod(resto, 60)
    if dias:
        return f"{dias}d {horas}h"
    if horas:
        return f"{horas}h {minutos}min"
    return f"{minutos}min"


def _tamanho_total_sqlite(path: Path) -> int:
    total = 0
    for p in (path, Path(str(path) + "-wal"), Path(str(path) + "-shm")):
        try:
            total += p.stat().st_size
        except OSError:
            pass
    return total


def _fmt_bytes(valor: int) -> str:
    n = float(valor)
    for unidade in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unidade == "TB":
            return f"{n:.1f} {unidade}".replace(".", ",")
        n /= 1024
    return f"{n:.1f} TB".replace(".", ",")


def _render_job_progresso(job: dict) -> None:
    atual = int(job.get("progresso_atual") or 0)
    total = int(job.get("progresso_total") or 0)
    frac = min(1.0, atual / total) if total else 0.0
    nome = "FTS5" if job["tipo"] == jm.TIPO_INDICE_FTS else "Classificação TIC"
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Status", formatar_status_job(job["status"]))
    c2.metric("Operação", "Construção" if job.get("operacao") == "build" else "Atualização")
    c3.metric("Etapa", job.get("etapa") or "Preparando")
    c4.metric("Progresso", f"{100*frac:.1f}%".replace(".", ","))
    st.progress(frac, text=f"{nome}: {atual:,} de {total:,} registros".replace(",", "."))


def mostrar_indice_pesquisa(ativo: dict | None) -> None:
    st.markdown("### Índice de pesquisa")
    st.caption(
        "O FTS5 cria um índice textual separado do compras.db para acelerar "
        "buscas em milhões de contratações e itens."
    )

    status = status_fts_cache(str(DB_PATH), str(DB_FTS), _assinatura_arquivo(DB_FTS))
    estado = status["estado"]
    mapa = {
        "ATUALIZADO": ("Atualizado", "ok"),
        "DESATUALIZADO": ("Desatualizado", "warn"),
        "NAO_CONSTRUIDO": ("Não construído", "info"),
        "INDISPONIVEL": ("FTS5 indisponível", "error"),
        "ERRO": ("Erro", "error"),
        "INCOMPLETO": ("Incompleto", "warn"),
    }
    rotulo, tipo = mapa.get(estado, (estado, "info"))
    st.markdown(ui.status_badge(rotulo, tipo), unsafe_allow_html=True)
    st.write(status["mensagem"])

    i1, i2, i3, i4 = st.columns(4)
    i1.metric("Contratações indexadas", f"{status['contratacoes_indexadas']:,}".replace(",", "."))
    i2.metric("Itens indexados", f"{status['itens_indexados']:,}".replace(",", "."))
    i3.metric("Tamanho do índice", _fmt_bytes(status["tamanho_bytes"]))
    i4.metric("Última atualização", _data_hora_br(status["atualizado_em"]))

    if ativo:
        st.warning(
            f"O execução #{ativo['id']} ({_tipo_job_legivel(ativo['tipo'])}) está ativo. "
            "A construção/atualização do FTS5 fica bloqueada para não disputar "
            "I/O com a coleta."
        )

    b1, b2, b3 = st.columns([1, 1, 2])
    pode = ativo is None and status["fts5_disponivel"]

    if not status["existe"]:
        if b1.button(
            "Construir índice FTS5",
            type="primary",
            disabled=not pode,
            use_container_width=True,
            key="fts_build",
        ):
            try:
                jid = jm.iniciar_indice_fts(
                    "build", DB_PATH, DB_FTS, JOBS_DB_PATH, LOG_DIR
                )
                st.success(f"Construção do índice iniciada — execução #{jid}.")
                st.rerun()
            except Exception as exc:
                st.error(f"Não foi possível iniciar: {exc}")
    else:
        if b1.button(
            "Atualizar novos registros",
            type="primary",
            disabled=not pode or estado == "ATUALIZADO",
            use_container_width=True,
            key="fts_update",
        ):
            try:
                jid = jm.iniciar_indice_fts(
                    "update", DB_PATH, DB_FTS, JOBS_DB_PATH, LOG_DIR
                )
                st.success(f"Atualização do índice iniciada — execução #{jid}.")
                st.rerun()
            except Exception as exc:
                st.error(f"Não foi possível iniciar: {exc}")

        if b2.button(
            "Reconstruir índice",
            disabled=not pode,
            use_container_width=True,
            key="fts_rebuild",
        ):
            st.session_state["confirmar_rebuild_fts"] = True

        if st.session_state.get("confirmar_rebuild_fts"):
            st.warning(
                "Reconstruir apaga e recria o índice de pesquisa. O compras.db "
                "não é alterado."
            )
            c1, c2 = st.columns([1, 1])
            if c1.button("Confirmar reconstrução", type="primary", key="fts_rebuild_confirm"):
                try:
                    jid = jm.iniciar_indice_fts(
                        "build", DB_PATH, DB_FTS, JOBS_DB_PATH, LOG_DIR
                    )
                    st.session_state.pop("confirmar_rebuild_fts", None)
                    st.success(f"Reconstrução iniciada — execução #{jid}.")
                    st.rerun()
                except Exception as exc:
                    st.error(f"Não foi possível iniciar: {exc}")
            if c2.button("Cancelar", key="fts_rebuild_cancel"):
                st.session_state.pop("confirmar_rebuild_fts", None)
                st.rerun()

    st.caption(
        "Enquanto o índice não estiver pronto e atualizado, a pesquisa continua "
        "usando o modo de compatibilidade (LIKE)."
    )


def mostrar_classificacao_tic(ativo: dict | None) -> None:
    st.markdown("### Classificação TIC")
    st.caption(
        "Classificação heurística e explicável. Ela auxilia triagem e monitoramento; "
        "não substitui o enquadramento jurídico/técnico da contratação."
    )
    status = status_tic_cache(str(DB_PATH), str(DB_TIC), _assinatura_arquivo(DB_TIC))
    mapa = {
        "ATUALIZADA": ("Atualizada", "ok"), "DESATUALIZADA": ("Desatualizada", "warn"),
        "NAO_CONSTRUIDA": ("Não construída", "info"), "ERRO": ("Erro", "error"),
        "BUILDING": ("Em construção", "info"), "INCOMPLETA": ("Incompleta", "warn"),
    }
    rotulo,tipo=mapa.get(status["estado"],(status["estado"],"info"))
    st.markdown(ui.status_badge(rotulo,tipo),unsafe_allow_html=True)
    st.write(status["mensagem"])
    a,b,c,d=st.columns(4)
    a.metric("Classificadas",f"{status['classificadas']:,}".replace(",","."))
    b.metric("Classificadas como TIC",f"{status['tic']:,}".replace(",","."))
    c.metric("Tamanho",_fmt_bytes(status["tamanho_bytes"]))
    d.metric("Última atualização",_data_hora_br(status["atualizado_em"]))
    pode = ativo is None
    if ativo:
        st.caption(f"Aguardando a execução #{ativo['id']} terminar para evitar disputa de I/O.")
    x,y,z=st.columns([1,1,2])
    if not status["existe"]:
        if x.button("Construir classificação TIC",type="primary",disabled=not pode,use_container_width=True,key="tic_build"):
            try:
                jid=jm.iniciar_classificacao_tic("build",DB_PATH,DB_TIC,JOBS_DB_PATH,LOG_DIR)
                st.success(f"Classificação iniciada — execução #{jid}."); st.rerun()
            except Exception as exc: st.error(f"Não foi possível iniciar: {exc}")
    else:
        if x.button("Classificar novos registros",type="primary",disabled=not pode or status["estado"]=="ATUALIZADA",use_container_width=True,key="tic_update"):
            try:
                jid=jm.iniciar_classificacao_tic("update",DB_PATH,DB_TIC,JOBS_DB_PATH,LOG_DIR)
                st.success(f"Atualização iniciada — execução #{jid}."); st.rerun()
            except Exception as exc: st.error(f"Não foi possível iniciar: {exc}")
        if y.button("Reclassificar tudo",disabled=not pode,use_container_width=True,key="tic_rebuild"):
            st.session_state["confirmar_rebuild_tic"]=True
        if st.session_state.get("confirmar_rebuild_tic"):
            st.warning("Isso recria radar_tic.db usando a versão atual das regras. O compras.db não é alterado.")
            c1,c2=st.columns(2)
            if c1.button("Confirmar reclassificação",type="primary",key="tic_rebuild_confirm"):
                try:
                    jid=jm.iniciar_classificacao_tic("build",DB_PATH,DB_TIC,JOBS_DB_PATH,LOG_DIR)
                    st.session_state.pop("confirmar_rebuild_tic",None); st.success(f"Execução #{jid} iniciado."); st.rerun()
                except Exception as exc: st.error(str(exc))
            if c2.button("Cancelar",key="tic_rebuild_cancel"):
                st.session_state.pop("confirmar_rebuild_tic",None); st.rerun()


def _render_job_ativo() -> None:
    jm.atualizar_jobs_interrompidos(JOBS_DB_PATH)
    job = jm.obter_job_ativo(JOBS_DB_PATH)

    if not job:
        st.info("Nenhuma operação está em execução neste momento.")
        return

    st.markdown(
        f"### Execução ativa"
    )
    st.caption(
        f"Execução #{job['id']} · {_tipo_job_legivel(job['tipo'])}"
    )

    if job["tipo"] == jm.TIPO_CARGA_HISTORICA:
        _render_job_historico(job)

        metricas = jm.obter_metricas_job_historico(job["id"], JOBS_DB_PATH)
        st.markdown("#### Desempenho da carga")
        d1, d2, d3, d4 = st.columns(4)
        d1.metric(
            "Média recente por dia",
            _fmt_duracao(metricas["media_recente_segundos"]),
        )
        d2.metric(
            "Velocidade recente",
            (
                f"{metricas['dias_hora']:.2f} dias/h".replace(".", ",")
                if metricas["dias_hora"] else "—"
            ),
        )
        d3.metric(
            "Estimativa restante",
            _fmt_duracao(metricas["estimativa_restante_segundos"]),
        )
        d4.metric(
            "Tamanho da base",
            _fmt_bytes(_tamanho_total_sqlite(DB_PATH)),
        )
        if metricas["amostra_dias"] < 3:
            st.caption("A estimativa ficará mais estável após alguns dias processados.")
    elif job["tipo"] == jm.TIPO_CARGA_CSV_COMPRASGOV:
        _render_job_csv(job)
    elif job["tipo"] in (jm.TIPO_ITENS_INCREMENTAL, jm.TIPO_ITENS_MUTIRAO):
        _render_job_itens(job)
    elif job["tipo"] in (jm.TIPO_ITENS_HIBRIDO, jm.TIPO_ITENS_HIBRIDO_PILOTO):
        _render_job_generico(job)
    elif job["tipo"] in (jm.TIPO_CARGA_BASE_CSV_COMPRASGOV, jm.TIPO_RECONCILIAR_STAGING_PNCP, jm.TIPO_VALIDAR_COMPLETUDE_PNCP):
        _render_job_base_csv(job)
    elif job["tipo"] in (jm.TIPO_CARGA_2026_COMPRASGOV, jm.TIPO_API_COMPRASGOV_INCREMENTAL, jm.TIPO_COLETA_MULTIFONTE_INCREMENTAL, jm.TIPO_ATUALIZACAO_DIARIA):
        _render_job_generico(job)
    elif job["tipo"] in (jm.TIPO_INDICE_FTS, jm.TIPO_CLASSIFICACAO_TIC):
        _render_job_progresso(job)
    elif job.get("data_referencia"):
        _render_job_diario(job)
    else:
        # Fail-safe: jobs futuros sem data_referencia nunca devem ser tratados
        # automaticamente como coleta diária.
        _render_job_generico(job)

    st.caption("O andamento é atualizado automaticamente a cada 5 segundos.")
    if st.checkbox("Mostrar mensagens técnicas recentes", key=f"mensagens_tecnicas_{job['id']}"):
        atividade = jm.ultimas_linhas_log(job.get("log_path"), 5)
        st.code(atividade, language=None)

    botoes = st.columns([1, 1, 3])
    if job["status"] != "PARADA_SOLICITADA":
        if botoes[0].button(
            "Solicitar parada",
            type="secondary",
            use_container_width=True,
            key=f"parar_job_{job['id']}",
        ):
            if jm.solicitar_parada(job["id"], JOBS_DB_PATH):
                st.warning(
                    "Parada solicitada. A operação será encerrada "
                    "e preservará o progresso já gravado."
                )
                st.rerun()
    else:
        botoes[0].button(
            "Parada solicitada",
            disabled=True,
            use_container_width=True,
            key=f"parada_aguardando_{job['id']}",
        )

    if botoes[1].button(
        "Atualizar agora",
        use_container_width=True,
        key=f"refresh_job_{job['id']}",
    ):
        st.rerun()

    if st.checkbox("Abrir log da execução", key=f"log_completo_{job['id']}"):
        log = jm.ler_log(job.get("log_path"), max_bytes=2_000_000)
        st.code(log or "O log ainda não recebeu mensagens.", language=None)

    st.caption(
        "Você pode fechar esta aba. O job roda em processo separado e continua "
        "enquanto o computador permanecer ligado."
    )


@st.fragment(run_every=5)
def painel_job_ativo_fragment() -> None:
    _render_job_ativo()


def _status_reconciliacao_db_sem_cache(db_path: Path = DB_PATH) -> dict:
    out = {
        "staging": 0, "pais": 0, "quarentena": 0, "itens_removidos_pncp": 0, "quarentena_contratacoes": 0,
        "pendentes_completude": 0, "fila_pendentes_api": 0, "fila_404": 0,
        "fila_410": 0, "fila_erros": 0, "fila_resolvidas": 0,
    }
    if not db_path.exists():
        return out
    try:
        con = sqlite3.connect(str(db_path), timeout=5)
        con.row_factory = sqlite3.Row
        garantir_proveniencia(con)
        def tabela(nome: str) -> bool:
            return con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (nome,)).fetchone() is not None
        if tabela("comprasgov_itens_orfaos"):
            out["staging"] = int(con.execute("SELECT COUNT(*) FROM comprasgov_itens_orfaos").fetchone()[0] or 0)
            out["pais"] = int(con.execute("SELECT COUNT(DISTINCT numero_controle_pncp) FROM comprasgov_itens_orfaos").fetchone()[0] or 0)
        if tabela("comprasgov_itens_quarentena"):
            out["quarentena"] = int(con.execute("SELECT COUNT(*) FROM comprasgov_itens_quarentena WHERE status NOT IN ('RECUPERADO','SUPERADO_POR_REVARREDURA')").fetchone()[0] or 0)
            out["itens_removidos_pncp"] = int(con.execute("SELECT COUNT(*) FROM comprasgov_itens_quarentena WHERE status='REMOVIDA_PNCP'").fetchone()[0] or 0)
        if tabela("comprasgov_contratacoes_quarentena"):
            out["quarentena_contratacoes"] = int(con.execute(
                "SELECT COUNT(*) FROM comprasgov_contratacoes_quarentena"
            ).fetchone()[0] or 0)
        if tabela("pncp_reconciliacao_staging"):
            filas = {str(r[0]): int(r[1]) for r in con.execute(
                "SELECT status,COUNT(*) FROM pncp_reconciliacao_staging GROUP BY status"
            ).fetchall()}
            out["fila_pendentes_api"] = sum(filas.get(k, 0) for k in (
                "PENDENTE", "ERRO_TRANSITORIO", "ERRO", "AGUARDANDO_RATE_LIMIT"
            ))
            out["fila_404"] = filas.get("NAO_ENCONTRADA", 0)
            out["fila_410"] = filas.get("REMOVIDA_PNCP", 0)
            out["fila_erros"] = filas.get("ERRO", 0) + filas.get("ERRO_TRANSITORIO", 0)
            out["fila_resolvidas"] = sum(filas.get(k, 0) for k in (
                "RESOLVIDA", "JA_EXISTIA", "PAI_IMPORTADO"
            ))
        if tabela("contratacoes"):
            cols = {r[1] for r in con.execute("PRAGMA table_info(contratacoes)").fetchall()}
            if "itens_coletados" in cols:
                status_expr = "COALESCE(itens_status,'PENDENTE')" if "itens_status" in cols else "'PENDENTE'"
                out["pendentes_completude"] = int(con.execute(
                    f"""SELECT COUNT(*) FROM contratacoes
                         WHERE COALESCE(itens_coletados,0)=0
                           AND {status_expr} NOT IN ('INDISPONIVEL_PNCP','REMOVIDA_PNCP')"""
                ).fetchone()[0] or 0)
        con.close()
    except Exception:
        pass
    return out



def _resumo_tecnico_job(job: dict) -> str:
    """Gera um bloco curto, copiável e suficiente para diagnóstico remoto."""
    cab = [
        f"Radar PNCP V{VERSION} · Execução #{job.get('id')}",
        f"Tipo: {_tipo_job_legivel(job.get('tipo'))}",
        f"Status: {formatar_status_job(job.get('status'))}",
        f"Período/escopo: {_periodo_job(job)}",
        f"Início: {_data_hora_br(job.get('iniciado_em'))} · Fim: {_data_hora_br(job.get('finalizado_em'))}",
    ]
    log = jm.ler_log(job.get("log_path"), max_bytes=2_000_000)
    linhas = [x.strip() for x in log.splitlines() if x.strip()]
    tipo = job.get("tipo")
    escolhidas: list[str] = []

    if tipo == jm.TIPO_ITENS_INCREMENTAL:
        for i, linha in enumerate(linhas):
            if "[Itens] Resumo:" in linha:
                escolhidas = linhas[i:min(len(linhas), i + 4)]
        # Acrescenta o encerramento se não veio nas quatro linhas seguintes.
        fim = next((x for x in reversed(linhas) if "JOB FINALIZADO" in x), None)
        if fim and fim not in escolhidas:
            escolhidas.append(fim)
    elif tipo == jm.TIPO_ITENS_MUTIRAO:
        for i, linha in enumerate(linhas):
            if "[Mutirão] Resumo geral:" in linha or "[Mutirao] Resumo geral:" in linha:
                escolhidas = linhas[i:min(len(linhas), i + 5)]
        fim = next((x for x in reversed(linhas) if "JOB FINALIZADO" in x), None)
        if fim and fim not in escolhidas:
            escolhidas.append(fim)
    elif tipo in (jm.TIPO_ITENS_HIBRIDO, jm.TIPO_ITENS_HIBRIDO_PILOTO):
        for i, linha in enumerate(linhas):
            if "[Híbrido]" in linha or "[Hibrido]" in linha:
                escolhidas = linhas[max(0,i-1):min(len(linhas),i+5)]
        fim = next((x for x in reversed(linhas) if "JOB FINALIZADO" in x), None)
        if fim and fim not in escolhidas:
            escolhidas.append(fim)
    else:
        # Para os demais jobs, prioriza linhas de resumo/encerramento; se o log
        # não tiver marcadores conhecidos, devolve as últimas 20 linhas.
        chaves = ("Resumo", "resumo", "Backlog", "STATS", "JOB FINALIZADO", "PARCIAL", "CONCLUIDA", "ERRO")
        escolhidas = [x for x in linhas[-120:] if any(k in x for k in chaves)][-20:]
        if not escolhidas:
            escolhidas = linhas[-20:]

    if escolhidas:
        return "\n".join(cab + ["", "Resumo do log:"] + escolhidas)
    return "\n".join(cab + ["", "Log não disponível ou ainda sem resumo."])


def _job_ativo_admin() -> dict | None:
    """Lê apenas o banco pequeno de jobs; não toca no compras.db."""
    jm.preparar_banco_jobs(JOBS_DB_PATH)
    jm.atualizar_jobs_interrompidos(JOBS_DB_PATH)
    return jm.obter_job_ativo(JOBS_DB_PATH)


def _preservar_campos_consulta() -> None:
    # Destaca o estado dos campos da limpeza de widgets de páginas não renderizadas.
    # Não inclui botões, resultados ou editores de seleção.
    campos = {"tipo_pesquisa", "sistema_area", "config_area", "base_visao",
              "forn_termo", "forn_item", "forn_inicio", "forn_fim", "forn_orgao",
              "forn_cancelados", "forn_tamanho",
              "busca_unidade", "busca_preco_min", "busca_preco_max", "busca_preco_tipo",
              "forn_unidade", "forn_preco_min", "forn_preco_max"}
    for chave in list(st.session_state):
        if chave in campos or (str(chave).startswith("busca_") and str(chave).endswith("_input")):
            st.session_state[chave] = st.session_state[chave]


def _trocar_pagina(destino: str) -> None:
    st.session_state["pagina_principal"] = destino


def _abrir_area_sistema(area: str, config=None) -> None:
    _trocar_pagina("Sistema")
    st.session_state["sistema_area"] = area
    if config:
        st.session_state["config_area"] = config


def mostrar_sistema(snapshot: dict) -> None:
    st.title("Sistema")
    st.caption("Consulte os dados disponíveis, acompanhe operações e configure o Radar PNCP.")
    if st.session_state.get("sistema_area") not in AREAS_SISTEMA:
        st.session_state["sistema_area"] = AREAS_SISTEMA[0]
    area = st.radio("Área do sistema", AREAS_SISTEMA, key="sistema_area", horizontal=True)
    if area == "Situação da base":
        pagina_banco(snapshot["meta"], snapshot["painel"], snapshot.get("mercado") or {},
                     snapshot.get("atualizado_em"), titulo=False)
    elif area == "Atividade":
        mostrar_execucoes_logs()
    elif area == "Configurações":
        mostrar_configuracoes()
    else:
        meta = snapshot["meta"]
        st.subheader("Base local")
        a, b = st.columns(2)
        a.metric("Contratações disponíveis", f"{meta['total_contratacoes']:,}".replace(",", "."))
        b.metric("Itens disponíveis", f"{meta['total_itens']:,}".replace(",", "."))
        if meta.get("total_contratacoes"):
            st.write(f"Publicações armazenadas: **{data_br(meta['data_min'])} a {data_br(meta['data_max'])}**.")
            st.caption("O período indica registros encontrados. Confira a cobertura e as pendências em Situação da base.")
        else:
            st.info("A base ainda não possui contratações. Use Sistema → Configurações → Recuperar histórico para começar.")
        if snapshot.get("atualizado_em"):
            st.caption(f"Resumo da base consultado em {_data_hora_br(snapshot['atualizado_em'])}.")
        st.subheader("O que deseja fazer?")
        a, b = st.columns(2)
        with a:
            st.button("Ver situação da base", key="sistema_abrir_base", use_container_width=True,
                      on_click=_abrir_area_sistema, args=("Situação da base",))
            st.caption("Cobertura, atualização dos dados e pendências de coleta.")
            st.button("Configurar atualização dos dados", key="sistema_abrir_agendas", use_container_width=True,
                      on_click=_abrir_area_sistema, args=("Configurações", "Atualização automática"))
            st.caption("Horários da atualização diária e coleta em segundo plano.")
        with b:
            st.button("Acompanhar operações", key="sistema_abrir_atividade", use_container_width=True,
                      on_click=_abrir_area_sistema, args=("Atividade",))
            st.caption("Execuções atuais, histórico e logs para diagnóstico.")
            st.button("Atualizar o Radar PNCP", key="sistema_abrir_atualizacao", use_container_width=True,
                      on_click=_abrir_area_sistema, args=("Configurações", "Atualizar sistema"))
            st.caption("Verificar e instalar os pacotes de novas versões.")


def _abrir_pagina_admin(destino: str) -> None:
    _trocar_pagina(destino)
    st.rerun()


def abrir_configuracoes(area="Visão geral"):
    st.session_state["config_area"] = area
    _abrir_pagina_admin("Configurações")


def mostrar_configuracoes() -> None:
    st.caption("Configure a atualização dos dados e do sistema. As tarefas de recuperação ficam nas áreas específicas.")
    if st.session_state.get("config_area") not in AREAS_CONFIG:
        st.session_state["config_area"] = AREAS_CONFIG[0]
    area = st.selectbox("Área de configuração", AREAS_CONFIG, key="config_area")
    if area == "Visão geral":
        st.write("A atualização diária busca alterações recentes. A coleta em segundo plano completa itens e resultados, aguardando outras operações terminarem.")
        for titulo, descricao in (
            ("Atualização automática", "Horários da atualização diária e da revisão semanal."),
            ("Coleta em segundo plano", "Completar itens e resultados e reservar horários para pesquisa."),
            ("Atualizar sistema", "Instalar um pacote de versão mais recente."),
            ("Recuperar histórico", "Importar arquivos oficiais ou buscar um período que falta na base."),
            ("Ferramentas técnicas", "Diagnosticar fontes, reparar dados e preparar o índice de pesquisa."),
        ):
            st.markdown(f"**{titulo}** — {descricao}")
        st.caption("Para acompanhar o que está executando, abra Sistema → Atividade. Para consultar cobertura e pendências, abra Sistema → Situação da base.")
    elif area == "Atualização automática":
        mostrar_coleta_agendamentos()
    elif area == "Coleta em segundo plano":
        mostrar_fila_mutirao()
    elif area == "Atualizar sistema":
        from painel_atualizacao import mostrar
        from radar_pncp_layout import raiz_da_instalacao
        mostrar(raiz_da_instalacao(BASE_DIR), BASE_DIR)
    elif area == "Recuperar histórico":
        st.info("Escolha a operação pelo período que falta. Importar arquivos permite pesquisar os dados recebidos; a confirmação integral dos itens continua em segundo plano.")
        ferramenta = st.selectbox("O que você precisa recuperar?", [
            "Importar base de 2026", "Recuperar período recente", "Importar arquivos mensais",
            "Importar histórico 2021–2024", "Varredura histórica do PNCP",
        ], key="recuperacao_ferramenta")
        mostrar_manutencao_avancada(ferramenta)
    elif area == "Ferramentas técnicas":
        st.caption("Ferramentas para diagnóstico e recuperação. Selecione uma para consultar seus detalhes.")
        ferramenta = st.selectbox("Ferramenta", ["Selecione uma ferramenta", "Índice e classificação TIC",
            "Diagnóstico das fontes", "Reparar e associar dados", "Confirmar itens pelo PNCP",
            "Processar lote de itens", "Coleta por uma data (legado)", "Coleta contínua sem pausa (legado)",
            "Estatísticas e armazenamento"], key="tecnica_ferramenta")
        if ferramenta == "Estatísticas e armazenamento":
            snapshot = obter_snapshot_interface()
            mostrar_detalhes_base(snapshot["meta"], snapshot["painel"], snapshot.get("mercado"), snapshot.get("atualizado_em"))
        elif ferramenta != "Selecione uma ferramenta":
            mostrar_manutencao_avancada(ferramenta)


def mostrar_coleta_agendamentos() -> None:
    st.caption("Defina os horários de atualização. O computador precisa estar ligado e com sua sessão do Windows aberta; a tela pode ficar bloqueada.")
    ativo = _job_ativo_admin()
    if ativo:
        st.info(f"Execução #{ativo['id']} em execução · {_tipo_job_legivel(ativo.get('tipo'))} · {_periodo_job(ativo)}")
        painel_job_ativo_fragment()

    st.markdown("### Execução manual")
    op1, op2 = st.columns(2)
    with op1:
        with st.container(border=True):
            st.markdown("#### Atualização diária · COLETA")
            st.caption("Revisa alterações de contratações nos últimos três dias. Os resultados usam um período próprio; na primeira execução sem resultados, podem ser recuperados desde janeiro.")
            if st.button("Atualizar dados recentes agora", type="primary", disabled=ativo is not None,
                         use_container_width=True, key="coleta_page_executar_diaria"):
                try:
                    jid = jm.iniciar_atualizacao_diaria(3, None, DB_PATH, JOBS_DB_PATH, LOG_DIR, modo="diario")
                except Exception as exc:
                    st.error(f"Não foi possível iniciar a COLETA diária: {exc}")
                else:
                    st.success(f"COLETA diária iniciada — execução #{jid}."); st.rerun()
    with op2:
        with st.container(border=True):
            st.markdown("#### Revisão semanal")
            st.caption("Revisa alterações dos últimos 30 dias e recupera lacunas de resultados.")
            if st.button("Executar revisão semanal agora", disabled=ativo is not None,
                         use_container_width=True, key="coleta_page_executar_semanal"):
                try:
                    jid = jm.iniciar_atualizacao_diaria(30, None, DB_PATH, JOBS_DB_PATH, LOG_DIR, modo="revisao")
                except Exception as exc:
                    st.error(f"Não foi possível iniciar a revisão semanal: {exc}")
                else:
                    st.success(f"Revisão semanal iniciada — execução #{jid}."); st.rerun()

    parcial_coleta = jm.obter_ultima_coleta_parcial(JOBS_DB_PATH)
    if parcial_coleta:
        st.warning(
            f"Há uma COLETA parcial pendente: execução #{parcial_coleta['id']} · "
            f"{data_br(parcial_coleta.get('data_inicio'))} → {data_br(parcial_coleta.get('data_fim'))}. "
            "Com a coleta automática ativa, a atualização incompleta é retomada após o intervalo de espera."
        )
        if st.button("Retomar agora", disabled=ativo is not None, key="coleta_page_retomar_parcial"):
            try:
                jid = jm.iniciar_retomada_coleta(int(parcial_coleta['id']), DB_PATH, JOBS_DB_PATH, LOG_DIR)
            except Exception as exc:
                st.error(f"Não foi possível retomar a COLETA: {exc}")
            else:
                st.success(f"Retomada iniciada — execução #{jid}."); st.rerun()

    st.markdown("### Agendamentos automáticos")
    status_d = status_agendamento_cache("diario", str(BASE_DIR))
    status_s = status_agendamento_cache("revisao", str(BASE_DIR))
    if TEST_MODE:
        st.warning("Agendamentos do Windows ficam desabilitados no modo de teste isolado.")
        return
    if not status_d.get("disponivel"):
        st.info("O gerenciamento integrado requer Windows com Agendador de Tarefas disponível.")
        return

    ad1, ad2 = st.columns(2)
    with ad1:
        with st.container(border=True):
            ativo_d = bool(status_d.get("ativo")); existe_d = bool(status_d.get("existe"))
            st.markdown("#### Atualização diária · COLETA")
            st.metric("Status", "ATIVO" if ativo_d else ("INATIVO" if existe_d else "NÃO CONFIGURADO"))
            if existe_d:
                st.caption(f"Contratações: **3 dias** · Horário: **{status_d.get('hora') or '?'}** · Próxima: **{_fmt_agendamento_iso(status_d.get('proxima_execucao'))}**")
                if status_d.get("aponta_versao_atual") is False:
                    st.warning("Aponta para versão anterior. Salve novamente para migrar para esta versão.")
            hora_d = st.time_input("Horário diário", value=_hora_para_obj(status_d.get("hora"),(0,30)), step=300, key="coleta_page_hora_diaria")
            a1,a2=st.columns(2)
            with a1:
                if st.button("Salvar" if existe_d else "Ativar", use_container_width=True, key="coleta_page_salvar_diaria"):
                    try: rsch.configurar("diario", hora_d.strftime("%H:%M"), 3, BASE_DIR)
                    except Exception as exc: st.error(f"Não foi possível salvar o agendamento: {exc}")
                    else: status_agendamento_cache.clear(); st.success("Agendamento diário salvo."); st.rerun()
            with a2:
                if existe_d and st.button("Desativar" if ativo_d else "Reativar", use_container_width=True, key="coleta_page_toggle_diaria"):
                    try: rsch.definir_ativo("diario", not ativo_d)
                    except Exception as exc: st.error(f"Não foi possível alterar o agendamento: {exc}")
                    else: status_agendamento_cache.clear(); st.rerun()
    with ad2:
        with st.container(border=True):
            ativo_s = bool(status_s.get("ativo")); existe_s = bool(status_s.get("existe"))
            st.markdown("#### Revisão semanal")
            st.metric("Status", "ATIVO" if ativo_s else ("INATIVO" if existe_s else "NÃO CONFIGURADO"))
            if existe_s:
                st.caption(f"Janela: **30 dias** · Dia: **{status_s.get('dia_semana') or '?'}** · Horário: **{status_s.get('hora') or '?'}** · Próxima: **{_fmt_agendamento_iso(status_s.get('proxima_execucao'))}**")
                if status_s.get("aponta_versao_atual") is False:
                    st.warning("Aponta para versão anterior. Salve novamente para migrar para esta versão.")
            dias_semana=list(rsch.DIAS_PT.values())
            dia_padrao=status_s.get("dia_semana") if status_s.get("dia_semana") in dias_semana else "Sábado"
            dia_s=st.selectbox("Dia da revisão", dias_semana, index=dias_semana.index(dia_padrao), key="coleta_page_dia_semanal")
            hora_s=st.time_input("Horário semanal", value=_hora_para_obj(status_s.get("hora"),(3,0)), step=300, key="coleta_page_hora_semanal")
            s1,s2=st.columns(2)
            with s1:
                if st.button("Salvar" if existe_s else "Ativar", use_container_width=True, key="coleta_page_salvar_semanal"):
                    try: rsch.configurar("revisao", hora_s.strftime("%H:%M"), 30, BASE_DIR, dia_semana=dia_s)
                    except Exception as exc: st.error(f"Não foi possível salvar a revisão semanal: {exc}")
                    else: status_agendamento_cache.clear(); st.success("Revisão semanal salva."); st.rerun()
            with s2:
                if existe_s and st.button("Desativar" if ativo_s else "Reativar", use_container_width=True, key="coleta_page_toggle_semanal"):
                    try: rsch.definir_ativo("revisao", not ativo_s)
                    except Exception as exc: st.error(f"Não foi possível alterar a revisão semanal: {exc}")
                    else: status_agendamento_cache.clear(); st.rerun()


def mostrar_saude_fontes() -> None:
    st.caption("Consulte a atualização registrada na base e teste a conexão com as fontes quando necessário.")
    ativo = _job_ativo_admin()
    sr_status = status_resultados_comprasgov_cache(str(DB_PATH), date.today().isoformat())
    frescor=str(sr_status.get("frescor") or "SEM_DADOS"); atraso=sr_status.get("defasagem_dias"); data_max_r=sr_status.get("data_max")
    h1,h2,h3=st.columns(3)
    h1.metric("Compras.gov · último resultado", data_br(data_max_r) if data_max_r else "—")
    h2.metric("Defasagem de resultados", f"{atraso} dia(s)" if atraso is not None else "—")
    h3.metric("Atualização local", frescor.replace("ATENCAO","ATENÇÃO").replace("CRITICO","CRÍTICO"))
    if frescor in ("ATRASADO","CRITICO"):
        st.warning("A base local ainda não possui resultados recentes. Verifique a coleta e a disponibilidade da fonte.")
    with st.container(border=True):
        st.markdown("### Teste PNCP sob demanda")
        st.caption("Os endpoints só são consultados quando você clicar no botão abaixo.")
        if st.button("Executar testes diretos", disabled=ativo is not None, key="saude_testar_pncp"):
            pid_consulta,url_consulta=_url_teste_reconciliacao_pncp(); pid_itens,url_itens=_url_teste_itens_pncp()
            with st.spinner("Testando endpoints PNCP..."):
                resultados={}
                if url_consulta: resultados["consulta"]=ph.testar_endpoint_publico(url_consulta,timeout=12)
                if url_itens: resultados["itens"]=ph.testar_endpoint_publico(url_itens,params={"pagina":1,"tamanhoPagina":10},timeout=12)
            st.session_state["saude_health_pncp"]=resultados
        resultados=st.session_state.get("saude_health_pncp") or {}
        if resultados.get("consulta"): _render_resultado_health("PNCP · consulta",resultados["consulta"])
        if resultados.get("itens"): _render_resultado_health("PNCP · itens",resultados["itens"])
        st.link_button("Monitor externo independente", "https://statuslicitacoes.com.br/api-pncp")


@st.cache_resource(show_spinner=False)
def servico_metricas_fila(caminho: str) -> pf.ResumoBackground:
    db = Path(caminho)
    return pf.ResumoBackground(lambda: pf.coletar_metricas(db), ttl=120,
                              arquivo=db.with_name(db.name + ".painel_fila_v1.json"),
                              identidade=str(db.resolve()))


@st.cache_resource(show_spinner=False)
def servico_agendamentos_fila(base: str) -> pf.ResumoBackground:
    def coletar():
        return {"task": msch.obter_status(Path(base)), "diaria": rsch.obter_status("diario", Path(base))}
    return pf.ResumoBackground(coletar, ttl=60)


@st.fragment(run_every=5)
def mostrar_fila_mutirao() -> None:
    st.caption("Complete itens e resultados automaticamente, priorizando os mais recentes. A coleta aguarda outras operações terminarem.")
    servico = servico_metricas_fila(str(DB_PATH))
    agendas = servico_agendamentos_fila(str(BASE_DIR))
    agendamento = agendas.obter()
    if agendamento.get("atualizando"):
        st.caption("Verificando os agendamentos em segundo plano…")
    if st.button("Verificar configuração", key="fila_refresh"):
        agendas.obter(forcar=True)
        st.rerun()
    flash=st.session_state.pop("adm_flash_mutirao",None)
    if flash: st.success(str(flash))
    with st.container(border=True):
        st.markdown("### Coleta em segundo plano")
        perfis={
            "Recomendado":(5000,6,95,5000,8000),
            "Conservador":(2000,4,80,2000,4000),
        }
        perfil=st.selectbox("Ritmo de coleta",list(perfis),index=0,key="fila_perfil_mutirao")
        try:
            cfg, estado = pf.ler_controle(JOBS_DB_PATH)
            task = (agendamento.get("dados") or {}).get("task") or {}
        except Exception as exc:
            cfg,estado,task={"ativo":0},{},{"existe":False,"estado":"ERRO","erro":str(exc)}
        persistente=bool(int(cfg.get("ativo") or 0))
        pm1,pm2,pm3,pm4=st.columns(4)
        pm1.metric("Coleta automática","ATIVA" if persistente else "INATIVA")
        pm2.metric("Agendamento", {"Ready": "Configurado", "Running": "Em execução", "Disabled": "Desativado"}.get(str(task.get("estado")), str(task.get("estado") or "Consultando…")))
        if agendamento.get("atualizado_em"):
            st.caption(f"Agendador consultado em {_data_hora_br(agendamento['atualizado_em'])}.")
        if agendamento.get("erro"):
            st.warning("Falha ao consultar o Agendador: " + agendamento["erro"])
        pm3.metric("Situação",estado_coleta(estado.get("estado")))
        pm4.metric("Consultas", "Itens e resultados" if cfg.get("resultados_ativo", 1) else "Itens")
        if st.checkbox("Mostrar parâmetros técnicos", key="coleta_parametros"):
            hl, hw, hr, lc, lp = perfis[perfil]
            st.caption(f"Compras.gov: lote de {hl:,}, {hw} consultas simultâneas, limite de {hr} requisições/minuto. PNCP: {lc:,} contratações / {lp:,} páginas por lote.".replace(",", "."))
        if estado.get("estado") in {"ERRO", "PAUSADO_SEM_COLETA", "PAUSADO_USUARIO"} and estado.get("detalhe"):
            st.warning(str(estado["detalhe"]))
        if estado.get("proximo_prioritario"):
            st.caption(f"Próximo prioritário: **{estado.get('proximo_prioritario')}** · deadline: **{_fmt_agendamento_iso(estado.get('deadline'))}**")
        if task.get("aponta_versao_atual") is False and task.get("existe"):
            st.warning("O controlador aponta para versão anterior. Ative/atualize nesta versão.")
        status_d=(agendamento.get("dados") or {}).get("diaria") or {}
        pode=(not TEST_MODE) and bool(status_d.get("ativo")) and bool(status_d.get("disponivel")) and status_d.get("aponta_versao_atual") is not False
        if not status_d:
            st.caption("Consultando os agendamentos em segundo plano para habilitar a ativação.")
        elif not pode:
            if status_d.get("aponta_versao_atual") is False:
                st.warning("O agendamento aponta para uma versão anterior. Abra Atualização automática e salve o horário novamente.")
            else:
                st.warning("Ative a atualização diária em Sistema → Configurações → Atualização automática para habilitar a coleta em segundo plano.")
        reserva = st.checkbox("Reservar horário para pesquisa", value=bool(cfg.get('pesquisa_protegida', 1)), key="fila_reserva_pesquisa")
        resultados_ativo = st.checkbox("Atualizar resultados por item do PNCP em segundo plano", value=bool(cfg.get('resultados_ativo', 1)), key="fila_resultados_ativo")
        ri, rf = st.columns(2)
        hora_inicio = ri.time_input("Pausar mutirão às", value=dt_time.fromisoformat(cfg.get('pesquisa_inicio', '07:00')), key="fila_pesquisa_inicio")
        hora_fim = rf.time_input("Retomar mutirão às", value=dt_time.fromisoformat(cfg.get('pesquisa_fim', '10:00')), key="fila_pesquisa_fim")
        st.caption("Horário local do computador, todos os dias. A reserva pausa as rotas automáticas de itens; COLETA e sincronização do índice continuam. Salve em Ativar/atualizar abaixo. Jobs já iniciados mantêm o limite anterior até encerrarem.")
        st.caption("Pausar a coleta automática impede novos lotes. Para encerrar a execução atual com segurança, use Solicitar parada em Atividade.")
        b1,b2=st.columns(2)
        with b1:
            if st.button("Ativar ou salvar coleta automática",type="primary",use_container_width=True,disabled=not pode,key="fila_ativar_mutirao"):
                hl,hw,hr,lc,lp=perfis[perfil]
                configurou = False
                try:
                    # O resumo exibido nunca autoriza uma ação: revalida no clique.
                    diario_atual = rsch.obter_status("diario", BASE_DIR)
                    if not diario_atual.get("disponivel") or not diario_atual.get("ativo") or diario_atual.get("aponta_versao_atual") is False:
                        raise RuntimeError("Configure e salve a COLETA diária nesta versão antes de ativar o controlador.")
                    mp.definir_config(ativo=True,hibrido_lote=hl,hibrido_workers=hw,hibrido_rpm=hr,lote_contratacoes=lc,lote_paginas=lp,margem_minutos=60,db_jobs=JOBS_DB_PATH,pesquisa_protegida=reserva,pesquisa_inicio=hora_inicio.strftime("%H:%M"),pesquisa_fim=hora_fim.strftime("%H:%M"),resultados_ativo=resultados_ativo)
                    configurou = True
                    mp.atualizar_estado(JOBS_DB_PATH,estado="ATIVANDO",detalhe="Ativação explícita solicitada pela interface.",ultima_acao_em=datetime.now().isoformat(timespec="seconds"))
                    task_nova=msch.configurar(BASE_DIR)
                    if not task_nova.get("existe"): raise RuntimeError("Tarefa do controlador não encontrada após configuração.")
                except Exception as exc:
                    if configurou:
                        try: mp.definir_config(ativo=False,db_jobs=JOBS_DB_PATH)
                        except Exception: pass
                    st.error(f"Não foi possível ativar o persistente: {exc}")
                else:
                    agendas.invalidar(); servico.invalidar(); status_mutirao_persistente_cache.clear()
                    st.session_state["adm_flash_mutirao"]="Configuração da coleta automática salva."
                    st.rerun()
        with b2:
            if st.button("Pausar coleta automática",use_container_width=True,disabled=not persistente and not task.get("existe"),key="fila_desativar_mutirao"):
                try:
                    mp.definir_config(ativo=False,db_jobs=JOBS_DB_PATH); msch.remover(); mp.atualizar_estado(JOBS_DB_PATH,estado="INATIVO",detalhe="Coleta automática pausada pela interface.",ultima_acao_em=datetime.now().isoformat(timespec="seconds"))
                except Exception as exc: st.error(f"Não foi possível desativar: {exc}")
                else: agendas.invalidar(); status_mutirao_persistente_cache.clear(); st.rerun()


def mostrar_execucoes_logs() -> None:
    st.caption("Acompanhe a execução atual e consulte operações anteriores.")
    ativo=_job_ativo_admin()
    if ativo:
        st.info(f"Execução #{ativo['id']} em execução · {_tipo_job_legivel(ativo.get('tipo'))} · {_periodo_job(ativo)}")
        painel_job_ativo_fragment()
    st.markdown("### Execuções recentes")
    jobs=jm.listar_jobs(JOBS_DB_PATH,limite=30)
    if not jobs:
        st.info("Ainda não há execuções registradas."); return
    linhas=[{"Job":j["id"],"Tipo":_tipo_job_legivel(j["tipo"]),"Período":_periodo_job(j),"Status":formatar_status_job(j["status"]),"Início":_data_hora_br(j["iniciado_em"]),"Fim":_data_hora_br(j["finalizado_em"])} for j in jobs]
    st.dataframe(pd.DataFrame(linhas),use_container_width=True,hide_index=True,height=420)
    visao = st.radio("Consultar histórico", ["Resumo", "Detalhes e logs", "Retomar execução anterior"], key="atividade_visao", horizontal=True)
    if visao == "Resumo":
        return
    if visao == "Retomar execução anterior":
        mostrar_retomadas_anteriores(jobs)
        return
    logs=[j for j in jobs if j.get("log_path")]
    if logs:
        opcoes={f"Execução #{j['id']} — {_tipo_job_legivel(j['tipo'])} — {formatar_status_job(j['status'])}":j for j in logs}
        escolhido=st.selectbox("Abrir execução",list(opcoes),key="logs_job")
        jlog=opcoes[escolhido]
        tab1,tab2=st.tabs(["Resumo técnico","Log recente"])
        with tab1: st.code(_resumo_tecnico_job(jlog),language=None)
        with tab2:
            limite=2_000_000; texto=jm.ler_log(jlog.get("log_path"),max_bytes=limite) or "Log não disponível."
            st.code(texto,language=None)


def mostrar_manutencao_avancada(ferramenta: str) -> None:
    """Executa apenas a ferramenta selecionada nas Configurações."""
    jm.preparar_banco_jobs(JOBS_DB_PATH)
    jm.atualizar_jobs_interrompidos(JOBS_DB_PATH)
    ativo = jm.obter_job_ativo(JOBS_DB_PATH)
    ontem = date.today() - timedelta(days=1)
    if ativo:
        st.info(f"A execução #{ativo['id']} está em andamento. Aguarde terminar para iniciar outra operação.")
        if st.button("Acompanhar em Atividade", key="manutencao_atividade"):
            _abrir_pagina_admin("Atividade")

    if ferramenta == 'Coleta por uma data (legado)':
        with st.container(border=True):
            st.markdown("### Coleta diária")
            st.write(
                "Escolha uma data já encerrada. O Radar executará contratações "
                "e itens usando o coletor incremental."
            )

            c1, c2 = st.columns([1, 2])
            with c1:
                data_coleta = st.date_input(
                    "Data de publicação",
                    value=ontem,
                    max_value=ontem,
                    format="DD/MM/YYYY",
                    key="data_coleta_gerenciamento",
                )

            progresso_pre = jm.obter_progresso_data(DB_PATH, data_coleta.isoformat())
            with c2:
                st.markdown("**Situação atual dessa data no banco**")
                st.write(
                    (
                        f"{progresso_pre['modalidades_concluidas']}/14 modalidades • "
                        f"{progresso_pre['contratacoes']:,} contratações • "
                        f"{progresso_pre['com_itens']:,} com itens • "
                        f"{progresso_pre['indisponiveis']:,} indisponível(is) no PNCP • "
                        f"{progresso_pre['pendentes']:,} pendentes"
                    ).replace(",", ".")
                )

            completa = (
                progresso_pre["modalidades_concluidas"] == 14
                and progresso_pre["pendentes"] == 0
            )
            if completa:
                st.info(
                    "Essa data já está completa. Se iniciar novamente, o coletor "
                    "incremental deverá pular o que já foi concluído."
                )

            if st.button(
                "Iniciar coleta diária",
                type="primary",
                disabled=ativo is not None,
                use_container_width=True,
                key="iniciar_coleta_diaria",
            ):
                try:
                    job_id = jm.iniciar_coleta_diaria(
                        data_referencia=data_coleta.isoformat(),
                        db_compras=DB_PATH,
                        db_jobs=JOBS_DB_PATH,
                        log_dir=LOG_DIR,
                    )
                except Exception as exc:
                    st.error(f"Não foi possível iniciar a coleta: {exc}")
                else:
                    st.success(f"Coleta iniciada em segundo plano — execução #{job_id}.")
                    st.rerun()

    elif ferramenta == 'Varredura histórica do PNCP':
        with st.container(border=True):
            st.markdown("### Carga histórica PNCP · varredura integral")
            st.write(
                "Esta rotina é a **varredura integral das 14 modalidades do PNCP, dia por dia**. "
                "Ela não representa sozinha a cobertura real do Radar PNCP, que também recebe dados dos CSVs "
                "do Compras.gov e da COLETA."
            )
            st.info(
                "Um dia pode ter contratações na base sem constar como ‘varrido integralmente pelo PNCP’. "
                "Não use a diferença entre o período total e a varredura PNCP como indicação automática de dados faltantes."
            )

            inicio_padrao = date(2025, 1, 1)
            fim_padrao = date(2025, 1, 31)
            if fim_padrao > ontem:
                fim_padrao = ontem
            if inicio_padrao > fim_padrao:
                inicio_padrao = fim_padrao

            c1, c2 = st.columns(2)
            with c1:
                data_inicio = st.date_input(
                    "Data inicial", value=inicio_padrao, max_value=ontem,
                    format="DD/MM/YYYY", key="historico_inicio",
                )
            with c2:
                data_fim = st.date_input(
                    "Data final", value=fim_padrao, max_value=ontem,
                    format="DD/MM/YYYY", key="historico_fim",
                )

            periodo_valido = data_inicio <= data_fim
            if not periodo_valido:
                st.error("A data final deve ser igual ou posterior à data inicial.")
            else:
                total_dias = (data_fim - data_inicio).days + 1
                st.write(f"**Período selecionado:** {total_dias} dia(s).")
                if st.button(
                    "Verificar cobertura do período", key="verificar_periodo_historico",
                    use_container_width=False,
                ):
                    with st.spinner("Calculando cobertura e certificação do período..."):
                        resumo = jm.obter_resumo_periodo_banco(
                            DB_PATH, data_inicio.isoformat(), data_fim.isoformat(), JOBS_DB_PATH
                        )
                    st.session_state["resumo_periodo_historico"] = {
                        "inicio": data_inicio.isoformat(), "fim": data_fim.isoformat(), **resumo,
                    }

                resumo_salvo = st.session_state.get("resumo_periodo_historico")
                if (resumo_salvo and resumo_salvo.get("inicio") == data_inicio.isoformat()
                        and resumo_salvo.get("fim") == data_fim.isoformat()):
                    st.markdown("#### Cobertura observada")
                    m1, m2, m3 = st.columns(3)
                    m1.metric("Dias com contratações na base", f"{resumo_salvo['dias_com_dados']}/{resumo_salvo['total_dias']}")
                    m2.metric("Varredura integral PNCP", f"{resumo_salvo['dias_varredura_pncp']}/{resumo_salvo['total_dias']}")
                    m3.metric("Dias alcançados pela COLETA", f"{resumo_salvo['dias_coleta']}/{resumo_salvo['total_dias']}")
                    m4, m5 = st.columns(2)
                    m4.metric("Dias com dados Compras.gov", f"{resumo_salvo['dias_com_dados_csv']}/{resumo_salvo['total_dias']}")
                    m5.metric("Dias com todos os conjuntos certificados PNCP", f"{resumo_salvo['dias_itens_confirmados_pncp']}/{resumo_salvo['total_dias']}")
                    st.caption(
                        "‘Dias com dados Compras.gov’ significa que há registros do CSV/API nessa data; não é uma afirmação "
                        "de que o arquivo sozinho certifica completude. ‘Varredura integral PNCP’ mantém a métrica histórica "
                        "das 14 modalidades + ausência de pendências de itens."
                    )

                if total_dias > 90:
                    st.warning(
                        "Varreduras integrais longas podem levar muitas horas ou dias. Só execute esta rotina quando houver "
                        "uma necessidade específica de certificação PNCP; a presença de dados na base pode já estar coberta por outras fontes."
                    )

                if st.button(
                    "Iniciar varredura histórica integral PNCP", type="primary",
                    disabled=ativo is not None or not periodo_valido,
                    use_container_width=True, key="iniciar_carga_historica",
                ):
                    try:
                        job_id = jm.iniciar_carga_historica(
                            data_inicio=data_inicio.isoformat(), data_fim=data_fim.isoformat(),
                            db_compras=DB_PATH, db_jobs=JOBS_DB_PATH, log_dir=LOG_DIR,
                        )
                    except Exception as exc:
                        st.error(f"Não foi possível iniciar a varredura histórica: {exc}")
                    else:
                        st.success(f"Varredura histórica PNCP iniciada — execução #{job_id}.")
                        st.rerun()

        st.markdown("#### Recuperação de dias incompletos")
        st.caption(
            "Usa o mesmo período selecionado acima e cria um job apenas para datas que ainda "
            "tenham modalidade ou itens pendentes. Quedas gerais do PNCP fazem o job aguardar "
            "e retomar o mesmo dia."
        )
        if 'data_inicio' in locals() and 'data_fim' in locals() and data_inicio <= data_fim:
            if st.button(
                "Reprocessar dias incompletos do período",
                disabled=ativo is not None,
                key="reprocessar_incompletos",
            ):
                try:
                    jid=jm.iniciar_reprocessamento_incompletos(
                        data_inicio.isoformat(),data_fim.isoformat(),DB_PATH,JOBS_DB_PATH,LOG_DIR
                    )
                    st.success(f"Reprocessamento iniciado — execução #{jid}."); st.rerun()
                except Exception as exc:
                    st.info(str(exc))

    elif ferramenta == 'Importar base de 2026':
        st.markdown("### Importar arquivos de 2026")
        st.caption(
            "Importa contratações, itens e resultados homologados dos arquivos oficiais do Compras.gov. "
            "São arquivos grandes; mantenha o computador ligado. A importação pode ser retomada. "
            "Os itens recebidos ainda precisam de confirmação integral no PNCP."
        )
        if st.button(
            "Carregar Compras.gov 2026 + resultados", type="primary",
            disabled=ativo is not None, use_container_width=True, key="carga_comprasgov_2026"
        ):
            try:
                jid = jm.iniciar_carga_2026_comprasgov(DB_PATH, JOBS_DB_PATH, LOG_DIR)
            except Exception as exc:
                st.error(f"Não foi possível iniciar a carga 2026: {exc}")
            else:
                st.success(f"Carga massiva 2026 iniciada — execução #{jid}.")
                st.rerun()

    elif ferramenta == 'Recuperar período recente':
        st.markdown("### Recuperar um período")
        st.caption(
            "Busca contratações publicadas no período selecionado, usando PNCP e Compras.gov. "
            "O progresso é salvo para recuperar dias pendentes sem repetir o que já foi concluído."
        )
        hoje_api = date.today() - timedelta(days=1)
        cobertura_2026 = cobertura_comprasgov_2026_cache(str(DB_PATH))
        padrao_api_ini = hoje_api - timedelta(days=3)
        if cobertura_2026:
            try:
                candidato = date.fromisoformat(cobertura_2026) + timedelta(days=1)
                if candidato <= hoje_api and (hoje_api-candidato).days <= 90:
                    padrao_api_ini = candidato
            except ValueError:
                pass
        if cobertura_2026:
            st.info(
                f"Carga massiva 2026 cobre publicações até **{data_br(cobertura_2026)}**. "
                "A COLETA pode continuar a partir do dia seguinte, sem depender da API de itens do PNCP."
            )
        ca1, ca2 = st.columns(2)
        with ca1:
            api_ini = st.date_input("Data inicial", value=padrao_api_ini, max_value=hoje_api, format="DD/MM/YYYY", key="coleta_multi_ini")
        with ca2:
            api_fim = st.date_input("Data final", value=hoje_api, max_value=hoje_api, format="DD/MM/YYYY", key="coleta_multi_fim")
        api_valido = api_ini <= api_fim and (api_fim-api_ini).days <= 90
        if api_ini > api_fim:
            st.error("A data final deve ser igual ou posterior à inicial.")
        elif (api_fim-api_ini).days > 90:
            st.warning("Divida a recuperação em janelas de no máximo 91 dias.")
        st.caption(
            "Se uma fonte ficar indisponível, a operação pode terminar com pendências externas. "
            "Os dados já recebidos ficam preservados."
        )
        if st.button(
            "Recuperar período selecionado", type="primary",
            disabled=ativo is not None or not api_valido, use_container_width=True, key="coleta_multifonte_incremental"
        ):
            try:
                jid=jm.iniciar_coleta_multifonte_incremental(
                    api_ini.isoformat(), api_fim.isoformat(),
                    DB_PATH, JOBS_DB_PATH, LOG_DIR
                )
            except Exception as exc:
                st.error(f"Não foi possível iniciar a COLETA multifuente: {exc}")
            else:
                st.success(f"COLETA multifuente iniciada — execução #{jid}.")
                st.rerun()

    elif ferramenta == 'Importar histórico 2021–2024':
        with st.expander("Importação de 2021 a 2024", expanded=False):
            st.markdown("### Importar histórico de 2021 a 2024")
            st.caption(
                "Importa arquivos anuais de contratações e itens de 2021 a 2024 e corrige marcações antigas de completude, quando existirem. "
                "Não importa o ano de 2025: para isso, use Importar arquivos mensais. O progresso é salvo para retomada."
            )
            if st.button(
                "Importar contratações e itens de 2021–2024", type="primary",
                disabled=ativo is not None, use_container_width=True, key="iniciar_carga_base_csv"
            ):
                try:
                    jid = jm.iniciar_carga_base_csv_comprasgov(DB_PATH, JOBS_DB_PATH, LOG_DIR)
                except Exception as exc:
                    st.error(f"Não foi possível iniciar a carga-base: {exc}")
                else:
                    st.success(f"Reparo + carga-base iniciados em segundo plano — execução #{jid}.")
                    st.rerun()

    elif ferramenta == 'Reparar e associar dados':
        st.markdown("### Reparar e associar dados")
        status_rec = status_reconciliacao_interface()
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Itens em staging", f"{status_rec['staging']:,}".replace(",", "."))
        c2.metric("Pais distintos", f"{status_rec['pais']:,}".replace(",", "."))
        c3.metric("Itens em quarentena", f"{status_rec['quarentena']:,}".replace(",", "."))
        c4.metric("Contratações em quarentena", f"{status_rec['quarentena_contratacoes']:,}".replace(",", "."))

        q1, q2, q3, q4 = st.columns(4)
        q1.metric("Pais aguardando API", f"{status_rec['fila_pendentes_api']:,}".replace(",", "."))
        q2.metric("HTTP 404 preservados", f"{status_rec['fila_404']:,}".replace(",", "."))
        q3.metric("Removidos no PNCP (410)", f"{status_rec['fila_410']:,}".replace(",", "."))
        q4.metric("Pais já resolvidos", f"{status_rec['fila_resolvidas']:,}".replace(",", "."))
        if status_rec.get("itens_removidos_pncp", 0):
            st.caption(
                f"{status_rec['itens_removidos_pncp']:,} item(ns) de contratações removidas pelo PNCP "
                "estão preservados na quarentena histórica e não contam mais como staging operacional."
            )

        anteriores_rec = [
            j for j in jm.listar_jobs(JOBS_DB_PATH, limite=20)
            if j.get("tipo") == jm.TIPO_RECONCILIAR_STAGING_PNCP
        ]
        ultima_rec = anteriores_rec[0] if anteriores_rec else None
        retomada_rec = bool(
            ativo is None and ultima_rec and ultima_rec.get("status") in jm.STATUS_REEXECUTAVEIS
            and status_rec["fila_pendentes_api"] > 0
        )
        if retomada_rec:
            st.info(
                f"Retomada disponível: a execução #{ultima_rec['id']} terminou como "
                f"{formatar_status_job(ultima_rec['status']).lower()}. "
                "O progresso já confirmado está no banco; a próxima execução pulará pais já resolvidos "
                "e continuará a fila restante."
            )
        st.caption(
            "Primeiro revalida os arquivos COMPRA já em cache e reconstrói registros CSV-only a partir "
            "de linhas estruturalmente íntegras, colocando linhas desalinhadas em quarentena. Em seguida "
            "higieniza o staging de itens e tenta recuperar IDs deslocados. Só depois consulta no PNCP as "
            "contratações-pai válidas que ainda não existem no banco. Respostas 410 são arquivadas "
            "fora do staging operacional, e qualquer pendência transitória mantém o job como PARCIAL. "
            "A fila é retomável."
        )

        rotulo_reconciliacao = (
            "Continuar reparo e reconciliação PNCP" if retomada_rec
            else "Reparar dados CSV, higienizar staging e reconciliar PNCP"
        )
        if st.button(
            rotulo_reconciliacao, type="primary",
            disabled=ativo is not None, use_container_width=True, key="reconciliar_staging_pncp"
        ):
            try:
                jid = jm.iniciar_reconciliar_staging_pncp(DB_PATH, JOBS_DB_PATH, LOG_DIR)
            except Exception as exc:
                st.error(f"Não foi possível iniciar a reconciliação: {exc}")
            else:
                st.success(f"Reconciliação PNCP iniciada — execução #{jid}.")
                st.rerun()

    elif ferramenta == 'Processar lote de itens':
        st.markdown("### Processar um lote de itens")
        try:
            fila_live = resumo_fila_itens_cache(str(DB_PATH))
        except Exception as exc:
            fila_live = {}
            st.warning(f"Não foi possível ler a fila de itens: {exc}")
        q1, q2, q3, q4, q5 = st.columns(5)
        q1.metric("Backlog", f"{int(fila_live.get('backlog_total',0)):,}".replace(",", "."))
        q2.metric("Elegíveis", f"{int(fila_live.get('backlog_elegivel',0)):,}".replace(",", "."))
        q3.metric("Na fila", f"{int(fila_live.get('pendentes',0)) + int(fila_live.get('em_andamento',0)) + int(fila_live.get('erro_transitorio',0)):,}".replace(",", "."))
        q4.metric("Erros", f"{int(fila_live.get('erro_transitorio',0)):,}".replace(",", "."))
        q5.metric("Staging", f"{int(fila_live.get('staging_itens',0)):,}".replace(",", "."))
        st.caption(
            "Se a fila estiver vazia, o próprio job abastece somente o lote necessário a partir do backlog global, "
            "priorizando registros mais recentes. A confirmação prévia do registro-pai não é mais necessária: um ID PNCP válido "
            "pode ter seus itens certificados diretamente pelo endpoint oficial. A COLETA diária processa até 250 contratações/400 páginas; "
            "a revisão semanal, até 2.000/4.000. Revalidações preservam o conjunto antigo até a nova leitura integral terminar."
        )
        op_fila = {
            "250 contratos / 400 páginas": (250, 400),
            "1.000 contratos / 2.000 páginas": (1000, 2000),
            "2.000 contratos / 4.000 páginas": (2000, 4000),
            "5.000 contratos / 8.000 páginas": (5000, 8000),
        }
        fila_rotulo = st.selectbox("Lote manual da fila", list(op_fila), index=1, key="lote_fila_itens")
        if st.button("Processar fila de itens agora", disabled=ativo is not None, use_container_width=True, key="processar_fila_itens"):
            lc, lp = op_fila[fila_rotulo]
            try:
                jid = jm.iniciar_itens_incremental(lc, lp, 0, DB_PATH, JOBS_DB_PATH, LOG_DIR)
            except Exception as exc:
                st.error(f"Não foi possível iniciar a fila de itens: {exc}")
            else:
                st.success(f"Fila incremental de itens iniciada — execução #{jid}.")
                st.rerun()

    elif ferramenta == 'Coleta contínua sem pausa (legado)':
        with st.expander("Mutirão contínuo sem janela de segurança", expanded=False):
            st.warning(
                "Modo legado: continua até esvaziar o trabalho elegível ou ocorrer instabilidade. "
                "Se durar vários dias, poderá impedir a COLETA diária/semanal de iniciar enquanto o job estiver ativo. "
                "Prefira Sistema → Configurações → Coleta em segundo plano."
            )
            confirmar_sem_limite = st.checkbox(
                "Entendo que este modo pode atravessar os horários da COLETA",
                key="confirmar_mutirao_sem_limite",
            )
            if st.button(
                "Iniciar mutirão até esvaziar",
                disabled=ativo is not None or not confirmar_sem_limite,
                use_container_width=True,
                key="iniciar_mutirao_sem_limite",
            ):
                try:
                    jid = jm.iniciar_itens_mutirao(
                        5000, 8000, "00:30", 60, True, DB_PATH, JOBS_DB_PATH, LOG_DIR
                    )
                except Exception as exc:
                    st.error(f"Não foi possível iniciar o mutirão contínuo: {exc}")
                else:
                    st.success(f"Mutirão contínuo iniciado — execução #{jid}.")
                    st.rerun()

    elif ferramenta == 'Confirmar itens pelo PNCP':
        status_rec = status_reconciliacao_interface()
        st.markdown("### Confirmar itens pelo PNCP")
        qtd_pendentes_fmt = f"{status_rec['pendentes_completude']:,}".replace(",", ".")
        st.caption(
            f"Há aproximadamente {qtd_pendentes_fmt} contratação(ões) com evidência de itens CSV, "
            "já confirmadas no PNCP e ainda sem confirmação integral dos itens. "
            "A validação preserva a origem dos dados já associados."
        )
        st.write(
            "Esta etapa consulta a lista completa de itens no PNCP e somente após uma resposta integral marca "
            "`itens_coletados = 1`. Recomendo executar em lotes para não depender de uma sessão muito longa da API."
        )
        op_limites = {"500": 500, "2.000": 2000, "5.000": 5000, "10.000": 10000, "Sem limite": 0}
        limite_rotulo = st.selectbox("Tamanho do lote de validação", list(op_limites), index=1, key="limite_validacao_pncp")
        if st.button(
            "Validar próximo lote no PNCP", disabled=ativo is not None,
            use_container_width=True, key="validar_completude_pncp"
        ):
            try:
                jid = jm.iniciar_validar_completude_pncp(op_limites[limite_rotulo], DB_PATH, JOBS_DB_PATH, LOG_DIR)
            except Exception as exc:
                st.error(f"Não foi possível iniciar a validação: {exc}")
            else:
                st.success(f"Validação de completude iniciada — execução #{jid}.")
                st.rerun()

    elif ferramenta == 'Importar arquivos mensais':
        st.markdown("### Importar arquivos mensais")
        st.caption(
            "Use esta etapa para incorporar registros novos/alterados de meses específicos. "
            "Ela complementa a base, mas não declara completude dos itens."
        )

        meses_opcoes = []
        y, m = 2021, 1
        # Evita oferecer o mês corrente, cujo arquivo mensal pode ainda não existir.
        ultimo_mes = date.today().replace(day=1) - timedelta(days=1)
        while (y, m) <= (ultimo_mes.year, ultimo_mes.month):
            meses_opcoes.append(f"{y:04d}-{m:02d}")
            if m == 12:
                y, m = y + 1, 1
            else:
                m += 1

        padrao_ini = "2025-01" if "2025-01" in meses_opcoes else meses_opcoes[0]
        padrao_fim = "2025-12" if "2025-12" in meses_opcoes else meses_opcoes[-1]
        c1, c2 = st.columns(2)
        with c1:
            mes_inicio_csv = st.selectbox(
                "Mês inicial", meses_opcoes, index=meses_opcoes.index(padrao_ini),
                format_func=_mes_br, key="csv_mes_inicio"
            )
        with c2:
            mes_fim_csv = st.selectbox(
                "Mês final", meses_opcoes, index=meses_opcoes.index(padrao_fim),
                format_func=_mes_br, key="csv_mes_fim"
            )
        csv_valido = mes_inicio_csv <= mes_fim_csv
        if not csv_valido:
            st.error("O mês final deve ser igual ou posterior ao mês inicial.")
        else:
            total_meses = len(jm._meses_periodo(mes_inicio_csv, mes_fim_csv))
            st.caption(
                f"Serão processados {total_meses} mês(es). Os CSVs ficam em "
                f"{DATA_DIR / 'csv_comprasgov'} e podem ser reutilizados em uma retomada."
            )
            if st.button(
                "Importar meses selecionados", type="secondary",
                disabled=ativo is not None, use_container_width=True, key="iniciar_carga_csv"
            ):
                try:
                    jid = jm.iniciar_carga_csv_comprasgov(
                        mes_inicio_csv, mes_fim_csv, DB_PATH, JOBS_DB_PATH, LOG_DIR
                    )
                except Exception as exc:
                    st.error(f"Não foi possível iniciar a carga CSV: {exc}")
                else:
                    st.success(f"Importação de deltas Compras.gov iniciada — execução #{jid}.")
                    st.rerun()

    elif ferramenta == "Índice e classificação TIC":
        mostrar_indice_pesquisa(ativo)
        st.divider()
        mostrar_classificacao_tic(ativo)
    elif ferramenta == "Diagnóstico das fontes":
        mostrar_saude_fontes()

def mostrar_retomadas_anteriores(jobs) -> None:
    retomaveis = [
        j for j in jobs
        if j["status"] in jm.STATUS_REEXECUTAVEIS
        and j["tipo"] in (jm.TIPO_COLETA_DIARIA, jm.TIPO_CARGA_HISTORICA, jm.TIPO_CARGA_CSV_COMPRASGOV, jm.TIPO_CARGA_BASE_CSV_COMPRASGOV, jm.TIPO_RECONCILIAR_STAGING_PNCP, jm.TIPO_VALIDAR_COMPLETUDE_PNCP, jm.TIPO_ITENS_INCREMENTAL, jm.TIPO_ITENS_MUTIRAO, jm.TIPO_ITENS_HIBRIDO, jm.TIPO_ITENS_HIBRIDO_PILOTO)
    ]
    if not retomaveis:
        st.info("Não há execuções anteriores disponíveis para retomada nesta lista. Atualizações incompletas da COLETA são tratadas em Sistema → Configurações → Atualização automática.")
    if retomaveis and not jm.obter_job_ativo(JOBS_DB_PATH):
        opcoes = {
            (
                f"Execução #{j['id']} — {_tipo_job_legivel(j['tipo'])} — "
                f"{_periodo_job(j)} — {formatar_status_job(j['status'])}"
            ): j
            for j in retomaveis
        }
        escolhido = st.selectbox(
            "Execução que pode ser retomada",
            list(opcoes),
            key="job_retomada",
        )
        origem = opcoes[escolhido]
        if origem["tipo"] == jm.TIPO_CARGA_HISTORICA:
            label_botao = "Retomar período"
        elif origem["tipo"] == jm.TIPO_CARGA_CSV_COMPRASGOV:
            label_botao = "Retomar deltas mensais"
        elif origem["tipo"] == jm.TIPO_CARGA_BASE_CSV_COMPRASGOV:
            label_botao = "Retomar carga-base"
        elif origem["tipo"] == jm.TIPO_RECONCILIAR_STAGING_PNCP:
            label_botao = "Retomar reconciliação"
        elif origem["tipo"] == jm.TIPO_VALIDAR_COMPLETUDE_PNCP:
            label_botao = "Retomar validação"
        elif origem["tipo"] == jm.TIPO_ITENS_INCREMENTAL:
            label_botao = "Retomar fila de itens"
        elif origem["tipo"] == jm.TIPO_ITENS_MUTIRAO:
            label_botao = "Continuar mutirão de itens"
        elif origem["tipo"] == jm.TIPO_ITENS_HIBRIDO:
            label_botao = "Continuar lote híbrido"
        elif origem["tipo"] == jm.TIPO_ITENS_HIBRIDO_PILOTO:
            label_botao = "Reexecutar piloto híbrido"
        else:
            label_botao = "Retomar essa data"
        if st.button(
            label_botao,
            use_container_width=False,
            key="retomar_job",
        ):
            try:
                if origem["tipo"] == jm.TIPO_CARGA_HISTORICA:
                    novo_id = jm.iniciar_carga_historica(
                        data_inicio=origem["data_inicio"],
                        data_fim=origem["data_fim"],
                        db_compras=DB_PATH,
                        db_jobs=JOBS_DB_PATH,
                        log_dir=LOG_DIR,
                    )
                elif origem["tipo"] == jm.TIPO_CARGA_CSV_COMPRASGOV:
                    novo_id = jm.iniciar_carga_csv_comprasgov(
                        origem["data_inicio"], origem["data_fim"],
                        DB_PATH, JOBS_DB_PATH, LOG_DIR
                    )
                elif origem["tipo"] == jm.TIPO_CARGA_BASE_CSV_COMPRASGOV:
                    novo_id = jm.iniciar_carga_base_csv_comprasgov(
                        DB_PATH, JOBS_DB_PATH, LOG_DIR
                    )
                elif origem["tipo"] == jm.TIPO_RECONCILIAR_STAGING_PNCP:
                    novo_id = jm.iniciar_reconciliar_staging_pncp(DB_PATH, JOBS_DB_PATH, LOG_DIR)
                elif origem["tipo"] == jm.TIPO_VALIDAR_COMPLETUDE_PNCP:
                    novo_id = jm.iniciar_validar_completude_pncp(int(origem.get("operacao") or 2000), DB_PATH, JOBS_DB_PATH, LOG_DIR)
                elif origem["tipo"] == jm.TIPO_ITENS_INCREMENTAL:
                    partes = str(origem.get("operacao") or "1000:2000:0").split(":")
                    novo_id = jm.iniciar_itens_incremental(int(partes[0]), int(partes[1]), int(partes[2] or 0) if len(partes)>2 else 0, DB_PATH, JOBS_DB_PATH, LOG_DIR)
                elif origem["tipo"] == jm.TIPO_ITENS_MUTIRAO:
                    partes = str(origem.get("operacao") or "2000:4000:0030:60:0").split(":")
                    hc = str(partes[2] if len(partes)>2 else "0030").zfill(4)
                    novo_id = jm.iniciar_itens_mutirao(
                        int(partes[0]), int(partes[1]), f"{hc[:2]}:{hc[2:4]}",
                        int(partes[3] or 60) if len(partes)>3 else 60,
                        bool(int(partes[4] or 0)) if len(partes)>4 else False,
                        DB_PATH, JOBS_DB_PATH, LOG_DIR,
                    )
                elif origem["tipo"] == jm.TIPO_ITENS_HIBRIDO:
                    partes = str(origem.get("operacao") or "5000:6:95:").split(":")
                    novo_id = jm.iniciar_itens_hibrido(
                        int(partes[0] or 5000), int(partes[1] or 6) if len(partes)>1 else 6,
                        int(partes[2] or 95) if len(partes)>2 else 95, DB_PATH, JOBS_DB_PATH, LOG_DIR,
                    )
                elif origem["tipo"] == jm.TIPO_ITENS_HIBRIDO_PILOTO:
                    partes = str(origem.get("operacao") or "500:6").split(":")
                    novo_id = jm.iniciar_itens_hibrido_piloto(
                        int(partes[0] or 500), int(partes[1] or 6) if len(partes)>1 else 6,
                        DB_PATH, JOBS_DB_PATH, LOG_DIR,
                    )
                else:
                    novo_id = jm.iniciar_coleta_diaria(
                        data_referencia=origem["data_referencia"],
                        db_compras=DB_PATH,
                        db_jobs=JOBS_DB_PATH,
                        log_dir=LOG_DIR,
                    )
            except Exception as exc:
                st.error(f"Não foi possível retomar: {exc}")
            else:
                st.success(
                    f"Retomada iniciada coma execução #{novo_id}. "
                    "O coletor incremental pulará o que já estiver completo."
                )
                st.rerun()


@st.cache_resource(show_spinner=False)
def servico_atualizacao_pesquisa(db: str, fts: str):
    return pf.ResumoBackground(lambda: pa.consultar(Path(db),Path(fts)), ttl=60)


@st.fragment(run_every=5)
def painel_atualizacao_pesquisa():
    resumo = servico_atualizacao_pesquisa(str(DB_PATH), str(DB_FTS)).obter()
    dados = resumo.get("dados")
    if not dados:
        st.caption("Verificando a atualização dos dados…")
        if resumo.get("erro"):
            st.warning("Não foi possível confirmar a atualização. Consulte Sistema → Situação da base.")
        return
    if resumo.get("erro"):
        st.warning("A verificação não foi renovada. As informações abaixo são da última consulta disponível.")
    if dados.get("verificada"):
        st.success(f"A janela recente de contratações e itens foi verificada até {data_br(dados['referencia'])}.")
    else:
        st.warning("A atualização recente ainda tem pendências ou verificações em andamento. Alguns dados podem estar incompletos.")
    st.caption("Resultados de fornecedores e histórico têm cobertura própria. Consulte Sistema → Situação da base para verificar o período da sua pesquisa.")
    if st.checkbox("Ver detalhes da atualização", key="pesquisa_ver_detalhes"):
        st.caption(f"Verificação consultada em {_data_hora_br(resumo.get('atualizado_em'))}.")
        janela = dados.get("janela")
        if janela:
            st.write(f"Contratações: {data_br(janela['inicio'])} a {data_br(janela['fim'])} · {janela['estado']}.")
        itens = dados.get("itens") or {}
        if itens:
            st.write(f"Itens: {itens['coleta_pendente']:,} pendentes, {itens['revalidacoes']:,} aguardando nova verificação e {itens['indisponiveis']:,} indisponíveis.".replace(",", "."))
        st.caption("Índice de pesquisa: " + dados["indice"]["mensagem"])


def pagina_pesquisa(meta: dict) -> None:
    st.title("Consultar")
    from painel_cestas import mostrar_destino
    from painel_buscas_salvas import mostrar_atalhos,aplicar,mostrar_salvar_atual
    pedido=mostrar_atalhos()
    if pedido:
        aplicar(pedido,meta)
        st.session_state['busca_periodo_input_calendario_geracao']=st.session_state.get('busca_periodo_input_calendario_geracao',0)+1
    tipo_pesquisa = st.radio("Pesquisar em", ["Contratações e itens", "Fornecedores e resultados"], horizontal=True, key="tipo_pesquisa")
    if tipo_pesquisa == "Fornecedores e resultados":
        import painel_fornecedores
        painel_fornecedores.mostrar(DB_PATH, mostrar_detalhes, mostrar_destino)
        return
    st.caption(
        "Pesquise em objetos, órgãos, processos e itens armazenados na base local."
    )

    min_data = iso_para_data(meta["data_min"]) or date.today()
    max_data = max(iso_para_data(meta["data_max"]) or date.today(), date.today())
    periodo_atual=st.session_state.get('busca_periodo_input',())
    datas_validas=[d for d in periodo_atual if isinstance(d,date)] if isinstance(periodo_atual,(tuple,list)) else []
    if datas_validas:
        min_data=min(min_data,*datas_validas);max_data=max(max_data,*datas_validas)
    status_fts = status_fts_cache(str(DB_PATH), str(DB_FTS), _assinatura_arquivo(DB_FTS))
    status_tic = status_tic_cache(str(DB_PATH), str(DB_TIC), _assinatura_arquivo(DB_TIC))
    categorias_tic = tm.categorias_disponiveis(DB_TIC) if status_tic["existe"] else []

    # ----------------------------------------------------------
    # Formulário compacto
    # ----------------------------------------------------------
    with st.form("radar_pncp_busca_form", border=True):
        termo_col, botao_col = st.columns([5, 1.4])
        with termo_col:
            texto = st.text_input(
                "Pesquisar por",
                placeholder="Ex.: Privacy Tools, Microsoft 365, Wi-Fi 7, firewall...",
                key="busca_texto_input",
            )
        with botao_col:
            st.write("")
            st.write("")
            pesquisar = st.form_submit_button(
                "Pesquisar",
                type="primary",
                use_container_width=True,
            )

        with st.expander("Filtros avançados", expanded=False):
            f1, f2, f3 = st.columns(3)
            with f1:
                modo_rotulo = st.selectbox(
                    "Combinação dos termos",
                    ["Todas as palavras", "Frase exata", "Qualquer palavra"],
                    index=0,
                    key="busca_modo_input",
                )
            with f2:
                import calendario_periodo
                periodo = calendario_periodo.selecionar((min_data,max_data),min_data,max_data,
                    chave="busca_periodo_input")
                st.caption('O filtro permite datas até hoje. A disponibilidade dos registros depende da cobertura da base.')
            with f3:
                somente_homologadas = st.checkbox(
                    "Somente contratações com resultados homologados",
                    key="busca_homologadas_input",
                    help="Exige pelo menos um resultado homologado não cancelado na base local. Não exige que todos os itens tenham resultado.",
                )
                status_rotulo = st.selectbox(
                    "Status dos itens",
                    [
                        "Todos",
                        "Itens coletados",
                        "Indisponível no PNCP",
                        "Pendente / retry",
                    ],
                    key="busca_status_input",
                )

            f4, f5, f6 = st.columns(3)
            with f4:
                modalidades = st.multiselect(
                    "Modalidade",
                    list(dict.fromkeys([*meta["modalidades"],*st.session_state.get('busca_modalidades_input',[])])),
                    key="busca_modalidades_input", placeholder="Todas as modalidades",
                )
            with f5:
                ufs = st.multiselect(
                    "UF",
                    list(dict.fromkeys([*meta["ufs"],*st.session_state.get('busca_ufs_input',[])])),
                    key="busca_ufs_input", placeholder="Todas as UFs",
                )
            with f6:
                orgao = st.text_input(
                    "Órgão / unidade / CNPJ",
                    placeholder="Opcional",
                    key="busca_orgao_input",
                )

            f7, f8 = st.columns(2)
            with f7:
                tic_rotulo = st.selectbox(
                    "Classificação TIC",
                    ["Todos", "Somente TIC", "Não TIC", "Não classificado"],
                    disabled=not status_tic["existe"],
                    key="busca_tic_status_input",
                )
            with f8:
                tic_categorias = st.multiselect(
                    "Categoria TIC", list(dict.fromkeys([*categorias_tic,*st.session_state.get('busca_tic_categoria_input',[])])),
                    disabled=not status_tic["existe"],
                    key="busca_tic_categoria_input", placeholder="Todas as categorias",
                )

            por_pagina = st.selectbox(
                "Resultados por página",
                [10, 20, 30, 50],
                index=1,
                key="busca_por_pagina_input",
            )
            unidade,preco_min,preco_max,preco_tipo=fprecos.campos('busca',tipo=True)
            st.caption('A contratação precisa ter pelo menos um item ou resultado na unidade e faixa informadas. O termo principal continua buscando também objeto e órgão. Homologados desconsideram resultados cancelados. O tipo de preço só restringe a busca quando você informa unidade ou faixa.')

    painel_atualizacao_pesquisa()
    mostrar_destino()

    mapa_modo = {
        "Todas as palavras": "todas",
        "Frase exata": "frase",
        "Qualquer palavra": "qualquer",
    }
    mapa_status = {
        "Todos": "todos", "Itens coletados": "ok",
        "Indisponível no PNCP": "indisponivel", "Pendente / retry": "pendente",
    }
    mapa_tic = {
        "Todos": "todos", "Somente TIC": "tic", "Não TIC": "nao_tic",
        "Não classificado": "nao_classificado",
    }

    # A consulta só passa a existir depois do clique em Pesquisar.
    if pesquisar:
        if periodo is None:
            st.error('Complete um período de publicação válido antes de pesquisar.');return
        try:
            preco_min=fprecos.ler_preco(preco_min);preco_max=fprecos.ler_preco(preco_max)
            fprecos.validar(FiltrosBusca(unidade=unidade,preco_min=preco_min,preco_max=preco_max,preco_tipo=preco_tipo))
        except ValueError as exc:
            st.session_state.pop('busca_executada',None)
            st.session_state.pop('detalhe_id',None)
            st.error(str(exc));return
        if isinstance(periodo, (tuple, list)) and len(periodo) == 2:
            data_inicio, data_fim = periodo
        else:
            data_inicio = data_fim = periodo

        st.session_state["busca_executada"] = {
            "texto": texto,
            "modo_texto": mapa_modo[modo_rotulo],
            "modo_rotulo": modo_rotulo,
            "data_inicio": data_inicio.isoformat() if data_inicio else None,
            "data_fim": data_fim.isoformat() if data_fim else None,
            "modalidades": tuple(modalidades),
            "ufs": tuple(ufs),
            "orgao": orgao,
            "status_itens": mapa_status[status_rotulo],
            "somente_homologadas": bool(somente_homologadas),
            "tic_status": mapa_tic.get(tic_rotulo, "todos"),
            "tic_categorias": tuple(tic_categorias),
            "por_pagina": int(por_pagina),
            "unidade":unidade,"preco_min":preco_min,"preco_max":preco_max,"preco_tipo":preco_tipo,
        }
        st.session_state["pagina_busca"] = 1
        st.session_state.pop("detalhe_id", None)

    busca_salva = st.session_state.get("busca_executada")
    if busca_salva:mostrar_salvar_atual('contratacoes',busca_salva,int(busca_salva['por_pagina']))

    if not busca_salva:
        motor = (
            "Pesquisa disponível"
            if status_fts["pronto_para_busca"]
            else "Índice em preparação ou com mudanças pendentes — consulta direta à base local (pode ser mais lenta)"
        )
        st.info(
            "Digite um termo ou escolha filtros e clique em **Pesquisar**. "
            "Você pode pesquisar pelo objeto, pela descrição de um item ou pelo órgão."
        )
        st.caption(motor)
        return

    # Limpeza explícita da consulta anterior.
    ac1, ac2 = st.columns([1, 5])
    with ac1:
        if st.button("Limpar resultados", use_container_width=True):
            st.session_state.pop("busca_executada", None)
            st.session_state["pagina_busca"] = 1
            st.session_state.pop("detalhe_id", None)
            st.rerun()

    filtros = FiltrosBusca(
        texto=busca_salva["texto"],
        modo_texto=busca_salva["modo_texto"],
        data_inicio=busca_salva["data_inicio"],
        data_fim=busca_salva["data_fim"],
        modalidades=tuple(busca_salva["modalidades"]),
        ufs=tuple(busca_salva["ufs"]),
        orgao=busca_salva["orgao"],
        status_itens=busca_salva["status_itens"],
        tic_status=busca_salva.get("tic_status", "todos"),
        tic_categorias=tuple(busca_salva.get("tic_categorias", ())),
        somente_homologadas=busca_salva.get("somente_homologadas", False),
        unidade=busca_salva.get('unidade',''),preco_min=busca_salva.get('preco_min'),
        preco_max=busca_salva.get('preco_max'),preco_tipo=busca_salva.get('preco_tipo','Estimado'),
    )

    pagina = int(st.session_state.get("pagina_busca", 1))
    with st.spinner("Pesquisando..."):
        resultado = buscar_contratacoes(
            DB_PATH,
            filtros,
            pagina,
            busca_salva["por_pagina"],
            DB_FTS,
            DB_TIC,
        )

    if resultado["pagina"] != pagina:
        st.session_state["pagina_busca"] = resultado["pagina"]
        pagina = resultado["pagina"]

    ids = [r["numero_controle_pncp"] for r in resultado["resultados"]]
    hits = buscar_itens_correspondentes(
        DB_PATH,
        ids,
        busca_salva["texto"],
        busca_salva["modo_texto"],
        max_por_contratacao=3,
        filtros=filtros,
    )

    if st.session_state.get("detalhe_id"):
        mostrar_detalhes(st.session_state["detalhe_id"])

    total = resultado["total"]
    motor = resultado.get("motor_busca", "LIKE")

    # Cabeçalho de resultados bem menor.
    h1, h2 = st.columns([4, 1])
    with h1:
        st.subheader("Resultados")
        if busca_salva["texto"].strip():
            st.caption(
                f'Pesquisa: "{busca_salva["texto"]}" · '
                f'{total:,} resultado(s)'.replace(",", ".")
            )
        else:
            st.caption(f"{total:,} resultado(s) com os filtros escolhidos".replace(",", "."))
    with h2:
        st.markdown(
            f'<div style="text-align:right;padding-top:10px">'
            '<span class="radar-search-engine">Base local</span></div>',
            unsafe_allow_html=True,
        )

    if total == 0:
        st.warning("Nenhuma contratação encontrada.")
        if fprecos.ativo(filtros):st.caption(fprecos.rotulo(filtros)+'. O filtro usa apenas itens e preços já presentes na base local.')
        if filtros.somente_homologadas:
            st.caption("O filtro considera apenas resultados homologados não cancelados já armazenados na base local. Uma contratação pode ter resultado na fonte que ainda não foi coletado.")
        return

    if filtros.somente_homologadas:
        st.caption("Filtro ativo: pelo menos um resultado homologado não cancelado na base local; outros itens da contratação podem ainda não ter resultado.")
    from painel_cestas import incluir_lote
    cesta_ativa=st.session_state.get('cesta_ativa_id')
    filtrado=fprecos.ativo(filtros)
    if filtrado:
        tipo_preco_lote=filtros.preco_tipo
        st.caption('Filtros executados: '+fprecos.rotulo(filtros))
        st.caption('A faixa se refere ao preço unitário. Valores nos cartões são totais da contratação completa. A prévia mostra até três itens na unidade e faixa escolhidas.')
    else:
        tipo_lote=st.selectbox('Preços para inclusão em lote', ['Estimados dos itens', 'Homologados dos resultados'],
                              index=int(filtros.somente_homologadas), key=f'cesta_tipo_lote_{int(filtros.somente_homologadas)}')
        tipo_preco_lote='Homologado' if tipo_lote=='Homologados dos resultados' else 'Estimado'
    if st.button("Adicionar todos os resultados filtrados à cesta" if filtrado else "Adicionar todos os resultados à cesta", key="cesta_busca_todos", disabled=not cesta_ativa):
        incluir_lote('salvar_itens',DB_PATH,filtros=filtros,db_fts=DB_FTS,db_tic=DB_TIC,tipo_preco=tipo_preco_lote)
    if filtrado:
        st.caption('Inclui todas as páginas, somente com referências na unidade e faixa executadas e com o mesmo tipo de preço. Não exige correspondência da descrição com o termo principal. As novas referências entram como A avaliar.')
    else:
        st.caption("Inclui todas as páginas da pesquisa e todos os itens ou resultados locais das contratações encontradas, mesmo os itens que não correspondem ao termo pesquisado. Usa o tipo de preço escolhido acima; resultados homologados cancelados são desconsiderados. As novas referências entram como A avaliar.")

    inicio = (resultado["pagina"] - 1) * resultado["por_pagina"] + 1
    fim = min(total, inicio + len(resultado["resultados"]) - 1)

    nav1, nav2, nav3 = st.columns([1, 3, 1])
    if nav1.button(
        "← Anterior",
        disabled=resultado["pagina"] <= 1,
        use_container_width=True,
        key="anterior_topo",
    ):
        st.session_state["pagina_busca"] = resultado["pagina"] - 1
        st.rerun()
    nav2.caption(
        f"{inicio}–{fim} de {total:,} · página "
        f"{resultado['pagina']} de {resultado['paginas']}".replace(",", ".")
    )
    if nav3.button(
        "Próxima →",
        disabled=resultado["pagina"] >= resultado["paginas"],
        use_container_width=True,
        key="proxima_topo",
    ):
        st.session_state["pagina_busca"] = resultado["pagina"] + 1
        st.rerun()

    for idx, r in enumerate(resultado["resultados"], start=inicio):
        item_hits = hits.get(r["numero_controle_pncp"], [])
        with st.container(border=True, key=f"radar_pncp_resultado_{idx}"):
            st.markdown(
                f'<div class="radar-result-meta">'
                f'{data_br(r.get("data_publicacao_pncp"))} · '
                f'{r.get("modalidade_nome") or "Modalidade não informada"} · '
                f'{r.get("uf_sigla") or "UF não informada"}'
                f'</div>',
                unsafe_allow_html=True,
            )
            st.markdown(
                f'<div class="radar-result-title">{idx}. '
                f'{r.get("orgao_razao_social") or "Órgão não informado"}</div>',
                unsafe_allow_html=True,
            )
            st.write(r.get("objeto") or "Objeto não informado")

            if r.get("tic_score") is not None:
                if int(r.get("tic_eh_tic") or 0) == 1:
                    st.markdown(
                        ui.status_badge(
                            f"TIC · {r.get('tic_categoria') or 'Sem categoria'} · {int(r.get('tic_score') or 0)}/100",
                            "ok" if int(r.get("tic_score") or 0) >= 70 else "info",
                        ),
                        unsafe_allow_html=True,
                    )
                    try:
                        motivos=json.loads(r.get("tic_motivos_json") or "[]")
                    except Exception:
                        motivos=[]
                    if motivos:
                        st.caption("Classificação: " + " • ".join(motivos[:3]))
                else:
                    st.caption(f"Classificação TIC: não confirmada · pontuação {int(r.get('tic_score') or 0)}/100")

            encontrado = campos_correspondentes(
                r,
                [] if filtrado else item_hits,
                busca_salva["texto"],
                busca_salva["modo_texto"],
            )
            if encontrado:
                st.caption("Correspondência: " + " · ".join(encontrado))

            if filtrado and item_hits:
                st.caption('Itens na unidade e faixa: '+', '.join(str(hit['numero_item']) for hit in item_hits))

            for hit in item_hits:
                descricao = texto_curto(
                    hit.get("descricao") or hit.get("informacao_complementar"),
                    240,
                )
                st.markdown(
                    f"**Item {hit.get('numero_item')}** — {descricao}"
                )

            status = r.get("itens_status_exibicao")
            tipo = {
                "OK": "ok",
                "INDISPONIVEL_PNCP": "warn",
                "ERRO_TEMPORARIO": "error",
                "PENDENTE": "info",
            }.get(status, "info")
            st.markdown(
                ui.status_badge(status_legivel(status), tipo),
                unsafe_allow_html=True,
            )

            info1, info2, info3 = st.columns(3)
            with info1: ui.dado_resumo("Valor estimado", brl(r.get("valor_total_estimado")))
            with info2: ui.dado_resumo("Itens", int(r.get("qtd_itens") or 0))
            with info3: ui.dado_resumo("Processo", r.get("processo") or "—")

            b1, b2, b3 = st.columns([1, 1, 3])
            if r.get("url_pncp"):
                b1.link_button(
                    "PNCP",
                    r["url_pncp"],
                    use_container_width=True,
                )
            if b2.button(
                "Detalhes",
                key=f"detalhe_{r['numero_controle_pncp']}",
                type="primary", use_container_width=True,
            ):
                st.session_state["detalhe_id"] = r["numero_controle_pncp"]
                mostrar_detalhes(r["numero_controle_pncp"])
            if b3.button("Adicionar referências filtradas à cesta" if filtrado else ("Adicionar todos os itens à cesta" if tipo_preco_lote=='Estimado' else "Adicionar todos os preços homologados à cesta"), key=f"cesta_itens_{r['numero_controle_pncp']}",
                         disabled=not cesta_ativa, use_container_width=True,
                         help="Guarda as referências locais desta contratação como A avaliar, respeitando a unidade e faixa quando informadas."):
                incluir_lote('salvar_itens',DB_PATH,numero_controle=r['numero_controle_pncp'],tipo_preco=tipo_preco_lote,filtros=filtros)

    st.divider()
    navb1, navb2, navb3 = st.columns([1, 3, 1])
    if navb1.button(
        "← Anterior",
        disabled=resultado["pagina"] <= 1,
        use_container_width=True,
        key="anterior_rodape",
    ):
        st.session_state["pagina_busca"] = resultado["pagina"] - 1
        st.rerun()
    navb2.caption(
        f"Página {resultado['pagina']} de {resultado['paginas']}"
    )
    if navb3.button(
        "Próxima →",
        disabled=resultado["pagina"] >= resultado["paginas"],
        use_container_width=True,
        key="proxima_rodape",
    ):
        st.session_state["pagina_busca"] = resultado["pagina"] + 1
        st.rerun()

def _fmt_intervalo_cobertura(r: dict) -> str:
    ini = data_br(r.get("inicio"))
    fim = data_br(r.get("fim"))
    dias = int(r.get("dias", 0) or 0)
    if r.get("inicio") == r.get("fim"):
        return f"{ini} · 1 dia"
    return f"{ini} → {fim} · {dias:,} dias".replace(",", ".")


def _df_intervalos(intervalos: list[dict], coluna: str = "Intervalo") -> pd.DataFrame:
    if not intervalos:
        return pd.DataFrame(columns=[coluna, "Dias"])
    return pd.DataFrame([
        {coluna: (data_br(r.get("inicio")) if r.get("inicio") == r.get("fim") else f"{data_br(r.get('inicio'))} → {data_br(r.get('fim'))}"),
         "Dias": int(r.get("dias", 0) or 0)}
        for r in intervalos
    ])


def _mostrar_intervalos_resumidos(titulo: str, bloco: dict, vazio: str = "Nenhum intervalo.", limite: int = 8) -> None:
    intervalos = list(bloco.get("intervalos") or [])
    if not intervalos:
        st.caption(vazio)
        return
    for r in intervalos[:limite]:
        st.write(f"• {_fmt_intervalo_cobertura(r)}")
    if len(intervalos) > limite:
        st.caption(f"... e mais {len(intervalos)-limite} intervalo(s). Abra os detalhes abaixo para ver todos.")


def _mostrar_cobertura_base(meta: dict) -> None:
    st.markdown("### Cobertura da base")
    st.caption(
        "Este mapa é calculado apenas com os bancos locais. **Cobertura operacional** indica que existe evidência de "
        "cobertura por uma fonte/rotina conhecida; **certificação PNCP por publicação** é mais estrita e exige uma "
        "varredura integral das modalidades por data de publicação. A COLETA moderna revisa por data de atualização e, "
        "por isso, aparece separadamente."
    )

    try:
        inicio_padrao = date.fromisoformat(str(meta.get("data_min") or date.today().isoformat())[:10])
    except Exception:
        inicio_padrao = date.today()
    hoje = date.today()
    ultimo_dia_encerrado = hoje - timedelta(days=1)
    if inicio_padrao > ultimo_dia_encerrado:
        inicio_padrao = ultimo_dia_encerrado

    chave_ini = "cobertura_inicio"
    chave_fim = "cobertura_fim"
    st.session_state.setdefault(chave_ini, inicio_padrao)
    st.session_state.setdefault(chave_fim, ultimo_dia_encerrado)
    f1, f2 = st.columns(2)
    ini_obj = f1.date_input("Início da análise", value=st.session_state[chave_ini], key="cobertura_inicio_input")
    fim_obj = f2.date_input("Fim da análise", value=st.session_state[chave_fim], key="cobertura_fim_input")
    st.session_state[chave_ini] = ini_obj
    st.session_state[chave_fim] = fim_obj
    if fim_obj < ini_obj:
        st.warning("A data final precisa ser igual ou posterior à inicial.")
        return
    if fim_obj >= hoje:
        st.info("O dia atual ainda está em andamento. Para responder quais dias estão realmente fechados, o padrão do Radar PNCP é analisar somente até ontem.")

    with st.spinner("Analisando cobertura local..."):
        mapa = cobertura_base_cache(
            str(DB_PATH), str(JOBS_DB_PATH), ini_obj.isoformat(), fim_obj.isoformat(),
            _assinatura_arquivo(DB_PATH), _assinatura_arquivo(JOBS_DB_PATH),
        )

    total = max(1, int(mapa.get("total_dias", 0) or 0))
    ct = mapa.get("contratacoes") or {}
    itens = mapa.get("itens") or {}
    op = ct.get("operacional") or {}
    lac = ct.get("lacunas_operacionais") or {}
    cert = ct.get("pncp_publicacao") or {}
    rev = ct.get("coleta_atualizacao") or {}
    it_ok = itens.get("completos") or {}
    it_pend = itens.get("pendentes") or {}

    a1, a2, a3, a4 = st.columns(4)
    a1.metric("Contratações · cobertura operacional", f"{int(op.get('dias',0)):,}/{int(mapa.get('total_dias',0)):,} dias".replace(",", "."))
    a2.metric("Lacunas operacionais", f"{int(lac.get('dias',0)):,} dia(s)".replace(",", "."))
    a3.metric("PNCP · publicação certificada", f"{int(cert.get('dias',0)):,} dia(s)".replace(",", "."))
    a4.metric("Itens · dias completos", f"{int(it_ok.get('dias',0)):,} dia(s)".replace(",", "."))

    st.progress(min(1.0, int(op.get("dias",0) or 0) / total))

    if int(lac.get("dias", 0) or 0) == 0:
        st.success(
            "**Contratações:** não há lacuna operacional conhecida no período selecionado. "
            "Isso não significa que todos os dias estejam certificados por uma varredura histórica do PNCP; "
            "essa certificação aparece separadamente abaixo."
        )
    else:
        st.warning(
            f"**Contratações:** há {int(lac.get('dias',0)):,} dia(s) sem evidência local de cobertura. "
            "Esses são os intervalos que realmente merecem uma nova busca de contratações nas APIs.".replace(",", ".")
        )
        _mostrar_intervalos_resumidos("", lac, vazio="Sem lacunas.")

    st.markdown("#### O que ainda precisa de ação")
    pend_contr = int(lac.get("dias", 0) or 0)
    pend_itens = int(itens.get("contratacoes_pendentes", 0) or 0)
    resultados = mapa.get("resultados") or {}
    frescor = str(resultados.get("frescor") or "SEM_DADOS")
    max_r = resultados.get("data_max")
    lag_r = resultados.get("defasagem_dias")

    linhas_acao = []
    if pend_contr:
        linhas_acao.append({"Camada":"Contratações", "Situação":f"{pend_contr} dia(s) sem cobertura", "Ação":"Buscar lacunas no PNCP/Compras.gov"})
    else:
        linhas_acao.append({"Camada":"Contratações", "Situação":"Sem lacunas operacionais", "Ação":"Nenhuma coleta histórica obrigatória"})
    if pend_itens:
        linhas_acao.append({"Camada":"Itens", "Situação":f"{pend_itens:,} contratação(ões) sem conjunto PNCP certificado".replace(",","."), "Ação":"Deixar a fila incremental processar / usar lote manual se necessário"})
    else:
        linhas_acao.append({"Camada":"Itens", "Situação":"Conjuntos do período completos", "Ação":"Nenhuma"})
    if frescor in {"ATRASADO","CRITICO","SEM_DADOS"}:
        linhas_acao.append({"Camada":"Resultados", "Situação":f"Resultado local mais recente: {data_br(max_r) if max_r else '—'} · {lag_r if lag_r is not None else '?'} dia(s) de defasagem · {frescor.replace('CRITICO','CRÍTICO')}", "Ação":"Verificar a coleta automática e o diagnóstico da fonte"})
    else:
        linhas_acao.append({"Camada":"Resultados", "Situação":f"Até {data_br(max_r) if max_r else '—'} · {frescor}", "Ação":"Nenhuma"})
    st.dataframe(pd.DataFrame(linhas_acao), use_container_width=True, hide_index=True)

    col_esq, col_dir = st.columns(2)
    with col_esq:
        st.markdown("#### Contratações · intervalos")
        st.caption("**Cobertura operacional conhecida**")
        _mostrar_intervalos_resumidos("", op, vazio="Nenhum dia coberto no período.")
        st.caption("**Revisão COLETA por atualização**")
        _mostrar_intervalos_resumidos("", rev, vazio="Nenhuma janela COLETA concluída no período.")
    with col_dir:
        st.markdown("#### Certificação e itens")
        st.caption("**PNCP · certificação por publicação**")
        _mostrar_intervalos_resumidos("", cert, vazio="Nenhum dia certificado por publicação no período.")
        st.caption("**Dias com todos os itens PNCP confirmados**")
        _mostrar_intervalos_resumidos("", it_ok, vazio="Nenhum intervalo integralmente confirmado para itens.")

    with st.expander("Ver mapa detalhado e todas as lacunas", expanded=False):
        abas = st.tabs(["Cobertura operacional", "Lacunas", "PNCP por publicação", "COLETA por atualização", "Itens pendentes", "Fontes Compras.gov"] )
        with abas[0]:
            st.dataframe(_df_intervalos(op.get("intervalos") or []), use_container_width=True, hide_index=True)
        with abas[1]:
            st.dataframe(_df_intervalos(lac.get("intervalos") or []), use_container_width=True, hide_index=True)
        with abas[2]:
            st.dataframe(_df_intervalos(cert.get("intervalos") or []), use_container_width=True, hide_index=True)
            sem = ct.get("sem_certificacao_publicacao") or {}
            st.caption(
                f"Dias cobertos operacionalmente, mas ainda sem certificação histórica PNCP por publicação: **{int(sem.get('dias',0)):,}**.".replace(",", ".")
            )
        with abas[3]:
            st.dataframe(_df_intervalos(rev.get("intervalos") or []), use_container_width=True, hide_index=True)
        with abas[4]:
            st.caption(
                f"Contratações pendentes de confirmação integral de itens no período: **{pend_itens:,}**.".replace(",", ".")
            )
            st.dataframe(_df_intervalos(it_pend.get("intervalos") or []), use_container_width=True, hide_index=True)
        with abas[5]:
            csvb = ct.get("comprasgov") or {}
            st.dataframe(_df_intervalos(csvb.get("intervalos") or []), use_container_width=True, hide_index=True)
            orig = ct.get("comprasgov_intervalos_origem") or []
            if orig:
                st.caption("Marcadores de origem utilizados para inferir a cobertura dos arquivos:")
                st.dataframe(pd.DataFrame(orig), use_container_width=True, hide_index=True)

    if pend_contr:
        if st.button("Recuperar período sem cobertura", use_container_width=False, key="abrir_admin_lacunas"):
            abrir_configuracoes("Recuperar histórico")


def pagina_banco(meta: dict, painel: dict, mercado=None, atualizado_em=None, *, titulo=True) -> None:
    if titulo:
        st.title("Situação da base")
    st.caption("Confira quais dados estão disponíveis e o que ainda precisa ser atualizado.")
    area = st.radio("Consultar", ["Visão geral", "Cobertura de um período"], horizontal=True, key="base_visao")
    if area == "Cobertura de um período":
        _mostrar_cobertura_base(meta)
        return
    a, b, c = st.columns(3)
    a.metric("Contratações disponíveis", f"{meta['total_contratacoes']:,}".replace(",", "."))
    b.metric("Itens disponíveis", f"{meta.get('itens_fisicos', meta['total_itens']):,}".replace(",", "."))
    c.metric("Contratações com itens a confirmar", f"{meta.get('pendentes_completude', meta.get('pendentes', 0)):,}".replace(",", "."))
    if meta.get("total_contratacoes"):
        st.write(f"**Publicações presentes na base:** {data_br(meta['data_min'])} a {data_br(meta['data_max'])}.")
        st.caption("Essas datas indicam registros encontrados. Não garantem que todo o intervalo esteja completo; consulte Cobertura de um período.")
    else:
        st.info("A base ainda não possui contratações. A importação inicial pode ser feita em Sistema → Configurações → Recuperar histórico.")
    painel_atualizacao_pesquisa()
    mercado = mercado or {}
    max_r = mercado.get("resultado_data_max")
    st.subheader("Resultados e fornecedores")
    st.write(f"{int(mercado.get('resultados_itens', 0)):,} resultados · {int(mercado.get('fornecedores_identificados', 0)):,} fornecedores identificados.".replace(",", "."))
    st.caption(f"Data mais recente de resultado armazenado: {data_br(max_r) if max_r else 'ainda não disponível'}. A confirmação da janela de contratações não certifica toda a camada de resultados.")
    if atualizado_em:
        st.caption(f"Resumo da base consultado em {_data_hora_br(atualizado_em)}. Os números podem mudar durante a coleta.")
    st.subheader("Coleta em segundo plano")
    cfg, estado = pf.ler_controle(JOBS_DB_PATH)
    st.write("**" + (estado_coleta(estado.get("estado")) if cfg.get("ativo") else "Desativada") + "**")
    if estado.get("detalhe"):
        st.caption(str(estado["detalhe"]))
    if st.button("Configurar coleta automática", key="base_configurar_coleta"):
        abrir_configuracoes("Coleta em segundo plano")
    if st.checkbox("Consultar pendências de itens", key="base_consultar_fila"):
        resumo = servico_metricas_fila(str(DB_PATH)).obter()
        fila = (resumo.get("dados") or {}).get("fila")
        if fila is None:
            st.info("Preparando o resumo de pendências em segundo plano…")
        else:
            st.write(f"Na fila: {int(fila.get('pendentes', 0)) + int(fila.get('em_andamento', 0)) + int(fila.get('erro_transitorio', 0)):,} contratações.".replace(",", "."))
            st.caption(f"Resumo consultado em {_data_hora_br(resumo.get('atualizado_em'))}.")
        if resumo.get("erro"):
            st.warning("Não foi possível renovar as pendências; o último resumo disponível foi mantido.")


def mostrar_detalhes_base(meta: dict, painel: dict, mercado: dict | None = None, atualizado_em: str | None = None) -> None:
    st.subheader("Estatísticas e armazenamento")
    st.caption("Banco conectado: " + str(DB_PATH))

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Contratações", f"{meta['total_contratacoes']:,}".replace(",", "."))
    m2.metric("Itens físicos no banco", f"{meta.get('itens_fisicos', meta['total_itens']):,}".replace(",", "."))
    m3.metric("Pais com item físico", f"{meta.get('pais_com_itens_fisicos', 0):,}".replace(",", "."))
    m4.metric("Armazenamento SQLite", _fmt_bytes(_tamanho_total_sqlite(DB_PATH)))

    st.markdown("### Completude dos itens")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Conjuntos confirmados PNCP", f"{meta.get('conjuntos_confirmados', meta.get('com_itens',0)):,}".replace(",", "."))
    c2.metric("Itens em conjuntos confirmados", f"{meta.get('itens_em_conjuntos_confirmados',0):,}".replace(",", "."))
    c3.metric("Aguardando confirmação", f"{meta.get('pendentes_completude', meta.get('pendentes',0)):,}".replace(",", "."))
    c4.metric("Indisponíveis PNCP", f"{meta['indisponiveis']:,}".replace(",", "."))

    qtd_agregado = int(meta.get('qtd_itens_agregado', 0) or 0)
    fisicos = int(meta.get('itens_fisicos', meta.get('total_itens',0)) or 0)
    diferenca = fisicos - qtd_agregado
    st.info(
        "**Duas métricas diferentes:** `Itens físicos no banco` conta as linhas realmente pesquisáveis "
        "da tabela `itens`. `SUM(qtd_itens)` é um contador mantido nos pais e pode ser menor enquanto "
        "há conjuntos CSV ainda não certificados. "
        f"Neste snapshot: SUM(qtd_itens) = **{qtd_agregado:,}** e a diferença para as linhas físicas = **{diferenca:,}**.".replace(",", ".")
    )

    total_c = max(1, int(meta.get("total_contratacoes", 0) or 0))
    pais_f = int(meta.get("pais_com_itens_fisicos", 0) or 0)
    confirmados = int(meta.get("conjuntos_confirmados", meta.get("com_itens", 0)) or 0)
    fila_itens = resumo_fila_itens_cache(str(DB_PATH))
    st.markdown("### Backlog e fila de itens")
    q1, q2, q3, q4 = st.columns(4)
    q1.metric("Itens aguardando confirmação", f"{int(fila_itens.get('backlog_total',0)):,}".replace(",", "."))
    q2.metric("Elegíveis para fila", f"{int(fila_itens.get('backlog_elegivel',0)):,}".replace(",", "."))
    q3.metric("Na fila agora", f"{int(fila_itens.get('pendentes',0)) + int(fila_itens.get('em_andamento',0)) + int(fila_itens.get('erro_transitorio',0)):,}".replace(",", "."))
    q4.metric("Indisponíveis PNCP", f"{int(fila_itens.get('indisponiveis',0)):,}".replace(",", "."))
    st.caption(
        "A fila usa checkpoint por contratação e página. Itens recebidos ficam em staging até todas as páginas PNCP "
        "terminarem; só então o conjunto ativo é substituído e marcado como completo. "
        f"Staging atual: {int(fila_itens.get('staging_itens',0)):,} item(ns) · "
        f"sem confirmação do pai (não bloqueia): {int(fila_itens.get('backlog_sem_pai_confirmado',0)):,} · "
        f"revalidações pendentes: {int(fila_itens.get('revalidacoes_pendentes',0)):,}.".replace(",", ".")
    )

    st.markdown("### Saúde local da cobertura")
    h1, h2 = st.columns(2)
    with h1:
        st.caption(f"Contratações com pelo menos um item físico: {pais_f:,} / {total_c:,}".replace(",", "."))
        st.progress(min(1.0, pais_f / total_c))
    with h2:
        st.caption(f"Conjuntos de itens já confirmados pelo PNCP: {confirmados:,} / {total_c:,}".replace(",", "."))
        st.progress(min(1.0, confirmados / total_c))

    itens_por_fonte = painel.get("itens_por_fonte") or []
    if itens_por_fonte:
        with st.expander("Proveniência das linhas de itens", expanded=False):
            df_fontes = pd.DataFrame(itens_por_fonte)
            mapa_fonte = {"PNCP_API": "PNCP (API)", "COMPRASGOV_CSV": "Compras.gov (CSV)", "COMPRASGOV_API": "Compras.gov (API)", "(não informada)": "Não informada"}
            df_fontes["fonte"] = df_fontes["fonte"].map(lambda x: mapa_fonte.get(str(x), str(x)))
            st.dataframe(df_fontes.rename(columns={"fonte":"Fonte mais recente", "quantidade":"Itens físicos"}), use_container_width=True, hide_index=True)

    mercado = mercado or {}
    if int(mercado.get("resultados_itens", 0) or 0) > 0:
        st.markdown("### Resultados homologados · Compras.gov")
        r1, r2, r3 = st.columns(3)
        r1.metric("Resultados de itens", f"{int(mercado.get('resultados_itens',0)):,}".replace(",", "."))
        r2.metric("Fornecedores identificados", f"{int(mercado.get('fornecedores_identificados',0)):,}".replace(",", "."))
        r3.metric("Resultados com valor", f"{int(mercado.get('resultados_com_valor',0)):,}".replace(",", "."))
        max_r = mercado.get("resultado_data_max")
        lag_r = mercado.get("resultado_defasagem_dias")
        frescor_r = str(mercado.get("resultado_frescor") or "SEM_DADOS").replace("ATENCAO", "ATENÇÃO").replace("CRITICO", "CRÍTICO")
        st.caption(
            "Esta camada alimenta a pesquisa de preços e fornecedores; não altera a confirmação de completude dos itens. "
            f"Último resultado conhecido: **{data_br(max_r) if max_r else '—'}** · defasagem: **{lag_r if lag_r is not None else '?'} dia(s)** · frescor: **{frescor_r}**."
        )

    texto_snapshot = f" Snapshot atualizado em {_data_hora_br(atualizado_em)}." if atualizado_em else ""
    st.caption(
        f"Período armazenado: {data_br(meta['data_min'])} a {data_br(meta['data_max'])}."
        + texto_snapshot
        + " A página lê estatísticas pré-calculadas e não executa GROUP BY na base principal a cada clique. "
        + "Durante mutirões manuais de itens, esse snapshot é propositalmente adiado para não gastar vários minutos após cada lote."
    )
    ativo_stats = jm.obter_job_ativo(JOBS_DB_PATH)
    if st.button(
        "Atualizar estatísticas agora",
        disabled=ativo_stats is not None,
        help=("Aguarde o job ativo terminar; o snapshot será atualizado automaticamente." if ativo_stats else "Recalcula o snapshot a partir do compras.db."),
        key="recalcular_stats_banco",
    ):
        with st.spinner("Atualizando snapshot da base..."):
            rs.reconstruir_estatisticas(DB_PATH, DB_STATS)
        carregar_snapshot_stats.clear()
        st.rerun()

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("### Contratações por modalidade")
        df_mod = pd.DataFrame(painel["por_modalidade"])
        if not df_mod.empty:
            st.bar_chart(df_mod.set_index("modalidade")["quantidade"])
    with c2:
        st.markdown("### Contratações por UF")
        df_uf = pd.DataFrame(painel["por_uf"])
        if not df_uf.empty:
            st.bar_chart(df_uf.set_index("uf")["quantidade"])

    st.markdown("### Últimos dias armazenados")
    df_dias = pd.DataFrame(painel["por_dia"])
    if not df_dias.empty:
        st.dataframe(df_dias, use_container_width=True, hide_index=True)

    if painel["execucoes"]:
        st.markdown("### Últimas execuções diárias")
        st.dataframe(
            pd.DataFrame(painel["execucoes"]),
            use_container_width=True,
            hide_index=True,
        )


def _garantir_banco_teste() -> None:
    """Cria somente no modo de teste um compras.db vazio com o schema oficial.

    O modo normal nunca cria silenciosamente uma nova base: se a base compartilhada
    de produção estiver ausente, a interface continua parando com erro.
    """
    if not TEST_MODE or DB_PATH.exists():
        return
    from coletar_pncp import conectar as conectar_pncp, criar_banco
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = conectar_pncp(DB_PATH)
    try:
        criar_banco(con)
    finally:
        con.close()
    validar_banco_cache.clear()


def main() -> None:
    inicio_pagina = time.perf_counter()
    ui.aplicar_tema()
    _preservar_campos_consulta()

    from atualizar_radar_pncp import em_atualizacao
    if em_atualizacao(JOBS_DB_PATH):
        from painel_atualizacao import mostrar_andamento
        mostrar_andamento(Path(JOBS_DB_PATH).parent.parent)
        st.stop()
    if (Path(JOBS_DB_PATH).parent/'radar_pncp_agendas_pendentes.json').exists():
        st.warning('Há agendas aguardando reposição após uma atualização. O mutirão dá prioridade a essas execuções.')
        if st.button('Retomar agendas pendentes',key='retomar_agendas_atualizacao'):
            from retomar_agendas import iniciar
            iniciar(JOBS_DB_PATH,BASE_DIR)
            st.info('Retomada solicitada. Consulte Sistema → Atividade; eventuais falhas ficam em logs/retomada_agendas.log.')

    _garantir_banco_teste()
    ok, mensagem = validar_banco_cache(str(DB_PATH))
    if not ok:
        st.error(mensagem)
        st.markdown(
            "O Radar PNCP usa a pasta de dados compartilhada entre versões. "
            f"O banco esperado é: `{DB_PATH}`."
        )
        st.stop()

    if TEST_MODE:
        st.warning(
            "🧪 **MODO DE TESTE ISOLADO** — esta execução usa `radar_dados_teste` "
            "dentro da pasta da versão. A base compartilhada de produção não está conectada."
        )

    snapshot = obter_snapshot_interface()
    meta = snapshot["meta"]
    painel = snapshot["painel"]

    st.sidebar.markdown("## Radar PNCP")
    st.sidebar.caption("Pesquisa de preços e aquisições")
    pagina, area_sistema, area_config = resolver_rota(st.session_state.get("pagina_principal", "Consultar"))
    if area_sistema:
        st.session_state["sistema_area"] = area_sistema
    if area_config:
        st.session_state["config_area"] = area_config
    st.session_state["pagina_principal"] = pagina
    for destino, rotulo, ajuda in zip(PAGINAS, ROTULOS_NAV, AJUDAS_NAV):
        st.sidebar.button(rotulo, use_container_width=True, help=ajuda,
                          type="primary" if pagina == destino else "secondary", key=f"nav_{destino}",
                          on_click=_trocar_pagina, args=(destino,))
    st.sidebar.caption("Versão " + VERSION)
    st.sidebar.caption("Tema claro/escuro: menu ⋮ → Settings.")
    from painel_monitoramento import iniciar_servico
    iniciar_servico()

    st.sidebar.divider()
    if TEST_MODE:
        st.sidebar.warning("🧪 Modo de teste")
    st.sidebar.caption("Base local")
    st.sidebar.write(
        f"**{meta['total_contratacoes']:,}** contratações".replace(",", ".")
    )
    st.sidebar.write(
        f"**{meta['total_itens']:,}** itens".replace(",", ".")
    )
    st.sidebar.caption(
        f"{data_br(meta['data_min'])} → {data_br(meta['data_max'])}"
    )

    # Somente a página escolhida é executada; a navegação fica sempre visível na lateral.
    if pagina == "Consultar":
        pagina_pesquisa(meta)
    elif pagina == "Minhas pesquisas":
        from painel_cestas import mostrar
        mostrar()
    elif pagina == "Acompanhamentos":
        from painel_monitoramento import mostrar
        mostrar()
    elif pagina == "Sistema":
        mostrar_sistema(snapshot)

    ativo_perf = jm.obter_job_ativo(JOBS_DB_PATH)
    if ativo_perf:
        st.sidebar.caption("Uma operação está em andamento. Consulte Sistema → Atividade para acompanhar.")



if __name__ == "__main__":
    main()
