#!/usr/bin/env python3
"""Fila incremental/resumível de itens — RADAR_PNCP/Radar PNCP V0.7.6.

A fila separa presença física de itens de confirmação de completude:
- Compras.gov pode enriquecer linhas locais, mas nunca certifica o conjunto;
- somente uma leitura integral de todas as páginas da API principal do PNCP
  finaliza a fila e marca ``itens_coletados=1``;
- cada página PNCP é persistida em staging antes do checkpoint avançar.

Isso permite retomar uma contratação na página exata após falha sem publicar um
conjunto parcial como se estivesse completo.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable
import time

import requests

from coletar_itens import BASE_PNCP, TAMANHO_PAGINA, extrair_lista, salvar_item
from comprasgov_api_incremental import URL_ITENS_ID, get_json as compras_get_json, sessao as compras_sessao, upsert_item_api
from pncp_rate import criar_sessao_controlada
from proveniencia import FONTE_COMPRASGOV_API, FONTE_PNCP, decompor_pncp_id, garantir_proveniencia, pncp_id_valido
from radar_paths import DB_COMPRAS

TRANSITORIOS = {408, 425, 429, 500, 502, 503, 504}
STATUS_VALIDADO_COMPRASGOV = "VALIDADO_COMPRASGOV"
STATUS_TRANSITORIO_COMPRASGOV = "ERRO_TRANSITORIO_COMPRASGOV"
STATUS_TERMINAIS = {"CONCLUIDA", "INDISPONIVEL_PNCP"}

PREFIXO_PROGRESSO = "__RADAR_PNCP_ITENS_PROGRESSO__"


def codificar_progresso(payload: dict[str, Any]) -> str:
    """Serializa um heartbeat compacto para a tabela ``jobs``.

    O prefixo permite que versões futuras da interface distingam a mensagem
    estruturada das mensagens humanas legadas sem migrar o schema de jobs.
    """
    return PREFIXO_PROGRESSO + json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def decodificar_progresso(texto: str | None) -> dict[str, Any] | None:
    if not texto or not str(texto).startswith(PREFIXO_PROGRESSO):
        return None
    try:
        obj = json.loads(str(texto)[len(PREFIXO_PROGRESSO):])
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


class ReiniciarPorMudancaGlobal(RuntimeError):
    pass


def agora() -> str:
    return datetime.now().isoformat(timespec="seconds")


def coluna_existe(con: sqlite3.Connection, tabela: str, coluna: str) -> bool:
    return any(str(r[1]) == coluna for r in con.execute(f"PRAGMA table_info({tabela})").fetchall())



def _hibrido_disponivel(con: sqlite3.Connection) -> bool:
    return coluna_existe(con, "contratacoes", "itens_validacao_comprasgov_status")


def _filtro_nao_validado(alias: str = "c") -> str:
    return f"(COALESCE({alias}.itens_coletados,0)=1 OR COALESCE({alias}.itens_validacao_comprasgov_status,'') NOT IN ('VALIDADO_COMPRASGOV','ERRO_TRANSITORIO_COMPRASGOV'))"

def garantir_estrutura(con: sqlite3.Connection) -> None:
    con.row_factory = sqlite3.Row
    garantir_proveniencia(con, assumir_legado_pncp=True)
    # Campos de diagnóstico já existiam em coletar_itens, mas a fila pode ser a
    # primeira funcionalidade de itens chamada num banco migrado.
    extras = {
        "itens_tentativas": "INTEGER NOT NULL DEFAULT 0",
        "itens_ultima_tentativa": "TEXT",
        "itens_erro": "TEXT",
        "itens_status": "TEXT NOT NULL DEFAULT 'PENDENTE'",
        "itens_http_status": "INTEGER",
        "itens_indisponivel_em": "TEXT",
        "itens_data_global_certificada": "TEXT",
        "itens_certificado_em": "TEXT",
    }
    for col, definicao in extras.items():
        if not coluna_existe(con, "contratacoes", col):
            con.execute(f"ALTER TABLE contratacoes ADD COLUMN {col} {definicao}")
    if not coluna_existe(con, "contratacoes", "comprasgov_id_compra"):
        con.execute("ALTER TABLE contratacoes ADD COLUMN comprasgov_id_compra TEXT")
    if not coluna_existe(con, "itens", "comprasgov_id_compra"):
        con.execute("ALTER TABLE itens ADD COLUMN comprasgov_id_compra TEXT")
    if not coluna_existe(con, "itens", "comprasgov_id_compra_item"):
        con.execute("ALTER TABLE itens ADD COLUMN comprasgov_id_compra_item TEXT")

    # Compatibilidade V0.6.5.8/5.9 -> V0.7.6: se a camada híbrida já
    # existe, completa as colunas de retry antes de qualquer COLETA tocar nela.
    if coluna_existe(con, "contratacoes", "itens_validacao_comprasgov_status"):
        if not coluna_existe(con, "contratacoes", "itens_validacao_comprasgov_tentativas"):
            con.execute("ALTER TABLE contratacoes ADD COLUMN itens_validacao_comprasgov_tentativas INTEGER NOT NULL DEFAULT 0")
        if not coluna_existe(con, "contratacoes", "itens_validacao_comprasgov_proxima_tentativa"):
            con.execute("ALTER TABLE contratacoes ADD COLUMN itens_validacao_comprasgov_proxima_tentativa TEXT")

    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS itens_fila (
            numero_controle_pncp TEXT PRIMARY KEY,
            origem TEXT NOT NULL,
            motivo TEXT,
            prioridade INTEGER NOT NULL DEFAULT 100,
            status TEXT NOT NULL DEFAULT 'PENDENTE',
            proxima_pagina INTEGER NOT NULL DEFAULT 1,
            total_paginas INTEGER,
            paginas_processadas INTEGER NOT NULL DEFAULT 0,
            itens_stage INTEGER NOT NULL DEFAULT 0,
            tentativas INTEGER NOT NULL DEFAULT 0,
            ultimo_http_status INTEGER,
            ultimo_erro TEXT,
            criado_em TEXT NOT NULL,
            atualizado_em TEXT NOT NULL,
            ultima_tentativa_em TEXT,
            concluido_em TEXT,
            enriquecido_comprasgov INTEGER NOT NULL DEFAULT 0,
            data_global_base TEXT,
            FOREIGN KEY (numero_controle_pncp) REFERENCES contratacoes(numero_controle_pncp) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_itens_fila_status_prioridade
            ON itens_fila(status, prioridade, atualizado_em);

        -- V0.7.6: índices de leitura operacional. A tela de Administração
        -- consulta estas dimensões repetidamente; sem índice, cada abertura da
        -- fila varria mais de um milhão de contratações.
        CREATE INDEX IF NOT EXISTS idx_contratacoes_backlog_status
            ON contratacoes(itens_coletados, itens_status);
        CREATE INDEX IF NOT EXISTS idx_contratacoes_backlog_pai
            ON contratacoes(itens_coletados, pncp_confirmado);

        CREATE TABLE IF NOT EXISTS itens_fila_staging (
            numero_controle_pncp TEXT NOT NULL,
            numero_item INTEGER NOT NULL,
            pagina INTEGER NOT NULL,
            raw_json TEXT NOT NULL,
            recebido_em TEXT NOT NULL,
            PRIMARY KEY (numero_controle_pncp, numero_item),
            FOREIGN KEY (numero_controle_pncp) REFERENCES contratacoes(numero_controle_pncp) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_itens_fila_staging_pagina
            ON itens_fila_staging(numero_controle_pncp, pagina);

        CREATE TABLE IF NOT EXISTS itens_fila_execucoes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            inicio_em TEXT NOT NULL,
            fim_em TEXT,
            modo TEXT NOT NULL,
            limite_contratacoes INTEGER,
            limite_paginas INTEGER,
            contratos_processados INTEGER NOT NULL DEFAULT 0,
            contratos_concluidos INTEGER NOT NULL DEFAULT 0,
            paginas_processadas INTEGER NOT NULL DEFAULT 0,
            itens_confirmados INTEGER NOT NULL DEFAULT 0,
            erros_transitorios INTEGER NOT NULL DEFAULT 0,
            indisponiveis INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'EM_ANDAMENTO'
        );
        """
    )
    if not coluna_existe(con, "itens_fila", "data_global_base"):
        con.execute("ALTER TABLE itens_fila ADD COLUMN data_global_base TEXT")
    con.commit()


def _prioridade_origem(origem: str) -> int:
    return {
        "COLETA_NOVA": 10,
        "COLETA_ATUALIZADA": 20,
        "RECENTE_PENDENTE": 40,
        "BACKLOG_SOB_DEMANDA": 80,
        "MANUAL": 5,
        "REVALIDACAO": 30,
    }.get(origem, 100)


def enfileirar_ids(con: sqlite3.Connection, ids: Iterable[str], origem: str, motivo: str | None = None,
                   prioridade: int | None = None) -> int:
    """Enfileira IDs PNCP válidos.

    V0.7.6: a confirmação do registro-pai no PNCP deixa de ser pré-requisito
    para certificar os itens. O próprio endpoint /itens é a autoridade para o
    conjunto de itens. Para REVALIDACAO, o conjunto ativo permanece publicado
    até a nova leitura integral terminar.
    """
    garantir_estrutura(con)
    stamp = agora()
    qtd = 0
    prio = int(prioridade if prioridade is not None else _prioridade_origem(origem))
    for pid0 in ids:
        pid = str(pid0 or "").strip()
        if not pncp_id_valido(pid):
            continue
        cols_h = ", itens_validacao_comprasgov_status" if _hibrido_disponivel(con) else ""
        c = con.execute(
            "SELECT COALESCE(itens_coletados,0) itens_coletados, COALESCE(pncp_confirmado,0) pncp_confirmado, "
            "data_atualizacao_global, itens_data_global_certificada" + cols_h + " FROM contratacoes WHERE numero_controle_pncp=?",
            (pid,),
        ).fetchone()
        if not c:
            continue
        itens_ok = int(c["itens_coletados"] or 0) == 1
        status_hibrido = str(c["itens_validacao_comprasgov_status"] or "") if "itens_validacao_comprasgov_status" in c.keys() else ""
        if not itens_ok and status_hibrido == STATUS_VALIDADO_COMPRASGOV:
            if origem == "COLETA_ATUALIZADA":
                # Mudança do pai invalida a validação rápida antiga; a COLETA pode então
                # buscar o estado atual diretamente no PNCP.
                con.execute(
                    """UPDATE contratacoes SET itens_validacao_comprasgov_status=NULL,
                              itens_validacao_comprasgov_detalhe='Validação Compras.gov invalidada por atualização COLETA do registro-pai',
                              itens_validacao_comprasgov_proxima_tentativa=NULL
                         WHERE numero_controle_pncp=?""", (pid,)
                )
                status_hibrido = ""
            elif origem != "MANUAL_PNCP":
                # Backlog/seed normal não desperdiça PNCP em conjuntos já validados.
                continue
        if not itens_ok and status_hibrido == STATUS_TRANSITORIO_COMPRASGOV and origem != "MANUAL_PNCP":
            # Erro transitório continua reservado à rota rápida; não vira fallback PNCP.
            continue
        if origem == "REVALIDACAO":
            if not itens_ok:
                # Pendências ainda não certificadas entram pelo backlog normal.
                continue
        elif itens_ok:
            # Conjunto já certificado não é rebaixado por simples reenfileiramento.
            continue

        global_atual = str(c["data_atualizacao_global"] or "") or None
        existente = con.execute(
            "SELECT status,proxima_pagina,data_global_base FROM itens_fila WHERE numero_controle_pncp=?",
            (pid,),
        ).fetchone()

        if origem == "REVALIDACAO":
            # Reabre inclusive uma fila anteriormente concluída/indisponível. Os itens
            # ativos NÃO são apagados agora; só serão trocados atomicamente no final.
            con.execute("DELETE FROM itens_fila_staging WHERE numero_controle_pncp=?", (pid,))
            con.execute(
                """INSERT INTO itens_fila(numero_controle_pncp,origem,motivo,prioridade,status,proxima_pagina,
                           criado_em,atualizado_em,data_global_base)
                   VALUES (?,?,?,?, 'PENDENTE',1,?,?,?)
                   ON CONFLICT(numero_controle_pncp) DO UPDATE SET
                       origem='REVALIDACAO', motivo=excluded.motivo,
                       prioridade=MIN(itens_fila.prioridade,excluded.prioridade),
                       status='PENDENTE', proxima_pagina=1,total_paginas=NULL,
                       paginas_processadas=0,itens_stage=0,ultimo_http_status=NULL,ultimo_erro=NULL,
                       concluido_em=NULL,atualizado_em=excluded.atualizado_em,data_global_base=excluded.data_global_base""",
                (pid, origem, motivo, prio, stamp, stamp, global_atual),
            )
            con.execute(
                "UPDATE contratacoes SET itens_status='REVALIDACAO_PENDENTE', itens_erro=NULL WHERE numero_controle_pncp=?",
                (pid,),
            )
            qtd += 1
            continue

        if existente and str(existente["status"]) == "CONCLUIDA":
            con.execute(
                "UPDATE itens_fila SET status='PENDENTE',proxima_pagina=1,total_paginas=NULL,paginas_processadas=0,"
                "itens_stage=0,concluido_em=NULL,data_global_base=?,atualizado_em=? WHERE numero_controle_pncp=?",
                (global_atual, stamp, pid),
            )
            con.execute("DELETE FROM itens_fila_staging WHERE numero_controle_pncp=?", (pid,))
        elif existente and int(existente["proxima_pagina"] or 1) > 1 and str(existente["data_global_base"] or "") != str(global_atual or ""):
            # A contratação mudou enquanto havia páginas em staging; recomeçar evita
            # combinar páginas de duas versões diferentes do conjunto remoto.
            con.execute("DELETE FROM itens_fila_staging WHERE numero_controle_pncp=?", (pid,))
            con.execute(
                "UPDATE itens_fila SET status='PENDENTE',proxima_pagina=1,total_paginas=NULL,paginas_processadas=0,"
                "itens_stage=0,data_global_base=?,atualizado_em=? WHERE numero_controle_pncp=?",
                (global_atual, stamp, pid),
            )
        cur = con.execute(
            """INSERT INTO itens_fila(numero_controle_pncp,origem,motivo,prioridade,status,proxima_pagina,
                       criado_em,atualizado_em,data_global_base)
               VALUES (?,?,?,?, 'PENDENTE',1,?,?,?)
               ON CONFLICT(numero_controle_pncp) DO UPDATE SET
                   origem=CASE WHEN itens_fila.status='INDISPONIVEL_PNCP' THEN excluded.origem ELSE itens_fila.origem END,
                   motivo=COALESCE(excluded.motivo,itens_fila.motivo),
                   prioridade=MIN(itens_fila.prioridade,excluded.prioridade),
                   status=CASE WHEN itens_fila.status='EM_ANDAMENTO' THEN 'EM_ANDAMENTO' ELSE 'PENDENTE' END,
                   atualizado_em=excluded.atualizado_em,
                   data_global_base=CASE WHEN itens_fila.status='EM_ANDAMENTO' THEN itens_fila.data_global_base ELSE excluded.data_global_base END""",
            (pid, origem, motivo, prio, stamp, stamp, global_atual),
        )
        if cur.rowcount:
            qtd += 1
    con.commit()
    return qtd

def enfileirar_recentes_pendentes(con: sqlite3.Connection, fim: str | date | None = None, dias: int = 60) -> int:
    """Semeia a fila com pendências recentes sem presumir completude de itens locais."""
    garantir_estrutura(con)
    d_fim = date.fromisoformat(str(fim)) if fim else date.today()
    inicio = (d_fim - timedelta(days=max(1, int(dias)) - 1)).isoformat()
    rows = con.execute(
        """SELECT numero_controle_pncp FROM contratacoes
             WHERE COALESCE(itens_coletados,0)=0
               AND substr(COALESCE(data_publicacao_pncp,''),1,10) BETWEEN ? AND ?
               AND COALESCE(itens_status,'PENDENTE') NOT IN ('INDISPONIVEL_PNCP','REMOVIDA_PNCP')""",
        (inicio, d_fim.isoformat()),
    ).fetchall()
    return enfileirar_ids(con, (r[0] for r in rows), "RECENTE_PENDENTE", f"janela recente {inicio}→{d_fim.isoformat()}")


def resumo_backlog(con: sqlite3.Connection, *, preparar: bool = True) -> dict[str, int]:
    """Resumo operacional indexado da fila/certificação.

    V0.7.6 evita ``SUM(CASE ...)`` sobre toda ``contratacoes``. Os números
    são obtidos por contagens seletivas que usam os índices de estado. Isso é
    importante tanto para a interface quanto para o controlador persistente.
    """
    if preparar:
        garantir_estrutura(con)
    h = _hibrido_disponivel(con)

    total = int(con.execute(
        "SELECT COUNT(*) FROM contratacoes WHERE itens_coletados=0"
    ).fetchone()[0] or 0)
    indisponiveis = int(con.execute(
        """SELECT COUNT(*) FROM contratacoes
             WHERE itens_coletados=0
               AND itens_status IN ('INDISPONIVEL_PNCP','REMOVIDA_PNCP')"""
    ).fetchone()[0] or 0)
    sem_pai = int(con.execute(
        """SELECT COUNT(*) FROM contratacoes
             WHERE itens_coletados=0 AND pncp_confirmado<>1"""
    ).fetchone()[0] or 0)

    validados = transitorios = sobreposicao = 0
    if h:
        validados = int(con.execute(
            """SELECT COUNT(*) FROM contratacoes
                 WHERE itens_validacao_comprasgov_status='VALIDADO_COMPRASGOV'
                   AND itens_coletados=0"""
        ).fetchone()[0] or 0)
        transitorios = int(con.execute(
            """SELECT COUNT(*) FROM contratacoes
                 WHERE itens_validacao_comprasgov_status='ERRO_TRANSITORIO_COMPRASGOV'
                   AND itens_coletados=0"""
        ).fetchone()[0] or 0)
        # Evita subtrair duas vezes um caso excepcional que tenha simultaneamente
        # um estado PNCP indisponível e um estado híbrido excluído da fila.
        sobreposicao = int(con.execute(
            """SELECT COUNT(*) FROM contratacoes
                 WHERE itens_coletados=0
                   AND itens_status IN ('INDISPONIVEL_PNCP','REMOVIDA_PNCP')
                   AND itens_validacao_comprasgov_status IN
                       ('VALIDADO_COMPRASGOV','ERRO_TRANSITORIO_COMPRASGOV')"""
        ).fetchone()[0] or 0)

    elegiveis = max(0, total - indisponiveis - validados - transitorios + sobreposicao)
    reval = int(con.execute(
        "SELECT COUNT(*) FROM itens_fila WHERE origem='REVALIDACAO' AND status IN ('PENDENTE','EM_ANDAMENTO','ERRO_TEMPORARIO')"
    ).fetchone()[0] or 0)
    return {
        "total": total,
        "elegiveis": elegiveis,
        "sem_pai_confirmado": sem_pai,
        "aguardando_pai": sem_pai,
        "indisponiveis": indisponiveis,
        "revalidacoes": reval,
        "validados_comprasgov": validados,
        "transitorios_comprasgov": transitorios,
    }

def contar_backlog_total(con: sqlite3.Connection) -> int:
    return resumo_backlog(con)["total"]


def abastecer_fila_backlog(con: sqlite3.Connection, limite_alvo: int, *, verbose: bool = False) -> int:
    """Completa a fila ativa, sob demanda, até ``limite_alvo`` entradas.

    Entradas já parcialmente processadas ou com erro recuperável permanecem na
    frente. Só o déficit é buscado no backlog global, priorizando publicações mais
    recentes. Assim o botão manual e as rotinas automáticas sempre conseguem
    trabalhar mesmo quando a fila persistente está vazia.
    """
    garantir_estrutura(con)
    alvo = max(1, int(limite_alvo))
    if _hibrido_disponivel(con):
        ativos = int(con.execute(
            """SELECT COUNT(*) FROM itens_fila f JOIN contratacoes c USING(numero_controle_pncp)
                 WHERE f.status IN ('EM_ANDAMENTO','PENDENTE','ERRO_TEMPORARIO')
                   AND COALESCE(c.itens_validacao_comprasgov_status,'') NOT IN ('VALIDADO_COMPRASGOV','ERRO_TRANSITORIO_COMPRASGOV')"""
        ).fetchone()[0] or 0)
    else:
        ativos = int(con.execute(
            "SELECT COUNT(*) FROM itens_fila WHERE status IN ('EM_ANDAMENTO','PENDENTE','ERRO_TEMPORARIO')"
        ).fetchone()[0] or 0)
    faltam = max(0, alvo - ativos)
    if faltam <= 0:
        return 0

    filtro_h = " AND " + _filtro_nao_validado("c") if _hibrido_disponivel(con) else ""
    rows = con.execute(
        f"""SELECT c.numero_controle_pncp
             FROM contratacoes c
             LEFT JOIN itens_fila f USING(numero_controle_pncp)
            WHERE COALESCE(c.itens_coletados,0)=0
              AND COALESCE(c.itens_status,'PENDENTE') NOT IN ('INDISPONIVEL_PNCP','REMOVIDA_PNCP')
              {filtro_h}
              AND (f.numero_controle_pncp IS NULL OR f.status='CONCLUIDA')
            ORDER BY substr(COALESCE(c.data_publicacao_pncp,''),1,10) DESC,
                     COALESCE(c.data_atualizacao_global,'') DESC,
                     c.numero_controle_pncp DESC
            LIMIT ?""",
        (faltam,),
    ).fetchall()
    ids = [str(r[0]) for r in rows]
    adicionadas = enfileirar_ids(
        con,
        ids,
        "BACKLOG_SOB_DEMANDA",
        "abastecimento automático do backlog de itens não certificados",
        prioridade=_prioridade_origem("BACKLOG_SOB_DEMANDA"),
    ) if ids else 0
    if verbose and adicionadas:
        print(
            f"[Itens] Backlog → fila: {adicionadas:,} contratação(ões) adicionada(s) sob demanda."
            .replace(",", "."),
            flush=True,
        )
    return adicionadas


def resumo_fila(con: sqlite3.Connection, *, preparar: bool = True, apenas_ativos: bool = False) -> dict[str, int]:
    if preparar:
        garantir_estrutura(con)
    out = {"pendentes": 0, "em_andamento": 0, "erro_transitorio": 0, "concluidas": 0,
           "indisponiveis": 0, "staging_itens": 0, "total": 0}
    mapa = {
        "PENDENTE": "pendentes", "EM_ANDAMENTO": "em_andamento", "ERRO_TEMPORARIO": "erro_transitorio",
        "CONCLUIDA": "concluidas", "INDISPONIVEL_PNCP": "indisponiveis",
    }
    filtro = " AND f.status IN ('PENDENTE','EM_ANDAMENTO','ERRO_TEMPORARIO')" if apenas_ativos else ""
    if _hibrido_disponivel(con):
        rows_status = con.execute(
            f"""SELECT f.status,COUNT(*) qtd FROM itens_fila f
                 JOIN contratacoes c USING(numero_controle_pncp)
                WHERE (c.itens_coletados=1 OR COALESCE(c.itens_validacao_comprasgov_status,'') NOT IN ('VALIDADO_COMPRASGOV','ERRO_TRANSITORIO_COMPRASGOV'))
                  {filtro}
                GROUP BY f.status"""
        ).fetchall()
    else:
        rows_status = con.execute("SELECT f.status,COUNT(*) qtd FROM itens_fila f WHERE 1=1 " + filtro + " GROUP BY f.status").fetchall()
    for r in rows_status:
        k = mapa.get(str(r[0]))
        if k:
            out[k] = int(r[1] or 0)
        out["total"] += int(r[1] or 0)
    out["staging_itens"] = int(con.execute("SELECT COUNT(*) FROM itens_fila_staging").fetchone()[0] or 0)
    backlog = resumo_backlog(con, preparar=False)
    out["backlog_total"] = backlog["total"]
    out["backlog_elegivel"] = backlog["elegiveis"]
    out["backlog_aguardando_pai"] = backlog["aguardando_pai"]
    out["backlog_sem_pai_confirmado"] = backlog["sem_pai_confirmado"]
    out["revalidacoes_pendentes"] = backlog["revalidacoes"]
    out["backlog_indisponivel"] = backlog["indisponiveis"]
    out["validados_comprasgov"] = backlog.get("validados_comprasgov",0)
    out["transitorios_comprasgov"] = backlog.get("transitorios_comprasgov",0)
    return out


def _http_status(exc: BaseException) -> int | None:
    resp = getattr(exc, "response", None)
    try:
        return int(resp.status_code) if resp is not None else None
    except Exception:
        return None


def _payload_pagina(sessao: requests.Session, pid: str, pagina: int, timeout: int) -> tuple[list[dict[str, Any]], int | None, int | None]:
    dec = decompor_pncp_id(pid)
    if not dec:
        raise ValueError(f"Número de controle PNCP inválido: {pid}")
    cnpj, ano, seq = dec
    url = f"{BASE_PNCP}/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
    resp = sessao.get(url, params={"pagina": pagina, "tamanhoPagina": TAMANHO_PAGINA}, timeout=(15, timeout))
    if resp.status_code == 204:
        return [], None, 0
    if resp.status_code >= 400:
        corpo = (resp.text or "").strip().replace("\r", " ").replace("\n", " ")[:800]
        raise requests.HTTPError(f"HTTP {resp.status_code} para {resp.url}" + (f" | {corpo}" if corpo else ""), response=resp)
    payload: Any = resp.json() if resp.content else None
    rows = extrair_lista(payload)
    total_paginas = None
    restantes = None
    if isinstance(payload, dict):
        try:
            total_paginas = int(payload.get("totalPaginas")) if payload.get("totalPaginas") is not None else None
        except Exception:
            total_paginas = None
        try:
            restantes = int(payload.get("paginasRestantes")) if payload.get("paginasRestantes") is not None else None
        except Exception:
            restantes = None
    return rows, total_paginas, restantes


def _staging_salvar_pagina(con: sqlite3.Connection, pid: str, pagina: int, rows: list[dict[str, Any]]) -> None:
    stamp = agora()
    for item in rows:
        n = item.get("numeroItem")
        if n is None:
            raise ValueError(f"PNCP retornou item sem numeroItem em {pid}, página {pagina}")
        con.execute(
            """INSERT INTO itens_fila_staging(numero_controle_pncp,numero_item,pagina,raw_json,recebido_em)
               VALUES (?,?,?,?,?)
               ON CONFLICT(numero_controle_pncp,numero_item) DO UPDATE SET
                 pagina=excluded.pagina,raw_json=excluded.raw_json,recebido_em=excluded.recebido_em""",
            (pid, int(n), int(pagina), json.dumps(item, ensure_ascii=False), stamp),
        )


def _finalizar_conjunto(con: sqlite3.Connection, pid: str) -> int:
    staged = con.execute(
        "SELECT raw_json FROM itens_fila_staging WHERE numero_controle_pncp=? ORDER BY numero_item", (pid,)
    ).fetchall()
    itens = [json.loads(str(r[0])) for r in staged]
    locais = int(con.execute("SELECT COUNT(*) FROM itens WHERE numero_controle_pncp=?", (pid,)).fetchone()[0] or 0)
    if not itens and locais > 0:
        raise RuntimeError("PNCP devolveu conjunto vazio para contratação que já possui itens locais; confirmação recusada por segurança")

    # Se a COLETA atualizou dataAtualizacaoGlobal enquanto páginas estavam em staging,
    # não misturamos versões. O conjunto ativo antigo permanece disponível e a fila
    # volta para a página 1 na próxima tentativa.
    vr = con.execute(
        """SELECT f.data_global_base,c.data_atualizacao_global
             FROM itens_fila f JOIN contratacoes c USING(numero_controle_pncp)
            WHERE f.numero_controle_pncp=?""",
        (pid,),
    ).fetchone()
    base_global = str(vr[0] or "") if vr else ""
    global_atual = str(vr[1] or "") if vr else ""
    if base_global and global_atual and base_global != global_atual:
        stamp = agora()
        con.execute("DELETE FROM itens_fila_staging WHERE numero_controle_pncp=?", (pid,))
        con.execute(
            """UPDATE itens_fila SET status='PENDENTE',proxima_pagina=1,total_paginas=NULL,
                      paginas_processadas=0,itens_stage=0,data_global_base=?,atualizado_em=?,ultimo_erro=NULL
                 WHERE numero_controle_pncp=?""",
            (global_atual, stamp, pid),
        )
        con.commit()
        raise ReiniciarPorMudancaGlobal(
            f"dataAtualizacaoGlobal mudou durante a coleta ({base_global} → {global_atual})"
        )

    stamp = agora()
    con.execute("BEGIN IMMEDIATE")
    try:
        con.execute("DELETE FROM itens WHERE numero_controle_pncp=?", (pid,))
        for item in itens:
            salvar_item(con, pid, item)
        con.execute(
            """UPDATE contratacoes SET itens_coletados=1,qtd_itens=?,itens_erro=NULL,itens_status='OK',
                      itens_http_status=NULL,itens_indisponivel_em=NULL,itens_ultima_tentativa=?,itens_fonte=?,
                      itens_data_global_certificada=?,itens_certificado_em=?
                 WHERE numero_controle_pncp=?""",
            (len(itens), stamp, FONTE_PNCP, global_atual or None, stamp, pid),
        )
        con.execute(
            """UPDATE itens_fila SET status='CONCLUIDA',concluido_em=?,atualizado_em=?,ultimo_erro=NULL,
                      ultimo_http_status=NULL,itens_stage=?,data_global_base=? WHERE numero_controle_pncp=?""",
            (stamp, stamp, len(itens), global_atual or None, pid),
        )
        con.execute("DELETE FROM itens_fila_staging WHERE numero_controle_pncp=?", (pid,))
        con.commit()
    except Exception:
        con.rollback()
        raise
    return len(itens)

def _enriquecer_comprasgov(con: sqlite3.Connection, sc: requests.Session, pid: str, timeout: int = 45) -> int:
    if not coluna_existe(con, "contratacoes", "comprasgov_id_compra"):
        return 0
    row = con.execute("SELECT comprasgov_id_compra FROM contratacoes WHERE numero_controle_pncp=?", (pid,)).fetchone()
    idc = str(row[0]).strip() if row and row[0] else ""
    if not idc:
        return 0
    obj = None
    for params in ({"tipo": "idCompra", "codigo": idc}, {"idCompra": idc}):
        try:
            obj = compras_get_json(sc, URL_ITENS_ID, params, timeout=(10, timeout))
            break
        except requests.HTTPError as exc:
            if _http_status(exc) not in (400, 404, 422):
                break
        except Exception:
            break
    if not obj:
        return 0
    rows = obj.get("resultado") or []
    if not isinstance(rows, list):
        return 0
    total = 0
    for item in rows:
        if upsert_item_api(con, item, agora()[:10], agora()):
            total += 1
    if total:
        con.execute("UPDATE itens_fila SET enriquecido_comprasgov=1,atualizado_em=? WHERE numero_controle_pncp=?", (agora(), pid))
        con.commit()
    return total


def _selecionar(con: sqlite3.Connection, limite: int) -> list[sqlite3.Row]:
    # EM_ANDAMENTO vem primeiro. Validados Compras.gov e erros transitórios da
    # rota rápida não são consumidos pelo fallback PNCP histórico.
    filtro_h = " AND " + _filtro_nao_validado("c") if _hibrido_disponivel(con) else ""
    ordem_recente = ''
    if con.execute("SELECT 1 FROM sqlite_master WHERE name='coleta_janelas_pesquisa'").fetchone():
        ordem_recente = """CASE WHEN EXISTS (
          SELECT 1 FROM coleta_alvos_pesquisa a
          WHERE a.numero_controle_pncp=f.numero_controle_pncp
            AND (a.inicio,a.fim)=(SELECT inicio,fim FROM coleta_janelas_pesquisa
                                   ORDER BY iniciado_em DESC LIMIT 1)
        ) THEN 0 ELSE 1 END,"""
    return con.execute(
        f"""SELECT f.*,c.comprasgov_id_compra,COALESCE(c.itens_coletados,0) AS itens_coletados_atual
             FROM itens_fila f
             JOIN contratacoes c USING(numero_controle_pncp)
            WHERE f.status IN ('EM_ANDAMENTO','PENDENTE','ERRO_TEMPORARIO')
              {filtro_h}
            ORDER BY CASE f.status WHEN 'EM_ANDAMENTO' THEN 0 WHEN 'PENDENTE' THEN 1 ELSE 2 END,
                     {ordem_recente}
                     f.prioridade ASC,f.atualizado_em ASC
            LIMIT ?""",
        (max(1, int(limite)),),
    ).fetchall()


def processar_fila(con: sqlite3.Connection, *, modo: str = "diario", limite_contratacoes: int | None = None,
                   limite_paginas: int | None = None, sessao_pncp: requests.Session | None = None,
                   limiter: Any | None = None, timeout: int = 90, verbose: bool = True,
                   parar_quando: Callable[[], bool] | None = None,
                   max_erros_transitorios: int | None = None,
                   progresso_cb: Callable[[dict[str, Any]], None] | None = None) -> dict[str, Any]:
    garantir_estrutura(con)
    if limite_contratacoes is None:
        limite_contratacoes = 250 if modo == "diario" else 2000
    if limite_paginas is None:
        limite_paginas = 400 if modo == "diario" else 4000
    if sessao_pncp is None or limiter is None:
        sessao_pncp, limiter = criar_sessao_controlada(1.25)
    sc = compras_sessao()

    ini = agora()
    ex = con.execute(
        """INSERT INTO itens_fila_execucoes(inicio_em,modo,limite_contratacoes,limite_paginas)
           VALUES (?,?,?,?)""", (ini, modo, int(limite_contratacoes), int(limite_paginas))
    )
    exid = int(ex.lastrowid)
    con.commit()

    stats = Counter()
    ids_confirmados: list[str] = []
    ids_enriquecidos: set[str] = set()
    stats["abastecidas_backlog"] = abastecer_fila_backlog(con, int(limite_contratacoes), verbose=verbose)
    rows = _selecionar(con, limite_contratacoes)
    # O heartbeat não faz COUNT no backlog global: essa varredura é evitada
    # durante o lote para não trocar observabilidade por I/O pesado.
    fila_inicio: dict[str, int] = {}
    t0_mono = time.monotonic()
    ultimo_heartbeat = 0.0

    def emitir_progresso(*, force: bool = False, contrato_atual: str | None = None, pagina_atual: int | None = None) -> None:
        nonlocal ultimo_heartbeat
        if progresso_cb is None:
            return
        agora_mono = time.monotonic()
        # Evita centenas de writes/segundo quando uma resposta traz várias páginas
        # rapidamente, mas mantém a tela viva em contratos lentos.
        if not force and agora_mono - ultimo_heartbeat < 4.0:
            return
        ultimo_heartbeat = agora_mono
        payload = {
            "kind": "itens",
            "modo": modo,
            "contratos_processados": int(stats["contratos"]),
            "contratos_selecionados": int(len(rows)),
            "contratos_limite": int(limite_contratacoes),
            "paginas_processadas": int(stats["paginas"]),
            "paginas_limite": int(limite_paginas),
            "conjuntos_confirmados": int(stats["concluidos"]),
            "itens_confirmados": int(stats["itens_confirmados"]),
            "erros_recuperaveis": int(stats["erros_transitorios"]),
            "indisponiveis": int(stats["indisponiveis"]),
            "revalidados": int(stats["revalidados"]),
            "backlog_inicial": int(fila_inicio.get("backlog_total", 0)),
            "backlog_elegivel_inicial": int(fila_inicio.get("backlog_elegivel", 0)),
            "elapsed_seconds": max(0.0, agora_mono - t0_mono),
            "contrato_atual": contrato_atual,
            "pagina_atual": pagina_atual,
        }
        try:
            progresso_cb(payload)
        except Exception:
            # Telemetria nunca pode derrubar uma coleta.
            pass

    def esperar_retentativa(func, *args):
        try:
            return func(*args, parar_quando)
        except InterruptedError:
            stats["parada_segura"] = 1
            return None

    emitir_progresso(force=True)
    if verbose:
        print(f"[Itens] Fila PNCP · modo {modo}: até {limite_contratacoes:,} contratação(ões) / {limite_paginas:,} página(s).".replace(",", "."), flush=True)
        print(f"        Selecionadas nesta execução: {len(rows):,}.".replace(",", "."), flush=True)

    for pos, row in enumerate(rows, 1):
        if parar_quando is not None and parar_quando():
            stats["parada_segura"] = 1
            break
        if stats["paginas"] >= int(limite_paginas):
            stats["orcamento_paginas"] = 1
            break
        pid = str(row["numero_controle_pncp"])
        pagina = max(1, int(row["proxima_pagina"] or 1))
        con.execute(
            """UPDATE itens_fila SET status='EM_ANDAMENTO',tentativas=tentativas+1,ultima_tentativa_em=?,atualizado_em=?
               WHERE numero_controle_pncp=?""", (agora(), agora(), pid)
        )
        con.execute("UPDATE contratacoes SET itens_tentativas=COALESCE(itens_tentativas,0)+1,itens_ultima_tentativa=? WHERE numero_controle_pncp=?", (agora(), pid))
        con.commit()
        stats["contratos"] += 1
        emitir_progresso(force=False, contrato_atual=pid, pagina_atual=pagina)

        trans = 0
        while stats["paginas"] < int(limite_paginas):
            if parar_quando is not None and parar_quando():
                stats["parada_segura"] = 1
                break
            try:
                rows_page, total_paginas, restantes = _payload_pagina(sessao_pncp, pid, pagina, timeout)
                trans = 0
                _staging_salvar_pagina(con, pid, pagina, rows_page)
                final = False
                if total_paginas is not None:
                    final = pagina >= total_paginas
                elif restantes is not None:
                    final = restantes <= 0
                elif not rows_page or len(rows_page) < TAMANHO_PAGINA:
                    final = True
                proxima = pagina + 1
                staged = int(con.execute("SELECT COUNT(*) FROM itens_fila_staging WHERE numero_controle_pncp=?", (pid,)).fetchone()[0] or 0)
                con.execute(
                    """UPDATE itens_fila SET proxima_pagina=?,total_paginas=COALESCE(?,total_paginas),
                              paginas_processadas=paginas_processadas+1,itens_stage=?,ultimo_erro=NULL,
                              ultimo_http_status=NULL,atualizado_em=? WHERE numero_controle_pncp=?""",
                    (proxima, total_paginas, staged, agora(), pid),
                )
                con.commit()
                stats["paginas"] += 1
                emitir_progresso(force=False, contrato_atual=pid, pagina_atual=pagina)
                if verbose and (pagina == 1 or pagina % 5 == 0 or final):
                    total_txt = f"/{total_paginas}" if total_paginas else ""
                    print(f"        ↳ {pid}: página {pagina}{total_txt} · staging {staged:,} item(ns)".replace(",", "."), flush=True)
                if final:
                    try:
                        n = _finalizar_conjunto(con, pid)
                    except ReiniciarPorMudancaGlobal as exc:
                        stats["reinicios_global"] += 1
                        if verbose:
                            print(f"        ↻ {pid}: {exc}; staging descartado e recomeço agendado.", flush=True)
                        break
                    stats["concluidos"] += 1
                    stats["itens_confirmados"] += n
                    if str(row["origem"] or "") == "REVALIDACAO":
                        stats["revalidados"] += 1
                    ids_confirmados.append(pid)
                    if verbose:
                        verbo = "revalidado" if str(row["origem"] or "") == "REVALIDACAO" else "confirmado"
                        print(f"        ✓ {pid}: conjunto PNCP {verbo} · {n:,} item(ns).".replace(",", "."), flush=True)
                    emitir_progresso(force=False, contrato_atual=pid, pagina_atual=pagina)
                    break
                pagina = proxima
            except requests.HTTPError as exc:
                code = _http_status(exc)
                if code == 429:
                    if verbose:
                        print(f"        HTTP 429 em {pid} · página {pagina}; cooldown adaptativo.", flush=True)
                    resultado_espera = esperar_retentativa(limiter.cooldown_429, getattr(exc, "response", None))
                    if resultado_espera is None:
                        break
                    espera, parar = resultado_espera
                    if parar:
                        code = 429
                        trans = 99
                    else:
                        continue
                if code in TRANSITORIOS:
                    trans += 1
                    if trans <= 3:
                        espera = esperar_retentativa(limiter.backoff_transitorio, trans)
                        if espera is None:
                            break
                        if verbose:
                            print(f"        HTTP {code} em {pid} · página {pagina}; backoff {espera:.0f}s.", flush=True)
                        continue
                stamp = agora()
                if code in (404, 410):
                    revalidando = str(row["origem"] or "") == "REVALIDACAO" and int(row["itens_coletados_atual"] or 0) == 1
                    con.execute(
                        """UPDATE itens_fila SET status='INDISPONIVEL_PNCP',ultimo_http_status=?,ultimo_erro=?,
                                  atualizado_em=?,concluido_em=? WHERE numero_controle_pncp=?""",
                        (code, str(exc)[:3000], stamp, stamp, pid),
                    )
                    if revalidando:
                        # Preserva o último conjunto certificado; só registra que a
                        # revalidação atual não pôde ser concluída.
                        con.execute(
                            """UPDATE contratacoes SET itens_status='REVALIDACAO_INDISPONIVEL_PNCP',
                                      itens_http_status=?,itens_erro=?,itens_indisponivel_em=?,itens_ultima_tentativa=?
                                 WHERE numero_controle_pncp=?""",
                            (code, str(exc)[:3000], stamp, stamp, pid),
                        )
                        stats["revalidacao_indisponivel"] += 1
                    else:
                        con.execute(
                            """UPDATE contratacoes SET itens_status='INDISPONIVEL_PNCP',itens_http_status=?,itens_erro=?,
                                      itens_indisponivel_em=?,itens_ultima_tentativa=? WHERE numero_controle_pncp=?""",
                            (code, str(exc)[:3000], stamp, stamp, pid),
                        )
                    con.commit(); stats["indisponiveis"] += 1
                else:
                    en = _enriquecer_comprasgov(con, sc, pid, timeout=45)
                    stats["enriquecidos_comprasgov"] += en
                    if en:
                        ids_enriquecidos.add(pid)
                    con.execute(
                        """UPDATE itens_fila SET status='ERRO_TEMPORARIO',ultimo_http_status=?,ultimo_erro=?,atualizado_em=?
                           WHERE numero_controle_pncp=?""", (code, str(exc)[:3000], stamp, pid)
                    )
                    if int(row["itens_coletados_atual"] or 0) == 1:
                        con.execute(
                            """UPDATE contratacoes SET itens_status='REVALIDACAO_PENDENTE',itens_http_status=?,itens_erro=?,itens_ultima_tentativa=?
                               WHERE numero_controle_pncp=?""", (code, str(exc)[:3000], stamp, pid)
                        )
                    else:
                        con.execute(
                            """UPDATE contratacoes SET itens_status='ERRO_TEMPORARIO',itens_http_status=?,itens_erro=?,itens_ultima_tentativa=?
                               WHERE numero_controle_pncp=?""", (code, str(exc)[:3000], stamp, pid)
                        )
                    con.commit(); stats["erros_transitorios"] += 1
                break
            except (requests.Timeout, requests.ConnectionError) as exc:
                trans += 1
                if trans <= 3:
                    espera = esperar_retentativa(limiter.backoff_transitorio, trans)
                    if espera is None:
                        break
                    if verbose:
                        print(f"        {type(exc).__name__} em {pid} · página {pagina}; backoff {espera:.0f}s.", flush=True)
                    continue
                stamp = agora(); en = _enriquecer_comprasgov(con, sc, pid, timeout=45)
                stats["enriquecidos_comprasgov"] += en
                if en:
                    ids_enriquecidos.add(pid)
                con.execute("UPDATE itens_fila SET status='ERRO_TEMPORARIO',ultimo_erro=?,atualizado_em=? WHERE numero_controle_pncp=?", (str(exc)[:3000], stamp, pid))
                if int(row["itens_coletados_atual"] or 0) == 1:
                    con.execute("UPDATE contratacoes SET itens_status='REVALIDACAO_PENDENTE',itens_erro=?,itens_ultima_tentativa=? WHERE numero_controle_pncp=?", (str(exc)[:3000], stamp, pid))
                con.commit(); stats["erros_transitorios"] += 1
                break
            except Exception as exc:
                stamp = agora()
                erro_txt = f"{type(exc).__name__}: {exc}"[:3000]
                con.execute("UPDATE itens_fila SET status='ERRO_TEMPORARIO',ultimo_erro=?,atualizado_em=? WHERE numero_controle_pncp=?", (erro_txt, stamp, pid))
                if int(row["itens_coletados_atual"] or 0) == 1:
                    con.execute("UPDATE contratacoes SET itens_status='REVALIDACAO_PENDENTE',itens_erro=?,itens_ultima_tentativa=? WHERE numero_controle_pncp=?", (erro_txt, stamp, pid))
                con.commit(); stats["erros_transitorios"] += 1
                if verbose:
                    print(f"        ERRO {pid} · página {pagina}: {type(exc).__name__}: {exc}", flush=True)
                break

        if stats.get("parada_segura"):
            break
        if max_erros_transitorios is not None and stats["erros_transitorios"] >= max(1, int(max_erros_transitorios)):
            stats["limite_erros_transitorios"] = 1
            if verbose:
                print(
                    f"[Itens] Limite de segurança atingido: {stats['erros_transitorios']} erro(s) recuperável(is) no lote.",
                    flush=True,
                )
            break

    fim = agora()
    status = "PAUSADA" if stats["parada_segura"] else ("CONCLUIDA" if not stats["erros_transitorios"] else "CONCLUIDA_COM_ERROS_RECUPERAVEIS")
    con.execute(
        """UPDATE itens_fila_execucoes SET fim_em=?,contratos_processados=?,contratos_concluidos=?,paginas_processadas=?,
                  itens_confirmados=?,erros_transitorios=?,indisponiveis=?,status=? WHERE id=?""",
        (fim, stats["contratos"], stats["concluidos"], stats["paginas"], stats["itens_confirmados"],
         stats["erros_transitorios"], stats["indisponiveis"], status, exid),
    )
    con.commit()
    out = dict(stats)
    out["status"] = status
    out["ids_confirmados"] = ids_confirmados
    out["ids_enriquecidos"] = sorted(ids_enriquecidos)
    out["fila"] = resumo_fila(con)
    emitir_progresso(force=True)
    if verbose:
        f = out["fila"]
        print(
            f"[Itens] Resumo: {stats['contratos']:,} contrato(s) processado(s) · {stats['paginas']:,} página(s) · "
            f"{stats['concluidos']:,} conjunto(s) confirmado(s) · {stats['itens_confirmados']:,} item(ns) PNCP."
            .replace(",", "."), flush=True,
        )
        print(
            f"        Fila: {f['pendentes']:,} pendente(s) · {f['em_andamento']:,} em andamento · "
            f"{f['erro_transitorio']:,} erro(s) recuperável(is) · {f['indisponiveis']:,} indisponível(is)."
            .replace(",", "."), flush=True,
        )
        print(
            f"        Backlog sem certificação: {f.get('backlog_total',0):,} total · "
            f"{f.get('backlog_elegivel',0):,} elegível(is) · "
            f"{f.get('backlog_sem_pai_confirmado',0):,} sem pai PNCP confirmado (não bloqueia) · "
            f"{f.get('backlog_indisponivel',0):,} indisponível(is)/removido(s) · "
            f"{f.get('revalidacoes_pendentes',0):,} revalidação(ões) pendente(s)."
            .replace(",", "."), flush=True,
        )
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Radar PNCP V0.7.6 — fila incremental de itens")
    ap.add_argument("--db", type=Path, default=DB_COMPRAS)
    ap.add_argument("--modo", choices=("diario", "revisao", "manual"), default="manual")
    ap.add_argument("--limite-contratacoes", type=int)
    ap.add_argument("--limite-paginas", type=int)
    ap.add_argument("--seed-dias", type=int, default=0, help="Semeia pendências dos últimos N dias antes de processar")
    ap.add_argument("--fim", help="Data final do seed AAAA-MM-DD")
    ap.add_argument("--jobs-db", type=Path, help="Banco de jobs para heartbeat da interface")
    ap.add_argument("--job-id", type=int, help="ID do job para heartbeat da interface")
    args = ap.parse_args()
    con = sqlite3.connect(str(args.db), timeout=60)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON")
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=60000")
    garantir_estrutura(con)
    if args.seed_dias:
        n = enfileirar_recentes_pendentes(con, args.fim, args.seed_dias)
        print(f"Seed recente: {n:,} contratação(ões) considerada(s) para a fila.".replace(",", "."), flush=True)
    progresso_cb = None
    if args.jobs_db is not None and args.job_id is not None:
        import job_manager as jm

        def progresso_cb(payload: dict[str, Any]) -> None:
            alvo = int(payload.get("contratos_selecionados") or payload.get("contratos_limite") or 1)
            atual = int(payload.get("contratos_processados") or 0)
            jm.atualizar_job(
                args.job_id,
                args.jobs_db,
                etapa=f"Itens · {atual:,}/{alvo:,} contratos".replace(",", "."),
                progresso_atual=atual,
                progresso_total=max(1, alvo),
                mensagem=codificar_progresso(payload),
            )

    out = processar_fila(
        con, modo=args.modo, limite_contratacoes=args.limite_contratacoes, limite_paginas=args.limite_paginas,
        progresso_cb=progresso_cb,
    )
    con.close()
    return 0 if not out.get("erros_transitorios") else 75


if __name__ == "__main__":
    raise SystemExit(main())
