#!/usr/bin/env python3
"""Rota híbrida de itens Compras.gov-first — RADAR_PNCP/Radar PNCP V0.7.6.

A V0.7.6 mantém duas garantias separadas:
- ``itens_coletados = 1`` continua significando exclusivamente que o conjunto
  foi lido integralmente da API do PNCP;
- o Compras.gov pode validar, em alta confiança, a identidade/conjunto de itens
  sem promover a contratação ao nível de certificação PNCP.

Em produção, a mesma política conservadora validada nos pilotos é aplicada:
a rota Compras.gov nunca apaga nem sobrescreve itens já existentes. Quando a API
apresenta um superconjunto, apenas números de item ainda ausentes são inseridos.
Respostas estruturais recusadas seguem para fallback PNCP; falhas transitórias
(429/5xx/rede) ficam na rota rápida para retry antes de qualquer fallback.
"""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
import statistics
import threading
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import job_manager as jm
import itens_incremental as ii
from comprasgov_api_incremental import URL_ITENS_ID
from importar_comprasgov_csv import (
    COLS_ITEM,
    ITEM_MAP,
    parametros_item,
    resolver_colunas,
)
from proveniencia import FONTE_COMPRASGOV_API
from radar_paths import DB_COMPRAS, DB_JOBS

VERSAO = "1.0.0"
STATUS_VALIDADO = "VALIDADO_COMPRASGOV"
STATUS_FALLBACK = "FALLBACK_PNCP"
STATUS_TRANSITORIO = "ERRO_TRANSITORIO_COMPRASGOV"
MAX_TENTATIVAS_TRANSITORIAS = 3
TRANSITORIOS = {408, 425, 429, 500, 502, 503, 504}
_THREAD_LOCAL = threading.local()

# Campos que definem o núcleo do item para detectar empate ambíguo entre duas
# linhas com a mesma data de atualização. Situação/resultado ficam fora porque
# os diagnósticos 2.3–2.5 mostraram versionamento e defasagem entre as fontes.
CORE_FIELDS = (
    "descricaoResumida", "descricaodetalhada", "materialOuServico",
    "materialOuServicoNome", "unidadeMedida", "quantidade",
    "valorUnitarioEstimado", "valorTotal", "criterioJulgamentoIdPncp",
    "criterioJulgamentoNome",
)


def agora() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _fmt(n: int) -> str:
    return f"{int(n):,}".replace(",", ".")


def coluna_existe(con: sqlite3.Connection, tabela: str, coluna: str) -> bool:
    return any(str(r[1]) == coluna for r in con.execute(f"PRAGMA table_info({tabela})"))


def garantir_estrutura(con: sqlite3.Connection) -> None:
    """Migração aditiva e idempotente da camada de validação Compras.gov."""
    con.row_factory = sqlite3.Row
    extras = {
        "itens_validacao_comprasgov_status": "TEXT",
        "itens_validacao_comprasgov_em": "TEXT",
        "itens_validacao_comprasgov_qtd": "INTEGER",
        "itens_validacao_comprasgov_novos": "INTEGER NOT NULL DEFAULT 0",
        "itens_validacao_comprasgov_data_max": "TEXT",
        "itens_validacao_comprasgov_detalhe": "TEXT",
        "itens_validacao_comprasgov_job_id": "INTEGER",
        "itens_validacao_comprasgov_tentativas": "INTEGER NOT NULL DEFAULT 0",
        "itens_validacao_comprasgov_proxima_tentativa": "TEXT",
    }
    for col, definicao in extras.items():
        if not coluna_existe(con, "contratacoes", col):
            con.execute(f"ALTER TABLE contratacoes ADD COLUMN {col} {definicao}")

    con.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_contratacoes_validacao_comprasgov
          ON contratacoes(itens_validacao_comprasgov_status, itens_coletados);
        CREATE INDEX IF NOT EXISTS idx_contratacoes_validacao_retry
          ON contratacoes(itens_validacao_comprasgov_status, itens_coletados, itens_validacao_comprasgov_proxima_tentativa);
        CREATE INDEX IF NOT EXISTS idx_contratacoes_hibrido_fresh
          ON contratacoes(itens_coletados, data_publicacao_pncp DESC, numero_controle_pncp)
          WHERE comprasgov_id_compra IS NOT NULL AND comprasgov_id_compra<>''
            AND (itens_validacao_comprasgov_status IS NULL OR itens_validacao_comprasgov_status='');
        CREATE INDEX IF NOT EXISTS idx_contratacoes_hibrido_retry_due
          ON contratacoes(itens_validacao_comprasgov_status, itens_coletados, itens_validacao_comprasgov_proxima_tentativa, numero_controle_pncp)
          WHERE comprasgov_id_compra IS NOT NULL AND comprasgov_id_compra<>'';

        CREATE TABLE IF NOT EXISTS itens_validacao_comprasgov_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            numero_controle_pncp TEXT NOT NULL,
            comprasgov_id_compra TEXT,
            executado_em TEXT NOT NULL,
            job_id INTEGER,
            status TEXT NOT NULL,
            motivo TEXT,
            total_registros INTEGER,
            linhas_recebidas INTEGER NOT NULL DEFAULT 0,
            linhas_pid INTEGER NOT NULL DEFAULT 0,
            linhas_pid_estrangeiro INTEGER NOT NULL DEFAULT 0,
            itens_locais_antes INTEGER NOT NULL DEFAULT 0,
            itens_api_unicos INTEGER NOT NULL DEFAULT 0,
            itens_novos INTEGER NOT NULL DEFAULT 0,
            grupos_duplicados INTEGER NOT NULL DEFAULT 0,
            empates_latest_core INTEGER NOT NULL DEFAULT 0,
            data_max_api TEXT,
            segundos REAL
        );
        CREATE INDEX IF NOT EXISTS idx_validacao_comprasgov_log_pid
          ON itens_validacao_comprasgov_log(numero_controle_pncp, executado_em);
        CREATE INDEX IF NOT EXISTS idx_validacao_comprasgov_log_job
          ON itens_validacao_comprasgov_log(job_id, status);
        """
    )
    con.commit()


def _nova_sessao() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=3, connect=3, read=3, backoff_factor=0.7,
        status_forcelist=tuple(sorted(TRANSITORIOS)),
        allowed_methods=frozenset({"GET"}), respect_retry_after_header=True,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=8, pool_maxsize=8))
    s.headers.update({
        "Accept": "application/json,*/*",
        "User-Agent": f"Mozilla/5.0 RADAR_PNCP-Itens-Hibrido/{VERSAO}",
    })
    return s


def _sessao_thread() -> requests.Session:
    s = getattr(_THREAD_LOCAL, "sessao", None)
    if s is None:
        s = _nova_sessao()
        _THREAD_LOCAL.sessao = s
    return s


def _parse_dt(v: Any) -> datetime | None:
    if v is None:
        return None
    s = str(v).strip().replace("Z", "").replace(" ", "T")
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _canon(v: Any) -> Any:
    if isinstance(v, str):
        return " ".join(v.split()).strip().casefold().replace("≥", ">=")
    if isinstance(v, float):
        return round(v, 8)
    return v


def _item_num(row: dict[str, Any]) -> int | None:
    v = row.get("numeroItemPncp")
    if v is None:
        v = row.get("numeroItemCompra")
    try:
        n = int(v)
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def _dt_rank(row: dict[str, Any]) -> datetime:
    return _parse_dt(row.get("dataAtualizacaoPncp") or row.get("dataInclusaoPncp")) or datetime.min


def selecionar_piloto(con: sqlite3.Connection, limite: int) -> list[dict[str, Any]]:
    """Seleciona pendências com idCompra e ao menos uma evidência local de item.

    Usa data recente primeiro porque é a região mais dinâmica do backlog. A
    cobertura histórica já foi medida separadamente no diagnóstico Fase 2.5.
    """
    rows = con.execute(
        """
        SELECT c.numero_controle_pncp, c.comprasgov_id_compra,
               c.data_publicacao_pncp, c.itens_status,
               c.itens_validacao_comprasgov_status
          FROM contratacoes c
         WHERE COALESCE(c.itens_coletados,0)=0
           AND NULLIF(TRIM(COALESCE(c.comprasgov_id_compra,'')),'') IS NOT NULL
           AND COALESCE(c.itens_status,'PENDENTE') NOT IN ('INDISPONIVEL_PNCP','REMOVIDA_PNCP')
           AND COALESCE(c.itens_validacao_comprasgov_status,'') <> ?
           AND EXISTS (
               SELECT 1 FROM itens i
                WHERE i.numero_controle_pncp=c.numero_controle_pncp
           )
         ORDER BY COALESCE(c.data_publicacao_pncp,'') DESC, c.numero_controle_pncp
         LIMIT ?
        """,
        (STATUS_VALIDADO, int(limite)),
    ).fetchall()
    return [dict(r) for r in rows]


def _consultar(rec: dict[str, Any]) -> dict[str, Any]:
    t0 = time.perf_counter()
    out = dict(rec)
    try:
        r = _sessao_thread().get(
            URL_ITENS_ID,
            params={
                "tipo": "idCompra", "codigo": str(rec["comprasgov_id_compra"]),
                "pagina": 1, "tamanhoPagina": 50,
            },
            timeout=(10, 75),
        )
        out["http"] = int(r.status_code)
        r.raise_for_status()
        obj = r.json()
        if not isinstance(obj, dict):
            raise RuntimeError("Resposta JSON do Compras.gov não é um objeto.")
        rows = obj.get("resultado") or []
        if not isinstance(rows, list):
            raise RuntimeError("Campo resultado do Compras.gov não é uma lista.")
        out["total_registros"] = obj.get("totalRegistros")
        out["total_paginas"] = obj.get("totalPaginas")
        out["rows"] = rows
    except Exception as exc:
        out["erro"] = f"{type(exc).__name__}: {exc}"[:2000]
        out["rows"] = []
    out["segundos"] = round(time.perf_counter() - t0, 3)
    return out


def _analisar(rec: dict[str, Any], local_nums: set[int]) -> dict[str, Any]:
    rows = rec.pop("rows", [])
    pid = str(rec["numero_controle_pncp"])
    grupos: dict[int, list[dict[str, Any]]] = defaultdict(list)
    foreign = 0
    sem_num = 0
    datas: list[datetime] = []

    for row in rows:
        if not isinstance(row, dict):
            continue
        rpid = str(row.get("idContratacaoPNCP") or row.get("numeroControlePNCPCompra") or "")
        if rpid != pid:
            foreign += 1
            continue
        n = _item_num(row)
        if n is None:
            sem_num += 1
            continue
        grupos[n].append(row)
        d = _parse_dt(row.get("dataAtualizacaoPncp") or row.get("dataInclusaoPncp"))
        if d:
            datas.append(d)

    api_nums = set(grupos)
    latest: dict[int, dict[str, Any]] = {}
    empates = 0
    grupos_dup = 0
    core_mudou = 0

    for n, rs in grupos.items():
        if len(rs) > 1:
            grupos_dup += 1
            assinaturas = {tuple(_canon(x.get(c)) for c in CORE_FIELDS) for x in rs}
            if len(assinaturas) > 1:
                core_mudou += 1
        maior = max(_dt_rank(x) for x in rs)
        tops = [x for x in rs if _dt_rank(x) == maior]
        if len(tops) > 1:
            assinaturas_top = {tuple(_canon(x.get(c)) for c in CORE_FIELDS) for x in tops}
            if len(assinaturas_top) > 1:
                empates += 1
                continue
        latest[n] = tops[-1]

    faltantes = local_nums - api_nums
    novos = api_nums - local_nums
    try:
        total_int = int(rec.get("total_registros"))
    except (TypeError, ValueError):
        total_int = None
    total_confere = total_int is None or total_int == len(rows)

    motivo = None
    if rec.get("erro"):
        motivo = "ERRO_HTTP_OU_JSON"
    elif not rows:
        motivo = "RESPOSTA_VAZIA"
    elif foreign:
        motivo = "PID_DIVERGENTE_NA_RESPOSTA"
    elif sem_num:
        motivo = "ITEM_SEM_NUMERO"
    elif not total_confere:
        motivo = "TOTAL_REGISTROS_DIVERGENTE"
    elif empates:
        motivo = "EMPATE_LATEST_AMBIGUO"
    elif not local_nums:
        motivo = "SEM_EVIDENCIA_LOCAL"
    elif faltantes:
        motivo = "API_PERDE_ITEM_LOCAL"

    rec.update({
        "linhas_recebidas": len(rows),
        "linhas_pid": sum(len(v) for v in grupos.values()),
        "foreign": foreign,
        "sem_num": sem_num,
        "itens_api_unicos": len(api_nums),
        "itens_locais": len(local_nums),
        "novos_nums": sorted(novos),
        "faltantes_nums": sorted(faltantes),
        "grupos_duplicados": grupos_dup,
        "core_mudou_entre_versoes": core_mudou,
        "empates_latest_core": empates,
        "data_max_api": max(datas).isoformat(timespec="seconds") if datas else None,
        "motivo": motivo,
        "aceito": motivo is None,
        "latest": latest,
    })
    return rec


def _local_nums(con: sqlite3.Connection, pid: str) -> set[int]:
    return {
        int(r[0]) for r in con.execute(
            "SELECT numero_item FROM itens WHERE numero_controle_pncp=?", (pid,)
        ).fetchall() if r[0] is not None
    }


def _inserir_ausentes(
    con: sqlite3.Connection, pid: str, rows_latest: dict[int, dict[str, Any]], novos: list[int]
) -> int:
    """Insere somente chaves ausentes; nunca executa UPDATE de item existente."""
    if not novos:
        return 0
    placeholders = ",".join("?" for _ in COLS_ITEM)
    sql = f"INSERT OR IGNORE INTO itens ({','.join(COLS_ITEM)}) VALUES ({placeholders})"
    inseridos = 0
    stamp = agora()
    for n in novos:
        row = rows_latest.get(int(n))
        if not row:
            raise RuntimeError(f"Item {n} sem versão latest disponível.")
        resolvido = resolver_colunas(list(row.keys()), ITEM_MAP)
        params, situ = parametros_item(row, resolvido, "HBRIDO", stamp, fonte=FONTE_COMPRASGOV_API)
        if params is None or situ != "VALIDO":
            raise RuntimeError(f"Item {n} não pôde ser convertido ({situ}).")
        # Defesa adicional contra resposta cruzada.
        d = dict(zip(COLS_ITEM, params))
        if str(d.get("numero_controle_pncp") or "") != pid or int(d.get("numero_item") or 0) != int(n):
            raise RuntimeError(f"Item {n} convertido para chave divergente.")
        cur = con.execute(sql, params)
        inseridos += max(0, int(cur.rowcount or 0))
    return inseridos


def _registrar_log(con: sqlite3.Connection, rec: dict[str, Any], status: str, motivo: str | None,
                   itens_novos: int, job_id: int | None) -> None:
    con.execute(
        """
        INSERT INTO itens_validacao_comprasgov_log(
            numero_controle_pncp,comprasgov_id_compra,executado_em,job_id,status,motivo,
            total_registros,linhas_recebidas,linhas_pid,linhas_pid_estrangeiro,
            itens_locais_antes,itens_api_unicos,itens_novos,grupos_duplicados,
            empates_latest_core,data_max_api,segundos
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            rec["numero_controle_pncp"], rec.get("comprasgov_id_compra"), agora(), job_id,
            status, motivo, rec.get("total_registros"), rec.get("linhas_recebidas",0),
            rec.get("linhas_pid",0), rec.get("foreign",0), rec.get("itens_locais",0),
            rec.get("itens_api_unicos",0), itens_novos, rec.get("grupos_duplicados",0),
            rec.get("empates_latest_core",0), rec.get("data_max_api"), rec.get("segundos"),
        ),
    )


def _atualizar_job(job_id: int | None, jobs_db: Path | None, atual: int, total: int, etapa: str,
                   mensagem: str | None = None) -> None:
    if job_id is None or jobs_db is None:
        return
    jm.atualizar_job(
        job_id, jobs_db, progresso_atual=int(atual), progresso_total=max(1,int(total)),
        etapa=etapa, mensagem=mensagem or etapa,
    )


def executar_piloto(db: Path, *, limite: int = 5000, workers: int = 6,
                    jobs_db: Path | None = None, job_id: int | None = None) -> dict[str, Any]:
    if limite <= 0:
        raise ValueError("limite deve ser > 0")
    if not 1 <= workers <= 12:
        raise ValueError("workers deve ficar entre 1 e 12")

    con = sqlite3.connect(str(db), timeout=60)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON")
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=60000")
    # Garante primeiro a estrutura/proveniência de itens da versão-base. Assim,
    # uma atualização direta de versão antiga não depende de ter executado antes
    # o mutirão PNCP ou outra rotina que crie essas colunas.
    ii.garantir_estrutura(con)
    garantir_estrutura(con)

    selecionados = selecionar_piloto(con, limite)
    total = len(selecionados)
    itens_antes = int(con.execute("SELECT COUNT(*) FROM itens").fetchone()[0] or 0)
    cert_antes = int(con.execute("SELECT COUNT(*) FROM contratacoes WHERE COALESCE(itens_coletados,0)=1").fetchone()[0] or 0)
    val_antes = int(con.execute(
        "SELECT COUNT(*) FROM contratacoes WHERE COALESCE(itens_coletados,0)=0 AND itens_validacao_comprasgov_status=?",
        (STATUS_VALIDADO,),
    ).fetchone()[0] or 0)

    print("="*96, flush=True)
    print(f"Radar PNCP V{VERSAO} — PILOTO HÍBRIDO DE ITENS · COMPRAS.GOV FIRST", flush=True)
    print(f"Selecionadas: {_fmt(total)} contratação(ões) · workers: {workers}.", flush=True)
    print("Regra: NÃO apaga/reescreve item existente e NÃO altera itens_coletados=1.", flush=True)
    print("Respostas recusadas permanecem para PNCP fallback.", flush=True)
    print("="*96, flush=True)

    if not selecionados:
        con.close()
        return {"selecionados":0,"validados":0,"fallback":0,"novos_itens":0,"status":"SEM_ELEGIVEIS"}

    _atualizar_job(job_id,jobs_db,0,total,"Piloto híbrido · consultando Compras.gov")
    stats = Counter()
    fallbacks = Counter()
    detalhes_fallback: list[dict[str, Any]] = []
    superconjuntos: list[dict[str, Any]] = []
    tempos: list[float] = []
    inicio = time.perf_counter()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(_consultar, r): r for r in selecionados}
        for idx, fut in enumerate(as_completed(futs), 1):
            base = futs[fut]
            try:
                bruto = fut.result()
            except Exception as exc:
                bruto = dict(base)
                bruto.update({"erro":f"{type(exc).__name__}: {exc}","rows":[],"segundos":0.0})

            pid = str(base["numero_controle_pncp"])
            local = _local_nums(con, pid)
            analisado = _analisar(bruto, local)
            analisado["numero_controle_pncp"] = pid
            if isinstance(analisado.get("segundos"),(int,float)):
                tempos.append(float(analisado["segundos"]))

            # Se outro processo externo certificou o PID após a seleção, não tocamos nele.
            ja_cert = int(con.execute(
                "SELECT COALESCE(itens_coletados,0) FROM contratacoes WHERE numero_controle_pncp=?", (pid,)
            ).fetchone()[0] or 0)
            if ja_cert:
                stats["ja_certificado"] += 1
                _registrar_log(con, analisado, "IGNORADO_CERTIFICADO_PNCP", "JA_CERTIFICADO_PNCP", 0, job_id)
                con.commit()
            elif analisado["aceito"]:
                novos_previstos = list(analisado["novos_nums"])
                try:
                    con.execute("SAVEPOINT piloto_hibrido")
                    inseridos = _inserir_ausentes(con, pid, analisado["latest"], novos_previstos)
                    detalhe = (
                        f"Conjunto Compras.gov válido: {analisado['itens_api_unicos']} item(ns) único(s); "
                        f"{analisado['itens_locais']} já existiam; {inseridos} novo(s) inserido(s). "
                        "Certificação PNCP permanece pendente."
                    )
                    con.execute(
                        """
                        UPDATE contratacoes
                           SET itens_validacao_comprasgov_status=?,
                               itens_validacao_comprasgov_em=?,
                               itens_validacao_comprasgov_qtd=?,
                               itens_validacao_comprasgov_novos=?,
                               itens_validacao_comprasgov_data_max=?,
                               itens_validacao_comprasgov_detalhe=?,
                               itens_validacao_comprasgov_job_id=?
                         WHERE numero_controle_pncp=? AND COALESCE(itens_coletados,0)=0
                        """,
                        (STATUS_VALIDADO, agora(), analisado["itens_api_unicos"], inseridos,
                         analisado.get("data_max_api"), detalhe, job_id, pid),
                    )
                    _registrar_log(con, analisado, STATUS_VALIDADO, None, inseridos, job_id)
                    con.execute("RELEASE piloto_hibrido")
                    con.commit()
                    stats["validados"] += 1
                    stats["novos_itens"] += inseridos
                    stats["duplicidades"] += analisado["grupos_duplicados"]
                    stats["core_versionado"] += analisado["core_mudou_entre_versoes"]
                    if novos_previstos and len(superconjuntos) < 30:
                        superconjuntos.append({
                            "pid":pid,"idCompra":base["comprasgov_id_compra"],
                            "locais":analisado["itens_locais"],"api":analisado["itens_api_unicos"],
                            "novos":novos_previstos[:30],"inseridos":inseridos,
                        })
                except Exception as exc:
                    try:
                        con.execute("ROLLBACK TO piloto_hibrido")
                        con.execute("RELEASE piloto_hibrido")
                    except Exception:
                        pass
                    motivo = f"FALHA_GRAVACAO:{type(exc).__name__}"
                    fallbacks[motivo] += 1
                    stats["fallback"] += 1
                    analisado["erro_gravacao"] = str(exc)[:1000]
                    con.execute(
                        """UPDATE contratacoes SET itens_validacao_comprasgov_status=?,
                                  itens_validacao_comprasgov_em=?,itens_validacao_comprasgov_detalhe=?,
                                  itens_validacao_comprasgov_job_id=?
                             WHERE numero_controle_pncp=? AND COALESCE(itens_coletados,0)=0""",
                        (STATUS_FALLBACK,agora(),motivo,job_id,pid),
                    )
                    _registrar_log(con, analisado, STATUS_FALLBACK, motivo, 0, job_id)
                    con.commit()
            else:
                motivo = str(analisado.get("motivo") or "RECUSADO")
                fallbacks[motivo] += 1
                stats["fallback"] += 1
                detalhe = (
                    f"Fallback PNCP: {motivo}; locais={analisado['itens_locais']}; "
                    f"api={analisado['itens_api_unicos']}; foreign={analisado['foreign']}; "
                    f"faltantes={analisado['faltantes_nums'][:20]}"
                )
                con.execute(
                    """UPDATE contratacoes SET itens_validacao_comprasgov_status=?,
                              itens_validacao_comprasgov_em=?,itens_validacao_comprasgov_qtd=?,
                              itens_validacao_comprasgov_novos=0,
                              itens_validacao_comprasgov_data_max=?,itens_validacao_comprasgov_detalhe=?,
                              itens_validacao_comprasgov_job_id=?
                         WHERE numero_controle_pncp=? AND COALESCE(itens_coletados,0)=0""",
                    (STATUS_FALLBACK,agora(),analisado["itens_api_unicos"],analisado.get("data_max_api"),
                     detalhe,job_id,pid),
                )
                _registrar_log(con, analisado, STATUS_FALLBACK, motivo, 0, job_id)
                con.commit()
                if len(detalhes_fallback) < 50:
                    detalhes_fallback.append({
                        "pid":pid,"idCompra":base["comprasgov_id_compra"],"motivo":motivo,
                        "linhas":analisado["linhas_recebidas"],"locais":analisado["itens_locais"],
                        "api":analisado["itens_api_unicos"],"foreign":analisado["foreign"],
                        "faltantes":analisado["faltantes_nums"][:20],"erro":analisado.get("erro"),
                    })

            if idx % 10 == 0 or idx == total:
                elapsed = max(0.001,time.perf_counter()-inicio)
                vel = idx/(elapsed/60.0)
                msg = (
                    f"Piloto híbrido {idx}/{total} · validados {stats['validados']} · "
                    f"fallback {stats['fallback']} · novos itens {stats['novos_itens']} · {vel:.1f} contr./min"
                )
                print(msg, flush=True)
                _atualizar_job(job_id,jobs_db,idx,total,f"Piloto híbrido · {idx}/{total}",msg)

    itens_depois = int(con.execute("SELECT COUNT(*) FROM itens").fetchone()[0] or 0)
    cert_depois = int(con.execute("SELECT COUNT(*) FROM contratacoes WHERE COALESCE(itens_coletados,0)=1").fetchone()[0] or 0)
    val_depois = int(con.execute(
        "SELECT COUNT(*) FROM contratacoes WHERE COALESCE(itens_coletados,0)=0 AND itens_validacao_comprasgov_status=?",
        (STATUS_VALIDADO,),
    ).fetchone()[0] or 0)
    con.close()

    dur = time.perf_counter()-inicio
    tempos_ord = sorted(tempos)
    med = statistics.median(tempos_ord) if tempos_ord else None
    p95 = tempos_ord[min(len(tempos_ord)-1, math.ceil(.95*len(tempos_ord))-1)] if tempos_ord else None
    tmax = max(tempos_ord) if tempos_ord else None
    lentas_10 = sum(1 for x in tempos_ord if x >= 10.0)
    lentas_30 = sum(1 for x in tempos_ord if x >= 30.0)
    lentas_45 = sum(1 for x in tempos_ord if x >= 45.0)
    resumo = {
        "selecionados": total, "validados": int(stats["validados"]),
        "fallback": int(stats["fallback"]), "ja_certificado": int(stats["ja_certificado"]),
        "novos_itens": int(stats["novos_itens"]), "grupos_duplicados": int(stats["duplicidades"]),
        "core_versionado": int(stats["core_versionado"]), "fallbacks": dict(fallbacks),
        "itens_antes":itens_antes,"itens_depois":itens_depois,
        "cert_antes":cert_antes,"cert_depois":cert_depois,
        "val_antes":val_antes,"val_depois":val_depois,
        "duracao_seg":dur,"tempo_mediano_http":med,"tempo_p95_http":p95,
        "tempo_max_http":tmax,"consultas_10s":lentas_10,"consultas_30s":lentas_30,"consultas_45s":lentas_45,
        "superconjuntos":superconjuntos,"detalhes_fallback":detalhes_fallback,
    }

    outdir = Path(db).parent / "diagnosticos"
    outdir.mkdir(parents=True,exist_ok=True)
    path = outdir / f"piloto_hibrido_v{VERSAO.replace(chr(46), chr(95))}_{datetime.now():%Y%m%d_%H%M%S}.txt"
    linhas = [
        f"Radar PNCP V{VERSAO} — PILOTO HÍBRIDO COMPRAS.GOV FIRST",
        f"Gerado em: {datetime.now():%d/%m/%Y %H:%M:%S}",
        f"Banco: {Path(db).resolve()}",
        "",
        "=== RESULTADO ===",
        f"Selecionados: {total}",
        f"Validados Compras.gov: {stats['validados']} ({(100*stats['validados']/total if total else 0):.3f}%)",
        f"Fallback PNCP: {stats['fallback']} ({(100*stats['fallback']/total if total else 0):.3f}%)",
        f"Já certificados durante o piloto: {stats['ja_certificado']}",
        f"Itens novos inseridos (somente chaves ausentes): {stats['novos_itens']}",
        f"Grupos duplicados aceitos: {stats['duplicidades']}",
        f"Grupos com core versionado: {stats['core_versionado']}",
        "",
        "=== INTEGRIDADE ===",
        f"Itens físicos antes/depois: {itens_antes} -> {itens_depois} (delta {itens_depois-itens_antes:+d})",
        f"Certificados PNCP antes/depois: {cert_antes} -> {cert_depois} (delta {cert_depois-cert_antes:+d})",
        f"Validados Compras.gov pendentes PNCP antes/depois: {val_antes} -> {val_depois} (delta {val_depois-val_antes:+d})",
        "Regra de segurança: nenhum item existente é apagado ou sobrescrito pelo piloto.",
        "itens_coletados=1 permanece reservado à certificação PNCP.",
        "",
        "=== DESEMPENHO ===",
        f"Duração total: {dur:.1f}s",
        f"Tempo HTTP mediano: {med:.3f}s" if med is not None else "Tempo HTTP mediano: n/d",
        f"Tempo HTTP p95: {p95:.3f}s" if p95 is not None else "Tempo HTTP p95: n/d",
        f"Tempo HTTP máximo: {tmax:.3f}s" if tmax is not None else "Tempo HTTP máximo: n/d",
        f"Consultas >=10s / >=30s / >=45s: {lentas_10} / {lentas_30} / {lentas_45}",
        f"Velocidade efetiva: {(total/(dur/60.0) if dur>0 else 0):.2f} contratações/min",
        "",
        "=== MOTIVOS DE FALLBACK ===",
    ]
    if fallbacks:
        linhas += [f"{k}: {v}" for k,v in fallbacks.most_common()]
    else:
        linhas.append("Nenhum.")
    linhas += ["", "=== SUPERCONJUNTOS / ITENS NOVOS (ATÉ 30) ==="]
    linhas += [json.dumps(x,ensure_ascii=False) for x in superconjuntos] or ["Nenhum."]
    linhas += ["", "=== FALLBACKS (ATÉ 50) ==="]
    linhas += [json.dumps(x,ensure_ascii=False) for x in detalhes_fallback] or ["Nenhum."]
    path.write_text("\n".join(linhas)+"\n",encoding="utf-8")
    resumo["relatorio"] = str(path)

    print("", flush=True)
    print("="*96, flush=True)
    print(f"[Híbrido] Validados: {_fmt(stats['validados'])} · fallback PNCP: {_fmt(stats['fallback'])} · novos itens: {_fmt(stats['novos_itens'])}.", flush=True)
    print(f"[Híbrido] Itens físicos: {_fmt(itens_antes)} -> {_fmt(itens_depois)} · certificados PNCP: {_fmt(cert_antes)} -> {_fmt(cert_depois)}.", flush=True)
    print(f"[Híbrido] Relatório: {path}", flush=True)
    print("="*96, flush=True)
    return resumo



from pausa_atualizacao import PausaAtualizacao, EXIT_ATUALIZACAO


class DeadlineHibrido(RuntimeError):
    pass


class PacedRateLimiter:
    """Limitador global suave para evitar bursts contra Dados Abertos.

    O piloto de 5.000 mostrou blocos rápidos seguidos de pausas ~60 s e 429.
    Em produção distribuímos as chamadas uniformemente: por padrão 95 req/min.
    """
    def __init__(self, rpm: int = 95, parar_quando=None):
        self.parar_quando = parar_quando
        if not 30 <= int(rpm) <= 120:
            raise ValueError("rpm deve ficar entre 30 e 120")
        self.rpm = int(rpm)
        self.intervalo = 60.0 / float(self.rpm)
        self._lock = threading.Lock()
        self._proximo = 0.0
        self._cooldown_ate = 0.0

    def aguardar(self, deadline: datetime | None = None) -> None:
        while True:
            if self.parar_quando and self.parar_quando():
                raise DeadlineHibrido("Atualização do sistema; nenhuma nova consulta será iniciada.")
            with self._lock:
                now = time.monotonic()
                alvo = max(self._proximo, self._cooldown_ate)
                if alvo <= now:
                    self._proximo = now + self.intervalo
                    return
                espera = alvo - now
            if deadline is not None and datetime.now() + timedelta(seconds=espera + 2.0) >= deadline:
                raise DeadlineHibrido("Janela de segurança atingida antes da próxima chamada Compras.gov.")
            time.sleep(min(espera, 1.0))

    def penalizar(self, segundos: float) -> None:
        segundos = max(1.0, float(segundos))
        with self._lock:
            self._cooldown_ate = max(self._cooldown_ate, time.monotonic() + segundos)


def _nova_sessao_producao() -> requests.Session:
    s = requests.Session()
    # Não escondemos 429 em retries internos: a produção aplica cooldown GLOBAL.
    retry = Retry(
        total=2, connect=2, read=2, status=0, backoff_factor=0.5,
        allowed_methods=frozenset({"GET"}), raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=8, pool_maxsize=8))
    s.headers.update({
        "Accept": "application/json,*/*",
        "User-Agent": f"Mozilla/5.0 RADAR_PNCP-Itens-Hibrido-Producao/{VERSAO}",
    })
    return s


def _sessao_thread_producao() -> requests.Session:
    s = getattr(_THREAD_LOCAL, "sessao_producao", None)
    if s is None:
        s = _nova_sessao_producao()
        _THREAD_LOCAL.sessao_producao = s
    return s


def _retry_after(resp: requests.Response | None, padrao: float = 62.0) -> float:
    if resp is None:
        return padrao
    raw = (resp.headers.get("Retry-After") or "").strip()
    try:
        return max(1.0, float(raw)) + 2.0
    except Exception:
        return padrao


def _consultar_producao(rec: dict[str, Any], limiter: PacedRateLimiter,
                        deadline: datetime | None = None) -> dict[str, Any]:
    t0 = time.perf_counter()
    out = dict(rec)
    ultimo_erro: str | None = None
    ultimo_http: int | None = None
    for tentativa in range(1, 4):
        try:
            limiter.aguardar(deadline)
        except DeadlineHibrido as exc:
            out.update({"parada_deadline": True, "erro": str(exc), "rows": []})
            out["segundos"] = round(time.perf_counter() - t0, 3)
            return out
        try:
            r = _sessao_thread_producao().get(
                URL_ITENS_ID,
                params={
                    "tipo": "idCompra", "codigo": str(rec["comprasgov_id_compra"]),
                    "pagina": 1, "tamanhoPagina": 50,
                },
                timeout=(10, 75),
            )
            ultimo_http = int(r.status_code)
            out["http"] = ultimo_http
            if ultimo_http == 429:
                espera = _retry_after(r)
                limiter.penalizar(espera)
                ultimo_erro = f"HTTP 429 Too Many Requests; cooldown global {espera:.0f}s"
                if tentativa < 3:
                    continue
                raise requests.HTTPError(ultimo_erro, response=r)
            if ultimo_http in TRANSITORIOS:
                espera = min(30.0, 3.0 * (2 ** (tentativa - 1)))
                limiter.penalizar(espera)
                ultimo_erro = f"HTTP {ultimo_http} transitório"
                if tentativa < 3:
                    continue
                raise requests.HTTPError(ultimo_erro, response=r)
            r.raise_for_status()
            obj = r.json()
            if not isinstance(obj, dict):
                raise RuntimeError("Resposta JSON do Compras.gov não é um objeto.")
            rows = obj.get("resultado") or []
            if not isinstance(rows, list):
                raise RuntimeError("Campo resultado do Compras.gov não é uma lista.")
            out["total_registros"] = obj.get("totalRegistros")
            out["total_paginas"] = obj.get("totalPaginas")
            out["rows"] = rows
            out["tentativas_http"] = tentativa
            out["segundos"] = round(time.perf_counter() - t0, 3)
            return out
        except (requests.Timeout, requests.ConnectionError) as exc:
            ultimo_erro = f"{type(exc).__name__}: {exc}"
            if tentativa < 3:
                limiter.penalizar(min(15.0, 2.0 * tentativa))
                continue
        except requests.HTTPError as exc:
            code = getattr(getattr(exc, "response", None), "status_code", None)
            if code not in TRANSITORIOS:
                out.update({"erro": f"{type(exc).__name__}: {exc}"[:2000], "rows": []})
                out["segundos"] = round(time.perf_counter() - t0, 3)
                return out
            ultimo_erro = f"{type(exc).__name__}: {exc}"
        except Exception as exc:
            # JSON inválido pode ser degradação transitória do serviço. Mantemos
            # a primeira falha fora do fallback PNCP imediato.
            ultimo_erro = f"{type(exc).__name__}: {exc}"
            if tentativa < 2:
                limiter.penalizar(2.0)
                continue
            break
    out.update({
        "erro": (ultimo_erro or "Falha transitória Compras.gov")[:2000],
        "erro_transitorio": True,
        "http": ultimo_http,
        "rows": [],
        "tentativas_http": 3,
        "segundos": round(time.perf_counter() - t0, 3),
    })
    return out


def selecionar_producao(con: sqlite3.Connection, limite: int) -> list[dict[str, Any]]:
    """Seleciona candidatos sem ordenar/varrer todo o backlog.

    Primeiro entram retries vencidos; depois candidatos ainda sem decisão. Os
    índices parciais da V0.7.6 permitem parar assim que o LIMIT é atendido.
    """
    garantir_estrutura(con)
    limite = max(1, int(limite))
    stamp = agora()
    cols = """c.numero_controle_pncp,c.comprasgov_id_compra,c.data_publicacao_pncp,c.itens_status,
              c.itens_validacao_comprasgov_status,COALESCE(c.itens_validacao_comprasgov_tentativas,0) AS tentativas_validacao"""
    out: list[dict[str, Any]] = []

    retries = con.execute(
        f"""SELECT {cols}
              FROM contratacoes c INDEXED BY idx_contratacoes_hibrido_retry_due
             WHERE c.itens_validacao_comprasgov_status=?
               AND c.itens_coletados=0
               AND c.itens_validacao_comprasgov_proxima_tentativa IS NOT NULL
               AND c.itens_validacao_comprasgov_proxima_tentativa<=?
               AND c.comprasgov_id_compra IS NOT NULL AND c.comprasgov_id_compra<>''
               AND (c.itens_status IS NULL OR c.itens_status NOT IN ('INDISPONIVEL_PNCP','REMOVIDA_PNCP'))
               AND EXISTS (SELECT 1 FROM itens i WHERE i.numero_controle_pncp=c.numero_controle_pncp)
             ORDER BY c.itens_validacao_comprasgov_proxima_tentativa,c.numero_controle_pncp
             LIMIT ?""",
        (STATUS_TRANSITORIO, stamp, limite),
    ).fetchall()
    out.extend(dict(r) for r in retries)

    faltam = limite - len(out)
    if faltam > 0:
        novos = con.execute(
            f"""SELECT {cols}
                  FROM contratacoes c INDEXED BY idx_contratacoes_hibrido_fresh
                 WHERE c.itens_coletados=0
                   AND c.comprasgov_id_compra IS NOT NULL AND c.comprasgov_id_compra<>''
                   AND (c.itens_validacao_comprasgov_status IS NULL OR c.itens_validacao_comprasgov_status='')
                   AND (c.itens_status IS NULL OR c.itens_status NOT IN ('INDISPONIVEL_PNCP','REMOVIDA_PNCP'))
                   AND EXISTS (SELECT 1 FROM itens i WHERE i.numero_controle_pncp=c.numero_controle_pncp)
                 ORDER BY c.data_publicacao_pncp DESC,c.numero_controle_pncp
                 LIMIT ?""",
            (faltam,),
        ).fetchall()
        out.extend(dict(r) for r in novos)
    return out


def resumo_status_rapido(con: sqlite3.Connection) -> dict[str, Any]:
    """Contagens indexadas usadas no relatório; evita varredura física de itens/backlog."""
    garantir_estrutura(con)
    def contar(status: str) -> int:
        return int(con.execute(
            "SELECT COUNT(*) FROM contratacoes WHERE itens_validacao_comprasgov_status=? AND itens_coletados=0",
            (status,),
        ).fetchone()[0] or 0)
    return {
        "validados": contar(STATUS_VALIDADO),
        "fallbacks": contar(STATUS_FALLBACK),
        "transitorios": contar(STATUS_TRANSITORIO),
    }


def resumo_controlador(con: sqlite3.Connection) -> dict[str, Any]:
    """Estado mínimo para o controlador persistente, sem varrer o backlog inteiro.

    O controlador só precisa saber se existe ao menos um candidato rápido e se
    há retries aguardando. Contagens globais completas continuam disponíveis em
    ``resumo_operacional`` para relatórios de job.
    """
    garantir_estrutura(con)
    stamp = agora()

    transitorios = int(con.execute(
        """SELECT COUNT(*) FROM contratacoes
             WHERE itens_validacao_comprasgov_status=? AND itens_coletados=0""",
        (STATUS_TRANSITORIO,),
    ).fetchone()[0] or 0)
    rretry = con.execute(
        """SELECT MIN(itens_validacao_comprasgov_proxima_tentativa)
             FROM contratacoes
            WHERE itens_validacao_comprasgov_status=? AND itens_coletados=0""",
        (STATUS_TRANSITORIO,),
    ).fetchone()
    proxima_retry = rretry[0] if rretry else None

    candidato = con.execute(
        """SELECT 1
             FROM contratacoes c
            WHERE c.itens_coletados=0
              AND NULLIF(TRIM(c.comprasgov_id_compra),'') IS NOT NULL
              AND c.itens_status NOT IN ('INDISPONIVEL_PNCP','REMOVIDA_PNCP')
              AND (c.itens_validacao_comprasgov_status IS NULL
                   OR c.itens_validacao_comprasgov_status NOT IN (?,?))
              AND (c.itens_validacao_comprasgov_status IS NULL
                   OR c.itens_validacao_comprasgov_status<>?
                   OR c.itens_validacao_comprasgov_proxima_tentativa IS NULL
                   OR c.itens_validacao_comprasgov_proxima_tentativa<=?)
              AND EXISTS (SELECT 1 FROM itens i
                           WHERE i.numero_controle_pncp=c.numero_controle_pncp)
            LIMIT 1""",
        (STATUS_VALIDADO, STATUS_FALLBACK, STATUS_TRANSITORIO, stamp),
    ).fetchone()
    return {
        "candidatos_rapidos": 1 if candidato else 0,
        "transitorios": transitorios,
        "proxima_retry": proxima_retry,
    }


def resumo_operacional(con: sqlite3.Connection) -> dict[str, Any]:
    """Resumo da camada híbrida sem confundir validação com certificado PNCP."""
    garantir_estrutura(con)
    stamp = agora()
    r = con.execute(
        """
        SELECT
          SUM(CASE WHEN COALESCE(itens_coletados,0)=0 AND itens_validacao_comprasgov_status=? THEN 1 ELSE 0 END) validados,
          SUM(CASE WHEN COALESCE(itens_coletados,0)=0 AND itens_validacao_comprasgov_status=? THEN 1 ELSE 0 END) fallbacks,
          SUM(CASE WHEN COALESCE(itens_coletados,0)=0 AND itens_validacao_comprasgov_status=? THEN 1 ELSE 0 END) transitorios,
          MIN(CASE WHEN COALESCE(itens_coletados,0)=0 AND itens_validacao_comprasgov_status=?
                   THEN itens_validacao_comprasgov_proxima_tentativa END) proxima_retry,
          SUM(CASE WHEN COALESCE(itens_coletados,0)=1 THEN 1 ELSE 0 END) certificados
        FROM contratacoes
        """,
        (STATUS_VALIDADO, STATUS_FALLBACK, STATUS_TRANSITORIO, STATUS_TRANSITORIO),
    ).fetchone()
    candidatos = int(con.execute(
        """
        SELECT COUNT(*) FROM contratacoes c
         WHERE COALESCE(c.itens_coletados,0)=0
           AND NULLIF(TRIM(COALESCE(c.comprasgov_id_compra,'')),'') IS NOT NULL
           AND COALESCE(c.itens_status,'PENDENTE') NOT IN ('INDISPONIVEL_PNCP','REMOVIDA_PNCP')
           AND COALESCE(c.itens_validacao_comprasgov_status,'') NOT IN (?,?)
           AND (COALESCE(c.itens_validacao_comprasgov_status,'')<>?
                OR c.itens_validacao_comprasgov_proxima_tentativa IS NULL
                OR c.itens_validacao_comprasgov_proxima_tentativa<=?)
           AND EXISTS (SELECT 1 FROM itens i WHERE i.numero_controle_pncp=c.numero_controle_pncp)
        """,
        (STATUS_VALIDADO, STATUS_FALLBACK, STATUS_TRANSITORIO, stamp),
    ).fetchone()[0] or 0)
    pncp_direto = int(con.execute(
        """
        SELECT COUNT(*) FROM contratacoes c
         WHERE COALESCE(c.itens_coletados,0)=0
           AND COALESCE(c.itens_status,'PENDENTE') NOT IN ('INDISPONIVEL_PNCP','REMOVIDA_PNCP')
           AND COALESCE(c.itens_validacao_comprasgov_status,'')<>?
           AND COALESCE(c.itens_validacao_comprasgov_status,'')<>?
           AND (
                c.itens_validacao_comprasgov_status=?
                OR NULLIF(TRIM(COALESCE(c.comprasgov_id_compra,'')),'') IS NULL
                OR NOT EXISTS (SELECT 1 FROM itens i WHERE i.numero_controle_pncp=c.numero_controle_pncp)
           )
        """,
        (STATUS_VALIDADO, STATUS_TRANSITORIO, STATUS_FALLBACK),
    ).fetchone()[0] or 0)
    return {
        "validados": int(r[0] or 0),
        "fallbacks": int(r[1] or 0),
        "transitorios": int(r[2] or 0),
        "proxima_retry": r[3],
        "certificados": int(r[4] or 0),
        "candidatos_rapidos": candidatos,
        "pncp_direto": pncp_direto,
    }


def _registrar_transitorio(con: sqlite3.Connection, rec: dict[str, Any], motivo: str,
                           job_id: int | None) -> str:
    pid = str(rec["numero_controle_pncp"])
    row = con.execute(
        "SELECT COALESCE(itens_validacao_comprasgov_tentativas,0) FROM contratacoes WHERE numero_controle_pncp=?",
        (pid,),
    ).fetchone()
    tentativas = int(row[0] or 0) + 1 if row else 1
    if tentativas >= MAX_TENTATIVAS_TRANSITORIAS:
        status = STATUS_FALLBACK
        proxima = None
        detalhe = f"Fallback PNCP após {tentativas} falhas transitórias Compras.gov: {motivo}"
    else:
        status = STATUS_TRANSITORIO
        proxima = (datetime.now() + timedelta(minutes=15)).isoformat(timespec="seconds")
        detalhe = f"Falha transitória Compras.gov {tentativas}/{MAX_TENTATIVAS_TRANSITORIAS}; nova tentativa após {proxima}: {motivo}"
    con.execute(
        """UPDATE contratacoes
              SET itens_validacao_comprasgov_status=?,itens_validacao_comprasgov_em=?,
                  itens_validacao_comprasgov_detalhe=?,itens_validacao_comprasgov_job_id=?,
                  itens_validacao_comprasgov_tentativas=?,itens_validacao_comprasgov_proxima_tentativa=?
            WHERE numero_controle_pncp=? AND COALESCE(itens_coletados,0)=0""",
        (status, agora(), detalhe[:3000], job_id, tentativas, proxima, pid),
    )
    _registrar_log(con, rec, status, motivo[:1000], 0, job_id)
    con.commit()
    return status


def executar_producao(db: Path, *, limite: int = 5000, workers: int = 6, rpm: int = 95,
                      jobs_db: Path | None = None, job_id: int | None = None,
                      deadline: datetime | None = None, max_erros_transitorios: int = 10) -> dict[str, Any]:
    if limite <= 0:
        raise ValueError("limite deve ser > 0")
    if not 1 <= workers <= 12:
        raise ValueError("workers deve ficar entre 1 e 12")
    pausa = PausaAtualizacao(jobs_db)
    limiter = PacedRateLimiter(rpm, parar_quando=pausa)

    con = sqlite3.connect(str(db), timeout=60)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON")
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=60000")
    ii.garantir_estrutura(con)
    garantir_estrutura(con)

    selecionados = selecionar_producao(con, limite)
    total = len(selecionados)
    antes = resumo_status_rapido(con)
    print("="*100, flush=True)
    print(f"Radar PNCP V{VERSAO} — MUTIRÃO HÍBRIDO · ROTA RÁPIDA COMPRAS.GOV", flush=True)
    print(f"Selecionadas: {_fmt(total)} · workers: {workers} · limitador global: {rpm} req/min.", flush=True)
    print("Regra: nunca apagar/sobrescrever item existente; itens_coletados=1 continua exclusivo do PNCP.", flush=True)
    if deadline:
        print(f"Janela de segurança: parar antes de {deadline:%d/%m/%Y %H:%M:%S}.", flush=True)
    print("="*100, flush=True)

    if not selecionados:
        con.close()
        return {"selecionados":0,"exit_code":0,"status":"SEM_ELEGIVEIS"}

    stats = Counter(); fallbacks = Counter(); transitorios = Counter()
    detalhes: list[dict[str, Any]] = []; superconjuntos: list[dict[str, Any]] = []
    tempos: list[float] = []; inicio=time.perf_counter(); processados=0
    motivo_fim="LOTE_CONCLUIDO"; exit_code=0
    chunk_size=max(workers*4, 20)

    _atualizar_job(job_id,jobs_db,0,total,"Mutirão híbrido · Compras.gov")

    for inicio_chunk in range(0,total,chunk_size):
        if pausa():
            motivo_fim="ATUALIZACAO_SISTEMA"; exit_code=EXIT_ATUALIZACAO; break
        if deadline is not None and datetime.now() >= deadline - timedelta(seconds=5):
            motivo_fim="JANELA_SEGURANCA"; exit_code=76; break
        bloco=selecionados[inicio_chunk:inicio_chunk+chunk_size]
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs={pool.submit(_consultar_producao,r,limiter,deadline):r for r in bloco}
            for fut in as_completed(futs):
                base=futs[fut]
                try:
                    bruto=fut.result()
                except Exception as exc:
                    bruto=dict(base); bruto.update({"erro":f"{type(exc).__name__}: {exc}","erro_transitorio":True,"rows":[],"segundos":0.0})
                if bruto.get("parada_deadline"):
                    motivo_fim="JANELA_SEGURANCA"; exit_code=76
                    continue
                pid=str(base["numero_controle_pncp"])
                local=_local_nums(con,pid)
                analisado=_analisar(bruto,local); analisado["numero_controle_pncp"]=pid
                processados += 1
                if isinstance(analisado.get("segundos"),(int,float)):
                    tempos.append(float(analisado["segundos"]))

                row_cert=con.execute("SELECT COALESCE(itens_coletados,0) FROM contratacoes WHERE numero_controle_pncp=?",(pid,)).fetchone()
                if row_cert and int(row_cert[0] or 0)==1:
                    stats["ja_certificado"] += 1
                    _registrar_log(con,analisado,"IGNORADO_CERTIFICADO_PNCP","JA_CERTIFICADO_PNCP",0,job_id); con.commit()
                    continue

                if analisado.get("erro_transitorio"):
                    motivo=str(analisado.get("erro") or "ERRO_TRANSITORIO")
                    status=_registrar_transitorio(con,analisado,motivo,job_id)
                    if status==STATUS_FALLBACK:
                        stats["fallback"] += 1; fallbacks["TRANSITORIO_REPETIDO"] += 1
                    else:
                        stats["transitorios"] += 1; transitorios["HTTP_OU_REDE"] += 1
                    if len(detalhes)<50:
                        detalhes.append({"pid":pid,"idCompra":base.get("comprasgov_id_compra"),"status":status,"motivo":motivo[:500]})
                    continue

                if analisado["aceito"]:
                    novos_previstos=list(analisado["novos_nums"])
                    try:
                        con.execute("SAVEPOINT hibrido_prod")
                        inseridos=_inserir_ausentes(con,pid,analisado["latest"],novos_previstos)
                        detalhe=(f"Conjunto Compras.gov validado em produção: {analisado['itens_api_unicos']} item(ns) único(s); "
                                 f"{analisado['itens_locais']} já existiam; {inseridos} novo(s) inserido(s). Certificação PNCP permanece separada.")
                        con.execute(
                            """UPDATE contratacoes SET itens_validacao_comprasgov_status=?,itens_validacao_comprasgov_em=?,
                                      itens_validacao_comprasgov_qtd=?,itens_validacao_comprasgov_novos=?,
                                      itens_validacao_comprasgov_data_max=?,itens_validacao_comprasgov_detalhe=?,
                                      itens_validacao_comprasgov_job_id=?,itens_validacao_comprasgov_tentativas=0,
                                      itens_validacao_comprasgov_proxima_tentativa=NULL
                                 WHERE numero_controle_pncp=? AND COALESCE(itens_coletados,0)=0""",
                            (STATUS_VALIDADO,agora(),analisado["itens_api_unicos"],inseridos,analisado.get("data_max_api"),detalhe,job_id,pid),
                        )
                        _registrar_log(con,analisado,STATUS_VALIDADO,None,inseridos,job_id)
                        con.execute("RELEASE hibrido_prod"); con.commit()
                        stats["validados"] += 1; stats["novos_itens"] += inseridos
                        stats["duplicidades"] += analisado["grupos_duplicados"]
                        stats["core_versionado"] += analisado["core_mudou_entre_versoes"]
                        if novos_previstos and len(superconjuntos)<30:
                            superconjuntos.append({"pid":pid,"idCompra":base.get("comprasgov_id_compra"),"locais":analisado["itens_locais"],"api":analisado["itens_api_unicos"],"novos":novos_previstos[:30],"inseridos":inseridos})
                    except Exception as exc:
                        try:
                            con.execute("ROLLBACK TO hibrido_prod"); con.execute("RELEASE hibrido_prod")
                        except Exception:
                            pass
                        motivo=f"FALHA_GRAVACAO:{type(exc).__name__}"
                        con.execute("""UPDATE contratacoes SET itens_validacao_comprasgov_status=?,itens_validacao_comprasgov_em=?,
                                          itens_validacao_comprasgov_detalhe=?,itens_validacao_comprasgov_job_id=?
                                      WHERE numero_controle_pncp=? AND COALESCE(itens_coletados,0)=0""",
                                    (STATUS_FALLBACK,agora(),motivo,job_id,pid))
                        _registrar_log(con,analisado,STATUS_FALLBACK,motivo,0,job_id); con.commit()
                        stats["fallback"] += 1; fallbacks[motivo] += 1
                else:
                    motivo=str(analisado.get("motivo") or "RECUSADO")
                    # Respostas estruturalmente recusadas são fallback real, não erro de rede.
                    con.execute("""UPDATE contratacoes SET itens_validacao_comprasgov_status=?,itens_validacao_comprasgov_em=?,
                                      itens_validacao_comprasgov_qtd=?,itens_validacao_comprasgov_novos=0,
                                      itens_validacao_comprasgov_data_max=?,itens_validacao_comprasgov_detalhe=?,
                                      itens_validacao_comprasgov_job_id=?,itens_validacao_comprasgov_proxima_tentativa=NULL
                                  WHERE numero_controle_pncp=? AND COALESCE(itens_coletados,0)=0""",
                                (STATUS_FALLBACK,agora(),analisado["itens_api_unicos"],analisado.get("data_max_api"),
                                 f"Fallback PNCP: {motivo}",job_id,pid))
                    _registrar_log(con,analisado,STATUS_FALLBACK,motivo,0,job_id); con.commit()
                    stats["fallback"] += 1; fallbacks[motivo] += 1

                if processados % 25 == 0:
                    elapsed=max(.001,time.perf_counter()-inicio); vel=processados/(elapsed/60.0)
                    msg=(f"Híbrido {processados}/{total} · validados {stats['validados']} · fallback {stats['fallback']} · "
                         f"transitórios {stats['transitorios']} · novos {stats['novos_itens']} · {vel:.1f} contr./min")
                    print(msg,flush=True); _atualizar_job(job_id,jobs_db,processados,total,f"Mutirão híbrido · {processados}/{total}",msg)

        if pausa():
            motivo_fim="ATUALIZACAO_SISTEMA"; exit_code=EXIT_ATUALIZACAO
            print("[Híbrido] Pausado para atualização; respostas recebidas foram gravadas.", flush=True)
            break
        if exit_code==76:
            break
        if stats["transitorios"] >= max(1,int(max_erros_transitorios)):
            motivo_fim="INSTABILIDADE_COMPRASGOV"; exit_code=75; break

    depois=resumo_status_rapido(con)
    con.close()
    dur=time.perf_counter()-inicio
    tempos_ord=sorted(tempos)
    med=statistics.median(tempos_ord) if tempos_ord else None
    p95=tempos_ord[min(len(tempos_ord)-1,math.ceil(.95*len(tempos_ord))-1)] if tempos_ord else None
    tmax=max(tempos_ord) if tempos_ord else None

    outdir=Path(db).parent/"diagnosticos"; outdir.mkdir(parents=True,exist_ok=True)
    path=outdir/f"mutirao_hibrido_v{VERSAO.replace(chr(46),chr(95))}_{datetime.now():%Y%m%d_%H%M%S}.txt"
    linhas=[
        f"Radar PNCP V{VERSAO} — MUTIRÃO HÍBRIDO DE PRODUÇÃO", f"Gerado em: {datetime.now():%d/%m/%Y %H:%M:%S}",
        f"Banco: {Path(db).resolve()}","", "=== RESULTADO ===", f"Selecionados: {total}", f"Processados: {processados}",
        f"Validados Compras.gov: {stats['validados']}", f"Fallback PNCP: {stats['fallback']}",
        f"Erros transitórios mantidos na rota rápida: {stats['transitorios']}", f"Já certificados: {stats['ja_certificado']}",
        f"Itens novos inseridos: {stats['novos_itens']}", f"Motivo do término: {motivo_fim}", f"Exit code: {exit_code}", "",
        "=== INTEGRIDADE ===", f"Itens novos inseridos neste lote: {stats['novos_itens']}",
        "Contagem global de itens não é varrida no início/fim do lote; a integridade é garantida pelas transações por contratação.",
        f"Validados pendentes PNCP: {antes['validados']} -> {depois['validados']}",
        f"Fallbacks PNCP: {antes['fallbacks']} -> {depois['fallbacks']}",
        f"Transitórios Compras.gov: {antes['transitorios']} -> {depois['transitorios']}",
        "itens_coletados=1 permanece reservado exclusivamente ao PNCP.","",
        "=== DESEMPENHO ===", f"Duração: {dur:.1f}s", f"RPM configurado: {rpm}",
        f"Tempo mediano por consulta (inclui espera do limitador): {med:.3f}s" if med is not None else "Mediana: n/d",
        f"P95: {p95:.3f}s" if p95 is not None else "P95: n/d", f"Máximo: {tmax:.3f}s" if tmax is not None else "Máximo: n/d",
        f"Velocidade efetiva: {(processados/(dur/60.0) if dur else 0):.2f} contratações/min", "", "=== FALLBACKS ==="
    ]
    linhas += [f"{k}: {v}" for k,v in fallbacks.most_common()] or ["Nenhum."]
    linhas += ["", "=== TRANSITÓRIOS ==="] + ([f"{k}: {v}" for k,v in transitorios.most_common()] or ["Nenhum."])
    linhas += ["", "=== SUPERCONJUNTOS (ATÉ 30) ==="] + ([json.dumps(x,ensure_ascii=False) for x in superconjuntos] or ["Nenhum."])
    linhas += ["", "=== DETALHES DE EXCEÇÕES (ATÉ 50) ==="] + ([json.dumps(x,ensure_ascii=False) for x in detalhes] or ["Nenhum."])
    path.write_text("\n".join(linhas)+"\n",encoding="utf-8")

    print("",flush=True); print("="*100,flush=True)
    print(f"[Híbrido] Validados {_fmt(stats['validados'])} · fallback {_fmt(stats['fallback'])} · transitórios {_fmt(stats['transitorios'])} · novos {_fmt(stats['novos_itens'])}.",flush=True)
    print(f"[Híbrido] Término: {motivo_fim} · relatório: {path}",flush=True); print("="*100,flush=True)
    return {"selecionados":total,"processados":processados,"validados":int(stats['validados']),"fallback":int(stats['fallback']),
            "transitorios":int(stats['transitorios']),"novos_itens":int(stats['novos_itens']),"exit_code":exit_code,
            "motivo_fim":motivo_fim,"relatorio":str(path)}

def main() -> int:
    ap = argparse.ArgumentParser(description=f"Radar PNCP V{VERSAO} — itens híbridos")
    ap.add_argument("--db",type=Path,default=DB_COMPRAS)
    ap.add_argument("--jobs-db",type=Path,default=DB_JOBS)
    ap.add_argument("--job-id",type=int)
    ap.add_argument("--modo",choices=("piloto","producao"),default="piloto")
    ap.add_argument("--limite",type=int,default=5000)
    ap.add_argument("--workers",type=int,default=6)
    ap.add_argument("--rpm",type=int,default=95)
    ap.add_argument("--deadline-iso")
    ap.add_argument("--max-erros-transitorios",type=int,default=10)
    args=ap.parse_args()
    jobs_db = args.jobs_db if args.job_id is not None else None
    try:
        if args.modo == "producao":
            deadline = _parse_dt(args.deadline_iso) if args.deadline_iso else None
            out = executar_producao(
                args.db, limite=args.limite, workers=args.workers, rpm=args.rpm,
                jobs_db=jobs_db, job_id=args.job_id, deadline=deadline,
                max_erros_transitorios=max(1,args.max_erros_transitorios),
            )
            return int(out.get("exit_code",0))
        executar_piloto(args.db,limite=args.limite,workers=args.workers,jobs_db=jobs_db,job_id=args.job_id)
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(f"ERRO FATAL: {type(exc).__name__}: {exc}",flush=True)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
