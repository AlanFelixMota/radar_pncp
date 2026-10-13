#!/usr/bin/env python3
"""Snapshot leve de estatísticas para a interface — Radar PNCP V0.7.6.

A interface não deve executar GROUP BY em milhões de linhas a cada rerun do
Streamlit. Este módulo materializa um pequeno radar_stats.db, atualizado ao fim
dos jobs que alteram compras.db.
"""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from radar_paths import DB_COMPRAS, DB_STATS

UFS = ("AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG",
       "PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO")
SQL_DATA_VALIDA = (
    "length(data_publicacao_pncp) >= 10 "
    "AND substr(data_publicacao_pncp,1,10) GLOB "
    "'[0-9][0-9][0-9][0-9]-[0-1][0-9]-[0-3][0-9]' "
    "AND substr(data_publicacao_pncp,1,10) BETWEEN '2021-01-01' AND '2099-12-31'"
)
SCHEMA_VERSION = 5


def _connect_read(path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(path), timeout=60)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA query_only=ON")
    con.execute("PRAGMA busy_timeout=60000")
    return con


def _table_exists(con: sqlite3.Connection, name: str) -> bool:
    return con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone() is not None


def _columns(con: sqlite3.Connection, name: str) -> set[str]:
    if not _table_exists(con, name):
        return set()
    return {str(r[1]) for r in con.execute(f"PRAGMA table_info({name})")}


def _status_expr(cols: set[str]) -> str:
    if "itens_status" in cols:
        return "COALESCE(itens_status, CASE WHEN itens_coletados=1 THEN 'OK' ELSE 'PENDENTE' END)"
    return "CASE WHEN itens_coletados=1 THEN 'OK' ELSE 'PENDENTE' END"


class LimiteEstatisticas(TimeoutError):
    pass


def construir_payload(compras_db: Path, *, max_segundos: float | None = None) -> dict[str, Any]:
    inicio = time.monotonic()
    con = _connect_read(compras_db)
    etapa = ['abertura', inicio]
    def marcar(nome):
        agora = time.monotonic()
        print(f"[STATS] {etapa[0]}: {agora-etapa[1]:.2f}s", flush=True)
        etapa[:] = [nome, agora]
        print(f"[STATS] Iniciando {nome}...", flush=True)
        if max_segundos is not None and agora-inicio >= max_segundos:
            raise LimiteEstatisticas(f'Limite de {max_segundos:g}s atingido antes de {nome}.')
    def esgotado():
        return max_segundos is not None and time.monotonic()-inicio >= max_segundos
    try:
        con.execute('PRAGMA busy_timeout=1000')
        con.set_progress_handler(lambda: int(esgotado()), 10000)
        con.execute('BEGIN')  # todas as contagens pertencem à mesma fotografia
        return _construir_payload(con, marcar)
    except sqlite3.OperationalError as exc:
        if esgotado():
            raise LimiteEstatisticas(f'Limite de {max_segundos:g}s atingido em {etapa[0]}.') from exc
        raise
    finally:
        con.set_progress_handler(None, 0)
        con.close()
        print(f"[STATS] {etapa[0]}: {time.monotonic()-etapa[1]:.2f}s; total {time.monotonic()-inicio:.2f}s", flush=True)


def _construir_payload(con, marcar) -> dict[str, Any]:
    cols = _columns(con, "contratacoes")
    if not cols:
        raise RuntimeError("Tabela contratacoes não encontrada.")
    status = _status_expr(cols)

    marcar("resumo de contratações")
    resumo = con.execute(f"""
        SELECT COUNT(*) total_contratacoes,
               MIN(CASE WHEN {SQL_DATA_VALIDA} THEN substr(data_publicacao_pncp,1,10) END) data_min,
               MAX(CASE WHEN {SQL_DATA_VALIDA} THEN substr(data_publicacao_pncp,1,10) END) data_max,
               SUM(CASE WHEN itens_coletados=1 THEN 1 ELSE 0 END) conjuntos_confirmados,
               COALESCE(SUM(CASE WHEN itens_coletados=1 THEN qtd_itens ELSE 0 END),0) itens_em_conjuntos_confirmados,
               SUM(CASE WHEN {status}='INDISPONIVEL_PNCP' THEN 1 ELSE 0 END) indisponiveis,
               SUM(CASE WHEN itens_coletados=0 AND {status}<>'INDISPONIVEL_PNCP' THEN 1 ELSE 0 END) pendentes,
               COALESCE(SUM(qtd_itens),0) qtd_itens_agregado
          FROM contratacoes
    """).fetchone()

    # V0.7.6: distingue as linhas físicas realmente armazenadas da soma do
    # contador qtd_itens dos pais. CSV pode fornecer itens úteis sem afirmar
    # completude; portanto essas duas métricas têm semânticas diferentes.
    marcar("itens por fonte")
    itens_fisicos = 0
    pais_com_itens_fisicos = 0
    itens_por_fonte: list[dict[str, Any]] = []
    if _table_exists(con, "itens"):
        icols = _columns(con, "itens")
        if "fonte_ultima" in icols:
            itens_por_fonte = [dict(r) for r in con.execute("""
                SELECT COALESCE(NULLIF(trim(fonte_ultima),''),'(não informada)') fonte,
                       COUNT(*) quantidade
                  FROM itens
                 GROUP BY COALESCE(NULLIF(trim(fonte_ultima),''),'(não informada)')
                 ORDER BY quantidade DESC
            """).fetchall()]
            itens_fisicos = sum(int(r.get("quantidade") or 0) for r in itens_por_fonte)
        else:
            itens_fisicos = int(con.execute("SELECT COUNT(*) FROM itens").fetchone()[0] or 0)
        marcar("contratações com itens físicos")
        pais_com_itens_fisicos = int(con.execute(
            "SELECT COUNT(*) FROM (SELECT DISTINCT numero_controle_pncp FROM itens WHERE numero_controle_pncp IS NOT NULL)"
        ).fetchone()[0] or 0)

    marcar("modalidades")
    modalidades = [dict(r) for r in con.execute("""
        SELECT CASE WHEN modalidade_id BETWEEN 1 AND 30
                          AND modalidade_nome IS NOT NULL
                          AND length(trim(modalidade_nome)) BETWEEN 3 AND 100
                          AND lower(trim(modalidade_nome)) NOT LIKE 'http%'
                    THEN trim(modalidade_nome) ELSE '(não informada)' END modalidade,
               COUNT(*) quantidade
          FROM contratacoes
         GROUP BY modalidade
         ORDER BY quantidade DESC
    """).fetchall()]

    marcar("unidades federativas")
    ufs_placeholders = ",".join("?" for _ in UFS)
    por_uf = [dict(r) for r in con.execute(f"""
        SELECT CASE WHEN upper(trim(uf_sigla)) IN ({ufs_placeholders})
                    THEN upper(trim(uf_sigla)) ELSE 'Sem UF' END uf,
               COUNT(*) quantidade
          FROM contratacoes
         GROUP BY uf
         ORDER BY quantidade DESC
    """, UFS).fetchall()]

    marcar("publicações por dia")
    por_dia = [dict(r) for r in con.execute(f"""
        SELECT substr(data_publicacao_pncp,1,10) data,
               COUNT(*) contratacoes,
               COALESCE(SUM(qtd_itens),0) itens
          FROM contratacoes
         WHERE data_publicacao_pncp IS NOT NULL AND {SQL_DATA_VALIDA}
         GROUP BY substr(data_publicacao_pncp,1,10)
         ORDER BY data DESC
         LIMIT 30
    """).fetchall()]

    marcar("estados de itens")
    estados = [dict(r) for r in con.execute(f"""
        SELECT {status} status, COUNT(*) quantidade
          FROM contratacoes
         GROUP BY {status}
         ORDER BY quantidade DESC
    """).fetchall()]

    marcar("execuções")
    execucoes: list[dict[str, Any]] = []
    if _table_exists(con, "execucoes_diarias"):
        ecols = _columns(con, "execucoes_diarias")
        indis = "contratacoes_itens_indisponiveis" if "contratacoes_itens_indisponiveis" in ecols else "0"
        execucoes = [dict(r) for r in con.execute(f"""
            SELECT data_referencia,inicio_em,fim_em,status,total_contratacoes,
                   contratacoes_com_itens,contratacoes_sem_itens,
                   {indis} contratacoes_itens_indisponiveis,total_itens,modalidades_concluidas
              FROM execucoes_diarias ORDER BY id DESC LIMIT 10
        """).fetchall()]

    modalidades_filtro = sorted({r["modalidade"] for r in modalidades if r["modalidade"] != "(não informada)"}, key=str.casefold)
    ufs_filtro = sorted({r["uf"] for r in por_uf if r["uf"] in UFS})

    # Métricas operacionais usadas apenas pelo Gerenciamento. São calculadas
    # quando o snapshot é reconstruído pelos jobs, nunca a cada rerun da UI.
    marcar("métricas operacionais")
    operacional = {
        "staging": 0,
        "pais_staging": 0,
        "quarentena_itens": 0,
        "itens_removidos_pncp": 0,
        "quarentena_contratacoes": 0,
        "pendentes_completude": 0,
        "fila_pendentes_api": 0,
        "fila_404": 0,
        "fila_410": 0,
        "fila_erros": 0,
        "fila_resolvidas": 0,
    }
    if _table_exists(con, "comprasgov_itens_orfaos"):
        rr = con.execute(
            "SELECT COUNT(*) qtd, COUNT(DISTINCT numero_controle_pncp) pais FROM comprasgov_itens_orfaos"
        ).fetchone()
        operacional["staging"] = int(rr["qtd"] or 0)
        operacional["pais_staging"] = int(rr["pais"] or 0)
    if _table_exists(con, "comprasgov_itens_quarentena"):
        operacional["quarentena_itens"] = int(con.execute(
            "SELECT COUNT(*) FROM comprasgov_itens_quarentena "
            "WHERE status NOT IN ('RECUPERADO','SUPERADO_POR_REVARREDURA')"
        ).fetchone()[0] or 0)
        operacional["itens_removidos_pncp"] = int(con.execute(
            "SELECT COUNT(*) FROM comprasgov_itens_quarentena WHERE status='REMOVIDA_PNCP'"
        ).fetchone()[0] or 0)
    if _table_exists(con, "comprasgov_contratacoes_quarentena"):
        operacional["quarentena_contratacoes"] = int(con.execute(
            "SELECT COUNT(*) FROM comprasgov_contratacoes_quarentena"
        ).fetchone()[0] or 0)
    if _table_exists(con, "pncp_reconciliacao_staging"):
        filas = {str(r[0]): int(r[1]) for r in con.execute(
            "SELECT status,COUNT(*) FROM pncp_reconciliacao_staging GROUP BY status"
        ).fetchall()}
        operacional["fila_pendentes_api"] = sum(filas.get(k, 0) for k in (
            "PENDENTE", "ERRO_TRANSITORIO", "ERRO", "AGUARDANDO_RATE_LIMIT"
        ))
        operacional["fila_404"] = filas.get("NAO_ENCONTRADA", 0)
        operacional["fila_410"] = filas.get("REMOVIDA_PNCP", 0)
        operacional["fila_erros"] = filas.get("ERRO", 0) + filas.get("ERRO_TRANSITORIO", 0)
        operacional["fila_resolvidas"] = sum(filas.get(k, 0) for k in (
            "RESOLVIDA", "JA_EXISTIA", "PAI_IMPORTADO"
        ))
    if "itens_coletados" in cols:
        operacional["pendentes_completude"] = int(con.execute(
            f"""SELECT COUNT(*) FROM contratacoes
                 WHERE COALESCE(itens_coletados,0)=0
                   AND {status} NOT IN ('INDISPONIVEL_PNCP','REMOVIDA_PNCP')"""
        ).fetchone()[0] or 0)
    # Camada de resultados homologados do Compras.gov (V0.7.6).
    # É a matéria-prima para inteligência de preços/fornecedores.
    marcar("resultados e fornecedores")
    mercado = {
        "resultados_itens": 0,
        "fornecedores_identificados": 0,
        "resultados_com_valor": 0,
        "resultado_data_max": None,
        "resultado_defasagem_dias": None,
        "resultado_frescor": "SEM_DADOS",
        "resultado_sync_ultima_execucao": None,
        "resultado_csv_status": None,
        "resultado_csv_last_modified": None,
        "resultado_csv_verificado_em": None,
    }
    if _table_exists(con, "resultados_itens"):
        mercado["resultados_itens"] = int(con.execute(
            "SELECT COUNT(*) FROM resultados_itens"
        ).fetchone()[0] or 0)
        mercado["fornecedores_identificados"] = int(con.execute(
            "SELECT COUNT(*) FROM (SELECT DISTINCT ni_fornecedor FROM resultados_itens "
            "WHERE ni_fornecedor IS NOT NULL AND trim(ni_fornecedor)<>'')"
        ).fetchone()[0] or 0)
        mercado["resultados_com_valor"] = int(con.execute(
            "SELECT COUNT(*) FROM resultados_itens "
            "WHERE valor_unitario_homologado IS NOT NULL OR valor_total_homologado IS NOT NULL"
        ).fetchone()[0] or 0)
        rmax = con.execute(
            """SELECT MAX(substr(data_resultado_pncp,1,10)) FROM resultados_itens
                 WHERE data_resultado_pncp IS NOT NULL
                   AND length(data_resultado_pncp)>=10
                   AND substr(data_resultado_pncp,1,10) BETWEEN '2021-01-01' AND ?""",
            (datetime.now().date().isoformat(),),
        ).fetchone()[0]
        if rmax:
            mercado["resultado_data_max"] = str(rmax)
            try:
                atraso = max(0, (datetime.now().date() - datetime.fromisoformat(str(rmax)).date()).days)
            except Exception:
                atraso = None
            mercado["resultado_defasagem_dias"] = atraso
            if atraso is None:
                mercado["resultado_frescor"] = "DESCONHECIDO"
            elif atraso <= 7:
                mercado["resultado_frescor"] = "ATUAL"
            elif atraso <= 30:
                mercado["resultado_frescor"] = "ATENCAO"
            elif atraso <= 45:
                mercado["resultado_frescor"] = "ATRASADO"
            else:
                mercado["resultado_frescor"] = "CRITICO"
        if _table_exists(con, "comprasgov_resultados_sync"):
            sr = con.execute(
                "SELECT ultima_execucao_em,csv_status,csv_last_modified,csv_verificado_em FROM comprasgov_resultados_sync WHERE id=1"
            ).fetchone()
            if sr:
                mercado["resultado_sync_ultima_execucao"] = sr[0]
                mercado["resultado_csv_status"] = sr[1]
                mercado["resultado_csv_last_modified"] = sr[2]
                mercado["resultado_csv_verificado_em"] = sr[3]

    marcar("finalização")

    return {
        "schema_version": SCHEMA_VERSION,
        "atualizado_em": datetime.now().isoformat(timespec="seconds"),
        "meta": {
            "total_contratacoes": int(resumo["total_contratacoes"] or 0),
            # total_itens passa a significar linhas físicas pesquisáveis da tabela itens.
            "total_itens": int(itens_fisicos),
            "itens_fisicos": int(itens_fisicos),
            "qtd_itens_agregado": int(resumo["qtd_itens_agregado"] or 0),
            "pais_com_itens_fisicos": int(pais_com_itens_fisicos),
            "conjuntos_confirmados": int(resumo["conjuntos_confirmados"] or 0),
            "itens_em_conjuntos_confirmados": int(resumo["itens_em_conjuntos_confirmados"] or 0),
            # compatibilidade: com_itens historicamente significava itens_coletados=1.
            "com_itens": int(resumo["conjuntos_confirmados"] or 0),
            "data_min": resumo["data_min"], "data_max": resumo["data_max"],
            "indisponiveis": int(resumo["indisponiveis"] or 0),
            "pendentes": int(resumo["pendentes"] or 0),
            "pendentes_completude": int(operacional.get("pendentes_completude") or 0),
            "modalidades": modalidades_filtro,
            "ufs": ufs_filtro,
            "tem_itens_status": "itens_status" in cols,
        },
        "painel": {"por_modalidade": modalidades, "por_uf": por_uf, "por_dia": por_dia,
                   "estados_itens": estados, "execucoes": execucoes,
                   "itens_por_fonte": itens_por_fonte},
        "mercado": mercado,
        "operacional": operacional,
    }


def salvar_payload(payload: dict[str, Any], stats_db: Path) -> None:
    stats_db = Path(stats_db)
    stats_db.parent.mkdir(parents=True, exist_ok=True)
    # Grava em arquivo temporário e troca atomicamente: a UI nunca vê snapshot parcial.
    fd, tmpname = tempfile.mkstemp(prefix=stats_db.stem + "_", suffix=".tmp", dir=str(stats_db.parent))
    os.close(fd)
    tmp = Path(tmpname)
    try:
        con = sqlite3.connect(str(tmp))
        con.execute("PRAGMA journal_mode=DELETE")
        con.execute("CREATE TABLE snapshot (id INTEGER PRIMARY KEY CHECK(id=1), payload_json TEXT NOT NULL, atualizado_em TEXT NOT NULL, schema_version INTEGER NOT NULL)")
        raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        con.execute("INSERT INTO snapshot(id,payload_json,atualizado_em,schema_version) VALUES(1,?,?,?)",
                    (raw, payload["atualizado_em"], int(payload["schema_version"])))
        con.commit(); con.close()
        os.replace(tmp, stats_db)
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass


def reconstruir_estatisticas(compras_db: Path = DB_COMPRAS, stats_db: Path = DB_STATS, *, max_segundos: float | None = None) -> dict[str, Any]:
    payload = construir_payload(Path(compras_db), max_segundos=max_segundos)
    salvar_payload(payload, Path(stats_db))
    return payload


def atualizar_automaticamente(compras_db: Path = DB_COMPRAS, stats_db: Path = DB_STATS,
                             *, max_segundos: float = 120) -> dict[str, Any] | None:
    """Resumo auxiliar não deve prender a coleta nem substituir dados por parciais."""
    try:
        return reconstruir_estatisticas(compras_db, stats_db, max_segundos=max_segundos)
    except (LimiteEstatisticas, sqlite3.Error, OSError, RuntimeError) as exc:
        print(f'[STATS] Resumo não atualizado: {exc} Último snapshot preservado com sua data original; dados coletados permanecem gravados.', flush=True)
        return None


def carregar_estatisticas(stats_db: Path = DB_STATS) -> dict[str, Any] | None:
    stats_db = Path(stats_db)
    if not stats_db.exists():
        return None
    try:
        con = sqlite3.connect(str(stats_db), timeout=5)
        row = con.execute("SELECT payload_json,schema_version FROM snapshot WHERE id=1").fetchone()
        con.close()
        if not row or int(row[1]) != SCHEMA_VERSION:
            return None
        payload = json.loads(row[0])
        return payload if isinstance(payload, dict) else None
    except (sqlite3.Error, OSError, ValueError, json.JSONDecodeError):
        return None


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", type=Path, default=DB_COMPRAS)
    ap.add_argument("--stats-db", type=Path, default=DB_STATS)
    args = ap.parse_args()
    p = reconstruir_estatisticas(args.db, args.stats_db)
    print(f"Snapshot atualizado: {p['atualizado_em']} · {p['meta']['total_contratacoes']:,} contratações")
