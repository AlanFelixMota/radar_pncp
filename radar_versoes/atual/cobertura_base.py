#!/usr/bin/env python3
"""Mapa local de cobertura da base — Radar PNCP V0.7.6.

A análise é deliberadamente conservadora e separa conceitos que antes apareciam
misturados na interface:

* cobertura operacional de CONTRATAÇÕES: há evidência de que o dia foi coberto por
  uma fonte/rotina conhecida (Compras.gov importado, varredura PNCP por publicação
  ou COLETA concluída por atualização);
* certificação PNCP por PUBLICAÇÃO: o dia foi integralmente varrido pelas 14
  modalidades no endpoint de publicação (coleta histórica/legacy catch-up);
* revisão COLETA por ATUALIZAÇÃO: o dia pertence a uma janela concluída da COLETA
  moderna. Isso protege a operação corrente, mas não é tratado como certificação
  histórica por data de publicação;
* completude de ITENS: todas as contratações armazenadas daquele dia estão com o
  conjunto integral de itens certificado pelo PNCP. Um dia operacionalmente coberto
  e sem contratações é considerado completo para itens por vacuidade;
* RESULTADOS Compras.gov: a fronteira é a maior data de resultado efetivamente
  disponível na base, e pode estar atrasada por causa da fonte externa.

Nenhuma consulta externa é feita aqui. O módulo somente lê compras.db/radar_jobs.db.
"""

from __future__ import annotations

import calendar
import sqlite3
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable, Any


STATUS_JOB_OK = {"CONCLUIDA", "CONCLUIDA_COM_PENDENCIAS_EXTERNAS"}
TIPO_ATUALIZACAO_DIARIA = "ATUALIZACAO_DIARIA"
TIPO_COLETA_LEGACY = "COLETA_MULTIFONTE_INCREMENTAL"


def _datas(inicio: str, fim: str) -> list[str]:
    a = date.fromisoformat(inicio)
    b = date.fromisoformat(fim)
    if b < a:
        raise ValueError("Data final anterior à data inicial.")
    out: list[str] = []
    while a <= b:
        out.append(a.isoformat())
        a += timedelta(days=1)
    return out


def _clip(a: str, b: str, inicio: str, fim: str) -> tuple[str, str] | None:
    x = max(date.fromisoformat(a), date.fromisoformat(inicio))
    y = min(date.fromisoformat(b), date.fromisoformat(fim))
    if x > y:
        return None
    return x.isoformat(), y.isoformat()


def _add_intervalo(destino: set[str], a: str, b: str, inicio: str, fim: str) -> None:
    c = _clip(a, b, inicio, fim)
    if not c:
        return
    x, y = map(date.fromisoformat, c)
    while x <= y:
        destino.add(x.isoformat())
        x += timedelta(days=1)


def comprimir_dias(dias: Iterable[str]) -> list[dict[str, Any]]:
    """Converte datas ISO em intervalos consecutivos com quantidade de dias."""
    ordenadas = sorted({date.fromisoformat(str(d)[:10]) for d in dias if d})
    if not ordenadas:
        return []
    saida: list[dict[str, Any]] = []
    ini = ant = ordenadas[0]
    for d in ordenadas[1:]:
        if d == ant + timedelta(days=1):
            ant = d
            continue
        saida.append({"inicio": ini.isoformat(), "fim": ant.isoformat(), "dias": (ant - ini).days + 1})
        ini = ant = d
    saida.append({"inicio": ini.isoformat(), "fim": ant.isoformat(), "dias": (ant - ini).days + 1})
    return saida


def _tabelas(con: sqlite3.Connection) -> set[str]:
    return {str(r[0]) for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _colunas(con: sqlite3.Connection, tabela: str) -> set[str]:
    return {str(r[1]) for r in con.execute(f"PRAGMA table_info({tabela})")}


def _normalizar_data_ref(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        try:
            return date.fromisoformat(s[:10]).isoformat()
        except Exception:
            return None
    if len(s) == 8 and s.isdigit():
        try:
            return date(int(s[:4]), int(s[4:6]), int(s[6:8])).isoformat()
        except Exception:
            return None
    return None


def _cobertura_csv(con: sqlite3.Connection, inicio: str, fim: str) -> tuple[set[str], list[dict[str, Any]], dict[str, str]]:
    """Reconstrói cobertura conhecida de contratações via arquivos Compras.gov.

    Preferimos marcadores de cobertura persistidos pela própria carga. Há fallback
    para a tabela de importações quando a base for anterior a esses marcadores.
    """
    dias: set[str] = set()
    intervalos: list[dict[str, Any]] = []
    marcadores: dict[str, str] = {}
    tabs = _tabelas(con)

    if "comprasgov_cobertura" in tabs:
        for k, v in con.execute("SELECT chave,valor FROM comprasgov_cobertura"):
            if v is not None:
                marcadores[str(k)] = str(v)

    if marcadores.get("deltas_comprasgov_status") == "PROCESSADOS":
        mi = marcadores.get("deltas_comprasgov_inicio")
        mf = marcadores.get("deltas_comprasgov_fim")
        if mi and mf and len(mi) >= 7 and len(mf) >= 7:
            try:
                ai, mm_i = map(int, mi[:7].split("-"))
                af, mm_f = map(int, mf[:7].split("-"))
                a = date(ai, mm_i, 1).isoformat()
                b = date(af, mm_f, calendar.monthrange(af, mm_f)[1]).isoformat()
                c = _clip(a, b, inicio, fim)
                if c:
                    _add_intervalo(dias, *c, inicio, fim)
                    intervalos.append({"inicio": c[0], "fim": c[1], "fonte": "Compras.gov · base/deltas"})
            except Exception:
                pass

    # Snapshot anual de 2026 importado pelo bootstrap.
    data_max_2026 = marcadores.get("comprasgov_2026_anual_data_max")
    if data_max_2026:
        try:
            date.fromisoformat(data_max_2026[:10])
            c = _clip("2026-01-01", data_max_2026[:10], inicio, fim)
            if c:
                _add_intervalo(dias, *c, inicio, fim)
                intervalos.append({"inicio": c[0], "fim": c[1], "fonte": "Compras.gov · snapshot 2026"})
        except Exception:
            pass

    # Fallback para bases onde a cadeia de importações está registrada mas os
    # marcadores de cobertura ainda não existiam.
    if not dias and "comprasgov_importacoes" in tabs:
        rows = con.execute(
            "SELECT ano_mes,recurso,status FROM comprasgov_importacoes WHERE status='CONCLUIDA'"
        ).fetchall()
        ok: dict[str, set[str]] = {}
        for ano_mes, recurso, _ in rows:
            ok.setdefault(str(ano_mes), set()).add(str(recurso))
        periodos = sorted(p for p, recs in ok.items() if {"COMPRA", "COMPRA_ITEM"}.issubset(recs))
        for p in periodos:
            try:
                if p.endswith("-ANUAL"):
                    ano = int(p[:4]); a = date(ano,1,1); b = date(ano,12,31)
                elif len(p) == 7 and p[4] == "-":
                    ano, mes = map(int, p.split("-")); a = date(ano,mes,1); b = date(ano,mes,calendar.monthrange(ano,mes)[1])
                else:
                    continue
                c = _clip(a.isoformat(), b.isoformat(), inicio, fim)
                if c:
                    _add_intervalo(dias, *c, inicio, fim)
                    intervalos.append({"inicio":c[0],"fim":c[1],"fonte":f"Compras.gov · {p}"})
            except Exception:
                continue

    return dias, intervalos, marcadores


def _cobertura_pncp_publicacao(con: sqlite3.Connection, inicio: str, fim: str) -> set[str]:
    dias: set[str] = set()
    if "coletas_modalidades" not in _tabelas(con):
        return dias
    rows = con.execute(
        """SELECT data_referencia
             FROM coletas_modalidades
            WHERE status='CONCLUIDA'
            GROUP BY data_referencia
           HAVING COUNT(DISTINCT modalidade_id) >= 14"""
    ).fetchall()
    for (raw,) in rows:
        d = _normalizar_data_ref(raw)
        if d and inicio <= d <= fim:
            dias.add(d)
    return dias


def _cobertura_jobs(db_jobs: Path, inicio: str, fim: str) -> tuple[set[str], set[str], list[dict[str, Any]]]:
    """Retorna (COLETA atualização, legacy por publicação, jobs usados)."""
    atualizacao: set[str] = set()
    publicacao_legacy: set[str] = set()
    usados: list[dict[str, Any]] = []
    if not Path(db_jobs).exists():
        return atualizacao, publicacao_legacy, usados
    con = sqlite3.connect(str(db_jobs), timeout=15)
    con.row_factory = sqlite3.Row
    try:
        tabs = _tabelas(con)
        if "jobs" not in tabs:
            return atualizacao, publicacao_legacy, usados
        cols = _colunas(con, "jobs")
        if not {"tipo","status","data_inicio","data_fim"}.issubset(cols):
            return atualizacao, publicacao_legacy, usados
        qs = ",".join("?" for _ in STATUS_JOB_OK)
        rows = con.execute(
            f"""SELECT id,tipo,status,data_inicio,data_fim,operacao
                  FROM jobs
                 WHERE status IN ({qs})
                   AND tipo IN (?,?)
                   AND data_inicio IS NOT NULL AND data_fim IS NOT NULL
                   AND data_fim>=? AND data_inicio<=?
                 ORDER BY id""",
            (*sorted(STATUS_JOB_OK), TIPO_ATUALIZACAO_DIARIA, TIPO_COLETA_LEGACY, inicio, fim),
        ).fetchall()
        for r in rows:
            try:
                a = date.fromisoformat(str(r["data_inicio"])[:10]).isoformat()
                b = date.fromisoformat(str(r["data_fim"])[:10]).isoformat()
            except Exception:
                continue
            c = _clip(a,b,inicio,fim)
            if not c:
                continue
            alvo = publicacao_legacy if r["tipo"] == TIPO_COLETA_LEGACY else atualizacao
            _add_intervalo(alvo, c[0], c[1], inicio, fim)
            usados.append({"id": int(r["id"]), "tipo": str(r["tipo"]), "inicio": c[0], "fim": c[1], "operacao": r["operacao"]})
    finally:
        con.close()
    return atualizacao, publicacao_legacy, usados


def _dados_e_itens(con: sqlite3.Connection, inicio: str, fim: str) -> dict[str, dict[str, int]]:
    if "contratacoes" not in _tabelas(con):
        return {}
    # Desde a higiene de proveniência, itens_coletados=1 é a flag canônica de
    # completude e só pode ser concedida após leitura integral do PNCP. Usá-la
    # diretamente também preserva conjuntos PNCP antigos cujo itens_fonte ainda
    # esteja vazio, sem reclassificá-los como pendentes por metadado incompleto.
    cert = "CASE WHEN COALESCE(itens_coletados,0)=1 THEN 1 ELSE 0 END"
    rows = con.execute(
        f"""SELECT substr(data_publicacao_pncp,1,10) dia,
                   COUNT(*) total,
                   SUM({cert}) itens_ok,
                   SUM(CASE WHEN COALESCE(itens_coletados,0)=0 THEN 1 ELSE 0 END) itens_pendentes
              FROM contratacoes
             WHERE data_publicacao_pncp>=?
               AND data_publicacao_pncp<?
               AND length(data_publicacao_pncp)>=10
             GROUP BY substr(data_publicacao_pncp,1,10)""",
        (inicio, (date.fromisoformat(fim)+timedelta(days=1)).isoformat()),
    ).fetchall()
    out: dict[str, dict[str,int]] = {}
    for r in rows:
        d = _normalizar_data_ref(r[0])
        if not d:
            continue
        out[d] = {
            "total": int(r[1] or 0),
            "itens_ok": int(r[2] or 0),
            "itens_pendentes": int(r[3] or 0),
        }
    return out


def _status_resultados(con: sqlite3.Connection, referencia: str) -> dict[str, Any]:
    tabs = _tabelas(con)
    maxd: str | None = None
    if "resultados_itens" in tabs:
        row = con.execute(
            """SELECT MAX(substr(data_resultado_pncp,1,10))
                 FROM resultados_itens
                WHERE data_resultado_pncp IS NOT NULL
                  AND length(data_resultado_pncp)>=10
                  AND substr(data_resultado_pncp,1,10) BETWEEN '2021-01-01' AND ?""",
            (referencia,),
        ).fetchone()
        if row and row[0]:
            try:
                maxd = date.fromisoformat(str(row[0])[:10]).isoformat()
            except Exception:
                maxd = None
    lag = None
    if maxd:
        lag = (date.fromisoformat(referencia) - date.fromisoformat(maxd)).days
    if lag is None:
        frescor = "SEM_DADOS"
    elif lag <= 7:
        frescor = "ATUAL"
    elif lag <= 30:
        frescor = "ATENCAO"
    elif lag <= 45:
        frescor = "ATRASADO"
    else:
        frescor = "CRITICO"
    sync = {}
    if "comprasgov_resultados_sync" in tabs:
        row = con.execute("SELECT * FROM comprasgov_resultados_sync WHERE id=1").fetchone()
        if row:
            sync = {k: row[k] for k in row.keys()} if isinstance(row, sqlite3.Row) else {}
    return {"data_max": maxd, "defasagem_dias": lag, "frescor": frescor, "sync": sync}


def analisar_cobertura(db_compras: Path, db_jobs: Path, inicio: str, fim: str) -> dict[str, Any]:
    """Analisa o que a base sabe sobre cada dia do período, sem acessar a internet."""
    dias_periodo = _datas(inicio, fim)
    universo = set(dias_periodo)
    con = sqlite3.connect(str(db_compras), timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout=30000")
    try:
        csv_dias, csv_intervalos, marcadores = _cobertura_csv(con, inicio, fim)
        pncp_sweep = _cobertura_pncp_publicacao(con, inicio, fim)
        info_dias = _dados_e_itens(con, inicio, fim)
        resultados = _status_resultados(con, fim)
    finally:
        con.close()

    coleta_update, coleta_legacy_pub, jobs_usados = _cobertura_jobs(Path(db_jobs), inicio, fim)

    # Legacy multifonte percorria publicações por modalidade; portanto pode compor
    # a certificação por publicação quando o job inteiro terminou concluído.
    pncp_publicacao = set(pncp_sweep) | set(coleta_legacy_pub)

    # Cobertura operacional: fonte oficial CSV, varredura histórica por publicação
    # ou COLETA moderna concluída por atualização. Esta métrica serve para dizer se a
    # Radar PNCP possui evidência operacional daquele dia, sem esconder a natureza da fonte.
    operacional = set(csv_dias) | set(pncp_publicacao) | set(coleta_update)
    lacunas_operacionais = universo - operacional

    presenca = set(info_dias)

    # Item completo requer dia operacionalmente coberto. Se o dia tem zero
    # contratações, a cobertura de itens é completa por vacuidade; se há pais,
    # todos precisam estar certificados pelo PNCP.
    itens_completos: set[str] = set()
    dias_itens_pendentes: set[str] = set()
    pendentes_total = 0
    for d in operacional:
        inf = info_dias.get(d)
        if not inf or int(inf.get("total",0)) == 0:
            itens_completos.add(d)
            continue
        if int(inf.get("itens_ok",0)) == int(inf.get("total",0)):
            itens_completos.add(d)
        else:
            dias_itens_pendentes.add(d)
            pendentes_total += max(0, int(inf.get("total",0)) - int(inf.get("itens_ok",0)))

    # Dias cobertos operacionalmente mas sem certificação histórica PNCP por
    # publicação. É uma oportunidade de auditoria, não uma lacuna operacional.
    sem_certificacao_publicacao = operacional - pncp_publicacao

    def bloco(dias: set[str]) -> dict[str, Any]:
        return {"dias": len(dias), "intervalos": comprimir_dias(dias)}

    return {
        "inicio": inicio,
        "fim": fim,
        "total_dias": len(dias_periodo),
        "presenca_dados": bloco(presenca),
        "contratacoes": {
            "operacional": bloco(operacional),
            "lacunas_operacionais": bloco(lacunas_operacionais),
            "pncp_publicacao": bloco(pncp_publicacao),
            "pncp_sweep_tabela": bloco(pncp_sweep),
            "coleta_legacy_publicacao": bloco(coleta_legacy_pub),
            "coleta_atualizacao": bloco(coleta_update),
            "comprasgov": bloco(csv_dias),
            "comprasgov_intervalos_origem": csv_intervalos,
            "sem_certificacao_publicacao": bloco(sem_certificacao_publicacao),
        },
        "itens": {
            "completos": bloco(itens_completos),
            "pendentes": bloco(dias_itens_pendentes),
            "contratacoes_pendentes": pendentes_total,
        },
        "resultados": resultados,
        "marcadores": marcadores,
        "jobs_usados": jobs_usados,
    }
