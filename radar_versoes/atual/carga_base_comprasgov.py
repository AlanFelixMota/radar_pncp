#!/usr/bin/env python3
"""Carga-base histórica Compras.gov para a RADAR_PNCP/Radar PNCP V0.7.6.

Reconstrói o fluxo de deltas antigos do Compras.gov sem inferir completude de itens:
  1) repara flags de completude criadas pelas V0.6.1/0.6.1.1;
  2) baixa/importa COMPRA anual 2021..2024;
  3) resolve staging de itens que agora possuem contratação-pai;
  4) baixa/importa COMPRA_ITEM anual 2021..2024;
  5) resolve novamente o staging;
  6) registra a cobertura de deltas disponível e reconstrói FTS/TIC existentes.

Os arquivos anuais também representam registros novos/alterados no período. Portanto,
nenhuma contratação recebe itens_coletados=1 apenas por causa do CSV.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import sys
from pathlib import Path

import job_manager as jm
from importar_comprasgov_csv import (
    CSV_DIR,
    agora,
    baixar,
    conectar_db,
    contar_orfaos,
    importar_arquivo,
    registrar_orfaos_resolvidos,
    resolver_itens_orfaos,
    sessao_http,
)
from proveniencia import (
    FONTE_COMPRASGOV_CSV,
    MIGRACAO_RESET_COMPLETUDE_CSV,
    registrar_cobertura,
)
from radar_paths import DB_COMPRAS, DB_FTS, DB_JOBS, DB_TIC, DATA_DIR, garantir_diretorios

BASE_ANUAL = "https://repositorio.dados.gov.br/seges/comprasgov/anual"
ANOS_BASE = (2021, 2022, 2023, 2024)


def atualizar_job(job_id: int | None, jobs_db: Path | None, etapa: str, atual: int = 0, total: int = 0, mensagem: str | None = None) -> None:
    if job_id is None or jobs_db is None:
        return
    jm.atualizar_job(
        job_id,
        jobs_db,
        etapa=etapa,
        progresso_atual=int(atual),
        progresso_total=int(total),
        mensagem=mensagem or etapa,
    )


def info_anual(ano: int, recurso: str) -> tuple[str, Path, str]:
    if recurso == "COMPRA":
        base = "VW_FT_PNCP_COMPRA"
    elif recurso == "COMPRA_ITEM":
        base = "VW_FT_PNCP_COMPRA_ITEM"
    else:
        raise ValueError(recurso)
    nome = f"comprasGOV-anual-{base}-{ano}.csv"
    url = f"{BASE_ANUAL}/{ano}/{nome}"
    path = CSV_DIR / "anual" / str(ano) / nome
    periodo = f"{ano}-ANUAL"
    return url, path, periodo


def _detalhes_reparo(con: sqlite3.Connection) -> int:
    row = con.execute(
        "SELECT detalhes_json FROM radar_pncp_migracoes WHERE chave=?",
        (MIGRACAO_RESET_COMPLETUDE_CSV,),
    ).fetchone()
    if not row or not row[0]:
        return 0
    try:
        return int(json.loads(row[0]).get("contratacoes_reparadas", 0))
    except Exception:
        return 0


def validar_espaco() -> tuple[int, int]:
    uso = shutil.disk_usage(DATA_DIR)
    livre = int(uso.free)
    recomendado = 6 * 1024**3
    minimo = 3 * 1024**3
    print(f"Espaço livre no disco de dados: {livre/1024**3:.1f} GB", flush=True)
    if livre < minimo:
        raise RuntimeError(
            "Há menos de 3 GB livres. A carga-base anual precisa de espaço para "
            "CSV, crescimento do SQLite e arquivos temporários de índices."
        )
    if livre < recomendado:
        print(
            "AVISO: há menos de 6 GB livres. A carga pode concluir, mas o espaço "
            "é apertado para reconstrução do FTS/TIC.",
            flush=True,
        )
    return livre, recomendado


def importar_anuais_compra(con: sqlite3.Connection, sessao, job_id, jobs_db) -> None:
    total = len(ANOS_BASE)
    for i, ano in enumerate(ANOS_BASE, 1):
        atualizar_job(job_id, jobs_db, f"Base anual · Contratações {ano}", i - 1, total)
        print(f"\n[BASE ANUAL · CONTRATAÇÕES][{ano}]", flush=True)
        url, path, periodo = info_anual(ano, "COMPRA")
        baixar(sessao, url, path, periodo, None, None, "COMPRA")
        importar_arquivo(con, path, url, periodo, "COMPRA", None, None)
        atualizar_job(job_id, jobs_db, f"Base anual · Contratações {ano}", i, total)


def resolver_staging(con: sqlite3.Connection, titulo: str, job_id, jobs_db) -> int:
    antes = contar_orfaos(con)
    print(f"\n[{titulo}] Staging antes: {antes:,} item(ns)", flush=True)
    ultimo = {-1}

    def progresso(atual: int, total: int) -> None:
        atualizar_job(job_id, jobs_db, titulo, atual, total)
        marco = atual // 10000 if atual else 0
        if marco not in ultimo or atual == total:
            ultimo.add(marco)
            print(f"    Staging resolvível: {atual:,}/{total:,} item(ns)", flush=True)

    resolvidos, por_mes = resolver_itens_orfaos(con, progress_callback=progresso)
    registrar_orfaos_resolvidos(con, por_mes)
    depois = contar_orfaos(con)
    print(f"    Resolvidos: {resolvidos:,} item(ns)", flush=True)
    print(f"    Staging restante: {depois:,} item(ns)", flush=True)
    return resolvidos


def importar_anuais_itens(con: sqlite3.Connection, sessao, job_id, jobs_db) -> None:
    total = len(ANOS_BASE)
    for i, ano in enumerate(ANOS_BASE, 1):
        atualizar_job(job_id, jobs_db, f"Base anual · Itens {ano}", i - 1, total)
        print(f"\n[BASE ANUAL · ITENS][{ano}]", flush=True)
        url, path, periodo = info_anual(ano, "COMPRA_ITEM")
        baixar(sessao, url, path, periodo, None, None, "COMPRA_ITEM")
        stats = importar_arquivo(con, path, url, periodo, "COMPRA_ITEM", None, None)
        pendentes = contar_orfaos(con, periodo)
        print(
            f"    Delta anual importado: {stats.get('linhas', 0):,} linha(s). "
            "Completude continua reservada ao PNCP.",
            flush=True,
        )
        if pendentes:
            print(f"    Staging do período: {pendentes:,} item(ns)", flush=True)
        atualizar_job(job_id, jobs_db, f"Base anual · Itens {ano}", i, total)


def status_cadeia(con: sqlite3.Connection) -> tuple[bool, list[str]]:
    faltantes: list[str] = []
    for ano in ANOS_BASE:
        periodo = f"{ano}-ANUAL"
        for recurso in ("COMPRA", "COMPRA_ITEM"):
            r = con.execute(
                "SELECT status FROM comprasgov_importacoes WHERE ano_mes=? AND recurso=?",
                (periodo, recurso),
            ).fetchone()
            if not r or r[0] != "CONCLUIDA":
                faltantes.append(f"{periodo}/{recurso}")

    # A cadeia até 2025 só pode ser considerada processada se os 12 deltas mensais
    # que o usuário já carregou estiverem persistidos como concluídos.
    for mes in range(1, 13):
        periodo = f"2025-{mes:02d}"
        for recurso in ("COMPRA", "COMPRA_ITEM"):
            r = con.execute(
                "SELECT status FROM comprasgov_importacoes WHERE ano_mes=? AND recurso=?",
                (periodo, recurso),
            ).fetchone()
            if not r or r[0] != "CONCLUIDA":
                faltantes.append(f"{periodo}/{recurso}")
    return not faltantes, faltantes


def registrar_resumo_cobertura(con: sqlite3.Connection) -> None:
    completa, faltantes = status_cadeia(con)
    if completa:
        registrar_cobertura(con, "deltas_comprasgov_inicio", "2021-01")
        registrar_cobertura(con, "deltas_comprasgov_fim", "2025-12")
        registrar_cobertura(con, "deltas_comprasgov_status", "PROCESSADOS")
        print("\n[Cobertura] Cadeia de deltas 2021-01 → 2025-12 processada.", flush=True)
    else:
        registrar_cobertura(con, "deltas_comprasgov_status", "PARCIAL")
        registrar_cobertura(con, "deltas_comprasgov_faltantes", ";".join(faltantes[:200]))
        print("\n[Cobertura] Cadeia de deltas ainda PARCIAL.", flush=True)
        print(f"    Recursos faltantes: {len(faltantes)}", flush=True)
        for x in faltantes[:20]:
            print(f"      - {x}", flush=True)
        if len(faltantes) > 20:
            print(f"      ... e mais {len(faltantes)-20}", flush=True)


def reconstruir_indices(db: Path, job_id: int | None, jobs_db: Path | None) -> None:
    # Cargas CSV podem complementar linhas antigas. Atualização incremental por rowid
    # não enxerga essas mudanças; por isso a base histórica faz reconstrução completa.
    if DB_FTS.exists():
        import fts_manager as fm
        print("\n[FINAL] Reconstruindo FTS5 para refletir itens históricos...", flush=True)
        atualizar_job(job_id, jobs_db, "Reconstruindo FTS5", 0, 0)
        fm.construir_ou_atualizar(
            db, DB_FTS, "build",
            progress_callback=(
                (lambda etapa, a, t: atualizar_job(job_id, jobs_db, f"FTS5 · {etapa}", a, t))
                if job_id is not None and jobs_db is not None else None
            ),
            stop_callback=(
                (lambda: jm.parada_solicitada(job_id, jobs_db))
                if job_id is not None and jobs_db is not None else None
            ),
        )
    else:
        print("\n[FINAL] FTS5 ainda não existe; nada a reconstruir.", flush=True)

    if DB_TIC.exists():
        import tic_manager as tm
        print("[FINAL] Reclassificando TIC para considerar itens históricos...", flush=True)
        atualizar_job(job_id, jobs_db, "Reclassificando TIC", 0, 0)
        tm.construir_ou_atualizar(
            db, DB_TIC, "build",
            progress_callback=(
                (lambda etapa, a, t: atualizar_job(job_id, jobs_db, f"TIC · {etapa}", a, t))
                if job_id is not None and jobs_db is not None else None
            ),
            stop_callback=(
                (lambda: jm.parada_solicitada(job_id, jobs_db))
                if job_id is not None and jobs_db is not None else None
            ),
        )
    else:
        print("[FINAL] Classificação TIC ainda não existe; nada a reconstruir.", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description="Carga-base Compras.gov 2021–2024 — V0.7.6")
    ap.add_argument("--db", type=Path, default=DB_COMPRAS)
    ap.add_argument("--jobs-db", type=Path, default=DB_JOBS)
    ap.add_argument("--job-id", type=int)
    args = ap.parse_args()
    jobs_db = args.jobs_db if args.job_id else None

    garantir_diretorios()
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    print("=" * 92, flush=True)
    print("Radar PNCP / Radar PNCP V0.7.6 — REPARO + CARGA-BASE COMPRAS.GOV", flush=True)
    print("=" * 92, flush=True)
    print(f"Banco: {args.db.resolve()}", flush=True)
    print("Base anual: 2021 → 2024; deltas mensais 2025 já existentes serão reaproveitados.", flush=True)
    print("Regra de segurança: CSV nunca confirma completude dos itens; PNCP é a autoridade.", flush=True)

    con: sqlite3.Connection | None = None
    sessao = None
    try:
        validar_espaco()
        atualizar_job(args.job_id, jobs_db, "Preparando banco e reparando flags", 0, 1)
        con = conectar_db(args.db)
        reparadas = _detalhes_reparo(con)
        print(f"\n[REPARO] Contratações cuja completude CSV foi desfeita: {reparadas:,}", flush=True)
        print("         Os itens já gravados foram preservados; apenas a flag de completude foi removida.", flush=True)
        atualizar_job(args.job_id, jobs_db, "Reparo concluído", 1, 1)

        staging_inicial = contar_orfaos(con)
        sessao = sessao_http()

        importar_anuais_compra(con, sessao, args.job_id, jobs_db)
        resolvidos_pais = resolver_staging(con, "Após contratações anuais", args.job_id, jobs_db)

        importar_anuais_itens(con, sessao, args.job_id, jobs_db)
        resolvidos_itens = resolver_staging(con, "Após itens anuais", args.job_id, jobs_db)

        registrar_resumo_cobertura(con)
        staging_final = contar_orfaos(con)
        compras_csv = int(con.execute(
            "SELECT COUNT(*) FROM contratacoes WHERE fonte_primeira=? OR fonte_ultima=?",
            (FONTE_COMPRASGOV_CSV, FONTE_COMPRASGOV_CSV),
        ).fetchone()[0] or 0)
        itens_csv = int(con.execute(
            "SELECT COUNT(*) FROM itens WHERE fonte_primeira=? OR fonte_ultima=?",
            (FONTE_COMPRASGOV_CSV, FONTE_COMPRASGOV_CSV),
        ).fetchone()[0] or 0)
        csv_marcados_completos = int(con.execute(
            "SELECT COUNT(*) FROM contratacoes WHERE COALESCE(itens_coletados,0)=1 AND itens_fonte=?",
            (FONTE_COMPRASGOV_CSV,),
        ).fetchone()[0] or 0)
        contratos_orfaos = int(con.execute(
            "SELECT COUNT(DISTINCT numero_controle_pncp) FROM comprasgov_itens_orfaos"
        ).fetchone()[0] or 0)
        orfaos_por_ano = [
            (str(r[0] or "?"), int(r[1] or 0))
            for r in con.execute(
                """SELECT substr(numero_controle_pncp,-4) AS ano, COUNT(*)
                     FROM comprasgov_itens_orfaos
                    GROUP BY substr(numero_controle_pncp,-4)
                    ORDER BY COUNT(*) DESC LIMIT 12"""
            ).fetchall()
        ]

        con.commit()
        con.close(); con = None

        reconstruir_indices(args.db.resolve(), args.job_id, jobs_db)
        atualizar_job(args.job_id, jobs_db, "Concluído", 1, 1)

        print("\n" + "=" * 92, flush=True)
        print("RESUMO DA CARGA-BASE", flush=True)
        print("=" * 92, flush=True)
        print(f"Staging no início:                    {staging_inicial:,} item(ns)", flush=True)
        print(f"Resolvidos após carregar pais:        {resolvidos_pais:,} item(ns)", flush=True)
        print(f"Resolvidos após carregar itens:       {resolvidos_itens:,} item(ns)", flush=True)
        print(f"Staging pendente ao final:            {staging_final:,} item(ns)", flush=True)
        print(f"Contratações-pai distintas no staging:{contratos_orfaos:>12,}", flush=True)
        if orfaos_por_ano:
            print("Staging por ano do ID PNCP:", flush=True)
            for ano, qtd in orfaos_por_ano:
                print(f"  {ano}: {qtd:,} item(ns)", flush=True)
        print(f"Contratações tocadas pelo CSV:        {compras_csv:,}", flush=True)
        print(f"Itens tocados pelo CSV:               {itens_csv:,}", flush=True)
        print(f"Completudes atribuídas somente a CSV: {csv_marcados_completos:,} (esperado: 0)", flush=True)
        if csv_marcados_completos:
            print("AVISO: existem flags CSV indevidas; não use a coleta PNCP até revisar.", flush=True)
            return 6
        print("\nCarga-base concluída com a regra de precedência PNCP preservada.", flush=True)
        return 0
    except InterruptedError:
        print("\nInterrompida pelo usuário; checkpoints e arquivos baixados foram preservados.", flush=True)
        return 130
    except KeyboardInterrupt:
        print("\nInterrompida pelo usuário; checkpoints preservados.", flush=True)
        return 130
    except Exception as exc:
        print(f"\nERRO: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        return 5
    finally:
        if sessao is not None:
            sessao.close()
        if con is not None:
            con.close()


if __name__ == "__main__":
    raise SystemExit(main())
