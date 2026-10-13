#!/usr/bin/env python3
"""Executa construção/atualização da classificação TIC como job."""
from __future__ import annotations
import argparse
from pathlib import Path
import job_manager as jm
import tic_manager as tm


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--modo", choices=["build", "update"], required=True)
    ap.add_argument("--db", required=True)
    ap.add_argument("--tic-db", required=True)
    ap.add_argument("--jobs-db", required=True)
    ap.add_argument("--job-id", type=int, required=True)
    args = ap.parse_args()
    jobs_db = Path(args.jobs_db).resolve()

    def progresso(etapa: str, atual: int, total: int):
        jm.atualizar_progresso_job(args.job_id, etapa, atual, total, jobs_db)
        pct = 100 * atual / total if total else 100
        print(f"[TIC] {etapa}: {atual}/{total} ({pct:.1f}%)", flush=True)

    try:
        res = tm.construir_ou_atualizar(
            Path(args.db), Path(args.tic_db), args.modo,
            progress_callback=progresso,
            stop_callback=lambda: jm.parada_solicitada(args.job_id, jobs_db),
        )
    except InterruptedError:
        print("Classificação TIC interrompida.", flush=True)
        return 130
    except Exception as exc:
        print(f"ERRO TIC: {type(exc).__name__}: {exc}", flush=True)
        return 2
    print(f"Classificação concluída: {res['classificadas']} registros; {res['tic']} TIC.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
