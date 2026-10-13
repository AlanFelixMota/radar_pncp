#!/usr/bin/env python3
"""Constrói/atualiza o índice FTS5 como job do Radar PNCP."""

from __future__ import annotations

import argparse
from datetime import datetime
from fts_sync import PausaIndice
from pathlib import Path

import fts_manager as fm
import job_manager as jm
from pausa_atualizacao import PausaAtualizacao, EXIT_ATUALIZACAO
from interface_radar_pncp import VERSION


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--modo", choices=["build", "update"], required=True)
    ap.add_argument("--db", required=True)
    ap.add_argument("--fts-db", required=True)
    ap.add_argument("--jobs-db", required=True)
    ap.add_argument("--job-id", type=int, required=True)
    ap.add_argument("--deadline-iso")
    ap.add_argument("--max-segundos", type=int, default=600)
    args = ap.parse_args()

    jobs_db = Path(args.jobs_db).resolve()
    pausa = PausaAtualizacao(jobs_db)

    def progresso(etapa: str, atual: int, total: int):
        jm.atualizar_progresso_job(
            args.job_id, etapa, atual, total, jobs_db
        )
        pct = (100 * atual / total) if total else 0
        print(
            f"[FTS5] {etapa}: {atual}/{total} ({pct:.1f}%)",
            flush=True,
        )

    def parar() -> bool:
        if pausa():
            raise InterruptedError("Atualização do sistema; checkpoint preservado.")
        if args.deadline_iso and datetime.now() >= datetime.fromisoformat(args.deadline_iso):
            raise PausaIndice("Janela de segurança da COLETA; checkpoint preservado.")
        return jm.parada_solicitada(args.job_id, jobs_db)

    print("=" * 90, flush=True)
    print(f"Radar PNCP V{VERSION} — ÍNDICE FTS5 ({args.modo})", flush=True)
    print(f"Base:   {args.db}", flush=True)
    print(f"Índice: {args.fts_db}", flush=True)
    print("=" * 90, flush=True)

    try:
        resultado = fm.construir_ou_atualizar(
            Path(args.db),
            Path(args.fts_db),
            args.modo,
            progress_callback=progresso,
            stop_callback=parar,
            max_segundos=args.max_segundos,
        )
    except PausaIndice as exc:
        print(str(exc), flush=True)
        return 76
    except InterruptedError:
        if pausa.solicitada:
            print("Índice pausado para atualização; checkpoint preservado.", flush=True)
            return EXIT_ATUALIZACAO
        print("Índice interrompido pelo usuário.", flush=True)
        return 130
    except Exception as exc:
        print(f"ERRO FTS5: {type(exc).__name__}: {exc}", flush=True)
        return 2

    print("", flush=True)
    print(
        f"Concluído: {resultado['contratacoes_indexadas']} contratações e "
        f"{resultado['itens_indexados']} itens indexados.",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
