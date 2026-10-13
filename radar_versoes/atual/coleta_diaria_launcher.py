#!/usr/bin/env python3
"""Launcher leve para Tarefas Agendadas do Windows — Radar PNCP V0.7.6."""
from __future__ import annotations

import argparse
from datetime import datetime

import job_manager as jm
from radar_paths import DB_COMPRAS, DB_JOBS, LOG_DIR


def main() -> int:
    ap = argparse.ArgumentParser(description="Inicia COLETA operacional em segundo plano")
    ap.add_argument("--dias", type=int, default=3)
    ap.add_argument("--modo", choices=("diario", "revisao"), default="diario")
    args = ap.parse_args()

    from atualizar_radar_pncp import em_atualizacao
    if em_atualizacao(DB_JOBS):
        print('Radar PNCP em atualização; aguarde a retomada dos agendamentos.')
        return 0

    jm.preparar_banco_jobs(DB_JOBS)
    jm.atualizar_jobs_interrompidos(DB_JOBS)
    ativo = jm.obter_job_ativo(DB_JOBS)
    if ativo:
        print(
            f"[{datetime.now().isoformat(timespec='seconds')}] COLETA não iniciada: "
            f"já existe job ativo #{ativo['id']} ({ativo['tipo']})."
        )
        return 0

    jid = jm.iniciar_atualizacao_diaria(
        janela_dias=args.dias,
        data_fim=None,
        db_compras=DB_COMPRAS,
        db_jobs=DB_JOBS,
        log_dir=LOG_DIR,
        modo=args.modo,
    )
    rotulo = "diária" if args.modo == "diario" else "revisão semanal"
    print(f"[{datetime.now().isoformat(timespec='seconds')}] COLETA {rotulo} iniciada: job #{jid} · janela {args.dias} dias.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
