#!/usr/bin/env python3
"""Carga histórica V0.7.6: resiliente a quedas do PNCP e retomável por dia."""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import job_manager as jm
from pncp_health import EXIT_PNCP_INDISPONIVEL, verificar_pncp
from radar_paths import DB_COMPRAS, DB_FTS, DB_TIC

BASE_DIR = Path(__file__).resolve().parent


def datas_periodo(inicio: str, fim: str):
    atual = date.fromisoformat(inicio); final = date.fromisoformat(fim)
    while atual <= final:
        yield atual.isoformat(); atual += timedelta(days=1)


def fmt_num(n: int) -> str:
    return f"{n:,}".replace(",", ".")


def aguardar_pncp(job_id: int, jobs_db: Path, intervalo: int) -> bool:
    tentativa = 0
    while True:
        if jm.parada_solicitada(job_id, jobs_db):
            return False
        tentativa += 1
        ok, detalhe = verificar_pncp(timeout=20)
        if ok:
            print(f"[PNCP] Serviço disponível novamente: {detalhe}", flush=True)
            jm.atualizar_job(job_id, jobs_db, etapa="PNCP disponível", mensagem="PNCP respondeu; retomando coleta.")
            return True
        espera = max(30, int(intervalo))
        print(f"[PNCP] Indisponível ({detalhe}). Nova tentativa em {espera//60 if espera>=60 else espera} {'min' if espera>=60 else 's'}.", flush=True)
        jm.atualizar_job(
            job_id, jobs_db, etapa="Aguardando PNCP",
            mensagem=f"PNCP indisponível. Tentativa {tentativa}; nova verificação em {espera}s.",
        )
        # Dorme em pedaços para a parada responder rápido.
        restante = espera
        while restante > 0:
            if jm.parada_solicitada(job_id, jobs_db):
                return False
            passo = min(5, restante); time.sleep(passo); restante -= passo


def atualizar_auxiliares(db_compras: Path, job_id: int, jobs_db: Path) -> None:
    if DB_FTS.exists():
        try:
            import fts_manager as fm
            st = fm.status_indice(db_compras, DB_FTS)
            if st["estado"] == "DESATUALIZADO":
                print("\n[FINAL] Atualizando FTS5...", flush=True)
                fm.construir_ou_atualizar(
                    db_compras, DB_FTS, "update",
                    progress_callback=lambda etapa,a,t: jm.atualizar_progresso_job(job_id, f"FTS5 · {etapa}", a, t, jobs_db),
                    stop_callback=lambda: jm.parada_solicitada(job_id, jobs_db),
                )
        except InterruptedError:
            raise
        except Exception as exc:
            print(f"AVISO: FTS5 não pôde ser atualizado: {exc}", flush=True)
    if DB_TIC.exists():
        try:
            import tic_manager as tm
            st = tm.status_classificacao(db_compras, DB_TIC)
            if st["estado"] == "DESATUALIZADA":
                print("[FINAL] Atualizando classificação TIC...", flush=True)
                tm.construir_ou_atualizar(
                    db_compras, DB_TIC, "update",
                    progress_callback=lambda etapa,a,t: jm.atualizar_progresso_job(job_id, f"TIC · {etapa}", a, t, jobs_db),
                    stop_callback=lambda: jm.parada_solicitada(job_id, jobs_db),
                )
        except InterruptedError:
            raise
        except Exception as exc:
            print(f"AVISO: classificação TIC não pôde ser atualizada: {exc}", flush=True)


def main() -> int:
    ap=argparse.ArgumentParser(description="Carga histórica resiliente do Radar PNCP V0.7.6")
    ap.add_argument("--inicio", required=True); ap.add_argument("--fim", required=True)
    ap.add_argument("--db", default=str(DB_COMPRAS)); ap.add_argument("--jobs-db", required=True)
    ap.add_argument("--job-id", type=int, required=True)
    ap.add_argument("--espera-pncp", type=int, default=300, help="Segundos entre testes enquanto o PNCP estiver fora.")
    ap.add_argument("--workers-itens", type=int, default=3)
    args=ap.parse_args()
    db_compras=Path(args.db).resolve(); jobs_db=Path(args.jobs_db).resolve()

    # Se o job foi criado como 'reprocessar incompletos', job_dias já contém somente essas datas.
    datas_job = jm.listar_dias_job(args.job_id, jobs_db)
    datas = [d["data_referencia"] for d in datas_job] if datas_job else list(datas_periodo(args.inicio,args.fim))
    total=len(datas); incompletos=0
    print("="*92); print("Radar PNCP V0.7.6 — CARGA HISTÓRICA 2.0")
    print(f"Período: {args.inicio} até {args.fim} | dias deste job: {total}")
    print(f"Banco: {db_compras}"); print("="*92, flush=True)

    for indice,data_ref in enumerate(datas,1):
        if jm.parada_solicitada(args.job_id,jobs_db): return 130
        jm.atualizar_job(args.job_id,jobs_db,data_atual=data_ref,etapa="Preparando dia",
                         mensagem=f"Preparando {data_ref} ({indice}/{total}).")
        antes=jm.obter_progresso_data(db_compras,data_ref)
        if antes["modalidades_concluidas"]==14 and antes["pendentes"]==0:
            jm.atualizar_job_dia(args.job_id,data_ref,jobs_db,status="JA_EXISTIA",iniciado_em=jm.agora(),
                                 finalizado_em=jm.agora(),exit_code=0,mensagem="Data já completa; pulada.")
            print(f"\nDIA {indice}/{total} — {data_ref}: já completo, pulando.", flush=True); continue

        # Não inicia/reinicia o dia enquanto a API principal não responder.
        if not aguardar_pncp(args.job_id,jobs_db,args.espera_pncp): return 130

        iniciado = jm.agora()
        jm.atualizar_job_dia(args.job_id,data_ref,jobs_db,status="EXECUTANDO",iniciado_em=iniciado,
                             finalizado_em=None,exit_code=None,mensagem="Executando radar_diario.py.")
        while True:
            if jm.parada_solicitada(args.job_id,jobs_db): return 130
            jm.atualizar_job(args.job_id,jobs_db,data_atual=data_ref,etapa="Coletando dia",
                             mensagem=f"Coletando {data_ref} ({indice}/{total}).")
            print("\n"+"#"*92); print(f"DIA {indice}/{total} — {data_ref}"); print("#"*92, flush=True)
            cmd=[sys.executable,"-u",str(BASE_DIR/"radar_diario.py"),"--data",data_ref,"--db",str(db_compras),
                 "--workers-itens",str(args.workers_itens)]
            rc=int(subprocess.run(cmd,cwd=str(BASE_DIR),stdin=subprocess.DEVNULL,check=False).returncode)
            if rc == EXIT_PNCP_INDISPONIVEL:
                jm.atualizar_job_dia(args.job_id,data_ref,jobs_db,status="AGUARDANDO_PNCP",mensagem="PNCP caiu durante o dia; aguardando para retomar.")
                print("\nO PNCP caiu durante a coleta. O dia NÃO será contado como incompleto; aguardando retorno...",flush=True)
                if not aguardar_pncp(args.job_id,jobs_db,args.espera_pncp): return 130
                jm.atualizar_job_dia(args.job_id,data_ref,jobs_db,status="EXECUTANDO",mensagem="PNCP voltou; retomando o mesmo dia.")
                continue
            break

        prog=jm.obter_progresso_data(db_compras,data_ref)
        completa=prog["modalidades_concluidas"]==14 and prog["pendentes"]==0
        if completa:
            status="CONCLUIDA_COM_PENDENCIAS_EXTERNAS" if prog["indisponiveis"] else "CONCLUIDA"
            msg="Data concluída." if not prog["indisponiveis"] else "Data concluída com indisponibilidades externas do PNCP."
        else:
            status="PARCIAL" if rc in (0,1) else "ERRO"; incompletos+=1
            msg=f"Data incompleta: modalidades {prog['modalidades_concluidas']}/14; {prog['pendentes']} pendência(s); código {rc}."
        jm.atualizar_job_dia(args.job_id,data_ref,jobs_db,status=status,finalizado_em=jm.agora(),exit_code=rc,mensagem=msg)
        print(f"Resultado: {status} | modalidades {prog['modalidades_concluidas']}/14 | contratações {fmt_num(prog['contratacoes'])} | itens {fmt_num(prog['itens_armazenados'])}",flush=True)

    jm.atualizar_job(args.job_id,jobs_db,data_atual=None,etapa="Atualizando índices")
    try:
        atualizar_auxiliares(db_compras,args.job_id,jobs_db)
    except InterruptedError:
        return 130
    jm.atualizar_job(args.job_id,jobs_db,data_atual=None,etapa="Finalizando")
    print("\n"+"="*92)
    if incompletos:
        print(f"Carga terminou com {incompletos} dia(s) incompleto(s). Use 'Reprocessar incompletos'.",flush=True); return 1
    print("Carga histórica concluída para todos os dias deste job.",flush=True); return 0


if __name__ == "__main__":
    raise SystemExit(main())
