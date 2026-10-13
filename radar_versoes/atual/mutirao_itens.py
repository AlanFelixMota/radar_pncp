#!/usr/bin/env python3
"""Mutirão automático e resumível de itens — RADAR_PNCP/Radar PNCP V0.7.6.

Executa lotes sucessivos da fila incremental sem recalcular o snapshot entre
eles. Por padrão, respeita uma janela de segurança antes da COLETA diária.

O mutirão não transforma uma execução de milhões de contratações em uma única
transação: cada conjunto é confirmado atomicamente e cada página continua
persistida no staging/checkpoint da fila incremental.
"""
from __future__ import annotations

import argparse
import sqlite3
import time
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable

import job_manager as jm
from pausa_atualizacao import PausaAtualizacao, EXIT_ATUALIZACAO
from fts_sync import PausaIndice
from itens_incremental import codificar_progresso, garantir_estrutura, processar_fila, resumo_fila
from pncp_rate import criar_sessao_controlada
from radar_paths import DB_COMPRAS, DB_JOBS

EXIT_OK = 0
EXIT_INSTABILIDADE = 75
EXIT_JANELA_SEGURANCA = 76
EXIT_SEM_PROGRESSO = 77
EXIT_INDICE_PENDENTE = 78


def _fmt(n: int) -> str:
    return f"{int(n):,}".replace(",", ".")


def _parse_hora(hora: str) -> tuple[int, int]:
    try:
        hh, mm = (int(x) for x in hora.split(":", 1))
    except Exception as exc:
        raise ValueError("Horário inválido; use HH:MM.") from exc
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        raise ValueError("Horário inválido; use HH:MM.")
    return hh, mm


def proxima_coleta(hora_coleta: str, agora_dt: datetime | None = None) -> datetime:
    agora_dt = agora_dt or datetime.now()
    hh, mm = _parse_hora(hora_coleta)
    alvo = agora_dt.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if alvo <= agora_dt:
        alvo += timedelta(days=1)
    return alvo


def calcular_deadline(hora_coleta: str, margem_minutos: int, agora_dt: datetime | None = None) -> datetime:
    return proxima_coleta(hora_coleta, agora_dt) - timedelta(minutes=max(0, int(margem_minutos)))


def executar_mutirao(
    con: sqlite3.Connection,
    *,
    lote_contratacoes: int = 2000,
    lote_paginas: int = 4000,
    hora_coleta: str = "00:30",
    margem_minutos: int = 60,
    sem_limite_horario: bool = False,
    deadline_absoluto: datetime | None = None,
    jobs_db: Path | None = None,
    job_id: int | None = None,
    processador: Callable[..., dict[str, Any]] = processar_fila,
    agora_fn: Callable[[], datetime] = datetime.now,
    dormir_fn: Callable[[float], None] = time.sleep,
) -> tuple[int, dict[str, Any]]:
    """Processa o backlog em lotes sucessivos e devolve (exit_code, resumo)."""
    if lote_contratacoes <= 0 or lote_paginas <= 0:
        raise ValueError("Os tamanhos de lote devem ser maiores que zero.")

    garantir_estrutura(con)
    inicio = agora_fn()
    if sem_limite_horario:
        deadline = None
    elif deadline_absoluto is not None:
        deadline = deadline_absoluto
    else:
        deadline = calcular_deadline(hora_coleta, margem_minutos, inicio)
    sessao, limiter = criar_sessao_controlada(1.25)
    totais = Counter()
    lote_num = 0

    inicial = resumo_fila(con)
    elegivel_inicial = int(inicial.get("backlog_elegivel", 0))
    ativos_iniciais = (
        int(inicial.get("pendentes", 0))
        + int(inicial.get("em_andamento", 0))
        + int(inicial.get("erro_transitorio", 0))
    )
    total_trabalho_inicial = elegivel_inicial + int(inicial.get("revalidacoes_pendentes", 0))

    print("=" * 92, flush=True)
    print("Radar PNCP V0.7.6 — MUTIRÃO AUTOMÁTICO DE ITENS", flush=True)
    print(
        f"Backlog elegível inicial: {_fmt(elegivel_inicial)} · fila ativa: {_fmt(ativos_iniciais)} · "
        f"revalidações: {_fmt(int(inicial.get('revalidacoes_pendentes', 0)))}.",
        flush=True,
    )
    print(
        f"Lotes: até {_fmt(lote_contratacoes)} contratação(ões) / {_fmt(lote_paginas)} página(s).",
        flush=True,
    )
    if deadline is not None:
        if deadline_absoluto is not None:
            print(
                f"Janela de segurança: parar até {deadline:%d/%m/%Y %H:%M} "
                "para liberar a COLETA ou o horário reservado à pesquisa.",
                flush=True,
            )
        else:
            print(
                f"Janela de segurança: parar até {deadline:%d/%m/%Y %H:%M} "
                f"({margem_minutos} min antes da COLETA diária das {hora_coleta}).",
                flush=True,
            )
    else:
        print("Janela de segurança desativada: continuar até esvaziar ou ocorrer instabilidade.", flush=True)
    print("Snapshot: adiado durante todo o mutirão.", flush=True)
    print("=" * 92, flush=True)

    if job_id is not None and jobs_db is not None:
        jm.atualizar_job(
            job_id,
            jobs_db,
            progresso_atual=0,
            progresso_total=max(1, total_trabalho_inicial),
            etapa="Mutirão de itens · iniciando",
            mensagem=codificar_progresso({
                "kind": "itens_mutirao",
                "lote_num": 0,
                "lote_contratos_processados": 0,
                "lote_contratos_limite": int(lote_contratacoes),
                "lote_paginas_processadas": 0,
                "lote_paginas_limite": int(lote_paginas),
                "sessao_conjuntos_confirmados": 0,
                "sessao_paginas": 0,
                "sessao_itens": 0,
                "sessao_erros": 0,
                "backlog_inicial": int(total_trabalho_inicial),
                "backlog_elegivel_inicial": int(elegivel_inicial),
                "backlog_restante_estimado": int(total_trabalho_inicial),
                "elapsed_seconds": 0.0,
                "deadline": deadline.isoformat(timespec="seconds") if deadline else None,
            }),
        )

    pausa = PausaAtualizacao(jobs_db)

    def atingiu_deadline() -> bool:
        return pausa() or (deadline is not None and agora_fn() >= deadline)

    if elegivel_inicial <= 0 and ativos_iniciais <= 0 and int(inicial.get("revalidacoes_pendentes", 0)) <= 0:
        print("[Mutirão] Backlog elegível e fila ativa já estão vazios.", flush=True)
        return EXIT_OK, {
            "motivo": "BACKLOG_ESVAZIADO",
            "backlog_elegivel": 0,
            "lotes": 0,
            "fila": inicial,
        }

    if pausa():
        print("[Mutirão] Pausado para atualização; nenhum novo lote iniciado.", flush=True)
        return EXIT_ATUALIZACAO, {"motivo": "ATUALIZACAO_SISTEMA", "lotes": 0}

    if atingiu_deadline():
        print("[Mutirão] Janela de segurança já alcançada; nenhum novo lote será iniciado.", flush=True)
        return EXIT_JANELA_SEGURANCA, {
            "motivo": "JANELA_SEGURANCA",
            "backlog_elegivel": elegivel_inicial,
            "lotes": 0,
        }

    atual = inicial
    while True:
        backlog_elegivel = int(atual.get("backlog_elegivel", 0))
        ativos = (
            int(atual.get("pendentes", 0))
            + int(atual.get("em_andamento", 0))
            + int(atual.get("erro_transitorio", 0))
        )
        revalidacoes = int(atual.get("revalidacoes_pendentes", 0))

        if backlog_elegivel <= 0 and ativos <= 0 and revalidacoes <= 0:
            print("[Mutirão] Backlog elegível e fila ativa esvaziados.", flush=True)
            codigo = EXIT_OK
            motivo = "BACKLOG_ESVAZIADO"
            break
        if pausa():
            codigo = EXIT_ATUALIZACAO
            motivo = "ATUALIZACAO_SISTEMA"
            break
        if atingiu_deadline():
            print("[Mutirão] Janela de segurança alcançada; progresso preservado para a próxima execução.", flush=True)
            codigo = EXIT_JANELA_SEGURANCA
            motivo = "JANELA_SEGURANCA"
            break

        lote_num += 1
        print("", flush=True)
        print(
            f"[Mutirão] Lote #{lote_num} · backlog elegível {_fmt(backlog_elegivel)} · "
            f"fila ativa {_fmt(ativos)} · revalidações {_fmt(revalidacoes)}.",
            flush=True,
        )
        feito_antes_lote = int(totais["concluidos"])
        paginas_antes_lote = int(totais["paginas"])
        itens_antes_lote = int(totais["itens"])
        erros_antes_lote = int(totais["erros"])

        def progresso_lote(p: dict[str, Any]) -> None:
            if job_id is None or jobs_db is None:
                return
            lote_conf = int(p.get("conjuntos_confirmados", 0))
            sessao_conf = feito_antes_lote + lote_conf
            sessao_pag = paginas_antes_lote + int(p.get("paginas_processadas", 0))
            sessao_itens = itens_antes_lote + int(p.get("itens_confirmados", 0))
            sessao_erros = erros_antes_lote + int(p.get("erros_recuperaveis", 0))
            restante_estimado = max(0, total_trabalho_inicial - sessao_conf)
            payload = {
                "kind": "itens_mutirao",
                "lote_num": int(lote_num),
                "lote_contratos_processados": int(p.get("contratos_processados", 0)),
                "lote_contratos_selecionados": int(p.get("contratos_selecionados", lote_contratacoes)),
                "lote_contratos_limite": int(lote_contratacoes),
                "lote_paginas_processadas": int(p.get("paginas_processadas", 0)),
                "lote_paginas_limite": int(lote_paginas),
                "lote_conjuntos_confirmados": lote_conf,
                "lote_itens_confirmados": int(p.get("itens_confirmados", 0)),
                "lote_erros": int(p.get("erros_recuperaveis", 0)),
                "sessao_conjuntos_confirmados": sessao_conf,
                "sessao_paginas": sessao_pag,
                "sessao_itens": sessao_itens,
                "sessao_erros": sessao_erros,
                "backlog_inicial": int(total_trabalho_inicial),
                "backlog_elegivel_inicial": int(elegivel_inicial),
                "backlog_restante_estimado": restante_estimado,
                "elapsed_seconds": max(0.0, (agora_fn() - inicio).total_seconds()),
                "lote_elapsed_seconds": float(p.get("elapsed_seconds", 0.0) or 0.0),
                "deadline": deadline.isoformat(timespec="seconds") if deadline else None,
                "contrato_atual": p.get("contrato_atual"),
                "pagina_atual": p.get("pagina_atual"),
            }
            jm.atualizar_job(
                job_id,
                jobs_db,
                progresso_atual=min(sessao_conf, max(1, total_trabalho_inicial)),
                progresso_total=max(1, total_trabalho_inicial),
                etapa=f"Mutirão · lote {lote_num} · {int(p.get('contratos_processados',0)):,}/{int(p.get('contratos_selecionados') or lote_contratacoes):,}".replace(",", "."),
                mensagem=codificar_progresso(payload),
            )

        if job_id is not None and jobs_db is not None:
            progresso_lote({
                "contratos_processados": 0,
                "contratos_selecionados": min(lote_contratacoes, max(0, backlog_elegivel + ativos + revalidacoes)),
                "paginas_processadas": 0,
                "conjuntos_confirmados": 0,
                "itens_confirmados": 0,
                "erros_recuperaveis": 0,
            })

        out = processador(
            con,
            modo="mutirao",
            limite_contratacoes=lote_contratacoes,
            limite_paginas=lote_paginas,
            sessao_pncp=sessao,
            limiter=limiter,
            timeout=90,
            verbose=False,
            parar_quando=atingiu_deadline,
            max_erros_transitorios=10,
            progresso_cb=progresso_lote,
        )
        indice_pendente = True
        try:
            if pausa():
                raise InterruptedError('Atualização do sistema')
            import fts_manager as fm
            from radar_paths import DB_FTS
            db_atual = Path(con.execute('PRAGMA database_list').fetchone()[2])
            if fm.status_indice(db_atual, DB_FTS)['estado'] in ('DESATUALIZADO', 'INCOMPLETO'):
                con.commit()
                fm.construir_ou_atualizar(db_atual, DB_FTS, 'update',
                                        stop_callback=atingiu_deadline, max_segundos=120)
            indice_pendente = fm.status_indice(db_atual, DB_FTS)['estado'] != 'ATUALIZADO'
        except (PausaIndice, InterruptedError):
            print('[FTS5] Sincronização pausada; checkpoint preservado.', flush=True)
        except Exception as exc:
            print(f'[FTS5] Sincronização pendente: {exc}', flush=True)
        totais["contratos"] += int(out.get("contratos", 0))
        totais["concluidos"] += int(out.get("concluidos", 0))
        totais["paginas"] += int(out.get("paginas", 0))
        totais["itens"] += int(out.get("itens_confirmados", 0))
        totais["erros"] += int(out.get("erros_transitorios", 0))
        totais["indisponiveis"] += int(out.get("indisponiveis", 0))
        totais["revalidados"] += int(out.get("revalidados", 0))

        fila = out.get("fila") or resumo_fila(con)
        atual = fila
        restante = int(fila.get("backlog_elegivel", 0))
        ativos_depois = (
            int(fila.get("pendentes", 0))
            + int(fila.get("em_andamento", 0))
            + int(fila.get("erro_transitorio", 0))
        )
        print(
            f"[Mutirão] Lote #{lote_num} concluído: {_fmt(int(out.get('contratos',0)))} contrato(s) · "
            f"{_fmt(int(out.get('paginas',0)))} página(s) · {_fmt(int(out.get('concluidos',0)))} conjunto(s) · "
            f"{_fmt(int(out.get('itens_confirmados',0)))} item(ns) · {_fmt(int(out.get('erros_transitorios',0)))} erro(s).",
            flush=True,
        )
        print(
            f"          Restante elegível: {_fmt(restante)} · fila ativa: {_fmt(ativos_depois)} · "
            f"indisponíveis/removidos: {_fmt(int(fila.get('backlog_indisponivel',0)))}.",
            flush=True,
        )

        if pausa():
            print("[Mutirão] Pausado para atualização; páginas e checkpoint preservados.", flush=True)
            codigo = EXIT_ATUALIZACAO
            motivo = "ATUALIZACAO_SISTEMA"
            break
        if out.get("parada_segura") or atingiu_deadline():
            print("[Mutirão] Pausado pela janela de segurança; checkpoint/staging preservados.", flush=True)
            codigo = EXIT_JANELA_SEGURANCA
            motivo = "JANELA_SEGURANCA"
            break
        if out.get("limite_erros_transitorios"):
            print("[Mutirão] Pausado por instabilidade: limite de 10 erros recuperáveis no lote.", flush=True)
            codigo = EXIT_INSTABILIDADE
            motivo = "INSTABILIDADE"
            break
        if int(out.get("contratos", 0)) == 0:
            print(
                "[Mutirão] Nenhum contrato pôde ser selecionado/processado apesar de ainda haver trabalho. "
                "Mutirão pausado para diagnóstico.",
                flush=True,
            )
            codigo = EXIT_SEM_PROGRESSO
            motivo = "SEM_PROGRESSO"
            break

        if indice_pendente:
            print('[Mutirão] Cedendo a vez à sincronização do índice; nenhum novo lote será iniciado nesta sessão.', flush=True)
            codigo = EXIT_INDICE_PENDENTE
            motivo = "INDICE_PENDENTE"
            break

        # Pequena folga entre lotes. O rate limiter principal continua sendo o
        # controlador de ritmo das requisições PNCP.
        dormir_fn(2.0)

    final = atual if 'atual' in locals() else resumo_fila(con)
    if job_id is not None and jobs_db is not None:
        restante = int(final.get("backlog_elegivel", 0)) + int(final.get("revalidacoes_pendentes", 0))
        feito = max(0, total_trabalho_inicial - restante)
        jm.atualizar_job(
            job_id,
            jobs_db,
            progresso_atual=min(feito, max(1, total_trabalho_inicial)),
            progresso_total=max(1, total_trabalho_inicial),
            etapa="Mutirão de itens · finalizando",
            mensagem=codificar_progresso({
                "kind": "itens_mutirao",
                "lote_num": int(lote_num),
                "lote_contratos_processados": 0,
                "lote_contratos_limite": int(lote_contratacoes),
                "lote_paginas_processadas": 0,
                "lote_paginas_limite": int(lote_paginas),
                "sessao_conjuntos_confirmados": int(totais["concluidos"]),
                "sessao_paginas": int(totais["paginas"]),
                "sessao_itens": int(totais["itens"]),
                "sessao_erros": int(totais["erros"]),
                "backlog_inicial": int(total_trabalho_inicial),
                "backlog_restante_estimado": int(restante),
                "elapsed_seconds": max(0.0, (agora_fn() - inicio).total_seconds()),
                "deadline": deadline.isoformat(timespec="seconds") if deadline else None,
            }),
        )

    print("", flush=True)
    print("=" * 92, flush=True)
    print(
        f"[Mutirão] Resumo geral: {lote_num} lote(s) · {_fmt(totais['contratos'])} contrato(s) · "
        f"{_fmt(totais['paginas'])} página(s) · {_fmt(totais['concluidos'])} conjunto(s) confirmado(s) · "
        f"{_fmt(totais['itens'])} item(ns) PNCP · {_fmt(totais['erros'])} erro(s) recuperável(is).",
        flush=True,
    )
    print(
        f"          Backlog elegível restante: {_fmt(int(final.get('backlog_elegivel',0)))} · "
        f"revalidações: {_fmt(int(final.get('revalidacoes_pendentes',0)))} · "
        f"motivo do término: {motivo}.",
        flush=True,
    )
    print("          Snapshot completo não foi recalculado durante o mutirão.", flush=True)
    print("=" * 92, flush=True)

    return codigo, {
        "motivo": motivo,
        "lotes": lote_num,
        "totais": dict(totais),
        "fila": final,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Radar PNCP V0.7.6 — mutirão automático de itens")
    ap.add_argument("--db", type=Path, default=DB_COMPRAS)
    ap.add_argument("--jobs-db", type=Path, default=DB_JOBS)
    ap.add_argument("--job-id", type=int)
    ap.add_argument("--lote-contratacoes", type=int, default=2000)
    ap.add_argument("--lote-paginas", type=int, default=4000)
    ap.add_argument("--hora-coleta", default="00:30")
    ap.add_argument("--margem-minutos", type=int, default=60)
    ap.add_argument("--sem-limite-horario", action="store_true")
    ap.add_argument(
        "--deadline-iso",
        help="Deadline absoluto local (ISO) usado pelo controlador persistente; tem precedência sobre --hora-coleta.",
    )
    args = ap.parse_args()

    deadline_absoluto = None
    if args.deadline_iso:
        try:
            deadline_absoluto = datetime.fromisoformat(args.deadline_iso)
            if deadline_absoluto.tzinfo is not None:
                deadline_absoluto = deadline_absoluto.astimezone().replace(tzinfo=None)
        except ValueError as exc:
            raise SystemExit(f"--deadline-iso inválido: {args.deadline_iso}") from exc

    con = sqlite3.connect(str(args.db), timeout=60)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON")
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=60000")
    try:
        codigo, _ = executar_mutirao(
            con,
            lote_contratacoes=args.lote_contratacoes,
            lote_paginas=args.lote_paginas,
            hora_coleta=args.hora_coleta,
            margem_minutos=args.margem_minutos,
            sem_limite_horario=args.sem_limite_horario,
            deadline_absoluto=deadline_absoluto,
            jobs_db=args.jobs_db if args.job_id else None,
            job_id=args.job_id,
        )
        return codigo
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
