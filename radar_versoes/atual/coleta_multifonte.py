#!/usr/bin/env python3
"""COLETA incremental multifuente — Radar PNCP / Radar PNCP V0.7.6.

Estratégia desta versão:
- PNCP /api/consulta é a fonte PRIMÁRIA para descobrir/atualizar contratações.
- Compras.gov API de contratações é uma fonte COMPLEMENTAR e oportunista.
- Compras.gov API de resultados é testada separadamente e, quando saudável,
  enriquece a camada de fornecedores/preços homologados.
- Nenhuma fonte complementar impede a atualização via PNCP.
- Itens detalhados NÃO são consultados por este job.
- V0.7.6: PNCP usa sessão com ritmo global adaptativo, cooldown cooperativo
  em HTTP 429 e checkpoint por dia+modalidade. O job não martela as demais
  modalidades quando o rate limit persiste.
- V0.7.6: jobs PNCP parciais atualizam FTS/TIC dos registros já gravados,
  mas adiam o snapshot completo (caro) até a janela atingir ponto estável.

Retornos:
  0   = PNCP completo e fontes complementares disponíveis/concluídas.
  75  = PNCP incompleto/indisponível (retomável).
  76  = PNCP completo; ao menos uma fonte complementar ficou indisponível.
  130 = interrupção pelo usuário.
"""
from __future__ import annotations

import argparse
import math
import sqlite3
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import requests

import job_manager as jm
import radar_stats as rs
from comprasgov_api_incremental import (
    URL_COMPRAS,
    URL_RESULTADOS,
    buscar_compras_dia,
    buscar_resultados_dia,
    sessao as sessao_compras,
)
from comprasgov_resultados import garantir_tabelas as garantir_resultados
from coletar_pncp import (
    BASE_CONSULTA,
    MODALIDADES,
    TAMANHO_PAGINA_CONTRATACOES,
    conectar as conectar_pncp,
    criar_banco,
    extrair_lista,
    get_json as pncp_get_json,
    modalidade_ja_concluida,
    marcar_modalidade,
    preparar_controle_modalidades,
    salvar_contratacao,
)
from importar_comprasgov_csv import atualizar_auxiliares
from pncp_health import eh_erro_transitorio
from pncp_rate import criar_sessao_controlada
from proveniencia import registrar_cobertura
from radar_paths import DB_COMPRAS, DB_JOBS, DB_STATS

EXIT_PARCIAL = 75
EXIT_COMPLEMENTAR_INDISPONIVEL = 76
PREFLIGHT_TAMANHO_PAGINA = 10
TRANSITORIOS = {408, 425, 500, 502, 503, 504}


class RateLimitPersistente(RuntimeError):
    """PNCP continuou devolvendo 429 mesmo após cooldowns progressivos."""


def datas(inicio: str, fim: str) -> list[str]:
    a = date.fromisoformat(inicio)
    b = date.fromisoformat(fim)
    if b < a:
        raise ValueError("Data final anterior à inicial.")
    if (b - a).days > 90:
        raise ValueError("Para segurança, processe no máximo 91 dias por job.")
    out: list[str] = []
    while a <= b:
        out.append(a.isoformat())
        a += timedelta(days=1)
    return out


def _atualizar(job_id: int | None, jobs_db: Path | None, etapa: str, atual: int, total: int, mensagem: str | None = None) -> None:
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


def _parada(job_id: int | None, jobs_db: Path | None) -> bool:
    return bool(job_id is not None and jobs_db is not None and jm.parada_solicitada(job_id, jobs_db))


def _status_http(exc: BaseException) -> int | None:
    resp = getattr(exc, "response", None)
    try:
        return int(resp.status_code) if resp is not None else None
    except Exception:
        return None


def _resposta_detalhada(sessao: requests.Session, url: str, params: dict[str, Any], timeout=(10, 30)) -> tuple[bool, str]:
    """Preflight sem esconder o corpo de erros 400/5xx."""
    try:
        r = sessao.get(url, params=params, timeout=timeout)
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"
    corpo = (r.text or "").strip().replace("\r", " ").replace("\n", " ")
    if len(corpo) > 800:
        corpo = corpo[:800] + "..."
    if 200 <= r.status_code < 300:
        return True, f"HTTP {r.status_code}"
    detalhe = f"HTTP {r.status_code}"
    if corpo:
        detalhe += f" · resposta: {corpo}"
    return False, detalhe


def preflight_pncp(
    sessao: requests.Session,
    limiter: Any,
    dia: str,
    job_id: int | None,
    jobs_db: Path | None,
) -> tuple[bool, str]:
    """Sonda PNCP com o mesmo controle de ritmo usado pela coleta real."""
    data_ref = dia.replace("-", "")
    params = {
        "dataInicial": data_ref,
        "dataFinal": data_ref,
        "codigoModalidadeContratacao": 6,
        "pagina": 1,
        "tamanhoPagina": PREFLIGHT_TAMANHO_PAGINA,
    }
    transitorios = 0
    while True:
        try:
            pncp_get_json(sessao, f"{BASE_CONSULTA}/contratacoes/publicacao", params=params, timeout=30)
            return True, f"HTTP 200/204 · ritmo {limiter.intervalo:.2f} s/requisição"
        except InterruptedError:
            raise
        except requests.HTTPError as exc:
            st = _status_http(exc)
            if st == 429:
                espera, parar = limiter.cooldown_429(getattr(exc, "response", None), lambda: _parada(job_id, jobs_db))
                print(
                    f"[PREFLIGHT] PNCP respondeu 429; cooldown de {espera:.0f} s concluído · "
                    f"ritmo {limiter.intervalo:.2f} s/requisição.",
                    flush=True,
                )
                if parar:
                    return False, "HTTP 429 persistente após cooldowns adaptativos"
                continue
            if st in TRANSITORIOS:
                transitorios += 1
                if transitorios > 3:
                    return False, f"HTTP {st} persistente"
                espera = limiter.backoff_transitorio(transitorios, lambda: _parada(job_id, jobs_db))
                print(f"[PREFLIGHT] PNCP HTTP {st}; backoff de {espera:.0f} s.", flush=True)
                continue
            return False, f"{type(exc).__name__}: {exc}"
        except (requests.Timeout, requests.ConnectionError) as exc:
            transitorios += 1
            if transitorios > 3:
                return False, f"{type(exc).__name__} persistente: {exc}"
            espera = limiter.backoff_transitorio(transitorios, lambda: _parada(job_id, jobs_db))
            print(f"[PREFLIGHT] PNCP {type(exc).__name__}; backoff de {espera:.0f} s.", flush=True)
        except Exception as exc:
            return False, f"{type(exc).__name__}: {exc}"


def preflight_compras_contratacoes(sessao: requests.Session, dia: str) -> tuple[bool, str]:
    return _resposta_detalhada(
        sessao,
        URL_COMPRAS,
        {
            "dataPublicacaoPncpInicial": dia,
            "dataPublicacaoPncpFinal": dia,
            "codigoModalidade": 6,
            "pagina": 1,
            "tamanhoPagina": PREFLIGHT_TAMANHO_PAGINA,
        },
    )


def preflight_compras_resultados(sessao: requests.Session, dia: str) -> tuple[bool, str]:
    return _resposta_detalhada(
        sessao,
        URL_RESULTADOS,
        {
            "dataResultadoPncpInicial": dia,
            "dataResultadoPncpFinal": dia,
            "pagina": 1,
            "tamanhoPagina": PREFLIGHT_TAMANHO_PAGINA,
        },
    )


def consultar_modalidade_adaptativa(
    sessao: requests.Session,
    limiter: Any,
    data_ref: str,
    modalidade_id: int,
    modalidade_nome: str,
    job_id: int | None,
    jobs_db: Path | None,
) -> list[dict[str, Any]]:
    """Consulta uma modalidade página a página, sem perder páginas por HTTP 429.

    O retry ocorre na MESMA página. Assim, um 429 na página 8 não obriga a
    baixar novamente as páginas 1..7 nem provoca rajadas entre modalidades.
    """
    url = f"{BASE_CONSULTA}/contratacoes/publicacao"
    todos: list[dict[str, Any]] = []
    pagina = 1
    transitorios_pagina = 0

    while True:
        if _parada(job_id, jobs_db):
            raise InterruptedError
        params = {
            "dataInicial": data_ref,
            "dataFinal": data_ref,
            "codigoModalidadeContratacao": modalidade_id,
            "pagina": pagina,
            "tamanhoPagina": TAMANHO_PAGINA_CONTRATACOES,
        }
        try:
            payload = pncp_get_json(sessao, url, params=params, timeout=120)
            transitorios_pagina = 0
        except InterruptedError:
            raise
        except requests.HTTPError as exc:
            st = _status_http(exc)
            if st == 429:
                print(
                    f"    HTTP 429 · {modalidade_nome} · página {pagina}. Entrando em cooldown...",
                    flush=True,
                )
                espera, parar = limiter.cooldown_429(getattr(exc, "response", None), lambda: _parada(job_id, jobs_db))
                print(
                    f"    Cooldown concluído após {espera:.0f} s; ritmo atual "
                    f"{limiter.intervalo:.2f} s/requisição. Retentando a mesma página.",
                    flush=True,
                )
                if parar:
                    raise RateLimitPersistente(
                        f"HTTP 429 persistente em {modalidade_nome}, página {pagina}"
                    ) from exc
                continue
            if st in TRANSITORIOS:
                transitorios_pagina += 1
                if transitorios_pagina > 3:
                    raise
                espera = limiter.backoff_transitorio(transitorios_pagina, lambda: _parada(job_id, jobs_db))
                print(
                    f"    HTTP {st} · {modalidade_nome} · página {pagina}; backoff de {espera:.0f} s.",
                    flush=True,
                )
                continue
            raise
        except (requests.Timeout, requests.ConnectionError) as exc:
            transitorios_pagina += 1
            if transitorios_pagina > 3:
                raise
            espera = limiter.backoff_transitorio(transitorios_pagina, lambda: _parada(job_id, jobs_db))
            print(
                f"    {type(exc).__name__} · {modalidade_nome} · página {pagina}; "
                f"backoff de {espera:.0f} s.",
                flush=True,
            )
            continue

        registros = extrair_lista(payload, "data")
        if not registros:
            break
        todos.extend(registros)

        total_paginas = payload.get("totalPaginas") if isinstance(payload, dict) else None
        restantes = payload.get("paginasRestantes") if isinstance(payload, dict) else None
        if total_paginas is not None:
            if pagina >= int(total_paginas):
                break
        elif restantes is not None:
            if int(restantes) <= 0:
                break
        elif len(registros) < TAMANHO_PAGINA_CONTRATACOES:
            break
        pagina += 1

    return todos


def coletar_pncp_dia(
    con: sqlite3.Connection,
    sessao: requests.Session,
    limiter: Any,
    dia: str,
    job_id: int | None,
    jobs_db: Path | None,
    dia_indice: int,
    dias_total: int,
) -> dict[str, int]:
    data_ref = dia.replace("-", "")
    novos = 0
    atualizados = 0
    encontrados = 0
    erros = 0
    puladas = 0
    concluidas_nesta_execucao = 0
    rate_limit_persistente = 0

    for pos, (modalidade_id, modalidade_nome) in enumerate(MODALIDADES.items(), 1):
        etapa = f"PNCP Consulta · {dia} · {modalidade_nome}"
        _atualizar(
            job_id,
            jobs_db,
            etapa,
            dia_indice - 1,
            dias_total,
            f"Dia {dia_indice}/{dias_total} · modalidade {pos}/{len(MODALIDADES)} · "
            f"ritmo {limiter.intervalo:.2f} s/req",
        )

        if modalidade_ja_concluida(con, data_ref, modalidade_id):
            puladas += 1
            continue

        try:
            registros = consultar_modalidade_adaptativa(
                sessao,
                limiter,
                data_ref,
                modalidade_id,
                modalidade_nome,
                job_id,
                jobs_db,
            )
            encontrados += len(registros)
            for registro in registros:
                if salvar_contratacao(con, registro):
                    novos += 1
                else:
                    atualizados += 1
            con.commit()
            paginas = int(math.ceil(len(registros) / TAMANHO_PAGINA_CONTRATACOES)) if registros else 0
            marcar_modalidade(
                con,
                data_ref,
                modalidade_id,
                modalidade_nome,
                "CONCLUIDA",
                total_registros=len(registros),
                ultima_pagina=paginas,
                erro=None,
            )
            concluidas_nesta_execucao += 1
        except InterruptedError:
            raise
        except RateLimitPersistente as exc:
            erros += 1
            rate_limit_persistente = 1
            msg = f"{type(exc).__name__}: {exc}"
            print(f"    AVISO PNCP · {modalidade_nome}: {msg}", flush=True)
            marcar_modalidade(
                con,
                data_ref,
                modalidade_id,
                modalidade_nome,
                "AGUARDANDO_RATE_LIMIT",
                total_registros=0,
                ultima_pagina=0,
                erro=msg[:1800],
            )
            con.commit()
            print(
                "    Rate limit persistente: as demais modalidades PNCP deste ciclo NÃO serão tentadas. "
                "Checkpoint preservado para retomada.",
                flush=True,
            )
            break
        except Exception as exc:
            erros += 1
            transitorio = eh_erro_transitorio(exc)
            estado = "ERRO_TRANSITORIO" if transitorio else "ERRO"
            msg = f"{type(exc).__name__}: {exc}"
            print(f"    AVISO PNCP · {modalidade_nome}: {msg}", flush=True)
            try:
                marcar_modalidade(
                    con,
                    data_ref,
                    modalidade_id,
                    modalidade_nome,
                    estado,
                    total_registros=0,
                    ultima_pagina=0,
                    erro=msg[:1800],
                )
                con.commit()
            except Exception:
                con.rollback()

    # Se o ciclo foi interrompido por rate limit, há modalidades não tentadas.
    # Elas continuam naturalmente pendentes no checkpoint; contabilizamos isso
    # no resumo para não dar a impressão de que sobrou apenas a modalidade que 429ou.
    if rate_limit_persistente:
        incompletas = max(1, len(MODALIDADES) - puladas - concluidas_nesta_execucao)
        erros = max(erros, incompletas)

    return {
        "novos": novos,
        "atualizados": atualizados,
        "encontrados": encontrados,
        "erros": erros,
        "modalidades_puladas": puladas,
        "modalidades_concluidas": concluidas_nesta_execucao,
        "rate_limit_persistente": rate_limit_persistente,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="COLETA incremental multifuente — V0.7.6")
    ap.add_argument("--inicio", required=True)
    ap.add_argument("--fim", required=True)
    ap.add_argument("--db", type=Path, default=DB_COMPRAS)
    ap.add_argument("--jobs-db", type=Path, default=DB_JOBS)
    ap.add_argument("--job-id", type=int)
    args = ap.parse_args()

    faixa = datas(args.inicio, args.fim)
    jobs_db = args.jobs_db if args.job_id is not None else None

    print("=" * 92, flush=True)
    print("Radar PNCP / Radar PNCP V0.7.6 — COLETA INCREMENTAL MULTIFONTE", flush=True)
    print("=" * 92, flush=True)
    print(f"Período: {args.inicio} → {args.fim} · {len(faixa)} dia(s)", flush=True)
    print("Fonte primária: PNCP Consulta (contratações).", flush=True)
    print("Fontes complementares: Compras.gov API (contratações e resultados), quando disponíveis.", flush=True)
    print("Itens detalhados não são consultados por este job.", flush=True)
    print("PNCP: ritmo adaptativo inicial de 1 requisição a cada 1.25 s; HTTP 429 ativa cooldown global.", flush=True)

    con = conectar_pncp(args.db)
    criar_banco(con)
    preparar_controle_modalidades(con)
    garantir_resultados(con)
    sp, limiter = criar_sessao_controlada(1.25)
    sp.set_parada_callback(lambda: _parada(args.job_id, jobs_db))
    sc = sessao_compras()

    totais = {
        "pncp_encontradas": 0,
        "pncp_novas": 0,
        "pncp_atualizadas": 0,
        "pncp_erros": 0,
        "compras_encontradas": 0,
        "compras_erros": 0,
        "resultados": 0,
        "resultados_quarentena": 0,
        "resultados_erros": 0,
    }
    houve_alteracao = False
    pncp_parcial = False
    complementares_pendentes = False
    interrompido_por_rate = False
    dias_processados = 0

    try:
        print("\n[PREFLIGHT] PNCP Consulta...", flush=True)
        ok_pncp, det_pncp = preflight_pncp(sp, limiter, faixa[0], args.job_id, jobs_db)
        print(f"[PREFLIGHT] PNCP Consulta: {'OK' if ok_pncp else 'FALHA'} · {det_pncp}", flush=True)
        if not ok_pncp:
            print("Fonte primária indisponível/limitada. Nenhum dia será iniciado; tente novamente mais tarde.", flush=True)
            _atualizar(args.job_id, jobs_db, "PNCP Consulta indisponível", 0, len(faixa))
            return EXIT_PARCIAL

        print("[PREFLIGHT] Compras.gov · contratações...", flush=True)
        compras_contr_ok, compras_contr_det = preflight_compras_contratacoes(sc, faixa[0])
        print(f"[PREFLIGHT] Compras.gov · contratações: {'OK' if compras_contr_ok else 'INDISPONÍVEL'} · {compras_contr_det}", flush=True)
        if not compras_contr_ok:
            complementares_pendentes = True
            print("[PREFLIGHT] A COLETA continuará pelo PNCP; esta fonte complementar será ignorada neste job.", flush=True)

        print("[PREFLIGHT] Compras.gov · resultados...", flush=True)
        compras_res_ok, compras_res_det = preflight_compras_resultados(sc, faixa[0])
        print(f"[PREFLIGHT] Compras.gov · resultados: {'OK' if compras_res_ok else 'INDISPONÍVEL'} · {compras_res_det}", flush=True)
        if not compras_res_ok:
            complementares_pendentes = True
            print("[PREFLIGHT] A COLETA continuará pelo PNCP; resultados serão retomados quando o serviço voltar.", flush=True)

        for idx, dia in enumerate(faixa, 1):
            if _parada(args.job_id, jobs_db):
                raise InterruptedError
            if args.job_id is not None and jobs_db is not None:
                jm.atualizar_job(args.job_id, jobs_db, data_atual=dia)
                jm.atualizar_job_dia(
                    args.job_id,
                    dia,
                    jobs_db,
                    status="EXECUTANDO",
                    iniciado_em=jm.agora(),
                    mensagem="Coletando fontes da COLETA.",
                )

            print(f"\n{'=' * 92}", flush=True)
            print(f"[{dia}] COLETA multifuente · dia {idx}/{len(faixa)}", flush=True)
            print("-" * 92, flush=True)
            print(f"[PNCP Consulta] Contratações · ritmo atual {limiter.intervalo:.2f} s/requisição", flush=True)
            rp = coletar_pncp_dia(con, sp, limiter, dia, args.job_id, jobs_db, idx, len(faixa))
            totais["pncp_encontradas"] += rp["encontrados"]
            totais["pncp_novas"] += rp["novos"]
            totais["pncp_atualizadas"] += rp["atualizados"]
            totais["pncp_erros"] += rp["erros"]
            if rp["novos"] or rp["atualizados"]:
                houve_alteracao = True
            if rp["erros"]:
                pncp_parcial = True
            if rp["rate_limit_persistente"]:
                interrompido_por_rate = True
            print(
                f"    Encontradas: {rp['encontrados']:,} · novas: {rp['novos']:,} · "
                f"atualizadas: {rp['atualizados']:,} · modalidades pendentes/erro: {rp['erros']:,} · "
                f"já concluídas/puladas: {rp['modalidades_puladas']:,} · "
                f"ritmo final: {limiter.intervalo:.2f} s/req",
                flush=True,
            )

            if compras_contr_ok:
                print("[Compras.gov] Contratações complementares", flush=True)
                rc = buscar_compras_dia(con, sc, dia, False, args.job_id, jobs_db)
                totais["compras_encontradas"] += int(rc.get("contratacoes") or 0)
                totais["compras_erros"] += int(rc.get("erros_modalidades") or 0)
                if int(rc.get("contratacoes") or 0):
                    houve_alteracao = True
                if int(rc.get("erros_modalidades") or 0):
                    complementares_pendentes = True
                print(
                    f"    Contratações distintas: {int(rc.get('contratacoes') or 0):,} · "
                    f"falhas de modalidade: {int(rc.get('erros_modalidades') or 0):,}",
                    flush=True,
                )

            if compras_res_ok:
                print("[Compras.gov] Resultados homologados", flush=True)
                rr, rq, re = buscar_resultados_dia(con, sc, dia)
                totais["resultados"] += rr
                totais["resultados_quarentena"] += rq
                totais["resultados_erros"] += re
                if rr:
                    houve_alteracao = True
                if re:
                    complementares_pendentes = True
                print(f"    Resultados aplicados: {rr:,} · quarentena: {rq:,} · falhas: {re:,}", flush=True)

            if rp["erros"] == 0:
                registrar_cobertura(con, "coleta_pncp_ultimo_dia_completo", dia)
            if compras_contr_ok and int(totais["compras_erros"]) == 0:
                registrar_cobertura(con, "coleta_compras_contratacoes_ultimo_dia_tentado", dia)
            if compras_res_ok and int(totais["resultados_erros"]) == 0:
                registrar_cobertura(con, "coleta_compras_resultados_ultimo_dia_tentado", dia)

            estado_dia = "PARCIAL" if rp["erros"] else "CONCLUIDA"
            if args.job_id is not None and jobs_db is not None:
                jm.atualizar_job_dia(
                    args.job_id,
                    dia,
                    jobs_db,
                    status=estado_dia,
                    finalizado_em=jm.agora(),
                    exit_code=(EXIT_PARCIAL if rp["erros"] else 0),
                    mensagem=(
                        f"PNCP: {rp['encontrados']} encontradas; {rp['erros']} modalidade(s) pendente(s)/erro. "
                        f"Compras.gov contratos: {'ativo' if compras_contr_ok else 'indisponível'}; "
                        f"resultados: {'ativo' if compras_res_ok else 'indisponível'}."
                    ),
                )
            dias_processados = idx
            _atualizar(args.job_id, jobs_db, f"COLETA multifuente · {dia} concluído", idx, len(faixa))

            if interrompido_por_rate:
                print(
                    "\n[PNCP] Rate limit persistente detectado. Encerrando a janela neste ponto para não "
                    "pressionar a API; dias/modalidades restantes ficam para a próxima retomada.",
                    flush=True,
                )
                break

        con.commit()
        con.close()
        con = None

        if houve_alteracao:
            print("\n[FINAL] Atualizando FTS5/TIC de forma incremental...", flush=True)
            atualizar_auxiliares(args.db, args.job_id, jobs_db)
            if pncp_parcial:
                print(
                    "[FINAL] Snapshot completo ADIADO: a janela PNCP ainda está parcial. "
                    "O snapshot atual permanece válido como última fotografia estável e será reconstruído "
                    "quando a retomada concluir a janela.",
                    flush=True,
                )
            else:
                print("[FINAL] Atualizando snapshot da interface...", flush=True)
                snap = rs.atualizar_automaticamente(args.db, DB_STATS)
                if snap is not None:
                    print(
                        f"[FINAL] Snapshot: {snap['meta']['total_contratacoes']:,} contratações · "
                        f"{snap['meta']['total_itens']:,} itens físicos.",
                        flush=True,
                    )
        else:
            print("\n[FINAL] Nenhum registro foi alterado; FTS/TIC/snapshot não precisam ser reconstruídos.", flush=True)

        print("\n" + "=" * 92, flush=True)
        print("RESUMO · COLETA INCREMENTAL MULTIFONTE", flush=True)
        print("=" * 92, flush=True)
        print(f"Dias percorridos nesta execução:         {dias_processados:,}/{len(faixa):,}", flush=True)
        print(f"PNCP · contratações encontradas:       {totais['pncp_encontradas']:,}", flush=True)
        print(f"PNCP · novas:                          {totais['pncp_novas']:,}", flush=True)
        print(f"PNCP · atualizadas:                    {totais['pncp_atualizadas']:,}", flush=True)
        print(f"PNCP · modalidades pendentes/erro:     {totais['pncp_erros']:,}", flush=True)
        print(f"Compras.gov · contratações distintas:  {totais['compras_encontradas']:,}", flush=True)
        print(f"Compras.gov · falhas de modalidade:    {totais['compras_erros']:,}", flush=True)
        print(f"Compras.gov · resultados aplicados:    {totais['resultados']:,}", flush=True)
        print(f"Compras.gov · resultados quarentena:   {totais['resultados_quarentena']:,}", flush=True)
        print(f"Compras.gov · falhas de resultados:    {totais['resultados_erros']:,}", flush=True)
        print(f"PNCP · ritmo final:                     {limiter.intervalo:.2f} s/requisição", flush=True)
        print(f"PNCP · parada por rate limit:            {'SIM' if interrompido_por_rate else 'NÃO'}", flush=True)
        print(f"Fonte Compras.gov contratações:         {'ATIVA' if compras_contr_ok else 'INDISPONÍVEL'}", flush=True)
        print(f"Fonte Compras.gov resultados:           {'ATIVA' if compras_res_ok else 'INDISPONÍVEL'}", flush=True)

        if pncp_parcial:
            print("Resultado: PARCIAL — checkpoint preservado; reexecute a mesma janela para retomar somente modalidades/dias ainda pendentes.", flush=True)
            return EXIT_PARCIAL
        if complementares_pendentes:
            print("Resultado: CONTRATAÇÕES PNCP CONCLUÍDAS COM PENDÊNCIAS EXTERNAS — fontes complementares poderão ser retomadas depois.", flush=True)
            return EXIT_COMPLEMENTAR_INDISPONIVEL
        print("Resultado: CONCLUÍDO — fontes previstas responderam sem pendências.", flush=True)
        return 0

    except InterruptedError:
        return 130
    except KeyboardInterrupt:
        return 130
    finally:
        try:
            sp.close()
        except Exception:
            pass
        try:
            sc.close()
        except Exception:
            pass
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
