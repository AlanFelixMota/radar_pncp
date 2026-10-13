#!/usr/bin/env python3
"""COLETA operacional — Radar PNCP / Radar PNCP V0.7.6.

Modos:
- diario: janela móvel curta (3 dias por padrão);
- revisao: janela móvel ampla (30 dias por padrão), pensada para execução semanal;
- retomada: repete a janela de um job parcial usando checkpoints por modalidade/página.

A fonte primária é o PNCP /contratacoes/atualizacao. O processamento é página a
página: cada página aplicada é commitada e o próximo número de página é salvo no
radar_jobs.db. Assim uma falha na página 902 não obriga a retransmitir as 901
páginas anteriores na retomada.
"""
from __future__ import annotations

import argparse
import pesquisa_atualizacao as pa
import sqlite3
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import requests

import job_manager as jm
import radar_stats as rs
from comprasgov_api_incremental import buscar_compras_dia, sessao as sessao_compras
from comprasgov_resultados import garantir_tabelas as garantir_resultados
from comprasgov_resultados_sync import (
    sincronizar_api as sincronizar_resultados_api, verificar_reconciliar_csv,
    data_max_local as resultados_data_max_local, classificar_frescor as classificar_frescor_resultados,
)
from coletar_pncp import (
    BASE_CONSULTA,
    MODALIDADES,
    TAMANHO_PAGINA_CONTRATACOES,
    conectar as conectar_pncp,
    criar_banco,
    extrair_lista,
    get_json as pncp_get_json,
    salvar_contratacao,
)
from importar_comprasgov_csv import atualizar_auxiliares
from pncp_health import eh_erro_transitorio
from pncp_rate import criar_sessao_controlada
from proveniencia import FONTE_COMPRASGOV_API, registrar_cobertura, obter_cobertura
from radar_paths import DB_COMPRAS, DB_JOBS, DB_STATS
from itens_incremental import enfileirar_ids, enfileirar_recentes_pendentes, processar_fila, resumo_fila as resumo_fila_itens

EXIT_PARCIAL = 75
EXIT_COMPLEMENTAR_INDISPONIVEL = 76
PREFLIGHT_TAMANHO_PAGINA = 10
TRANSITORIOS = {408, 425, 500, 502, 503, 504}
URL_ATUALIZACAO = f"{BASE_CONSULTA}/contratacoes/atualizacao"
HEARTBEAT_PAGINAS = 25
HEARTBEAT_SEGUNDOS = 90
PROGRESSO_POR_ETAPA = 1000
PROGRESSO_TOTAL = 16 * PROGRESSO_POR_ETAPA  # 14 modalidades + 2 complementos


class RateLimitPersistente(RuntimeError):
    pass


def janela(data_fim: str | None, dias: int) -> tuple[str, str]:
    if dias < 1 or dias > 60:
        raise ValueError("A janela da COLETA deve ter entre 1 e 60 dias.")
    fim = date.fromisoformat(data_fim) if data_fim else date.today()
    inicio = fim - timedelta(days=dias - 1)
    return inicio.isoformat(), fim.isoformat()


def _parada(job_id: int | None, jobs_db: Path | None) -> bool:
    return bool(job_id is not None and jobs_db is not None and jm.parada_solicitada(job_id, jobs_db))


def _atualizar(job_id: int | None, jobs_db: Path | None, etapa: str, atual: int, total: int, msg: str | None = None) -> None:
    if job_id is None or jobs_db is None:
        return
    jm.atualizar_job(
        job_id, jobs_db,
        etapa=etapa, progresso_atual=int(atual), progresso_total=int(total),
        mensagem=msg or etapa,
    )


def _atualizar_modalidade(job_id: int | None, jobs_db: Path | None, pos: int, nome: str,
                          pagina: int, total_paginas: int | None, recebidos: int,
                          inicio: str, fim: str) -> None:
    base = (pos - 1) * PROGRESSO_POR_ETAPA
    if total_paginas and total_paginas > 0:
        frac = min(999, int(min(pagina, total_paginas) / total_paginas * 1000))
        texto_pag = f"página {pagina:,}/{total_paginas:,}".replace(",", ".")
    else:
        frac = min(950, max(1, pagina * 5))
        texto_pag = f"página {pagina:,}".replace(",", ".")
    _atualizar(
        job_id, jobs_db, f"COLETA · PNCP · {nome}", base + frac, PROGRESSO_TOTAL,
        f"Modalidade {pos}/14 · {texto_pag} · {recebidos:,} registros nesta execução · janela {inicio} → {fim}".replace(",", "."),
    )


def _status_http(exc: BaseException) -> int | None:
    resp = getattr(exc, "response", None)
    try:
        return int(resp.status_code) if resp is not None else None
    except Exception:
        return None


def _corpo_http(exc: BaseException, limite: int = 700) -> str:
    resp = getattr(exc, "response", None)
    if resp is None:
        return ""
    try:
        return (resp.text or "").strip().replace("\r", " ").replace("\n", " ")[:limite]
    except Exception:
        return ""


def _erro_backend_incoerente(exc: requests.HTTPError, inicio: str, fim: str) -> bool:
    """Reconhece respostas 400/422 que o PNCP já emitiu por falha interna.

    Só trata como transitório mensagens conhecidas e quando a janela enviada é
    objetivamente válida. Erros reais de parâmetro continuam fatais.
    """
    st = _status_http(exc)
    if st not in (400, 422):
        return False
    dias = (date.fromisoformat(fim) - date.fromisoformat(inicio)).days + 1
    corpo = _corpo_http(exc).lower()
    if dias <= 365 and "período inicial e final maior que 365 dias" in corpo:
        return True
    if "for input string" in corpo:
        return True
    return False


def preflight_atualizacao(sessao: requests.Session, limiter: Any, inicio: str, fim: str,
                          job_id: int | None, jobs_db: Path | None) -> tuple[bool, str]:
    params = {
        "dataInicial": inicio.replace("-", ""),
        "dataFinal": fim.replace("-", ""),
        "codigoModalidadeContratacao": 6,
        "pagina": 1,
        "tamanhoPagina": PREFLIGHT_TAMANHO_PAGINA,
    }
    transitorios = 0
    while True:
        try:
            pncp_get_json(sessao, URL_ATUALIZACAO, params=params, timeout=30)
            return True, f"HTTP 200/204 · ritmo {limiter.intervalo:.2f} s/requisição"
        except InterruptedError:
            raise
        except requests.HTTPError as exc:
            st = _status_http(exc)
            if st == 429:
                espera, parar = limiter.cooldown_429(getattr(exc, "response", None), lambda: _parada(job_id, jobs_db))
                print(f"[PREFLIGHT] PNCP atualização 429; cooldown {espera:.0f} s · ritmo {limiter.intervalo:.2f} s/req.", flush=True)
                if parar:
                    return False, "HTTP 429 persistente"
                continue
            if st in TRANSITORIOS or _erro_backend_incoerente(exc, inicio, fim):
                transitorios += 1
                if transitorios > 3:
                    return False, f"HTTP {st} persistente"
                espera = limiter.backoff_transitorio(transitorios, lambda: _parada(job_id, jobs_db))
                print(f"[PREFLIGHT] PNCP atualização HTTP {st}; backoff {espera:.0f} s.", flush=True)
                continue
            corpo = _corpo_http(exc)
            return False, f"HTTP {st or '?'}" + (f" · resposta: {corpo}" if corpo else "")
        except (requests.Timeout, requests.ConnectionError) as exc:
            transitorios += 1
            if transitorios > 3:
                return False, f"{type(exc).__name__} persistente: {exc}"
            espera = limiter.backoff_transitorio(transitorios, lambda: _parada(job_id, jobs_db))
            print(f"[PREFLIGHT] PNCP atualização {type(exc).__name__}; backoff {espera:.0f} s.", flush=True)
        except Exception as exc:
            return False, f"{type(exc).__name__}: {exc}"


def aplicar_se_mudou(con: sqlite3.Connection, registro: dict[str, Any]) -> tuple[str, str | None, bool]:
    """Classifica a mudança sem confundir pai e dependentes."""
    pid = registro.get("numeroControlePNCP")
    if not pid:
        return "INALTERADO", None, False
    atual = con.execute(
        "SELECT data_atualizacao, data_atualizacao_global FROM contratacoes WHERE numero_controle_pncp=?",
        (str(pid),),
    ).fetchone()
    if atual is None:
        salvar_contratacao(con, registro)
        return "NOVO", str(pid), False

    local_pai = atual[0] if not isinstance(atual, sqlite3.Row) else atual["data_atualizacao"]
    local_global = atual[1] if not isinstance(atual, sqlite3.Row) else atual["data_atualizacao_global"]
    remoto_pai = registro.get("dataAtualizacao")
    remoto_global = registro.get("dataAtualizacaoGlobal")
    global_mudou = remoto_global is not None and str(remoto_global) != str(local_global or "")

    if remoto_pai is not None and str(remoto_pai) != str(local_pai or ""):
        salvar_contratacao(con, registro)
        return "ATUALIZADO", str(pid), global_mudou

    if global_mudou:
        import json
        con.execute(
            "UPDATE contratacoes SET data_atualizacao_global=?, raw_json=?, ultima_coleta_em=? WHERE numero_controle_pncp=?",
            (str(remoto_global), json.dumps(registro, ensure_ascii=False), datetime.now().isoformat(timespec="seconds"), str(pid)),
        )
        return "DEPENDENCIAS_ATUALIZADAS", str(pid), True

    return "INALTERADO", str(pid), False


def processar_modalidade_atualizacao(
    con: sqlite3.Connection,
    sessao: requests.Session,
    limiter: Any,
    inicio: str,
    fim: str,
    modalidade_id: int,
    modalidade_nome: str,
    pos: int,
    job_id: int | None,
    jobs_db: Path | None,
    checkpoint_con: sqlite3.Connection | None,
    totais_global: dict[str, int],
    ids_novos_global: set[str],
    ids_atualizados_global: set[str],
    ids_global_mudou_global: set[str],
) -> dict[str, Any]:
    checkpoint = None
    if jobs_db is not None:
        checkpoint = jm.obter_checkpoint_coleta(inicio, fim, modalidade_id, jobs_db, con=checkpoint_con)
    if checkpoint and checkpoint.get("status") == "CONCLUIDA":
        _atualizar(job_id, jobs_db, f"COLETA · PNCP · {modalidade_nome}", pos * PROGRESSO_POR_ETAPA, PROGRESSO_TOTAL,
                  f"Modalidade {pos}/14 já concluída nesta janela; pulando.")
        print(f"    {modalidade_nome}: checkpoint CONCLUÍDA · pulando.", flush=True)
        return {"pulada": True, "recebidas": 0, "novas": 0, "atualizadas": 0, "dependencias": 0, "inalteradas": 0,
                "ids_novos": set(), "ids_atualizados": set()}

    pagina = max(1, int((checkpoint or {}).get("proxima_pagina") or 1))
    if pagina > 1:
        print(f"    {modalidade_nome}: retomando do checkpoint na página {pagina:,}.".replace(",", "."), flush=True)

    recebidas = novas = atualizadas = dependencias = inalteradas = 0
    ids_novos: set[str] = set()
    ids_atualizados: set[str] = set()
    transitorios = 0
    total_paginas: int | None = None
    ultimo_heartbeat = time.monotonic()
    ultima_pagina_heartbeat = pagina - 1
    data_ini = inicio.replace("-", "")
    data_fim = fim.replace("-", "")

    while True:
        if _parada(job_id, jobs_db):
            raise InterruptedError
        params = {
            "dataInicial": data_ini,
            "dataFinal": data_fim,
            "codigoModalidadeContratacao": modalidade_id,
            "pagina": pagina,
            "tamanhoPagina": TAMANHO_PAGINA_CONTRATACOES,
        }
        try:
            payload = pncp_get_json(sessao, URL_ATUALIZACAO, params=params, timeout=120)
            transitorios = 0
        except InterruptedError:
            raise
        except requests.HTTPError as exc:
            st = _status_http(exc)
            if st == 429:
                print(f"    HTTP 429 · {modalidade_nome} · página {pagina}. Entrando em cooldown...", flush=True)
                espera, parar = limiter.cooldown_429(getattr(exc, "response", None), lambda: _parada(job_id, jobs_db))
                print(f"    Cooldown concluído após {espera:.0f} s; ritmo {limiter.intervalo:.2f} s/req. Retentando a mesma página.", flush=True)
                if parar:
                    if jobs_db is not None:
                        jm.salvar_checkpoint_coleta(inicio, fim, modalidade_id, modalidade_nome, "ERRO", pagina, total_paginas,
                                                  f"HTTP 429 persistente na página {pagina}", jobs_db, con=checkpoint_con)
                    raise RateLimitPersistente(f"429 persistente em {modalidade_nome}, página {pagina}") from exc
                continue
            if st in TRANSITORIOS or _erro_backend_incoerente(exc, inicio, fim):
                transitorios += 1
                if transitorios > 3:
                    if jobs_db is not None:
                        jm.salvar_checkpoint_coleta(inicio, fim, modalidade_id, modalidade_nome, "ERRO", pagina, total_paginas,
                                                  f"HTTP {st}: {_corpo_http(exc)}", jobs_db, con=checkpoint_con)
                    raise
                espera = limiter.backoff_transitorio(transitorios, lambda: _parada(job_id, jobs_db))
                detalhe = " (resposta incoerente do backend tratada como transitória)" if st in (400, 422) else ""
                print(f"    HTTP {st} · {modalidade_nome} · página {pagina}; backoff {espera:.0f} s.{detalhe}", flush=True)
                continue
            if jobs_db is not None:
                jm.salvar_checkpoint_coleta(inicio, fim, modalidade_id, modalidade_nome, "ERRO", pagina, total_paginas,
                                          f"HTTP {st}: {_corpo_http(exc)}", jobs_db, con=checkpoint_con)
            raise
        except (requests.Timeout, requests.ConnectionError) as exc:
            transitorios += 1
            if transitorios > 3:
                if jobs_db is not None:
                    jm.salvar_checkpoint_coleta(inicio, fim, modalidade_id, modalidade_nome, "ERRO", pagina, total_paginas,
                                              f"{type(exc).__name__}: {exc}", jobs_db, con=checkpoint_con)
                raise
            espera = limiter.backoff_transitorio(transitorios, lambda: _parada(job_id, jobs_db))
            print(f"    {type(exc).__name__} · {modalidade_nome} · página {pagina}; backoff {espera:.0f} s.", flush=True)
            continue

        registros = extrair_lista(payload, "data")
        if isinstance(payload, dict) and payload.get("totalPaginas") is not None:
            try:
                total_paginas = int(payload.get("totalPaginas") or 0) or None
            except Exception:
                total_paginas = None

        if not registros:
            if jobs_db is not None:
                jm.salvar_checkpoint_coleta(inicio, fim, modalidade_id, modalidade_nome, "CONCLUIDA", pagina, total_paginas, None, jobs_db, con=checkpoint_con)
            break

        for reg in registros:
            estado, pid, global_mudou = aplicar_se_mudou(con, reg)
            pa.registrar(con, inicio, fim, [pid], estado == 'ATUALIZADO', global_mudou)
            if global_mudou and pid:
                ids_global_mudou_global.add(pid)
            if estado == "NOVO":
                novas += 1; totais_global["novos"] += 1
                if pid:
                    ids_novos.add(pid); ids_novos_global.add(pid)
            elif estado == "ATUALIZADO":
                atualizadas += 1; totais_global["atualizados"] += 1
                if pid:
                    ids_atualizados.add(pid); ids_atualizados_global.add(pid)
            elif estado == "DEPENDENCIAS_ATUALIZADAS":
                dependencias += 1; totais_global["dependencias"] += 1
            else:
                inalteradas += 1; totais_global["inalterados"] += 1
        recebidas += len(registros)
        con.commit()

        final = False
        restantes = payload.get("paginasRestantes") if isinstance(payload, dict) else None
        if total_paginas is not None:
            final = pagina >= total_paginas
        elif restantes is not None:
            try:
                final = int(restantes) <= 0
            except Exception:
                final = False
        elif len(registros) < TAMANHO_PAGINA_CONTRATACOES:
            final = True

        proxima = pagina + 1
        if jobs_db is not None:
            jm.salvar_checkpoint_coleta(
                inicio, fim, modalidade_id, modalidade_nome,
                "CONCLUIDA" if final else "EM_ANDAMENTO",
                proxima if not final else pagina,
                total_paginas, None, jobs_db, con=checkpoint_con,
            )

        if pagina == 1 or pagina % 5 == 0 or final:
            _atualizar_modalidade(job_id, jobs_db, pos, modalidade_nome, pagina, total_paginas, recebidas, inicio, fim)
        agora_mono = time.monotonic()
        if (pagina - ultima_pagina_heartbeat >= HEARTBEAT_PAGINAS or
                agora_mono - ultimo_heartbeat >= HEARTBEAT_SEGUNDOS or final):
            total_txt = f"/{total_paginas:,}".replace(",", ".") if total_paginas else ""
            print(
                f"    ↳ {modalidade_nome}: página {pagina:,}{total_txt} · {recebidas:,} registros nesta execução · "
                f"ritmo {limiter.intervalo:.2f} s/req".replace(",", "."),
                flush=True,
            )
            ultimo_heartbeat = agora_mono
            ultima_pagina_heartbeat = pagina

        if final:
            break
        pagina = proxima

    return {
        "pulada": False,
        "recebidas": recebidas,
        "novas": novas,
        "atualizadas": atualizadas,
        "dependencias": dependencias,
        "inalteradas": inalteradas,
        "ids_novos": ids_novos,
        "ids_atualizados": ids_atualizados,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="COLETA operacional — V0.7.6")
    ap.add_argument("--dias", type=int, default=3)
    ap.add_argument("--fim", help="Data de referência AAAA-MM-DD; padrão = hoje")
    ap.add_argument("--modo", choices=("diario", "revisao", "retomada"), default="diario")
    ap.add_argument("--db", type=Path, default=DB_COMPRAS)
    ap.add_argument("--jobs-db", type=Path, default=DB_JOBS)
    ap.add_argument("--job-id", type=int)
    args = ap.parse_args()
    inicio, fim = janela(args.fim, args.dias)
    jobs_db = args.jobs_db if args.job_id is not None else None

    rotulo = "COLETA DIÁRIA" if args.modo == "diario" else "COLETA REVISÃO SEMANAL" if args.modo == "revisao" else "COLETA RETOMADA"
    print("=" * 92, flush=True)
    print(f"Radar PNCP / Radar PNCP V0.7.6 — {rotulo}", flush=True)
    print("=" * 92, flush=True)
    print(f"Janela móvel: {inicio} → {fim} · {args.dias} dia(s)", flush=True)
    print("PNCP primário: contratações por DATA DE ATUALIZAÇÃO.", flush=True)
    print("Checkpoints: modalidade + página; uma retomada não retransmite páginas já aplicadas.", flush=True)
    print(
        "Compras.gov: publicações do dia final + RESULTADOS com fronteira própria. "
        "Na diária, a sincronização é leve (entorno da fronteira + dias recentes); na revisão, percorre toda a lacuna.",
        flush=True,
    )
    if args.modo == "revisao":
        print("Revisão semanal: também verifica o snapshot CSV anual de resultados e reconcilia somente linhas ausentes quando houver atualização.", flush=True)
    print("Itens recentes: fila incremental PNCP ativa; páginas são checkpointadas e só o conjunto integral certifica completude.", flush=True)

    con = conectar_pncp(args.db)
    criar_banco(con)
    garantir_resultados(con)
    checkpoint_con = None
    if jobs_db is not None:
        jm.preparar_banco_jobs(jobs_db)
        checkpoint_con = jm.conectar_jobs(jobs_db)
    from fts_sync import instalar
    instalar(con)
    pa.preparar(con)
    anterior = con.execute('SELECT estado FROM coleta_janelas_pesquisa WHERE inicio=? AND fim=?',(inicio,fim)).fetchone()
    if anterior is None or anterior[0] == 'PNCP_CONCLUIDO':
        # Uma nova verificação não reutiliza páginas de uma consulta já concluída.
        # Checkpoints legados sem alvos auditáveis também exigem releitura.
        if checkpoint_con is not None:
            checkpoint_con.execute('DELETE FROM coleta_atualizacao_checkpoints WHERE janela_inicio=? AND janela_fim=?',(inicio,fim))
            checkpoint_con.commit()
        con.execute('DELETE FROM coleta_alvos_pesquisa WHERE inicio=? AND fim=?',(inicio,fim))
        con.commit()
    pa.iniciar(con, inicio, fim)
    sp, limiter = criar_sessao_controlada(1.25)
    sp.set_parada_callback(lambda: _parada(args.job_id, jobs_db))
    sc = sessao_compras()

    novos = atualizados = inalterados = dependencias_atualizadas = erros_pncp = 0
    totais_global = {"novos": 0, "atualizados": 0, "dependencias": 0, "inalterados": 0}
    ids_atualizados_existentes: set[str] = set()
    ids_global_mudou: set[str] = set()
    ids_novos: set[str] = set()
    modalidades_puladas = 0
    compras_erros = compras_distintas = 0
    resultados = resultados_novos = resultados_alterados = resultados_identicos = 0
    resultados_quarentena = resultados_erros = 0
    resultados_fronteira = None
    resultados_defasagem = None
    resultados_frescor = 'SEM_DADOS'
    csv_resultados_status = 'NÃO_VERIFICADO'
    csv_resultados_novos = 0
    complementares_pendentes = False
    itens_seed = itens_enfileirados = 0
    itens_stats: dict[str, Any] = {}
    itens_ids_material: set[str] = set()

    try:
        print("\n[PREFLIGHT] PNCP · contratações por atualização...", flush=True)
        ok, det = preflight_atualizacao(sp, limiter, inicio, fim, args.job_id, jobs_db)
        print(f"[PREFLIGHT] PNCP atualização: {'OK' if ok else 'FALHA'} · {det}", flush=True)
        if not ok:
            print("A rotina não fará fallback para revarrer publicações; nenhum dado PNCP foi iniciado.", flush=True)
            pa.concluir(con, inicio, fim, False)
            return EXIT_PARCIAL

        from coleta_multifonte import preflight_compras_contratacoes, preflight_compras_resultados
        c_ok, c_det = preflight_compras_contratacoes(sc, fim)
        r_ok, r_det = preflight_compras_resultados(sc, fim)
        print(f"[PREFLIGHT] Compras.gov · contratações: {'OK' if c_ok else 'INDISPONÍVEL'} · {c_det}", flush=True)
        print(f"[PREFLIGHT] Compras.gov · resultados: {'OK' if r_ok else 'INDISPONÍVEL'} · {r_det}", flush=True)
        complementares_pendentes = not (c_ok and r_ok)

        print("\n[PNCP] Revisando contratações atualizadas na janela...", flush=True)
        for pos, (mid, nome) in enumerate(MODALIDADES.items(), 1):
            try:
                rc = processar_modalidade_atualizacao(
                    con, sp, limiter, inicio, fim, mid, nome, pos, args.job_id, jobs_db, checkpoint_con,
                    totais_global, ids_novos, ids_atualizados_existentes, ids_global_mudou,
                )
                if rc["pulada"]:
                    modalidades_puladas += 1
                    continue
                print(
                    f"    {nome}: recebidas nesta execução {int(rc['recebidas']):,} · novas {int(rc['novas']):,} · "
                    f"pai alterado {int(rc['atualizadas']):,} · dependentes/global {int(rc['dependencias']):,} · "
                    f"já iguais {int(rc['inalteradas']):,}".replace(",", "."), flush=True,
                )
            except InterruptedError:
                raise
            except RateLimitPersistente as exc:
                erros_pncp += 1
                print(f"    AVISO {nome}: {exc}. Encerrando fonte primária para preservar o rate limit.", flush=True)
                break
            except Exception as exc:
                erros_pncp += 1
                trans = eh_erro_transitorio(exc) or (isinstance(exc, requests.HTTPError) and _erro_backend_incoerente(exc, inicio, fim))
                print(f"    AVISO {nome}: {'transitório' if trans else 'erro'} · {type(exc).__name__}: {exc}", flush=True)

        _atualizar(args.job_id, jobs_db, "COLETA · Compras.gov · contratações", 14 * PROGRESSO_POR_ETAPA, PROGRESSO_TOTAL,
                  f"PNCP finalizado · {erros_pncp} modalidade(s) pendente(s)")
        if c_ok:
            print(f"\n[Compras.gov] Contratações publicadas em {fim} (complemento)", flush=True)
            rc = buscar_compras_dia(con, sc, fim, False, args.job_id, jobs_db)
            compras_distintas = int(rc.get("contratacoes") or 0)
            compras_erros = int(rc.get("erros_modalidades") or 0)
            complementares_pendentes = complementares_pendentes or bool(compras_erros)
            print(f"    Contratações distintas: {compras_distintas:,} · falhas: {compras_erros:,}".replace(",", "."), flush=True)

        _atualizar(args.job_id, jobs_db, "COLETA · Compras.gov · resultados", 15 * PROGRESSO_POR_ETAPA, PROGRESSO_TOTAL,
                  "Sincronizando resultados por fronteira própria")
        if r_ok:
            print("\n[Compras.gov] Resultados homologados · sincronização independente", flush=True)
            def _prog_resultados(atual: int, total_dias: int, dia: str) -> None:
                frac = int((atual / max(1, total_dias)) * 850)
                _atualizar(
                    args.job_id, jobs_db, "COLETA · Compras.gov · resultados",
                    15 * PROGRESSO_POR_ETAPA + min(850, frac), PROGRESSO_TOTAL,
                    f"Resultados · {dia} · {atual}/{total_dias} dias da sincronização",
                )
            estrategia_resultados = "completa" if (args.modo == "revisao" or args.dias >= 30) else "diaria"
            sr = sincronizar_resultados_api(
                con, sc, fim, estrategia=estrategia_resultados, progresso=_prog_resultados,
                parada=lambda: _parada(args.job_id, jobs_db),
            )
            resultados = int(sr.get("recebidos") or sr.get("resultados") or 0)
            resultados_novos = int(sr.get("novos") or 0)
            resultados_alterados = int(sr.get("alterados") or 0)
            resultados_identicos = int(sr.get("identicos") or 0)
            resultados_quarentena = int(sr.get("quarentena") or 0)
            resultados_erros = int(sr.get("falhas") or 0)
            resultados_fronteira = sr.get("fronteira_depois")
            resultados_defasagem = sr.get("defasagem_dias")
            resultados_frescor = str(sr.get("frescor") or "SEM_DADOS")
            complementares_pendentes = complementares_pendentes or bool(resultados_erros)
            print(
                f"    API: recebidos {resultados:,} · novos {resultados_novos:,} · alterados {resultados_alterados:,} · "
                f"idênticos {resultados_identicos:,} · {resultados_quarentena:,} quarentena · {resultados_erros:,} falha(s).".replace(",", "."),
                flush=True,
            )
        else:
            resultados_fronteira = resultados_data_max_local(con, fim)
            resultados_defasagem, resultados_frescor = classificar_frescor_resultados(resultados_fronteira, fim)

        # O CSV anual é uma rede de segurança independente da API: na revisão
        # semanal ele é verificado mesmo se o endpoint de resultados estiver fora.
        if args.modo == "revisao":
            ano_csv = date.fromisoformat(fim).year
            print(f"\n[Compras.gov] Resultados · reconciliação CSV anual {ano_csv} (somente ausentes)", flush=True)
            cr = verificar_reconciliar_csv(
                con, sc, ano_csv, permitir_download=True,
                job_id=args.job_id, jobs_db=jobs_db,
            )
            csv_resultados_status = str(cr.get("status") or "DESCONHECIDO")
            csv_resultados_novos = int(cr.get("novos") or 0)
            if csv_resultados_status == "ERRO":
                complementares_pendentes = True
                print(f"    AVISO CSV resultados: {cr.get('erro') or 'falha não detalhada'}", flush=True)
            elif csv_resultados_status == "RECONCILIADO":
                print(f"    Snapshot novo reconciliado · {csv_resultados_novos:,} resultado(s) ausente(s) inserido(s).".replace(",", "."), flush=True)
            elif csv_resultados_status == "SEM_ALTERACAO":
                print("    Snapshot anual sem alteração desde a última reconciliação.", flush=True)
            elif csv_resultados_status == "COBERTO_PELO_CACHE":
                print("    Snapshot remoto já coberto pelo arquivo anual anteriormente importado.", flush=True)
            elif csv_resultados_status == "NOVO_SNAPSHOT_PENDENTE":
                print("    Snapshot novo detectado; reconciliação ficou pendente para uma revisão com download habilitado.", flush=True)
            else:
                print(f"    Estado do snapshot anual: {csv_resultados_status}.", flush=True)

        # V0.7.6 — itens recentes entram numa fila própria, retomável por contratação+página.
        # O primeiro uso faz um bootstrap conservador dos últimos 60 dias; depois, cada
        # execução semeia apenas a própria janela e os pais novos/alterados.
        print("\n[Itens] Enriquecimento incremental e confirmação PNCP", flush=True)
        if obter_cobertura(con, "itens_fila_bootstrap_60d_v0650") is None:
            itens_seed = enfileirar_recentes_pendentes(con, fim, 60)
            registrar_cobertura(con, "itens_fila_bootstrap_60d_v0650", fim)
            print(f"    Bootstrap inicial: últimos 60 dias considerados · {itens_seed:,} entrada(s)/atualização(ões) de fila.".replace(",", "."), flush=True)
        else:
            itens_seed = enfileirar_recentes_pendentes(con, fim, max(3, args.dias))
            print(f"    Janela atual semeada: {itens_seed:,} entrada(s)/atualização(ões) de fila.".replace(",", "."), flush=True)
        # Recupera decisões das páginas anteriores inclusive após uma retomada.
        alvos = con.execute('SELECT numero_controle_pncp,pai_mudou,global_mudou FROM coleta_alvos_pesquisa WHERE inicio=? AND fim=?',(inicio,fim)).fetchall()
        ids_novos.update(r[0] for r in alvos)
        ids_atualizados_existentes.update(r[0] for r in alvos if r[1] or r[2])
        ids_global_mudou.update(r[0] for r in alvos if r[2])
        itens_enfileirados += enfileirar_ids(con, ids_novos, "COLETA_NOVA", f"nova contratação na janela {inicio}→{fim}")
        itens_enfileirados += enfileirar_ids(con, ids_atualizados_existentes, "COLETA_ATUALIZADA", f"pai alterado na janela {inicio}→{fim}")
        itens_revalidar = enfileirar_ids(
            con, [pid for pid in ids_global_mudou if con.execute("SELECT 1 FROM contratacoes WHERE numero_controle_pncp=? AND COALESCE(data_atualizacao_global,'')<>COALESCE(itens_data_global_certificada,'')",(pid,)).fetchone()], "REVALIDACAO",
            f"dataAtualizacaoGlobal avançou na janela {inicio}→{fim}",
        )
        if itens_revalidar:
            print(f"    Revalidação automática: {itens_revalidar:,} conjunto(s) certificado(s) reaberto(s) por mudança global.".replace(",", "."), flush=True)

        if erros_pncp == 0:
            modo_itens = "revisao" if args.modo == "revisao" else "diario"
            itens_stats = processar_fila(
                con, modo=modo_itens, sessao_pncp=sp, limiter=limiter,
                limite_contratacoes=(2000 if modo_itens == "revisao" else 250),
                limite_paginas=(4000 if modo_itens == "revisao" else 400),
                verbose=True,
            )
            itens_ids_material.update(str(x) for x in itens_stats.get("ids_confirmados", []))
            itens_ids_material.update(str(x) for x in itens_stats.get("ids_enriquecidos", []))
        else:
            itens_stats = {"fila": resumo_fila_itens(con), "status": "ADIADA_POR_PNCP_PARCIAL"}
            print("    Processamento da fila adiado: a fonte PNCP de contratações terminou parcial; fila preservada.", flush=True)

        pa.concluir(con, inicio, fim, erros_pncp == 0)
        registrar_cobertura(con, f"coleta_{args.modo}_janela_inicio", inicio)
        registrar_cobertura(con, f"coleta_{args.modo}_janela_fim", fim)
        registrar_cobertura(con, f"coleta_{args.modo}_janela_dias", str(args.dias))
        con.commit()
        con.close(); con = None

        novos = totais_global["novos"]
        atualizados = totais_global["atualizados"]
        dependencias_atualizadas = totais_global["dependencias"]
        inalterados = totais_global["inalterados"]
        houve_contratacao = bool(novos or atualizados or compras_distintas)
        houve_itens_material = bool(itens_ids_material)
        if houve_contratacao or houve_itens_material:
            print("\n[FINAL] Atualizando FTS5/TIC...", flush=True)
            ids_reindexar = set(ids_atualizados_existentes) | set(itens_ids_material)
            atualizar_auxiliares(args.db, args.job_id, jobs_db, contratacoes_existentes_alteradas=ids_reindexar)
        else:
            print("\n[FINAL] Nenhuma contratação nova/alterada; FTS5/TIC permanecem válidos.", flush=True)

        _atualizar(args.job_id, jobs_db, "COLETA · Finalização", PROGRESSO_TOTAL - 1, PROGRESSO_TOTAL,
                  "Finalizando janela e estatísticas")
        houve_resultado_material = bool(resultados_novos or resultados_alterados or csv_resultados_novos)
        if erros_pncp == 0 and (houve_contratacao or houve_resultado_material or houve_itens_material):
            print("[FINAL] Atualizando snapshot da interface...", flush=True)
            snap = rs.atualizar_automaticamente(args.db, DB_STATS)
            if snap is not None:
                print(f"[FINAL] Snapshot: {snap['meta']['total_contratacoes']:,} contratações · {snap['meta']['total_itens']:,} itens físicos.".replace(",", "."), flush=True)
        elif erros_pncp:
            print("[FINAL] Snapshot completo adiado porque a fonte primária terminou parcial.", flush=True)
        else:
            print("[FINAL] Snapshot não mudou; reconstrução dispensada.", flush=True)
        _atualizar(args.job_id, jobs_db, "COLETA · Finalização", PROGRESSO_TOTAL, PROGRESSO_TOTAL, "Execução finalizada")

        print("\n" + "=" * 92, flush=True)
        print(f"RESUMO · {rotulo}", flush=True)
        print("=" * 92, flush=True)
        print(f"Janela revisada:                         {inicio} → {fim} ({args.dias} dias)", flush=True)
        print(f"PNCP · novas:                           {novos:,}".replace(",", "."), flush=True)
        print(f"PNCP · existentes realmente alteradas: {atualizados:,}".replace(",", "."), flush=True)
        print(f"PNCP · dependentes/global alterados:    {dependencias_atualizadas:,}".replace(",", "."), flush=True)
        print(f"PNCP · registros já idênticos:          {inalterados:,}".replace(",", "."), flush=True)
        print(f"PNCP · modalidades puladas/checkpoint:  {modalidades_puladas:,}".replace(",", "."), flush=True)
        print(f"PNCP · modalidades com erro:            {erros_pncp:,}".replace(",", "."), flush=True)
        print(f"Compras.gov · contratações do dia:      {compras_distintas:,}".replace(",", "."), flush=True)
        print(f"Compras.gov · resultados recebidos API: {resultados:,}".replace(",", "."), flush=True)
        print(f"Compras.gov · resultados novos:         {resultados_novos:,}".replace(",", "."), flush=True)
        print(f"Compras.gov · resultados alterados:     {resultados_alterados:,}".replace(",", "."), flush=True)
        print(f"Compras.gov · resultados idênticos:     {resultados_identicos:,}".replace(",", "."), flush=True)
        print(f"Compras.gov · quarentena resultados:    {resultados_quarentena:,}".replace(",", "."), flush=True)
        print(f"Compras.gov · fronteira de resultados:  {resultados_fronteira or '—'}", flush=True)
        print(f"Compras.gov · defasagem resultados:     {resultados_defasagem if resultados_defasagem is not None else '?'} dia(s) · {resultados_frescor}", flush=True)
        if args.modo == "revisao":
            print(f"Compras.gov · CSV anual resultados:     {csv_resultados_status} · {csv_resultados_novos:,} novo(s) reconciliado(s)".replace(",", "."), flush=True)
        fila_itens = itens_stats.get("fila") or {}
        print(f"Itens · conjuntos confirmados na execução: {int(itens_stats.get('concluidos',0)):,}".replace(",", "."), flush=True)
        print(f"Itens · itens PNCP confirmados na execução: {int(itens_stats.get('itens_confirmados',0)):,}".replace(",", "."), flush=True)
        print(f"Itens · páginas PNCP processadas:          {int(itens_stats.get('paginas',0)):,}".replace(",", "."), flush=True)
        print(f"Itens · fila pendente:                     {int(fila_itens.get('pendentes',0)):,}".replace(",", "."), flush=True)
        print(f"Itens · fila em andamento:                 {int(fila_itens.get('em_andamento',0)):,}".replace(",", "."), flush=True)
        print(f"Itens · erros recuperáveis na fila:        {int(fila_itens.get('erro_transitorio',0)):,}".replace(",", "."), flush=True)
        print(f"PNCP · ritmo final:                      {limiter.intervalo:.2f} s/requisição", flush=True)

        if erros_pncp:
            print("Resultado: PARCIAL — use ‘Retomar última execução parcial’; modalidades completas serão puladas e a pendente recomeça da página salva.", flush=True)
            return EXIT_PARCIAL
        if complementares_pendentes:
            print("Resultado: CONCLUÍDO COM PENDÊNCIAS EXTERNAS — PNCP completo; complementos serão tentados novamente.", flush=True)
            return EXIT_COMPLEMENTAR_INDISPONIVEL
        print("Resultado: CONCLUÍDO — janela revisada sem pendências.", flush=True)
        return 0
    except (InterruptedError, KeyboardInterrupt):
        if con is not None:
            pa.concluir(con, inicio, fim, False)
        return 130
    finally:
        try: sp.close()
        except Exception: pass
        try: sc.close()
        except Exception: pass
        if checkpoint_con is not None:
            try: checkpoint_con.close()
            except Exception: pass
        if con is not None:
            try: con.close()
            except Exception: pass


if __name__ == "__main__":
    raise SystemExit(main())
