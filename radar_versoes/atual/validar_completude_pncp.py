#!/usr/bin/env python3
"""Validação de completude de itens via PNCP — RADAR_PNCP/Radar PNCP V0.7.6.

Processa apenas contratações tocadas por CSV, já confirmadas no PNCP e ainda sem
itens_coletados=1. Cada sucesso substitui o conjunto local de itens pelo conjunto
oficial retornado pelo PNCP e então marca a contratação como completa.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from collections import Counter
from pathlib import Path

import requests

import job_manager as jm
from coletar_itens import BASE_PNCP, consultar_itens, preparar_banco, salvar_item
from importar_comprasgov_csv import atualizar_auxiliares
from proveniencia import (
    FONTE_COMPRASGOV_CSV, FONTE_PNCP, MIGRACAO_RESET_FALSOS_404_ITENS,
    decompor_pncp_id, garantir_proveniencia, pncp_id_valido,
)
from radar_paths import DB_COMPRAS, DB_JOBS, garantir_diretorios
from pncp_rate import criar_sessao_controlada

TRANSITORIOS = {408, 425, 429, 500, 502, 503, 504}


def agora() -> str:
    from datetime import datetime
    return datetime.now().isoformat(timespec="seconds")


def conectar(path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(path), timeout=60)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON")
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA busy_timeout=60000")
    preparar_banco(con)
    garantir_proveniencia(con)
    return con


def atualizar_job(job_id: int | None, jobs_db: Path | None, etapa: str, atual: int, total: int, mensagem: str = "") -> None:
    if job_id is None or jobs_db is None:
        return
    jm.atualizar_job(job_id, jobs_db, etapa=etapa, progresso_atual=atual,
                     progresso_total=total, mensagem=mensagem or etapa)


def parada(job_id: int | None, jobs_db: Path | None) -> bool:
    return bool(job_id is not None and jobs_db is not None and jm.parada_solicitada(job_id, jobs_db))


def candidatos(con: sqlite3.Connection, limite: int | None, rever_404: bool) -> list[sqlite3.Row]:
    cond404 = "" if rever_404 else "AND COALESCE(itens_status,'PENDENTE') NOT IN ('INDISPONIVEL_PNCP','REMOVIDA_PNCP')"
    sql = f"""SELECT numero_controle_pncp,orgao_cnpj,ano_compra,sequencial_compra,
                     COALESCE(qtd_itens,0) qtd_itens,itens_status
                FROM contratacoes
               WHERE COALESCE(itens_coletados,0)=0
                 AND COALESCE(pncp_confirmado,0)=1
                 AND (fonte_primeira=? OR fonte_ultima=? OR itens_fonte=?)
                 AND orgao_cnpj IS NOT NULL AND ano_compra IS NOT NULL AND sequencial_compra IS NOT NULL
                 {cond404}
               ORDER BY COALESCE(data_publicacao_pncp,'') DESC, numero_controle_pncp"""
    params: list[object] = [FONTE_COMPRASGOV_CSV, FONTE_COMPRASGOV_CSV, FONTE_COMPRASGOV_CSV]
    if limite and limite > 0:
        sql += " LIMIT ?"; params.append(int(limite))
    rows = con.execute(sql, params).fetchall()
    return [r for r in rows if pncp_id_valido(r[0])]


def main() -> int:
    ap = argparse.ArgumentParser(description="Valida completude de itens via PNCP — V0.7.6")
    ap.add_argument("--db", type=Path, default=DB_COMPRAS)
    ap.add_argument("--jobs-db", type=Path, default=DB_JOBS)
    ap.add_argument("--job-id", type=int)
    ap.add_argument("--limite", type=int, default=2000, help="0 = sem limite")
    ap.add_argument("--pausa", type=float, default=1.25, help="intervalo inicial mínimo entre requisições PNCP")
    ap.add_argument("--timeout", type=int, default=90)
    ap.add_argument("--rever-404", action="store_true")
    args = ap.parse_args()
    jobs_db = args.jobs_db if args.job_id else None

    garantir_diretorios()
    print("=" * 92, flush=True)
    print("Radar PNCP / Radar PNCP V0.7.6 — VALIDAÇÃO DE COMPLETUDE PELO PNCP", flush=True)
    print("=" * 92, flush=True)
    print(f"Banco: {args.db.resolve()}", flush=True)
    print("Somente o retorno integral da API PNCP marca itens_coletados=1.", flush=True)

    con = conectar(args.db)
    try:
        mig = con.execute(
            "SELECT detalhes_json FROM radar_pncp_migracoes WHERE chave=?",
            (MIGRACAO_RESET_FALSOS_404_ITENS,),
        ).fetchone()
        if mig and mig[0]:
            detalhes = json.loads(str(mig[0]))
            reabertas = int(detalhes.get("validacoes_reabertas", 0) or 0)
            if reabertas:
                print(
                    f"Correção V0.6.3.1 já aplicada: {reabertas:,} falso(s) 404/410 da rota antiga reaberto(s) para validação.",
                    flush=True,
                )
    except Exception:
        pass
    rows = candidatos(con, None if args.limite == 0 else args.limite, args.rever_404)
    total = len(rows)
    print(f"Selecionadas: {total:,} contratação(ões).", flush=True)
    if not total:
        con.close(); return 0

    sessao, limiter = criar_sessao_controlada(max(1.0, float(args.pausa or 1.25)))
    sessao.set_parada_callback(lambda: parada(args.job_id, jobs_db))
    print(f"Ritmo PNCP: {limiter.intervalo:.2f} s/requisição; HTTP 429 ativa cooldown adaptativo.", flush=True)
    stats = Counter()
    consecutivos = 0

    probe = rows[0]
    probe_dec = decompor_pncp_id(probe["numero_controle_pncp"])
    if not probe_dec:
        print("Preflight cancelado: primeiro candidato possui ID PNCP inválido.", flush=True)
        sessao.close(); con.close(); return 2
    probe_cnpj, probe_ano, probe_seq = probe_dec
    probe_url = (
        f"{BASE_PNCP}/orgaos/{probe_cnpj}/compras/"
        f"{probe_ano}/{probe_seq}/itens"
    )
    print(f"Endpoint de itens: {BASE_PNCP}/orgaos/{{cnpj}}/compras/{{ano}}/{{sequencial}}/itens", flush=True)

    def preflight_recuperacao(rotulo: str) -> bool:
        try:
            resp = sessao.get(
                probe_url,
                params={"pagina": 1, "tamanhoPagina": 10},
                timeout=(20, min(60, int(args.timeout))),
            )
            if resp.status_code in (200, 404, 410):
                sufixo = " · API operacional; recurso removido" if resp.status_code == 410 else ""
                print(f"    {rotulo}: PNCP respondeu HTTP {resp.status_code}{sufixo}.", flush=True)
                return True
            if resp.status_code == 429:
                stats["429"] += 1
                print(f"    {rotulo}: HTTP 429; cooldown adicional.", flush=True)
                limiter.cooldown_429(resp, lambda: parada(args.job_id, jobs_db))
                return False
            print(f"    {rotulo}: PNCP ainda instável (HTTP {resp.status_code}).", flush=True)
            return False
        except InterruptedError:
            raise
        except (requests.Timeout, requests.ConnectionError) as exc:
            print(f"    {rotulo}: {type(exc).__name__}; PNCP ainda instável.", flush=True)
            return False
        except Exception as exc:
            print(f"    {rotulo}: {type(exc).__name__}: {exc}", flush=True)
            return False

    # V0.7.6: health-check rápido ANTES de tocar no primeiro item do lote.
    # Se a API de itens estiver em 429/5xx ou sem conexão, encerra PARCIAL
    # imediatamente em vez de gastar vários minutos em backoffs globais.
    print("Preflight inicial da API de itens...", flush=True)
    try:
        resp0 = sessao.get(
            probe_url,
            params={"pagina": 1, "tamanhoPagina": 10},
            timeout=(6, min(15, int(args.timeout))),
        )
        http0 = int(resp0.status_code)
        if http0 in (200, 404, 410):
            detalhe0 = " · recurso sondado removido/indisponível" if http0 in (404, 410) else ""
            print(f"    Preflight OK: HTTP {http0}{detalhe0}.", flush=True)
        elif http0 == 429:
            stats["429"] += 1
            print("    Preflight bloqueado: HTTP 429 (rate limit). Lote não iniciado; tente novamente mais tarde.", flush=True)
            atualizar_job(args.job_id, jobs_db, "PNCP indisponível", 0, total, "API de itens em rate limit; lote não iniciado")
            sessao.close(); con.close(); return 75
        elif http0 in TRANSITORIOS:
            print(f"    Preflight falhou: HTTP {http0}. Lote não iniciado; estado local preservado.", flush=True)
            atualizar_job(args.job_id, jobs_db, "PNCP indisponível", 0, total, f"API de itens respondeu HTTP {http0}; lote não iniciado")
            sessao.close(); con.close(); return 75
        else:
            print(f"    Preflight recebeu HTTP {http0}; serviço respondeu e o lote continuará.", flush=True)
    except (requests.Timeout, requests.ConnectionError) as exc:
        print(f"    Preflight falhou: {type(exc).__name__}. Lote não iniciado; estado local preservado.", flush=True)
        atualizar_job(args.job_id, jobs_db, "PNCP indisponível", 0, total, f"{type(exc).__name__} no preflight; lote não iniciado")
        sessao.close(); con.close(); return 75
    except InterruptedError:
        sessao.close(); con.close(); return 130
    except Exception as exc:
        print(f"    Preflight falhou: {type(exc).__name__}: {exc}", flush=True)
        atualizar_job(args.job_id, jobs_db, "PNCP indisponível", 0, total, f"Falha no preflight: {type(exc).__name__}")
        sessao.close(); con.close(); return 75

    try:
        for i, c in enumerate(rows, 1):
            if parada(args.job_id, jobs_db):
                raise InterruptedError
            pid = str(c["numero_controle_pncp"])
            atualizar_job(args.job_id, jobs_db, "Validando itens no PNCP", i - 1, total, pid)
            stamp = agora()
            interromper_rate = False
            while True:
                try:
                    dec = decompor_pncp_id(pid)
                    if not dec:
                        raise ValueError(f"Número de controle PNCP inválido: {pid}")
                    cnpj_api, ano_api, seq_api = dec
                    itens, paginas = consultar_itens(
                        sessao, cnpj_api, ano_api, seq_api, int(args.timeout),
                    )
                    local_qtd = int(con.execute(
                        "SELECT COUNT(*) FROM itens WHERE numero_controle_pncp=?", (pid,)
                    ).fetchone()[0] or 0)
                    if not itens and local_qtd > 0:
                        raise RuntimeError(
                            f"PNCP retornou coleção vazia, mas há {local_qtd} item(ns) locais; "
                            "completude não será confirmada nesta tentativa."
                        )
                    con.execute("DELETE FROM itens WHERE numero_controle_pncp=?", (pid,))
                    for item in itens:
                        salvar_item(con, pid, item)
                    con.execute(
                        """UPDATE contratacoes SET itens_coletados=1,qtd_itens=?,itens_erro=NULL,
                                  itens_status='OK',itens_http_status=NULL,itens_indisponivel_em=NULL,
                                  itens_ultima_tentativa=?,itens_fonte=?,pncp_confirmado=1,
                                  fonte_primeira=COALESCE(fonte_primeira,?),fonte_ultima=?
                            WHERE numero_controle_pncp=?""",
                        (len(itens), stamp, FONTE_PNCP, FONTE_PNCP, FONTE_PNCP, pid),
                    )
                    con.commit(); stats["ok"] += 1; stats["itens"] += len(itens); consecutivos = 0
                    if i % 50 == 0 or i == total:
                        print(f"    {i:,}/{total:,} · OK {stats['ok']:,} · itens oficiais {stats['itens']:,} · 429 {stats['429']:,}", flush=True)
                    break
                except requests.HTTPError as exc:
                    status = exc.response.status_code if exc.response is not None else None
                    msg = str(exc)[:3000]
                    if status == 429:
                        stats["429"] += 1
                        con.execute(
                            """UPDATE contratacoes SET itens_erro=?,itens_status='AGUARDANDO_RATE_LIMIT',
                                      itens_http_status=429,itens_ultima_tentativa=? WHERE numero_controle_pncp=?""",
                            (msg, stamp, pid),
                        )
                        con.commit()
                        print(f"    HTTP 429 ao validar {pid}; entrando em cooldown...", flush=True)
                        espera, parar_rate = limiter.cooldown_429(exc.response, lambda: parada(args.job_id, jobs_db))
                        print(f"    Cooldown de {espera:.0f} s concluído; ritmo {limiter.intervalo:.2f} s/requisição.", flush=True)
                        if parar_rate:
                            stats["rate_limit_pause"] = 1; interromper_rate = True; break
                        continue
                    if status in (404, 410):
                        con.execute(
                            """UPDATE contratacoes SET itens_coletados=0,qtd_itens=0,itens_erro=?,
                                      itens_status='INDISPONIVEL_PNCP',itens_http_status=?,
                                      itens_indisponivel_em=?,itens_ultima_tentativa=?
                                WHERE numero_controle_pncp=?""",
                            (msg, status, stamp, stamp, pid),
                        )
                        stats[str(status)] += 1; consecutivos = 0
                    else:
                        con.execute(
                            """UPDATE contratacoes SET itens_erro=?,itens_status='ERRO_TEMPORARIO',
                                      itens_http_status=?,itens_ultima_tentativa=?
                                WHERE numero_controle_pncp=?""",
                            (msg, status, stamp, pid),
                        )
                        stats["erro"] += 1
                        consecutivos = consecutivos + 1 if status in TRANSITORIOS else 0
                        if status in TRANSITORIOS:
                            espera = limiter.backoff_transitorio(min(4, consecutivos), lambda: parada(args.job_id, jobs_db))
                            print(f"    HTTP {status}; backoff de {espera:.0f} s.", flush=True)
                    con.commit(); break
                except InterruptedError:
                    raise
                except (requests.Timeout, requests.ConnectionError) as exc:
                    con.execute(
                        """UPDATE contratacoes SET itens_erro=?,itens_status='ERRO_TEMPORARIO',
                                  itens_ultima_tentativa=? WHERE numero_controle_pncp=?""",
                        (f"{type(exc).__name__}: {exc}"[:3000], stamp, pid),
                    )
                    con.commit(); stats["erro"] += 1; consecutivos += 1
                    espera = limiter.backoff_transitorio(min(4, consecutivos), lambda: parada(args.job_id, jobs_db))
                    print(f"    {type(exc).__name__}; backoff de {espera:.0f} s.", flush=True)
                    break
                except Exception as exc:
                    con.execute(
                        """UPDATE contratacoes SET itens_erro=?,itens_status='ERRO_TEMPORARIO',
                                  itens_ultima_tentativa=? WHERE numero_controle_pncp=?""",
                        (f"{type(exc).__name__}: {exc}"[:3000], stamp, pid),
                    )
                    con.commit(); stats["erro"] += 1; consecutivos = 0
                    break

            if interromper_rate:
                print("    Rate limit persistente; parando com progresso preservado.", flush=True)
                break
            if consecutivos >= 5:
                stats["pausas_globais"] += 1
                print("    5 falhas transitórias consecutivas; pausa global de 120 s...", flush=True)
                limiter.pausa_global(120, lambda: parada(args.job_id, jobs_db))
                if preflight_recuperacao("Preflight após pausa de 120 s"):
                    consecutivos = 0
                    print(f"    PNCP recuperado; retomando em {limiter.intervalo:.2f} s/requisição.", flush=True)
                    continue

                stats["pausas_globais"] += 1
                print("    PNCP ainda instável; segunda pausa global de 300 s...", flush=True)
                limiter.pausa_global(300, lambda: parada(args.job_id, jobs_db))
                if preflight_recuperacao("Preflight após pausa de 300 s"):
                    consecutivos = 0
                    print(f"    PNCP recuperado; retomando em {limiter.intervalo:.2f} s/requisição.", flush=True)
                    continue

                print("    PNCP permaneceu instável; parando com estado preservado.", flush=True)
                stats["outage"] = 1
                break
            atualizar_job(args.job_id, jobs_db, "Validando itens no PNCP", i, total, pid)
    except InterruptedError:
        print("\nInterrompida pelo usuário; progresso já confirmado foi preservado.", flush=True)
        return 130
    finally:
        sessao.close(); con.close()

    print("\n" + "=" * 92, flush=True)
    print("RESUMO DA VALIDAÇÃO DE COMPLETUDE", flush=True)
    print("=" * 92, flush=True)
    print(f"Contratações confirmadas: {stats['ok']:,}", flush=True)
    print(f"Itens oficiais gravados:  {stats['itens']:,}", flush=True)
    print(f"PNCP 404:                 {stats['404']:,}", flush=True)
    print(f"PNCP 410 (removida):      {stats['410']:,}", flush=True)
    print(f"Erros temporários:        {stats['erro']:,}", flush=True)
    print(f"HTTP 429 observados:       {stats['429']:,}", flush=True)
    print(f"Pausas globais por instabilidade: {stats['pausas_globais']:,}", flush=True)

    atualizar_auxiliares(args.db.resolve(), args.job_id, jobs_db)
    atualizar_job(args.job_id, jobs_db, "Concluído", 1, 1, "Validação de completude finalizada")
    if stats.get("rate_limit_pause"):
        return 75
    if stats.get("outage"):
        return 75
    if stats.get("erro"):
        print("Há erros temporários pendentes; job marcado como PARCIAL para permitir retomada.", flush=True)
        return 75
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
