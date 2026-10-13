#!/usr/bin/env python3
"""Reconciliação PNCP do staging Compras.gov — RADAR_PNCP/Radar PNCP V0.7.6.

Fluxo:
1. move IDs malformados do staging legado para quarentena sem apagá-los;
2. revarre os CSVs COMPRA_ITEM já em cache e tenta recuperar linhas cujo campo
   esperado de ID está inválido, mas outro valor da mesma linha contém um ID PNCP válido;
3. resolve imediatamente itens cujo pai já existe;
4. monta uma fila por contratação-pai válida ausente;
5. consulta somente esses pais na API oficial do PNCP;
6. move do staging para itens tudo que passou a ter pai;
7. grava diagnóstico CSV em radar_dados/diagnosticos.

A rotina é retomável: o estado por pai fica em pncp_reconciliacao_staging.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sqlite3
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

import job_manager as jm
from coletar_pncp import BASE_CONSULTA, salvar_contratacao
from importar_comprasgov_csv import (
    COLS_COMPRA, COLS_ITEM, COMPRA_MAP, CSV_DIR, ITEM_MAP, SQL_ITEM, abrir_csv, agora,
    conectar_db, contar_orfaos, guardar_compra_quarentena, guardar_item_quarentena,
    guardar_itens_orfaos, linha_csv_desalinhada, compra_linha_semantica_invalida, numero_controle_item,
    parametros_compra, parametros_item, registrar_orfaos_resolvidos, resolver_colunas,
    resolver_itens_orfaos, row_json_seguro, separar_itens_com_pai, texto,
    atualizar_auxiliares, valores_escalares_row,
)
from proveniencia import decompor_pncp_id, pncp_id_valido
from radar_paths import DATA_DIR, DB_COMPRAS, DB_JOBS, garantir_diretorios
from pncp_rate import criar_sessao_controlada

TRANSITORIOS = {408, 425, 429, 500, 502, 503, 504}


def atualizar_job(job_id: int | None, jobs_db: Path | None, etapa: str, atual: int = 0, total: int = 0, mensagem: str | None = None) -> None:
    if job_id is None or jobs_db is None:
        return
    jm.atualizar_job(
        job_id, jobs_db, etapa=etapa,
        progresso_atual=int(atual), progresso_total=int(total),
        mensagem=mensagem or etapa,
    )


def parada(job_id: int | None, jobs_db: Path | None) -> bool:
    return bool(job_id is not None and jobs_db is not None and jm.parada_solicitada(job_id, jobs_db))


def chave_legado(pid: str, numero_item: int, ano_mes: str, payload: str) -> str:
    base = f"LEGADO\n{pid}\n{numero_item}\n{ano_mes}\n{payload}"
    return hashlib.sha256(base.encode("utf-8", errors="replace")).hexdigest()


def higienizar_staging_legado(con: sqlite3.Connection) -> int:
    """Move para quarentena apenas os IDs estruturalmente inválidos do staging antigo."""
    rows = con.execute(
        "SELECT numero_controle_pncp,numero_item,ano_mes,payload_json,primeira_tentativa_em,ultima_tentativa_em FROM comprasgov_itens_orfaos"
    ).fetchall()
    mover = [r for r in rows if not pncp_id_valido(r[0])]
    if not mover:
        return 0
    for r in mover:
        pid = str(r[0] or "")
        ni = int(r[1])
        periodo = str(r[2])
        payload = str(r[3] or "{}")
        key = chave_legado(pid, ni, periodo, payload)
        stamp = agora()
        con.execute(
            """INSERT INTO comprasgov_itens_quarentena(
                   chave_linha,ano_mes,numero_controle_informado,numero_item,motivo,
                   payload_json,raw_row_json,status,criado_em,atualizado_em
               ) VALUES (?,?,?,?,?,?,NULL,'LEGADO_MALFORMADO',?,?)
               ON CONFLICT(chave_linha) DO UPDATE SET atualizado_em=excluded.atualizado_em""",
            (key, periodo, pid, ni, "ID_MALFORMADO_LEGADO", payload, stamp, stamp),
        )
    con.executemany(
        "DELETE FROM comprasgov_itens_orfaos WHERE numero_controle_pncp=? AND numero_item=?",
        [(str(r[0]), int(r[1])) for r in mover],
    )
    con.commit()
    return len(mover)



def _sql_reparo_compra() -> str:
    """Upsert de reparo: uma linha CSV estruturalmente íntegra substitui campos
    de registros ainda não confirmados pelo PNCP, sem tocar em confirmações PNCP.
    """
    updates: list[str] = []
    for campo in COMPRA_MAP:
        if campo == "numero_controle_pncp":
            continue
        updates.append(
            f"{campo}=CASE WHEN COALESCE(contratacoes.pncp_confirmado,0)=0 "
            f"THEN excluded.{campo} "
            f"WHEN contratacoes.{campo} IS NULL THEN excluded.{campo} "
            f"ELSE contratacoes.{campo} END"
        )
    updates += [
        "url_pncp=CASE WHEN COALESCE(contratacoes.pncp_confirmado,0)=0 THEN excluded.url_pncp ELSE contratacoes.url_pncp END",
        "fonte_ultima=CASE WHEN COALESCE(contratacoes.pncp_confirmado,0)=0 THEN 'COMPRASGOV_CSV' ELSE contratacoes.fonte_ultima END",
        "csv_compra_mes=CASE WHEN COALESCE(contratacoes.pncp_confirmado,0)=0 THEN excluded.csv_compra_mes ELSE contratacoes.csv_compra_mes END",
        "csv_compra_importada_em=CASE WHEN COALESCE(contratacoes.pncp_confirmado,0)=0 THEN excluded.csv_compra_importada_em ELSE contratacoes.csv_compra_importada_em END",
    ]
    return (
        f"INSERT INTO contratacoes ({','.join(COLS_COMPRA)}) "
        f"VALUES ({','.join('?' for _ in COLS_COMPRA)}) "
        f"ON CONFLICT(numero_controle_pncp) DO UPDATE SET {','.join(updates)}"
    )


SQL_REPARO_COMPRA = _sql_reparo_compra()


def _deduplicar_cache_por_nome(arquivos: list[Path]) -> list[Path]:
    """Mantém uma única cópia de cada arquivo lógico do repositório Compras.gov.

    Em versões anteriores, a mesma competência podia existir em duas subpastas do
    cache e era processada duas vezes. Em caso de duplicidade por nome, preferimos
    a cópia maior e, em empate, a mais recente.
    """
    unicos: dict[str, Path] = {}
    for path in arquivos:
        chave = path.name.casefold()
        atual = unicos.get(chave)
        if atual is None:
            unicos[chave] = path
            continue
        try:
            st_new = path.stat()
            st_old = atual.stat()
            if (st_new.st_size, st_new.st_mtime_ns, str(path)) > (st_old.st_size, st_old.st_mtime_ns, str(atual)):
                unicos[chave] = path
        except OSError:
            # Se uma das cópias desaparecer durante a listagem, preserva a que
            # ainda estiver acessível na próxima execução.
            if path.exists() and not atual.exists():
                unicos[chave] = path
    return list(unicos.values())


def arquivos_compra_cache() -> list[Path]:
    if not CSV_DIR.exists():
        return []
    arquivos = _deduplicar_cache_por_nome([
        p for p in CSV_DIR.rglob("*.csv")
        if "PNCP_COMPRA" in p.name.upper()
        and "COMPRA_ITEM" not in p.name.upper()
        and p.is_file()
    ])
    def chave(p: Path):
        per = periodo_do_arquivo(p) or "9999-99"
        m = re.search(r"^(\d{4})-(\d{2})$", per)
        if m:
            return (int(m.group(1)), int(m.group(2)), str(p))
        m = re.search(r"^(\d{4})-ANUAL$", per)
        if m:
            return (int(m.group(1)), 0, str(p))
        return (9999, 99, str(p))
    return sorted(arquivos, key=chave)


def _checkpoint_reparo_compra_ok(con: sqlite3.Connection, arquivos: list[Path]) -> bool:
    if not arquivos:
        return True
    rows = {
        str(r[0]): (int(r[1]), int(r[2]), str(r[3]))
        for r in con.execute(
            "SELECT arquivo,tamanho_bytes,mtime_ns,status FROM comprasgov_reparo_compra_arquivos"
        ).fetchall()
    }
    if len(rows) < len(arquivos):
        return False
    for path in arquivos:
        stf = path.stat()
        meta = rows.get(str(path.resolve()))
        if not meta or meta[2] != "CONCLUIDA" or meta[0] != stf.st_size or meta[1] != stf.st_mtime_ns:
            return False
    return True


def _bootstrap_checkpoint_reparo_de_job_anterior(
    con: sqlite3.Connection,
    arquivos: list[Path],
    job_id: int | None,
    jobs_db: Path | None,
) -> bool:
    """V0.7.6: aproveita uma revalidação concluída pelo job imediatamente
    anterior, sem obrigar uma varredura completa apenas para criar o novo
    checkpoint. Só faz isso se o log comprovar o término do REPARO BANCO e se
    nenhum arquivo COMPRA tiver sido modificado depois do fim daquele job.
    """
    if not arquivos or jobs_db is None or not Path(jobs_db).exists():
        return False
    if con.execute("SELECT COUNT(*) FROM comprasgov_reparo_compra_arquivos").fetchone()[0]:
        return False
    try:
        cj = sqlite3.connect(str(jobs_db), timeout=5)
        cj.row_factory = sqlite3.Row
        sql = """SELECT id,finalizado_em,log_path,status
                   FROM jobs
                  WHERE tipo=?
                    AND status IN ('PARCIAL','CONCLUIDA')
        """
        params: list[Any] = [jm.TIPO_RECONCILIAR_STAGING_PNCP]
        if job_id is not None:
            sql += " AND id<>?"
            params.append(int(job_id))
        sql += " ORDER BY id DESC LIMIT 5"
        candidatos = cj.execute(sql, params).fetchall()
        cj.close()
    except Exception:
        return False

    for r in candidatos:
        fim_raw = r["finalizado_em"]
        log_path = r["log_path"]
        if not fim_raw or not log_path:
            continue
        try:
            fim_ts = datetime.fromisoformat(str(fim_raw)).timestamp()
            lp = Path(str(log_path))
            if not lp.exists():
                continue
            # O marcador só é escrito depois de concluir a varredura e o reparo.
            with lp.open("rb") as f:
                tamanho = lp.stat().st_size
                f.seek(max(0, tamanho - 120_000))
                log_tail = f.read().decode("utf-8", errors="replace")
            if "[REPARO BANCO] Linhas válidas reaplicadas:" not in log_tail:
                continue
            if any(path.stat().st_mtime > fim_ts + 2 for path in arquivos):
                continue
        except Exception:
            continue

        stamp = agora()
        con.executemany(
            """INSERT INTO comprasgov_reparo_compra_arquivos(
                   arquivo,tamanho_bytes,mtime_ns,status,atualizado_em
               ) VALUES (?,?,?,'CONCLUIDA',?)
               ON CONFLICT(arquivo) DO UPDATE SET
                   tamanho_bytes=excluded.tamanho_bytes,
                   mtime_ns=excluded.mtime_ns,
                   status='CONCLUIDA',
                   atualizado_em=excluded.atualizado_em""",
            [
                (str(path.resolve()), path.stat().st_size, path.stat().st_mtime_ns, stamp)
                for path in arquivos
            ],
        )
        con.commit()
        return True
    return False


def _gravar_checkpoint_reparo_compra(con: sqlite3.Connection, arquivos: list[Path]) -> None:
    stamp = agora()
    con.executemany(
        """INSERT INTO comprasgov_reparo_compra_arquivos(
               arquivo,tamanho_bytes,mtime_ns,status,atualizado_em
           ) VALUES (?,?,?,'CONCLUIDA',?)
           ON CONFLICT(arquivo) DO UPDATE SET
               tamanho_bytes=excluded.tamanho_bytes,
               mtime_ns=excluded.mtime_ns,
               status='CONCLUIDA',
               atualizado_em=excluded.atualizado_em""",
        [
            (str(path.resolve()), path.stat().st_size, path.stat().st_mtime_ns, stamp)
            for path in arquivos
        ],
    )
    con.commit()


def _primeiro_id_valido_row(row: dict[Any, Any]) -> str | None:
    achados: list[str] = []
    for v in valores_escalares_row(row):
        tv = texto(v)
        if tv and pncp_id_valido(tv) and tv not in achados:
            achados.append(tv)
    return achados[0] if len(achados) == 1 else None


def _quarentenar_contrato_existente(con: sqlite3.Connection, pid: str, motivo: str) -> None:
    row = con.execute("SELECT * FROM contratacoes WHERE numero_controle_pncp=?", (pid,)).fetchone()
    if not row:
        return
    raw = json.dumps(dict(row), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    chave = hashlib.sha256(("BANCO\n" + pid + "\n" + motivo + "\n" + raw).encode("utf-8", errors="replace")).hexdigest()
    stamp = agora()
    con.execute(
        """INSERT INTO comprasgov_contratacoes_quarentena(
               chave_linha,ano_mes,numero_controle_informado,motivo,raw_row_json,status,criado_em,atualizado_em
           ) VALUES (?,'REPARO-0621',?,?,?,'REMOVIDA_DO_BANCO',?,?)
           ON CONFLICT(chave_linha) DO UPDATE SET atualizado_em=excluded.atualizado_em""",
        (chave, pid, motivo, raw, stamp, stamp),
    )


def _preservar_itens_antes_excluir_pai(con: sqlite3.Connection, pid: str) -> int:
    """Preserva itens antes de remover uma contratação CSV corrompida.

    Com ID PNCP válido, os itens voltam ao staging para futura reconciliação.
    Com ID inválido, são preservados na quarentena de itens.
    """
    cols_db = {str(r[1]) for r in con.execute("PRAGMA table_info(itens)").fetchall()}
    cols = [c for c in COLS_ITEM if c in cols_db]
    if not cols:
        return 0
    rows = con.execute(
        f"SELECT {','.join(cols)} FROM itens WHERE numero_controle_pncp=?",
        (pid,),
    ).fetchall()
    if not rows:
        return 0
    total = 0
    if pncp_id_valido(pid):
        params_lote: list[tuple[Any, ...]] = []
        for r in rows:
            d = dict(r)
            params_lote.append(tuple(d.get(c) for c in COLS_ITEM))
        guardar_itens_orfaos(con, "REPARO-0621", params_lote)
        total = len(params_lote)
    else:
        stamp = agora()
        for r in rows:
            d = dict(r)
            raw = json.dumps(d, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            ni = d.get("numero_item")
            chave = hashlib.sha256(("ITEM_BANCO\n" + pid + "\n" + str(ni) + "\n" + raw).encode("utf-8", errors="replace")).hexdigest()
            con.execute(
                """INSERT INTO comprasgov_itens_quarentena(
                       chave_linha,ano_mes,numero_controle_informado,numero_item,motivo,
                       payload_json,raw_row_json,status,criado_em,atualizado_em
                   ) VALUES (?,'REPARO-0621',?,?, 'PAI_INVALIDO_REPARO', ?,NULL,'PRESERVADO',?,?)
                   ON CONFLICT(chave_linha) DO UPDATE SET atualizado_em=excluded.atualizado_em""",
                (chave, pid, int(ni) if ni is not None else None, raw, stamp, stamp),
            )
            total += 1
    return total


def reparar_contratacoes_csv(con: sqlite3.Connection, job_id: int | None, jobs_db: Path | None) -> dict[str, int]:
    """Revarre COMPRA em cache, ignora linhas desalinhadas e reconstrói os
    registros CSV-only a partir da última linha estruturalmente íntegra.

    Também remove do banco registros CSV-only que só existem em linhas
    malformadas ou possuem número de controle PNCP inválido. Antes da remoção,
    tudo é preservado em quarentena/staging.
    """
    arquivos = arquivos_compra_cache()
    stats = Counter()
    if not arquivos:
        return dict(stats)

    if _checkpoint_reparo_compra_ok(con, arquivos):
        print(f"\n[REPARO BANCO] {len(arquivos)} arquivo(s) COMPRA já validados e inalterados; pulando revarredura.", flush=True)
        for ai, path in enumerate(arquivos, 1):
            print(f"[REPARO BANCO][{ai}/{len(arquivos)}] {path.name} — já validado; pulando.", flush=True)
            atualizar_job(job_id, jobs_db, "Reparo Banco · cache", ai, len(arquivos), path.name)
        stats["arquivos_cache_reparo"] = len(arquivos)
        return dict(stats)

    if _bootstrap_checkpoint_reparo_de_job_anterior(con, arquivos, job_id, jobs_db):
        print(f"\n[REPARO BANCO] Checkpoint criado a partir da revalidação concluída no job anterior.", flush=True)
        for ai, path in enumerate(arquivos, 1):
            print(f"[REPARO BANCO][{ai}/{len(arquivos)}] {path.name} — já validado; pulando.", flush=True)
            atualizar_job(job_id, jobs_db, "Reparo Banco · cache", ai, len(arquivos), path.name)
        stats["arquivos_cache_reparo"] = len(arquivos)
        return dict(stats)

    con.execute("DROP TABLE IF EXISTS temp._radar_pncp_compra_validos")
    con.execute("DROP TABLE IF EXISTS temp._radar_pncp_compra_malformados")
    con.execute("CREATE TEMP TABLE _radar_pncp_compra_validos(pid TEXT PRIMARY KEY)")
    con.execute("CREATE TEMP TABLE _radar_pncp_compra_malformados(pid TEXT PRIMARY KEY)")
    con.commit()

    print(f"\n[REPARO BANCO] Revalidando {len(arquivos)} arquivo(s) COMPRA em cache...", flush=True)
    for ai, path in enumerate(arquivos, 1):
        if parada(job_id, jobs_db):
            raise InterruptedError
        periodo = periodo_do_arquivo(path) or "DESCONHECIDO"
        print(f"[REPARO BANCO][{ai}/{len(arquivos)}] {path.name}", flush=True)
        atualizar_job(job_id, jobs_db, f"Reparo Banco · {periodo}", ai - 1, len(arquivos), path.name)
        f, reader = abrir_csv(path)
        try:
            headers = reader.fieldnames or []
            resolvido = resolver_colunas(headers, COMPRA_MAP)
            idcol = resolvido.get("numero_controle_pncp")
            if not idcol:
                stats["arquivos_sem_id"] += 1
                continue
            batch: list[tuple[Any, ...]] = []
            lidas = 0
            stamp = agora()
            for row in reader:
                lidas += 1
                if lidas % 100000 == 0:
                    print(f"    Varredura: {lidas:,} linhas", flush=True)
                    if parada(job_id, jobs_db):
                        raise InterruptedError

                if linha_csv_desalinhada(row, headers):
                    stats["linhas_desalinhadas"] += 1
                    pid = _primeiro_id_valido_row(row)
                    if pid:
                        con.execute("INSERT OR IGNORE INTO _radar_pncp_compra_malformados(pid) VALUES (?)", (pid,))
                    guardar_compra_quarentena(con, periodo, row, "LINHA_DESALINHADA_REPARO")
                    continue

                if compra_linha_semantica_invalida(row, resolvido):
                    stats["linhas_estruturalmente_invalidas"] += 1
                    pid = _primeiro_id_valido_row(row)
                    if pid:
                        con.execute("INSERT OR IGNORE INTO _radar_pncp_compra_malformados(pid) VALUES (?)", (pid,))
                    guardar_compra_quarentena(con, periodo, row, "LINHA_ESTRUTURALMENTE_INVALIDA_REPARO")
                    continue

                params = parametros_compra(row, resolvido, periodo, stamp)
                if params is None:
                    stats["ids_invalidos_csv"] += 1
                    pid = _primeiro_id_valido_row(row)
                    if pid:
                        con.execute("INSERT OR IGNORE INTO _radar_pncp_compra_malformados(pid) VALUES (?)", (pid,))
                    guardar_compra_quarentena(con, periodo, row, "ID_PNCP_INVALIDO_REPARO")
                    continue

                pid = str(params[COLS_COMPRA.index("numero_controle_pncp")])
                con.execute("INSERT OR IGNORE INTO _radar_pncp_compra_validos(pid) VALUES (?)", (pid,))
                batch.append(params)
                if len(batch) >= 2000:
                    con.executemany(SQL_REPARO_COMPRA, batch)
                    con.commit()
                    stats["linhas_validas"] += len(batch)
                    batch.clear()

            if batch:
                con.executemany(SQL_REPARO_COMPRA, batch)
                stats["linhas_validas"] += len(batch)
                batch.clear()
            con.commit()
        finally:
            f.close()
        atualizar_job(job_id, jobs_db, f"Reparo Banco · {periodo}", ai, len(arquivos), path.name)

    # 1) IDs estruturalmente inválidos já existentes e pertencentes somente ao CSV.
    csv_only = con.execute(
        """SELECT numero_controle_pncp FROM contratacoes
           WHERE COALESCE(pncp_confirmado,0)=0"""
    ).fetchall()
    bad_ids: set[str] = {str(r[0]) for r in csv_only if not pncp_id_valido(r[0])}

    # 2) IDs válidos que só apareceram em linhas desalinhadas, sem nenhuma linha
    #    estruturalmente íntegra nos arquivos em cache.
    rows = con.execute(
        """SELECT m.pid
             FROM _radar_pncp_compra_malformados m
             LEFT JOIN _radar_pncp_compra_validos v ON v.pid=m.pid
             JOIN contratacoes c ON c.numero_controle_pncp=m.pid
            WHERE v.pid IS NULL AND COALESCE(c.pncp_confirmado,0)=0"""
    ).fetchall()
    bad_ids.update(str(r[0]) for r in rows)

    preservados = 0
    removidos = 0
    for i, pid in enumerate(sorted(bad_ids), 1):
        motivo = "ID_PNCP_INVALIDO_NO_BANCO" if not pncp_id_valido(pid) else "SEM_LINHA_ESTRUTURALMENTE_INTEGRA"
        _quarentenar_contrato_existente(con, pid, motivo)
        preservados += _preservar_itens_antes_excluir_pai(con, pid)
        con.execute(
            "DELETE FROM contratacoes WHERE numero_controle_pncp=? AND COALESCE(pncp_confirmado,0)=0",
            (pid,),
        )
        removidos += con.total_changes > 0
        if i % 1000 == 0:
            con.commit()
            print(f"    Registros CSV corrompidos tratados: {i:,}/{len(bad_ids):,}", flush=True)
    con.commit()
    stats["contratacoes_removidas"] = len(bad_ids)
    stats["itens_preservados"] = preservados
    print(
        f"[REPARO BANCO] Linhas válidas reaplicadas: {stats['linhas_validas']:,} · "
        f"linhas desalinhadas: {stats['linhas_desalinhadas']:,} · "
        f"linhas estruturais inválidas: {stats['linhas_estruturalmente_invalidas']:,} · "
        f"contratações CSV corrompidas removidas: {len(bad_ids):,} · "
        f"itens preservados: {preservados:,}",
        flush=True,
    )
    _gravar_checkpoint_reparo_compra(con, arquivos)
    print(f"[REPARO BANCO] Checkpoint salvo para {len(arquivos)} arquivo(s) COMPRA.", flush=True)
    return dict(stats)


def periodo_do_arquivo(path: Path) -> str | None:
    m = re.search(r"-(\d{4})-(\d{2})\.csv$", path.name, flags=re.I)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    m = re.search(r"-(\d{4})\.csv$", path.name, flags=re.I)
    if m:
        return f"{m.group(1)}-ANUAL"
    return None


def arquivos_item_cache() -> list[Path]:
    if not CSV_DIR.exists():
        return []
    return sorted(_deduplicar_cache_por_nome([
        p for p in CSV_DIR.rglob("*.csv")
        if "COMPRA_ITEM" in p.name.upper() and p.is_file()
    ]))


def _chave_item_params(params: tuple[Any, ...] | None) -> tuple[str, int] | None:
    if params is None:
        return None
    pid = params[COLS_ITEM.index("numero_controle_pncp")]
    ni = params[COLS_ITEM.index("numero_item")]
    if not pncp_id_valido(pid) or ni is None:
        return None
    try:
        ni_i = int(ni)
    except (TypeError, ValueError, OverflowError):
        return None
    # Número de item é um identificador ordinal; este limite é deliberadamente
    # amplo e serve apenas para barrar deslocamentos absurdos de coluna.
    if ni_i <= 0 or ni_i > 2_147_483_647:
        return None
    return str(pid), ni_i


def _reverter_recuperacoes_inseguras_0621(con: sqlite3.Connection) -> dict[str, int]:
    """Reverte recuperações automáticas feitas pela V0.6.2.1.

    A V0.6.2.1 tentava aproveitar uma linha cujo ID estava deslocado procurando um
    único ID PNCP válido em outro campo. Descobrimos em produção que, nesses casos,
    os demais campos da linha também podem estar deslocados. Portanto, a V0.6.2.4
    não considera mais segura essa reconstrução automática.

    Para não apagar informação correta, removemos apenas itens CSV-only associados
    às linhas marcadas como RECUPERADO e devolvemos a evidência à quarentena. Itens
    já confirmados pela API do PNCP são preservados. Os checkpoints de higiene são
    limpos uma única vez para que todos os arquivos sejam reavaliados sob a regra
    conservadora nova.
    """
    chave_mig = "v0622_reverter_recuperacao_linha_deslocada"
    feito = con.execute("SELECT 1 FROM radar_pncp_migracoes WHERE chave=?", (chave_mig,)).fetchone()
    if feito:
        return {"ja_executado": 1}

    rows = con.execute(
        """SELECT chave_linha,payload_json
             FROM comprasgov_itens_quarentena
            WHERE status='RECUPERADO'
              AND motivo='RECUPERADO_NA_REVARREDURA'
              AND payload_json IS NOT NULL"""
    ).fetchall()
    chaves: set[tuple[str, int]] = set()
    for r in rows:
        try:
            d = json.loads(r[1])
            pid = str(d.get("numero_controle_pncp") or "")
            ni = d.get("numero_item")
            if pncp_id_valido(pid) and ni is not None:
                ni_i = int(ni)
                if 0 < ni_i <= 2_147_483_647:
                    chaves.add((pid, ni_i))
        except (TypeError, ValueError, OverflowError, json.JSONDecodeError):
            continue

    apagados_itens = 0
    apagados_staging = 0
    for i, (pid, ni) in enumerate(chaves, 1):
        cur = con.execute(
            """DELETE FROM itens
                  WHERE numero_controle_pncp=? AND numero_item=?
                    AND COALESCE(pncp_confirmado,0)=0
                    AND COALESCE(fonte_primeira,'COMPRASGOV_CSV')='COMPRASGOV_CSV'""",
            (pid, ni),
        )
        apagados_itens += max(0, int(cur.rowcount or 0))
        cur = con.execute(
            "DELETE FROM comprasgov_itens_orfaos WHERE numero_controle_pncp=? AND numero_item=?",
            (pid, ni),
        )
        apagados_staging += max(0, int(cur.rowcount or 0))
        if i % 5000 == 0:
            con.commit()

    stamp = agora()
    con.execute(
        """UPDATE comprasgov_itens_quarentena
              SET status='RECUPERACAO_REVERTIDA_0622',
                  motivo='LINHA_COM_ID_DESLOCADO_QUARENTENA',
                  atualizado_em=?
            WHERE status='RECUPERADO'
              AND motivo='RECUPERADO_NA_REVARREDURA'""",
        (stamp,),
    )
    con.execute("DELETE FROM comprasgov_higiene_arquivos")
    con.execute(
        """INSERT INTO radar_pncp_migracoes(chave,executada_em,detalhes_json)
           VALUES (?,?,?)""",
        (chave_mig, stamp, json.dumps({
            "linhas_revertidas": len(rows),
            "chaves": len(chaves),
            "itens_csv_only_removidos": apagados_itens,
            "staging_removido": apagados_staging,
        }, ensure_ascii=False)),
    )
    con.commit()
    return {
        "linhas_revertidas": len(rows),
        "chaves": len(chaves),
        "itens_csv_only_removidos": apagados_itens,
        "staging_removido": apagados_staging,
    }


def _chaves_revertidas_0622(con: sqlite3.Connection) -> set[tuple[str, int]]:
    out: set[tuple[str, int]] = set()
    rows = con.execute(
        """SELECT payload_json FROM comprasgov_itens_quarentena
            WHERE status='RECUPERACAO_REVERTIDA_0622' AND payload_json IS NOT NULL"""
    ).fetchall()
    for r in rows:
        try:
            d = json.loads(r[0])
            pid = str(d.get("numero_controle_pncp") or "")
            ni = d.get("numero_item")
            if pncp_id_valido(pid) and ni is not None:
                ni_i = int(ni)
                if 0 < ni_i <= 2_147_483_647:
                    out.add((pid, ni_i))
        except (TypeError, ValueError, OverflowError, json.JSONDecodeError):
            pass
    return out


def revarrer_csvs_anomalos(con: sqlite3.Connection, job_id: int | None, jobs_db: Path | None) -> dict[str, int]:
    """Revarre linhas cujo ID primário está inválido e as mantém em quarentena.

    V0.7.6: encontrar um ID PNCP válido em outra coluna NÃO é suficiente para
    reconstruir a linha. Se o ID deslocou, outros campos também podem ter deslocado.
    A linha é preservada integralmente para auditoria, mas não é inserida em itens
    nem no staging operacional.
    """
    arquivos = arquivos_item_cache()
    stats = Counter()
    chaves_revertidas = _chaves_revertidas_0622(con)
    if chaves_revertidas:
        print(f"[REPARO 0.6.2.2] {len(chaves_revertidas):,} chave(s) de item serão revalidadas contra linhas íntegras.", flush=True)
    for ai, path in enumerate(arquivos, 1):
        if parada(job_id, jobs_db):
            raise InterruptedError
        periodo = periodo_do_arquivo(path) or "DESCONHECIDO"
        st_file = path.stat()
        meta = con.execute(
            "SELECT status,tamanho_bytes,mtime_ns,linhas_lidas,anomalias,recuperadas FROM comprasgov_higiene_arquivos WHERE arquivo=?",
            (str(path.resolve()),),
        ).fetchone()
        if meta and meta[0] == "CONCLUIDA" and int(meta[1]) == st_file.st_size and int(meta[2]) == st_file.st_mtime_ns:
            print(f"\n[HIGIENE CSV][{ai}/{len(arquivos)}] {path.name} — já verificado; pulando.", flush=True)
            stats["arquivos_cache_higiene"] += 1
            atualizar_job(job_id, jobs_db, f"Higiene CSV · {periodo}", ai, len(arquivos), path.name)
            continue
        con.execute(
            """INSERT INTO comprasgov_higiene_arquivos(arquivo,tamanho_bytes,mtime_ns,status,linhas_lidas,anomalias,recuperadas,atualizado_em)
               VALUES (?,?,?,'EXECUTANDO',0,0,0,?)
               ON CONFLICT(arquivo) DO UPDATE SET tamanho_bytes=excluded.tamanho_bytes,mtime_ns=excluded.mtime_ns,
                   status='EXECUTANDO',linhas_lidas=0,anomalias=0,recuperadas=0,atualizado_em=excluded.atualizado_em""",
            (str(path.resolve()), st_file.st_size, st_file.st_mtime_ns, agora()),
        )
        con.commit()
        print(f"\n[HIGIENE CSV][{ai}/{len(arquivos)}] {path.name}", flush=True)
        atualizar_job(job_id, jobs_db, f"Higiene CSV · {periodo}", ai - 1, len(arquivos), path.name)
        f, reader = abrir_csv(path)
        try:
            headers = reader.fieldnames or []
            resolvido = resolver_colunas(headers, ITEM_MAP)
            idcol = resolvido.get("numero_controle_pncp")
            if not idcol:
                print("    Sem coluna de ID reconhecida; arquivo ignorado nesta etapa.", flush=True)
                stats["arquivos_sem_id"] += 1
                continue
            lidas = 0
            file_anomalias = 0
            file_quarentena = 0
            stamp = agora()
            for row in reader:
                lidas += 1
                if lidas % 100000 == 0:
                    print(f"    Varredura: {lidas:,} linhas", flush=True)
                    if parada(job_id, jobs_db):
                        raise InterruptedError
                informado = texto(row.get(idcol))
                if pncp_id_valido(informado):
                    # Se uma chave removida por ter vindo de uma linha deslocada
                    # também aparece em uma linha estruturalmente íntegra, reaplica
                    # esta versão segura. Assim o rollback não perde duplicatas boas.
                    if chaves_revertidas:
                        params_ok, situacao_ok = parametros_item(row, resolvido, periodo, stamp)
                        chave_ok = _chave_item_params(params_ok) if situacao_ok == "VALIDO" else None
                        if chave_ok in chaves_revertidas:
                            validos, orfaos = separar_itens_com_pai(con, [params_ok])
                            if validos:
                                con.executemany(SQL_ITEM, validos)
                            if orfaos:
                                guardar_itens_orfaos(con, periodo, orfaos)
                            stats["reaplicadas_de_linha_valida"] += 1
                            if stats["reaplicadas_de_linha_valida"] % 1000 == 0:
                                con.commit()
                    continue

                stats["linhas_id_primario_invalido"] += 1
                file_anomalias += 1
                params, situacao = parametros_item(row, resolvido, periodo, stamp)
                candidato = _chave_item_params(params)
                motivo = "ID_PRIMARIO_INVALIDO"
                if linha_csv_desalinhada(row, headers):
                    motivo = "LINHA_DESALINHADA_ITEM"
                elif candidato is not None:
                    # Guardamos o candidato como evidência, sem importar a linha.
                    motivo = "ID_DESLOCADO_NAO_RECUPERADO_AUTOMATICAMENTE"
                elif situacao == "RECUPERADO_NA_LINHA":
                    motivo = "ID_CANDIDATO_COM_CAMPOS_INVALIDOS"
                elif situacao:
                    motivo = situacao

                qkey = guardar_item_quarentena(con, periodo, row, params, motivo)
                if candidato is not None:
                    con.execute(
                        """UPDATE comprasgov_itens_quarentena
                              SET id_recuperado=?,status='CANDIDATO_NAO_APLICADO',atualizado_em=?
                            WHERE chave_linha=?""",
                        (candidato[0], agora(), qkey),
                    )
                    stats["candidatos_nao_aplicados"] += 1
                stats["quarentena"] += 1
                file_quarentena += 1

                if file_quarentena % 2000 == 0:
                    con.commit()

            con.commit()
            print(
                f"    Anomalias encontradas: {stats['linhas_id_primario_invalido']:,} acumuladas · "
                f"mantidas em quarentena: {stats['quarentena']:,} acumuladas",
                flush=True,
            )
            con.execute(
                """UPDATE comprasgov_higiene_arquivos SET status='CONCLUIDA',linhas_lidas=?,
                          anomalias=?,recuperadas=0,atualizado_em=? WHERE arquivo=?""",
                (lidas, file_anomalias, agora(), str(path.resolve())),
            )
            con.commit()
        finally:
            f.close()
        atualizar_job(job_id, jobs_db, f"Higiene CSV · {periodo}", ai, len(arquivos), path.name)
    return dict(stats)

def classificar_fila(con: sqlite3.Connection) -> Counter:
    """Sincroniza uma linha de diagnóstico por contratação-pai ainda no staging."""
    grupos = con.execute(
        """SELECT numero_controle_pncp,COUNT(*) qtd
             FROM comprasgov_itens_orfaos
            GROUP BY numero_controle_pncp"""
    ).fetchall()
    vistos: set[str] = set()
    for r in grupos:
        pid = str(r[0])
        vistos.add(pid)
        cls = "ID_VALIDO" if pncp_id_valido(pid) else "ID_INVALIDO"
        ano = None
        dec = decompor_pncp_id(pid)
        if dec:
            ano = dec[1]
        con.execute(
            """INSERT INTO pncp_reconciliacao_staging(
                   numero_controle_pncp,classificacao,qtd_itens,ano_min,ano_max,status
               ) VALUES (?,?,?,?,?,'PENDENTE')
               ON CONFLICT(numero_controle_pncp) DO UPDATE SET
                   classificacao=excluded.classificacao,qtd_itens=excluded.qtd_itens,
                   ano_min=excluded.ano_min,ano_max=excluded.ano_max,
                   status=CASE
                       WHEN pncp_reconciliacao_staging.status IN ('RESOLVIDA','JA_EXISTIA')
                            THEN pncp_reconciliacao_staging.status
                       ELSE pncp_reconciliacao_staging.status END""",
            (pid, cls, int(r[1]), ano, ano),
        )
    # Pais que saíram do staging desde a última execução são marcados resolvidos.
    rows_antigas = con.execute("SELECT numero_controle_pncp,status FROM pncp_reconciliacao_staging").fetchall()
    status_terminais = {"RESOLVIDA", "JA_EXISTIA", "REMOVIDA_PNCP", "NAO_ENCONTRADA"}
    for r in rows_antigas:
        pid = str(r[0])
        if pid not in vistos and str(r[1]) not in status_terminais:
            con.execute(
                "UPDATE pncp_reconciliacao_staging SET status='RESOLVIDA',resolvido_em=? WHERE numero_controle_pncp=?",
                (agora(), pid),
            )
    con.commit()
    return Counter(
        {str(r[0]): int(r[1]) for r in con.execute(
            "SELECT status,COUNT(*) FROM pncp_reconciliacao_staging GROUP BY status"
        ).fetchall()}
    )


def resolver_existentes(con: sqlite3.Connection, job_id: int | None, jobs_db: Path | None) -> int:
    antes = contar_orfaos(con)
    if not antes:
        return 0
    print(f"\n[STAGING] Reavaliando {antes:,} item(ns) contra os pais já existentes...", flush=True)
    def prog(a: int, t: int) -> None:
        atualizar_job(job_id, jobs_db, "Resolvendo staging local", a, t)
        if a == t or (a and a % 10000 == 0):
            print(f"    Resolvidos localmente: {a:,}/{t:,}", flush=True)
        if parada(job_id, jobs_db):
            raise InterruptedError
    resolvidos, por_mes = resolver_itens_orfaos(con, progress_callback=prog)
    registrar_orfaos_resolvidos(con, por_mes)
    classificar_fila(con)
    print(f"    Movidos para itens: {resolvidos:,}", flush=True)
    return resolvidos


def marcar_fila(con: sqlite3.Connection, pid: str, status: str, http: int | None, msg: str | None, sucesso: bool = False) -> None:
    stamp = agora()
    con.execute(
        """UPDATE pncp_reconciliacao_staging
              SET status=?,tentativas=tentativas+1,ultimo_http=?,ultima_mensagem=?,
                  primeira_tentativa_em=COALESCE(primeira_tentativa_em,?),
                  ultima_tentativa_em=?,resolvido_em=CASE WHEN ? THEN ? ELSE resolvido_em END
            WHERE numero_controle_pncp=?""",
        (status, http, (msg or "")[:2000] or None, stamp, stamp, int(sucesso), stamp, pid),
    )


def normalizar_status_fila(con: sqlite3.Connection) -> Counter:
    """Corrige rótulos herdados das tentativas 0.6.2.2/0.6.2.3.

    301 era a rota antiga; 410 indica recurso removido; 429 é rate limit, não
    erro fatal. Respostas 5xx/rede continuam transitórias. Pais já presentes são
    resolvidos por classificar_fila.
    """
    stats = Counter()
    cur = con.execute(
        """UPDATE pncp_reconciliacao_staging SET status='REMOVIDA_PNCP'
             WHERE status NOT IN ('RESOLVIDA','JA_EXISTIA','REMOVIDA_PNCP')
               AND (
                    ultimo_http=410
                    OR lower(COALESCE(ultima_mensagem,'')) LIKE '%http 410%'
                    OR lower(COALESCE(ultima_mensagem,'')) LIKE '%410 gone%'
               )"""
    )
    stats["410_para_removida"] = max(0, int(cur.rowcount or 0))
    cur = con.execute(
        """UPDATE pncp_reconciliacao_staging SET status='PENDENTE'
             WHERE status NOT IN ('RESOLVIDA','JA_EXISTIA','REMOVIDA_PNCP')
               AND (
                    ultimo_http=301
                    OR (status='ERRO' AND lower(COALESCE(ultima_mensagem,'')) LIKE '%endpoint foi movido%')
                    OR (status='ERRO' AND lower(COALESCE(ultima_mensagem,'')) LIKE '%301%')
               )"""
    )
    stats["301_para_pendente"] = max(0, int(cur.rowcount or 0))
    cur = con.execute(
        """UPDATE pncp_reconciliacao_staging SET status='AGUARDANDO_RATE_LIMIT'
             WHERE status NOT IN ('RESOLVIDA','JA_EXISTIA','REMOVIDA_PNCP')
               AND (
                    ultimo_http=429
                    OR (status='ERRO' AND lower(COALESCE(ultima_mensagem,'')) LIKE '%429%')
                    OR (status='ERRO' AND lower(COALESCE(ultima_mensagem,'')) LIKE '%too many%')
               )"""
    )
    stats["429_reclassificados"] = max(0, int(cur.rowcount or 0))
    ph = ",".join("?" for _ in (408,425,500,502,503,504))
    cur = con.execute(
        f"""UPDATE pncp_reconciliacao_staging SET status='ERRO_TRANSITORIO'
              WHERE status NOT IN ('RESOLVIDA','JA_EXISTIA','REMOVIDA_PNCP')
                AND (
                     ultimo_http IN ({ph})
                     OR (status='ERRO' AND lower(COALESCE(ultima_mensagem,'')) LIKE '%connectionerror%')
                     OR (status='ERRO' AND lower(COALESCE(ultima_mensagem,'')) LIKE '%timeout%')
                     OR (status='ERRO' AND lower(COALESCE(ultima_mensagem,'')) LIKE '%remote end closed%')
                )""",
        (408,425,500,502,503,504),
    )
    stats["transitorios_reclassificados"] = max(0, int(cur.rowcount or 0))
    con.commit()
    return stats


def arquivar_itens_removidos_pncp(con: sqlite3.Connection) -> dict[str, int]:
    """Retira do staging operacional itens cujo pai foi confirmado como 410 Gone.

    Os itens não são apagados: cada linha é preservada em comprasgov_itens_quarentena
    com status REMOVIDA_PNCP e motivo PAI_REMOVIDO_PNCP. A linha da fila por pai
    permanece em pncp_reconciliacao_staging como tombstone/auditoria.
    """
    rows = con.execute(
        """SELECT o.numero_controle_pncp,o.numero_item,o.ano_mes,o.payload_json,
                  r.ultimo_http,r.ultima_mensagem
             FROM comprasgov_itens_orfaos o
             JOIN pncp_reconciliacao_staging r
               ON r.numero_controle_pncp=o.numero_controle_pncp
            WHERE r.status='REMOVIDA_PNCP'
            ORDER BY o.numero_controle_pncp,o.numero_item"""
    ).fetchall()
    if not rows:
        return {"itens": 0, "pais": 0}

    pais: set[str] = set()
    stamp = agora()
    for r in rows:
        pid = str(r[0])
        numero_item = int(r[1])
        periodo = str(r[2])
        payload = str(r[3] or "{}")
        pais.add(pid)
        base = f"REMOVIDA_PNCP\n{pid}\n{numero_item}\n{periodo}\n{payload}"
        key = hashlib.sha256(base.encode("utf-8", errors="replace")).hexdigest()
        detalhe = str(r[5] or "PNCP retornou 410 Gone")
        raw = json.dumps(
            {
                "origem": "comprasgov_itens_orfaos",
                "numero_controle_pncp": pid,
                "numero_item": numero_item,
                "ano_mes": periodo,
                "ultimo_http": r[4],
                "mensagem": detalhe,
            },
            ensure_ascii=False,
        )
        con.execute(
            """INSERT INTO comprasgov_itens_quarentena(
                   chave_linha,ano_mes,numero_controle_informado,numero_item,motivo,
                   payload_json,raw_row_json,id_recuperado,status,criado_em,atualizado_em
               ) VALUES (?,?,?,?,?,?,?,NULL,'REMOVIDA_PNCP',?,?)
               ON CONFLICT(chave_linha) DO UPDATE SET
                   motivo=excluded.motivo,
                   payload_json=COALESCE(excluded.payload_json,comprasgov_itens_quarentena.payload_json),
                   raw_row_json=COALESCE(excluded.raw_row_json,comprasgov_itens_quarentena.raw_row_json),
                   status='REMOVIDA_PNCP',
                   atualizado_em=excluded.atualizado_em""",
            (key, periodo, pid, numero_item, "PAI_REMOVIDO_PNCP", payload, raw, stamp, stamp),
        )

    con.executemany(
        "DELETE FROM comprasgov_itens_orfaos WHERE numero_controle_pncp=? AND numero_item=?",
        [(str(r[0]), int(r[1])) for r in rows],
    )
    con.commit()
    return {"itens": len(rows), "pais": len(pais)}


def consultar_pais(con: sqlite3.Connection, job_id: int | None, jobs_db: Path | None, limite: int | None, pausa: float) -> dict[str, int]:
    classificar_fila(con)
    norm = normalizar_status_fila(con)
    if sum(norm.values()):
        print(
            "\n[FILA] Status legados normalizados: "
            f"301→PENDENTE {norm['301_para_pendente']:,} · "
            f"410→REMOVIDA {norm['410_para_removida']:,} · "
            f"429→RATE_LIMIT {norm['429_reclassificados']:,} · "
            f"5xx→TRANSITÓRIO {norm['transitorios_reclassificados']:,}",
            flush=True,
        )

    sql = """SELECT r.numero_controle_pncp,r.qtd_itens
               FROM pncp_reconciliacao_staging r
               LEFT JOIN contratacoes c ON c.numero_controle_pncp=r.numero_controle_pncp
              WHERE r.classificacao='ID_VALIDO'
                AND c.numero_controle_pncp IS NULL
                AND r.status IN ('PENDENTE','ERRO_TRANSITORIO','ERRO','AGUARDANDO_RATE_LIMIT')
              ORDER BY r.qtd_itens DESC,r.numero_controle_pncp"""
    params: list[Any] = []
    if limite and limite > 0:
        sql += " LIMIT ?"; params.append(int(limite))
    rows = con.execute(sql, params).fetchall()
    total = len(rows)
    stats = Counter()
    if not total:
        print("\n[PNCP] Nenhuma contratação-pai válida ausente aguardando consulta.", flush=True)
        return dict(stats)

    intervalo = max(1.0, float(pausa or 1.25))
    print(f"\n[PNCP] Consultando {total:,} contratação(ões)-pai realmente ausente(s)...", flush=True)
    print(f"    Ritmo inicial: 1 requisição a cada {intervalo:.2f} s; 429 ativa cooldown adaptativo.", flush=True)
    sessao, limiter = criar_sessao_controlada(intervalo)
    sessao.set_parada_callback(lambda: parada(job_id, jobs_db))

    print(f"    Preflight da API: {BASE_CONSULTA}/orgaos/{{cnpj}}/compras/{{ano}}/{{sequencial}}", flush=True)
    preflight_ok = False
    preflight_fatais: list[tuple[int | None, str]] = []
    preflight_probe_pid: str | None = None
    preflight_probe_url: str | None = None
    for pr in rows[: min(3, total)]:
        ppid = str(pr[0])
        pdec = decompor_pncp_id(ppid)
        if not pdec:
            continue
        pcnpj, pano, pseq = pdec
        purl = f"{BASE_CONSULTA}/orgaos/{pcnpj}/compras/{pano}/{pseq}"
        if preflight_probe_url is None:
            preflight_probe_pid, preflight_probe_url = ppid, purl
        for _tent in range(3):
            try:
                presp = sessao.get(purl, timeout=(20, 60))
                pst = int(presp.status_code)
                if pst in (200, 404, 410):
                    preflight_ok = True
                    sufixo = " · API operacional; recurso removido" if pst == 410 else ""
                    print(f"    Preflight OK: HTTP {pst} ({ppid}){sufixo}", flush=True)
                    break
                if pst == 429:
                    marcar_fila(con, ppid, "AGUARDANDO_RATE_LIMIT", 429, "Rate limit detectado no preflight")
                    con.commit(); stats["429"] += 1
                    espera, parar_rate = limiter.cooldown_429(presp, lambda: parada(job_id, jobs_db))
                    print(f"    PNCP limitou requisições no preflight; aguardado cooldown de {espera:.0f} s.", flush=True)
                    if parar_rate:
                        stats["rate_limit_pause"] = 1
                        break
                    continue
                if pst in (408,425,500,502,503,504):
                    stats["preflight_transitorio"] += 1
                    espera = limiter.backoff_transitorio(min(4, stats["preflight_transitorio"]), lambda: parada(job_id, jobs_db))
                    print(f"    Preflight HTTP {pst}; aguardado backoff de {espera:.0f} s.", flush=True)
                    continue
                preflight_fatais.append((pst, (presp.text or '')[:500])); break
            except InterruptedError:
                raise
            except (requests.Timeout, requests.ConnectionError) as exc:
                stats["preflight_transitorio"] += 1
                espera = limiter.backoff_transitorio(min(4, stats["preflight_transitorio"]), lambda: parada(job_id, jobs_db))
                print(f"    Preflight {type(exc).__name__}; aguardado backoff de {espera:.0f} s.", flush=True)
            except Exception as exc:
                preflight_fatais.append((None, f"{type(exc).__name__}: {exc}")); break
        if preflight_ok or stats.get("rate_limit_pause"):
            break

    if not preflight_ok:
        sessao.close()
        if stats.get("rate_limit_pause"):
            print("    Rate limit persistente no preflight; fila preservada para retomada.", flush=True)
            return dict(stats)
        if preflight_fatais:
            st, msg = preflight_fatais[0]
            print(f"    PREFLIGHT FALHOU: HTTP {st if st is not None else '-'} · {msg[:300]}", flush=True)
            print("    Fila NÃO será percorrida.", flush=True)
            stats["preflight_falhou"] = 1
            return dict(stats)
        print("    PNCP instável no preflight; fila preservada para nova tentativa.", flush=True)
        stats["outage"] = 1
        return dict(stats)

    def preflight_recuperacao(rotulo: str) -> bool:
        if not preflight_probe_url:
            return False
        try:
            resp_probe = sessao.get(preflight_probe_url, timeout=(20, 60))
            st_probe = int(resp_probe.status_code)
            if st_probe in (200, 404, 410):
                sufixo = " · API operacional; recurso removido" if st_probe == 410 else ""
                print(
                    f"    {rotulo}: PNCP respondeu HTTP {st_probe}"
                    + (f" ({preflight_probe_pid})" if preflight_probe_pid else "")
                    + sufixo,
                    flush=True,
                )
                return True
            if st_probe == 429:
                stats["429"] += 1
                print(f"    {rotulo}: HTTP 429; aplicando cooldown antes de nova avaliação.", flush=True)
                espera, _ = limiter.cooldown_429(resp_probe, lambda: parada(job_id, jobs_db))
                print(f"    Cooldown adicional de {espera:.0f} s concluído.", flush=True)
                return False
            if st_probe in (408,425,500,502,503,504):
                print(f"    {rotulo}: PNCP ainda instável (HTTP {st_probe}).", flush=True)
                return False
            print(f"    {rotulo}: resposta inesperada HTTP {st_probe}; preservando fila.", flush=True)
            return False
        except InterruptedError:
            raise
        except (requests.Timeout, requests.ConnectionError) as exc:
            print(f"    {rotulo}: {type(exc).__name__}; PNCP ainda instável.", flush=True)
            return False
        except Exception as exc:
            print(f"    {rotulo}: {type(exc).__name__}: {exc}", flush=True)
            return False

    consecutivos_transitorios = 0
    consecutivos_fatais = 0
    ultimo_fatal: int | None = None
    interromper = False
    try:
        for i, r in enumerate(rows, 1):
            if parada(job_id, jobs_db):
                raise InterruptedError
            pid = str(r[0])
            dec = decompor_pncp_id(pid)
            if not dec:
                marcar_fila(con, pid, "ID_INVALIDO", None, "Falhou validação estrutural")
                con.commit(); stats["invalidos"] += 1; continue
            cnpj, ano, seq = dec
            url = f"{BASE_CONSULTA}/orgaos/{cnpj}/compras/{ano}/{seq}"
            atualizar_job(job_id, jobs_db, "Reconciliação PNCP · pais", i - 1, total, pid)

            while True:
                try:
                    resp = sessao.get(url, timeout=(20, 90))
                    status = int(resp.status_code)
                    if status == 200:
                        payload = resp.json()
                        if not isinstance(payload, dict):
                            raise ValueError("Resposta PNCP 200 não é um objeto JSON.")
                        salvar_contratacao(con, payload)
                        marcar_fila(con, pid, "PAI_IMPORTADO", status, "Contratação recuperada no PNCP", sucesso=True)
                        con.commit(); stats["importados"] += 1
                        consecutivos_transitorios = 0; consecutivos_fatais = 0; ultimo_fatal = None
                        break
                    if status == 404:
                        marcar_fila(con, pid, "NAO_ENCONTRADA", status, "PNCP retornou 404")
                        con.commit(); stats["404"] += 1
                        consecutivos_transitorios = 0; consecutivos_fatais = 0; ultimo_fatal = None
                        break
                    if status == 410:
                        marcar_fila(con, pid, "REMOVIDA_PNCP", status, "PNCP retornou 410 Gone")
                        con.commit(); stats["410"] += 1
                        consecutivos_transitorios = 0; consecutivos_fatais = 0; ultimo_fatal = None
                        break
                    if status == 429:
                        stats["429"] += 1
                        marcar_fila(con, pid, "AGUARDANDO_RATE_LIMIT", 429, "HTTP 429 — aguardando cooldown")
                        con.commit()
                        print(f"    HTTP 429 em {pid}. Entrando em cooldown...", flush=True)
                        espera, parar_rate = limiter.cooldown_429(resp, lambda: parada(job_id, jobs_db))
                        print(f"    Cooldown concluído após {espera:.0f} s; ritmo atual {limiter.intervalo:.2f} s/requisição.", flush=True)
                        if parar_rate:
                            print("    Rate limit persistente após 4 cooldowns; encerrando com checkpoint preservado.", flush=True)
                            stats["rate_limit_pause"] = 1; interromper = True; break
                        continue
                    if status in (408,425,500,502,503,504):
                        marcar_fila(con, pid, "ERRO_TRANSITORIO", status, f"HTTP {status}")
                        con.commit(); stats["transitorios"] += 1; consecutivos_transitorios += 1
                        consecutivos_fatais = 0; ultimo_fatal = None
                        espera = limiter.backoff_transitorio(min(4, consecutivos_transitorios), lambda: parada(job_id, jobs_db))
                        print(f"    HTTP {status}; backoff de {espera:.0f} s.", flush=True)
                        break
                    marcar_fila(con, pid, "ERRO", status, (resp.text or f"HTTP {status}")[:1000])
                    con.commit(); stats["erros"] += 1; consecutivos_transitorios = 0
                    consecutivos_fatais = consecutivos_fatais + 1 if ultimo_fatal == status else 1
                    ultimo_fatal = status
                    break
                except InterruptedError:
                    raise
                except (requests.Timeout, requests.ConnectionError) as exc:
                    marcar_fila(con, pid, "ERRO_TRANSITORIO", None, f"{type(exc).__name__}: {exc}")
                    con.commit(); stats["transitorios"] += 1; consecutivos_transitorios += 1
                    consecutivos_fatais = 0; ultimo_fatal = None
                    espera = limiter.backoff_transitorio(min(4, consecutivos_transitorios), lambda: parada(job_id, jobs_db))
                    print(f"    {type(exc).__name__}; backoff de {espera:.0f} s.", flush=True)
                    break
                except Exception as exc:
                    marcar_fila(con, pid, "ERRO", None, f"{type(exc).__name__}: {exc}")
                    con.commit(); stats["erros"] += 1; consecutivos_transitorios = 0
                    break

            if i == total or i % 100 == 0:
                print(
                    f"    {i:,}/{total:,} · pais importados {stats['importados']:,} · "
                    f"404 {stats['404']:,} · 410 {stats['410']:,} · 429 {stats['429']:,} · transitórios {stats['transitorios']:,}",
                    flush=True,
                )
            if interromper:
                break
            if consecutivos_transitorios >= 5:
                stats["pausas_globais"] += 1
                print(
                    "    5 falhas transitórias consecutivas. Pausa global de 120 s antes de testar o PNCP novamente...",
                    flush=True,
                )
                limiter.pausa_global(120, lambda: parada(job_id, jobs_db))
                if preflight_recuperacao("Preflight após pausa de 120 s"):
                    consecutivos_transitorios = 0
                    print(
                        f"    PNCP recuperado; retomando fila em {limiter.intervalo:.2f} s/requisição.",
                        flush=True,
                    )
                    continue

                stats["pausas_globais"] += 1
                print(
                    "    PNCP ainda instável. Segunda pausa global de 300 s antes da última tentativa...",
                    flush=True,
                )
                limiter.pausa_global(300, lambda: parada(job_id, jobs_db))
                if preflight_recuperacao("Preflight após pausa de 300 s"):
                    consecutivos_transitorios = 0
                    print(
                        f"    PNCP recuperado; retomando fila em {limiter.intervalo:.2f} s/requisição.",
                        flush=True,
                    )
                    continue

                print(
                    "    PNCP permaneceu instável após as duas pausas globais; encerrando PARCIAL com checkpoint preservado.",
                    flush=True,
                )
                stats["outage"] = 1
                break
            if consecutivos_fatais >= 5:
                print(f"    Circuit breaker: 5 respostas HTTP {ultimo_fatal} consecutivas; fila interrompida para diagnóstico.", flush=True)
                stats["circuit_breaker"] = 1; break
            atualizar_job(job_id, jobs_db, "Reconciliação PNCP · pais", i, total, pid)
    finally:
        sessao.close()
    return dict(stats)

def auditar_integridade_reconciliacao(con: sqlite3.Connection) -> dict[str, int]:
    """Checagens estruturais baratas antes de encerrar cada tentativa.

    A auditoria não exige que a API tenha resolvido toda a fila; ela verifica
    apenas invariantes que devem permanecer verdadeiros mesmo em uma execução
    PARCIAL ou interrompida entre retomadas.
    """
    ids = [str(r[0] or "") for r in con.execute(
        "SELECT DISTINCT numero_controle_pncp FROM comprasgov_itens_orfaos"
    ).fetchall()]
    invalidos = sum(1 for pid in ids if not pncp_id_valido(pid))
    com_pai = int(con.execute(
        """SELECT COUNT(DISTINCT o.numero_controle_pncp)
             FROM comprasgov_itens_orfaos o
             JOIN contratacoes c ON c.numero_controle_pncp=o.numero_controle_pncp"""
    ).fetchone()[0] or 0)
    importado_sem_pai = int(con.execute(
        """SELECT COUNT(*)
             FROM pncp_reconciliacao_staging r
             LEFT JOIN contratacoes c ON c.numero_controle_pncp=r.numero_controle_pncp
            WHERE r.status='PAI_IMPORTADO' AND c.numero_controle_pncp IS NULL"""
    ).fetchone()[0] or 0)
    resolvida_ainda_staging = int(con.execute(
        """SELECT COUNT(*)
             FROM pncp_reconciliacao_staging r
            WHERE r.status IN ('RESOLVIDA','JA_EXISTIA')
              AND EXISTS (SELECT 1 FROM comprasgov_itens_orfaos o
                           WHERE o.numero_controle_pncp=r.numero_controle_pncp)"""
    ).fetchone()[0] or 0)
    removidos_ainda_staging = int(con.execute(
        """SELECT COUNT(*)
             FROM comprasgov_itens_orfaos o
             JOIN pncp_reconciliacao_staging r
               ON r.numero_controle_pncp=o.numero_controle_pncp
            WHERE r.status='REMOVIDA_PNCP'"""
    ).fetchone()[0] or 0)
    pendentes_api = int(con.execute(
        """SELECT COUNT(*) FROM pncp_reconciliacao_staging
            WHERE status IN ('PENDENTE','ERRO_TRANSITORIO','ERRO','AGUARDANDO_RATE_LIMIT')"""
    ).fetchone()[0] or 0)
    return {
        "ids_invalidos_no_staging": invalidos,
        "pais_ja_existentes_ainda_no_staging": com_pai,
        "pai_importado_sem_registro_pai": importado_sem_pai,
        "fila_resolvida_ainda_no_staging": resolvida_ainda_staging,
        "removidos_pncp_ainda_no_staging": removidos_ainda_staging,
        "pais_pendentes_api": pendentes_api,
        "ok_estrutural": int(
            invalidos == 0 and com_pai == 0 and importado_sem_pai == 0
            and resolvida_ainda_staging == 0 and removidos_ainda_staging == 0
        ),
    }


def gerar_relatorio_auditoria(auditoria: dict[str, int]) -> Path:
    outdir = DATA_DIR / "diagnosticos"
    outdir.mkdir(parents=True, exist_ok=True)
    path = outdir / f"auditoria_reconciliacao_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["verificacao", "valor", "resultado"])
        for chave, valor in auditoria.items():
            if chave == "ok_estrutural":
                resultado = "OK" if valor else "FALHA"
            elif chave == "pais_pendentes_api":
                resultado = "INFORMATIVO"
            else:
                resultado = "OK" if int(valor) == 0 else "FALHA"
            w.writerow([chave, int(valor), resultado])
    return path


def gerar_relatorio(con: sqlite3.Connection) -> Path:
    outdir = DATA_DIR / "diagnosticos"
    outdir.mkdir(parents=True, exist_ok=True)
    path = outdir / f"reconciliacao_staging_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    rows = con.execute(
        """SELECT r.numero_controle_pncp,r.classificacao,r.qtd_itens,r.status,r.tentativas,
                  r.ultimo_http,r.ultima_mensagem,r.ultima_tentativa_em,
                  CASE WHEN c.numero_controle_pncp IS NULL THEN 0 ELSE 1 END AS pai_no_banco
             FROM pncp_reconciliacao_staging r
             LEFT JOIN contratacoes c ON c.numero_controle_pncp=r.numero_controle_pncp
            ORDER BY r.status,r.qtd_itens DESC,r.numero_controle_pncp"""
    ).fetchall()
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["numero_controle_pncp","classificacao","qtd_itens","status","tentativas","http","mensagem","ultima_tentativa","pai_no_banco"])
        for r in rows:
            w.writerow(list(r))
    return path


def resumo(con: sqlite3.Connection) -> dict[str, Any]:
    staging = contar_orfaos(con)
    pais = int(con.execute("SELECT COUNT(DISTINCT numero_controle_pncp) FROM comprasgov_itens_orfaos").fetchone()[0] or 0)
    invalidos_staging = int(con.execute(
        "SELECT COUNT(*) FROM comprasgov_itens_orfaos WHERE numero_controle_pncp NOT GLOB '[0-9]*-[0-9]-[0-9][0-9][0-9][0-9][0-9][0-9]/[0-9][0-9][0-9][0-9]'"
    ).fetchone()[0] or 0)
    quarentena = int(con.execute("SELECT COUNT(*) FROM comprasgov_itens_quarentena").fetchone()[0] or 0)
    qrec = int(con.execute("SELECT COUNT(*) FROM comprasgov_itens_quarentena WHERE status IN ('RECUPERADO','SUPERADO_POR_REVARREDURA')").fetchone()[0] or 0)
    qrem = int(con.execute("SELECT COUNT(*) FROM comprasgov_itens_quarentena WHERE status='REMOVIDA_PNCP'").fetchone()[0] or 0)
    fila = {str(r[0]): int(r[1]) for r in con.execute("SELECT status,COUNT(*) FROM pncp_reconciliacao_staging GROUP BY status").fetchall()}
    return {"staging": staging, "pais": pais, "invalidos_staging": invalidos_staging, "quarentena": quarentena, "quarentena_recuperada": qrec, "quarentena_removida_pncp": qrem, "fila": fila}


def main() -> int:
    ap = argparse.ArgumentParser(description="Reparo CSV + reconciliação do staging com PNCP — V0.7.6")
    ap.add_argument("--db", type=Path, default=DB_COMPRAS)
    ap.add_argument("--jobs-db", type=Path, default=DB_JOBS)
    ap.add_argument("--job-id", type=int)
    ap.add_argument("--limite-pais", type=int, default=0, help="0 = todos os pais válidos pendentes")
    ap.add_argument("--pausa", type=float, default=1.25, help="intervalo inicial mínimo entre requisições PNCP")
    ap.add_argument("--sem-revarrer-csv", action="store_true")
    args = ap.parse_args()
    jobs_db = args.jobs_db if args.job_id else None

    garantir_diretorios()
    print("=" * 92, flush=True)
    print("Radar PNCP / Radar PNCP V0.7.6 — REPARO CSV + HIGIENE + RECONCILIAÇÃO PNCP", flush=True)
    print("=" * 92, flush=True)
    print(f"Banco: {args.db.resolve()}", flush=True)
    print("Regra: somente IDs PNCP estruturalmente válidos podem chegar à fila da API.", flush=True)

    con: sqlite3.Connection | None = None
    try:
        con = conectar_db(args.db)
        inicial = resumo(con)
        print(f"\n[INÍCIO] Staging: {inicial['staging']:,} item(ns) · {inicial['pais']:,} pai(s) distinto(s)", flush=True)

        rollback_0621 = _reverter_recuperacoes_inseguras_0621(con)
        if not rollback_0621.get("ja_executado"):
            print(
                "[REPARO 0.6.2.2] Recuperações automáticas da 0.6.2.1 revertidas: "
                f"{int(rollback_0621.get('linhas_revertidas',0)):,} linha(s); "
                f"{int(rollback_0621.get('itens_csv_only_removidos',0)):,} item(ns) CSV-only removido(s); "
                f"{int(rollback_0621.get('staging_removido',0)):,} entrada(s) de staging removida(s).",
                flush=True,
            )

        atualizar_job(args.job_id, jobs_db, "Reparando integridade CSV", 0, 1)
        reparo_banco = reparar_contratacoes_csv(con, args.job_id, jobs_db)
        atualizar_job(args.job_id, jobs_db, "Reparando integridade CSV", 1, 1)

        atualizar_job(args.job_id, jobs_db, "Higienizando staging", 0, 1)
        movidos = higienizar_staging_legado(con)
        print(f"[HIGIENE] Entradas legadas com ID malformado movidas para quarentena: {movidos:,}", flush=True)
        atualizar_job(args.job_id, jobs_db, "Higienizando staging", 1, 1)

        rec_csv: dict[str, int] = {}
        if not args.sem_revarrer_csv:
            rec_csv = revarrer_csvs_anomalos(con, args.job_id, jobs_db)

        locais_antes = resolver_existentes(con, args.job_id, jobs_db)
        classificar_fila(con)
        arquivados_antes = arquivar_itens_removidos_pncp(con)
        if arquivados_antes["itens"]:
            print(
                f"[STAGING] Removidos PNCP arquivados: {arquivados_antes['itens']:,} item(ns) "
                f"de {arquivados_antes['pais']:,} pai(s); retirados da fila operacional.",
                flush=True,
            )
            classificar_fila(con)
        api_stats = consultar_pais(
            con, args.job_id, jobs_db,
            args.limite_pais if args.limite_pais > 0 else None,
            max(0.0, args.pausa),
        )
        arquivados_depois = arquivar_itens_removidos_pncp(con)
        if arquivados_depois["itens"]:
            print(
                f"[STAGING] Novos 410 arquivados: {arquivados_depois['itens']:,} item(ns) "
                f"de {arquivados_depois['pais']:,} pai(s).",
                flush=True,
            )
        locais_depois = resolver_existentes(con, args.job_id, jobs_db)
        classificar_fila(con)

        auditoria = auditar_integridade_reconciliacao(con)
        rel_auditoria = gerar_relatorio_auditoria(auditoria)
        print("\n[AUDITORIA] Integridade estrutural do reparo:", flush=True)
        print(f"    IDs inválidos ainda no staging: {auditoria['ids_invalidos_no_staging']:,}", flush=True)
        print(f"    Pais já existentes ainda no staging: {auditoria['pais_ja_existentes_ainda_no_staging']:,}", flush=True)
        print(f"    PAI_IMPORTADO sem contratação-pai: {auditoria['pai_importado_sem_registro_pai']:,}", flush=True)
        print(f"    Fila resolvida ainda no staging: {auditoria['fila_resolvida_ainda_no_staging']:,}", flush=True)
        print(f"    Itens de pais removidos ainda no staging: {auditoria['removidos_pncp_ainda_no_staging']:,}", flush=True)
        print(f"    Pais ainda aguardando API: {auditoria['pais_pendentes_api']:,}", flush=True)
        print(f"    Resultado estrutural: {'OK' if auditoria['ok_estrutural'] else 'FALHA'}", flush=True)
        print(f"    Relatório: {rel_auditoria}", flush=True)

        rel = gerar_relatorio(con)
        fim = resumo(con)
        print("\n" + "=" * 92, flush=True)
        print("RESUMO DA RECONCILIAÇÃO", flush=True)
        print("=" * 92, flush=True)
        print(f"Staging no início:                         {inicial['staging']:,} item(ns)", flush=True)
        print(f"Contratações CSV corrompidas removidas:    {int(reparo_banco.get('contratacoes_removidas',0)):,}", flush=True)
        print(f"Itens preservados durante reparo:          {int(reparo_banco.get('itens_preservados',0)):,}", flush=True)
        print(f"Linhas COMPRA desalinhadas detectadas:     {int(reparo_banco.get('linhas_desalinhadas',0)):,}", flush=True)
        print(f"Linhas COMPRA estruturalmente inválidas:   {int(reparo_banco.get('linhas_estruturalmente_invalidas',0)):,}", flush=True)
        print(f"IDs legados movidos à quarentena:          {movidos:,} item(ns)", flush=True)
        print(f"Linhas anômalas mantidas em quarentena:    {int(rec_csv.get('quarentena',0)):,}", flush=True)
        print(f"Candidatos de ID não aplicados:             {int(rec_csv.get('candidatos_nao_aplicados',0)):,}", flush=True)
        print(f"Chaves reaplicadas de linha íntegra:        {int(rec_csv.get('reaplicadas_de_linha_valida',0)):,}", flush=True)
        print(f"Itens resolvidos por pai já existente:     {locais_antes:,}", flush=True)
        print(f"Pais importados diretamente do PNCP:       {int(api_stats.get('importados',0)):,}", flush=True)
        print(f"HTTP 404 nesta execução:                   {int(api_stats.get('404',0)):,}", flush=True)
        print(f"HTTP 410 (recurso removido) nesta execução: {int(api_stats.get('410',0)):,}", flush=True)
        print(f"HTTP 429 observados nesta execução:        {int(api_stats.get('429',0)):,}", flush=True)
        print(f"Falhas transitórias no preflight:          {int(api_stats.get('preflight_transitorio',0)):,}", flush=True)
        print(f"Falhas transitórias durante a fila:        {int(api_stats.get('transitorios',0)):,}", flush=True)
        print(f"Pausas globais por instabilidade:          {int(api_stats.get('pausas_globais',0)):,}", flush=True)
        print(f"Arquivos COMPRA reaproveitados por checkpoint: {int(reparo_banco.get('arquivos_cache_reparo',0)):,}", flush=True)
        print(f"Itens resolvidos após consulta ao PNCP:    {locais_depois:,}", flush=True)
        print(f"Itens 410 arquivados nesta execução:       {arquivados_antes['itens'] + arquivados_depois['itens']:,}", flush=True)
        print(f"Itens 410 arquivados no total:             {fim['quarentena_removida_pncp']:,}", flush=True)
        print(f"Staging operacional ao final:              {fim['staging']:,} item(ns)", flush=True)
        print(f"Pais distintos ainda no staging:           {fim['pais']:,}", flush=True)
        print(f"Quarentena total:                          {fim['quarentena']:,} linha(s)", flush=True)
        print(f"Quarentena recuperada:                     {fim['quarentena_recuperada']:,} linha(s)", flush=True)
        if fim["fila"]:
            print("Fila por status:", flush=True)
            for k, v in sorted(fim["fila"].items()):
                print(f"  {k}: {v:,}", flush=True)
        print(f"Relatório da fila: {rel}", flush=True)
        print(f"Auditoria estrutural: {rel_auditoria}", flush=True)

        con.commit(); con.close(); con = None
        pendentes_api_finais = int(auditoria.get("pais_pendentes_api", 0) or 0)
        parcial_api = bool(
            pendentes_api_finais > 0
            or api_stats.get("rate_limit_pause") or api_stats.get("outage")
            or api_stats.get("preflight_falhou") or api_stats.get("circuit_breaker")
        )
        if not auditoria.get("ok_estrutural"):
            print("\nERRO DE INTEGRIDADE: a auditoria estrutural encontrou inconsistências internas.", flush=True)
            print("A fila foi preservada; não avance para a validação de completude antes de revisar o relatório.", flush=True)
            return 6
        if parcial_api:
            print("\n[FINAL] Execução parcial: FTS5/TIC serão atualizados somente quando a reconciliação alcançar um ponto estável.", flush=True)
        else:
            atualizar_auxiliares(args.db.resolve(), args.job_id, jobs_db)
        atualizar_job(args.job_id, jobs_db, "Concluído", 1, 1)
        if api_stats.get("rate_limit_pause"):
            print("\nReconciliação parcial: PNCP aplicou rate limit persistente; fila preservada para retomada.", flush=True)
            return 75
        if api_stats.get("outage"):
            print("\nReconciliação parcial: PNCP instável; execute novamente para retomar.", flush=True)
            return 75
        if api_stats.get("preflight_falhou") or api_stats.get("circuit_breaker"):
            print("\nReconciliação parcial: falha de API detectada; fila preservada para diagnóstico.", flush=True)
            return 75
        if pendentes_api_finais > 0:
            print(
                f"\nReconciliação parcial: {pendentes_api_finais:,} pai(s) ainda dependem da API; "
                "fila preservada para retomada.",
                flush=True,
            )
            return 75
        print("\nReconciliação concluída: staging operacional sem pendências de API.", flush=True)
        return 0
    except InterruptedError:
        print("\nInterrompida pelo usuário; fila e checkpoints preservados.", flush=True)
        return 130
    except KeyboardInterrupt:
        print("\nInterrompida; fila e checkpoints preservados.", flush=True)
        return 130
    except Exception as exc:
        print(f"\nERRO: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        return 5
    finally:
        if con is not None:
            con.close()


if __name__ == "__main__":
    raise SystemExit(main())
