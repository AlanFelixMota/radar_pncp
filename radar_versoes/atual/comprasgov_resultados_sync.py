#!/usr/bin/env python3
"""Sincronização resiliente dos resultados do Compras.gov — Radar PNCP V0.7.6.

Resultados possuem ciclo de atualização próprio e não devem herdar a janela de
3/30 dias usada para contratações. A sincronização usa uma fronteira local
(MAX(data_resultado_pncp)) com sobreposição, de modo que uma fonte atrasada seja
consultada até alcançar o presente. A revisão semanal também verifica o snapshot
CSV anual "latest" e, quando ele muda, faz reconciliação ADITIVA: o CSV preenche
linhas ausentes sem sobrescrever resultados já mantidos pela API.
"""
from __future__ import annotations

import json
import os
import sqlite3
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable

import requests

from comprasgov_api_incremental import URL_RESULTADOS, paginar as compras_paginar
from comprasgov_resultados import garantir_tabelas, importar_csv_resultados, upsert_json_rows_detalhado
from importar_comprasgov_csv import CSV_DIR, meta_importacao
from proveniencia import FONTE_COMPRASGOV_API

OVERLAP_DIAS = 7
LOOKAHEAD_DIARIO_DIAS = 7
RECENTE_DIARIO_DIAS = 3
MAX_BACKFILL_DIAS = 365
BASE_ANUAL = "https://repositorio.dados.gov.br/seges/comprasgov/anual"


def agora() -> str:
    return datetime.now().isoformat(timespec="seconds")


def garantir_estado(con: sqlite3.Connection) -> None:
    garantir_tabelas(con)
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS comprasgov_resultados_sync (
            id INTEGER PRIMARY KEY CHECK(id=1),
            ultima_execucao_em TEXT,
            inicio_consulta TEXT,
            fim_consulta TEXT,
            fronteira_antes TEXT,
            fronteira_depois TEXT,
            max_observada_api TEXT,
            resultados_aplicados INTEGER NOT NULL DEFAULT 0,
            resultados_recebidos INTEGER NOT NULL DEFAULT 0,
            resultados_novos INTEGER NOT NULL DEFAULT 0,
            resultados_alterados INTEGER NOT NULL DEFAULT 0,
            resultados_identicos INTEGER NOT NULL DEFAULT 0,
            quarentena INTEGER NOT NULL DEFAULT 0,
            falhas INTEGER NOT NULL DEFAULT 0,
            defasagem_dias INTEGER,
            frescor TEXT,
            csv_url TEXT,
            csv_token_remoto TEXT,
            csv_etag TEXT,
            csv_last_modified TEXT,
            csv_content_length INTEGER,
            csv_verificado_em TEXT,
            csv_token_importado TEXT,
            csv_importado_em TEXT,
            csv_status TEXT,
            csv_erro TEXT
        );
        INSERT OR IGNORE INTO comprasgov_resultados_sync(id) VALUES(1);

        CREATE TABLE IF NOT EXISTS comprasgov_resultados_sync_dias (
            data_resultado TEXT PRIMARY KEY,
            ultima_consulta_em TEXT NOT NULL,
            linhas_recebidas INTEGER NOT NULL DEFAULT 0,
            linhas_novas INTEGER NOT NULL DEFAULT 0,
            linhas_alteradas INTEGER NOT NULL DEFAULT 0,
            linhas_identicas INTEGER NOT NULL DEFAULT 0,
            quarentena INTEGER NOT NULL DEFAULT 0,
            falhas INTEGER NOT NULL DEFAULT 0
        );
        """
    )
    # Migração idempotente para bancos criados pela V0.6.4.2.
    col_sync = {str(r[1]) for r in con.execute("PRAGMA table_info(comprasgov_resultados_sync)")}
    for nome in ("resultados_recebidos", "resultados_novos", "resultados_alterados", "resultados_identicos"):
        if nome not in col_sync:
            con.execute(f"ALTER TABLE comprasgov_resultados_sync ADD COLUMN {nome} INTEGER NOT NULL DEFAULT 0")
    col_dias = {str(r[1]) for r in con.execute("PRAGMA table_info(comprasgov_resultados_sync_dias)")}
    for nome in ("linhas_novas", "linhas_alteradas", "linhas_identicas"):
        if nome not in col_dias:
            con.execute(f"ALTER TABLE comprasgov_resultados_sync_dias ADD COLUMN {nome} INTEGER NOT NULL DEFAULT 0")
    con.commit()


def _data_valida(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()[:10]
    try:
        date.fromisoformat(s)
        return s
    except Exception:
        return None


def data_max_local(con: sqlite3.Connection, fim: str | None = None) -> str | None:
    garantir_estado(con)
    limite = fim or date.today().isoformat()
    row = con.execute(
        """SELECT MAX(substr(data_resultado_pncp,1,10))
             FROM resultados_itens
            WHERE data_resultado_pncp IS NOT NULL
              AND length(data_resultado_pncp) >= 10
              AND substr(data_resultado_pncp,1,10) BETWEEN '2021-01-01' AND ?""",
        (limite,),
    ).fetchone()
    return _data_valida(row[0] if row else None)


def classificar_frescor(max_data: str | None, referencia: str | None = None) -> tuple[int | None, str]:
    if not max_data:
        return None, "SEM_DADOS"
    ref = date.fromisoformat(referencia) if referencia else date.today()
    d = date.fromisoformat(max_data)
    atraso = max(0, (ref - d).days)
    if atraso <= 7:
        return atraso, "ATUAL"
    if atraso <= 30:
        return atraso, "ATENCAO"
    if atraso <= 45:
        return atraso, "ATRASADO"
    return atraso, "CRITICO"


def inicio_backfill(fronteira: str | None, fim: str, overlap: int = OVERLAP_DIAS) -> str:
    d_fim = date.fromisoformat(fim)
    piso = d_fim - timedelta(days=MAX_BACKFILL_DIAS - 1)
    if fronteira:
        inicio = date.fromisoformat(fronteira) - timedelta(days=max(0, int(overlap)))
    else:
        # Banco novo/sem resultados: começa no início do ano corrente, limitado a 365 dias.
        inicio = date(d_fim.year, 1, 1)
    if inicio < piso:
        inicio = piso
    if inicio > d_fim:
        inicio = d_fim
    return inicio.isoformat()


def _datas(inicio: str, fim: str):
    a = date.fromisoformat(inicio)
    b = date.fromisoformat(fim)
    while a <= b:
        yield a.isoformat()
        a += timedelta(days=1)


def _faixas_diarias(fronteira: str | None, fim: str, overlap: int = OVERLAP_DIAS) -> tuple[list[str], list[tuple[str, str]]]:
    """Faixa leve: entorno da fronteira + poucos dias recentes.

    A revisão semanal continua responsável pela lacuna completa. Em banco sem
    fronteira, a primeira execução mantém o backfill integral para estabelecer
    uma posição segura.
    """
    if not fronteira:
        ini = inicio_backfill(None, fim, overlap)
        return list(_datas(ini, fim)), [(ini, fim)]
    d_fim = date.fromisoformat(fim)
    d_front = date.fromisoformat(fronteira)
    a1 = max(date(2021, 1, 1), d_front - timedelta(days=max(0, int(overlap))))
    b1 = min(d_fim, d_front + timedelta(days=LOOKAHEAD_DIARIO_DIAS))
    a2 = max(date(2021, 1, 1), d_fim - timedelta(days=RECENTE_DIARIO_DIAS - 1))
    b2 = d_fim
    dias = sorted(set(_datas(a1.isoformat(), b1.isoformat())) | set(_datas(a2.isoformat(), b2.isoformat())))
    faixas: list[tuple[str, str]] = []
    if dias:
        ini = prev = date.fromisoformat(dias[0])
        for ds in dias[1:]:
            d = date.fromisoformat(ds)
            if d != prev + timedelta(days=1):
                faixas.append((ini.isoformat(), prev.isoformat()))
                ini = d
            prev = d
        faixas.append((ini.isoformat(), prev.isoformat()))
    return dias, faixas


def _extrair_max_data(rows: list[dict[str, Any]]) -> str | None:
    vals: list[str] = []
    for row in rows:
        for k in ("dataResultadoPncp", "dataResultado"):
            d = _data_valida(row.get(k))
            if d:
                vals.append(d)
                break
    return max(vals) if vals else None


def sincronizar_api(
    con: sqlite3.Connection,
    sessao: requests.Session,
    fim: str,
    *,
    overlap: int = OVERLAP_DIAS,
    estrategia: str = "completa",
    progresso: Callable[[int, int, str], None] | None = None,
    parada: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    """Sincroniza resultados por fronteira própria.

    ``diaria`` limita requisições ao entorno da fronteira e aos dias mais
    recentes; ``completa`` percorre toda a lacuna e é usada na revisão semanal.
    """
    garantir_estado(con)
    fronteira_antes = data_max_local(con, fim)
    if estrategia == "diaria":
        ds, faixas = _faixas_diarias(fronteira_antes, fim, overlap)
        inicio = ds[0] if ds else fim
        texto_faixas = " + ".join(f"{a}→{b}" for a, b in faixas) if faixas else fim
        print(
            f"    Fronteira local antes: {fronteira_antes or 'sem resultados'} · estratégia diária leve: "
            f"{len(ds)} dia(s) em {len(faixas)} faixa(s): {texto_faixas}.",
            flush=True,
        )
    else:
        inicio = inicio_backfill(fronteira_antes, fim, overlap)
        ds = list(_datas(inicio, fim))
        faixas = [(inicio, fim)]
        print(
            f"    Fronteira local antes: {fronteira_antes or 'sem resultados'} · revisão completa: "
            f"{inicio} → {fim} · sobreposição {overlap} dia(s).",
            flush=True,
        )

    recebidos = novos = alterados = identicos = quarentena = falhas = 0
    max_api: str | None = None

    for pos, dia in enumerate(ds, 1):
        if parada and parada():
            raise InterruptedError
        dia_recebidos = dia_novos = dia_alterados = dia_identicos = dia_q = dia_falhas = 0
        try:
            base = {"dataResultadoPncpInicial": dia, "dataResultadoPncpFinal": dia}
            for rows, _obj, _pag in compras_paginar(sessao, URL_RESULTADOS, base):
                if parada and parada():
                    raise InterruptedError
                met = upsert_json_rows_detalhado(con, rows, fonte=FONTE_COMPRASGOV_API, periodo=dia)
                dia_recebidos += int(met.get("recebidos") or 0)
                dia_novos += int(met.get("novos") or 0)
                dia_alterados += int(met.get("alterados") or 0)
                dia_identicos += int(met.get("identicos") or 0)
                dia_q += int(met.get("quarentena") or 0)
                dmax = _extrair_max_data(rows)
                if dmax and (max_api is None or dmax > max_api):
                    max_api = dmax
        except InterruptedError:
            raise
        except Exception as exc:
            dia_falhas = 1
            falhas += 1
            print(f"    AVISO resultados {dia}: {type(exc).__name__}: {exc}", flush=True)

        recebidos += dia_recebidos
        novos += dia_novos
        alterados += dia_alterados
        identicos += dia_identicos
        quarentena += dia_q
        con.execute(
            """INSERT INTO comprasgov_resultados_sync_dias(
                   data_resultado,ultima_consulta_em,linhas_recebidas,linhas_novas,linhas_alteradas,linhas_identicas,quarentena,falhas)
               VALUES(?,?,?,?,?,?,?,?)
               ON CONFLICT(data_resultado) DO UPDATE SET
                 ultima_consulta_em=excluded.ultima_consulta_em,
                 linhas_recebidas=excluded.linhas_recebidas,
                 linhas_novas=excluded.linhas_novas,
                 linhas_alteradas=excluded.linhas_alteradas,
                 linhas_identicas=excluded.linhas_identicas,
                 quarentena=excluded.quarentena,
                 falhas=excluded.falhas""",
            (dia, agora(), dia_recebidos, dia_novos, dia_alterados, dia_identicos, dia_q, dia_falhas),
        )
        con.commit()

        if dia_recebidos or dia_q or dia_falhas or pos == 1 or pos == len(ds) or pos % 10 == 0:
            print(
                f"    ↳ Resultados {dia}: recebidos {dia_recebidos:,} · novos {dia_novos:,} · "
                f"alterados {dia_alterados:,} · idênticos {dia_identicos:,} · {dia_q:,} quarentena · "
                f"{dia_falhas} falha(s) · dia {pos}/{len(ds)}".replace(",", "."),
                flush=True,
            )
        if progresso:
            progresso(pos, len(ds), dia)

    fronteira_depois = data_max_local(con, fim)
    atraso, frescor = classificar_frescor(fronteira_depois, fim)
    con.execute(
        """UPDATE comprasgov_resultados_sync SET
             ultima_execucao_em=?, inicio_consulta=?, fim_consulta=?,
             fronteira_antes=?, fronteira_depois=?, max_observada_api=?,
             resultados_aplicados=?, resultados_recebidos=?, resultados_novos=?, resultados_alterados=?, resultados_identicos=?,
             quarentena=?, falhas=?, defasagem_dias=?, frescor=?
           WHERE id=1""",
        (agora(), inicio, fim, fronteira_antes, fronteira_depois, max_api,
         recebidos, recebidos, novos, alterados, identicos, quarentena, falhas, atraso, frescor),
    )
    con.commit()
    print(
        f"    Fronteira local depois: {fronteira_depois or 'sem resultados'} · "
        f"defasagem: {atraso if atraso is not None else '?'} dia(s) · frescor {frescor}.",
        flush=True,
    )
    return {
        "inicio": inicio,
        "fim": fim,
        "faixas": faixas,
        "estrategia": estrategia,
        "fronteira_antes": fronteira_antes,
        "fronteira_depois": fronteira_depois,
        "max_observada_api": max_api,
        "resultados": recebidos,
        "recebidos": recebidos,
        "novos": novos,
        "alterados": alterados,
        "identicos": identicos,
        "quarentena": quarentena,
        "falhas": falhas,
        "defasagem_dias": atraso,
        "frescor": frescor,
    }

def _csv_url(ano: int) -> str:
    # O alias "latest" anual fica na raiz /anual/. As subpastas por ano
    # contêm os snapshots com nome fixo (ex.: ...-2026.csv).
    nome = "comprasGOV-anual-VW_DM_PNCP_ITEM_RESULTADO-latest.csv"
    return f"{BASE_ANUAL}/{nome}"


def _csv_path(ano: int) -> Path:
    return CSV_DIR / "anual" / str(ano) / "comprasGOV-anual-VW_DM_PNCP_ITEM_RESULTADO-latest.csv"


def _csv_path_fixo(ano: int) -> Path:
    return CSV_DIR / "anual" / str(ano) / f"comprasGOV-anual-VW_DM_PNCP_ITEM_RESULTADO-{ano}.csv"


def _cabecalhos_csv(sessao: requests.Session, url: str) -> dict[str, Any]:
    resp = None
    try:
        resp = sessao.head(url, allow_redirects=True, timeout=(15, 45))
        if resp.status_code in (403, 405) or resp.status_code >= 500:
            resp.close()
            resp = sessao.get(url, stream=True, allow_redirects=True, timeout=(15, 45), headers={"Range": "bytes=0-0"})
        if resp.status_code == 404:
            raise FileNotFoundError(f"CSV anual de resultados não disponível: {url}")
        resp.raise_for_status()
        h = resp.headers
        tamanho = h.get("Content-Length")
        # GET com Range pode devolver Content-Length=1; Content-Range traz o total real.
        cr = h.get("Content-Range") or ""
        if "/" in cr:
            tail = cr.rsplit("/", 1)[-1]
            if tail.isdigit():
                tamanho = tail
        out = {
            "url": str(resp.url or url),
            "etag": h.get("ETag"),
            "last_modified": h.get("Last-Modified"),
            "content_length": int(tamanho) if tamanho and str(tamanho).isdigit() else None,
        }
        partes = [out.get("etag"), out.get("last_modified"), out.get("content_length")]
        out["token"] = "|".join(str(v or "") for v in partes) if any(v not in (None, "") for v in partes) else None
        return out
    finally:
        if resp is not None:
            try:
                resp.close()
            except Exception:
                pass


def _http_date(data: str | None) -> datetime | None:
    if not data:
        return None
    try:
        dt = parsedate_to_datetime(data)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _meta_finalizado(meta: sqlite3.Row | None) -> datetime | None:
    if not meta or not meta["finalizado_em"]:
        return None
    try:
        dt = datetime.fromisoformat(str(meta["finalizado_em"]))
        if dt.tzinfo is None:
            # Hora local desconhecida; comparar por data é suficiente para o bootstrap.
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _baixar_forcado(sessao: requests.Session, url: str, destino: Path) -> Path:
    """Baixa snapshot novo sem destruir o anterior e permite retomar .part."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    parcial = destino.with_suffix(destino.suffix + ".part")
    existente = parcial.stat().st_size if parcial.exists() else 0
    headers = {"Range": f"bytes={existente}-"} if existente > 0 else {}
    if existente:
        print(f"    Retomando snapshot atualizado em {existente/1024**2:.1f} MB: {destino.name}", flush=True)
    else:
        print(f"    Baixando snapshot atualizado: {destino.name}", flush=True)
    with sessao.get(url, stream=True, timeout=(30, 180), headers=headers) as resp:
        if resp.status_code == 416 and existente > 1000:
            os.replace(parcial, destino)
            return destino
        resp.raise_for_status()
        retomou = existente > 0 and resp.status_code == 206
        if retomou:
            atual = existente
            total = existente + int(resp.headers.get("Content-Length") or 0)
            modo = "ab"
        else:
            atual = 0
            total = int(resp.headers.get("Content-Length") or 0)
            modo = "wb"
        ultimo = atual
        with parcial.open(modo) as f:
            for bloco in resp.iter_content(chunk_size=1024 * 1024):
                if not bloco:
                    continue
                f.write(bloco)
                atual += len(bloco)
                if atual - ultimo >= 50 * 1024 * 1024 or (total and atual >= total):
                    if total:
                        print(f"      Download: {atual/1024**2:.1f}/{total/1024**2:.1f} MB", flush=True)
                    else:
                        print(f"      Download: {atual/1024**2:.1f} MB", flush=True)
                    ultimo = atual
    if parcial.stat().st_size < 1000:
        raise RuntimeError("Snapshot anual de resultados baixado com tamanho inválido.")
    os.replace(parcial, destino)
    return destino


def verificar_reconciliar_csv(
    con: sqlite3.Connection,
    sessao: requests.Session,
    ano: int,
    *,
    permitir_download: bool,
    job_id: int | None = None,
    jobs_db: Path | None = None,
) -> dict[str, Any]:
    """Verifica o CSV anual latest; baixa apenas quando um snapshot novo é detectado.

    No primeiro uso tenta reconhecer o snapshot já importado pelo bootstrap antigo
    comparando tamanho e data do arquivo fixo. Assim não baixa ~400 MB à toa.
    """
    garantir_estado(con)
    url = _csv_url(ano)
    stamp = agora()
    try:
        remoto = _cabecalhos_csv(sessao, url)
    except Exception as exc:
        con.execute(
            "UPDATE comprasgov_resultados_sync SET csv_url=?,csv_verificado_em=?,csv_status='ERRO',csv_erro=? WHERE id=1",
            (url, stamp, f"{type(exc).__name__}: {exc}"),
        )
        con.commit()
        return {"status": "ERRO", "erro": f"{type(exc).__name__}: {exc}", "baixou": False, "novos": 0}

    estado = con.execute("SELECT * FROM comprasgov_resultados_sync WHERE id=1").fetchone()
    token_importado = estado["csv_token_importado"] if isinstance(estado, sqlite3.Row) else None
    token_remoto = remoto.get("token") or None

    # Bootstrap: a carga 2026 anterior usava o arquivo de nome fixo. Se ele foi
    # importado depois da modificação remota e tem o mesmo tamanho, consideramos
    # que o snapshot corrente já está coberto e apenas passamos a rastrear o token.
    bootstrap_cache = False
    if not token_importado:
        try:
            meta = meta_importacao(con, f"{ano}-ANUAL", "ITEM_RESULTADO")
        except sqlite3.OperationalError:
            meta = None
        fixo = _csv_path_fixo(ano)
        remoto_len = remoto.get("content_length")
        remoto_dt = _http_date(remoto.get("last_modified"))
        meta_dt = _meta_finalizado(meta)
        tamanho_ok = bool(remoto_len and fixo.exists() and fixo.stat().st_size == int(remoto_len))
        data_ok = bool(meta_dt and (remoto_dt is None or meta_dt.date() >= remoto_dt.date()))
        meta_ok = bool(meta and meta["status"] == "CONCLUIDA")
        if meta_ok and tamanho_ok and data_ok:
            token_importado = token_remoto
            bootstrap_cache = True
            con.execute(
                "UPDATE comprasgov_resultados_sync SET csv_token_importado=?,csv_importado_em=?,csv_status='COBERTO_PELO_CACHE' WHERE id=1",
                (token_importado, str(meta["finalizado_em"] or stamp)),
            )
            con.commit()

    mudou = bool(token_remoto and token_importado and token_remoto != token_importado)
    primeiro_sem_cobertura = bool(token_remoto and not token_importado)
    precisa = mudou or primeiro_sem_cobertura

    base_update = (
        remoto.get("url") or url, token_remoto, remoto.get("etag"), remoto.get("last_modified"),
        remoto.get("content_length"), stamp,
    )
    con.execute(
        """UPDATE comprasgov_resultados_sync SET
             csv_url=?,csv_token_remoto=?,csv_etag=?,csv_last_modified=?,csv_content_length=?,csv_verificado_em=?,csv_erro=NULL
           WHERE id=1""",
        base_update,
    )
    con.commit()

    if not precisa:
        status_ok = "COBERTO_PELO_CACHE" if bootstrap_cache else "SEM_ALTERACAO"
        con.execute("UPDATE comprasgov_resultados_sync SET csv_status=? WHERE id=1", (status_ok,))
        con.commit()
        return {"status": status_ok, "baixou": False, "novos": 0, **remoto}

    if not permitir_download:
        con.execute("UPDATE comprasgov_resultados_sync SET csv_status='NOVO_SNAPSHOT_PENDENTE' WHERE id=1")
        con.commit()
        return {"status": "NOVO_SNAPSHOT_PENDENTE", "baixou": False, "novos": 0, **remoto}

    path = _csv_path(ano)
    try:
        _baixar_forcado(sessao, remoto.get("url") or url, path)
        periodo = f"{ano}-ANUAL-RESULTADOS-LATEST"
        imp = importar_csv_resultados(
            con, path, remoto.get("url") or url, periodo,
            job_id, jobs_db, forcar=True, somente_novos=True,
        )
        novos = int(imp.get("gravados") or 0)
        con.execute(
            """UPDATE comprasgov_resultados_sync SET
                 csv_token_importado=?,csv_importado_em=?,csv_status='RECONCILIADO',csv_erro=NULL
               WHERE id=1""",
            (token_remoto, agora()),
        )
        con.commit()
        return {"status": "RECONCILIADO", "baixou": True, "novos": novos, **remoto}
    except Exception as exc:
        con.execute(
            "UPDATE comprasgov_resultados_sync SET csv_status='ERRO',csv_erro=? WHERE id=1",
            (f"{type(exc).__name__}: {exc}",),
        )
        con.commit()
        return {"status": "ERRO", "erro": f"{type(exc).__name__}: {exc}", "baixou": False, "novos": 0, **remoto}


def obter_status_local(db: Path | str, referencia: str | None = None) -> dict[str, Any]:
    ref = referencia or date.today().isoformat()
    con = sqlite3.connect(str(db), timeout=10)
    con.row_factory = sqlite3.Row
    try:
        existe = con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='resultados_itens'").fetchone()
        if not existe:
            return {"data_max": None, "defasagem_dias": None, "frescor": "SEM_DADOS", "sync": {}}
        row = con.execute(
            """SELECT MAX(substr(data_resultado_pncp,1,10)) FROM resultados_itens
                 WHERE data_resultado_pncp IS NOT NULL
                   AND length(data_resultado_pncp)>=10
                   AND substr(data_resultado_pncp,1,10) BETWEEN '2021-01-01' AND ?""",
            (ref,),
        ).fetchone()
        maxd = _data_valida(row[0] if row else None)
        atraso, frescor = classificar_frescor(maxd, ref)
        st = {}
        if con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='comprasgov_resultados_sync'").fetchone():
            row = con.execute("SELECT * FROM comprasgov_resultados_sync WHERE id=1").fetchone()
            st = dict(row) if row else {}
        return {"data_max": maxd, "defasagem_dias": atraso, "frescor": frescor, "sync": st}
    finally:
        con.close()
