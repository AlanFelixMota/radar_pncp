#!/usr/bin/env python3
"""Infraestrutura de índice FTS5 do Radar PNCP."""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from radar_paths import DB_COMPRAS, DB_FTS


SCHEMA_VERSION = "2"


def _agora() -> str:
    return datetime.now().isoformat(timespec="seconds")


def fts5_disponivel() -> bool:
    con = sqlite3.connect(":memory:")
    try:
        con.execute("CREATE VIRTUAL TABLE __teste_fts USING fts5(texto)")
        return True
    except sqlite3.Error:
        return False
    finally:
        con.close()


def _meta(con: sqlite3.Connection) -> dict[str, str]:
    try:
        rows = con.execute("SELECT chave, valor FROM meta").fetchall()
        return {str(k): str(v) for k, v in rows}
    except sqlite3.OperationalError as exc:
        if 'no such table' in str(exc).lower():
            return {}
        raise


def _max_rowids(db_path: Path) -> tuple[int, int]:
    if not Path(db_path).exists():
        return 0, 0
    con = sqlite3.connect(str(db_path), timeout=10)
    try:
        mc = int(con.execute("SELECT COALESCE(MAX(rowid),0) FROM contratacoes").fetchone()[0] or 0)
        mi = int(con.execute("SELECT COALESCE(MAX(rowid),0) FROM itens").fetchone()[0] or 0)
        return mc, mi
    finally:
        con.close()


def status_indice(db_compras: Path = DB_COMPRAS, db_fts: Path = DB_FTS) -> dict[str, Any]:
    from fts_sync import status
    return status(db_compras, db_fts)


def criar_schema(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS meta (
            chave TEXT PRIMARY KEY,
            valor TEXT NOT NULL
        );

        CREATE VIRTUAL TABLE IF NOT EXISTS contratacoes_fts USING fts5(
            numero_controle_pncp UNINDEXED,
            texto,
            tokenize='unicode61 remove_diacritics 2'
        );

        CREATE VIRTUAL TABLE IF NOT EXISTS itens_fts USING fts5(
            numero_controle_pncp UNINDEXED,
            numero_item UNINDEXED,
            texto,
            tokenize='unicode61 remove_diacritics 2'
        );
        """
    )


def set_meta(con: sqlite3.Connection, **valores: Any) -> None:
    con.executemany(
        """
        INSERT INTO meta (chave, valor)
        VALUES (?, ?)
        ON CONFLICT(chave) DO UPDATE SET valor=excluded.valor
        """,
        [(k, str(v) if v is not None else "") for k, v in valores.items()],
    )


def _texto_partes(*partes: Any) -> str:
    return " ".join(
        " ".join(str(p).split())
        for p in partes
        if p is not None and str(p).strip()
    )



def atualizar_contratacoes_especificas(db_compras, db_fts, numero_controles):
    # V2 acompanha todos os textos via diário transacional, inclusive itens.
    # A sincronização completa é solicitada pelo chamador na etapa seguinte.
    return 0


def construir_ou_atualizar(db_compras, db_fts, modo, progress_callback=None,
                          stop_callback=None, lote=2000, max_segundos=None):
    from fts_sync import sincronizar
    return sincronizar(db_compras, db_fts, modo, progress_callback, stop_callback, lote, max_segundos)
