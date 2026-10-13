#!/usr/bin/env python3
"""Caminhos persistentes compartilhados do Radar PNCP."""

from __future__ import annotations

import os
from pathlib import Path


APP_DIR = Path(__file__).resolve().parent
from radar_pncp_layout import raiz_da_instalacao
PROJECT_DIR = raiz_da_instalacao(APP_DIR)

# Pasta persistente fora da pasta de cada versão.
# Pode ser substituída futuramente por RADAR_DATA_DIR.
DATA_DIR = Path(
    os.environ.get("RADAR_DATA_DIR", PROJECT_DIR / "radar_dados")
).expanduser().resolve()

DB_COMPRAS = Path(
    os.environ.get("RADAR_DB", DATA_DIR / "compras.db")
).expanduser().resolve()

DB_JOBS = Path(
    os.environ.get("RADAR_JOBS_DB", DATA_DIR / "radar_jobs.db")
).expanduser().resolve()

DB_FTS = Path(
    os.environ.get("RADAR_FTS_DB", DATA_DIR / "radar_fts.db")
).expanduser().resolve()

DB_TIC = Path(
    os.environ.get("RADAR_TIC_DB", DATA_DIR / "radar_tic.db")
).expanduser().resolve()

DB_STATS = Path(
    os.environ.get("RADAR_STATS_DB", DATA_DIR / "radar_stats.db")
).expanduser().resolve()

# Dados de trabalho do usuário; separados da base coletada e das versões.
DB_PESQUISAS = DATA_DIR / 'radar_pesquisas.db'

LOG_DIR = Path(
    os.environ.get("RADAR_LOG_DIR", DATA_DIR / "logs")
).expanduser().resolve()


def garantir_diretorios() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

# Modo explicitamente isolado para testes. Nunca é ativado por padrão.
TEST_MODE = os.environ.get("RADAR_TEST_MODE", "").strip().lower() in {"1", "true", "yes", "sim"}
