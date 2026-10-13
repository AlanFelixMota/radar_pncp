"""Cria uma base vazia sem substituir bancos existentes ou iniciar coletas."""
from datetime import datetime
from pathlib import Path
import json
import os
import sys

ROOT = Path(__file__).resolve().parent
BASE = ROOT / "radar_versoes" / "atual"


def criar(data=None):
    data = Path(data or ROOT / "radar_dados").resolve()
    if data.exists() and any(data.rglob("*.db")):
        raise RuntimeError("Ja existem bancos no destino. Nenhum banco foi substituido.")
    if not (BASE / "app.py").is_file():
        raise RuntimeError("Codigo do Radar PNCP nao encontrado.")
    # Todo o processo usa o mesmo destino, inclusive os imports com defaults.
    os.environ.update(RADAR_DATA_DIR=str(data), RADAR_DB=str(data / "compras.db"),
                      RADAR_JOBS_DB=str(data / "radar_jobs.db"),
                      RADAR_FTS_DB=str(data / "radar_fts.db"),
                      RADAR_TIC_DB=str(data / "radar_tic.db"),
                      RADAR_STATS_DB=str(data / "radar_stats.db"),
                      RADAR_LOG_DIR=str(data / "logs"))
    sys.path.insert(0, str(BASE))
    from coletar_pncp import conectar, criar_banco
    from job_manager import preparar_banco_jobs
    from cestas import preparar
    from mutirao_persistente import definir_config
    data.mkdir(parents=True, exist_ok=True)
    (data / "logs").mkdir(exist_ok=True)
    con = conectar(data / "compras.db")
    try:
        criar_banco(con)
    finally:
        con.close()
    preparar_banco_jobs(data / "radar_jobs.db")
    preparar(data / "radar_pesquisas.db")
    definir_config(ativo=False, db_jobs=data / "radar_jobs.db")
    (data / "base_inicial_criada.json").write_text(json.dumps({
        "version": "1.0.0", "criada_em": datetime.now().astimezone().isoformat(),
        "coletas_iniciadas": False}, ensure_ascii=False, indent=2), encoding="utf-8")
    print("[OK] Base vazia criada. Nenhuma coleta ou agenda foi iniciada.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(criar())
    except Exception as exc:
        print("Nao foi possivel criar a base: " + str(exc), file=sys.stderr)
        raise SystemExit(1)
