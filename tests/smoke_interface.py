"""Executa a interface em processo e bases isolados, sem acesso as fontes externas."""
from pathlib import Path
from unittest.mock import patch
import os
import subprocess
import sys
import tempfile

from streamlit.testing.v1 import AppTest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "radar_versoes" / "atual"))


def main():
    with tempfile.TemporaryDirectory(prefix="radar_interface_") as td:
        data = Path(td)
        os.environ.update(RADAR_DATA_DIR=td, RADAR_DB=str(data / "compras.db"),
                          RADAR_JOBS_DB=str(data / "radar_jobs.db"),
                          RADAR_FTS_DB=str(data / "radar_fts.db"),
                          RADAR_TIC_DB=str(data / "radar_tic.db"),
                          RADAR_STATS_DB=str(data / "radar_stats.db"),
                          RADAR_LOG_DIR=str(data / "logs"), RADAR_TEST_MODE="0",
                          GIT_PYTHON_REFRESH="quiet")
        codigo = "import sys;sys.path.insert(0,sys.argv[1]);from criar_base_inicial import criar;criar(sys.argv[2])"
        subprocess.run([sys.executable, "-c", codigo, str(ROOT), td], check=True)
        with patch("requests.sessions.Session.request", side_effect=AssertionError("Consulta externa nao permitida no teste")):
            app = AppTest.from_file(ROOT / "radar_versoes" / "atual" / "app.py", default_timeout=30).run()
            assert len(app.exception) == 0, [x.message for x in app.exception]
            assert any("Radar PNCP" in x.value for x in app.sidebar.markdown)
            for pagina in ("Minhas pesquisas", "Acompanhamentos", "Sistema", "Consultar"):
                app.button(key="nav_" + pagina).click().run()
                assert len(app.exception) == 0, (pagina, [x.message for x in app.exception])
                print("[OK] " + pagina)


if __name__ == "__main__":
    main()
