"""Abre a interface da instalação fixa sem criar outro servidor na mesma porta."""
import json
import subprocess
import sys
import urllib.request
import webbrowser
from pathlib import Path


def main():
    base=Path(__file__).resolve().parent
    config=base/'instalacao.json'
    port=json.loads(config.read_text(encoding='utf-8')).get('port',8501) if config.exists() else 8501
    url=f'http://localhost:{int(port)}'
    try:
        with urllib.request.urlopen(url+'/_stcore/health',timeout=2) as r:
            if r.status==200:webbrowser.open(url);return 0
    except OSError:pass
    return subprocess.call([sys.executable,'-m','streamlit','run',str(base/'app.py'),
                            '--server.port',str(port),'--server.address','127.0.0.1'],cwd=base)


if __name__=='__main__':raise SystemExit(main())
