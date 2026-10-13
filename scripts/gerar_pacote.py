"""Recompõe hashes e gera o pacote local de atualização do código vigente."""
from pathlib import Path
import hashlib
import json
import zipfile

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "radar_versoes" / "atual"


def gerar():
    manifest_path = BASE / "release.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {
        "version": "1.0.0", "layout": 2, "update_protocol": 1, "cooperative_pause": 1}
    arquivos = {}
    for p in sorted(BASE.rglob("*")):
        if not p.is_file() or "__pycache__" in p.parts or p.name in {"release.json", "instalacao.json"}:
            continue
        if p.suffix in {".pyc", ".log", ".db", ".lock"}:
            continue
        arquivos[p.relative_to(BASE).as_posix()] = hashlib.sha256(p.read_bytes()).hexdigest()
    manifest["files"] = arquivos
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    out = ROOT / "dist"
    out.mkdir(exist_ok=True)
    path = out / ("Radar_PNCP_Atualizacao_" + manifest["version"] + ".zip")
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        for name in [*arquivos, "release.json"]:
            z.write(BASE / name, name)
    print(path)
    return path


if __name__ == "__main__":
    gerar()
