"""Verificacao offline do manifesto, sintaxe, imports locais e bancos basicos."""
from pathlib import Path
import argparse
import ast
import hashlib
import json
import sqlite3
from contextlib import closing

ROOT = Path(__file__).resolve().parent
BASE = ROOT / "radar_versoes" / "atual"


def verificar(somente_codigo=False):
    manifest = json.loads((BASE / "release.json").read_text(encoding="utf-8"))
    erros = []
    for name, digest in manifest["files"].items():
        path = BASE / name
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            erros.append("Arquivo ausente ou alterado: " + name)
    locais = {p.stem for p in BASE.glob("*.py")}
    for path in ROOT.rglob("*.py"):
        if any(part in {".venv", "python", "__pycache__", "anterior", "historico", "operacoes"} for part in path.parts):
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
            # Referencias locais sao validadas no manifesto e pela compilacao.
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom) and node.module in locais:
                    if not (BASE / (node.module + ".py")).is_file():
                        erros.append("Import local ausente: " + node.module)
        except (SyntaxError, UnicodeError) as exc:
            erros.append(str(exc))
    if not somente_codigo:
        for name, required in (("compras.db", {"contratacoes", "itens"}),
                               ("radar_jobs.db", {"jobs"}),
                               ("radar_pesquisas.db", {"pesquisas", "referencias"})):
            path = ROOT / "radar_dados" / name
            if not path.is_file():
                erros.append("Banco nao encontrado: " + name)
                continue
            with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as con:
                names = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                if not required <= names:
                    erros.append("Estrutura incompleta: " + name)
    for erro in erros:
        print("[ERRO] " + erro)
    if not erros:
        print("[OK] Codigo verificado." if somente_codigo else "[OK] Codigo e estruturas basicas verificados.")
    return int(bool(erros))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--somente-codigo", action="store_true")
    raise SystemExit(verificar(ap.parse_args().somente_codigo))
