#!/usr/bin/env python3
"""
Busca simples no banco do Radar PNCP V0.

Exemplos:
    python pesquisar.py "privacy"
    python pesquisar.py "proteção de dados"
    python pesquisar.py "wi-fi" --limite 50
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from radar_paths import DB_COMPRAS, garantir_diretorios


garantir_diretorios()
DB_PADRAO = DB_COMPRAS


def main() -> int:
    parser = argparse.ArgumentParser(description="Pesquisa contratações no SQLite.")
    parser.add_argument("termo", help="Termo a pesquisar.")
    parser.add_argument("--db", default=str(DB_PADRAO))
    parser.add_argument("--limite", type=int, default=30)
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Banco não encontrado: {db_path}")
        print(f"Banco compartilhado esperado em: {DB_PADRAO}")
        return 2

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    like = f"%{args.termo}%"

    sql = """
    SELECT DISTINCT
        c.numero_controle_pncp,
        c.data_publicacao_pncp,
        c.orgao_razao_social,
        c.uf_sigla,
        c.modalidade_nome,
        c.objeto,
        c.valor_total_estimado,
        c.url_pncp
    FROM contratacoes c
    LEFT JOIN itens i
        ON i.numero_controle_pncp = c.numero_controle_pncp
    WHERE
        c.objeto LIKE ? COLLATE NOCASE
        OR c.informacao_complementar LIKE ? COLLATE NOCASE
        OR i.descricao LIKE ? COLLATE NOCASE
        OR i.informacao_complementar LIKE ? COLLATE NOCASE
        OR i.ncm_nbs_descricao LIKE ? COLLATE NOCASE
    ORDER BY c.data_publicacao_pncp DESC
    LIMIT ?
    """

    rows = con.execute(sql, (like, like, like, like, like, args.limite)).fetchall()

    print(f"\nResultados para: {args.termo!r}")
    print(f"Encontrados (até o limite): {len(rows)}\n")

    for n, r in enumerate(rows, 1):
        print("=" * 90)
        print(f"{n}. {r['orgao_razao_social'] or '(órgão não informado)'}")
        print(f"   Data: {r['data_publicacao_pncp'] or '-'} | UF: {r['uf_sigla'] or '-'}")
        print(f"   Modalidade: {r['modalidade_nome'] or '-'}")
        if r["valor_total_estimado"] is not None:
            print(f"   Valor estimado: R$ {r['valor_total_estimado']:,.2f}")
        print(f"   PNCP: {r['numero_controle_pncp']}")
        print(f"   Objeto: {r['objeto'] or '-'}")
        if r["url_pncp"]:
            print(f"   Link: {r['url_pncp']}")

    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
