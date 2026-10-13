#!/usr/bin/env python3
"""Construção e leitura da classificação TIC heurística."""

from __future__ import annotations

import json
import os
import re
import sqlite3
import unicodedata
from functools import lru_cache
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from radar_paths import DB_COMPRAS, DB_TIC
from tic_rules import CATEGORIES, NEGATIVE_TERMS, RULES_VERSION, TIC_THRESHOLD


def agora() -> str:
    return datetime.now().isoformat(timespec="seconds")


def normalizar(texto: Any) -> str:
    s = " ".join(str(texto or "").lower().split())
    s = unicodedata.normalize("NFKD", s)
    return "".join(c for c in s if not unicodedata.combining(c))


@lru_cache(maxsize=512)
def _normalizar_termo(termo: str) -> str:
    return normalizar(termo)


def _termo_presente(texto_norm: str, termo: str) -> bool:
    t = _normalizar_termo(termo)
    if not t:
        return False
    # Frases/termos com pontuação são buscados como substring normalizada.
    # Para termos curtos (EDR, XDR, SOC etc.), usa limites de palavra.
    if len(t) <= 4 and " " not in t:
        return re.search(rf"(?<!\w){re.escape(t)}(?!\w)", texto_norm) is not None
    return t in texto_norm


def classificar(
    objeto: str | None,
    informacao_complementar: str | None,
    itens_texto: Iterable[str],
) -> dict[str, Any]:
    fontes = {
        "objeto": (normalizar(objeto), 1.50),
        "informação complementar": (normalizar(informacao_complementar), 1.25),
    }
    itens_norm = [normalizar(x) for x in itens_texto if x and str(x).strip()]
    itens_unidos = " ".join(itens_norm)

    scores: dict[str, int] = defaultdict(int)
    motivos: dict[str, list[tuple[int, str]]] = defaultdict(list)

    for categoria, regras in CATEGORIES.items():
        for termo, peso in regras:
            # Uma mesma regra pontua no máximo uma vez por tipo de fonte.
            for nome_fonte, (texto_norm, mult) in fontes.items():
                if texto_norm and _termo_presente(texto_norm, termo):
                    pts = int(round(peso * mult))
                    scores[categoria] += pts
                    motivos[categoria].append(
                        (pts, f'"{termo}" em {nome_fonte} (+{pts})')
                    )
            if itens_unidos and _termo_presente(itens_unidos, termo):
                pts = int(peso)
                scores[categoria] += pts
                motivos[categoria].append((pts, f'"{termo}" em item (+{pts})'))

    # Redutores simples de falso positivo.
    tudo = " ".join([fontes["objeto"][0], fontes["informação complementar"][0], itens_unidos])
    reducao = sum(peso for termo, peso in NEGATIVE_TERMS if _termo_presente(tudo, termo))

    if scores:
        categoria = max(scores, key=lambda c: scores[c])
        score = max(0, min(100, scores[categoria] - reducao))
    else:
        categoria = None
        score = 0

    eh_tic = int(score >= TIC_THRESHOLD)
    if score >= 70:
        confianca = "ALTA"
    elif score >= TIC_THRESHOLD:
        confianca = "MEDIA"
    elif score > 0:
        confianca = "BAIXA"
    else:
        confianca = "SEM_EVIDENCIA"

    lista_motivos = []
    if categoria:
        lista_motivos = [m for _, m in sorted(motivos[categoria], reverse=True)[:8]]
    if reducao:
        lista_motivos.append(f"Redução por possível falso positivo (-{reducao})")

    return {
        "eh_tic": eh_tic,
        "categoria": categoria,
        "score": score,
        "confianca": confianca,
        "motivos": lista_motivos,
    }


def _meta(con: sqlite3.Connection) -> dict[str, str]:
    try:
        return {str(k): str(v) for k, v in con.execute("SELECT chave, valor FROM meta")}
    except sqlite3.Error:
        return {}


def criar_schema(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS meta (
            chave TEXT PRIMARY KEY,
            valor TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS classificacoes (
            numero_controle_pncp TEXT PRIMARY KEY,
            eh_tic INTEGER NOT NULL,
            categoria TEXT,
            score INTEGER NOT NULL,
            confianca TEXT NOT NULL,
            motivos_json TEXT NOT NULL,
            regras_versao TEXT NOT NULL,
            source_rowid INTEGER NOT NULL,
            atualizado_em TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_tic_eh_tic ON classificacoes(eh_tic);
        CREATE INDEX IF NOT EXISTS idx_tic_categoria ON classificacoes(categoria);
        CREATE INDEX IF NOT EXISTS idx_tic_score ON classificacoes(score DESC);
        """
    )


def set_meta(con: sqlite3.Connection, **valores: Any) -> None:
    con.executemany(
        """
        INSERT INTO meta(chave, valor) VALUES (?, ?)
        ON CONFLICT(chave) DO UPDATE SET valor=excluded.valor
        """,
        [(k, str(v) if v is not None else "") for k, v in valores.items()],
    )


def status_classificacao(
    db_compras: Path = DB_COMPRAS,
    db_tic: Path = DB_TIC,
) -> dict[str, Any]:
    db_compras, db_tic = Path(db_compras), Path(db_tic)
    base = {
        "existe": db_tic.exists(), "estado": "NAO_CONSTRUIDA",
        "mensagem": "Classificação TIC ainda não construída.",
        "classificadas": 0, "tic": 0, "last_rowid": 0,
        "source_max_rowid": 0, "atualizado_em": None,
        "tamanho_bytes": db_tic.stat().st_size if db_tic.exists() else 0,
        "pronta": False,
    }
    if not db_compras.exists():
        base.update(estado="ERRO", mensagem="compras.db não encontrado.")
        return base
    src = sqlite3.connect(str(db_compras), timeout=10)
    max_rowid = int(src.execute("SELECT COALESCE(MAX(rowid),0) FROM contratacoes").fetchone()[0] or 0)
    src.close()
    base["source_max_rowid"] = max_rowid
    if not db_tic.exists():
        return base
    try:
        con = sqlite3.connect(str(db_tic), timeout=10)
        meta = _meta(con)
        classificadas = int(con.execute("SELECT COUNT(*) FROM classificacoes").fetchone()[0] or 0)
        tic = int(con.execute("SELECT COUNT(*) FROM classificacoes WHERE eh_tic=1").fetchone()[0] or 0)
        con.close()
        last = int(meta.get("last_contratacao_rowid", "0") or 0)
        base.update(
            classificadas=classificadas,
            tic=tic,
            last_rowid=last,
            atualizado_em=meta.get("atualizado_em"),
        )
        if meta.get("estado") == "READY" and last >= max_rowid:
            base.update(
                estado="ATUALIZADA", pronta=True,
                mensagem="Classificação heurística alinhada com a base principal.",
            )
        elif meta.get("estado") == "READY":
            base.update(
                estado="DESATUALIZADA",
                mensagem="Há novas contratações ainda não classificadas.",
            )
        else:
            base.update(estado=meta.get("estado", "INCOMPLETA"), mensagem="Classificação incompleta.")
    except sqlite3.Error as exc:
        base.update(estado="ERRO", mensagem=f"Erro ao ler classificação: {exc}")
    return base


def categorias_disponiveis(db_tic: Path = DB_TIC) -> list[str]:
    if not Path(db_tic).exists():
        return sorted(CATEGORIES)
    con = sqlite3.connect(str(db_tic), timeout=5)
    try:
        rows = con.execute(
            "SELECT DISTINCT categoria FROM classificacoes WHERE categoria IS NOT NULL ORDER BY categoria"
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        con.close()



def atualizar_contratacoes_especificas(
    db_compras: Path,
    db_tic: Path,
    numero_controles: list[str] | tuple[str, ...] | set[str],
) -> int:
    """Reclassifica contratos já existentes alterados pela COLETA diária."""
    ids=list(dict.fromkeys(str(x) for x in numero_controles if x))
    if not ids or not Path(db_tic).exists():
        return 0
    src=sqlite3.connect(str(Path(db_compras).resolve()),timeout=30)
    src.row_factory=sqlite3.Row
    src.execute("PRAGMA busy_timeout=30000")
    out=sqlite3.connect(str(Path(db_tic).resolve()),timeout=30)
    out.execute("PRAGMA busy_timeout=30000")
    criar_schema(out)
    total=0
    try:
        for ini in range(0,len(ids),250):
            bloco=ids[ini:ini+250]
            ph=",".join("?" for _ in bloco)
            contratos=src.execute(
                f"SELECT rowid,numero_controle_pncp,objeto,informacao_complementar FROM contratacoes WHERE numero_controle_pncp IN ({ph})",
                bloco,
            ).fetchall()
            if not contratos:
                continue
            ids2=[r["numero_controle_pncp"] for r in contratos]
            ph2=",".join("?" for _ in ids2)
            itens_por: dict[str,list[str]]=defaultdict(list)
            for it in src.execute(
                f"""SELECT numero_controle_pncp, descricao, informacao_complementar,
                    catalogo_codigo_item, catalogo_nome, categoria_catalogo_nome,
                    ncm_nbs_codigo, ncm_nbs_descricao, material_ou_servico_nome
                    FROM itens WHERE numero_controle_pncp IN ({ph2})""", ids2):
                txt=" ".join(str(it[k] or "") for k in it.keys() if k!="numero_controle_pncp")
                if txt.strip(): itens_por[it["numero_controle_pncp"]].append(txt)
            dados=[]
            for c in contratos:
                res=classificar(c["objeto"],c["informacao_complementar"],itens_por.get(c["numero_controle_pncp"],[]))
                dados.append((
                    c["numero_controle_pncp"],res["eh_tic"],res["categoria"],res["score"],
                    res["confianca"],json.dumps(res["motivos"],ensure_ascii=False),
                    RULES_VERSION,int(c["rowid"]),agora(),
                ))
            out.executemany(
                """INSERT INTO classificacoes(
                    numero_controle_pncp,eh_tic,categoria,score,confianca,motivos_json,
                    regras_versao,source_rowid,atualizado_em
                ) VALUES (?,?,?,?,?,?,?,?,?)
                ON CONFLICT(numero_controle_pncp) DO UPDATE SET
                    eh_tic=excluded.eh_tic,categoria=excluded.categoria,score=excluded.score,
                    confianca=excluded.confianca,motivos_json=excluded.motivos_json,
                    regras_versao=excluded.regras_versao,source_rowid=excluded.source_rowid,
                    atualizado_em=excluded.atualizado_em""",
                dados,
            )
            total += len(dados)
        set_meta(out,atualizado_em=agora())
        out.commit()
        return total
    finally:
        out.close(); src.close()

def construir_ou_atualizar(
    db_compras: Path,
    db_tic: Path,
    modo: str,
    progress_callback=None,
    stop_callback=None,
    lote: int = 300,
) -> dict[str, Any]:
    if modo not in {"build", "update"}:
        raise ValueError("modo deve ser build ou update")
    db_compras, db_tic = Path(db_compras).resolve(), Path(db_tic).resolve()
    db_tic.parent.mkdir(parents=True, exist_ok=True)
    temp = db_tic.with_suffix(db_tic.suffix + ".building")
    destino = temp if modo == "build" else db_tic
    if modo == "build":
        temp.unlink(missing_ok=True)
    elif not db_tic.exists():
        raise RuntimeError("Classificação inexistente; use build primeiro.")

    src = sqlite3.connect(str(db_compras), timeout=30)
    src.row_factory = sqlite3.Row
    src.execute("PRAGMA busy_timeout=30000")
    out = sqlite3.connect(str(destino), timeout=30)
    out.execute("PRAGMA journal_mode=WAL")
    criar_schema(out)
    meta = _meta(out)
    last = 0 if modo == "build" else int(meta.get("last_contratacao_rowid", "0") or 0)
    max_rowid = int(src.execute("SELECT COALESCE(MAX(rowid),0) FROM contratacoes").fetchone()[0] or 0)
    total = max(0, max_rowid - last)
    feito = 0
    set_meta(
        out, estado="BUILDING", regras_versao=RULES_VERSION,
        threshold=TIC_THRESHOLD, atualizado_em=agora(), last_contratacao_rowid=last,
    )
    out.commit()

    while last < max_rowid:
        if stop_callback and stop_callback():
            raise InterruptedError("Classificação interrompida pelo usuário.")
        contratos = src.execute(
            """
            SELECT rowid, numero_controle_pncp, objeto, informacao_complementar
            FROM contratacoes WHERE rowid>? ORDER BY rowid LIMIT ?
            """,
            (last, lote),
        ).fetchall()
        if not contratos:
            break
        ids = [r["numero_controle_pncp"] for r in contratos]
        itens_por: dict[str, list[str]] = defaultdict(list)
        for inicio in range(0, len(ids), 250):
            bloco = ids[inicio:inicio+250]
            ph = ",".join("?" for _ in bloco)
            for it in src.execute(
                f"""
                SELECT numero_controle_pncp, descricao, informacao_complementar,
                       catalogo_codigo_item, catalogo_nome, categoria_catalogo_nome,
                       ncm_nbs_codigo, ncm_nbs_descricao, material_ou_servico_nome
                FROM itens WHERE numero_controle_pncp IN ({ph})
                """,
                bloco,
            ):
                txt = " ".join(str(it[k] or "") for k in it.keys() if k != "numero_controle_pncp")
                if txt.strip():
                    itens_por[it["numero_controle_pncp"]].append(txt)

        dados = []
        for c in contratos:
            res = classificar(c["objeto"], c["informacao_complementar"], itens_por.get(c["numero_controle_pncp"], []))
            dados.append(
                (
                    c["numero_controle_pncp"], res["eh_tic"], res["categoria"], res["score"],
                    res["confianca"], json.dumps(res["motivos"], ensure_ascii=False),
                    RULES_VERSION, int(c["rowid"]), agora(),
                )
            )
        out.executemany(
            """
            INSERT INTO classificacoes(
                numero_controle_pncp, eh_tic, categoria, score, confianca,
                motivos_json, regras_versao, source_rowid, atualizado_em
            ) VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT(numero_controle_pncp) DO UPDATE SET
                eh_tic=excluded.eh_tic, categoria=excluded.categoria,
                score=excluded.score, confianca=excluded.confianca,
                motivos_json=excluded.motivos_json, regras_versao=excluded.regras_versao,
                source_rowid=excluded.source_rowid, atualizado_em=excluded.atualizado_em
            """,
            dados,
        )
        last = int(contratos[-1]["rowid"])
        feito += len(contratos)
        set_meta(out, last_contratacao_rowid=last, atualizado_em=agora())
        out.commit()
        if progress_callback:
            progress_callback("Classificando", feito, total)

    set_meta(out, estado="READY", regras_versao=RULES_VERSION, last_contratacao_rowid=last, atualizado_em=agora())
    out.commit()
    try:
        out.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except sqlite3.Error:
        pass
    out.close(); src.close()
    if modo == "build":
        for suf in ("-wal", "-shm"):
            Path(str(temp)+suf).unlink(missing_ok=True)
        os.replace(temp, db_tic)

    status = status_classificacao(db_compras, db_tic)
    return {"classificadas": status["classificadas"], "tic": status["tic"], "last_rowid": last}
