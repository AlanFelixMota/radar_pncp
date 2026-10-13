#!/usr/bin/env python3
"""Camada de leitura do SQLite para a interface do Radar PNCP V0.7.6."""

from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable
import filtros_precos as fp


@dataclass(frozen=True)
class FiltrosBusca:
    texto: str = ""
    modo_texto: str = "todas"  # todas | frase | qualquer
    data_inicio: str | None = None
    data_fim: str | None = None
    modalidades: tuple[str, ...] = ()
    ufs: tuple[str, ...] = ()
    orgao: str = ""
    status_itens: str = "todos"  # todos | ok | indisponivel | pendente
    tic_status: str = "todos"  # todos | tic | nao_tic | nao_classificado
    tic_categorias: tuple[str, ...] = ()
    somente_homologadas: bool = False
    unidade: str = ""
    preco_min: float | None = None
    preco_max: float | None = None
    preco_tipo: str = "Estimado"


CAMPOS_TEXTO_CONTRATACAO = (
    "c.objeto",
    "c.informacao_complementar",
    "c.orgao_razao_social",
    "c.unidade_nome",
    "c.processo",
    "c.numero_compra",
    "c.numero_controle_pncp",
)

CAMPOS_TEXTO_ITEM = (
    "i.descricao",
    "i.informacao_complementar",
    "i.ncm_nbs_codigo",
    "i.ncm_nbs_descricao",
    "i.catalogo_codigo_item",
    "i.catalogo_nome",
    "i.categoria_catalogo_nome",
)


UFS_VALIDAS = (
    "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG",
    "PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO",
)
_SQL_DATA_PUBLICACAO_VALIDA = (
    "length(data_publicacao_pncp) >= 10 "
    "AND substr(data_publicacao_pncp,1,10) GLOB "
    "'[0-9][0-9][0-9][0-9]-[0-1][0-9]-[0-3][0-9]' "
    "AND substr(data_publicacao_pncp,1,10) BETWEEN '2021-01-01' AND '2099-12-31'"
)


def conectar(db_path: Path) -> sqlite3.Connection:
    """Abre uma conexão de leitura lógica (query_only) com o SQLite."""
    con = sqlite3.connect(str(db_path), timeout=10)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA query_only = ON")
    con.execute("PRAGMA busy_timeout = 10000")
    fp.registrar(con)
    return con


def tabela_existe(con: sqlite3.Connection, tabela: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (tabela,),
    ).fetchone()
    return row is not None


def colunas_tabela(con: sqlite3.Connection, tabela: str) -> set[str]:
    if not tabela_existe(con, tabela):
        return set()
    return {row["name"] for row in con.execute(f"PRAGMA table_info({tabela})")}


def validar_banco(db_path: Path) -> tuple[bool, str]:
    if not db_path.exists():
        return False, f"Banco não encontrado: {db_path}"
    try:
        con = conectar(db_path)
        tabelas = {
            row["name"]
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        con.close()
    except sqlite3.Error as exc:
        return False, f"Não foi possível abrir o banco: {exc}"

    faltantes = {"contratacoes", "itens"} - tabelas
    if faltantes:
        return False, "Banco sem as tabelas esperadas: " + ", ".join(sorted(faltantes))
    return True, "OK"


def _status_expr(colunas: set[str]) -> str:
    if "itens_status" in colunas:
        return "COALESCE(c.itens_status, CASE WHEN c.itens_coletados = 1 THEN 'OK' ELSE 'PENDENTE' END)"
    return "CASE WHEN c.itens_coletados = 1 THEN 'OK' ELSE 'PENDENTE' END"


def _termos(texto: str, modo: str) -> list[str]:
    texto = " ".join((texto or "").strip().split())
    if not texto:
        return []
    if modo == "frase":
        return [texto]
    palavras = [p for p in texto.split(" ") if p]
    return palavras or [texto]


def _like_campos(campos: Iterable[str]) -> str:
    return "(" + " OR ".join(
        f"COALESCE({campo}, '') LIKE ? COLLATE NOCASE" for campo in campos
    ) + ")"


def _clausula_termo_contratacao() -> str:
    campos_c = _like_campos(CAMPOS_TEXTO_CONTRATACAO)
    campos_i = _like_campos(CAMPOS_TEXTO_ITEM)
    return (
        "(" + campos_c + " OR EXISTS ("
        "SELECT 1 FROM itens i "
        "WHERE i.numero_controle_pncp = c.numero_controle_pncp "
        "AND " + campos_i + "))"
    )


def _params_termo_contratacao(padrao: str) -> list[str]:
    return [padrao] * (len(CAMPOS_TEXTO_CONTRATACAO) + len(CAMPOS_TEXTO_ITEM))


def _clausula_texto_item() -> str:
    return _like_campos(CAMPOS_TEXTO_ITEM)


def _params_texto_item(padrao: str) -> list[str]:
    return [padrao] * len(CAMPOS_TEXTO_ITEM)



def _fts_literal(texto: str) -> str:
    return '"' + texto.replace('"', '""') + '"'


def consulta_fts(texto: str, modo: str) -> str:
    termos = _termos(texto, modo)
    if not termos:
        return ""
    if modo == "frase":
        return _fts_literal(" ".join(termos))
    op = " OR " if modo == "qualquer" else " AND "
    return op.join(_fts_literal(t) for t in termos)


def indice_fts_pronto(db_fts: Path | None, fonte=None) -> bool:
    if not db_fts or not Path(db_fts).exists() or fonte is None:
        return False
    criou_transacao = not fonte.in_transaction
    anexou = pronto = False
    probe = None
    try:
        # A consulta de compatibilidade não deve carregar uma transação de
        # leitura do índice rejeitado durante a varredura lenta da base.
        probe = sqlite3.connect(Path(db_fts).resolve().as_uri()+'?mode=ro', uri=True, timeout=.2)
        meta_previa = dict(probe.execute('SELECT chave,valor FROM meta').fetchall())
        probe.close(); probe = None
        if meta_previa.get('schema_version') != '2' or meta_previa.get('estado') != 'READY':
            return False
        if criou_transacao: fonte.execute('BEGIN')
        token = fonte.execute('SELECT token FROM main.fts_fonte WHERE id=1').fetchone()
        row = fonte.execute("SELECT seq FROM main.sqlite_sequence WHERE name='fts_alteracoes'").fetchone()
        seq = int(row[0]) if row else 0
        if not token or meta_previa.get('fonte_id') != token[0] or int(meta_previa.get('seq_aplicada',-1)) != seq:
            return False
        fonte.execute("ATTACH DATABASE ? AS fts", (str(Path(db_fts).resolve()),))
        anexou = True
        meta = dict(fonte.execute('SELECT chave,valor FROM fts.meta').fetchall())
        pronto = bool(token and meta.get('fonte_id') == token[0] and meta.get('schema_version') == '2'
                      and meta.get('estado') == 'READY' and int(meta.get('seq_aplicada',-1)) == seq)
        return pronto
    except (sqlite3.Error,ValueError):
        return False
    finally:
        if probe: probe.close()
        if not pronto and criou_transacao:
            if fonte.in_transaction: fonte.rollback()
            if anexou:
                fonte.execute('DETACH DATABASE fts')


def _where_filtros_sem_texto(
    filtros: FiltrosBusca,
    colunas_contratacoes: set[str],
    tem_tic: bool = False,
) -> tuple[str, list[Any]]:
    """Mesmos filtros da busca, exceto texto; usado depois do FTS."""
    sem_texto = replace(filtros,texto="")
    return _where_busca(sem_texto, colunas_contratacoes, tem_tic=tem_tic)

def _where_busca(
    filtros: FiltrosBusca,
    colunas_contratacoes: set[str],
    tem_tic: bool = False,
) -> tuple[str, list[Any]]:
    condicoes: list[str] = []
    params: list[Any] = []
    fp.validar(filtros)

    termos = _termos(filtros.texto, filtros.modo_texto)
    if termos:
        clausulas_termos: list[str] = []
        for termo in termos:
            clausulas_termos.append(_clausula_termo_contratacao())
            params.extend(_params_termo_contratacao(f"%{termo}%"))
        operador = " OR " if filtros.modo_texto == "qualquer" else " AND "
        condicoes.append("(" + operador.join(clausulas_termos) + ")")

    # Comparação direta mantém utilizável o índice idx_contratacoes_data_publicacao.
    # data_publicacao_pncp é ISO (data ou timestamp); o limite superior exclusivo
    # inclui todo o último dia sem aplicar substr() sobre milhões de linhas.
    if filtros.data_inicio:
        condicoes.append("c.data_publicacao_pncp >= ?")
        params.append(filtros.data_inicio)
    if filtros.data_fim:
        try:
            limite_exclusivo = (date.fromisoformat(filtros.data_fim) + timedelta(days=1)).isoformat()
        except ValueError:
            limite_exclusivo = filtros.data_fim + "T23:59:59.999999"
        condicoes.append("c.data_publicacao_pncp < ?")
        params.append(limite_exclusivo)

    if filtros.modalidades:
        ph = ",".join("?" for _ in filtros.modalidades)
        condicoes.append(f"c.modalidade_nome IN ({ph})")
        params.extend(filtros.modalidades)

    if filtros.ufs:
        ph = ",".join("?" for _ in filtros.ufs)
        condicoes.append(f"c.uf_sigla IN ({ph})")
        params.extend(filtros.ufs)

    orgao = " ".join((filtros.orgao or "").strip().split())
    if orgao:
        padrao = f"%{orgao}%"
        condicoes.append(
            "(COALESCE(c.orgao_razao_social,'') LIKE ? COLLATE NOCASE "
            "OR COALESCE(c.unidade_nome,'') LIKE ? COLLATE NOCASE "
            "OR COALESCE(c.orgao_cnpj,'') LIKE ? COLLATE NOCASE)"
        )
        params.extend([padrao, padrao, padrao])

    if filtros.status_itens == "ok":
        condicoes.append("c.itens_coletados = 1")
    elif filtros.status_itens == "indisponivel":
        if "itens_status" in colunas_contratacoes:
            condicoes.append("COALESCE(c.itens_status, '') = 'INDISPONIVEL_PNCP'")
        else:
            condicoes.append("0 = 1")
    elif filtros.status_itens == "pendente":
        if "itens_status" in colunas_contratacoes:
            condicoes.append(
                "c.itens_coletados = 0 AND "
                "COALESCE(c.itens_status, 'PENDENTE') <> 'INDISPONIVEL_PNCP'"
            )
        else:
            condicoes.append("c.itens_coletados = 0")

    if filtros.tic_status != "todos":
        if not tem_tic:
            if filtros.tic_status != "nao_classificado":
                condicoes.append("0=1")
        elif filtros.tic_status == "tic":
            condicoes.append("tc.eh_tic = 1")
        elif filtros.tic_status == "nao_tic":
            condicoes.append("tc.eh_tic = 0")
        elif filtros.tic_status == "nao_classificado":
            condicoes.append("tc.numero_controle_pncp IS NULL")
    if filtros.tic_categorias:
        if not tem_tic:
            condicoes.append("0=1")
        else:
            ph = ",".join("?" for _ in filtros.tic_categorias)
            condicoes.append(f"tc.categoria IN ({ph})")
            params.extend(filtros.tic_categorias)

    if filtros.somente_homologadas:
        from pesquisa_fornecedores import CANCELADO
        condicoes.append("EXISTS (SELECT 1 FROM resultados_itens r "
                         "WHERE r.numero_controle_pncp=c.numero_controle_pncp "
                         "AND NOT " + CANCELADO + ")")

    if fp.ativo(filtros):
        item_where,item_params=fp.contratacao(filtros)
        condicoes.append(item_where)
        params.extend(item_params)

    return (" AND ".join(condicoes) if condicoes else "1=1"), params


def obter_metadados(db_path: Path) -> dict[str, Any]:
    con = conectar(db_path)
    cols = colunas_tabela(con, "contratacoes")
    status_expr = _status_expr(cols)
    resumo = con.execute(
        f"""
        SELECT
            COUNT(*) AS total_contratacoes,
            MIN(CASE WHEN {_SQL_DATA_PUBLICACAO_VALIDA} THEN substr(data_publicacao_pncp, 1, 10) END) AS data_min,
            MAX(CASE WHEN {_SQL_DATA_PUBLICACAO_VALIDA} THEN substr(data_publicacao_pncp, 1, 10) END) AS data_max,
            SUM(CASE WHEN itens_coletados = 1 THEN 1 ELSE 0 END) AS com_itens,
            SUM(CASE WHEN {status_expr} = 'INDISPONIVEL_PNCP' THEN 1 ELSE 0 END)
                AS indisponiveis,
            SUM(CASE WHEN itens_coletados = 0 AND {status_expr} <> 'INDISPONIVEL_PNCP'
                     THEN 1 ELSE 0 END) AS pendentes,
            COALESCE(SUM(qtd_itens), 0) AS total_itens
        FROM contratacoes c
        """
    ).fetchone()
    total_itens = resumo["total_itens"]

    modalidades = [
        row["modalidade_nome"]
        for row in con.execute(
            """
            SELECT DISTINCT modalidade_nome
            FROM contratacoes
            WHERE modalidade_nome IS NOT NULL
              AND trim(modalidade_nome) <> ''
              AND length(trim(modalidade_nome)) BETWEEN 3 AND 100
              AND lower(trim(modalidade_nome)) NOT LIKE 'http%'
              AND modalidade_id IS NOT NULL
              AND modalidade_id BETWEEN 1 AND 30
            ORDER BY modalidade_nome COLLATE NOCASE
            """
        )
    ]
    ufs = [
        row["uf_sigla"]
        for row in con.execute(
            """
            SELECT DISTINCT upper(trim(uf_sigla)) AS uf_sigla
            FROM contratacoes
            WHERE upper(trim(uf_sigla)) IN ('AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO')
            ORDER BY uf_sigla
            """
        )
    ]

    con.close()
    return {
        "total_contratacoes": int(resumo["total_contratacoes"] or 0),
        "total_itens": int(total_itens or 0),
        "data_min": resumo["data_min"],
        "data_max": resumo["data_max"],
        "com_itens": int(resumo["com_itens"] or 0),
        "indisponiveis": int(resumo["indisponiveis"] or 0),
        "pendentes": int(resumo["pendentes"] or 0),
        "modalidades": modalidades,
        "ufs": ufs,
        "tem_itens_status": "itens_status" in cols,
    }


def buscar_contratacoes(
    db_path: Path,
    filtros: FiltrosBusca,
    pagina: int = 1,
    por_pagina: int = 20,
    db_fts: Path | None = None,
    db_tic: Path | None = None,
) -> dict[str, Any]:
    fp.validar(filtros)
    con = conectar(db_path)
    try:
        return _buscar_contratacoes_aberta(con, filtros, pagina, por_pagina, db_fts, db_tic)
    finally:
        # Inclui erros e cancelamentos do Streamlit; uma consulta interrompida
        # não pode deixar leituras presas até a coleta de lixo do Python.
        con.close()


def _buscar_contratacoes_aberta(con, filtros, pagina, por_pagina, db_fts, db_tic):
    pagina = max(1, int(pagina))
    por_pagina = max(5, min(100, int(por_pagina)))

    fp.validar(filtros)
    if fp.exige_resultados(filtros) and not tabela_existe(con, "resultados_itens"):
        con.close()
        return {"total": 0, "pagina": 1, "por_pagina": por_pagina,
                "paginas": 1, "resultados": [], "motor_busca": "LIKE"}
    cols = colunas_tabela(con, "contratacoes")
    status_expr = _status_expr(cols)
    tem_tic = bool(db_tic and Path(db_tic).exists())
    if tem_tic:
        con.execute("ATTACH DATABASE ? AS tic", (str(Path(db_tic).resolve()),))
    join_tic = "LEFT JOIN tic.classificacoes tc ON tc.numero_controle_pncp=c.numero_controle_pncp" if tem_tic else ""
    select_tic = (
        "tc.eh_tic AS tic_eh_tic, tc.categoria AS tic_categoria, tc.score AS tic_score, "
        "tc.confianca AS tic_confianca, tc.motivos_json AS tic_motivos_json"
        if tem_tic else
        "NULL AS tic_eh_tic, NULL AS tic_categoria, NULL AS tic_score, NULL AS tic_confianca, NULL AS tic_motivos_json"
    )

    usar_fts = bool(filtros.texto.strip()) and indice_fts_pronto(db_fts, con)
    fts_query = consulta_fts(filtros.texto, filtros.modo_texto) if usar_fts else ""

    if usar_fts and fts_query:
        where, params = _where_filtros_sem_texto(filtros, cols, tem_tic)
        cte = """
        WITH fts_hits AS (
            SELECT numero_controle_pncp, rank AS relevancia
            FROM fts.contratacoes_fts
            WHERE contratacoes_fts MATCH ?
            UNION ALL
            SELECT numero_controle_pncp, rank AS relevancia
            FROM fts.itens_fts
            WHERE itens_fts MATCH ?
        ),
        fts_rank AS (
            SELECT numero_controle_pncp, MIN(relevancia) AS relevancia
            FROM fts_hits
            GROUP BY numero_controle_pncp
        )
        """
        count_sql = (
            cte
            + f"""
            SELECT COUNT(*) AS qtd
            FROM contratacoes c
            JOIN fts_rank fr
              ON fr.numero_controle_pncp = c.numero_controle_pncp
            {join_tic}
            WHERE {where}
            """
        )
        count_params = [fts_query, fts_query, *params]
        total = int(con.execute(count_sql, count_params).fetchone()["qtd"] or 0)
        paginas = max(1, math.ceil(total / por_pagina)) if total else 1
        pagina = min(pagina, paginas)
        offset = (pagina - 1) * por_pagina

        sql = cte + f"""
        SELECT
            c.numero_controle_pncp,
            c.numero_compra,
            c.ano_compra,
            c.processo,
            c.modalidade_nome,
            c.situacao_nome,
            c.objeto,
            c.informacao_complementar,
            c.valor_total_estimado,
            c.valor_total_homologado,
            c.data_abertura_proposta,
            c.data_encerramento_proposta,
            c.data_publicacao_pncp,
            c.data_atualizacao,
            c.orgao_cnpj,
            c.orgao_razao_social,
            c.unidade_nome,
            c.municipio_nome,
            c.uf_sigla,
            c.url_pncp,
            c.link_sistema_origem,
            c.itens_coletados,
            c.qtd_itens,
            {status_expr} AS itens_status_exibicao,
            fr.relevancia,
            {select_tic}
        FROM contratacoes c
        JOIN fts_rank fr
          ON fr.numero_controle_pncp = c.numero_controle_pncp
        {join_tic}
        WHERE {where}
        ORDER BY fr.relevancia ASC,
                 substr(c.data_publicacao_pncp, 1, 10) DESC,
                 c.numero_controle_pncp DESC
        LIMIT ? OFFSET ?
        """
        rows = [
            dict(row)
            for row in con.execute(
                sql, [fts_query, fts_query, *params, por_pagina, offset]
            )
        ]
        con.close()
        return {
            "total": total,
            "pagina": pagina,
            "por_pagina": por_pagina,
            "paginas": paginas,
            "resultados": rows,
            "motor_busca": "FTS5",
        }

    where, params = _where_busca(filtros, cols, tem_tic)
    total = int(
        con.execute(
            f"SELECT COUNT(*) AS qtd FROM contratacoes c {join_tic} WHERE {where}",
            params,
        ).fetchone()["qtd"]
        or 0
    )
    paginas = max(1, math.ceil(total / por_pagina)) if total else 1
    pagina = min(pagina, paginas)
    offset = (pagina - 1) * por_pagina

    sql = f"""
    SELECT
        c.numero_controle_pncp,
        c.numero_compra,
        c.ano_compra,
        c.processo,
        c.modalidade_nome,
        c.situacao_nome,
        c.objeto,
        c.informacao_complementar,
        c.valor_total_estimado,
        c.valor_total_homologado,
        c.data_abertura_proposta,
        c.data_encerramento_proposta,
        c.data_publicacao_pncp,
        c.data_atualizacao,
        c.orgao_cnpj,
        c.orgao_razao_social,
        c.unidade_nome,
        c.municipio_nome,
        c.uf_sigla,
        c.url_pncp,
        c.link_sistema_origem,
        c.itens_coletados,
        c.qtd_itens,
        {status_expr} AS itens_status_exibicao,
        {select_tic}
    FROM contratacoes c
    {join_tic}
    WHERE {where}
    ORDER BY
        substr(c.data_publicacao_pncp, 1, 10) DESC,
        c.data_publicacao_pncp DESC,
        c.numero_controle_pncp DESC
    LIMIT ? OFFSET ?
    """
    rows = [dict(row) for row in con.execute(sql, [*params, por_pagina, offset])]
    con.close()
    return {
        "total": total,
        "pagina": pagina,
        "por_pagina": por_pagina,
        "paginas": paginas,
        "resultados": rows,
        "motor_busca": "LIKE",
    }


def buscar_itens_correspondentes(
    db_path: Path,
    numeros_controle: list[str],
    texto: str,
    modo_texto: str,
    max_por_contratacao: int = 3,
    *, filtros: FiltrosBusca | None = None,
) -> dict[str, list[dict[str, Any]]]:
    numeros_controle = [x for x in numeros_controle if x]
    if filtros is not None:fp.validar(filtros)
    termos = _termos(texto, modo_texto)
    filtrado=bool(filtros and fp.ativo(filtros))
    if not numeros_controle or (not termos and not filtrado):
        return {}

    cond_termos: list[str] = []
    params_texto: list[Any] = []
    for termo in termos:
        cond_termos.append(_clausula_texto_item())
        params_texto.extend(_params_texto_item(f"%{termo}%"))
    # Para os trechos exibidos na tela, basta mostrar itens que contenham
    # algum dos termos. O filtro principal da contratação continua respeitando
    # o modo "todas", "frase" ou "qualquer".
    operador = " OR " if len(cond_termos) > 1 else ""
    pred_texto = "(" + operador.join(cond_termos) + ")" if cond_termos else '1=1'
    if filtrado:
        # Prévia dos itens qualificados; o termo principal também busca o objeto.
        pred_texto,params_texto=fp.item_contratacao(filtros)

    ph = ",".join("?" for _ in numeros_controle)
    sql = f"""
    WITH hits AS (
        SELECT
            i.numero_controle_pncp,
            i.numero_item,
            i.descricao,
            i.informacao_complementar,
            i.quantidade,
            i.unidade_medida,
            i.valor_unitario_estimado,
            i.valor_total,
            i.catalogo_codigo_item,
            i.catalogo_nome,
            i.ncm_nbs_codigo,
            i.ncm_nbs_descricao,
            ROW_NUMBER() OVER (
                PARTITION BY i.numero_controle_pncp
                ORDER BY i.numero_item
            ) AS rn
        FROM itens i
        WHERE i.numero_controle_pncp IN ({ph})
          AND {pred_texto}
    )
    SELECT * FROM hits
    WHERE rn <= ?
    ORDER BY numero_controle_pncp, numero_item
    """
    con = conectar(db_path)
    if filtrado and fp.exige_resultados(filtros) and not tabela_existe(con,'resultados_itens'):
        con.close();return {}
    rows = con.execute(
        sql,
        [*numeros_controle, *params_texto, int(max_por_contratacao)],
    ).fetchall()
    con.close()

    saida: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        d = dict(row)
        d.pop("rn", None)
        saida.setdefault(d["numero_controle_pncp"], []).append(d)
    return saida


def obter_contratacao(db_path: Path, numero_controle: str) -> dict[str, Any] | None:
    con = conectar(db_path)
    cols = colunas_tabela(con, "contratacoes")
    status_expr = _status_expr(cols)
    row = con.execute(
        f"""
        SELECT c.*, {status_expr} AS itens_status_exibicao
        FROM contratacoes c
        WHERE c.numero_controle_pncp = ?
        """,
        (numero_controle,),
    ).fetchone()
    con.close()
    return dict(row) if row else None


def obter_itens_contratacao(db_path: Path, numero_controle: str) -> list[dict[str, Any]]:
    con = conectar(db_path)
    rows = con.execute(
        """
        SELECT
            numero_item,
            descricao,
            informacao_complementar,
            material_ou_servico_nome,
            quantidade,
            unidade_medida,
            valor_unitario_estimado,
            valor_total,
            situacao_item_nome,
            criterio_julgamento_nome,
            catalogo_codigo_item,
            catalogo_nome,
            categoria_catalogo_nome,
            ncm_nbs_codigo,
            ncm_nbs_descricao,
            tem_resultado,
            data_inclusao,
            data_atualizacao,
            fonte_ultima
        FROM itens
        WHERE numero_controle_pncp = ?
        ORDER BY numero_item
        """,
        (numero_controle,),
    ).fetchall()
    con.close()
    return [dict(row) for row in rows]


def estatisticas_painel(db_path: Path) -> dict[str, Any]:
    con = conectar(db_path)
    cols = colunas_tabela(con, "contratacoes")
    status_expr = _status_expr(cols)

    por_modalidade = [
        dict(row)
        for row in con.execute(
            """
            SELECT
                   CASE
                     WHEN modalidade_id BETWEEN 1 AND 30
                      AND modalidade_nome IS NOT NULL
                      AND length(trim(modalidade_nome)) BETWEEN 3 AND 100
                      AND lower(trim(modalidade_nome)) NOT LIKE 'http%'
                     THEN trim(modalidade_nome)
                     ELSE '(não informada)'
                   END AS modalidade,
                   COUNT(*) AS quantidade
            FROM contratacoes
            GROUP BY modalidade
            ORDER BY quantidade DESC
            """
        ).fetchall()
    ]
    por_uf = [
        dict(row)
        for row in con.execute(
            """
            SELECT
                   CASE WHEN upper(trim(uf_sigla)) IN ('AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO')
                        THEN upper(trim(uf_sigla)) ELSE '--' END AS uf,
                   COUNT(*) AS quantidade
            FROM contratacoes
            GROUP BY uf
            ORDER BY quantidade DESC
            """
        ).fetchall()
    ]
    por_dia = [
        dict(row)
        for row in con.execute(
            f"""
            SELECT substr(data_publicacao_pncp, 1, 10) AS data,
                   COUNT(*) AS contratacoes,
                   SUM(qtd_itens) AS itens
            FROM contratacoes
            WHERE data_publicacao_pncp IS NOT NULL
              AND {_SQL_DATA_PUBLICACAO_VALIDA}
            GROUP BY substr(data_publicacao_pncp, 1, 10)
            ORDER BY data DESC
            LIMIT 30
            """
        ).fetchall()
    ]

    estados = [
        dict(row)
        for row in con.execute(
            f"""
            SELECT {status_expr} AS status, COUNT(*) AS quantidade
            FROM contratacoes c
            GROUP BY {status_expr}
            ORDER BY quantidade DESC
            """
        ).fetchall()
    ]

    execucoes: list[dict[str, Any]] = []
    if tabela_existe(con, "execucoes_diarias"):
        campos_exec = colunas_tabela(con, "execucoes_diarias")
        indisponivel_col = (
            "contratacoes_itens_indisponiveis"
            if "contratacoes_itens_indisponiveis" in campos_exec
            else "0"
        )
        execucoes = [
            dict(row)
            for row in con.execute(
                f"""
                SELECT data_referencia, inicio_em, fim_em, status,
                       total_contratacoes, contratacoes_com_itens,
                       contratacoes_sem_itens,
                       {indisponivel_col} AS contratacoes_itens_indisponiveis,
                       total_itens, modalidades_concluidas
                FROM execucoes_diarias
                ORDER BY id DESC
                LIMIT 10
                """
            ).fetchall()
        ]

    con.close()
    return {
        "por_modalidade": por_modalidade,
        "por_uf": por_uf,
        "por_dia": por_dia,
        "estados_itens": estados,
        "execucoes": execucoes,
    }


def iso_para_data(valor: str | None) -> date | None:
    if not valor:
        return None
    try:
        return datetime.strptime(valor[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None
