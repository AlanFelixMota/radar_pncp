#!/usr/bin/env python3
"""
Radar PNCP V0
Coleta contratações publicadas no PNCP em uma data e grava contratações + itens em SQLite.

Uso:
    python coletar_pncp.py --data 2026-08-27
    python coletar_pncp.py --data 20260827
    python coletar_pncp.py --data 2026-08-27 --sem-itens
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

from radar_paths import DB_COMPRAS, garantir_diretorios
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pncp_health import EXIT_PNCP_INDISPONIVEL, eh_erro_transitorio
from proveniencia import FONTE_PNCP, garantir_proveniencia


BASE_CONSULTA = "https://pncp.gov.br/api/consulta/v1"
BASE_PNCP = "https://pncp.gov.br/api/pncp/v1"
garantir_diretorios()
DB_PADRAO = DB_COMPRAS


class ServicoPNCPIndisponivel(RuntimeError):
    pass
TAMANHO_PAGINA_CONTRATACOES = 50
TAMANHO_PAGINA_ITENS = 50

# Tabela de domínio oficial do PNCP.
MODALIDADES = {
    1: "Leilão - Eletrônico",
    2: "Diálogo Competitivo",
    3: "Concurso",
    4: "Concorrência - Eletrônica",
    5: "Concorrência - Presencial",
    6: "Pregão - Eletrônico",
    7: "Pregão - Presencial",
    8: "Dispensa de Licitação",
    9: "Inexigibilidade",
    10: "Manifestação de Interesse",
    11: "Pré-qualificação",
    12: "Credenciamento",
    13: "Leilão - Presencial",
    14: "Inaplicabilidade da Licitação",
}


def criar_sessao() -> requests.Session:
    sessao = requests.Session()
    retry = Retry(
        total=2,
        connect=2,
        read=2,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
    )
    sessao.mount("https://", HTTPAdapter(max_retries=retry))
    sessao.headers.update(
        {
            "Accept": "application/json",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
            # O WAF do PNCP pode recusar clientes com User-Agent não semelhante
            # a navegador. Mantemos um UA convencional para as consultas GET públicas.
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/151.0.0.0 Safari/537.36"
            ),
        }
    )
    return sessao


def normalizar_data(valor: str | None) -> str:
    """Retorna AAAAMMDD. Sem parâmetro, usa ontem."""
    if not valor:
        return (date.today() - timedelta(days=1)).strftime("%Y%m%d")

    valor = valor.strip()
    for formato in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(valor, formato).strftime("%Y%m%d")
        except ValueError:
            pass

    raise ValueError("Data inválida. Use AAAA-MM-DD ou AAAAMMDD.")


def conectar(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON")
    con.execute("PRAGMA journal_mode = WAL")
    con.execute("PRAGMA synchronous = NORMAL")
    return con


def criar_banco(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS contratacoes (
            numero_controle_pncp TEXT PRIMARY KEY,
            numero_compra TEXT,
            ano_compra INTEGER,
            sequencial_compra INTEGER,
            processo TEXT,

            tipo_instrumento_id INTEGER,
            tipo_instrumento_nome TEXT,
            modalidade_id INTEGER,
            modalidade_nome TEXT,
            modo_disputa_id INTEGER,
            modo_disputa_nome TEXT,
            situacao_id INTEGER,
            situacao_nome TEXT,

            objeto TEXT,
            informacao_complementar TEXT,
            srp INTEGER,
            amparo_legal_codigo INTEGER,
            amparo_legal_nome TEXT,

            valor_total_estimado REAL,
            valor_total_homologado REAL,

            data_abertura_proposta TEXT,
            data_encerramento_proposta TEXT,
            data_publicacao_pncp TEXT,
            data_inclusao TEXT,
            data_atualizacao TEXT,
            data_atualizacao_global TEXT,

            orgao_cnpj TEXT,
            orgao_razao_social TEXT,
            poder_id TEXT,
            esfera_id TEXT,

            unidade_codigo TEXT,
            unidade_nome TEXT,
            municipio_ibge TEXT,
            municipio_nome TEXT,
            uf_sigla TEXT,
            uf_nome TEXT,

            usuario_nome TEXT,
            link_sistema_origem TEXT,
            url_pncp TEXT,

            itens_coletados INTEGER NOT NULL DEFAULT 0,
            qtd_itens INTEGER NOT NULL DEFAULT 0,

            raw_json TEXT,
            primeira_coleta_em TEXT NOT NULL,
            ultima_coleta_em TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_contratacoes_data_publicacao
            ON contratacoes(data_publicacao_pncp);
        CREATE INDEX IF NOT EXISTS idx_contratacoes_modalidade
            ON contratacoes(modalidade_id);
        CREATE INDEX IF NOT EXISTS idx_contratacoes_orgao_cnpj
            ON contratacoes(orgao_cnpj);
        CREATE INDEX IF NOT EXISTS idx_contratacoes_uf
            ON contratacoes(uf_sigla);

        CREATE TABLE IF NOT EXISTS itens (
            numero_controle_pncp TEXT NOT NULL,
            numero_item INTEGER NOT NULL,

            material_ou_servico TEXT,
            material_ou_servico_nome TEXT,
            descricao TEXT,
            informacao_complementar TEXT,
            quantidade REAL,
            unidade_medida TEXT,
            valor_unitario_estimado REAL,
            valor_total REAL,

            situacao_item_id INTEGER,
            situacao_item_nome TEXT,
            criterio_julgamento_id INTEGER,
            criterio_julgamento_nome TEXT,
            tem_resultado INTEGER,

            ncm_nbs_codigo TEXT,
            ncm_nbs_descricao TEXT,
            catalogo_codigo_item TEXT,
            catalogo_id INTEGER,
            catalogo_nome TEXT,
            categoria_catalogo_id INTEGER,
            categoria_catalogo_nome TEXT,

            data_inclusao TEXT,
            data_atualizacao TEXT,
            raw_json TEXT,

            PRIMARY KEY (numero_controle_pncp, numero_item),
            FOREIGN KEY (numero_controle_pncp)
                REFERENCES contratacoes(numero_controle_pncp)
                ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_itens_catalogo_codigo
            ON itens(catalogo_codigo_item);

        CREATE TABLE IF NOT EXISTS coletas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data_referencia TEXT NOT NULL,
            inicio_em TEXT NOT NULL,
            fim_em TEXT,
            status TEXT NOT NULL,
            contratacoes_recebidas INTEGER NOT NULL DEFAULT 0,
            contratacoes_gravadas INTEGER NOT NULL DEFAULT 0,
            itens_gravados INTEGER NOT NULL DEFAULT 0,
            erros_itens INTEGER NOT NULL DEFAULT 0,
            observacao TEXT
        );

        CREATE TABLE IF NOT EXISTS erros_coleta (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            coleta_id INTEGER,
            etapa TEXT NOT NULL,
            numero_controle_pncp TEXT,
            url TEXT,
            erro TEXT NOT NULL,
            criado_em TEXT NOT NULL,
            FOREIGN KEY (coleta_id) REFERENCES coletas(id)
        );
        """
    )
    # V0.7.6: o endpoint de consulta por atualização usa a atualização global
    # (inclui mudanças em dependentes). Mantemos esse carimbo separado da
    # atualização do registro-pai para distinguir alteração da contratação de
    # alteração apenas em itens/resultados/documentos.
    cols_contratacoes = {r[1] for r in con.execute("PRAGMA table_info(contratacoes)").fetchall()}
    if "data_atualizacao_global" not in cols_contratacoes:
        con.execute("ALTER TABLE contratacoes ADD COLUMN data_atualizacao_global TEXT")
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_contratacoes_data_atualizacao_global "
        "ON contratacoes(data_atualizacao_global)"
    )

    garantir_proveniencia(con, assumir_legado_pncp=True)
    con.commit()


def get_json(
    sessao: requests.Session,
    url: str,
    params: dict[str, Any] | None = None,
    timeout: int = 120,
) -> Any:
    resposta = sessao.get(url, params=params, timeout=timeout)

    # O endpoint de consultas pode responder 204 quando não há registros.
    if resposta.status_code == 204:
        return None

    if resposta.status_code >= 400:
        corpo = resposta.text.strip()
        if len(corpo) > 1200:
            corpo = corpo[:1200] + "..."
        detalhe = (
            f"HTTP {resposta.status_code} para {resposta.url}"
            + (f" | Resposta PNCP: {corpo}" if corpo else "")
        )
        raise requests.HTTPError(detalhe, response=resposta)

    if not resposta.content:
        return None

    return resposta.json()


def extrair_lista(payload: Any, *chaves: str) -> list[dict[str, Any]]:
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for chave in chaves:
            valor = payload.get(chave)
            if isinstance(valor, list):
                return valor
    return []


def numero_float(valor: Any) -> float | None:
    if valor is None or valor == "":
        return None
    try:
        return float(valor)
    except (TypeError, ValueError):
        return None


def bool_int(valor: Any) -> int | None:
    if valor is None:
        return None
    return 1 if bool(valor) else 0


def salvar_contratacao(con: sqlite3.Connection, c: dict[str, Any]) -> bool:
    agora = datetime.now().isoformat(timespec="seconds")

    orgao = c.get("orgaoEntidade") or {}
    unidade = c.get("unidadeOrgao") or {}
    amparo = c.get("amparoLegal") or {}

    numero_controle = c.get("numeroControlePNCP")
    if not numero_controle:
        raise ValueError("Registro sem numeroControlePNCP.")

    ano = c.get("anoCompra")
    sequencial = c.get("sequencialCompra")
    cnpj = orgao.get("cnpj")

    url_pncp = None
    if cnpj and ano is not None and sequencial is not None:
        url_pncp = f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{sequencial}"

    existia = (
        con.execute(
            "SELECT 1 FROM contratacoes WHERE numero_controle_pncp = ?",
            (numero_controle,),
        ).fetchone()
        is not None
    )

    con.execute(
        """
        INSERT INTO contratacoes (
            numero_controle_pncp, numero_compra, ano_compra, sequencial_compra, processo,
            tipo_instrumento_id, tipo_instrumento_nome,
            modalidade_id, modalidade_nome,
            modo_disputa_id, modo_disputa_nome,
            situacao_id, situacao_nome,
            objeto, informacao_complementar, srp,
            amparo_legal_codigo, amparo_legal_nome,
            valor_total_estimado, valor_total_homologado,
            data_abertura_proposta, data_encerramento_proposta,
            data_publicacao_pncp, data_inclusao, data_atualizacao, data_atualizacao_global,
            orgao_cnpj, orgao_razao_social, poder_id, esfera_id,
            unidade_codigo, unidade_nome, municipio_ibge,
            municipio_nome, uf_sigla, uf_nome,
            usuario_nome, link_sistema_origem, url_pncp,
            raw_json, primeira_coleta_em, ultima_coleta_em
        )
        VALUES (
            ?, ?, ?, ?, ?,
            ?, ?,
            ?, ?,
            ?, ?,
            ?, ?,
            ?, ?, ?,
            ?, ?,
            ?, ?,
            ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?
        )
        ON CONFLICT(numero_controle_pncp) DO UPDATE SET
            numero_compra = excluded.numero_compra,
            ano_compra = excluded.ano_compra,
            sequencial_compra = excluded.sequencial_compra,
            processo = excluded.processo,
            tipo_instrumento_id = excluded.tipo_instrumento_id,
            tipo_instrumento_nome = excluded.tipo_instrumento_nome,
            modalidade_id = excluded.modalidade_id,
            modalidade_nome = excluded.modalidade_nome,
            modo_disputa_id = excluded.modo_disputa_id,
            modo_disputa_nome = excluded.modo_disputa_nome,
            situacao_id = excluded.situacao_id,
            situacao_nome = excluded.situacao_nome,
            objeto = excluded.objeto,
            informacao_complementar = excluded.informacao_complementar,
            srp = excluded.srp,
            amparo_legal_codigo = excluded.amparo_legal_codigo,
            amparo_legal_nome = excluded.amparo_legal_nome,
            valor_total_estimado = excluded.valor_total_estimado,
            valor_total_homologado = excluded.valor_total_homologado,
            data_abertura_proposta = excluded.data_abertura_proposta,
            data_encerramento_proposta = excluded.data_encerramento_proposta,
            data_publicacao_pncp = excluded.data_publicacao_pncp,
            data_inclusao = excluded.data_inclusao,
            data_atualizacao = excluded.data_atualizacao,
            data_atualizacao_global = excluded.data_atualizacao_global,
            orgao_cnpj = excluded.orgao_cnpj,
            orgao_razao_social = excluded.orgao_razao_social,
            poder_id = excluded.poder_id,
            esfera_id = excluded.esfera_id,
            unidade_codigo = excluded.unidade_codigo,
            unidade_nome = excluded.unidade_nome,
            municipio_ibge = excluded.municipio_ibge,
            municipio_nome = excluded.municipio_nome,
            uf_sigla = excluded.uf_sigla,
            uf_nome = excluded.uf_nome,
            usuario_nome = excluded.usuario_nome,
            link_sistema_origem = excluded.link_sistema_origem,
            url_pncp = excluded.url_pncp,
            raw_json = excluded.raw_json,
            ultima_coleta_em = excluded.ultima_coleta_em
        """,
        (
            numero_controle,
            c.get("numeroCompra"),
            ano,
            sequencial,
            c.get("processo"),
            (c.get("tipoInstrumentoConvocatorioId") if c.get("tipoInstrumentoConvocatorioId") is not None else c.get("tipoInstrumentoConvocatorioCodigo")),
            c.get("tipoInstrumentoConvocatorioNome"),
            c.get("modalidadeId"),
            c.get("modalidadeNome"),
            c.get("modoDisputaId"),
            c.get("modoDisputaNome"),
            c.get("situacaoCompraId"),
            c.get("situacaoCompraNome"),
            c.get("objetoCompra"),
            c.get("informacaoComplementar"),
            bool_int(c.get("srp")),
            amparo.get("codigo"),
            amparo.get("nome"),
            numero_float(c.get("valorTotalEstimado")),
            numero_float(c.get("valorTotalHomologado")),
            c.get("dataAberturaProposta"),
            c.get("dataEncerramentoProposta"),
            c.get("dataPublicacaoPncp"),
            c.get("dataInclusao"),
            c.get("dataAtualizacao"),
            c.get("dataAtualizacaoGlobal"),
            cnpj,
            orgao.get("razaosocial") or orgao.get("razaoSocial"),
            orgao.get("poderId"),
            orgao.get("esferaId"),
            unidade.get("codigoUnidade"),
            unidade.get("nomeUnidade"),
            str(unidade.get("codigoIbge") or unidade.get("municipioId") or "") or None,
            unidade.get("municipioNome"),
            unidade.get("ufSigla"),
            unidade.get("ufNome"),
            c.get("usuarioNome"),
            c.get("linkSistemaOrigem"),
            url_pncp,
            json.dumps(c, ensure_ascii=False),
            agora,
            agora,
        ),
    )
    con.execute(
        """UPDATE contratacoes SET fonte_primeira=COALESCE(fonte_primeira, ?),
                  fonte_ultima=?, pncp_confirmado=1 WHERE numero_controle_pncp=?""",
        (FONTE_PNCP, FONTE_PNCP, numero_controle),
    )
    return not existia


def salvar_item(
    con: sqlite3.Connection,
    numero_controle: str,
    item: dict[str, Any],
) -> None:
    catalogo = item.get("catalogo") or {}
    categoria = item.get("categoriaItemCatalogo") or {}

    con.execute(
        """
        INSERT INTO itens (
            numero_controle_pncp, numero_item,
            material_ou_servico, material_ou_servico_nome,
            descricao, informacao_complementar,
            quantidade, unidade_medida,
            valor_unitario_estimado, valor_total,
            situacao_item_id, situacao_item_nome,
            criterio_julgamento_id, criterio_julgamento_nome,
            tem_resultado,
            ncm_nbs_codigo, ncm_nbs_descricao,
            catalogo_codigo_item, catalogo_id, catalogo_nome,
            categoria_catalogo_id, categoria_catalogo_nome,
            data_inclusao, data_atualizacao, raw_json
        )
        VALUES (
            ?, ?,
            ?, ?,
            ?, ?,
            ?, ?,
            ?, ?,
            ?, ?,
            ?, ?,
            ?,
            ?, ?,
            ?, ?, ?,
            ?, ?,
            ?, ?, ?
        )
        ON CONFLICT(numero_controle_pncp, numero_item) DO UPDATE SET
            material_ou_servico = excluded.material_ou_servico,
            material_ou_servico_nome = excluded.material_ou_servico_nome,
            descricao = excluded.descricao,
            informacao_complementar = excluded.informacao_complementar,
            quantidade = excluded.quantidade,
            unidade_medida = excluded.unidade_medida,
            valor_unitario_estimado = excluded.valor_unitario_estimado,
            valor_total = excluded.valor_total,
            situacao_item_id = excluded.situacao_item_id,
            situacao_item_nome = excluded.situacao_item_nome,
            criterio_julgamento_id = excluded.criterio_julgamento_id,
            criterio_julgamento_nome = excluded.criterio_julgamento_nome,
            tem_resultado = excluded.tem_resultado,
            ncm_nbs_codigo = excluded.ncm_nbs_codigo,
            ncm_nbs_descricao = excluded.ncm_nbs_descricao,
            catalogo_codigo_item = excluded.catalogo_codigo_item,
            catalogo_id = excluded.catalogo_id,
            catalogo_nome = excluded.catalogo_nome,
            categoria_catalogo_id = excluded.categoria_catalogo_id,
            categoria_catalogo_nome = excluded.categoria_catalogo_nome,
            data_inclusao = excluded.data_inclusao,
            data_atualizacao = excluded.data_atualizacao,
            raw_json = excluded.raw_json
        """,
        (
            numero_controle,
            item.get("numeroItem"),
            item.get("materialOuServico"),
            item.get("materialOuServicoNome"),
            item.get("descricao"),
            item.get("informacaoComplementar"),
            numero_float(item.get("quantidade")),
            item.get("unidadeMedida"),
            numero_float(item.get("valorUnitarioEstimado")),
            numero_float(item.get("valorTotal")),
            (item.get("situacaoCompraItemId") if item.get("situacaoCompraItemId") is not None else item.get("situacaoCompraItem")),
            item.get("situacaoCompraItemNome"),
            item.get("criterioJulgamentoId"),
            item.get("criterioJulgamentoNome"),
            bool_int(item.get("temResultado")),
            item.get("ncmNbsCodigo"),
            item.get("ncmNbsDescricao"),
            (item.get("catalogoCodigoItem") or item.get("codItemCatalogo")),
            catalogo.get("id") or item.get("catalogoId"),
            (catalogo.get("nome") or item.get("catalogoNome")),
            categoria.get("id") or item.get("categoriaItemCatalogoId") or item.get("itemCategoriaIdPncp"),
            (categoria.get("nome") or item.get("categoriaItemCatalogoNome") or item.get("itemCategoriaNome")),
            item.get("dataInclusao"),
            item.get("dataAtualizacao"),
            json.dumps(item, ensure_ascii=False),
        ),
    )
    con.execute(
        """UPDATE itens SET fonte_primeira=COALESCE(fonte_primeira, ?),
                  fonte_ultima=?, pncp_confirmado=1
            WHERE numero_controle_pncp=? AND numero_item=?""",
        (FONTE_PNCP, FONTE_PNCP, numero_controle, item.get("numeroItem")),
    )


def consultar_contratacoes_modalidade(
    sessao: requests.Session,
    data_ref: str,
    modalidade_id: int,
) -> list[dict[str, Any]]:
    url = f"{BASE_CONSULTA}/contratacoes/publicacao"
    todos: list[dict[str, Any]] = []
    pagina = 1

    while True:
        params = {
            "dataInicial": data_ref,
            "dataFinal": data_ref,
            "codigoModalidadeContratacao": modalidade_id,
            "pagina": pagina,
            "tamanhoPagina": TAMANHO_PAGINA_CONTRATACOES,
        }

        payload = get_json(sessao, url, params=params)
        registros = extrair_lista(payload, "data")

        if not registros:
            break

        todos.extend(registros)

        total_paginas = payload.get("totalPaginas") if isinstance(payload, dict) else None
        restantes = payload.get("paginasRestantes") if isinstance(payload, dict) else None

        if total_paginas is not None:
            if pagina >= int(total_paginas):
                break
        elif restantes is not None:
            if int(restantes) <= 0:
                break
        elif len(registros) < TAMANHO_PAGINA_CONTRATACOES:
            break

        pagina += 1

    return todos


def consultar_itens(
    sessao: requests.Session,
    cnpj: str,
    ano: int,
    sequencial: int,
) -> list[dict[str, Any]]:
    url = f"{BASE_PNCP}/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens"
    todos: list[dict[str, Any]] = []
    pagina = 1

    while True:
        params = {"pagina": pagina, "tamanhoPagina": TAMANHO_PAGINA_ITENS}
        payload = get_json(sessao, url, params=params)

        # A API já teve formatos diferentes ao longo das versões.
        # Aceitamos lista direta, {"itens": [...]}, ou {"data": [...]}.
        registros = extrair_lista(payload, "itens", "data")

        if not registros:
            break

        todos.extend(registros)

        if isinstance(payload, list):
            break

        total_paginas = payload.get("totalPaginas") if isinstance(payload, dict) else None
        restantes = payload.get("paginasRestantes") if isinstance(payload, dict) else None

        if total_paginas is not None:
            if pagina >= int(total_paginas):
                break
        elif restantes is not None:
            if int(restantes) <= 0:
                break
        elif len(registros) < TAMANHO_PAGINA_ITENS:
            break

        pagina += 1

    return todos


def registrar_erro(
    con: sqlite3.Connection,
    coleta_id: int,
    etapa: str,
    erro: Exception,
    numero_controle: str | None = None,
    url: str | None = None,
) -> None:
    con.execute(
        """
        INSERT INTO erros_coleta (
            coleta_id, etapa, numero_controle_pncp, url, erro, criado_em
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            coleta_id,
            etapa,
            numero_controle,
            url,
            repr(erro),
            datetime.now().isoformat(timespec="seconds"),
        ),
    )



def preparar_controle_modalidades(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS coletas_modalidades (
            data_referencia TEXT NOT NULL,
            modalidade_id INTEGER NOT NULL,
            modalidade_nome TEXT NOT NULL,
            status TEXT NOT NULL,
            total_registros INTEGER NOT NULL DEFAULT 0,
            ultima_pagina INTEGER NOT NULL DEFAULT 0,
            erro TEXT,
            atualizado_em TEXT NOT NULL,
            PRIMARY KEY (data_referencia, modalidade_id)
        )
        """
    )
    con.commit()


def modalidade_ja_concluida(
    con: sqlite3.Connection,
    data_ref: str,
    modalidade_id: int,
) -> bool:
    row = con.execute(
        """
        SELECT status
        FROM coletas_modalidades
        WHERE data_referencia = ? AND modalidade_id = ?
        """,
        (data_ref, modalidade_id),
    ).fetchone()
    return bool(row and row["status"] == "CONCLUIDA")


def marcar_modalidade(
    con: sqlite3.Connection,
    data_ref: str,
    modalidade_id: int,
    modalidade_nome: str,
    status: str,
    total_registros: int = 0,
    ultima_pagina: int = 0,
    erro: str | None = None,
) -> None:
    con.execute(
        """
        INSERT INTO coletas_modalidades (
            data_referencia, modalidade_id, modalidade_nome, status,
            total_registros, ultima_pagina, erro, atualizado_em
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(data_referencia, modalidade_id) DO UPDATE SET
            modalidade_nome = excluded.modalidade_nome,
            status = excluded.status,
            total_registros = excluded.total_registros,
            ultima_pagina = excluded.ultima_pagina,
            erro = excluded.erro,
            atualizado_em = excluded.atualizado_em
        """,
        (
            data_ref,
            modalidade_id,
            modalidade_nome,
            status,
            total_registros,
            ultima_pagina,
            erro[:4000] if erro else None,
            datetime.now().isoformat(timespec="seconds"),
        ),
    )
    con.commit()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Coleta incremental de contratações publicadas no PNCP."
    )
    parser.add_argument(
        "--data",
        help="Data da publicação: AAAA-MM-DD ou AAAAMMDD. Padrão: ontem.",
    )
    parser.add_argument(
        "--db",
        default=str(DB_PADRAO),
        help=f"Caminho do SQLite. Padrão: {DB_PADRAO}",
    )
    parser.add_argument(
        "--modalidade",
        type=int,
        choices=sorted(MODALIDADES),
        help="Coleta somente uma modalidade específica (1 a 14).",
    )
    parser.add_argument(
        "--forcar",
        action="store_true",
        help="Reconsulta modalidades já marcadas como concluídas.",
    )
    # Mantido por compatibilidade com os comandos das versões anteriores.
    # Na V0.4 os itens são sempre coletados separadamente por coletar_itens.py.
    parser.add_argument(
        "--sem-itens",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--falhas-outage",
        type=int,
        default=3,
        help="Falhas transitórias consecutivas para sinalizar queda geral do PNCP.",
    )
    args = parser.parse_args()

    try:
        data_ref = normalizar_data(args.data)
    except ValueError as e:
        print(f"ERRO: {e}", file=sys.stderr)
        return 2

    db_path = Path(args.db)
    con = conectar(db_path)
    criar_banco(con)
    preparar_controle_modalidades(con)
    sessao = criar_sessao()

    inicio = datetime.now().isoformat(timespec="seconds")
    cur = con.execute(
        """
        INSERT INTO coletas (data_referencia, inicio_em, status)
        VALUES (?, ?, 'EM_ANDAMENTO')
        """,
        (data_ref, inicio),
    )
    coleta_id = int(cur.lastrowid)
    con.commit()

    print("=" * 86)
    print("Radar PNCP V0.7.6 - Coleta incremental de contratações")
    print("=" * 86)
    print(f"Data:  {data_ref[6:8]}/{data_ref[4:6]}/{data_ref[:4]}")
    print(f"Banco: {db_path.resolve()}")
    print()

    recebidas = 0
    novas = 0
    atualizadas = 0
    modalidades_com_erro = 0
    modalidades_puladas = 0
    falhas_transitorias_consecutivas = 0
    current_modalidade: tuple[int, str] | None = None
    current_pagina = 0
    current_total = 0

    modalidades_execucao = (
        [(args.modalidade, MODALIDADES[args.modalidade])]
        if args.modalidade
        else list(MODALIDADES.items())
    )

    try:
        for modalidade_id, modalidade_nome in modalidades_execucao:
            current_modalidade = (modalidade_id, modalidade_nome)
            current_pagina = 0
            current_total = 0

            if not args.forcar and modalidade_ja_concluida(
                con, data_ref, modalidade_id
            ):
                modalidades_puladas += 1
                print(
                    f"[{modalidade_id:02d}] {modalidade_nome}: "
                    "já concluída — pulando"
                )
                continue

            print(f"[{modalidade_id:02d}] {modalidade_nome}")
            marcar_modalidade(
                con,
                data_ref,
                modalidade_id,
                modalidade_nome,
                "EM_ANDAMENTO",
            )

            url_contratacoes = f"{BASE_CONSULTA}/contratacoes/publicacao"
            pagina = 1
            total_modalidade = 0
            erro_modalidade = False

            while True:
                current_pagina = pagina
                current_total = total_modalidade

                params = {
                    "dataInicial": data_ref,
                    "dataFinal": data_ref,
                    "codigoModalidadeContratacao": modalidade_id,
                    "pagina": pagina,
                    "tamanhoPagina": TAMANHO_PAGINA_CONTRATACOES,
                }

                try:
                    payload = get_json(sessao, url_contratacoes, params=params)
                    registros = extrair_lista(payload, "data")
                    falhas_transitorias_consecutivas = 0
                except Exception as e:
                    erro_modalidade = True
                    modalidades_com_erro += 1
                    erro_msg = str(e)
                    print(f"     página {pagina}: ERRO ({erro_msg})")
                    registrar_erro(
                        con,
                        coleta_id,
                        f"contratacoes_modalidade_{modalidade_id}_pagina_{pagina}",
                        e,
                        url=url_contratacoes,
                    )
                    marcar_modalidade(
                        con,
                        data_ref,
                        modalidade_id,
                        modalidade_nome,
                        "ERRO",
                        total_modalidade,
                        pagina,
                        erro_msg,
                    )
                    if eh_erro_transitorio(e):
                        falhas_transitorias_consecutivas += 1
                    else:
                        falhas_transitorias_consecutivas = 0
                    if falhas_transitorias_consecutivas >= max(1, args.falhas_outage):
                        raise ServicoPNCPIndisponivel(
                            f"{falhas_transitorias_consecutivas} falhas transitórias consecutivas no PNCP."
                        )
                    break

                if not registros:
                    if pagina == 1:
                        print("     0 registros")
                    break

                print(f"     página {pagina}: {len(registros)} registros")
                recebidas += len(registros)
                total_modalidade += len(registros)

                for c in registros:
                    try:
                        eh_nova = salvar_contratacao(con, c)
                        if eh_nova:
                            novas += 1
                        else:
                            atualizadas += 1
                    except Exception as e:
                        registrar_erro(
                            con,
                            coleta_id,
                            "salvar_contratacao",
                            e,
                            c.get("numeroControlePNCP"),
                        )

                # Cada página fica persistida imediatamente.
                con.commit()

                total_paginas = (
                    payload.get("totalPaginas")
                    if isinstance(payload, dict)
                    else None
                )
                restantes = (
                    payload.get("paginasRestantes")
                    if isinstance(payload, dict)
                    else None
                )

                if total_paginas is not None:
                    if pagina >= int(total_paginas):
                        break
                elif restantes is not None:
                    if int(restantes) <= 0:
                        break
                elif len(registros) < TAMANHO_PAGINA_CONTRATACOES:
                    break

                pagina += 1

            if not erro_modalidade:
                marcar_modalidade(
                    con,
                    data_ref,
                    modalidade_id,
                    modalidade_nome,
                    "CONCLUIDA",
                    total_modalidade,
                    pagina,
                )
                print(f"     total modalidade: {total_modalidade}")

        fim = datetime.now().isoformat(timespec="seconds")
        status_final = (
            "CONCLUIDA"
            if modalidades_com_erro == 0
            else "CONCLUIDA_COM_ERROS"
        )
        observacao_partes = []
        if modalidades_com_erro:
            observacao_partes.append(
                f"{modalidades_com_erro} modalidade(s) tiveram erro."
            )
        if modalidades_puladas:
            observacao_partes.append(
                f"{modalidades_puladas} modalidade(s) já estavam concluídas."
            )
        observacao = " ".join(observacao_partes) or None

        con.execute(
            """
            UPDATE coletas
            SET fim_em = ?, status = ?,
                contratacoes_recebidas = ?,
                contratacoes_gravadas = ?,
                itens_gravados = 0,
                erros_itens = 0,
                observacao = ?
            WHERE id = ?
            """,
            (
                fim,
                status_final,
                recebidas,
                novas + atualizadas,
                observacao,
                coleta_id,
            ),
        )
        con.commit()

    except ServicoPNCPIndisponivel as exc:
        fim = datetime.now().isoformat(timespec="seconds")
        con.execute(
            """
            UPDATE coletas
            SET fim_em = ?, status = 'AGUARDANDO_PNCP',
                contratacoes_recebidas = ?, contratacoes_gravadas = ?, observacao = ?
            WHERE id = ?
            """,
            (fim, recebidas, novas + atualizadas, str(exc), coleta_id),
        )
        con.commit()
        print("\n>>> PNCP APARENTEMENTE INDISPONÍVEL <<<")
        print(str(exc))
        print("O orquestrador histórico aguardará o serviço voltar antes de continuar.")
        return EXIT_PNCP_INDISPONIVEL

    except KeyboardInterrupt:
        fim = datetime.now().isoformat(timespec="seconds")

        if current_modalidade is not None:
            modalidade_id, modalidade_nome = current_modalidade
            marcar_modalidade(
                con,
                data_ref,
                modalidade_id,
                modalidade_nome,
                "INTERROMPIDA",
                current_total,
                current_pagina,
                "Execução interrompida pelo usuário.",
            )

        con.execute(
            """
            UPDATE coletas
            SET fim_em = ?, status = 'INTERROMPIDA',
                contratacoes_recebidas = ?,
                contratacoes_gravadas = ?,
                observacao = ?
            WHERE id = ?
            """,
            (
                fim,
                recebidas,
                novas + atualizadas,
                "Execução interrompida pelo usuário.",
                coleta_id,
            ),
        )
        con.commit()
        print("\nColeta interrompida. As modalidades concluídas serão puladas na retomada.")
        return 130

    finally:
        sessao.close()
        con.close()

    print()
    print("-" * 86)
    print("RESUMO — CONTRATAÇÕES")
    print(f"Recebidas nesta execução: {recebidas}")
    print(f"Novas:                    {novas}")
    print(f"Atualizadas:               {atualizadas}")
    print(f"Modalidades puladas:       {modalidades_puladas}")
    print(f"Modalidades com erro:      {modalidades_com_erro}")
    print(f"Banco:                     {db_path.resolve()}")
    print("-" * 86)

    return 0 if modalidades_com_erro == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
