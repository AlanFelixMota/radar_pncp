#!/usr/bin/env python3
"""
Radar PNCP V0.4.1 - Coletor incremental de itens do PNCP.

Ele lê as contratações já existentes em dados/compras.db e consulta SOMENTE
aquelas que ainda possuem itens_coletados = 0.

Exemplos:
    python coletar_itens.py --limite 100
    python coletar_itens.py
    python coletar_itens.py --data 2026-08-26
    python coletar_itens.py --modalidade 6 --limite 200
    python coletar_itens.py --somente-erros
    python coletar_itens.py --reprocessar --limite 10
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
import threading
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime
from pathlib import Path

from radar_paths import DB_COMPRAS, garantir_diretorios
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from proveniencia import FONTE_PNCP, garantir_proveniencia

from pncp_health import (
    EXIT_PNCP_INDISPONIVEL, RateController, eh_erro_transitorio, status_http_erro,
)


BASE_CONSULTA = "https://pncp.gov.br/api/consulta/v1"
BASE_PNCP = "https://pncp.gov.br/api/pncp/v1"
garantir_diretorios()
DB_PADRAO = DB_COMPRAS
TAMANHO_PAGINA = 50


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
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/151.0.0.0 Safari/537.36"
            ),
        }
    )
    return sessao


def normalizar_data(valor: str | None) -> str | None:
    if not valor:
        return None

    valor = valor.strip()
    for formato in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(valor, formato).strftime("%Y-%m-%d")
        except ValueError:
            pass

    raise ValueError("Data inválida. Use AAAA-MM-DD ou AAAAMMDD.")


def conectar(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON")
    con.execute("PRAGMA journal_mode = WAL")
    con.execute("PRAGMA synchronous = NORMAL")
    return con


def coluna_existe(con: sqlite3.Connection, tabela: str, coluna: str) -> bool:
    rows = con.execute(f"PRAGMA table_info({tabela})").fetchall()
    return any(row["name"] == coluna for row in rows)


def preparar_banco(con: sqlite3.Connection) -> None:
    """
    Migração compatível com o compras.db das versões anteriores.
    Não apaga nem recria os dados existentes.
    """
    if not coluna_existe(con, "contratacoes", "itens_tentativas"):
        con.execute(
            "ALTER TABLE contratacoes "
            "ADD COLUMN itens_tentativas INTEGER NOT NULL DEFAULT 0"
        )

    if not coluna_existe(con, "contratacoes", "itens_ultima_tentativa"):
        con.execute(
            "ALTER TABLE contratacoes "
            "ADD COLUMN itens_ultima_tentativa TEXT"
        )

    if not coluna_existe(con, "contratacoes", "itens_erro"):
        con.execute(
            "ALTER TABLE contratacoes "
            "ADD COLUMN itens_erro TEXT"
        )

    if not coluna_existe(con, "contratacoes", "itens_status"):
        con.execute(
            "ALTER TABLE contratacoes "
            "ADD COLUMN itens_status TEXT NOT NULL DEFAULT 'PENDENTE'"
        )

    if not coluna_existe(con, "contratacoes", "itens_http_status"):
        con.execute(
            "ALTER TABLE contratacoes "
            "ADD COLUMN itens_http_status INTEGER"
        )

    if not coluna_existe(con, "contratacoes", "itens_indisponivel_em"):
        con.execute(
            "ALTER TABLE contratacoes "
            "ADD COLUMN itens_indisponivel_em TEXT"
        )

    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS execucoes_itens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            inicio_em TEXT NOT NULL,
            fim_em TEXT,
            status TEXT NOT NULL,
            filtro_data TEXT,
            filtro_modalidade INTEGER,
            limite INTEGER,
            somente_erros INTEGER NOT NULL DEFAULT 0,
            reprocessar INTEGER NOT NULL DEFAULT 0,
            pendentes_inicio INTEGER NOT NULL DEFAULT 0,
            processadas INTEGER NOT NULL DEFAULT 0,
            sucessos INTEGER NOT NULL DEFAULT 0,
            erros INTEGER NOT NULL DEFAULT 0,
            itens_gravados INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS erros_itens_coleta (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            execucao_id INTEGER NOT NULL,
            numero_controle_pncp TEXT NOT NULL,
            url TEXT,
            erro TEXT NOT NULL,
            criado_em TEXT NOT NULL,
            FOREIGN KEY (execucao_id) REFERENCES execucoes_itens(id)
        );

        CREATE INDEX IF NOT EXISTS idx_contratacoes_itens_pendentes
            ON contratacoes(itens_coletados, data_publicacao_pncp);

        CREATE INDEX IF NOT EXISTS idx_erros_itens_numero_controle
            ON erros_itens_coleta(numero_controle_pncp);
        """
    )

    if not coluna_existe(con, "execucoes_itens", "indisponiveis"):
        con.execute(
            "ALTER TABLE execucoes_itens "
            "ADD COLUMN indisponiveis INTEGER NOT NULL DEFAULT 0"
        )

    # Migração dos registros das versões anteriores.
    # 1) Tudo que já teve itens coletados é OK.
    con.execute(
        """
        UPDATE contratacoes
        SET itens_status = 'OK',
            itens_http_status = NULL,
            itens_indisponivel_em = NULL
        WHERE itens_coletados = 1
        """
    )

    # 2) 404/410 já registrados passam a ser tratados como indisponibilidade
    #    externa do PNCP, não como pendência eterna. O código HTTP é preservado.
    con.execute(
        """
        UPDATE contratacoes
        SET itens_status = 'INDISPONIVEL_PNCP',
            itens_http_status = CASE
                WHEN itens_http_status = 410 OR LOWER(COALESCE(itens_erro, '')) LIKE '%http 410%' THEN 410
                ELSE 404
            END,
            itens_indisponivel_em =
                COALESCE(itens_indisponivel_em, itens_ultima_tentativa)
        WHERE itens_coletados = 0
          AND (
                itens_http_status IN (404, 410)
                OR LOWER(COALESCE(itens_erro, '')) LIKE '%http 404%'
                OR LOWER(COALESCE(itens_erro, '')) LIKE '%http 410%'
                OR LOWER(COALESCE(itens_erro, '')) LIKE '%410 gone%'
                OR LOWER(COALESCE(itens_erro, '')) LIKE '%contratação não cadastrada%'
                OR LOWER(COALESCE(itens_erro, '')) LIKE '%contratacao nao cadastrada%'
              )
        """
    )

    # 3) Outros erros anteriores permanecem elegíveis para nova tentativa.
    con.execute(
        """
        UPDATE contratacoes
        SET itens_status = 'ERRO_TEMPORARIO'
        WHERE itens_coletados = 0
          AND itens_erro IS NOT NULL
          AND COALESCE(itens_status, 'PENDENTE') <> 'INDISPONIVEL_PNCP'
        """
    )

    garantir_proveniencia(con, assumir_legado_pncp=True)
    con.commit()


def get_json(
    sessao: requests.Session,
    url: str,
    params: dict[str, Any] | None,
    timeout: int,
) -> Any:
    resposta = sessao.get(url, params=params, timeout=timeout)

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


def extrair_lista(payload: Any) -> list[dict[str, Any]]:
    if payload is None:
        return []

    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        for chave in ("itens", "data"):
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


def consultar_itens(
    sessao: requests.Session,
    cnpj: str,
    ano: int,
    sequencial: int,
    timeout: int,
) -> tuple[list[dict[str, Any]], int]:
    """Consulta todas as páginas de itens de forma robusta.

    O PNCP pode retornar a coleção como lista direta ou dentro de um objeto.
    Continuamos enquanto a página vier cheia, independentemente do formato,
    e detectamos repetição de página para evitar loop infinito.
    """
    url = f"{BASE_PNCP}/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens"
    todos: list[dict[str, Any]] = []
    pagina = 1
    paginas_consultadas = 0
    assinaturas_paginas: set[tuple[Any, ...]] = set()
    max_paginas_seguranca = 1000

    while True:
        if pagina > max_paginas_seguranca:
            raise RuntimeError(
                f"Limite de segurança de {max_paginas_seguranca} páginas atingido."
            )

        params = {"pagina": pagina, "tamanhoPagina": TAMANHO_PAGINA}
        payload = get_json(sessao, url, params=params, timeout=timeout)
        paginas_consultadas += 1
        registros = extrair_lista(payload)

        if not registros:
            break

        assinatura = tuple(item.get("numeroItem") for item in registros)
        if assinatura in assinaturas_paginas:
            raise RuntimeError(
                f"O PNCP repetiu a mesma página de itens na página {pagina}; "
                "consulta interrompida para evitar loop e dados incompletos."
            )
        assinaturas_paginas.add(assinatura)
        todos.extend(registros)

        total_paginas = payload.get("totalPaginas") if isinstance(payload, dict) else None
        restantes = payload.get("paginasRestantes") if isinstance(payload, dict) else None

        if total_paginas is not None:
            if pagina >= int(total_paginas):
                break
        elif restantes is not None:
            if int(restantes) <= 0:
                break
        elif len(registros) < TAMANHO_PAGINA:
            break

        pagina += 1

    return todos, paginas_consultadas
def salvar_item(
    con: sqlite3.Connection,
    numero_controle: str,
    item: dict[str, Any],
) -> None:
    catalogo = item.get("catalogo") or {}
    categoria = item.get("categoriaItemCatalogo") or {}

    numero_item = item.get("numeroItem")
    if numero_item is None:
        raise ValueError("Item retornado sem numeroItem.")

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
            numero_item,
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
        (FONTE_PNCP, FONTE_PNCP, numero_controle, numero_item),
    )


def construir_filtro(
    data_ref: str | None,
    modalidade: int | None,
    somente_erros: bool,
    reprocessar: bool,
    reprocessar_50: bool = False,
    rever_indisponiveis: bool = False,
) -> tuple[str, list[Any]]:
    condicoes: list[str] = []
    parametros: list[Any] = []

    if rever_indisponiveis:
        condicoes.append("c.itens_status = 'INDISPONIVEL_PNCP'")
    elif reprocessar_50:
        condicoes.append("c.itens_coletados = 1")
        condicoes.append("c.qtd_itens = 50")
    elif not reprocessar:
        condicoes.append("c.itens_coletados = 0")
        condicoes.append(
            "COALESCE(c.itens_status, 'PENDENTE') <> 'INDISPONIVEL_PNCP'"
        )

    if data_ref:
        # dataPublicacaoPncp normalmente traz timestamp; substr funciona para ambos
        # os formatos YYYY-MM-DD e YYYY-MM-DDTHH:MM:SS.
        condicoes.append("substr(c.data_publicacao_pncp, 1, 10) = ?")
        parametros.append(data_ref)

    if modalidade is not None:
        condicoes.append("c.modalidade_id = ?")
        parametros.append(modalidade)

    if somente_erros:
        condicoes.append(
            "COALESCE(c.itens_status, 'PENDENTE') = 'ERRO_TEMPORARIO'"
        )

    where = " AND ".join(condicoes) if condicoes else "1=1"
    return where, parametros


def selecionar_contratacoes(
    con: sqlite3.Connection,
    data_ref: str | None,
    modalidade: int | None,
    somente_erros: bool,
    reprocessar: bool,
    limite: int | None,
    reprocessar_50: bool = False,
    rever_indisponiveis: bool = False,
) -> list[sqlite3.Row]:
    where, parametros = construir_filtro(
        data_ref,
        modalidade,
        somente_erros,
        reprocessar,
        reprocessar_50,
        rever_indisponiveis,
    )

    sql = f"""
        SELECT
            c.numero_controle_pncp,
            c.orgao_cnpj,
            c.ano_compra,
            c.sequencial_compra,
            c.orgao_razao_social,
            c.modalidade_nome,
            c.objeto,
            c.itens_tentativas,
            c.itens_coletados
        FROM contratacoes c
        WHERE {where}
        ORDER BY
            c.itens_tentativas ASC,
            c.data_publicacao_pncp ASC,
            c.numero_controle_pncp ASC
    """

    if limite is not None:
        sql += " LIMIT ?"
        parametros.append(limite)

    return con.execute(sql, parametros).fetchall()


def contar_contratacoes(
    con: sqlite3.Connection,
    data_ref: str | None,
    modalidade: int | None,
    somente_erros: bool,
    reprocessar: bool,
    reprocessar_50: bool = False,
    rever_indisponiveis: bool = False,
) -> int:
    where, parametros = construir_filtro(
        data_ref,
        modalidade,
        somente_erros,
        reprocessar,
        reprocessar_50,
        rever_indisponiveis,
    )
    row = con.execute(
        f"SELECT COUNT(*) AS qtd FROM contratacoes c WHERE {where}",
        parametros,
    ).fetchone()
    return int(row["qtd"])


def resumir(texto: str | None, limite: int = 72) -> str:
    if not texto:
        return "-"
    texto = " ".join(texto.split())
    return texto if len(texto) <= limite else texto[: limite - 3] + "..."


def _sessao_threadlocal_factory():
    local = threading.local()
    def obter():
        if not hasattr(local, "sessao"):
            local.sessao = criar_sessao()
        return local.sessao
    return obter


def _buscar_um(c: dict[str, Any], timeout: int, rate: RateController, get_sessao):
    numero_controle = c["numero_controle_pncp"]
    cnpj = c["orgao_cnpj"]
    ano = c["ano_compra"]
    sequencial = c["sequencial_compra"]
    if not (numero_controle and cnpj and ano is not None and sequencial is not None):
        return {"tipo": "erro_local", "c": c, "erro": "Faltam CNPJ, ano ou sequencial para consultar itens."}
    rate.aguardar()
    try:
        itens, paginas = consultar_itens(get_sessao(), str(cnpj), int(ano), int(sequencial), timeout)
        rate.sucesso()
        return {"tipo": "ok", "c": c, "itens": itens, "paginas": paginas}
    except Exception as exc:
        status = status_http_erro(exc)
        if eh_erro_transitorio(exc):
            rate.erro_transitorio(status)
        return {"tipo": "erro", "c": c, "erro": str(exc), "exc": exc, "http_status": status}


def _finalizar_execucao(con, execucao_id, status, processadas, sucessos, erros, indisponiveis, itens_gravados):
    con.execute(
        """
        UPDATE execucoes_itens
        SET fim_em=?, status=?, processadas=?, sucessos=?, erros=?, indisponiveis=?, itens_gravados=?
        WHERE id=?
        """,
        (datetime.now().isoformat(timespec="seconds"), status, processadas, sucessos, erros,
         indisponiveis, itens_gravados, execucao_id),
    )
    con.commit()


def main() -> int:
    parser = argparse.ArgumentParser(description="Coleta incremental/resumível de itens do PNCP — V0.7.6.")
    parser.add_argument("--db", default=str(DB_PADRAO))
    parser.add_argument("--limite", type=int)
    parser.add_argument("--data")
    parser.add_argument("--modalidade", type=int, choices=range(1, 15))
    parser.add_argument("--somente-erros", action="store_true")
    parser.add_argument("--reprocessar", action="store_true")
    parser.add_argument("--reprocessar-50", action="store_true")
    parser.add_argument("--rever-indisponiveis", action="store_true")
    parser.add_argument("--pausa", type=float, default=0.05,
                        help="Pausa-base adaptativa entre requisições. Padrão: 0.05 s.")
    parser.add_argument("--timeout", type=int, default=90)
    parser.add_argument("--workers", type=int, default=3,
                        help="Concorrência controlada. Padrão: 3; recomendado 1–4.")
    parser.add_argument("--falhas-outage", type=int, default=3,
                        help="Falhas transitórias consecutivas para sinalizar indisponibilidade geral.")
    args = parser.parse_args()

    if args.limite is not None and args.limite <= 0:
        print("ERRO: --limite deve ser maior que zero.", file=sys.stderr); return 2
    if not 1 <= args.workers <= 8:
        print("ERRO: --workers deve estar entre 1 e 8.", file=sys.stderr); return 2
    if args.pausa < 0:
        print("ERRO: --pausa não pode ser negativa.", file=sys.stderr); return 2
    modos_especiais = sum(bool(x) for x in (args.reprocessar, args.reprocessar_50, args.rever_indisponiveis))
    if modos_especiais > 1:
        print("ERRO: use apenas um modo especial de reprocessamento.", file=sys.stderr); return 2
    try:
        data_ref = normalizar_data(args.data)
    except ValueError as e:
        print(f"ERRO: {e}", file=sys.stderr); return 2

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"ERRO: banco não encontrado: {db_path}", file=sys.stderr); return 2
    con = conectar(db_path); preparar_banco(con)
    total_pendente = contar_contratacoes(con, data_ref, args.modalidade, args.somente_erros,
        args.reprocessar, args.reprocessar_50, args.rever_indisponiveis)
    rows = selecionar_contratacoes(con, data_ref, args.modalidade, args.somente_erros,
        args.reprocessar, args.limite, args.reprocessar_50, args.rever_indisponiveis)
    selecionadas = [dict(r) for r in rows]

    inicio = datetime.now().isoformat(timespec="seconds")
    cur = con.execute(
        """
        INSERT INTO execucoes_itens(inicio_em,status,filtro_data,filtro_modalidade,limite,
            somente_erros,reprocessar,pendentes_inicio)
        VALUES (?,'EM_ANDAMENTO',?,?,?,?,?,?)
        """,
        (inicio, data_ref, args.modalidade, args.limite, int(args.somente_erros), int(args.reprocessar), total_pendente),
    )
    execucao_id = int(cur.lastrowid); con.commit()

    print("="*86)
    print("Radar PNCP V0.7.6 — Coleta de itens concorrente e adaptativa")
    print("="*86)
    print(f"Banco:                  {db_path.resolve()}")
    print(f"Pendentes no filtro:    {total_pendente}")
    print(f"Selecionadas:           {len(selecionadas)}")
    print(f"Workers:                {args.workers}")
    print(f"Pausa-base inicial:     {args.pausa:.3f} s")
    if data_ref: print(f"Data:                   {data_ref}")
    print()

    if not selecionadas:
        _finalizar_execucao(con, execucao_id, "CONCLUIDA", 0, 0, 0, 0, 0)
        con.close(); print("Nenhuma contratação para processar."); return 0

    processadas=sucessos=erros=indisponiveis=itens_gravados=0
    consecutivas_transitorias=0
    outage=False
    rate=RateController(delay_inicial=args.pausa)
    get_sessao=_sessao_threadlocal_factory()

    def marcar_tentativa(c):
        agora=datetime.now().isoformat(timespec="seconds")
        con.execute("UPDATE contratacoes SET itens_tentativas=itens_tentativas+1,itens_ultima_tentativa=? WHERE numero_controle_pncp=?",
                    (agora,c["numero_controle_pncp"]))
        con.commit()

    def gravar_resultado(res):
        nonlocal processadas,sucessos,erros,indisponiveis,itens_gravados,consecutivas_transitorias,outage
        c=res["c"]; numero=c["numero_controle_pncp"]; processadas+=1
        if res["tipo"]=="ok":
            itens=res["itens"]
            con.execute("DELETE FROM itens WHERE numero_controle_pncp=?",(numero,))
            for item in itens: salvar_item(con,numero,item)
            agora=datetime.now().isoformat(timespec="seconds")
            con.execute("""UPDATE contratacoes SET itens_coletados=1,qtd_itens=?,itens_erro=NULL,
                itens_status='OK',itens_http_status=NULL,itens_indisponivel_em=NULL,itens_ultima_tentativa=?,itens_fonte=?
                WHERE numero_controle_pncp=?""",(len(itens),agora,FONTE_PNCP,numero))
            con.commit(); sucessos+=1; itens_gravados+=len(itens); consecutivas_transitorias=0
            pg=f" | {res['paginas']} pág." if res['paginas']>1 else ""
            print(f"    OK {numero}: {len(itens)} item(ns){pg}")
            return
        erro_msg=res.get("erro","Erro desconhecido")[:4000]
        agora=datetime.now().isoformat(timespec="seconds")
        http_status=res.get("http_status")
        if http_status in (404, 410):
            indisponiveis+=1; consecutivas_transitorias=0
            con.execute("""UPDATE contratacoes SET itens_coletados=0,qtd_itens=0,itens_erro=?,
                itens_status='INDISPONIVEL_PNCP',itens_http_status=?,itens_indisponivel_em=?,itens_ultima_tentativa=?
                WHERE numero_controle_pncp=?""",(erro_msg,http_status,agora,agora,numero))
            con.commit(); print(f"    INDISPONÍVEL {http_status}: {numero}"); return
        erros+=1
        exc=res.get("exc")
        transitorio = bool(exc and eh_erro_transitorio(exc))
        consecutivas_transitorias = consecutivas_transitorias+1 if transitorio else 0
        con.execute("""UPDATE contratacoes SET itens_erro=?,itens_status='ERRO_TEMPORARIO',
            itens_http_status=?,itens_ultima_tentativa=? WHERE numero_controle_pncp=?""",
            (erro_msg,http_status,agora,numero))
        con.execute("INSERT INTO erros_itens_coleta(execucao_id,numero_controle_pncp,url,erro,criado_em) VALUES (?,?,?,?,?)",
            (execucao_id,numero,None,erro_msg,agora))
        con.commit(); print(f"    ERRO TEMPORÁRIO {numero}: {erro_msg}")
        if consecutivas_transitorias>=args.falhas_outage:
            outage=True

    iterator=iter(selecionadas)
    in_flight={}
    try:
        with ThreadPoolExecutor(max_workers=args.workers, thread_name_prefix="pncp-itens") as pool:
            def preencher():
                if outage: return
                while len(in_flight)<args.workers:
                    try: c=next(iterator)
                    except StopIteration: break
                    marcar_tentativa(c)
                    fut=pool.submit(_buscar_um,c,args.timeout,rate,get_sessao)
                    in_flight[fut]=c
            preencher()
            while in_flight:
                done,_=wait(tuple(in_flight),return_when=FIRST_COMPLETED)
                for fut in done:
                    in_flight.pop(fut,None)
                    try: res=fut.result()
                    except Exception as exc:
                        # Defesa: erros inesperados do worker ainda ficam recuperáveis.
                        c={"numero_controle_pncp":"?"}
                        print(f"ERRO INTERNO DO WORKER: {exc}")
                        erros+=1; processadas+=1
                        continue
                    gravar_resultado(res)
                if outage:
                    print("\n>>> PNCP aparentemente indisponível: várias falhas transitórias consecutivas. <<<")
                    print("A coleta será pausada pelo orquestrador histórico em vez de avançar dias incompletos.")
                    # Deixa apenas os requests já em voo terminarem; não agenda novos.
                    for fut in list(in_flight):
                        try: gravar_resultado(fut.result())
                        except Exception: pass
                        in_flight.pop(fut,None)
                    break
                preencher()
    except KeyboardInterrupt:
        _finalizar_execucao(con,execucao_id,"INTERROMPIDA",processadas,sucessos,erros,indisponiveis,itens_gravados)
        con.close(); return 130

    if outage:
        _finalizar_execucao(con,execucao_id,"AGUARDANDO_PNCP",processadas,sucessos,erros,indisponiveis,itens_gravados)
        con.close(); return EXIT_PNCP_INDISPONIVEL

    status="CONCLUIDA_COM_ERROS" if erros else ("CONCLUIDA_COM_PENDENCIAS_EXTERNAS" if indisponiveis else "CONCLUIDA")
    _finalizar_execucao(con,execucao_id,status,processadas,sucessos,erros,indisponiveis,itens_gravados)
    print("\n"+"-"*86)
    print(f"Processadas: {processadas} | Sucessos: {sucessos} | Erros: {erros} | Indisponíveis: {indisponiveis}")
    print(f"Itens gravados: {itens_gravados} | Status: {status}")
    print("-"*86)
    con.close()
    return 0 if erros==0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
