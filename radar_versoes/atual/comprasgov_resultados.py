#!/usr/bin/env python3
"""Resultados de itens do Compras.gov para a RADAR_PNCP/Radar PNCP V0.7.6.

Os resultados são uma camada própria porque representam fornecedor/quantidade/valor
homologado e podem chegar antes ou depois do item/contratação correspondente.
Por isso a tabela não usa FK rígida; o vínculo lógico é pelo numeroControlePNCP + item.
"""
from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from importar_comprasgov_csv import (
    agora, abrir_csv, canon, contar_linhas, data_iso, inteiro, numero, texto,
    meta_importacao, salvar_meta, linha_csv_desalinhada, row_json_seguro,
)
from proveniencia import FONTE_COMPRASGOV_API, FONTE_COMPRASGOV_CSV, pncp_id_valido

RESULTADO_MAP: dict[str, tuple[list[str], Any]] = {
    "reserva_remanescente_id": (["reservaRemanescenteId"], inteiro),
    "id_compra_item": (["idCompraItem", "id_compra_item"], texto),
    "id_compra": (["idCompra", "id_compra"], texto),
    "numero_controle_pncp": (["idContratacaoPNCP", "numeroControlePNCP"], texto),
    "unidade_codigo": (["unidadeOrgaoCodigoUnidade"], texto),
    "numero_item": (["numeroItemPncp", "numeroItem"], inteiro),
    "sequencial_resultado": (["sequencialResultado"], inteiro),
    "ni_fornecedor": (["niFornecedor", "codFornecedor"], texto),
    "tipo_pessoa": (["tipoPessoa"], texto),
    "nome_fornecedor": (["nomeRazaoSocialFornecedor", "nomeFornecedor"], texto),
    "codigo_pais": (["codigoPais"], texto),
    "indicador_subcontratacao": (["indicadorSubcontratacao"], inteiro),
    "ordem_classificacao_srp": (["ordemClassificacaoSrp"], inteiro),
    "quantidade_homologada": (["quantidadeHomologada", "quantidadeResultado"], numero),
    "valor_unitario_homologado": (["valorUnitarioHomologado", "valorUnitarioResultado"], numero),
    "valor_total_homologado": (["valorTotalHomologado", "valorTotalResultado"], numero),
    "percentual_desconto": (["percentualDesconto"], numero),
    "situacao_resultado_id": (["situacaoCompraItemResultadoId"], inteiro),
    "situacao_resultado_nome": (["situacaoCompraItemResultadoNome"], texto),
    "motivo_cancelamento": (["motivoCancelamento"], texto),
    "porte_fornecedor_id": (["porteFornecedorId"], inteiro),
    "porte_fornecedor_nome": (["porteFornecedorNome"], texto),
    "natureza_juridica_nome": (["naturezaJuridicaNome"], texto),
    "natureza_juridica_id": (["naturezaJuridicaId"], texto),
    "data_inclusao_pncp": (["dataInclusaoPncp"], data_iso),
    "data_atualizacao_pncp": (["dataAtualizacaoPncp"], data_iso),
    "data_cancelamento_pncp": (["dataCancelamentoPncp"], data_iso),
    "data_resultado_pncp": (["dataResultadoPncp", "dataResultado"], data_iso),
    "aplicacao_margem_preferencia": (["aplicacaoMargemPreferencia"], inteiro),
    "amparo_margem_id": (["amparoLegalMargemPreferenciaId"], inteiro),
    "amparo_margem_nome": (["amparoLegalMargemPreferenciaNome"], texto),
    "aplicacao_beneficio_mepp": (["aplicacaoBeneficioMeepp", "aplicacaoBeneficioMepp"], inteiro),
    "aplicacao_criterio_desempate": (["aplicacaoCriterioDesempate"], inteiro),
    "amparo_desempate_id": (["amparoLegalCriterioDesempateId"], inteiro),
    "amparo_desempate_nome": (["amparoLegalCriterioDesempateNome"], texto),
    "moeda_estrangeira_id": (["moedaEstrangeiraId"], inteiro),
    "data_cotacao_moeda": (["dataCotacaoMoedaEstrangeira"], data_iso),
    "valor_nominal_moeda": (["valorNominalMoedaEstrangeira"], numero),
    "pais_origem_produto_servico_id": (["paisOrigemProdutoServicoId"], texto),
    "timezone_cotacao_moeda": (["timezoneCotacaoMoedaEstrangeira"], texto),
}

COLS = list(RESULTADO_MAP) + ["fonte_primeira","fonte_ultima","periodo_origem","importado_em"]


def garantir_tabelas(con: sqlite3.Connection) -> None:
    con.executescript("""
    CREATE TABLE IF NOT EXISTS resultados_itens (
        numero_controle_pncp TEXT NOT NULL,
        numero_item INTEGER NOT NULL,
        sequencial_resultado INTEGER NOT NULL,
        id_compra_item TEXT,
        id_compra TEXT,
        unidade_codigo TEXT,
        ni_fornecedor TEXT,
        tipo_pessoa TEXT,
        nome_fornecedor TEXT,
        codigo_pais TEXT,
        indicador_subcontratacao INTEGER,
        ordem_classificacao_srp INTEGER,
        quantidade_homologada REAL,
        valor_unitario_homologado REAL,
        valor_total_homologado REAL,
        percentual_desconto REAL,
        situacao_resultado_id INTEGER,
        situacao_resultado_nome TEXT,
        motivo_cancelamento TEXT,
        porte_fornecedor_id INTEGER,
        porte_fornecedor_nome TEXT,
        natureza_juridica_nome TEXT,
        natureza_juridica_id TEXT,
        data_inclusao_pncp TEXT,
        data_atualizacao_pncp TEXT,
        data_cancelamento_pncp TEXT,
        data_resultado_pncp TEXT,
        aplicacao_margem_preferencia INTEGER,
        amparo_margem_id INTEGER,
        amparo_margem_nome TEXT,
        aplicacao_beneficio_mepp INTEGER,
        aplicacao_criterio_desempate INTEGER,
        amparo_desempate_id INTEGER,
        amparo_desempate_nome TEXT,
        moeda_estrangeira_id INTEGER,
        data_cotacao_moeda TEXT,
        valor_nominal_moeda REAL,
        pais_origem_produto_servico_id TEXT,
        timezone_cotacao_moeda TEXT,
        fonte_primeira TEXT,
        fonte_ultima TEXT,
        periodo_origem TEXT,
        importado_em TEXT NOT NULL,
        PRIMARY KEY(numero_controle_pncp, numero_item, sequencial_resultado)
    );
    CREATE INDEX IF NOT EXISTS idx_resultados_fornecedor ON resultados_itens(ni_fornecedor);
    CREATE INDEX IF NOT EXISTS idx_resultados_fornecedor_nome ON resultados_itens(nome_fornecedor);
    CREATE INDEX IF NOT EXISTS idx_resultados_data ON resultados_itens(data_resultado_pncp);
    CREATE INDEX IF NOT EXISTS idx_resultados_id_compra ON resultados_itens(id_compra);
    CREATE INDEX IF NOT EXISTS idx_resultados_item ON resultados_itens(numero_controle_pncp, numero_item);

    CREATE TABLE IF NOT EXISTS comprasgov_resultados_quarentena (
        chave_linha TEXT PRIMARY KEY,
        periodo TEXT,
        motivo TEXT NOT NULL,
        raw_json TEXT,
        criado_em TEXT NOT NULL
    );
    """)
    cols={r[1] for r in con.execute('PRAGMA table_info(resultados_itens)')}
    for nome,definicao in {'reserva_remanescente_id':'INTEGER',
                           'pncp_consultado_em':'TEXT','pncp_ausente':'INTEGER NOT NULL DEFAULT 0'}.items():
        if nome not in cols: con.execute(f'ALTER TABLE resultados_itens ADD COLUMN {nome} {definicao}')
    con.commit()


def _resolver(headers: Iterable[str]) -> dict[str, str | None]:
    ch = {canon(h): h for h in headers}
    return {campo: next((ch[canon(a)] for a in aliases if canon(a) in ch), None)
            for campo,(aliases,_conv) in RESULTADO_MAP.items()}


def _valor(row: dict[str, Any], campo: str, resolvido: dict[str,str|None]) -> Any:
    col = resolvido.get(campo)
    if not col:
        return None
    return RESULTADO_MAP[campo][1](row.get(col))


def parametros_resultado(row: dict[str, Any], resolvido: dict[str,str|None], *, fonte: str, periodo: str, stamp: str) -> tuple[Any,...] | None:
    vals = {c:_valor(row,c,resolvido) for c in RESULTADO_MAP}
    pid = vals.get("numero_controle_pncp")
    ni = vals.get("numero_item")
    seq = vals.get("sequencial_resultado")
    if not pncp_id_valido(pid) or ni is None or seq is None:
        return None
    try:
        if int(ni) <= 0 or int(seq) < 0:
            return None
    except Exception:
        return None
    vals.update({"fonte_primeira":fonte,"fonte_ultima":fonte,"periodo_origem":periodo,"importado_em":stamp})
    return tuple(vals.get(c) for c in COLS)


_UPSERT_GENERIC_COLS = [
    c for c in COLS
    if c not in (
        'numero_controle_pncp','numero_item','sequencial_resultado',
        'fonte_primeira','fonte_ultima','periodo_origem','importado_em'
    )
]

SQL_UPSERT = f"""INSERT INTO resultados_itens ({','.join(COLS)}) VALUES ({','.join('?' for _ in COLS)})
ON CONFLICT(numero_controle_pncp,numero_item,sequencial_resultado) DO UPDATE SET
{','.join(f'{c}=COALESCE(excluded.{c},resultados_itens.{c})' for c in _UPSERT_GENERIC_COLS)},
fonte_ultima=excluded.fonte_ultima,
periodo_origem=excluded.periodo_origem,
importado_em=excluded.importado_em,
pncp_ausente=0"""

# Reconciliação por snapshot: o CSV anual é uma rede de segurança para preencher
# resultados que a API incremental possa ter deixado para trás. Ele não deve
# rebaixar/reescrever uma linha que já chegou pela API, portanto conflitos são
# ignorados nesse modo aditivo.
SQL_INSERT_IGNORE = f"INSERT OR IGNORE INTO resultados_itens ({','.join(COLS)}) VALUES ({','.join('?' for _ in COLS)})"


def _raw_json_seguro(row: dict[Any, Any]) -> str:
    """Serializa DictReader/API sem permitir chaves None quebrarem sort_keys.

    CSVs oficiais podem conter linhas com campos excedentes. O csv.DictReader
    representa esses excedentes na chave None; row_json_seguro os preserva em
    __campos_excedentes__ antes da serialização.
    """
    return json.dumps(
        row_json_seguro(row), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


def _quarentenar_resultado(con: sqlite3.Connection, row: dict[Any, Any], *, periodo: str, motivo: str, stamp: str) -> None:
    raw = _raw_json_seguro(row)
    chave = hashlib.sha256((periodo + "\n" + motivo + "\n" + raw).encode("utf-8", errors="replace")).hexdigest()
    con.execute(
        "INSERT OR IGNORE INTO comprasgov_resultados_quarentena(chave_linha,periodo,motivo,raw_json,criado_em) VALUES(?,?,?,?,?)",
        (chave, periodo, motivo, raw, stamp),
    )


def _valores_existentes(con: sqlite3.Connection, params: list[tuple[Any, ...]]) -> dict[tuple[str, int, int], dict[str, Any]]:
    """Carrega em lote apenas as colunas úteis para distinguir novo/alterado/idêntico."""
    if not params:
        return {}
    idx = {c: i for i, c in enumerate(COLS)}
    chaves = [
        (str(p[idx["numero_controle_pncp"]]), int(p[idx["numero_item"]]), int(p[idx["sequencial_resultado"]]))
        for p in params
    ]
    out: dict[tuple[str, int, int], dict[str, Any]] = {}
    cols_sql = ["numero_controle_pncp", "numero_item", "sequencial_resultado", *_UPSERT_GENERIC_COLS]
    # 3 parâmetros por chave; manter boa margem abaixo do limite SQLite mais conservador.
    for i in range(0, len(chaves), 250):
        bloco = chaves[i:i + 250]
        ph = ",".join("(?,?,?)" for _ in bloco)
        flat = [v for chave in bloco for v in chave]
        cur = con.execute(
            f"SELECT {','.join(cols_sql)} FROM resultados_itens "
            f"WHERE (numero_controle_pncp,numero_item,sequencial_resultado) IN ({ph})",
            flat,
        )
        nomes = [d[0] for d in cur.description]
        for row in cur.fetchall():
            d = dict(zip(nomes, row))
            chave = (str(d["numero_controle_pncp"]), int(d["numero_item"]), int(d["sequencial_resultado"]))
            out[chave] = d
    return out


def upsert_json_rows_detalhado(
    con: sqlite3.Connection,
    rows: list[dict[str, Any]],
    *,
    fonte: str = FONTE_COMPRASGOV_API,
    periodo: str,
) -> dict[str, int]:
    """UPSERT com métricas semânticas: recebidos, novos, alterados e idênticos.

    Metadados de proveniência/última importação não transformam por si só um
    resultado idêntico em "alterado". A comparação segue as colunas de negócio
    que o UPSERT pode efetivamente preencher/atualizar.
    """
    garantir_tabelas(con)
    if not rows:
        return {"recebidos": 0, "novos": 0, "alterados": 0, "identicos": 0, "quarentena": 0}
    resolvido = _resolver(rows[0].keys())
    stamp = agora()
    batch: list[tuple[Any, ...]] = []
    rejeitadas = 0
    for row in rows:
        p = parametros_resultado(row, resolvido, fonte=fonte, periodo=periodo, stamp=stamp)
        if p is None:
            rejeitadas += 1
            _quarentenar_resultado(con, row, periodo=periodo, motivo="CHAVE_RESULTADO_INVALIDA", stamp=stamp)
        else:
            batch.append(p)

    existentes = _valores_existentes(con, batch)
    idx = {c: i for i, c in enumerate(COLS)}
    novos = alterados = identicos = 0
    for p in batch:
        chave = (
            str(p[idx["numero_controle_pncp"]]),
            int(p[idx["numero_item"]]),
            int(p[idx["sequencial_resultado"]]),
        )
        atual = existentes.get(chave)
        if atual is None:
            novos += 1
            atual = {c: p[idx[c]] for c in _UPSERT_GENERIC_COLS}
            existentes[chave] = atual
            continue
        mudou = False
        for c in _UPSERT_GENERIC_COLS:
            remoto = p[idx[c]]
            if remoto is None:
                continue
            if atual.get(c) != remoto:
                mudou = True
                atual[c] = remoto
        if mudou:
            alterados += 1
        else:
            identicos += 1

    if batch:
        con.executemany(SQL_UPSERT, batch)
    con.commit()
    return {
        "recebidos": len(batch),
        "novos": novos,
        "alterados": alterados,
        "identicos": identicos,
        "quarentena": rejeitadas,
    }


def upsert_json_rows(con: sqlite3.Connection, rows: list[dict[str,Any]], *, fonte: str = FONTE_COMPRASGOV_API, periodo: str) -> tuple[int,int]:
    """Compatibilidade com chamadores antigos; retorna válidos + quarentena."""
    r = upsert_json_rows_detalhado(con, rows, fonte=fonte, periodo=periodo)
    return int(r["recebidos"]), int(r["quarentena"])


def importar_csv_resultados(con: sqlite3.Connection, path: Path, url: str, periodo: str, job_id: int|None=None, jobs_db: Path|None=None, batch_size: int=5000, *, forcar: bool=False, somente_novos: bool=False) -> dict[str,int]:
    garantir_tabelas(con)
    total=contar_linhas(path)
    meta=meta_importacao(con,periodo,"ITEM_RESULTADO")
    tamanho=path.stat().st_size
    resume=0
    if (not forcar) and meta and meta["tamanho_bytes"]==tamanho and meta["status"]=="CONCLUIDA":
        print("    ITEM_RESULTADO: já importado anteriormente; pulando.",flush=True)
        return {"linhas":int(meta["linhas_processadas"] or total),"gravados":int(meta["registros_novos"] or 0),"rejeitados":0,"pulou":1}
    if meta and meta["tamanho_bytes"]==tamanho and meta["status"] in ("EXECUTANDO","INTERROMPIDA","ERRO","PARCIAL"):
        resume=int(meta["linhas_processadas"] or 0)
        if resume:
            print(f"    ITEM_RESULTADO: retomando do checkpoint após {resume:,} linha(s).", flush=True)
        elif meta["status"] == "ERRO":
            print("    ITEM_RESULTADO: tentativa anterior falhou antes do primeiro checkpoint; reiniciando importação do arquivo em cache.", flush=True)
    salvar_meta(con,periodo,"ITEM_RESULTADO",status="EXECUTANDO",url=url,arquivo=str(path.resolve()),tamanho_bytes=tamanho,
                linhas_total=total,linhas_processadas=resume,atualizado_em=agora(),iniciado_em=(meta["iniciado_em"] if meta and meta["iniciado_em"] else agora()),erro=None)
    f,reader=abrir_csv(path)
    processadas=0; gravadas=0; rejeitadas=0; batch=[]
    sql_gravacao = SQL_INSERT_IGNORE if somente_novos else SQL_UPSERT
    try:
        headers=reader.fieldnames or []
        resolvido=_resolver(headers)
        obrig=["numero_controle_pncp","numero_item","sequencial_resultado"]
        faltam=[c for c in obrig if not resolvido.get(c)]
        if faltam:
            raise RuntimeError(f"Colunas obrigatórias de resultado ausentes: {faltam}. Cabeçalho: {headers}")
        stamp=agora()
        for row in reader:
            processadas += 1
            if processadas <= resume:
                continue
            if linha_csv_desalinhada(row, headers):
                rejeitadas += 1
                _quarentenar_resultado(con, row, periodo=periodo, motivo="LINHA_DESALINHADA", stamp=stamp)
                # Não é seguro mapear semanticamente uma linha com número de campos
                # diferente do cabeçalho; preservamos integralmente para auditoria.
                if processadas % 50000 < 2:
                    con.commit()
                continue
            p=parametros_resultado(row,resolvido,fonte=FONTE_COMPRASGOV_CSV,periodo=periodo,stamp=stamp)
            if p is None:
                rejeitadas += 1
                _quarentenar_resultado(con, row, periodo=periodo, motivo="CHAVE_RESULTADO_INVALIDA", stamp=stamp)
            else:
                batch.append(p)
            if len(batch)>=batch_size:
                antes_mudancas = con.total_changes
                con.executemany(sql_gravacao,batch)
                gravadas += (con.total_changes - antes_mudancas) if somente_novos else len(batch)
                batch.clear(); con.commit()
                salvar_meta(con,periodo,"ITEM_RESULTADO",linhas_processadas=processadas,registros_novos=gravadas,atualizado_em=agora())
                if processadas % 50000 < batch_size:
                    print(f"      Importação ITEM_RESULTADO: {processadas:,}/{total:,} linhas",flush=True)
        if batch:
            antes_mudancas = con.total_changes
            con.executemany(sql_gravacao,batch)
            gravadas += (con.total_changes - antes_mudancas) if somente_novos else len(batch)
            batch.clear(); con.commit()
        salvar_meta(con,periodo,"ITEM_RESULTADO",status="CONCLUIDA",linhas_processadas=processadas,registros_novos=gravadas,
                    registros_existentes=0,registros_orfaos=rejeitadas,atualizado_em=agora(),finalizado_em=agora(),erro=None)
        acao = 'nova(s) inserida(s)' if somente_novos else 'aplicada(s)'
        print(f"    ITEM_RESULTADO: {processadas:,} linha(s); {gravadas:,} {acao}; {rejeitadas:,} em quarentena.",flush=True)
        return {"linhas":processadas,"gravados":gravadas,"rejeitados":rejeitadas,"pulou":0}
    except Exception as exc:
        con.rollback(); salvar_meta(con,periodo,"ITEM_RESULTADO",status="ERRO",atualizado_em=agora(),erro=f"{type(exc).__name__}: {exc}"); raise
    finally:
        f.close()
