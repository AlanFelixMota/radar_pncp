#!/usr/bin/env python3
"""Importação incremental da RADAR_PNCP/Radar PNCP via CSV mensal do Compras.gov.

Regras da V0.7.6:
- Os CSVs mensais são DELTAS de registros novos e/ou alterados, não snapshots completos.
- Dados já confirmados pela API do PNCP nunca são sobrescritos pelo CSV.
- O CSV pode preencher campos nulos/ausentes em registros PNCP já existentes.
- Registros ainda não confirmados pelo PNCP podem ser atualizados por um snapshot
  CSV mais recente (data_atualizacao maior ou igual).
- Itens vindos de CSV mensal são úteis para pesquisa/classificação, mas NUNCA
  marcam a contratação como itens_coletados=1. A completude continua dependendo
  da API do PNCP.
- A carga é mensal, retomável por checkpoint e mantém staging para itens sem pai.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sqlite3
import sys
import time
import unicodedata
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import job_manager as jm
from coletar_itens import preparar_banco as preparar_banco_itens
from coletar_pncp import criar_banco as criar_banco_base
from proveniencia import FONTE_COMPRASGOV_CSV, FONTE_COMPRASGOV_API, FONTE_PNCP, garantir_proveniencia, pncp_id_valido
from radar_paths import DATA_DIR, DB_COMPRAS, DB_JOBS, DB_FTS, DB_TIC, garantir_diretorios

BASE_REPOSITORIO = "https://repositorio.dados.gov.br/seges/comprasgov/mensal"
CSV_DIR = DATA_DIR / "csv_comprasgov"
BATCH = 2000


def agora() -> str:
    return datetime.now().isoformat(timespec="seconds")


def canon(valor: str | None) -> str:
    s = unicodedata.normalize("NFKD", str(valor or ""))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", "", s.casefold())


def texto(valor: Any) -> str | None:
    if valor is None:
        return None
    s = str(valor).strip()
    return s if s else None


SQLITE_INT_MIN = -(2**63)
SQLITE_INT_MAX = 2**63 - 1


def inteiro(valor: Any) -> int | None:
    """Converte inteiro sem ultrapassar o intervalo aceito pelo SQLite.

    Algumas linhas anômalas dos CSVs podem deslocar textos/números gigantes para
    campos que normalmente são INTEGER. Antes da V0.6.2.4 isso podia produzir um
    ``OverflowError`` no binding do sqlite3. Valores fora do intervalo signed
    64-bit são agora considerados inválidos e retornam ``None``.
    """
    if valor is None or str(valor).strip() == "":
        return None
    s = str(valor).strip().replace("\xa0", "").replace(" ", "")
    if "," in s and "." not in s:
        s = s.replace(",", ".")
    try:
        d = Decimal(s)
        if not d.is_finite() or d != d.to_integral_value():
            return None
        n = int(d)
        if n < SQLITE_INT_MIN or n > SQLITE_INT_MAX:
            return None
        return n
    except (InvalidOperation, TypeError, ValueError, OverflowError):
        return None


def numero(valor: Any) -> float | None:
    if valor is None or str(valor).strip() == "":
        return None
    s = str(valor).strip().replace("\xa0", "").replace(" ", "")
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def booleano(valor: Any) -> int | None:
    if valor is None or str(valor).strip() == "":
        return None
    s = str(valor).strip().casefold()
    if s in {"1", "true", "t", "sim", "s", "yes", "y"}:
        return 1
    if s in {"0", "false", "f", "nao", "não", "n", "no"}:
        return 0
    return inteiro(valor)


def codigo_texto(valor: Any) -> str | None:
    s = texto(valor)
    if s is None:
        return None
    if re.fullmatch(r"[+-]?\d+\.0+", s):
        return s.split(".", 1)[0]
    return s


def data_iso(valor: Any) -> str | None:
    s = texto(valor)
    if not s:
        return None
    s = s.replace("Z", "")
    # formatos usuais dos arquivos mensais
    formatos = (
        "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y",
    )
    for fmt in formatos:
        try:
            dt = datetime.strptime(s, fmt)
            if "%H" in fmt:
                return dt.isoformat(timespec="seconds")
            return dt.date().isoformat()
        except ValueError:
            pass
    return s


# campo SQLite -> (aliases do CSV, conversor)
COMPRA_MAP: dict[str, tuple[list[str], Callable[[Any], Any]]] = {
    "numero_controle_pncp": (["numeroControlePNCP"], texto),
    "comprasgov_id_compra": (["idCompra", "id_compra"], texto),
    "comprasgov_contratacao_excluida": (["contratacaoExcluida"], booleano),
    "comprasgov_existe_resultado": (["existeResultado"], booleano),
    "numero_compra": (["numeroCompra"], texto),
    "ano_compra": (["anoCompraPncp", "anoCompra"], inteiro),
    "sequencial_compra": (["sequencialCompraPncp", "sequencialCompra"], inteiro),
    "processo": (["processo", "numeroProcesso"], texto),
    "tipo_instrumento_id": (["tipoInstrumentoConvocatorioCodigoPncp", "tipoInstrumentoConvocatorioCodigo", "tipoInstrumentoConvocatorioId"], inteiro),
    "tipo_instrumento_nome": (["tipoInstrumentoConvocatorioNome"], texto),
    "modalidade_id": (["modalidadeIdPncp", "modalidadeId"], inteiro),
    "modalidade_nome": (["modalidadeNome", "modalidadeNomePncp"], texto),
    "modo_disputa_id": (["modoDisputaIdPncp", "modoDisputaId"], inteiro),
    "modo_disputa_nome": (["modoDisputaNomePncp", "modoDisputaNome"], texto),
    "situacao_id": (["situacaoCompraIdPncp", "situacaoCompraId", "situacaoId"], inteiro),
    "situacao_nome": (["situacaoCompraNomePncp", "situacaoCompraNome", "situacaoNome"], texto),
    "objeto": (["objetoCompra", "objeto"], texto),
    "informacao_complementar": (["informacaoComplementar"], texto),
    "srp": (["srp", "sistemaRegistroPrecos"], booleano),
    "amparo_legal_codigo": (["amparoLegalCodigoPncp", "amparoLegalCodigo"], inteiro),
    "amparo_legal_nome": (["amparoLegalNome"], texto),
    "valor_total_estimado": (["valorTotalEstimado"], numero),
    "valor_total_homologado": (["valorTotalHomologado"], numero),
    "data_abertura_proposta": (["dataAberturaPropostaPncp", "dataAberturaProposta"], data_iso),
    "data_encerramento_proposta": (["dataEncerramentoPropostaPncp", "dataEncerramentoProposta"], data_iso),
    "data_publicacao_pncp": (["dataPublicacaoPncp"], data_iso),
    "data_inclusao": (["dataInclusaoPncp", "dataInclusao"], data_iso),
    "data_atualizacao": (["dataAtualizacaoPncp", "dataAualizacaoPncp", "dataAtualizacao"], data_iso),
    "orgao_cnpj": (["orgaoEntidadeCnpj"], codigo_texto),
    "orgao_razao_social": (["orgaoEntidadeRazaoSocial"], texto),
    "poder_id": (["orgaoEntidadePoderId", "poderId"], texto),
    "esfera_id": (["orgaoEntidadeEsferaId", "esferaId"], texto),
    "unidade_codigo": (["unidadeOrgaoCodigoUnidade", "codigoUnidade"], codigo_texto),
    "unidade_nome": (["unidadeOrgaoNomeUnidade", "nomeUnidade"], texto),
    "municipio_ibge": (["unidadeOrgaoCodigoIbge", "codigoIbge"], codigo_texto),
    "municipio_nome": (["unidadeOrgaoMunicipioNome", "municipioNome"], texto),
    "uf_sigla": (["unidadeOrgaoUfSigla", "ufSigla", "uf"], texto),
    "uf_nome": (["unidadeOrgaoUfNome", "ufNome"], texto),
    "usuario_nome": (["usuarioNome"], texto),
    "link_sistema_origem": (["linkSistemaOrigem"], texto),
}

ITEM_MAP: dict[str, tuple[list[str], Callable[[Any], Any]]] = {
    "numero_controle_pncp": (["idContratacaoPNCP", "numeroControlePNCP"], texto),
    "comprasgov_id_compra": (["idCompra", "id_compra"], texto),
    "comprasgov_id_compra_item": (["idCompraItem", "id_compra_item"], texto),
    "numero_item": (["numeroItemPncp", "numeroItem"], inteiro),
    "material_ou_servico": (["materialOuServico"], texto),
    "material_ou_servico_nome": (["materialOuServicoNome"], texto),
    "descricao": (["descricao", "descricaoResumida", "descricaoDetalhada", "descricaodetalhada"], texto),
    "informacao_complementar": (["informacaoComplementar"], texto),
    "quantidade": (["quantidade"], numero),
    "unidade_medida": (["unidadeMedida"], texto),
    "valor_unitario_estimado": (["valorUnitarioEstimado"], numero),
    "valor_total": (["valorTotal"], numero),
    "situacao_item_id": (["situacaoCompraItemIdPncp", "situacaoCompraItemId", "situacaoCompraItem"], inteiro),
    "situacao_item_nome": (["situacaoCompraItemNome"], texto),
    "criterio_julgamento_id": (["criterioJulgamentoIdPncp", "criterioJulgamentoId"], inteiro),
    "criterio_julgamento_nome": (["criterioJulgamentoNome"], texto),
    "tem_resultado": (["temResultado"], booleano),
    "ncm_nbs_codigo": (["codigoNCM", "ncmNbsCodigo"], codigo_texto),
    "ncm_nbs_descricao": (["descricaoNCM", "ncmNbsDescricao"], texto),
    "catalogo_codigo_item": (["codItemCatalogo", "catalogoCodigoItem"], codigo_texto),
    "catalogo_id": (["catalogoId"], inteiro),
    "catalogo_nome": (["catalogoNome"], texto),
    "categoria_catalogo_id": (["itemCategoriaIdPncp", "categoriaItemCatalogoId"], inteiro),
    "categoria_catalogo_nome": (["itemCategoriaNome", "categoriaItemCatalogoNome"], texto),
    "data_inclusao": (["dataInclusaoPncp", "dataInclusao"], data_iso),
    "data_atualizacao": (["dataAtualizacaoPncp", "dataAtualizacao"], data_iso),
}

TEXT_COMPRA = {k for k, (_, conv) in COMPRA_MAP.items() if conv in (texto, codigo_texto, data_iso)}
TEXT_ITEM = {k for k, (_, conv) in ITEM_MAP.items() if conv in (texto, codigo_texto, data_iso)}


def meses(inicio: str, fim: str) -> list[str]:
    return jm._meses_periodo(inicio, fim)


def sessao_http() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5, backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}), respect_retry_after_header=True,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({
        "Accept": "text/csv,*/*",
        "User-Agent": "Mozilla/5.0 RADAR_PNCP-Radar PNCP/0.6.2 CSV-delta-loader",
    })
    return s


def recurso_info(ano_mes: str, recurso: str) -> tuple[str, Path]:
    ano, mes = ano_mes.split("-")
    if recurso == "COMPRA":
        base = "VW_FT_PNCP_COMPRA"
    elif recurso == "COMPRA_ITEM":
        base = "VW_FT_PNCP_COMPRA_ITEM"
    else:
        raise ValueError(recurso)
    nome = f"comprasGOV-mensal-{base}-{ano_mes}.csv"
    url = f"{BASE_REPOSITORIO}/{ano}/{mes}/{nome}"
    path = CSV_DIR / ano / mes / nome
    return url, path


def atualizar_mes(job_id: int | None, jobs_db: Path | None, ano_mes: str, **campos: Any) -> None:
    if job_id is not None and jobs_db is not None:
        jm.atualizar_job_csv_mes(job_id, ano_mes, jobs_db, **campos)


def atualizar_job(job_id: int | None, jobs_db: Path | None, **campos: Any) -> None:
    if job_id is not None and jobs_db is not None:
        jm.atualizar_job(job_id, jobs_db, **campos)


def baixar(
    sessao: requests.Session, url: str, destino: Path, ano_mes: str,
    job_id: int | None, jobs_db: Path | None, recurso: str,
) -> Path:
    """Baixa com cache e tenta retomar um .part via HTTP Range."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    if destino.exists() and destino.stat().st_size > 1000:
        print(f"    Arquivo em cache: {destino.name} ({destino.stat().st_size/1024/1024:.1f} MB)", flush=True)
        return destino

    parcial = destino.with_suffix(destino.suffix + ".part")
    existente = parcial.stat().st_size if parcial.exists() else 0
    headers = {"Range": f"bytes={existente}-"} if existente > 0 else {}
    if existente:
        print(f"    Retomando download de {destino.name} a partir de {existente/1024/1024:.1f} MB", flush=True)
    else:
        print(f"    Baixando {destino.name}", flush=True)

    with sessao.get(url, stream=True, timeout=(30, 180), headers=headers) as resp:
        if resp.status_code == 404:
            raise FileNotFoundError(f"CSV não disponível no repositório: {url}")
        if resp.status_code == 416 and existente > 1000:
            # Range além do fim: trata o parcial como completo; uma futura validação
            # do CSV falhará de forma explícita se o arquivo estiver truncado.
            parcial.replace(destino)
            return destino
        resp.raise_for_status()

        retomou = existente > 0 and resp.status_code == 206
        if retomou:
            atual = existente
            total = existente + int(resp.headers.get("Content-Length") or 0)
            modo = "ab"
        else:
            # Servidor ignorou Range: reinicia o arquivo para não concatenar conteúdo.
            atual = 0
            total = int(resp.headers.get("Content-Length") or 0)
            modo = "wb"

        atualizar_mes(
            job_id, jobs_db, ano_mes, etapa=f"Baixando {recurso}",
            progresso_atual=atual, progresso_total=total, unidade_progresso="bytes",
            mensagem=f"Baixando {recurso}" + (" · retomado" if retomou else ""),
        )
        with parcial.open(modo) as f:
            ultimo_ui = 0.0
            ultimo_log_bytes = atual
            for bloco in resp.iter_content(chunk_size=1024 * 1024):
                if not bloco:
                    continue
                f.write(bloco); atual += len(bloco)
                agora_t = time.monotonic()
                if agora_t - ultimo_ui >= 1.0:
                    atualizar_mes(job_id, jobs_db, ano_mes, progresso_atual=atual,
                                  progresso_total=total, unidade_progresso="bytes")
                    ultimo_ui = agora_t
                if atual - ultimo_log_bytes >= 50 * 1024 * 1024 or (total and atual >= total):
                    if total:
                        print(f"      Download: {atual/1024/1024:.1f}/{total/1024/1024:.1f} MB", flush=True)
                    else:
                        print(f"      Download: {atual/1024/1024:.1f} MB", flush=True)
                    ultimo_log_bytes = atual
        parcial.replace(destino)

    atualizar_mes(job_id, jobs_db, ano_mes, progresso_atual=destino.stat().st_size,
                  progresso_total=destino.stat().st_size, unidade_progresso="bytes")
    print(f"    Salvo: {destino} ({destino.stat().st_size/1024/1024:.1f} MB)", flush=True)
    return destino


def abrir_csv(path: Path):
    raw = path.read_bytes()[:65536]
    enc = "utf-8-sig"
    try:
        raw.decode(enc)
    except UnicodeDecodeError:
        enc = "latin-1"
    f = path.open("r", encoding=enc, errors="replace", newline="")
    amostra = f.read(65536); f.seek(0)
    try:
        dialeto = csv.Sniffer().sniff(amostra, delimiters=";,|\t")
    except csv.Error:
        dialeto = csv.excel; dialeto.delimiter = ";"
    return f, csv.DictReader(f, dialect=dialeto)


def contar_linhas(path: Path) -> int:
    n = 0
    with path.open("rb") as f:
        while True:
            b = f.read(8 * 1024 * 1024)
            if not b:
                break
            n += b.count(b"\n")
    return max(0, n - 1)


def resolver_colunas(headers: list[str], mapa: dict[str, tuple[list[str], Callable]]) -> dict[str, str | None]:
    canon_headers = {canon(h): h for h in headers}
    out: dict[str, str | None] = {}
    for campo, (aliases, _conv) in mapa.items():
        out[campo] = next((canon_headers[canon(a)] for a in aliases if canon(a) in canon_headers), None)
    return out


def linha_csv_desalinhada(row: dict[Any, Any], headers: list[str]) -> bool:
    """Detecta linhas em que o número de campos não corresponde ao cabeçalho.

    O csv.DictReader usa a chave None para campos excedentes e preenche com None
    colunas ausentes. Qualquer uma dessas situações torna inseguro mapear a linha
    por nome, pois os campos posteriores podem ficar deslocados.
    """
    if None in row:
        return True
    return any(h not in row or row.get(h) is None for h in headers)


def row_json_seguro(row: dict[Any, Any]) -> dict[str, Any]:
    """Converte a saída do DictReader em JSON sem chaves None.

    Campos excedentes produzidos pelo DictReader ficam em ``__campos_excedentes__``.
    """
    out: dict[str, Any] = {}
    extras: list[Any] = []
    for k, v in row.items():
        if k is None:
            if isinstance(v, list):
                extras.extend(v)
            elif v is not None:
                extras.append(v)
            continue
        out[str(k)] = v
    if extras:
        out["__campos_excedentes__"] = extras
    return out


def valores_escalares_row(row: dict[Any, Any]):
    """Itera valores escalares inclusive dentro dos campos excedentes do DictReader."""
    for v in row.values():
        if isinstance(v, list):
            yield from v
        else:
            yield v



UFS_VALIDAS_CSV = {
    "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG",
    "PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO",
}


def compra_linha_semantica_invalida(
    row: dict[Any, Any], resolvido: dict[str, str | None]
) -> bool:
    """Validação conservadora de campos estruturais de uma contratação.

    Não tenta validar conteúdo de objeto/processo. O objetivo é detectar
    deslocamentos de coluna que podem ter exatamente a mesma quantidade de
    campos do cabeçalho e, portanto, escapar da checagem de largura.
    """
    idcol = resolvido.get("numero_controle_pncp")
    pid = texto(row.get(idcol)) if idcol else None
    if not pncp_id_valido(pid):
        return True

    col_data = resolvido.get("data_publicacao_pncp")
    if col_data:
        raw = texto(row.get(col_data))
        if raw:
            conv = data_iso(raw)
            if not conv or not re.match(r"^\d{4}-\d{2}-\d{2}(?:T|$)", str(conv)):
                return True

    col_uf = resolvido.get("uf_sigla")
    if col_uf:
        uf = texto(row.get(col_uf))
        if uf and uf.upper() not in UFS_VALIDAS_CSV:
            return True

    col_cnpj = resolvido.get("orgao_cnpj")
    if col_cnpj:
        cnpj = codigo_texto(row.get(col_cnpj))
        if cnpj and (len(cnpj) != 14 or not cnpj.isdigit()):
            return True

    col_ano = resolvido.get("ano_compra")
    if col_ano:
        raw = texto(row.get(col_ano))
        if raw:
            ano = inteiro(raw)
            if ano is None or not (2021 <= ano <= 2099):
                return True

    col_seq = resolvido.get("sequencial_compra")
    if col_seq:
        raw = texto(row.get(col_seq))
        if raw:
            seq = inteiro(raw)
            if seq is None or seq < 0:
                return True

    return False


def valor_linha(row: dict[str, str], campo: str, mapa, resolvido):
    col = resolvido.get(campo)
    if not col:
        return None
    return mapa[campo][1](row.get(col))


def expr_merge(tabela: str, campo: str, text_field: bool) -> str:
    atual = f"{tabela}.{campo}"
    vazio = f"({atual} IS NULL" + (f" OR trim({atual})=''" if text_field else "") + ")"
    novo = f"excluded.{campo}"
    # PNCP confirmado: apenas complementa ausência. CSV-only: snapshot mais recente pode atualizar.
    return (
        "CASE "
        f"WHEN COALESCE({tabela}.pncp_confirmado,0)=0 "
        f" AND COALESCE(excluded.data_atualizacao,'') >= COALESCE({tabela}.data_atualizacao,'') "
        f"THEN COALESCE({novo}, {atual}) "
        f"WHEN {vazio} THEN COALESCE({novo}, {atual}) "
        f"ELSE {atual} END"
    )


def sql_upsert_compra(fonte_label: str = FONTE_COMPRASGOV_CSV) -> tuple[str, list[str]]:
    campos = list(COMPRA_MAP)
    extras = ["url_pncp", "raw_json", "primeira_coleta_em", "ultima_coleta_em",
              "fonte_primeira", "fonte_ultima", "pncp_confirmado",
              "csv_compra_mes", "csv_compra_importada_em"]
    cols = campos + extras
    set_parts = []
    for c in campos:
        if c == "numero_controle_pncp":
            continue
        set_parts.append(f"{c}={expr_merge('contratacoes', c, c in TEXT_COMPRA)}")
    set_parts += [
        f"fonte_ultima=CASE WHEN COALESCE(contratacoes.pncp_confirmado,0)=0 AND COALESCE(excluded.data_atualizacao,'') >= COALESCE(contratacoes.data_atualizacao,'') THEN '{fonte_label}' ELSE contratacoes.fonte_ultima END",
        "csv_compra_mes=CASE WHEN contratacoes.csv_compra_mes IS NULL OR COALESCE(excluded.data_atualizacao,'') >= COALESCE(contratacoes.data_atualizacao,'') THEN excluded.csv_compra_mes ELSE contratacoes.csv_compra_mes END",
        "csv_compra_importada_em=CASE WHEN contratacoes.csv_compra_mes IS NULL OR COALESCE(excluded.data_atualizacao,'') >= COALESCE(contratacoes.data_atualizacao,'') THEN excluded.csv_compra_importada_em ELSE contratacoes.csv_compra_importada_em END",
    ]
    sql = f"""INSERT INTO contratacoes ({','.join(cols)}) VALUES ({','.join('?' for _ in cols)})
              ON CONFLICT(numero_controle_pncp) DO UPDATE SET {','.join(set_parts)}"""
    return sql, cols


def sql_upsert_item(fonte_label: str = FONTE_COMPRASGOV_CSV) -> tuple[str, list[str]]:
    campos = list(ITEM_MAP)
    extras = ["raw_json", "fonte_primeira", "fonte_ultima", "pncp_confirmado",
              "csv_item_mes", "csv_item_importada_em"]
    cols = campos + extras
    set_parts = []
    for c in campos:
        if c in ("numero_controle_pncp", "numero_item"):
            continue
        set_parts.append(f"{c}={expr_merge('itens', c, c in TEXT_ITEM)}")
    set_parts += [
        f"fonte_ultima=CASE WHEN COALESCE(itens.pncp_confirmado,0)=0 AND COALESCE(excluded.data_atualizacao,'') >= COALESCE(itens.data_atualizacao,'') THEN '{fonte_label}' ELSE itens.fonte_ultima END",
        "csv_item_mes=CASE WHEN itens.csv_item_mes IS NULL OR COALESCE(excluded.data_atualizacao,'') >= COALESCE(itens.data_atualizacao,'') THEN excluded.csv_item_mes ELSE itens.csv_item_mes END",
        "csv_item_importada_em=CASE WHEN itens.csv_item_mes IS NULL OR COALESCE(excluded.data_atualizacao,'') >= COALESCE(itens.data_atualizacao,'') THEN excluded.csv_item_importada_em ELSE itens.csv_item_importada_em END",
    ]
    sql = f"""INSERT INTO itens ({','.join(cols)}) VALUES ({','.join('?' for _ in cols)})
              ON CONFLICT(numero_controle_pncp,numero_item) DO UPDATE SET {','.join(set_parts)}"""
    return sql, cols


SQL_COMPRA, COLS_COMPRA = sql_upsert_compra(FONTE_COMPRASGOV_CSV)
SQL_ITEM, COLS_ITEM = sql_upsert_item(FONTE_COMPRASGOV_CSV)
SQL_COMPRA_API, _ = sql_upsert_compra(FONTE_COMPRASGOV_API)
SQL_ITEM_API, _ = sql_upsert_item(FONTE_COMPRASGOV_API)


def _chunks(seq: list[str], tamanho: int = 500):
    for i in range(0, len(seq), tamanho):
        yield seq[i:i + tamanho]


def separar_itens_com_pai(
    con: sqlite3.Connection, batch: list[tuple[Any, ...]]
) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]]]:
    """Separa itens que podem respeitar a FK daqueles cujo pai ainda não chegou.

    Os arquivos mensais COMPRA e COMPRA_ITEM não são garantidos como uma relação
    pai-filho fechada dentro do mesmo mês. Um item pode aparecer no arquivo mensal
    antes de a contratação correspondente estar disponível no banco/carga corrente.
    Em vez de desligar a FK (ou perder o item), mantemos o registro em staging.
    """
    if not batch:
        return [], []
    idx_pid = COLS_ITEM.index("numero_controle_pncp")
    pids = sorted({str(params[idx_pid]) for params in batch if params[idx_pid]})
    existentes: set[str] = set()
    for pedaco in _chunks(pids):
        ph = ",".join("?" for _ in pedaco)
        existentes.update(
            str(r[0]) for r in con.execute(
                f"SELECT numero_controle_pncp FROM contratacoes "
                f"WHERE numero_controle_pncp IN ({ph})",
                pedaco,
            ).fetchall()
        )
    validos: list[tuple[Any, ...]] = []
    orfaos: list[tuple[Any, ...]] = []
    for params in batch:
        (validos if str(params[idx_pid]) in existentes else orfaos).append(params)
    return validos, orfaos


def guardar_itens_orfaos(
    con: sqlite3.Connection, ano_mes: str, batch: list[tuple[Any, ...]]
) -> int:
    """Guarda itens sem contratação-pai para resolução em uma carga posterior."""
    if not batch:
        return 0
    stamp = agora()
    idx_pid = COLS_ITEM.index("numero_controle_pncp")
    idx_item = COLS_ITEM.index("numero_item")
    rows = []
    for params in batch:
        payload = dict(zip(COLS_ITEM, params))
        rows.append((
            str(params[idx_pid]), int(params[idx_item]), ano_mes,
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            stamp, stamp,
        ))
    con.executemany(
        """INSERT INTO comprasgov_itens_orfaos(
               numero_controle_pncp,numero_item,ano_mes,payload_json,
               primeira_tentativa_em,ultima_tentativa_em
           ) VALUES (?,?,?,?,?,?)
           ON CONFLICT(numero_controle_pncp,numero_item) DO UPDATE SET
               ano_mes=CASE WHEN excluded.ano_mes >= comprasgov_itens_orfaos.ano_mes
                            THEN excluded.ano_mes ELSE comprasgov_itens_orfaos.ano_mes END,
               payload_json=CASE WHEN excluded.ano_mes >= comprasgov_itens_orfaos.ano_mes
                                 THEN excluded.payload_json ELSE comprasgov_itens_orfaos.payload_json END,
               ultima_tentativa_em=excluded.ultima_tentativa_em""",
        rows,
    )
    return len(rows)


def resolver_itens_orfaos(
    con: sqlite3.Connection,
    limite: int = 10000,
    progress_callback: Callable[[int, int], None] | None = None,
) -> tuple[int, dict[str, int]]:
    """Move do staging para `itens` os órfãos cujo pai já existe.

    Retorna (quantidade_resolvida, {ano_mes: quantidade}). A resolução depende
    apenas da existência da contratação-pai no banco; a V0.6.2.4 também pode
    carregar os arquivos anuais 2021–2024 para recuperar pais históricos.
    """
    total_resolvivel = int(con.execute(
        """SELECT COUNT(*)
             FROM comprasgov_itens_orfaos o
             JOIN contratacoes c
               ON c.numero_controle_pncp=o.numero_controle_pncp"""
    ).fetchone()[0])
    total = 0
    por_mes: dict[str, int] = {}
    if progress_callback:
        progress_callback(0, total_resolvivel)
    while True:
        rows = con.execute(
            """SELECT o.numero_controle_pncp,o.numero_item,o.ano_mes,o.payload_json
                 FROM comprasgov_itens_orfaos o
                 JOIN contratacoes c
                   ON c.numero_controle_pncp=o.numero_controle_pncp
                ORDER BY o.ano_mes,o.numero_controle_pncp,o.numero_item
                LIMIT ?""",
            (limite,),
        ).fetchall()
        if not rows:
            break
        params_lote: list[tuple[Any, ...]] = []
        chaves: list[tuple[str, int]] = []
        for r in rows:
            payload = json.loads(r[3])
            params_lote.append(tuple(payload.get(c) for c in COLS_ITEM))
            chaves.append((str(r[0]), int(r[1])))
            mes = str(r[2])
            por_mes[mes] = por_mes.get(mes, 0) + 1
        con.executemany(SQL_ITEM, params_lote)
        con.executemany(
            "DELETE FROM comprasgov_itens_orfaos "
            "WHERE numero_controle_pncp=? AND numero_item=?",
            chaves,
        )
        # Os itens que acabam de sair do staging continuam sendo evidência CSV.
        # Se o conjunto ainda não foi confirmado integralmente pelo PNCP, registre
        # isso no pai para que a validação de completude consiga selecioná-lo.
        pais_lote = sorted({pid for pid, _ in chaves})
        con.executemany(
            """UPDATE contratacoes SET itens_fonte=?
                 WHERE numero_controle_pncp=? AND COALESCE(itens_coletados,0)=0""",
            [(FONTE_COMPRASGOV_CSV, pid) for pid in pais_lote],
        )
        total += len(rows)
        con.commit()
        if progress_callback:
            progress_callback(total, total_resolvivel)
        if len(rows) < limite:
            break
    return total, por_mes


def registrar_orfaos_resolvidos(
    con: sqlite3.Connection, por_mes: dict[str, int]
) -> None:
    """Atualiza as métricas persistentes dos meses que saíram do staging."""
    if not por_mes:
        return
    for ano_mes, qtd in por_mes.items():
        pendentes = contar_orfaos(con, ano_mes)
        con.execute(
            """UPDATE comprasgov_importacoes
                  SET registros_orfaos=?,
                      orfaos_resolvidos=COALESCE(orfaos_resolvidos,0)+?,
                      atualizado_em=?
                WHERE ano_mes=? AND recurso='COMPRA_ITEM'""",
            (pendentes, int(qtd), agora(), ano_mes),
        )
    con.commit()


def contar_orfaos(con: sqlite3.Connection, ano_mes: str | None = None) -> int:
    if ano_mes is None:
        return int(con.execute("SELECT COUNT(*) FROM comprasgov_itens_orfaos").fetchone()[0])
    return int(con.execute(
        "SELECT COUNT(*) FROM comprasgov_itens_orfaos WHERE ano_mes=?", (ano_mes,)
    ).fetchone()[0])


def meta_importacao(con: sqlite3.Connection, ano_mes: str, recurso: str) -> sqlite3.Row | None:
    return con.execute(
        "SELECT * FROM comprasgov_importacoes WHERE ano_mes=? AND recurso=?", (ano_mes, recurso)
    ).fetchone()


def salvar_meta(con: sqlite3.Connection, ano_mes: str, recurso: str, **campos: Any) -> None:
    con.execute(
        "INSERT OR IGNORE INTO comprasgov_importacoes(ano_mes,recurso,status) VALUES (?,?,'PENDENTE')",
        (ano_mes, recurso),
    )
    if campos:
        partes = ",".join(f"{k}=?" for k in campos)
        con.execute(
            f"UPDATE comprasgov_importacoes SET {partes} WHERE ano_mes=? AND recurso=?",
            list(campos.values()) + [ano_mes, recurso],
        )
    con.commit()


def parametros_compra(row, resolvido, ano_mes: str, stamp: str, fonte: str = FONTE_COMPRASGOV_CSV) -> tuple[Any, ...] | None:
    vals = {c: valor_linha(row, c, COMPRA_MAP, resolvido) for c in COMPRA_MAP}
    pid = vals["numero_controle_pncp"]
    if not pncp_id_valido(pid):
        return None
    url_pncp = None
    if vals.get("orgao_cnpj") and vals.get("ano_compra") is not None and vals.get("sequencial_compra") is not None:
        url_pncp = f"https://pncp.gov.br/app/editais/{vals['orgao_cnpj']}/{vals['ano_compra']}/{vals['sequencial_compra']}"
    extras = {
        "url_pncp": url_pncp, "raw_json": None,
        "primeira_coleta_em": stamp, "ultima_coleta_em": stamp,
        "fonte_primeira": fonte, "fonte_ultima": fonte,
        "pncp_confirmado": 0, "csv_compra_mes": (ano_mes if fonte == FONTE_COMPRASGOV_CSV else None), "csv_compra_importada_em": (stamp if fonte == FONTE_COMPRASGOV_CSV else None),
    }
    vals.update(extras)
    return tuple(vals[c] for c in COLS_COMPRA)


def numero_controle_item(row: dict[str, str], resolvido: dict[str, str | None]) -> tuple[str | None, str]:
    """Resolve o ID PNCP do item com validação estrutural.

    Alguns arquivos do Compras.gov contêm linhas anômalas em que o valor lido na
    coluna esperada não tem formato de número de controle PNCP. Nesses casos, a
    V0.6.2.4 procura um único valor estruturalmente válido na própria linha antes de
    mandar o registro para quarentena.
    """
    col = resolvido.get("numero_controle_pncp")
    informado = texto(row.get(col)) if col else None
    if pncp_id_valido(informado):
        return informado, "VALIDO"

    candidatos: list[str] = []
    for valor in valores_escalares_row(row):
        v = texto(valor)
        if v and pncp_id_valido(v) and v not in candidatos:
            candidatos.append(v)
    if len(candidatos) == 1:
        return candidatos[0], "RECUPERADO_NA_LINHA"
    if not informado:
        return None, "ID_AUSENTE"
    if len(candidatos) > 1:
        return informado, "ID_AMBIGUO"
    return informado, "ID_MALFORMADO"


def parametros_item(row, resolvido, ano_mes: str, stamp: str, fonte: str = FONTE_COMPRASGOV_CSV) -> tuple[tuple[Any, ...] | None, str]:
    vals = {c: valor_linha(row, c, ITEM_MAP, resolvido) for c in ITEM_MAP}
    pid, situacao_id = numero_controle_item(row, resolvido)
    vals["numero_controle_pncp"] = pid
    if not pid or vals["numero_item"] is None:
        return None, situacao_id
    extras = {
        "raw_json": None,
        "fonte_primeira": fonte, "fonte_ultima": fonte,
        "pncp_confirmado": 0, "csv_item_mes": (ano_mes if fonte == FONTE_COMPRASGOV_CSV else None), "csv_item_importada_em": (stamp if fonte == FONTE_COMPRASGOV_CSV else None),
    }
    vals.update(extras)
    return tuple(vals[c] for c in COLS_ITEM), situacao_id


def guardar_item_quarentena(
    con: sqlite3.Connection, ano_mes: str, row: dict[str, str],
    params: tuple[Any, ...] | None, motivo: str,
) -> str:
    stamp = agora()
    pid = None
    ni = None
    payload = None
    if params is not None:
        d = dict(zip(COLS_ITEM, params))
        pid = d.get("numero_controle_pncp")
        ni = d.get("numero_item")
        payload = json.dumps(d, ensure_ascii=False, separators=(",", ":"))
    raw = json.dumps(row_json_seguro(row), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    chave = hashlib.sha256((ano_mes + "\n" + raw).encode("utf-8", errors="replace")).hexdigest()
    con.execute(
        """INSERT INTO comprasgov_itens_quarentena(
               chave_linha,ano_mes,numero_controle_informado,numero_item,motivo,
               payload_json,raw_row_json,status,criado_em,atualizado_em
           ) VALUES (?,?,?,?,?,?,?,'PENDENTE',?,?)
           ON CONFLICT(chave_linha) DO UPDATE SET
               motivo=excluded.motivo,
               payload_json=COALESCE(excluded.payload_json,comprasgov_itens_quarentena.payload_json),
               raw_row_json=COALESCE(excluded.raw_row_json,comprasgov_itens_quarentena.raw_row_json),
               atualizado_em=excluded.atualizado_em""",
        (chave, ano_mes, texto(pid), inteiro(ni), motivo, payload, raw, stamp, stamp),
    )
    return chave



def guardar_compra_quarentena(
    con: sqlite3.Connection, ano_mes: str, row: dict[Any, Any], motivo: str,
) -> str:
    stamp = agora()
    seguro = row_json_seguro(row)
    raw = json.dumps(seguro, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    pid = None
    for v in valores_escalares_row(row):
        tv = texto(v)
        if tv and pncp_id_valido(tv):
            pid = tv
            break
    chave = hashlib.sha256((ano_mes + "\nCOMPRA\n" + raw).encode("utf-8", errors="replace")).hexdigest()
    con.execute(
        """INSERT INTO comprasgov_contratacoes_quarentena(
               chave_linha,ano_mes,numero_controle_informado,motivo,raw_row_json,
               status,criado_em,atualizado_em
           ) VALUES (?,?,?,?,?,'PENDENTE',?,?)
           ON CONFLICT(chave_linha) DO UPDATE SET
               motivo=excluded.motivo,
               raw_row_json=COALESCE(excluded.raw_row_json,comprasgov_contratacoes_quarentena.raw_row_json),
               atualizado_em=excluded.atualizado_em""",
        (chave, ano_mes, pid, motivo, raw, stamp, stamp),
    )
    return chave


def importar_arquivo(
    con: sqlite3.Connection, path: Path, url: str, ano_mes: str, recurso: str,
    job_id: int | None, jobs_db: Path | None,
) -> dict[str, int]:
    total = contar_linhas(path)
    meta = meta_importacao(con, ano_mes, recurso)
    tamanho = path.stat().st_size
    resume = 0
    acumulado_novos = 0
    acumulado_orfaos = 0
    if meta and meta["tamanho_bytes"] == tamanho and meta["status"] in ("EXECUTANDO", "INTERROMPIDA", "ERRO", "PARCIAL"):
        resume = int(meta["linhas_processadas"] or 0)
        acumulado_novos = int(meta["registros_novos"] or 0)
        acumulado_orfaos = int(meta["registros_orfaos"] or 0) if "registros_orfaos" in meta.keys() else 0
    elif meta and meta["status"] == "CONCLUIDA" and meta["tamanho_bytes"] == tamanho:
        print(f"    {recurso}: já importado anteriormente; pulando.", flush=True)
        return {"linhas": int(meta["linhas_processadas"] or total), "novos": int(meta["registros_novos"] or 0), "orfaos": int(meta["registros_orfaos"] or 0) if "registros_orfaos" in meta.keys() else 0, "pulou": 1}
    else:
        resume = 0; acumulado_novos = 0; acumulado_orfaos = 0

    stamp = agora()
    salvar_meta(con, ano_mes, recurso, status="EXECUTANDO", url=url, arquivo=str(path.resolve()),
                tamanho_bytes=tamanho, linhas_total=total, linhas_processadas=resume,
                registros_novos=acumulado_novos, atualizado_em=stamp,
                iniciado_em=(meta["iniciado_em"] if meta and meta["iniciado_em"] else stamp), erro=None)
    atualizar_mes(job_id, jobs_db, ano_mes, etapa=f"Importando {recurso}", progresso_atual=resume,
                  progresso_total=total, unidade_progresso="linhas",
                  mensagem=f"{ano_mes}: importando {recurso}")

    f, reader = abrir_csv(path)
    try:
        headers = reader.fieldnames or []
        mapa = COMPRA_MAP if recurso == "COMPRA" else ITEM_MAP
        resolvido = resolver_colunas(headers, mapa)
        obrig = ["numero_controle_pncp"] + (["numero_item"] if recurso == "COMPRA_ITEM" else [])
        faltam = [c for c in obrig if not resolvido.get(c)]
        if faltam:
            raise RuntimeError(f"Colunas obrigatórias não encontradas em {path.name}: {faltam}. Cabeçalho: {headers}")

        # Importações iniciais normalmente adicionam muitos registros; WAL + executemany
        # reduz drasticamente o custo sem sacrificar retomada por lote.
        antes = con.execute("SELECT COUNT(*) FROM contratacoes" if recurso == "COMPRA" else "SELECT COUNT(*) FROM itens").fetchone()[0]
        batch: list[tuple[Any, ...]] = []
        processadas = 0
        efetivas = 0
        orfaos_run = acumulado_orfaos

        def gravar_batch() -> None:
            nonlocal batch, orfaos_run
            if not batch:
                return
            if recurso == "COMPRA":
                con.executemany(SQL_COMPRA, batch)
            else:
                validos, orfaos = separar_itens_com_pai(con, batch)
                if validos:
                    con.executemany(SQL_ITEM, validos)
                if orfaos:
                    orfaos_run += guardar_itens_orfaos(con, ano_mes, orfaos)
            con.commit()
            batch.clear()

        for row in reader:
            processadas += 1
            if processadas <= resume:
                continue

            if linha_csv_desalinhada(row, headers):
                if recurso == "COMPRA":
                    guardar_compra_quarentena(con, ano_mes, row, "LINHA_DESALINHADA")
                else:
                    guardar_item_quarentena(con, ano_mes, row, None, "LINHA_DESALINHADA")
                if processadas % 50000 < 2:
                    con.commit()
                continue

            if recurso == "COMPRA":
                if compra_linha_semantica_invalida(row, resolvido):
                    guardar_compra_quarentena(con, ano_mes, row, "LINHA_ESTRUTURALMENTE_INVALIDA")
                    continue
                params = parametros_compra(row, resolvido, ano_mes, stamp)
                if params is not None:
                    batch.append(params); efetivas += 1
                else:
                    guardar_compra_quarentena(con, ano_mes, row, "ID_PNCP_INVALIDO")
            else:
                params, situacao_id = parametros_item(row, resolvido, ano_mes, stamp)
                # V0.6.2.4: não reconstruir automaticamente linha cujo ID PNCP
                # apareceu em outra coluna. Um ID deslocado é evidência de que os
                # demais campos também podem estar deslocados. Só a posição
                # estrutural esperada (situacao_id == VALIDO) entra no operacional.
                if situacao_id != "VALIDO":
                    motivo_q = ("ID_DESLOCADO_NAO_APLICADO"
                                if situacao_id == "RECUPERADO_NA_LINHA" else (situacao_id or "ID_PNCP_INVALIDO"))
                    guardar_item_quarentena(con, ano_mes, row, params, motivo_q)
                    continue
                if params is None:
                    guardar_item_quarentena(con, ano_mes, row, None, "ITEM_ESTRUTURALMENTE_INVALIDO")
                    continue
                pid_item = params[COLS_ITEM.index("numero_controle_pncp")]
                ni_item = params[COLS_ITEM.index("numero_item")]
                try:
                    ni_ok = ni_item is not None and 0 < int(ni_item) <= 2_147_483_647
                except (TypeError, ValueError, OverflowError):
                    ni_ok = False
                if not pncp_id_valido(pid_item) or not ni_ok:
                    guardar_item_quarentena(con, ano_mes, row, params, "ITEM_ESTRUTURALMENTE_INVALIDO")
                    continue
                batch.append(params); efetivas += 1
            if len(batch) >= BATCH:
                gravar_batch()
                salvar_meta(con, ano_mes, recurso, linhas_processadas=processadas,
                            registros_orfaos=orfaos_run, atualizado_em=agora())
                atualizar_mes(job_id, jobs_db, ano_mes, progresso_atual=processadas, progresso_total=total,
                              unidade_progresso="linhas")
                if processadas % 50000 < BATCH:
                    print(f"      Importação {recurso}: {processadas:,}/{total:,} linhas", flush=True)
        gravar_batch()
        depois = con.execute("SELECT COUNT(*) FROM contratacoes" if recurso == "COMPRA" else "SELECT COUNT(*) FROM itens").fetchone()[0]
        novos_run = max(0, int(depois) - int(antes))
        novos = acumulado_novos + novos_run
        orfaos_pendentes = contar_orfaos(con, ano_mes) if recurso == "COMPRA_ITEM" else 0
        salvar_meta(con, ano_mes, recurso, status="CONCLUIDA", linhas_processadas=processadas,
                    registros_novos=novos,
                    registros_existentes=max(0, efetivas - novos_run - max(0, orfaos_run - acumulado_orfaos)),
                    registros_orfaos=orfaos_pendentes,
                    atualizado_em=agora(), finalizado_em=agora(), erro=None)
        atualizar_mes(job_id, jobs_db, ano_mes, progresso_atual=total, progresso_total=total, unidade_progresso="linhas")
        sufixo = f"; {orfaos_pendentes:,} item(ns) aguardando contratação-pai" if recurso == "COMPRA_ITEM" and orfaos_pendentes else ""
        print(f"    {recurso}: {processadas:,} linha(s); {novos_run:,} novo(s) nesta execução{sufixo}.", flush=True)
        return {"linhas": processadas, "novos": novos, "orfaos": orfaos_run, "pulou": 0}
    except Exception as exc:
        con.rollback()
        salvar_meta(con, ano_mes, recurso, status="ERRO", atualizado_em=agora(), erro=f"{type(exc).__name__}: {exc}")
        raise
    finally:
        f.close()


# A V0.6.2.4 removeu deliberadamente a antiga função que inferia completude
# comparando contagens do CSV com o banco. Arquivos de período são deltas e
# essa inferência não é válida.

def _iniciar_mes_fase(
    job_id: int | None, jobs_db: Path | None, ano_mes: str, fase: str
) -> None:
    atualizar_mes(job_id, jobs_db, ano_mes, status="EXECUTANDO", etapa=fase,
                  progresso_atual=0, progresso_total=0, unidade_progresso=None,
                  iniciado_em=agora(), finalizado_em=None, mensagem=f"{fase} · {ano_mes}")
    atualizar_job(job_id, jobs_db, data_atual=ano_mes, etapa=fase)


def processar_compra_mes(
    con: sqlite3.Connection, sessao: requests.Session, ano_mes: str,
    job_id: int | None, jobs_db: Path | None,
) -> None:
    _iniciar_mes_fase(job_id, jobs_db, ano_mes, "Fase 1/2 · Contratações")
    print(f"\n[FASE 1/2 · CONTRATAÇÕES][{ano_mes}]", flush=True)
    try:
        url, path = recurso_info(ano_mes, "COMPRA")
        baixar(sessao, url, path, ano_mes, job_id, jobs_db, "COMPRA")
        importar_arquivo(con, path, url, ano_mes, "COMPRA", job_id, jobs_db)
        atualizar_mes(job_id, jobs_db, ano_mes, status="PENDENTE",
                      etapa="Contratações concluídas · aguardando itens",
                      progresso_atual=1, progresso_total=1, unidade_progresso="etapa",
                      mensagem=f"{ano_mes}: COMPRA pronta; aguardando Fase 2/2")
    except Exception as exc:
        atualizar_mes(job_id, jobs_db, ano_mes, status="ERRO", etapa="Erro em contratações",
                      finalizado_em=agora(), mensagem=f"{type(exc).__name__}: {exc}")
        raise


def processar_item_mes(
    con: sqlite3.Connection, sessao: requests.Session, ano_mes: str,
    job_id: int | None, jobs_db: Path | None,
) -> tuple[int, int, int]:
    _iniciar_mes_fase(job_id, jobs_db, ano_mes, "Fase 2/2 · Itens")
    print(f"\n[FASE 2/2 · ITENS][{ano_mes}]", flush=True)
    try:
        url, path = recurso_info(ano_mes, "COMPRA_ITEM")
        baixar(sessao, url, path, ano_mes, job_id, jobs_db, "COMPRA_ITEM")
        stats = importar_arquivo(con, path, url, ano_mes, "COMPRA_ITEM", job_id, jobs_db)

        # IMPORTANTE: o arquivo mensal é um DELTA (novos/alterados). A presença
        # de itens no CSV não comprova o conjunto integral da contratação.
        pendentes_mes = contar_orfaos(con, ano_mes)
        pendentes_total = contar_orfaos(con)
        mensagem = (
            f"{ano_mes}: delta de itens importado ({stats.get('linhas', 0)} linhas); "
            "completude mantida para validação pelo PNCP"
        )
        if pendentes_mes:
            mensagem += f" · {pendentes_mes} item(ns) aguardando contratação-pai"
        atualizar_mes(job_id, jobs_db, ano_mes, status="CONCLUIDA", etapa="Delta de itens concluído",
                      progresso_atual=1, progresso_total=1, unidade_progresso="etapa",
                      finalizado_em=agora(), mensagem=mensagem)
        print(
            "    Delta de itens importado. Nenhuma contratação foi marcada como "
            "itens_coletados=1 pelo CSV.", flush=True
        )
        if pendentes_mes:
            print(f"    Integridade: {pendentes_mes:,} item(ns) deste período permanecem em staging.", flush=True)
        if pendentes_total:
            print(f"    Staging total: {pendentes_total:,} item(ns) órfão(s) pendente(s).", flush=True)
        return 0, int(stats.get("linhas", 0)), pendentes_mes
    except Exception as exc:
        atualizar_mes(job_id, jobs_db, ano_mes, status="ERRO", etapa="Erro em itens",
                      finalizado_em=agora(), mensagem=f"{type(exc).__name__}: {exc}")
        raise


def conectar_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(path), timeout=60)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON")
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA busy_timeout=60000")
    criar_banco_base(con)
    preparar_banco_itens(con)
    garantir_proveniencia(con, assumir_legado_pncp=True)
    # V0.7.6: identificadores nativos do Compras.gov para cruzar API/CSV e enriquecer itens.
    cols_c = {str(r[1]) for r in con.execute("PRAGMA table_info(contratacoes)").fetchall()}
    if "comprasgov_id_compra" not in cols_c:
        con.execute("ALTER TABLE contratacoes ADD COLUMN comprasgov_id_compra TEXT")
    if "comprasgov_contratacao_excluida" not in cols_c:
        con.execute("ALTER TABLE contratacoes ADD COLUMN comprasgov_contratacao_excluida INTEGER")
    if "comprasgov_existe_resultado" not in cols_c:
        con.execute("ALTER TABLE contratacoes ADD COLUMN comprasgov_existe_resultado INTEGER")
    cols_i = {str(r[1]) for r in con.execute("PRAGMA table_info(itens)").fetchall()}
    if "comprasgov_id_compra" not in cols_i:
        con.execute("ALTER TABLE itens ADD COLUMN comprasgov_id_compra TEXT")
    if "comprasgov_id_compra_item" not in cols_i:
        con.execute("ALTER TABLE itens ADD COLUMN comprasgov_id_compra_item TEXT")
    con.execute("CREATE INDEX IF NOT EXISTS idx_contratacoes_comprasgov_id ON contratacoes(comprasgov_id_compra)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_itens_comprasgov_id ON itens(comprasgov_id_compra_item)")
    con.commit()
    return con


def atualizar_auxiliares(
    db_compras: Path, job_id: int | None, jobs_db: Path | None,
    contratacoes_existentes_alteradas: list[str] | tuple[str, ...] | set[str] | None = None,
) -> None:
    """Atualiza FTS/TIC.

    Além de acrescentar rowids novos, a V0.7.6 pode reindexar/reclassificar
    contratos existentes alterados pela COLETA diária, pois uma atualização não
    muda o rowid da tabela principal.
    """
    alteradas = list(dict.fromkeys(str(x) for x in (contratacoes_existentes_alteradas or []) if x))
    if DB_FTS.exists():
        try:
            import fts_manager as fm
            if alteradas:
                n = fm.atualizar_contratacoes_especificas(db_compras, DB_FTS, alteradas)
                if n:
                    print(f"\n[FINAL] FTS5 · {n:,} contratação(ões) existente(s) reindexada(s) por alteração.", flush=True)
            st = fm.status_indice(db_compras, DB_FTS)
            if st.get("estado") in ("DESATUALIZADO", "INCOMPLETO"):
                print("\n[FINAL] Atualizando FTS5...", flush=True)
                fm.construir_ou_atualizar(
                    db_compras, DB_FTS, "update", max_segundos=120,
                    progress_callback=(
                        (lambda etapa, a, t: jm.atualizar_progresso_job(job_id, f"FTS5 · {etapa}", a, t, jobs_db))
                        if job_id is not None and jobs_db is not None else None
                    ),
                    stop_callback=(
                        (lambda: jm.parada_solicitada(job_id, jobs_db))
                        if job_id is not None and jobs_db is not None else None
                    ),
                )
        except InterruptedError:
            raise
        except Exception as exc:
            print(f"AVISO: FTS5 não pôde ser atualizado: {exc}", flush=True)

    if DB_TIC.exists():
        try:
            import tic_manager as tm
            if alteradas:
                n = tm.atualizar_contratacoes_especificas(db_compras, DB_TIC, alteradas)
                if n:
                    print(f"[FINAL] TIC · {n:,} contratação(ões) existente(s) reclassificada(s) por alteração.", flush=True)
            st = tm.status_classificacao(db_compras, DB_TIC)
            if st.get("estado") == "DESATUALIZADA":
                print("[FINAL] Atualizando classificação TIC...", flush=True)
                tm.construir_ou_atualizar(
                    db_compras, DB_TIC, "update",
                    progress_callback=(
                        (lambda etapa, a, t: jm.atualizar_progresso_job(job_id, f"TIC · {etapa}", a, t, jobs_db))
                        if job_id is not None and jobs_db is not None else None
                    ),
                    stop_callback=(
                        (lambda: jm.parada_solicitada(job_id, jobs_db))
                        if job_id is not None and jobs_db is not None else None
                    ),
                )
        except InterruptedError:
            raise
        except Exception as exc:
            print(f"AVISO: classificação TIC não pôde ser atualizada: {exc}", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description="Importação de deltas mensais via CSV do Compras.gov — V0.7.6")
    ap.add_argument("--inicio", required=True, help="Mês inicial AAAA-MM")
    ap.add_argument("--fim", required=True, help="Mês final AAAA-MM")
    ap.add_argument("--db", type=Path, default=DB_COMPRAS)
    ap.add_argument("--jobs-db", type=Path, default=DB_JOBS)
    ap.add_argument("--job-id", type=int)
    args = ap.parse_args()

    garantir_diretorios(); CSV_DIR.mkdir(parents=True, exist_ok=True)
    faixa = meses(args.inicio, args.fim)
    jobs_db = args.jobs_db if args.job_id else None
    print("=" * 92, flush=True)
    print("Radar PNCP / Radar PNCP V0.7.6 — DELTAS MENSAIS VIA COMPRAS.GOV CSV", flush=True)
    print("=" * 92, flush=True)
    print(f"Banco: {args.db.resolve()}", flush=True)
    print(f"Período mensal: {faixa[0]} → {faixa[-1]} ({len(faixa)} mês(es))", flush=True)
    print("Ordem: Fase 1/2 = deltas de CONTRATAÇÕES; Fase 2/2 = deltas de ITENS.", flush=True)
    print("Precedência: PNCP_API > COMPRASGOV_CSV. CSV mensal é delta e não confirma completude de itens.", flush=True)

    con = conectar_db(args.db)
    sessao = sessao_http()
    staging_inicio = contar_orfaos(con)
    resolvidos_total = 0
    try:
        etapas_total = len(faixa) * 2
        etapas_feitas = 0
        atualizar_job(args.job_id, jobs_db, progresso_atual=0, progresso_total=etapas_total,
                      etapa="Fase 1/2 · Contratações")

        print("\n" + "=" * 92, flush=True)
        print("FASE 1/2 — IMPORTANDO DELTAS DE CONTRATAÇÕES DO PERÍODO", flush=True)
        print("=" * 92, flush=True)
        for mes in faixa:
            processar_compra_mes(con, sessao, mes, args.job_id, jobs_db)
            etapas_feitas += 1
            atualizar_job(args.job_id, jobs_db, progresso_atual=etapas_feitas,
                          progresso_total=etapas_total, etapa=f"Fase 1/2 concluída · {mes}")

        # Só depois de todas as COMPRA estarem presentes tentamos resolver o staging
        # herdado de execuções anteriores (ex.: janeiro/2025 da primeira V0.6.1).
        staging_antes_resolucao = contar_orfaos(con)
        if staging_antes_resolucao:
            print("\n[ENTRE FASES] Reavaliando staging após importar os deltas de contratações...", flush=True)
            ultimo_impresso = {-1}
            def progresso_staging(atual: int, total: int) -> None:
                atualizar_job(args.job_id, jobs_db, data_atual=None, etapa="Reavaliando staging",
                              progresso_atual=etapas_feitas, progresso_total=etapas_total)
                marco = atual // 10000 if atual else 0
                if marco not in ultimo_impresso or atual == total:
                    ultimo_impresso.add(marco)
                    print(f"    Staging resolvível: {atual:,}/{total:,} item(ns)", flush=True)
            resolvidos, por_mes = resolver_itens_orfaos(con, progress_callback=progresso_staging)
            registrar_orfaos_resolvidos(con, por_mes)
            resolvidos_total += resolvidos
            print(f"    Resolvidos após Fase 1: {resolvidos:,} item(ns).", flush=True)
            print(f"    Staging restante: {contar_orfaos(con):,} item(ns).", flush=True)

        print("\n" + "=" * 92, flush=True)
        print("FASE 2/2 — IMPORTANDO DELTAS DE ITENS DO PERÍODO", flush=True)
        print("=" * 92, flush=True)
        for mes in faixa:
            processar_item_mes(con, sessao, mes, args.job_id, jobs_db)
            etapas_feitas += 1
            atualizar_job(args.job_id, jobs_db, progresso_atual=etapas_feitas,
                          progresso_total=etapas_total, etapa=f"Fase 2/2 concluída · {mes}")

        # Última tentativa, útil para casos em que um item foi posto em staging por
        # alguma inconsistência transitória durante a própria Fase 2.
        resolvidos_finais, por_mes_finais = resolver_itens_orfaos(con)
        if resolvidos_finais:
            registrar_orfaos_resolvidos(con, por_mes_finais)
            resolvidos_total += resolvidos_finais
            print(f"\n[FINAL] Mais {resolvidos_finais:,} item(ns) resolvido(s) do staging.", flush=True)

        staging_fim = contar_orfaos(con)
        placeholders = ",".join("?" for _ in faixa)
        staging_periodo = int(con.execute(
            f"SELECT COUNT(*) FROM comprasgov_itens_orfaos WHERE ano_mes IN ({placeholders})",
            faixa,
        ).fetchone()[0]) if faixa else 0

        atualizar_job(args.job_id, jobs_db, data_atual=None,
                      progresso_atual=etapas_total, progresso_total=etapas_total, etapa="Atualizando índices")
        try:
            atualizar_auxiliares(args.db.resolve(), args.job_id, jobs_db)
        except InterruptedError:
            return 130
        atualizar_job(args.job_id, jobs_db, data_atual=None,
                      progresso_atual=etapas_total, progresso_total=etapas_total, etapa="Concluído")

        print("\n" + "=" * 92, flush=True)
        print("RESUMO DE INTEGRIDADE DOS DELTAS CSV", flush=True)
        print("=" * 92, flush=True)
        print(f"Staging no início do job:       {staging_inicio:,} item(ns)", flush=True)
        print(f"Órfãos resolvidos neste job:    {resolvidos_total:,} item(ns)", flush=True)
        print(f"Staging pendente ao final:      {staging_fim:,} item(ns)", flush=True)
        print(f"Pendentes do período solicitado:{staging_periodo:>12,} item(ns)", flush=True)
        print("\nCarga CSV concluída.", flush=True)
        return 0
    except KeyboardInterrupt:
        print("\nInterrompida pelo usuário.", flush=True); return 130
    except Exception as exc:
        print(f"\nERRO: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True); return 5
    finally:
        sessao.close(); con.close()


if __name__ == "__main__":
    raise SystemExit(main())
