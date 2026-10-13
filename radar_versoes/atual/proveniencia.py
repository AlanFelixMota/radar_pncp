#!/usr/bin/env python3
"""Migrações e utilitários de proveniência de dados da RADAR_PNCP/Radar PNCP."""

from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime

FONTE_PNCP = "PNCP_API"
FONTE_COMPRASGOV_CSV = "COMPRASGOV_CSV"
FONTE_COMPRASGOV_API = "COMPRASGOV_API"
MIGRACAO_RESET_COMPLETUDE_CSV = "v0.6.1.2_reset_completude_csv_mensal"
MIGRACAO_PROVENIENCIA_STAGING = "v0.6.3.0_backfill_itens_fonte_staging"
MIGRACAO_RESET_FALSOS_404_ITENS = "v0.6.3.1_reset_falsos_404_itens_endpoint_consulta"
PNCP_ID_RE = re.compile(r"^(?P<cnpj>\d{14})-(?P<tipo>\d)-(?P<sequencial>\d{6})/(?P<ano>\d{4})$")


def pncp_id_valido(valor: object) -> bool:
    if valor is None:
        return False
    return PNCP_ID_RE.fullmatch(str(valor).strip()) is not None


def decompor_pncp_id(valor: object) -> tuple[str, int, int] | None:
    """Retorna (cnpj, ano, sequencial) para um número de controle PNCP válido."""
    if valor is None:
        return None
    m = PNCP_ID_RE.fullmatch(str(valor).strip())
    if not m:
        return None
    return m.group("cnpj"), int(m.group("ano")), int(m.group("sequencial"))


def agora() -> str:
    return datetime.now().isoformat(timespec="seconds")


def tabela_existe(con: sqlite3.Connection, tabela: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (tabela,)
    ).fetchone()
    return row is not None


def coluna_existe(con: sqlite3.Connection, tabela: str, coluna: str) -> bool:
    if not tabela_existe(con, tabela):
        return False
    rows = con.execute(f"PRAGMA table_info({tabela})").fetchall()
    for row in rows:
        nome = row["name"] if isinstance(row, sqlite3.Row) else row[1]
        if nome == coluna:
            return True
    return False


def _add_column(con: sqlite3.Connection, tabela: str, coluna: str, definicao: str) -> None:
    if tabela_existe(con, tabela) and not coluna_existe(con, tabela, coluna):
        con.execute(f"ALTER TABLE {tabela} ADD COLUMN {coluna} {definicao}")


def _garantir_tabelas_controle(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS radar_pncp_migracoes (
            chave TEXT PRIMARY KEY,
            executada_em TEXT NOT NULL,
            detalhes_json TEXT
        );

        CREATE TABLE IF NOT EXISTS comprasgov_cobertura (
            chave TEXT PRIMARY KEY,
            valor TEXT,
            atualizado_em TEXT NOT NULL
        );
        """
    )


def registrar_cobertura(con: sqlite3.Connection, chave: str, valor: str) -> None:
    con.execute(
        """INSERT INTO comprasgov_cobertura(chave,valor,atualizado_em)
           VALUES (?,?,?)
           ON CONFLICT(chave) DO UPDATE SET
               valor=excluded.valor,
               atualizado_em=excluded.atualizado_em""",
        (chave, valor, agora()),
    )
    con.commit()


def obter_cobertura(con: sqlite3.Connection, chave: str) -> str | None:
    if not tabela_existe(con, "comprasgov_cobertura"):
        return None
    row = con.execute(
        "SELECT valor FROM comprasgov_cobertura WHERE chave=?", (chave,)
    ).fetchone()
    return str(row[0]) if row and row[0] is not None else None


def reparar_completude_csv_mensal(con: sqlite3.Connection, forcar: bool = False) -> int:
    """Desfaz marcações de completude feitas por CSVs mensais na V0.6.1/1.1.

    Os CSVs mensal/anual do Compras.gov são deltas de registros novos/alterados,
    portanto a presença de N itens num arquivo de período não comprova que a
    contratação possua somente aqueles N itens. Os itens gravados permanecem no
    banco; apenas a flag que fazia o coletor PNCP pular a contratação é reparada.

    A migração é idempotente e não toca em contratações cuja completude foi
    confirmada pela API do PNCP (itens_fonte=PNCP_API).
    """
    _garantir_tabelas_controle(con)
    if not forcar:
        feito = con.execute(
            "SELECT 1 FROM radar_pncp_migracoes WHERE chave=?", (MIGRACAO_RESET_COMPLETUDE_CSV,)
        ).fetchone()
        if feito:
            return 0

    if not tabela_existe(con, "contratacoes") or not coluna_existe(con, "contratacoes", "itens_fonte"):
        qtd = 0
    else:
        qtd = int(con.execute(
            """SELECT COUNT(*) FROM contratacoes
               WHERE COALESCE(itens_coletados,0)=1
                 AND itens_fonte=?""",
            (FONTE_COMPRASGOV_CSV,),
        ).fetchone()[0] or 0)
        if qtd:
            # Não removemos os itens já importados. Apenas retiramos a afirmação
            # de completude, zeramos a contagem potencialmente parcial e devolvemos
            # a contratação à fila de validação do PNCP.
            con.execute(
                """UPDATE contratacoes
                      SET itens_coletados=0,
                          qtd_itens=0,
                          itens_status='PENDENTE_VALIDACAO_PNCP',
                          itens_erro=NULL,
                          itens_http_status=NULL,
                          itens_indisponivel_em=NULL,
                          itens_fonte=?,
                          itens_ultima_tentativa=NULL
                    WHERE COALESCE(itens_coletados,0)=1
                      AND itens_fonte=?""",
                (FONTE_COMPRASGOV_CSV, FONTE_COMPRASGOV_CSV),
            )

    con.execute(
        """INSERT INTO radar_pncp_migracoes(chave,executada_em,detalhes_json)
           VALUES (?,?,?)
           ON CONFLICT(chave) DO UPDATE SET
               executada_em=excluded.executada_em,
               detalhes_json=excluded.detalhes_json""",
        (
            MIGRACAO_RESET_COMPLETUDE_CSV,
            agora(),
            json.dumps({"contratacoes_reparadas": qtd}, ensure_ascii=False),
        ),
    )
    con.commit()
    return qtd




def reparar_proveniencia_itens_staging(con: sqlite3.Connection, forcar: bool = False) -> int:
    """Recupera a proveniência CSV de pais cujos itens saíram do staging.

    Durante a reconciliação 0.6.2.x, um pai ausente podia ser criado pela API do
    PNCP e, em seguida, seus itens CSV eram movidos do staging para `itens`. O pai
    ficava corretamente pncp_confirmado=1, mas `itens_fonte` não registrava que o
    conjunto local ainda era proveniente do CSV e precisava de validação integral.

    A migração usa apenas pais que transitaram pela fila de reconciliação e que ainda
    não estão marcados como completos; portanto não rebaixa conjuntos já confirmados
    pela API do PNCP. É idempotente e executada uma única vez por banco.
    """
    _garantir_tabelas_controle(con)
    if not forcar:
        feito = con.execute(
            "SELECT 1 FROM radar_pncp_migracoes WHERE chave=?",
            (MIGRACAO_PROVENIENCIA_STAGING,),
        ).fetchone()
        if feito:
            return 0

    obrig = (
        tabela_existe(con, "contratacoes")
        and tabela_existe(con, "itens")
        and tabela_existe(con, "pncp_reconciliacao_staging")
        and coluna_existe(con, "contratacoes", "itens_fonte")
        and coluna_existe(con, "contratacoes", "itens_coletados")
    )
    qtd = 0
    if obrig:
        cur = con.execute(
            """UPDATE contratacoes
                  SET itens_fonte=?
                WHERE COALESCE(itens_coletados,0)=0
                  AND numero_controle_pncp IN (
                      SELECT numero_controle_pncp
                        FROM pncp_reconciliacao_staging
                       WHERE status IN ('RESOLVIDA','JA_EXISTIA','PAI_IMPORTADO')
                  )
                  AND EXISTS (
                      SELECT 1 FROM itens i
                       WHERE i.numero_controle_pncp=contratacoes.numero_controle_pncp
                         AND (i.fonte_primeira=? OR i.fonte_ultima=? OR i.csv_item_mes IS NOT NULL)
                  )""",
            (FONTE_COMPRASGOV_CSV, FONTE_COMPRASGOV_CSV, FONTE_COMPRASGOV_CSV),
        )
        qtd = max(0, int(cur.rowcount or 0))

    con.execute(
        """INSERT INTO radar_pncp_migracoes(chave,executada_em,detalhes_json)
           VALUES (?,?,?)
           ON CONFLICT(chave) DO UPDATE SET
             executada_em=excluded.executada_em, detalhes_json=excluded.detalhes_json""",
        (MIGRACAO_PROVENIENCIA_STAGING, agora(), json.dumps({"pais_corrigidos": qtd}, ensure_ascii=False)),
    )
    con.commit()
    return qtd



def reparar_falsos_404_itens_endpoint_consulta(con: sqlite3.Connection, forcar: bool = False) -> int:
    """Reabre validações de itens que receberam 404/410 usando a rota errada.

    Até a V0.6.3.0, a coleta detalhada de itens chamava indevidamente
    ``/api/consulta/v1/orgaos/.../compras/.../itens``. O serviço oficial de
    detalhe pertence à API principal, em ``/api/pncp/v1``. Como o texto do erro
    persistido contém a URL requisitada, esta migração é deliberadamente
    conservadora: só reabre linhas cuja evidência aponta explicitamente para a
    rota antiga. Itens locais são preservados e ``qtd_itens`` é recomposta a
    partir deles.
    """
    _garantir_tabelas_controle(con)
    if not forcar:
        feito = con.execute(
            "SELECT 1 FROM radar_pncp_migracoes WHERE chave=?",
            (MIGRACAO_RESET_FALSOS_404_ITENS,),
        ).fetchone()
        if feito:
            return 0

    qtd = 0
    colunas_reparo = (
        "itens_coletados", "pncp_confirmado", "itens_http_status", "itens_erro",
        "itens_status", "itens_indisponivel_em", "itens_ultima_tentativa", "qtd_itens",
    )
    pode_reparar = (
        tabela_existe(con, "contratacoes")
        and tabela_existe(con, "itens")
        and all(coluna_existe(con, "contratacoes", c) for c in colunas_reparo)
    )
    if pode_reparar:
        candidatos = int(con.execute(
            """SELECT COUNT(*) FROM contratacoes
                 WHERE COALESCE(itens_coletados,0)=0
                   AND COALESCE(pncp_confirmado,0)=1
                   AND itens_http_status IN (404,410)
                   AND LOWER(COALESCE(itens_erro,'')) LIKE '%/api/consulta/v1/orgaos/%/compras/%/itens%'"""
        ).fetchone()[0] or 0)
        if candidatos:
            con.execute(
                """UPDATE contratacoes
                      SET itens_status='PENDENTE_VALIDACAO_PNCP',
                          itens_erro=NULL,
                          itens_http_status=NULL,
                          itens_indisponivel_em=NULL,
                          itens_ultima_tentativa=NULL,
                          qtd_itens=(
                              SELECT COUNT(*) FROM itens i
                               WHERE i.numero_controle_pncp=contratacoes.numero_controle_pncp
                          )
                    WHERE COALESCE(itens_coletados,0)=0
                      AND COALESCE(pncp_confirmado,0)=1
                      AND itens_http_status IN (404,410)
                      AND LOWER(COALESCE(itens_erro,'')) LIKE '%/api/consulta/v1/orgaos/%/compras/%/itens%'"""
            )
            qtd = candidatos

    con.execute(
        """INSERT INTO radar_pncp_migracoes(chave,executada_em,detalhes_json)
           VALUES (?,?,?)
           ON CONFLICT(chave) DO UPDATE SET
             executada_em=excluded.executada_em, detalhes_json=excluded.detalhes_json""",
        (
            MIGRACAO_RESET_FALSOS_404_ITENS,
            agora(),
            json.dumps({"validacoes_reabertas": qtd}, ensure_ascii=False),
        ),
    )
    con.commit()
    return qtd

def garantir_proveniencia(con: sqlite3.Connection, assumir_legado_pncp: bool = True) -> None:
    """Adiciona metadados sem recriar tabelas e sem apagar dados existentes."""
    if tabela_existe(con, "contratacoes"):
        _add_column(con, "contratacoes", "fonte_primeira", "TEXT DEFAULT 'PNCP_API'")
        _add_column(con, "contratacoes", "fonte_ultima", "TEXT DEFAULT 'PNCP_API'")
        _add_column(con, "contratacoes", "pncp_confirmado", "INTEGER NOT NULL DEFAULT 1")
        _add_column(con, "contratacoes", "csv_compra_mes", "TEXT")
        _add_column(con, "contratacoes", "csv_compra_importada_em", "TEXT")
        _add_column(con, "contratacoes", "itens_fonte", "TEXT DEFAULT 'PNCP_API'")
        _add_column(con, "contratacoes", "itens_csv_mes", "TEXT")
        _add_column(con, "contratacoes", "itens_csv_historico_inicio", "TEXT")
        _add_column(con, "contratacoes", "itens_csv_historico_fim", "TEXT")
        _add_column(con, "contratacoes", "itens_csv_historico_em", "TEXT")

        # Bancos até a V0.6 só recebiam dados pela API. Os DEFAULTs conferem
        # semântica compatível às linhas legadas sem um UPDATE massivo.
        if assumir_legado_pncp:
            pass

    if tabela_existe(con, "itens"):
        _add_column(con, "itens", "fonte_primeira", "TEXT DEFAULT 'PNCP_API'")
        _add_column(con, "itens", "fonte_ultima", "TEXT DEFAULT 'PNCP_API'")
        _add_column(con, "itens", "pncp_confirmado", "INTEGER NOT NULL DEFAULT 1")
        _add_column(con, "itens", "csv_item_mes", "TEXT")
        _add_column(con, "itens", "csv_item_importada_em", "TEXT")
        if assumir_legado_pncp:
            pass

    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS comprasgov_importacoes (
            ano_mes TEXT NOT NULL,
            recurso TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'PENDENTE',
            url TEXT,
            arquivo TEXT,
            tamanho_bytes INTEGER,
            linhas_total INTEGER,
            linhas_processadas INTEGER NOT NULL DEFAULT 0,
            registros_novos INTEGER NOT NULL DEFAULT 0,
            registros_existentes INTEGER NOT NULL DEFAULT 0,
            campos_complementados INTEGER NOT NULL DEFAULT 0,
            iniciado_em TEXT,
            atualizado_em TEXT,
            finalizado_em TEXT,
            erro TEXT,
            PRIMARY KEY (ano_mes, recurso)
        );

        CREATE INDEX IF NOT EXISTS idx_comprasgov_importacoes_status
            ON comprasgov_importacoes(status);

        CREATE TABLE IF NOT EXISTS comprasgov_itens_orfaos (
            numero_controle_pncp TEXT NOT NULL,
            numero_item INTEGER NOT NULL,
            ano_mes TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            primeira_tentativa_em TEXT NOT NULL,
            ultima_tentativa_em TEXT NOT NULL,
            PRIMARY KEY (numero_controle_pncp, numero_item)
        );

        CREATE INDEX IF NOT EXISTS idx_comprasgov_itens_orfaos_mes
            ON comprasgov_itens_orfaos(ano_mes);

        CREATE TABLE IF NOT EXISTS comprasgov_itens_quarentena (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chave_linha TEXT NOT NULL UNIQUE,
            ano_mes TEXT NOT NULL,
            numero_controle_informado TEXT,
            numero_item INTEGER,
            motivo TEXT NOT NULL,
            payload_json TEXT,
            raw_row_json TEXT,
            id_recuperado TEXT,
            status TEXT NOT NULL DEFAULT 'PENDENTE',
            criado_em TEXT NOT NULL,
            atualizado_em TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_comprasgov_quarentena_status
            ON comprasgov_itens_quarentena(status);
        CREATE INDEX IF NOT EXISTS idx_comprasgov_quarentena_periodo
            ON comprasgov_itens_quarentena(ano_mes);

        CREATE TABLE IF NOT EXISTS comprasgov_contratacoes_quarentena (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chave_linha TEXT NOT NULL UNIQUE,
            ano_mes TEXT NOT NULL,
            numero_controle_informado TEXT,
            motivo TEXT NOT NULL,
            raw_row_json TEXT,
            status TEXT NOT NULL DEFAULT 'PENDENTE',
            criado_em TEXT NOT NULL,
            atualizado_em TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_comprasgov_contratacoes_quarentena_status
            ON comprasgov_contratacoes_quarentena(status);
        CREATE INDEX IF NOT EXISTS idx_comprasgov_contratacoes_quarentena_periodo
            ON comprasgov_contratacoes_quarentena(ano_mes);

        CREATE TABLE IF NOT EXISTS pncp_reconciliacao_staging (
            numero_controle_pncp TEXT PRIMARY KEY,
            classificacao TEXT NOT NULL,
            qtd_itens INTEGER NOT NULL DEFAULT 0,
            ano_min INTEGER,
            ano_max INTEGER,
            status TEXT NOT NULL DEFAULT 'PENDENTE',
            tentativas INTEGER NOT NULL DEFAULT 0,
            ultimo_http INTEGER,
            ultima_mensagem TEXT,
            primeira_tentativa_em TEXT,
            ultima_tentativa_em TEXT,
            resolvido_em TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_pncp_reconciliacao_status
            ON pncp_reconciliacao_staging(status);

        CREATE TABLE IF NOT EXISTS comprasgov_higiene_arquivos (
            arquivo TEXT PRIMARY KEY,
            tamanho_bytes INTEGER NOT NULL,
            mtime_ns INTEGER NOT NULL,
            status TEXT NOT NULL,
            linhas_lidas INTEGER NOT NULL DEFAULT 0,
            anomalias INTEGER NOT NULL DEFAULT 0,
            recuperadas INTEGER NOT NULL DEFAULT 0,
            atualizado_em TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS comprasgov_reparo_compra_arquivos (
            arquivo TEXT PRIMARY KEY,
            tamanho_bytes INTEGER NOT NULL,
            mtime_ns INTEGER NOT NULL,
            status TEXT NOT NULL,
            atualizado_em TEXT NOT NULL
        );
        """
    )
    _add_column(con, "comprasgov_importacoes", "registros_orfaos", "INTEGER NOT NULL DEFAULT 0")
    _add_column(con, "comprasgov_importacoes", "orfaos_resolvidos", "INTEGER NOT NULL DEFAULT 0")
    _add_column(con, "comprasgov_importacoes", "registros_quarentena", "INTEGER NOT NULL DEFAULT 0")
    _garantir_tabelas_controle(con)
    con.commit()

    # Correção de segurança da V0.6.1.2. Executa uma única vez por banco.
    reparar_completude_csv_mensal(con, forcar=False)
    reparar_proveniencia_itens_staging(con, forcar=False)
    reparar_falsos_404_itens_endpoint_consulta(con, forcar=False)
