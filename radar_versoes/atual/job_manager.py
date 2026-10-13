#!/usr/bin/env python3
"""Gerenciamento de jobs em segundo plano do Radar PNCP V0.7.6."""

from __future__ import annotations

import ctypes
import os
import sqlite3
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable


BASE_DIR = Path(__file__).resolve().parent

from radar_paths import DB_COMPRAS, DB_JOBS, DB_FTS, DB_TIC, LOG_DIR, garantir_diretorios

garantir_diretorios()

DB_COMPRAS_PADRAO = DB_COMPRAS
DB_JOBS_PADRAO = DB_JOBS
LOG_DIR_PADRAO = LOG_DIR

STATUS_ATIVOS = ("AGUARDANDO", "EXECUTANDO", "PARADA_SOLICITADA")
STATUS_REEXECUTAVEIS = (
    "INTERROMPIDA",
    "INTERROMPIDA_USUARIO",
    "ERRO",
    "PARCIAL",
)

TIPO_COLETA_DIARIA = "COLETA_DIARIA"
TIPO_CARGA_HISTORICA = "CARGA_HISTORICA"
TIPO_CARGA_CSV_COMPRASGOV = "CARGA_CSV_COMPRASGOV"
TIPO_CARGA_BASE_CSV_COMPRASGOV = "CARGA_BASE_CSV_COMPRASGOV"
TIPO_CARGA_2026_COMPRASGOV = "CARGA_2026_COMPRASGOV"
TIPO_API_COMPRASGOV_INCREMENTAL = "API_COMPRASGOV_INCREMENTAL"
TIPO_COLETA_MULTIFONTE_INCREMENTAL = "COLETA_MULTIFONTE_INCREMENTAL"
TIPO_ATUALIZACAO_DIARIA = "ATUALIZACAO_DIARIA"
TIPO_RECONCILIAR_STAGING_PNCP = "RECONCILIAR_STAGING_PNCP"
TIPO_VALIDAR_COMPLETUDE_PNCP = "VALIDAR_COMPLETUDE_PNCP"
TIPO_ITENS_INCREMENTAL = "ITENS_INCREMENTAL"
TIPO_RESULTADOS_PNCP = "RESULTADOS_PNCP"
TIPO_ITENS_MUTIRAO = "ITENS_MUTIRAO"
TIPO_ITENS_HIBRIDO_PILOTO = "ITENS_HIBRIDO_PILOTO"
TIPO_ITENS_HIBRIDO = "ITENS_HIBRIDO"
TIPO_INDICE_FTS = "INDICE_FTS"
TIPO_CLASSIFICACAO_TIC = "CLASSIFICACAO_TIC"

STATUS_DIA_RESOLVIDO = (
    "JA_EXISTIA",
    "CONCLUIDA",
    "CONCLUIDA_COM_PENDENCIAS_EXTERNAS",
)


def agora() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _datas_periodo(inicio: str, fim: str) -> list[str]:
    d1 = date.fromisoformat(inicio)
    d2 = date.fromisoformat(fim)
    if d2 < d1:
        raise ValueError("A data final não pode ser anterior à data inicial.")
    out: list[str] = []
    atual = d1
    while atual <= d2:
        out.append(atual.isoformat())
        atual += timedelta(days=1)
    return out


def _meses_periodo(inicio: str, fim: str) -> list[str]:
    """Retorna YYYY-MM inclusive. Aceita YYYY-MM ou uma data ISO completa."""
    def parse(v: str) -> tuple[int, int]:
        v = (v or "").strip()
        if len(v) >= 7:
            try:
                d = date.fromisoformat(v[:10]) if len(v) >= 10 else date.fromisoformat(v[:7] + "-01")
                return d.year, d.month
            except ValueError:
                pass
        raise ValueError("Mês inválido. Use AAAA-MM.")

    y1, m1 = parse(inicio)
    y2, m2 = parse(fim)
    if (y2, m2) < (y1, m1):
        raise ValueError("O mês final não pode ser anterior ao mês inicial.")
    out: list[str] = []
    y, m = y1, m1
    while (y, m) <= (y2, m2):
        out.append(f"{y:04d}-{m:02d}")
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1
    return out


def conectar_jobs(db_path: Path = DB_JOBS_PADRAO) -> sqlite3.Connection:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(db_path), timeout=15)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode = WAL")
    con.execute("PRAGMA busy_timeout = 15000")
    return con


def coluna_existe(con: sqlite3.Connection, tabela: str, coluna: str) -> bool:
    rows = con.execute(f"PRAGMA table_info({tabela})").fetchall()
    return any(row["name"] == coluna for row in rows)


def preparar_banco_jobs(db_path: Path = DB_JOBS_PADRAO) -> None:
    """
    Compatível com o radar_jobs.db criado pela V0.5.1/V0.5.1.1.
    As colunas novas são adicionadas automaticamente.
    """
    con = conectar_jobs(db_path)
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tipo TEXT NOT NULL,
            data_referencia TEXT,
            criado_em TEXT NOT NULL,
            iniciado_em TEXT,
            finalizado_em TEXT,
            status TEXT NOT NULL,
            worker_pid INTEGER,
            process_pid INTEGER,
            exit_code INTEGER,
            stop_requested INTEGER NOT NULL DEFAULT 0,
            log_path TEXT,
            mensagem TEXT,
            db_compras TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_jobs_status
            ON jobs(status);

        CREATE INDEX IF NOT EXISTS idx_jobs_criado
            ON jobs(criado_em DESC);

        CREATE INDEX IF NOT EXISTS idx_jobs_data
            ON jobs(data_referencia);
        """
    )

    # Migração transparente da V0.5.1.1.
    novas_colunas = {
        "data_inicio": "TEXT",
        "data_fim": "TEXT",
        "data_atual": "TEXT",
        "operacao": "TEXT",
        "artefato_path": "TEXT",
        "deadline_fts": "TEXT",
        "progresso_atual": "INTEGER",
        "progresso_total": "INTEGER",
        "etapa": "TEXT",
    }
    for coluna, tipo in novas_colunas.items():
        if not coluna_existe(con, "jobs", coluna):
            con.execute(f"ALTER TABLE jobs ADD COLUMN {coluna} {tipo}")

    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS job_dias (
            job_id INTEGER NOT NULL,
            data_referencia TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'PENDENTE',
            iniciado_em TEXT,
            finalizado_em TEXT,
            exit_code INTEGER,
            mensagem TEXT,
            PRIMARY KEY (job_id, data_referencia),
            FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_job_dias_job_status
            ON job_dias(job_id, status);

        CREATE INDEX IF NOT EXISTS idx_job_dias_data
            ON job_dias(data_referencia);
        """
    )

    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS job_csv_meses (
            job_id INTEGER NOT NULL,
            ano_mes TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'PENDENTE',
            etapa TEXT,
            progresso_atual INTEGER NOT NULL DEFAULT 0,
            progresso_total INTEGER NOT NULL DEFAULT 0,
            unidade_progresso TEXT,
            iniciado_em TEXT,
            finalizado_em TEXT,
            mensagem TEXT,
            PRIMARY KEY (job_id, ano_mes),
            FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_job_csv_meses_status
            ON job_csv_meses(job_id, status);

        CREATE TABLE IF NOT EXISTS coleta_atualizacao_checkpoints (
            janela_inicio TEXT NOT NULL,
            janela_fim TEXT NOT NULL,
            modalidade_id INTEGER NOT NULL,
            modalidade_nome TEXT,
            status TEXT NOT NULL DEFAULT 'PENDENTE',
            proxima_pagina INTEGER NOT NULL DEFAULT 1,
            total_paginas INTEGER,
            ultimo_erro TEXT,
            atualizado_em TEXT NOT NULL,
            PRIMARY KEY (janela_inicio, janela_fim, modalidade_id)
        );
        CREATE INDEX IF NOT EXISTS idx_coleta_checkpoint_status
            ON coleta_atualizacao_checkpoints(janela_inicio, janela_fim, status);
        """
    )

    con.commit()
    con.close()


def _row_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(row) if row is not None else None


def processo_existe(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False

    if os.name == "nt":
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        handle = ctypes.windll.kernel32.OpenProcess(
            PROCESS_QUERY_LIMITED_INFORMATION, False, int(pid)
        )
        if handle:
            ctypes.windll.kernel32.CloseHandle(handle)
            return True
        return False

    try:
        os.kill(int(pid), 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False


def atualizar_jobs_interrompidos(db_path: Path = DB_JOBS_PADRAO) -> int:
    preparar_banco_jobs(db_path)
    con = conectar_jobs(db_path)
    rows = con.execute(
        f"""
        SELECT *
        FROM jobs
        WHERE status IN ({','.join('?' for _ in STATUS_ATIVOS)})
        ORDER BY id
        """,
        STATUS_ATIVOS,
    ).fetchall()

    alterados = 0
    for row in rows:
        worker_pid = row["worker_pid"]
        # Job recém-criado pode ainda não ter recebido o PID.
        if row["status"] == "AGUARDANDO" and not worker_pid:
            continue

        if not processo_existe(worker_pid):
            fim = agora()
            con.execute(
                """
                UPDATE jobs
                SET status = 'INTERROMPIDA',
                    finalizado_em = COALESCE(finalizado_em, ?),
                    worker_pid = NULL,
                    process_pid = NULL,
                    etapa = CASE
                        WHEN tipo = 'RECONCILIAR_STAGING_PNCP'
                        THEN 'Interrompida · pronta para retomada'
                        ELSE etapa
                    END,
                    mensagem = CASE
                        WHEN mensagem IS NULL OR trim(mensagem) = ''
                        THEN 'Execução interrompida inesperadamente; progresso persistido e pronto para retomada.'
                        WHEN instr(mensagem, 'pronto para retomada') = 0
                        THEN mensagem || ' · Processo encerrado inesperadamente; progresso persistido e pronto para retomada.'
                        ELSE mensagem
                    END
                WHERE id = ?
                """,
                (fim, row["id"]),
            )
            con.execute(
                """
                UPDATE job_dias
                SET status = 'INTERROMPIDA',
                    finalizado_em = COALESCE(finalizado_em, ?),
                    mensagem = COALESCE(
                        mensagem,
                        'Dia interrompido porque o job deixou de estar ativo.'
                    )
                WHERE job_id = ?
                  AND status = 'EXECUTANDO'
                """,
                (fim, row["id"]),
            )
            con.execute(
                """
                UPDATE job_csv_meses
                   SET status='INTERROMPIDA',
                       finalizado_em=COALESCE(finalizado_em, ?),
                       mensagem=COALESCE(mensagem, 'Mês interrompido porque o job deixou de estar ativo.')
                 WHERE job_id=? AND status='EXECUTANDO'
                """,
                (fim, row["id"]),
            )
            alterados += 1

    con.commit()
    con.close()
    return alterados


def obter_job(job_id: int, db_path: Path = DB_JOBS_PADRAO) -> dict[str, Any] | None:
    preparar_banco_jobs(db_path)
    con = conectar_jobs(db_path)
    row = con.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    con.close()
    return _row_dict(row)



def listar_dias_job(job_id: int, db_path: Path = DB_JOBS_PADRAO) -> list[dict[str, Any]]:
    preparar_banco_jobs(db_path)
    con = conectar_jobs(db_path)
    rows = con.execute(
        "SELECT * FROM job_dias WHERE job_id=? ORDER BY data_referencia", (job_id,)
    ).fetchall()
    con.close()
    return [dict(r) for r in rows]


def datas_incompletas_periodo(
    db_compras: Path,
    data_inicio: str,
    data_fim: str,
) -> list[str]:
    return [d for d in _datas_periodo(data_inicio, data_fim) if not data_esta_completa(db_compras, d)]

def obter_job_ativo(db_path: Path = DB_JOBS_PADRAO) -> dict[str, Any] | None:
    atualizar_jobs_interrompidos(db_path)
    con = conectar_jobs(db_path)
    row = con.execute(
        f"""
        SELECT *
        FROM jobs
        WHERE status IN ({','.join('?' for _ in STATUS_ATIVOS)})
        ORDER BY id DESC
        LIMIT 1
        """,
        STATUS_ATIVOS,
    ).fetchone()
    con.close()
    return _row_dict(row)


def listar_jobs(
    db_path: Path = DB_JOBS_PADRAO,
    limite: int = 30,
) -> list[dict[str, Any]]:
    preparar_banco_jobs(db_path)
    atualizar_jobs_interrompidos(db_path)
    con = conectar_jobs(db_path)
    rows = con.execute(
        """
        SELECT *
        FROM jobs
        ORDER BY id DESC
        LIMIT ?
        """,
        (max(1, int(limite)),),
    ).fetchall()
    con.close()
    return [dict(r) for r in rows]


def _garantir_sem_job_ativo(con: sqlite3.Connection) -> None:
    if con.execute("SELECT 1 FROM sqlite_master WHERE name='radar_pncp_atualizacao'").fetchone():
        if con.execute('SELECT 1 FROM radar_pncp_atualizacao WHERE id=1').fetchone():
            raise RuntimeError('O Radar PNCP está sendo atualizada. Aguarde a conclusão para iniciar outro job.')
    ativo = con.execute(
        f"""
        SELECT id
        FROM jobs
        WHERE status IN ({','.join('?' for _ in STATUS_ATIVOS)})
        LIMIT 1
        """,
        STATUS_ATIVOS,
    ).fetchone()
    if ativo is not None:
        raise RuntimeError(
            f"Já existe uma execução ativa (job #{ativo['id']}). "
            "Aguarde ou solicite a parada antes de iniciar outra."
        )


def criar_job_coleta_diaria(
    data_referencia: str,
    db_compras: Path = DB_COMPRAS_PADRAO,
    db_jobs: Path = DB_JOBS_PADRAO,
    log_dir: Path = LOG_DIR_PADRAO,
) -> int:
    preparar_banco_jobs(db_jobs)
    atualizar_jobs_interrompidos(db_jobs)

    # Valida data.
    date.fromisoformat(data_referencia)

    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    con = conectar_jobs(db_jobs)
    try:
        con.execute("BEGIN IMMEDIATE")
        _garantir_sem_job_ativo(con)

        cur = con.execute(
            """
            INSERT INTO jobs (
                tipo, data_referencia, criado_em, status,
                db_compras, data_inicio, data_fim, data_atual
            )
            VALUES (?, ?, ?, 'AGUARDANDO', ?, ?, ?, ?)
            """,
            (
                TIPO_COLETA_DIARIA,
                data_referencia,
                agora(),
                str(Path(db_compras).resolve()),
                data_referencia,
                data_referencia,
                data_referencia,
            ),
        )
        job_id = int(cur.lastrowid)
        log_path = log_dir / f"job_{job_id:06d}_diario_{data_referencia}.log"
        con.execute(
            "UPDATE jobs SET log_path = ? WHERE id = ?",
            (str(log_path.resolve()), job_id),
        )
        con.commit()
        return job_id
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def criar_job_carga_historica(
    data_inicio: str,
    data_fim: str,
    db_compras: Path = DB_COMPRAS_PADRAO,
    db_jobs: Path = DB_JOBS_PADRAO,
    log_dir: Path = LOG_DIR_PADRAO,
) -> int:
    preparar_banco_jobs(db_jobs)
    atualizar_jobs_interrompidos(db_jobs)

    datas = _datas_periodo(data_inicio, data_fim)
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    con = conectar_jobs(db_jobs)
    try:
        con.execute("BEGIN IMMEDIATE")
        _garantir_sem_job_ativo(con)

        cur = con.execute(
            """
            INSERT INTO jobs (
                tipo, data_referencia, criado_em, status,
                db_compras, data_inicio, data_fim, data_atual
            )
            VALUES (?, NULL, ?, 'AGUARDANDO', ?, ?, ?, NULL)
            """,
            (
                TIPO_CARGA_HISTORICA,
                agora(),
                str(Path(db_compras).resolve()),
                data_inicio,
                data_fim,
            ),
        )
        job_id = int(cur.lastrowid)

        con.executemany(
            """
            INSERT INTO job_dias (job_id, data_referencia, status)
            VALUES (?, ?, 'PENDENTE')
            """,
            [(job_id, d) for d in datas],
        )

        log_path = (
            log_dir
            / f"job_{job_id:06d}_historico_{data_inicio}_a_{data_fim}.log"
        )
        con.execute(
            "UPDATE jobs SET log_path = ? WHERE id = ?",
            (str(log_path.resolve()), job_id),
        )
        con.commit()
        return job_id
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()




def criar_job_carga_csv_comprasgov(
    mes_inicio: str,
    mes_fim: str,
    db_compras: Path = DB_COMPRAS_PADRAO,
    db_jobs: Path = DB_JOBS_PADRAO,
    log_dir: Path = LOG_DIR_PADRAO,
) -> int:
    preparar_banco_jobs(db_jobs)
    atualizar_jobs_interrompidos(db_jobs)
    meses = _meses_periodo(mes_inicio, mes_fim)
    log_dir = Path(log_dir); log_dir.mkdir(parents=True, exist_ok=True)
    con = conectar_jobs(db_jobs)
    try:
        con.execute("BEGIN IMMEDIATE")
        _garantir_sem_job_ativo(con)
        cur = con.execute(
            """INSERT INTO jobs(
                   tipo, criado_em, status, db_compras, data_inicio, data_fim,
                   operacao, progresso_atual, progresso_total, etapa
               ) VALUES (?, ?, 'AGUARDANDO', ?, ?, ?, 'IMPORTAR', 0, ?, 'Preparando')""",
            (TIPO_CARGA_CSV_COMPRASGOV, agora(), str(Path(db_compras).resolve()),
             meses[0], meses[-1], len(meses) * 2),
        )
        jid = int(cur.lastrowid)
        con.executemany(
            "INSERT INTO job_csv_meses(job_id, ano_mes, status) VALUES (?, ?, 'PENDENTE')",
            [(jid, m) for m in meses],
        )
        log_path = log_dir / f"job_{jid:06d}_comprasgov_csv_{meses[0]}_a_{meses[-1]}.log"
        con.execute("UPDATE jobs SET log_path=? WHERE id=?", (str(log_path.resolve()), jid))
        con.commit(); return jid
    except Exception:
        con.rollback(); raise
    finally:
        con.close()


def iniciar_carga_csv_comprasgov(
    mes_inicio: str,
    mes_fim: str,
    db_compras: Path = DB_COMPRAS_PADRAO,
    db_jobs: Path = DB_JOBS_PADRAO,
    log_dir: Path = LOG_DIR_PADRAO,
) -> int:
    jid = criar_job_carga_csv_comprasgov(mes_inicio, mes_fim, db_compras, db_jobs, log_dir)
    iniciar_worker(jid, db_jobs)
    return jid


def criar_job_carga_base_csv_comprasgov(
    db_compras: Path = DB_COMPRAS_PADRAO,
    db_jobs: Path = DB_JOBS_PADRAO,
    log_dir: Path = LOG_DIR_PADRAO,
) -> int:
    """Cria o job único de reparo + carga-base anual 2021–2024."""
    preparar_banco_jobs(db_jobs)
    atualizar_jobs_interrompidos(db_jobs)
    log_dir = Path(log_dir); log_dir.mkdir(parents=True, exist_ok=True)
    con = conectar_jobs(db_jobs)
    try:
        con.execute("BEGIN IMMEDIATE")
        _garantir_sem_job_ativo(con)
        cur = con.execute(
            """INSERT INTO jobs(
                   tipo, criado_em, status, db_compras, data_inicio, data_fim,
                   operacao, progresso_atual, progresso_total, etapa
               ) VALUES (?, ?, 'AGUARDANDO', ?, '2021', '2024',
                         'BASE_HISTORICA', 0, 1, 'Preparando reparo')""",
            (TIPO_CARGA_BASE_CSV_COMPRASGOV, agora(), str(Path(db_compras).resolve())),
        )
        jid = int(cur.lastrowid)
        log_path = log_dir / f"job_{jid:06d}_comprasgov_base_2021_a_2024.log"
        con.execute("UPDATE jobs SET log_path=? WHERE id=?", (str(log_path.resolve()), jid))
        con.commit()
        return jid
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def iniciar_carga_base_csv_comprasgov(
    db_compras: Path = DB_COMPRAS_PADRAO,
    db_jobs: Path = DB_JOBS_PADRAO,
    log_dir: Path = LOG_DIR_PADRAO,
) -> int:
    jid = criar_job_carga_base_csv_comprasgov(db_compras, db_jobs, log_dir)
    iniciar_worker(jid, db_jobs)
    return jid




def criar_job_carga_2026_comprasgov(
    db_compras: Path = DB_COMPRAS_PADRAO,
    db_jobs: Path = DB_JOBS_PADRAO,
    log_dir: Path = LOG_DIR_PADRAO,
) -> int:
    preparar_banco_jobs(db_jobs); atualizar_jobs_interrompidos(db_jobs)
    log_dir=Path(log_dir); log_dir.mkdir(parents=True,exist_ok=True)
    con=conectar_jobs(db_jobs)
    try:
        con.execute("BEGIN IMMEDIATE"); _garantir_sem_job_ativo(con)
        cur=con.execute(
            """INSERT INTO jobs(tipo,criado_em,status,db_compras,data_inicio,data_fim,operacao,
                                 progresso_atual,progresso_total,etapa)
               VALUES (?,?,'AGUARDANDO',?,'2026','2026','BOOTSTRAP_2026',0,3,'Preparando')""",
            (TIPO_CARGA_2026_COMPRASGOV,agora(),str(Path(db_compras).resolve())),
        )
        jid=int(cur.lastrowid)
        log_path=log_dir/f"job_{jid:06d}_comprasgov_2026.log"
        con.execute("UPDATE jobs SET log_path=? WHERE id=?",(str(log_path.resolve()),jid)); con.commit(); return jid
    except Exception:
        con.rollback(); raise
    finally:
        con.close()


def iniciar_carga_2026_comprasgov(
    db_compras: Path = DB_COMPRAS_PADRAO,
    db_jobs: Path = DB_JOBS_PADRAO,
    log_dir: Path = LOG_DIR_PADRAO,
) -> int:
    jid=criar_job_carga_2026_comprasgov(db_compras,db_jobs,log_dir); iniciar_worker(jid,db_jobs); return jid


def criar_job_api_comprasgov_incremental(
    data_inicio: str,
    data_fim: str,
    incluir_itens: bool = False,
    db_compras: Path = DB_COMPRAS_PADRAO,
    db_jobs: Path = DB_JOBS_PADRAO,
    log_dir: Path = LOG_DIR_PADRAO,
) -> int:
    # valida datas e limita o job a uma janela razoável
    dias=_datas_periodo(data_inicio,data_fim)
    if len(dias)>91:
        raise ValueError("A API Compras.gov deve ser processada em janelas de até 91 dias.")
    preparar_banco_jobs(db_jobs); atualizar_jobs_interrompidos(db_jobs)
    log_dir=Path(log_dir); log_dir.mkdir(parents=True,exist_ok=True)
    con=conectar_jobs(db_jobs)
    try:
        con.execute("BEGIN IMMEDIATE"); _garantir_sem_job_ativo(con)
        operacao="COM_ITENS" if incluir_itens else "SEM_ITENS"
        cur=con.execute(
            """INSERT INTO jobs(tipo,criado_em,status,db_compras,data_inicio,data_fim,operacao,
                                 progresso_atual,progresso_total,etapa)
               VALUES (?,?,'AGUARDANDO',?,?,?,?,0,?,'Preparando')""",
            (TIPO_API_COMPRASGOV_INCREMENTAL,agora(),str(Path(db_compras).resolve()),
             data_inicio,data_fim,operacao,len(dias)),
        )
        jid=int(cur.lastrowid)
        log_path=log_dir/f"job_{jid:06d}_comprasgov_api_{data_inicio}_a_{data_fim}.log"
        con.execute("UPDATE jobs SET log_path=? WHERE id=?",(str(log_path.resolve()),jid)); con.commit(); return jid
    except Exception:
        con.rollback(); raise
    finally:
        con.close()


def iniciar_api_comprasgov_incremental(
    data_inicio: str,
    data_fim: str,
    incluir_itens: bool = False,
    db_compras: Path = DB_COMPRAS_PADRAO,
    db_jobs: Path = DB_JOBS_PADRAO,
    log_dir: Path = LOG_DIR_PADRAO,
) -> int:
    jid=criar_job_api_comprasgov_incremental(data_inicio,data_fim,incluir_itens,db_compras,db_jobs,log_dir)
    iniciar_worker(jid,db_jobs); return jid


def criar_job_coleta_multifonte_incremental(
    data_inicio: str,
    data_fim: str,
    db_compras: Path = DB_COMPRAS_PADRAO,
    db_jobs: Path = DB_JOBS_PADRAO,
    log_dir: Path = LOG_DIR_PADRAO,
) -> int:
    datas = _datas_periodo(data_inicio, data_fim)
    if len(datas) > 91:
        raise ValueError("A COLETA multifuente deve ser processada em janelas de até 91 dias.")
    preparar_banco_jobs(db_jobs)
    atualizar_jobs_interrompidos(db_jobs)
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    con = conectar_jobs(db_jobs)
    try:
        con.execute("BEGIN IMMEDIATE")
        _garantir_sem_job_ativo(con)
        cur = con.execute(
            """INSERT INTO jobs(
                   tipo, criado_em, status, db_compras, data_inicio, data_fim,
                   operacao, progresso_atual, progresso_total, etapa
               ) VALUES (?,?,'AGUARDANDO',?,?,?,'PNCP_PRIMARIO_COMPRAS_COMPLEMENTAR',0,?,'Preparando')""",
            (
                TIPO_COLETA_MULTIFONTE_INCREMENTAL, agora(),
                str(Path(db_compras).resolve()), data_inicio, data_fim, len(datas),
            ),
        )
        jid = int(cur.lastrowid)
        con.executemany(
            "INSERT INTO job_dias(job_id,data_referencia,status) VALUES (?,?,'PENDENTE')",
            [(jid, d) for d in datas],
        )
        log_path = log_dir / f"job_{jid:06d}_coleta_multifonte_{data_inicio}_a_{data_fim}.log"
        con.execute("UPDATE jobs SET log_path=? WHERE id=?", (str(log_path.resolve()), jid))
        con.commit()
        return jid
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def iniciar_coleta_multifonte_incremental(
    data_inicio: str,
    data_fim: str,
    db_compras: Path = DB_COMPRAS_PADRAO,
    db_jobs: Path = DB_JOBS_PADRAO,
    log_dir: Path = LOG_DIR_PADRAO,
) -> int:
    jid = criar_job_coleta_multifonte_incremental(
        data_inicio, data_fim, db_compras, db_jobs, log_dir
    )
    iniciar_worker(jid, db_jobs)
    return jid

def criar_job_atualizacao_diaria(
    janela_dias: int = 3,
    data_fim: str | None = None,
    db_compras: Path = DB_COMPRAS_PADRAO,
    db_jobs: Path = DB_JOBS_PADRAO,
    log_dir: Path = LOG_DIR_PADRAO,
    modo: str = "diario",
) -> int:
    janela_dias = int(janela_dias)
    if janela_dias < 1 or janela_dias > 60:
        raise ValueError("A janela móvel da COLETA deve ter entre 1 e 60 dias.")
    if modo not in {"diario", "revisao", "retomada"}:
        raise ValueError("Modo COLETA inválido.")
    fim = date.fromisoformat(data_fim) if data_fim else date.today()
    inicio = fim - timedelta(days=janela_dias - 1)
    inicio_s, fim_s = inicio.isoformat(), fim.isoformat()
    preparar_banco_jobs(db_jobs)
    atualizar_jobs_interrompidos(db_jobs)
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    con = conectar_jobs(db_jobs)
    try:
        con.execute("BEGIN IMMEDIATE")
        _garantir_sem_job_ativo(con)
        op = f"{modo.upper()}_ATUALIZACAO_{janela_dias}D"
        cur = con.execute(
            """INSERT INTO jobs(
                   tipo, criado_em, status, db_compras, data_inicio, data_fim,
                   operacao, progresso_atual, progresso_total, etapa
               ) VALUES (?,?,'AGUARDANDO',?,?,?, ?,0,16000,'Preparando')""",
            (
                TIPO_ATUALIZACAO_DIARIA, agora(), str(Path(db_compras).resolve()),
                inicio_s, fim_s, op,
            ),
        )
        jid = int(cur.lastrowid)
        log_path = log_dir / f"job_{jid:06d}_coleta_{modo}_{inicio_s}_a_{fim_s}.log"
        con.execute("UPDATE jobs SET log_path=? WHERE id=?", (str(log_path.resolve()), jid))
        con.commit()
        return jid
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def iniciar_atualizacao_diaria(
    janela_dias: int = 3,
    data_fim: str | None = None,
    db_compras: Path = DB_COMPRAS_PADRAO,
    db_jobs: Path = DB_JOBS_PADRAO,
    log_dir: Path = LOG_DIR_PADRAO,
    modo: str = "diario",
) -> int:
    jid = criar_job_atualizacao_diaria(janela_dias, data_fim, db_compras, db_jobs, log_dir, modo)
    iniciar_worker(jid, db_jobs)
    return jid


def obter_checkpoint_coleta(
    janela_inicio: str, janela_fim: str, modalidade_id: int,
    db_path: Path = DB_JOBS_PADRAO, con: sqlite3.Connection | None = None,
) -> dict[str, Any] | None:
    propria = con is None
    if propria:
        preparar_banco_jobs(db_path)
        con = conectar_jobs(db_path)
    assert con is not None
    row = con.execute(
        """SELECT * FROM coleta_atualizacao_checkpoints
           WHERE janela_inicio=? AND janela_fim=? AND modalidade_id=?""",
        (janela_inicio, janela_fim, int(modalidade_id)),
    ).fetchone()
    if propria:
        con.close()
    return _row_dict(row)


def salvar_checkpoint_coleta(
    janela_inicio: str, janela_fim: str, modalidade_id: int, modalidade_nome: str,
    status: str, proxima_pagina: int, total_paginas: int | None, ultimo_erro: str | None,
    db_path: Path = DB_JOBS_PADRAO, con: sqlite3.Connection | None = None,
) -> None:
    propria = con is None
    if propria:
        preparar_banco_jobs(db_path)
        con = conectar_jobs(db_path)
    assert con is not None
    con.execute(
        """INSERT INTO coleta_atualizacao_checkpoints(
               janela_inicio,janela_fim,modalidade_id,modalidade_nome,status,
               proxima_pagina,total_paginas,ultimo_erro,atualizado_em
           ) VALUES (?,?,?,?,?,?,?,?,?)
           ON CONFLICT(janela_inicio,janela_fim,modalidade_id) DO UPDATE SET
               modalidade_nome=excluded.modalidade_nome, status=excluded.status,
               proxima_pagina=excluded.proxima_pagina, total_paginas=excluded.total_paginas,
               ultimo_erro=excluded.ultimo_erro, atualizado_em=excluded.atualizado_em""",
        (janela_inicio, janela_fim, int(modalidade_id), modalidade_nome, status,
         max(1, int(proxima_pagina)), total_paginas, ultimo_erro, agora()),
    )
    con.commit()
    if propria:
        con.close()


def obter_ultima_coleta_parcial(db_path: Path = DB_JOBS_PADRAO) -> dict[str, Any] | None:
    """Última COLETA parcial não coberta por uma execução posterior concluída."""
    preparar_banco_jobs(db_path)
    con = conectar_jobs(db_path)
    row = con.execute(
        """SELECT j.* FROM jobs j
           WHERE j.tipo=? AND j.status IN ('PARCIAL','INTERROMPIDA','INTERROMPIDA_USUARIO')
             AND NOT EXISTS (
                 SELECT 1 FROM jobs x
                  WHERE x.tipo=j.tipo AND x.id>j.id
                    AND x.data_inicio<=j.data_inicio AND x.data_fim>=j.data_fim
                    AND x.status IN ('CONCLUIDA','CONCLUIDA_COM_PENDENCIAS_EXTERNAS')
             )
           ORDER BY j.id DESC LIMIT 1""",
        (TIPO_ATUALIZACAO_DIARIA,),
    ).fetchone()
    con.close()
    return _row_dict(row)


def obter_coleta_parcial_para_retomada_automatica(db_path: Path = DB_JOBS_PADRAO) -> dict[str, Any] | None:
    """COLETA parcial recuperável automaticamente; nunca retoma stop explícito do usuário."""
    row = obter_ultima_coleta_parcial(db_path)
    if not row:
        return None
    if str(row.get("status") or "") == "INTERROMPIDA_USUARIO":
        return None
    return row


def iniciar_retomada_coleta(
    job_origem_id: int | None = None,
    db_compras: Path = DB_COMPRAS_PADRAO,
    db_jobs: Path = DB_JOBS_PADRAO,
    log_dir: Path = LOG_DIR_PADRAO,
) -> int:
    preparar_banco_jobs(db_jobs)
    con = conectar_jobs(db_jobs)
    if job_origem_id is None:
        row = con.execute(
            """SELECT * FROM jobs WHERE tipo=? AND status IN ('PARCIAL','INTERROMPIDA','INTERROMPIDA_USUARIO')
               ORDER BY id DESC LIMIT 1""", (TIPO_ATUALIZACAO_DIARIA,),
        ).fetchone()
    else:
        row = con.execute("SELECT * FROM jobs WHERE id=? AND tipo=?", (int(job_origem_id), TIPO_ATUALIZACAO_DIARIA)).fetchone()
    con.close()
    if not row:
        raise ValueError("Nenhuma execução COLETA parcial disponível para retomada.")
    origem = dict(row)
    # Sem checkpoint (ex.: timeout no preflight), reinicia a mesma janela.
    # Com checkpoint, o coletor já retoma automaticamente a próxima página.
    ini = date.fromisoformat(origem["data_inicio"]); fim = date.fromisoformat(origem["data_fim"])
    dias = (fim - ini).days + 1
    # A chave do checkpoint é a própria janela. Criar um novo job com o mesmo fim
    # reaproveita automaticamente modalidades CONCLUÍDAS e a próxima página das pendentes.
    return iniciar_atualizacao_diaria(dias, fim.isoformat(), db_compras, db_jobs, log_dir, modo="retomada")


def criar_job_reconciliar_staging_pncp(
    db_compras: Path = DB_COMPRAS_PADRAO,
    db_jobs: Path = DB_JOBS_PADRAO,
    log_dir: Path = LOG_DIR_PADRAO,
) -> int:
    preparar_banco_jobs(db_jobs); atualizar_jobs_interrompidos(db_jobs)
    log_dir = Path(log_dir); log_dir.mkdir(parents=True, exist_ok=True)
    con = conectar_jobs(db_jobs)
    try:
        con.execute("BEGIN IMMEDIATE"); _garantir_sem_job_ativo(con)
        cur = con.execute(
            """INSERT INTO jobs(tipo,criado_em,status,db_compras,operacao,progresso_atual,progresso_total,etapa)
               VALUES (?,?,'AGUARDANDO',?,'HIGIENE_RECONCILIACAO',0,1,'Preparando')""",
            (TIPO_RECONCILIAR_STAGING_PNCP, agora(), str(Path(db_compras).resolve())),
        )
        jid=int(cur.lastrowid)
        log_path=log_dir/f"job_{jid:06d}_reconciliacao_staging_pncp.log"
        con.execute("UPDATE jobs SET log_path=? WHERE id=?",(str(log_path.resolve()),jid)); con.commit(); return jid
    except Exception:
        con.rollback(); raise
    finally:
        con.close()


def iniciar_reconciliar_staging_pncp(
    db_compras: Path = DB_COMPRAS_PADRAO, db_jobs: Path = DB_JOBS_PADRAO, log_dir: Path = LOG_DIR_PADRAO
) -> int:
    jid=criar_job_reconciliar_staging_pncp(db_compras,db_jobs,log_dir); iniciar_worker(jid,db_jobs); return jid


def criar_job_validar_completude_pncp(
    limite: int = 2000,
    db_compras: Path = DB_COMPRAS_PADRAO,
    db_jobs: Path = DB_JOBS_PADRAO,
    log_dir: Path = LOG_DIR_PADRAO,
) -> int:
    if limite < 0:
        raise ValueError("limite deve ser >= 0")
    preparar_banco_jobs(db_jobs); atualizar_jobs_interrompidos(db_jobs)
    log_dir=Path(log_dir); log_dir.mkdir(parents=True,exist_ok=True)
    con=conectar_jobs(db_jobs)
    try:
        con.execute("BEGIN IMMEDIATE"); _garantir_sem_job_ativo(con)
        cur=con.execute(
            """INSERT INTO jobs(tipo,criado_em,status,db_compras,operacao,progresso_atual,progresso_total,etapa)
               VALUES (?,?,'AGUARDANDO',?,?,0,1,'Preparando')""",
            (TIPO_VALIDAR_COMPLETUDE_PNCP,agora(),str(Path(db_compras).resolve()),str(int(limite))),
        )
        jid=int(cur.lastrowid)
        log_path=log_dir/f"job_{jid:06d}_validar_completude_pncp.log"
        con.execute("UPDATE jobs SET log_path=? WHERE id=?",(str(log_path.resolve()),jid)); con.commit(); return jid
    except Exception:
        con.rollback(); raise
    finally:
        con.close()


def iniciar_validar_completude_pncp(
    limite: int = 2000, db_compras: Path = DB_COMPRAS_PADRAO,
    db_jobs: Path = DB_JOBS_PADRAO, log_dir: Path = LOG_DIR_PADRAO
) -> int:
    jid=criar_job_validar_completude_pncp(limite,db_compras,db_jobs,log_dir); iniciar_worker(jid,db_jobs); return jid

def criar_job_itens_incremental(
    limite_contratacoes: int = 1000, limite_paginas: int = 2000, seed_dias: int = 0,
    db_compras: Path = DB_COMPRAS_PADRAO, db_jobs: Path = DB_JOBS_PADRAO,
    log_dir: Path = LOG_DIR_PADRAO,
) -> int:
    if limite_contratacoes <= 0 or limite_paginas <= 0 or seed_dias < 0:
        raise ValueError("limites devem ser > 0 e seed_dias >= 0")
    preparar_banco_jobs(db_jobs); atualizar_jobs_interrompidos(db_jobs)
    log_dir=Path(log_dir); log_dir.mkdir(parents=True,exist_ok=True)
    con=conectar_jobs(db_jobs)
    try:
        con.execute("BEGIN IMMEDIATE"); _garantir_sem_job_ativo(con)
        op=f"{int(limite_contratacoes)}:{int(limite_paginas)}:{int(seed_dias)}"
        cur=con.execute(
            """INSERT INTO jobs(tipo,criado_em,status,db_compras,operacao,progresso_atual,progresso_total,etapa)
               VALUES (?,?,'AGUARDANDO',?,?,0,1,'Preparando fila de itens')""",
            (TIPO_ITENS_INCREMENTAL,agora(),str(Path(db_compras).resolve()),op),
        )
        jid=int(cur.lastrowid)
        log_path=log_dir/f"job_{jid:06d}_itens_incremental.log"
        con.execute("UPDATE jobs SET log_path=? WHERE id=?",(str(log_path.resolve()),jid)); con.commit(); return jid
    except Exception:
        con.rollback(); raise
    finally:
        con.close()


def iniciar_itens_incremental(
    limite_contratacoes: int = 1000, limite_paginas: int = 2000, seed_dias: int = 0,
    db_compras: Path = DB_COMPRAS_PADRAO, db_jobs: Path = DB_JOBS_PADRAO,
    log_dir: Path = LOG_DIR_PADRAO,
) -> int:
    jid=criar_job_itens_incremental(limite_contratacoes,limite_paginas,seed_dias,db_compras,db_jobs,log_dir)
    iniciar_worker(jid,db_jobs); return jid


def criar_job_itens_mutirao(
    lote_contratacoes: int = 2000, lote_paginas: int = 4000, hora_coleta: str = "00:30",
    margem_minutos: int = 60, sem_limite_horario: bool = False,
    db_compras: Path = DB_COMPRAS_PADRAO, db_jobs: Path = DB_JOBS_PADRAO,
    log_dir: Path = LOG_DIR_PADRAO, deadline_iso: str | None = None,
) -> int:
    if lote_contratacoes <= 0 or lote_paginas <= 0 or margem_minutos < 0:
        raise ValueError("lotes devem ser > 0 e margem_minutos >= 0")
    if not sem_limite_horario:
        try:
            hh, mm = (int(x) for x in str(hora_coleta).split(":", 1))
        except Exception as exc:
            raise ValueError("hora_coleta inválida; use HH:MM") from exc
        if not (0 <= hh <= 23 and 0 <= mm <= 59):
            raise ValueError("hora_coleta inválida; use HH:MM")
    preparar_banco_jobs(db_jobs); atualizar_jobs_interrompidos(db_jobs)
    log_dir=Path(log_dir); log_dir.mkdir(parents=True, exist_ok=True)
    con=conectar_jobs(db_jobs)
    try:
        con.execute("BEGIN IMMEDIATE"); _garantir_sem_job_ativo(con)
        hora_compacta = str(hora_coleta).replace(":", "") if not sem_limite_horario else "0000"
        deadline_compacto = ""
        if deadline_iso:
            try:
                d = datetime.fromisoformat(str(deadline_iso))
                if d.tzinfo is not None:
                    d = d.astimezone().replace(tzinfo=None)
                deadline_compacto = d.strftime("%Y%m%dT%H%M%S")
            except ValueError as exc:
                raise ValueError("deadline_iso inválido; use ISO local.") from exc
        op=f"{int(lote_contratacoes)}:{int(lote_paginas)}:{hora_compacta}:{int(margem_minutos)}:{1 if sem_limite_horario else 0}:{deadline_compacto}"
        cur=con.execute(
            """INSERT INTO jobs(tipo,criado_em,status,db_compras,operacao,progresso_atual,progresso_total,etapa)
               VALUES (?,?,'AGUARDANDO',?,?,0,1,'Preparando mutirão de itens')""",
            (TIPO_ITENS_MUTIRAO, agora(), str(Path(db_compras).resolve()), op),
        )
        jid=int(cur.lastrowid)
        log_path=log_dir/f"job_{jid:06d}_mutirao_itens.log"
        con.execute("UPDATE jobs SET log_path=? WHERE id=?", (str(log_path.resolve()), jid)); con.commit(); return jid
    except Exception:
        con.rollback(); raise
    finally:
        con.close()


def iniciar_itens_mutirao(
    lote_contratacoes: int = 2000, lote_paginas: int = 4000, hora_coleta: str = "00:30",
    margem_minutos: int = 60, sem_limite_horario: bool = False,
    db_compras: Path = DB_COMPRAS_PADRAO, db_jobs: Path = DB_JOBS_PADRAO,
    log_dir: Path = LOG_DIR_PADRAO, deadline_iso: str | None = None,
) -> int:
    jid = criar_job_itens_mutirao(
        lote_contratacoes, lote_paginas, hora_coleta, margem_minutos, sem_limite_horario,
        db_compras, db_jobs, log_dir, deadline_iso,
    )
    iniciar_worker(jid, db_jobs)
    return jid

def criar_job_itens_hibrido_piloto(
    limite: int = 500, workers: int = 6,
    db_compras: Path = DB_COMPRAS_PADRAO, db_jobs: Path = DB_JOBS_PADRAO,
    log_dir: Path = LOG_DIR_PADRAO,
) -> int:
    """Cria o piloto Compras.gov-first sem alterar a semântica de itens_coletados."""
    if limite <= 0:
        raise ValueError("limite deve ser > 0")
    if not 1 <= int(workers) <= 12:
        raise ValueError("workers deve ficar entre 1 e 12")
    preparar_banco_jobs(db_jobs); atualizar_jobs_interrompidos(db_jobs)
    log_dir = Path(log_dir); log_dir.mkdir(parents=True, exist_ok=True)
    con = conectar_jobs(db_jobs)
    try:
        con.execute("BEGIN IMMEDIATE"); _garantir_sem_job_ativo(con)
        op = f"{int(limite)}:{int(workers)}"
        cur = con.execute(
            """INSERT INTO jobs(tipo,criado_em,status,db_compras,operacao,progresso_atual,progresso_total,etapa)
               VALUES (?,?,'AGUARDANDO',?,?,0,?,'Preparando piloto híbrido')""",
            (TIPO_ITENS_HIBRIDO_PILOTO, agora(), str(Path(db_compras).resolve()), op, int(limite)),
        )
        jid = int(cur.lastrowid)
        log_path = log_dir / f"job_{jid:06d}_itens_hibrido_piloto.log"
        con.execute("UPDATE jobs SET log_path=? WHERE id=?", (str(log_path.resolve()), jid))
        con.commit(); return jid
    except Exception:
        con.rollback(); raise
    finally:
        con.close()


def iniciar_itens_hibrido_piloto(
    limite: int = 500, workers: int = 6,
    db_compras: Path = DB_COMPRAS_PADRAO, db_jobs: Path = DB_JOBS_PADRAO,
    log_dir: Path = LOG_DIR_PADRAO,
) -> int:
    jid = criar_job_itens_hibrido_piloto(limite, workers, db_compras, db_jobs, log_dir)
    iniciar_worker(jid, db_jobs)
    return jid


def criar_job_itens_hibrido(
    limite: int = 5000, workers: int = 6, rpm: int = 95,
    db_compras: Path = DB_COMPRAS_PADRAO, db_jobs: Path = DB_JOBS_PADRAO,
    log_dir: Path = LOG_DIR_PADRAO, deadline_iso: str | None = None,
) -> int:
    """Cria um lote de produção da rota rápida Compras.gov-first."""
    if limite <= 0:
        raise ValueError("limite deve ser > 0")
    if not 1 <= int(workers) <= 12:
        raise ValueError("workers deve ficar entre 1 e 12")
    if not 30 <= int(rpm) <= 120:
        raise ValueError("rpm deve ficar entre 30 e 120")
    deadline_compacto = ""
    if deadline_iso:
        try:
            d = datetime.fromisoformat(str(deadline_iso))
            if d.tzinfo is not None:
                d = d.astimezone().replace(tzinfo=None)
            deadline_compacto = d.strftime("%Y%m%dT%H%M%S")
        except ValueError as exc:
            raise ValueError("deadline_iso inválido; use ISO local.") from exc
    preparar_banco_jobs(db_jobs); atualizar_jobs_interrompidos(db_jobs)
    log_dir = Path(log_dir); log_dir.mkdir(parents=True, exist_ok=True)
    con = conectar_jobs(db_jobs)
    try:
        con.execute("BEGIN IMMEDIATE"); _garantir_sem_job_ativo(con)
        op = f"{int(limite)}:{int(workers)}:{int(rpm)}:{deadline_compacto}"
        cur = con.execute(
            """INSERT INTO jobs(tipo,criado_em,status,db_compras,operacao,progresso_atual,progresso_total,etapa)
               VALUES (?,?,'AGUARDANDO',?,?,0,?,'Preparando mutirão híbrido')""",
            (TIPO_ITENS_HIBRIDO, agora(), str(Path(db_compras).resolve()), op, int(limite)),
        )
        jid = int(cur.lastrowid)
        log_path = log_dir / f"job_{jid:06d}_itens_hibrido.log"
        con.execute("UPDATE jobs SET log_path=? WHERE id=?", (str(log_path.resolve()), jid))
        con.commit(); return jid
    except Exception:
        con.rollback(); raise
    finally:
        con.close()


def iniciar_itens_hibrido(
    limite: int = 5000, workers: int = 6, rpm: int = 95,
    db_compras: Path = DB_COMPRAS_PADRAO, db_jobs: Path = DB_JOBS_PADRAO,
    log_dir: Path = LOG_DIR_PADRAO, deadline_iso: str | None = None,
) -> int:
    jid = criar_job_itens_hibrido(limite, workers, rpm, db_compras, db_jobs, log_dir, deadline_iso)
    iniciar_worker(jid, db_jobs)
    return jid

def criar_job_resultados_pncp(db_compras=DB_COMPRAS_PADRAO, db_jobs=DB_JOBS_PADRAO,
                             log_dir=LOG_DIR_PADRAO, deadline_iso=None):
    preparar_banco_jobs(db_jobs); atualizar_jobs_interrompidos(db_jobs)
    if deadline_iso: datetime.fromisoformat(deadline_iso)
    Path(log_dir).mkdir(parents=True,exist_ok=True)
    con=conectar_jobs(db_jobs)
    try:
        con.execute('BEGIN IMMEDIATE'); _garantir_sem_job_ativo(con)
        cur=con.execute('''INSERT INTO jobs(tipo,criado_em,status,db_compras,deadline_fts,etapa)
          VALUES(?,?,'AGUARDANDO',?,?,'Preparando resultados PNCP')''',
          (TIPO_RESULTADOS_PNCP,agora(),str(Path(db_compras).resolve()),deadline_iso))
        jid=cur.lastrowid
        con.execute('UPDATE jobs SET log_path=? WHERE id=?',
                    (str((Path(log_dir)/f'job_{jid:06d}_resultados_pncp.log').resolve()),jid))
        con.commit(); return jid
    except Exception:
        con.rollback(); raise
    finally: con.close()


def iniciar_resultados_pncp(db_compras=DB_COMPRAS_PADRAO,db_jobs=DB_JOBS_PADRAO,
                           log_dir=LOG_DIR_PADRAO,deadline_iso=None):
    jid=criar_job_resultados_pncp(db_compras,db_jobs,log_dir,deadline_iso)
    iniciar_worker(jid,db_jobs)
    return jid


def atualizar_job_csv_mes(
    job_id: int,
    ano_mes: str,
    db_path: Path = DB_JOBS_PADRAO,
    **campos: Any,
) -> None:
    permitidos = {
        'status', 'etapa', 'progresso_atual', 'progresso_total',
        'unidade_progresso', 'iniciado_em', 'finalizado_em', 'mensagem'
    }
    invalidos = set(campos) - permitidos
    if invalidos:
        raise ValueError(f"Campos inválidos de job_csv_meses: {sorted(invalidos)}")
    if not campos:
        return
    partes = ', '.join(f"{k}=?" for k in campos)
    valores = list(campos.values()) + [job_id, ano_mes]
    con = conectar_jobs(db_path)
    con.execute(f"UPDATE job_csv_meses SET {partes} WHERE job_id=? AND ano_mes=?", valores)
    con.commit(); con.close()


def obter_resumo_job_csv(
    job_id: int,
    db_path: Path = DB_JOBS_PADRAO,
) -> dict[str, Any]:
    preparar_banco_jobs(db_path)
    con = conectar_jobs(db_path)
    rows = con.execute(
        "SELECT * FROM job_csv_meses WHERE job_id=? ORDER BY ano_mes", (job_id,)
    ).fetchall()
    con.close()
    meses = [dict(r) for r in rows]
    concluidos = sum(1 for r in meses if r['status'] in ('CONCLUIDA','JA_EXISTIA'))
    erros = sum(1 for r in meses if r['status'] in ('ERRO','INTERROMPIDA','PARCIAL'))
    return {
        'total': len(meses), 'concluidos': concluidos, 'erros': erros,
        'pendentes': max(0, len(meses)-concluidos), 'meses': meses,
    }


def criar_job_reprocessar_incompletos(
    data_inicio: str,
    data_fim: str,
    db_compras: Path = DB_COMPRAS_PADRAO,
    db_jobs: Path = DB_JOBS_PADRAO,
    log_dir: Path = LOG_DIR_PADRAO,
) -> int:
    datas = datas_incompletas_periodo(db_compras, data_inicio, data_fim)
    if not datas:
        raise RuntimeError("Não há dias incompletos no período selecionado.")
    preparar_banco_jobs(db_jobs); atualizar_jobs_interrompidos(db_jobs)
    con = conectar_jobs(db_jobs)
    try:
        con.execute("BEGIN IMMEDIATE"); _garantir_sem_job_ativo(con)
        cur = con.execute(
            """INSERT INTO jobs(tipo,criado_em,status,db_compras,data_inicio,data_fim,operacao)
               VALUES (?,?,'AGUARDANDO',?,?,?,'INCOMPLETOS')""",
            (TIPO_CARGA_HISTORICA, agora(), str(Path(db_compras).resolve()), data_inicio, data_fim),
        )
        job_id=int(cur.lastrowid)
        con.executemany(
            "INSERT INTO job_dias(job_id,data_referencia,status) VALUES (?,?,'PENDENTE')",
            [(job_id,d) for d in datas],
        )
        log_path=Path(log_dir)/f"job_{job_id:06d}_incompletos_{data_inicio}_a_{data_fim}.log"
        con.execute("UPDATE jobs SET log_path=? WHERE id=?",(str(log_path.resolve()),job_id))
        con.commit(); return job_id
    except Exception:
        con.rollback(); raise
    finally:
        con.close()


def iniciar_reprocessamento_incompletos(
    data_inicio: str,
    data_fim: str,
    db_compras: Path = DB_COMPRAS_PADRAO,
    db_jobs: Path = DB_JOBS_PADRAO,
    log_dir: Path = LOG_DIR_PADRAO,
) -> int:
    job_id=criar_job_reprocessar_incompletos(data_inicio,data_fim,db_compras,db_jobs,log_dir)
    iniciar_worker(job_id,db_jobs); return job_id


def criar_job_indice_fts(
    modo: str,
    db_compras: Path = DB_COMPRAS_PADRAO,
    db_fts: Path = DB_FTS,
    db_jobs: Path = DB_JOBS_PADRAO,
    log_dir: Path = LOG_DIR_PADRAO,
) -> int:
    if modo not in ("build", "update"):
        raise ValueError("Modo do índice deve ser 'build' ou 'update'.")

    preparar_banco_jobs(db_jobs)
    atualizar_jobs_interrompidos(db_jobs)
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    con = conectar_jobs(db_jobs)
    try:
        con.execute("BEGIN IMMEDIATE")
        _garantir_sem_job_ativo(con)
        cur = con.execute(
            """
            INSERT INTO jobs (
                tipo, criado_em, status, db_compras,
                operacao, artefato_path, progresso_atual, progresso_total, etapa
            )
            VALUES (?, ?, 'AGUARDANDO', ?, ?, ?, 0, 0, 'Preparando')
            """,
            (
                TIPO_INDICE_FTS,
                agora(),
                str(Path(db_compras).resolve()),
                modo,
                str(Path(db_fts).resolve()),
            ),
        )
        job_id = int(cur.lastrowid)
        log_path = log_dir / f"job_{job_id:06d}_fts_{modo}.log"
        con.execute(
            "UPDATE jobs SET log_path = ? WHERE id = ?",
            (str(log_path.resolve()), job_id),
        )
        con.commit()
        return job_id
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def iniciar_indice_fts(
    modo: str,
    db_compras: Path = DB_COMPRAS_PADRAO,
    db_fts: Path = DB_FTS,
    db_jobs: Path = DB_JOBS_PADRAO,
    log_dir: Path = LOG_DIR_PADRAO,
    deadline_iso: str | None = None,
) -> int:
    job_id = criar_job_indice_fts(
        modo=modo,
        db_compras=db_compras,
        db_fts=db_fts,
        db_jobs=db_jobs,
        log_dir=log_dir,
    )
    if deadline_iso:
        atualizar_job(job_id, db_jobs, deadline_fts=deadline_iso)
    iniciar_worker(job_id, db_jobs)
    return job_id



def criar_job_classificacao_tic(
    modo: str,
    db_compras: Path = DB_COMPRAS_PADRAO,
    db_tic: Path = DB_TIC,
    db_jobs: Path = DB_JOBS_PADRAO,
    log_dir: Path = LOG_DIR_PADRAO,
) -> int:
    if modo not in ("build", "update"):
        raise ValueError("Modo deve ser build ou update.")
    preparar_banco_jobs(db_jobs); atualizar_jobs_interrompidos(db_jobs)
    con=conectar_jobs(db_jobs)
    try:
        con.execute("BEGIN IMMEDIATE"); _garantir_sem_job_ativo(con)
        cur=con.execute(
            """INSERT INTO jobs(tipo,criado_em,status,db_compras,operacao,artefato_path,
                       progresso_atual,progresso_total,etapa)
               VALUES (?,?,'AGUARDANDO',?,?,?,0,0,'Preparando')""",
            (TIPO_CLASSIFICACAO_TIC, agora(), str(Path(db_compras).resolve()), modo, str(Path(db_tic).resolve())),
        )
        jid=int(cur.lastrowid)
        log_path=Path(log_dir)/f"job_{jid:06d}_tic_{modo}.log"
        con.execute("UPDATE jobs SET log_path=? WHERE id=?",(str(log_path.resolve()),jid)); con.commit(); return jid
    except Exception:
        con.rollback(); raise
    finally: con.close()


def iniciar_classificacao_tic(
    modo: str,
    db_compras: Path = DB_COMPRAS_PADRAO,
    db_tic: Path = DB_TIC,
    db_jobs: Path = DB_JOBS_PADRAO,
    log_dir: Path = LOG_DIR_PADRAO,
) -> int:
    jid=criar_job_classificacao_tic(modo,db_compras,db_tic,db_jobs,log_dir)
    iniciar_worker(jid,db_jobs); return jid


def atualizar_progresso_job(
    job_id: int,
    etapa: str,
    atual: int,
    total: int,
    db_path: Path = DB_JOBS_PADRAO,
) -> None:
    con = conectar_jobs(db_path)
    con.execute(
        """
        UPDATE jobs
        SET etapa = ?, progresso_atual = ?, progresso_total = ?,
            mensagem = ?
        WHERE id = ?
        """,
        (
            etapa,
            int(atual),
            int(total),
            f"Índice FTS5: {etapa} — {int(atual)}/{int(total)}",
            job_id,
        ),
    )
    con.commit()
    con.close()


def obter_metricas_job_historico(
    job_id: int,
    db_path: Path = DB_JOBS_PADRAO,
) -> dict[str, Any]:
    """Métricas baratas baseadas apenas em job_dias, sem varrer compras.db."""
    preparar_banco_jobs(db_path)
    con = conectar_jobs(db_path)

    rows = con.execute(
        """
        SELECT data_referencia, status, iniciado_em, finalizado_em
        FROM job_dias
        WHERE job_id = ?
          AND status IN ('CONCLUIDA', 'CONCLUIDA_COM_PENDENCIAS_EXTERNAS')
          AND iniciado_em IS NOT NULL
          AND finalizado_em IS NOT NULL
        ORDER BY data_referencia
        """,
        (job_id,),
    ).fetchall()

    resumo = obter_resumo_job_historico(job_id, db_path)

    duracoes = []
    for r in rows:
        try:
            ini = datetime.fromisoformat(r["iniciado_em"])
            fim = datetime.fromisoformat(r["finalizado_em"])
            seg = (fim - ini).total_seconds()
            if seg > 1:
                duracoes.append(seg)
        except Exception:
            pass

    media = sum(duracoes) / len(duracoes) if duracoes else None
    recentes = duracoes[-7:]
    media_recente = sum(recentes) / len(recentes) if recentes else media
    restantes = max(0, resumo["total"] - resumo["resolvidos"])
    estimativa = media_recente * restantes if media_recente else None
    dias_hora = 3600 / media_recente if media_recente and media_recente > 0 else None

    con.close()
    return {
        "media_segundos": media,
        "media_recente_segundos": media_recente,
        "dias_hora": dias_hora,
        "estimativa_restante_segundos": estimativa,
        "amostra_dias": len(duracoes),
        "dias_restantes": restantes,
    }

def _flags_worker_destacado() -> dict[str, Any]:
    if os.name == "nt":
        # CREATE_NO_WINDOW evita a janela preta vazia do python.exe observada
        # nas versões anteriores, mantendo o worker independente do Streamlit.
        return {
            "creationflags": (
                subprocess.CREATE_NEW_PROCESS_GROUP
                | getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000)
            )
        }
    return {"start_new_session": True}


def iniciar_worker(
    job_id: int,
    db_jobs: Path = DB_JOBS_PADRAO,
) -> int:
    preparar_banco_jobs(db_jobs)
    comando = [
        sys.executable,
        "-u",
        str(BASE_DIR / "job_worker.py"),
        "--job-id",
        str(job_id),
        "--jobs-db",
        str(Path(db_jobs).resolve()),
    ]
    try:
        proc = subprocess.Popen(
            comando,
            cwd=str(BASE_DIR),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=(os.name != "nt"),
            **_flags_worker_destacado(),
        )
    except Exception as exc:
        con = conectar_jobs(db_jobs)
        con.execute(
            """
            UPDATE jobs
            SET status = 'ERRO',
                finalizado_em = ?,
                mensagem = ?
            WHERE id = ?
            """,
            (agora(), f"Falha ao iniciar worker: {exc}", job_id),
        )
        con.commit()
        con.close()
        raise

    con = conectar_jobs(db_jobs)
    con.execute(
        "UPDATE jobs SET worker_pid = ? WHERE id = ?",
        (proc.pid, job_id),
    )
    con.commit()
    con.close()
    return int(proc.pid)


def iniciar_coleta_diaria(
    data_referencia: str,
    db_compras: Path = DB_COMPRAS_PADRAO,
    db_jobs: Path = DB_JOBS_PADRAO,
    log_dir: Path = LOG_DIR_PADRAO,
) -> int:
    job_id = criar_job_coleta_diaria(
        data_referencia=data_referencia,
        db_compras=db_compras,
        db_jobs=db_jobs,
        log_dir=log_dir,
    )
    iniciar_worker(job_id, db_jobs)
    return job_id


def iniciar_carga_historica(
    data_inicio: str,
    data_fim: str,
    db_compras: Path = DB_COMPRAS_PADRAO,
    db_jobs: Path = DB_JOBS_PADRAO,
    log_dir: Path = LOG_DIR_PADRAO,
) -> int:
    job_id = criar_job_carga_historica(
        data_inicio=data_inicio,
        data_fim=data_fim,
        db_compras=db_compras,
        db_jobs=db_jobs,
        log_dir=log_dir,
    )
    iniciar_worker(job_id, db_jobs)
    return job_id


def solicitar_parada(
    job_id: int,
    db_path: Path = DB_JOBS_PADRAO,
) -> bool:
    preparar_banco_jobs(db_path)
    con = conectar_jobs(db_path)
    cur = con.execute(
        f"""
        UPDATE jobs
        SET stop_requested = 1,
            status = CASE
                WHEN status IN ({','.join('?' for _ in STATUS_ATIVOS)})
                THEN 'PARADA_SOLICITADA'
                ELSE status
            END,
            mensagem = 'Parada solicitada pelo usuário.'
        WHERE id = ?
          AND status IN ({','.join('?' for _ in STATUS_ATIVOS)})
        """,
        (*STATUS_ATIVOS, job_id, *STATUS_ATIVOS),
    )
    con.commit()
    alterou = cur.rowcount > 0
    con.close()
    return alterou


def atualizar_job(
    job_id: int,
    db_path: Path = DB_JOBS_PADRAO,
    **campos: Any,
) -> None:
    if not campos:
        return
    permitidos = {
        "iniciado_em",
        "finalizado_em",
        "status",
        "worker_pid",
        "process_pid",
        "exit_code",
        "stop_requested",
        "log_path",
        "mensagem",
        "data_atual",
        "operacao",
        "artefato_path",
        "deadline_fts",
        "progresso_atual",
        "progresso_total",
        "etapa",
    }
    invalidos = set(campos) - permitidos
    if invalidos:
        raise ValueError(f"Campos inválidos: {sorted(invalidos)}")

    partes = ", ".join(f"{k} = ?" for k in campos)
    valores = list(campos.values()) + [job_id]
    con = conectar_jobs(db_path)
    con.execute(f"UPDATE jobs SET {partes} WHERE id = ?", valores)
    con.commit()
    con.close()


def atualizar_job_dia(
    job_id: int,
    data_referencia: str,
    db_path: Path = DB_JOBS_PADRAO,
    **campos: Any,
) -> None:
    permitidos = {
        "status",
        "iniciado_em",
        "finalizado_em",
        "exit_code",
        "mensagem",
    }
    invalidos = set(campos) - permitidos
    if invalidos:
        raise ValueError(f"Campos inválidos de job_dias: {sorted(invalidos)}")
    if not campos:
        return
    partes = ", ".join(f"{k} = ?" for k in campos)
    valores = list(campos.values()) + [job_id, data_referencia]
    con = conectar_jobs(db_path)
    con.execute(
        f"""
        UPDATE job_dias
        SET {partes}
        WHERE job_id = ?
          AND data_referencia = ?
        """,
        valores,
    )
    con.commit()
    con.close()


def parada_solicitada(
    job_id: int,
    db_path: Path = DB_JOBS_PADRAO,
) -> bool:
    con = conectar_jobs(db_path)
    row = con.execute(
        "SELECT stop_requested FROM jobs WHERE id = ?",
        (job_id,),
    ).fetchone()
    con.close()
    return bool(row and row["stop_requested"])


def obter_progresso_data(
    db_compras: Path,
    data_referencia: str,
) -> dict[str, int]:
    """Lê o progresso de uma data diretamente do banco principal."""
    db_compras = Path(db_compras)
    vazio = {
        "modalidades_concluidas": 0,
        "modalidades_total": 14,
        "contratacoes": 0,
        "com_itens": 0,
        "indisponiveis": 0,
        "pendentes": 0,
        "itens_armazenados": 0,
    }
    if not db_compras.exists():
        return vazio.copy()

    con = sqlite3.connect(str(db_compras), timeout=10)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout = 10000")

    tabelas = {
        r["name"]
        for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    if "contratacoes" not in tabelas:
        con.close()
        return vazio.copy()

    cols = {
        r["name"]
        for r in con.execute("PRAGMA table_info(contratacoes)").fetchall()
    }
    status_expr = (
        "COALESCE(itens_status, CASE WHEN itens_coletados=1 THEN 'OK' ELSE 'PENDENTE' END)"
        if "itens_status" in cols
        else "CASE WHEN itens_coletados=1 THEN 'OK' ELSE 'PENDENTE' END"
    )

    row = con.execute(
        f"""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN itens_coletados = 1 THEN 1 ELSE 0 END) AS com_itens,
            SUM(CASE WHEN {status_expr} = 'INDISPONIVEL_PNCP' THEN 1 ELSE 0 END)
                AS indisponiveis,
            SUM(
                CASE
                    WHEN itens_coletados = 0
                     AND {status_expr} <> 'INDISPONIVEL_PNCP'
                    THEN 1 ELSE 0
                END
            ) AS pendentes,
            COALESCE(
                SUM(CASE WHEN itens_coletados = 1 THEN qtd_itens ELSE 0 END),
                0
            ) AS itens_armazenados
        FROM contratacoes
        WHERE data_publicacao_pncp >= ?
          AND data_publicacao_pncp < ?
        """,
        (
            data_referencia,
            (date.fromisoformat(data_referencia) + timedelta(days=1)).isoformat(),
        ),
    ).fetchone()

    modalidades = 0
    if "coletas_modalidades" in tabelas:
        compacta = data_referencia.replace("-", "")
        modalidades = int(
            con.execute(
                """
                SELECT COUNT(*) AS qtd
                FROM coletas_modalidades
                WHERE data_referencia = ?
                  AND status = 'CONCLUIDA'
                """,
                (compacta,),
            ).fetchone()["qtd"]
            or 0
        )

    con.close()
    return {
        "modalidades_concluidas": modalidades,
        "modalidades_total": 14,
        "contratacoes": int(row["total"] or 0),
        "com_itens": int(row["com_itens"] or 0),
        "indisponiveis": int(row["indisponiveis"] or 0),
        "pendentes": int(row["pendentes"] or 0),
        "itens_armazenados": int(row["itens_armazenados"] or 0),
    }


def data_esta_completa(db_compras: Path, data_referencia: str) -> bool:
    p = obter_progresso_data(db_compras, data_referencia)
    return p["modalidades_concluidas"] == 14 and p["pendentes"] == 0


def obter_resumo_job_historico(
    job_id: int,
    db_path: Path = DB_JOBS_PADRAO,
) -> dict[str, Any]:
    preparar_banco_jobs(db_path)
    con = conectar_jobs(db_path)
    rows = con.execute(
        """
        SELECT status, COUNT(*) AS qtd
        FROM job_dias
        WHERE job_id = ?
        GROUP BY status
        """,
        (job_id,),
    ).fetchall()
    contagens = {r["status"]: int(r["qtd"]) for r in rows}

    total = sum(contagens.values())
    resolvidos = sum(contagens.get(s, 0) for s in STATUS_DIA_RESOLVIDO)
    parciais = (
        contagens.get("PARCIAL", 0)
        + contagens.get("ERRO", 0)
        + contagens.get("INTERROMPIDA", 0)
    )
    executando = contagens.get("EXECUTANDO", 0)
    pendentes = contagens.get("PENDENTE", 0)

    ultimos = con.execute(
        """
        SELECT data_referencia, status, exit_code, mensagem
        FROM job_dias
        WHERE job_id = ?
          AND status <> 'PENDENTE'
        ORDER BY data_referencia DESC
        LIMIT 12
        """,
        (job_id,),
    ).fetchall()
    con.close()

    return {
        "total": total,
        "resolvidos": resolvidos,
        "parciais": parciais,
        "executando": executando,
        "pendentes": pendentes,
        "contagens": contagens,
        "ultimos": [dict(r) for r in ultimos],
    }


def obter_resumo_periodo_banco(
    db_compras: Path,
    data_inicio: str,
    data_fim: str,
    db_jobs: Path | None = None,
) -> dict[str, int]:
    """Resume cobertura sem confundir presença de dados com varredura PNCP integral."""
    datas = _datas_periodo(data_inicio, data_fim)
    total_dias = len(datas)
    db_compras = Path(db_compras)
    vazio = {
        "total_dias": total_dias, "dias_com_dados": 0, "dias_varredura_pncp": 0,
        "dias_com_dados_csv": 0, "dias_coleta": 0, "dias_itens_confirmados_pncp": 0,
        "dias_completos": 0, "dias_pendentes": total_dias,
    }
    if not db_compras.exists():
        return vazio

    con = sqlite3.connect(str(db_compras), timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout=30000")
    tabelas = {r["name"] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "contratacoes" not in tabelas:
        con.close(); return vazio
    cols = {r["name"] for r in con.execute("PRAGMA table_info(contratacoes)")}
    csv_expr = (
        "CASE WHEN fonte_primeira IN ('COMPRASGOV_CSV','COMPRASGOV_API') "
        "OR fonte_ultima IN ('COMPRASGOV_CSV','COMPRASGOV_API') OR csv_compra_mes IS NOT NULL THEN 1 ELSE 0 END"
        if {"fonte_primeira","fonte_ultima","csv_compra_mes"}.issubset(cols) else "0"
    )
    itens_pncp_expr = (
        "CASE WHEN COALESCE(itens_coletados,0)=1 AND COALESCE(itens_fonte,'')='PNCP_API' THEN 1 ELSE 0 END"
        if "itens_fonte" in cols else "CASE WHEN COALESCE(itens_coletados,0)=1 THEN 1 ELSE 0 END"
    )
    rows = con.execute(
        f"""SELECT substr(data_publicacao_pncp,1,10) dia, COUNT(*) total,
                   SUM({csv_expr}) csv_qtd, SUM({itens_pncp_expr}) itens_pncp_qtd,
                   SUM(CASE WHEN COALESCE(itens_coletados,0)=0 THEN 1 ELSE 0 END) pendentes
              FROM contratacoes
             WHERE data_publicacao_pncp>=? AND data_publicacao_pncp<?
             GROUP BY substr(data_publicacao_pncp,1,10)""",
        (data_inicio, (date.fromisoformat(data_fim)+timedelta(days=1)).isoformat()),
    ).fetchall()
    info = {str(r["dia"]): dict(r) for r in rows if r["dia"]}
    dias_com_dados = len(info)
    dias_csv = sum(1 for r in info.values() if int(r.get("csv_qtd") or 0)>0)
    dias_itens = sum(1 for r in info.values() if int(r.get("total") or 0)>0 and int(r.get("itens_pncp_qtd") or 0)==int(r.get("total") or 0))

    dias_modalidades: set[str] = set()
    if "coletas_modalidades" in tabelas:
        for r in con.execute(
            """SELECT data_referencia FROM coletas_modalidades
               WHERE data_referencia BETWEEN ? AND ? AND status='CONCLUIDA'
               GROUP BY data_referencia HAVING COUNT(DISTINCT modalidade_id)>=14""",
            (data_inicio.replace('-',''), data_fim.replace('-','')),
        ).fetchall():
            raw=str(r[0])
            if len(raw)==8: raw=f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
            dias_modalidades.add(raw)
    con.close()
    # Mantém a antiga semântica de 'completo': 14 modalidades + zero item pendente.
    dias_varredura = sum(1 for d in dias_modalidades if int(info.get(d, {}).get("pendentes") or 0)==0)

    dias_coleta_set: set[str] = set()
    if db_jobs is not None and Path(db_jobs).exists():
        preparar_banco_jobs(db_jobs)
        cj = conectar_jobs(db_jobs)
        rr = cj.execute(
            """SELECT data_inicio,data_fim FROM jobs
               WHERE tipo IN (?,?) AND status IN ('CONCLUIDA','CONCLUIDA_COM_PENDENCIAS_EXTERNAS')
                 AND data_inicio IS NOT NULL AND data_fim IS NOT NULL
                 AND data_fim>=? AND data_inicio<=?""",
            (TIPO_COLETA_MULTIFONTE_INCREMENTAL, TIPO_ATUALIZACAO_DIARIA, data_inicio, data_fim),
        ).fetchall()
        cj.close()
        limite_ini=date.fromisoformat(data_inicio); limite_fim=date.fromisoformat(data_fim)
        for r in rr:
            a=max(limite_ini,date.fromisoformat(r[0])); b=min(limite_fim,date.fromisoformat(r[1]))
            while a<=b:
                dias_coleta_set.add(a.isoformat()); a += timedelta(days=1)

    return {
        "total_dias": total_dias,
        "dias_com_dados": dias_com_dados,
        "dias_varredura_pncp": dias_varredura,
        "dias_com_dados_csv": dias_csv,
        "dias_coleta": len(dias_coleta_set),
        "dias_itens_confirmados_pncp": dias_itens,
        "dias_completos": dias_varredura,
        "dias_pendentes": total_dias - dias_varredura,
    }


def ler_log(
    log_path: str | None,
    max_bytes: int = 30000,
) -> str:
    if not log_path:
        return ""
    p = Path(log_path)
    if not p.exists():
        return ""
    try:
        with p.open("rb") as f:
            tamanho = p.stat().st_size
            f.seek(max(0, tamanho - max_bytes))
            dados = f.read()
        return dados.decode("utf-8", errors="replace")
    except OSError:
        return ""


def ultimas_linhas_log(log_path: str | None, quantidade: int = 5) -> str:
    """Retorna as últimas linhas não vazias do log para o painel compacto."""
    qtd = max(1, int(quantidade or 5))
    texto = ler_log(log_path, max_bytes=max(12000, qtd * 4000))
    linhas = [x.rstrip() for x in texto.splitlines() if x.strip()]
    if not linhas:
        return "Aguardando atividade..."
    return "\n".join(linhas[-qtd:])


def ultima_linha_log(log_path: str | None) -> str:
    return ultimas_linhas_log(log_path, 1)
