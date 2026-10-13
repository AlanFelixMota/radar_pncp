#!/usr/bin/env python3
"""
Radar PNCP V0.7.6 - Orquestrador diário.

Uso normal:
    python radar_diario.py

Sem --data, processa automaticamente ONTEM.

Data específica:
    python radar_diario.py --data 2026-08-27

Teste com limite de itens:
    python radar_diario.py --data 2026-08-27 --limite-itens 100
"""

from __future__ import annotations

import argparse
import sqlite3
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

from radar_paths import DB_COMPRAS, DB_FTS, DB_TIC, garantir_diretorios
from pncp_health import EXIT_PNCP_INDISPONIVEL


BASE_DIR = Path(__file__).resolve().parent
garantir_diretorios()
DB_PADRAO = DB_COMPRAS
TOTAL_MODALIDADES = 14


def normalizar_data(valor: str | None) -> str:
    if not valor:
        return (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    valor = valor.strip()
    for formato in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(valor, formato).strftime("%Y-%m-%d")
        except ValueError:
            pass

    raise ValueError("Data inválida. Use AAAA-MM-DD ou AAAAMMDD.")


def data_compacta(data_iso: str) -> str:
    return datetime.strptime(data_iso, "%Y-%m-%d").strftime("%Y%m%d")


def conectar(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON")
    con.execute("PRAGMA journal_mode = WAL")
    return con


def coluna_existe(con: sqlite3.Connection, tabela: str, coluna: str) -> bool:
    rows = con.execute(f"PRAGMA table_info({tabela})").fetchall()
    return any(row["name"] == coluna for row in rows)


def preparar_banco_diario(con: sqlite3.Connection) -> None:
    # Também cria o controle por modalidade para permitir --status e
    # --somente-itens em bancos migrados de versões anteriores.
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
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS execucoes_diarias (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data_referencia TEXT NOT NULL,
            inicio_em TEXT NOT NULL,
            fim_em TEXT,
            status TEXT NOT NULL,
            etapa_contratacoes_exit INTEGER,
            etapa_itens_exit INTEGER,
            total_contratacoes INTEGER NOT NULL DEFAULT 0,
            contratacoes_com_itens INTEGER NOT NULL DEFAULT 0,
            contratacoes_sem_itens INTEGER NOT NULL DEFAULT 0,
            total_itens INTEGER NOT NULL DEFAULT 0,
            modalidades_concluidas INTEGER NOT NULL DEFAULT 0,
            modalidades_pendentes INTEGER NOT NULL DEFAULT 14,
            observacao TEXT
        )
        """
    )

    # A V0.4.1 diferencia pendências reais de itens indisponíveis no PNCP.
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

    if not coluna_existe(con, "execucoes_diarias", "contratacoes_itens_indisponiveis"):
        con.execute(
            "ALTER TABLE execucoes_diarias "
            "ADD COLUMN contratacoes_itens_indisponiveis INTEGER NOT NULL DEFAULT 0"
        )

    con.execute(
        """
        UPDATE contratacoes
        SET itens_status = 'OK',
            itens_http_status = NULL,
            itens_indisponivel_em = NULL
        WHERE itens_coletados = 1
        """
    )
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
    con.execute(
        """
        UPDATE contratacoes
        SET itens_status = 'ERRO_TEMPORARIO'
        WHERE itens_coletados = 0
          AND itens_erro IS NOT NULL
          AND COALESCE(itens_status, 'PENDENTE') <> 'INDISPONIVEL_PNCP'
        """
    )

    con.commit()


def executar_etapa(comando: list[str]) -> int:
    print()
    print("Comando:", " ".join(f'"{x}"' if " " in x else x for x in comando))
    print()
    try:
        proc = subprocess.run(comando, cwd=BASE_DIR)
        return int(proc.returncode)
    except KeyboardInterrupt:
        return 130


def estatisticas(con: sqlite3.Connection, data_iso: str) -> dict[str, int]:
    row = con.execute(
        """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN itens_coletados = 1 THEN 1 ELSE 0 END) AS com_itens,
            SUM(
                CASE
                    WHEN itens_coletados = 0
                     AND COALESCE(itens_status, 'PENDENTE') <> 'INDISPONIVEL_PNCP'
                    THEN 1 ELSE 0
                END
            ) AS sem_itens,
            SUM(
                CASE WHEN itens_status = 'INDISPONIVEL_PNCP'
                     THEN 1 ELSE 0 END
            ) AS indisponiveis,
            COALESCE(SUM(CASE WHEN itens_coletados = 1 THEN qtd_itens ELSE 0 END), 0)
                AS total_itens
        FROM contratacoes
        WHERE substr(data_publicacao_pncp, 1, 10) = ?
        """,
        (data_iso,),
    ).fetchone()

    compacta = data_compacta(data_iso)
    concluidas = con.execute(
        """
        SELECT COUNT(*) AS qtd
        FROM coletas_modalidades
        WHERE data_referencia = ? AND status = 'CONCLUIDA'
        """,
        (compacta,),
    ).fetchone()["qtd"]

    total = int(row["total"] or 0)
    com_itens = int(row["com_itens"] or 0)
    sem_itens = int(row["sem_itens"] or 0)
    indisponiveis = int(row["indisponiveis"] or 0)
    total_itens = int(row["total_itens"] or 0)
    concluidas = int(concluidas or 0)

    return {
        "total": total,
        "com_itens": com_itens,
        "sem_itens": sem_itens,
        "indisponiveis": indisponiveis,
        "total_itens": total_itens,
        "modalidades_concluidas": concluidas,
        "modalidades_pendentes": max(0, TOTAL_MODALIDADES - concluidas),
    }


def imprimir_status(data_iso: str, stats: dict[str, int]) -> None:
    print()
    print("=" * 86)
    print(f"STATUS DO DIA — {data_iso}")
    print("=" * 86)
    print(
        f"Modalidades:             "
        f"{stats['modalidades_concluidas']}/{TOTAL_MODALIDADES} concluídas"
    )
    print(f"Contratações:            {stats['total']}")
    print(f"Com itens coletados:     {stats['com_itens']}")
    print(f"Indisponíveis no PNCP:   {stats['indisponiveis']}")
    print(f"Pendentes/retry:         {stats['sem_itens']}")
    print(f"Itens armazenados:       {stats['total_itens']}")
    print("=" * 86)



def atualizar_indices_auxiliares(db_path: Path) -> None:
    """Atualiza índices auxiliares já existentes; falhas não invalidam a coleta."""
    if DB_FTS.exists():
        try:
            import fts_manager as fm
            st = fm.status_indice(db_path, DB_FTS)
            if st["estado"] == "DESATUALIZADO":
                print("\
Atualizando FTS5 com novos registros...")
                fm.construir_ou_atualizar(db_path, DB_FTS, "update")
                print("FTS5 atualizado.")
        except Exception as exc:
            print(f"AVISO: não foi possível atualizar o FTS5: {exc}")
    if DB_TIC.exists():
        try:
            import tic_manager as tm
            st = tm.status_classificacao(db_path, DB_TIC)
            if st["estado"] == "DESATUALIZADA":
                print("Atualizando classificação TIC com novos registros...")
                tm.construir_ou_atualizar(db_path, DB_TIC, "update")
                print("Classificação TIC atualizada.")
        except Exception as exc:
            print(f"AVISO: não foi possível atualizar a classificação TIC: {exc}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Executa o ciclo diário completo do Radar PNCP."
    )
    parser.add_argument(
        "--data",
        help="Data a processar. Padrão: ontem.",
    )
    parser.add_argument(
        "--db",
        default=str(DB_PADRAO),
        help=f"Caminho do SQLite. Padrão: {DB_PADRAO}",
    )
    grupo = parser.add_mutually_exclusive_group()
    grupo.add_argument(
        "--somente-contratacoes",
        action="store_true",
        help="Executa apenas a etapa de contratações.",
    )
    grupo.add_argument(
        "--somente-itens",
        action="store_true",
        help="Executa apenas a etapa de itens.",
    )
    parser.add_argument(
        "--limite-itens",
        type=int,
        help="Limita quantas contratações terão itens processados nesta execução.",
    )
    parser.add_argument(
        "--forcar-contratacoes",
        action="store_true",
        help="Reconsulta inclusive modalidades já concluídas.",
    )
    parser.add_argument(
        "--pausa-itens",
        type=float,
        default=0.05,
        help="Pausa-base adaptativa na etapa de itens. Padrão: 0.05 s.",
    )
    parser.add_argument(
        "--timeout-itens",
        type=int,
        default=90,
        help="Timeout por requisição de itens. Padrão: 90 s.",
    )
    parser.add_argument(
        "--workers-itens", type=int, default=3,
        help="Workers simultâneos para itens. Padrão: 3.",
    )
    parser.add_argument(
        "--atualizar-indices", action="store_true",
        help="Ao final, atualiza FTS5 e classificação TIC se já existirem.",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Apenas mostra o estado atual da data e não consulta o PNCP.",
    )
    args = parser.parse_args()

    if args.limite_itens is not None and args.limite_itens <= 0:
        print("ERRO: --limite-itens deve ser maior que zero.", file=sys.stderr)
        return 2

    try:
        data_iso = normalizar_data(args.data)
    except ValueError as e:
        print(f"ERRO: {e}", file=sys.stderr)
        return 2

    db_path = Path(args.db).resolve()

    # --status pode ser usado somente depois que o banco existir.
    if args.status and not db_path.exists():
        print(f"ERRO: banco não encontrado: {db_path}", file=sys.stderr)
        return 2

    if not db_path.exists():
        db_path.parent.mkdir(parents=True, exist_ok=True)

    # O coletor de contratações cria a estrutura base. Se estivermos em
    # --somente-itens, exigimos que ela já exista.
    if args.somente_itens and not db_path.exists():
        print(
            "ERRO: --somente-itens exige um banco já criado pela etapa de contratações.",
            file=sys.stderr,
        )
        return 2

    if args.status:
        con = conectar(db_path)
        preparar_banco_diario(con)
        try:
            stats = estatisticas(con, data_iso)
        except sqlite3.OperationalError as e:
            print(f"ERRO ao ler o banco: {e}", file=sys.stderr)
            con.close()
            return 2
        imprimir_status(data_iso, stats)
        con.close()
        return 0

    inicio = datetime.now().isoformat(timespec="seconds")
    etapa_contratacoes_exit: int | None = None
    etapa_itens_exit: int | None = None
    execucao_id: int | None = None

    print("=" * 86)
    print("Radar PNCP V0.7.6 - RADAR DIÁRIO")
    print("=" * 86)
    print(f"Data processada: {data_iso}")
    print(f"Banco:           {db_path}")
    print("Sem --data, este comando processa automaticamente o dia anterior.")
    print("=" * 86)

    # Etapa 1
    if not args.somente_itens:
        print()
        print("#" * 86)
        print("ETAPA 1/2 — CONTRATAÇÕES")
        print("#" * 86)
        cmd = [
            sys.executable,
            "-u",
            str(BASE_DIR / "coletar_pncp.py"),
            "--data",
            data_iso,
            "--db",
            str(db_path),
            "--sem-itens",
        ]
        if args.forcar_contratacoes:
            cmd.append("--forcar")

        etapa_contratacoes_exit = executar_etapa(cmd)

        if etapa_contratacoes_exit == 130:
            print("Radar diário interrompido durante a coleta de contratações.")
            return 130
        if etapa_contratacoes_exit == EXIT_PNCP_INDISPONIVEL:
            print("PNCP indisponível durante a coleta de contratações.")
            return EXIT_PNCP_INDISPONIVEL

    # Neste ponto o banco deve existir e ter a estrutura base.
    if not db_path.exists():
        print(f"ERRO: banco não foi criado em {db_path}", file=sys.stderr)
        return 2

    con = conectar(db_path)
    preparar_banco_diario(con)
    cur = con.execute(
        """
        INSERT INTO execucoes_diarias (
            data_referencia, inicio_em, status, etapa_contratacoes_exit
        )
        VALUES (?, ?, 'EM_ANDAMENTO', ?)
        """,
        (data_iso, inicio, etapa_contratacoes_exit),
    )
    execucao_id = int(cur.lastrowid)
    con.commit()
    con.close()

    # Etapa 2
    if not args.somente_contratacoes:
        print()
        print("#" * 86)
        print("ETAPA 2/2 — ITENS")
        print("#" * 86)
        cmd = [
            sys.executable,
            "-u",
            str(BASE_DIR / "coletar_itens.py"),
            "--data",
            data_iso,
            "--db",
            str(db_path),
            "--pausa",
            str(args.pausa_itens),
            "--timeout",
            str(args.timeout_itens),
            "--workers",
            str(args.workers_itens),
        ]
        if args.limite_itens is not None:
            cmd.extend(["--limite", str(args.limite_itens)])

        etapa_itens_exit = executar_etapa(cmd)

        if etapa_itens_exit == EXIT_PNCP_INDISPONIVEL:
            con = conectar(db_path)
            stats = estatisticas(con, data_iso)
            con.execute(
                """UPDATE execucoes_diarias SET fim_em=?, status='AGUARDANDO_PNCP',
                   etapa_itens_exit=?, total_contratacoes=?, contratacoes_com_itens=?,
                   contratacoes_sem_itens=?, contratacoes_itens_indisponiveis=?, total_itens=?,
                   modalidades_concluidas=?, modalidades_pendentes=?, observacao=? WHERE id=?""",
                (datetime.now().isoformat(timespec="seconds"), etapa_itens_exit,
                 stats["total"], stats["com_itens"], stats["sem_itens"], stats["indisponiveis"],
                 stats["total_itens"], stats["modalidades_concluidas"], stats["modalidades_pendentes"],
                 "PNCP aparentemente indisponível; execução será retomada depois.", execucao_id),
            )
            con.commit(); con.close()
            return EXIT_PNCP_INDISPONIVEL

        if etapa_itens_exit == 130:
            con = conectar(db_path)
            stats = estatisticas(con, data_iso)
            con.execute(
                """
                UPDATE execucoes_diarias
                SET fim_em = ?, status = 'INTERROMPIDA',
                    etapa_itens_exit = ?,
                    total_contratacoes = ?,
                    contratacoes_com_itens = ?,
                    contratacoes_sem_itens = ?,
                    contratacoes_itens_indisponiveis = ?,
                    total_itens = ?,
                    modalidades_concluidas = ?,
                    modalidades_pendentes = ?,
                    observacao = ?
                WHERE id = ?
                """,
                (
                    datetime.now().isoformat(timespec="seconds"),
                    etapa_itens_exit,
                    stats["total"],
                    stats["com_itens"],
                    stats["sem_itens"],
                    stats["indisponiveis"],
                    stats["total_itens"],
                    stats["modalidades_concluidas"],
                    stats["modalidades_pendentes"],
                    "Execução interrompida durante a etapa de itens.",
                    execucao_id,
                ),
            )
            con.commit()
            imprimir_status(data_iso, stats)
            con.close()
            return 130

    con = conectar(db_path)
    stats = estatisticas(con, data_iso)

    # O dia só é considerado completo quando todas as modalidades foram
    # coletadas e nenhuma contratação daquela data ficou sem itens.
    completo = (
        stats["modalidades_concluidas"] == TOTAL_MODALIDADES
        and stats["sem_itens"] == 0
    )

    if args.somente_contratacoes:
        status_final = "ETAPA_CONTRATACOES_CONCLUIDA"
    elif args.somente_itens:
        if stats["sem_itens"] > 0:
            status_final = "PARCIAL"
        elif stats["indisponiveis"] > 0:
            status_final = "ETAPA_ITENS_CONCLUIDA_COM_PENDENCIAS_EXTERNAS"
        else:
            status_final = "ETAPA_ITENS_CONCLUIDA"
    else:
        if not completo:
            status_final = "PARCIAL"
        elif stats["indisponiveis"] > 0:
            status_final = "CONCLUIDA_COM_PENDENCIAS_EXTERNAS"
        else:
            status_final = "CONCLUIDA"

    observacoes = []
    if etapa_contratacoes_exit not in (None, 0):
        observacoes.append(
            f"Etapa de contratações retornou código {etapa_contratacoes_exit}."
        )
    if etapa_itens_exit not in (None, 0):
        observacoes.append(
            f"Etapa de itens retornou código {etapa_itens_exit}."
        )
    if stats["modalidades_pendentes"]:
        observacoes.append(
            f"{stats['modalidades_pendentes']} modalidade(s) ainda não concluída(s)."
        )
    if stats["sem_itens"]:
        observacoes.append(
            f"{stats['sem_itens']} contratação(ões) ainda sem itens e elegível(is) para retry."
        )
    if stats["indisponiveis"]:
        observacoes.append(
            f"{stats['indisponiveis']} contratação(ões) com endpoint de itens "
            "indisponível no PNCP; não bloqueiam a conclusão diária."
        )

    con.execute(
        """
        UPDATE execucoes_diarias
        SET fim_em = ?, status = ?,
            etapa_contratacoes_exit = ?,
            etapa_itens_exit = ?,
            total_contratacoes = ?,
            contratacoes_com_itens = ?,
            contratacoes_sem_itens = ?,
            contratacoes_itens_indisponiveis = ?,
            total_itens = ?,
            modalidades_concluidas = ?,
            modalidades_pendentes = ?,
            observacao = ?
        WHERE id = ?
        """,
        (
            datetime.now().isoformat(timespec="seconds"),
            status_final,
            etapa_contratacoes_exit,
            etapa_itens_exit,
            stats["total"],
            stats["com_itens"],
            stats["sem_itens"],
            stats["indisponiveis"],
            stats["total_itens"],
            stats["modalidades_concluidas"],
            stats["modalidades_pendentes"],
            " ".join(observacoes) or None,
            execucao_id,
        ),
    )
    con.commit()

    imprimir_status(data_iso, stats)
    print(f"Status da execução:      {status_final}")
    if observacoes:
        print("Observações:")
        for obs in observacoes:
            print(f"  - {obs}")

    if not completo and not (args.somente_contratacoes or args.somente_itens):
        print()
        print(
            "Pode executar exatamente o mesmo comando novamente: "
            "modalidades concluídas e itens já coletados serão pulados."
        )
    elif stats["indisponiveis"] and not args.somente_contratacoes:
        print()
        print(
            "O dia está completo para o fluxo normal. Itens indisponíveis no PNCP "
            "podem ser rechecados futuramente com coletar_itens.py "
            "--rever-indisponiveis."
        )

    con.close()

    if args.atualizar_indices:
        atualizar_indices_auxiliares(db_path)

    # Execução parcial não é uma falha fatal: o banco permanece consistente
    # e o mesmo comando pode retomar depois. Código 1 sinaliza pendências.
    if args.somente_contratacoes:
        return 0 if etapa_contratacoes_exit in (None, 0) else 1
    if args.somente_itens:
        return 0 if stats["sem_itens"] == 0 else 1
    return 0 if completo else 1


if __name__ == "__main__":
    raise SystemExit(main())
