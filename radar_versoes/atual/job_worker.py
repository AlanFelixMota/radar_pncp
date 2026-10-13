#!/usr/bin/env python3
"""Worker destacado para jobs do Radar PNCP V0.7.6."""

from __future__ import annotations

import argparse
import os
import signal
import sqlite3
import subprocess
import sys
import time
import threading
from datetime import date, datetime
from pathlib import Path

import job_manager as jm
from interface_radar_pncp import VERSION
import radar_stats as rs
from radar_paths import DB_STATS


BASE_DIR = Path(__file__).resolve().parent


def encerrar_arvore_processos(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return

    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(proc.pid), "/T", "/F"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        return

    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        return

    try:
        proc.wait(timeout=8)
        return
    except subprocess.TimeoutExpired:
        pass

    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


_LOG_LOCK = threading.Lock()


def escrever(log, texto: str, *, timestamp: bool = True) -> None:
    """Grava no log, prefixando cada linha não vazia com horário local."""
    partes = str(texto).replace("\r\n", "\n").replace("\r", "\n").split("\n")
    # Evita criar uma linha vazia extra quando texto termina em quebra de linha.
    if partes and partes[-1] == "":
        partes = partes[:-1]
    if not partes:
        partes = [""]
    with _LOG_LOCK:
        for linha in partes:
            linha = linha.rstrip()
            if linha and timestamp:
                log.write(f"[{datetime.now().strftime('%H:%M:%S')}] {linha}\n")
            else:
                log.write(linha + "\n")
        log.flush()


def _encaminhar_saida_processo(pipe, log) -> None:
    """Drena stdout/stderr do subprocesso e acrescenta timestamp ao log."""
    try:
        for linha in pipe:
            escrever(log, linha.rstrip("\r\n"))
    finally:
        try:
            pipe.close()
        except Exception:
            pass


def comando_job(job: dict, jobs_db: Path) -> list[str]:
    if job['tipo'] == jm.TIPO_RESULTADOS_PNCP:
        return [sys.executable,'-u',str(BASE_DIR/'resultados_pncp.py'),'--db',job['db_compras'],
                '--jobs-db',str(jobs_db),'--job-id',str(job['id']),
                *(['--deadline',job['deadline_fts']] if job.get('deadline_fts') else [])]
    db_compras = Path(job["db_compras"]).resolve()

    if job["tipo"] == jm.TIPO_COLETA_DIARIA:
        return [
            sys.executable,
            "-u",
            str(BASE_DIR / "radar_diario.py"),
            "--data",
            job["data_referencia"],
            "--db",
            str(db_compras),
            "--atualizar-indices",
        ]

    if job["tipo"] == jm.TIPO_CARGA_HISTORICA:
        return [
            sys.executable,
            "-u",
            str(BASE_DIR / "coletar_historico.py"),
            "--inicio",
            job["data_inicio"],
            "--fim",
            job["data_fim"],
            "--db",
            str(db_compras),
            "--jobs-db",
            str(jobs_db),
            "--job-id",
            str(job["id"]),
        ]

    if job["tipo"] == jm.TIPO_CARGA_CSV_COMPRASGOV:
        return [
            sys.executable,
            "-u",
            str(BASE_DIR / "importar_comprasgov_csv.py"),
            "--inicio", job["data_inicio"],
            "--fim", job["data_fim"],
            "--db", str(db_compras),
            "--jobs-db", str(jobs_db),
            "--job-id", str(job["id"]),
        ]

    if job["tipo"] == jm.TIPO_CARGA_BASE_CSV_COMPRASGOV:
        return [
            sys.executable,
            "-u",
            str(BASE_DIR / "carga_base_comprasgov.py"),
            "--db", str(db_compras),
            "--jobs-db", str(jobs_db),
            "--job-id", str(job["id"]),
        ]

    if job["tipo"] == jm.TIPO_CARGA_2026_COMPRASGOV:
        return [
            sys.executable, "-u", str(BASE_DIR / "carga_2026_comprasgov.py"),
            "--db", str(db_compras), "--jobs-db", str(jobs_db), "--job-id", str(job["id"]),
        ]

    if job["tipo"] == jm.TIPO_API_COMPRASGOV_INCREMENTAL:
        cmd = [
            sys.executable, "-u", str(BASE_DIR / "comprasgov_api_incremental.py"),
            "--inicio", job["data_inicio"], "--fim", job["data_fim"],
            "--db", str(db_compras), "--jobs-db", str(jobs_db), "--job-id", str(job["id"]),
        ]
        if str(job.get("operacao") or "") == "SEM_ITENS":
            cmd.append("--sem-itens")
        return cmd

    if job["tipo"] == jm.TIPO_COLETA_MULTIFONTE_INCREMENTAL:
        return [
            sys.executable, "-u", str(BASE_DIR / "coleta_multifonte.py"),
            "--inicio", job["data_inicio"], "--fim", job["data_fim"],
            "--db", str(db_compras), "--jobs-db", str(jobs_db), "--job-id", str(job["id"]),
        ]

    if job["tipo"] == jm.TIPO_ATUALIZACAO_DIARIA:
        op = str(job.get("operacao") or "DIARIO_ATUALIZACAO_3D")
        dias = max(1, (date.fromisoformat(job["data_fim"]) - date.fromisoformat(job["data_inicio"])).days + 1)
        modo = "revisao" if op.startswith("REVISAO_") else "retomada" if op.startswith("RETOMADA_") else "diario"
        return [
            sys.executable, "-u", str(BASE_DIR / "coleta_diaria.py"),
            "--dias", str(dias), "--fim", job["data_fim"], "--modo", modo,
            "--db", str(db_compras), "--jobs-db", str(jobs_db), "--job-id", str(job["id"]),
        ]

    if job["tipo"] == jm.TIPO_RECONCILIAR_STAGING_PNCP:
        return [
            sys.executable, "-u", str(BASE_DIR / "reconciliar_pncp.py"),
            "--db", str(db_compras), "--jobs-db", str(jobs_db), "--job-id", str(job["id"]),
        ]

    if job["tipo"] == jm.TIPO_VALIDAR_COMPLETUDE_PNCP:
        limite = int(job.get("operacao") or 2000)
        return [
            sys.executable, "-u", str(BASE_DIR / "validar_completude_pncp.py"),
            "--db", str(db_compras), "--jobs-db", str(jobs_db), "--job-id", str(job["id"]),
            "--limite", str(limite),
        ]

    if job["tipo"] == jm.TIPO_ITENS_INCREMENTAL:
        partes = str(job.get("operacao") or "1000:2000:0").split(":")
        lc = int(partes[0] or 1000); lp = int(partes[1] or 2000); sd = int(partes[2] or 0) if len(partes) > 2 else 0
        cmd = [
            sys.executable, "-u", str(BASE_DIR / "itens_incremental.py"),
            "--db", str(db_compras), "--modo", "manual",
            "--limite-contratacoes", str(lc), "--limite-paginas", str(lp),
            "--jobs-db", str(jobs_db), "--job-id", str(job["id"]),
        ]
        if sd > 0:
            cmd += ["--seed-dias", str(sd)]
        return cmd

    if job["tipo"] == jm.TIPO_ITENS_MUTIRAO:
        partes = str(job.get("operacao") or "2000:4000:0030:60:0").split(":")
        lc = int(partes[0] or 2000)
        lp = int(partes[1] or 4000)
        hc = str(partes[2] or "0030").zfill(4)
        margem = int(partes[3] or 60) if len(partes) > 3 else 60
        sem_limite = bool(int(partes[4] or 0)) if len(partes) > 4 else False
        deadline_compacto = str(partes[5] or "") if len(partes) > 5 else ""
        hora_coleta = f"{hc[:2]}:{hc[2:4]}"
        cmd = [
            sys.executable, "-u", str(BASE_DIR / "mutirao_itens.py"),
            "--db", str(db_compras), "--jobs-db", str(jobs_db), "--job-id", str(job["id"]),
            "--lote-contratacoes", str(lc), "--lote-paginas", str(lp),
            "--hora-coleta", hora_coleta, "--margem-minutos", str(margem),
        ]
        if sem_limite:
            cmd.append("--sem-limite-horario")
        if deadline_compacto:
            try:
                d = datetime.strptime(deadline_compacto, "%Y%m%dT%H%M%S")
                cmd += ["--deadline-iso", d.isoformat(timespec="seconds")]
            except ValueError:
                pass
        return cmd

    if job["tipo"] == jm.TIPO_ITENS_HIBRIDO_PILOTO:
        partes = str(job.get("operacao") or "500:6").split(":")
        limite = int(partes[0] or 500)
        workers = int(partes[1] or 6) if len(partes) > 1 else 6
        return [
            sys.executable, "-u", str(BASE_DIR / "itens_hibrido.py"),
            "--db", str(db_compras), "--jobs-db", str(jobs_db), "--job-id", str(job["id"]),
            "--limite", str(limite), "--workers", str(workers),
        ]

    if job["tipo"] == jm.TIPO_ITENS_HIBRIDO:
        partes = str(job.get("operacao") or "5000:6:95:").split(":")
        limite = int(partes[0] or 5000)
        workers = int(partes[1] or 6) if len(partes) > 1 else 6
        rpm = int(partes[2] or 95) if len(partes) > 2 else 95
        deadline_compacto = str(partes[3] or "") if len(partes) > 3 else ""
        cmd = [
            sys.executable, "-u", str(BASE_DIR / "itens_hibrido.py"),
            "--modo", "producao", "--db", str(db_compras), "--jobs-db", str(jobs_db),
            "--job-id", str(job["id"]), "--limite", str(limite), "--workers", str(workers),
            "--rpm", str(rpm),
        ]
        if deadline_compacto:
            try:
                d = datetime.strptime(deadline_compacto, "%Y%m%dT%H%M%S")
                cmd += ["--deadline-iso", d.isoformat(timespec="seconds")]
            except ValueError:
                pass
        return cmd

    if job["tipo"] == jm.TIPO_INDICE_FTS:
        return [
            sys.executable, "-u", str(BASE_DIR / "construir_indice_fts.py"),
            "--modo", job["operacao"], "--db", str(db_compras),
            "--fts-db", job["artefato_path"], "--jobs-db", str(jobs_db),
            "--job-id", str(job["id"]),
            *(["--deadline-iso",job["deadline_fts"]] if job.get("deadline_fts") else []),
        ]

    if job["tipo"] == jm.TIPO_CLASSIFICACAO_TIC:
        return [
            sys.executable, "-u", str(BASE_DIR / "classificar_tic.py"),
            "--modo", job["operacao"], "--db", str(db_compras),
            "--tic-db", job["artefato_path"], "--jobs-db", str(jobs_db),
            "--job-id", str(job["id"]),
        ]

    raise ValueError(f"Tipo de job desconhecido: {job['tipo']}")


def classificar_final(job: dict, exit_code: int) -> tuple[str, str]:
    if job.get('tipo') == jm.TIPO_RESULTADOS_PNCP:
        if exit_code==0: return 'CONCLUIDA','Sessão de resultados concluída; o controlador continuará as pendências em outros lotes.'
        if exit_code==75: return 'PARCIAL','PNCP indisponível ou limitando consultas; retomada após a pausa registrada na fila.'
        return 'ERRO',f'Coleta de resultados interrompida (código {exit_code}); checkpoints preservados.'

    if exit_code == 79:
        return "PARCIAL", "Pausa para atualização do sistema; progresso e checkpoints preservados para retomada."
    db_compras = Path(job["db_compras"]).resolve()

    if job["tipo"] == jm.TIPO_COLETA_DIARIA:
        progresso = jm.obter_progresso_data(db_compras, job["data_referencia"])
        completa = (
            progresso["modalidades_concluidas"] == 14
            and progresso["pendentes"] == 0
        )
        if completa and progresso["indisponiveis"] > 0:
            return (
                "CONCLUIDA_COM_PENDENCIAS_EXTERNAS",
                "Coleta concluída; há registros indisponíveis no PNCP que não "
                "bloqueiam o fechamento do dia.",
            )
        if completa:
            return "CONCLUIDA", "Coleta diária concluída."
        if exit_code in (0, 1):
            return (
                "PARCIAL",
                "A coleta terminou, mas ainda há pendências recuperáveis.",
            )
        return "ERRO", f"O coletor terminou com código {exit_code}."

    if job["tipo"] == jm.TIPO_CARGA_HISTORICA:
        resumo = jm.obter_resumo_job_historico(job["id"], Path(job["_jobs_db"]))
        if resumo["resolvidos"] == resumo["total"] and resumo["total"] > 0:
            if resumo["contagens"].get("CONCLUIDA_COM_PENDENCIAS_EXTERNAS", 0) > 0:
                return (
                    "CONCLUIDA_COM_PENDENCIAS_EXTERNAS",
                    "Carga histórica concluída para todo o período; alguns dias "
                    "possuem registros indisponíveis no PNCP.",
                )
            return "CONCLUIDA", "Carga histórica concluída para todo o período."

        if exit_code in (0, 1):
            return (
                "PARCIAL",
                "Carga histórica terminou com dias incompletos. A faixa pode ser "
                "retomada e os dias já completos serão pulados.",
            )
        return "ERRO", f"A carga histórica terminou com código {exit_code}."

    if job["tipo"] == jm.TIPO_CARGA_CSV_COMPRASGOV:
        resumo = jm.obter_resumo_job_csv(job["id"], Path(job["_jobs_db"]))
        if exit_code == 0 and resumo["concluidos"] == resumo["total"] and resumo["total"] > 0:
            return "CONCLUIDA", "Carga mensal via Compras.gov CSV concluída."
        if exit_code == 130:
            return "INTERROMPIDA_USUARIO", "Carga CSV interrompida pelo usuário; checkpoints preservados."
        if resumo["concluidos"] > 0:
            return "PARCIAL", "Carga CSV terminou parcialmente; meses concluídos e checkpoints foram preservados."
        return "ERRO", f"A carga CSV terminou com código {exit_code}."

    if job["tipo"] == jm.TIPO_CARGA_BASE_CSV_COMPRASGOV:
        if exit_code == 0:
            return "CONCLUIDA", "Reparo e carga-base anual Compras.gov concluídos."
        if exit_code == 130:
            return "INTERROMPIDA_USUARIO", "Carga-base interrompida; downloads e checkpoints foram preservados."
        return "ERRO", f"A carga-base Compras.gov terminou com código {exit_code}."

    if job["tipo"] == jm.TIPO_CARGA_2026_COMPRASGOV:
        if exit_code == 0:
            return "CONCLUIDA", "Carga massiva Compras.gov 2026 concluída."
        if exit_code == 130:
            return "INTERROMPIDA_USUARIO", "Carga 2026 interrompida; downloads e checkpoints preservados."
        return "ERRO", f"A carga 2026 terminou com código {exit_code}."

    if job["tipo"] == jm.TIPO_API_COMPRASGOV_INCREMENTAL:
        if exit_code == 0:
            return "CONCLUIDA", "COLETA incremental via API Compras.gov concluída."
        if exit_code == 75:
            return "PARCIAL", "COLETA Compras.gov concluiu parcialmente; dados obtidos foram preservados e falhas complementares podem ser retomadas."
        if exit_code == 130:
            return "INTERROMPIDA_USUARIO", "COLETA Compras.gov interrompida; dados já gravados foram preservados."
        return "ERRO", f"A COLETA Compras.gov terminou com código {exit_code}."

    if job["tipo"] == jm.TIPO_COLETA_MULTIFONTE_INCREMENTAL:
        if exit_code == 0:
            return "CONCLUIDA", "COLETA multifuente concluída; PNCP e fontes complementares processados."
        if exit_code == 76:
            return (
                "CONCLUIDA_COM_PENDENCIAS_EXTERNAS",
                "Contratações PNCP atualizadas; uma ou mais fontes complementares ficaram indisponíveis e poderão ser retomadas depois.",
            )
        if exit_code == 75:
            return (
                "PARCIAL",
                "COLETA multifuente parcial: a fonte primária PNCP teve pendências recuperáveis; reexecute a mesma janela para retomar somente modalidades faltantes.",
            )
        if exit_code == 130:
            return "INTERROMPIDA_USUARIO", "COLETA multifuente interrompida; dados já gravados foram preservados."
        return "ERRO", f"A COLETA multifuente terminou com código {exit_code}."

    if job["tipo"] == jm.TIPO_ATUALIZACAO_DIARIA:
        if exit_code == 0:
            return "CONCLUIDA", "COLETA diária concluída; janela móvel revisada por data de atualização."
        if exit_code == 76:
            return (
                "CONCLUIDA_COM_PENDENCIAS_EXTERNAS",
                "COLETA diária concluiu o PNCP; uma fonte complementar ficou pendente e será tentada novamente na próxima execução.",
            )
        if exit_code == 75:
            return (
                "PARCIAL",
                "COLETA parcial: a fonte primária PNCP teve pendência recuperável; checkpoint por modalidade/página preservado para retomada.",
            )
        if exit_code == 130:
            return "INTERROMPIDA_USUARIO", "COLETA diária interrompida; dados já gravados foram preservados."
        return "ERRO", f"A COLETA diária terminou com código {exit_code}."

    if job["tipo"] == jm.TIPO_RECONCILIAR_STAGING_PNCP:
        if exit_code == 0:
            return "CONCLUIDA", "Higiene do staging e reconciliação de pais PNCP concluídas."
        if exit_code == 75:
            return "PARCIAL", "Reconciliação parcial: ainda há pendências recuperáveis, rate limit ou instabilidade do PNCP; fila preservada para retomada."
        if exit_code == 130:
            return "INTERROMPIDA_USUARIO", "Reconciliação interrompida; fila e progresso foram preservados."
        if exit_code == 6:
            return "ERRO", "Auditoria estrutural encontrou inconsistências internas; fila preservada para diagnóstico."
        return "ERRO", f"A reconciliação PNCP terminou com código {exit_code}."

    if job["tipo"] == jm.TIPO_VALIDAR_COMPLETUDE_PNCP:
        if exit_code == 0:
            return "CONCLUIDA", "Lote de validação de completude pelo PNCP concluído."
        if exit_code == 75:
            return "PARCIAL", "Validação pausada por rate limit ou instabilidade do PNCP; confirmações já feitas foram preservadas."
        if exit_code == 130:
            return "INTERROMPIDA_USUARIO", "Validação interrompida; confirmações já feitas foram preservadas."
        return "ERRO", f"A validação de completude terminou com código {exit_code}."

    if job["tipo"] == jm.TIPO_ITENS_INCREMENTAL:
        if exit_code == 0:
            return "CONCLUIDA", "Lote da fila incremental de itens concluído."
        if exit_code == 75:
            return "PARCIAL", "Fila de itens encontrou falhas recuperáveis; páginas e staging foram preservados para retomada."
        if exit_code == 130:
            return "INTERROMPIDA_USUARIO", "Fila de itens interrompida; checkpoints e staging foram preservados."
        return "ERRO", f"A fila incremental de itens terminou com código {exit_code}."

    if job["tipo"] == jm.TIPO_ITENS_MUTIRAO:
        if exit_code == 0:
            return "CONCLUIDA", "Mutirão automático de itens concluiu todo o trabalho elegível."
        if exit_code == 78:
            return "PARCIAL", "Mutirão cedeu a vez à sincronização do índice; coleta e checkpoint preservados."
        if exit_code == 76:
            return "PARCIAL", "Mutirão pausado no horário reservado à pesquisa ou na janela da COLETA; progresso preservado."
        if exit_code == 75:
            return "PARCIAL", "Mutirão pausado por instabilidade do PNCP; checkpoints e staging preservados."
        if exit_code == 77:
            return "PARCIAL", "Mutirão pausado por ausência de progresso; backlog preservado para diagnóstico."
        if exit_code == 130:
            return "INTERROMPIDA_USUARIO", "Mutirão interrompido; checkpoints e staging foram preservados."
        return "ERRO", f"O mutirão automático de itens terminou com código {exit_code}."

    if job["tipo"] == jm.TIPO_ITENS_HIBRIDO:
        if exit_code == 0:
            return "CONCLUIDA", "Lote híbrido Compras.gov concluído; validações e fallbacks foram persistidos."
        if exit_code == 76:
            return "PARCIAL", "Lote híbrido pausado na janela de segurança da COLETA; progresso já gravado foi preservado."
        if exit_code == 75:
            return "PARCIAL", "Lote híbrido pausado por instabilidade do Compras.gov; erros transitórios permaneceram na rota rápida."
        if exit_code == 130:
            return "INTERROMPIDA_USUARIO", "Lote híbrido interrompido; validações já gravadas foram preservadas."
        return "ERRO", f"O lote híbrido de itens terminou com código {exit_code}."

    if job["tipo"] == jm.TIPO_ITENS_HIBRIDO_PILOTO:
        if exit_code == 0:
            return "CONCLUIDA", "Piloto híbrido concluído; validações Compras.gov e fallbacks PNCP foram registrados sem promover itens_coletados."
        if exit_code == 130:
            return "INTERROMPIDA_USUARIO", "Piloto híbrido interrompido; validações já gravadas foram preservadas."
        return "ERRO", f"O piloto híbrido de itens terminou com código {exit_code}."

    if job["tipo"] == jm.TIPO_INDICE_FTS:
        if exit_code == 76:
            return "PARCIAL", "Índice pausado por limite de tempo, janela de segurança ou arquivo ocupado; checkpoint preservado. Consulte a etapa e o arquivo no log."
        if exit_code == 0:
            return "CONCLUIDA", "Índice de pesquisa FTS5 concluído."
        if exit_code == 130:
            return "INTERROMPIDA_USUARIO", "Construção do índice interrompida pelo usuário."
        return "ERRO", f"A construção do índice terminou com código {exit_code}."

    if job["tipo"] == jm.TIPO_CLASSIFICACAO_TIC:
        if exit_code == 0:
            return "CONCLUIDA", "Classificação TIC concluída."
        if exit_code == 130:
            return "INTERROMPIDA_USUARIO", "Classificação TIC interrompida pelo usuário."
        return "ERRO", f"A classificação TIC terminou com código {exit_code}."

    return "ERRO", "Tipo de job desconhecido."


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-id", type=int, required=True)
    parser.add_argument("--jobs-db", required=True)
    args = parser.parse_args()

    jobs_db = Path(args.jobs_db).resolve()
    jm.preparar_banco_jobs(jobs_db)
    job = jm.obter_job(args.job_id, jobs_db)
    if not job:
        return 2

    job["_jobs_db"] = str(jobs_db)
    log_path = Path(job["log_path"]).resolve()
    log_path.parent.mkdir(parents=True, exist_ok=True)

    if job["tipo"] == jm.TIPO_COLETA_DIARIA:
        descricao = f"Coleta diária {job['data_referencia']}"
    elif job["tipo"] == jm.TIPO_CARGA_HISTORICA:
        descricao = f"Carga histórica {job['data_inicio']} até {job['data_fim']}"
    elif job["tipo"] == jm.TIPO_CARGA_CSV_COMPRASGOV:
        descricao = f"Deltas Compras.gov CSV {job['data_inicio']} até {job['data_fim']}"
    elif job["tipo"] == jm.TIPO_CARGA_BASE_CSV_COMPRASGOV:
        descricao = "Reparo + carga-base Compras.gov 2021–2024"
    elif job["tipo"] == jm.TIPO_CARGA_2026_COMPRASGOV:
        descricao = "Carga massiva Compras.gov 2026 + resultados"
    elif job["tipo"] == jm.TIPO_API_COMPRASGOV_INCREMENTAL:
        descricao = f"COLETA incremental Compras.gov API {job['data_inicio']} até {job['data_fim']}"
    elif job["tipo"] == jm.TIPO_COLETA_MULTIFONTE_INCREMENTAL:
        descricao = f"COLETA incremental multifuente {job['data_inicio']} até {job['data_fim']}"
    elif job["tipo"] == jm.TIPO_ATUALIZACAO_DIARIA:
        op = str(job.get("operacao") or "DIARIO_ATUALIZACAO_3D")
        prefixo = "COLETA revisão semanal" if op.startswith("REVISAO_") else "COLETA retomada" if op.startswith("RETOMADA_") else "COLETA diária"
        descricao = f"{prefixo} · janela {job['data_inicio']} até {job['data_fim']}"
    elif job["tipo"] == jm.TIPO_RECONCILIAR_STAGING_PNCP:
        descricao = "Higiene do staging + reconciliação de pais PNCP"
    elif job["tipo"] == jm.TIPO_VALIDAR_COMPLETUDE_PNCP:
        descricao = f"Validação de completude PNCP (limite {job.get('operacao') or '2000'})"
    elif job["tipo"] == jm.TIPO_ITENS_INCREMENTAL:
        descricao = f"Fila incremental de itens · limites {job.get('operacao') or '1000:2000:0'}"
    elif job["tipo"] == jm.TIPO_ITENS_MUTIRAO:
        partes = str(job.get("operacao") or "2000:4000:0030:60:0").split(":")
        lc, lp = int(partes[0]), int(partes[1])
        hc = str(partes[2]).zfill(4)
        margem = int(partes[3]) if len(partes) > 3 else 60
        sem_limite = bool(int(partes[4])) if len(partes) > 4 else False
        deadline_compacto = str(partes[5] or "") if len(partes) > 5 else ""
        if sem_limite:
            janela = "até esvaziar"
        elif deadline_compacto:
            try:
                dd = datetime.strptime(deadline_compacto, "%Y%m%dT%H%M%S")
                janela = f"protege próximo job COLETA até {dd:%d/%m %H:%M}"
            except ValueError:
                janela = f"protege COLETA {hc[:2]}:{hc[2:4]} (-{margem} min)"
        else:
            janela = f"protege COLETA {hc[:2]}:{hc[2:4]} (-{margem} min)"
        descricao = f"Mutirão automático de itens · lotes {lc}:{lp} · {janela}"
    elif job["tipo"] == jm.TIPO_ITENS_HIBRIDO:
        partes = str(job.get("operacao") or "5000:6:95:").split(":")
        limite = int(partes[0] or 5000)
        workers = int(partes[1] or 6) if len(partes) > 1 else 6
        rpm = int(partes[2] or 95) if len(partes) > 2 else 95
        descricao = f"Mutirão híbrido Compras.gov-first · até {limite} contratações · {workers} workers · {rpm} req/min"
    elif job["tipo"] == jm.TIPO_ITENS_HIBRIDO_PILOTO:
        partes = str(job.get("operacao") or "500:6").split(":")
        limite = int(partes[0] or 500)
        workers = int(partes[1] or 6) if len(partes) > 1 else 6
        descricao = f"Piloto híbrido Compras.gov-first · até {limite} contratações · {workers} workers"
    elif job["tipo"] == jm.TIPO_INDICE_FTS:
        descricao = f"Índice FTS5 ({job['operacao']})"
    elif job["tipo"] == jm.TIPO_CLASSIFICACAO_TIC:
        descricao = f"Classificação TIC ({job['operacao']})"
    else:
        descricao = job["tipo"]

    jm.atualizar_job(
        args.job_id,
        jobs_db,
        status="EXECUTANDO",
        worker_pid=os.getpid(),
        iniciado_em=jm.agora(),
        mensagem=f"{descricao} iniciada.",
    )

    comando = comando_job(job, jobs_db)

    try:
        with log_path.open("a", encoding="utf-8", buffering=1) as log:
            if job['tipo'] != jm.TIPO_INDICE_FTS:
                import sqlite3
                from fts_sync import instalar
                with sqlite3.connect(str(job['db_compras']), timeout=30) as fonte_fts:
                    tabelas = {r[0] for r in fonte_fts.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                    if {'contratacoes','itens'} <= tabelas:
                        instalar(fonte_fts)
            escrever(log, "=" * 92)
            escrever(log, f"Radar PNCP V{VERSION} — job #{args.job_id}")
            escrever(log, descricao)
            escrever(log, f"Banco: {job['db_compras']}")
            escrever(log, "Comando: " + " ".join(comando))
            escrever(log, "=" * 92)

            kwargs = {}
            if os.name == "nt":
                kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
            else:
                kwargs["start_new_session"] = True

            env = os.environ.copy()
            env["PYTHONIOENCODING"] = "utf-8"
            env["PYTHONUTF8"] = "1"

            proc = subprocess.Popen(
                comando,
                cwd=str(BASE_DIR),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                env=env,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                **kwargs,
            )
            if proc.stdout is None:
                raise RuntimeError("Não foi possível capturar a saída do subprocesso.")
            thread_log = threading.Thread(
                target=_encaminhar_saida_processo,
                args=(proc.stdout, log),
                name=f"job-log-{args.job_id}",
                daemon=True,
            )
            thread_log.start()
            jm.atualizar_job(
                args.job_id,
                jobs_db,
                process_pid=proc.pid,
                mensagem=f"{descricao} em execução.",
            )

            parada = False
            while proc.poll() is None:
                if jm.parada_solicitada(args.job_id, jobs_db):
                    parada = True
                    escrever(log, "")
                    escrever(log, ">>> PARADA SOLICITADA PELO USUÁRIO <<<")
                    escrever(
                        log,
                        "Encerrando a árvore de processos. O progresso já gravado "
                        "no SQLite será preservado.",
                    )
                    encerrar_arvore_processos(proc)
                    break
                time.sleep(1.0)

            try:
                exit_code = proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                encerrar_arvore_processos(proc)
                exit_code = proc.wait(timeout=10)

            # Garante que as últimas linhas do subprocesso foram drenadas antes do resumo do worker.
            thread_log.join(timeout=5)

            # V0.7.6: materializa as estatísticas fora do processo da interface.
            # Assim o Streamlit não faz GROUP BY em milhões de linhas a cada rerun.
            tipos_dados = {
                jm.TIPO_COLETA_DIARIA, jm.TIPO_CARGA_HISTORICA,
                jm.TIPO_CARGA_CSV_COMPRASGOV, jm.TIPO_CARGA_BASE_CSV_COMPRASGOV,
                jm.TIPO_RECONCILIAR_STAGING_PNCP, jm.TIPO_VALIDAR_COMPLETUDE_PNCP, jm.TIPO_ITENS_INCREMENTAL, jm.TIPO_ITENS_MUTIRAO, jm.TIPO_ITENS_HIBRIDO, jm.TIPO_ITENS_HIBRIDO_PILOTO,
            }
            pular_stats = False
            if job["tipo"] == jm.TIPO_VALIDAR_COMPLETUDE_PNCP and exit_code == 75:
                atual_stats = jm.obter_job(args.job_id, jobs_db)
                pular_stats = bool(
                    atual_stats
                    and str(atual_stats.get("etapa") or "") == "PNCP indisponível"
                    and int(atual_stats.get("progresso_atual") or 0) == 0
                )
            elif job["tipo"] in (jm.TIPO_ITENS_INCREMENTAL, jm.TIPO_ITENS_MUTIRAO, jm.TIPO_ITENS_HIBRIDO, jm.TIPO_ITENS_HIBRIDO_PILOTO):
                # V0.7.6: lotes manuais/mutirões de itens são alimentação da
                # base. O snapshot completo percorre milhões de linhas e no teste
                # real levou mais tempo que a própria coleta de 250 contratos.
                # Os dados principais já estão gravados em compras.db; o snapshot
                # será atualizado pela próxima COLETA, revisão semanal ou pelo botão
                # manual da tela Status da base.
                pular_stats = True
            if job["tipo"] in tipos_dados and not pular_stats:
                try:
                    escrever(log, "")
                    escrever(log, "[STATS] Atualizando snapshot da interface...")
                    jm.atualizar_job(args.job_id, jobs_db, etapa="Atualizando estatísticas", mensagem="Atualizando snapshot da interface.")
                    snap = rs.atualizar_automaticamente(Path(job["db_compras"]).resolve(), DB_STATS)
                    if snap is not None:
                        escrever(log, f"[STATS] Snapshot atualizado: {snap['meta']['total_contratacoes']:,} contratações · {snap['meta']['total_itens']:,} itens físicos.")
                    else:
                        escrever(log, "[STATS] Snapshot não atualizado nesta sessão (limite de tempo ou falha); último resumo e data original preservados. Dados coletados já estão gravados.")
                except Exception as exc_stats:
                    escrever(log, f"[STATS] Aviso: não foi possível atualizar radar_stats.db: {type(exc_stats).__name__}: {exc_stats}")
            elif pular_stats:
                escrever(log, "")
                if job["tipo"] in (jm.TIPO_ITENS_INCREMENTAL, jm.TIPO_ITENS_MUTIRAO, jm.TIPO_ITENS_HIBRIDO, jm.TIPO_ITENS_HIBRIDO_PILOTO):
                    escrever(log, "[STATS] Snapshot completo adiado durante processamento de itens; compras.db já foi atualizado. A próxima COLETA/revisão ou a atualização manual do Status da base recalculará as estatísticas.")
                else:
                    escrever(log, "[STATS] Snapshot não reconstruído: o preflight falhou antes do início do lote e a base não foi alterada.")

            if parada:
                status = "INTERROMPIDA_USUARIO"
                mensagem = (
                    "Execução interrompida pelo usuário. O progresso já gravado "
                    "foi preservado e a execução pode ser retomada."
                )
                if job["tipo"] == jm.TIPO_CARGA_HISTORICA:
                    atual = jm.obter_job(args.job_id, jobs_db)
                    if atual and atual.get("data_atual"):
                        jm.atualizar_job_dia(
                            args.job_id,
                            atual["data_atual"],
                            jobs_db,
                            status="INTERROMPIDA",
                            finalizado_em=jm.agora(),
                            mensagem="Dia interrompido pelo usuário.",
                        )
                elif job["tipo"] == jm.TIPO_CARGA_CSV_COMPRASGOV:
                    atual = jm.obter_job(args.job_id, jobs_db)
                    if atual and atual.get("data_atual"):
                        jm.atualizar_job_csv_mes(
                            args.job_id, atual["data_atual"], jobs_db,
                            status="INTERROMPIDA", finalizado_em=jm.agora(),
                            mensagem="Mês interrompido pelo usuário; checkpoint preservado.",
                        )
                elif job["tipo"] == jm.TIPO_COLETA_MULTIFONTE_INCREMENTAL:
                    atual = jm.obter_job(args.job_id, jobs_db)
                    if atual and atual.get("data_atual"):
                        jm.atualizar_job_dia(
                            args.job_id, atual["data_atual"], jobs_db,
                            status="INTERROMPIDA", finalizado_em=jm.agora(),
                            mensagem="Dia interrompido pelo usuário; modalidades PNCP já concluídas permanecem em checkpoint.",
                        )
            else:
                # Recarrega para obter data_atual/estado mais recente.
                job_final = jm.obter_job(args.job_id, jobs_db) or job
                job_final["_jobs_db"] = str(jobs_db)
                status, mensagem = classificar_final(job_final, int(exit_code))

            escrever(log, "")
            escrever(log, f">>> JOB FINALIZADO: {status} (código {exit_code}) <<<")
            jm.atualizar_job(
                args.job_id,
                jobs_db,
                status=status,
                finalizado_em=jm.agora(),
                exit_code=int(exit_code),
                process_pid=None,
                data_atual=None,
                mensagem=mensagem,
            )
            return 0 if status in (
                "CONCLUIDA",
                "CONCLUIDA_COM_PENDENCIAS_EXTERNAS",
            ) else 1

    except Exception as exc:
        try:
            with log_path.open("a", encoding="utf-8") as log:
                escrever(log, "")
                escrever(log, f">>> ERRO DO WORKER: {type(exc).__name__}: {exc}")
        except OSError:
            pass
        jm.atualizar_job(
            args.job_id,
            jobs_db,
            status="ERRO",
            finalizado_em=jm.agora(),
            process_pid=None,
            mensagem=f"Erro no worker: {type(exc).__name__}: {exc}",
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
