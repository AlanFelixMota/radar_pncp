#!/usr/bin/env python3
"""Controlador persistente do mutirão de itens — Radar PNCP V0.7.6.

Mantém o preenchimento histórico trabalhando em segundo plano entre os jobs
prioritários da COLETA. O controlador não coleta itens diretamente: ele inicia
jobs híbridos Compras.gov-first e, depois, jobs PNCP apenas para fallbacks. Espera enquanto qualquer job estiver ativo e retoma quando houver uma janela segura.

A COLETA diária e a revisão semanal sempre têm prioridade. O controlador usa o
Agendador do Windows para conhecer as próximas execuções e fornece um deadline
absoluto ao mutirão, que pode parar inclusive no meio de um lote (entre páginas)
para liberar a base antes do job prioritário.
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import itens_incremental as ii
import itens_hibrido as ih
import job_manager as jm
import coleta_scheduler as rsch
from radar_paths import DB_COMPRAS, DB_JOBS, DB_FTS, LOG_DIR

CONFIG_TABLE = "mutirao_persistente_config"
ESTADO_TABLE = "mutirao_persistente_estado"
HOLD_ATRASO_HORAS = 6
COOLDOWN_INSTABILIDADE_MIN = 60
COOLDOWN_COMPRASGOV_MIN = 15
COOLDOWN_SEM_PROGRESSO_MIN = 30
COOLDOWN_COLETA_PARCIAL_MIN = 15


def agora_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def garantir_estrutura(db_jobs: Path = DB_JOBS) -> None:
    jm.preparar_banco_jobs(db_jobs)
    con = jm.conectar_jobs(db_jobs)
    con.executescript(
        f"""
        CREATE TABLE IF NOT EXISTS {CONFIG_TABLE} (
            id INTEGER PRIMARY KEY CHECK(id=1),
            ativo INTEGER NOT NULL DEFAULT 0,
            lote_contratacoes INTEGER NOT NULL DEFAULT 5000,
            lote_paginas INTEGER NOT NULL DEFAULT 8000,
            margem_minutos INTEGER NOT NULL DEFAULT 60,
            atualizado_em TEXT
        );
        INSERT OR IGNORE INTO {CONFIG_TABLE}
            (id,ativo,lote_contratacoes,lote_paginas,margem_minutos,atualizado_em)
        VALUES (1,0,5000,8000,60,NULL);

        CREATE TABLE IF NOT EXISTS {ESTADO_TABLE} (
            id INTEGER PRIMARY KEY CHECK(id=1),
            controller_pid INTEGER,
            heartbeat_em TEXT,
            estado TEXT,
            detalhe TEXT,
            proximo_prioritario TEXT,
            deadline TEXT,
            ultimo_job_id INTEGER,
            ultima_acao_em TEXT
        );
        INSERT OR IGNORE INTO {ESTADO_TABLE}(id,estado) VALUES (1,'INATIVO');
        """
    )
    cols = {str(r[1]) for r in con.execute(f"PRAGMA table_info({CONFIG_TABLE})").fetchall()}
    extras = {
        "hibrido_lote": "INTEGER NOT NULL DEFAULT 5000",
        "hibrido_workers": "INTEGER NOT NULL DEFAULT 6",
        "hibrido_rpm": "INTEGER NOT NULL DEFAULT 95",
        "pesquisa_inicio": "TEXT NOT NULL DEFAULT '07:00'",
        "pesquisa_fim": "TEXT NOT NULL DEFAULT '10:00'",
        "pesquisa_protegida": "INTEGER NOT NULL DEFAULT 1",
        "resultados_ativo": "INTEGER NOT NULL DEFAULT 1",
    }
    for col, definicao in extras.items():
        if col not in cols:
            con.execute(f"ALTER TABLE {CONFIG_TABLE} ADD COLUMN {col} {definicao}")
    con.commit(); con.close()


def ler_config(db_jobs: Path = DB_JOBS) -> dict[str, Any]:
    garantir_estrutura(db_jobs)
    con = jm.conectar_jobs(db_jobs)
    row = con.execute(f"SELECT * FROM {CONFIG_TABLE} WHERE id=1").fetchone()
    con.close()
    return dict(row) if row else {
        "ativo": 0, "lote_contratacoes": 5000, "lote_paginas": 8000,
        "hibrido_lote": 5000, "hibrido_workers": 6, "hibrido_rpm": 95,
        "margem_minutos": 60, "atualizado_em": None,
    }


def definir_config(
    *, ativo: bool | None = None, lote_contratacoes: int | None = None,
    lote_paginas: int | None = None, margem_minutos: int | None = None,
    hibrido_lote: int | None = None, hibrido_workers: int | None = None,
    hibrido_rpm: int | None = None, db_jobs: Path = DB_JOBS,
    pesquisa_inicio: str | None = None, pesquisa_fim: str | None = None,
    pesquisa_protegida: bool | None = None,
    resultados_ativo: bool | None = None,
) -> dict[str, Any]:
    garantir_estrutura(db_jobs)
    atual = ler_config(db_jobs)
    novo = {
        "ativo": int(bool(atual.get("ativo"))) if ativo is None else int(bool(ativo)),
        "lote_contratacoes": int(atual.get("lote_contratacoes") or 5000) if lote_contratacoes is None else int(lote_contratacoes),
        "lote_paginas": int(atual.get("lote_paginas") or 8000) if lote_paginas is None else int(lote_paginas),
        "hibrido_lote": int(atual.get("hibrido_lote") or 5000) if hibrido_lote is None else int(hibrido_lote),
        "hibrido_workers": int(atual.get("hibrido_workers") or 6) if hibrido_workers is None else int(hibrido_workers),
        "hibrido_rpm": int(atual.get("hibrido_rpm") or 95) if hibrido_rpm is None else int(hibrido_rpm),
        "margem_minutos": int(atual.get("margem_minutos") or 60) if margem_minutos is None else int(margem_minutos),
        "pesquisa_inicio": atual.get("pesquisa_inicio", "07:00") if pesquisa_inicio is None else pesquisa_inicio,
        "pesquisa_fim": atual.get("pesquisa_fim", "10:00") if pesquisa_fim is None else pesquisa_fim,
        "pesquisa_protegida": int(atual.get("pesquisa_protegida", 1)) if pesquisa_protegida is None else int(pesquisa_protegida),
        "resultados_ativo": int(atual.get('resultados_ativo',1)) if resultados_ativo is None else int(resultados_ativo),
    }
    if novo["lote_contratacoes"] <= 0 or novo["lote_paginas"] <= 0 or novo["hibrido_lote"] <= 0:
        raise ValueError("Os lotes do mutirão persistente devem ser maiores que zero.")
    if not 1 <= novo["hibrido_workers"] <= 12:
        raise ValueError("Workers híbridos devem ficar entre 1 e 12.")
    if not 30 <= novo["hibrido_rpm"] <= 120:
        raise ValueError("Limite Compras.gov deve ficar entre 30 e 120 req/min.")
    if not 15 <= novo["margem_minutos"] <= 240:
        raise ValueError("A margem de segurança deve ficar entre 15 e 240 minutos.")
    janela_pesquisa(novo, datetime.now())  # valida os horários antes de salvar
    con = jm.conectar_jobs(db_jobs)
    con.execute(
        f"""UPDATE {CONFIG_TABLE}
               SET ativo=?,lote_contratacoes=?,lote_paginas=?,hibrido_lote=?,hibrido_workers=?,hibrido_rpm=?,
                   margem_minutos=?,pesquisa_inicio=?,pesquisa_fim=?,pesquisa_protegida=?,resultados_ativo=?,atualizado_em=?
             WHERE id=1""",
        (novo["ativo"], novo["lote_contratacoes"], novo["lote_paginas"], novo["hibrido_lote"],
         novo["hibrido_workers"], novo["hibrido_rpm"], novo["margem_minutos"],
         novo["pesquisa_inicio"], novo["pesquisa_fim"], novo["pesquisa_protegida"], novo['resultados_ativo'], agora_iso()),
    )
    con.commit(); con.close()
    return ler_config(db_jobs)


def janela_pesquisa(cfg: dict, agora: datetime) -> tuple[bool, datetime | None, datetime | None]:
    """Retorna (dentro da reserva, próximo início, fim), no relógio local.

    Aceita intervalos que cruzam a meia-noite. A reserva limita apenas os jobs
    automáticos de itens; a COLETA e o índice mantêm suas próprias prioridades.
    """
    from mutirao_itens import _parse_hora
    hi, mi = _parse_hora(str(cfg.get('pesquisa_inicio', '07:00')))
    hf, mf = _parse_hora(str(cfg.get('pesquisa_fim', '10:00')))
    if (hi, mi) == (hf, mf):
        raise ValueError('O início e o fim do horário de pesquisa devem ser diferentes.')
    if not int(cfg.get('pesquisa_protegida', 1)):
        return False, None, None
    inicio = agora.replace(hour=hi, minute=mi, second=0, microsecond=0)
    fim = agora.replace(hour=hf, minute=mf, second=0, microsecond=0)
    if fim <= inicio:
        fim += timedelta(days=1)
        if agora < fim - timedelta(days=1):
            inicio -= timedelta(days=1)
            fim -= timedelta(days=1)
    if inicio <= agora < fim:
        return True, inicio, fim
    if agora >= fim:
        inicio += timedelta(days=1)
        fim += timedelta(days=1)
    return False, inicio, fim


def atualizar_estado(db_jobs: Path = DB_JOBS, **campos: Any) -> None:
    garantir_estrutura(db_jobs)
    permitidos = {
        "controller_pid", "heartbeat_em", "estado", "detalhe",
        "proximo_prioritario", "deadline", "ultimo_job_id", "ultima_acao_em",
    }
    invalidos = set(campos) - permitidos
    if invalidos:
        raise ValueError(f"Campos de estado inválidos: {sorted(invalidos)}")
    campos.setdefault("heartbeat_em", agora_iso())
    partes = ",".join(f"{k}=?" for k in campos)
    con = jm.conectar_jobs(db_jobs)
    con.execute(f"UPDATE {ESTADO_TABLE} SET {partes} WHERE id=1", (*campos.values(),))
    con.commit(); con.close()


def ler_estado(db_jobs: Path = DB_JOBS) -> dict[str, Any]:
    garantir_estrutura(db_jobs)
    con = jm.conectar_jobs(db_jobs)
    row = con.execute(f"SELECT * FROM {ESTADO_TABLE} WHERE id=1").fetchone()
    con.close()
    return dict(row) if row else {"estado": "INATIVO"}


def _dt_local(valor: str | None) -> datetime | None:
    if not valor:
        return None
    try:
        d = datetime.fromisoformat(str(valor))
    except ValueError:
        return None
    if d.tzinfo is not None:
        d = d.astimezone().replace(tzinfo=None)
    return d


def _hora(status: dict[str, Any]) -> tuple[int, int] | None:
    h = str(status.get("hora") or "")
    try:
        hh, mm = (int(x) for x in h.split(":", 1))
    except Exception:
        return None
    return (hh, mm) if 0 <= hh <= 23 and 0 <= mm <= 59 else None


def _ultima_ocorrencia_programada(status: dict[str, Any], agora: datetime) -> datetime | None:
    hm = _hora(status)
    if hm is None:
        return None
    hh, mm = hm
    if status.get("modo") == "diario":
        d = agora.replace(hour=hh, minute=mm, second=0, microsecond=0)
        return d if d <= agora else d - timedelta(days=1)

    # revisão semanal
    dia_en = str(status.get("dia_semana_en") or "")
    mapa = {
        "Monday": 0, "Tuesday": 1, "Wednesday": 2, "Thursday": 3,
        "Friday": 4, "Saturday": 5, "Sunday": 6,
    }
    alvo = mapa.get(dia_en)
    if alvo is None:
        return None
    delta = (agora.weekday() - alvo) % 7
    d = (agora - timedelta(days=delta)).replace(hour=hh, minute=mm, second=0, microsecond=0)
    if d > agora:
        d -= timedelta(days=7)
    return d


def _proxima_ocorrencia(status: dict[str, Any], agora: datetime) -> datetime | None:
    """Calcula a próxima ocorrência com fail-safe de virada de dia.

    V0.7.6: o controlador não confia cegamente em ``NextRunTime``. Em 08/09
    houve uma leitura em que o próximo horário diário deixou de proteger a janela
    das 00:30 e o mutirão recebeu um deadline semanal. A agenda declarada no
    trigger é, portanto, calculada localmente e usada como limite superior: se o
    Agendador devolver uma ocorrência mais distante, prevalece a ocorrência
    derivada mais próxima.
    """
    hm = _hora(status)
    derivada: datetime | None = None
    if hm is not None:
        hh, mm = hm
        if status.get("modo") == "diario":
            derivada = agora.replace(hour=hh, minute=mm, second=0, microsecond=0)
            if derivada <= agora:
                derivada += timedelta(days=1)
        else:
            dia_en = str(status.get("dia_semana_en") or "")
            mapa = {"Monday":0,"Tuesday":1,"Wednesday":2,"Thursday":3,"Friday":4,"Saturday":5,"Sunday":6}
            alvo = mapa.get(dia_en)
            if alvo is not None:
                delta = (alvo - agora.weekday()) % 7
                derivada = (agora + timedelta(days=delta)).replace(hour=hh, minute=mm, second=0, microsecond=0)
                if derivada <= agora:
                    derivada += timedelta(days=7)

    agendador = _dt_local(status.get("proxima_execucao"))
    if agendador is not None and agendador <= agora:
        agendador = None
    if derivada is not None and agendador is not None:
        return min(derivada, agendador)
    return derivada or agendador


def estado_prioridades(base_dir: Path, margem_minutos: int, agora: datetime | None = None) -> dict[str, Any]:
    agora = agora or datetime.now()
    candidatos: list[tuple[datetime, str, dict[str, Any]]] = []
    atrasados: list[tuple[datetime, str]] = []
    rodando: list[str] = []

    for modo, nome in (("diario", "COLETA diária"), ("revisao", "Revisão semanal")):
        try:
            st = rsch.obter_status(modo, base_dir)
        except Exception as exc:
            return {
                "bloqueado": True,
                "motivo": f"Falha ao verificar {nome} no Agendador; mutirão pausado por segurança: {type(exc).__name__}: {exc}",
                "nome": nome,
            }
        if st.get("erro") or str(st.get("estado") or "").casefold() == "erro":
            return {
                "bloqueado": True,
                "motivo": f"Não foi possível confirmar o estado da {nome}; mutirão pausado por segurança: {st.get('erro') or 'estado ERRO'}",
                "nome": nome,
            }
        if not st.get("existe") or not st.get("ativo"):
            continue
        if str(st.get("estado") or "").casefold() == "running":
            rodando.append(nome)
        prox = _proxima_ocorrencia(st, agora)
        if prox is not None:
            candidatos.append((prox, nome, st))

        ult_occ = _ultima_ocorrencia_programada(st, agora)
        ult_run = _dt_local(st.get("ultima_execucao"))
        if ult_occ is not None and timedelta(0) <= agora - ult_occ <= timedelta(hours=HOLD_ATRASO_HORAS):
            # Se não há evidência de que a ocorrência programada chegou a iniciar,
            # não deixamos o mutirão tomar a frente de um StartWhenAvailable atrasado.
            if ult_run is None or ult_run < ult_occ - timedelta(minutes=1):
                atrasados.append((ult_occ, nome))

    if rodando:
        return {"bloqueado": True, "motivo": f"Job prioritário do Windows em execução: {', '.join(rodando)}.", "nome": rodando[0]}
    if atrasados:
        occ, nome = min(atrasados, key=lambda x: x[0])
        return {
            "bloqueado": True,
            "motivo": f"{nome} programada para {occ:%d/%m %H:%M} ainda não iniciou; aguardando o Agendador do Windows.",
            "nome": nome,
        }
    if not candidatos:
        return {"bloqueado": True, "motivo": "Nenhum agendamento ativo da COLETA foi encontrado; mutirão persistente pausado por segurança."}

    prox, nome, st = min(candidatos, key=lambda x: x[0])
    deadline = prox - timedelta(minutes=max(0, int(margem_minutos)))
    return {
        "bloqueado": agora >= deadline,
        "motivo": (
            f"Janela de segurança do próximo job prioritário ({nome} em {prox:%d/%m %H:%M})."
            if agora >= deadline else "Janela livre para o mutirão."
        ),
        "nome": nome,
        "proxima_execucao": prox,
        "deadline": deadline,
        "hora_diaria": str(st.get("hora") or "00:30") if st.get("modo") == "diario" else None,
    }


def _ultimo_mutirao(db_jobs: Path) -> dict[str, Any] | None:
    jm.preparar_banco_jobs(db_jobs)
    con = jm.conectar_jobs(db_jobs)
    row = con.execute(
        "SELECT * FROM jobs WHERE tipo IN (?,?,?) ORDER BY id DESC LIMIT 1",
        (jm.TIPO_ITENS_MUTIRAO, jm.TIPO_ITENS_HIBRIDO, jm.TIPO_RESULTADOS_PNCP),
    ).fetchone()
    con.close()
    return dict(row) if row else None


def _cooldown_necessario(
    db_jobs: Path, agora: datetime, ativado_em: str | None = None
) -> tuple[bool, str, datetime | None]:
    j = _ultimo_mutirao(db_jobs)
    if not j or not j.get("finalizado_em"):
        return False, "", None
    fim = _dt_local(j.get("finalizado_em"))
    if fim is None:
        return False, "", None
    status = str(j.get("status") or "")
    try:
        exit_code = int(j.get("exit_code")) if j.get("exit_code") is not None else None
    except Exception:
        exit_code = None

    if (status == "INTERROMPIDA_USUARIO" or exit_code == 130) and agora - fim <= timedelta(hours=12):
        # V0.7.6: uma ativação explícita posterior ao stop manual funciona
        # como reconhecimento desse stop. Na 0.6.6.0, o controlador via o
        # último job interrompido nas 12h anteriores e se desativava de novo
        # imediatamente, mesmo após o usuário clicar em Ativar.
        ativacao = _dt_local(ativado_em)
        if ativacao is None or fim > ativacao:
            return True, "Mutirão foi interrompido manualmente; retomada persistente será desativada.", None
    minutos = None
    motivo = ""
    if exit_code == 75:
        if str(j.get("tipo") or "") == jm.TIPO_ITENS_HIBRIDO:
            minutos = COOLDOWN_COMPRASGOV_MIN
            motivo = "Cooldown após instabilidade do Compras.gov"
        else:
            minutos = COOLDOWN_INSTABILIDADE_MIN
            motivo = "Cooldown após instabilidade do PNCP"
    elif exit_code == 77:
        minutos = COOLDOWN_SEM_PROGRESSO_MIN
        motivo = "Cooldown após lote sem progresso"
    if minutos is None:
        return False, "", None
    ate = fim + timedelta(minutes=minutos)
    return agora < ate, motivo, ate


def turno_resultados(db_compras,db_jobs,cfg,agora):
    """Alterna lotes com itens; se só houver resultados, continua após 60 s.

    A COLETA, manutenção, FTS e horário de pesquisa já foram verificados pelo
    controlador. Falhas inesperadas recebem cooldown para evitar uma rajada de jobs.
    """
    # Configurações antigas/test fixtures ainda não possuem a coluna nova;
    # a migração de ``garantir_estrutura`` grava 1 explicitamente. Até lá,
    # não devemos deslocar um mutirão de itens existente para a nova rota.
    if not cfg.get('resultados_ativo',0): return False
    # Durante a inicialização do controlador a base pode ainda não existir;
    # nesse caso não há contratação a descobrir e a rota de itens legada deve
    # conservar o comportamento de aguardar a criação da base.
    if not Path(db_compras).is_file(): return False
    import resultados_pncp as rp
    s=rp.resumo(db_compras)
    if not rp.precisa_sessao(s,agora.isoformat(timespec='seconds')): return False
    ultimo=_ultimo_mutirao(db_jobs)
    if ultimo and ultimo['tipo']==jm.TIPO_RESULTADOS_PNCP:
        fim=_dt_local(ultimo.get('finalizado_em')) or agora
        segundos=900 if ultimo['status']=='ERRO' else 60
        return (agora-fim).total_seconds()>=segundos
    return True


def _resumo_backlog(db_compras: Path) -> dict[str, Any]:
    con = sqlite3.connect(str(db_compras), timeout=60)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON")
    con.execute("PRAGMA busy_timeout=60000")
    try:
        ii.garantir_estrutura(con)
        return ii.resumo_fila(con)
    finally:
        con.close()



def _resumo_hibrido(db_compras: Path) -> dict[str, Any]:
    con = sqlite3.connect(str(db_compras), timeout=60)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON")
    con.execute("PRAGMA busy_timeout=60000")
    try:
        ii.garantir_estrutura(con)
        ih.garantir_estrutura(con)
        return ih.resumo_controlador(con)
    finally:
        con.close()


def _rotacionar_log(caminho: Path, limite: int = 5_000_000) -> None:
    try:
        if caminho.exists() and caminho.stat().st_size > limite:
            ant = caminho.with_suffix(caminho.suffix + ".1")
            ant.unlink(missing_ok=True)
            caminho.replace(ant)
    except OSError:
        pass


def executar_controlador(
    *, db_compras: Path = DB_COMPRAS, db_jobs: Path = DB_JOBS,
    log_dir: Path = LOG_DIR, base_dir: Path | None = None,
    check_seconds: int = 30,
) -> int:
    base_dir = Path(base_dir or Path(__file__).resolve().parent).resolve()
    log_dir = Path(log_dir); log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "mutirao_persistente_controller.log"
    _rotacionar_log(log_path)

    def log(msg: str) -> None:
        linha = f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}"
        try:
            with log_path.open("a", encoding="utf-8") as f:
                f.write(linha + "\n")
        except OSError:
            pass

    garantir_estrutura(db_jobs)
    pid = os.getpid()
    atualizar_estado(db_jobs, controller_pid=pid, estado="INICIANDO", detalhe="Controlador persistente iniciado.", ultima_acao_em=agora_iso())
    log(f"Controlador iniciado · pid {pid} · base {base_dir}")
    ultimo_estado = None
    ultima_retomada_agendas = 0.0

    while True:
        from atualizar_radar_pncp import em_atualizacao
        if em_atualizacao(db_jobs):
            log('Atualização do Radar PNCP em andamento; controlador encerrando sem interromper os jobs.')
            return 0
        cfg = ler_config(db_jobs)
        if not int(cfg.get("ativo") or 0):
            atualizar_estado(db_jobs, controller_pid=pid, estado="INATIVO", detalhe="Retomada automática desativada.", ultima_acao_em=agora_iso())
            log("Configuração desativada; controlador encerrando.")
            return 0

        if (Path(db_jobs).parent/'radar_pncp_agendas_pendentes.json').exists():
            log('Aguardando reposição das agendas que venceram durante a atualização.')
            if time.monotonic()-ultima_retomada_agendas >= 300:
                from retomar_agendas import iniciar
                iniciar(db_jobs,base_dir)
                ultima_retomada_agendas=time.monotonic()
            time.sleep(max(10, int(check_seconds)))
            continue

        agora = datetime.now()
        ativo = jm.obter_job_ativo(db_jobs)
        if ativo is not None:
            tipo = str(ativo.get("tipo") or "job")
            novo_estado = ("AGUARDANDO_JOB", f"Job #{ativo.get('id')} ({tipo}) está ativo.")
            atualizar_estado(db_jobs, controller_pid=pid, estado=novo_estado[0], detalhe=novo_estado[1], ultimo_job_id=int(ativo.get("id") or 0))
            if novo_estado != ultimo_estado:
                log(novo_estado[1]); ultimo_estado = novo_estado
            time.sleep(max(10, int(check_seconds)))
            continue

        # COLETA agendada continua acima de qualquer retomada parcial. Assim uma
        # janela antiga não inicia dentro da margem de segurança da próxima
        # diária/revisão semanal.
        prioridade = estado_prioridades(base_dir, int(cfg.get("margem_minutos") or 60), agora)
        prox = prioridade.get("proxima_execucao")
        deadline = prioridade.get("deadline")
        if prioridade.get("bloqueado"):
            novo_estado = ("AGUARDANDO_COLETA", str(prioridade.get("motivo") or "Aguardando janela segura."))
            atualizar_estado(
                db_jobs, controller_pid=pid, estado=novo_estado[0], detalhe=novo_estado[1],
                proximo_prioritario=(f"{prioridade.get('nome')} · {prox:%d/%m/%Y %H:%M}" if isinstance(prox, datetime) else prioridade.get("nome")),
                deadline=(deadline.isoformat(timespec="seconds") if isinstance(deadline, datetime) else None),
            )
            if novo_estado != ultimo_estado:
                log(novo_estado[1]); ultimo_estado = novo_estado
            time.sleep(max(15, int(check_seconds)))
            continue

        # V0.7.6: uma COLETA parcial continua sendo prioridade. O controlador
        # aguarda um cooldown curto e retoma a mesma janela/checkpoint antes de
        # devolver a máquina ao mutirão. Interrupção explícita do usuário nunca
        # é retomada automaticamente.
        parcial = jm.obter_coleta_parcial_para_retomada_automatica(db_jobs)
        if parcial:
            fim_parcial = _dt_local(parcial.get("finalizado_em")) or agora
            retry_em = fim_parcial + timedelta(minutes=COOLDOWN_COLETA_PARCIAL_MIN)
            if agora < retry_em:
                detalhe = (f"COLETA parcial job #{parcial.get('id')} aguardando retomada automática às "
                           f"{retry_em:%H:%M}; mutirão permanece pausado.")
                novo_estado = ("AGUARDANDO_COLETA_PARCIAL", detalhe)
                atualizar_estado(db_jobs, controller_pid=pid, estado=novo_estado[0], detalhe=novo_estado[1])
                if novo_estado != ultimo_estado:
                    log(detalhe); ultimo_estado = novo_estado
                time.sleep(max(15, min(int((retry_em-agora).total_seconds()), 60)))
                continue
            try:
                jid_coleta = jm.iniciar_retomada_coleta(int(parcial["id"]), db_compras, db_jobs, log_dir)
            except RuntimeError as exc:
                detalhe = f"COLETA parcial aguardando vaga para retomada: {exc}"
                atualizar_estado(db_jobs, controller_pid=pid, estado="AGUARDANDO_JOB", detalhe=detalhe)
                time.sleep(max(10, int(check_seconds)))
                continue
            except Exception as exc:
                detalhe = f"Falha ao retomar COLETA parcial: {type(exc).__name__}: {exc}"
                atualizar_estado(db_jobs, controller_pid=pid, estado="ERRO_COLETA_PARCIAL", detalhe=detalhe, ultima_acao_em=agora_iso())
                log(detalhe); time.sleep(max(60, int(check_seconds))); continue
            detalhe = f"COLETA parcial job #{parcial.get('id')} retomada automaticamente como job #{jid_coleta}."
            atualizar_estado(db_jobs, controller_pid=pid, estado="COLETA_RETORNANDO", detalhe=detalhe, ultimo_job_id=jid_coleta, ultima_acao_em=agora_iso())
            log(detalhe); ultimo_estado=("COLETA_RETORNANDO",detalhe)
            time.sleep(max(10, int(check_seconds)))
            continue


        em_cooldown, motivo_cd, ate_cd = _cooldown_necessario(db_jobs, agora, cfg.get("atualizado_em"))
        if em_cooldown:
            if ate_cd is None and "interrompido manualmente" in motivo_cd:
                definir_config(ativo=False, db_jobs=db_jobs)
                atualizar_estado(db_jobs, controller_pid=pid, estado="PAUSADO_USUARIO", detalhe=motivo_cd, ultima_acao_em=agora_iso())
                log(motivo_cd)
                return 0
        # Sem job e dentro da janela segura: a rota rápida vem sempre primeiro.
        if not isinstance(deadline, datetime):
            atualizar_estado(db_jobs, controller_pid=pid, estado="PAUSADO_SEM_COLETA", detalhe="Não foi possível calcular deadline do próximo job prioritário.")
            time.sleep(max(15, int(check_seconds)))
            continue

        import fts_manager as fm
        estado_fts = fm.status_indice(db_compras, DB_FTS)
        if estado_fts.get('estado') != 'ATUALIZADO':
            ultimos = jm.listar_jobs(db_jobs, limite=20)
            anterior_fts = next((j for j in ultimos if j['tipo'] == jm.TIPO_INDICE_FTS), None)
            fim_fts = _dt_local(anterior_fts.get('finalizado_em')) if anterior_fts else None
            falha_recente = anterior_fts and anterior_fts.get('status') in ('ERRO','INTERROMPIDA_USUARIO')
            cfg_em = _dt_local(cfg.get('atualizado_em'))
            pausa_usuario = anterior_fts and anterior_fts.get('status') == 'INTERROMPIDA_USUARIO' and (not cfg_em or not fim_fts or cfg_em <= fim_fts)
            if pausa_usuario:
                definir_config(ativo=False, db_jobs=db_jobs)
                atualizar_estado(db_jobs, estado='PAUSADO_USUARIO', detalhe='Sincronização do índice interrompida pelo usuário. Reative o controlador para continuar.')
                return 0
            if not (falha_recente and fim_fts and (agora-fim_fts).total_seconds()<1800):
                try:
                    jid = jm.iniciar_indice_fts('update', db_compras, DB_FTS, db_jobs, log_dir,
                                               deadline_iso=deadline.isoformat(timespec='seconds'))
                    log(f'Job #{jid} de sincronização FTS iniciado antes do histórico.')
                    atualizar_estado(db_jobs, estado='SINCRONIZANDO_PESQUISA', ultimo_job_id=jid,
                                     detalhe='Índice de pesquisa em atualização; coleta histórica aguarda esta sessão.')
                    time.sleep(max(10,int(check_seconds)))
                    continue
                except Exception as exc:
                    log(f'Não foi possível iniciar sincronização FTS: {exc}')

            # Índice pendente sempre bloqueia mais ingestão, inclusive em erro.
            detalhe = 'Índice ainda pendente; coleta aguarda sincronização ou nova tentativa após erro.'
            atualizar_estado(db_jobs, estado='AGUARDANDO_INDICE', detalhe=detalhe)
            if ultimo_estado != ('AGUARDANDO_INDICE', detalhe):
                log(detalhe); ultimo_estado = ('AGUARDANDO_INDICE', detalhe)
            time.sleep(max(15, min(int(check_seconds), 60)))
            continue

        reservada, inicio_pesquisa, fim_pesquisa = janela_pesquisa(cfg, datetime.now())
        if reservada:
            detalhe = (f'Horário reservado à pesquisa até {fim_pesquisa:%H:%M}; '
                       'mutirão aguarda. COLETA agendada mantém prioridade.')
            atualizar_estado(db_jobs, estado='HORARIO_PESQUISA', detalhe=detalhe)
            if ultimo_estado != ('HORARIO_PESQUISA', detalhe):
                log(detalhe); ultimo_estado = ('HORARIO_PESQUISA', detalhe)
            time.sleep(max(15, min(int(check_seconds), 60)))
            continue
        if inicio_pesquisa is not None:
            deadline = min(deadline, inicio_pesquisa)

        # Pausas de rede não impedem sincronizar os dados já recebidos.
        if em_cooldown:
            detalhe = f"{motivo_cd}; retomada após {ate_cd:%H:%M}." if ate_cd else motivo_cd
            novo_estado = ("COOLDOWN", detalhe)
            atualizar_estado(db_jobs, controller_pid=pid, estado=novo_estado[0], detalhe=novo_estado[1])
            if novo_estado != ultimo_estado:
                log(detalhe); ultimo_estado = novo_estado
            time.sleep(max(15, min(int(check_seconds), 60)))
            continue

        # Fatias curtas evitam que um lote histórico de itens monopolize a
        # máquina enquanto resultados recentes aguardam a sua vez.
        try:
            if turno_resultados(db_compras,db_jobs,cfg,datetime.now()):
                deadline_resultados=min(deadline,datetime.now()+timedelta(minutes=20))
                jid=jm.iniciar_resultados_pncp(db_compras,db_jobs,log_dir,deadline_resultados.isoformat(timespec='seconds'))
                detalhe=f'Job #{jid}: resultados PNCP, dos recentes aos antigos; sessão de até 20 minutos.'
                atualizar_estado(db_jobs,estado='RESULTADOS_PNCP',detalhe=detalhe,ultimo_job_id=jid,
                                 ultima_acao_em=agora_iso(),deadline=deadline_resultados.isoformat(timespec='seconds'))
                log(detalhe)
                time.sleep(max(10,int(check_seconds)))
                continue
        except Exception as exc:
            log(f'Resultados PNCP aguardando próxima oportunidade: {type(exc).__name__}: {exc}')

        h = _resumo_hibrido(db_compras)
        candidatos_rapidos = int(h.get("candidatos_rapidos",0))
        transitorios = int(h.get("transitorios",0))
        proxima_retry = _dt_local(h.get("proxima_retry"))

        if candidatos_rapidos > 0:
            try:
                jid = jm.iniciar_itens_hibrido(
                    int(cfg.get("hibrido_lote") or 5000), int(cfg.get("hibrido_workers") or 6),
                    int(cfg.get("hibrido_rpm") or 95), db_compras, db_jobs, log_dir,
                    deadline.isoformat(timespec="seconds"),
                )
            except RuntimeError as exc:
                detalhe = f"Outro job ganhou prioridade antes da rota rápida: {exc}"
                atualizar_estado(db_jobs, controller_pid=pid, estado="AGUARDANDO_JOB", detalhe=detalhe)
                if ("AGUARDANDO_JOB", detalhe) != ultimo_estado:
                    log(detalhe); ultimo_estado=("AGUARDANDO_JOB",detalhe)
                time.sleep(max(10,int(check_seconds)))
                continue
            except Exception as exc:
                detalhe=f"Falha ao iniciar rota rápida Compras.gov: {type(exc).__name__}: {exc}"
                atualizar_estado(db_jobs,controller_pid=pid,estado="ERRO",detalhe=detalhe,ultima_acao_em=agora_iso())
                log(detalhe); time.sleep(max(30,int(check_seconds))); continue

            detalhe=(f"Job #{jid} híbrido iniciado · lote {int(cfg.get('hibrido_lote') or 5000):,} · "
                     f"{int(cfg.get('hibrido_rpm') or 95)} req/min · deadline {deadline:%d/%m %H:%M}.").replace(",",".")
            atualizar_estado(db_jobs,controller_pid=pid,estado="HIBRIDO_ATIVO",detalhe=detalhe,ultimo_job_id=jid,
                             ultima_acao_em=agora_iso(),
                             proximo_prioritario=(f"{prioridade.get('nome')} · {prox:%d/%m/%Y %H:%M}" if isinstance(prox,datetime) else prioridade.get("nome")),
                             deadline=deadline.isoformat(timespec="seconds"))
            log(detalhe); ultimo_estado=("HIBRIDO_ATIVO",detalhe)
            time.sleep(max(10,int(check_seconds)))
            continue

        # Se há erros transitórios aguardando sua janela, não os convertemos em PNCP
        # só porque o Compras.gov pediu cooldown.
        if transitorios > 0 and proxima_retry is not None and proxima_retry > agora:
            detalhe=f"{transitorios:,} validação(ões) Compras.gov aguardando retry até {proxima_retry:%H:%M}; fallback PNCP não será antecipado.".replace(",",".")
            novo_estado=("AGUARDANDO_COMPRASGOV",detalhe)
            atualizar_estado(db_jobs,controller_pid=pid,estado=novo_estado[0],detalhe=novo_estado[1])
            if novo_estado != ultimo_estado:
                log(detalhe); ultimo_estado=novo_estado
            espera=max(15,min(int((proxima_retry-agora).total_seconds()),60))
            time.sleep(espera)
            continue

        # Rota rápida esgotada: só agora o PNCP assume fallbacks, contratos sem
        # idCompra/evidência local e revalidações formais.
        fila = _resumo_backlog(db_compras)
        trabalho = int(fila.get("backlog_elegivel",0)) + int(fila.get("revalidacoes_pendentes",0))
        ativos_fila = int(fila.get("pendentes",0)) + int(fila.get("em_andamento",0)) + int(fila.get("erro_transitorio",0))
        if trabalho <= 0 and ativos_fila <= 0:
            detalhe=(f"Manutenção em dia neste momento: rota rápida esvaziada e nenhum fallback PNCP operacional restante. "
                     f"Validados Compras.gov pendentes de certificação formal: {int(h.get('validados',0)):,}.").replace(",",".")
            atualizar_estado(db_jobs,controller_pid=pid,estado="AGUARDANDO_ATUALIZACOES",detalhe=detalhe,ultima_acao_em=agora_iso())
            if ultimo_estado != ("CONCLUIDO", detalhe):
                log(detalhe); ultimo_estado=("CONCLUIDO", detalhe)
            time.sleep(max(30,int(check_seconds)))
            continue

        try:
            jid = jm.iniciar_itens_mutirao(
                int(cfg.get("lote_contratacoes") or 5000), int(cfg.get("lote_paginas") or 8000),
                "00:30", int(cfg.get("margem_minutos") or 60), False,
                db_compras, db_jobs, log_dir, deadline.isoformat(timespec="seconds"),
            )
        except RuntimeError as exc:
            detalhe=f"Outro job ganhou prioridade antes do fallback PNCP: {exc}"
            atualizar_estado(db_jobs,controller_pid=pid,estado="AGUARDANDO_JOB",detalhe=detalhe)
            if ("AGUARDANDO_JOB",detalhe)!=ultimo_estado:
                log(detalhe); ultimo_estado=("AGUARDANDO_JOB",detalhe)
            time.sleep(max(10,int(check_seconds))); continue
        except Exception as exc:
            detalhe=f"Falha ao iniciar fallback PNCP: {type(exc).__name__}: {exc}"
            atualizar_estado(db_jobs,controller_pid=pid,estado="ERRO",detalhe=detalhe,ultima_acao_em=agora_iso())
            log(detalhe); time.sleep(max(30,int(check_seconds))); continue

        detalhe=(f"Job #{jid} fallback PNCP iniciado · lote {int(cfg.get('lote_contratacoes') or 5000):,}/"
                 f"{int(cfg.get('lote_paginas') or 8000):,} · deadline {deadline:%d/%m %H:%M}.").replace(",",".")
        atualizar_estado(db_jobs,controller_pid=pid,estado="PNCP_FALLBACK_ATIVO",detalhe=detalhe,ultimo_job_id=jid,
                         ultima_acao_em=agora_iso(),
                         proximo_prioritario=(f"{prioridade.get('nome')} · {prox:%d/%m/%Y %H:%M}" if isinstance(prox,datetime) else prioridade.get("nome")),
                         deadline=deadline.isoformat(timespec="seconds"))
        log(detalhe); ultimo_estado=("PNCP_FALLBACK_ATIVO",detalhe)
        time.sleep(max(10,int(check_seconds)))


def main() -> int:
    ap = argparse.ArgumentParser(description="Radar PNCP V0.7.6 — controlador persistente do mutirão de itens")
    ap.add_argument("--db", type=Path, default=DB_COMPRAS)
    ap.add_argument("--jobs-db", type=Path, default=DB_JOBS)
    ap.add_argument("--log-dir", type=Path, default=LOG_DIR)
    ap.add_argument("--base-dir", type=Path, default=Path(__file__).resolve().parent)
    ap.add_argument("--check-seconds", type=int, default=30)
    args = ap.parse_args()
    return executar_controlador(
        db_compras=args.db, db_jobs=args.jobs_db, log_dir=args.log_dir,
        base_dir=args.base_dir, check_seconds=max(10, args.check_seconds),
    )


if __name__ == "__main__":
    raise SystemExit(main())
