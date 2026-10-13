#!/usr/bin/env python3
"""Gerenciamento dos agendamentos da COLETA pela interface — Radar PNCP V0.7.6.

A interface é o fluxo principal. O módulo usa o Agendador de Tarefas do Windows
por baixo dos panos e não exige que o usuário execute arquivos .bat/.ps1.
"""
from __future__ import annotations

import json
import os
import platform
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

TASK_DIARIA = "RADAR_PNCP_COLETA_DIARIA"
TASK_SEMANAL = "RADAR_PNCP_COLETA_REVISAO_SEMANAL"

DIAS_PT = {
    "Monday": "Segunda-feira",
    "Tuesday": "Terça-feira",
    "Wednesday": "Quarta-feira",
    "Thursday": "Quinta-feira",
    "Friday": "Sexta-feira",
    "Saturday": "Sábado",
    "Sunday": "Domingo",
}
DIAS_EN = {v: k for k, v in DIAS_PT.items()}


def disponivel() -> bool:
    return os.name == "nt" or platform.system().lower().startswith("win")


def _powershell() -> str:
    # Windows PowerShell 5.1 existe nas versões de Windows suportadas pelo projeto.
    return "powershell.exe"


def _subprocess_sem_janela() -> dict[str, int]:
    """Evita flashes de PowerShell/CMD quando chamado por pythonw no Windows."""
    if os.name == "nt":
        return {"creationflags": int(getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000))}
    return {}


def _executar_ps(script: str, args: list[str], timeout: int = 30) -> subprocess.CompletedProcess[str]:
    # -File é muito mais previsível do que -Command para caminhos Windows com espaços.
    tmp_path: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".ps1", encoding="utf-8-sig", delete=False
        ) as tmp:
            tmp.write(script)
            tmp_path = tmp.name
        cmd = [
            _powershell(), "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
            "-File", tmp_path, *args,
        ]
        return subprocess.run(
            cmd, capture_output=True, text=True, encoding="utf-8", errors="replace",
            timeout=timeout, check=False, **_subprocess_sem_janela(),
        )
    finally:
        if tmp_path:
            try:
                Path(tmp_path).unlink(missing_ok=True)
            except OSError:
                pass


def _task_name(modo: str) -> str:
    if modo == "diario":
        return TASK_DIARIA
    if modo == "revisao":
        return TASK_SEMANAL
    raise ValueError(f"Modo de agendamento inválido: {modo}")


def _extrair_dias_argumentos(argumentos: str | None) -> int | None:
    if not argumentos:
        return None
    m = re.search(r"(?:^|\s)--dias\s+(\d+)(?:\s|$)", argumentos)
    return int(m.group(1)) if m else None


def _extrair_modo_argumentos(argumentos: str | None) -> str | None:
    if not argumentos:
        return None
    m = re.search(r"(?:^|\s)--modo\s+(diario|revisao)(?:\s|$)", argumentos)
    return m.group(1) if m else None


def _normalizar_hora(start_boundary: str | None) -> str | None:
    if not start_boundary:
        return None
    # Ex.: 2026-09-07T00:30:00-03:00 ou 2026-09-07T00:30:00
    m = re.search(r"T(\d{2}:\d{2})", str(start_boundary))
    return m.group(1) if m else None


def _dia_en_por_mask(mask: int | str | None) -> str | None:
    try:
        valor = int(mask or 0)
    except (TypeError, ValueError):
        return None
    # TASK_TRIGGER_WEEKLY usa bitmask: Sunday=1, Monday=2, Tuesday=4, ... Saturday=64.
    mapa = [(2, "Monday"), (4, "Tuesday"), (8, "Wednesday"), (16, "Thursday"),
            (32, "Friday"), (64, "Saturday"), (1, "Sunday")]
    for bit, nome in mapa:
        if valor & bit:
            return nome
    return None


def obter_status(modo: str, base_dir: Path | None = None) -> dict[str, Any]:
    """Lê o estado real da tarefa no Windows Task Scheduler."""
    task = _task_name(modo)
    base = {
        "modo": modo, "task_name": task, "disponivel": disponivel(), "existe": False,
        "ativo": False, "estado": "INDISPONÍVEL" if not disponivel() else "NÃO CONFIGURADO",
        "proxima_execucao": None, "ultima_execucao": None, "ultimo_resultado": None,
        "hora": None, "dia_semana_en": None, "dia_semana": None, "janela_dias": None,
        "argumentos": None, "erro": None, "aponta_versao_atual": None,
    }
    if not disponivel():
        return base

    script = r'''
$ErrorActionPreference = "Stop"
$TaskName = $args[0]
$t = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($null -eq $t) {
    [pscustomobject]@{exists=$false} | ConvertTo-Json -Compress
    exit 0
}
$i = Get-ScheduledTaskInfo -TaskName $TaskName -ErrorAction SilentlyContinue
$a = @($t.Actions)[0]
$tr = @($t.Triggers)[0]
$days = $null
try { $days = $tr.DaysOfWeek } catch {}
[pscustomobject]@{
    exists=$true
    state=[string]$t.State
    enabled=([string]$t.State -ne 'Disabled')
    next_run=if($i -and $i.NextRunTime -and $i.NextRunTime.Year -gt 1900){$i.NextRunTime.ToString('o')}else{$null}
    last_run=if($i -and $i.LastRunTime -and $i.LastRunTime.Year -gt 1900){$i.LastRunTime.ToString('o')}else{$null}
    last_result=if($i){$i.LastTaskResult}else{$null}
    start_boundary=if($tr){$tr.StartBoundary}else{$null}
    days_of_week=$days
    arguments=if($a){$a.Arguments}else{$null}
    execute=if($a){$a.Execute}else{$null}
} | ConvertTo-Json -Compress -Depth 4
'''
    try:
        cp = _executar_ps(script, [task])
    except Exception as exc:
        base["erro"] = f"{type(exc).__name__}: {exc}"
        base["estado"] = "ERRO"
        return base
    if cp.returncode != 0:
        base["erro"] = (cp.stderr or cp.stdout or f"PowerShell retornou {cp.returncode}").strip()
        base["estado"] = "ERRO"
        return base
    try:
        payload = json.loads((cp.stdout or "{}").strip().splitlines()[-1])
    except Exception as exc:
        base["erro"] = f"Resposta inválida do Agendador: {exc}; saída={cp.stdout!r}"
        base["estado"] = "ERRO"
        return base
    if not payload.get("exists"):
        return base

    argumentos = payload.get("arguments")
    dia_en = _dia_en_por_mask(payload.get("days_of_week"))
    estado = str(payload.get("state") or "Desconhecido")
    ativo = bool(payload.get("enabled")) and estado.lower() != "disabled"
    aponta_atual = None
    if base_dir is not None and argumentos:
        atual = str((Path(base_dir).resolve() / "coleta_diaria_launcher.py")).casefold()
        aponta_atual = atual in str(argumentos).casefold()
    base.update({
        "existe": True,
        "ativo": ativo,
        "estado": estado,
        "proxima_execucao": payload.get("next_run"),
        "ultima_execucao": payload.get("last_run"),
        "ultimo_resultado": payload.get("last_result"),
        "hora": _normalizar_hora(payload.get("start_boundary")),
        "dia_semana_en": dia_en,
        "dia_semana": DIAS_PT.get(dia_en) if dia_en else None,
        "janela_dias": _extrair_dias_argumentos(argumentos),
        "argumentos": argumentos,
        "aponta_versao_atual": aponta_atual,
    })
    return base


def configurar(
    modo: str,
    hora: str,
    janela_dias: int,
    base_dir: Path,
    dia_semana: str | None = None,
) -> dict[str, Any]:
    """Cria ou atualiza o agendamento diretamente pelo Task Scheduler."""
    if not disponivel():
        raise RuntimeError("O gerenciamento automático de agendamentos está disponível apenas no Windows.")
    if not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", hora or ""):
        raise ValueError("Horário inválido. Use HH:mm.")
    janela_dias = int(janela_dias)
    if modo == "diario" and not 1 <= janela_dias <= 14:
        raise ValueError("A janela diária deve estar entre 1 e 14 dias.")
    if modo == "revisao" and not 7 <= janela_dias <= 60:
        raise ValueError("A janela semanal deve estar entre 7 e 60 dias.")
    if modo == "revisao":
        if not dia_semana:
            raise ValueError("Informe o dia da semana para a revisão semanal.")
        dia_en = DIAS_EN.get(dia_semana, dia_semana if dia_semana in DIAS_PT else None)
        if not dia_en:
            raise ValueError("Dia da semana inválido.")
    else:
        dia_en = ""

    base_dir = Path(base_dir).resolve()
    launcher = base_dir / "coleta_diaria_launcher.py"
    if not launcher.exists():
        raise FileNotFoundError(f"Launcher não encontrado: {launcher}")
    python_exe = Path(sys.executable).resolve()
    task = _task_name(modo)

    script = r'''
$ErrorActionPreference = "Stop"
$TaskName = $args[0]
$Modo = $args[1]
$Hora = $args[2]
$Janela = [int]$args[3]
$Dia = $args[4]
$Python = $args[5]
$Launcher = $args[6]
$BaseDir = $args[7]
$h = $Hora.Split(':')
$At = (Get-Date).Date.AddHours([int]$h[0]).AddMinutes([int]$h[1])
$ActionArgs = '-u "' + $Launcher + '" --dias ' + $Janela + ' --modo ' + $Modo
$Action = New-ScheduledTaskAction -Execute $Python -Argument $ActionArgs -WorkingDirectory $BaseDir
if ($Modo -eq 'diario') {
    $Trigger = New-ScheduledTaskTrigger -Daily -At $At
    $Limit = New-TimeSpan -Hours 8
} else {
    $Trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek $Dia -At $At
    $Limit = New-TimeSpan -Hours 12
}
$Settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -MultipleInstances IgnoreNew -ExecutionTimeLimit $Limit
$UserId = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$Principal = New-ScheduledTaskPrincipal -UserId $UserId -LogonType Interactive -RunLevel Limited
$Descricao = 'Radar PNCP COLETA;modo=' + $Modo + ';dias=' + $Janela
$Existente = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($null -ne $Existente) { Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false }
Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Settings $Settings -Principal $Principal -Description $Descricao | Out-Null
Enable-ScheduledTask -TaskName $TaskName | Out-Null
Write-Output 'OK'
'''
    cp = _executar_ps(script, [task, modo, hora, str(janela_dias), dia_en, str(python_exe), str(launcher), str(base_dir)])
    if cp.returncode != 0 or "OK" not in (cp.stdout or ""):
        msg = (cp.stderr or cp.stdout or f"PowerShell retornou {cp.returncode}").strip()
        raise RuntimeError(f"Não foi possível configurar o agendamento: {msg}")
    return obter_status(modo, base_dir)


def definir_ativo(modo: str, ativo: bool) -> dict[str, Any]:
    if not disponivel():
        raise RuntimeError("O gerenciamento automático de agendamentos está disponível apenas no Windows.")
    task = _task_name(modo)
    cmdlet = "Enable-ScheduledTask" if ativo else "Disable-ScheduledTask"
    script = rf'''
$ErrorActionPreference = "Stop"
$TaskName = $args[0]
$t = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($null -eq $t) {{ throw "Agendamento não encontrado: $TaskName" }}
{cmdlet} -TaskName $TaskName | Out-Null
Write-Output 'OK'
'''
    cp = _executar_ps(script, [task])
    if cp.returncode != 0 or "OK" not in (cp.stdout or ""):
        msg = (cp.stderr or cp.stdout or f"PowerShell retornou {cp.returncode}").strip()
        raise RuntimeError(f"Não foi possível {'ativar' if ativo else 'desativar'} o agendamento: {msg}")
    return obter_status(modo)


def remover(modo: str) -> None:
    if not disponivel():
        raise RuntimeError("O gerenciamento automático de agendamentos está disponível apenas no Windows.")
    task = _task_name(modo)
    script = r'''
$ErrorActionPreference = "Stop"
$TaskName = $args[0]
$t = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($null -ne $t) { Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false }
Write-Output 'OK'
'''
    cp = _executar_ps(script, [task])
    if cp.returncode != 0:
        msg = (cp.stderr or cp.stdout or f"PowerShell retornou {cp.returncode}").strip()
        raise RuntimeError(f"Não foi possível remover o agendamento: {msg}")
