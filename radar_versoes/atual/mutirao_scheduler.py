#!/usr/bin/env python3
"""Agendamento do controlador persistente de itens — Radar PNCP V0.7.6."""
from __future__ import annotations

import json
import os
import platform
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

TASK_MUTIRAO = "RADAR_PNCP_MUTIRAO_ITENS_PERSISTENTE"


def disponivel() -> bool:
    return os.name == "nt" or platform.system().lower().startswith("win")


def _powershell() -> str:
    return "powershell.exe"


def _subprocess_sem_janela() -> dict[str, int]:
    """Evita flashes de PowerShell/CMD quando chamado por pythonw no Windows."""
    if os.name == "nt":
        return {"creationflags": int(getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000))}
    return {}


def _executar_ps(script: str, args: list[str], timeout: int = 30) -> subprocess.CompletedProcess[str]:
    tmp_path: str | None = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".ps1", encoding="utf-8-sig", delete=False) as tmp:
            tmp.write(script)
            tmp_path = tmp.name
        return subprocess.run(
            [_powershell(), "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", tmp_path, *args],
            capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout, check=False, **_subprocess_sem_janela(),
        )
    finally:
        if tmp_path:
            try:
                Path(tmp_path).unlink(missing_ok=True)
            except OSError:
                pass


def configurar(base_dir: Path) -> dict[str, Any]:
    if not disponivel():
        raise RuntimeError("O mutirão persistente via Agendador está disponível apenas no Windows.")
    base_dir = Path(base_dir).resolve()
    launcher = base_dir / "mutirao_persistente.py"
    if not launcher.exists():
        raise FileNotFoundError(f"Controlador não encontrado: {launcher}")
    python_exe = Path(sys.executable).resolve()
    # V0.7.6: o controlador não precisa de console. Quando disponível,
    # pythonw.exe evita que uma janela CMD/Python fique aberta e garante que
    # fechar uma janela visível não mate o controlador persistente.
    pythonw = python_exe.with_name("pythonw.exe")
    controlador_exe = pythonw if pythonw.exists() else python_exe
    script = r'''
$ErrorActionPreference = "Stop"
$TaskName = $args[0]
$Python = $args[1]
$Launcher = $args[2]
$BaseDir = $args[3]
$UserId = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$Existente = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($null -ne $Existente) {
    try { Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue } catch {}
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}
$ActionArgs = '-u "' + $Launcher + '" --base-dir "' + $BaseDir + '"'
$Action = New-ScheduledTaskAction -Execute $Python -Argument $ActionArgs -WorkingDirectory $BaseDir
$Trigger = New-ScheduledTaskTrigger -AtLogOn -User $UserId
$Settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -MultipleInstances IgnoreNew -RestartCount 10 -RestartInterval (New-TimeSpan -Minutes 1) -ExecutionTimeLimit ([TimeSpan]::Zero)
$Principal = New-ScheduledTaskPrincipal -UserId $UserId -LogonType Interactive -RunLevel Limited
Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Settings $Settings -Principal $Principal -Description 'Radar PNCP;mutirao_hibrido_persistente=1;versao=0.6.6.4' | Out-Null
Enable-ScheduledTask -TaskName $TaskName | Out-Null
Start-ScheduledTask -TaskName $TaskName
Write-Output 'OK'
'''
    cp = _executar_ps(script, [TASK_MUTIRAO, str(controlador_exe), str(launcher), str(base_dir)])
    if cp.returncode != 0 or "OK" not in (cp.stdout or ""):
        raise RuntimeError((cp.stderr or cp.stdout or f"PowerShell retornou {cp.returncode}").strip())
    return obter_status(base_dir)


def remover() -> None:
    if not disponivel():
        return
    script = r'''
$ErrorActionPreference = "Stop"
$TaskName = $args[0]
$t = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($null -ne $t) {
    try { Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue } catch {}
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}
Write-Output 'OK'
'''
    cp = _executar_ps(script, [TASK_MUTIRAO])
    if cp.returncode != 0:
        raise RuntimeError((cp.stderr or cp.stdout or f"PowerShell retornou {cp.returncode}").strip())


def obter_status(base_dir: Path | None = None) -> dict[str, Any]:
    out = {
        "disponivel": disponivel(), "existe": False, "ativo": False,
        "estado": "INDISPONÍVEL" if not disponivel() else "NÃO CONFIGURADO",
        "ultima_execucao": None, "ultimo_resultado": None,
        "argumentos": None, "execute": None, "aponta_versao_atual": None, "erro": None,
    }
    if not disponivel():
        return out
    script = r'''
$ErrorActionPreference = "Stop"
$TaskName = $args[0]
$t = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($null -eq $t) { [pscustomobject]@{exists=$false} | ConvertTo-Json -Compress; exit 0 }
$i = Get-ScheduledTaskInfo -TaskName $TaskName -ErrorAction SilentlyContinue
$a = @($t.Actions)[0]
[pscustomobject]@{
    exists=$true
    state=[string]$t.State
    enabled=([string]$t.State -ne 'Disabled')
    last_run=if($i -and $i.LastRunTime -and $i.LastRunTime.Year -gt 1900){$i.LastRunTime.ToString('o')}else{$null}
    last_result=if($i){$i.LastTaskResult}else{$null}
    arguments=if($a){$a.Arguments}else{$null}
    execute=if($a){$a.Execute}else{$null}
} | ConvertTo-Json -Compress
'''
    try:
        cp = _executar_ps(script, [TASK_MUTIRAO])
        if cp.returncode != 0:
            raise RuntimeError((cp.stderr or cp.stdout or f"PowerShell retornou {cp.returncode}").strip())
        payload = json.loads((cp.stdout or "{}").strip().splitlines()[-1])
    except Exception as exc:
        out.update({"estado": "ERRO", "erro": f"{type(exc).__name__}: {exc}"})
        return out
    if not payload.get("exists"):
        return out
    argumentos = str(payload.get("arguments") or "")
    aponta = None
    if base_dir is not None:
        atual = str((Path(base_dir).resolve() / "mutirao_persistente.py")).casefold()
        aponta = atual in argumentos.casefold()
    out.update({
        "existe": True,
        "ativo": bool(payload.get("enabled")),
        "estado": str(payload.get("state") or "Desconhecido"),
        "ultima_execucao": payload.get("last_run"),
        "ultimo_resultado": payload.get("last_result"),
        "argumentos": argumentos,
        "execute": payload.get("execute"),
        "aponta_versao_atual": aponta,
    })
    return out
