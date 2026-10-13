"""Adaptador Windows do atualizador. Não encerra workers nem altera gatilhos."""
from __future__ import annotations
import json
import os
import re
import socket
import subprocess
import sys
import time
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path
from coleta_scheduler import _executar_ps

TAREFAS = {'RADAR_PNCP_COLETA_DIARIA':'coleta_diaria_launcher.py',
           'RADAR_PNCP_COLETA_REVISAO_SEMANAL':'coleta_diaria_launcher.py',
           'RADAR_PNCP_MUTIRAO_ITENS_PERSISTENTE':'mutirao_persistente.py'}
NS={'t':'http://schemas.microsoft.com/windows/2004/02/mit/task'}


def executar(script, args):
    script="[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding($false)\n$OutputEncoding = [Console]::OutputEncoding\n"+script
    cp=_executar_ps(script,[str(a) for a in args],timeout=60)
    if cp.returncode:
        raise RuntimeError((cp.stderr or cp.stdout or 'Falha no Agendador do Windows').strip())
    return cp.stdout.strip()


def acao(xml):
    root=ET.fromstring(xml)
    actions=root.find('t:Actions',NS)
    if actions is None or len(actions)!=1 or not actions[0].tag.endswith('}Exec'):
        raise ValueError('A tarefa deve ter uma única ação de execução; configuração não reconhecida.')
    node=actions[0]
    return {k:node.findtext('t:'+k,default='',namespaces=NS) for k in ('Command','Arguments','WorkingDirectory')}


def pasta_tarefa(task):
    a=acao(task['xml'])
    if re.search(r'(?:^|\s)--(?:db|jobs-db|log-dir)(?:[=\s]|$)',a['Arguments']):
        raise ValueError('Agenda com caminhos de dados personalizados: migração automática indisponível.')
    scripts=re.findall(r'"([^"\r\n]+\.py)"|([^\s"]+\.py)',a['Arguments'],re.I)
    paths=[Path(x or y) for x,y in scripts]
    expected=TAREFAS[task['name']]
    if len(paths)!=1 or paths[0].name.lower()!=expected or not paths[0].is_absolute():
        raise ValueError('Ação não reconhecida em '+task['name']+'. Nenhuma alteração realizada.')
    return paths[0].parent.resolve()


class Windows:
    def snapshot(self):
        if os.name!='nt':raise RuntimeError('Execute Atualizar_RADAR_PNCP.bat no Windows onde o Radar PNCP está instalada.')
        raw=executar(r'''
$ErrorActionPreference='Stop'
$out=@()
foreach($name in $args){
 $ts=@(Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue)
 if($ts.Count -gt 1){throw "Mais de uma tarefa com o nome $name"}
 if($ts.Count -eq 1){
  $t=$ts[0]; $i=$t | Get-ScheduledTaskInfo
  $out+=@{name=$name;path=$t.TaskPath;enabled=[bool]$t.Settings.Enabled;
   next_run=$(if($null -ne $i -and $null -ne $i.NextRunTime -and $i.NextRunTime.Year -gt 1900){$i.NextRunTime.ToString('o')}else{$null});xml=($t | Export-ScheduledTask)}
 }
}
ConvertTo-Json -InputObject @($out) -Compress -Depth 5
''',list(TAREFAS))
        return json.loads(raw or '[]')

    def enable(self, task, enabled):
        executar(r'''
$ErrorActionPreference='Stop'
if($args[2] -eq '1'){Enable-ScheduledTask -TaskName $args[0] -TaskPath $args[1] | Out-Null}
else{Disable-ScheduledTask -TaskName $args[0] -TaskPath $args[1] | Out-Null}
''',[task['name'],task['path'],int(enabled)])

    def action(self, task, target=None):
        a=acao(task['xml'])
        if target is not None:
            old=pasta_tarefa(task)
            a['Arguments']=re.sub(re.escape(str(old)),lambda _:str(target),a['Arguments'],flags=re.I)
            a['WorkingDirectory']=str(target)
        # Set-ScheduledTask modifica somente a ação. Mantém principal, gatilhos,
        # intervalos, limites, permissões e estado de ativação existentes.
        executar(r'''
$ErrorActionPreference='Stop'
$params=@{Execute=$args[2];Argument=$args[3]}
if($args[4]){$params.WorkingDirectory=$args[4]}
$a=New-ScheduledTaskAction @params
Set-ScheduledTask -TaskName $args[0] -TaskPath $args[1] -Action $a | Out-Null
''',[task['name'],task['path'],a['Command'],a['Arguments'],a['WorkingDirectory']])

    def start_task(self, task):
        executar("$ErrorActionPreference='Stop'; Start-ScheduledTask -TaskName $args[0] -TaskPath $args[1]",[task['name'],task['path']])

    def processes(self):
        # Projeta valores simples; não serializa objetos CIM nem chama
        # CreationDate.ToString(), que falhou no Windows de Alan.
        try:raw=executar(r'''
$ErrorActionPreference='Stop'
$out=@()
foreach($item in @(Get-CimInstance Win32_Process -Filter "Name LIKE 'python%.exe'" -Property ProcessId,Name,CommandLine)){
 $name=[string]$item.CimInstanceProperties['Name'].Value
 if($name -notmatch '^python(w|[0-9.]*)?\.exe$'){continue}
 $out+=@{ProcessId=[int]$item.CimInstanceProperties['ProcessId'].Value;
         CommandLine=[string]$item.CimInstanceProperties['CommandLine'].Value}
}
ConvertTo-Json -InputObject @($out) -Compress
''',[])
        except RuntimeError as exc:raise RuntimeError('Consulta dos processos do Radar PNCP falhou: '+str(exc)) from exc
        from processos_windows import inicio
        records=json.loads(raw or '[]')
        for p in records:p['Created']=inicio(p['ProcessId'])
        return records

    def running(self, base):
        # Só considera caminhos absolutos completos da instalação, não nomes
        # genéricos de processo. Instalações antigas com app.py relativo ficam
        # com tela de redirecionamento; não são encerradas por suposição.
        return [p for p in self.processes() if str(base).casefold() in (p.get('CommandLine') or '').casefold()]

    def stop_ui(self, base):
        for p in self.running(base):
            cmd=p.get('CommandLine') or ''
            if not re.search(r'\bstreamlit\s+run\s+"?'+re.escape(str(base/'app.py'))+r'"?(?:\s|$)',cmd,re.I):
                continue
            from processos_windows import encerrar_interface
            encerrar_interface(p['ProcessId'],p['Created'])

    def start_ui(self, base, port):
        from radar_pncp_layout import raiz_da_instalacao
        log=raiz_da_instalacao(base)/'radar_dados'/'logs'/'interface_atualizacao.log'
        log.parent.mkdir(parents=True,exist_ok=True)
        with log.open('ab') as f:
            proc=subprocess.Popen([sys.executable,'-m','streamlit','run',str(base/'app.py'),
                    '--server.port',str(port),'--server.address','127.0.0.1','--server.headless','true'],
                    cwd=base,stdin=subprocess.DEVNULL,stdout=f,stderr=f,
                    creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
        end=time.monotonic()+45
        while time.monotonic()<end:
            if proc.poll() is not None:raise RuntimeError('A nova interface encerrou. Consulte '+str(log))
            try:
                with urllib.request.urlopen(f'http://127.0.0.1:{port}/_stcore/health',timeout=1) as r:
                    if r.status==200:return
            except (OSError,ValueError):pass
            time.sleep(.5)
        # UI apenas; nunca encerra a árvore de processos nem um worker.
        proc.terminate()
        raise RuntimeError('A interface não ficou pronta em 45 segundos. Consulte '+str(log))


def porta_livre(preferida=8501):
    for port in range(preferida,preferida+20):
        with socket.socket() as s:
            try:s.bind(('127.0.0.1',port));return port
            except OSError:pass
    raise RuntimeError('Nenhuma porta livre para abrir a interface.')
