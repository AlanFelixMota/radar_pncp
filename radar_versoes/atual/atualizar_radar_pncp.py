#!/usr/bin/env python3
"""Atualização assistida, com diário de recuperação e bloqueio de novos jobs.

Execute a partir do pacote extraído, ao lado de radar_dados e da instalação.
Os bancos de compras/FTS nunca são copiados ou substituídos pelo atualizador.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
import tomllib
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

# Entradas externas devem importar somente os módulos do motor estável.
sys.path.insert(0,str(Path(__file__).resolve().parent))
from atualizacao_windows import Windows, pasta_tarefa, porta_livre
from radar_pncp_layout import raiz_da_instalacao
from pacotes_radar_pncp import validar_manifesto, versao

VERSION='1.0.0'
ACTIVE=('AGUARDANDO','EXECUTANDO','PARADA_SOLICITADA')


def gravar(path, data):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_name(path.name+'.tmp')
    with temp.open('w',encoding='utf-8') as f:
        json.dump(data,f,ensure_ascii=False,indent=2);f.flush();os.fsync(f.fileno())
    os.replace(temp,path)


@contextmanager
def exclusividade(path):
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('a+b') as f:
        f.seek(0);f.write(b'0');f.flush();f.seek(0)
        try:
            if os.name=='nt':
                import msvcrt
                msvcrt.locking(f.fileno(),msvcrt.LK_NBLCK,1)
            else:
                import fcntl
                fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except OSError as exc:
            raise RuntimeError('Outro atualizador já está em execução.') from exc
        try:yield
        finally:
            f.seek(0)
            if os.name=='nt':msvcrt.locking(f.fileno(),msvcrt.LK_UNLCK,1)
            else:fcntl.flock(f,fcntl.LOCK_UN)


def validar_pacote(source, *, instalado=False):
    manifest=json.loads((source/'release.json').read_text(encoding='utf-8'))
    validar_manifesto(manifest)
    for name,digest in manifest['files'].items():
        rel=Path(name)
        if rel.is_absolute() or '..' in rel.parts or rel.suffix.lower() in ('.db','.sqlite','.sqlite3'):
            raise ValueError('Caminho inválido no pacote: '+name)
        path=source/rel
        if path.is_symlink() or not path.is_file():
            raise ValueError('Arquivo ausente ou modificado no pacote: '+name)
        data=path.read_bytes()
        # O ZIP é imutável. Na instalação, somente a configuração de aparência
        # é editável e já vinha sendo preservada pela troca de versão.
        if instalado and name=='.streamlit/config.toml':
            try:tomllib.loads(data.decode('utf-8-sig'))
            except (UnicodeError,tomllib.TOMLDecodeError) as exc:
                raise ValueError('Configuração Streamlit inválida: '+name) from exc
        elif hashlib.sha256(data).hexdigest()!=digest:
            raise ValueError('Arquivo ausente ou modificado no pacote: '+name)
    for required in ('app.py','job_manager.py','radar_paths.py','iniciar_interface.bat'):
        if required not in manifest['files']:raise ValueError('Pacote incompleto: '+required)
    return manifest


def preparar(source, stage, manifest):
    stage.mkdir()
    for name in manifest['files']:
        dst=stage/name;dst.parent.mkdir(parents=True,exist_ok=True)
        shutil.copy2(source/name,dst)
    shutil.copy2(source/'release.json',stage/'release.json')
    cp=subprocess.run([sys.executable,'-c',
        'import sys; sys.path.insert(0,sys.argv[1]); '
        'import requests,pandas,streamlit; import app,job_manager,mutirao_persistente; '
        'from pathlib import Path; '
        'assert Path(app.__file__).resolve().parent==Path(sys.argv[1]).resolve(), "Aplicacao importada da pasta errada"',str(stage)],
        cwd=stage,capture_output=True,text=True,timeout=60)
    if cp.returncode:raise RuntimeError('Verificação da nova versão falhou: '+cp.stderr[-4000:])


def pausar(db, token):
    # O snapshot da ativação e o bloqueio são atômicos no mesmo banco.
    with sqlite3.connect(db,timeout=30) as c:
        c.execute('BEGIN IMMEDIATE')
        c.execute('CREATE TABLE IF NOT EXISTS radar_pncp_atualizacao(id INTEGER PRIMARY KEY CHECK(id=1),token TEXT,ativo_anterior INTEGER)')
        if 'parada_legada' not in {r[1] for r in c.execute('PRAGMA table_info(radar_pncp_atualizacao)')}:
            c.execute('ALTER TABLE radar_pncp_atualizacao ADD COLUMN parada_legada INTEGER NOT NULL DEFAULT 0')
        if c.execute('SELECT 1 FROM radar_pncp_atualizacao').fetchone():
            raise RuntimeError('Existe uma atualização pendente de recuperação.')
        has_cfg=c.execute("SELECT 1 FROM sqlite_master WHERE name='mutirao_persistente_config'").fetchone()
        row=c.execute('SELECT ativo FROM mutirao_persistente_config WHERE id=1').fetchone() if has_cfg else None
        ativo=int(row[0]) if row else None
        c.execute('INSERT INTO radar_pncp_atualizacao(id,token,ativo_anterior) VALUES (1,?,?)',(token,ativo))
        if row:c.execute('UPDATE mutirao_persistente_config SET ativo=0 WHERE id=1')
        c.execute("""CREATE TRIGGER radar_pncp_bloquear_jobs BEFORE INSERT ON jobs
            BEGIN SELECT RAISE(ROLLBACK,'Radar PNCP em atualização. Aguarde a conclusão.'); END""")
        if has_cfg:
            c.execute("""CREATE TRIGGER radar_pncp_bloquear_config BEFORE UPDATE ON mutirao_persistente_config
                BEGIN SELECT RAISE(ROLLBACK,'Radar PNCP em atualização. Aguarde a conclusão.'); END""")
    return ativo


def liberar(db, token):
    with sqlite3.connect(db,timeout=30) as c:
        c.execute('BEGIN IMMEDIATE')
        if not c.execute("SELECT 1 FROM sqlite_master WHERE name='radar_pncp_atualizacao'").fetchone():return
        row=c.execute('SELECT token,ativo_anterior FROM radar_pncp_atualizacao WHERE id=1').fetchone()
        if not row:return
        if row[0]!=token:raise RuntimeError('O bloqueio pertence a outra atualização.')
        c.execute('DROP TRIGGER IF EXISTS radar_pncp_bloquear_config')
        c.execute('DROP TRIGGER IF EXISTS radar_pncp_bloquear_jobs')
        if row[1] is not None:c.execute('UPDATE mutirao_persistente_config SET ativo=? WHERE id=1',(row[1],))
        cols={r[1] for r in c.execute('PRAGMA table_info(radar_pncp_atualizacao)')}
        if row[1] and 'parada_legada' in cols and c.execute('SELECT parada_legada FROM radar_pncp_atualizacao WHERE id=1').fetchone()[0]:
            # Reconhece somente a parada autorizada nesta migração. Sem isso,
            # o controlador legado interpretaria o código 130 como pausa manual.
            c.execute('UPDATE mutirao_persistente_config SET atualizado_em=? WHERE id=1',
                      (datetime.now().isoformat(timespec='seconds'),))
        c.execute('DELETE FROM radar_pncp_atualizacao WHERE id=1')


def em_atualizacao(db):
    if not Path(db).exists():return False
    with sqlite3.connect(Path(db).resolve().as_uri()+'?mode=ro',uri=True,timeout=1) as c:
        exists=c.execute("SELECT 1 FROM sqlite_master WHERE name='radar_pncp_atualizacao'").fetchone()
        return bool(exists and c.execute('SELECT 1 FROM radar_pncp_atualizacao WHERE id=1').fetchone())


def jobs_ativos(db):
    from contextlib import closing
    with closing(sqlite3.connect(Path(db).resolve().as_uri()+'?mode=ro',uri=True,timeout=2)) as c:
        c.row_factory=sqlite3.Row
        return [dict(r) for r in c.execute('SELECT * FROM jobs WHERE status IN (?,?,?) ORDER BY id',ACTIVE)]


def detalhe_job(job):
    campos=('id','tipo','status','etapa','progresso_atual','progresso_total','mensagem')
    detalhe={k:job.get(k) for k in campos}
    prefixo='__RADAR_PNCP_ITENS_PROGRESSO__'
    msg=detalhe.get('mensagem') or ''
    if msg.startswith(prefixo):
        try:
            p=json.loads(msg[len(prefixo):])
            detalhe['mensagem']=(f"Páginas salvas: {p.get('sessao_paginas',0)} · Itens confirmados: {p.get('sessao_itens',0)}"
                + (f" · Página atual: {p['pagina_atual']}" if p.get('pagina_atual') else ''))
            detalhe['contrato_atual']=p.get('contrato_atual')
        except (ValueError,TypeError,AttributeError):
            detalhe['mensagem']='Progresso detalhado indisponível.'
    return detalhe


def oferecer_parada_legada(db, old, confirmar=input, log=print):
    """Migração de workers antigos: apenas por escolha explícita no console."""
    path=Path(old)/'release.json'
    manifest=json.loads(path.read_text(encoding='utf-8')) if path.exists() else {}
    if manifest.get('cooperative_pause')==1:return
    tipos=('ITENS_MUTIRAO','ITENS_HIBRIDO','INDICE_FTS')
    jobs=[j for j in jobs_ativos(db) if j.get('tipo') in tipos and not j.get('stop_requested')]
    if not jobs:return
    ids=[j['id'] for j in jobs]
    log('A instalação antiga não possui pausa cooperativa. Jobs: '+str(ids)+'.')
    log('Solicitar parada usa o mecanismo da versão antiga: encerra a coleta, preserva dados já gravados e pode exigir refazer trabalho ainda não confirmado.')
    if confirmar('Solicitar essa parada para concluir a atualização agora? [s/N]: ').strip().lower()!='s':return
    with sqlite3.connect(db,timeout=30) as c:
        if not c.execute('SELECT 1 FROM radar_pncp_atualizacao WHERE id=1').fetchone():
            raise RuntimeError('Manutenção não está ativa; nenhuma parada foi solicitada.')
        for jid in ids:
            changed=c.execute('UPDATE jobs SET stop_requested=1,status=?,mensagem=? WHERE id=? AND tipo IN (?,?,?) AND status IN (?,?,?)',
                      ('PARADA_SOLICITADA','Parada autorizada para migração do atualizador.',jid,*tipos,*ACTIVE))
            if changed.rowcount:c.execute('UPDATE radar_pncp_atualizacao SET parada_legada=1 WHERE id=1')
    log('Parada solicitada. Aguardando a confirmação do worker antes de trocar os arquivos.')


def aguardar(db, old, host, timeout, log=print, cancelar=None, observacao=None, token=None):
    inicio=time.monotonic()
    mudanca=inicio; assinatura=None; ultimo_log=-float('inf')
    deadline=time.monotonic()+timeout
    while True:
        if cancelar is not None and Path(cancelar).exists():raise InterruptedError('Atualização cancelada pela interface.')
        jobs=jobs_ativos(db)
        active=[(j['id'],j['status']) for j in jobs]
        others=[p for p in host.running(old) if any(x in (p.get('CommandLine') or '').lower()
                for x in ('job_worker.py','mutirao_persistente.py','coleta_diaria_launcher.py','retomar_agendas.py'))]
        now=time.monotonic()
        detalhes=[detalhe_job(j) for j in jobs]
        sig=repr(detalhes)+repr([p.get('ProcessId') for p in others])
        mudou=sig!=assinatura
        if mudou:assinatura=sig;mudanca=now
        if observacao:
            gravar(observacao,{'token':token,'atualizado_em':datetime.now().isoformat(timespec='seconds'),
                'espera_segundos':int(now-inicio),'sem_mudanca_segundos':int(now-mudanca),
                'jobs':detalhes,'processos':[{'pid':p.get('ProcessId'),'script':next((s for s in
                    ('job_worker.py','mutirao_persistente.py','coleta_diaria_launcher.py','retomar_agendas.py')
                    if s in (p.get('CommandLine') or '').lower()),'operação')} for p in others]})
        if not active and not others:return
        if mudou or now-ultimo_log>=30:
            log('Aguardando encerramento seguro. Jobs: '+str(active)+'; processos de operação: '+str(len(others))+
                f'. Espera: {int(now-inicio)}s; sem mudança de progresso: {int(now-mudanca)}s. Ctrl+C cancela e restaura a operação anterior.')
            for j in detalhes:
                log(f"  Job {j['id']} · {j['tipo']} · {j['etapa'] or 'aguardando'} · {j['progresso_atual'] or 0}/{j['progresso_total'] or '?'}")
                if j.get('mensagem'):log('  '+str(j['mensagem']))
            ultimo_log=now
        if time.monotonic()>=deadline:
            raise TimeoutError('Tempo de espera esgotado; nenhum job foi encerrado à força.')
        time.sleep(min(5,max(0,deadline-time.monotonic())))


def retomar(host, tasks, db, base, inicio, helper, controlador_manual=False):
    """Restaura ativação e enfileira agendas que venceram durante a manutenção."""
    pendentes=[]
    for task in tasks:
        if not task['enabled']:continue
        if task['name']!='RADAR_PNCP_MUTIRAO_ITENS_PERSISTENTE':
            try:
                due=datetime.fromisoformat(task['next_run']).replace(tzinfo=None)
                if datetime.fromisoformat(inicio)<=due<=datetime.now():pendentes.append(task['name'])
            except (ValueError,KeyError,TypeError):pass
    if pendentes:
        path=Path(db).parent/'radar_pncp_agendas_pendentes.json'
        previous=json.loads(path.read_text()) if path.exists() else []
        gravar(path,list(dict.fromkeys(previous+pendentes)))
    for task in tasks:host.enable(task,task['enabled'])
    if (Path(db).parent/'radar_pncp_agendas_pendentes.json').exists():
        subprocess.Popen([sys.executable,str(helper/'retomar_agendas.py'),'--db',str(db),'--base',str(base)],cwd=base,
                         stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,
                         **({'creationflags':getattr(subprocess,'CREATE_NO_WINDOW',0)} if os.name=='nt' else {'start_new_session':True}))
    for task in tasks:
        if task['enabled'] and task['name']=='RADAR_PNCP_MUTIRAO_ITENS_PERSISTENTE':host.start_task(task)
    if controlador_manual:
        subprocess.Popen([sys.executable,str(base/'mutirao_persistente.py'),'--base-dir',str(base)],cwd=base,
            stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,
            **({'creationflags':getattr(subprocess,'CREATE_NO_WINDOW',0)} if os.name=='nt' else {'start_new_session':True}))


def redirecionar(old, target, port, work):
    # A primeira migração pode ter uma interface antiga com caminho relativo.
    # Redireciona o app conhecido em disco; nunca mata outro Python por nome.
    for name in ('app.py','iniciar_interface.bat'):
        path=old/name
        if path.exists() and not (work/name).exists():shutil.copy2(path,work/name)
    (old/'app.py').write_text("import streamlit as st\nst.info('O Radar PNCP foi atualizada. Abra a instalação fixa para continuar.')\nst.link_button('Abrir Radar PNCP', "+repr(f'http://localhost:{port}')+")\nst.stop()\n",encoding='utf-8')
    (old/'iniciar_interface.bat').write_text('@echo off\nchcp 65001 >nul\ncall "'+str(target/'iniciar_interface.bat')+'"\n',encoding='utf-8')


class Atualizador:
    def __init__(self, source, *, root=None, host=None, timeout=21600, log=print, confirmar_legado=None):
        self.confirmar_legado=confirmar_legado
        self.source=Path(source).resolve()
        self.parent=Path(root).resolve() if root else raiz_da_instalacao(self.source)
        self.versions=self.parent/'radar_versoes'
        self.target=self.versions/'atual';self.data=self.parent/'radar_dados'
        self.db=self.data/'radar_jobs.db';self.host=host or Windows()
        self.timeout=timeout;self.log=log
        self.journal=self.data/'radar_pncp_atualizacao.json'

    def salvar(self, j, fase):
        j['fase']=fase;gravar(self.journal,j)

    def recuperar(self,j):
        """Idempotente. Mantém bloqueio/diário se a recuperação externa falhar."""
        old=Path(j['old']);backup=Path(j['backup']);work=Path(j['work'])
        self.log('Restaurando a instalação e as configurações anteriores…')
        # Impede gatilhos durante restauração, inclusive se a liberação começou.
        for task in j['tasks']:self.host.enable(task,False)
        if j['fase'] in ('troca','trocado','validado','restaurando'):
            self.host.stop_ui(self.target)
            if backup.exists() and j['target_existed']:
                if self.target.exists():
                    failed=work/'versao_com_falha'
                    if failed.exists():shutil.rmtree(failed)
                    self.target.rename(failed)
                backup.rename(self.target)
            elif not j['target_existed'] and self.target.exists():
                failed=work/'versao_com_falha'
                if not failed.exists():self.target.rename(failed)
            for name in ('app.py','iniciar_interface.bat'):
                if (work/name).exists():shutil.copy2(work/name,old/name)
        for name in ('iniciar.bat','radar_pncp_iniciar.py'):
            anterior=work/'inicializador_anterior'/name
            if anterior.exists():shutil.copy2(anterior,self.parent/name)
        for task in j['tasks']:self.host.action(task)
        liberar(self.db,j['token'])
        retomar(self.host,j['tasks'],self.db,old,j['inicio'],self.source,j.get('controller_manual',False))
        self.salvar(j,'revertido')
        self.log('Operação anterior restaurada. Abra iniciar_interface.bat da instalação anterior.')

    def instalar_inicializador(self,j):
        pasta=Path(j['work'])/'inicializador_anterior';pasta.mkdir(exist_ok=True)
        for name in ('iniciar.bat','radar_pncp_iniciar.py'):
            if (self.parent/name).exists():shutil.copy2(self.parent/name,pasta/name)
        shutil.copy2(self.source/'radar_pncp_iniciar.py',self.parent/'radar_pncp_iniciar.py')
        linhas=['@echo off','setlocal','chcp 65001 >nul','cd /d "%~dp0"',
            'if exist "%~dp0.venv\\Scripts\\python.exe" (',
            '  "%~dp0.venv\\Scripts\\python.exe" -u "%~dp0radar_pncp_iniciar.py"',
            ') else if exist "%~dp0python\\python.exe" (',
            '  "%~dp0python\\python.exe" -u "%~dp0radar_pncp_iniciar.py"',
            ') else (','  python -u "%~dp0radar_pncp_iniciar.py"',')',
            'set "resultado=%ERRORLEVEL%"','pause','exit /b %resultado%']
        (self.parent/'iniciar.bat').write_bytes(('\r\n'.join(linhas)+'\r\n').encode('utf-8'))

    def publicar_anterior(self,j):
        if 'target' not in j:return  # Recuperação de diário da V0.7.2.
        backup=Path(j['backup'])
        if not backup.exists():return
        anterior=self.versions/'anterior'
        marca=anterior/'backup_origem.json'
        if marca.exists() and json.loads(marca.read_text()).get('token')==j['token']:return
        preparo=Path(j['work'])/'anterior_publicacao'
        if not preparo.exists():shutil.copytree(backup,preparo)
        gravar(preparo/'backup_origem.json',{'token':j['token']})
        if anterior.exists():
            historico=self.versions/'historico';historico.mkdir(exist_ok=True)
            anterior.rename(historico/j['token'])
        preparo.rename(anterior)

    def executar(self, *, recuperar_apenas=False):
        if self.source==self.target:raise ValueError('Execute o atualizador a partir do NOVO pacote extraído, fora da pasta radar_versoes/atual.')
        if not self.db.is_file():raise FileNotFoundError('Coloque a pasta do pacote ao lado de radar_dados. Banco de jobs não encontrado: '+str(self.db))
        if any(os.environ.get(n) for n in ('RADAR_DATA_DIR','RADAR_DB','RADAR_JOBS_DB','RADAR_FTS_DB','RADAR_TIC_DB','RADAR_STATS_DB','RADAR_LOG_DIR')):
            raise ValueError('Esta migração exige os caminhos padrão em radar_dados. Variáveis RADAR_* personalizadas foram detectadas; nenhuma alteração realizada.')
        with exclusividade(self.data/'radar_pncp_atualizador.lock'):
            if self.journal.exists():
                previous=json.loads(self.journal.read_text(encoding='utf-8'))
                if previous['fase'] not in ('concluido','revertido'):
                    self.target=Path(previous.get('target',self.parent/'radar_pncp'))
                if previous['fase']=='retomando':
                    liberar(self.db,previous['token'])
                    retomar(self.host,previous['tasks'],self.db,self.target,previous['inicio'],self.source,previous.get('controller_manual',False))
                    self.publicar_anterior(previous)
                    self.salvar(previous,'concluido')
                    self.log('Retomada concluída. Abra iniciar.bat na pasta raiz.')
                    return None
                if previous['fase'] not in ('concluido','revertido'):
                    self.recuperar(previous)
                    self.log('Recuperação concluída. Execute Atualizar_RADAR_PNCP.bat novamente para tentar a atualização.')
                    return None
            if recuperar_apenas:return None
            manifest=validar_pacote(self.source)
            nova_versao=manifest['version']
            tasks=self.host.snapshot()
            roots={pasta_tarefa(t) for t in tasks}
            if len(roots)>1:raise ValueError('As agendas apontam para pastas diferentes. Atualização cancelada sem alterar a instalação.')
            old=next(iter(roots),self.target if self.target.exists() else self.parent/'radar_pncp' if (self.parent/'radar_pncp'/'app.py').is_file() else None)
            if old is None or (old!=self.target and old.parent!=self.parent) or not (old/'app.py').is_file():
                raise ValueError('Não foi possível identificar a instalação anterior ao lado do pacote. Mantenha as pastas de versão e radar_dados no mesmo diretório.')
            if old==self.source:raise ValueError('O pacote novo já está sendo usado pelas agendas. Extraia outra cópia para atualizar a instalação.')
            if self.target.exists() and old!=self.target:raise ValueError('Já existe uma pasta radar_versoes/atual diferente da instalação agendada. Nenhum arquivo foi substituído.')
            if Path(sys.executable).resolve().is_relative_to(self.target):raise ValueError('Use um Python instalado fora da pasta Radar PNCP para executar o atualizador.')
            if (self.target/'release.json').exists():
                atual=json.loads((self.target/'release.json').read_text(encoding='utf-8'))
                if versao(nova_versao)<versao(atual['version']):raise ValueError('O pacote é anterior à versão instalada.')
            token=uuid.uuid4().hex
            work=self.versions/'operacoes'/token;work.mkdir(parents=True)
            (self.versions/'pacotes').mkdir(exist_ok=True)
            self.target.parent.mkdir(parents=True,exist_ok=True)
            stage=work/'nova';backup=work/'anterior'
            self.log('Verificando arquivos e dependências da V'+nova_versao+'…')
            preparar(self.source,stage,manifest)
            if (old/'.streamlit').is_dir():shutil.copytree(old/'.streamlit',stage/'.streamlit',dirs_exist_ok=True)
            j={'token':token,'old':str(old),'backup':str(backup),'work':str(work),
               'target_existed':self.target.exists(),'tasks':tasks,'inicio':datetime.now().isoformat(),
               'version':nova_versao,'target':str(self.target),'helper':str(Path(__file__).resolve().parent),
               'controller_manual':not any(t['name']=='RADAR_PNCP_MUTIRAO_ITENS_PERSISTENTE' and t['enabled'] for t in tasks)
                   and any('mutirao_persistente.py' in (p.get('CommandLine') or '').lower() for p in self.host.running(old))}
            (self.versions/'cancelar_atualizacao').unlink(missing_ok=True)
            self.salvar(j,'preparado')
            try:
                self.instalar_inicializador(j)
                pausar(self.db,token)
                self.salvar(j,'pausado')
                for task in tasks:self.host.enable(task,False)
                self.log('Novos jobs bloqueados. Mutirão, híbrido e índice compatíveis pausarão preservando o progresso; outras operações serão aguardadas.')
                if self.confirmar_legado is not None:
                    oferecer_parada_legada(self.db,old,self.confirmar_legado,self.log)
                aguardar(self.db,old,self.host,self.timeout,self.log,cancelar=self.versions/'cancelar_atualizacao',
                         observacao=self.versions/'espera_atualizacao.json',token=token)
                self.host.stop_ui(old)
                self.salvar(j,'troca')
                if self.target.exists():self.target.rename(backup)
                else:
                    shutil.copytree(old,backup,ignore=shutil.ignore_patterns('__pycache__','.pytest_cache','.venv','venv','radar_dados','dados','*.db','*.db-*','*.sqlite*','.git'))
                stage.rename(self.target)
                self.salvar(j,'trocado')
                for task in tasks:self.host.action(task,self.target)
                old_config=old/'instalacao.json' if old!=self.target else backup/'instalacao.json'
                port_anterior=json.loads(old_config.read_text()).get('port',8501) if old_config.exists() else 8501
                port=porta_livre(port_anterior)
                gravar(self.target/'instalacao.json',{'version':nova_versao,'port':port})
                if old!=self.target:redirecionar(old,self.target,port,work)
                self.host.start_ui(self.target,port)
                self.salvar(j,'validado')
                # Depois de validado, falha na retomada não desfaz código sob
                # workers novos. Uma próxima execução conclui a retomada.
                self.salvar(j,'retomando')
                liberar(self.db,token)
                retomar(self.host,tasks,self.db,self.target,j['inicio'],self.target,j.get('controller_manual',False))
                self.publicar_anterior(j)
                self.salvar(j,'concluido')
            except BaseException:
                if j['fase']=='retomando':
                    self.log('A versão foi instalada, mas a retomada ficou pendente. Execute Atualizar_RADAR_PNCP.bat novamente para concluí-la.')
                else:
                    try:self.recuperar(j)
                    except Exception as exc:self.log('Recuperação pendente: '+str(exc)+'. Execute novamente este atualizador.')
                raise
            self.log('Radar PNCP V'+nova_versao+' pronta. Abra iniciar.bat na raiz. A versão anterior está em radar_versoes/anterior.')
            return port


def main(argv=None):
    parser=argparse.ArgumentParser(description='Atualiza Radar PNCP mantendo dados e agendas')
    parser.add_argument('--timeout',type=int,default=21600)
    parser.add_argument('--root',type=Path)
    parser.add_argument('--pacote',type=Path)
    parser.add_argument('--recuperar',action='store_true')
    parser.add_argument('--sem-navegador',action='store_true')
    parser.add_argument('--perguntar-parada-legada',action='store_true',
                        help='Oferece parada dos workers antigos sem pausa cooperativa, após bloquear novos jobs.')
    args=parser.parse_args(argv)
    root=args.root or raiz_da_instalacao(Path(__file__).parent,obrigatoria=True)
    source=Path(__file__).parent
    # Extração ocorre fora de atual/, antes de pausar qualquer operação.
    if args.pacote:
        from pacotes_radar_pncp import extrair
        source=extrair(args.pacote,root/'radar_versoes'/'operacoes'/('pacote_'+uuid.uuid4().hex))
    updater=Atualizador(source,root=root,timeout=max(0,args.timeout),
                        confirmar_legado=input if args.perguntar_parada_legada else None)
    def log(msg):
        print(msg,flush=True)
        path=updater.data/'logs'/'atualizacao.log'
        path.parent.mkdir(parents=True,exist_ok=True)
        with path.open('a',encoding='utf-8') as f:f.write(datetime.now().isoformat(timespec='seconds')+' '+msg+'\n')
    updater.log=log
    try:
        port=updater.executar(recuperar_apenas=args.recuperar)
        if port and not args.sem_navegador:
            import webbrowser
            webbrowser.open(f'http://localhost:{port}')
        return 0
    except KeyboardInterrupt:log('Atualização cancelada.');return 1
    except Exception as exc:log('Não foi possível concluir: '+str(exc));return 1


if __name__=='__main__':
    try:raise SystemExit(main())
    except Exception as exc:
        print('Não foi possível preparar a atualização: '+str(exc));raise SystemExit(1)
