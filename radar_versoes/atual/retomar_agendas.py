"""Repõe agendas que venceram enquanto o atualizador aguardava os jobs."""
import argparse
import json
import sqlite3
import subprocess
import sys
import os
import time
from pathlib import Path
import job_manager as jm
from atualizar_radar_pncp import em_atualizacao, exclusividade, gravar
from atualizacao_windows import Windows, acao
from coleta_scheduler import _extrair_dias_argumentos, _extrair_modo_argumentos


def iniciar(db, base):
    if not (Path(db).parent/'radar_pncp_agendas_pendentes.json').exists():return
    subprocess.Popen([sys.executable,str(Path(__file__).resolve()),'--db',str(db),'--base',str(base)],
        cwd=base,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,
        **({'creationflags':getattr(subprocess,'CREATE_NO_WINDOW',0)} if os.name=='nt' else {'start_new_session':True}))


def executar(db, base):
    fila=db.parent/'radar_pncp_agendas_pendentes.json'
    with exclusividade(db.parent/'radar_pncp_retomada.lock'):
        while fila.exists():
            if em_atualizacao(db):return
            nomes=json.loads(fila.read_text(encoding='utf-8'))
            if not nomes:fila.unlink(missing_ok=True);return
            if jm.obter_job_ativo(db):time.sleep(10);continue
            task=next((t for t in Windows().snapshot() if t['name']==nomes[0]),None)
            if task and task['enabled']:
                args=acao(task['xml'])['Arguments']
                modo=_extrair_modo_argumentos(args)
                dias=_extrair_dias_argumentos(args)
                if not modo or not dias:raise ValueError('Agenda pendente com argumentos não reconhecidos.')
                jm.BASE_DIR=base
                try:
                    jm.iniciar_atualizacao_diaria(janela_dias=dias,db_compras=db.parent/'compras.db',
                        db_jobs=db,log_dir=db.parent/'logs',modo=modo)
                except (RuntimeError,sqlite3.IntegrityError):
                    # Outro disparador ganhou a disputa ou a atualização recomeçou.
                    time.sleep(10);continue
            gravar(fila,nomes[1:])


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--db',type=Path,required=True)
    p.add_argument('--base',type=Path,required=True);a=p.parse_args()
    try:executar(a.db,a.base)
    except Exception as exc:
        log=a.db.parent/'logs'/'retomada_agendas.log';log.parent.mkdir(parents=True,exist_ok=True)
        with log.open('a',encoding='utf-8') as f:f.write(str(exc)+'\n')
        raise
