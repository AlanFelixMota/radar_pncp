"""Processo independente da interface, executado de uma cópia estável do motor."""
import argparse
import json
import os
import sys
from pathlib import Path
from datetime import datetime
# O runtime embutido ignora a pasta do script no sys.path. Nunca carregue
# o motor de atual/, que será substituído durante a própria execução.
sys.path.insert(0,str(Path(__file__).resolve().parent))
from atualizar_radar_pncp import main, gravar


def executar(root,pacote,pedido):
    state=json.loads(pedido.read_text(encoding='utf-8'))
    state['pid']=os.getpid();gravar(pedido,state)
    result=1
    try:result=main(['--root',str(root),'--pacote',str(pacote),'--sem-navegador'])
    except BaseException as exc:print('Falha no atualizador: '+str(exc),flush=True)
    finally:
        state=json.loads(pedido.read_text(encoding='utf-8'))
        state.update(status='concluido' if result==0 else 'erro',finalizado_em=datetime.now().isoformat(),codigo=result)
        gravar(pedido,state)
    return result


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True)
    p.add_argument('--pacote',type=Path,required=True);p.add_argument('--pedido',type=Path,required=True)
    a=p.parse_args();raise SystemExit(executar(a.root,a.pacote,a.pedido))
