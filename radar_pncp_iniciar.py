"""Inicializador estável da raiz. Funciona mesmo durante recuperação de troca."""
import json
import os
import subprocess
import sys
from pathlib import Path


def motor_recuperacao(root,state):
    """Diários antigos podem apontar o helper para a própria pasta atual."""
    atual=(root/'radar_versoes'/'atual').resolve()
    candidates=[state.get('helper')]
    pedido=root/'radar_versoes'/'solicitacao.json'
    if pedido.is_file():
        candidates.append(json.loads(pedido.read_text(encoding='utf-8')).get('motor'))
    for folder in candidates:
        if not folder:continue
        helper=Path(folder).resolve()/'atualizar_radar_pncp.py'
        if helper.is_file() and helper.parent!=atual:return helper
    return None


def comando_script(path,*args):
    # Serve também para motores antigos, que não inseriam seu próprio caminho.
    bootstrap='import sys,runpy; sys.path.insert(0,sys.argv[1]); sys.argv=sys.argv[2:]; runpy.run_path(sys.argv[0],run_name="__main__")'
    return [sys.executable,'-u','-c',bootstrap,str(path.parent),str(path),*map(str,args)]


def main():
    root=Path(__file__).resolve().parent
    os.environ['STREAMLIT_GLOBAL_DEVELOPMENT_MODE']='false'
    os.environ.setdefault('STREAMLIT_SERVER_FILE_WATCHER_TYPE','none')
    os.environ.setdefault('PYTHONIOENCODING','utf-8')
    if (root/'python'/'python.exe').is_file():
        os.environ['PATH']=str(root/'python')+os.pathsep+os.environ.get('PATH','')
    journal=root/'radar_dados'/'radar_pncp_atualizacao.json'
    state=json.loads(journal.read_text(encoding='utf-8')) if journal.exists() else {}
    if state and state.get('fase') not in ('concluido','revertido'):
        helper=motor_recuperacao(root,state)
        if helper is None:
            print('Recuperação pendente. Execute Atualizar_RADAR_PNCP.bat do pacote usado na atualização.');return 1
        code=subprocess.call(comando_script(helper,'--root',root,'--recuperar'),cwd=helper.parent)
        if code:return code
        state=json.loads(journal.read_text(encoding='utf-8'))
    base=root/'radar_versoes'/'atual'
    if not (base/'app.py').is_file() and state.get('fase')=='revertido':base=Path(state['old'])
    if not (base/'app.py').is_file():
        print('Instalação não encontrada. Execute Atualizar_RADAR_PNCP.bat do pacote novo para migrar o Radar PNCP.');return 1
    if (base/'abrir_radar_pncp.py').is_file():
        return subprocess.call([sys.executable,str(base/'abrir_radar_pncp.py')],cwd=base)
    return subprocess.call([sys.executable,'-m','streamlit','run',str(base/'app.py')],cwd=base)


if __name__=='__main__':
    try:raise SystemExit(main())
    except Exception as exc:
        print('Não foi possível abrir o Radar PNCP: '+str(exc),flush=True)
        raise SystemExit(1)
