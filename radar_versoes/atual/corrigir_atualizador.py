"""Instala a correção usando o motor do pacote, fora de atual/."""
from pathlib import Path
import json
import os
import sys

SOURCE=Path(__file__).resolve().parent
sys.path.insert(0,str(SOURCE))
from atualizar_radar_pncp import main as atualizar, validar_pacote
from radar_pncp_layout import raiz_da_instalacao
from pacotes_radar_pncp import versao


def main():
    try:
        root=raiz_da_instalacao(SOURCE,obrigatoria=True)
        base=root/'radar_versoes'/'atual'
        if SOURCE==base:raise RuntimeError('Extraia o ZIP na raiz do Radar PNCP, fora de radar_versoes/atual.')
        novo=validar_pacote(SOURCE);atual=validar_pacote(base,instalado=True)
        journal=root/'radar_dados'/'radar_pncp_atualizacao.json'
        if journal.exists() and json.loads(journal.read_text(encoding='utf-8')).get('fase') not in ('concluido','revertido'):
            raise RuntimeError('A tentativa anterior ainda não terminou. Aguarde o cancelamento/restauração antes de executar esta correção.')
        if versao(atual['version'])>=versao(novo['version']):
            print('A correção já está instalada. Abra iniciar.bat na raiz.');return 0
        os.environ['STREAMLIT_GLOBAL_DEVELOPMENT_MODE']='false'
        os.environ.setdefault('STREAMLIT_SERVER_FILE_WATCHER_TYPE','none')
        os.environ.setdefault('PYTHONIOENCODING','utf-8')
        print('Instalando Radar PNCP '+novo['version']+' com o motor corrigido.',flush=True)
        print('Coletas compatíveis pausarão com o progresso salvo. Não é necessário desativá-las manualmente.',flush=True)
        return atualizar(['--root',str(root),'--timeout','600'])
    except Exception as exc:
        print('Não foi possível aplicar a correção: '+str(exc),flush=True);return 1


if __name__=='__main__':raise SystemExit(main())
