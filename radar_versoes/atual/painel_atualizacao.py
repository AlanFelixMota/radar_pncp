"""Atualização local: identifica pacotes e entrega a troca a um processo externo."""
from datetime import datetime
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import uuid

from atualizar_radar_pncp import exclusividade, gravar, validar_pacote
from pacotes_radar_pncp import descobrir, inspecionar, versao
import job_manager as jm


def ler_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8')) if Path(path).exists() else {}


def em_execucao(root):
    state=ler_json(Path(root)/'radar_versoes'/'solicitacao.json')
    if state.get('status')!='iniciado':return False
    if state.get('pid'):return jm.processo_existe(state['pid'])
    try:return (datetime.now()-datetime.fromisoformat(state['iniciado_em'])).total_seconds()<30
    except (KeyError,ValueError):return False


def iniciar(root,base,pacote):
    root=Path(root).resolve();base=Path(base).resolve();pacote=Path(pacote).resolve()
    versions=root/'radar_versoes'
    with exclusividade(versions/'solicitacao.lock'):
        if em_execucao(root):raise RuntimeError('Já existe uma atualização em andamento.')
        info=inspecionar(pacote,conferir_hashes=True)
        atual=validar_pacote(base,instalado=True)
        if versao(info['version'])<=versao(atual['version']):raise ValueError('O pacote precisa ser mais recente que a versão instalada.')
        folder=versions/'operacoes'/('motor_'+uuid.uuid4().hex)
        folder.mkdir(parents=True)
        # O motor precisa sobreviver à substituição da pasta atual/.
        for name in (*atual['files'],'release.json'):
            target=folder/name;target.parent.mkdir(parents=True,exist_ok=True)
            shutil.copy2(base/name,target)
        copia=folder/'pacote.zip'
        shutil.copy2(pacote,copia)
        if inspecionar(copia,conferir_hashes=True)['identity']!=info['identity']:
            raise ValueError('O pacote mudou durante a preparação. Verifique novamente.')
        pedido=versions/'solicitacao.json'
        state={'status':'iniciado','version':info['version'],'pacote':str(pacote),
               'motor':str(folder),'iniciado_em':datetime.now().isoformat(),'pid':None}
        gravar(pedido,state)
        log=root/'radar_dados'/'logs'/'atualizador_interface.log'
        log.parent.mkdir(parents=True,exist_ok=True)
        try:
            with log.open('ab') as f:
                proc=subprocess.Popen([sys.executable,str(folder/'executar_atualizacao.py'),
                     '--root',str(root),'--pacote',str(copia),'--pedido',str(pedido)],
                     cwd=folder,stdin=subprocess.DEVNULL,stdout=f,stderr=f,
                     **({'creationflags':getattr(subprocess,'CREATE_NO_WINDOW',0)} if os.name=='nt' else {'start_new_session':True}))
            # O próprio processo registra seu PID; a interface não sobrescreve
            # seu estado de conclusão se ele terminar rapidamente.
        except Exception:
            state['status']='erro';gravar(pedido,state);raise
        return state


def mostrar_andamento(root):
    import streamlit as st
    root=Path(root)
    j=ler_json(root/'radar_dados'/'radar_pncp_atualizacao.json')
    st.info('Atualização em andamento. As operações compatíveis estão pausando com o progresso salvo; as demais serão aguardadas.')
    phases={'preparado':'Verificando a instalação','pausado':'Aguardando jobs ativos',
            'troca':'Trocando arquivos','trocado':'Ajustando agendas','validado':'Interface validada',
            'retomando':'Retomando a operação','concluido':'Concluída','revertido':'Instalação anterior restaurada'}
    st.write('Etapa: '+phases.get(j.get('fase'), 'Preparando atualização'))
    espera=ler_json(root/'radar_versoes'/'espera_atualizacao.json')
    if j.get('fase')=='pausado' and espera.get('token')==j.get('token') and espera:
        st.caption('Última verificação: '+espera.get('atualizado_em','')+
                   f" · Espera: {espera.get('espera_segundos',0)} s · Sem mudança no progresso: {espera.get('sem_mudanca_segundos',0)} s")
        for job in espera.get('jobs',[]):
            st.write(f"Job **{job['id']}** · {job.get('tipo')} · {job.get('status')}")
            st.write(job.get('etapa') or 'Aguardando encerramento')
            if job.get('mensagem'):st.caption(job['mensagem'])
            atual=job.get('progresso_atual') or 0;total=job.get('progresso_total') or 0
            if total:st.progress(max(0.0,min(1.0,atual/total)),text=f'{atual:,} / {total:,}'.replace(',','.'))
            if job.get('tipo') in ('ITENS_MUTIRAO','ITENS_HIBRIDO','INDICE_FTS'):
                st.caption('Aguarda a consulta ou gravação atual para salvar o ponto de retomada. Requisições lentas ainda podem levar alguns minutos.')
            else:
                st.caption('Esta operação ainda precisa terminar normalmente antes da troca de versão.')
        if espera.get('processos'):
            st.caption('Processos: '+', '.join(f"{p['script']} (PID {p['pid']})" for p in espera['processos']))
        if espera.get('sem_mudanca_segundos',0)>=180:
            st.warning('Sem mudança de progresso há pelo menos 3 minutos. Isso pode indicar uma consulta lenta; não comprova travamento. Você pode cancelar a atualização e consultar Execuções e logs.')
    if j.get('fase')=='pausado' and st.button('Cancelar atualização',key='cancelar_atualizacao'):
        (root/'radar_versoes'/'cancelar_atualizacao').touch()
        st.info('Cancelamento solicitado. Aguarde a restauração das configurações.')
    st.button('Atualizar andamento',key='refresh_atualizacao')
    st.caption('Se a conexão cair durante o reinício, atualize esta aba ou abra iniciar.bat na raiz.')
    path=root/'radar_dados'/'logs'/'atualizacao.log'
    if path.exists():
        with path.open('rb') as f:
            f.seek(max(0,path.stat().st_size-6000));tail=f.read().decode('utf-8',errors='replace')
        st.code('\n'.join(tail.splitlines()[-12:]),language=None)


def mostrar(root,base):
    import streamlit as st
    root=Path(root);base=Path(base)
    st.subheader('Atualizar sistema')
    st.write('Coloque o ZIP da nova versão na pasta abaixo e clique em Verificar pacotes.')
    st.code(str(root/'radar_versoes'/'pacotes'),language=None)
    st.caption('Também são reconhecidos ZIPs diretamente em radar_versoes. Os bancos e logs continuam em radar_dados.')
    version=ler_json(base/'release.json').get('version','não identificada')
    st.metric('Versão instalada',version)
    state=ler_json(root/'radar_versoes'/'solicitacao.json')
    if em_execucao(root):mostrar_andamento(root);return
    if state.get('status')=='erro' or (state.get('status')=='iniciado' and not em_execucao(root)):
        st.warning('A última tentativa não concluiu. Abra iniciar.bat para recuperar uma atualização interrompida. Consulte radar_dados/logs/atualizador_interface.log para detalhes.')
    candidates,errors=descobrir(root)
    st.button('Verificar pacotes',key='verificar_pacotes')
    for error in errors:st.warning(error)
    if not candidates:
        st.info('Nenhum pacote compatível encontrado.');return
    latest=candidates[0]
    st.write('Pacote mais recente: **'+latest['version']+'** — '+Path(latest['path']).name)
    if version!='não identificada' and versao(latest['version'])<=versao(version):
        st.success('A versão instalada já é igual ou posterior aos pacotes disponíveis.');return
    st.caption('Mutirão, híbrido e índice pausarão com o progresso salvo; outras operações serão aguardadas. Você pode fechar a aba: o atualizador continuará em segundo plano.')
    if st.button('Atualizar sistema',type='primary',key='instalar_pacote'):
        try:
            iniciar(root,base,latest['path']);st.rerun()
        except (OSError,ValueError,RuntimeError) as exc:st.error('Não foi possível iniciar: '+str(exc))
