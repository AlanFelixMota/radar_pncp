"""Confirmações de exclusão dos dados de trabalho, com alcance explícito."""
from pathlib import Path
import sqlite3
import streamlit as st
import exclusoes


def _limpar_estado(tipo,ident,monitor=None):
    if tipo=='cesta':
        for k in list(st.session_state):
            if k in ('cesta_ativa_id','cesta_pesquisa_escolhida','cesta_destino_pesquisa') or (k.startswith('destino_local_') and k.endswith('_escolha')):
                if st.session_state.get(k)==ident:st.session_state.pop(k,None)
        for k in ('cesta_excel','cesta_relatorio_word'):
            arquivo=st.session_state.get(k)
            if isinstance(arquivo,dict) and arquivo.get('id')==ident:st.session_state.pop(k,None)
        st.session_state['cesta_flash']='Cesta apagada. Escolha outra pesquisa ou crie uma nova.'
    else:
        for k in ('salvas_escolhida','busca_salva_foco'):
            if st.session_state.get(k)==ident:st.session_state.pop(k,None)
        for k in ('busca_salva_contexto','forn_busca_salva_pendente'):
            v=st.session_state.get(k)
            if isinstance(v,dict) and v.get('id')==ident:st.session_state.pop(k,None)
        if monitor:
            mid=monitor['id']
            for k in ('monitoramento_acompanhamento','monitoramento_foco'):
                if st.session_state.get(k)==mid:st.session_state.pop(k,None)
            mensagem=st.session_state.get('monitoramento_acao_msg')
            if mensagem and mensagem.get('mid')==mid:st.session_state.pop('monitoramento_acao_msg',None)
        st.session_state['buscas_salvas_flash']='Pesquisa salva excluída.'+(' O acompanhamento MONITORAMENTO e seu histórico também foram excluídos.' if monitor else '')



def _confirmar():
    pedido=st.session_state['exclusao_dialogo'];p=pedido['preview']
    try:
        resultado=exclusoes.apagar(pedido['db'],pedido['tipo'],pedido['id'],
            assinatura=p['assinatura'],confirmacao=p['nome'],
            excluir_monitoramento=bool(p['monitor']))
        _limpar_estado(pedido['tipo'],pedido['id'],resultado['monitor'])
        st.session_state['exclusao_concluida']=True
    except (ValueError,OSError,sqlite3.Error) as exc:
        st.session_state['exclusao_erro']=str(exc)
        # Conferir novamente não exclui. Uma mudança exige outro clique em Sim
        # sobre a pergunta atualizada, inclusive quando a MONITORAMENTO foi vinculada.
        try:pedido['preview']=exclusoes.prever(pedido['db'],pedido['tipo'],pedido['id'])
        except (ValueError,OSError,sqlite3.Error):pass


@st.dialog('Confirmar exclusão',width='small')
def _dialogo():
    if st.session_state.pop('exclusao_concluida',False):
        st.session_state.pop('exclusao_dialogo',None)
        st.rerun()
    pedido=st.session_state['exclusao_dialogo'];p=pedido['preview']
    if pedido['tipo']=='cesta':
        st.write(f'Tem certeza de que deseja apagar a cesta “{p["nome"]}”?')
        st.caption('As referências e os registros desta cesta serão apagados definitivamente. A base coletada será preservada.')
    elif p['monitor']:
        st.write(f'Tem certeza de que deseja excluir a pesquisa salva “{p["nome"]}” e seu acompanhamento MONITORAMENTO?')
        st.caption('As novidades e o histórico da MONITORAMENTO também serão apagados. Referências já copiadas para cestas serão preservadas.')
    else:
        st.write(f'Tem certeza de que deseja excluir a pesquisa salva “{p["nome"]}”?')
        st.caption('A exclusão é definitiva. As cestas e a base coletada serão preservadas.')
    if p['ocupado']:st.info('A MONITORAMENTO está verificando esta pesquisa. Aguarde terminar e abra esta confirmação novamente.')
    if st.session_state.get('exclusao_erro'):st.error(st.session_state['exclusao_erro'])
    nao,sim=st.columns(2)
    if nao.button('Não',key='exclusao_cancelar',use_container_width=True):
        st.session_state.pop('exclusao_dialogo',None);st.rerun()
    sim.button('Sim',type='primary',disabled=p['ocupado'],key='exclusao_confirmar',on_click=_confirmar,use_container_width=True)


def abrir(db,tipo,ident):
    try:preview=exclusoes.prever(db,tipo,ident)
    except (ValueError,OSError,sqlite3.Error) as exc:
        st.error('Não foi possível abrir a confirmação: '+str(exc));return
    for k in ('exclusao_nome','exclusao_monitoramento','exclusao_erro','exclusao_concluida'):st.session_state.pop(k,None)
    st.session_state['exclusao_dialogo']={'db':str(Path(db).resolve()),'tipo':tipo,'id':ident,'preview':preview}
    _dialogo()
