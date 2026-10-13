"""Intervalo de publicação com navegação independente das datas selecionadas."""
from datetime import date
from pathlib import Path
import streamlit as st
import streamlit.components.v1 as components

_calendario=components.declare_component('radar_pncp_calendario_periodo',path=str(Path(__file__).parent/'calendario_periodo'))


def validar(dados,minimo,maximo):
    if not isinstance(dados,(tuple,list)) or len(dados)!=2:
        raise ValueError('Escolha a data inicial e a data final do período.')
    inicio,fim=(date.fromisoformat(d) if isinstance(d,str) else d for d in dados)
    if type(inicio) is not date or type(fim) is not date or not minimo<=inicio<=fim<=maximo:
        raise ValueError('Informe um período válido, com início anterior ou igual ao fim e dentro das datas permitidas.')
    return inicio,fim


def selecionar(valor,minimo,maximo,*,chave,hoje=None):
    """Mantém a chave usada nas pesquisas salvas; mensagens antigas não a sobrescrevem."""
    atual=st.session_state.get(chave,valor)
    base=[d.isoformat() for d in validar(atual,minimo,maximo)]
    geracao=chave+'_calendario_geracao'
    ultima=chave+'_calendario_ultima'
    if st.session_state.get(ultima) not in (None,base):
        st.session_state[geracao]=st.session_state.get(geracao,0)+1
    resposta=_calendario(value=base,min_date=minimo.isoformat(),max_date=maximo.isoformat(),
        today=(hoje or date.today()).isoformat(),key=chave+'_calendario_'+str(st.session_state.get(geracao,0)),default=None)
    st.session_state[ultima]=base
    if isinstance(resposta,dict) and resposta.get('base')==base:
        try:
            if resposta.get('error'):raise ValueError(resposta['error'])
            atual=validar(resposta.get('dates'),minimo,maximo)
        except (ValueError,TypeError):
            st.caption('Complete um período válido antes de pesquisar.')
            return None
    st.session_state[chave]=tuple(atual)
    st.session_state[ultima]=[d.isoformat() for d in atual]
    return tuple(atual)
