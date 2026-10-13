#!/usr/bin/env python3
"""Paleta e componentes do Radar PNCP; preços e seleção não são alterados pelo estilo."""
from __future__ import annotations
import html
from pathlib import Path
from datetime import datetime
import pandas as pd
import streamlit as st

PALETAS = {
    'light': dict(fundo='#eef2f7', superficie='#ffffff', suave='#e4ecf6', texto='#243348',
                  apoio='#4d5c70', borda='#aab8ca', azul='#2455a1', alternada='#f4f7fb',
                  selecao='#dbeafe', selecao_texto='#173b67', lateral='#dde5f0'),
    'dark': dict(fundo='#101722', superficie='#1d2735', suave='#27364b', texto='#e8eef7',
                 apoio='#b6c4d6', borda='#536881', azul='#a8caff', alternada='#243041',
                 selecao='#284b73', selecao_texto='#f0f6ff', lateral='#182232'),
}


def tema_atual():
    tipo = st.session_state.get('radar_pncp_tema_nativo') or st.context.theme.type
    return tipo if tipo in PALETAS else (st.get_option('theme.base') or 'light')


CSS = """
<style>
.stApp { background:var(--radar_pncp-fundo); color:var(--radar_pncp-texto); }
[data-testid="stHeader"] { background:var(--radar_pncp-fundo); }
[data-testid="stSidebar"] { background:var(--radar_pncp-lateral); border-right:1px solid var(--radar_pncp-borda); }
.stElementContainer:has(iframe[title="radar_theme.sincronizar_tema"]) { display:none; }
[data-testid="stRadio"] label:has(input:checked) > div:first-child { background:#4a81ca; }
[data-testid="stCheckbox"] label:has(input:checked) > span { background:#2455a1; border-color:#2455a1; }
[data-baseweb="input"]:focus-within, [data-baseweb="textarea"]:focus-within {
    border-color:#4a81ca; outline-color:#4a81ca;
}
[data-testid="stSelectbox"] [data-baseweb="select"]:focus-within > div:first-child { border-color:#4a81ca; }
[class*="st-key-radar_pncp_busca_form"] [data-testid="stFormSubmitButton"] button { min-width:108px; white-space:nowrap; }
@media(max-width:1100px) {
    .st-key-radar_pncp_busca_form [data-testid="stHorizontalBlock"]:has([data-testid="stFormSubmitButton"]) { flex-wrap:wrap; }
    .st-key-radar_pncp_busca_form [data-testid="stColumn"]:has([data-testid="stFormSubmitButton"]) { min-width:108px; flex:0 0 108px; }
}
[data-testid="stHeading"] h1 { font-size:1.9rem; font-weight:600; }
[data-testid="stHeading"] h2 { font-size:1.4rem; font-weight:600; }
[data-testid="stSidebar"] [data-testid="stButton"] button { justify-content:flex-start; }
[data-testid="stSidebar"] [data-testid="stButton"] button p { font-weight:500; }
[data-testid="stCaptionContainer"] { color:var(--radar_pncp-apoio); opacity:1; }
[data-testid="stCaptionContainer"] p { color:inherit; }
[data-testid="stForm"], [data-testid="stExpander"],
[class*="st-key-radar_pncp_resultado_"], .st-key-radar_pncp_destino_pesquisa,
.st-key-radar_pncp_detalhes_resumo {
    background:var(--radar_pncp-superficie);
    border-color:var(--radar_pncp-borda);
    border-radius:8px;
}
[data-testid="stExpander"] details > summary {
    background:var(--radar_pncp-suave); border-radius:7px; font-weight:500;
}
[data-testid="stDialog"] [role="dialog"] {
    background:var(--radar_pncp-superficie); color:var(--radar_pncp-texto); border:1px solid var(--radar_pncp-borda);
}
[data-testid="stMetric"] {
    background:var(--radar_pncp-superficie); border:1px solid var(--radar_pncp-borda);
    border-radius:8px; padding:.8rem 1rem;
}
[data-testid="stDialog"] [data-testid="stMetric"] { border:none; padding:.2rem .3rem; }
[data-testid="stDialog"] [data-testid="stMetricValue"] > div { font-size:1.35rem; }
[data-testid="stTabs"] [role="tablist"] {
    background:var(--radar_pncp-suave); padding:0 .65rem;
    border-radius:7px 7px 0 0; border-bottom:1px solid var(--radar_pncp-borda);
}
[data-testid="stTabs"] [role="tab"][aria-selected="true"] { font-weight:650; color:var(--radar_pncp-azul); }
[data-baseweb="tab-highlight"] { background-color:var(--radar_pncp-azul); }
button[data-testid^="stBaseButton-primary"]:not(:disabled) {
    background:#2455a1; border-color:#2455a1; color:#ffffff;
}
button[data-testid^="stBaseButton-primary"]:not(:disabled):hover { background:#1d478a; border-color:#1d478a; }
button[data-testid^="stBaseButton-primary"]:focus-visible { outline:2px solid var(--radar_pncp-azul); outline-offset:2px; }
button[data-testid^="stBaseButton"]:focus:not(:disabled) {
    border-color:var(--radar_pncp-azul); box-shadow:0 0 0 2px var(--radar_pncp-azul);
}
button[data-testid^="stBaseButton-secondary"]:hover:not(:disabled) {
    color:var(--radar_pncp-texto); border-color:var(--radar_pncp-azul);
}
[data-testid="stDataFrame"] { border:1px solid var(--radar_pncp-borda); border-radius:8px; }
.radar-status {
    display:inline-flex; align-items:center; border-radius:999px;
    border:1px solid var(--radar_pncp-borda); padding:.15rem .50rem;
    font-size:.76rem; line-height:1.4; font-weight:650;
}
.radar-status.ok { color:var(--radar_pncp-ok); background:rgba(34,197,94,.12); }
.radar-status.info { color:var(--radar_pncp-azul); background:rgba(59,130,246,.12); }
.radar-status.warn { color:var(--radar_pncp-aviso); background:rgba(245,158,11,.12); }
.radar-status.error { color:var(--radar_pncp-erro); background:rgba(239,68,68,.12); }
.radar-result-meta { color:var(--radar_pncp-apoio); font-size:.8rem; font-weight:600; }
.radar-result-title { font-size:1.02rem; font-weight:700; margin:.1rem 0 .3rem; }
.radar-search-engine {
    display:inline-flex; font-size:.77rem; color:var(--radar_pncp-apoio);
    border:1px solid var(--radar_pncp-borda); border-radius:999px; padding:.15rem .48rem;
}
.radar-dado-rotulo { color:var(--radar_pncp-apoio); font-size:.82rem; margin-bottom:.25rem; }
.radar-dado-valor { color:var(--radar_pncp-texto); font-size:1rem; font-weight:600; overflow-wrap:anywhere; }
</style>
"""


def aplicar_tema():
    # O componente acompanha o tema nativo também quando não há clique em widget.
    # Atributo próprio no documento atualiza CSS imediatamente; o valor sincroniza
    # as cores do canvas na próxima execução, sem escrever dados da pesquisa.
    from streamlit.components.v1 import declare_component
    sincronizar = declare_component('sincronizar_tema', path=str(Path(__file__).parent/'tema_nativo'))
    tipo = sincronizar(aplicado=tema_atual(), default=tema_atual(), key='radar_pncp_tema_sincronizacao')
    if tipo in PALETAS:
        st.session_state['radar_pncp_tema_nativo'] = tipo
    regras=[]
    for nome,p in PALETAS.items():
        extras=({'ok':'#166534','aviso':'#854d0e','erro':'#991b1b'} if nome=='light'
                else {'ok':'#86efac','aviso':'#fde68a','erro':'#fca5a5'})
        variaveis=''.join(f'--radar_pncp-{k}:{v};' for k,v in {**p,**extras}.items())
        seletor=':root' if nome=='light' else ':root[data-radar_pncp-theme="dark"]'
        regras.append(seletor+'{'+variaveis+'}')
    st.markdown('<style>'+''.join(regras)+'</style>'+CSS,unsafe_allow_html=True)


def data_hora(valor):
    """Formata só a apresentação; o timestamp original continua no banco."""
    if not valor:return 'Não informado'
    try:return datetime.fromisoformat(str(valor).replace('Z','+00:00')).strftime('%d/%m/%Y às %H:%M:%S')
    except (ValueError,TypeError):return str(valor)


def dado_resumo(rotulo,valor):
    st.markdown(f'<div class="radar-dado-rotulo">{html.escape(str(rotulo))}</div>'
                f'<div class="radar-dado-valor">{html.escape(str(valor))}</div>',unsafe_allow_html=True)


def tabela_selecao(tabela,chave,marcar=False):
    """Estiliza somente a cópia de apresentação; mantém dados e chave do editor.

    O Styler é aplicado às colunas de leitura. A marcação real continua sendo
    a caixa nativa, sem escrever a coluna de seleção ou qualquer preço.
    """
    p=PALETAS[tema_atual()]
    estado=st.session_state.get(chave) or {}
    alteracoes=estado.get('edited_rows',{}) if isinstance(estado,dict) else {}
    estilos=[]
    for i in range(len(tabela)):
        linha=alteracoes.get(i,alteracoes.get(str(i),{}))
        selecionada=bool(linha.get('Selecionar',marcar))
        if selecionada:
            css=f"background-color:{p['selecao']};color:{p['selecao_texto']};font-weight:600"
        else:
            css=f"background-color:{p['alternada'] if i%2 else p['superficie']};color:{p['texto']}"
        estilos.append([css if c!='Selecionar' else '' for c in tabela.columns])
    return tabela.style.apply(lambda _:pd.DataFrame(estilos,index=tabela.index,columns=tabela.columns),axis=None)


def status_badge(texto,tipo='info'):
    tipo=tipo if tipo in {'ok','info','warn','error'} else 'info'
    return f'<span class="radar-status {tipo}">{html.escape(str(texto))}</span>'
