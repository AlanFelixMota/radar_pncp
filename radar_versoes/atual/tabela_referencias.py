"""Tabela de referências: leitura completa e seleção por índice de origem."""
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components
import pandas as pd


_tabela = components.declare_component(
    'radar_pncp_tabela_referencias', path=str(Path(__file__).parent / 'tabela_referencias'))


def selecionar(tabela, chave, marcar=False):
    """Somente as caixas são editáveis; os dados retornam da tabela original.

    O estado usa o contrato edited_rows do editor nativo. Ordenar e redimensionar
    no navegador não mudam a identidade das linhas nem os preços copiados.
    """
    inicial = {'edited_rows': {}, 'added_rows': [], 'deleted_rows': []}
    estado = st.session_state.get(chave, inicial)
    estado = _tabela(
        columns=list(tabela.columns),
        rows=[{k: (None if pd.isna(v) else str(v)) for k, v in row.items()
               if k != 'Selecionar'} for row in tabela.to_dict('records')],
        checked=marcar, state=estado, key=chave, default=estado)
    editada = tabela.copy()
    edits = estado.get('edited_rows', {}) if isinstance(estado, dict) else {}
    editada['Selecionar'] = [
        edits.get(str(i), edits.get(i, {})).get('Selecionar', marcar) is True
        for i in range(len(editada))]
    return editada
