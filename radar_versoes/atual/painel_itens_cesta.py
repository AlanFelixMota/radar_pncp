"""Cadastro e edição dos itens definidos pelo responsável pela pesquisa."""
import json
import sqlite3
import streamlit as st
from radar_theme import data_hora
import cestas
import itens_cesta as ic


def revisoes_vistas(pesquisa):
    chave='itens_revisoes_'+str(pesquisa['id'])
    if chave not in st.session_state:
        st.session_state[chave]={i['id']:i['revisao'] for i in pesquisa.get('itens',[])}
    return st.session_state[chave]


def revisao_para(pesquisa,item_id):
    if not item_id:return None
    revisao=revisoes_vistas(pesquisa).get(item_id)
    if revisao is None:raise ValueError('A lista de itens mudou. Clique em Recarregar itens antes de salvar.')
    return revisao


def limpar_revisoes(pid):st.session_state.pop('itens_revisoes_'+str(pid),None)


def mostrar(db,pesquisa,referencias):
    pid=pesquisa['id'];itens=pesquisa.get('itens',[])
    with st.expander('Itens desta pesquisa',expanded=bool(itens)):
        st.caption('Cadastre os itens que você pretende pesquisar. Depois vincule as referências na avaliação individual ou em lote. O número deste cadastro é diferente do número do item na contratação de origem.')
        if itens:
            st.dataframe([{'Número':i['numero'],'Item da pesquisa':i['nome'],'Unidade pretendida':i['unidade'],
                'Referências':i['referencias'],'Incluir':i['incluidas'],'A avaliar':i['a_avaliar'],'Excluir':i['excluidas']}
                for i in ic.painel(itens,referencias)],hide_index=True,use_container_width=True)
            soltas=sum(not r.get('item_pesquisa_id') for r in referencias)
            st.caption(f'{soltas} referência(s) sem item estruturado; seus grupos livres continuam preservados.')
            if st.button('Recarregar itens',key='item_recarregar_'+str(pid)):
                st.session_state[f'item_editar_foco_{pid}']=st.session_state.get(f'item_editar_{pid}')
                limpar_revisoes(pid)
                st.session_state.pop(f'item_contexto_{pid}',None)
                st.session_state['item_edit_epoch_'+str(pid)]=st.session_state.get('item_edit_epoch_'+str(pid),0)+1
                st.rerun()
        epoch=st.session_state.get('item_novo_epoch_'+str(pid),0)
        prefix=f'item_novo_{pid}_{epoch}'
        numeros={i['numero'] for i in itens};proximo=next((n for n in range(1,10000) if n not in numeros),9999)
        with st.form(prefix):
            a,b=st.columns([1,4])
            numero=a.number_input('Número do item da pesquisa',min_value=1,max_value=9999,value=proximo,step=1,key=prefix+'_numero')
            nome=b.text_input('Nome do item da pesquisa',max_chars=130,key=prefix+'_nome')
            descricao=st.text_area('Descrição / critérios de comparabilidade',max_chars=4000,key=prefix+'_descricao')
            unidade=st.text_input('Unidade pretendida (opcional)',max_chars=200,key=prefix+'_unidade')
            st.caption('Descrição e unidade orientam sua conferência; não convertem preços nem aprovam referências automaticamente.')
            if st.form_submit_button('Cadastrar item',disabled=bool(pesquisa['arquivada']),key='item_cadastrar_'+str(pid)):
                try:
                    ic.salvar(db,pid,numero=int(numero),nome=nome,descricao=descricao,unidade=unidade)
                    limpar_revisoes(pid)
                    st.session_state['cesta_flash']='Item da pesquisa cadastrado. Vincule as referências pela avaliação.'
                    st.session_state['item_novo_epoch_'+str(pid)]=epoch+1
                    st.rerun()
                except (ValueError,sqlite3.Error,OSError) as exc:st.error('Não foi possível cadastrar o item: '+str(exc))
        if not itens:return
        por_id={i['id']:i for i in itens};escolha_key=f'item_editar_{pid}'
        foco=f'item_editar_foco_{pid}'
        atual=st.session_state.pop(foco) if foco in st.session_state else st.session_state.get(escolha_key)
        st.session_state[escolha_key]=atual if atual in por_id else None
        escolhido=st.selectbox('Editar item da pesquisa',[None,*por_id],
            format_func=lambda i:'Selecione um item' if i is None else ic.rotulo(por_id[i]),key=escolha_key,placeholder='Selecione um item')
        contexto_key=f'item_contexto_{pid}'
        if escolhido is None:st.session_state.pop(contexto_key,None);return
        contexto=st.session_state.get(contexto_key)
        if contexto is None or contexto['id']!=escolhido:
            contexto=por_id[escolhido];st.session_state[contexto_key]=contexto
        epoch=st.session_state.get('item_edit_epoch_'+str(pid),0)
        prefix=f"item_edit_{escolhido}_{contexto['revisao']}_{epoch}"
        with st.form(prefix):
            numero=st.number_input('Número',min_value=1,max_value=9999,value=contexto['numero'],step=1,key=prefix+'_numero')
            nome=st.text_input('Nome do item',value=contexto['nome'],max_chars=130,key=prefix+'_nome')
            descricao=st.text_area('Descrição do item',value=contexto['descricao'],max_chars=4000,key=prefix+'_descricao')
            unidade=st.text_input('Unidade pretendida',value=contexto['unidade'],max_chars=200,key=prefix+'_unidade')
            if st.form_submit_button('Salvar item',disabled=bool(pesquisa['arquivada']),key='item_salvar_'+str(pid)):
                try:
                    ic.salvar(db,pid,numero=int(numero),nome=nome,descricao=descricao,unidade=unidade,
                              item_id=escolhido,revisao=contexto['revisao'])
                    st.session_state.pop(contexto_key,None)
                    limpar_revisoes(pid)
                    st.session_state['cesta_flash']='Item atualizado. As referências continuam vinculadas ao mesmo item.'
                    st.rerun()
                except (ValueError,sqlite3.Error,OSError) as exc:st.error('Não foi possível editar o item: '+str(exc))
        if st.checkbox('Ver histórico do item',key=f'item_historico_{escolhido}'):
            with cestas.conectar(db) as con:
                registros=con.execute('SELECT * FROM historico_itens_pesquisa WHERE item_id=? ORDER BY id DESC',(escolhido,)).fetchall()
            st.dataframe([{'Revisão':r['revisao'],'Data':data_hora(r['alterado_em']),**json.loads(r['dados_json'])} for r in registros],
                         hide_index=True,use_container_width=True)
