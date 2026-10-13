"""Atalhos e gestão das configurações de busca, sem executar consultas ao abrir."""
from datetime import date,datetime
import sqlite3

import streamlit as st
import buscas_salvas as bs
import filtros_precos as fp
from radar_paths import DB_PESQUISAS

ROTULOS={'contratacoes':'Contratações e itens','fornecedores':'Fornecedores e resultados'}
PERIODOS={'Manter o período original':'original','Últimos dias':'ultimos_dias','Todo o período disponível':'todo_periodo'}
MODOS={'todas':'Todas as palavras','frase':'Frase exata','qualquer':'Qualquer palavra'}
STATUS={'todos':'Todos','ok':'Itens coletados','indisponivel':'Indisponível no PNCP','pendente':'Pendente / retry'}
TIC={'todos':'Todos','tic':'Somente TIC','nao_tic':'Não TIC','nao_classificado':'Não classificado'}


def _erro(exc):
    st.error('Não foi possível concluir a operação da busca salva: '+str(exc))


def mostrar_atalhos():
    if st.session_state.get('buscas_salvas_flash'):st.success(st.session_state.pop('buscas_salvas_flash'))
    with st.expander('Pesquisas salvas'):
        st.caption('Guarde termos e filtros para executar novamente sobre a base local atual.')
        arquivadas=st.checkbox('Mostrar buscas arquivadas',key='salvas_arquivadas')
        try:rows=bs.listar(DB_PESQUISAS,arquivadas)
        except (ValueError,sqlite3.Error,OSError) as exc:_erro(exc);return None
        por_id={r['id']:r for r in rows}
        if 'busca_salva_foco' in st.session_state:
            foco=st.session_state.pop('busca_salva_foco')
            st.session_state['salvas_escolhida']=foco if foco in por_id else None
            st.session_state.pop('busca_salva_contexto',None)
        atual=st.session_state.get('salvas_escolhida')
        st.session_state['salvas_escolhida']=atual if atual in por_id else None
        escolhido=st.selectbox('Busca salva',[None,*por_id],key='salvas_escolhida',placeholder='Selecione uma busca',
            format_func=lambda i:'Selecione uma busca' if i is None else por_id[i]['nome']+' · '+ROTULOS.get(por_id[i]['tipo'],'Tipo desconhecido')+(' · arquivada' if por_id[i]['arquivada'] else ''))
        selecionada=por_id.get(escolhido)
        if selecionada:
            anterior=st.session_state.get('busca_salva_contexto')
            if anterior and anterior['id']==escolhido:
                # A revisão que o usuário selecionou acompanha seus formulários.
                # Reler a lista no envio não autoriza sobrescrever outra aba.
                selecionada['revisao']=anterior['revisao']
            st.session_state['busca_salva_contexto']=selecionada
            modo=selecionada['periodo']
            periodo=(f"Últimos {selecionada['dias']} dias, até "+('hoje' if selecionada['incluir_hoje'] else 'ontem')) if modo=='ultimos_dias' else ('Todo o período disponível' if modo=='todo_periodo' else 'Datas originais da busca')
            st.caption('Período: '+periodo)
            if selecionada['ultimo_uso_em']:
                try:ultimo=datetime.fromisoformat(selecionada['ultimo_uso_em']).strftime('%d/%m/%Y às %H:%M')
                except (ValueError,TypeError):ultimo=str(selecionada['ultimo_uso_em'])
                st.caption('Última execução solicitada: '+ultimo)
        else:
            st.session_state.pop('busca_salva_contexto',None)
            if not rows:st.info('Depois de pesquisar, use Salvar esta busca para criar seu primeiro atalho.')
        executar,excluir=st.columns([2,1])
        if executar.button('Executar busca salva',key='salvas_executar',disabled=selecionada is None or bool(selecionada['arquivada'])):
            try:
                atualizada=bs.obter(DB_PESQUISAS,escolhido)
                pedido=bs.resolver(atualizada)
                st.session_state['busca_salva_contexto']=atualizada
                bs.registrar_uso(DB_PESQUISAS,escolhido)
                return pedido
            except (ValueError,sqlite3.Error,OSError) as exc:_erro(exc)
        if excluir.button('Excluir pesquisa salva',key='salvas_excluir',disabled=selecionada is None):
            from painel_exclusoes import abrir
            abrir(DB_PESQUISAS,'busca',escolhido)
        if selecionada and st.checkbox('Editar nome ou arquivamento',key=f"salvas_editar_{escolhido}"):
            with st.form(f'salvas_dados_{escolhido}'):
                nome=st.text_input('Nome da busca salva',selecionada['nome'],max_chars=160,key=f'salvas_nome_{escolhido}')
                arquivar=st.checkbox('Busca arquivada',value=bool(selecionada['arquivada']),key=f'salvas_arquivar_{escolhido}')
                if st.form_submit_button('Salvar dados da busca',key='salvas_salvar_dados'):
                    try:
                        bs.alterar_dados(DB_PESQUISAS,escolhido,nome=nome,arquivada=arquivar,revisao=selecionada['revisao'])
                        st.session_state['buscas_salvas_flash']='Dados da busca salvos.'
                        st.session_state['busca_salva_foco']=escolhido
                        st.rerun()
                    except (ValueError,sqlite3.Error,OSError) as exc:_erro(exc)
    return None


def aplicar(pedido,meta):
    """Restaura widgets antes de sua criação e registra uma execução explícita."""
    f=pedido['filtros'];tipo=pedido['tipo']
    st.session_state['tipo_pesquisa']=ROTULOS[tipo]
    selecionada=st.session_state.get('busca_salva_contexto')
    if selecionada and selecionada['id']==pedido['id']:
        st.session_state['salvar_periodo_'+tipo]=next(rotulo for rotulo,valor in PERIODOS.items() if valor==selecionada['periodo'])
        st.session_state['salvar_dias_'+tipo]=selecionada['dias']
        st.session_state['salvar_hoje_'+tipo]=bool(selecionada['incluir_hoje'])
    st.session_state.pop('detalhe_id',None)
    prefix='busca' if tipo=='contratacoes' else 'forn'
    st.session_state[prefix+'_unidade']=f.get('unidade','')
    st.session_state[prefix+'_preco_min']=fp.texto_preco(f.get('preco_min'))
    st.session_state[prefix+'_preco_max']=fp.texto_preco(f.get('preco_max'))
    if tipo=='contratacoes':st.session_state['busca_preco_tipo']=f.get('preco_tipo','Estimado')
    if tipo=='contratacoes':
        valores={'busca_texto_input':f['texto'],'busca_modo_input':MODOS[f['modo_texto']],
            'busca_status_input':STATUS[f['status_itens']],'busca_homologadas_input':f['somente_homologadas'],
            'busca_modalidades_input':list(f['modalidades']),'busca_ufs_input':list(f['ufs']),
            'busca_orgao_input':f['orgao'],'busca_tic_status_input':TIC[f['tic_status']],
            'busca_tic_categoria_input':list(f['tic_categorias']),'busca_por_pagina_input':pedido['por_pagina']}
        inicio=f['data_inicio'] or meta.get('data_min');fim=f['data_fim'] or meta.get('data_max')
        valores['busca_periodo_input']=(date.fromisoformat(str(inicio)[:10]) if inicio else date.today(),
                                      date.fromisoformat(str(fim)[:10]) if fim else date.today())
        if valores['busca_periodo_input'][0]>valores['busca_periodo_input'][1]:
            # Uma base sem dados no intervalo não altera os filtros executados.
            valores['busca_periodo_input']=(date.fromisoformat(str(f['data_inicio'] or f['data_fim'])[:10]),)*2
        for k,v in valores.items():st.session_state[k]=v
        st.session_state['busca_executada']={**f,'modo_rotulo':MODOS[f['modo_texto']],'por_pagina':pedido['por_pagina']}
        st.session_state['pagina_busca']=1
    else:
        valores={'forn_termo':f['fornecedor'],'forn_item':f['item'],'forn_orgao':f['orgao'],
            'forn_inicio':date.fromisoformat(f['data_inicio']) if f['data_inicio'] else None,
            'forn_fim':date.fromisoformat(f['data_fim']) if f['data_fim'] else None,
            'forn_cancelados':f['incluir_cancelados'],'forn_tamanho':pedido['por_pagina']}
        for k,v in valores.items():st.session_state[k]=v
        st.session_state['forn_busca_salva_pendente']=pedido


def mostrar_salvar_atual(tipo,filtros,por_pagina):
    if not filtros:return
    with st.expander('Salvar esta busca'):
        st.caption('Guarda os termos e filtros da última busca executada. Para salvar mudanças feitas nos campos, clique primeiro em Pesquisar.')
        periodo=st.selectbox('Período ao executar novamente',list(PERIODOS),key='salvar_periodo_'+tipo)
        dias=30;incluir_hoje=False
        if PERIODOS[periodo]=='ultimos_dias':
            dias=int(st.number_input('Quantidade de dias',min_value=1,max_value=3650,value=30,step=1,key='salvar_dias_'+tipo))
            incluir_hoje=st.checkbox('Incluir hoje na janela',value=False,key='salvar_hoje_'+tipo)
            st.caption('A janela é recalculada quando você executa a busca salva.')
        selecionada=st.session_state.get('busca_salva_contexto')
        pode_atualizar=bool(selecionada and selecionada['tipo']==tipo and not selecionada['arquivada'])
        if not pode_atualizar:st.session_state['salvar_substituir_'+tipo]=False
        if pode_atualizar:st.caption('Busca selecionada para atualização: '+selecionada['nome'])
        with st.form('salvar_busca_'+tipo):
            nome=st.text_input('Nome da busca',max_chars=160,key='salvar_nome_'+tipo,
                               help='Ao atualizar uma busca selecionada, deixar vazio mantém seu nome.')
            atualizar=st.checkbox('Atualizar os filtros da busca selecionada',disabled=not pode_atualizar,key='salvar_substituir_'+tipo)
            if st.form_submit_button('Salvar busca',key='salvar_executada_'+tipo):
                try:
                    if atualizar and not pode_atualizar:raise ValueError('Selecione uma busca ativa do mesmo tipo para atualizar.')
                    campos={k:v for k,v in filtros.items() if k in (bs.FiltrosBusca.__dataclass_fields__ if tipo=='contratacoes' else bs.FiltrosFornecedor.__dataclass_fields__)}
                    ident=bs.salvar(DB_PESQUISAS,nome.strip() or (selecionada['nome'] if atualizar else ''),tipo,campos,por_pagina,
                        periodo=PERIODOS[periodo],dias=dias,incluir_hoje=incluir_hoje,
                        busca_id=selecionada['id'] if atualizar else None,revisao=selecionada['revisao'] if atualizar else None)
                    st.session_state['buscas_salvas_flash']='Busca salva. Use Pesquisas salvas para executá-la novamente.'
                    st.session_state['busca_salva_foco']=ident
                    st.rerun()
                except (ValueError,sqlite3.Error,OSError) as exc:_erro(exc)
