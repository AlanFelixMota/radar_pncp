"""Seleção explícita de referências e revisão de cestas de pesquisa."""
import hashlib
import json
from pathlib import Path
import sqlite3

import streamlit as st
import pandas as pd
import cestas
import radar_paths
import radar_theme as ui
import tabela_referencias


def _db(db=None):
    return Path(db) if db is not None else radar_paths.DB_PESQUISAS


def _erro(exc):
    st.error('Não foi possível salvar a pesquisa: '+str(exc))


def _confirmar_inclusao(resultado):
    mensagem=f"{resultado['adicionadas']} referência(s) adicionada(s) como A avaliar. {resultado['ja_existiam']} já estava(m) salva(s); as cópias e avaliações anteriores foram mantidas."
    if resultado.get('item_destino') and resultado['adicionadas']:
        mensagem+=f" Novas referências vinculadas a {resultado['item_destino']}."
    if resultado.get('contratacoes') is not None:
        mensagem+=f" {resultado['contratacoes']} contratação(ões) consultada(s)."
    if resultado.get('sem_referencias'):
        falta='resultados homologados não cancelados' if resultado.get('tipo_preco')=='Homologado' else 'itens'
        mensagem+=f" {resultado['sem_referencias']} contratação(ões) sem {falta} na base local; nenhuma referência delas foi adicionada."
    st.session_state['cesta_flash']=mensagem


def incluir_lote(operacao,db_compras,**opcoes):
    """Chamado somente depois do clique explícito no botão de inclusão."""
    pid=st.session_state.get('cesta_ativa_id')
    if not pid:
        st.info('Escolha uma cesta antes de adicionar referências.');return
    import cestas_lote
    executar=getattr(cestas_lote,operacao)
    andamento=st.empty()
    try:
        with st.spinner('Adicionando referências à cesta…'):
            resultado=executar(db_compras,_db(),pid,
                progresso=lambda n:andamento.caption(f'{n:,} referências processadas…'.replace(',','.')),
                **opcoes)
        _confirmar_inclusao(resultado)
        st.rerun()
    except (ValueError,sqlite3.Error,OSError) as exc:
        andamento.empty();_erro(exc)


def criar_formulario(db,contexto,compacto=False):
    contexto_visual=st.popover('Nova cesta',use_container_width=True) if compacto else st.expander('Nova cesta de pesquisa')
    with contexto_visual:
        with st.form('nova_cesta_'+contexto):
            nome=st.text_input('Nome da pesquisa',max_chars=160,key='nome_cesta_'+contexto)
            objetivo=st.text_area('Objeto / objetivo da pesquisa',max_chars=4000,key='objetivo_cesta_'+contexto)
            if st.form_submit_button('Criar cesta',key='criar_cesta_'+contexto):
                try:
                    st.session_state['cesta_ativa_id']=cestas.criar(db,nome,objetivo)
                    st.session_state['cesta_flash']='Cesta criada. Selecione as referências que deseja guardar.'
                    st.rerun()
                except (ValueError,sqlite3.Error,OSError) as exc:_erro(exc)


def _trocar_destino(chave):
    st.session_state['cesta_ativa_id']=st.session_state.get(chave)


def _criar_destino_local(db,prefix):
    try:
        pid=cestas.criar(db,st.session_state.get(prefix+'_nome',''),
                         st.session_state.get(prefix+'_objetivo',''))
        st.session_state['cesta_ativa_id']=pid
        st.session_state[prefix+'_nome']=''
        st.session_state[prefix+'_objetivo']=''
        st.session_state[prefix+'_mensagem']='Cesta criada e selecionada. Confira a seleção e confirme a inclusão.'
    except (ValueError,sqlite3.Error,OSError) as exc:
        st.session_state[prefix+'_erro']='Não foi possível criar a cesta: '+str(exc)


def destino_na_selecao(db,contexto):
    """Destino no contexto da seleção; mudar a cesta não recria a tabela."""
    prefix='destino_local_'+hashlib.sha256((str(db.resolve())+'|'+contexto).encode()).hexdigest()[:16]
    try:pesquisas=cestas.listar(db)
    except (sqlite3.Error,OSError) as exc:_erro(exc);return None
    por_id={p['id']:p for p in pesquisas}
    atual=st.session_state.get('cesta_ativa_id')
    if atual not in por_id:atual=None
    chave=prefix+'_escolha'
    st.session_state[chave]=atual
    pid=st.selectbox('Cesta de destino',[None,*por_id],key=chave,
        format_func=lambda i:'Escolha uma cesta' if i is None else f"{por_id[i]['nome']} (#{i})",
        on_change=_trocar_destino,args=(chave,),placeholder='Escolha uma cesta')
    st.session_state['cesta_ativa_id']=pid
    with st.expander('Criar nova cesta aqui'):
        with st.form(prefix+'_nova'):
            st.text_input('Nome da nova cesta',max_chars=160,key=prefix+'_nome')
            st.text_area('Objeto / objetivo da nova cesta (opcional)',max_chars=4000,key=prefix+'_objetivo')
            st.form_submit_button('Criar e selecionar cesta',key=prefix+'_criar',
                on_click=_criar_destino_local,args=(db,prefix))
    if st.session_state.get(prefix+'_mensagem'):st.success(st.session_state.pop(prefix+'_mensagem'))
    if st.session_state.get(prefix+'_erro'):st.error(st.session_state.pop(prefix+'_erro'))
    if not pid:st.info('Marque as referências e escolha ou crie uma cesta aqui para adicioná-las.')
    return pid


def mostrar_destino(db=None):
    with st.container(border=True,key='radar_pncp_destino_pesquisa'):
        _mostrar_destino(db)


def _mostrar_destino(db=None):
    db=_db(db)
    if st.session_state.get('cesta_base')!=str(db.resolve()):
        st.session_state['cesta_base']=str(db.resolve())
        st.session_state.pop('cesta_ativa_id',None)
    try:pesquisas=cestas.listar(db)
    except (sqlite3.Error,OSError) as exc:
        st.warning('Não foi possível abrir as cestas: '+str(exc));return
    if st.session_state.get('cesta_flash'):st.success(st.session_state.pop('cesta_flash'))
    if pesquisas:
        por_id = {p['id']: p for p in pesquisas}
        opcoes = list(por_id)
        atual = st.session_state.get('cesta_ativa_id')
        if atual in por_id:st.session_state['cesta_destino_pesquisa']=atual
        selecionada = st.selectbox('Cesta para guardar referências', opcoes,
            index=opcoes.index(atual) if atual in opcoes else 0,
            format_func=lambda i: por_id[i]['nome'], key='cesta_destino_pesquisa',
            on_change=_trocar_destino,args=('cesta_destino_pesquisa',))
        st.session_state['cesta_ativa_id'] = selecionada
        st.caption(f"{por_id[selecionada]['total']} referência(s) salva(s). Adicione referências individualmente ou em lote; as novas entram como A avaliar.")
    else:
        st.session_state.pop('cesta_ativa_id', None)
        st.caption('Crie uma cesta em Minhas pesquisas para guardar referências e exportar para Excel.')
    if st.button('Abrir minhas pesquisas', key='abrir_cesta_pesquisa'):
        st.session_state['pagina_principal'] = 'Minhas pesquisas'
        st.rerun()


def rotulo(d):
    def valor(v):
        return 'sem preço' if v is None else 'R$ '+f'{v:,.4f}'.replace(',','X').replace('.',',').replace('X','.')
    seq=f" · resultado {d['sequencial_resultado']}" if d.get('sequencial_resultado') is not None else ''
    fornecedor=' · '+d['nome_fornecedor'] if d.get('nome_fornecedor') else ''
    cancelado=' · CANCELADO' if d.get('cancelado') else ''
    return f"Item {d['numero_item']}{seq} · {d.get('descricao') or 'Sem descrição'} · {d['tipo_preco']} · {valor(d.get('valor_unitario'))}{fornecedor}{cancelado}"


def _marcar_tabela(prefix, valor):
    st.session_state[prefix+'_marcar'] = valor
    st.session_state[prefix+'_epoch'] = st.session_state.get(prefix+'_epoch',0)+1


def _selecionadas_do_editor(referencias, estado, marcar=False):
    """Resolve só as caixas; preços e identidades vêm da cópia exibida."""
    edits = estado.get('edited_rows',{}) if isinstance(estado,dict) else {}
    return [r for i,r in enumerate(referencias)
            if edits.get(i,edits.get(str(i),{})).get('Selecionar',marcar) is True]


def _adicionar_tabela(db, pid, referencias, prefix, editor_key, marcar, item=None):
    # O callback limpa a seleção antes da renderização e mantém o popup aberto.
    selecionadas = _selecionadas_do_editor(referencias,st.session_state.get(editor_key,{}),marcar)
    if not selecionadas:
        st.session_state[prefix+'_erro']='Marque pelo menos uma referência na tabela.'
        return
    try:
        resultado=cestas.adicionar(db,pid,selecionadas,
            item_pesquisa_id=item['id'] if item else None,item_revisao=item['revisao'] if item else None)
        _confirmar_inclusao(resultado)
        st.session_state[prefix+'_confirmacao']=st.session_state['cesta_flash']
        _marcar_tabela(prefix,False)
    except (ValueError,sqlite3.Error,OSError) as exc:
        st.session_state[prefix+'_erro']='Não foi possível adicionar: '+str(exc)


def _valor_tabela(valor, moeda=False):
    if valor is None:return '—'
    try:
        texto=f'{float(valor):,.4f}'.replace(',','X').replace('.',',').replace('X','.')
        return ('R$ ' if moeda else '')+texto
    except (TypeError,ValueError):return str(valor)


def _descricao_tabela(referencia):
    descricao=referencia.get('descricao') or 'Descrição ainda não disponível na base local'
    complementar=str(referencia.get('informacao_complementar') or '').strip()
    if complementar and complementar!=str(descricao).strip():
        descricao+='\n\nInformação complementar: '+complementar
    return descricao


def selecionar(referencias,contexto,db=None):
    """Recebe somente os registros exibidos, sem consultar ou expandir a busca."""
    if not referencias:return
    db=_db(db)
    pid=destino_na_selecao(db,contexto)
    import itens_cesta
    item=None
    if pid:
        try:itens=itens_cesta.listar(db,pid)
        except (sqlite3.Error,OSError) as exc:_erro(exc);return
        por_item={i['id']:i for i in itens}
        chave_item='item_destino_'+hashlib.sha256((str(db.resolve())+'|'+contexto+'|'+str(pid)).encode()).hexdigest()[:16]
        if st.session_state.get(chave_item) not in por_item:st.session_state[chave_item]=None
        ident=st.selectbox('Item da pesquisa para vincular',[None,*por_item],key=chave_item,
            format_func=lambda i:'Sem vincular por enquanto' if i is None else itens_cesta.rotulo(por_item[i]))
        item=por_item.get(ident)
        if item:
            st.caption('Todas as novas referências marcadas serão vinculadas a '+itens_cesta.rotulo(item)+'.')
        elif not itens:
            st.caption('Esta cesta ainda não tem itens cadastrados. Você pode adicioná-los em Minhas pesquisas → Itens e dados.')
    por_chave={cestas.chave(r):r for r in referencias}
    referencias=list(por_chave.values())
    # Renova seleção se a página ou os preços mudarem; trocar a cesta e o horário
    # de renderização isolado não deve apagar a seleção no clique seguinte.
    assinatura=[{k:v for k,v in r.items() if k!='consultado_em'} for r in referencias]
    digest=hashlib.sha256(json.dumps(assinatura,sort_keys=True,ensure_ascii=False,default=str).encode()).hexdigest()[:16]
    base=hashlib.sha256(str(db.resolve()).encode()).hexdigest()[:12]
    prefix=f'cesta_sel_{contexto}_{base}_{digest}'
    epoch=st.session_state.get(prefix+'_epoch',0)
    if st.session_state.get(prefix+'_confirmacao'):
        st.success(st.session_state.pop(prefix+'_confirmacao'))
    if st.session_state.get(prefix+'_erro'):
        st.error(st.session_state.pop(prefix+'_erro'))
    tudo,limpar=st.columns(2)
    tudo.button('Marcar toda esta tabela',key=prefix+'_todas',
                on_click=_marcar_tabela,args=(prefix,True))
    limpar.button('Limpar seleção',key=prefix+'_limpar',
                  on_click=_marcar_tabela,args=(prefix,False))
    marcar=st.session_state.get(prefix+'_marcar',False)
    linhas=[]
    homologados=any(r['tipo_preco']=='Homologado' for r in referencias)
    for r in referencias:
        linha={'Selecionar':marcar,'Item':r['numero_item'],
               'Descrição':_descricao_tabela(r),
               'Unidade':r.get('unidade_medida') or '—',
               'Quantidade':_valor_tabela(r.get('quantidade')),
               'Preço unitário':_valor_tabela(r.get('valor_unitario'),True),
               'Tipo de preço':r['tipo_preco']}
        if homologados:
            linha.update({'Fornecedor':r.get('nome_fornecedor') or r.get('ni_fornecedor') or 'Não informado',
                          'Resultado':r.get('sequencial_resultado'),
                          'Cancelado':'Sim' if r.get('cancelado') else 'Não'})
        linhas.append(linha)
    tabela=pd.DataFrame(linhas)
    editor_key=prefix+'_tabela_'+str(epoch)
    editada=tabela_referencias.selecionar(tabela,editor_key,marcar)
    quantidade=int(editada['Selecionar'].sum())
    st.button(f'Adicionar selecionados ({quantidade})',disabled=not pid or not quantidade,
              type='primary',key=prefix+'_adicionar',on_click=_adicionar_tabela,
              args=(db,pid,referencias,prefix,editor_key,marcar,item))
    st.caption(f'{len(referencias)} referência(s) nesta tabela. As selecionadas serão salvas como A avaliar; preços e dados de origem são preservados.')


def mostrar_resumo(refs):
    from resumo_cesta import consolidar
    resumo=consolidar(refs)
    with st.expander('Resumo de preços por grupo',expanded=True):
        st.caption('Somente referências marcadas como Incluir, com grupo, unidade e preço unitário positivo, sem indicação de cancelamento. Preços estimados e homologados e unidades diferentes são calculados separadamente.')
        if resumo['grupos']:
            st.dataframe([{'Grupo':g['grupo'],'Tipo de preço':g['tipo_preco'],'Unidade':g['unidade'],
                'Referências':g['referencias'],'Contratações':g['contratacoes'],'Fornecedores':g['fornecedores'],
                'Média (R$)':g['media'],'Mediana (R$)':g['mediana'],'Mínimo (R$)':g['minimo'],
                'Máximo (R$)':g['maximo'],'Desvio padrão (R$)':g['desvio_padrao'],'CV (%)':g['cv_percentual']}
                for g in resumo['grupos']], hide_index=True,use_container_width=True,
                column_config={**{n:st.column_config.NumberColumn(format='R$ %.4f') for n in
                    ['Média (R$)','Mediana (R$)','Mínimo (R$)','Máximo (R$)','Desvio padrão (R$)']},
                    'CV (%)':st.column_config.NumberColumn(format='%.2f')})
            st.caption('Uma observação por referência, sem ponderar pela quantidade comprada. Desvio padrão amostral e CV só aparecem com duas ou mais referências. O sistema não escolhe o preço final nem exclui valores extremos automaticamente.')
        else:
            st.info('Ainda não há referências aptas para calcular o resumo. Informe um grupo e marque referências comparáveis como Incluir, individualmente ou em lote.')
        if resumo['fora_do_resumo']:
            st.warning(f"{resumo['fora_do_resumo']} referência(s) marcada(s) como Incluir ficou(ram) fora dos cálculos. Confira os motivos abaixo.")
            st.dataframe([{'Motivo':m,'Referências':n} for m,n in resumo['motivos'].items()],
                         hide_index=True,use_container_width=True)
            st.caption('Uma referência pode apresentar mais de um motivo. A decisão salva na cesta é preservada.')


def mostrar_avaliacao_lote(db,pesquisa,refs,termo='',tipo='Todos',*,mostrar_item=False):
    from painel_itens_cesta import revisao_para,revisoes_vistas
    import conferencia_cesta as cc
    revisoes_vistas(pesquisa)
    pid=pesquisa['id']
    st.caption('Marque as referências para organizar ou avaliar; preços e fontes são somente para leitura.')
    candidatos=[r for r in refs if (tipo=='Todos' or r['dados']['tipo_preco']==tipo)
        and termo.strip().casefold() in str(r['dados'].get('descricao') or '').casefold()]
    por_id={r['id']:r for r in candidatos}
    assinatura=hashlib.sha256(json.dumps([str(Path(db).resolve()),pid,termo.strip().casefold(),tipo,
        [(r['id'],r['revisao'],cc.estado(r)) for r in candidatos]],ensure_ascii=False).encode()).hexdigest()[:16]
    prefix=f'revisao_tabela_{pid}_{assinatura}'
    tudo,limpar=st.columns(2)
    tudo.button('Marcar todas as referências deste filtro',key=prefix+'_todas',
        disabled=not candidatos or bool(pesquisa['arquivada']),on_click=_marcar_tabela,args=(prefix,True))
    limpar.button('Limpar marcações da revisão',key=prefix+'_limpar',
        disabled=not candidatos or bool(pesquisa['arquivada']),on_click=_marcar_tabela,args=(prefix,False))
    marcar=st.session_state.get(prefix+'_marcar',False)
    linhas=[{'Selecionar':marcar,'ID':r['id'],'Situação':r['situacao'],
        'Item da sua pesquisa':r.get('item_pesquisa_nome') or r['grupo'] or 'Sem vínculo',
        'Tipo de preço':r['dados']['tipo_preco'],'Item na contratação':r['dados']['numero_item'],
        'Descrição':r['dados'].get('descricao') or 'Sem descrição',
        'Unidade':r['dados'].get('unidade_medida') or '—',
        'Preço unitário':_valor_tabela(r['dados'].get('valor_unitario'),True),
        'Fornecedor':r['dados'].get('nome_fornecedor') or r['dados'].get('ni_fornecedor') or '—',
        'Cancelado':'Sim' if r['dados'].get('cancelado') else 'Não',
        'Órgão':r['dados'].get('orgao_razao_social'),'Controle PNCP':r['dados']['numero_controle_pncp'],
        'Conferência local':cc.estado(r)} for r in candidatos]
    selecionadas=[]
    if linhas:
        tabela=pd.DataFrame(linhas)
        editor_key=prefix+'_tabela_'+str(st.session_state.get(prefix+'_epoch',0))
        editada=st.data_editor(ui.tabela_selecao(tabela,editor_key,marcar),key=editor_key,
            hide_index=True,num_rows='fixed',use_container_width=True,height=min(420,36*(len(linhas)+1)+4),
            disabled=True if pesquisa['arquivada'] else [c for c in tabela.columns if c!='Selecionar'],
            column_order=['Selecionar','Situação',*(['Item da sua pesquisa'] if mostrar_item else []),'Descrição','Unidade','Preço unitário','Tipo de preço'],
            column_config={'Selecionar':st.column_config.CheckboxColumn(width=65),
                           'ID':st.column_config.NumberColumn(width='small'),
                           'Situação':st.column_config.TextColumn(width=100),
                           'Descrição':st.column_config.TextColumn(width=320),
                           'Unidade':st.column_config.TextColumn(width=90),
                           'Preço unitário':st.column_config.TextColumn(width=145),
                           'Item da sua pesquisa':st.column_config.TextColumn(width='medium'),
                           'Tipo de preço':st.column_config.TextColumn(width=125)})
        selecionadas=[r['id'] for i,r in enumerate(candidatos) if bool(editada.iloc[i]['Selecionar'])]
    else:st.info('Nenhuma referência nos filtros de revisão selecionados.')
    st.caption(f'{len(candidatos)} referência(s) neste filtro · {len(selecionadas)} marcada(s) para avaliação. Resumo e exportações usam a cesta completa.')
    with st.expander('Organizar e avaliar as referências marcadas',expanded=bool(selecionadas)):
        st.caption('Somente as referências marcadas na tabela serão alteradas. Campos deixados vazios preservam os valores existentes.')
        with st.form(f"avaliar_lote_{pid}"):
            decisao=st.selectbox('Decisão para o lote',['Manter decisões atuais',*cestas.SITUACOES],key=f'lote_decisao_{pid}')
            itens={i['id']:i for i in pesquisa.get('itens',[])}
            alvo_item=None
            if itens:
                from itens_cesta import rotulo as rotulo_item
                chave_item=f'lote_item_{pid}'
                atual=st.session_state.get(chave_item)
                st.session_state[chave_item]=atual if atual in [None,0,*itens] else None
                alvo_item=st.selectbox('Vinculação do lote ao item da pesquisa',[None,0,*itens],
                    format_func=lambda i:'Manter vinculações atuais' if i is None else ('Remover vinculação ao item' if i==0 else rotulo_item(itens[i])),
                    key=chave_item,placeholder='Manter vinculações atuais')
                st.caption('A vinculação não muda a decisão. Grupo livre só pode ser alterado em referências sem item; para desvincular, escolha Remover vinculação ao item.')
            grupo=st.text_input('Grupo / item para o lote',max_chars=160,key=f'lote_grupo_{pid}',
                               disabled=bool(alvo_item),
                               help='Use o mesmo grupo apenas para referências que você considera comparáveis. Unidades e tipos de preço continuam separados no resumo.')
            justificativa=st.text_area('Justificativa para o lote',max_chars=4000,key=f'lote_justificativa_{pid}',
                                      help='Para Excluir, cada referência precisa de uma justificativa nova ou já salva.')
            observacoes=st.text_area('Observações para o lote',max_chars=8000,key=f'lote_observacoes_{pid}')
            if st.form_submit_button(f'Salvar avaliação das selecionadas ({len(selecionadas)})',disabled=bool(pesquisa['arquivada']) or not selecionadas,key=f'lote_salvar_{pid}'):
                alvos=selecionadas
                try:
                    r=cestas.avaliar_lote(db,pid,{rid:por_id[rid]['revisao'] for rid in alvos},
                        situacao=None if decisao=='Manter decisões atuais' else decisao,
                        grupo=grupo.strip() or None,justificativa=justificativa.strip() or None,
                        observacoes=observacoes.strip() or None,item_pesquisa_id=alvo_item,
                        item_revisao=revisao_para(pesquisa,alvo_item),revisoes_itens=revisoes_vistas(pesquisa))
                    st.session_state['cesta_flash']=f"{r['alteradas']} avaliação(ões) salva(s). {r['sem_mudanca']} já estava(m) com esses valores."
                    st.rerun()
                except (ValueError,sqlite3.Error,OSError) as exc:_erro(exc)

    return selecionadas


def mostrar(db=None):
    db=_db(db)
    st.title('Minhas pesquisas')
    st.caption('Referências salvas para conferência e exportação. Os preços e as fontes ficam preservados como estavam na seleção.')
    aviso=st.empty()
    if st.session_state.get('cesta_flash'):aviso.success(st.session_state.pop('cesta_flash'))
    arquivadas=st.session_state.get('cestas_arquivadas',False)
    try:pesquisas=cestas.listar(db,arquivadas)
    except (sqlite3.Error,OSError) as exc:_erro(exc);return
    if not pesquisas:
        criar_formulario(db,'pagina')
        st.checkbox('Mostrar pesquisas arquivadas',key='cestas_arquivadas')
        st.info('Nenhuma cesta nesta lista. Crie uma pesquisa e adicione referências pela página Consultar.');return
    por_id={p['id']:p for p in pesquisas};opcoes=list(por_id)
    atual=st.session_state.get('cesta_ativa_id')
    chave_escolha='cesta_pesquisa_escolhida'
    st.session_state[chave_escolha]=atual if atual in por_id else opcoes[0]
    escolher,nova,apagar=st.columns([5,1.2,1.3],vertical_alignment='bottom')
    with escolher:
        pid=st.selectbox('Pesquisa',opcoes,index=opcoes.index(atual) if atual in opcoes else 0,
            format_func=lambda i:f"{por_id[i]['nome']} (#{i})"+(' — arquivada' if por_id[i]['arquivada'] else ''),
            key=chave_escolha,on_change=_trocar_destino,args=(chave_escolha,))
    with nova:criar_formulario(db,'pagina',compacto=True)
    if apagar.button('Apagar cesta',key='cesta_apagar',use_container_width=True):
        from painel_exclusoes import abrir
        abrir(db,'cesta',pid)
    st.session_state['cesta_ativa_id']=pid
    try:p,refs=cestas.carregar(db,pid)
    except (ValueError,sqlite3.Error,OSError) as exc:_erro(exc);return
    resumo,opcoes_lista=st.columns([5,2])
    with resumo:
        st.caption(f"{len(refs)} referências · Incluídas: {sum(r['situacao']=='Incluir' for r in refs)} · "
                   f"A avaliar: {sum(r['situacao']=='A avaliar' for r in refs)} · Excluídas: {sum(r['situacao']=='Excluir' for r in refs)}")
    with opcoes_lista:st.checkbox('Mostrar pesquisas arquivadas',key='cestas_arquivadas')
    if p['arquivada']:st.info('Pesquisa arquivada. Para reabrir, vá a Itens e dados → Dados da pesquisa, desmarque Arquivada e salve.')
    referencias,itens,analise,exportar=st.tabs(['Referências','Itens e dados','Análise e conferência','Exportar'])
    with referencias:
        if refs:mostrar_revisao(db,p,refs)
        else:st.info('Esta cesta está vazia. Pesquise e adicione os itens ou resultados desejados.')
    with itens:
        if p['objetivo']:st.write('**Objeto / objetivo:** '+p['objetivo'])
        from painel_itens_cesta import mostrar as mostrar_itens
        mostrar_itens(db,p,refs)
        with st.expander('Dados da pesquisa'):
            with st.form(f"editar_pesquisa_{pid}_{p['revisao']}"):
                nome=st.text_input('Nome',p['nome'],max_chars=160)
                objetivo=st.text_area('Objeto / objetivo',p['objetivo'],max_chars=4000)
                arquivar=st.checkbox('Arquivada',value=bool(p['arquivada']))
                if st.form_submit_button('Salvar pesquisa',key='salvar_pesquisa'):
                    try:
                        cestas.alterar_pesquisa(db,pid,nome=nome,objetivo=objetivo,arquivada=arquivar,revisao=p['revisao'])
                        st.rerun()
                    except (ValueError,sqlite3.Error,OSError) as exc:_erro(exc)
    with analise:
        if refs:
            import painel_conferencia_cesta as pc
            import conferencia_cesta as cc
            pc.mostrar(db,p,refs)
            mostrar_resumo(refs)
        else:st.info('Adicione referências para conferir e analisar os preços.')
    with exportar:
        if refs:
            from painel_relatorio import mostrar as mostrar_relatorio
            mostrar_relatorio(db,p,refs)
            with st.expander('Exportar para Excel',expanded=True):
                st.caption('O Excel inclui todas as referências, sua origem, o resumo de preços e os registros de conferência disponíveis. O resumo usa os preços e as avaliações salvos na cesta; o preço local da conferência aparece em coluna separada. Alterações feitas apenas no Excel não recalculam o resumo.')
                if st.button('Preparar Excel',key='cesta_preparar_excel'):
                    try:
                        from exportar_cesta import gerar_xlsx
                        st.session_state['cesta_excel']={'db':str(db.resolve()),'id':pid,'revisao':p['revisao'],
                            'bytes':gerar_xlsx(p,refs),'nome':f'RADAR_PNCP_pesquisa_{pid}.xlsx'}
                    except (ValueError,OSError) as exc:st.error('Não foi possível exportar: '+str(exc))
                arquivo=st.session_state.get('cesta_excel')
                if arquivo and (arquivo['db'],arquivo['id'],arquivo['revisao'])==(str(db.resolve()),pid,p['revisao']):
                    st.download_button('Baixar Excel',arquivo['bytes'],file_name=arquivo['nome'],
                        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',key='cesta_baixar_excel')
        else:st.info('Adicione referências antes de preparar os documentos.')

def _trocar_item_revisao(pid):
    # Ao mudar o alcance, nenhuma marcação de outro item deve reaparecer.
    prefix=f'revisao_tabela_{pid}_'
    for chave in list(st.session_state):
        if chave.startswith(prefix):st.session_state.pop(chave,None)


def mostrar_revisao(db,p,refs):
    import conferencia_cesta as cc
    import painel_conferencia_cesta as pc
    pid=p['id']
    st.subheader('Revisar referências')
    itens={i['id']:i for i in p.get('itens',[])}
    from painel_itens_cesta import revisoes_vistas,revisao_para
    revisoes_vistas(p)
    filtro_item=None
    if itens:
        from itens_cesta import rotulo as rotulo_item
        chave=f'cesta_filtro_item_{pid}';opcoes=[None,0,*itens]
        atual=st.session_state.get(chave)
        st.session_state[chave]=atual if atual in opcoes else None
        filtro_item=st.selectbox('Item da pesquisa para revisar',opcoes,
            format_func=lambda i:'Todos os itens e grupos livres' if i is None else ('Sem item estruturado' if i==0 else rotulo_item(itens[i])),
            key=chave,placeholder='Todos os itens e grupos livres',on_change=_trocar_item_revisao,args=(pid,))
        universo=[r for r in refs if filtro_item is None or (r.get('item_pesquisa_id') or 0)==filtro_item]
        alcance='Na cesta' if filtro_item is None else ('Sem item estruturado' if filtro_item==0 else 'Neste item, na cesta')
        st.caption(f"{alcance}: {len(universo)} referência(s) · Incluir: {sum(r['situacao']=='Incluir' for r in universo)} · A avaliar: {sum(r['situacao']=='A avaliar' for r in universo)} · Excluir: {sum(r['situacao']=='Excluir' for r in universo)}")
        if filtro_item:
            item=itens[filtro_item]
            with st.expander('Critérios deste item'):
                st.write(item['descricao'] or 'Nenhum critério de comparabilidade cadastrado.')
                st.caption('Unidade pretendida: '+(item['unidade'] or 'Não informada'))
                st.caption('Edite em Itens e dados → Itens desta pesquisa. Estes critérios orientam sua avaliação; não convertem preços nem aprovam referências automaticamente.')
        elif filtro_item==0:
            st.caption('Para organizar estas referências, marque-as na tabela e use Vinculação do lote ao item da pesquisa. Os grupos livres continuam preservados.')
    with st.expander('Filtros de revisão'):
        filtro=st.selectbox('Situação da referência',['Todas',*cestas.SITUACOES],key='cesta_filtro_situacao')
        filtro_conferencia='Todas'
        if any(r.get('conferencia') for r in refs):
            estados=['Todas','Não conferida',*cc.ESTADOS]
            filtro_conferencia=st.selectbox('Resultado da conferência para revisar',estados,key=f'cesta_filtro_conferencia_{pid}')
        a,b=st.columns([3,1])
        termo=a.text_input('Localizar na descrição para avaliar',key=f'lote_termo_{pid}')
        tipo=b.selectbox('Tipo de preço no lote',['Todos','Estimado','Homologado'],key=f'lote_tipo_{pid}')
        st.caption('Estes filtros limitam a revisão e o lote abaixo. Resumo, relatório Word e Excel abrangem a cesta completa.')
    ativos=[]
    if filtro!='Todas':ativos.append(filtro)
    if filtro_item is not None:ativos.append('Sem item estruturado' if filtro_item==0 else rotulo_item(itens[filtro_item]))
    if filtro_conferencia!='Todas':ativos.append(filtro_conferencia)
    if termo.strip():ativos.append('Descrição: '+termo.strip())
    if tipo!='Todos':ativos.append(tipo)
    if ativos:st.caption('Filtros ativos: '+' · '.join(ativos))
    visiveis=[r for r in refs if (filtro=='Todas' or r['situacao']==filtro)
        and (filtro_item is None or (r.get('item_pesquisa_id') or 0)==filtro_item)
        and (filtro_conferencia=='Todas' or cc.estado(r)==filtro_conferencia)]
    selecionadas=mostrar_avaliacao_lote(db,p,visiveis,termo,tipo,mostrar_item=bool(itens) and filtro_item is None)
    if not visiveis:return
    with st.expander('Conferir e avaliar uma referência',expanded=len(selecionadas)==1):
        por_ref={r['id']:r for r in visiveis}
        chave_ref=f'cesta_referencia_{pid}_{filtro}_{filtro_item}'+('' if filtro_conferencia=='Todas' else '_'+filtro_conferencia)
        foco=tuple(selecionadas)
        if st.session_state.get(chave_ref+'_foco')!=foco:
            if len(foco)==1:st.session_state[chave_ref]=foco[0]
            st.session_state[chave_ref+'_foco']=foco
        rid=st.selectbox('Referência para conferir',list(por_ref),
            format_func=lambda i:f"#{i} · "+rotulo(por_ref[i]['dados']),key=chave_ref)
        ref=por_ref[rid];d=ref['dados']
        pc.mostrar_referencia(db,p,ref)
        st.caption('Salva em '+ui.data_hora(ref['selecionado_em'])+' · Controle PNCP: '+d['numero_controle_pncp'])
        st.write('**'+d['tipo_preco']+'** · '+(d.get('descricao') or 'Descrição ausente'))
        if d.get('cancelado'):st.warning('A referência já tinha indicação de cancelamento quando foi salva.')
        if d.get('valor_unitario') is None:st.warning('Preço unitário ausente na fonte. O campo ficará vazio no Excel.')
        if not d.get('unidade_medida'):st.warning('Unidade de medida ausente. Confira a documentação de origem.')
        for label,k in [('Abrir no PNCP','url_pncp'),('Abrir sistema de origem','link_sistema_origem')]:
            if str(d.get(k) or '').startswith(('https://','http://')):st.link_button(label,d[k])
        with st.expander('Dados salvos e origem'):
            from exportar_cesta import COLUNAS
            def exibir(valor,tipo):
                if valor is None:return 'Não informado'
                if tipo=='bool':return 'Sim' if valor else 'Não'
                return str(valor)
            st.dataframe([{'Campo':label,'Valor':exibir(d.get(campo),tipo)} for label,campo,tipo in COLUNAS
                          if campo in d],hide_index=True,use_container_width=True)
        alvo_item=None
        if itens:
            chave_item=f"referencia_item_{rid}_{ref['revisao']}"
            atual=st.session_state.get(chave_item,ref.get('item_pesquisa_id') or 0)
            st.session_state[chave_item]=atual if atual in [0,*itens] else 0
            alvo_item=st.selectbox('Item da pesquisa para esta referência',[0,*itens],
                format_func=lambda i:'Sem item estruturado / grupo livre' if i==0 else rotulo_item(itens[i]),key=chave_item)
            if alvo_item:
                item=itens[alvo_item]
                if item['descricao']:st.caption('Critérios do item: '+item['descricao'])
                if item['unidade']:
                    st.caption('Unidade pretendida: '+item['unidade'])
                    from filtros_precos import unidade as unidade_normalizada
                    if unidade_normalizada(item['unidade'])!=unidade_normalizada(d.get('unidade_medida')):
                        st.info('A unidade salva nesta referência difere da pretendida. Confira a comparabilidade antes de incluir; não há conversão automática.')
        with st.form(f"avaliar_{rid}_{ref['revisao']}"):
            situacao=st.selectbox('Decisão',cestas.SITUACOES,index=cestas.SITUACOES.index(ref['situacao']))
            grupo=st.text_input('Grupo / item da sua pesquisa',ref['grupo'],max_chars=160,
                disabled=bool(alvo_item),
                help='Ex.: Licença anual por usuário. Use para organizar referências comparáveis.')
            justificativa=st.text_area('Justificativa da decisão',ref['justificativa'],max_chars=4000,
                help='Obrigatória para excluir uma referência.')
            observacoes=st.text_area('Observações de comparabilidade',ref['observacoes'],max_chars=8000)
            if st.form_submit_button('Salvar avaliação',disabled=bool(p['arquivada']),key='cesta_salvar_avaliacao'):
                try:
                    cestas.avaliar(db,pid,rid,situacao=situacao,grupo=grupo,justificativa=justificativa,
                        observacoes=observacoes,revisao=ref['revisao'],item_pesquisa_id=alvo_item,
                        item_revisao=revisao_para(p,alvo_item))
                    st.session_state['cesta_flash']='Avaliação salva.';st.rerun()
                except (ValueError,sqlite3.Error,OSError) as exc:_erro(exc)
        with st.expander('Histórico de avaliações'):
            for h in cestas.historico(db,rid):
                registro=json.loads(h['avaliacao_json'])
                st.caption(ui.data_hora(h['alterado_em']))
                st.write(registro['situacao']+(' · '+registro['grupo'] if registro['grupo'] else ''))
                if registro['justificativa']:st.write('Justificativa: '+registro['justificativa'])
                if registro['observacoes']:st.write('Observações: '+registro['observacoes'])
