"""MONITORAMENTO: ativação, revisão paginada e seleção de novidades para a cesta."""
from pathlib import Path
import hashlib,json,sqlite3
from datetime import datetime
import streamlit as st
import pandas as pd
import monitoramento,cestas,buscas_salvas as bs,filtros_precos as fp
import radar_paths as rp
import radar_theme as ui
from painel_cestas import destino_na_selecao,_marcar_tabela,_selecionadas_do_editor,_valor_tabela
from painel_buscas_salvas import ROTULOS,MODOS,STATUS,TIC


@st.cache_resource(show_spinner=False)
def servico(compras,db,fts,tic,jobs):
    from monitoramento_servico import Servico
    return Servico(compras,db,fts,tic,jobs)


def iniciar_servico():
    if rp.TEST_MODE:return
    servico(str(rp.DB_COMPRAS),str(rp.DB_PESQUISAS),str(rp.DB_FTS),str(rp.DB_TIC),str(rp.DB_JOBS)).acordar()


def _erro(exc):st.error('Não foi possível concluir o acompanhamento: '+str(exc))


def _flash(msg):
    st.session_state['monitoramento_flash']=msg;iniciar_servico();st.rerun()


def _estado(m):
    if m['arquivada']:return 'Busca arquivada'
    if m['token']:return 'Verificando'
    if not m['ativo']:return 'Pausado'
    return 'Precisa de atenção' if m['erro'] else 'Ativo'


def data_hora(v):
    if not v:return 'Ainda não disponível'
    try:return datetime.fromisoformat(str(v).replace('Z','+00:00')).strftime('%d/%m/%Y às %H:%M:%S')
    except ValueError:return str(v)


def mostrar_filtros(pedido):
    f=pedido['filtros'];st.caption('Busca verificada: '+pedido['nome']+' · '+ROTULOS[pedido['tipo']])
    inicio=f['data_inicio'] or 'sem limite inicial';fim=f['data_fim'] or 'sem limite final'
    campo='Publicação da contratação' if pedido['tipo']=='contratacoes' else 'Data do resultado'
    st.caption(campo+': '+inicio+' a '+fim)
    labels={'texto':'Termos','fornecedor':'Fornecedor','item':'Descrição do item','orgao':'Órgão',
            'modo_texto':'Modo de texto','status_itens':'Coleta dos itens','somente_homologadas':'Somente homologadas',
            'modalidades':'Modalidades','ufs':'UFs','tic_status':'Filtro TIC','tic_categorias':'Categorias TIC',
            'unidade':'Unidade exata','preco_min':'Preço mínimo','preco_max':'Preço máximo',
            'preco_tipo':'Tipo de preço dos filtros','incluir_cancelados':'Incluir cancelados'}
    linhas=[]
    for k,label in labels.items():
        v=f.get(k)
        if v is None or v=='' or v==[]:continue
        if k in ('status_itens','tic_status') and v=='todos':continue
        if k=='somente_homologadas' and not v:continue
        if k=='preco_tipo' and not (f.get('unidade') or f.get('preco_min') is not None or f.get('preco_max') is not None):continue
        mapa={'modo_texto':MODOS,'status_itens':STATUS,'tic_status':TIC}.get(k)
        if mapa:v=mapa.get(v,v)
        if isinstance(v,(list,tuple)):v=', '.join(v)
        elif type(v) is bool:v='Sim' if v else 'Não'
        linhas.append({'Filtro':label,'Valor':str(v)})
    if linhas:st.dataframe(linhas,hide_index=True,use_container_width=True)


def _nome(r):
    p=r['registro'];d=p['dados']
    if p['tipo']=='contratacoes':return d['numero_controle_pncp']+' · '+str(d.get('objeto') or 'Sem objeto')[:180]
    from painel_cestas import rotulo
    return d['numero_controle_pncp']+' · '+rotulo(d)


def _acao_selecao(mid,rows,editor_key,marcar,acao,destino=None,tipo='Estimado'):
    """Usa IDs/revisões da cópia exibida, nunca colunas editadas no navegador."""
    selecionadas=_selecionadas_do_editor(rows,st.session_state.get(editor_key,{}),marcar)
    try:
        if not selecionadas:raise ValueError('Marque pelo menos uma novidade na tabela.')
        lote={r['id']:r['revisao'] for r in selecionadas}
        if acao=='adicionar':
            if not destino:raise ValueError('Escolha ou crie uma cesta de destino.')
            r=monitoramento.adicionar_cesta(rp.DB_COMPRAS,rp.DB_PESQUISAS,mid,lote,destino,tipo_preco=tipo)
            mensagem=(f"{r['adicionadas']} referência(s) adicionada(s) como A avaliar; "
                f"{r['ja_existiam']} já estava(m) na cesta; "
                f"{r['sem_referencias']} contratação(ões) sem referências disponíveis para estes filtros. "
                'A seleção foi mantida. As novidades continuam com a situação de revisão anterior.')
        else:
            estado='Revisado' if acao=='revisar' else 'Pendente'
            r=monitoramento.revisar(rp.DB_PESQUISAS,mid,lote,estado=estado)
            mensagem=f"{r['alteradas']} novidade(s) marcada(s) como {estado}."
        st.session_state['monitoramento_acao_msg']={'mid':mid,'tipo':'success','texto':mensagem}
    except (ValueError,OSError,sqlite3.Error) as exc:
        st.session_state['monitoramento_acao_msg']={'mid':mid,'tipo':'error','texto':'Não foi possível concluir: '+str(exc)}


def mostrar_revisao(m):
    mid=m['id']
    mensagem=st.session_state.get('monitoramento_acao_msg')
    if mensagem and mensagem['mid']==mid:getattr(st,mensagem['tipo'])(mensagem['texto'])
    geracoes=list(range(m['geracao'],0,-1));key=f'monitoramento_geracao_{mid}'
    atual=st.session_state.get(key);st.session_state[key]=atual if atual in geracoes else m['geracao']
    g=st.selectbox('Referência inicial para revisar',geracoes,
        format_func=lambda i:f'Referência inicial {i}'+(' · atual' if i==m['geracao'] else ' · anterior'),key=key)
    estado=st.selectbox('Situação da revisão',['Pendente','Revisado','Todas'],key=f'monitoramento_estado_{mid}')
    context=f'{mid}_{g}_{estado}'
    pagina=int(st.session_state.get('monitoramento_pagina_'+context,1))
    result=monitoramento.novidades(rp.DB_PESQUISAS,mid,geracao=g,estado=estado,pagina=pagina)
    st.session_state['monitoramento_pagina_'+context]=result['pagina']
    st.caption(f"{result['total']} novidade(s) nesta lista. Página {result['pagina']} de {result['paginas']}.")
    a,b=st.columns(2)
    if a.button('Página anterior',disabled=result['pagina']<=1,key='monitoramento_anterior_'+context):
        st.session_state['monitoramento_pagina_'+context]=result['pagina']-1;st.rerun()
    if b.button('Próxima página',disabled=result['pagina']>=result['paginas'],key='monitoramento_proxima_'+context):
        st.session_state['monitoramento_pagina_'+context]=result['pagina']+1;st.rerun()
    rows=result['registros']
    base=hashlib.sha256(str(rp.DB_PESQUISAS.resolve()).encode()).hexdigest()[:12]
    chave_contexto='monitoramento_contexto_selecao_'+base
    if not rows:
        st.session_state[chave_contexto]='vazio_'+context
        st.info('Nenhuma novidade nesta lista. Isso não garante ausência de dados nas fontes externas; confira a última verificação e a cobertura da base.');return
    por_id={r['id']:r for r in rows}
    digest=hashlib.sha256(json.dumps([(r['id'],r['revisao']) for r in rows]).encode()).hexdigest()[:12]
    prefix=f'monitoramento_selecao_{base}_{context}_{result["pagina"]}_{digest}'
    if st.session_state.get(chave_contexto)!=prefix:
        _marcar_tabela(prefix,False)
        st.session_state[chave_contexto]=prefix
    epoch=st.session_state.get(prefix+'_epoch',0)
    editor_key=prefix+'_tabela_'+str(epoch)
    marcar=st.session_state.get(prefix+'_marcar',False)
    tudo,limpar=st.columns(2)
    tudo.button('Marcar todas desta página',key=prefix+'_todas',
        on_click=_marcar_tabela,args=(prefix,True))
    limpar.button('Limpar seleção',key=prefix+'_limpar',
        on_click=_marcar_tabela,args=(prefix,False))
    linhas=[]
    for r in rows:
        d=r['registro']['dados']
        linha={'Selecionar':marcar,'Revisão':r['estado'],
            'Descrição':(d.get('objeto') if m['tipo']=='contratacoes' else d.get('descricao')) or 'Descrição não disponível na detecção',
            'Órgão':d.get('orgao_razao_social') or 'Não informado'}
        if m['tipo']=='fornecedores':
            linha.update({'Item':d.get('numero_item'),
                'Fornecedor':d.get('nome_fornecedor') or d.get('ni_fornecedor') or 'Não informado',
                'Preço na detecção':_valor_tabela(d.get('valor_unitario'),True),
                'Cancelado':'Sim' if d.get('cancelado') else 'Não'})
        linhas.append(linha)
    tabela=pd.DataFrame(linhas)
    st.data_editor(ui.tabela_selecao(tabela,editor_key,marcar),key=editor_key,
        hide_index=True,num_rows='fixed',use_container_width=True,
        height=min(420,36*(len(linhas)+1)+4),
        disabled=[c for c in tabela.columns if c!='Selecionar'],
        column_config={'Selecionar':st.column_config.CheckboxColumn(width=80),
            'Revisão':st.column_config.TextColumn(width=105),
            'Descrição':st.column_config.TextColumn(width=320),
            'Órgão':st.column_config.TextColumn(width=220),
            'Preço na detecção':st.column_config.TextColumn(width=160)})
    selecionadas=_selecionadas_do_editor(rows,st.session_state.get(editor_key,{}),marcar)
    quantidade=len(selecionadas)
    st.caption(f'{quantidade} de {len(rows)} novidade(s) selecionada(s) nesta página. '
        'A seleção vale para a página e o filtro atuais; trocar a cesta mantém as marcações.')
    destino=destino_na_selecao(rp.DB_PESQUISAS,f'monitoramento_{mid}_{g}')
    tipo='Homologado' if m['tipo']=='fornecedores' else 'Estimado'
    if m['tipo']=='contratacoes':
        cfg=monitoramento.configuracao(rp.DB_PESQUISAS,mid,g)['filtros']
        from radar_db import FiltrosBusca
        f=FiltrosBusca(**cfg) if cfg is not None else None
        opcoes=[f.preco_tipo] if f is not None and fp.ativo(f) else ['Estimado','Homologado']
        key=f'monitoramento_tipo_{mid}_{g}'
        if st.session_state.get(key) not in opcoes:st.session_state[key]=opcoes[0]
        tipo=st.selectbox('Preços para levar à cesta',opcoes,key=key)
        st.caption('A inclusão lê os itens/preços disponíveis agora nas contratações selecionadas e respeita os filtros de unidade e preço da detecção. Sem esses filtros, todos os itens/preços do tipo escolhido são elegíveis. Contratações sem referências locais são informadas.')
    else:
        st.caption('A inclusão leva a cópia do resultado homologado registrada na detecção, com fornecedor e preço daquele momento. Use a conferência da cesta para comparar com a base atual.')
    st.caption('Adicionar à cesta não marca a novidade como revisada. Duplicatas preservam a primeira cópia e sua avaliação.')
    st.button(f'Adicionar à cesta ({quantidade})',disabled=not quantidade or not destino,
        type='primary',key=f'monitoramento_adicionar_{mid}',on_click=_acao_selecao,
        args=(mid,rows,editor_key,marcar,'adicionar',destino,tipo))
    a,b=st.columns(2)
    a.button(f'Marcar como revisadas ({quantidade})',disabled=not quantidade,
        key=f'monitoramento_revisar_{mid}',on_click=_acao_selecao,
        args=(mid,rows,editor_key,marcar,'revisar'))
    b.button(f'Devolver para pendentes ({quantidade})',disabled=not quantidade,
        key=f'monitoramento_pendentes_{mid}',on_click=_acao_selecao,
        args=(mid,rows,editor_key,marcar,'pendentes'))
    with st.expander('Origem e detalhes de uma novidade'):
        chosen=st.selectbox('Novidade para conferir',list(por_id),format_func=lambda i:_nome(por_id[i]),key='monitoramento_detalhe_'+context+'_'+digest)
        d=por_id[chosen]['registro']['dados']
        st.caption('Cópia registrada em '+data_hora(por_id[chosen]['detectado_em'])+'. Uma detecção posterior não significa publicação recente nem preço ainda vigente.')
        for label,k in [('Abrir no PNCP','url_pncp'),('Abrir sistema de origem','link_sistema_origem')]:
            if str(d.get(k) or '').startswith(('http://','https://')):st.link_button(label,d[k])
        labels={'numero_controle_pncp':'Controle PNCP','numero_item':'Item','sequencial_resultado':'Resultado',
            'objeto':'Objeto','descricao':'Descrição','data_publicacao_pncp':'Publicação',
            'data_preco':'Data do resultado','valor_unitario':'Preço unitário','unidade_medida':'Unidade',
            'nome_fornecedor':'Fornecedor','ni_fornecedor':'Identificação do fornecedor',
            'fonte_ultima':'Fonte do preço','importado_em':'Importado em','data_atualizacao_fonte':'Atualizado na fonte',
            'cancelado':'Cancelado na detecção','situacao_resultado_nome':'Situação na fonte'}
        st.dataframe([{'Campo':label,'Valor':str(d[k])} for k,label in labels.items() if d.get(k) is not None],hide_index=True,use_container_width=True)


@st.fragment(run_every=5)
def mostrar():
    st.title('Acompanhamentos')
    st.caption('Acompanhe pesquisas salvas e revise as novas identidades encontradas na base local.')
    with st.expander('Como funciona o acompanhamento'):
        st.caption('Verificações automáticas com intervalo de 15 minutos, em sequência, enquanto o servidor do Radar PNCP estiver aberto no PC, mesmo com esta página fechada. Buscas demoradas podem atrasar a próxima verificação. Fechar o servidor ou desligar o PC interrompe o acompanhamento; as verificações retornam quando você reabre o Radar PNCP. A MONITORAMENTO usa os dados coletados pela COLETA e pelo mutirão.')
    if st.session_state.get('monitoramento_flash'):st.success(st.session_state.pop('monitoramento_flash'))
    try:
        monitores=monitoramento.listar(rp.DB_PESQUISAS);buscas=bs.listar(rp.DB_PESQUISAS)
        if not buscas and not monitores:
            st.info('Faça uma pesquisa em Consultar e use Salvar esta busca. Depois ative o acompanhamento aqui.');return
        cadastradas={m['busca_id'] for m in monitores};disponiveis={b['id']:b for b in buscas if b['id'] not in cadastradas}
        with st.expander('Novo acompanhamento',expanded=not monitores):
            st.caption('Na ativação, os resultados que já aparecem na pesquisa formam a referência inicial e não são alertas novos. Nas próximas verificações, uma contratação é identificada pelo controle PNCP e um resultado homologado por controle, item e sequencial. Mudanças no mesmo registro não são novas identidades; use a conferência da cesta para comparar preços.')
            if disponiveis:
                key='monitoramento_nova_busca';atual=st.session_state.get(key)
                st.session_state[key]=atual if atual in disponiveis else next(iter(disponiveis))
                bid=st.selectbox('Pesquisa salva para acompanhar',list(disponiveis),
                    format_func=lambda i:disponiveis[i]['nome']+' · '+ROTULOS[disponiveis[i]['tipo']],key=key)
                mostrar_filtros(bs.resolver(disponiveis[bid]))
                if st.button('Ativar acompanhamento',key='monitoramento_ativar'):
                    with st.spinner('Registrando referência inicial da pesquisa…'):
                        r=monitoramento.iniciar(rp.DB_COMPRAS,rp.DB_PESQUISAS,bid,revisao_busca=disponiveis[bid]['revisao'],db_fts=rp.DB_FTS,db_tic=rp.DB_TIC)
                    st.session_state['monitoramento_foco']=r['id'];_flash(f"Acompanhamento ativado. {r['total_inicial']} identidade(s) já existente(s) registrada(s); novas aparições serão destacadas nas próximas verificações.")
            else:st.caption('Todas as pesquisas salvas ativas já têm acompanhamento. Você pode salvar outra busca em Consultar.')
        if not monitores:return
        st.dataframe([{'Pesquisa':m['nome'],'Tipo':ROTULOS[m['tipo']],'Estado':_estado(m),
            'Novidades pendentes atuais':m['pendentes'],'Pendentes de todas as referências iniciais':m['pendentes_total'],
            'Verificação concluída em':data_hora(m['verificado_em'])} for m in monitores],hide_index=True,use_container_width=True)
        por_id={m['id']:m for m in monitores};key='monitoramento_acompanhamento'
        foco=st.session_state.pop('monitoramento_foco',st.session_state.get(key));st.session_state[key]=foco if foco in por_id else next(iter(por_id))
        mid=st.selectbox('Acompanhamento',list(por_id),format_func=lambda i:por_id[i]['nome']+' · '+ROTULOS[por_id[i]['tipo']],key=key);m=por_id[mid]
        st.caption('Estado: '+_estado(m)+' · Última verificação concluída: '+data_hora(m['verificado_em']))
        if m['erro']:st.warning('Última tentativa não concluída: '+m['erro']+' Os resultados anteriores foram mantidos.')
        b=bs.obter(rp.DB_PESQUISAS,m['busca_id']);mudou=False
        if b['arquivada']:st.info('Busca arquivada. O acompanhamento não verifica enquanto ela estiver arquivada; a revisão das novidades continua disponível.')
        else:
            mudou=monitoramento.assinatura(b)!=m['assinatura']
            if mudou:st.warning('Os filtros da busca foram alterados. As verificações estão bloqueadas até criar uma nova referência inicial. Novidades anteriores continuam disponíveis para revisão.')
        a,c=st.columns(2)
        if a.button('Verificar agora',disabled=not m['ativo'] or bool(b['arquivada']) or mudou or bool(m['token']),key=f'monitoramento_verificar_{mid}'):
            with st.spinner('Consultando a pesquisa salva na base local…'):
                r=monitoramento.verificar(rp.DB_COMPRAS,rp.DB_PESQUISAS,mid,db_fts=rp.DB_FTS,db_tic=rp.DB_TIC)
            _flash(f"Verificação concluída: {r['novas']} nova(s) identidade(s) para este acompanhamento. Zero novidades não garante cobertura completa nas fontes externas.")
        if c.button('Pausar acompanhamento' if m['ativo'] else 'Retomar acompanhamento',disabled=bool(b['arquivada']) or (mudou and not m['ativo']),key=f'monitoramento_pausar_{mid}'):
            monitoramento.ativar(rp.DB_PESQUISAS,mid,ativo=not m['ativo'],revisao=m['revisao']);_flash('Configuração do acompanhamento salva.')
        with st.expander('Referência inicial e histórico das verificações'):
            historico=monitoramento.ciclos(rp.DB_PESQUISAS,mid)
            st.caption('Uma nova referência inicial começa outra série com os filtros atuais. Todos os resultados nela presentes deixam de ser alertas novos dessa série. As pendências e verificações da série anterior permanecem disponíveis; não são marcadas como revisadas.')
            if st.button('Criar nova referência inicial',disabled=bool(b['arquivada']) or bool(m['token']),key=f'monitoramento_reiniciar_{mid}'):
                with st.spinner('Registrando nova referência inicial…'):
                    r=monitoramento.iniciar(rp.DB_COMPRAS,rp.DB_PESQUISAS,m['busca_id'],revisao_busca=b['revisao'],revisao_monitor=m['revisao'],db_fts=rp.DB_FTS,db_tic=rp.DB_TIC)
                st.session_state.pop(f'monitoramento_geracao_{mid}',None);_flash(f"Referência inicial {r['geracao']} registrada. {r['total_inicial']} identidade(s) já conhecida(s) nesta nova série.")
            st.dataframe([{'Data':data_hora(h['consultado_em']),'Referência inicial':h['geracao'],'Tipo':h['tipo'],
                'Identidades encontradas':h['total'],'Novas':h['novas']} for h in historico],hide_index=True,use_container_width=True)
            st.caption('Exibindo até as 20 últimas verificações.')
            if historico:
                por_ciclo={h['id']:h for h in historico};key=f'monitoramento_ciclo_{mid}'
                atual=st.session_state.get(key);st.session_state[key]=atual if atual in por_ciclo else next(iter(por_ciclo))
                ciclo=st.selectbox('Verificação para conferir',list(por_ciclo),
                    format_func=lambda i:data_hora(por_ciclo[i]['consultado_em'])+' · '+por_ciclo[i]['tipo']+f' · #{i}',key=key)
                h=por_ciclo[ciclo];mostrar_filtros(h['pedido']);cov=h['cobertura']
                st.warning(cov['aviso'])
                raf=cov.get('coleta')
                if raf:st.caption('Última janela COLETA registrada nessa verificação: '+str(raf.get('inicio'))+' a '+str(raf.get('fim'))+' · '+str(raf.get('estado'))+' · Concluída em '+str(raf.get('concluido_em') or 'não concluída'))
                else:st.caption('Sem evidência de uma janela COLETA recente neste registro.')
                st.caption('O período da COLETA é de atualização da fonte; o período da pesquisa é de publicação ou resultado. Essa janela isolada não certifica a cobertura da busca inteira. Itens e resultados de fornecedores podem ter pendências próprias. Consulte Situação da base.')
        mostrar_revisao(m)
    except (ValueError,OSError,sqlite3.Error) as exc:_erro(exc)
