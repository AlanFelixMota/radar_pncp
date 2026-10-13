"""Pesquisa de fornecedores com consultas apenas por ação explícita."""
from dataclasses import asdict
from datetime import date
from pathlib import Path
import sqlite3

import streamlit as st
from radar_theme import data_hora
import pesquisa_fornecedores as pf
import filtros_precos as fp


def numero(valor, casas=2, moeda=False):
    if valor is None:
        return '—'
    texto = f'{float(valor):,.{casas}f}'.replace(',', 'X').replace('.', ',').replace('X', '.')
    return ('R$ ' if moeda else '') + texto


def data(valor):
    if not valor:
        return '—'
    try:
        return date.fromisoformat(str(valor)[:10]).strftime('%d/%m/%Y')
    except ValueError:
        return str(valor)


def mostrar(db: Path, mostrar_detalhes, mostrar_destino=None):
    st.caption('Pesquise fornecedores nos resultados de itens importados do Compras.gov. Cada contratação aparece uma vez, com seus itens e valores homologados reunidos.')
    st.caption('Os resultados e fornecedores têm cobertura própria. Confira Situação da base se não encontrar uma referência esperada.')
    with st.form('form_fornecedores', border=False):
        fornecedor = st.text_input('Nome ou CNPJ do fornecedor', placeholder='Ex.: EVERY TI ou 08.925.028/0001-41', key='forn_termo')
        item = st.text_input('Descrição do item contém', placeholder='Opcional: licença, privacidade, firewall…', key='forn_item')
        with st.expander('Filtros de resultados', expanded=False):
            a,b=st.columns(2)
            inicio=a.date_input('Resultado a partir de', value=None, format='DD/MM/YYYY',key='forn_inicio')
            fim=b.date_input('Resultado até',value=None,format='DD/MM/YYYY',key='forn_fim')
            orgao=st.text_input('Órgão / unidade / CNPJ do órgão',key='forn_orgao')
            cancelados=st.checkbox('Incluir resultados com indicação de cancelamento',value=False,key='forn_cancelados')
            por_pagina=st.selectbox('Contratações por página',[10,20,50],index=1,key='forn_tamanho')
            unidade,preco_min,preco_max,_=fp.campos('forn')
            st.caption('Nesta busca, a faixa considera o preço unitário homologado de cada resultado. Unidade e faixa também limitam a inclusão na cesta.')
            st.caption('O período se refere à data do resultado. Filtrar descrição ou órgão exige que o item ou a contratação correspondente esteja na base local.')
        pesquisar=st.form_submit_button('Pesquisar fornecedor',type='primary',key='forn_pesquisar')

    if mostrar_destino:
        mostrar_destino()

    def consultar(filtros, pagina, tamanho):
        # O resultado visível corresponde sempre a estes filtros, inclusive
        # quando a consulta falha: nunca rotular um resultado antigo como novo.
        st.session_state.pop('forn_resultado',None)
        st.session_state.pop('forn_erro',None)
        st.session_state['forn_paginas_itens']={}
        st.session_state['forn_erros_itens']={}
        st.session_state['forn_filtros']=asdict(filtros)
        st.session_state['forn_tamanho_executado']=tamanho
        st.session_state['forn_base']=str(Path(db).resolve())
        try:
            with st.spinner('Consultando resultados do fornecedor…'):
                st.session_state['forn_resultado']=pf.buscar_agrupado(db,filtros,pagina,tamanho)
        except (ValueError, pf.ConsultaDemorada) as exc:
            st.session_state['forn_erro']=str(exc)
        except (sqlite3.Error, OSError) as exc:
            st.session_state['forn_erro']=f'Não foi possível consultar os resultados locais: {exc}'

    if st.session_state.get('forn_base') not in (None,str(Path(db).resolve())):
        for chave in ('forn_resultado','forn_erro','forn_filtros'):
            st.session_state.pop(chave,None)
    if pesquisar:
        st.session_state.pop('detalhe_id',None)
        try:
            minimo=fp.ler_preco(preco_min);maximo=fp.ler_preco(preco_max)
        except ValueError as exc:
            st.session_state.pop('forn_resultado',None);st.session_state.pop('forn_filtros',None)
            st.session_state['forn_erro']=str(exc)
        else:
            consultar(pf.FiltrosFornecedor(fornecedor=fornecedor,item=item,orgao=orgao,
                      data_inicio=inicio.isoformat() if inicio else None,
                      data_fim=fim.isoformat() if fim else None,incluir_cancelados=cancelados,
                      unidade=unidade,preco_min=minimo,preco_max=maximo),1,por_pagina)
    pedido=st.session_state.pop('forn_busca_salva_pendente',None)
    if pedido:
        consultar(pf.FiltrosFornecedor(**pedido['filtros']),1,pedido['por_pagina'])
    if st.session_state.get('forn_filtros'):
        from painel_buscas_salvas import mostrar_salvar_atual
        mostrar_salvar_atual('fornecedores',st.session_state['forn_filtros'],int(st.session_state['forn_tamanho_executado']))
    if st.session_state.get('forn_erro'):
        st.warning(st.session_state['forn_erro'])
    resultado=st.session_state.get('forn_resultado')
    if resultado is not None and 'contratacoes' not in resultado:
        st.session_state.pop('forn_resultado',None)
        resultado=None
    if resultado is None:
        if not st.session_state.get('forn_erro'):
            st.info('Informe um fornecedor e clique em Pesquisar fornecedor. Use o CNPJ para uma busca mais precisa.')
        return
    if not resultado['disponivel']:
        st.info('A base local ainda não possui a camada de resultados. Consulte Sistema → Configurações → Recuperar histórico.')
        return
    filtros=pf.FiltrosFornecedor(**st.session_state['forn_filtros'])
    tamanho=st.session_state['forn_tamanho_executado']
    st.subheader('Resultados do fornecedor')
    st.write(f'Fornecedor pesquisado: {filtros.fornecedor}')
    if filtros.item: st.caption('Descrição filtrada: '+filtros.item)
    if filtros.orgao: st.caption('Órgão filtrado: '+filtros.orgao)
    if fp.ativo(filtros):st.caption('Filtros executados: '+fp.rotulo(filtros))
    st.caption(f'Período dos resultados: {data(filtros.data_inicio) if filtros.data_inicio else "sem início definido"} a {data(filtros.data_fim) if filtros.data_fim else "sem fim definido"}. ' + ('Inclui indicações de cancelamento.' if filtros.incluir_cancelados else 'Oculta indicações de cancelamento.'))
    st.caption('Consulta local realizada em '+data_hora(resultado['consultado_em'])+'. Use Atualizar consulta para receber mudanças posteriores.')
    sync=resultado.get('sync') or {}
    if sync.get('ultima_execucao_em'):
        st.caption('Última execução registrada da sincronização de resultados: '+str(sync['ultima_execucao_em']).replace('T',' às ')+'. Última data na fronteira registrada: '+data(sync.get('fronteira_depois'))+'. Esse registro não garante cobertura integral.')
    else:
        st.caption('Sem registro de sincronização disponível para informar a atualização desta camada. Confira a data e a origem de cada resultado.')
    a,b=st.columns(2)
    if a.button('Atualizar consulta',key='forn_atualizar'):
        consultar(filtros,1,tamanho);st.rerun()
    if b.button('Limpar resultados',key='forn_limpar'):
        for chave in ('forn_resultado','forn_erro','forn_filtros','detalhe_id'):
            st.session_state.pop(chave,None)
        st.rerun()
    if st.session_state.get('detalhe_id'):
        mostrar_detalhes(st.session_state['detalhe_id'])
    rows=resultado['contratacoes']
    if not rows:
        st.info('Nenhum resultado encontrado nesta página com estes filtros. Isso não comprova ausência de contratação: tente o CNPJ ou outra razão social, amplie o período e confira a cobertura dos resultados.')
    from painel_cestas import incluir_lote
    if st.button('Adicionar todos os resultados à cesta',key='cesta_fornecedor_todos',
                 disabled=not rows or not st.session_state.get('cesta_ativa_id')):
        incluir_lote('salvar_resultados',db,filtros=filtros)
    st.caption('Inclui todos os resultados homologados correspondentes aos filtros desta pesquisa, em todas as páginas. As novas referências entram como A avaliar.')
    st.caption(f"Página {resultado['pagina']} · {len(rows)} contratação(ões) · {sum(g['total_itens'] for g in rows)} item(ns) · {sum(g['total_resultados'] for g in rows)} resultado(s) correspondentes aos filtros nesta página.")
    nav1,nav2=st.columns(2)
    if nav1.button('← Anterior',disabled=resultado['pagina']<=1,key='forn_anterior'):
        consultar(filtros,resultado['pagina']-1,tamanho);st.rerun()
    if nav2.button('Próxima →',disabled=not resultado['tem_mais'],key='forn_proxima'):
        consultar(filtros,resultado['pagina']+1,tamanho);st.rerun()
    renderizar_contratacoes(db, rows, filtros, mostrar_detalhes)


def tabela_resultados(rows):
    """Uma linha por resultado: mantém sequenciais, nulos e origem explícitos."""
    st.dataframe([{
        'Item': r['numero_item'],
        'Descrição': r.get('descricao') or 'Descrição ainda não disponível na base local',
        'Quantidade homologada': numero(r.get('quantidade_homologada'),4),
        'Unidade': r.get('unidade_medida') or '—',
        'Valor unitário homologado': numero(r.get('valor_unitario_homologado'),4,True),
        'Valor total homologado': numero(r.get('valor_total_homologado'),2,True),
        'Data do resultado': data(r.get('data_resultado_pncp')),
        'Fornecedor': r.get('nome_fornecedor') or 'Não informado',
        'CNPJ / identificação': r.get('ni_fornecedor') or 'Não informado',
        'Sequencial': r['sequencial_resultado'],
        'Situação': r.get('situacao_resultado_nome') or 'Não informada',
        'Indicação de cancelamento': 'Sim' if r.get('cancelado') else 'Não',
        'Data do cancelamento': data(r.get('data_cancelamento_pncp')),
        'Motivo do cancelamento': r.get('motivo_cancelamento') or '—',
        'Origem': r.get('fonte_ultima') or 'Não informada',
        'Importado em': r.get('importado_em') or 'Não informado',
        'Atualizado na fonte': r.get('data_atualizacao_pncp') or 'Não informado',
    } for r in rows], hide_index=True, use_container_width=True)
    st.caption('Valores homologados informados pela fonte; — indica campo ausente. As colunas à direita mostram situação, cancelamento e origem. Um item pode ter mais de um resultado.')


def renderizar_contratacoes(db, grupos, filtros, mostrar_detalhes=None):
    paginas=st.session_state.setdefault('forn_paginas_itens',{})
    erros=st.session_state.setdefault('forn_erros_itens',{})
    for indice, grupo in enumerate(grupos):
        pid=grupo['numero_controle_pncp']
        r=grupo['previa']['resultados'][0]
        with st.container(border=True,key=f"radar_pncp_resultado_fornecedor_{indice}"):
            st.subheader(r.get('orgao_razao_social') or 'Contratação sem órgão disponível localmente')
            st.write(r.get('objeto') or 'Objeto ainda não disponível na base local')
            st.caption('Controle PNCP: '+pid)
            if not r['contratacao_local']:
                st.info('Os resultados estão armazenados, mas a contratação ainda não está disponível localmente.')
            st.caption(f"{grupo['total_itens']} item(ns) · {grupo['total_resultados']} resultado(s) correspondentes aos filtros · resultado mais recente: {data(grupo['ultimo_resultado'])}")
            c1,c2=st.columns(2)
            if r['contratacao_local'] and c1.button('Detalhes da contratação',key='forn_detalhe_'+pid,type='primary'):
                st.session_state['detalhe_id']=pid
                if mostrar_detalhes is not None:mostrar_detalhes(pid)
                else:st.rerun()
            url=r.get('url_pncp')
            if url and str(url).startswith(('https://','http://')):
                c2.link_button('Abrir no PNCP',url)
            from painel_cestas import incluir_lote
            if st.button('Adicionar todos os resultados desta contratação',key='cesta_forn_contratacao_'+pid,
                         disabled=not st.session_state.get('cesta_ativa_id')):
                incluir_lote('salvar_resultados',db,numero_controle=pid,filtros=filtros)
            st.caption('A inclusão desta contratação respeita os filtros do fornecedor e inclui também as outras páginas de resultados.')
            with st.expander('Itens e resultados encontrados',expanded=True):
                atual=paginas.get(pid,grupo['previa'])
                if atual['resultados']:
                    import cestas
                    from painel_cestas import selecionar
                    selecionar([cestas.referencia_resultado(x,atual['consultado_em']) for x in atual['resultados']],
                               'fornecedor_'+pid)
                    with st.expander('Mais informações dos resultados'):
                        tabela_resultados(atual['resultados'])
                else:
                    st.info('Nenhum resultado nesta página. A base pode ter mudado; atualize a consulta.')
                if pid in erros:
                    st.warning(erros[pid])
                if atual['tem_mais'] or atual['pagina']>1:
                    st.caption(f"Página de resultados {atual['pagina']} · consulta em {data_hora(atual['consultado_em'])}. Atualizar consulta renova também as contagens.")
                    a,b=st.columns(2)
                    destino=None
                    if a.button('← Itens anteriores',key='forn_itens_ant_'+pid,disabled=atual['pagina']<=1):
                        destino=atual['pagina']-1
                    if b.button('Mais itens →',key='forn_itens_prox_'+pid,disabled=not atual['tem_mais']):
                        destino=atual['pagina']+1
                    if destino is not None:
                        try:
                            with st.spinner('Consultando itens desta contratação…'):
                                paginas[pid]=pf.por_contratacao(db,pid,destino,20,filtros=filtros)
                            erros.pop(pid,None)
                        except (ValueError,sqlite3.Error,OSError,pf.ConsultaDemorada) as exc:
                            erros[pid]=f'Não foi possível mudar a página de itens; a página anterior foi mantida: {exc}'
                        st.rerun()


def renderizar_resultados(rows, mostrar_detalhes=None):
    for indice, r in enumerate(rows):
        chave=f"{r['numero_controle_pncp']}_{r['numero_item']}_{r['sequencial_resultado']}"
        with st.container(border=True,key=f"radar_pncp_resultado_previa_{indice}"):
            st.write(r.get('nome_fornecedor') or 'Fornecedor sem nome informado')
            st.caption('Identificação do fornecedor: '+str(r.get('ni_fornecedor') or 'não informada'))
            st.write(f"Item {r['numero_item']} — {r.get('descricao') or 'Descrição ainda não disponível na base local'}")
            st.caption('Unidade: '+str(r.get('unidade_medida') or 'não informada'))
            q,u,t=st.columns(3)
            q.metric('Quantidade homologada',numero(r.get('quantidade_homologada'),4))
            u.metric('Valor unitário homologado',numero(r.get('valor_unitario_homologado'),4,True))
            t.metric('Valor total homologado',numero(r.get('valor_total_homologado'),2,True))
            st.caption('Valores informados pela fonte. Campo ausente aparece como —; o total não é calculado a partir da quantidade.')
            if r.get('cancelado'):
                st.warning('Resultado com indicação de cancelamento: '+str(r.get('motivo_cancelamento') or r.get('situacao_resultado_nome') or data(r.get('data_cancelamento_pncp'))))
            st.caption(f"Resultado em {data(r.get('data_resultado_pncp'))} · situação: {r.get('situacao_resultado_nome') or 'não informada'} · sequencial {r['sequencial_resultado']}")
            if r['contratacao_local']:
                st.write(r.get('orgao_razao_social') or 'Órgão sem nome informado')
                st.write(r.get('objeto') or 'Objeto não informado')
            else:
                st.info('O resultado está armazenado, mas a contratação ainda não está disponível localmente.')
            if not r['item_local']:
                st.caption('Item ainda não disponível localmente; quantidade e valores acima pertencem ao resultado importado.')
            st.caption('Controle PNCP: '+r['numero_controle_pncp'])
            st.caption(f"Origem: {r.get('fonte_ultima') or 'não informada'} · importado em {r.get('importado_em') or 'não informado'} · atualização na fonte: {r.get('data_atualizacao_pncp') or 'não informada'}")
            c1,c2=st.columns(2)
            if mostrar_detalhes is not None and r['contratacao_local'] and c1.button('Detalhes da contratação',key='forn_detalhe_'+chave,type='primary'):
                st.session_state['detalhe_id']=r['numero_controle_pncp'];mostrar_detalhes(r['numero_controle_pncp'])
            url=r.get('url_pncp')
            if url and str(url).startswith(('https://','http://')):
                c2.link_button('Abrir no PNCP',url)


def mostrar_da_contratacao(db: Path, numero_controle: str):
    st.caption('Resultados locais de todos os fornecedores desta contratação, incluindo indicações de cancelamento. São distintos dos valores estimados dos itens.')
    chave='resultados_detalhe_'+numero_controle
    def carregar(pagina):
        st.session_state.pop(chave,None)
        st.session_state.pop(chave+'_erro',None)
        try:
            with st.spinner('Consultando resultados da contratação…'):
                st.session_state[chave]=pf.por_contratacao(db,numero_controle,pagina)
        except (sqlite3.Error,OSError,pf.ConsultaDemorada) as exc:
            st.session_state[chave+'_erro']=f'Não foi possível consultar os resultados: {exc}'
    if st.button('Carregar/atualizar resultados homologados',key='carregar_'+chave):
        carregar(1)
    if st.session_state.get(chave+'_erro'):
        st.warning(st.session_state[chave+'_erro'])
    r=st.session_state.get(chave)
    if r is None:
        return
    if not r['disponivel'] or not r['resultados']:
        st.info('Nenhum resultado disponível localmente para esta contratação nesta página. Isso não comprova ausência de homologação na fonte.')
    st.caption(f"Página {r['pagina']} · consulta local em {data_hora(r.get('consultado_em'))}")
    a,b=st.columns(2)
    if a.button('← Resultados anteriores',key='ant_'+chave,disabled=r['pagina']<=1):
        carregar(r['pagina']-1);st.rerun()
    if b.button('Próximos resultados →',key='prox_'+chave,disabled=not r['tem_mais']):
        carregar(r['pagina']+1);st.rerun()
    import cestas
    from painel_cestas import selecionar,incluir_lote
    if st.button('Adicionar todos os resultados homologados desta contratação',key='cesta_homologados_'+numero_controle,
                 disabled=not st.session_state.get('cesta_ativa_id')):
        incluir_lote('salvar_resultados',db,numero_controle=numero_controle)
    st.caption('Inclui todos os resultados locais desta contratação, em todas as páginas, preservando indicações de cancelamento para revisão.')
    selecionar([cestas.referencia_resultado(x,r['consultado_em']) for x in r['resultados']],
               'detalhe_resultados_'+numero_controle)
    with st.expander('Mais informações dos resultados'):
        tabela_resultados(r['resultados'])
