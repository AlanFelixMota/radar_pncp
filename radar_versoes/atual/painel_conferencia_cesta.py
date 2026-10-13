"""Conferência explícita da cesta e consulta de resultados/histórico já salvos."""
import sqlite3
import streamlit as st
from radar_theme import data_hora
import conferencia_cesta as cc
import radar_paths


def valor(v):
    if v is None:return 'Não informado'
    if isinstance(v,bool):return 'Sim' if v else 'Não'
    return str(v)


def mostrar(db,pesquisa,refs):
    pid=pesquisa['id']
    with st.expander('Conferir referências com a base local'):
        st.caption('Compara a cesta inteira com os registros disponíveis na base local. Não consulta o PNCP em tempo real. Preços salvos, decisões e cálculos continuam usando a cópia original; revise as divergências antes de concluir sua pesquisa.')
        st.caption('A COLETA e o mutirão alimentam a base. A data desta conferência indica quando o Radar PNCP fez a comparação, não que a fonte externa esteja atualizada até esse instante. Veja também Situação da base.')
        if st.button('Conferir cesta agora',key=f'conferir_cesta_{pid}',disabled=bool(pesquisa['arquivada'])):
            andamento=st.empty()
            try:
                with st.spinner('Comparando referências com a base local…'):
                    r=cc.conferir(radar_paths.DB_COMPRAS,db,pid,revisao=pesquisa['revisao'],
                        progresso=lambda n:andamento.caption(f'{n} referência(s) comparada(s)…'))
                st.session_state['cesta_flash']=f"Conferência registrada para {r['total']} referência(s). Consulte os resultados e as divergências na cesta."
                st.rerun()
            except (ValueError,OSError,sqlite3.Error) as exc:
                andamento.empty();st.error('Não foi possível concluir a conferência: '+str(exc))
        verificadas=[r for r in refs if r.get('conferencia')]
        if not verificadas:
            st.info('Esta cesta ainda não foi conferida. A comparação será feita somente ao clicar em Conferir cesta agora.');return
        ultima=max(r['conferencia']['consultado_em'] for r in verificadas)
        st.caption(f'Última conferência registrada: {data_hora(ultima)}. {len(refs)-len(verificadas)} referência(s) ainda não conferida(s). Novas referências precisam de uma nova conferência.')
        contagens={}
        for ref in refs:contagens[cc.estado(ref)]=contagens.get(cc.estado(ref),0)+1
        st.dataframe([{'Resultado':k,'Referências':n} for k,n in contagens.items()],hide_index=True,use_container_width=True)
        st.caption('Sem divergência local significa igualdade nos campos comparados naquele momento. Base insuficiente, registro anterior ou referência não localizada pedem conferência da origem; ausência não comprova cancelamento.')
    pendentes=[r for r in refs if r['situacao']=='Incluir' and r.get('conferencia') and cc.estado(r)!=cc.SEM]
    if pendentes:
        st.warning(f'{len(pendentes)} referência(s) marcada(s) como Incluir tem alerta na última conferência. O resumo ainda usa o preço salvo. Confira o alerta e, se necessário, altere sua avaliação ou adicione uma referência atualizada em outra cesta.')


def detalhe(conferencia):
    st.write('**'+conferencia['estado']+'**')
    st.caption('Comparação local em '+data_hora(conferencia['consultado_em']))
    atual=conferencia.get('atual') or {}
    if atual:
        st.caption('Fonte registrada no preço local: '+valor(atual.get('fonte_ultima'))+
            ' · Importado/coletado em: '+valor(atual.get('importado_em'))+
            ' · Atualizado na fonte em: '+valor(atual.get('data_atualizacao_fonte')))
    if conferencia.get('pncp_consultado_em'):
        st.caption('Última consulta ao PNCP registrada para este resultado: '+data_hora(conferencia['pncp_consultado_em']))
    for aviso in conferencia.get('avisos',[]):st.warning(aviso)
    if conferencia['divergencias']:
        st.dataframe([{'Campo':d['rotulo'],'Cópia salva na cesta':valor(d['salvo']),
            'Registro local na conferência':valor(d['atual'])} for d in conferencia['divergencias']],
            hide_index=True,use_container_width=True)
    elif conferencia['estado']==cc.SEM:
        st.caption('Não foram encontradas diferenças nos campos comparados. Esta verificação não garante a situação atual da fonte externa.')


def mostrar_referencia(db,pesquisa,ref):
    with st.expander('Atualização e histórico da referência',expanded=bool(ref.get('conferencia') and cc.estado(ref)!=cc.SEM)):
        if ref.get('conferencia'):detalhe(ref['conferencia'])
        else:st.info('Referência ainda não conferida. Use Conferir cesta agora para comparar todas as referências com a base local.')
        st.caption('A comparação não altera sua avaliação nem substitui a cópia salva. Use a avaliação abaixo para decidir se a referência continua comparável; documente sua decisão nas observações ou justificativa.')
        if ref.get('conferencia') and st.checkbox('Ver histórico de conferências',key=f'conferencia_hist_{ref["id"]}'):
            try:historico=cc.historico(db,pesquisa['id'],ref['id'])
            except (ValueError,OSError,sqlite3.Error) as exc:
                st.error('Não foi possível abrir o histórico: '+str(exc));return
            por_id={h['id']:h for h in historico}
            if por_id:
                chave=f'conferencia_hist_escolha_{ref["id"]}'
                selecionada=st.session_state.get(chave)
                st.session_state[chave]=selecionada if selecionada in por_id else next(iter(por_id))
                hid=st.selectbox('Conferência para consultar',list(por_id),
                    format_func=lambda i:data_hora(por_id[i]['consultado_em'])+' · '+por_id[i]['estado'],key=chave)
                detalhe(por_id[hid]);st.caption('Exibindo até as 50 conferências mais recentes desta referência.')
