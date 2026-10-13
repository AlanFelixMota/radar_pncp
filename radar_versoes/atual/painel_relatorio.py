"""Formulário de texto e preparo explícito de minuta editável, sem consultas externas."""
import sqlite3
import streamlit as st
from radar_theme import data_hora
import relatorios_pesquisa as rel


def mostrar(db,pesquisa,refs):
    pid=pesquisa['id'];base=str(db.resolve());chave=f'relatorio_contexto_{base}_{pid}'
    with st.expander('Relatório da pesquisa — Word'):
        st.caption('Gera uma minuta editável com o objeto, itens, critérios, decisões, estatísticas, fontes e alertas da cesta completa. Preencha e salve os textos abaixo antes de preparar. O relatório não escolhe o preço final; confira o documento no Word antes de utilizá-lo.')
        atual=pesquisa.get('relatorio') or rel.vazio()
        contexto=st.session_state.get(chave)
        if contexto is None:
            contexto={**atual,'revisao_pesquisa':pesquisa['revisao']}
            st.session_state[chave]=contexto
        if (contexto['revisao'],contexto['revisao_pesquisa'])!=(atual['revisao'],pesquisa['revisao']):
            st.info('A cesta ou os dados salvos mudaram desde que este formulário foi aberto. Seus textos ainda aparecem abaixo. Copie eventuais alterações não salvas e use Recarregar dados do relatório antes de salvar novamente.')
        if st.button('Recarregar dados do relatório',key=f'relatorio_recarregar_{pid}'):
            st.session_state.pop(chave,None)
            epoch=f'relatorio_epoch_{base}_{pid}';st.session_state[epoch]=st.session_state.get(epoch,0)+1
            st.rerun()
        epoch=st.session_state.get(f'relatorio_epoch_{base}_{pid}',0)
        prefix=f'relatorio_{pid}_{contexto["revisao"]}_{epoch}'
        with st.form(prefix+'_form'):
            valores={}
            for k in ('orgao','responsavel','processo'):
                label,limite=rel.CAMPOS[k]
                valores[k]=st.text_input(label,contexto['dados'].get(k,''),max_chars=limite,key=prefix+'_'+k)
            st.caption('Os textos a seguir são seus registros de pesquisa. Campos vazios serão sinalizados na minuta, sem conclusão automática.')
            for k in ('contexto','metodo','analise','encaminhamento'):
                label,limite=rel.CAMPOS[k]
                valores[k]=st.text_area(label,contexto['dados'].get(k,''),max_chars=limite,key=prefix+'_'+k,height=110)
            if st.form_submit_button('Salvar dados do relatório',disabled=bool(pesquisa['arquivada']),key=f'relatorio_salvar_{pid}'):
                try:
                    rel.salvar(db,pid,valores,revisao=contexto['revisao'],revisao_pesquisa=contexto['revisao_pesquisa'])
                    st.session_state.pop(chave,None)
                    st.session_state['cesta_flash']='Dados do relatório salvos. Prepare novamente para incluir os textos no Word.'
                    st.rerun()
                except (ValueError,sqlite3.Error,OSError) as exc:st.error('Não foi possível salvar os dados do relatório: '+str(exc))
        from relatorio_cesta import pendencias,gerar_docx
        avisos=pendencias(pesquisa,refs)
        if avisos:
            st.caption('Pontos que também aparecerão na minuta:')
            for aviso in avisos:st.write('• '+aviso)
        st.caption('A geração usa os textos e avaliações já salvos. Campos em edição não entram no documento antes de Salvar dados do relatório. Preços originais e locais da conferência são apresentados separadamente; o resumo usa a cópia salva.')
        if st.button('Preparar relatório Word',key=f'relatorio_preparar_{pid}'):
            try:
                with st.spinner('Preparando minuta da pesquisa…'):
                    blob=gerar_docx(pesquisa,refs)
                st.session_state['cesta_relatorio_word']={'db':base,'id':pid,'revisao':pesquisa['revisao'],
                    'bytes':blob,'nome':f'RADAR_PNCP_relatorio_pesquisa_{pid}.docx'}
            except (ValueError,OSError) as exc:st.error('Não foi possível gerar o relatório: '+str(exc))
        arquivo=st.session_state.get('cesta_relatorio_word')
        if arquivo and (arquivo['db'],arquivo['id'],arquivo['revisao'])==(base,pid,pesquisa['revisao']):
            st.download_button('Baixar relatório Word',arquivo['bytes'],file_name=arquivo['nome'],
                mime='application/vnd.openxmlformats-officedocument.wordprocessingml.document',key=f'relatorio_baixar_{pid}')
        st.caption('O Excel continua sendo a planilha para auditoria de cada preço. Os IDs das referências são os mesmos nos dois arquivos. Mudanças feitas no Word ou Excel baixados não são importadas para o Radar PNCP.')
        if atual['revisao'] and st.checkbox('Ver histórico dos textos do relatório',key=f'relatorio_historico_{pid}'):
            try:hist=rel.historico(db,pid)
            except (ValueError,OSError,sqlite3.Error) as exc:
                st.error('Não foi possível abrir o histórico: '+str(exc));return
            if hist:
                por_id={h['id']:h for h in hist};key=f'relatorio_historico_escolha_{pid}'
                selecionada=st.session_state.get(key);st.session_state[key]=selecionada if selecionada in por_id else next(iter(por_id))
                hid=st.selectbox('Registro do relatório para consultar',list(por_id),
                    format_func=lambda i:f"Revisão {por_id[i]['revisao']} · {data_hora(por_id[i]['alterado_em'])}",key=key)
                for k,(label,_) in rel.CAMPOS.items():
                    st.write('**'+label+'**');st.text(por_id[hid]['dados'].get(k) or 'Não preenchido')
                st.caption('Exibindo até as 20 últimas revisões dos textos salvos.')
