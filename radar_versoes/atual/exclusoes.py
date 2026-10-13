"""Exclusão explícita e atômica dos dados de trabalho; não abre as bases coletadas."""
import hashlib
import json
import cestas
import identificadores

DONOS={'cesta':'pesquisas','busca':'buscas_salvas'}
# Ordem de remoção: dependentes antes dos registros referenciados.
CESTA=(
 ('conferencias_referencia','referencia_id IN (SELECT id FROM referencias WHERE pesquisa_id=?)'),
 ('conferencias_cesta','pesquisa_id=?'),
 ('historico_referencias','referencia_id IN (SELECT id FROM referencias WHERE pesquisa_id=?)'),
 ('referencias','pesquisa_id=?'),
 ('historico_itens_pesquisa','item_id IN (SELECT id FROM itens_pesquisa WHERE pesquisa_id=?)'),
 ('itens_pesquisa','pesquisa_id=?'),
 ('historico_relatorios_pesquisa','pesquisa_id=?'),
 ('relatorios_pesquisa','pesquisa_id=?'))
MONITORAMENTO=(
 ('monitoramento_novidades','acompanhamento_id IN (SELECT id FROM monitoramento_acompanhamentos WHERE busca_id=?)'),
 ('monitoramento_conhecidos','acompanhamento_id IN (SELECT id FROM monitoramento_acompanhamentos WHERE busca_id=?)'),
 ('monitoramento_ciclos','acompanhamento_id IN (SELECT id FROM monitoramento_acompanhamentos WHERE busca_id=?)'),
 ('monitoramento_acompanhamentos','busca_id=?'))


def _existe(con,tabela):
    return con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(tabela,)).fetchone() is not None


def _prever(con,tipo,ident):
    if tipo not in DONOS:raise ValueError('Tipo de exclusão inválido.')
    tabela=DONOS[tipo]
    if not _existe(con,tabela):raise ValueError('Registro não encontrado. Atualize a lista.')
    dono=con.execute(f'SELECT * FROM {tabela} WHERE id=?',(ident,)).fetchone()
    if dono is None:raise ValueError('Registro já excluído ou não encontrado. Atualize a lista.')
    dados={'dono':dict(dono)};contagens={};monitor=None
    for child,where in CESTA if tipo=='cesta' else MONITORAMENTO:
        if not _existe(con,child):continue
        # O snapshot cobre conteúdo e histórico: mudanças após abrir a
        # confirmação invalidam a operação inteira, inclusive novas referências.
        rows=[dict(r) for r in con.execute(f'SELECT * FROM {child} WHERE {where}',(ident,))]
        rows.sort(key=lambda r:json.dumps(r,sort_keys=True,ensure_ascii=False))
        dados[child]=rows;contagens[child]=len(rows)
        if child=='monitoramento_acompanhamentos' and rows:monitor=rows[0]
    assinatura=hashlib.sha256(json.dumps(dados,sort_keys=True,ensure_ascii=False).encode()).hexdigest()
    return {'tipo':tipo,'id':ident,'nome':dono['nome'],'assinatura':assinatura,
        'contagens':contagens,'monitor':monitor,'ocupado':bool(monitor and monitor['token'])}


def prever(db,tipo,ident):
    with cestas.conectar(db) as con:
        con.execute('BEGIN')
        return _prever(con,tipo,ident)


def apagar(db,tipo,ident,*,assinatura,confirmacao,excluir_monitoramento=False):
    with cestas.conectar(db) as con:
        con.execute('BEGIN IMMEDIATE')
        atual=_prever(con,tipo,ident)
        if atual['ocupado']:
            raise ValueError('A MONITORAMENTO está verificando esta busca. Aguarde o término e abra a confirmação novamente.')
        if atual['assinatura']!=assinatura:
            raise ValueError('Os dados mudaram depois de abrir a confirmação. Confira novamente; nenhum dado foi excluído.')
        if not isinstance(confirmacao,str) or confirmacao.strip()!=atual['nome']:
            raise ValueError('Digite o nome exatamente como aparece na confirmação.')
        if atual['monitor'] and excluir_monitoramento is not True:
            raise ValueError('Confirme também a exclusão do acompanhamento MONITORAMENTO e de seu histórico.')
        tabela=DONOS[tipo]
        identificadores.registrar(con,tabela,ident)
        if atual['monitor']:identificadores.registrar(con,'monitoramento_acompanhamentos',atual['monitor']['id'])
        for child,where in CESTA if tipo=='cesta' else MONITORAMENTO:
            if _existe(con,child):con.execute(f'DELETE FROM {child} WHERE {where}',(ident,))
        con.execute(f'DELETE FROM {tabela} WHERE id=?',(ident,))
        return atual
