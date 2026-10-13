"""Itens da pesquisa, distintos dos números de item informados pelas fontes."""
import json
from pathlib import Path
import sqlite3
import cestas
from collections import Counter


def preparar(con):
    con.execute('''CREATE TABLE IF NOT EXISTS itens_pesquisa(
        id INTEGER PRIMARY KEY, pesquisa_id INTEGER NOT NULL REFERENCES pesquisas(id),
        numero INTEGER NOT NULL, nome TEXT NOT NULL, descricao TEXT NOT NULL DEFAULT '',
        unidade TEXT NOT NULL DEFAULT '', criado_em TEXT NOT NULL, atualizado_em TEXT NOT NULL,
        revisao INTEGER NOT NULL DEFAULT 1, UNIQUE(pesquisa_id,numero))''')
    con.execute('''CREATE TABLE IF NOT EXISTS historico_itens_pesquisa(
        id INTEGER PRIMARY KEY, item_id INTEGER NOT NULL REFERENCES itens_pesquisa(id),
        alterado_em TEXT NOT NULL, revisao INTEGER NOT NULL, dados_json TEXT NOT NULL)''')
    if 'item_pesquisa_id' not in {r[1] for r in con.execute('PRAGMA table_info(referencias)')}:
        con.execute('ALTER TABLE referencias ADD COLUMN item_pesquisa_id INTEGER REFERENCES itens_pesquisa(id)')
    con.execute('''CREATE TRIGGER IF NOT EXISTS cesta_item_mesma_pesquisa_insert BEFORE INSERT ON referencias
        WHEN NEW.item_pesquisa_id IS NOT NULL AND NOT EXISTS
        (SELECT 1 FROM itens_pesquisa i WHERE i.id=NEW.item_pesquisa_id AND i.pesquisa_id=NEW.pesquisa_id)
        BEGIN SELECT RAISE(ABORT,'O item deve pertencer à mesma pesquisa.'); END''')
    con.execute('''CREATE TRIGGER IF NOT EXISTS cesta_item_mesma_pesquisa_update BEFORE UPDATE OF item_pesquisa_id,pesquisa_id ON referencias
        WHEN NEW.item_pesquisa_id IS NOT NULL AND NOT EXISTS
        (SELECT 1 FROM itens_pesquisa i WHERE i.id=NEW.item_pesquisa_id AND i.pesquisa_id=NEW.pesquisa_id)
        BEGIN SELECT RAISE(ABORT,'O item deve pertencer à mesma pesquisa.'); END''')


def listar_con(con,pesquisa_id):
    if not con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='itens_pesquisa'").fetchone():return []
    return [dict(r) for r in con.execute('SELECT * FROM itens_pesquisa WHERE pesquisa_id=? ORDER BY numero,id',(pesquisa_id,))]


def listar(db,pesquisa_id):
    if not Path(db).exists():return []
    with cestas.conectar(db) as con:return listar_con(con,pesquisa_id)


def obter(con,pesquisa_id,item_id,revisao=None):
    if type(item_id) is not int or item_id<1:raise ValueError('Item da pesquisa inválido.')
    if revisao is not None and (type(revisao) is not int or revisao<1):raise ValueError('Revisão do item inválida.')
    row=con.execute('SELECT * FROM itens_pesquisa WHERE id=? AND pesquisa_id=?',(item_id,pesquisa_id)).fetchone()
    if row is None:raise ValueError('O item não pertence a esta pesquisa. Atualize a lista.')
    if revisao is not None and row['revisao']!=revisao:
        raise ValueError('O item foi alterado em outra aba. Recarregue antes de salvar.')
    return dict(row)


def rotulo(item):return f"Item {item['numero']} — {item['nome']}"


def salvar(db,pesquisa_id,*,numero,nome,descricao='',unidade='',item_id=None,revisao=None):
    if type(numero) is not int or not 1<=numero<=9999:raise ValueError('O número do item deve estar entre 1 e 9.999.')
    nome=cestas.texto(nome,130,'o nome do item',True)
    descricao=cestas.texto(descricao,4000,'a descrição do item')
    unidade=cestas.texto(unidade,200,'a unidade pretendida')
    cestas.preparar(db);stamp=cestas.agora()
    try:
        with cestas.conectar(db) as con:
            con.execute('BEGIN IMMEDIATE')
            if cestas.obter(db,pesquisa_id,con)['arquivada']:raise ValueError('Reabra a pesquisa antes de cadastrar ou editar itens.')
            if item_id is None:
                item_id=con.execute('''INSERT INTO itens_pesquisa(pesquisa_id,numero,nome,descricao,unidade,criado_em,atualizado_em)
                    VALUES(?,?,?,?,?,?,?)''',(pesquisa_id,numero,nome,descricao,unidade,stamp,stamp)).lastrowid
                nova_revisao=1
            else:
                atual=obter(con,pesquisa_id,item_id,revisao)
                if revisao is None:raise ValueError('Confira a revisão do item antes de editar.')
                if (numero,nome,descricao,unidade)==tuple(atual[k] for k in ('numero','nome','descricao','unidade')):return item_id
                con.execute('''UPDATE itens_pesquisa SET numero=?,nome=?,descricao=?,unidade=?,atualizado_em=?,revisao=revisao+1 WHERE id=?''',
                            (numero,nome,descricao,unidade,stamp,item_id))
                nova_revisao=revisao+1
            dados=dict(numero=numero,nome=nome,descricao=descricao,unidade=unidade)
            con.execute('INSERT INTO historico_itens_pesquisa(item_id,alterado_em,revisao,dados_json) VALUES(?,?,?,?)',
                        (item_id,stamp,nova_revisao,json.dumps(dados,ensure_ascii=False)))
            con.execute('UPDATE pesquisas SET atualizado_em=?,revisao=revisao+1 WHERE id=?',(stamp,pesquisa_id))
            return item_id
    except sqlite3.IntegrityError as exc:raise ValueError('Já existe um item com esse número nesta pesquisa. Escolha outro número.') from exc


def vinculo(con,pesquisa_id,row,escolha,grupo,revisao_item=None,revisoes_itens=None):
    """None mantém; 0 remove; inteiro positivo atribui, com guarda de revisão."""
    if escolha is not None and (type(escolha) is not int or escolha<0):raise ValueError('Vinculação de item inválida.')
    atual=dict(row).get('item_pesquisa_id')
    ident=atual if escolha is None else (escolha or None)
    if ident and revisao_item is None and revisoes_itens is not None:
        if not isinstance(revisoes_itens,dict) or ident not in revisoes_itens:
            raise ValueError('A lista de itens mudou. Recarregue os itens antes de salvar.')
        revisao_item=revisoes_itens[ident]
        if revisao_item is None:raise ValueError('Revisão do item inválida.')
    item=obter(con,pesquisa_id,ident,revisao_item) if ident else None
    if item:
        if escolha is None and grupo and grupo!=rotulo(item):
            raise ValueError('Para alterar o grupo livre de uma referência vinculada, primeiro escolha Remover vinculação ao item.')
        return ident,rotulo(item),item
    if grupo is None:
        grupo=rotulo(obter(con,pesquisa_id,atual)) if atual else row['grupo']
    return None,grupo,None


def enriquecer(referencia,item):
    referencia['item_pesquisa']=item
    referencia['item_pesquisa_id']=item['id'] if item else None
    if item:
        referencia['grupo_salvo']=referencia['grupo']
        referencia['grupo']=rotulo(item)
    for campo in ('numero','nome','descricao','unidade'):
        referencia['item_pesquisa_'+campo]=item[campo] if item else None


def painel(itens,referencias):
    """Contagens de todas as decisões; não decide comparabilidade nem preço."""
    saida=[];contagens={}
    for ref in referencias:
        ident=ref.get('item_pesquisa_id')
        if ident:contagens.setdefault(ident,Counter()).update([ref['situacao']])
    for item in itens:
        counts=contagens.get(item['id'],Counter())
        saida.append({**item,'referencias':sum(counts.values()),
            'incluidas':counts['Incluir'],'a_avaliar':counts['A avaliar'],'excluidas':counts['Excluir']})
    return saida
