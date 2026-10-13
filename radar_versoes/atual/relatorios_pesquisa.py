"""Dados textuais do relatório, separados das cópias e avaliações de preços."""
import json
from pathlib import Path
import cestas

CAMPOS={'orgao':('Órgão / setor',300),'responsavel':('Responsável pela pesquisa',300),
        'processo':('Processo / identificação',200),'contexto':('Contexto e finalidade',12000),
        'metodo':('Método e notas da pesquisa',12000),'analise':('Análise dos resultados',12000),
        'encaminhamento':('Conclusão / encaminhamento',12000)}


def vazio():return {'dados':{k:'' for k in CAMPOS},'revisao':0,'atualizado_em':None}


def carregar_con(con,pesquisa_id):
    if not con.execute("SELECT 1 FROM sqlite_master WHERE name='relatorios_pesquisa'").fetchone():return vazio()
    row=con.execute('SELECT * FROM relatorios_pesquisa WHERE pesquisa_id=?',(pesquisa_id,)).fetchone()
    if not row:return vazio()
    return {'dados':{**vazio()['dados'],**json.loads(row['dados_json'])},
            'revisao':row['revisao'],'atualizado_em':row['atualizado_em']}


def preparar(con):
    con.execute('''CREATE TABLE IF NOT EXISTS relatorios_pesquisa(
        pesquisa_id INTEGER PRIMARY KEY REFERENCES pesquisas(id),dados_json TEXT NOT NULL,
        atualizado_em TEXT NOT NULL,revisao INTEGER NOT NULL)''')
    con.execute('''CREATE TABLE IF NOT EXISTS historico_relatorios_pesquisa(
        id INTEGER PRIMARY KEY,pesquisa_id INTEGER NOT NULL REFERENCES pesquisas(id),
        alterado_em TEXT NOT NULL,revisao INTEGER NOT NULL,dados_json TEXT NOT NULL)''')
    con.execute('CREATE INDEX IF NOT EXISTS idx_historico_relatorio ON historico_relatorios_pesquisa(pesquisa_id,id)')


def salvar(db,pesquisa_id,dados,*,revisao,revisao_pesquisa):
    if not isinstance(dados,dict) or set(dados)!=set(CAMPOS):raise ValueError('Dados do relatório inválidos. Recarregue os campos.')
    normal={}
    for k,(label,limite) in CAMPOS.items():
        if not isinstance(dados[k],str):raise ValueError('Informe texto em '+label+'.')
        normal[k]=cestas.texto(dados[k],limite,label)
    if type(revisao) is not int or revisao<0:raise ValueError('Revisão do relatório inválida.')
    stamp=cestas.agora();raw=json.dumps(normal,ensure_ascii=False,allow_nan=False)
    with cestas.conectar(db) as con:
        con.execute('BEGIN IMMEDIATE');p=cestas.obter(db,pesquisa_id,con)
        if p['arquivada']:raise ValueError('Reabra a pesquisa antes de editar os dados do relatório.')
        atual=carregar_con(con,pesquisa_id)
        if p['revisao']!=revisao_pesquisa or atual['revisao']!=revisao:
            raise ValueError('A cesta ou o relatório mudou em outra aba. Recarregue os dados antes de salvar; seu texto não foi substituído.')
        if atual['dados']==normal:return atual
        preparar(con)
        con.execute('''INSERT INTO relatorios_pesquisa VALUES(?,?,?,?)
            ON CONFLICT(pesquisa_id) DO UPDATE SET dados_json=excluded.dados_json,
            atualizado_em=excluded.atualizado_em,revisao=excluded.revisao''',(pesquisa_id,raw,stamp,revisao+1))
        con.execute('INSERT INTO historico_relatorios_pesquisa(pesquisa_id,alterado_em,revisao,dados_json) VALUES(?,?,?,?)',
                    (pesquisa_id,stamp,revisao+1,raw))
        con.execute('UPDATE pesquisas SET atualizado_em=?,revisao=revisao+1 WHERE id=?',(stamp,pesquisa_id))
    return {'dados':normal,'revisao':revisao+1,'atualizado_em':stamp}


def historico(db,pesquisa_id,limite=20):
    if not Path(db).is_file():return []
    with cestas.conectar(db) as con:
        if not con.execute("SELECT 1 FROM sqlite_master WHERE name='historico_relatorios_pesquisa'").fetchone():return []
        return [{**dict(r),'dados':json.loads(r['dados_json'])} for r in con.execute(
            'SELECT * FROM historico_relatorios_pesquisa WHERE pesquisa_id=? ORDER BY id DESC LIMIT ?',
            (pesquisa_id,min(100,max(1,int(limite)))))]
