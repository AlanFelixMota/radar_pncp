"""Cestas persistentes com cópia imutável da referência selecionada."""
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
import json
import math
import sqlite3

SITUACOES=('A avaliar','Incluir','Excluir')


def agora():
    return datetime.now().astimezone().isoformat(timespec='microseconds')


@contextmanager
def conectar(db):
    con=sqlite3.connect(db,timeout=3)
    con.row_factory=sqlite3.Row
    con.execute('PRAGMA foreign_keys=ON')
    try:
        with con:yield con
    finally:con.close()


def preparar(db):
    Path(db).parent.mkdir(parents=True,exist_ok=True)
    with conectar(db) as c:
        c.execute('PRAGMA journal_mode=WAL')
        c.executescript('''
          CREATE TABLE IF NOT EXISTS pesquisas(
            id INTEGER PRIMARY KEY, nome TEXT NOT NULL, objetivo TEXT NOT NULL DEFAULT '',
            criado_em TEXT NOT NULL, atualizado_em TEXT NOT NULL, arquivada INTEGER NOT NULL DEFAULT 0,
            revisao INTEGER NOT NULL DEFAULT 1);
          CREATE TABLE IF NOT EXISTS referencias(
            id INTEGER PRIMARY KEY, pesquisa_id INTEGER NOT NULL REFERENCES pesquisas(id),
            chave TEXT NOT NULL, dados_json TEXT NOT NULL, selecionado_em TEXT NOT NULL,
            situacao TEXT NOT NULL DEFAULT 'A avaliar', grupo TEXT NOT NULL DEFAULT '',
            justificativa TEXT NOT NULL DEFAULT '', observacoes TEXT NOT NULL DEFAULT '',
            revisado_em TEXT, revisao INTEGER NOT NULL DEFAULT 1,
            UNIQUE(pesquisa_id,chave));
          CREATE INDEX IF NOT EXISTS idx_referencias_pesquisa ON referencias(pesquisa_id,id);
          CREATE TABLE IF NOT EXISTS historico_referencias(
            id INTEGER PRIMARY KEY, referencia_id INTEGER NOT NULL REFERENCES referencias(id),
            alterado_em TEXT NOT NULL, revisao INTEGER NOT NULL, avaliacao_json TEXT NOT NULL);
        ''')
        import itens_cesta
        c.execute('BEGIN IMMEDIATE')
        itens_cesta.preparar(c)


def texto(valor,limite,rotulo,obrigatorio=False):
    s=str(valor or '').strip()
    if obrigatorio and not s:raise ValueError(f'Informe {rotulo}.')
    if len(s)>limite:raise ValueError(f'{rotulo.capitalize()} deve ter até {limite} caracteres.')
    return s


def criar(db,nome,objetivo=''):
    nome=texto(nome,160,'o nome da pesquisa',True)
    objetivo=texto(objetivo,4000,'o objetivo')
    preparar(db)
    stamp=agora()
    with conectar(db) as c:
        c.execute('BEGIN IMMEDIATE')
        from identificadores import proximo
        ident=proximo(c,'pesquisas')
        c.execute('INSERT INTO pesquisas(id,nome,objetivo,criado_em,atualizado_em) VALUES(?,?,?,?,?)',
                         (ident,nome,objetivo,stamp,stamp))
        return ident


def listar(db,arquivadas=False):
    if not Path(db).exists():return []
    with conectar(db) as c:
        return [dict(r) for r in c.execute('''SELECT p.*,COUNT(r.id) AS total,
            SUM(CASE WHEN r.situacao='Incluir' THEN 1 ELSE 0 END) AS incluidas
            FROM pesquisas p LEFT JOIN referencias r ON r.pesquisa_id=p.id
            WHERE (? OR p.arquivada=0) GROUP BY p.id ORDER BY p.atualizado_em DESC,p.id DESC''',(int(arquivadas),))]


def obter(db,pesquisa_id,con=None):
    if con is None:
        with conectar(db) as c:return obter(db,pesquisa_id,c)
    row=con.execute('SELECT * FROM pesquisas WHERE id=?',(pesquisa_id,)).fetchone()
    if not row:raise ValueError('Pesquisa não encontrada. Atualize a página.')
    return dict(row)


def alterar_pesquisa(db,pesquisa_id,*,nome,objetivo,arquivada,revisao):
    nome=texto(nome,160,'o nome da pesquisa',True)
    objetivo=texto(objetivo,4000,'o objetivo')
    with conectar(db) as c:
        cur=c.execute('''UPDATE pesquisas SET nome=?,objetivo=?,arquivada=?,atualizado_em=?,revisao=revisao+1
                         WHERE id=? AND revisao=?''',(nome,objetivo,int(bool(arquivada)),agora(),pesquisa_id,revisao))
        if not cur.rowcount:raise ValueError('A pesquisa foi alterada em outra aba. Recarregue antes de salvar.')


def _numero(v):
    if v is None:return None
    n=float(v)
    if not math.isfinite(n):raise ValueError('A referência possui um número inválido.')
    return n


def referencia_resultado(r,consultado_em=None):
    dados={k:r.get(k) for k in ('numero_controle_pncp','numero_item','sequencial_resultado',
        'descricao','informacao_complementar','unidade_medida','orgao_razao_social','orgao_cnpj',
        'unidade_nome','uf_sigla','objeto','processo','numero_compra','ano_compra','url_pncp',
        'link_sistema_origem','ni_fornecedor','nome_fornecedor','situacao_resultado_nome',
        'data_cancelamento_pncp','motivo_cancelamento','fonte_ultima','importado_em',
        'contratacao_local','item_local','data_publicacao_pncp')}
    dados.update(tipo_preco='Homologado',quantidade=_numero(r.get('quantidade_homologada')),
        valor_unitario=_numero(r.get('valor_unitario_homologado')),
        valor_total=_numero(r.get('valor_total_homologado')),data_preco=r.get('data_resultado_pncp'),
        data_atualizacao_fonte=r.get('data_atualizacao_pncp'),cancelado=bool(r.get('cancelado')),
        consultado_em=consultado_em)
    return dados


def referencia_item(contratacao,item,consultado_em=None):
    dados={k:contratacao.get(k) for k in ('numero_controle_pncp','orgao_razao_social','orgao_cnpj',
        'unidade_nome','uf_sigla','objeto','processo','numero_compra','ano_compra','url_pncp',
        'link_sistema_origem','data_publicacao_pncp')}
    dados.update({k:item.get(k) for k in ('numero_item','descricao','informacao_complementar','unidade_medida')})
    dados.update(tipo_preco='Estimado',sequencial_resultado=None,quantidade=_numero(item.get('quantidade')),
        valor_unitario=_numero(item.get('valor_unitario_estimado')),valor_total=_numero(item.get('valor_total')),
        data_preco=None,data_atualizacao_fonte=item.get('data_atualizacao'),
        fonte_ultima=item.get('fonte_ultima'),importado_em=item.get('ultima_coleta_em'),
        situacao_resultado_nome=item.get('situacao_item_nome'),cancelado=None,
        contratacao_local=True,item_local=True,consultado_em=consultado_em)
    return dados


def chave(dados):
    tipo=dados.get('tipo_preco')
    if tipo not in ('Homologado','Estimado'):raise ValueError('Tipo de preço inválido.')
    pid=texto(dados.get('numero_controle_pncp'),150,'o controle PNCP',True)
    item=int(dados['numero_item'])
    seq=int(dados['sequencial_resultado']) if tipo=='Homologado' else None
    return json.dumps([tipo,pid,item,seq],ensure_ascii=False,separators=(',',':'))


def adicionar(db,pesquisa_id,referencias,*,item_pesquisa_id=None,item_revisao=None):
    """Insere um iterável em uma transação; duplicatas preservam a primeira cópia.

    Uma falha de leitura/validação no meio de um lote desfaz todas as inserções.
    """
    stamp=agora();novas=0;total=0
    with conectar(db) as c:
        c.execute('BEGIN IMMEDIATE')
        if obter(db,pesquisa_id,c)['arquivada']:raise ValueError('Reabra a pesquisa antes de adicionar referências.')
        import itens_cesta
        item=itens_cesta.obter(c,pesquisa_id,item_pesquisa_id,item_revisao) if item_pesquisa_id is not None else None
        grupo=itens_cesta.rotulo(item) if item else ''
        for r in referencias:
            k=chave(r);raw=json.dumps(r,ensure_ascii=False,allow_nan=False)
            total+=1
            cur=c.execute('''INSERT INTO referencias(pesquisa_id,chave,dados_json,selecionado_em,item_pesquisa_id,grupo)
                VALUES(?,?,?,?,?,?) ON CONFLICT(pesquisa_id,chave) DO NOTHING''',
                (pesquisa_id,k,raw,stamp,item_pesquisa_id,grupo))
            novas+=cur.rowcount
            if cur.rowcount and item:
                c.execute('INSERT INTO historico_referencias(referencia_id,alterado_em,revisao,avaliacao_json) VALUES(?,?,?,?)',
                    (cur.lastrowid,stamp,1,json.dumps(dict(situacao='A avaliar',grupo=grupo,
                        justificativa='',observacoes='',item_pesquisa_id=item_pesquisa_id,item_pesquisa=item),ensure_ascii=False)))
        if not total:raise ValueError('Nenhuma referência disponível para adicionar à cesta.')
        if novas:c.execute('UPDATE pesquisas SET atualizado_em=?,revisao=revisao+1 WHERE id=?',(stamp,pesquisa_id))
    return {'adicionadas':novas,'ja_existiam':total-novas,'item_destino':grupo if item else None}


def carregar(db,pesquisa_id):
    with conectar(db) as c:
        c.execute('BEGIN')
        pesquisa=obter(db,pesquisa_id,c)
        from relatorios_pesquisa import carregar_con
        pesquisa['relatorio']=carregar_con(c,pesquisa_id)
        rows=[dict(r) for r in c.execute('SELECT * FROM referencias WHERE pesquisa_id=? ORDER BY id',(pesquisa_id,))]
        import itens_cesta
        pesquisa['itens']=itens_cesta.listar_con(c,pesquisa_id)
        por_id={i['id']:i for i in pesquisa['itens']}
        import conferencia_cesta
        conferencia_cesta.enriquecer(c,pesquisa_id,rows)
    for r in rows:
        r['dados']=json.loads(r.pop('dados_json'))
        itens_cesta.enriquecer(r,por_id.get(r.get('item_pesquisa_id')))
    return pesquisa,rows


def avaliar(db,pesquisa_id,referencia_id,*,situacao,grupo='',justificativa='',observacoes='',revisao,
            item_pesquisa_id=None,item_revisao=None):
    if situacao not in SITUACOES:raise ValueError('Situação inválida.')
    grupo=texto(grupo,160,'o grupo')
    justificativa=texto(justificativa,4000,'a justificativa',situacao=='Excluir')
    observacoes=texto(observacoes,8000,'as observações')
    preparar(db);stamp=agora()
    with conectar(db) as c:
        c.execute('BEGIN IMMEDIATE')
        if obter(db,pesquisa_id,c)['arquivada']:raise ValueError('Reabra a pesquisa antes de avaliar referências.')
        row=c.execute('SELECT * FROM referencias WHERE id=? AND pesquisa_id=?',(referencia_id,pesquisa_id)).fetchone()
        if row is None or row['revisao']!=revisao:raise ValueError('A referência foi alterada em outra aba. Recarregue antes de salvar.')
        import itens_cesta
        item_pesquisa_id,grupo,item=itens_cesta.vinculo(c,pesquisa_id,row,item_pesquisa_id,grupo,item_revisao)
        cur=c.execute('''UPDATE referencias SET situacao=?,grupo=?,justificativa=?,observacoes=?,item_pesquisa_id=?,revisado_em=?,revisao=revisao+1
            WHERE id=? AND pesquisa_id=? AND revisao=?''',
            (situacao,grupo,justificativa,observacoes,item_pesquisa_id,stamp,referencia_id,pesquisa_id,revisao))
        if not cur.rowcount:raise ValueError('A referência foi alterada em outra aba. Recarregue antes de salvar.')
        c.execute('INSERT INTO historico_referencias(referencia_id,alterado_em,revisao,avaliacao_json) VALUES(?,?,?,?)',
            (referencia_id,stamp,revisao+1,json.dumps(dict(situacao=situacao,grupo=grupo,justificativa=justificativa,
                observacoes=observacoes,item_pesquisa_id=item_pesquisa_id,item_pesquisa=item),ensure_ascii=False)))
        c.execute('UPDATE pesquisas SET atualizado_em=?,revisao=revisao+1 WHERE id=?',(stamp,pesquisa_id))


def historico(db,referencia_id):
    with conectar(db) as c:
        return [dict(r) for r in c.execute('SELECT * FROM historico_referencias WHERE referencia_id=? ORDER BY id DESC',(referencia_id,))]


def avaliar_lote(db,pesquisa_id,selecionadas,*,situacao=None,grupo=None,justificativa=None,observacoes=None,
                 item_pesquisa_id=None,item_revisao=None,revisoes_itens=None):
    """Atualiza campos explícitos em uma transação e audita cada referência.

    selecionadas é um mapa {id: revisão vista}; None preserva o campo existente.
    """
    if not selecionadas:raise ValueError('Selecione pelo menos uma referência para avaliar.')
    if situacao is not None and situacao not in SITUACOES:raise ValueError('Situação inválida.')
    if all(x is None for x in (situacao,grupo,justificativa,observacoes,item_pesquisa_id)):
        raise ValueError('Informe uma decisão ou um campo para alterar.')
    if grupo is not None:grupo=texto(grupo,160,'o grupo')
    if justificativa is not None:justificativa=texto(justificativa,4000,'a justificativa')
    if observacoes is not None:observacoes=texto(observacoes,8000,'as observações')
    preparar(db);stamp=agora();alteradas=0
    with conectar(db) as c:
        c.execute('BEGIN IMMEDIATE')
        if obter(db,pesquisa_id,c)['arquivada']:raise ValueError('Reabra a pesquisa antes de avaliar referências.')
        for rid,revisao in selecionadas.items():
            row=c.execute('SELECT * FROM referencias WHERE id=? AND pesquisa_id=?',(rid,pesquisa_id)).fetchone()
            if row is None or row['revisao']!=revisao:
                raise ValueError('Uma referência do lote foi alterada em outra aba. Recarregue antes de salvar; nenhuma avaliação deste lote foi aplicada.')
            import itens_cesta
            ident,grupo_atual,item=itens_cesta.vinculo(c,pesquisa_id,row,item_pesquisa_id,grupo,item_revisao,revisoes_itens)
            valores={'situacao':situacao if situacao is not None else row['situacao'],
                     'grupo':grupo_atual,'item_pesquisa_id':ident,
                     'justificativa':justificativa if justificativa is not None else row['justificativa'],
                     'observacoes':observacoes if observacoes is not None else row['observacoes']}
            texto(valores['justificativa'],4000,'a justificativa',valores['situacao']=='Excluir')
            if all(valores[k]==row[k] for k in valores):continue
            c.execute('''UPDATE referencias SET situacao=?,grupo=?,item_pesquisa_id=?,justificativa=?,observacoes=?,
                         revisado_em=?,revisao=revisao+1 WHERE id=? AND pesquisa_id=? AND revisao=?''',
                      (*valores.values(),stamp,rid,pesquisa_id,revisao))
            c.execute('INSERT INTO historico_referencias(referencia_id,alterado_em,revisao,avaliacao_json) VALUES(?,?,?,?)',
                      (rid,stamp,revisao+1,json.dumps({**valores,'item_pesquisa':item},ensure_ascii=False)))
            alteradas+=1
        if alteradas:c.execute('UPDATE pesquisas SET atualizado_em=?,revisao=revisao+1 WHERE id=?',(stamp,pesquisa_id))
    return {'alteradas':alteradas,'sem_mudanca':len(selecionadas)-alteradas}
