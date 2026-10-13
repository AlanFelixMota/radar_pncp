"""MONITORAMENTO: identidades novas para buscas salvas, com baseline e revisão explícita."""
from datetime import datetime,date,timedelta
from pathlib import Path
import hashlib,json,secrets,sqlite3,time

import cestas,buscas_salvas as bs,cestas_lote as lote,radar_db as rd
import pesquisa_fornecedores as pf,filtros_precos as fp

INTERVALO=15*60
MAX_RESULTADOS=200000


def assinatura(busca):
    p=bs.resolver(busca)
    # Datas móveis dependem do dia, mas a configuração do acompanhamento não.
    d=json.loads(busca['filtros_json'])
    raw={'tipo':p['tipo'],'filtros':bs._normalizar(p['tipo'],d['filtros'],p['por_pagina'])['filtros'],
         'periodo':busca['periodo']}
    if busca['periodo']=='ultimos_dias':raw.update(dias=busca['dias'],incluir_hoje=bool(busca['incluir_hoje']))
    if busca['periodo']=='todo_periodo':raw['filtros'].update(data_inicio=None,data_fim=None)
    return hashlib.sha256(json.dumps(raw,sort_keys=True,ensure_ascii=False).encode()).hexdigest()


def preparar(con):
    con.execute('''CREATE TABLE IF NOT EXISTS monitoramento_acompanhamentos(
        id INTEGER PRIMARY KEY,busca_id INTEGER NOT NULL UNIQUE REFERENCES buscas_salvas(id),
        ativo INTEGER NOT NULL,geracao INTEGER NOT NULL,assinatura TEXT NOT NULL,
        iniciado_em TEXT NOT NULL,verificado_em TEXT,tentativa_em TEXT,erro TEXT NOT NULL DEFAULT '',
        total_atual INTEGER NOT NULL DEFAULT 0,revisao INTEGER NOT NULL DEFAULT 1,
        token TEXT,ocupado_em TEXT)''')
    con.execute('''CREATE TABLE IF NOT EXISTS monitoramento_conhecidos(
        acompanhamento_id INTEGER NOT NULL REFERENCES monitoramento_acompanhamentos(id),
        geracao INTEGER NOT NULL,chave TEXT NOT NULL,
        PRIMARY KEY(acompanhamento_id,geracao,chave))''')
    con.execute('''CREATE TABLE IF NOT EXISTS monitoramento_ciclos(
        id INTEGER PRIMARY KEY,acompanhamento_id INTEGER NOT NULL REFERENCES monitoramento_acompanhamentos(id),
        geracao INTEGER NOT NULL,tipo TEXT NOT NULL,consultado_em TEXT NOT NULL,
        pedido_json TEXT NOT NULL,total INTEGER NOT NULL,novas INTEGER NOT NULL,cobertura_json TEXT NOT NULL)''')
    con.execute('CREATE INDEX IF NOT EXISTS idx_monitoramento_ciclos ON monitoramento_ciclos(acompanhamento_id,id)')
    con.execute('CREATE INDEX IF NOT EXISTS idx_monitoramento_geracao ON monitoramento_ciclos(acompanhamento_id,geracao,id)')
    con.execute('''CREATE TABLE IF NOT EXISTS monitoramento_novidades(
        id INTEGER PRIMARY KEY,acompanhamento_id INTEGER NOT NULL REFERENCES monitoramento_acompanhamentos(id),
        geracao INTEGER NOT NULL,ciclo_id INTEGER NOT NULL REFERENCES monitoramento_ciclos(id),chave TEXT NOT NULL,
        dados_json TEXT NOT NULL,detectado_em TEXT NOT NULL,
        estado TEXT NOT NULL DEFAULT 'Pendente',revisado_em TEXT,revisao INTEGER NOT NULL DEFAULT 1,
        UNIQUE(acompanhamento_id,geracao,chave))''')
    con.execute('CREATE INDEX IF NOT EXISTS idx_monitoramento_revisao ON monitoramento_novidades(acompanhamento_id,geracao,estado,id)')


def existe(con):
    return bool(con.execute("SELECT 1 FROM sqlite_master WHERE name='monitoramento_acompanhamentos'").fetchone())


def listar(db):
    if not Path(db).is_file():return []
    with cestas.conectar(db) as con:
        if not existe(con):return []
        return [dict(r) for r in con.execute('''SELECT a.*,b.nome,b.tipo,b.arquivada,
            (SELECT count(*) FROM monitoramento_novidades n WHERE n.acompanhamento_id=a.id
             AND n.geracao=a.geracao AND n.estado='Pendente') AS pendentes,
            (SELECT count(*) FROM monitoramento_novidades n WHERE n.acompanhamento_id=a.id AND n.estado='Pendente') AS pendentes_total
            FROM monitoramento_acompanhamentos a JOIN buscas_salvas b ON b.id=a.busca_id ORDER BY b.nome,a.id''')]


def obter(con,ident):
    r=con.execute('SELECT * FROM monitoramento_acompanhamentos WHERE id=?',(ident,)).fetchone()
    if r is None:raise ValueError('Acompanhamento não encontrado. Recarregue a página.')
    return dict(r)


def _busca(con,ident):
    r=con.execute('SELECT * FROM buscas_salvas WHERE id=?',(ident,)).fetchone()
    if r is None:raise ValueError('Busca salva não encontrada.')
    d=dict(r);bs.resolver(d);return d


def _limite(max_segundos,cancelar):
    inicio=time.monotonic()
    def verificar():
        if time.monotonic()-inicio>=max_segundos:
            raise ValueError('A verificação excedeu o tempo disponível; nenhum resultado parcial foi registrado. Refine os filtros da busca salva.')
        if cancelar and cancelar():raise ValueError('Verificação interrompida para atualização ou encerramento. Nenhuma novidade parcial foi registrada.')
    return verificar


def _evidencia(con,pedido):
    out={'periodo_inicio':pedido['filtros']['data_inicio'],'periodo_fim':pedido['filtros']['data_fim'],
         'tipo':pedido['tipo'],'coleta':None,'resultados_sync':None,
         'aviso':'Acompanhamento da base local, sem certificação de cobertura completa. Ausência de novidades não comprova ausência de contratações ou resultados na fonte externa.'}
    if rd.tabela_existe(con,'coleta_janelas_pesquisa'):
        r=con.execute('SELECT * FROM coleta_janelas_pesquisa ORDER BY iniciado_em DESC LIMIT 1').fetchone()
        if r:out['coleta']=dict(r)
    if rd.tabela_existe(con,'comprasgov_resultados_sync'):
        r=con.execute('SELECT * FROM comprasgov_resultados_sync WHERE id=1').fetchone()
        if r:out['resultados_sync']=dict(r)
    return out


def _consultar(db_compras,pedido,db_fts,db_tic,verificar):
    if not Path(db_compras).is_file():raise ValueError('Base de contratações indisponível. Confira Situação da base.')
    encontrados={};pendentes=0
    with lote._leitura(db_compras,120) as (con,_):
        # Limite também cobre a execução SQL antes de a primeira linha aparecer.
        def interromper():
            try:verificar();return 0
            except ValueError:return 1
        con.set_progress_handler(interromper,1000)
        if not {'contratacoes','itens'}<={r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}:
            raise ValueError('A base local ainda não possui as tabelas de contratações e itens.')
        tipo=pedido['tipo'];f=pedido['filtros']
        if tipo=='contratacoes':
            filtros=rd.FiltrosBusca(**f)
            if fp.exige_resultados(filtros) and not rd.tabela_existe(con,'resultados_itens'):
                raise ValueError('A busca exige resultados homologados ainda indisponíveis na base local.')
            if (filtros.tic_status!='todos' or filtros.tic_categorias) and (not db_tic or not Path(db_tic).is_file()):
                raise ValueError('O filtro de TIC exige a base de classificação. Aguarde sua disponibilidade antes de criar a referência inicial.')
            rows=lote._contratacoes(con,filtros,None,db_fts,db_tic)
        else:
            if not rd.tabela_existe(con,'resultados_itens'):raise ValueError('Resultados de fornecedores ainda indisponíveis na base local.')
            where,params=pf._condicoes(con,pf.FiltrosFornecedor(**f))
            rows=con.execute(pf.SELECT_RESULTADOS+' WHERE '+where,params)
        stamp=cestas.agora()
        for row in rows:
            verificar();d=dict(row)
            if tipo=='contratacoes':
                chave=json.dumps(['contratacoes',d['numero_controle_pncp']],ensure_ascii=False,separators=(',',':'))
                pendentes+=not bool(d.get('itens_coletados'))
                dados={k:d.get(k) for k in ('numero_controle_pncp','objeto','orgao_razao_social','orgao_cnpj',
                    'unidade_nome','uf_sigla','processo','numero_compra','ano_compra','data_publicacao_pncp',
                    'data_atualizacao_global','ultima_coleta_em','url_pncp','link_sistema_origem','itens_status')}
            else:
                dados=cestas.referencia_resultado(d,stamp);chave=cestas.chave(dados)
            encontrados[chave]={'tipo':tipo,'dados':dados}
            if len(encontrados)>MAX_RESULTADOS:raise ValueError('A busca retornou mais de 200.000 identidades. Refine seus filtros; nenhum resultado parcial foi salvo.')
        cobertura=_evidencia(con,pedido);cobertura['contratacoes_encontradas_com_itens_pendentes']=int(pendentes)
    verificar();return encontrados,stamp,cobertura


def _ciclo(con,monitor,pedido,tipo,encontrados,stamp,cobertura,novas):
    return con.execute('''INSERT INTO monitoramento_ciclos(acompanhamento_id,geracao,tipo,consultado_em,pedido_json,total,novas,cobertura_json)
        VALUES(?,?,?,?,?,?,?,?)''',(monitor['id'],monitor['geracao'],tipo,stamp,
            json.dumps(pedido,ensure_ascii=False,allow_nan=False),len(encontrados),len(novas),json.dumps(cobertura,ensure_ascii=False,allow_nan=False))).lastrowid


def iniciar(db_compras,db,busca_id,*,revisao_busca,revisao_monitor=None,db_fts=None,db_tic=None,
            hoje=None,max_segundos=120,cancelar=None):
    busca=bs.obter(db,busca_id)
    if busca['revisao']!=revisao_busca:raise ValueError('A busca mudou em outra aba. Recarregue antes de ativar.')
    sig=assinatura(busca);pedido=bs.resolver(busca,hoje=hoje);verificar=_limite(max_segundos,cancelar)
    try:encontrados,stamp,cobertura=_consultar(db_compras,pedido,db_fts,db_tic,verificar)
    except sqlite3.OperationalError:verificar();raise
    with cestas.conectar(db) as con:
        con.execute('BEGIN IMMEDIATE');verificar()
        atual=_busca(con,busca_id)
        if atual['revisao']!=revisao_busca:raise ValueError('A busca mudou durante a ativação. Nenhuma referência inicial foi salva.')
        preparar(con)
        row=con.execute('SELECT * FROM monitoramento_acompanhamentos WHERE busca_id=?',(busca_id,)).fetchone()
        if row:
            if revisao_monitor is None or row['revisao']!=revisao_monitor:raise ValueError('O acompanhamento mudou. Recarregue antes de criar uma nova referência inicial.')
            ident=row['id'];geracao=row['geracao']+1
            con.execute('''UPDATE monitoramento_acompanhamentos SET ativo=1,geracao=?,assinatura=?,iniciado_em=?,
                verificado_em=?,tentativa_em=?,erro='',total_atual=?,revisao=revisao+1,token=NULL,ocupado_em=NULL WHERE id=?''',
                (geracao,sig,stamp,stamp,stamp,len(encontrados),ident))
        else:
            if revisao_monitor is not None:raise ValueError('Acompanhamento anterior não encontrado.')
            from identificadores import proximo
            ident=proximo(con,'monitoramento_acompanhamentos')
            con.execute('''INSERT INTO monitoramento_acompanhamentos(id,busca_id,ativo,geracao,assinatura,iniciado_em,verificado_em,tentativa_em,total_atual)
                VALUES(?,?,1,1,?,?,?,?,?)''',(ident,busca_id,sig,stamp,stamp,stamp,len(encontrados)))
            geracao=1
        monitor={'id':ident,'geracao':geracao}
        _ciclo(con,monitor,pedido,'Inicial',encontrados,stamp,cobertura,[])
        for chave in encontrados:
            verificar();con.execute('INSERT INTO monitoramento_conhecidos VALUES(?,?,?)',(ident,geracao,chave))
    return {'id':ident,'geracao':geracao,'total_inicial':len(encontrados)}


def ativar(db,ident,*,ativo,revisao):
    if type(ativo) is not bool:raise ValueError('Opção inválida.')
    with cestas.conectar(db) as con:
        con.execute('BEGIN IMMEDIATE');m=obter(con,ident)
        if m['revisao']!=revisao:raise ValueError('O acompanhamento mudou. Recarregue antes de continuar.')
        if ativo and assinatura(_busca(con,m['busca_id']))!=m['assinatura']:
            raise ValueError('Os filtros da busca mudaram. Crie uma nova referência inicial antes de retomar.')
        con.execute('UPDATE monitoramento_acompanhamentos SET ativo=?,token=NULL,ocupado_em=NULL,revisao=revisao+1 WHERE id=?',(int(ativo),ident))


def verificar(db_compras,db,ident,*,db_fts=None,db_tic=None,hoje=None,max_segundos=120,cancelar=None):
    token=secrets.token_hex(16);check=_limite(max_segundos,cancelar)
    with cestas.conectar(db) as con:
        con.execute('BEGIN IMMEDIATE');m=obter(con,ident);b=_busca(con,m['busca_id'])
        if not m['ativo']:raise ValueError('Retome o acompanhamento antes de verificar.')
        if assinatura(b)!=m['assinatura']:raise ValueError('Os filtros da busca mudaram. Crie uma nova referência inicial; as novidades anteriores continuam preservadas.')
        if m['token'] and m['ocupado_em'] and datetime.fromisoformat(m['ocupado_em'])>datetime.now().astimezone()-timedelta(minutes=5):
            raise ValueError('Este acompanhamento já está sendo verificado. Aguarde a conclusão.')
        con.execute('UPDATE monitoramento_acompanhamentos SET token=?,ocupado_em=?,tentativa_em=? WHERE id=?',(token,cestas.agora(),cestas.agora(),ident))
        conhecidos={r[0] for r in con.execute('SELECT chave FROM monitoramento_conhecidos WHERE acompanhamento_id=? AND geracao=?',(ident,m['geracao']))}
    try:
        pedido=bs.resolver(b,hoje=hoje)
        try:encontrados,stamp,cobertura=_consultar(db_compras,pedido,db_fts,db_tic,check)
        except sqlite3.OperationalError:check();raise
        novos={k:v for k,v in encontrados.items() if k not in conhecidos}
        with cestas.conectar(db) as con:
            con.execute('BEGIN IMMEDIATE');check();atual=obter(con,ident);busca=_busca(con,m['busca_id'])
            if atual['token']!=token or atual['revisao']!=m['revisao'] or not atual['ativo'] or assinatura(busca)!=m['assinatura']:
                raise ValueError('A configuração mudou durante a verificação. Nenhuma novidade parcial foi salva.')
            cid=_ciclo(con,m,pedido,'Verificação',encontrados,stamp,cobertura,novos)
            for chave,dados in novos.items():
                check();con.execute('INSERT INTO monitoramento_conhecidos VALUES(?,?,?)',(ident,m['geracao'],chave))
                con.execute('''INSERT INTO monitoramento_novidades(acompanhamento_id,geracao,ciclo_id,chave,dados_json,detectado_em)
                    VALUES(?,?,?,?,?,?)''',(ident,m['geracao'],cid,chave,json.dumps(dados,ensure_ascii=False,allow_nan=False),stamp))
            con.execute("UPDATE monitoramento_acompanhamentos SET verificado_em=?,total_atual=?,erro='',token=NULL,ocupado_em=NULL,revisao=revisao+1 WHERE id=?",(stamp,len(encontrados),ident))
        return {'novas':len(novos),'total':len(encontrados),'consultado_em':stamp}
    except Exception as exc:
        with cestas.conectar(db) as con:
            con.execute("UPDATE monitoramento_acompanhamentos SET token=NULL,ocupado_em=NULL,erro=? WHERE id=? AND token=?",(str(exc)[:1200],ident,token))
        raise


def ciclos(db,ident,limite=20):
    with cestas.conectar(db) as con:
        if not existe(con):return []
        rows=con.execute('SELECT * FROM monitoramento_ciclos WHERE acompanhamento_id=? ORDER BY id DESC LIMIT ?',(ident,min(100,max(1,int(limite)))))
        return [{**dict(r),'pedido':json.loads(r['pedido_json']),'cobertura':json.loads(r['cobertura_json'])} for r in rows]


def configuracao(db,ident,geracao):
    with cestas.conectar(db) as con:
        row=con.execute('SELECT pedido_json FROM monitoramento_ciclos WHERE acompanhamento_id=? AND geracao=? ORDER BY id DESC LIMIT 1',(ident,geracao)).fetchone()
        if row is None:raise ValueError('Referência inicial não encontrada. Recarregue a página.')
        return json.loads(row['pedido_json'])


def novidades(db,ident,*,geracao=None,estado='Pendente',pagina=1,por_pagina=20):
    pagina=max(1,int(pagina));por_pagina=min(50,max(1,int(por_pagina)))
    with cestas.conectar(db) as con:
        con.execute('BEGIN');m=obter(con,ident)
        geracao=m['geracao'] if geracao is None else int(geracao)
        where='acompanhamento_id=? AND geracao=?';params=[ident,geracao]
        if estado!='Todas':where+=' AND estado=?';params.append(estado)
        total=con.execute('SELECT count(*) FROM monitoramento_novidades WHERE '+where,params).fetchone()[0]
        pagina=min(pagina,max(1,(total+por_pagina-1)//por_pagina))
        rows=[dict(r) for r in con.execute('SELECT * FROM monitoramento_novidades WHERE '+where+' ORDER BY id DESC LIMIT ? OFFSET ?',params+[por_pagina,(pagina-1)*por_pagina])]
    for r in rows:r['registro']=json.loads(r.pop('dados_json'))
    return {'total':total,'pagina':pagina,'paginas':max(1,(total+por_pagina-1)//por_pagina),'registros':rows}


def _selecionadas(con,ident,selecionadas):
    if not selecionadas:raise ValueError('Selecione pelo menos uma novidade.')
    out=[]
    for nid,rev in selecionadas.items():
        r=con.execute('''SELECT n.*,c.pedido_json FROM monitoramento_novidades n JOIN monitoramento_ciclos c ON c.id=n.ciclo_id
            WHERE n.id=? AND n.acompanhamento_id=?''',(nid,ident)).fetchone()
        if r is None or r['revisao']!=rev:raise ValueError('Uma novidade mudou em outra aba. Recarregue; nenhuma alteração do lote foi aplicada.')
        out.append({**dict(r),'registro':json.loads(r['dados_json']),'pedido':json.loads(r['pedido_json'])})
    return out


def revisar(db,ident,selecionadas,*,estado='Revisado'):
    if estado not in ('Pendente','Revisado'):raise ValueError('Estado de revisão inválido.')
    stamp=cestas.agora();n=0
    with cestas.conectar(db) as con:
        con.execute('BEGIN IMMEDIATE');obter(con,ident);rows=_selecionadas(con,ident,selecionadas)
        for r in rows:
            if r['estado']==estado:continue
            con.execute('UPDATE monitoramento_novidades SET estado=?,revisado_em=?,revisao=revisao+1 WHERE id=?',(estado,stamp,r['id']));n+=1
    return {'alteradas':n}


def adicionar_cesta(db_compras,db,ident,selecionadas,pesquisa_id,*,tipo_preco='Estimado',max_segundos=120):
    # As cópias de resultados são as registradas na detecção; itens de contratos
    # são lidos agora, com os filtros unitários da verificação que gerou o alerta.
    with cestas.conectar(db) as con:
        con.execute('BEGIN');rows=_selecionadas(con,ident,selecionadas)
    resumo={'sem_referencias':0,'contratacoes':0}
    with lote._leitura(db_compras,max_segundos) as (con,checar):
        def referencias():
            for r in rows:
                checar();p=r['registro'];d=p['dados']
                if p['tipo']=='fornecedores':yield d;continue
                if tipo_preco not in ('Estimado','Homologado'):raise ValueError('Tipo de preço inválido.')
                f=rd.FiltrosBusca(**r['pedido']['filtros'])
                if fp.ativo(f) and f.preco_tipo!=tipo_preco:raise ValueError('Use o tipo de preço dos filtros acompanhados.')
                resumo['contratacoes']+=1
                c=con.execute('SELECT * FROM contratacoes WHERE numero_controle_pncp=?',(d['numero_controle_pncp'],)).fetchone()
                n=0
                if c:
                    if tipo_preco=='Estimado':
                        where,params=fp.clausula(f,'i.unidade_medida','i.valor_unitario_estimado')
                        itens=con.execute('SELECT i.* FROM itens i WHERE i.numero_controle_pncp=? AND '+where,[d['numero_controle_pncp'],*params])
                    else:
                        if not rd.tabela_existe(con,'resultados_itens'):raise ValueError('Resultados homologados ainda indisponíveis na base local.')
                        where,params=fp.clausula(f,'i.unidade_medida','r.valor_unitario_homologado')
                        itens=con.execute(pf.SELECT_RESULTADOS+' WHERE r.numero_controle_pncp=? AND NOT '+pf.CANCELADO+' AND '+where,[d['numero_controle_pncp'],*params])
                    for item in itens:
                        checar();n+=1;yield cestas.referencia_item(dict(c),dict(item),cestas.agora()) if tipo_preco=='Estimado' else cestas.referencia_resultado(dict(item),cestas.agora())
                if not n:resumo['sem_referencias']+=1
        result=cestas.adicionar(db,pesquisa_id,referencias())
    return {**result,**resumo}
