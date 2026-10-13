"""Compara cópias da cesta com a base local; nunca escreve nas fontes ou cópias."""
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
import json
import sqlite3
import time

import cestas

SEM='Sem divergência local'
ALTERADA='Divergência encontrada'
CANCELADA='Cancelamento indicado'
AUSENTE_PNCP='Ausência informada pelo PNCP'
NAO_LOCALIZADA='Não localizada na base'
INSUFICIENTE='Base insuficiente'
ANTERIOR='Base anterior à cópia'
ESTADOS=(SEM,ALTERADA,CANCELADA,AUSENTE_PNCP,NAO_LOCALIZADA,INSUFICIENTE,ANTERIOR)
CAMPOS={
    'valor_unitario':'Preço unitário (R$)', 'valor_total':'Valor total (R$)',
    'quantidade':'Quantidade', 'unidade_medida':'Unidade de medida',
    'descricao':'Descrição', 'informacao_complementar':'Informação complementar',
    'ni_fornecedor':'Identificação do fornecedor', 'nome_fornecedor':'Fornecedor',
    'situacao_resultado_nome':'Situação na fonte', 'cancelado':'Cancelado',
    'data_cancelamento_pncp':'Data do cancelamento', 'motivo_cancelamento':'Motivo do cancelamento',
    'data_preco':'Data do resultado',
}
ALERTA=('CANCELADO','CANCELADA','ANULADO','ANULADA','REVOGADO','REVOGADA',
        'RESULTADO CANCELADO','RESULTADO ANULADO','RESULTADO REVOGADO')


def preparar(con):
    # Chamado somente por uma conferência explícita, dentro de sua transação.
    con.execute('''CREATE TABLE IF NOT EXISTS conferencias_cesta(
        id INTEGER PRIMARY KEY,pesquisa_id INTEGER NOT NULL REFERENCES pesquisas(id),
        consultado_em TEXT NOT NULL,total INTEGER NOT NULL)''')
    con.execute('''CREATE TABLE IF NOT EXISTS conferencias_referencia(
        id INTEGER PRIMARY KEY,conferencia_id INTEGER NOT NULL REFERENCES conferencias_cesta(id),
        referencia_id INTEGER NOT NULL REFERENCES referencias(id),estado TEXT NOT NULL,
        dados_json TEXT NOT NULL,UNIQUE(conferencia_id,referencia_id))''')
    con.execute('CREATE INDEX IF NOT EXISTS idx_conferencia_ref ON conferencias_referencia(referencia_id,id)')
    con.execute('CREATE INDEX IF NOT EXISTS idx_conferencia_cesta ON conferencias_cesta(pesquisa_id,id)')


def _existe(con,tabela):
    return con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(tabela,)).fetchone() is not None


def _registro(row):
    d=json.loads(row['dados_json'])
    return {**d,'id':row['id'],'conferencia_id':row['conferencia_id'],
            'estado':row['estado'],'consultado_em':row['consultado_em']}


def ultimas(con,pesquisa_id):
    if not _existe(con,'conferencias_referencia'):return {}
    rows=con.execute('''SELECT r.*,c.consultado_em FROM conferencias_referencia r
        JOIN conferencias_cesta c ON c.id=r.conferencia_id
        WHERE c.pesquisa_id=? AND NOT EXISTS(
            SELECT 1 FROM conferencias_referencia nova WHERE nova.referencia_id=r.referencia_id AND nova.id>r.id)''',
        (pesquisa_id,))
    return {r['referencia_id']:_registro(r) for r in rows}


def historico(db,pesquisa_id,referencia_id,limite=50):
    if not Path(db).is_file():return []
    with cestas.conectar(db) as con:
        if not _existe(con,'conferencias_referencia'):return []
        return [_registro(r) for r in con.execute('''SELECT r.*,c.consultado_em
            FROM conferencias_referencia r JOIN conferencias_cesta c ON c.id=r.conferencia_id
            WHERE c.pesquisa_id=? AND r.referencia_id=? ORDER BY r.id DESC LIMIT ?''',
            (pesquisa_id,referencia_id,min(200,max(1,int(limite)))))]


@contextmanager
def _leitura(db,verificar):
    con=sqlite3.connect(Path(db).resolve().as_uri()+'?mode=ro',uri=True,timeout=3)
    con.row_factory=sqlite3.Row
    con.execute('PRAGMA query_only=ON')
    con.set_progress_handler(lambda:int(not verificar(False)),1000)
    try:
        con.execute('BEGIN')
        yield con
    finally:
        con.set_progress_handler(None,0);con.close()


def _anterior(atual,salva):
    try:
        a=datetime.fromisoformat(str(atual).replace('Z','+00:00'))
        b=datetime.fromisoformat(str(salva).replace('Z','+00:00'))
        # Datas sem fuso não permitem comparação com horários de outro fuso.
        if (a.tzinfo is None)!=(b.tzinfo is None):return a.date()<b.date()
        if a.tzinfo:a=a.astimezone(timezone.utc);b=b.astimezone(timezone.utc)
        return a<b
    except (ValueError,TypeError):return False


class Fonte:
    def __init__(self,con):
        self.con=con;self.contratos={}
        self.cols={t:{r[1] for r in con.execute('PRAGMA table_info('+t+')')}
                   for t in ('itens','contratacoes','resultados_itens')}

    def linha(self,tabela,pid,item=None,seq=None):
        keys=['numero_controle_pncp'];params=[pid]
        if item is not None:keys.append('numero_item');params.append(item)
        if seq is not None:keys.append('sequencial_resultado');params.append(seq)
        if not set(keys)<=self.cols[tabela]:return None
        campos=set(CAMPOS)|{'numero_controle_pncp','numero_item','sequencial_resultado',
            'valor_unitario_estimado','valor_unitario_homologado','valor_total_homologado',
            'quantidade_homologada','situacao_item_nome','data_atualizacao','ultima_coleta_em',
            'data_atualizacao_pncp','data_resultado_pncp','fonte_ultima','importado_em',
            'pncp_ausente','pncp_consultado_em','orgao_razao_social','orgao_cnpj','unidade_nome',
            'uf_sigla','objeto','processo','numero_compra','ano_compra','url_pncp',
            'link_sistema_origem','data_publicacao_pncp'}
        # Seleção por chave indexada; não lê raw_json nem outros campos volumosos.
        selecao=','.join(sorted(campos&self.cols[tabela]))
        row=self.con.execute('SELECT '+selecao+' FROM '+tabela+' WHERE '+' AND '.join(k+'=?' for k in keys),params).fetchone()
        return dict(row) if row else None

    def comparar(self,salva):
        pid=salva['numero_controle_pncp'];item=salva['numero_item'];tipo=salva['tipo_preco']
        resultado={'atual':None,'divergencias':[],'avisos':[],'pncp_ausente':False,
                   'pncp_consultado_em':None,'parcial':False,'base_anterior':False}
        tabela='itens' if tipo=='Estimado' else 'resultados_itens'
        ids={'numero_controle_pncp','numero_item'}|({'sequencial_resultado'} if tipo=='Homologado' else set())
        if not ids<=self.cols[tabela]:
            return {**resultado,'estado':INSUFICIENTE,'avisos':['A base ainda não tem a estrutura necessária para localizar este tipo de referência.']}
        r=self.linha(tabela,pid,item,salva.get('sequencial_resultado') if tipo=='Homologado' else None)
        if r is None:return {**resultado,'estado':NAO_LOCALIZADA,'avisos':['A identidade exata não foi localizada. Isso não comprova cancelamento ou exclusão na fonte.']}
        if pid not in self.contratos:self.contratos[pid]=self.linha('contratacoes',pid)
        c=self.contratos[pid]
        i=r if tipo=='Estimado' else self.linha('itens',pid,item)
        try:
            if tipo=='Estimado':
                atual=cestas.referencia_item(c or {'numero_controle_pncp':pid},r)
                atual['contratacao_local']=c is not None
            else:
                raw={**(c or {}),**(i or {}),**r}
                raw['contratacao_local']=c is not None;raw['item_local']=i is not None
                raw['cancelado']=bool(str(r.get('data_cancelamento_pncp') or '').strip()
                    or str(r.get('motivo_cancelamento') or '').strip()
                    or str(r.get('situacao_resultado_nome') or '').strip().upper() in ALERTA)
                atual=cestas.referencia_resultado(raw)
        except (ValueError,TypeError,OverflowError):
            return {**resultado,'estado':INSUFICIENTE,'avisos':['Há um valor numérico inválido no registro local. Confira a fonte antes de usar o preço.']}
        mapa={'valor_unitario':'valor_unitario_estimado' if tipo=='Estimado' else 'valor_unitario_homologado',
              'valor_total':'valor_total' if tipo=='Estimado' else 'valor_total_homologado',
              'quantidade':'quantidade' if tipo=='Estimado' else 'quantidade_homologada',
              'situacao_resultado_nome':'situacao_item_nome' if tipo=='Estimado' else 'situacao_resultado_nome',
              'data_preco':'data_resultado_pncp'}
        conhecidos=set()
        for k in CAMPOS:
            if k in ('descricao','informacao_complementar','unidade_medida'):
                if i is not None and k in self.cols['itens']:conhecidos.add(k)
            elif tipo=='Homologado' and k=='cancelado':
                if {'data_cancelamento_pncp','motivo_cancelamento','situacao_resultado_nome'}<=self.cols[tabela]:conhecidos.add(k)
            elif mapa.get(k,k) in self.cols[tabela]:conhecidos.add(k)
        relevantes={'valor_unitario','valor_total','quantidade','situacao_resultado_nome','descricao','unidade_medida'}
        if tipo=='Homologado':relevantes|={'cancelado','ni_fornecedor','nome_fornecedor','data_preco'}
        if not relevantes<=conhecidos or c is None:
            resultado['parcial']=True
            resultado['avisos'].append('Há campos, contratação ou item indisponíveis na base local; a comparação é parcial.')
        for k in CAMPOS:
            if k in conhecidos and salva.get(k)!=atual.get(k):
                resultado['divergencias'].append({'campo':k,'rotulo':CAMPOS[k],
                                                 'salvo':salva.get(k),'atual':atual.get(k)})
        resultado['atual']=atual
        resultado['pncp_ausente']=bool(r.get('pncp_ausente')) if tipo=='Homologado' else False
        resultado['pncp_consultado_em']=r.get('pncp_consultado_em') if tipo=='Homologado' else None
        if resultado['pncp_ausente']:resultado['avisos'].append('A última consulta registrada ao PNCP não retornou este resultado. Ausência não equivale a cancelamento.')
        resultado['base_anterior']=_anterior(atual.get('data_atualizacao_fonte'),salva.get('data_atualizacao_fonte')) or _anterior(atual.get('importado_em'),salva.get('importado_em'))
        if resultado['base_anterior']:resultado['avisos'].append('O registro local tem uma data anterior à cópia salva. A divergência pode decorrer de uma base antiga ou restaurada.')
        cancelado=atual.get('cancelado') or str(atual.get('situacao_resultado_nome') or '').strip().upper() in ALERTA
        resultado['estado']=(CANCELADA if cancelado else AUSENTE_PNCP if resultado['pncp_ausente'] else
            ANTERIOR if resultado['base_anterior'] else ALTERADA if resultado['divergencias'] else
            INSUFICIENTE if resultado['parcial'] else SEM)
        return resultado


def conferir(db_compras,db_cestas,pesquisa_id,*,revisao,max_segundos=120,progresso=None):
    """Grava a cesta inteira atomicamente, após ler a fonte numa única transação."""
    inicio=time.monotonic()
    def verificar(levantar=True):
        valido=time.monotonic()-inicio<float(max_segundos)
        if not valido and levantar:raise ValueError('A conferência excedeu o tempo disponível. Nenhuma conferência foi salva; tente novamente com a base menos ocupada.')
        return valido
    pesquisa,refs=cestas.carregar(db_cestas,pesquisa_id)
    if pesquisa['arquivada']:raise ValueError('Reabra a pesquisa antes de registrar uma conferência.')
    if pesquisa['revisao']!=revisao:raise ValueError('A cesta mudou em outra aba. Recarregue antes de conferir.')
    if not refs:raise ValueError('Adicione referências antes de conferir a cesta.')
    if not Path(db_compras).is_file():raise ValueError('A base de contratações não está disponível. Confira Situação da base antes de tentar novamente.')
    resultados=[]
    try:
        with _leitura(db_compras,verificar) as con:
            fonte=Fonte(con);stamp=cestas.agora()
            for n,ref in enumerate(refs,1):
                verificar();resultados.append((ref['id'],fonte.comparar(ref['dados'])))
                if progresso and (n%100==0 or n==len(refs)):progresso(n)
    except sqlite3.OperationalError:
        verificar();raise
    with cestas.conectar(db_cestas) as con:
        con.execute('BEGIN IMMEDIATE');verificar()
        atual=cestas.obter(db_cestas,pesquisa_id,con)
        if atual['revisao']!=revisao or atual['arquivada']:
            raise ValueError('A cesta mudou durante a conferência. Nenhum resultado foi salvo. Recarregue antes de tentar novamente.')
        preparar(con)
        cid=con.execute('INSERT INTO conferencias_cesta(pesquisa_id,consultado_em,total) VALUES(?,?,?)',
                        (pesquisa_id,stamp,len(refs))).lastrowid
        contagens={}
        for rid,d in resultados:
            verificar();contagens[d['estado']]=contagens.get(d['estado'],0)+1
            con.execute('INSERT INTO conferencias_referencia(conferencia_id,referencia_id,estado,dados_json) VALUES(?,?,?,?)',
                        (cid,rid,d['estado'],json.dumps(d,ensure_ascii=False,allow_nan=False)))
        con.execute('UPDATE pesquisas SET atualizado_em=?,revisao=revisao+1 WHERE id=?',(cestas.agora(),pesquisa_id))
    return {'id':cid,'total':len(refs),'contagens':contagens,'consultado_em':stamp}


def enriquecer(con,pesquisa_id,refs):
    por_id=ultimas(con,pesquisa_id)
    for ref in refs:
        ref['conferencia']=por_id.get(ref['id'])


def estado(ref):
    return (ref.get('conferencia') or {}).get('estado','Não conferida')


def colunas_exportacao(ref):
    d=ref.get('conferencia') or {};atual=d.get('atual') or {}
    return {'conferencia_estado':estado(ref),'conferencia_em':d.get('consultado_em'),
        'conferencia_preco_local':atual.get('valor_unitario'),
        'conferencia_situacao_local':atual.get('situacao_resultado_nome'),
        'conferencia_fonte':atual.get('fonte_ultima'),
        'conferencia_importado_em':atual.get('importado_em'),
        'conferencia_atualizado_fonte':atual.get('data_atualizacao_fonte'),
        'conferencia_pncp_em':d.get('pncp_consultado_em'),
        'conferencia_avisos':' '.join(d.get('avisos',[]))}
