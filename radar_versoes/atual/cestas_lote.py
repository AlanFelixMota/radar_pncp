"""Inclusão local em lote, com leitura consistente e gravação atômica da cesta."""
from contextlib import contextmanager
from pathlib import Path
import sqlite3
import time

import cestas
import radar_db as rd
import pesquisa_fornecedores as pf
import filtros_precos as fp


@contextmanager
def _leitura(db, max_segundos):
    inicio=time.monotonic()
    con=sqlite3.connect(Path(db).resolve().as_uri()+'?mode=ro',uri=True,timeout=3)
    con.row_factory=sqlite3.Row
    con.execute('PRAGMA query_only=ON')
    con.create_function('normalizar_busca',1,pf.normalizar,deterministic=True)
    fp.registrar(con)
    def verificar():
        if time.monotonic()-inicio >= max_segundos:
            raise ValueError('A inclusão excedeu o tempo disponível. Nenhuma referência deste lote foi salva. Refine a pesquisa ou adicione uma contratação por vez.')
    con.set_progress_handler(lambda:int(time.monotonic()-inicio >= max_segundos),1000)
    try:
        con.execute('BEGIN')
        yield con,verificar
    except sqlite3.OperationalError:
        verificar()
        raise
    finally:
        con.set_progress_handler(None,0)
        con.close()


def _contratacoes(con, filtros, numero_controle, db_fts, db_tic):
    if numero_controle is not None:
        return con.execute('SELECT c.* FROM contratacoes c WHERE numero_controle_pncp=?',(numero_controle,))
    if filtros is None:raise ValueError('Faça uma pesquisa antes de incluir seus resultados.')
    if fp.exige_resultados(filtros) and not rd.tabela_existe(con,'resultados_itens'):
        return iter(())
    tem_tic=bool(db_tic and Path(db_tic).exists())
    if tem_tic:con.execute('ATTACH DATABASE ? AS tic',(str(Path(db_tic).resolve()),))
    join='LEFT JOIN tic.classificacoes tc ON tc.numero_controle_pncp=c.numero_controle_pncp' if tem_tic else ''
    cols=rd.colunas_tabela(con,'contratacoes')
    usar_fts=bool(filtros.texto.strip()) and rd.indice_fts_pronto(db_fts,con)
    query=rd.consulta_fts(filtros.texto,filtros.modo_texto) if usar_fts else ''
    if usar_fts and query:
        where,params=rd._where_filtros_sem_texto(filtros,cols,tem_tic)
        # Mesma união de contratações e itens da busca paginada, sem rank/ordenação.
        sql='''WITH hits AS (
            SELECT numero_controle_pncp FROM fts.contratacoes_fts WHERE contratacoes_fts MATCH ?
            UNION SELECT numero_controle_pncp FROM fts.itens_fts WHERE itens_fts MATCH ?)
            SELECT c.* FROM contratacoes c JOIN hits h ON h.numero_controle_pncp=c.numero_controle_pncp
            '''+join+' WHERE '+where
        return con.execute(sql,[query,query,*params])
    where,params=rd._where_busca(filtros,cols,tem_tic)
    return con.execute('SELECT c.* FROM contratacoes c '+join+' WHERE '+where,params)


def salvar_itens(db_compras, db_cestas, pesquisa_id, *, filtros=None,
                 numero_controle=None, db_fts=None, db_tic=None, progresso=None,
                 max_segundos=120, tipo_preco='Estimado'):
    """Todos os itens estimados ou todos os resultados válidos das contratações."""
    if tipo_preco not in ('Estimado','Homologado'):raise ValueError('Tipo de preço inválido.')
    if filtros is not None:
        fp.validar(filtros)
        if fp.ativo(filtros) and filtros.preco_tipo!=tipo_preco:
            raise ValueError('Use o mesmo tipo de preço dos filtros executados para adicionar à cesta.')
    resumo={'contratacoes':0,'sem_referencias':0,'referencias':0,'tipo_preco':tipo_preco}
    stamp=cestas.agora()
    with _leitura(db_compras,max_segundos) as (con,verificar):
        if tipo_preco=='Homologado' and not rd.tabela_existe(con,'resultados_itens'):
            raise ValueError('A base local ainda não possui resultados homologados.')
        rows=_contratacoes(con,filtros,numero_controle,db_fts,db_tic)
        def referencias():
            for row in rows:
                verificar();contratacao=dict(row);resumo['contratacoes']+=1
                quantidade=0
                if tipo_preco=='Estimado':
                    where,params=fp.clausula(filtros,'i.unidade_medida','i.valor_unitario_estimado') if filtros is not None else ('1=1',[])
                    itens=con.execute('SELECT i.* FROM itens i WHERE numero_controle_pncp=? AND '+where+' ORDER BY numero_item',
                                      [contratacao['numero_controle_pncp'],*params])
                else:
                    where,params=fp.clausula(filtros,'i.unidade_medida','r.valor_unitario_homologado') if filtros is not None else ('1=1',[])
                    itens=con.execute(pf.SELECT_RESULTADOS+' WHERE r.numero_controle_pncp=? AND NOT '+pf.CANCELADO+
                                      ' AND '+where+' ORDER BY r.numero_item,r.sequencial_resultado',
                                      [contratacao['numero_controle_pncp'],*params])
                for item in itens:
                    verificar();quantidade+=1;resumo['referencias']+=1
                    yield (cestas.referencia_item(contratacao,dict(item),stamp) if tipo_preco=='Estimado'
                           else cestas.referencia_resultado(dict(item),stamp))
                    if progresso and resumo['referencias']%250==0:progresso(resumo['referencias'])
                if not quantidade:resumo['sem_referencias']+=1
        resultado=cestas.adicionar(db_cestas,pesquisa_id,referencias())
    return {**resumo,**resultado}


def salvar_resultados(db_compras, db_cestas, pesquisa_id, *, filtros=None,
                      numero_controle=None, progresso=None, max_segundos=120):
    """Todos os resultados homologados correspondentes, sem o limite da página."""
    stamp=cestas.agora();resumo={'contratacoes':0,'sem_referencias':0,'referencias':0}
    with _leitura(db_compras,max_segundos) as (con,verificar):
        if not rd.tabela_existe(con,'resultados_itens'):
            raise ValueError('A base local ainda não possui resultados homologados.')
        if filtros is not None:
            pf.validar(filtros);where,params=pf._condicoes(con,filtros)
        else:
            where,params='1',[]
        if numero_controle is not None:
            where='('+where+') AND r.numero_controle_pncp=?';params.append(numero_controle)
        if filtros is None and numero_controle is None:
            raise ValueError('Informe a contratação ou faça uma pesquisa de fornecedor.')
        rows=con.execute(pf.SELECT_RESULTADOS+' WHERE '+where+
                         ' ORDER BY r.numero_controle_pncp,r.numero_item,r.sequencial_resultado',params)
        def referencias():
            anterior=None
            for row in rows:
                verificar();r=dict(row)
                if r['numero_controle_pncp']!=anterior:
                    resumo['contratacoes']+=1;anterior=r['numero_controle_pncp']
                resumo['referencias']+=1
                yield cestas.referencia_resultado(r,stamp)
                if progresso and resumo['referencias']%250==0:progresso(resumo['referencias'])
        resultado=cestas.adicionar(db_cestas,pesquisa_id,referencias())
    return {**resumo,**resultado}
