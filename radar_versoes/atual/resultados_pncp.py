"""Resultados PNCP persistentes: descoberta incremental e uma transação por item.

O histórico é descoberto por atualização global/publicação (ordem decrescente).
Triggers pequenos registram contratações/itens alterados; não fazem chamadas de
rede e não percorrem o histórico. Nenhuma migração é executada pela interface.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import datetime, timedelta
import json
import math
from pathlib import Path
import re
import sqlite3
import time

import requests
import comprasgov_resultados as cr
from pausa_atualizacao import PausaAtualizacao

BASE = 'https://pncp.gov.br/api/pncp/v1'
DATA_ORDEM = "COALESCE(NULLIF(data_atualizacao_global,''),NULLIF(data_publicacao_pncp,''),'')"
ESTADOS = ('PENDENTE', 'CONFIRMADO', 'SEM_RESULTADO', 'RETENTAR', 'INDISPONIVEL')


def agora():
    return datetime.now().isoformat(timespec='seconds')


def existe(con, tabela):
    return bool(con.execute("SELECT 1 FROM sqlite_master WHERE name=?", (tabela,)).fetchone())


def preparar(con):
    """Somente no worker. DDL idempotente; índice do histórico criado fora da UI."""
    cr.garantir_tabelas(con)
    con.executescript(f'''
      CREATE TABLE IF NOT EXISTS pncp_resultados_fila(
        numero_controle_pncp TEXT NOT NULL, numero_item INTEGER NOT NULL,
        prioridade TEXT NOT NULL, estado TEXT NOT NULL DEFAULT 'PENDENTE',
        geracao INTEGER NOT NULL DEFAULT 1, tentativas INTEGER NOT NULL DEFAULT 0,
        retry_em TEXT, consultado_em TEXT, qtd_resultados INTEGER, erro TEXT,
        PRIMARY KEY(numero_controle_pncp,numero_item));
      CREATE INDEX IF NOT EXISTS idx_rpncp_fila ON pncp_resultados_fila
        (estado,prioridade DESC,numero_controle_pncp,numero_item);
      CREATE INDEX IF NOT EXISTS idx_rpncp_retry ON pncp_resultados_fila(retry_em)
        WHERE retry_em IS NOT NULL;
      CREATE TABLE IF NOT EXISTS pncp_resultados_eventos(
        numero_controle_pncp TEXT PRIMARY KEY, versao INTEGER NOT NULL DEFAULT 1,
        cursor_item INTEGER NOT NULL DEFAULT 0);
      CREATE TABLE IF NOT EXISTS pncp_resultados_estado(
        id INTEGER PRIMARY KEY CHECK(id=1), bootstrap_fim INTEGER NOT NULL DEFAULT 0,
        cursor_data TEXT, cursor_pid TEXT, cursor_item INTEGER NOT NULL DEFAULT 0,
        cursor_ativo INTEGER NOT NULL DEFAULT 0, contratos_examinados INTEGER NOT NULL DEFAULT 0,
        cooldown_ate TEXT, mensagem TEXT, atualizado_em TEXT, ultima_consulta_em TEXT,
        consultas INTEGER NOT NULL DEFAULT 0, resultados_recebidos INTEGER NOT NULL DEFAULT 0);
      INSERT OR IGNORE INTO pncp_resultados_estado(id) VALUES(1);
      CREATE TABLE IF NOT EXISTS pncp_resultados_contagens(estado TEXT PRIMARY KEY,qtd INTEGER NOT NULL DEFAULT 0);
      CREATE TABLE IF NOT EXISTS pncp_resultados_evidencias(
        numero_controle_pncp TEXT NOT NULL,numero_item INTEGER NOT NULL,sequencial_resultado INTEGER NOT NULL,
        payload_json TEXT NOT NULL,consultado_em TEXT NOT NULL,
        PRIMARY KEY(numero_controle_pncp,numero_item,sequencial_resultado));
      CREATE TRIGGER IF NOT EXISTS rpncp_conta_insert AFTER INSERT ON pncp_resultados_fila BEGIN
        INSERT INTO pncp_resultados_contagens VALUES(NEW.estado,1)
          ON CONFLICT(estado) DO UPDATE SET qtd=qtd+1;
      END;
      CREATE TRIGGER IF NOT EXISTS rpncp_conta_update AFTER UPDATE OF estado ON pncp_resultados_fila
        WHEN NEW.estado IS NOT OLD.estado BEGIN
        UPDATE pncp_resultados_contagens SET qtd=qtd-1 WHERE estado=OLD.estado;
        INSERT INTO pncp_resultados_contagens VALUES(NEW.estado,1)
          ON CONFLICT(estado) DO UPDATE SET qtd=qtd+1;
      END;
      CREATE TRIGGER IF NOT EXISTS rpncp_conta_delete AFTER DELETE ON pncp_resultados_fila BEGIN
        UPDATE pncp_resultados_contagens SET qtd=qtd-1 WHERE estado=OLD.estado;
      END;
      CREATE INDEX IF NOT EXISTS idx_rpncp_historico ON contratacoes
        ({DATA_ORDEM} DESC,numero_controle_pncp DESC);
    ''')
    # Eventos por contratação são deduplicados e retomáveis, inclusive depois
    # de uma carga de CSV. Campos iguais não invalidam a confirmação já feita.
    for tabela, evento, condicao in (
        ('contratacoes','INSERT','1'),
        ('contratacoes','UPDATE', 'NEW.data_atualizacao_global IS NOT OLD.data_atualizacao_global OR NEW.data_publicacao_pncp IS NOT OLD.data_publicacao_pncp'),
        ('itens','INSERT','1'),
        ('itens','UPDATE','NEW.tem_resultado IS NOT OLD.tem_resultado OR NEW.data_atualizacao IS NOT OLD.data_atualizacao'),
    ):
        con.executescript(f'''
          CREATE TRIGGER IF NOT EXISTS rpncp_evento_{tabela}_{evento.lower()}
          AFTER {evento} ON {tabela} WHEN {condicao} BEGIN
            INSERT INTO pncp_resultados_eventos(numero_controle_pncp) VALUES(NEW.numero_controle_pncp)
              ON CONFLICT(numero_controle_pncp) DO UPDATE SET versao=versao+1,cursor_item=0;
          END;
        ''')
    con.commit()


def _semear_itens(con, pid, depois=0, limite=500, invalidar=False):
    # Paginação por chave, nunca OFFSET sobre milhões de itens.
    rows = con.execute('''SELECT i.numero_item,i.tem_resultado,i.data_atualizacao,
        c.data_atualizacao_global,c.data_publicacao_pncp,
        (SELECT MAX(r.data_resultado_pncp) FROM resultados_itens r
         WHERE r.numero_controle_pncp=i.numero_controle_pncp AND r.numero_item=i.numero_item) AS data_resultado,
        EXISTS(SELECT 1 FROM pncp_resultados_fila f
         WHERE f.numero_controle_pncp=i.numero_controle_pncp AND f.numero_item=i.numero_item) AS rastreado
        FROM itens i LEFT JOIN contratacoes c ON c.numero_controle_pncp=i.numero_controle_pncp
        WHERE i.numero_controle_pncp=? AND i.numero_item>? ORDER BY i.numero_item LIMIT ?''',
        (pid, depois, limite)).fetchall()
    for row in rows:
        ni, tem, atual, global_, pub, resultado, rastreado = row
        # False conhecido e sem qualquer resultado local: aguarda a alteração
        # sinalizada pela COLETA/itens. Null é desconhecido e será consultado.
        if tem == 0 and not resultado and not rastreado:
            continue
        prioridade = max(str(v or '')[:10] for v in (atual, global_, pub, resultado))
        conflito = ('''DO UPDATE SET estado='PENDENTE',geracao=geracao+1,
          prioridade=MAX(prioridade,excluded.prioridade),retry_em=NULL,erro=NULL,tentativas=0'''
          if invalidar else 'DO NOTHING')
        con.execute('''INSERT INTO pncp_resultados_fila(numero_controle_pncp,numero_item,prioridade)
            VALUES(?,?,?) ON CONFLICT(numero_controle_pncp,numero_item) '''+conflito, (pid,ni,prioridade))
    return (rows[-1][0] if rows else depois), len(rows) < limite


def descobrir(con, blocos=20, parar=lambda:False):
    """Blocos curtos e checkpoint transacional. Eventos novos precedem histórico."""
    for _ in range(blocos):
        if parar(): break
        with con:
            evento = con.execute('SELECT * FROM pncp_resultados_eventos ORDER BY rowid LIMIT 1').fetchone()
            if evento:
                pid, versao, cursor = evento
                cursor, fim = _semear_itens(con,pid,cursor,invalidar=True)
                if fim: con.execute('DELETE FROM pncp_resultados_eventos WHERE numero_controle_pncp=? AND versao=?',(pid,versao))
                else: con.execute('UPDATE pncp_resultados_eventos SET cursor_item=? WHERE numero_controle_pncp=? AND versao=?',(cursor,pid,versao))
                continue
            s = con.execute('SELECT * FROM pncp_resultados_estado WHERE id=1').fetchone()
            if s['bootstrap_fim']: break
            if s['cursor_ativo']:
                pid, data, cursor = s['cursor_pid'], s['cursor_data'], s['cursor_item']
            else:
                where, params = ('',[]) if s['cursor_data'] is None else (
                    f'WHERE ({DATA_ORDEM},numero_controle_pncp)<(?,?)', [s['cursor_data'],s['cursor_pid']])
                pai = con.execute(f'''SELECT numero_controle_pncp,{DATA_ORDEM} FROM contratacoes
                    {where} ORDER BY {DATA_ORDEM} DESC,numero_controle_pncp DESC LIMIT 1''',params).fetchone()
                if not pai:
                    con.execute('UPDATE pncp_resultados_estado SET bootstrap_fim=1,atualizado_em=? WHERE id=1',(agora(),))
                    break
                pid, data = pai; cursor=0
            cursor, fim = _semear_itens(con,pid,cursor)
            con.execute('''UPDATE pncp_resultados_estado SET cursor_pid=?,cursor_data=?,cursor_item=?,
                cursor_ativo=?,contratos_examinados=contratos_examinados+?,atualizado_em=? WHERE id=1''',
                (pid,data,cursor,int(not fim),int(fim),agora()))


class FalhaConsulta(RuntimeError):
    def __init__(self, mensagem, *, global_=False, espera=3600, indisponivel=False):
        super().__init__(mensagem)
        self.global_, self.espera, self.indisponivel = global_, espera, indisponivel


def url_resultados(pid, item):
    # O órgão é sempre o CNPJ completo (14 dígitos). Alguns CNPJs começam
    # com zero, portanto a validação deve ser textual, não numérica.
    m = re.fullmatch(r'(\d{14})-1-(\d{6})/(\d{4})', str(pid or ''))
    if not m or int(item) < 1: raise FalhaConsulta('Identificação PNCP inválida',indisponivel=True,espera=86400)
    cnpj, seq, ano = m.groups()
    return f'{BASE}/orgaos/{cnpj}/compras/{ano}/{int(seq)}/itens/{int(item)}/resultados'


def consultar(sessao,pid,item):
    try:
        resp = sessao.get(url_resultados(pid,item),timeout=(10,30),allow_redirects=False)
    except requests.RequestException as exc:
        raise FalhaConsulta(f'Falha de conexão: {type(exc).__name__}',global_=True,espera=900) from exc
    if resp.status_code == 429:
        from pncp_rate import _retry_after_seconds
        raise FalhaConsulta('PNCP limitou as consultas (HTTP 429).',global_=True,
                            espera=max(900,_retry_after_seconds(resp) or 0))
    if resp.status_code in (401,403) or 'html' in resp.headers.get('Content-Type','').lower():
        raise FalhaConsulta('PNCP exigiu autorização/verificação ou retornou uma página HTML. Coleta suspensa.',global_=True)
    if resp.status_code in (404,410):
        raise FalhaConsulta(f'Item/resultado indisponível no PNCP (HTTP {resp.status_code}); dados preservados.',indisponivel=True,espera=86400)
    if resp.status_code >= 500:
        raise FalhaConsulta(f'PNCP indisponível (HTTP {resp.status_code})',global_=True,espera=900)
    if resp.status_code == 204: return []
    if resp.status_code != 200:
        raise FalhaConsulta(f'Resposta não esperada: HTTP {resp.status_code}',global_=True)
    try: payload = resp.json()
    except ValueError as exc: raise FalhaConsulta('PNCP não retornou JSON válido.',global_=True) from exc
    if isinstance(payload,dict):
        if int(payload.get('totalPaginas') or 1)>1 or int(payload.get('paginasRestantes') or 0)>0:
            raise FalhaConsulta('Resposta paginada não prevista no contrato desta API; confirmação adiada.',global_=True)
        payload = payload.get('listaResultados')
    if not isinstance(payload,list):
        raise FalhaConsulta('Formato de resultados não reconhecido; confirmação adiada.',global_=True)
    return payload


def normalizar(rows,pid,item):
    """Valida todo o item antes de gravar. Guarda também JSON original para auditoria."""
    saida=[]; vistos=set()
    for r in rows:
        if not isinstance(r,dict): raise ValueError('Resultado não é um objeto JSON')
        seq=r.get('sequencialResultado')
        if isinstance(seq,bool) or not isinstance(seq,int) or seq<1 or seq in vistos:
            raise ValueError('Sequencial ausente, inválido ou duplicado')
        vistos.add(seq)
        if r.get('numeroItem',item)!=item or r.get('numeroControlePNCPCompra',pid)!=pid:
            raise ValueError('Resultado pertence a outro item/contratação')
        flat=dict(r,numeroControlePNCP=pid,numeroItem=item)
        for campo in ('dataInclusao','dataAtualizacao','dataCancelamento'):
            if campo in r: flat[campo+'Pncp']=r[campo]
        for obj, col in (('amparoLegalMargemPreferencia','amparoLegalMargemPreferencia'),
                         ('amparoLegalCriterioDesempate','amparoLegalCriterioDesempate'),
                         ('moedaEstrangeira','moedaEstrangeira'),
                         ('paisOrigemProdutoServico','paisOrigemProdutoServico')):
            if isinstance(r.get(obj),dict):
                for sufixo,chave in (('Id','id'),('Nome','nome')): flat[col+sufixo]=r[obj].get(chave)
        reserva=r.get('reservaRemanescenteId')
        flat['reservaRemanescenteId']=reserva.get('codigo') if isinstance(reserva,dict) else reserva
        for campo in ('quantidadeHomologada','valorUnitarioHomologado','valorTotalHomologado','percentualDesconto'):
            v=r.get(campo)
            if v is not None and (isinstance(v,bool) or not isinstance(v,(float,int)) or not math.isfinite(v) or v<0):
                raise ValueError(f'Valor inválido em {campo}')
        for campo in ('dataInclusao','dataAtualizacao','dataCancelamento','dataResultado'):
            v=r.get(campo)
            if v: datetime.fromisoformat(str(v).replace('Z','+00:00'))
        # Campos só presentes em uma fonte não são apagados pela outra.
        resolvido=cr._resolver(flat.keys())
        valores={c:cr._valor(flat,c,resolvido) for c in cr.RESULTADO_MAP if resolvido.get(c)}
        # Cancelamento nulo/vazio é informação explícita na API detalhada.
        saida.append((seq,valores,json.dumps(r,ensure_ascii=False,allow_nan=False)))
    return saida


def aplicar(con,alvo,rows,stamp=None):
    pid,item=alvo['numero_controle_pncp'],alvo['numero_item']
    stamp=stamp or agora()
    registros=normalizar(rows,pid,item)
    with con:
        # Nenhum avanço de checkpoint se a contratação mudou durante a consulta.
        if con.execute('SELECT 1 FROM pncp_resultados_eventos WHERE numero_controle_pncp=?',(pid,)).fetchone(): return False
        atual=con.execute('SELECT geracao FROM pncp_resultados_fila WHERE numero_controle_pncp=? AND numero_item=?',(pid,item)).fetchone()
        if not atual or atual[0]!=alvo['geracao']: return False
        for seq,valores,raw in registros:
            antigo=con.execute('SELECT * FROM resultados_itens WHERE numero_controle_pncp=? AND numero_item=? AND sequencial_resultado=?',(pid,item,seq)).fetchone()
            nova_data=valores.get('data_atualizacao_pncp') or valores.get('data_inclusao_pncp')
            velha_data=antigo['data_atualizacao_pncp'] if antigo else None
            mais_velho=bool(velha_data and (not nova_data or str(nova_data)<str(velha_data)))
            if not mais_velho:
                valores.update(numero_controle_pncp=pid,numero_item=item,sequencial_resultado=seq,
                    fonte_ultima='PNCP_API',importado_em=stamp,periodo_origem=stamp[:10],pncp_ausente=0,
                    pncp_consultado_em=stamp)
                if antigo:
                    editar={k:v for k,v in valores.items() if k not in ('numero_controle_pncp','numero_item','sequencial_resultado')}
                    con.execute('UPDATE resultados_itens SET '+','.join(k+'=?' for k in editar)+
                        ' WHERE numero_controle_pncp=? AND numero_item=? AND sequencial_resultado=?',[*editar.values(),pid,item,seq])
                else:
                    valores['fonte_primeira']='PNCP_API'
                    con.execute('INSERT INTO resultados_itens('+','.join(valores)+') VALUES('+','.join('?' for _ in valores)+')',list(valores.values()))
            con.execute('''INSERT INTO pncp_resultados_evidencias VALUES(?,?,?,?,?)
                ON CONFLICT(numero_controle_pncp,numero_item,sequencial_resultado) DO UPDATE SET
                payload_json=excluded.payload_json,consultado_em=excluded.consultado_em''',(pid,item,seq,raw,stamp))
        # Uma lista vazia NÃO apaga preço nem significa cancelamento.
        # Somente linhas previamente obtidas do PNCP recebem indicação de ausência.
        presentes={r[0] for r in registros}
        for antigo in con.execute("SELECT sequencial_resultado FROM resultados_itens WHERE numero_controle_pncp=? AND numero_item=? AND fonte_ultima='PNCP_API'",(pid,item)).fetchall():
            if antigo[0] not in presentes:
                con.execute('UPDATE resultados_itens SET pncp_ausente=1,pncp_consultado_em=? WHERE numero_controle_pncp=? AND numero_item=? AND sequencial_resultado=?',(stamp,pid,item,antigo[0]))
        estado='CONFIRMADO' if registros else 'SEM_RESULTADO'
        # Sem resultado pode ser atraso na publicação: revisão depois de uma semana.
        retry=None if registros else (datetime.fromisoformat(stamp)+timedelta(days=7)).isoformat(timespec='seconds')
        con.execute('''UPDATE pncp_resultados_fila SET estado=?,consultado_em=?,qtd_resultados=?,
            erro=NULL,tentativas=0,retry_em=? WHERE numero_controle_pncp=? AND numero_item=?''',(estado,stamp,len(registros),retry,pid,item))
        con.execute('''UPDATE pncp_resultados_estado SET consultas=consultas+1,
            resultados_recebidos=resultados_recebidos+?,ultima_consulta_em=?,atualizado_em=? WHERE id=1''',(len(registros),stamp,stamp))
    return True


def falhar(con,alvo,exc):
    n=alvo['tentativas']+1
    espera=exc.espera if isinstance(exc,FalhaConsulta) else min(86400,900*2**min(n-1,6))
    retry=(datetime.now()+timedelta(seconds=espera)).isoformat(timespec='seconds')
    estado='INDISPONIVEL' if isinstance(exc,FalhaConsulta) and exc.indisponivel else 'RETENTAR'
    with con:
        con.execute('''UPDATE pncp_resultados_fila SET estado=?,tentativas=?,retry_em=?,erro=?
            WHERE numero_controle_pncp=? AND numero_item=? AND geracao=?''',
            (estado,n,retry,str(exc)[:1000],alvo['numero_controle_pncp'],alvo['numero_item'],alvo['geracao']))
        if isinstance(exc,FalhaConsulta) and exc.global_:
            con.execute('UPDATE pncp_resultados_estado SET cooldown_ate=?,mensagem=?,atualizado_em=? WHERE id=1',(retry,str(exc)[:1000],agora()))


def resumo(db):
    """Leitura constante para a UI/controlador, inclusive antes da primeira migração."""
    if not Path(db).is_file(): return {'iniciado':False}
    with closing(sqlite3.connect(Path(db).resolve().as_uri()+'?mode=ro',uri=True,timeout=.5)) as con:
        con.row_factory=sqlite3.Row
        if not existe(con,'pncp_resultados_estado'): return {'iniciado':False}
        s=dict(con.execute('SELECT * FROM pncp_resultados_estado WHERE id=1').fetchone())
        s.update(iniciado=True,contagens=dict(con.execute('SELECT estado,qtd FROM pncp_resultados_contagens')),
                 eventos_pendentes=bool(con.execute('SELECT 1 FROM pncp_resultados_eventos LIMIT 1').fetchone()))
        s['proxima_retry']=con.execute('SELECT MIN(retry_em) FROM pncp_resultados_fila WHERE retry_em IS NOT NULL').fetchone()[0]
        return s


def precisa_sessao(s,stamp=None):
    stamp=stamp or agora()
    if not s.get('iniciado'): return True
    if s.get('cooldown_ate') and s['cooldown_ate']>stamp: return False
    return bool(not s['bootstrap_fim'] or s['eventos_pendentes'] or s['contagens'].get('PENDENTE',0)
                or (s.get('proxima_retry') and s['proxima_retry']<=stamp))


def executar(db,*,jobs_db=None,job_id=None,deadline=None,limite=300,sessao=None,intervalo=2.0,parar_extra=lambda:False):
    import job_manager as jm
    pausa=PausaAtualizacao(jobs_db)
    fim=datetime.fromisoformat(deadline) if deadline else datetime.now()+timedelta(minutes=20)
    def parar():
        return pausa() or datetime.now()>=fim or parar_extra() or bool(job_id and jm.parada_solicitada(job_id,jobs_db))
    if parar(): return 79 if pausa() else 0
    con=sqlite3.connect(db,timeout=10);con.row_factory=sqlite3.Row
    con.execute('PRAGMA journal_mode=WAL')
    con.set_progress_handler(lambda:int(parar()),50000)
    propria=sessao is None
    if propria:
        sessao=requests.Session()
        sessao.headers.update({'Accept':'application/json','User-Agent':'RADAR_PNCP/0.7.6 (consulta de resultados PNCP)'})
    feitos=0;ultima=0.0
    try:
        print('Resultados PNCP: preparando fila e descoberta do histórico...',flush=True)
        preparar(con)
        if not precisa_sessao(resumo(db)): return 0
        with con:
            con.execute('UPDATE pncp_resultados_estado SET cooldown_ate=NULL,mensagem=NULL WHERE id=1')
            con.execute("UPDATE pncp_resultados_fila SET estado='PENDENTE',retry_em=NULL WHERE retry_em<=?",(agora(),))
        while feitos<limite and not parar():
            descobrir(con,blocos=20,parar=parar)
            if parar(): break
            alvo=con.execute("SELECT * FROM pncp_resultados_fila WHERE estado='PENDENTE' ORDER BY prioridade DESC,numero_controle_pncp,numero_item LIMIT 1").fetchone()
            if not alvo:
                if con.execute('SELECT bootstrap_fim FROM pncp_resultados_estado').fetchone()[0]: break
                continue
            alvo=dict(alvo)
            from pncp_rate import RateLimiter
            RateLimiter._esperar(max(0,intervalo-(time.monotonic()-ultima)),parar)
            ultima=time.monotonic()
            print(f"Consultando {alvo['numero_controle_pncp']} · item {alvo['numero_item']} · referência {alvo['prioridade']}",flush=True)
            if job_id: jm.atualizar_job(job_id,jobs_db,etapa='Resultados PNCP',progresso_atual=feitos,progresso_total=limite,mensagem=f"Item {alvo['numero_item']} · {alvo['numero_controle_pncp']}")
            try:
                rows=consultar(sessao,alvo['numero_controle_pncp'],alvo['numero_item'])
                # A resposta completa pode ser salva antes da pausa entre itens.
                if not parar(): aplicar(con,alvo,rows)
            except (FalhaConsulta,ValueError,TypeError,OverflowError) as exc:
                if not parar(): falhar(con,alvo,exc)
                print(f'Consulta adiada: {exc}',flush=True)
                if isinstance(exc,FalhaConsulta) and exc.global_: return 75
            feitos+=1
        return 79 if pausa() else 0
    except InterruptedError:
        return 79 if pausa() else 0
    except sqlite3.OperationalError:
        if parar(): return 79 if pausa() else 0
        raise
    finally:
        con.set_progress_handler(None,0)
        con.close()
        if propria: sessao.close()
        print(f'Sessão de resultados encerrada · {feitos} itens tentados. Checkpoints preservados.',flush=True)


def main():
    from radar_paths import DB_COMPRAS, DB_JOBS
    ap=argparse.ArgumentParser()
    ap.add_argument('--db',type=Path,default=DB_COMPRAS)
    ap.add_argument('--jobs-db',type=Path,default=DB_JOBS)
    ap.add_argument('--job-id',type=int)
    ap.add_argument('--deadline')
    a=ap.parse_args()
    return executar(a.db,jobs_db=a.jobs_db,job_id=a.job_id,deadline=a.deadline)


if __name__=='__main__': raise SystemExit(main())
