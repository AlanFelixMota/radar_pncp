"""Evidência por janela da COLETA, sem inferir completude da data máxima da base."""
from datetime import date, datetime, timedelta
from pathlib import Path
import sqlite3
import time


def preparar(con):
    con.executescript('''
      CREATE TABLE IF NOT EXISTS coleta_janelas_pesquisa(
        inicio TEXT NOT NULL, fim TEXT NOT NULL, estado TEXT NOT NULL,
        iniciado_em TEXT NOT NULL, concluido_em TEXT,
        PRIMARY KEY(inicio,fim));
      CREATE TABLE IF NOT EXISTS coleta_alvos_pesquisa(
        inicio TEXT NOT NULL, fim TEXT NOT NULL, numero_controle_pncp TEXT NOT NULL, pai_mudou INTEGER NOT NULL DEFAULT 0, global_mudou INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY(inicio,fim,numero_controle_pncp));
    ''')


def iniciar(con, inicio, fim):
    preparar(con)
    con.execute('''INSERT INTO coleta_janelas_pesquisa VALUES (?,?,'COLETANDO',?,NULL)
      ON CONFLICT(inicio,fim) DO UPDATE SET estado='COLETANDO', iniciado_em=excluded.iniciado_em, concluido_em=NULL''',
      (inicio,fim,datetime.now().isoformat(timespec='seconds')))
    # Alvos são preservados na retomada por checkpoint da mesma janela.
    con.commit()


def registrar(con,inicio,fim,ids,pai_mudou=False,global_mudou=False):
    con.executemany("""INSERT INTO coleta_alvos_pesquisa VALUES (?,?,?,?,?)
      ON CONFLICT(inicio,fim,numero_controle_pncp) DO UPDATE SET
        pai_mudou=MAX(coleta_alvos_pesquisa.pai_mudou,excluded.pai_mudou),
        global_mudou=MAX(coleta_alvos_pesquisa.global_mudou,excluded.global_mudou)""",
      [(inicio,fim,str(pid),int(pai_mudou),int(global_mudou)) for pid in ids if pid])


def concluir(con,inicio,fim,sucesso):
    con.execute('UPDATE coleta_janelas_pesquisa SET estado=?,concluido_em=? WHERE inicio=? AND fim=?',
                ('PNCP_CONCLUIDO' if sucesso else 'PARCIAL',datetime.now().isoformat(timespec='seconds'),inicio,fim))
    con.commit()


def consultar(db,fts,hoje=None):
    import fts_manager as fm
    hoje = hoje or date.today()
    ontem = (hoje-timedelta(days=1)).isoformat()
    out={'referencia':ontem,'indice':fm.status_indice(Path(db),Path(fts)), 'janela':None,
         'verificada':False,'motivo':'Aguardando uma execução COLETA desta versão; o histórico anterior não prova atualização dos itens.'}
    con=sqlite3.connect(Path(db).resolve().as_uri()+'?mode=ro',uri=True,timeout=.5)
    con.row_factory=sqlite3.Row
    limite=time.monotonic()+60
    con.set_progress_handler(lambda:int(time.monotonic()>limite),10000)
    try:
        con.execute('BEGIN')
        if not con.execute("SELECT 1 FROM sqlite_master WHERE name='coleta_janelas_pesquisa'").fetchone(): return out
        row=con.execute('''SELECT * FROM coleta_janelas_pesquisa WHERE inicio<=? AND fim>=?
                           ORDER BY iniciado_em DESC LIMIT 1''',(ontem,ontem)).fetchone()
        if not row: return out
        out['janela']=dict(row)
        rows=con.execute('''SELECT
          COUNT(*) AS alvos,
          SUM(CASE WHEN c.numero_controle_pncp IS NULL THEN 1 ELSE 0 END) AS ausentes,
          SUM(CASE WHEN COALESCE(c.itens_coletados,0)=0 AND COALESCE(c.itens_validacao_comprasgov_status,'')<>'VALIDADO_COMPRASGOV' THEN 1 ELSE 0 END) AS coleta_pendente,
          SUM(CASE WHEN EXISTS(SELECT 1 FROM itens_fila f WHERE f.numero_controle_pncp=a.numero_controle_pncp AND f.origem='REVALIDACAO' AND f.status IN ('PENDENTE','EM_ANDAMENTO','ERRO_TEMPORARIO')) THEN 1 ELSE 0 END) AS revalidacoes,
          SUM(CASE WHEN COALESCE(c.itens_coletados,0)=0 AND c.itens_validacao_comprasgov_status='VALIDADO_COMPRASGOV' THEN 1 ELSE 0 END) AS validados_comprasgov,
          SUM(CASE WHEN c.itens_status IN ('INDISPONIVEL_PNCP','REMOVIDA_PNCP','REVALIDACAO_INDISPONIVEL_PNCP') THEN 1 ELSE 0 END) AS indisponiveis
          FROM coleta_alvos_pesquisa a LEFT JOIN contratacoes c USING(numero_controle_pncp)
          WHERE a.inicio=? AND a.fim=?''',(row['inicio'],row['fim'])).fetchone()
        out['itens']={k:int(rows[k] or 0) for k in rows.keys()}
        recente=(row['concluido_em'] or '')[:10]>=hoje.isoformat()
        from radar_db import indice_fts_pronto
        indice_confirmado = indice_fts_pronto(Path(fts),con)
        out['verificada']=bool(row['estado']=='PNCP_CONCLUIDO' and recente and
               not any(out['itens'][k] for k in ('ausentes','coleta_pendente','revalidacoes','indisponiveis')) and indice_confirmado)
        out['motivo']='Janela da COLETA verificada e índice sincronizado na consulta deste painel.' if out['verificada'] else 'A atualização da janela até ontem ainda tem etapas ou verificações pendentes.'
        out['coleta_recente']=recente
        return out
    finally: con.close()
