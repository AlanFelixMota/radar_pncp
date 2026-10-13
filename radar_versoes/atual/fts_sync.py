"""FTS v2: diário transacional de mudanças e reconstrução retomável.

O diário só é limpo depois do checkpoint persistido no índice publicado.
O arquivo .building guarda a reconstrução inicial e nunca é usado na busca.
"""
from __future__ import annotations
import os
import sqlite3
import time
import uuid
from datetime import datetime
from pathlib import Path

CAMPOS = {
    'contratacoes': ('numero_controle_pncp','objeto','informacao_complementar','orgao_razao_social','unidade_nome','processo','numero_compra'),
    'itens': ('numero_controle_pncp','numero_item','descricao','informacao_complementar','ncm_nbs_codigo','ncm_nbs_descricao','catalogo_codigo_item','catalogo_nome','categoria_catalogo_nome','material_ou_servico_nome'),
}

class PausaIndice(RuntimeError):
    """Orçamento de tempo atingido; checkpoint preservado."""


def _ocupado(exc):
    code = getattr(exc, 'sqlite_errorcode', 0) or 0
    return (code & 255) in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED) or 'locked' in str(exc).lower()


def _rastreamento_instalado(con):
    esperados = {'fts_fonte', 'fts_alteracoes'} | {
        f'fts_v2_{t}_{op}' for t in CAMPOS for op in ('insert', 'delete', 'update')
    }
    nomes = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type IN ('table','trigger')")}
    return esperados <= nomes and con.execute('SELECT token FROM fts_fonte WHERE id=1').fetchone() is not None


def instalar(con):
    # Não executado em leituras da interface. Triggers permanecem no banco e
    # cobrem também outros importadores que gravem nas mesmas tabelas.
    if _rastreamento_instalado(con):
        return
    con.execute('CREATE TABLE IF NOT EXISTS fts_fonte(id INTEGER PRIMARY KEY CHECK(id=1), token TEXT NOT NULL)')
    con.execute('INSERT OR IGNORE INTO fts_fonte VALUES (1,?)',(uuid.uuid4().hex,))
    con.execute('CREATE TABLE IF NOT EXISTS fts_alteracoes(seq INTEGER PRIMARY KEY AUTOINCREMENT, tabela TEXT NOT NULL, registro INTEGER NOT NULL)')
    for tabela, campos in CAMPOS.items():
        if not con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(tabela,)).fetchone():
            raise RuntimeError(f'Tabela ausente para rastreamento FTS: {tabela}')
        for op, ref in [('INSERT','NEW'),('DELETE','OLD')]:
            con.execute(f'''CREATE TRIGGER IF NOT EXISTS fts_v2_{tabela}_{op.lower()}
              AFTER {op} ON {tabela} BEGIN
              INSERT INTO fts_alteracoes(tabela,registro) VALUES ('{tabela}',{ref}.rowid);
              END''')
        cond = ' OR '.join(f'OLD.{c} IS NOT NEW.{c}' for c in campos)
        con.execute(f'''CREATE TRIGGER IF NOT EXISTS fts_v2_{tabela}_update
          AFTER UPDATE ON {tabela} WHEN OLD.rowid IS NOT NEW.rowid OR {cond}
          BEGIN
          INSERT INTO fts_alteracoes(tabela,registro) VALUES ('{tabela}',OLD.rowid);
          INSERT INTO fts_alteracoes(tabela,registro) SELECT '{tabela}',NEW.rowid WHERE OLD.rowid IS NOT NEW.rowid;
          END''')
    con.commit()


def fronteira(con):
    row = con.execute("SELECT seq FROM sqlite_sequence WHERE name='fts_alteracoes'").fetchone()
    return int(row[0]) if row else 0


def _meta(con):
    import fts_manager as fm
    return fm._meta(con)


def _abrir(path):
    con = sqlite3.connect(str(path), timeout=2)
    con.row_factory = sqlite3.Row
    return con


def status(db, indice):
    import fts_manager as fm
    out = dict(fts5_disponivel=fm.fts5_disponivel(), existe=Path(indice).exists(), estado='NAO_CONSTRUIDO',
               mensagem='Índice ainda não construído.', pronto_para_busca=False, atualizado_em=None,
               construido_em=None, contratacoes_indexadas=0, itens_indexados=0,
               last_contratacao_rowid=0,last_item_rowid=0,source_max_contratacao_rowid=0,source_max_item_rowid=0,
               tamanho_bytes=Path(indice).stat().st_size if Path(indice).exists() else 0)
    if not out['fts5_disponivel'] or not Path(db).exists():
        return out
    src = idx = None
    arquivo_verificado = db
    try:
        src = sqlite3.connect(Path(db).resolve().as_uri()+'?mode=ro',uri=True,timeout=.2)
        fonte = src.execute("SELECT token FROM fts_fonte WHERE id=1").fetchone()
        ultimo = fronteira(src)
        out['versao_base'] = ultimo
        if not out['existe']:
            return out
        arquivo_verificado = indice
        idx = sqlite3.connect(Path(indice).resolve().as_uri()+'?mode=ro',uri=True,timeout=.2)
        m = _meta(idx)
        for k in ('atualizado_em','construido_em'):
            out[k] = m.get(k)
        for k in ('contratacoes_indexadas','itens_indexados','last_contratacao_rowid','last_item_rowid'):
            out[k] = int(m.get(k,0) or 0)
        if m.get('schema_version') != '2' or not fonte or m.get('fonte_id') != fonte[0]:
            out.update(estado='RECONSTRUCAO_NECESSARIA',mensagem='Índice anterior ao rastreamento de alterações; reconstrução inicial necessária.')
        elif m.get('estado') != 'READY':
            out.update(estado='INCOMPLETO',mensagem='Sincronização pendente; progresso preservado para continuação.')
        elif int(m.get('seq_aplicada',-1)) != ultimo:
            out.update(estado='DESATUALIZADO',mensagem='Há inserções, alterações ou exclusões aguardando sincronização.')
        else:
            out.update(estado='ATUALIZADO',mensagem='Índice sincronizado com as alterações registradas na base local.',pronto_para_busca=True)
        out['versao_indice'] = int(m.get('seq_aplicada',-1))
        return out
    except sqlite3.Error as exc:
        out['erro_verificacao'] = str(exc)
        out['arquivo_verificacao'] = str(arquivo_verificado)
        if 'no such table' in str(exc):
            out.update(estado='RECONSTRUCAO_NECESSARIA',mensagem='Aguardando reconstrução inicial com rastreamento de alterações.')
        else:
            out.update(estado='VERIFICACAO_PENDENTE',mensagem='Não foi possível confirmar a sincronização do índice neste momento.')
        return out
    finally:
        if idx: idx.close()
        if src: src.close()


def _dados(tabela, row):
    import fts_manager as fm
    if tabela == 'contratacoes':
        return (row['rowid'], row['numero_controle_pncp'], fm._texto_partes(*(row[c] for c in CAMPOS[tabela])))
    return (row['rowid'], row['numero_controle_pncp'], row['numero_item'],
            fm._texto_partes(*(row[c] for c in CAMPOS[tabela][2:])))


def _inserir(idx, tabela, rows):
    if tabela == 'contratacoes':
        sql = 'INSERT OR REPLACE INTO contratacoes_fts(rowid,numero_controle_pncp,texto) VALUES (?,?,?)'
    else:
        sql = 'INSERT OR REPLACE INTO itens_fts(rowid,numero_controle_pncp,numero_item,texto) VALUES (?,?,?,?)'
    idx.executemany(sql,[_dados(tabela,r) for r in rows])


def sincronizar(db, indice, modo='update', progress_callback=None, stop_callback=None, lote=2000,
                max_segundos=None):
    inicio = time.monotonic()
    etapa = ['Preparação', str(Path(indice).resolve())]
    primeira_espera = None
    def marcar(nome, arquivo):
        etapa[:] = [nome, str(arquivo)]
        print(f'[FTS5] {nome} · arquivo: {arquivo}', flush=True)
    while True:
        if stop_callback and stop_callback():
            raise InterruptedError('Parada solicitada; checkpoint preservado.')
        restante = None if max_segundos is None else max_segundos - (time.monotonic()-inicio)
        if restante is not None and restante <= 0:
            raise PausaIndice('Tempo da sessão esgotado; checkpoint preservado.')
        try:
            return _sincronizar_uma_vez(db, indice, modo, progress_callback, stop_callback,
                                       lote, restante, marcar)
        except sqlite3.OperationalError as exc:
            if not _ocupado(exc):
                raise RuntimeError(f'{etapa[0]} · {etapa[1]}: {exc}') from exc
            if primeira_espera is None:
                primeira_espera = time.monotonic()
            if time.monotonic()-primeira_espera >= 60:
                raise PausaIndice(f'Arquivo ocupado: {etapa[1]} · etapa: {etapa[0]}. '
                                 'Checkpoint preservado; tente novamente após encerrar a operação que usa o arquivo.') from exc
            print(f'[FTS5] Aguardando liberação · etapa: {etapa[0]} · arquivo: {etapa[1]}', flush=True)
            # Esperas curtas mantêm a parada cooperativa e o limite da sessão.
            for _ in range(5):
                if stop_callback and stop_callback():
                    raise InterruptedError('Parada solicitada; checkpoint preservado.')
                if max_segundos is not None and time.monotonic()-inicio >= max_segundos:
                    raise PausaIndice(f'Tempo esgotado aguardando {etapa[1]}; checkpoint preservado.')
                time.sleep(.1)


def _sincronizar_uma_vez(db, indice, modo, progress_callback, stop_callback, lote,
                       max_segundos, marcar):
    import fts_manager as fm
    if modo not in ('build','update'): raise ValueError('Modo inválido para índice FTS.')
    db, indice = Path(db).resolve(),Path(indice).resolve()
    indice.parent.mkdir(parents=True,exist_ok=True)
    inicio = time.monotonic()
    def verificar():
        if stop_callback and stop_callback(): raise InterruptedError('Parada solicitada; checkpoint preservado.')
        if max_segundos is not None and time.monotonic()-inicio >= max_segundos:
            raise PausaIndice('Tempo da sessão esgotado; próxima sessão continuará do checkpoint.')
    # Lock entre processos, automaticamente liberado em caso de encerramento.
    lock = sqlite3.connect(str(indice)+'.sync-lock',timeout=.2)
    src = idx = None
    try:
        marcar('Reservar sincronização', str(indice)+'.sync-lock')
        lock.execute('BEGIN EXCLUSIVE')
        src = _abrir(db)
        marcar('Verificar rastreamento da base', db)
        instalar(src)
        token = src.execute('SELECT token FROM fts_fonte WHERE id=1').fetchone()[0]
        st = status(db,indice)
        if st['estado'] == 'VERIFICACAO_PENDENTE':
            erro = sqlite3.OperationalError(st.get('erro_verificacao', 'Falha ao verificar índice'))
            marcar('Verificar sincronização do índice', st.get('arquivo_verificacao', indice))
            if _ocupado(erro):
                raise erro
            raise RuntimeError(f'Não foi possível verificar {indice}: {erro}')
        temp = indice.with_suffix(indice.suffix+'.building')
        baseline = modo=='build' or st['estado'] in ('NAO_CONSTRUIDO','RECONSTRUCAO_NECESSARIA') or temp.exists()
        target = temp if baseline else indice
        marcar('Abrir índice de trabalho', target)
        idx = _abrir(target)
        idx.execute('PRAGMA journal_mode=DELETE')
        fm.criar_schema(idx)
        m = _meta(idx)
        if baseline and (m.get('schema_version')!='2' or m.get('fonte_id')!=token):
            # Começa do banco atual; eventos posteriores ao início corrigem toda
            # mudança concorrente, inclusive rowid reaproveitado durante a leitura.
            src.execute('BEGIN')
            seq = fronteira(src)
            maximos = {t:int(src.execute(f'SELECT COALESCE(MAX(rowid),0) FROM {t}').fetchone()[0]) for t in CAMPOS}
            src.rollback()
            idx.execute('DELETE FROM contratacoes_fts');idx.execute('DELETE FROM itens_fts');idx.execute('DELETE FROM meta')
            fm.set_meta(idx,schema_version='2',fonte_id=token,estado='BUILDING',seq_aplicada=seq,
                        last_contratacao_rowid=0,last_item_rowid=0,contratacoes_indexadas=0,itens_indexados=0,
                        max_contratacoes=maximos['contratacoes'],max_itens=maximos['itens'],construido_em=fm._agora())
            idx.commit();m=_meta(idx)
        if baseline:
            for tabela,chave in [('contratacoes','contratacao'),('itens','item')]:
                marcar('Reconstrução · '+tabela, target)
                last=int(m.get(f'last_{chave}_rowid',0)); teto=int(m.get(f'max_{tabela}',0))
                cont=int(m.get(f'{tabela}_indexadas' if tabela=='contratacoes' else 'itens_indexados',0))
                ck='contratacoes_indexadas' if tabela=='contratacoes' else 'itens_indexados'
                while last<teto:
                    verificar()
                    rows=src.execute(f"SELECT rowid,{','.join(CAMPOS[tabela])} FROM {tabela} WHERE rowid>? AND rowid<=? ORDER BY rowid LIMIT ?",(last,teto,lote)).fetchall()
                    if not rows: break
                    _inserir(idx,tabela,rows);last=rows[-1]['rowid'];cont+=len(rows)
                    fm.set_meta(idx,**{f'last_{chave}_rowid':last,ck:cont});idx.commit()
                    if progress_callback: progress_callback(f'Reconstrução · {tabela}',last,teto)
        # Catch-up com visão consistente por lote. O checkpoint é confirmado junto
        # com o conteúdo FTS; uma falha antes disso permite repetir o lote inteiro.
        src.execute('BEGIN'); limite=fronteira(src); src.rollback()
        atual_meta = _meta(idx)
        aplicada=int(atual_meta.get('seq_aplicada',0))
        totais={'contratacoes':int(atual_meta.get('contratacoes_indexadas',0)), 'itens':int(atual_meta.get('itens_indexados',0))}
        while aplicada<limite:
            if aplicada == int(atual_meta.get('seq_aplicada',0)):
                marcar('Aplicar alterações e exclusões', target)
            verificar();src.execute('BEGIN')
            eventos=src.execute('SELECT seq,tabela,registro FROM fts_alteracoes WHERE seq>? AND seq<=? ORDER BY seq LIMIT ?',(aplicada,limite,lote)).fetchall()
            if not eventos:
                src.rollback();raise RuntimeError('Diário incompleto: reconstrução do índice necessária.')
            try:
                for tabela,rid in sorted({(r['tabela'],r['registro']) for r in eventos}):
                    existia = idx.execute(f'SELECT 1 FROM {tabela}_fts WHERE rowid=?',(rid,)).fetchone() is not None
                    idx.execute(f'DELETE FROM {tabela}_fts WHERE rowid=?',(rid,))
                    totais[tabela] -= int(existia)
                    row=src.execute(f"SELECT rowid,{','.join(CAMPOS[tabela])} FROM {tabela} WHERE rowid=?",(rid,)).fetchone()
                    if row:
                        _inserir(idx,tabela,[row]); totais[tabela] += 1
                aplicada=eventos[-1]['seq']
                fm.set_meta(idx,seq_aplicada=aplicada,estado='SYNCING',atualizado_em=fm._agora(),
                            contratacoes_indexadas=totais['contratacoes'],itens_indexados=totais['itens']);idx.commit()
            finally:
                src.rollback()
            if progress_callback: progress_callback('Alterações e exclusões',aplicada,limite)
        # Totais mantidos junto ao checkpoint; não reconta milhões de documentos.
        marcar('Confirmar checkpoint do índice', target)
        fm.set_meta(idx,estado='READY',atualizado_em=fm._agora(),schema_version='2',fonte_id=token,
                    contratacoes_indexadas=totais['contratacoes'],itens_indexados=totais['itens'])
        idx.commit();idx.close();idx=None
        if baseline:
            marcar('Publicar reconstrução', indice)
            # O destino antigo pode estar em WAL. Nunca remover sidecars de um
            # banco em uso. O lock exclusivo/checkpoint deve preceder a troca.
            if indice.exists():
                old=sqlite3.connect(str(indice),timeout=1)
                try:
                    resultado=old.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()
                    if resultado and resultado[0]: raise RuntimeError('Índice em uso; publicação será retomada.')
                    old.execute('PRAGMA journal_mode=DELETE')
                finally: old.close()
            os.replace(temp,indice)
        # A limpeza é manutenção, não condição para usar o checkpoint publicado.
        # Nunca apaga eventos ainda não aplicados; uma leitura/coleta longa pode
        # adiá-la sem transformar uma sincronização concluída em erro.
        marcar('Limpar eventos já aplicados', db)
        try:
            src.execute('DELETE FROM fts_alteracoes WHERE seq<=?',(aplicada,));src.commit()
        except sqlite3.OperationalError as exc:
            src.rollback()
            if not _ocupado(exc):
                raise
            print('[FTS5] Índice confirmado. Limpeza do diário adiada: base ocupada; eventos preservados.', flush=True)
        return {'contratacoes_indexadas':totais['contratacoes'],'itens_indexados':totais['itens'],
                'last_contratacao_rowid':0,'last_item_rowid':0,'seq_aplicada':aplicada}
    finally:
        if idx: idx.close()
        if src: src.close()
        lock.close()
