"""Termos e filtros persistentes, separados das referências e cestas."""
from dataclasses import asdict
from datetime import date, timedelta
from pathlib import Path
import json
import sqlite3
import filtros_precos as fp

import cestas
from radar_db import FiltrosBusca, UFS_VALIDAS
from pesquisa_fornecedores import FiltrosFornecedor, validar as validar_fornecedor

TIPOS=('contratacoes','fornecedores')
PERIODOS=('original','ultimos_dias','todo_periodo')


def preparar(db):
    cestas.preparar(db)
    with cestas.conectar(db) as con:
        con.execute('''CREATE TABLE IF NOT EXISTS buscas_salvas(
            id INTEGER PRIMARY KEY, nome TEXT NOT NULL, nome_chave TEXT NOT NULL,
            tipo TEXT NOT NULL, filtros_json TEXT NOT NULL, periodo TEXT NOT NULL,
            dias INTEGER NOT NULL DEFAULT 30, incluir_hoje INTEGER NOT NULL DEFAULT 0,
            criado_em TEXT NOT NULL, atualizado_em TEXT NOT NULL, ultimo_uso_em TEXT,
            arquivada INTEGER NOT NULL DEFAULT 0, revisao INTEGER NOT NULL DEFAULT 1,
            UNIQUE(tipo,nome_chave))''')


def listar(db,arquivadas=False):
    if not Path(db).exists():return []
    with cestas.conectar(db) as con:
        if not con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='buscas_salvas'").fetchone():return []
        return [dict(r) for r in con.execute('SELECT * FROM buscas_salvas WHERE (? OR arquivada=0) ORDER BY atualizado_em DESC,id DESC',(int(arquivadas),))]


def _normalizar(tipo,filtros,por_pagina):
    if tipo not in TIPOS:raise ValueError('Tipo de pesquisa inválido.')
    if not isinstance(filtros,dict):raise ValueError('Filtros inválidos.')
    tamanhos=(10,20,30,50) if tipo=='contratacoes' else (10,20,50)
    if type(por_pagina) is not int or por_pagina not in tamanhos:raise ValueError('Tamanho de página inválido.')
    try:
        if tipo=='contratacoes':
            f=FiltrosBusca(**filtros)
            fp.validar(f)
            if f.modo_texto not in ('todas','frase','qualquer') or f.status_itens not in ('todos','ok','indisponivel','pendente') or f.tic_status not in ('todos','tic','nao_tic','nao_classificado'):
                raise ValueError('Opção de filtro inválida.')
            if type(f.somente_homologadas) is not bool:raise ValueError('Filtro de homologação inválido.')
            for campo in ('modalidades','ufs','tic_categorias'):
                lista=getattr(f,campo)
                if not isinstance(lista,(list,tuple)) or len(lista)>200 or any(not isinstance(v,str) or not v.strip() or len(v)>200 for v in lista):
                    raise ValueError('Lista de filtros inválida.')
            if any(v not in UFS_VALIDAS for v in f.ufs):raise ValueError('UF inválida.')
        else:
            f=FiltrosFornecedor(**filtros)
            if type(f.incluir_cancelados) is not bool:raise ValueError('Filtro de cancelamento inválido.')
            validar_fornecedor(f)
        campos_texto=('texto','orgao') if tipo=='contratacoes' else ('fornecedor','orgao','item')
        for campo in campos_texto:
            v=getattr(f,campo)
            if not isinstance(v,str) or len(v)>4000:raise ValueError('Texto do filtro inválido ou muito longo.')
        for valor in (f.data_inicio,f.data_fim):
            if valor is not None and (not isinstance(valor,str) or date.fromisoformat(valor).isoformat()!=valor):
                raise ValueError('Data de filtro inválida. Use AAAA-MM-DD.')
        if f.data_inicio and f.data_fim and f.data_inicio>f.data_fim:raise ValueError('A data inicial não pode ser posterior à final.')
    except (TypeError,AttributeError) as exc:
        raise ValueError('A configuração da pesquisa está inválida ou é incompatível com esta versão.') from exc
    return {'versao':1,'filtros':asdict(f),'por_pagina':por_pagina}


def salvar(db,nome,tipo,filtros,por_pagina,*,periodo='original',dias=30,incluir_hoje=False,busca_id=None,revisao=None):
    nome=cestas.texto(nome,160,'o nome da busca',True)
    if periodo not in PERIODOS:raise ValueError('Período inválido.')
    if type(dias) is not int or not 1<=dias<=3650:raise ValueError('Escolha de 1 a 3.650 dias.')
    if type(incluir_hoje) is not bool:raise ValueError('Opção de período inválida.')
    dados=_normalizar(tipo,filtros,por_pagina)
    raw=json.dumps(dados,ensure_ascii=False,allow_nan=False);stamp=cestas.agora();chave=' '.join(nome.split()).casefold()
    preparar(db)
    try:
        with cestas.conectar(db) as con:
            con.execute('BEGIN IMMEDIATE')
            if busca_id is None:
                from identificadores import proximo
                ident=proximo(con,'buscas_salvas')
                con.execute('''INSERT INTO buscas_salvas(id,nome,nome_chave,tipo,filtros_json,periodo,dias,incluir_hoje,criado_em,atualizado_em)
                    VALUES(?,?,?,?,?,?,?,?,?,?)''',(ident,nome,chave,tipo,raw,periodo,dias,int(incluir_hoje),stamp,stamp))
                return ident
            cur=con.execute('''UPDATE buscas_salvas SET nome=?,nome_chave=?,filtros_json=?,periodo=?,dias=?,incluir_hoje=?,atualizado_em=?,revisao=revisao+1
                WHERE id=? AND tipo=? AND revisao=? AND arquivada=0''',(nome,chave,raw,periodo,dias,int(incluir_hoje),stamp,busca_id,tipo,revisao))
            if not cur.rowcount:raise ValueError('A busca foi alterada ou arquivada. Atualize a lista antes de salvar.')
            return busca_id
    except sqlite3.IntegrityError as exc:
        raise ValueError('Já existe uma busca desse tipo com esse nome, inclusive entre as arquivadas. Escolha outro nome ou atualize a busca existente.') from exc


def obter(db,busca_id):
    with cestas.conectar(db) as con:
        row=con.execute('SELECT * FROM buscas_salvas WHERE id=?',(busca_id,)).fetchone()
        if row is None:raise ValueError('Busca salva não encontrada. Atualize a lista.')
        return dict(row)


def resolver(busca,*,hoje=None):
    if busca.get('arquivada'):raise ValueError('Reabra a busca arquivada antes de executar.')
    try:
        dados=json.loads(busca['filtros_json'])
        if dados.get('versao')!=1:raise ValueError('A busca salva requer outra versão do Radar PNCP.')
        dados=_normalizar(busca['tipo'],dados['filtros'],dados['por_pagina'])
        f=dados['filtros'];modo=busca['periodo']
        if modo=='ultimos_dias':
            dias=busca['dias']
            if type(dias) is not int or not 1<=dias<=3650:raise ValueError('Janela de dias inválida.')
            fim=(hoje or date.today())-timedelta(days=0 if busca['incluir_hoje'] else 1)
            f.update(data_inicio=(fim-timedelta(days=dias-1)).isoformat(),data_fim=fim.isoformat())
        elif modo=='todo_periodo':f.update(data_inicio=None,data_fim=None)
        elif modo!='original':raise ValueError('Período salvo inválido.')
        return {'id':busca['id'],'nome':busca['nome'],'tipo':busca['tipo'],**dados}
    except (KeyError,TypeError,AttributeError,json.JSONDecodeError) as exc:
        raise ValueError('Não foi possível interpretar os filtros desta busca salva.') from exc


def alterar_dados(db,busca_id,*,nome,arquivada,revisao):
    if type(arquivada) is not bool:raise ValueError('Opção de arquivamento inválida.')
    nome=cestas.texto(nome,160,'o nome da busca',True);chave=' '.join(nome.split()).casefold()
    try:
        with cestas.conectar(db) as con:
            cur=con.execute('UPDATE buscas_salvas SET nome=?,nome_chave=?,arquivada=?,atualizado_em=?,revisao=revisao+1 WHERE id=? AND revisao=?',
                            (nome,chave,int(bool(arquivada)),cestas.agora(),busca_id,revisao))
            if not cur.rowcount:raise ValueError('A busca foi alterada em outra aba. Atualize a lista antes de salvar.')
    except sqlite3.IntegrityError as exc:raise ValueError('Já existe uma busca desse tipo com esse nome.') from exc


def registrar_uso(db,busca_id):
    with cestas.conectar(db) as con:
        con.execute('UPDATE buscas_salvas SET ultimo_uso_em=? WHERE id=?',(cestas.agora(),busca_id))
