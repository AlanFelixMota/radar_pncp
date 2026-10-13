"""Consulta somente leitura dos resultados importados do Compras.gov.

Não depende do FTS de objetos/itens, não cria índices ao abrir a interface e
preserva resultados sem contratação/item local. Cada linha é um resultado,
identificado por contratação + item + sequencial (não um contrato inteiro).
"""
from __future__ import annotations

import re
import sqlite3
import time
import unicodedata
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
import filtros_precos as fp


class ConsultaDemorada(RuntimeError):
    pass


class RefineFornecedor(ValueError):
    pass


@dataclass(frozen=True)
class FiltrosFornecedor:
    fornecedor: str
    data_inicio: str | None = None
    data_fim: str | None = None
    orgao: str = ''
    item: str = ''
    incluir_cancelados: bool = False
    unidade: str = ''
    preco_min: float | None = None
    preco_max: float | None = None


def normalizar(texto) -> str:
    return ''.join(c for c in unicodedata.normalize('NFKD', str(texto or ''))
                   if not unicodedata.combining(c)).casefold()


def _like(texto: str) -> str:
    return '%' + texto.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_') + '%'


def documentos(termo: str) -> tuple[str, str] | None:
    if not re.fullmatch(r'[0-9./\-\s]+', termo):
        return None
    digitos = re.sub(r'\D', '', termo)
    if len(digitos) != 14:
        raise ValueError('Informe o CNPJ completo, com 14 dígitos, ou parte do nome do fornecedor.')
    return digitos, f'{digitos[:2]}.{digitos[2:5]}.{digitos[5:8]}/{digitos[8:12]}-{digitos[12:]}'


def validar(f: FiltrosFornecedor) -> None:
    fp.validar(f)
    termo = f.fornecedor.strip()
    if not termo or (documentos(termo) is None and len(termo) < 3):
        raise ValueError('Informe um CNPJ completo ou pelo menos três caracteres do nome do fornecedor.')
    for d in (f.data_inicio, f.data_fim):
        if d:
            date.fromisoformat(d)
    if f.data_inicio and f.data_fim and f.data_inicio > f.data_fim:
        raise ValueError('A data inicial não pode ser posterior à data final.')


@contextmanager
def leitura(db: Path, max_segundos: float = 8):
    inicio = time.monotonic()
    con = sqlite3.connect(Path(db).resolve().as_uri() + '?mode=ro', uri=True, timeout=1)
    con.row_factory = sqlite3.Row
    con.execute('PRAGMA query_only=ON')
    con.create_function('normalizar_busca', 1, normalizar, deterministic=True)
    fp.registrar(con)
    def esgotado(): return time.monotonic() - inicio >= max_segundos
    con.set_progress_handler(lambda: int(esgotado()), 1000)
    try:
        if esgotado():
            raise ConsultaDemorada('A consulta excedeu o limite de tempo. Refine pelo CNPJ ou pelo período.')
        con.execute('BEGIN')
        yield con
    except sqlite3.OperationalError as exc:
        if esgotado():
            raise ConsultaDemorada('A consulta excedeu o limite de tempo. Refine pelo CNPJ ou pelo período.') from exc
        raise
    finally:
        con.set_progress_handler(None, 0)
        con.close()


def _existe(con, tabela):
    return con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (tabela,)).fetchone() is not None


CANCELADO = """(NULLIF(trim(r.data_cancelamento_pncp),'') IS NOT NULL
    OR NULLIF(trim(r.motivo_cancelamento),'') IS NOT NULL
    OR upper(trim(COALESCE(r.situacao_resultado_nome,''))) IN
       ('CANCELADO','CANCELADA','ANULADO','ANULADA','REVOGADO','REVOGADA',
        'RESULTADO CANCELADO','RESULTADO ANULADO','RESULTADO REVOGADO'))"""


def _condicoes(con, f):
    doc = documentos(f.fornecedor.strip())
    if doc:
        # Igualdade sem funções na coluna utiliza idx_resultados_fornecedor.
        termos = ['r.ni_fornecedor IN (?,?)']; params = list(doc)
    else:
        # Apenas o índice de nomes é percorrido; não lê as linhas largas de
        # resultados. Normalização ocorre por grafia distinta, depois do grupo.
        nomes = [r[0] for r in con.execute("""
            SELECT nome_fornecedor FROM resultados_itens
            GROUP BY nome_fornecedor
            HAVING normalizar_busca(MIN(nome_fornecedor)) LIKE ? ESCAPE '\\'
            LIMIT 101
        """, (_like(normalizar(f.fornecedor.strip())),))]
        if len(nomes) > 100:
            raise RefineFornecedor('Mais de 100 grafias de fornecedores correspondem ao nome. Informe um nome mais específico ou o CNPJ.')
        if not nomes:
            return '0', []
        termos = ['r.nome_fornecedor IN (' + ','.join('?' for _ in nomes) + ')']; params = nomes
    if f.data_inicio:
        termos.append('r.data_resultado_pncp >= ?');params.append(f.data_inicio)
    if f.data_fim:
        termos.append('r.data_resultado_pncp < ?');params.append((date.fromisoformat(f.data_fim)+timedelta(days=1)).isoformat())
    if f.orgao.strip():
        termos.append("(normalizar_busca(c.orgao_razao_social) LIKE ? ESCAPE '\\' OR normalizar_busca(c.unidade_nome) LIKE ? ESCAPE '\\' OR c.orgao_cnpj IN (?,?))")
        params.extend([_like(normalizar(f.orgao.strip()))]*2 + [f.orgao.strip(), re.sub(r'\D','',f.orgao)])
    if f.item.strip():
        termos.append("normalizar_busca(COALESCE(i.descricao,'') || ' ' || COALESCE(i.informacao_complementar,'')) LIKE ? ESCAPE '\\'")
        params.append(_like(normalizar(f.item.strip())))
    if not f.incluir_cancelados:
        termos.append('NOT ' + CANCELADO)
    where,p=fp.clausula(f,'i.unidade_medida','r.valor_unitario_homologado')
    termos.append(where);params.extend(p)
    return ' AND '.join(termos), params


FROM_RESULTADOS = """
FROM resultados_itens r
LEFT JOIN contratacoes c ON c.numero_controle_pncp=r.numero_controle_pncp
LEFT JOIN itens i ON i.numero_controle_pncp=r.numero_controle_pncp AND i.numero_item=r.numero_item
"""

SELECT_RESULTADOS = f"""
SELECT r.numero_controle_pncp, r.numero_item, r.sequencial_resultado,
       r.ni_fornecedor, r.nome_fornecedor, r.tipo_pessoa,
       r.quantidade_homologada, r.valor_unitario_homologado, r.valor_total_homologado,
       r.situacao_resultado_nome, r.data_resultado_pncp, r.data_atualizacao_pncp,
       r.data_cancelamento_pncp, r.motivo_cancelamento, r.fonte_ultima, r.importado_em,
       {CANCELADO} AS cancelado,
       c.numero_controle_pncp IS NOT NULL AS contratacao_local,
       i.numero_controle_pncp IS NOT NULL AS item_local,
       c.orgao_razao_social, c.orgao_cnpj, c.unidade_nome, c.uf_sigla, c.objeto, c.url_pncp,
       c.processo, c.numero_compra, c.ano_compra, c.link_sistema_origem,
       c.data_publicacao_pncp, i.descricao, i.informacao_complementar, i.unidade_medida
""" + FROM_RESULTADOS


def buscar_agrupado(db: Path, filtros: FiltrosFornecedor, pagina: int = 1,
                    por_pagina: int = 20, *, max_segundos: float = 8) -> dict:
    """Pagina contratações, preservando apenas os resultados que satisfazem os filtros.

    Até 20 resultados por contratação são carregados inicialmente. A navegação
    interna usa por_contratacao com os mesmos filtros e não divide o cartão.
    Contagens e prévias pertencem ao mesmo snapshot de leitura.
    """
    validar(filtros)
    pagina = max(1, int(pagina)); por_pagina = min(50, max(1, int(por_pagina)))
    with leitura(db, max_segundos) as con:
        if not _existe(con, 'resultados_itens'):
            return {'disponivel': False, 'contratacoes': [], 'tem_mais': False,
                    'pagina': pagina, 'por_pagina': por_pagina}
        where, params = _condicoes(con, filtros)
        sql = '''SELECT r.numero_controle_pncp, COUNT(*) AS total_resultados,
                        COUNT(DISTINCT r.numero_item) AS total_itens,
                        MAX(r.data_resultado_pncp) AS ultimo_resultado
                 ''' + FROM_RESULTADOS + ' WHERE ' + where + '''
                 GROUP BY r.numero_controle_pncp
                 ORDER BY ultimo_resultado DESC, r.numero_controle_pncp
                 LIMIT ? OFFSET ?'''
        grupos = [dict(r) for r in con.execute(sql, [*params, por_pagina+1, (pagina-1)*por_pagina])]
        for grupo in grupos[:por_pagina]:
            grupo['previa'] = _pagina(con, '('+where+') AND r.numero_controle_pncp=?',
                                     [*params, grupo['numero_controle_pncp']], 1, 20,
                                     incluir_sync=False)
        return {'disponivel': True, 'contratacoes': grupos[:por_pagina],
                'tem_mais': len(grupos)>por_pagina, 'pagina': pagina,
                'por_pagina': por_pagina, 'sync': _sync(con),
                'consultado_em': datetime.now().isoformat(timespec='seconds')}


def buscar(db: Path, filtros: FiltrosFornecedor, pagina: int = 1, por_pagina: int = 20,
           *, max_segundos: float = 8) -> dict:
    validar(filtros)
    pagina = max(1, int(pagina));por_pagina = min(50, max(1, int(por_pagina)))
    with leitura(db,max_segundos) as con:
        if not _existe(con,'resultados_itens'):
            return {'disponivel':False,'resultados':[],'tem_mais':False,'pagina':pagina,'por_pagina':por_pagina}
        where, params = _condicoes(con,filtros)
        return _pagina(con,where,params,pagina,por_pagina)


def _sync(con):
    if _existe(con, 'comprasgov_resultados_sync'):
        row = con.execute('SELECT * FROM comprasgov_resultados_sync WHERE id=1').fetchone()
        if row:
            return dict(row)
    return {}


def _pagina(con, where, params, pagina, por_pagina, *, incluir_sync=True):
    sql = SELECT_RESULTADOS + ' WHERE ' + where + '''
        ORDER BY r.data_resultado_pncp DESC, r.numero_controle_pncp, r.numero_item, r.sequencial_resultado
        LIMIT ? OFFSET ?'''
    rows = [dict(r) for r in con.execute(sql,[*params,por_pagina+1,(pagina-1)*por_pagina])]
    return {'disponivel':True,'resultados':rows[:por_pagina], 'tem_mais':len(rows)>por_pagina,
            'pagina':pagina,'por_pagina':por_pagina,'consultado_em':datetime.now().isoformat(timespec='seconds'),
            'sync':_sync(con) if incluir_sync else {}}


def por_contratacao(db: Path, numero_controle: str, pagina: int = 1, por_pagina: int = 20,
                    *, max_segundos: float = 8, filtros: FiltrosFornecedor | None = None) -> dict:
    pagina=max(1,int(pagina));por_pagina=min(50,max(1,int(por_pagina)))
    if filtros is not None:
        validar(filtros)
    with leitura(db,max_segundos) as con:
        if not _existe(con,'resultados_itens'):
            return {'disponivel':False,'resultados':[],'tem_mais':False,'pagina':pagina,'por_pagina':por_pagina}
        where, params = _condicoes(con, filtros) if filtros is not None else ('1', [])
        return _pagina(con,'('+where+') AND r.numero_controle_pncp=?',
                       [*params,numero_controle],pagina,por_pagina)
