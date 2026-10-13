"""Evita reutilizar identidades de cestas, buscas e acompanhamentos excluídos."""
TABELAS={'pesquisas','buscas_salvas','monitoramento_acompanhamentos'}


def registrar(con,tabela,ident):
    if tabela not in TABELAS:raise ValueError('Tipo de identidade inválido.')
    con.execute('CREATE TABLE IF NOT EXISTS radar_pncp_identificadores(tipo TEXT PRIMARY KEY,ultimo_id INTEGER NOT NULL)')
    con.execute('''INSERT INTO radar_pncp_identificadores VALUES(?,?)
        ON CONFLICT(tipo) DO UPDATE SET ultimo_id=MAX(ultimo_id,excluded.ultimo_id)''',(tabela,ident))


def proximo(con,tabela):
    # O chamador mantém BEGIN IMMEDIATE para reservar e inserir atomicamente.
    if tabela not in TABELAS:raise ValueError('Tipo de identidade inválido.')
    atual=con.execute(f'SELECT COALESCE(MAX(id),0) FROM {tabela}').fetchone()[0]
    registrar(con,tabela,atual)
    ultimo=con.execute('SELECT ultimo_id FROM radar_pncp_identificadores WHERE tipo=?',(tabela,)).fetchone()[0]
    ident=ultimo+1;registrar(con,tabela,ident)
    return ident
