"""Migra por copia uma instalacao encerrada; a origem e aberta somente para leitura.

Os prefixos anteriores sao descobertos pela estrutura do banco, sem aliases
de marca no codigo novo. Nenhum arquivo de codigo antigo e importado.
"""
from contextlib import closing
from datetime import datetime
from pathlib import Path, PureWindowsPath
import argparse
import json
import re
import shutil
import sqlite3
import tempfile

ROOT = Path(__file__).resolve().parent
# Sufixo, prefixo novo e colunas obrigatorias: apenas tabelas do aplicativo.
REGRAS = (
    ("_identificadores", "radar_pncp", {"tipo", "ultimo_id"}),
    ("_migracoes", "radar_pncp", {"chave", "executada_em", "detalhes_json"}),
    ("_atualizacao", "radar_pncp", {"id", "token", "ativo_anterior"}),
    ("_atualizacao_checkpoints", "coleta", {"janela_inicio", "janela_fim", "modalidade_id"}),
    ("_janelas_pesquisa", "coleta", {"inicio", "fim", "estado"}),
    ("_alvos_pesquisa", "coleta", {"inicio", "fim", "numero_controle_pncp"}),
    ("_acompanhamentos", "monitoramento", {"id", "busca_id", "geracao", "assinatura"}),
    ("_conhecidos", "monitoramento", {"acompanhamento_id", "geracao", "chave"}),
    ("_ciclos", "monitoramento", {"id", "acompanhamento_id", "geracao", "pedido_json"}),
    ("_novidades", "monitoramento", {"id", "acompanhamento_id", "geracao", "ciclo_id"}),
)


def identificador(nome):
    return '"' + nome.replace('"', '""') + '"'


def tabelas(con):
    return {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def colunas(con, nome):
    return {r[1] for r in con.execute("PRAGMA table_info(" + identificador(nome) + ")")}


def descobrir_prefixos(con):
    out = {}
    for nome in tabelas(con):
        for sufixo, novo, esperado in REGRAS:
            if nome.endswith(sufixo) and esperado <= colunas(con, nome):
                anterior = nome[:-len(sufixo)]
                if anterior != novo:
                    if anterior in out and out[anterior] != novo:
                        raise ValueError("Estrutura ambigua; migracao cancelada.")
                    out[anterior] = novo
    return out


def substituir(valor, prefixos):
    if not isinstance(valor, str):
        return valor
    # Fronteiras de tokens: altera prefixos/enum, preserva palavras comuns.
    pares = {a: b for antigo, novo in prefixos.items()
             for a, b in ((antigo, novo), (antigo.upper(), novo.upper()))}
    if not pares:
        return valor
    exp = r"(?<![A-Za-z0-9])(?:" + "|".join(re.escape(a) for a in sorted(pares, key=len, reverse=True)) + r")(?![A-Za-z0-9])"
    resultado = re.sub(exp, lambda m: pares[m.group()], valor)
    # A coleta PNCP de um dia e a atualizacao operacional de janela movel
    # sao tipos distintos. O enum ja existente nunca muda de significado.
    if resultado == "COLETA_DIARIA" and valor != resultado:
        return "ATUALIZACAO_DIARIA"
    return resultado


def adaptar(con, prefixos, destino):
    con.execute("PRAGMA foreign_keys=ON")
    con.execute("BEGIN IMMEDIATE")
    try:
        nomes = tabelas(con)
        renomes = {}
        for nome in nomes:
            for sufixo, novo, esperado in REGRAS:
                if nome.endswith(sufixo) and esperado <= colunas(con, nome):
                    anterior = nome[:-len(sufixo)]
                    if anterior in prefixos:
                        renomes[nome] = novo + sufixo
        for antigo, novo in renomes.items():
            if novo in nomes:
                raise ValueError("Tabelas de duas geracoes coexistem; nao sera feita mesclagem automatica.")
            con.execute("ALTER TABLE " + identificador(antigo) + " RENAME TO " + identificador(novo))
        # SQLite atualiza as referencias a tabelas, mas nao os nomes de indices.
        for tipo, nome, sql in con.execute("SELECT type,name,sql FROM sqlite_master WHERE type IN ('index','trigger','view') AND sql IS NOT NULL").fetchall():
            novo = substituir(nome, prefixos)
            novo_sql = substituir(sql, prefixos)
            if nome != novo or sql != novo_sql:
                con.execute("DROP " + tipo.upper() + " " + identificador(nome))
                con.execute(novo_sql)
        # Campos tecnicos que o programa interpreta; textos de pesquisas e
        # referencias do usuario sao preservados integralmente.
        campos = {
            "jobs": ("tipo", "operacao", "etapa", "mensagem"),
            "job_dias": ("etapa", "mensagem"),
            "comprasgov_cobertura": ("chave",),
            "radar_pncp_identificadores": ("tipo",),
            "itens_fila": ("origem",),
            "monitoramento_ciclos": ("cobertura_json",),
        }
        for tabela, candidatos in campos.items():
            if tabela not in tabelas(con):
                continue
            cols = colunas(con, tabela)
            for campo in set(candidatos) & cols:
                # A selecao e indexada pelo valor quando houver indice. Nao
                # consulta textos ou payloads da tabela principal de itens.
                for valor, in con.execute("SELECT DISTINCT " + identificador(campo) + " FROM " + identificador(tabela)).fetchall():
                    novo = substituir(valor, prefixos)
                    if novo != valor:
                        con.execute("UPDATE " + identificador(tabela) + " SET " + identificador(campo) + "=? WHERE " + identificador(campo) + "=?", (novo, valor))
        if "jobs" in tabelas(con):
            cols = colunas(con, "jobs")
            if "db_compras" in cols:
                con.execute("UPDATE jobs SET db_compras=? WHERE db_compras IS NOT NULL", (str(destino / "compras.db"),))
            # Os logs da instalacao anterior continuam no backup/origem.
            if {"id", "log_path"} <= cols:
                for jid, path in con.execute("SELECT id,log_path FROM jobs WHERE log_path IS NOT NULL").fetchall():
                    nome = PureWindowsPath(path).name if "\\" in path else Path(path).name
                    con.execute("UPDATE jobs SET log_path=? WHERE id=?", (str(destino / "logs" / substituir(nome, prefixos)), jid))
        if "mutirao_persistente_config" in tabelas(con):
            con.execute("UPDATE mutirao_persistente_config SET ativo=0")
        if "mutirao_persistente_estado" in tabelas(con):
            con.execute("DELETE FROM mutirao_persistente_estado")
        if "monitoramento_acompanhamentos" in tabelas(con):
            con.execute("UPDATE monitoramento_acompanhamentos SET token=NULL,ocupado_em=NULL")
        con.commit()
    except BaseException:
        con.rollback()
        raise
    return len(renomes)


def validar_origem(con):
    for nome in tabelas(con):
        cols = colunas(con, nome)
        if nome.endswith("_atualizacao") and {"id", "token", "ativo_anterior"} <= cols:
            if con.execute("SELECT 1 FROM " + identificador(nome) + " LIMIT 1").fetchone():
                raise ValueError("A origem esta em atualizacao. Conclua ou recupere essa operacao antes de copiar.")
    if "jobs" in tabelas(con) and "status" in colunas(con, "jobs"):
        if con.execute("SELECT 1 FROM jobs WHERE status IN ('AGUARDANDO','EXECUTANDO','PARADA_SOLICITADA') LIMIT 1").fetchone():
            raise ValueError("Ha jobs ativos na origem. Pare as coletas e aguarde seu encerramento antes de copiar.")


def migrar(origem, destino=None, progresso=print):
    origem = Path(origem).expanduser().resolve()
    if (origem / "radar_dados").is_dir():
        origem /= "radar_dados"
    destino = Path(destino or ROOT / "radar_dados").expanduser().resolve()
    if destino == origem or destino.is_relative_to(origem):
        raise ValueError("O destino precisa ficar fora da pasta de dados original.")
    bancos = sorted(origem.glob("*.db"))
    if not (origem / "compras.db").is_file():
        raise ValueError("A origem deve conter compras.db; use a pasta de dados completa do computador.")
    if destino.exists() and any(p.name != ".gitkeep" for p in destino.iterdir()):
        raise ValueError("O destino precisa estar vazio. Nenhum arquivo foi substituido.")
    destino.parent.mkdir(parents=True, exist_ok=True)
    tamanho = sum(p.stat().st_size + (Path(str(p) + "-wal").stat().st_size if Path(str(p) + "-wal").exists() else 0) for p in bancos)
    if shutil.disk_usage(destino.parent).free < tamanho * 1.1 + 100 * 1024 * 1024:
        raise ValueError("Espaco insuficiente para copiar todos os bancos com margem de seguranca.")
    prefixos = {}
    for path in bancos:
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as con:
            validar_origem(con)
            for anterior, novo in descobrir_prefixos(con).items():
                if anterior in prefixos and prefixos[anterior] != novo:
                    raise ValueError("Bancos com prefixos ambiguos; nenhuma copia publicada.")
                prefixos[anterior] = novo
    # Publicacao da pasta apenas depois de todas as copias e verificacoes.
    # Em caso de interrupcao abrupta, a origem e o destino seguem intactos.
    with tempfile.TemporaryDirectory(prefix=".migracao_", dir=destino.parent) as pasta:
        stage = Path(pasta) / "dados"
        stage.mkdir()
        for path in bancos:
            progresso("Copiando " + path.name + "...")
            with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as src, closing(sqlite3.connect(stage / path.name)) as dst:
                src.backup(dst, pages=8192)
                validar_origem(dst)
                # Falhas de FK existentes sao relatadas, nao causadas pela
                # renomeacao; o comparativo evita rejeitar historico valido.
                antes = set(dst.execute("PRAGMA foreign_key_check"))
                n = adaptar(dst, prefixos, destino)
                depois = {tuple(r) for r in dst.execute("PRAGMA foreign_key_check")}
                # Os nomes nas mensagens de FK acompanham a migracao.
                antes = {tuple(substituir(v, prefixos) if isinstance(v, str) else v for v in r) for r in antes}
                if depois != antes:
                    raise ValueError("A renomeacao alterou as violacoes de chave estrangeira em " + path.name)
                if dst.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                    raise ValueError("Falha de integridade em " + path.name)
                progresso(f"[OK] {path.name}: {n} tabelas adaptadas; {len(depois)} violacoes de FK preexistentes.")
                # Consolida WAL antes da publicacao da pasta.
                dst.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        (stage / "logs").mkdir()
        (stage / "migracao.json").write_text(json.dumps({
            "version": "1.0.0", "copiado_em": datetime.now().astimezone().isoformat(),
            "bancos": [p.name for p in bancos], "coleta_automatica_ativa": False}, indent=2), encoding="utf-8")
        # Revalida o destino para nao sobrescrever uma abertura concorrente.
        if destino.exists():
            if any(p.name != ".gitkeep" for p in destino.iterdir()):
                raise ValueError("O destino mudou durante a copia; nenhuma publicacao realizada.")
            (destino / ".gitkeep").unlink(missing_ok=True)
            destino.rmdir()
        stage.rename(destino)
    progresso("[OK] Dados copiados. Origem preservada. Reconfigure as agendas na nova instalacao.")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--origem", type=Path, required=True)
    parser.add_argument("--destino", type=Path, default=ROOT / "radar_dados")
    args = parser.parse_args()
    try:
        raise SystemExit(migrar(args.origem, args.destino))
    except (ValueError, OSError, sqlite3.Error) as exc:
        parser.exit(1, "Migracao nao concluida: " + str(exc) + "\n")
