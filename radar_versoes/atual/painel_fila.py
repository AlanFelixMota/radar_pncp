"""Resumos em segundo plano: nenhum SQL pesado ou PowerShell na renderização.

O serviço pertence ao processo da interface e não controla os workers de coleta.
Snapshots antigos continuam visíveis (com data) se uma atualização falhar.
"""
from __future__ import annotations

import copy
import json
import os
import sqlite3
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Callable


class ResumoBackground:
    def __init__(self, coletar: Callable[[], dict], *, ttl: float = 120,
                 arquivo: Path | None = None, identidade: str = ""):
        self.coletar, self.ttl = coletar, ttl
        self.arquivo, self.identidade = arquivo, identidade
        self._lock = threading.Lock()
        self._data = None
        self._atualizado = None
        self._erro = None
        self._rodando = False
        self._ultima_tentativa = float('-inf')
        self._invalidado = False
        if arquivo:
            try:
                obj = json.loads(arquivo.read_text(encoding="utf-8"))
                if obj.get('identidade') == identidade and obj.get('schema') == 1:
                    self._data = obj['dados']
                    self._atualizado = obj['atualizado_em']
            except (OSError, ValueError, KeyError, TypeError):
                pass

    def obter(self, *, forcar: bool = False) -> dict:
        """Retorna imediatamente; no máximo um coletor por serviço/processo."""
        with self._lock:
            if not self._rodando and (forcar or self._invalidado or time.monotonic() - self._ultima_tentativa >= self.ttl):
                self._rodando = True
                self._invalidado = False
                self._ultima_tentativa = time.monotonic()
                threading.Thread(target=self._executar, daemon=True, name='radar_pncp-resumo-fila').start()
            return {'dados': copy.deepcopy(self._data), 'atualizado_em': self._atualizado,
                    'erro': self._erro, 'atualizando': self._rodando}

    def invalidar(self):
        with self._lock:
            self._invalidado = True

    def _executar(self):
        try:
            dados = self.coletar()
            stamp = datetime.now().isoformat(timespec='seconds')
            aviso = None
            if self.arquivo:
                tmp = self.arquivo.with_name(self.arquivo.name + f'.{os.getpid()}.{threading.get_ident()}.tmp')
                try:
                    self.arquivo.parent.mkdir(parents=True, exist_ok=True)
                    tmp.write_text(json.dumps({'schema': 1, 'identidade': self.identidade,
                                              'atualizado_em': stamp, 'dados': dados}, ensure_ascii=False), encoding='utf-8')
                    os.replace(tmp, self.arquivo)
                except OSError as exc:
                    aviso = f'Resumo atualizado nesta sessão; não foi possível salvar o cache: {exc}'
                finally:
                    tmp.unlink(missing_ok=True)
            with self._lock:
                self._data, self._atualizado, self._erro = dados, stamp, aviso
        except Exception as exc:
            with self._lock:
                self._erro = f'{type(exc).__name__}: {exc}'
        finally:
            with self._lock:
                self._rodando = False
                self._ultima_tentativa = time.monotonic()


def conectar_leitura(caminho: Path, timeout: float = 1) -> sqlite3.Connection:
    con = sqlite3.connect(caminho.resolve().as_uri() + '?mode=ro', uri=True, timeout=timeout)
    con.row_factory = sqlite3.Row
    con.execute('PRAGMA query_only=ON')
    return con


def coletar_metricas(caminho: Path) -> dict:
    import itens_incremental as ii
    con = conectar_leitura(caminho)
    limite = time.monotonic() + 60
    con.set_progress_handler(lambda: int(time.monotonic() > limite), 10000)
    try:
        # Uma única visão consistente; não executa migrações, CREATE ou UPDATE.
        con.execute('BEGIN')
        fila = ii.resumo_fila(con, preparar=False, apenas_ativos=True)
        h = {'validados_pendentes': fila['validados_comprasgov'], 'transitorios': fila['transitorios_comprasgov'],
             'fallbacks': 0, 'novos_itens': 0, 'ultima_execucao': None}
        cols = {r[1] for r in con.execute('PRAGMA table_info(contratacoes)')}
        if 'itens_validacao_comprasgov_status' in cols:
            h['fallbacks'] = con.execute("SELECT COUNT(*) FROM contratacoes WHERE itens_coletados=0 AND itens_validacao_comprasgov_status='FALLBACK_PNCP'").fetchone()[0]
            h['novos_itens'] = con.execute("SELECT COALESCE(SUM(itens_validacao_comprasgov_novos),0) FROM contratacoes WHERE itens_coletados=0 AND itens_validacao_comprasgov_status='VALIDADO_COMPRASGOV'").fetchone()[0]
        if con.execute("SELECT 1 FROM sqlite_master WHERE name='itens_validacao_comprasgov_log' AND type='table'").fetchone():
            row = con.execute('SELECT executado_em FROM itens_validacao_comprasgov_log ORDER BY id DESC LIMIT 1').fetchone()
            h['ultima_execucao'] = row[0] if row else None
        return {'fila': fila, 'hibrido': h}
    finally:
        con.close()


def ler_controle(caminho: Path) -> tuple[dict, dict]:
    """Lê o banco pequeno sem preparar/migrar estruturas no caminho da UI."""
    if not caminho.exists():
        return {}, {}
    con = conectar_leitura(caminho, timeout=0.2)
    try:
        out = []
        for tabela in ('mutirao_persistente_config', 'mutirao_persistente_estado'):
            existe = con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (tabela,)).fetchone()
            row = con.execute(f'SELECT * FROM {tabela} WHERE id=1').fetchone() if existe else None
            out.append(dict(row) if row else {})
        return tuple(out)
    finally:
        con.close()
