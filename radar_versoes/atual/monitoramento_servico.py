"""Verificações sequenciais no processo da interface, sem instalação/agendamento."""
from datetime import datetime
from pathlib import Path
import sqlite3,threading
import monitoramento,cestas
from pausa_atualizacao import PausaAtualizacao


class Servico:
    def __init__(self,db_compras,db,db_fts=None,db_tic=None,db_jobs=None):
        self.compras=db_compras;self.db=db;self.fts=db_fts;self.tic=db_tic;self.jobs=db_jobs
        self.evento=threading.Event();self.parar=threading.Event();self.lock=threading.Lock();self.thread=None

    def acordar(self):
        with self.lock:
            if self.thread is None or not self.thread.is_alive():
                self.parar.clear();self.thread=threading.Thread(target=self._loop,name='RADAR_PNCP-MONITORAMENTO',daemon=True);self.thread.start()
        self.evento.set()

    def encerrar(self):
        self.parar.set();self.evento.set()
        if self.thread and self.thread is not threading.current_thread():self.thread.join(timeout=2)

    def ciclo(self):
        for monitor in monitoramento.listar(self.db):
            if self.parar.is_set():return
            if not monitor['ativo'] or monitor['arquivada']:continue
            if monitor['tentativa_em']:
                try:
                    if (datetime.now().astimezone()-datetime.fromisoformat(monitor['tentativa_em'])).total_seconds()<monitoramento.INTERVALO:continue
                except ValueError:pass
            pausa=PausaAtualizacao(self.jobs)
            if pausa():return
            try:
                monitoramento.verificar(self.compras,self.db,monitor['id'],db_fts=self.fts,db_tic=self.tic,
                    max_segundos=45,cancelar=lambda:self.parar.is_set() or pausa())
            except (ValueError,sqlite3.Error,OSError) as exc:
                # Falhas antes da aquisição da verificação (ex.: filtro alterado)
                # também ficam visíveis e respeitam o intervalo de nova tentativa.
                with cestas.conectar(self.db) as con:
                    con.execute('''UPDATE monitoramento_acompanhamentos SET erro=?,tentativa_em=?
                        WHERE id=? AND revisao=? AND token IS NULL''',
                        (str(exc)[:1200],cestas.agora(),monitor['id'],monitor['revisao']))

    def _loop(self):
        while not self.parar.is_set():
            self.evento.clear()
            try:self.ciclo()
            except (ValueError,sqlite3.Error,OSError):pass # Próxima rodada tenta novamente.
            self.evento.wait(30)
