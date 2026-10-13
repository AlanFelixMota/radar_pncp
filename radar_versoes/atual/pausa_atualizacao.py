"""Sinal de manutenção cooperativo: nunca encerra processos ou altera jobs."""
from contextlib import closing
from pathlib import Path
import sqlite3
import threading
import time

EXIT_ATUALIZACAO = 79


class PausaAtualizacao:
    """Consulta barata, compartilhável entre threads; uma pausa observada é mantida."""
    def __init__(self, jobs_db, intervalo=1.0):
        self.db = Path(jobs_db).resolve() if jobs_db else None
        self.intervalo = intervalo
        self.solicitada = False
        self.proxima = 0.0
        self.lock = threading.Lock()

    def __call__(self):
        with self.lock:
            if self.solicitada or self.db is None:
                return self.solicitada
            now = time.monotonic()
            if now < self.proxima:
                return False
            self.proxima = now + self.intervalo
            try:
                with closing(sqlite3.connect(self.db.as_uri()+'?mode=ro', uri=True, timeout=.2)) as con:
                    existe = con.execute("SELECT 1 FROM sqlite_master WHERE name='radar_pncp_atualizacao'").fetchone()
                    self.solicitada = bool(existe and con.execute('SELECT 1 FROM radar_pncp_atualizacao WHERE id=1').fetchone())
            except sqlite3.Error:
                # Uma indisponibilidade momentânea não simula uma ordem de parada.
                # O atualizador continua aguardando; a leitura será tentada novamente.
                pass
            return self.solicitada
