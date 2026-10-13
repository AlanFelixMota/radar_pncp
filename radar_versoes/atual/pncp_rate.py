#!/usr/bin/env python3
"""Controle cooperativo de ritmo para consultas públicas do PNCP — V0.7.6."""
from __future__ import annotations

import email.utils
import time
from datetime import datetime, timezone
from typing import Callable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/151.0.0.0 Safari/537.36"
)


def _retry_after_seconds(resp: requests.Response | None) -> float | None:
    if resp is None:
        return None
    raw = (resp.headers.get("Retry-After") or "").strip()
    if not raw:
        return None
    try:
        return max(0.0, float(raw))
    except ValueError:
        pass
    try:
        dt = email.utils.parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(0.0, (dt - datetime.now(timezone.utc)).total_seconds())
    except Exception:
        return None


class RateLimiter:
    def __init__(self, intervalo_inicial: float = 1.25, intervalo_min: float = 1.0, intervalo_max: float = 6.0):
        self.intervalo = max(intervalo_min, intervalo_inicial)
        self.intervalo_min = intervalo_min
        self.intervalo_max = intervalo_max
        self.ultima_requisicao = 0.0
        self.sucessos_desde_ajuste = 0
        self.strikes_429 = 0

    def esperar_ritmo(self, parada: Callable[[], bool] | None = None) -> None:
        falta = self.intervalo - (time.monotonic() - self.ultima_requisicao)
        self._esperar(max(0.0, falta), parada)
        self.ultima_requisicao = time.monotonic()

    @staticmethod
    def _esperar(segundos: float, parada: Callable[[], bool] | None = None) -> None:
        fim = time.monotonic() + max(0.0, segundos)
        while True:
            if parada and parada():
                raise InterruptedError
            restante = fim - time.monotonic()
            if restante <= 0:
                return
            time.sleep(min(0.5, restante))

    def sucesso(self) -> None:
        self.strikes_429 = 0
        self.sucessos_desde_ajuste += 1
        # Recuperação lenta: nunca fica mais agressivo que 1 req/s.
        if self.sucessos_desde_ajuste >= 30 and self.intervalo > self.intervalo_min:
            self.intervalo = max(self.intervalo_min, self.intervalo * 0.92)
            self.sucessos_desde_ajuste = 0

    def cooldown_429(self, resp: requests.Response | None, parada: Callable[[], bool] | None = None) -> tuple[float, bool]:
        self.strikes_429 += 1
        self.sucessos_desde_ajuste = 0
        self.intervalo = min(self.intervalo_max, max(self.intervalo, 1.25) * 1.5)
        header = _retry_after_seconds(resp)
        fallback = min(300.0, 30.0 * (2 ** (self.strikes_429 - 1)))
        espera = max(header or 0.0, fallback)
        self._esperar(espera, parada)
        # Após 4 cooldowns no mesmo ciclo, é melhor preservar a fila e retomar depois.
        return espera, self.strikes_429 >= 4

    def backoff_transitorio(self, consecutivos: int, parada: Callable[[], bool] | None = None) -> float:
        espera = min(60.0, 5.0 * (2 ** max(0, consecutivos - 1)))
        self._esperar(espera, parada)
        return espera

    def pausa_global(self, segundos: float, parada: Callable[[], bool] | None = None) -> float:
        """Pausa cooperativa longa usada quando o PNCP oscila globalmente."""
        espera = max(0.0, float(segundos))
        self._esperar(espera, parada)
        # Depois de uma indisponibilidade, retome um pouco mais conservador.
        self.intervalo = min(self.intervalo_max, max(self.intervalo, 1.25) * 1.10)
        self.sucessos_desde_ajuste = 0
        return espera


class SessaoPNCPControlada(requests.Session):
    """Session sem retries automáticos de status e com espaçamento entre GETs."""
    def __init__(self, limiter: RateLimiter):
        super().__init__()
        self.limiter = limiter
        retry = Retry(total=0, connect=0, read=0, redirect=0, status=0, raise_on_status=False)
        self.mount("https://", HTTPAdapter(max_retries=retry))
        self.headers.update({
            "Accept": "application/json",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
            "User-Agent": USER_AGENT,
        })
        self._parada_callback: Callable[[], bool] | None = None

    def set_parada_callback(self, cb: Callable[[], bool] | None) -> None:
        self._parada_callback = cb

    def request(self, method, url, *args, **kwargs):  # type: ignore[override]
        if str(method).upper() == "GET":
            self.limiter.esperar_ritmo(self._parada_callback)
        resp = super().request(method, url, *args, **kwargs)
        if 200 <= int(resp.status_code) < 300 or resp.status_code in (404, 410):
            self.limiter.sucesso()
        return resp


def criar_sessao_controlada(intervalo: float = 1.25) -> tuple[SessaoPNCPControlada, RateLimiter]:
    limiter = RateLimiter(intervalo_inicial=intervalo)
    return SessaoPNCPControlada(limiter), limiter
