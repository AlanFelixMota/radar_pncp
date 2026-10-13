#!/usr/bin/env python3
"""Detecção de indisponibilidade e backoff do PNCP."""

from __future__ import annotations

import threading
import time
from datetime import date, timedelta
from typing import Any

import requests


TRANSIENT_HTTP = {429, 500, 502, 503, 504}
EXIT_PNCP_INDISPONIVEL = 75


def status_http_erro(exc: BaseException) -> int | None:
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        return int(exc.response.status_code)
    return None


def eh_erro_transitorio(exc: BaseException) -> bool:
    status = status_http_erro(exc)
    if status in TRANSIENT_HTTP:
        return True
    return isinstance(
        exc,
        (
            requests.ConnectionError,
            requests.Timeout,
            requests.exceptions.ChunkedEncodingError,
        ),
    )


def descrever_erro(exc: BaseException) -> str:
    status = status_http_erro(exc)
    if status:
        return f"HTTP {status}: {exc}"
    return f"{type(exc).__name__}: {exc}"


def criar_sessao_health() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "Accept": "application/json",
            "Accept-Language": "pt-BR,pt;q=0.9",
            "User-Agent": "RADAR_PNCP/0.6.6.4 health-check",
        }
    )
    return s


def verificar_pncp(timeout: int = 20) -> tuple[bool, str]:
    """Testa a API usada pelo coletor. Um 4xx não-5xx prova que o servidor responde."""
    ontem = (date.today() - timedelta(days=1)).strftime("%Y%m%d")
    url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    params = {
        "dataInicial": ontem,
        "dataFinal": ontem,
        "codigoModalidadeContratacao": 6,
        "pagina": 1,
        "tamanhoPagina": 50,
    }
    s = criar_sessao_health()
    try:
        r = s.get(url, params=params, timeout=timeout)
        if r.status_code in TRANSIENT_HTTP:
            return False, f"PNCP respondeu HTTP {r.status_code}"
        if 200 <= r.status_code < 500:
            return True, f"PNCP respondeu HTTP {r.status_code}"
        return False, f"PNCP respondeu HTTP {r.status_code}"
    except (requests.ConnectionError, requests.Timeout) as exc:
        return False, f"{type(exc).__name__}: {exc}"
    finally:
        s.close()


def testar_endpoint_publico(
    url: str,
    params: dict[str, Any] | None = None,
    timeout: int = 12,
) -> dict[str, Any]:
    """Faz um único health-check sem retries automáticos.

    Retorna um dicionário estável para UI/log. 404/410 significam que o
    serviço respondeu, ainda que o recurso sondado não esteja disponível.
    429 indica rate limit e 5xx/conexão/timeout indicam instabilidade.
    """
    inicio = time.perf_counter()
    s = criar_sessao_health()
    try:
        # O health-check deve ser rápido e previsível: não há retry automático.
        adapter = requests.adapters.HTTPAdapter(max_retries=0)
        s.mount("https://", adapter)
        resp = s.get(url, params=params, timeout=(min(6, timeout), timeout))
        http = int(resp.status_code)
        if 200 <= http < 300:
            estado = "OK"
            detalhe = f"HTTP {http}"
        elif http in (404, 410):
            estado = "RESPONDENDO"
            detalhe = f"HTTP {http} · serviço respondeu; recurso sondado indisponível"
        elif http == 429:
            estado = "LIMITADO"
            detalhe = "HTTP 429 · rate limit ativo"
        elif http in TRANSIENT_HTTP:
            estado = "INSTAVEL"
            detalhe = f"HTTP {http} · erro transitório"
        elif 400 <= http < 500:
            estado = "RESPONDENDO"
            detalhe = f"HTTP {http} · serviço respondeu"
        else:
            estado = "ERRO"
            detalhe = f"HTTP {http}"
        return {
            "estado": estado, "http": http, "detalhe": detalhe,
            "segundos": round(time.perf_counter() - inicio, 3), "url": resp.url,
        }
    except (requests.ConnectionError, requests.Timeout) as exc:
        return {
            "estado": "INSTAVEL", "http": None,
            "detalhe": f"{type(exc).__name__}: {exc}",
            "segundos": round(time.perf_counter() - inicio, 3), "url": url,
        }
    except Exception as exc:
        return {
            "estado": "ERRO", "http": None,
            "detalhe": f"{type(exc).__name__}: {exc}",
            "segundos": round(time.perf_counter() - inicio, 3), "url": url,
        }
    finally:
        s.close()


class RateController:
    """Pausa compartilhada simples e adaptativa entre workers."""

    def __init__(
        self,
        delay_inicial: float = 0.05,
        delay_min: float = 0.02,
        delay_max: float = 2.0,
    ) -> None:
        self.delay = max(0.0, float(delay_inicial))
        self.delay_min = max(0.0, float(delay_min))
        self.delay_max = max(self.delay_min, float(delay_max))
        self.pause_until = 0.0
        self._lock = threading.Lock()

    def aguardar(self) -> None:
        with self._lock:
            delay = self.delay
            pause_until = self.pause_until
        agora = time.monotonic()
        espera = max(delay, pause_until - agora)
        if espera > 0:
            time.sleep(espera)

    def sucesso(self) -> None:
        with self._lock:
            # Recuperação lenta para não oscilar demais.
            self.delay = max(self.delay_min, self.delay * 0.98)

    def erro_transitorio(self, status: int | None = None) -> None:
        with self._lock:
            self.delay = min(self.delay_max, max(self.delay_min, self.delay * 1.8))
            pausa = 0.0
            if status == 429:
                pausa = 8.0
            elif status in {502, 503, 504}:
                pausa = 5.0
            elif status == 500:
                pausa = 2.0
            else:
                pausa = 3.0
            self.pause_until = max(self.pause_until, time.monotonic() + pausa)

    def estado(self) -> dict[str, Any]:
        with self._lock:
            return {"delay": self.delay, "pause_until": self.pause_until}
