"""HTTPS client for the Databricks REST API.

Wraps ``requests.Session`` with:

- Bearer-token auth, never logged.
- Connect/read timeouts on every request (10s / 30s).
- ``urllib3.Retry`` exponential backoff on 429 and 5xx, honoring
  ``Retry-After`` on 429.
- A single ``paginate()`` generator that works for every token-based
  Databricks pagination style we use.
- Structured per-request logging: method, path, status, latency_ms.

401 fails fast via :class:`AuthError`; all other 4xx/5xx raise
:class:`ClientError` so collectors can catch them and return
``status="unavailable"``.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Iterator

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import AppConfig

log = logging.getLogger("dbx_metrics.client")

_CONNECT_TIMEOUT_S = 10
_READ_TIMEOUT_S = 30
_TIMEOUT = (_CONNECT_TIMEOUT_S, _READ_TIMEOUT_S)

_RETRY_TOTAL = 3
_RETRY_BACKOFF_FACTOR = 1.0
_RETRY_STATUS = (429, 500, 502, 503, 504)


class ClientError(RuntimeError):
    """Non-2xx response that retries couldn't recover."""

    def __init__(self, method: str, path: str, status: int, body: str) -> None:
        snippet = body[:200].replace("\n", " ") if body else ""
        super().__init__(f"{method} {path} -> {status}: {snippet}")
        self.method = method
        self.path = path
        self.status = status
        self.body = body


class AuthError(ClientError):
    """401 — fail fast, bad credentials."""


class DatabricksClient:
    def __init__(self, host: str, token: str) -> None:
        self._host = host.rstrip("/")
        self._token = token
        self._session = self._build_session()

    @classmethod
    def from_config(cls, config: AppConfig) -> "DatabricksClient":
        return cls(host=config.host, token=config.token)

    def _build_session(self) -> requests.Session:
        retry = Retry(
            total=_RETRY_TOTAL,
            status_forcelist=_RETRY_STATUS,
            allowed_methods=frozenset(["GET"]),
            backoff_factor=_RETRY_BACKOFF_FACTOR,
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        sess = requests.Session()
        sess.mount("https://", adapter)
        sess.mount("http://", adapter)
        sess.headers.update({
            "Authorization": f"Bearer {self._token}",
            "User-Agent": "dbx-metrics-poc/0.1",
            "Accept": "application/json",
        })
        return sess

    def __repr__(self) -> str:
        return f"DatabricksClient(host={self._host!r}, token='***')"

    @property
    def host(self) -> str:
        return self._host

    @property
    def session(self) -> requests.Session:
        return self._session

    def _url(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        return self._host + path

    def get(self, path: str, params: dict | None = None) -> dict:
        return self._request("GET", path, params=params, parse="json")

    def get_text(self, path: str, params: dict | None = None) -> str:
        return self._request("GET", path, params=params, parse="text")

    def _request(self, method: str, path: str, *, params: dict | None, parse: str) -> Any:
        url = self._url(path)
        start = time.monotonic()
        resp = self._session.request(method, url, params=params, timeout=_TIMEOUT)
        latency_ms = int((time.monotonic() - start) * 1000)
        log.info("%s %s status=%d latency_ms=%d", method, path, resp.status_code, latency_ms)

        if resp.status_code == 401:
            raise AuthError(method, path, 401, resp.text)
        if resp.status_code >= 400:
            raise ClientError(method, path, resp.status_code, resp.text)

        if parse == "json":
            return resp.json() if resp.text else {}
        return resp.text

    def paginate(
        self,
        path: str,
        *,
        items_key: str,
        params: dict | None = None,
        request_token_key: str = "page_token",
        response_token_key: str = "next_page_token",
    ) -> Iterator[dict]:
        """Yield items across pages.

        Databricks' current 2.1 endpoints (jobs, clusters, UC catalogs/
        schemas/tables) all use ``page_token`` as the request param and
        ``next_page_token`` in the response. Kept parametrized so we
        can absorb any endpoint that picks different names.
        """
        call_params = dict(params or {})
        while True:
            resp = self.get(path, params=call_params)
            for item in resp.get(items_key, []) or []:
                yield item
            token = resp.get(response_token_key)
            if not token:
                return
            call_params[request_token_key] = token
