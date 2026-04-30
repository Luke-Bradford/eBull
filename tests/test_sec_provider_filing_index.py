"""Tests for SecFilingsProvider.fetch_filing_index — host pin (#477).

Regression: ``fetch_filing_index`` previously routed through
``self._http`` (configured for ``data.sec.gov``), but the
``/Archives/edgar/data/...`` path is only served by
``www.sec.gov``. Every fetch returned 404, the service layer logged
``fetch_errors=N`` per run, and ``filing_documents`` never
populated. The fix routes through ``self._http_tickers``
(configured for ``www.sec.gov``) with a fully-qualified URL.

These tests pin the host so a future refactor that swaps clients
fails loudly instead of silently 404ing.
"""

from __future__ import annotations

import json

import httpx
import pytest

from app.providers.implementations.sec_edgar import SecFilingsProvider
from app.providers.resilient_client import ResilientClient


def _rewire_tickers_transport(
    provider: SecFilingsProvider,
    transport: httpx.MockTransport,
) -> None:
    """Swap the provider's tickers client for one backed by a MockTransport.

    Mirrors the pattern in ``test_sec_provider_master_index.py`` —
    `_http_tickers` is the www.sec.gov-targeted client. After the
    #477 fix, ``fetch_filing_index`` must go through this client, so
    rewiring it lets the test intercept every call.
    """
    provider._tickers_client = httpx.Client(  # noqa: SLF001
        headers={"User-Agent": "test test@example.com"},
        transport=transport,
    )
    provider._http_tickers = ResilientClient(  # noqa: SLF001
        provider._tickers_client,
        min_request_interval_s=0.0,
    )


def test_filing_index_request_hits_www_sec_gov() -> None:
    """Pin the host: every filing-index fetch must go to
    ``www.sec.gov``. Regression for #477 where ``data.sec.gov`` was
    used and 404'd 100% of the time."""
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, content=json.dumps({"directory": {"item": []}}))

    provider = SecFilingsProvider(user_agent="test test@example.com")
    _rewire_tickers_transport(provider, httpx.MockTransport(handler))

    result = provider.fetch_filing_index("0000320193-24-000001")
    assert result == {"directory": {"item": []}}
    assert len(captured) == 1
    assert captured[0].url.host == "www.sec.gov"
    # Path must use the int-coerced CIK (no leading zeros) per SEC's
    # archive layout convention. The manifest filename is plain
    # ``index.json`` (no accession prefix) — pre-#723 the code
    # targeted ``{accession}-index.json`` which doesn't exist on SEC.
    assert captured[0].url.path == "/Archives/edgar/data/320193/000032019324000001/index.json"


def test_filing_index_returns_none_on_404() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    provider = SecFilingsProvider(user_agent="test test@example.com")
    _rewire_tickers_transport(provider, httpx.MockTransport(handler))

    result = provider.fetch_filing_index("0000320193-24-000099")
    assert result is None


def test_filing_index_returns_none_when_body_is_not_object() -> None:
    """Defensive: if SEC returns a non-object JSON shape (array, scalar)
    we return None rather than crashing the consumer."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=json.dumps([1, 2, 3]))

    provider = SecFilingsProvider(user_agent="test test@example.com")
    _rewire_tickers_transport(provider, httpx.MockTransport(handler))

    assert provider.fetch_filing_index("0000320193-24-000001") is None


def test_filing_index_raises_on_500() -> None:
    """Server errors propagate — the service layer's per-filing
    try/except decides retry vs skip."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    provider = SecFilingsProvider(user_agent="test test@example.com")
    _rewire_tickers_transport(provider, httpx.MockTransport(handler))

    with pytest.raises(httpx.HTTPStatusError):
        provider.fetch_filing_index("0000320193-24-000001")
