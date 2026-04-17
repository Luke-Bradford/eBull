"""Tests for SecFilingsProvider.fetch_master_index — conditional GET."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import httpx

from app.providers.implementations.sec_edgar import (
    MasterIndexFetchResult,
    SecFilingsProvider,
)
from app.providers.resilient_client import ResilientClient

FIXTURE = Path("tests/fixtures/sec/master_20260415.idx")


def _rewire_tickers_transport(
    provider: SecFilingsProvider,
    transport: httpx.MockTransport,
) -> None:
    """Swap the provider's tickers client for one backed by a MockTransport."""
    provider._tickers_client = httpx.Client(
        headers={"User-Agent": "test test@example.com"},
        transport=transport,
    )
    provider._http_tickers = ResilientClient(
        provider._tickers_client,
        min_request_interval_s=0.0,
    )


def test_fetch_returns_result_with_body_and_last_modified() -> None:
    body = FIXTURE.read_bytes()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=body,
            headers={"Last-Modified": "Wed, 15 Apr 2026 22:00:00 GMT"},
        )

    provider = SecFilingsProvider(user_agent="test test@example.com")
    _rewire_tickers_transport(provider, httpx.MockTransport(handler))

    result = provider.fetch_master_index(date(2026, 4, 15), if_modified_since=None)

    assert isinstance(result, MasterIndexFetchResult)
    assert result.body == body
    assert result.last_modified == "Wed, 15 Apr 2026 22:00:00 GMT"
    assert len(result.body_hash) == 64  # sha256 hex


def test_fetch_returns_none_on_304() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(304)

    provider = SecFilingsProvider(user_agent="test test@example.com")
    _rewire_tickers_transport(provider, httpx.MockTransport(handler))

    result = provider.fetch_master_index(
        date(2026, 4, 15),
        if_modified_since="Wed, 15 Apr 2026 22:00:00 GMT",
    )
    assert result is None


def test_fetch_sends_if_modified_since_header() -> None:
    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured.update(request.headers)
        return httpx.Response(304)

    provider = SecFilingsProvider(user_agent="test test@example.com")
    _rewire_tickers_transport(provider, httpx.MockTransport(handler))

    provider.fetch_master_index(
        date(2026, 4, 15),
        if_modified_since="Wed, 15 Apr 2026 22:00:00 GMT",
    )
    assert captured.get("if-modified-since") == "Wed, 15 Apr 2026 22:00:00 GMT"


def test_fetch_returns_none_on_404_weekend() -> None:
    # SEC doesn't publish master-index on weekends. Provider stays
    # dumb — returns None on 404 so the service layer decides policy.
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    provider = SecFilingsProvider(user_agent="test test@example.com")
    _rewire_tickers_transport(provider, httpx.MockTransport(handler))

    result = provider.fetch_master_index(date(2026, 4, 18), if_modified_since=None)
    assert result is None


def test_fetch_url_uses_correct_quarter_for_date() -> None:
    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        return httpx.Response(304)

    # Q1: Jan-Mar. Q2: Apr-Jun. Q3: Jul-Sep. Q4: Oct-Dec.
    provider = SecFilingsProvider(user_agent="test test@example.com")
    _rewire_tickers_transport(provider, httpx.MockTransport(handler))

    provider.fetch_master_index(date(2026, 4, 15), if_modified_since=None)
    assert "/2026/QTR2/master.20260415.idx" in captured["url"]

    captured.clear()
    provider.fetch_master_index(date(2026, 1, 5), if_modified_since=None)
    assert "/2026/QTR1/master.20260105.idx" in captured["url"]

    captured.clear()
    provider.fetch_master_index(date(2026, 12, 31), if_modified_since=None)
    assert "/2026/QTR4/master.20261231.idx" in captured["url"]
