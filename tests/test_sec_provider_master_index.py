"""Tests for SecFilingsProvider.fetch_master_index — conditional GET."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
import pytest

from app.providers.implementations import sec_edgar as sec_edgar_mod
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


def _pin_now_et(monkeypatch: pytest.MonkeyPatch, iso_instant: str) -> None:
    """Freeze datetime.now(_ET) inside sec_edgar to return iso_instant (ET-local)."""
    frozen = datetime.fromisoformat(iso_instant).replace(tzinfo=ZoneInfo("America/New_York"))

    class _FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            return frozen.astimezone(tz) if tz else frozen.replace(tzinfo=None)

    monkeypatch.setattr(sec_edgar_mod, "datetime", _FrozenDatetime)


def test_fetch_returns_none_on_403_for_today_before_publish_cutoff(monkeypatch: pytest.MonkeyPatch) -> None:
    # SEC returns 403 (not 404) for current-day master.idx before the
    # ~22:00-ET publish cutoff. Provider treats this as "not yet
    # available" rather than raising.
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403)

    provider = SecFilingsProvider(user_agent="test test@example.com")
    _rewire_tickers_transport(provider, httpx.MockTransport(handler))

    _pin_now_et(monkeypatch, "2026-04-23T12:00:00")  # mid-day ET, before 22:00 publish

    result = provider.fetch_master_index(date(2026, 4, 23), if_modified_since=None)
    assert result is None


def test_fetch_raises_on_403_for_today_after_publish_cutoff(monkeypatch: pytest.MonkeyPatch) -> None:
    # After 22:00 ET the current-day file should exist. A 403 at that
    # point is SEC actively refusing us — must raise so ops notice.
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403)

    provider = SecFilingsProvider(user_agent="test test@example.com")
    _rewire_tickers_transport(provider, httpx.MockTransport(handler))

    _pin_now_et(monkeypatch, "2026-04-23T23:00:00")  # 23:00 ET, past 22:00 publish

    with pytest.raises(httpx.HTTPStatusError):
        provider.fetch_master_index(date(2026, 4, 23), if_modified_since=None)


def test_fetch_returns_none_on_403_for_future_date(monkeypatch: pytest.MonkeyPatch) -> None:
    # Future-dated 403s tolerated — callers may iterate a lookback
    # window whose endpoints straddle midnight across timezones.
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403)

    provider = SecFilingsProvider(user_agent="test test@example.com")
    _rewire_tickers_transport(provider, httpx.MockTransport(handler))

    _pin_now_et(monkeypatch, "2026-04-23T12:00:00")

    result = provider.fetch_master_index(date(2026, 4, 24), if_modified_since=None)
    assert result is None


def test_fetch_raises_on_403_for_past_date(monkeypatch: pytest.MonkeyPatch) -> None:
    # Past-dated 403 is not a publish-window race — it indicates SEC
    # is actively blocking us (UA / rate limit / WAF). Must raise so
    # the scheduler surfaces the incident.
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403)

    provider = SecFilingsProvider(user_agent="test test@example.com")
    _rewire_tickers_transport(provider, httpx.MockTransport(handler))

    _pin_now_et(monkeypatch, "2026-04-23T12:00:00")

    with pytest.raises(httpx.HTTPStatusError):
        provider.fetch_master_index(date(2026, 4, 20), if_modified_since=None)
