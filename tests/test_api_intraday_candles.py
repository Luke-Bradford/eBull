"""Tests for GET /instruments/{symbol}/intraday-candles (#600).

The endpoint resolves a symbol, loads eToro creds, and proxies to
the live eToro REST endpoint via the in-process cache. We mock the
cred loader, the provider, and the DB cursor so the unit test stays
isolated.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch
from uuid import UUID

import httpx
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.providers.market_data import IntradayBar


@pytest.fixture
def client() -> TestClient:
    # tests/conftest.py installs a global no-op for
    # `require_session_or_service_token`, so the new authed endpoint
    # is reachable in tests without per-suite auth setup.
    return TestClient(app)


@pytest.fixture(autouse=True)
def _clear_cache() -> Iterator[None]:
    from app.services.intraday_candles import get_intraday_cache

    get_intraday_cache().clear()
    yield
    get_intraday_cache().clear()


def _make_cursor(row: object) -> MagicMock:
    cur = MagicMock()
    cur.__enter__.return_value = cur
    cur.__exit__.return_value = None
    cur.fetchone.return_value = row
    cur.fetchall.return_value = []
    return cur


def _mock_conn_with_lookup(row: object) -> MagicMock:
    conn = MagicMock()
    conn.cursor.return_value = _make_cursor(row)
    return conn


def _bar(close: str = "100.5") -> IntradayBar:
    return IntradayBar(
        timestamp=datetime(2026, 4, 27, 14, 30, tzinfo=UTC),
        open=Decimal(close),
        high=Decimal(close),
        low=Decimal(close),
        close=Decimal(close),
        volume=1000,
    )


_OPERATOR_ID = UUID("00000000-0000-0000-0000-000000000001")
_DEFAULT_CRED_SECRETS = ("api-key-value", "user-key-value")


def test_intraday_unknown_symbol_returns_404(client: TestClient) -> None:
    from app.db import get_conn

    def _db_conn() -> Iterator[MagicMock]:
        yield _mock_conn_with_lookup(None)

    app.dependency_overrides[get_conn] = _db_conn
    try:
        resp = client.get("/instruments/NOTREAL/intraday-candles?interval=OneMinute&count=100")
    finally:
        app.dependency_overrides.pop(get_conn, None)
    assert resp.status_code == 404


def test_intraday_invalid_interval_returns_422(client: TestClient) -> None:
    from app.db import get_conn

    def _db_conn() -> Iterator[MagicMock]:
        yield MagicMock()

    app.dependency_overrides[get_conn] = _db_conn
    try:
        resp = client.get("/instruments/AAPL/intraday-candles?interval=BogusInterval&count=100")
    finally:
        app.dependency_overrides.pop(get_conn, None)
    assert resp.status_code == 422


def test_intraday_count_above_cap_returns_422(client: TestClient) -> None:
    from app.db import get_conn

    def _db_conn() -> Iterator[MagicMock]:
        yield MagicMock()

    app.dependency_overrides[get_conn] = _db_conn
    try:
        resp = client.get("/instruments/AAPL/intraday-candles?interval=OneMinute&count=99999")
    finally:
        app.dependency_overrides.pop(get_conn, None)
    assert resp.status_code == 422


def test_intraday_missing_credentials_returns_503(client: TestClient) -> None:
    from app.db import get_conn
    from app.services.broker_credentials import CredentialNotFound

    def _db_conn() -> Iterator[MagicMock]:
        yield _mock_conn_with_lookup({"instrument_id": 42, "symbol": "AAPL"})

    app.dependency_overrides[get_conn] = _db_conn
    try:
        with (
            patch("app.api.instruments.sole_operator_id", return_value=_OPERATOR_ID),
            patch(
                "app.api.instruments.load_credential_for_provider_use",
                side_effect=CredentialNotFound("api_key not configured"),
            ),
        ):
            resp = client.get("/instruments/AAPL/intraday-candles?interval=OneMinute&count=100")
    finally:
        app.dependency_overrides.pop(get_conn, None)
    assert resp.status_code == 503
    assert "credentials" in resp.text.lower()


def test_intraday_happy_path_returns_bars_with_iso_timestamps(client: TestClient) -> None:
    from app.db import get_conn

    def _db_conn() -> Iterator[MagicMock]:
        yield _mock_conn_with_lookup({"instrument_id": 42, "symbol": "AAPL"})

    app.dependency_overrides[get_conn] = _db_conn
    try:
        with (
            patch("app.api.instruments.sole_operator_id", return_value=_OPERATOR_ID),
            patch(
                "app.api.instruments.load_credential_for_provider_use",
                side_effect=list(_DEFAULT_CRED_SECRETS),
            ),
            patch("app.api.instruments.EtoroMarketDataProvider") as MockProv,
        ):
            mock_provider = MagicMock()
            mock_provider.__enter__.return_value = mock_provider
            mock_provider.get_intraday_candles.return_value = [_bar("180.50")]
            MockProv.return_value = mock_provider
            resp = client.get(
                "/instruments/AAPL/intraday-candles?interval=OneMinute&count=100",
            )
    finally:
        app.dependency_overrides.pop(get_conn, None)

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["symbol"] == "AAPL"
    assert body["interval"] == "OneMinute"
    # `count` reflects bars actually returned, not the request — the
    # provider returned 1 bar even though we asked for 100.
    assert body["count"] == 1
    assert body["persisted"] is False
    assert len(body["rows"]) == 1
    # Timestamp is ISO-8601 with timezone — frontend parses via Date.
    assert body["rows"][0]["timestamp"].startswith("2026-04-27T14:30:00")
    assert body["rows"][0]["close"] == "180.50"


def test_intraday_rate_limit_returns_503_with_retry_after(client: TestClient) -> None:
    from app.db import get_conn

    def _db_conn() -> Iterator[MagicMock]:
        yield _mock_conn_with_lookup({"instrument_id": 42, "symbol": "AAPL"})

    app.dependency_overrides[get_conn] = _db_conn
    try:
        with (
            patch("app.api.instruments.sole_operator_id", return_value=_OPERATOR_ID),
            patch(
                "app.api.instruments.load_credential_for_provider_use",
                side_effect=list(_DEFAULT_CRED_SECRETS),
            ),
            patch("app.api.instruments.EtoroMarketDataProvider") as MockProv,
        ):
            mock_provider = MagicMock()
            mock_provider.__enter__.return_value = mock_provider
            response_429 = httpx.Response(429, headers={"Retry-After": "60"}, request=httpx.Request("GET", "https://x"))
            mock_provider.get_intraday_candles.side_effect = httpx.HTTPStatusError(
                "rate limited", request=response_429.request, response=response_429
            )
            MockProv.return_value = mock_provider
            resp = client.get(
                "/instruments/AAPL/intraday-candles?interval=OneMinute&count=100",
            )
    finally:
        app.dependency_overrides.pop(get_conn, None)

    assert resp.status_code == 503
    assert resp.headers.get("retry-after") == "60"


def test_intraday_provider_5xx_returns_502(client: TestClient) -> None:
    from app.db import get_conn

    def _db_conn() -> Iterator[MagicMock]:
        yield _mock_conn_with_lookup({"instrument_id": 42, "symbol": "AAPL"})

    app.dependency_overrides[get_conn] = _db_conn
    try:
        with (
            patch("app.api.instruments.sole_operator_id", return_value=_OPERATOR_ID),
            patch(
                "app.api.instruments.load_credential_for_provider_use",
                side_effect=list(_DEFAULT_CRED_SECRETS),
            ),
            patch("app.api.instruments.EtoroMarketDataProvider") as MockProv,
        ):
            mock_provider = MagicMock()
            mock_provider.__enter__.return_value = mock_provider
            response_500 = httpx.Response(500, request=httpx.Request("GET", "https://x"))
            mock_provider.get_intraday_candles.side_effect = httpx.HTTPStatusError(
                "server error", request=response_500.request, response=response_500
            )
            MockProv.return_value = mock_provider
            resp = client.get(
                "/instruments/AAPL/intraday-candles?interval=OneMinute&count=100",
            )
    finally:
        app.dependency_overrides.pop(get_conn, None)

    assert resp.status_code == 502


def test_intraday_second_call_within_ttl_skips_provider(client: TestClient) -> None:
    """Two successive requests should hit the provider once, not twice."""
    from app.db import get_conn

    def _db_conn() -> Iterator[MagicMock]:
        yield _mock_conn_with_lookup({"instrument_id": 42, "symbol": "AAPL"})

    app.dependency_overrides[get_conn] = _db_conn
    try:
        with (
            patch("app.api.instruments.sole_operator_id", return_value=_OPERATOR_ID),
            # Cred patch returns 4 secrets — 2 per request × 2 requests.
            patch(
                "app.api.instruments.load_credential_for_provider_use",
                side_effect=["api-key", "user-key", "api-key", "user-key"],
            ),
            patch("app.api.instruments.EtoroMarketDataProvider") as MockProv,
        ):
            mock_provider = MagicMock()
            mock_provider.__enter__.return_value = mock_provider
            mock_provider.get_intraday_candles.return_value = [_bar()]
            MockProv.return_value = mock_provider

            r1 = client.get("/instruments/AAPL/intraday-candles?interval=OneMinute&count=100")
            r2 = client.get("/instruments/AAPL/intraday-candles?interval=OneMinute&count=100")
    finally:
        app.dependency_overrides.pop(get_conn, None)

    assert r1.status_code == 200
    assert r2.status_code == 200
    # Provider hit once across the two requests — second served from cache.
    assert mock_provider.get_intraday_candles.call_count == 1
