"""Tests for GET /instruments/{symbol}/summary.

After #498/#499 retired yfinance, the endpoint sources:

- Identity from the local ``instruments`` row.
- Price from the local ``quotes`` table.
- Market cap from SEC XBRL × ``quotes`` (via ``compute_market_cap``).
- Key stats from SEC ``financial_periods_ttm`` + dividend summary.

Tests stub the DB with the row shape the endpoint expects and verify
the response carries the right values from the right source.
"""

from __future__ import annotations

from collections.abc import Iterator
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def _make_conn(*, row: dict[str, object] | None) -> MagicMock:
    """Build a conn whose first cursor.fetchone returns ``row``.

    Subsequent cursors (used by ``_has_sec_cik`` etc.) return None
    so the SEC paths short-circuit cleanly.
    """
    conn_mock = MagicMock()
    fetched = [row, None, None, None, None]
    cursors_iter = iter(fetched)

    def _next_cursor(*_args: object, **_kwargs: object) -> MagicMock:
        cur = MagicMock()
        cur.__enter__.return_value = cur
        cur.__exit__.return_value = None
        try:
            cur.fetchone.return_value = next(cursors_iter)
        except StopIteration:
            cur.fetchone.return_value = None
        return cur

    conn_mock.cursor.side_effect = _next_cursor
    return conn_mock


def _install_conn(conn: MagicMock) -> None:
    from app.db import get_conn

    def _override() -> Iterator[MagicMock]:
        yield conn

    app.dependency_overrides[get_conn] = _override


def _clear_conn() -> None:
    from app.db import get_conn

    app.dependency_overrides.pop(get_conn, None)


@pytest.fixture
def db_conn() -> Iterator[MagicMock]:
    """Yields a default conn (404 row). Override ``conn.cursor.side_effect``
    in individual tests when the row should be present."""
    conn_mock = _make_conn(row=None)
    _install_conn(conn_mock)
    try:
        yield conn_mock
    finally:
        _clear_conn()


def test_unknown_symbol_returns_404(client: TestClient, db_conn: MagicMock) -> None:
    resp = client.get("/instruments/NOTREAL/summary")
    assert resp.status_code == 404


def test_happy_path_with_quote_and_no_sec(client: TestClient) -> None:
    """Happy path: DB row + quotes row, no SEC coverage. Price comes
    from ``quotes.last``; identity from the local row; market cap +
    key stats null because no SEC."""
    row = {
        "instrument_id": 100000,
        "symbol": "BTC",
        "company_name": "Bitcoin",
        "exchange": "8",
        "currency": "USD",
        "sector": None,
        "industry": None,
        "country": None,
        "is_tradable": True,
        "coverage_tier": 3,
        "bid": Decimal("60000.50"),
        "ask": Decimal("60001.00"),
        "last": Decimal("60000.75"),
    }
    conn = _make_conn(row=row)

    _install_conn(conn)
    try:
        resp = client.get("/instruments/BTC/summary")
    finally:
        _clear_conn()

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["instrument_id"] == 100000
    assert body["identity"]["symbol"] == "BTC"
    assert body["identity"]["display_name"] == "Bitcoin"
    assert body["identity"]["currency"] == "USD"
    # No yfinance fill-in for sector/industry/country.
    assert body["identity"]["sector"] is None
    assert body["identity"]["industry"] is None
    # Price reflects ``quotes.last``, not yfinance.
    assert body["price"]["current"] == "60000.75"
    assert body["price"]["currency"] == "USD"
    # Day change + 52w stay null until SEC-derived computations land.
    assert body["price"]["day_change"] is None
    assert body["price"]["week_52_high"] is None
    # No SEC → key_stats unavailable; market cap stays null.
    assert body["key_stats"] is None
    assert body["identity"]["market_cap"] is None
    # Source map reflects the post-retire shape.
    assert body["source"]["identity"] == "local_db"
    assert body["source"]["price"] == "quotes"
    assert body["source"]["key_stats"] == "unavailable"


def test_no_quote_returns_null_price_block(client: TestClient) -> None:
    """When the quotes row is missing (eToro WS hasn't pushed yet),
    the price block is null. Frontend renders '—' rather than reaching
    for a non-canonical source."""
    row = {
        "instrument_id": 100050,
        "symbol": "LRC",
        "company_name": "Loopring",
        "exchange": "8",
        "currency": "USD",
        "sector": None,
        "industry": None,
        "country": None,
        "is_tradable": True,
        "coverage_tier": 3,
        "bid": None,
        "ask": None,
        "last": None,
    }
    conn = _make_conn(row=row)

    _install_conn(conn)
    try:
        resp = client.get("/instruments/LRC/summary")
    finally:
        _clear_conn()

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["price"] is None
    assert body["source"]["price"] == "unavailable"


def test_bid_fallback_when_last_missing(client: TestClient) -> None:
    """Some instruments only publish bid/ask; ``last`` is null. The
    endpoint falls back to ``bid`` so the operator sees a number."""
    row = {
        "instrument_id": 100,
        "symbol": "FOO",
        "company_name": "Foo Inc",
        "exchange": "NYSE",
        "currency": "USD",
        "sector": None,
        "industry": None,
        "country": None,
        "is_tradable": True,
        "coverage_tier": 2,
        "bid": Decimal("12.34"),
        "ask": Decimal("12.36"),
        "last": None,
    }
    conn = _make_conn(row=row)

    _install_conn(conn)
    try:
        resp = client.get("/instruments/FOO/summary")
    finally:
        _clear_conn()

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["price"]["current"] == "12.34"


def test_local_company_name_authoritative(client: TestClient) -> None:
    """Local DB company_name is the only source — no yfinance display
    fallback can override it."""
    row = {
        "instrument_id": 1,
        "symbol": "AAPL",
        "company_name": "Apple Inc.",
        "exchange": "NMS",
        "currency": "USD",
        "sector": "Technology",
        "industry": "Consumer Electronics",
        "country": "United States",
        "is_tradable": True,
        "coverage_tier": 1,
        "bid": Decimal("180.00"),
        "ask": Decimal("180.05"),
        "last": Decimal("180.02"),
    }
    conn = _make_conn(row=row)

    _install_conn(conn)
    try:
        resp = client.get("/instruments/aapl/summary")  # case-insensitive
    finally:
        _clear_conn()

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["identity"]["display_name"] == "Apple Inc."
    assert body["identity"]["symbol"] == "AAPL"
    assert body["identity"]["sector"] == "Technology"
    assert body["identity"]["industry"] == "Consumer Electronics"


def test_sec_path_populates_key_stats() -> None:
    """When ``_has_sec_cik`` is True and ``_fetch_local_fundamentals``
    returns SEC values, ``_build_local_stats`` constructs the key
    stats block from SEC data only."""
    from app.api.instruments import _build_local_stats

    local = {
        "eps": Decimal("6.00"),
        "book_value": Decimal("4.00"),
        "shares_outstanding": Decimal("15000000000"),
        "net_debt": None,
        "debt": Decimal("100000000000"),
        "net_income": Decimal("100000000000"),
        "shareholders_equity": Decimal("60000000000"),
        "total_assets": Decimal("350000000000"),
        "total_liabilities": None,
        "revenue": Decimal("400000000000"),
    }
    stats = _build_local_stats(
        local,
        current_price=Decimal("180.00"),
        dividend_yield=Decimal("0.5"),
    )
    assert stats is not None
    # pe = 180 / 6 = 30
    assert stats.pe_ratio == Decimal("30")
    # pb = 180 / 4 = 45
    assert stats.pb_ratio == Decimal("45")
    # debt/equity = 100B / 60B
    assert stats.debt_to_equity is not None
    # roe = 100B / 60B
    assert stats.roe is not None
    # roa = 100B / 350B
    assert stats.roa is not None
    # Dividend yield from #426 dividend summary, not yfinance.
    assert stats.dividend_yield == Decimal("0.5")
    assert stats.field_source is not None
    assert stats.field_source["pe_ratio"] == "sec_xbrl"
    # Dividend yield carries its own provenance — sourced from
    # instrument_dividend_summary, NOT raw XBRL. Prevention assert
    # for the mislabel Codex caught on the first PR round.
    assert stats.field_source["dividend_yield"] == "sec_dividend_summary"
    assert stats.field_source["dividend_yield"] != "sec_xbrl"
    # Fields the SEC pipeline doesn't yet derive — surfaced as
    # unavailable, never silently filled from elsewhere.
    assert stats.payout_ratio is None
    assert stats.field_source["payout_ratio"] == "unavailable"
    assert stats.revenue_growth_yoy is None
    assert stats.earnings_growth_yoy is None


def test_price_blocked_pe_pb_signal_when_price_missing() -> None:
    """When SEC EPS / book value exist but price is missing, the
    field_source for pe/pb says ``sec_xbrl_price_missing`` so the UI
    can render an actionable hint instead of an ambiguous '—'."""
    from app.api.instruments import _build_local_stats

    local = {
        "eps": Decimal("6.00"),
        "book_value": Decimal("4.00"),
        "shares_outstanding": None,
        "net_debt": None,
        "debt": None,
        "net_income": None,
        "shareholders_equity": None,
        "total_assets": None,
        "total_liabilities": None,
        "revenue": None,
    }
    stats = _build_local_stats(local, current_price=None, dividend_yield=None)
    assert stats is not None
    assert stats.pe_ratio is None
    assert stats.pb_ratio is None
    assert stats.field_source is not None
    assert stats.field_source["pe_ratio"] == "sec_xbrl_price_missing"
    assert stats.field_source["pb_ratio"] == "sec_xbrl_price_missing"


def test_empty_input_returns_none() -> None:
    """No SEC data, no price, no dividend yield → no stats block
    rather than an all-null shell."""
    from app.api.instruments import _build_local_stats

    assert _build_local_stats({}, current_price=None, dividend_yield=None) is None


def test_id_override_pinned_lookup(client: TestClient) -> None:
    """?id=<n> pins the lookup to a specific instrument_id when a
    symbol collides across exchanges. The server still verifies the
    pinned id's symbol matches the path symbol."""
    row = {
        "instrument_id": 12220,
        "symbol": "BTC.US",
        "company_name": "Grayscale Bitcoin Mini Trust",
        "exchange": "5",
        "currency": "USD",
        "sector": None,
        "industry": None,
        "country": "United States",
        "is_tradable": True,
        "coverage_tier": 3,
        "bid": Decimal("34.30"),
        "ask": Decimal("34.40"),
        "last": Decimal("34.37"),
    }
    conn = _make_conn(row=row)
    _install_conn(conn)
    try:
        resp = client.get("/instruments/BTC.US/summary?id=12220")
    finally:
        _clear_conn()
    assert resp.status_code == 200, resp.text
    assert resp.json()["instrument_id"] == 12220


def test_id_override_symbol_mismatch_returns_404(client: TestClient) -> None:
    """?id=<n> pinned to an instrument whose symbol does not match
    the path returns 404 — never silently surfaces a wrong-instrument
    response (regression guard for the #316 spec §2 contract)."""
    # The DB query has WHERE i.instrument_id = %(id)s AND UPPER(symbol) = %(symbol)s,
    # so a mismatch yields no row → handler raises 404.
    conn = _make_conn(row=None)
    _install_conn(conn)
    try:
        resp = client.get("/instruments/BTC/summary?id=12220")
    finally:
        _clear_conn()
    assert resp.status_code == 404


def test_dividend_only_partial_stats_surface(client: TestClient) -> None:
    """An instrument with a dividend yield but no SEC fundamentals
    row should still surface the yield in key_stats — without this,
    the dividend summary fetch is wasted (Codex round 2 finding)."""
    row = {
        "instrument_id": 1234,
        "symbol": "DIV",
        "company_name": "Dividend Co",
        "exchange": "NYSE",
        "currency": "USD",
        "sector": None,
        "industry": None,
        "country": "United States",
        "is_tradable": True,
        "coverage_tier": 2,
        "bid": Decimal("50"),
        "ask": Decimal("50.10"),
        "last": Decimal("50.05"),
    }
    conn = _make_conn(row=row)
    _install_conn(conn)

    # _has_sec_cik returns False for this instrument so the SEC
    # fundamentals path short-circuits. The dividend summary fetch
    # uses its own conn cursor — patch the service-level helper to
    # return a yield without going through DB.
    from unittest.mock import patch

    from app.services.dividends import DividendSummary

    div_summary = DividendSummary(
        has_dividend=True,
        ttm_dps=None,
        ttm_dividends_paid=None,
        ttm_yield_pct=Decimal("3.50"),
        latest_dps=None,
        latest_dividend_at=None,
        dividend_streak_q=4,
        dividend_currency="USD",
    )
    with patch("app.services.dividends.get_dividend_summary", return_value=div_summary):
        try:
            resp = client.get("/instruments/DIV/summary")
        finally:
            _clear_conn()

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["key_stats"] is not None
    assert body["key_stats"]["dividend_yield"] == "3.50"
    # Block-level source tag must NOT mislabel a dividend-only block
    # as ``sec_xbrl`` — the dividend summary is its own provenance
    # (Codex review on PR for #499).
    assert body["source"]["key_stats"] == "sec_dividend_summary"
    assert body["key_stats"]["field_source"]["dividend_yield"] == "sec_dividend_summary"
