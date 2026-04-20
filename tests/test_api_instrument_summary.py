"""Tests for GET /instruments/{symbol}/summary (Phase 2.2)."""

from __future__ import annotations

from collections.abc import Iterator
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from app.api.instruments import get_yfinance_provider
from app.main import app
from app.providers.implementations.yfinance_provider import (
    YFinanceKeyStats,
    YFinanceProfile,
    YFinanceQuote,
    YFinanceSnapshot,
)


def _install_stub_provider(provider: object) -> None:
    app.dependency_overrides[get_yfinance_provider] = lambda: provider


def _clear_provider_override() -> None:
    app.dependency_overrides.pop(get_yfinance_provider, None)


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture(autouse=True)
def cleanup() -> Iterator[None]:
    yield
    _clear_provider_override()


def _stub_profile(symbol: str = "AAPL") -> YFinanceProfile:
    return YFinanceProfile(
        symbol=symbol,
        display_name="Apple Inc.",
        sector="Technology",
        industry="Consumer Electronics",
        exchange="NMS",
        country="United States",
        currency="USD",
        market_cap=Decimal("3000000000000"),
        employees=150_000,
        website="https://apple.com",
        long_business_summary="Makes iPhones.",
    )


def _stub_quote(symbol: str = "AAPL") -> YFinanceQuote:
    return YFinanceQuote(
        symbol=symbol,
        price=Decimal("200.50"),
        day_change=Decimal("1.50"),
        day_change_pct=Decimal("0.00753"),
        week_52_high=Decimal("250.00"),
        week_52_low=Decimal("140.00"),
        currency="USD",
    )


def _stub_stats(symbol: str = "AAPL") -> YFinanceKeyStats:
    return YFinanceKeyStats(
        symbol=symbol,
        pe_ratio=Decimal("28.5"),
        pb_ratio=Decimal("40.2"),
        dividend_yield=Decimal("0.005"),
        payout_ratio=Decimal("0.15"),
        roe=Decimal("1.5"),
        roa=Decimal("0.3"),
        debt_to_equity=Decimal("195.0"),
        revenue_growth_yoy=Decimal("0.08"),
        earnings_growth_yoy=Decimal("0.12"),
    )


def test_summary_unknown_symbol_returns_404(client: TestClient, monkeypatch) -> None:
    """Symbol not in the local instruments table must 404 without hitting yfinance."""
    stub_provider = MagicMock()
    stub_provider.get_snapshot.return_value = YFinanceSnapshot(profile=None, quote=None, key_stats=None)
    _install_stub_provider(stub_provider)

    # Force the DB lookup to return nothing so 404 is exercised without
    # depending on what's in the dev DB.
    def _empty_conn():
        conn_mock = MagicMock()
        cur_mock = MagicMock()
        cur_mock.__enter__.return_value = cur_mock
        cur_mock.fetchone.return_value = None
        conn_mock.cursor.return_value = cur_mock
        yield conn_mock

    from app.db import get_conn

    app.dependency_overrides[get_conn] = _empty_conn
    try:
        resp = client.get("/instruments/NOTREAL/summary")
    finally:
        app.dependency_overrides.pop(get_conn, None)
    assert resp.status_code == 404
    # yfinance must NOT be called for unknown symbols — the local DB is
    # authoritative for whether a symbol exists at all.
    stub_provider.get_snapshot.assert_not_called()


def test_summary_happy_path_merges_db_and_yfinance(client: TestClient) -> None:
    stub_provider = MagicMock()
    stub_provider.get_snapshot.return_value = YFinanceSnapshot(
        profile=_stub_profile("AAPL"),
        quote=_stub_quote("AAPL"),
        key_stats=_stub_stats("AAPL"),
    )
    _install_stub_provider(stub_provider)

    def _db_conn():
        conn_mock = MagicMock()
        cur_mock = MagicMock()
        cur_mock.__enter__.return_value = cur_mock
        cur_mock.fetchone.return_value = {
            "instrument_id": 42,
            "symbol": "AAPL",
            "company_name": "Apple Inc.",
            "exchange": None,
            "currency": None,
            "sector": None,
            "industry": None,
            "country": None,
            "is_tradable": True,
            "coverage_tier": 1,
        }
        conn_mock.cursor.return_value = cur_mock
        yield conn_mock

    from app.db import get_conn

    app.dependency_overrides[get_conn] = _db_conn
    try:
        resp = client.get("/instruments/AAPL/summary")
    finally:
        app.dependency_overrides.pop(get_conn, None)

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["instrument_id"] == 42
    assert body["is_tradable"] is True
    assert body["coverage_tier"] == 1

    # Identity merges: local DB wins if non-null, otherwise yfinance fills.
    assert body["identity"]["symbol"] == "AAPL"
    assert body["identity"]["display_name"] == "Apple Inc."
    # Sector etc. come from yfinance because the DB row had them as None.
    assert body["identity"]["sector"] == "Technology"
    assert body["identity"]["industry"] == "Consumer Electronics"
    assert body["identity"]["market_cap"] == "3000000000000"

    # Single .info call (not three): get_snapshot called once.
    stub_provider.get_snapshot.assert_called_once_with("AAPL")
    assert body["price"]["current"] == "200.50"
    assert body["key_stats"]["pe_ratio"] == "28.5"

    # Source map names the effective contributors.
    assert body["source"]["identity"] == "local_db+yfinance"
    assert body["source"]["price"] == "yfinance"
    assert body["source"]["key_stats"] == "yfinance"


def test_summary_yfinance_failure_returns_null_sections(client: TestClient) -> None:
    """When yfinance returns None for quote/stats, the UI should see null
    sections (not 500). Identity still renders from the local DB."""
    stub_provider = MagicMock()
    stub_provider.get_snapshot.return_value = YFinanceSnapshot(
        profile=None,
        quote=None,
        key_stats=None,
    )
    _install_stub_provider(stub_provider)

    def _db_conn():
        conn_mock = MagicMock()
        cur_mock = MagicMock()
        cur_mock.__enter__.return_value = cur_mock
        cur_mock.fetchone.return_value = {
            "instrument_id": 7,
            "symbol": "VOD.L",
            "company_name": "Vodafone Group PLC",
            "exchange": "LSE",
            "currency": "GBP",
            "sector": "Telecom",
            "industry": None,
            "country": "United Kingdom",
            "is_tradable": True,
            "coverage_tier": None,
        }
        conn_mock.cursor.return_value = cur_mock
        yield conn_mock

    from app.db import get_conn

    app.dependency_overrides[get_conn] = _db_conn
    try:
        resp = client.get("/instruments/vod.l/summary")  # case-insensitive
    finally:
        app.dependency_overrides.pop(get_conn, None)

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["identity"]["symbol"] == "VOD.L"
    assert body["identity"]["display_name"] == "Vodafone Group PLC"
    assert body["identity"]["sector"] == "Telecom"
    assert body["price"] is None
    assert body["key_stats"] is None
    assert body["source"]["price"] == "unavailable"
    assert body["source"]["key_stats"] == "unavailable"


def test_summary_local_company_name_beats_yfinance(client: TestClient) -> None:
    """Local DB company_name is authoritative — yfinance display_name must
    not overwrite it when both are present. Addresses Codex review P2."""
    stub_provider = MagicMock()
    stub_provider.get_snapshot.return_value = YFinanceSnapshot(
        profile=YFinanceProfile(
            symbol="AAPL",
            display_name="Apple (Yahoo-stale brand)",
            sector=None,
            industry=None,
            exchange=None,
            country=None,
            currency=None,
            market_cap=None,
            employees=None,
            website=None,
            long_business_summary=None,
        ),
        quote=None,
        key_stats=None,
    )
    _install_stub_provider(stub_provider)

    def _db_conn():
        conn_mock = MagicMock()
        cur_mock = MagicMock()
        cur_mock.__enter__.return_value = cur_mock
        cur_mock.fetchone.return_value = {
            "instrument_id": 1,
            "symbol": "AAPL",
            "company_name": "Apple Inc.",
            "exchange": "NMS",
            "currency": "USD",
            "sector": "Technology",
            "industry": None,
            "country": "United States",
            "is_tradable": True,
            "coverage_tier": 1,
        }
        conn_mock.cursor.return_value = cur_mock
        yield conn_mock

    from app.db import get_conn

    app.dependency_overrides[get_conn] = _db_conn
    try:
        resp = client.get("/instruments/AAPL/summary")
    finally:
        app.dependency_overrides.pop(get_conn, None)

    assert resp.status_code == 200
    body = resp.json()
    # Local DB wins over yfinance's display_name.
    assert body["identity"]["display_name"] == "Apple Inc."


def test_summary_numeric_symbol_routes_to_summary_not_detail(client: TestClient) -> None:
    """Numeric ticker symbols (e.g. Tokyo 7203) must route to /summary, not
    to the /{instrument_id} detail endpoint. The /summary path suffix
    differentiates the routes even when the symbol segment is all digits."""
    stub_provider = MagicMock()
    stub_provider.get_snapshot.return_value = YFinanceSnapshot(profile=None, quote=None, key_stats=None)
    _install_stub_provider(stub_provider)

    def _db_conn():
        conn_mock = MagicMock()
        cur_mock = MagicMock()
        cur_mock.__enter__.return_value = cur_mock
        cur_mock.fetchone.return_value = {
            "instrument_id": 99,
            "symbol": "7203",
            "company_name": "Toyota Motor Corp",
            "exchange": "TSE",
            "currency": "JPY",
            "sector": "Consumer Cyclical",
            "industry": None,
            "country": "Japan",
            "is_tradable": True,
            "coverage_tier": None,
        }
        conn_mock.cursor.return_value = cur_mock
        yield conn_mock

    from app.db import get_conn

    app.dependency_overrides[get_conn] = _db_conn
    try:
        resp = client.get("/instruments/7203/summary")
    finally:
        app.dependency_overrides.pop(get_conn, None)

    assert resp.status_code == 200
    body = resp.json()
    # InstrumentSummary response shape — NOT InstrumentDetail which has
    # external_identifiers / first_seen_at.
    assert "identity" in body
    assert "external_identifiers" not in body
    assert body["identity"]["symbol"] == "7203"


def test_summary_prefers_local_sec_xbrl_for_us_ticker(client: TestClient) -> None:
    """#357: a US ticker (primary SEC CIK present) with local
    fundamentals_snapshot + financial_periods data uses those values
    over yfinance, reports key_stats source='local_sec_xbrl+yfinance',
    and surfaces per-field provenance."""
    from decimal import Decimal
    from unittest.mock import MagicMock

    from app.db import get_conn

    stub_provider = MagicMock()
    stub_provider.get_snapshot.return_value = YFinanceSnapshot(
        profile=_stub_profile("AAPL"),
        quote=_stub_quote("AAPL"),
        key_stats=_stub_stats("AAPL"),  # yfinance has all stats
    )
    _install_stub_provider(stub_provider)

    def _db_conn() -> Iterator[MagicMock]:
        conn_mock = MagicMock()
        cur_mock = MagicMock()
        cur_mock.__enter__.return_value = cur_mock
        cur_mock.__exit__.return_value = None
        # Sequence: instrument lookup, has_sec_cik lookup, fundamentals_snapshot,
        # financial_periods.
        cur_mock.fetchone.side_effect = [
            {
                "instrument_id": 42,
                "symbol": "AAPL",
                "company_name": "Apple Inc.",
                "exchange": "NMS",
                "currency": "USD",
                "sector": "Technology",
                "industry": None,
                "country": "United States",
                "is_tradable": True,
                "coverage_tier": 1,
            },
            (1,),  # has_sec_cik
            {
                "eps": Decimal("6.50"),
                "book_value": Decimal("4.20"),
                "shares_outstanding": Decimal("15000000000"),
                "cash": Decimal("50000000000"),
                "debt": Decimal("120000000000"),
                "net_debt": Decimal("70000000000"),
                "revenue_ttm": Decimal("400000000000"),
            },
            {
                "net_income": Decimal("100000000000"),
                "shareholders_equity": Decimal("63000000000"),
                "total_assets": Decimal("350000000000"),
                "total_liabilities": Decimal("287000000000"),
                "revenue": Decimal("400000000000"),
            },
        ]
        conn_mock.cursor.return_value = cur_mock
        yield conn_mock

    app.dependency_overrides[get_conn] = _db_conn
    try:
        resp = client.get("/instruments/AAPL/summary")
    finally:
        app.dependency_overrides.pop(get_conn, None)

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["source"]["key_stats"] == "local_sec_xbrl+yfinance"

    ks = body["key_stats"]
    # PE = 200.50 / 6.50 ≈ 30.85 (overrides yfinance's 28.5)
    assert Decimal(ks["pe_ratio"]) == Decimal("200.50") / Decimal("6.50")
    # PB = 200.50 / 4.20 ≈ 47.74 (overrides yfinance's 40.2)
    assert Decimal(ks["pb_ratio"]) == Decimal("200.50") / Decimal("4.20")
    # debt/equity = 120B / 63B ≈ 1.904 (overrides yfinance's 195.0)
    assert Decimal(ks["debt_to_equity"]) == Decimal("120000000000") / Decimal("63000000000")
    # ROE = 100B / 63B ≈ 1.587
    assert Decimal(ks["roe"]) == Decimal("100000000000") / Decimal("63000000000")
    # Field source map reports SEC origin for computed fields,
    # yfinance for dividend/payout/growth.
    fs = ks["field_source"]
    assert fs["pe_ratio"] == "sec_xbrl"
    assert fs["pb_ratio"] == "sec_xbrl"
    assert fs["debt_to_equity"] == "sec_xbrl"
    assert fs["roe"] == "sec_xbrl"
    assert fs["roa"] == "sec_xbrl"
    assert fs["dividend_yield"] == "yfinance"
    assert fs["revenue_growth_yoy"] == "yfinance"


def test_summary_sec_preference_missing_local_falls_through_to_yfinance(
    client: TestClient,
) -> None:
    """A US ticker (CIK present) but no local fundamentals rows — must
    cleanly fall through to the pure-yfinance path."""
    from unittest.mock import MagicMock

    from app.db import get_conn

    stub_provider = MagicMock()
    stub_provider.get_snapshot.return_value = YFinanceSnapshot(
        profile=_stub_profile("AAPL"),
        quote=_stub_quote("AAPL"),
        key_stats=_stub_stats("AAPL"),
    )
    _install_stub_provider(stub_provider)

    def _db_conn() -> Iterator[MagicMock]:
        conn_mock = MagicMock()
        cur_mock = MagicMock()
        cur_mock.__enter__.return_value = cur_mock
        cur_mock.__exit__.return_value = None
        cur_mock.fetchone.side_effect = [
            {
                "instrument_id": 42,
                "symbol": "AAPL",
                "company_name": "Apple Inc.",
                "exchange": "NMS",
                "currency": "USD",
                "sector": "Technology",
                "industry": None,
                "country": "United States",
                "is_tradable": True,
                "coverage_tier": 1,
            },
            (1,),  # has_sec_cik → True
            None,  # no fundamentals_snapshot row
            None,  # no financial_periods row
        ]
        conn_mock.cursor.return_value = cur_mock
        yield conn_mock

    app.dependency_overrides[get_conn] = _db_conn
    try:
        resp = client.get("/instruments/AAPL/summary")
    finally:
        app.dependency_overrides.pop(get_conn, None)

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["source"]["key_stats"] == "yfinance"
    assert body["key_stats"]["pe_ratio"] == "28.5"  # yfinance value


def test_summary_empty_symbol_returns_400(client: TestClient) -> None:
    # Whitespace-only symbol should reject with 400 rather than a DB probe.
    stub_provider = MagicMock()
    stub_provider.get_snapshot.return_value = YFinanceSnapshot(profile=None, quote=None, key_stats=None)
    _install_stub_provider(stub_provider)

    def _db_conn():
        yield MagicMock()

    from app.db import get_conn

    app.dependency_overrides[get_conn] = _db_conn
    try:
        resp = client.get("/instruments/%20%20%20/summary")
    finally:
        app.dependency_overrides.pop(get_conn, None)
    assert resp.status_code == 400
    stub_provider.get_snapshot.assert_not_called()
