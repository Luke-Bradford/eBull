"""
Tests for app.api.portfolio — GET /portfolio endpoint.

Test strategy:
  Mock DB via FastAPI dependency override (same pattern as test_api_instruments).
  The ``get_conn`` dependency is replaced with a mock connection that returns
  ``dict_row``-style dicts.

Structure:
  - TestGetPortfolio — happy path, empty positions, no cash, quote fallback,
    ordering, AUM calculation
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from app.db import get_conn
from app.main import app
from app.services.runtime_config import RuntimeConfig

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 4, 6, 12, 0, 0, tzinfo=UTC)


def _make_position_row(
    instrument_id: int = 1,
    symbol: str = "AAPL",
    company_name: str = "Apple Inc",
    currency: str | None = "USD",
    open_date: date | None = date(2026, 1, 15),
    avg_cost: float | None = 180.00,
    current_units: float = 10.0,
    cost_basis: float = 1800.00,
    source: str = "ebull",
    updated_at: datetime = _NOW,
    last: float | None = 190.00,
) -> dict[str, Any]:
    """Build a dict matching the positions+instruments+quotes joined query shape."""
    return {
        "instrument_id": instrument_id,
        "symbol": symbol,
        "company_name": company_name,
        "currency": currency,
        "open_date": open_date,
        "avg_cost": avg_cost,
        "current_units": current_units,
        "cost_basis": cost_basis,
        "source": source,
        "updated_at": updated_at,
        "last": last,
    }


def _make_cash_row(cash_balance: float | None = 5000.00) -> dict[str, Any]:
    return {"cash_balance": cash_balance}


def _make_mirror_row(
    mirror_id: int = 1001,
    parent_username: str = "thomaspj",
    active: bool = True,
    initial_investment: float = 10000.0,
    deposit_summary: float = 0.0,
    withdrawal_summary: float = 0.0,
    available_amount: float = 500.0,
    closed_positions_net_profit: float = 0.0,
    positions_mv: float = 0.0,
    position_count: int = 0,
    started_copy_date: datetime = _NOW,
) -> dict[str, Any]:
    """Build a dict matching the load_mirror_breakdowns query shape."""
    return {
        "mirror_id": mirror_id,
        "parent_username": parent_username,
        "active": active,
        "initial_investment": initial_investment,
        "deposit_summary": deposit_summary,
        "withdrawal_summary": withdrawal_summary,
        "available_amount": available_amount,
        "closed_positions_net_profit": closed_positions_net_profit,
        "positions_mv": positions_mv,
        "position_count": position_count,
        "started_copy_date": started_copy_date,
    }


def _mock_conn(cursor_results: list[list[dict[str, Any]]]) -> MagicMock:
    """Build a mock psycopg.Connection.

    ``cursor_results`` is a list of result sets, one per ``cur.execute()`` call.
    """
    cur = MagicMock()
    result_iter = iter(cursor_results)

    def _on_execute(*_args: Any, **_kwargs: Any) -> None:
        rows = next(result_iter)
        cur.fetchone.return_value = rows[0] if rows else None
        cur.fetchall.return_value = rows

    cur.execute.side_effect = _on_execute
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)

    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn


def _with_conn(cursor_results: list[list[dict[str, Any]]]) -> MagicMock:
    # The endpoint runs four DB queries in order:
    #   1. positions  2. cash  3. broker_positions  4. mirror_breakdowns
    # Existing callers supply [positions, cash] only — pad with empty
    # broker_positions and mirror_breakdowns results.
    padded = list(cursor_results)
    while len(padded) < 4:
        padded.append([])
    conn = _mock_conn(padded)

    def _override() -> Iterator[MagicMock]:
        yield conn

    app.dependency_overrides[get_conn] = _override
    return conn


def _cleanup() -> None:
    app.dependency_overrides[get_conn] = _fallback_conn


def _fallback_conn() -> Iterator[MagicMock]:
    yield _mock_conn([])


app.dependency_overrides.setdefault(get_conn, _fallback_conn)

client = TestClient(app)

# Default RuntimeConfig for tests: display_currency="USD" means no conversion
# is applied, so existing test values pass through unchanged.
_DEFAULT_CONFIG = RuntimeConfig(
    enable_auto_trading=False,
    enable_live_trading=False,
    display_currency="USD",
    updated_at=_NOW,
    updated_by="test",
    reason="test",
)


# ---------------------------------------------------------------------------
# TestGetPortfolio
# ---------------------------------------------------------------------------


class TestGetPortfolio:
    """GET /portfolio — current positions with mark-to-market valuation."""

    def setup_method(self) -> None:
        # Patch FX/config service functions so existing tests (all USD) pass unchanged.
        self._patch_config = patch(
            "app.api.portfolio.get_runtime_config",
            return_value=_DEFAULT_CONFIG,
        )
        self._patch_fx_meta = patch(
            "app.api.portfolio.load_live_fx_rates_with_metadata",
            return_value={},
        )
        self._patch_config.start()
        self._patch_fx_meta.start()

    def teardown_method(self) -> None:
        self._patch_config.stop()
        self._patch_fx_meta.stop()
        _cleanup()

    def test_happy_path_with_quote(self) -> None:
        """Position with quote: market_value = units * last, unrealized_pnl computed."""
        pos = _make_position_row(current_units=10.0, cost_basis=1800.0, last=190.0)
        cash = _make_cash_row(5000.0)
        _with_conn([[pos], [cash]])

        resp = client.get("/portfolio")
        assert resp.status_code == 200
        body = resp.json()

        assert body["position_count"] == 1
        item = body["positions"][0]
        assert item["instrument_id"] == 1
        assert item["symbol"] == "AAPL"
        assert item["market_value"] == 1900.0  # 10 * 190
        assert item["unrealized_pnl"] == 100.0  # 1900 - 1800
        assert body["cash_balance"] == 5000.0
        assert body["total_aum"] == 6900.0  # 1900 + 5000

    def test_no_quote_falls_back_to_cost_basis(self) -> None:
        """No quote: market_value = cost_basis, unrealized_pnl = 0."""
        pos = _make_position_row(current_units=10.0, cost_basis=1800.0, last=None)
        cash = _make_cash_row(5000.0)
        _with_conn([[pos], [cash]])

        resp = client.get("/portfolio")
        assert resp.status_code == 200
        body = resp.json()

        item = body["positions"][0]
        assert item["market_value"] == 1800.0  # fallback to cost_basis
        assert item["unrealized_pnl"] == 0.0  # no price signal
        assert body["total_aum"] == 6800.0  # 1800 + 5000

    def test_empty_positions_returns_empty_list(self) -> None:
        cash = _make_cash_row(5000.0)
        _with_conn([[], [cash]])

        resp = client.get("/portfolio")
        assert resp.status_code == 200
        body = resp.json()

        assert body["positions"] == []
        assert body["position_count"] == 0
        assert body["total_aum"] == 5000.0
        assert body["cash_balance"] == 5000.0

    def test_no_cash_returns_null_cash_balance(self) -> None:
        """Empty cash_ledger: SUM returns NULL → cash_balance is null."""
        pos = _make_position_row(current_units=10.0, cost_basis=1800.0, last=190.0)
        _with_conn([[pos], [_make_cash_row(None)]])

        resp = client.get("/portfolio")
        assert resp.status_code == 200
        body = resp.json()

        assert body["cash_balance"] is None
        assert body["total_aum"] == 1900.0  # positions only

    def test_empty_portfolio_and_no_cash(self) -> None:
        _with_conn([[], [_make_cash_row(None)]])

        resp = client.get("/portfolio")
        assert resp.status_code == 200
        body = resp.json()

        assert body["positions"] == []
        assert body["position_count"] == 0
        assert body["cash_balance"] is None
        assert body["total_aum"] == 0.0

    def test_ordering_by_market_value_desc(self) -> None:
        """Positions ordered by market_value DESC, instrument_id ASC."""
        small = _make_position_row(
            instrument_id=1,
            symbol="SMALL",
            company_name="Small Co",
            current_units=1.0,
            cost_basis=100.0,
            last=100.0,
        )
        large = _make_position_row(
            instrument_id=2,
            symbol="LARGE",
            company_name="Large Co",
            current_units=100.0,
            cost_basis=10000.0,
            last=200.0,
        )
        cash = _make_cash_row(0.0)
        # DB returns in cost_basis order (small first) — endpoint re-sorts by market_value.
        _with_conn([[small, large], [cash]])

        resp = client.get("/portfolio")
        assert resp.status_code == 200
        body = resp.json()

        assert body["positions"][0]["symbol"] == "LARGE"  # 20000 market_value
        assert body["positions"][1]["symbol"] == "SMALL"  # 100 market_value

    def test_ordering_tiebreak_by_instrument_id(self) -> None:
        """Same market_value: ordered by instrument_id ASC."""
        a = _make_position_row(
            instrument_id=5,
            symbol="AAA",
            company_name="A Co",
            current_units=10.0,
            cost_basis=1000.0,
            last=100.0,
        )
        b = _make_position_row(
            instrument_id=2,
            symbol="BBB",
            company_name="B Co",
            current_units=10.0,
            cost_basis=1000.0,
            last=100.0,
        )
        _with_conn([[a, b], [_make_cash_row(0.0)]])

        resp = client.get("/portfolio")
        assert resp.status_code == 200
        body = resp.json()

        # Same market_value (1000), so instrument_id ASC: 2 before 5.
        assert body["positions"][0]["instrument_id"] == 2
        assert body["positions"][1]["instrument_id"] == 5

    def test_optional_fields_null(self) -> None:
        """open_date and avg_cost can be null."""
        pos = _make_position_row(open_date=None, avg_cost=None)
        _with_conn([[pos], [_make_cash_row(0.0)]])

        resp = client.get("/portfolio")
        assert resp.status_code == 200
        item = resp.json()["positions"][0]
        assert item["open_date"] is None
        assert item["avg_cost"] is None

    def test_negative_unrealized_pnl(self) -> None:
        """Position trading below cost basis shows negative P&L."""
        pos = _make_position_row(current_units=10.0, cost_basis=2000.0, last=150.0)
        _with_conn([[pos], [_make_cash_row(0.0)]])

        resp = client.get("/portfolio")
        assert resp.status_code == 200
        item = resp.json()["positions"][0]
        assert item["market_value"] == 1500.0  # 10 * 150
        assert item["unrealized_pnl"] == -500.0  # 1500 - 2000

    def test_open_date_serialised_as_iso_date(self) -> None:
        """open_date (DB DATE column) serialises as ISO-8601 string."""
        pos = _make_position_row(open_date=date(2026, 1, 15))
        _with_conn([[pos], [_make_cash_row(0.0)]])

        resp = client.get("/portfolio")
        assert resp.status_code == 200
        assert resp.json()["positions"][0]["open_date"] == "2026-01-15"

    def test_source_exposed_in_response(self) -> None:
        """source column (ebull vs broker_sync) is passed through to the API response."""
        ebull_pos = _make_position_row(instrument_id=1, symbol="AAPL", source="ebull")
        broker_pos = _make_position_row(instrument_id=2, symbol="MSFT", source="broker_sync")
        _with_conn([[ebull_pos, broker_pos], [_make_cash_row(0.0)]])

        resp = client.get("/portfolio")
        assert resp.status_code == 200
        items = resp.json()["positions"]
        by_symbol = {item["symbol"]: item for item in items}
        assert by_symbol["AAPL"]["source"] == "ebull"
        assert by_symbol["MSFT"]["source"] == "broker_sync"

    def test_zero_unit_positions_excluded_by_sql_filter(self) -> None:
        """Zero-unit positions are excluded via WHERE filter in the SQL query.

        The SQL has WHERE p.current_units > 0, so zero-unit rows never reach
        the endpoint. This test verifies the WHERE clause is present.
        """
        conn = _with_conn([[], [_make_cash_row(0.0)]])
        client.get("/portfolio")

        cur = conn.cursor.return_value
        positions_sql: str = cur.execute.call_args_list[0][0][0]
        assert "current_units > 0" in positions_sql

    def test_display_currency_in_response(self) -> None:
        """Response includes display_currency field from runtime_config."""
        _with_conn([[], [_make_cash_row(0.0)]])

        resp = client.get("/portfolio")
        assert resp.status_code == 200
        assert resp.json()["display_currency"] == "USD"

    def test_fx_rates_used_empty_when_same_currency(self) -> None:
        """No FX rates reported when display_currency matches all positions."""
        pos = _make_position_row(currency="USD")
        _with_conn([[pos], [_make_cash_row(5000.0)]])

        resp = client.get("/portfolio")
        assert resp.status_code == 200
        assert resp.json()["fx_rates_used"] == {}


# ---------------------------------------------------------------------------
# TestPortfolioFxConversion
# ---------------------------------------------------------------------------


_FX_QUOTED_AT = datetime(2026, 4, 13, 12, 0, 0, tzinfo=UTC)


class TestPortfolioFxConversion:
    """GET /portfolio — FX conversion to display_currency."""

    def setup_method(self) -> None:
        gbp_config = RuntimeConfig(
            enable_auto_trading=False,
            enable_live_trading=False,
            display_currency="GBP",
            updated_at=_NOW,
            updated_by="test",
            reason="test",
        )
        self._patch_config = patch(
            "app.api.portfolio.get_runtime_config",
            return_value=gbp_config,
        )
        self._patch_fx_meta = patch(
            "app.api.portfolio.load_live_fx_rates_with_metadata",
            return_value={
                ("USD", "GBP"): {
                    "rate": Decimal("0.78"),
                    "quoted_at": _FX_QUOTED_AT,
                },
            },
        )
        self._patch_config.start()
        self._patch_fx_meta.start()

    def teardown_method(self) -> None:
        self._patch_config.stop()
        self._patch_fx_meta.stop()
        _cleanup()

    def test_position_values_converted_to_gbp(self) -> None:
        """USD instrument with quote: market_value/cost_basis/pnl converted at 0.78."""
        pos = _make_position_row(
            currency="USD",
            current_units=10.0,
            cost_basis=1000.0,
            avg_cost=100.0,
            last=100.0,
        )
        _with_conn([[pos], [_make_cash_row(None)]])

        resp = client.get("/portfolio")
        assert resp.status_code == 200
        body = resp.json()

        item = body["positions"][0]
        # market_value: 10 * 100 = 1000 USD -> 780 GBP
        assert item["market_value"] == 780.0
        # cost_basis: 1000 USD -> 780 GBP
        assert item["cost_basis"] == 780.0
        # unrealized_pnl: (1000 - 1000) = 0 USD -> 0 GBP
        assert item["unrealized_pnl"] == 0.0
        # avg_cost: 100 USD -> 78 GBP
        assert item["avg_cost"] == 78.0

    def test_cash_balance_converted_to_gbp(self) -> None:
        """Cash balance (always USD for eToro) converted to display currency."""
        _with_conn([[], [_make_cash_row(5000.0)]])

        resp = client.get("/portfolio")
        assert resp.status_code == 200
        body = resp.json()

        # 5000 USD * 0.78 = 3900 GBP
        assert body["cash_balance"] == 3900.0

    def test_mirror_equity_converted_to_gbp(self) -> None:
        """Mirror equity (always USD for eToro) converted to display currency."""
        # available_amount=500 + positions_mv=1500 → mirror_equity=2000 USD.
        mirror = _make_mirror_row(available_amount=500.0, positions_mv=1500.0)
        _with_conn([[], [_make_cash_row(None)], [], [mirror]])

        resp = client.get("/portfolio")
        assert resp.status_code == 200
        body = resp.json()

        # 2000 USD * 0.78 = 1560 GBP
        assert body["mirror_equity"] == 1560.0

    def test_aum_computed_in_display_currency(self) -> None:
        """AUM is the sum of converted positions + converted cash + converted mirror."""
        pos = _make_position_row(
            currency="USD",
            current_units=10.0,
            cost_basis=1000.0,
            last=100.0,
        )
        # available_amount=500 + positions_mv=1500 → mirror_equity=2000 USD.
        mirror = _make_mirror_row(available_amount=500.0, positions_mv=1500.0)
        _with_conn([[pos], [_make_cash_row(5000.0)], [], [mirror]])

        resp = client.get("/portfolio")
        assert resp.status_code == 200
        body = resp.json()

        # market_value: 1000 * 0.78 = 780
        # cash: 5000 * 0.78 = 3900
        # mirror: 2000 * 0.78 = 1560
        # AUM = 780 + 3900 + 1560 = 6240
        assert body["total_aum"] == 6240.0

    def test_display_currency_in_response_gbp(self) -> None:
        """Response includes display_currency = GBP."""
        _with_conn([[], [_make_cash_row(0.0)]])

        resp = client.get("/portfolio")
        assert resp.status_code == 200
        assert resp.json()["display_currency"] == "GBP"

    def test_fx_rates_used_metadata(self) -> None:
        """fx_rates_used includes the USD rate and quoted_at when converting."""
        pos = _make_position_row(currency="USD")
        _with_conn([[pos], [_make_cash_row(0.0)]])

        resp = client.get("/portfolio")
        assert resp.status_code == 200
        body = resp.json()

        assert "USD" in body["fx_rates_used"]
        assert body["fx_rates_used"]["USD"]["rate"] == 0.78
        assert body["fx_rates_used"]["USD"]["quoted_at"] == "2026-04-13T12:00:00+00:00"

    def test_null_currency_falls_back_to_usd(self) -> None:
        """Instrument with NULL currency treated as USD (eToro default)."""
        pos = _make_position_row(
            currency=None,
            current_units=10.0,
            cost_basis=1000.0,
            last=100.0,
        )
        _with_conn([[pos], [_make_cash_row(None)]])

        resp = client.get("/portfolio")
        assert resp.status_code == 200
        item = resp.json()["positions"][0]

        # Falls back to USD, converted at 0.78: 1000 * 0.78 = 780
        assert item["market_value"] == 780.0

    def test_no_quote_converted_correctly(self) -> None:
        """Position without a quote: cost_basis fallback is also converted."""
        pos = _make_position_row(
            currency="USD",
            current_units=10.0,
            cost_basis=1800.0,
            last=None,
        )
        _with_conn([[pos], [_make_cash_row(None)]])

        resp = client.get("/portfolio")
        assert resp.status_code == 200
        item = resp.json()["positions"][0]

        # cost_basis fallback: 1800 * 0.78 = 1404
        assert item["market_value"] == 1404.0
        assert item["unrealized_pnl"] == 0.0

    def test_missing_fx_rate_returns_unconverted(self) -> None:
        """FxRateNotFound: position values returned unconverted, endpoint does not crash."""
        pos = _make_position_row(
            currency="EUR",
            current_units=5.0,
            cost_basis=500.0,
            avg_cost=100.0,
            last=120.0,
        )
        # No EUR→GBP rate in the mock — only USD→GBP exists.
        _with_conn([[pos], [_make_cash_row(None)]])

        resp = client.get("/portfolio")
        assert resp.status_code == 200
        body = resp.json()

        item = body["positions"][0]
        # No EUR→GBP rate → values stay in EUR (unconverted).
        assert item["market_value"] == 600.0  # 5 * 120
        assert item["cost_basis"] == 500.0
        assert item["unrealized_pnl"] == 100.0  # 600 - 500
        assert item["avg_cost"] == 100.0
        assert body["display_currency"] == "GBP"

    def test_fx_rates_used_no_spurious_usd_when_mirror_zero(self) -> None:
        """fx_rates_used omits USD when mirror_equity is 0 and no USD positions exist."""
        pos = _make_position_row(currency="GBP", current_units=10.0, cost_basis=1000.0, last=100.0)
        # No cash, mirror_equity = 0 (default third cursor result).
        _with_conn([[pos], [_make_cash_row(None)]])

        resp = client.get("/portfolio")
        assert resp.status_code == 200
        body = resp.json()

        # No USD positions, no cash, mirror_equity = 0 → USD should not appear.
        assert "USD" not in body["fx_rates_used"]


# ---------------------------------------------------------------------------
# TestPortfolioMirrors — mirrors field in portfolio response (#221)
# ---------------------------------------------------------------------------


class TestPortfolioMirrors:
    """GET /portfolio — mirrors list populated from load_mirror_breakdowns."""

    def setup_method(self) -> None:
        self._patch_config = patch(
            "app.api.portfolio.get_runtime_config",
            return_value=_DEFAULT_CONFIG,
        )
        self._patch_fx_meta = patch(
            "app.api.portfolio.load_live_fx_rates_with_metadata",
            return_value={},
        )
        self._patch_config.start()
        self._patch_fx_meta.start()

    def teardown_method(self) -> None:
        self._patch_config.stop()
        self._patch_fx_meta.stop()
        _cleanup()

    def test_mirrors_empty_when_no_mirrors(self) -> None:
        """No mirrors → mirrors list is empty, mirror_equity = 0."""
        _with_conn([[], [_make_cash_row(1000.0)]])

        resp = client.get("/portfolio")
        assert resp.status_code == 200
        body = resp.json()

        assert body["mirrors"] == []
        assert body["mirror_equity"] == 0.0

    def test_mirrors_populated_with_breakdown(self) -> None:
        """Mirror breakdown appears in the mirrors list with correct fields."""
        mirror = _make_mirror_row(
            mirror_id=1001,
            parent_username="thomaspj",
            initial_investment=10000.0,
            deposit_summary=2000.0,
            withdrawal_summary=500.0,
            available_amount=1000.0,
            positions_mv=12000.0,
            position_count=42,
        )
        _with_conn([[], [_make_cash_row(None)], [], [mirror]])

        resp = client.get("/portfolio")
        assert resp.status_code == 200
        body = resp.json()

        assert len(body["mirrors"]) == 1
        m = body["mirrors"][0]
        assert m["mirror_id"] == 1001
        assert m["parent_username"] == "thomaspj"
        assert m["active"] is True
        # funded = 10000 + 2000 - 500 = 11500
        assert m["funded"] == 11500.0
        # mirror_equity = 1000 + 12000 = 13000
        assert m["mirror_equity"] == 13000.0
        # unrealized_pnl = 13000 - 11500 = 1500
        assert m["unrealized_pnl"] == 1500.0
        assert m["position_count"] == 42

    def test_mirror_equity_equals_sum_of_mirrors(self) -> None:
        """Total mirror_equity = sum of individual mirror equities."""
        m1 = _make_mirror_row(
            mirror_id=1001,
            available_amount=500.0,
            positions_mv=1500.0,
        )
        m2 = _make_mirror_row(
            mirror_id=1002,
            parent_username="other",
            available_amount=300.0,
            positions_mv=700.0,
        )
        _with_conn([[], [_make_cash_row(None)], [], [m1, m2]])

        resp = client.get("/portfolio")
        assert resp.status_code == 200
        body = resp.json()

        assert len(body["mirrors"]) == 2
        sum_equity = sum(m["mirror_equity"] for m in body["mirrors"])
        assert body["mirror_equity"] == sum_equity

    def test_unrealized_pnl_excludes_realised_profits(self) -> None:
        """unrealized_pnl subtracts closed_positions_net_profit (#226)."""
        mirror = _make_mirror_row(
            initial_investment=10000.0,
            available_amount=3500.0,  # includes $500 realised profit + $3000 undeployed
            closed_positions_net_profit=500.0,
            positions_mv=7000.0,  # open positions at market value
        )
        _with_conn([[], [_make_cash_row(None)], [], [mirror]])

        resp = client.get("/portfolio")
        assert resp.status_code == 200
        m = resp.json()["mirrors"][0]

        # funded = 10000, equity = 3500 + 7000 = 10500
        assert m["mirror_equity"] == 10500.0
        assert m["funded"] == 10000.0
        # unrealized = total_return - realised = (10500 - 10000) - 500 = 0
        assert m["unrealized_pnl"] == 0.0
