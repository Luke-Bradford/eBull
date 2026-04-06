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
from typing import Any
from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from app.db import get_conn
from app.main import app

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 4, 6, 12, 0, 0, tzinfo=UTC)


def _make_position_row(
    instrument_id: int = 1,
    symbol: str = "AAPL",
    company_name: str = "Apple Inc",
    open_date: date | None = date(2026, 1, 15),
    avg_cost: float | None = 180.00,
    current_units: float = 10.0,
    cost_basis: float = 1800.00,
    updated_at: datetime = _NOW,
    last: float | None = 190.00,
) -> dict[str, Any]:
    """Build a dict matching the positions+instruments+quotes joined query shape."""
    return {
        "instrument_id": instrument_id,
        "symbol": symbol,
        "company_name": company_name,
        "open_date": open_date,
        "avg_cost": avg_cost,
        "current_units": current_units,
        "cost_basis": cost_basis,
        "updated_at": updated_at,
        "last": last,
    }


def _make_cash_row(cash_balance: float | None = 5000.00) -> dict[str, Any]:
    return {"cash_balance": cash_balance}


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
    conn = _mock_conn(cursor_results)

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


# ---------------------------------------------------------------------------
# TestGetPortfolio
# ---------------------------------------------------------------------------


class TestGetPortfolio:
    """GET /portfolio — current positions with mark-to-market valuation."""

    def teardown_method(self) -> None:
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
