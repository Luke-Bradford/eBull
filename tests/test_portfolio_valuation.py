"""Tests for portfolio valuation hierarchy: quote → daily_close → cost_basis."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from app.api.portfolio import _parse_position


def _row(
    *,
    instrument_id: int = 1,
    symbol: str = "AAPL",
    company_name: str = "Apple Inc.",
    currency: str = "USD",
    open_date: date | None = date(2024, 1, 1),
    avg_cost: float = 150.0,
    current_units: float = 10.0,
    cost_basis: float = 1500.0,
    source: str = "broker_sync",
    updated_at: datetime = datetime(2026, 4, 13, 12, 0, tzinfo=UTC),
    last: float | None = None,
    daily_close: float | None = None,
) -> dict[str, object]:
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
        "daily_close": daily_close,
    }


class TestValuationHierarchy:
    """Verify the three-tier pricing fallback: quote → daily_close → cost_basis."""

    def test_quote_is_primary(self) -> None:
        """When quote.last exists, use it regardless of daily_close."""
        row = _row(current_units=10.0, cost_basis=1500.0, last=160.0, daily_close=155.0)
        rates: dict[tuple[str, str], Decimal] = {}
        pos = _parse_position(row, "USD", rates)

        assert pos.valuation_source == "quote"
        assert pos.market_value == 10.0 * 160.0
        assert pos.unrealized_pnl == (10.0 * 160.0) - 1500.0

    def test_daily_close_is_secondary(self) -> None:
        """When no quote but daily_close exists, use daily_close."""
        row = _row(current_units=10.0, cost_basis=1500.0, last=None, daily_close=155.0)
        rates: dict[tuple[str, str], Decimal] = {}
        pos = _parse_position(row, "USD", rates)

        assert pos.valuation_source == "daily_close"
        assert pos.market_value == 10.0 * 155.0
        assert pos.unrealized_pnl == (10.0 * 155.0) - 1500.0

    def test_cost_basis_is_fallback(self) -> None:
        """When neither quote nor daily_close exists, fall back to cost_basis."""
        row = _row(current_units=10.0, cost_basis=1500.0, last=None, daily_close=None)
        rates: dict[tuple[str, str], Decimal] = {}
        pos = _parse_position(row, "USD", rates)

        assert pos.valuation_source == "cost_basis"
        assert pos.market_value == 1500.0
        assert pos.unrealized_pnl == 0.0

    def test_fx_conversion_applied_to_daily_close(self) -> None:
        """Daily close values are converted to display currency."""
        row = _row(currency="USD", current_units=10.0, cost_basis=1500.0, daily_close=155.0)
        rates = {("USD", "GBP"): Decimal("0.78")}
        pos = _parse_position(row, "GBP", rates)

        assert pos.valuation_source == "daily_close"
        # market_value = 10 * 155 * 0.78 = 1209.0
        expected_mv = float(Decimal("1550.0") * Decimal("0.78"))
        assert abs(pos.market_value - expected_mv) < 0.01

    def test_zero_last_price_is_not_none(self) -> None:
        """A quote.last of 0.0 is a valid price, not a missing value.

        This matters for instruments that can trade at very low prices.
        parse_optional_float returns 0.0 for zero values, so the quote
        path should be chosen.
        """
        row = _row(current_units=100.0, cost_basis=500.0, last=0.0, daily_close=5.0)
        rates: dict[tuple[str, str], Decimal] = {}
        pos = _parse_position(row, "USD", rates)

        # parse_optional_float returns 0.0 for a zero value, which is not None
        assert pos.valuation_source == "quote"
        assert pos.market_value == 0.0
