"""Tests for copy-trading position mark-to-market computation."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from app.api.copy_trading import _compute_position_mtm


def _pos_row(
    *,
    mirror_id: int = 1,
    position_id: int = 100,
    instrument_id: int = 42,
    symbol: str | None = "AAPL",
    company_name: str | None = "Apple Inc.",
    is_buy: bool = True,
    units: float = 10.0,
    amount: float = 1500.0,
    open_rate: float = 150.0,
    open_conversion_rate: float = 1.0,
    open_date_time: datetime = datetime(2026, 4, 1, 12, 0, tzinfo=UTC),
    quote_last: float | None = None,
    daily_close: float | None = None,
) -> dict[str, object]:
    return {
        "mirror_id": mirror_id,
        "position_id": position_id,
        "instrument_id": instrument_id,
        "symbol": symbol,
        "company_name": company_name,
        "is_buy": is_buy,
        "units": units,
        "amount": amount,
        "open_rate": open_rate,
        "open_conversion_rate": open_conversion_rate,
        "open_date_time": open_date_time,
        "quote_last": quote_last,
        "daily_close": daily_close,
    }


class TestPriceHierarchy:
    """Verify the three-tier pricing: quote → daily_close → open_rate."""

    def test_quote_is_primary(self) -> None:
        row = _pos_row(units=10, amount=1500, open_rate=150.0, quote_last=160.0, daily_close=155.0)
        rates: dict[tuple[str, str], Decimal] = {}
        pos = _compute_position_mtm(row, "USD", rates)

        assert pos.current_price is not None
        assert abs(pos.current_price - 160.0) < 0.01
        # P&L = 1 * 10 * (160 - 150) * 1.0 = 100
        assert abs(pos.unrealized_pnl - 100.0) < 0.01
        # market_value = 1500 + 100 = 1600
        assert abs(pos.market_value - 1600.0) < 0.01

    def test_daily_close_is_secondary(self) -> None:
        row = _pos_row(units=10, amount=1500, open_rate=150.0, quote_last=None, daily_close=155.0)
        rates: dict[tuple[str, str], Decimal] = {}
        pos = _compute_position_mtm(row, "USD", rates)

        assert pos.current_price is not None
        assert abs(pos.current_price - 155.0) < 0.01
        # P&L = 1 * 10 * (155 - 150) * 1.0 = 50
        assert abs(pos.unrealized_pnl - 50.0) < 0.01
        assert abs(pos.market_value - 1550.0) < 0.01

    def test_open_rate_is_fallback(self) -> None:
        row = _pos_row(units=10, amount=1500, open_rate=150.0, quote_last=None, daily_close=None)
        rates: dict[tuple[str, str], Decimal] = {}
        pos = _compute_position_mtm(row, "USD", rates)

        # No price signal — current_price is None, P&L is zero
        assert pos.current_price is None
        assert pos.unrealized_pnl == 0.0
        assert pos.market_value == 1500.0


class TestFxConversion:
    """Verify USD → display_currency conversion."""

    def test_converts_to_gbp(self) -> None:
        row = _pos_row(units=10, amount=1500, open_rate=150.0, daily_close=160.0)
        rates = {("USD", "GBP"): Decimal("0.75")}
        pos = _compute_position_mtm(row, "GBP", rates)

        # P&L in USD = 10 * (160-150) * 1.0 = 100
        # MV in USD = 1500 + 100 = 1600
        # MV in GBP = 1600 * 0.75 = 1200
        assert abs(pos.market_value - 1200.0) < 0.01
        assert abs(pos.unrealized_pnl - 75.0) < 0.01
        assert pos.current_price is not None
        # current_price = 160 * 1.0 (open_conversion_rate) * 0.75 = 120
        assert abs(pos.current_price - 120.0) < 0.01

    def test_no_conversion_when_display_is_usd(self) -> None:
        row = _pos_row(units=10, amount=1500, open_rate=150.0, daily_close=160.0)
        rates: dict[tuple[str, str], Decimal] = {}
        pos = _compute_position_mtm(row, "USD", rates)

        assert abs(pos.market_value - 1600.0) < 0.01


class TestShortPositions:
    """Verify short (is_buy=False) MTM direction."""

    def test_short_position_gains_on_price_drop(self) -> None:
        row = _pos_row(is_buy=False, units=10, amount=1500, open_rate=150.0, daily_close=140.0)
        rates: dict[tuple[str, str], Decimal] = {}
        pos = _compute_position_mtm(row, "USD", rates)

        # P&L = -1 * 10 * (140 - 150) * 1.0 = 100 (gain)
        assert abs(pos.unrealized_pnl - 100.0) < 0.01
        assert abs(pos.market_value - 1600.0) < 0.01

    def test_short_position_loses_on_price_rise(self) -> None:
        row = _pos_row(is_buy=False, units=10, amount=1500, open_rate=150.0, daily_close=160.0)
        rates: dict[tuple[str, str], Decimal] = {}
        pos = _compute_position_mtm(row, "USD", rates)

        # P&L = -1 * 10 * (160 - 150) * 1.0 = -100 (loss)
        assert abs(pos.unrealized_pnl - (-100.0)) < 0.01
        assert abs(pos.market_value - 1400.0) < 0.01


class TestOpenConversionRate:
    """Verify open_conversion_rate is applied correctly for non-USD instruments."""

    def test_non_usd_instrument(self) -> None:
        # GBP-denominated instrument: open_rate in GBP, conversion 1.25 (GBP→USD)
        row = _pos_row(
            units=100,
            amount=12500,
            open_rate=100.0,
            open_conversion_rate=1.25,
            daily_close=110.0,
        )
        rates: dict[tuple[str, str], Decimal] = {}
        pos = _compute_position_mtm(row, "USD", rates)

        # P&L = 1 * 100 * (110 - 100) * 1.25 = 1250
        assert abs(pos.unrealized_pnl - 1250.0) < 0.01
        assert abs(pos.market_value - 13750.0) < 0.01
