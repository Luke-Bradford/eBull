"""
Unit tests for market data normalisation, feature computation, and spread checks.

No network calls, no database — all tests use in-memory fixtures.
"""

from datetime import date
from decimal import Decimal

import pytest

from app.providers.implementations.etoro import (
    _normalise_candle,
    _normalise_candles,
    _normalise_quote,
)
from app.providers.market_data import OHLCVBar, Quote
from app.services.market_data import (
    DEFAULT_MAX_SPREAD_PCT,
    _compute_rolling_returns,
    _compute_volatility_30d,
    compute_spread_pct,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

FIXTURE_CANDLE_CAMEL = {
    "Date": "2024-06-15T00:00:00",
    "Open": "185.00",
    "High": "187.50",
    "Low": "184.20",
    "Close": "186.80",
    "Volume": "55000000",
}

FIXTURE_CANDLE_SNAKE = {
    "date": "2024-06-16",
    "open": "186.80",
    "high": "189.00",
    "low": "185.50",
    "close": "188.20",
    "volume": "48000000",
}

FIXTURE_CANDLES_RESPONSE = {
    "Candles": [
        FIXTURE_CANDLE_CAMEL,
        FIXTURE_CANDLE_SNAKE,
        {
            "Date": "2024-06-14T00:00:00",
            "Open": "183.00",
            "High": "185.10",
            "Low": "182.50",
            "Close": "185.00",
            "Volume": "60000000",
        },
    ]
}

FIXTURE_QUOTE_CAMEL = {
    "Bid": "186.50",
    "Ask": "186.70",
    "Last": "186.60",
    "Time": "2024-06-17T14:30:00Z",
}

FIXTURE_QUOTE_WRAPPED = {
    "quotes": [
        {
            "bid": "186.50",
            "ask": "186.70",
            "last": "186.60",
            "timestamp": "2024-06-17T14:30:00Z",
        }
    ]
}


# ---------------------------------------------------------------------------
# Candle normalisation
# ---------------------------------------------------------------------------


class TestNormaliseCandle:
    def test_camel_case_fields(self) -> None:
        bar = _normalise_candle("AAPL", FIXTURE_CANDLE_CAMEL)
        assert bar is not None
        assert bar.symbol == "AAPL"
        assert bar.price_date == date(2024, 6, 15)
        assert bar.open == Decimal("185.00")
        assert bar.high == Decimal("187.50")
        assert bar.low == Decimal("184.20")
        assert bar.close == Decimal("186.80")
        assert bar.volume == 55000000

    def test_snake_case_fields(self) -> None:
        bar = _normalise_candle("AAPL", FIXTURE_CANDLE_SNAKE)
        assert bar is not None
        assert bar.price_date == date(2024, 6, 16)
        assert bar.close == Decimal("188.20")

    def test_missing_close_returns_none(self) -> None:
        item = {**FIXTURE_CANDLE_CAMEL}
        del item["Close"]
        assert _normalise_candle("AAPL", item) is None

    def test_missing_date_returns_none(self) -> None:
        item = {**FIXTURE_CANDLE_CAMEL}
        del item["Date"]
        assert _normalise_candle("AAPL", item) is None

    def test_zero_volume_becomes_none(self) -> None:
        item = {**FIXTURE_CANDLE_CAMEL, "Volume": "0"}
        bar = _normalise_candle("AAPL", item)
        assert bar is not None
        assert bar.volume is None

    def test_absent_volume_becomes_none(self) -> None:
        item = {k: v for k, v in FIXTURE_CANDLE_CAMEL.items() if k != "Volume"}
        bar = _normalise_candle("AAPL", item)
        assert bar is not None
        assert bar.volume is None

    def test_returns_ohlcv_bar(self) -> None:
        bar = _normalise_candle("AAPL", FIXTURE_CANDLE_CAMEL)
        assert isinstance(bar, OHLCVBar)


class TestNormaliseCandles:
    def test_sorted_oldest_first(self) -> None:
        bars = _normalise_candles("AAPL", FIXTURE_CANDLES_RESPONSE)
        assert len(bars) == 3
        assert bars[0].price_date < bars[1].price_date < bars[2].price_date

    def test_snake_case_response_shape(self) -> None:
        raw = {"candles": [FIXTURE_CANDLE_SNAKE]}
        bars = _normalise_candles("AAPL", raw)
        assert len(bars) == 1
        assert bars[0].price_date == date(2024, 6, 16)

    def test_empty_list(self) -> None:
        assert _normalise_candles("AAPL", {"Candles": []}) == []

    def test_non_dict_response_raises(self) -> None:
        with pytest.raises(ValueError, match="Expected dict"):
            _normalise_candles("AAPL", ["not", "a", "dict"])

    def test_bad_items_skipped(self) -> None:
        raw = {
            "Candles": [
                FIXTURE_CANDLE_CAMEL,
                {"Date": "2024-06-16"},  # missing OHLC → skipped
                "not a dict",  # not a dict → skipped
            ]
        }
        bars = _normalise_candles("AAPL", raw)
        assert len(bars) == 1


# ---------------------------------------------------------------------------
# Quote normalisation
# ---------------------------------------------------------------------------


class TestNormaliseQuote:
    def test_camel_case_top_level(self) -> None:
        quote = _normalise_quote("AAPL", FIXTURE_QUOTE_CAMEL)
        assert quote is not None
        assert quote.symbol == "AAPL"
        assert quote.bid == Decimal("186.50")
        assert quote.ask == Decimal("186.70")
        assert quote.last == Decimal("186.60")

    def test_wrapped_quotes_list(self) -> None:
        quote = _normalise_quote("AAPL", FIXTURE_QUOTE_WRAPPED)
        assert quote is not None
        assert quote.bid == Decimal("186.50")

    def test_missing_bid_returns_none(self) -> None:
        item = {k: v for k, v in FIXTURE_QUOTE_CAMEL.items() if k != "Bid"}
        assert _normalise_quote("AAPL", item) is None

    def test_missing_ask_returns_none(self) -> None:
        item = {k: v for k, v in FIXTURE_QUOTE_CAMEL.items() if k != "Ask"}
        assert _normalise_quote("AAPL", item) is None

    def test_non_dict_returns_none(self) -> None:
        assert _normalise_quote("AAPL", ["not", "a", "dict"]) is None

    def test_returns_quote(self) -> None:
        quote = _normalise_quote("AAPL", FIXTURE_QUOTE_CAMEL)
        assert isinstance(quote, Quote)


# ---------------------------------------------------------------------------
# Rolling returns
# ---------------------------------------------------------------------------


def _make_prices(closes: list[float], start: date | None = None) -> list[tuple[date, Decimal]]:
    """Build a prices list from a sequence of closes, one per day from start."""
    base = start or date(2024, 1, 2)
    return [(date.fromordinal(base.toordinal() + i), Decimal(str(c))) for i, c in enumerate(closes)]


class TestComputeRollingReturns:
    def test_1w_return_correct(self) -> None:
        # 8 prices spanning 7 intervals: anchor at index 0 (100), latest at index 7 (110)
        # target_date = latest_date - 7, which falls on index 0 → return = (110/100) - 1 = 0.10
        prices = _make_prices([100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 110.0])
        results = _compute_rolling_returns(prices)
        r1w = results["return_1w"]
        assert r1w is not None
        assert abs(float(r1w) - 0.10) < 0.001

    def test_insufficient_history_returns_none(self) -> None:
        # Only 3 days — not enough for any window
        prices = _make_prices([100.0, 101.0, 102.0])
        results = _compute_rolling_returns(prices)
        assert results["return_1m"] is None
        assert results["return_3m"] is None
        assert results["return_1y"] is None

    def test_empty_prices_all_none(self) -> None:
        results = _compute_rolling_returns([])
        assert all(v is None for v in results.values())

    def test_flat_prices_return_zero(self) -> None:
        # 40 days all at 100 → all returns should be ~0
        prices = _make_prices([100.0] * 40)
        results = _compute_rolling_returns(prices)
        r1m = results["return_1m"]
        assert r1m is not None
        assert abs(float(r1m)) < 0.001

    def test_negative_return(self) -> None:
        # 40 days: starts at 100, ends at 90
        closes = [100.0] + [100.0] * 38 + [90.0]
        prices = _make_prices(closes)
        results = _compute_rolling_returns(prices)
        r1m = results["return_1m"]
        assert r1m is not None
        assert float(r1m) < 0


# ---------------------------------------------------------------------------
# Volatility
# ---------------------------------------------------------------------------


class TestComputeVolatility30d:
    def test_flat_prices_near_zero_volatility(self) -> None:
        prices = _make_prices([100.0] * 35)
        vol = _compute_volatility_30d(prices)
        assert vol is not None
        assert float(vol) < 0.01  # flat prices → near-zero volatility

    def test_insufficient_history_returns_none(self) -> None:
        prices = _make_prices([100.0, 101.0, 102.0])
        assert _compute_volatility_30d(prices) is None

    def test_empty_prices_returns_none(self) -> None:
        assert _compute_volatility_30d([]) is None

    def test_volatile_prices_higher_than_flat(self) -> None:
        flat = _make_prices([100.0] * 35)
        volatile = _make_prices([100.0 + (i % 5) * 5.0 for i in range(35)])
        vol_flat = _compute_volatility_30d(flat)
        vol_volatile = _compute_volatility_30d(volatile)
        assert vol_flat is not None
        assert vol_volatile is not None
        assert vol_volatile > vol_flat

    def test_returns_decimal(self) -> None:
        prices = _make_prices([100.0 + i * 0.5 for i in range(35)])
        vol = _compute_volatility_30d(prices)
        assert isinstance(vol, Decimal)


# ---------------------------------------------------------------------------
# Spread check
# ---------------------------------------------------------------------------


class TestComputeSpreadPct:
    def test_normal_spread(self) -> None:
        spread = compute_spread_pct(Decimal("186.50"), Decimal("186.70"))
        assert spread is not None
        # spread = 0.20, mid = 186.60 → 0.20/186.60*100 ≈ 0.107%
        assert abs(float(spread) - 0.107) < 0.001

    def test_zero_mid_returns_none(self) -> None:
        assert compute_spread_pct(Decimal("0"), Decimal("0")) is None

    def test_wide_spread_exceeds_default_threshold(self) -> None:
        # bid=100, ask=103 → spread=3%, mid=101.5 → spread_pct ≈ 2.96% > 1%
        spread = compute_spread_pct(Decimal("100"), Decimal("103"))
        assert spread is not None
        assert spread > DEFAULT_MAX_SPREAD_PCT

    def test_tight_spread_within_default_threshold(self) -> None:
        # bid=100, ask=100.50 → spread=0.5%, mid=100.25 → spread_pct ≈ 0.499% < 1%
        spread = compute_spread_pct(Decimal("100"), Decimal("100.50"))
        assert spread is not None
        assert spread < DEFAULT_MAX_SPREAD_PCT
