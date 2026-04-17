"""
Unit tests for market data normalisation, feature computation, and spread checks.

No network calls, no database — all tests use in-memory fixtures.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest

from app.providers.implementations.etoro import (
    _normalise_candle,
    _normalise_candles,
    _normalise_instrument,
    _normalise_instruments,
    _normalise_rate,
    _normalise_rates,
)
from app.providers.market_data import OHLCVBar, Quote
from app.services.market_data import (
    _INCREMENTAL_FETCH_BARS,
    DEFAULT_MAX_SPREAD_PCT,
    _candles_are_fresh,
    _candles_fetch_count,
    _compute_and_store_features,
    _compute_rolling_returns,
    _compute_volatility_30d,
    _most_recent_trading_day,
    compute_spread_pct,
)

# ---------------------------------------------------------------------------
# Fixtures — real eToro API response shapes
# ---------------------------------------------------------------------------

FIXTURE_INSTRUMENT = {
    "instrumentID": 1001,
    "symbolFull": "AAPL",
    "instrumentDisplayName": "Apple",
    "exchangeID": 10,
    "stocksIndustryId": 42,
    "priceSource": "Nasdaq",
    "isInternalInstrument": False,
}

FIXTURE_INSTRUMENT_INTERNAL = {
    **FIXTURE_INSTRUMENT,
    "instrumentID": 9999,
    "isInternalInstrument": True,
}

FIXTURE_CANDLE = {
    "fromDate": "2024-06-15T00:00:00",
    "open": 185.00,
    "high": 187.50,
    "low": 184.20,
    "close": 186.80,
    "volume": 55000000,
}

FIXTURE_CANDLE_2 = {
    "fromDate": "2024-06-16T00:00:00",
    "open": 186.80,
    "high": 189.00,
    "low": 185.50,
    "close": 188.20,
    "volume": 48000000,
}

FIXTURE_CANDLE_3 = {
    "fromDate": "2024-06-14T00:00:00",
    "open": 183.00,
    "high": 185.10,
    "low": 182.50,
    "close": 185.00,
    "volume": 60000000,
}

# Real API candle response: nested { candles: [{ instrumentId, candles: [...] }] }
FIXTURE_CANDLES_RESPONSE = {
    "candles": [
        {
            "instrumentId": 1001,
            "candles": [FIXTURE_CANDLE_3, FIXTURE_CANDLE, FIXTURE_CANDLE_2],
        }
    ]
}

FIXTURE_RATE = {
    "instrumentID": 1001,
    "bid": 186.50,
    "ask": 186.70,
    "lastExecution": 186.60,
    "date": "2024-06-17T14:30:00Z",
}

FIXTURE_RATE_2 = {
    "instrumentID": 1002,
    "bid": 50.10,
    "ask": 50.30,
    "lastExecution": 50.20,
    "date": "2024-06-17T14:30:00Z",
}

FIXTURE_RATES_RESPONSE = {"rates": [FIXTURE_RATE, FIXTURE_RATE_2]}


# ---------------------------------------------------------------------------
# Instrument normalisation
# ---------------------------------------------------------------------------


class TestNormaliseInstrument:
    def test_valid_instrument(self) -> None:
        rec = _normalise_instrument(FIXTURE_INSTRUMENT)
        assert rec is not None
        assert rec.provider_id == "1001"
        assert rec.symbol == "AAPL"
        assert rec.company_name == "Apple"
        assert rec.exchange == "10"
        assert rec.sector == "42"
        assert rec.is_tradable is True

    def test_currency_is_none_without_enrichment(self) -> None:
        """currency is None — eToro instruments endpoint does not expose it."""
        rec = _normalise_instrument(FIXTURE_INSTRUMENT)
        assert rec is not None
        assert rec.currency is None

    def test_internal_instrument_skipped(self) -> None:
        assert _normalise_instrument(FIXTURE_INSTRUMENT_INTERNAL) is None

    def test_missing_id_returns_none(self) -> None:
        item = {k: v for k, v in FIXTURE_INSTRUMENT.items() if k != "instrumentID"}
        assert _normalise_instrument(item) is None

    def test_missing_symbol_returns_none(self) -> None:
        item = {k: v for k, v in FIXTURE_INSTRUMENT.items() if k != "symbolFull"}
        assert _normalise_instrument(item) is None


class TestNormaliseInstruments:
    def test_filters_internals(self) -> None:
        raw = {"instrumentDisplayDatas": [FIXTURE_INSTRUMENT, FIXTURE_INSTRUMENT_INTERNAL]}
        records = _normalise_instruments(raw)
        assert len(records) == 1
        assert records[0].symbol == "AAPL"

    def test_empty_list(self) -> None:
        assert _normalise_instruments({"instrumentDisplayDatas": []}) == []

    def test_non_dict_response_raises(self) -> None:
        with pytest.raises(ValueError, match="Expected dict"):
            _normalise_instruments(["not", "a", "dict"])

    def test_bad_items_skipped(self) -> None:
        raw = {"instrumentDisplayDatas": [FIXTURE_INSTRUMENT, "not a dict", {}]}
        records = _normalise_instruments(raw)
        assert len(records) == 1


# ---------------------------------------------------------------------------
# Candle normalisation
# ---------------------------------------------------------------------------


class TestNormaliseCandle:
    def test_valid_candle(self) -> None:
        bar = _normalise_candle(FIXTURE_CANDLE)
        assert bar is not None
        assert bar.price_date == date(2024, 6, 15)
        assert bar.open == Decimal("185.0")
        assert bar.high == Decimal("187.5")
        assert bar.low == Decimal("184.2")
        assert bar.close == Decimal("186.8")
        assert bar.volume == 55000000

    def test_missing_close_returns_none(self) -> None:
        item = {k: v for k, v in FIXTURE_CANDLE.items() if k != "close"}
        assert _normalise_candle(item) is None

    def test_missing_date_returns_none(self) -> None:
        item = {k: v for k, v in FIXTURE_CANDLE.items() if k != "fromDate"}
        assert _normalise_candle(item) is None

    def test_zero_volume_becomes_none(self) -> None:
        item = {**FIXTURE_CANDLE, "volume": 0}
        bar = _normalise_candle(item)
        assert bar is not None
        assert bar.volume is None

    def test_absent_volume_becomes_none(self) -> None:
        item = {k: v for k, v in FIXTURE_CANDLE.items() if k != "volume"}
        bar = _normalise_candle(item)
        assert bar is not None
        assert bar.volume is None

    def test_empty_string_date_returns_none(self) -> None:
        item = {**FIXTURE_CANDLE, "fromDate": ""}
        assert _normalise_candle(item) is None

    def test_returns_ohlcv_bar(self) -> None:
        bar = _normalise_candle(FIXTURE_CANDLE)
        assert isinstance(bar, OHLCVBar)


class TestNormaliseCandles:
    def test_nested_response_shape(self) -> None:
        """Real API: { candles: [{ instrumentId, candles: [...] }] }"""
        bars = _normalise_candles(FIXTURE_CANDLES_RESPONSE)
        assert len(bars) == 3

    def test_preserves_order_from_api(self) -> None:
        """asc direction means API returns oldest-first; normaliser preserves order."""
        # Fixture has candles in order: 2024-06-14, 2024-06-15, 2024-06-16
        bars = _normalise_candles(FIXTURE_CANDLES_RESPONSE)
        assert bars[0].price_date == date(2024, 6, 14)
        assert bars[1].price_date == date(2024, 6, 15)
        assert bars[2].price_date == date(2024, 6, 16)

    def test_empty_list(self) -> None:
        assert _normalise_candles({"candles": []}) == []

    def test_non_dict_response_raises(self) -> None:
        with pytest.raises(ValueError, match="Expected dict"):
            _normalise_candles(["not", "a", "dict"])

    def test_bad_items_skipped(self) -> None:
        raw = {
            "candles": [
                {
                    "instrumentId": 1001,
                    "candles": [
                        FIXTURE_CANDLE,
                        {"fromDate": "2024-06-16"},  # missing OHLC → skipped
                        "not a dict",  # not a dict → skipped
                    ],
                }
            ]
        }
        bars = _normalise_candles(raw)
        assert len(bars) == 1


# ---------------------------------------------------------------------------
# Rate / quote normalisation
# ---------------------------------------------------------------------------


class TestNormaliseRate:
    def test_valid_rate(self) -> None:
        quote = _normalise_rate(FIXTURE_RATE)
        assert quote is not None
        assert quote.instrument_id == 1001
        assert quote.bid == Decimal("186.5")
        assert quote.ask == Decimal("186.7")
        assert quote.last == Decimal("186.6")

    def test_missing_instrument_id_returns_none(self) -> None:
        item = {k: v for k, v in FIXTURE_RATE.items() if k != "instrumentID"}
        assert _normalise_rate(item) is None

    def test_missing_bid_returns_none(self) -> None:
        item = {k: v for k, v in FIXTURE_RATE.items() if k != "bid"}
        assert _normalise_rate(item) is None

    def test_missing_ask_returns_none(self) -> None:
        item = {k: v for k, v in FIXTURE_RATE.items() if k != "ask"}
        assert _normalise_rate(item) is None

    def test_zero_bid_returns_none(self) -> None:
        item = {**FIXTURE_RATE, "bid": 0}
        assert _normalise_rate(item) is None

    def test_zero_ask_returns_none(self) -> None:
        item = {**FIXTURE_RATE, "ask": 0}
        assert _normalise_rate(item) is None

    def test_returns_quote(self) -> None:
        quote = _normalise_rate(FIXTURE_RATE)
        assert isinstance(quote, Quote)

    def test_none_last_execution(self) -> None:
        item = {k: v for k, v in FIXTURE_RATE.items() if k != "lastExecution"}
        quote = _normalise_rate(item)
        assert quote is not None
        assert quote.last is None

    def test_timestamp_parsed(self) -> None:
        quote = _normalise_rate(FIXTURE_RATE)
        assert quote is not None
        assert quote.timestamp == datetime(2024, 6, 17, 14, 30, tzinfo=UTC)


class TestNormaliseRates:
    def test_batch_response(self) -> None:
        quotes = _normalise_rates(FIXTURE_RATES_RESPONSE)
        assert len(quotes) == 2
        ids = {q.instrument_id for q in quotes}
        assert ids == {1001, 1002}

    def test_empty_rates(self) -> None:
        assert _normalise_rates({"rates": []}) == []

    def test_non_dict_response_raises(self) -> None:
        with pytest.raises(ValueError, match="Expected dict"):
            _normalise_rates(["not", "a", "dict"])


# ---------------------------------------------------------------------------
# Provider get_quotes chunking
# ---------------------------------------------------------------------------


class TestGetQuotesChunking:
    """Test that get_quotes chunks IDs at 50 and builds correct params."""

    @patch("app.providers.implementations.etoro._persist_raw")
    def test_empty_list_no_http_call(self, _mock_persist: MagicMock) -> None:
        from app.providers.implementations.etoro import EtoroMarketDataProvider

        with EtoroMarketDataProvider(api_key="k", user_key="u") as provider:
            provider._http = MagicMock()
            result = provider.get_quotes([])
            assert result == []
            provider._http.get.assert_not_called()

    @patch("app.providers.implementations.etoro._persist_raw")
    def test_single_batch_params(self, _mock_persist: MagicMock) -> None:
        """instrumentIds are inlined in the URL with raw commas (not percent-encoded)."""
        from app.providers.implementations.etoro import EtoroMarketDataProvider

        mock_resp = MagicMock()
        mock_resp.json.return_value = {"rates": [FIXTURE_RATE]}
        mock_resp.raise_for_status = MagicMock()

        with EtoroMarketDataProvider(api_key="k", user_key="u") as provider:
            provider._http = MagicMock()
            provider._http.get.return_value = mock_resp

            provider.get_quotes([1001, 1002, 1003])

            provider._http.get.assert_called_once()
            url_arg = provider._http.get.call_args.args[0]
            assert "instrumentIds=1001,1002,1003" in url_arg

    @patch("app.providers.implementations.etoro._persist_raw")
    def test_chunking_at_51_ids(self, _mock_persist: MagicMock) -> None:
        """51 IDs should produce exactly 2 HTTP requests (50 + 1)."""
        from app.providers.implementations.etoro import EtoroMarketDataProvider

        mock_resp = MagicMock()
        mock_resp.json.return_value = {"rates": []}
        mock_resp.raise_for_status = MagicMock()

        with EtoroMarketDataProvider(api_key="k", user_key="u") as provider:
            provider._http = MagicMock()
            provider._http.get.return_value = mock_resp

            ids = list(range(1, 52))  # 51 IDs
            provider.get_quotes(ids)

            assert provider._http.get.call_count == 2
            # First call: 50 IDs inlined in URL
            first_url = provider._http.get.call_args_list[0].args[0]
            first_ids = first_url.split("instrumentIds=")[1].split("&")[0]
            assert len(first_ids.split(",")) == 50
            # Second call: 1 ID
            second_url = provider._http.get.call_args_list[1].args[0]
            second_ids = second_url.split("instrumentIds=")[1].split("&")[0]
            assert len(second_ids.split(",")) == 1

    @patch("app.providers.implementations.etoro._persist_raw")
    def test_failed_chunk_does_not_poison_others(self, _mock_persist: MagicMock) -> None:
        """If one chunk 500s, the rest still return quotes."""
        from app.providers.implementations.etoro import EtoroMarketDataProvider

        ok_resp = MagicMock()
        ok_resp.json.return_value = {"rates": [FIXTURE_RATE]}
        ok_resp.raise_for_status = MagicMock()

        error_response = httpx.Response(500, content=b'{"error":"internal"}')
        fail_resp = MagicMock()
        fail_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            "500",
            request=httpx.Request("GET", "https://x"),
            response=error_response,
        )

        with EtoroMarketDataProvider(api_key="k", user_key="u") as provider:
            provider._http = MagicMock()
            # First chunk succeeds, second fails
            provider._http.get.side_effect = [ok_resp, fail_resp]

            ids = list(range(1, 52))  # 51 IDs → 2 chunks
            result = provider.get_quotes(ids)

            # Should return the quotes from the successful chunk
            assert len(result) == 1
            assert provider._http.get.call_count == 2
            # Error response body must be persisted for diagnosis
            persist_calls = {call[0][0]: call[0][1] for call in _mock_persist.call_args_list}
            assert "rates_batch1_error" in persist_calls
            assert '{"error":"internal"}' in persist_calls["rates_batch1_error"]


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


# ---------------------------------------------------------------------------
# Candle freshness skip
# ---------------------------------------------------------------------------


def _mock_conn_with_latest_date(latest_date: date | None) -> MagicMock:
    """Build a mock connection whose execute().fetchone() returns (latest_date,)."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    # Aggregate always returns one row; column is None if table empty.
    mock_cursor.fetchone.return_value = (latest_date,) if latest_date is not None else (None,)
    mock_conn.execute.return_value = mock_cursor
    return mock_conn


class TestCandlesAreFresh:
    def test_fresh_when_latest_is_today(self) -> None:
        today = date(2026, 4, 10)
        conn = _mock_conn_with_latest_date(today)
        assert _candles_are_fresh(conn, 1, today) is True

    def test_stale_when_latest_is_yesterday_weekday(self) -> None:
        """On Friday, yesterday (Thursday) is stale — today's candle is the target."""
        today = date(2026, 4, 10)  # Friday
        conn = _mock_conn_with_latest_date(date(2026, 4, 9))  # Thursday
        assert _candles_are_fresh(conn, 1, today) is False

    def test_fresh_over_weekend(self) -> None:
        """Friday candle is fresh on Saturday/Sunday (weekends target Friday)."""
        saturday = date(2026, 4, 11)
        friday = date(2026, 4, 10)
        conn = _mock_conn_with_latest_date(friday)
        assert _candles_are_fresh(conn, 1, saturday) is True

    def test_stale_friday_candle_on_monday(self) -> None:
        """Friday candle is stale on Monday — Monday's candle is the target."""
        monday = date(2026, 4, 13)
        friday = date(2026, 4, 10)
        conn = _mock_conn_with_latest_date(friday)
        assert _candles_are_fresh(conn, 1, monday) is False

    def test_stale_when_latest_is_four_days_ago(self) -> None:
        today = date(2026, 4, 10)
        conn = _mock_conn_with_latest_date(date(2026, 4, 6))
        assert _candles_are_fresh(conn, 1, today) is False

    def test_stale_when_no_data(self) -> None:
        today = date(2026, 4, 10)
        conn = _mock_conn_with_latest_date(None)
        assert _candles_are_fresh(conn, 1, today) is False


# ---------------------------------------------------------------------------
# Weekday-aware candle freshness
# ---------------------------------------------------------------------------


def _candles_are_fresh_standalone(latest_date: date, today: date) -> bool:
    return latest_date >= _most_recent_trading_day(today)


class TestCandleFreshness:
    """Tests for the weekday-aware candle freshness check."""

    def test_friday_candle_fresh_on_saturday(self) -> None:
        assert _candles_are_fresh_standalone(date(2026, 4, 10), date(2026, 4, 11))  # Fri, Sat

    def test_friday_candle_fresh_on_sunday(self) -> None:
        assert _candles_are_fresh_standalone(date(2026, 4, 10), date(2026, 4, 12))  # Fri, Sun

    def test_friday_candle_stale_on_monday(self) -> None:
        # Monday's target is Monday itself; Friday's candle is stale.
        assert not _candles_are_fresh_standalone(date(2026, 4, 10), date(2026, 4, 13))  # Fri, Mon

    def test_monday_candle_fresh_on_monday(self) -> None:
        # Monday's target is Monday itself; Monday's candle is fresh.
        assert _candles_are_fresh_standalone(date(2026, 4, 13), date(2026, 4, 13))  # Mon, Mon

    def test_wednesday_candle_stale_on_friday(self) -> None:
        assert not _candles_are_fresh_standalone(date(2026, 4, 8), date(2026, 4, 10))

    def test_thursday_candle_stale_on_friday(self) -> None:
        # Friday's target is Friday itself; Thursday's candle is stale.
        assert not _candles_are_fresh_standalone(date(2026, 4, 9), date(2026, 4, 10))

    def test_friday_candle_fresh_on_friday(self) -> None:
        # Friday's target is Friday itself; Friday's candle is fresh.
        assert _candles_are_fresh_standalone(date(2026, 4, 10), date(2026, 4, 10))

    def test_monday_candle_stale_on_wednesday(self) -> None:
        assert not _candles_are_fresh_standalone(date(2026, 4, 6), date(2026, 4, 8))

    def test_same_day_weekday(self) -> None:
        assert _candles_are_fresh_standalone(date(2026, 4, 13), date(2026, 4, 13))


# ---------------------------------------------------------------------------
# TA integration in _compute_and_store_features
# ---------------------------------------------------------------------------

_TA_COLUMN_NAMES = [
    "sma_20",
    "sma_50",
    "sma_200",
    "ema_12",
    "ema_26",
    "macd_line",
    "macd_signal",
    "macd_histogram",
    "rsi_14",
    "stoch_k",
    "stoch_d",
    "bb_upper",
    "bb_lower",
    "atr_14",
]


def _make_mock_execute(results_queue: Sequence[Sequence[Any]]) -> object:
    """Return a side_effect callable that pops from a pre-built results queue.

    Each call to conn.execute() returns a MagicMock whose .fetchall()
    yields the next list from the queue.  Calls beyond the queue length
    return empty results (for the UPDATE).
    """
    queue = list(results_queue)
    idx = [0]

    def _side_effect(*args: object, **kwargs: object) -> MagicMock:
        mock = MagicMock()
        if idx[0] < len(queue):
            mock.fetchall.return_value = queue[idx[0]]
            mock.fetchone.return_value = queue[idx[0]][0] if queue[idx[0]] else None
        else:
            mock.fetchall.return_value = []
            mock.fetchone.return_value = None
        idx[0] += 1
        return mock

    return _side_effect


def _generate_price_rows(n: int) -> list[tuple[date, Decimal]]:
    """Generate n rows of (price_date, close) with a gentle uptrend."""
    base = date(2025, 1, 1)
    return [(date.fromordinal(base.toordinal() + i), Decimal(str(100 + i * 0.5))) for i in range(n)]


def _generate_ohlcv_rows(n: int) -> list[tuple[date, Decimal, Decimal, Decimal, Decimal, int]]:
    """Generate n OHLCV tuples (price_date, open, high, low, close, volume).

    Dates match _generate_price_rows so the TA date-alignment check passes.
    """
    base = date(2025, 1, 1)
    return [
        (
            date.fromordinal(base.toordinal() + i),  # price_date
            Decimal(str(100 + i * 0.5)),  # open
            Decimal(str(101 + i * 0.5)),  # high
            Decimal(str(99 + i * 0.5)),  # low
            Decimal(str(100 + i * 0.5)),  # close
            1000000,  # volume
        )
        for i in range(n)
    ]


class TestComputeAndStoreFeaturesTA:
    """Tests for TA indicator integration in _compute_and_store_features."""

    def test_ta_columns_in_update_sql(self) -> None:
        """With 250+ rows, the UPDATE SQL contains all TA columns and params."""
        n = 250
        price_rows = _generate_price_rows(n)
        # DB returns newest-first; function reverses internally
        close_rows = list(reversed(price_rows))
        ohlcv_rows = list(reversed(_generate_ohlcv_rows(n)))

        conn = MagicMock()
        conn.execute.side_effect = _make_mock_execute([close_rows, ohlcv_rows])

        result = _compute_and_store_features(conn, instrument_id=42)
        assert result == 1

        # The third conn.execute call is the UPDATE
        assert conn.execute.call_count == 3
        update_call = conn.execute.call_args_list[2]
        sql = update_call[0][0]
        params = update_call[0][1]

        # Verify every TA column appears in both the SQL and params dict
        for col in _TA_COLUMN_NAMES:
            assert f"%({col})s" in sql, f"Missing placeholder for {col}"
            assert col in params, f"Missing param key for {col}"

        # With 250 bars, at least RSI and SMA-20 should be non-None
        assert params["rsi_14"] is not None
        assert params["sma_20"] is not None
        # Values should be Decimal (not float)
        assert isinstance(params["rsi_14"], Decimal)
        assert isinstance(params["sma_20"], Decimal)

    def test_ta_none_with_insufficient_data(self) -> None:
        """With only 10 rows, long-window indicators (sma_200) are None."""
        n = 10
        price_rows = _generate_price_rows(n)
        close_rows = list(reversed(price_rows))
        ohlcv_rows = list(reversed(_generate_ohlcv_rows(n)))

        conn = MagicMock()
        conn.execute.side_effect = _make_mock_execute([close_rows, ohlcv_rows])

        result = _compute_and_store_features(conn, instrument_id=42)
        assert result == 1

        update_call = conn.execute.call_args_list[2]
        params = update_call[0][1]

        # sma_200 needs 200 bars — with 10 rows it must be None
        assert params["sma_200"] is None
        # sma_50 needs 50 bars — with 10 rows it must be None
        assert params["sma_50"] is None
        # All TA keys must still be present (even if None)
        for col in _TA_COLUMN_NAMES:
            assert col in params

    def test_ta_skipped_when_no_rows(self) -> None:
        """When there are no price rows, function returns 0 with no crash."""
        conn = MagicMock()
        conn.execute.side_effect = _make_mock_execute([[]])

        result = _compute_and_store_features(conn, instrument_id=42)
        assert result == 0
        # Only the initial close-prices SELECT was executed; no OHLCV or UPDATE
        assert conn.execute.call_count == 1

    def test_ta_values_are_rounded_decimals(self) -> None:
        """TA float values are converted to Decimal with 6dp precision."""
        n = 250
        price_rows = _generate_price_rows(n)
        close_rows = list(reversed(price_rows))
        ohlcv_rows = list(reversed(_generate_ohlcv_rows(n)))

        conn = MagicMock()
        conn.execute.side_effect = _make_mock_execute([close_rows, ohlcv_rows])

        _compute_and_store_features(conn, instrument_id=42)

        update_call = conn.execute.call_args_list[2]
        params = update_call[0][1]

        for col in _TA_COLUMN_NAMES:
            val = params[col]
            if val is not None:
                assert isinstance(val, Decimal), f"{col} should be Decimal, got {type(val)}"
                # Verify at most 6 decimal places
                _, _, exponent = val.as_tuple()
                assert isinstance(exponent, int)
                assert abs(exponent) <= 6, f"{col} has more than 6dp: {val}"

    def test_ta_skipped_when_dates_misaligned(self) -> None:
        """When latest OHLCV date != latest close date, all TA params are None.

        This tests the stale-TA guard: if the latest candle has close but
        incomplete OHLC, the OHLCV query returns the prior complete bar,
        creating a date mismatch that should suppress TA computation.
        """
        n = 250
        price_rows = _generate_price_rows(n)
        close_rows = list(reversed(price_rows))

        # OHLCV rows end one day earlier than close rows — simulates a
        # partial candle where close is set but OHLC is still NULL.
        ohlcv_rows = list(reversed(_generate_ohlcv_rows(n - 1)))

        conn = MagicMock()
        conn.execute.side_effect = _make_mock_execute([close_rows, ohlcv_rows])

        result = _compute_and_store_features(conn, instrument_id=42)
        assert result == 1

        update_call = conn.execute.call_args_list[2]
        params = update_call[0][1]

        # Every TA column must be None due to date mismatch
        for col in _TA_COLUMN_NAMES:
            assert params[col] is None, f"{col} should be None when dates misalign"

    def test_string_indicators_excluded_from_ta_params(self) -> None:
        """compute_indicators returns price_vs_sma200 and trend_sma_cross as
        strings — these must NOT appear in the numeric ta_params dict."""
        n = 250
        price_rows = _generate_price_rows(n)
        close_rows = list(reversed(price_rows))
        ohlcv_rows = list(reversed(_generate_ohlcv_rows(n)))

        conn = MagicMock()
        conn.execute.side_effect = _make_mock_execute([close_rows, ohlcv_rows])

        _compute_and_store_features(conn, instrument_id=42)

        update_call = conn.execute.call_args_list[2]
        params = update_call[0][1]

        assert "price_vs_sma200" not in params
        assert "trend_sma_cross" not in params


class TestCandlesFetchCount:
    """_candles_fetch_count (#271) — two-mode backfill vs incremental."""

    def _mock_conn(self, fetchone_return):
        """Return a MagicMock conn whose .execute().fetchone() yields the row."""
        conn = MagicMock()
        cursor = MagicMock()
        cursor.fetchone.return_value = fetchone_return
        conn.execute.return_value = cursor
        return conn

    def test_returns_default_for_instrument_with_no_prior_candles(self) -> None:
        """Backfill mode — no price_daily rows, so we pull the full
        lookback_days window (caller default: 400)."""
        conn = self._mock_conn(fetchone_return=None)
        assert _candles_fetch_count(conn, 42, default=400) == 400

    def test_returns_incremental_for_instrument_with_history(self) -> None:
        """Incremental mode — prior candles exist, pull only
        _INCREMENTAL_FETCH_BARS (yesterday + today + correction)."""
        conn = self._mock_conn(fetchone_return=(1,))
        assert _candles_fetch_count(conn, 42, default=400) == _INCREMENTAL_FETCH_BARS
        assert _INCREMENTAL_FETCH_BARS == 3  # sanity — pinning the documented value
