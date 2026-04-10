"""
Unit tests for market data normalisation, feature computation, and spread checks.

No network calls, no database — all tests use in-memory fixtures.
"""

from datetime import UTC, date, datetime
from decimal import Decimal
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
    DEFAULT_MAX_SPREAD_PCT,
    _candles_are_fresh,
    _compute_rolling_returns,
    _compute_volatility_30d,
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

    def test_currency_is_placeholder(self) -> None:
        """currency defaults to 'USD' as a placeholder — not from the API."""
        rec = _normalise_instrument(FIXTURE_INSTRUMENT)
        assert rec is not None
        assert rec.currency == "USD"

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
        """instrumentIds param is comma-separated ints."""
        from app.providers.implementations.etoro import EtoroMarketDataProvider

        mock_resp = MagicMock()
        mock_resp.json.return_value = {"rates": [FIXTURE_RATE]}
        mock_resp.raise_for_status = MagicMock()

        with EtoroMarketDataProvider(api_key="k", user_key="u") as provider:
            provider._http = MagicMock()
            provider._http.get.return_value = mock_resp

            provider.get_quotes([1001, 1002, 1003])

            provider._http.get.assert_called_once()
            call_kwargs = provider._http.get.call_args
            assert call_kwargs.kwargs["params"]["instrumentIds"] == "1001,1002,1003"

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
            # First call: 50 IDs
            first_params = provider._http.get.call_args_list[0].kwargs["params"]["instrumentIds"]
            assert len(first_params.split(",")) == 50
            # Second call: 1 ID
            second_params = provider._http.get.call_args_list[1].kwargs["params"]["instrumentIds"]
            assert len(second_params.split(",")) == 1

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

    def test_fresh_when_latest_is_yesterday(self) -> None:
        today = date(2026, 4, 10)
        conn = _mock_conn_with_latest_date(date(2026, 4, 9))
        assert _candles_are_fresh(conn, 1, today) is True

    def test_fresh_over_weekend(self) -> None:
        """Friday candle is fresh on Monday (3-day gap covers weekends)."""
        monday = date(2026, 4, 13)  # Monday
        friday = date(2026, 4, 10)  # previous Friday
        conn = _mock_conn_with_latest_date(friday)
        assert _candles_are_fresh(conn, 1, monday) is True

    def test_stale_when_latest_is_four_days_ago(self) -> None:
        today = date(2026, 4, 10)
        conn = _mock_conn_with_latest_date(date(2026, 4, 6))
        assert _candles_are_fresh(conn, 1, today) is False

    def test_stale_when_no_data(self) -> None:
        today = date(2026, 4, 10)
        conn = _mock_conn_with_latest_date(None)
        assert _candles_are_fresh(conn, 1, today) is False
