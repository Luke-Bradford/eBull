"""Tests for the pure TA computation module."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

import pytest

from app.services.technical_analysis import (
    OHLCVRow,
    atr,
    bollinger_bands,
    compute_indicators,
    ema,
    macd,
    rsi,
    sma,
    stochastic,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_closes(values: list[float]) -> list[Decimal]:
    return [Decimal(str(v)) for v in values]


def _make_ohlcv(
    values: Sequence[tuple[float, float, float, float]],
) -> list[OHLCVRow]:
    return [
        OHLCVRow(
            open=Decimal(str(op)),
            high=Decimal(str(hi)),
            low=Decimal(str(lo)),
            close=Decimal(str(cl)),
            volume=None,
        )
        for op, hi, lo, cl in values
    ]


# ===================================================================
# SMA
# ===================================================================


class TestSMA:
    def test_sma_20_exact(self) -> None:
        closes = _make_closes([float(i) for i in range(1, 21)])
        result = sma(closes, 20)
        assert result == pytest.approx(10.5, rel=1e-4)

    def test_sma_longer_series(self) -> None:
        closes = _make_closes([1, 2, 3, 4, 5])
        result = sma(closes, 3)
        assert result == pytest.approx(4.0, rel=1e-4)

    def test_sma_insufficient_data(self) -> None:
        closes = _make_closes([1, 2])
        assert sma(closes, 3) is None


# ===================================================================
# EMA
# ===================================================================


class TestEMA:
    def test_ema_seeded_with_sma(self) -> None:
        # EMA(3) of [1,2,3,4,5]:
        # seed = SMA(1,2,3) = 2.0
        # mult = 2/(3+1) = 0.5
        # EMA_4 = 4*0.5 + 2.0*0.5 = 3.0
        # EMA_5 = 5*0.5 + 3.0*0.5 = 4.0
        closes = _make_closes([1, 2, 3, 4, 5])
        result = ema(closes, 3)
        assert result == pytest.approx(4.0, rel=1e-4)

    def test_ema_12_matches_reference(self) -> None:
        closes = _make_closes([float(i) for i in range(1, 21)])
        result = ema(closes, 12)
        assert result is not None
        assert result > 13.0

    def test_ema_insufficient_data(self) -> None:
        closes = _make_closes([1, 2])
        assert ema(closes, 3) is None


# ===================================================================
# RSI
# ===================================================================


class TestRSI:
    def test_rsi_all_gains(self) -> None:
        closes = _make_closes([float(i) for i in range(1, 20)])
        result = rsi(closes, 14)
        assert result is not None
        assert result > 95

    def test_rsi_all_losses(self) -> None:
        closes = _make_closes([float(i) for i in range(20, 1, -1)])
        result = rsi(closes, 14)
        assert result is not None
        assert result < 5

    def test_rsi_flat_market(self) -> None:
        closes = _make_closes([100.0] * 20)
        result = rsi(closes, 14)
        assert result == 50.0

    def test_rsi_insufficient_data(self) -> None:
        closes = _make_closes([1, 2, 3])
        assert rsi(closes, 14) is None

    def test_rsi_known_value(self) -> None:
        # Alternating +1/-0.5 from 100, 29 values
        vals: list[float] = [100.0]
        for i in range(28):
            if i % 2 == 0:
                vals.append(vals[-1] + 1.0)
            else:
                vals.append(vals[-1] - 0.5)
        closes = _make_closes(vals)
        result = rsi(closes, 14)
        assert result is not None
        assert 55 <= result <= 75


# ===================================================================
# MACD
# ===================================================================


class TestMACD:
    def test_macd_components(self) -> None:
        closes = _make_closes([float(i) for i in range(1, 51)])
        result = macd(closes)
        assert result is not None
        line, signal, histogram = result
        assert line > 0
        assert histogram == pytest.approx(line - signal, rel=1e-4)

    def test_macd_insufficient_data(self) -> None:
        closes = _make_closes([float(i) for i in range(1, 34)])
        assert macd(closes) is None

    def test_macd_flat_market(self) -> None:
        closes = _make_closes([50.0] * 50)
        result = macd(closes)
        assert result is not None
        line, signal, histogram = result
        assert line == pytest.approx(0.0, abs=1e-6)
        assert signal == pytest.approx(0.0, abs=1e-6)
        assert histogram == pytest.approx(0.0, abs=1e-6)


# ===================================================================
# Bollinger Bands
# ===================================================================


class TestBollingerBands:
    def test_bollinger_flat_market(self) -> None:
        closes = _make_closes([100.0] * 20)
        result = bollinger_bands(closes)
        assert result is not None
        upper, lower = result
        assert upper == pytest.approx(100.0, abs=1e-6)
        assert lower == pytest.approx(100.0, abs=1e-6)

    def test_bollinger_known_spread(self) -> None:
        closes = _make_closes([float(i) for i in range(1, 21)])
        result = bollinger_bands(closes)
        assert result is not None
        upper, lower = result
        mean = 10.5
        # Population stddev of 1..20
        variance = sum((i - mean) ** 2 for i in range(1, 21)) / 20
        std = variance**0.5
        assert upper == pytest.approx(mean + 2 * std, rel=1e-4)
        assert lower == pytest.approx(mean - 2 * std, rel=1e-4)

    def test_bollinger_insufficient_data(self) -> None:
        closes = _make_closes([1, 2, 3])
        assert bollinger_bands(closes) is None


# ===================================================================
# ATR
# ===================================================================


class TestATR:
    def test_atr_known_value(self) -> None:
        # Constant range bars: high=105, low=95, close=100
        # Need period+1 = 15 bars for period=14
        bars = _make_ohlcv([(100, 105, 95, 100)] * 15)
        result = atr(bars, 14)
        assert result is not None
        assert result == pytest.approx(10.0, rel=1e-4)

    def test_atr_with_gaps(self) -> None:
        # 14 constant bars then one with gap
        base_bars = [(100, 105, 95, 100)] * 14
        # Gap bar: prev_close=100, high=115, low=108
        # TR = max(115-108, |115-100|, |108-100|) = max(7, 15, 8) = 15
        gap_bar = (110, 115, 108, 112)
        bars = _make_ohlcv(base_bars + [gap_bar])
        result = atr(bars, 14)
        assert result is not None
        # Initial ATR (first 14 TRs, indices 1-14) = SMA of 13 * 10 + 15 / 14
        # Bars: index 0 is anchor. Bars 1..13 have TR=10 each (13 bars).
        # Bar 14 (gap bar) has TR=15. That's 14 TRs total.
        # Initial ATR = (13*10 + 15)/14 = 145/14 ≈ 10.357
        assert result == pytest.approx(145 / 14, rel=1e-4)

    def test_atr_insufficient_data(self) -> None:
        bars = _make_ohlcv([(100, 105, 95, 100)] * 5)
        assert atr(bars, 14) is None


# ===================================================================
# Stochastic
# ===================================================================


class TestStochastic:
    def test_stochastic_at_high(self) -> None:
        # Close at the top of range for all bars
        bars = _make_ohlcv([(100, 110, 90, 110)] * 16)
        result = stochastic(bars)
        assert result is not None
        k, d = result
        assert k == pytest.approx(100.0, abs=1e-6)
        assert d == pytest.approx(100.0, abs=1e-6)

    def test_stochastic_at_low(self) -> None:
        # Close at the bottom of range for all bars
        bars = _make_ohlcv([(100, 110, 90, 90)] * 16)
        result = stochastic(bars)
        assert result is not None
        k, d = result
        assert k == pytest.approx(0.0, abs=1e-6)
        assert d == pytest.approx(0.0, abs=1e-6)

    def test_stochastic_midpoint(self) -> None:
        # Close at the midpoint of range
        bars = _make_ohlcv([(100, 110, 90, 100)] * 16)
        result = stochastic(bars)
        assert result is not None
        k, _d = result
        assert k == pytest.approx(50.0, abs=1e-6)

    def test_stochastic_insufficient_data(self) -> None:
        bars = _make_ohlcv([(100, 110, 90, 100)] * 5)
        assert stochastic(bars) is None


# ===================================================================
# compute_indicators (orchestrator)
# ===================================================================


class TestComputeIndicators:
    def _make_ascending_bars(self, n: int) -> list[OHLCVRow]:
        """Create n ascending bars with realistic OHLCV shape."""
        return [
            OHLCVRow(
                open=Decimal(str(100 + i)),
                high=Decimal(str(105 + i)),
                low=Decimal(str(95 + i)),
                close=Decimal(str(102 + i)),
                volume=1000,
            )
            for i in range(n)
        ]

    def test_full_dataset_returns_all_indicators(self) -> None:
        bars = self._make_ascending_bars(250)
        result = compute_indicators(bars)
        assert result is not None
        stored_keys = [
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
        for key in stored_keys:
            assert key in result, f"Missing key: {key}"
            assert result[key] is not None, f"Key {key} is None"

    def test_insufficient_history_returns_none_for_long_indicators(self) -> None:
        bars = self._make_ascending_bars(30)
        result = compute_indicators(bars)
        assert result is not None
        assert result["sma_200"] is None
        assert result["sma_20"] is not None

    def test_empty_bars_returns_none(self) -> None:
        assert compute_indicators([]) is None

    def test_price_above_sma200(self) -> None:
        # Ascending bars — latest close well above SMA(200)
        bars = self._make_ascending_bars(250)
        result = compute_indicators(bars)
        assert result is not None
        assert result["price_vs_sma200"] == "above"

    def test_trend_sma_cross_golden(self) -> None:
        # Ascending data: SMA(50) > SMA(200) → golden cross
        bars = self._make_ascending_bars(250)
        result = compute_indicators(bars)
        assert result is not None
        assert result["trend_sma_cross"] == "golden"

    def test_trend_sma_cross_none_when_insufficient(self) -> None:
        # Not enough data for SMA(200) → cross signal should be "none"
        bars = self._make_ascending_bars(100)
        result = compute_indicators(bars)
        assert result is not None
        assert result["trend_sma_cross"] == "none"
