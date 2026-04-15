"""
Pure technical analysis computation module.

All functions are pure — no DB, no I/O.  They take OHLCV data
(oldest-first) and return indicator values as floats.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from decimal import Decimal
from typing import TypedDict


class OHLCVRow(TypedDict):
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int | None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _ema_series(closes: Sequence[Decimal], period: int) -> list[float] | None:
    """Full EMA series from index (period-1) onward.

    Seed is the SMA of the first *period* values.
    Returns None if len(closes) < period.
    """
    if len(closes) < period:
        return None

    seed = float(sum(closes[:period])) / period
    mult = 2.0 / (period + 1)
    result = [seed]
    for i in range(period, len(closes)):
        val = float(closes[i]) * mult + result[-1] * (1.0 - mult)
        result.append(val)
    return result


# ---------------------------------------------------------------------------
# Individual indicators
# ---------------------------------------------------------------------------


def sma(closes: Sequence[Decimal], period: int) -> float | None:
    """Simple moving average of the last *period* closes.

    Returns None if insufficient data.
    """
    if len(closes) < period:
        return None
    return float(sum(closes[-period:])) / period


def ema(closes: Sequence[Decimal], period: int) -> float | None:
    """Exponential moving average.

    Seed = SMA of first *period* values.  Multiplier = 2 / (period + 1).
    Returns None if insufficient data.
    """
    series = _ema_series(closes, period)
    if series is None:
        return None
    return series[-1]


def rsi(closes: Sequence[Decimal], period: int = 14) -> float | None:
    """Relative Strength Index with Wilder smoothing.

    Requires at least period + 1 data points.
    """
    if len(closes) < period + 1:
        return None

    deltas = [float(closes[i] - closes[i - 1]) for i in range(1, len(closes))]

    # Initial averages over the first *period* deltas
    gains = [d if d > 0 else 0.0 for d in deltas[:period]]
    losses = [-d if d < 0 else 0.0 for d in deltas[:period]]

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    # Wilder smoothing for remaining deltas
    for d in deltas[period:]:
        avg_gain = (avg_gain * (period - 1) + (d if d > 0 else 0.0)) / period
        avg_loss = (avg_loss * (period - 1) + (-d if d < 0 else 0.0)) / period

    if avg_gain == 0.0 and avg_loss == 0.0:
        return 50.0
    if avg_loss == 0.0:
        return 100.0

    rs = avg_gain / avg_loss
    return 100.0 - 100.0 / (1.0 + rs)


def macd(
    closes: Sequence[Decimal],
    fast: int = 12,
    slow: int = 26,
    signal_period: int = 9,
) -> tuple[float, float, float] | None:
    """MACD line, signal line, and histogram.

    Returns (line, signal, histogram) or None if insufficient data.
    Needs at least slow + signal_period - 1 data points.
    """
    if len(closes) < slow + signal_period - 1:
        return None

    fast_series = _ema_series(closes, fast)
    slow_series = _ema_series(closes, slow)
    if fast_series is None or slow_series is None:
        return None

    # Align: fast_series starts at index (fast-1), slow at (slow-1).
    # MACD line series starts where both are available — aligned to
    # the slow series start.
    offset = slow - fast  # how many extra fast values before slow starts
    macd_series = [fast_series[offset + i] - slow_series[i] for i in range(len(slow_series))]

    if len(macd_series) < signal_period:
        return None

    # Signal = EMA of the MACD line series
    macd_decimals = [Decimal(str(v)) for v in macd_series]
    signal_series = _ema_series(macd_decimals, signal_period)
    if signal_series is None:
        return None

    line = macd_series[-1]
    signal_val = signal_series[-1]
    histogram = line - signal_val
    return (line, signal_val, histogram)


def bollinger_bands(
    closes: Sequence[Decimal],
    period: int = 20,
    num_std: float = 2.0,
) -> tuple[float, float] | None:
    """Bollinger Bands (upper, lower).

    Uses population standard deviation (ddof=0).
    Returns None if insufficient data.
    """
    if len(closes) < period:
        return None

    window = [float(c) for c in closes[-period:]]
    mean = sum(window) / period
    variance = sum((x - mean) ** 2 for x in window) / period
    std = math.sqrt(variance)
    return (mean + num_std * std, mean - num_std * std)


def atr(bars: Sequence[OHLCVRow], period: int = 14) -> float | None:
    """Average True Range with Wilder smoothing.

    True Range = max(high - low, |high - prev_close|, |low - prev_close|).
    Needs period + 1 bars (first bar establishes prev_close).
    """
    if len(bars) < period + 1:
        return None

    # Compute true ranges (first bar is anchor only)
    trs: list[float] = []
    for i in range(1, len(bars)):
        high = float(bars[i]["high"])
        low = float(bars[i]["low"])
        prev_close = float(bars[i - 1]["close"])
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)

    # Initial ATR = SMA of first *period* TRs
    atr_val = sum(trs[:period]) / period

    # Wilder smoothing for remaining
    for tr in trs[period:]:
        atr_val = (atr_val * (period - 1) + tr) / period

    return atr_val


def stochastic(
    bars: Sequence[OHLCVRow],
    k_period: int = 14,
    d_period: int = 3,
) -> tuple[float, float] | None:
    """Stochastic oscillator (%K, %D).

    %K = (close - lowest_low) / (highest_high - lowest_low) * 100.
    %D = SMA(d_period) of %K values.
    Needs k_period + d_period - 1 bars.
    """
    required = k_period + d_period - 1
    if len(bars) < required:
        return None

    # Compute %K for each window of k_period bars, producing d_period values
    k_values: list[float] = []
    for end_idx in range(len(bars) - d_period + 1, len(bars) + 1):
        start_idx = end_idx - k_period
        window = bars[start_idx:end_idx]
        highest = max(float(b["high"]) for b in window)
        lowest = min(float(b["low"]) for b in window)
        close = float(window[-1]["close"])

        if highest == lowest:
            k_values.append(50.0)
        else:
            k_values.append((close - lowest) / (highest - lowest) * 100.0)

    pct_k = k_values[-1]
    pct_d = sum(k_values[-d_period:]) / d_period
    return (pct_k, pct_d)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def compute_indicators(
    bars: Sequence[OHLCVRow],
) -> dict[str, float | str | None] | None:
    """Compute all technical indicators from OHLCV bars.

    Returns a dict keyed by price_daily column names, plus derived
    cross-signals (price_vs_sma200, trend_sma_cross).
    Returns None for empty bars.
    """
    if not bars:
        return None

    closes = [b["close"] for b in bars]

    # Individual indicators
    sma_20 = sma(closes, 20)
    sma_50 = sma(closes, 50)
    sma_200 = sma(closes, 200)
    ema_12 = ema(closes, 12)
    ema_26 = ema(closes, 26)

    macd_result = macd(closes)
    macd_line: float | None = None
    macd_signal: float | None = None
    macd_histogram: float | None = None
    if macd_result is not None:
        macd_line, macd_signal, macd_histogram = macd_result

    rsi_14 = rsi(closes, 14)

    stoch_result = stochastic(bars)
    stoch_k: float | None = None
    stoch_d: float | None = None
    if stoch_result is not None:
        stoch_k, stoch_d = stoch_result

    bb_result = bollinger_bands(closes)
    bb_upper: float | None = None
    bb_lower: float | None = None
    if bb_result is not None:
        bb_upper, bb_lower = bb_result

    atr_14 = atr(bars, 14)

    # Derived cross-signals (not stored in DB)
    latest_close = float(closes[-1])

    price_vs_sma200: str | None
    if sma_200 is not None:
        price_vs_sma200 = "above" if latest_close > sma_200 else "below"
    else:
        price_vs_sma200 = None

    trend_sma_cross: str
    if sma_50 is not None and sma_200 is not None:
        if sma_50 > sma_200:
            trend_sma_cross = "golden"
        elif sma_50 < sma_200:
            trend_sma_cross = "death"
        else:
            trend_sma_cross = "none"
    else:
        trend_sma_cross = "none"

    return {
        "sma_20": sma_20,
        "sma_50": sma_50,
        "sma_200": sma_200,
        "ema_12": ema_12,
        "ema_26": ema_26,
        "macd_line": macd_line,
        "macd_signal": macd_signal,
        "macd_histogram": macd_histogram,
        "rsi_14": rsi_14,
        "stoch_k": stoch_k,
        "stoch_d": stoch_d,
        "bb_upper": bb_upper,
        "bb_lower": bb_lower,
        "atr_14": atr_14,
        "price_vs_sma200": price_vs_sma200,
        "trend_sma_cross": trend_sma_cross,
    }
