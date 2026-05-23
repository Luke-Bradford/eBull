# Technical Analysis Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Compute TA indicators from existing OHLCV data in `price_daily` and fold them into the existing `momentum_score` family to produce a richer, multi-signal momentum assessment.

**Architecture:** Pure Python computation module reads 400 days of OHLCV data (already fetched by `_compute_and_store_features` in `market_data.py`), computes ~15 indicators per instrument, stores latest-only values as new columns on `price_daily`, and the scoring engine's `_momentum_score()` is enhanced with three TA subcomponents (trend confirmation 40%, momentum quality 30%, volatility regime 30%) that blend with the existing return-based score.

**Tech Stack:** Python stdlib (math, decimal), psycopg3, pytest. No new dependencies.

---

## Settled decisions that apply

- **v1 scoring is heuristic, explicit, and auditable** — TA indicators are textbook formulas with pinned variants, no ML.
- **Penalties are additive in v1** — no change; TA enhances family score, not penalties.
- **Each score row carries enough detail to explain how it was produced** — TA subcomponent notes flow into the existing explanation.
- **model_version includes scoring mode, default is v1-balanced** — no model_version change since this enhances momentum, not a new family.
- **Providers are thin adapters; domain logic lives in services** — TA computation is a pure service function.
- **Do not add libraries casually** — pure Python, no pandas-ta.

## Prevention log entries that apply

- **Guard ALL required fields before casting** (`float(None)` crash risk) — TA functions must handle None OHLCV values gracefully; guard before float conversion.
- **Shared params dict** — verify each key consumed by every query that receives it. The UPDATE for TA columns must match the params dict exactly.
- **Product name drift** — grep for all name variants when adding TA scoring references in docs or comments.

## File structure

| File | Responsibility |
|------|---------------|
| `sql/025_technical_analysis_columns.sql` | Migration: add ~15 TA indicator columns to `price_daily` |
| `app/services/technical_analysis.py` | Pure computation: takes list of OHLCV dicts, returns dict of latest indicator values |
| `app/services/market_data.py` | Extended: call TA computation at tail end of `_compute_and_store_features()` |
| `app/services/scoring.py` | Enhanced: `_momentum_score()` gains TA subcomponents |
| `tests/test_technical_analysis.py` | Unit tests for each indicator formula against known reference values |
| `tests/test_market_data.py` | Integration test for TA column persistence |
| `tests/test_scoring.py` | New tests for enhanced momentum score with TA inputs |

---

### Task 1: Migration — add TA indicator columns to `price_daily`

**Files:**
- Create: `sql/025_technical_analysis_columns.sql`

- [ ] **Step 1: Create the migration file**

```sql
-- Migration 025: add technical analysis indicator columns to price_daily
--
-- These columns store the latest-only computed TA values for each instrument.
-- Values are recomputed on every daily candle refresh (tail end of
-- _compute_and_store_features). Only the most recent price_date row per
-- instrument carries values; historical rows remain NULL.
--
-- Formula variants pinned for auditability:
--   RSI: Wilder smoothing (EMA with alpha = 1/period)
--   ATR: Wilder smoothing
--   EMA: seeded with SMA of first `period` values
--   Bollinger: population stddev (ddof=0)
--   Stochastic %K: raw = (close - low14) / (high14 - low14) * 100
--   Stochastic %D: SMA(3) of %K

-- Trend indicators
ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS sma_20 NUMERIC(18,6);
ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS sma_50 NUMERIC(18,6);
ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS sma_200 NUMERIC(18,6);
ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS ema_12 NUMERIC(18,6);
ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS ema_26 NUMERIC(18,6);
ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS macd_line NUMERIC(18,6);
ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS macd_signal NUMERIC(18,6);
ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS macd_histogram NUMERIC(18,6);

-- Momentum indicators
ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS rsi_14 NUMERIC(10,4);
ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS stoch_k NUMERIC(10,4);
ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS stoch_d NUMERIC(10,4);

-- Volatility indicators
ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS bb_upper NUMERIC(18,6);
ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS bb_lower NUMERIC(18,6);
ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS atr_14 NUMERIC(18,6);
```

- [ ] **Step 2: Verify migration runs against dev DB**

Run: `psql $DATABASE_URL -f sql/025_technical_analysis_columns.sql`
Expected: 14 `ALTER TABLE` statements succeed (or no-op if re-run).

- [ ] **Step 3: Commit**

```bash
git add sql/025_technical_analysis_columns.sql
git commit -m "feat(#200): migration 025 — add TA indicator columns to price_daily"
```

---

### Task 2: Pure TA computation module — SMA, EMA, RSI

**Files:**
- Create: `app/services/technical_analysis.py`
- Create: `tests/test_technical_analysis.py`

This task implements the first three indicators. Subsequent tasks add the remaining indicators to the same module.

- [ ] **Step 1: Write failing tests for SMA**

```python
"""Tests for app.services.technical_analysis — pure indicator computation.

Reference values computed by hand or cross-checked against TradingView.
All functions take a list of OHLCVRow dicts (oldest-first) and return
the indicator value for the latest bar, or None if insufficient history.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from app.services.technical_analysis import (
    OHLCVRow,
    compute_indicators,
    sma,
    ema,
    rsi,
)


def _approx(value: float, rel: float = 1e-4) -> object:
    return pytest.approx(value, rel=rel)


def _make_closes(values: list[float]) -> list[Decimal]:
    return [Decimal(str(v)) for v in values]


class TestSMA:
    def test_sma_20_exact(self) -> None:
        """SMA(20) of 20 values = arithmetic mean."""
        closes = _make_closes([float(i) for i in range(1, 21)])
        result = sma(closes, 20)
        assert result == _approx(10.5)

    def test_sma_longer_series(self) -> None:
        """SMA(3) of [1,2,3,4,5] = mean of last 3 = 4.0."""
        closes = _make_closes([1.0, 2.0, 3.0, 4.0, 5.0])
        result = sma(closes, 3)
        assert result == _approx(4.0)

    def test_sma_insufficient_data(self) -> None:
        """Returns None when fewer than `period` values."""
        closes = _make_closes([1.0, 2.0])
        result = sma(closes, 20)
        assert result is None
```

- [ ] **Step 2: Write failing tests for EMA**

```python
class TestEMA:
    def test_ema_seeded_with_sma(self) -> None:
        """EMA(3) of [1,2,3,4,5]: seed = SMA(3) of first 3 = 2.0.
        Then: EMA_4 = 4 * (2/4) + 2.0 * (1 - 2/4) = 2 + 1 = 3.0
              EMA_5 = 5 * (2/4) + 3.0 * (1 - 2/4) = 2.5 + 1.5 = 4.0
        """
        closes = _make_closes([1.0, 2.0, 3.0, 4.0, 5.0])
        result = ema(closes, 3)
        assert result == _approx(4.0)

    def test_ema_12_matches_reference(self) -> None:
        """EMA(12) with 20 data points — verify against hand-computed value."""
        # 20 ascending values; seed = SMA(12) of first 12 = 6.5
        closes = _make_closes([float(i) for i in range(1, 21)])
        result = ema(closes, 12)
        # EMA(12) multiplier = 2/(12+1) = 0.153846...
        # Computed iteratively from seed 6.5 through values 13..20
        assert result is not None
        assert result > 13.0  # must track upward trend

    def test_ema_insufficient_data(self) -> None:
        closes = _make_closes([1.0, 2.0])
        result = ema(closes, 12)
        assert result is None
```

- [ ] **Step 3: Write failing tests for RSI**

```python
class TestRSI:
    def test_rsi_all_gains(self) -> None:
        """Monotonically increasing prices → RSI near 100."""
        closes = _make_closes([float(i) for i in range(1, 30)])
        result = rsi(closes, 14)
        assert result is not None
        assert result > 95.0

    def test_rsi_all_losses(self) -> None:
        """Monotonically decreasing prices → RSI near 0."""
        closes = _make_closes([float(30 - i) for i in range(29)])
        result = rsi(closes, 14)
        assert result is not None
        assert result < 5.0

    def test_rsi_flat_market(self) -> None:
        """All same price → RSI = 50 (no gains, no losses, Wilder smoothing)."""
        closes = _make_closes([100.0] * 30)
        result = rsi(closes, 14)
        # With zero gains and zero losses, RSI is undefined (0/0).
        # Convention: return 50.0 (neutral).
        assert result == _approx(50.0)

    def test_rsi_insufficient_data(self) -> None:
        """Need at least period+1 values for one smoothing step."""
        closes = _make_closes([1.0] * 10)
        result = rsi(closes, 14)
        assert result is None

    def test_rsi_known_value(self) -> None:
        """RSI(14) with a known sequence.
        Alternating +1/-0.5 gives avg_gain=1, avg_loss=0.5 after initial window.
        RS = 1/0.5 = 2, RSI = 100 - 100/(1+2) = 66.67.
        """
        # Build: start at 100, alternate +1, -0.5
        prices = [100.0]
        for i in range(28):
            if i % 2 == 0:
                prices.append(prices[-1] + 1.0)
            else:
                prices.append(prices[-1] - 0.5)
        closes = _make_closes(prices)
        result = rsi(closes, 14)
        assert result is not None
        # After Wilder smoothing, RSI should be in 60-70 range
        assert 55.0 < result < 75.0
```

- [ ] **Step 4: Run tests to verify they fail**

Run: `uv run pytest tests/test_technical_analysis.py -v`
Expected: ImportError (module doesn't exist yet)

- [ ] **Step 5: Implement SMA, EMA, RSI in `technical_analysis.py`**

```python
"""
Technical analysis indicator computation.

Pure functions — no DB, no I/O. Each function takes a list of Decimal
close prices (oldest-first) and returns the indicator value for the
latest bar, or None if there is insufficient history.

Formula variants (pinned for auditability):
  - RSI: Wilder smoothing (alpha = 1/period)
  - EMA: seeded with SMA of the first `period` values
  - ATR: Wilder smoothing over true range
  - Bollinger: population stddev (ddof=0)
  - Stochastic %K: (close - low14) / (high14 - low14) * 100
  - Stochastic %D: SMA(3) of %K

All prices are assumed split-adjusted (eToro provides adjusted candles
for US equities). If candles are corrected, TA is recomputed on the
next refresh.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal
from typing import TypedDict


class OHLCVRow(TypedDict):
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int | None


def sma(closes: list[Decimal], period: int) -> float | None:
    """Simple moving average of the last `period` closes."""
    if len(closes) < period:
        return None
    window = closes[-period:]
    return float(sum(window)) / period


def ema(closes: list[Decimal], period: int) -> float | None:
    """Exponential moving average, seeded with SMA of the first `period` values.

    Multiplier: 2 / (period + 1).
    """
    if len(closes) < period:
        return None
    seed = float(sum(closes[:period])) / period
    multiplier = 2.0 / (period + 1)
    value = seed
    for close in closes[period:]:
        value = float(close) * multiplier + value * (1.0 - multiplier)
    return value


def rsi(closes: list[Decimal], period: int = 14) -> float | None:
    """Relative Strength Index using Wilder smoothing.

    Requires at least period + 1 data points.
    Returns 50.0 for flat markets (zero gains and zero losses).
    """
    if len(closes) < period + 1:
        return None

    deltas = [float(closes[i]) - float(closes[i - 1]) for i in range(1, len(closes))]

    # Initial average gain/loss (SMA of first `period` deltas)
    gains = [max(d, 0.0) for d in deltas[:period]]
    losses = [max(-d, 0.0) for d in deltas[:period]]
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    # Wilder smoothing for remaining deltas
    for d in deltas[period:]:
        avg_gain = (avg_gain * (period - 1) + max(d, 0.0)) / period
        avg_loss = (avg_loss * (period - 1) + max(-d, 0.0)) / period

    if avg_gain == 0.0 and avg_loss == 0.0:
        return 50.0  # flat market — neutral by convention

    if avg_loss == 0.0:
        return 100.0

    rs = avg_gain / avg_loss
    return 100.0 - 100.0 / (1.0 + rs)
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest tests/test_technical_analysis.py -v`
Expected: All SMA, EMA, RSI tests PASS

- [ ] **Step 7: Commit**

```bash
git add app/services/technical_analysis.py tests/test_technical_analysis.py
git commit -m "feat(#200): SMA, EMA, RSI indicator computation with tests"
```

---

### Task 3: TA computation module — MACD, Bollinger, ATR, Stochastic

**Files:**
- Modify: `app/services/technical_analysis.py`
- Modify: `tests/test_technical_analysis.py`

- [ ] **Step 1: Write failing tests for MACD**

```python
from app.services.technical_analysis import macd, bollinger_bands, atr, stochastic


class TestMACD:
    def test_macd_components(self) -> None:
        """MACD with 30 ascending values — line should be positive (fast > slow)."""
        closes = _make_closes([float(i) for i in range(1, 31)])
        result = macd(closes)
        assert result is not None
        line, signal, histogram = result
        assert line > 0  # fast EMA > slow EMA in uptrend
        assert histogram == _approx(line - signal)

    def test_macd_insufficient_data(self) -> None:
        """Need at least 26 + 9 - 1 = 34 values for signal line."""
        closes = _make_closes([float(i) for i in range(1, 20)])
        result = macd(closes)
        assert result is None

    def test_macd_flat_market(self) -> None:
        """Flat prices → MACD line, signal, and histogram all near zero."""
        closes = _make_closes([100.0] * 50)
        result = macd(closes)
        assert result is not None
        line, signal, histogram = result
        assert abs(line) < 0.01
        assert abs(signal) < 0.01
        assert abs(histogram) < 0.01
```

- [ ] **Step 2: Write failing tests for Bollinger Bands**

```python
class TestBollingerBands:
    def test_bollinger_flat_market(self) -> None:
        """Flat prices → upper = lower = SMA (zero stddev)."""
        closes = _make_closes([100.0] * 25)
        result = bollinger_bands(closes)
        assert result is not None
        upper, lower = result
        assert upper == _approx(100.0)
        assert lower == _approx(100.0)

    def test_bollinger_known_spread(self) -> None:
        """[1,2,3,...,20] — SMA(20)=10.5, pop stddev known."""
        closes = _make_closes([float(i) for i in range(1, 21)])
        result = bollinger_bands(closes, period=20, num_std=2.0)
        assert result is not None
        upper, lower = result
        # Population stddev of 1..20 = sqrt((20^2 - 1)/12) = sqrt(399/12) ≈ 5.7663
        import math
        pop_std = math.sqrt(sum((i - 10.5) ** 2 for i in range(1, 21)) / 20)
        assert upper == _approx(10.5 + 2.0 * pop_std)
        assert lower == _approx(10.5 - 2.0 * pop_std)

    def test_bollinger_insufficient_data(self) -> None:
        closes = _make_closes([1.0] * 10)
        result = bollinger_bands(closes, period=20)
        assert result is None
```

- [ ] **Step 3: Write failing tests for ATR**

```python
def _make_ohlcv(
    values: list[tuple[float, float, float, float]],
) -> list[OHLCVRow]:
    """Build OHLCV rows from (open, high, low, close) tuples."""
    return [
        OHLCVRow(
            open=Decimal(str(o)),
            high=Decimal(str(h)),
            low=Decimal(str(l)),
            close=Decimal(str(c)),
            volume=None,
        )
        for o, h, l, c in values
    ]


class TestATR:
    def test_atr_known_value(self) -> None:
        """ATR(14) with constant range bars.
        Each bar: open=100, high=105, low=95, close=100.
        True range = max(105-95, |105-100|, |95-100|) = 10.
        ATR should converge to 10.
        """
        bars = _make_ohlcv([(100.0, 105.0, 95.0, 100.0)] * 30)
        result = atr(bars, 14)
        assert result == _approx(10.0)

    def test_atr_with_gaps(self) -> None:
        """When prev_close is outside today's range, true range includes the gap."""
        bars = _make_ohlcv([
            (100.0, 105.0, 95.0, 100.0),  # TR = 10
        ] * 14 + [
            (110.0, 115.0, 108.0, 112.0),  # prev_close=100, TR = max(7, 15, 8) = 15
        ])
        result = atr(bars, 14)
        assert result is not None
        # After 14 bars of TR=10, one bar of TR=15
        # Wilder: (10 * 13 + 15) / 14 = 145/14 ≈ 10.357
        assert result == _approx(145.0 / 14.0)

    def test_atr_insufficient_data(self) -> None:
        bars = _make_ohlcv([(100.0, 105.0, 95.0, 100.0)] * 5)
        result = atr(bars, 14)
        assert result is None
```

- [ ] **Step 4: Write failing tests for Stochastic**

```python
class TestStochastic:
    def test_stochastic_at_high(self) -> None:
        """Close at the top of the 14-day range → %K = 100."""
        # 14 bars with range 90-110, close at 110
        bars = _make_ohlcv([(100.0, 110.0, 90.0, 110.0)] * 20)
        result = stochastic(bars)
        assert result is not None
        k, d = result
        assert k == _approx(100.0)
        assert d == _approx(100.0)  # SMA(3) of [100, 100, 100]

    def test_stochastic_at_low(self) -> None:
        """Close at the bottom of the range → %K = 0."""
        bars = _make_ohlcv([(100.0, 110.0, 90.0, 90.0)] * 20)
        result = stochastic(bars)
        assert result is not None
        k, d = result
        assert k == _approx(0.0)
        assert d == _approx(0.0)

    def test_stochastic_midpoint(self) -> None:
        """Close at midpoint of 14-day range → %K = 50."""
        bars = _make_ohlcv([(100.0, 110.0, 90.0, 100.0)] * 20)
        result = stochastic(bars)
        assert result is not None
        k, d = result
        assert k == _approx(50.0)

    def test_stochastic_insufficient_data(self) -> None:
        """Need at least 14 + 2 bars for %K(14) and %D = SMA(3)."""
        bars = _make_ohlcv([(100.0, 110.0, 90.0, 100.0)] * 10)
        result = stochastic(bars)
        assert result is None
```

- [ ] **Step 5: Run tests to verify they fail**

Run: `uv run pytest tests/test_technical_analysis.py::TestMACD tests/test_technical_analysis.py::TestBollingerBands tests/test_technical_analysis.py::TestATR tests/test_technical_analysis.py::TestStochastic -v`
Expected: ImportError or AttributeError (functions not defined yet)

- [ ] **Step 6: Implement MACD**

Add to `app/services/technical_analysis.py`:

```python
def macd(
    closes: list[Decimal],
    fast: int = 12,
    slow: int = 26,
    signal_period: int = 9,
) -> tuple[float, float, float] | None:
    """MACD: (macd_line, signal_line, histogram).

    macd_line = EMA(fast) - EMA(slow)
    signal = EMA(signal_period) of macd_line series
    histogram = macd_line - signal

    Requires at least slow + signal_period - 1 data points.
    """
    min_required = slow + signal_period - 1
    if len(closes) < min_required:
        return None

    # Compute full EMA series for MACD line
    fast_ema_series = _ema_series(closes, fast)
    slow_ema_series = _ema_series(closes, slow)
    if fast_ema_series is None or slow_ema_series is None:
        return None

    # MACD line: align from the point where both EMAs are available
    # slow EMA starts at index (slow - 1), fast at (fast - 1)
    start = slow - 1  # slow EMA available from this index onward
    macd_values = [
        fast_ema_series[i] - slow_ema_series[i - start + (slow - 1)]
        for i in range(start, len(closes))
    ]

    if len(macd_values) < signal_period:
        return None

    # Signal line: EMA of MACD values
    signal_seed = sum(macd_values[:signal_period]) / signal_period
    multiplier = 2.0 / (signal_period + 1)
    signal_value = signal_seed
    for v in macd_values[signal_period:]:
        signal_value = v * multiplier + signal_value * (1.0 - multiplier)

    macd_line = macd_values[-1]
    histogram = macd_line - signal_value
    return (macd_line, signal_value, histogram)
```

Also add the `_ema_series` helper:

```python
def _ema_series(closes: list[Decimal], period: int) -> list[float] | None:
    """Compute the full EMA series (one value per input from index period-1 onward)."""
    if len(closes) < period:
        return None
    seed = float(sum(closes[:period])) / period
    multiplier = 2.0 / (period + 1)
    result = [seed]
    for close in closes[period:]:
        result.append(float(close) * multiplier + result[-1] * (1.0 - multiplier))
    return result
```

- [ ] **Step 7: Implement Bollinger Bands**

```python
def bollinger_bands(
    closes: list[Decimal],
    period: int = 20,
    num_std: float = 2.0,
) -> tuple[float, float] | None:
    """Bollinger Bands: (upper, lower). Uses population stddev (ddof=0)."""
    if len(closes) < period:
        return None
    window = [float(c) for c in closes[-period:]]
    mean = sum(window) / period
    variance = sum((x - mean) ** 2 for x in window) / period
    std = math.sqrt(variance)
    return (mean + num_std * std, mean - num_std * std)
```

- [ ] **Step 8: Implement ATR**

```python
def atr(bars: list[OHLCVRow], period: int = 14) -> float | None:
    """Average True Range using Wilder smoothing.

    True Range = max(high - low, |high - prev_close|, |low - prev_close|).
    Requires at least period + 1 bars (period TRs for the initial average,
    plus one bar to establish prev_close).
    """
    if len(bars) < period + 1:
        return None

    true_ranges: list[float] = []
    for i in range(1, len(bars)):
        high = float(bars[i]["high"])
        low = float(bars[i]["low"])
        prev_close = float(bars[i - 1]["close"])
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        true_ranges.append(tr)

    # Initial ATR: SMA of first `period` true ranges
    atr_value = sum(true_ranges[:period]) / period

    # Wilder smoothing for remaining
    for tr in true_ranges[period:]:
        atr_value = (atr_value * (period - 1) + tr) / period

    return atr_value
```

- [ ] **Step 9: Implement Stochastic**

```python
def stochastic(
    bars: list[OHLCVRow],
    k_period: int = 14,
    d_period: int = 3,
) -> tuple[float, float] | None:
    """Stochastic oscillator: (%K, %D).

    %K = (close - lowest_low_14) / (highest_high_14 - lowest_low_14) * 100
    %D = SMA(d_period) of %K values

    Requires at least k_period + d_period - 1 bars.
    """
    min_required = k_period + d_period - 1
    if len(bars) < min_required:
        return None

    k_values: list[float] = []
    for i in range(k_period - 1, len(bars)):
        window = bars[i - k_period + 1 : i + 1]
        highest = max(float(b["high"]) for b in window)
        lowest = min(float(b["low"]) for b in window)
        close = float(bars[i]["close"])
        if highest == lowest:
            k_values.append(50.0)  # flat range — neutral
        else:
            k_values.append((close - lowest) / (highest - lowest) * 100.0)

    if len(k_values) < d_period:
        return None

    # %D = SMA of last d_period %K values
    latest_k = k_values[-1]
    d_value = sum(k_values[-d_period:]) / d_period
    return (latest_k, d_value)
```

- [ ] **Step 10: Run tests to verify they pass**

Run: `uv run pytest tests/test_technical_analysis.py -v`
Expected: All tests PASS

- [ ] **Step 11: Commit**

```bash
git add app/services/technical_analysis.py tests/test_technical_analysis.py
git commit -m "feat(#200): MACD, Bollinger, ATR, Stochastic indicators with tests"
```

---

### Task 4: `compute_indicators` orchestrator and cross-signal detection

**Files:**
- Modify: `app/services/technical_analysis.py`
- Modify: `tests/test_technical_analysis.py`

- [ ] **Step 1: Write failing tests for `compute_indicators`**

```python
class TestComputeIndicators:
    def test_full_dataset_returns_all_indicators(self) -> None:
        """With 200+ bars, all indicators should be populated."""
        bars = _make_ohlcv([(100.0 + i * 0.5, 102.0 + i * 0.5, 98.0 + i * 0.5, 100.5 + i * 0.5) for i in range(250)])
        result = compute_indicators(bars)
        assert result is not None
        # All keys present
        for key in [
            "sma_20", "sma_50", "sma_200",
            "ema_12", "ema_26",
            "macd_line", "macd_signal", "macd_histogram",
            "rsi_14", "stoch_k", "stoch_d",
            "bb_upper", "bb_lower", "atr_14",
        ]:
            assert key in result, f"Missing key: {key}"
            assert result[key] is not None, f"None for key: {key}"

    def test_insufficient_history_returns_none_for_long_indicators(self) -> None:
        """With only 30 bars, SMA(200) should be None, but SMA(20) and RSI should work."""
        bars = _make_ohlcv([(100.0, 105.0, 95.0, 100.0)] * 30)
        result = compute_indicators(bars)
        assert result is not None
        assert result["sma_200"] is None
        assert result["sma_50"] is None
        assert result["sma_20"] is not None
        assert result["rsi_14"] is not None

    def test_empty_bars_returns_none(self) -> None:
        result = compute_indicators([])
        assert result is None

    def test_golden_cross_detected(self) -> None:
        """When SMA(50) crosses above SMA(200), golden_cross should be True."""
        # Build a series where prices rise enough for SMA(50) > SMA(200)
        # at the end but SMA(50) < SMA(200) one bar earlier.
        # Use 250 bars: flat at 100 for first 200, then rise to 150.
        bars = _make_ohlcv(
            [(100.0, 101.0, 99.0, 100.0)] * 200
            + [(100.0 + i, 101.0 + i, 99.0 + i, 100.0 + i) for i in range(1, 51)]
        )
        result = compute_indicators(bars)
        assert result is not None
        # At minimum, the cross-signal fields should be present
        assert "trend_sma_cross" in result

    def test_price_above_sma200(self) -> None:
        """Close above SMA(200) → price_vs_sma200 = 'above'."""
        bars = _make_ohlcv(
            [(50.0, 51.0, 49.0, 50.0)] * 200
            + [(150.0, 151.0, 149.0, 150.0)] * 50
        )
        result = compute_indicators(bars)
        assert result is not None
        assert result.get("price_vs_sma200") == "above"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_technical_analysis.py::TestComputeIndicators -v`
Expected: FAIL (compute_indicators not implemented yet or missing keys)

- [ ] **Step 3: Implement `compute_indicators`**

Add to `app/services/technical_analysis.py`:

```python
def compute_indicators(bars: list[OHLCVRow]) -> dict[str, float | str | None] | None:
    """Compute all TA indicators for the latest bar.

    Returns a dict keyed by column name (matching price_daily columns),
    or None if no bars are provided.

    Cross signals (golden/death cross, price vs SMA200) are derived from
    recent raw candles at computation time — not stored as columns.
    """
    if not bars:
        return None

    closes = [b["close"] for b in bars]

    # Trend
    sma_20 = sma(closes, 20)
    sma_50 = sma(closes, 50)
    sma_200 = sma(closes, 200)
    ema_12 = ema(closes, 12)
    ema_26 = ema(closes, 26)

    # MACD
    macd_result = macd(closes)
    macd_line_val = macd_result[0] if macd_result else None
    macd_signal_val = macd_result[1] if macd_result else None
    macd_histogram_val = macd_result[2] if macd_result else None

    # Momentum
    rsi_14 = rsi(closes, 14)
    stoch_result = stochastic(bars)
    stoch_k_val = stoch_result[0] if stoch_result else None
    stoch_d_val = stoch_result[1] if stoch_result else None

    # Volatility
    bb_result = bollinger_bands(closes, 20, 2.0)
    bb_upper_val = bb_result[0] if bb_result else None
    bb_lower_val = bb_result[1] if bb_result else None
    atr_14 = atr(bars, 14)

    # Cross-signal detection (derived, not stored)
    current_close = float(closes[-1])

    # Price vs SMA(200)
    price_vs_sma200: str | None = None
    if sma_200 is not None:
        price_vs_sma200 = "above" if current_close > sma_200 else "below"

    # Golden/death cross: SMA(50) vs SMA(200) comparison
    # We need SMA(50) and SMA(200) for both latest and one-bar-earlier
    trend_sma_cross: str | None = None
    if sma_50 is not None and sma_200 is not None and len(closes) > 200:
        prev_sma_50 = sma(closes[:-1], 50)
        prev_sma_200 = sma(closes[:-1], 200)
        if prev_sma_50 is not None and prev_sma_200 is not None:
            if sma_50 > sma_200 and prev_sma_50 <= prev_sma_200:
                trend_sma_cross = "golden"
            elif sma_50 < sma_200 and prev_sma_50 >= prev_sma_200:
                trend_sma_cross = "death"
            else:
                trend_sma_cross = "none"

    return {
        # Stored columns (match price_daily column names)
        "sma_20": sma_20,
        "sma_50": sma_50,
        "sma_200": sma_200,
        "ema_12": ema_12,
        "ema_26": ema_26,
        "macd_line": macd_line_val,
        "macd_signal": macd_signal_val,
        "macd_histogram": macd_histogram_val,
        "rsi_14": rsi_14,
        "stoch_k": stoch_k_val,
        "stoch_d": stoch_d_val,
        "bb_upper": bb_upper_val,
        "bb_lower": bb_lower_val,
        "atr_14": atr_14,
        # Derived (not stored — used at scoring time)
        "price_vs_sma200": price_vs_sma200,
        "trend_sma_cross": trend_sma_cross,
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_technical_analysis.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add app/services/technical_analysis.py tests/test_technical_analysis.py
git commit -m "feat(#200): compute_indicators orchestrator with cross-signal detection"
```

---

### Task 5: Integrate TA computation into market data refresh

**Files:**
- Modify: `app/services/market_data.py`
- Modify: `tests/test_market_data.py` (or create if it doesn't exist)

- [ ] **Step 1: Write failing test for TA column persistence**

Add to `tests/test_market_data.py`:

```python
"""Test that _compute_and_store_features calls TA computation and persists results."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, call, patch

import pytest

from app.services.market_data import _compute_and_store_features


class TestComputeAndStoreFeaturesTA:
    """Verify that TA indicators are computed and written to price_daily."""

    def test_ta_columns_included_in_update(self) -> None:
        """When enough OHLCV data exists, TA columns should appear in the UPDATE params."""
        # Build 250 rows of fake OHLCV data (enough for all indicators)
        mock_rows = [
            (date(2025, 1, 1) + __import__("datetime").timedelta(days=i), Decimal("100.0") + Decimal(str(i * 0.1)))
            for i in range(250)
        ]
        # Also need OHLCV for TA — we need to mock the second query
        mock_ohlcv = [
            {
                "open": Decimal("100.0") + Decimal(str(i * 0.1)),
                "high": Decimal("101.0") + Decimal(str(i * 0.1)),
                "low": Decimal("99.0") + Decimal(str(i * 0.1)),
                "close": Decimal("100.0") + Decimal(str(i * 0.1)),
                "volume": 1000,
            }
            for i in range(250)
        ]

        conn = MagicMock()
        execute_results = iter([
            # First call: price_date + close query
            MagicMock(fetchall=MagicMock(return_value=list(reversed(mock_rows)))),
            # Second call: OHLCV query for TA
            MagicMock(fetchall=MagicMock(return_value=list(reversed([
                (r["open"], r["high"], r["low"], r["close"], r["volume"]) for r in mock_ohlcv
            ])))),
            # Third call: UPDATE
            MagicMock(),
        ])
        conn.execute.side_effect = lambda *a, **kw: next(execute_results)

        result = _compute_and_store_features(conn, instrument_id=1)
        assert result == 1

        # The UPDATE call should include TA column params
        update_call = conn.execute.call_args_list[-1]
        update_sql = str(update_call.args[0])
        assert "rsi_14" in update_sql
        assert "sma_20" in update_sql
        assert "macd_line" in update_sql
        assert "atr_14" in update_sql

    def test_ta_graceful_with_insufficient_data(self) -> None:
        """With only 10 rows, TA indicators should be None but returns still computed."""
        mock_rows = [
            (date(2025, 1, 1) + __import__("datetime").timedelta(days=i), Decimal("100.0"))
            for i in range(10)
        ]
        mock_ohlcv = [
            (Decimal("100.0"), Decimal("101.0"), Decimal("99.0"), Decimal("100.0"), 1000)
            for _ in range(10)
        ]

        conn = MagicMock()
        execute_results = iter([
            MagicMock(fetchall=MagicMock(return_value=list(reversed(mock_rows)))),
            MagicMock(fetchall=MagicMock(return_value=list(reversed(mock_ohlcv)))),
            MagicMock(),
        ])
        conn.execute.side_effect = lambda *a, **kw: next(execute_results)

        result = _compute_and_store_features(conn, instrument_id=1)
        assert result == 1

        update_params = conn.execute.call_args_list[-1].args[1]
        # SMA(200) should be None with only 10 rows
        assert update_params["sma_200"] is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_market_data.py::TestComputeAndStoreFeaturesTA -v`
Expected: FAIL (UPDATE SQL doesn't include TA columns yet)

- [ ] **Step 3: Extend `_compute_and_store_features` to call TA computation**

Modify `app/services/market_data.py`:

1. Add import at top:
```python
from app.services.technical_analysis import OHLCVRow, compute_indicators
```

2. Inside `_compute_and_store_features`, after the existing `rows` fetch and before the UPDATE, add a second query to fetch OHLCV data and compute TA:

```python
    # --- TA indicators (computed from full OHLCV, not just close) ---
    ohlcv_rows = conn.execute(
        """
        SELECT open, high, low, close, volume
        FROM price_daily
        WHERE instrument_id = %(instrument_id)s
          AND close IS NOT NULL
        ORDER BY price_date DESC
        LIMIT 400
        """,
        {"instrument_id": instrument_id},
    ).fetchall()

    ta_indicators: dict[str, float | str | None] = {}
    if ohlcv_rows:
        # Reverse to oldest-first for TA computation
        bars: list[OHLCVRow] = [
            OHLCVRow(open=r[0], high=r[1], low=r[2], close=r[3], volume=r[4])
            for r in reversed(ohlcv_rows)
        ]
        result = compute_indicators(bars)
        if result is not None:
            # Only keep storable columns (not derived cross-signals)
            ta_indicators = {
                k: v for k, v in result.items()
                if k not in ("price_vs_sma200", "trend_sma_cross")
            }
```

3. Extend the UPDATE statement to include TA columns:

```python
    conn.execute(
        """
        UPDATE price_daily SET
            return_1w      = %(return_1w)s,
            return_1m      = %(return_1m)s,
            return_3m      = %(return_3m)s,
            return_6m      = %(return_6m)s,
            return_1y      = %(return_1y)s,
            volatility_30d = %(volatility_30d)s,
            sma_20         = %(sma_20)s,
            sma_50         = %(sma_50)s,
            sma_200        = %(sma_200)s,
            ema_12         = %(ema_12)s,
            ema_26         = %(ema_26)s,
            macd_line      = %(macd_line)s,
            macd_signal    = %(macd_signal)s,
            macd_histogram = %(macd_histogram)s,
            rsi_14         = %(rsi_14)s,
            stoch_k        = %(stoch_k)s,
            stoch_d        = %(stoch_d)s,
            bb_upper       = %(bb_upper)s,
            bb_lower       = %(bb_lower)s,
            atr_14         = %(atr_14)s
        WHERE instrument_id = %(instrument_id)s
          AND price_date = %(price_date)s
        """,
        {
            "instrument_id": instrument_id,
            "price_date": latest_date,
            "return_1w": returns.get("return_1w"),
            "return_1m": returns.get("return_1m"),
            "return_3m": returns.get("return_3m"),
            "return_6m": returns.get("return_6m"),
            "return_1y": returns.get("return_1y"),
            "volatility_30d": volatility,
            **{k: (Decimal(str(round(v, 6))) if isinstance(v, float) else None) for k, v in ta_indicators.items()},
            # Ensure all TA keys present even if compute_indicators returned None
            **{k: None for k in [
                "sma_20", "sma_50", "sma_200", "ema_12", "ema_26",
                "macd_line", "macd_signal", "macd_histogram",
                "rsi_14", "stoch_k", "stoch_d",
                "bb_upper", "bb_lower", "atr_14",
            ] if k not in ta_indicators},
        },
    )
```

Note: The `**{k: None ...}` dict is built first, then `**ta_indicators` overwrites with actual values. Reverse the merge order to get this right:

```python
    ta_params: dict[str, Decimal | None] = {
        k: None for k in [
            "sma_20", "sma_50", "sma_200", "ema_12", "ema_26",
            "macd_line", "macd_signal", "macd_histogram",
            "rsi_14", "stoch_k", "stoch_d",
            "bb_upper", "bb_lower", "atr_14",
        ]
    }
    for k, v in ta_indicators.items():
        ta_params[k] = Decimal(str(round(v, 6))) if isinstance(v, float) else None

    conn.execute(
        """...""",
        {
            "instrument_id": instrument_id,
            "price_date": latest_date,
            "return_1w": returns.get("return_1w"),
            "return_1m": returns.get("return_1m"),
            "return_3m": returns.get("return_3m"),
            "return_6m": returns.get("return_6m"),
            "return_1y": returns.get("return_1y"),
            "volatility_30d": volatility,
            **ta_params,
        },
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_market_data.py tests/test_technical_analysis.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add app/services/market_data.py tests/test_market_data.py
git commit -m "feat(#200): integrate TA computation into daily candle refresh"
```

---

### Task 6: Enhance `_momentum_score` with TA subcomponents

**Files:**
- Modify: `app/services/scoring.py`
- Modify: `tests/test_scoring.py`

The enhanced momentum score blends the existing return-based score with three new TA subcomponents:
- **Trend confirmation (40%)**: Price vs SMA(200), MACD histogram direction
- **Momentum quality (30%)**: RSI regime, Stochastic position
- **Volatility regime (30%)**: Bollinger Band position, ATR relative to price

When TA data is unavailable (insufficient history, pre-migration), the score falls back to pure return-based scoring — backward compatible.

- [ ] **Step 1: Write failing tests for enhanced momentum score**

Add to `tests/test_scoring.py`:

```python
class TestEnhancedMomentumScore:
    """Tests for _momentum_score with TA inputs."""

    def test_backward_compatible_no_ta(self) -> None:
        """When no TA dict is passed, score matches original return-only behavior."""
        score_old, notes_old = _momentum_score(
            return_1m=0.10, return_3m=0.20, return_6m=0.30,
        )
        score_new, notes_new = _momentum_score(
            return_1m=0.10, return_3m=0.20, return_6m=0.30,
            ta_indicators=None,
        )
        assert score_new == _approx(score_old)

    def test_strong_trend_confirmation_boosts_score(self) -> None:
        """Price above SMA(200) + positive MACD + positive returns → high score."""
        ta = {
            "sma_200": 90.0,
            "macd_histogram": 2.5,
            "rsi_14": 60.0,
            "stoch_k": 70.0,
            "stoch_d": 65.0,
            "bb_upper": 120.0,
            "bb_lower": 80.0,
            "atr_14": 3.0,
            "current_close": 110.0,
        }
        score, notes = _momentum_score(
            return_1m=0.10, return_3m=0.20, return_6m=0.30,
            ta_indicators=ta,
        )
        assert score > 0.7
        assert not any("TA" in n and "missing" in n for n in notes)

    def test_bearish_ta_drags_score_down(self) -> None:
        """Price below SMA(200) + negative MACD + overbought RSI → lower score."""
        ta = {
            "sma_200": 120.0,
            "macd_histogram": -3.0,
            "rsi_14": 80.0,  # overbought
            "stoch_k": 90.0,
            "stoch_d": 85.0,
            "bb_upper": 115.0,
            "bb_lower": 105.0,
            "atr_14": 5.0,
            "current_close": 100.0,
        }
        score_no_ta, _ = _momentum_score(
            return_1m=0.10, return_3m=0.20, return_6m=0.30,
        )
        score_with_ta, _ = _momentum_score(
            return_1m=0.10, return_3m=0.20, return_6m=0.30,
            ta_indicators=ta,
        )
        # Bearish TA should pull score below pure return-based score
        assert score_with_ta < score_no_ta

    def test_partial_ta_data_uses_available(self) -> None:
        """When some TA values are None, available ones still contribute."""
        ta = {
            "sma_200": None,  # insufficient history
            "macd_histogram": 1.0,
            "rsi_14": 55.0,
            "stoch_k": None,
            "stoch_d": None,
            "bb_upper": None,
            "bb_lower": None,
            "atr_14": None,
            "current_close": 100.0,
        }
        score, notes = _momentum_score(
            return_1m=0.10, return_3m=0.20, return_6m=0.30,
            ta_indicators=ta,
        )
        assert 0.0 <= score <= 1.0
        # Should note missing TA components
        assert any("sma_200" in n.lower() or "TA" in n for n in notes)

    def test_all_missing_returns_and_no_ta(self) -> None:
        """No returns + no TA = neutral 0.5."""
        score, notes = _momentum_score(None, None, None, ta_indicators=None)
        assert score == _approx(0.5)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_scoring.py::TestEnhancedMomentumScore -v`
Expected: FAIL (signature doesn't accept ta_indicators yet)

- [ ] **Step 3: Update `_momentum_score` signature and implement TA blending**

Modify `_momentum_score` in `app/services/scoring.py`:

```python
def _momentum_score(
    return_1m: float | None,
    return_3m: float | None,
    return_6m: float | None,
    *,
    ta_indicators: dict[str, float | None] | None = None,
) -> tuple[float, list[str]]:
    """
    Blended momentum score combining return-based signals with TA indicators.

    When ta_indicators is None or empty, falls back to pure return scoring
    (backward compatible).

    TA blending (when available):
      - Return-based score: 40% weight
      - Trend confirmation: 25% weight (price vs SMA200, MACD)
      - Momentum quality: 20% weight (RSI regime, Stochastic)
      - Volatility regime: 15% weight (Bollinger position, ATR context)

    Returns (score, notes) where notes lists missing components.
    """
    notes: list[str] = []

    # --- Return-based component (original logic) ---
    return_components: list[tuple[float, float]] = []

    if return_1m is not None:
        s1m = _clip((return_1m + 0.10) / 0.30)
        return_components.append((s1m, 0.20))
    else:
        notes.append("return_1m missing")

    if return_3m is not None:
        s3m = _clip((return_3m + 0.15) / 0.45)
        return_components.append((s3m, 0.50))
    else:
        notes.append("return_3m missing")

    if return_6m is not None:
        s6m = _clip((return_6m + 0.20) / 0.60)
        return_components.append((s6m, 0.30))
    else:
        notes.append("return_6m missing")

    if not return_components:
        return_score: float | None = None
    else:
        total_w = sum(w for _, w in return_components)
        return_score = sum(s * w / total_w for s, w in return_components)

    # --- If no TA data, fall back to return-only scoring ---
    if not ta_indicators or not any(
        ta_indicators.get(k) is not None
        for k in ("sma_200", "macd_histogram", "rsi_14", "stoch_k", "bb_upper", "atr_14")
    ):
        if return_score is None:
            return 0.5, notes
        return _clip(return_score), notes

    # --- TA subcomponents ---
    current_close = ta_indicators.get("current_close")

    # 1. Trend confirmation (SMA200 + MACD histogram)
    trend_parts: list[tuple[float, float]] = []

    sma_200 = ta_indicators.get("sma_200")
    if sma_200 is not None and current_close is not None:
        # Above SMA200 → bullish (0.7-1.0 based on distance)
        # Below → bearish (0.0-0.3)
        pct_from_sma = (current_close - sma_200) / sma_200 if sma_200 != 0 else 0
        trend_parts.append((_clip(0.5 + pct_from_sma * 2.5), 0.60))
    else:
        notes.append("TA: sma_200 unavailable")

    macd_hist = ta_indicators.get("macd_histogram")
    if macd_hist is not None:
        # Positive histogram → bullish, negative → bearish
        # Normalise: histogram in typical range [-5, 5]
        macd_signal = _clip(0.5 + macd_hist / 10.0)
        trend_parts.append((macd_signal, 0.40))
    else:
        notes.append("TA: macd_histogram unavailable")

    trend_score: float | None = None
    if trend_parts:
        tw = sum(w for _, w in trend_parts)
        trend_score = sum(s * w / tw for s, w in trend_parts)

    # 2. Momentum quality (RSI + Stochastic)
    mq_parts: list[tuple[float, float]] = []

    rsi_val = ta_indicators.get("rsi_14")
    if rsi_val is not None:
        # RSI 30-70 is healthy momentum territory.
        # Overbought (>70) penalised, oversold (<30) penalised.
        # Sweet spot: 50-65 → highest score.
        if rsi_val < 30:
            rsi_score = rsi_val / 60.0  # 0→0, 30→0.5
        elif rsi_val <= 70:
            rsi_score = 0.5 + (rsi_val - 30) / 80.0  # 30→0.5, 70→1.0
        else:
            rsi_score = max(0.0, 1.0 - (rsi_val - 70) / 30.0)  # 70→1.0, 100→0.0
        mq_parts.append((_clip(rsi_score), 0.60))
    else:
        notes.append("TA: rsi_14 unavailable")

    stoch_k = ta_indicators.get("stoch_k")
    if stoch_k is not None:
        # Similar to RSI: mid-range is good, extremes are warning
        if stoch_k < 20:
            stoch_score = stoch_k / 40.0
        elif stoch_k <= 80:
            stoch_score = 0.5 + (stoch_k - 20) / 120.0
        else:
            stoch_score = max(0.0, 1.0 - (stoch_k - 80) / 20.0)
        mq_parts.append((_clip(stoch_score), 0.40))
    else:
        notes.append("TA: stoch_k unavailable")

    mq_score: float | None = None
    if mq_parts:
        mw = sum(w for _, w in mq_parts)
        mq_score = sum(s * w / mw for s, w in mq_parts)

    # 3. Volatility regime (Bollinger position + ATR)
    vol_parts: list[tuple[float, float]] = []

    bb_upper = ta_indicators.get("bb_upper")
    bb_lower = ta_indicators.get("bb_lower")
    if bb_upper is not None and bb_lower is not None and current_close is not None:
        bb_width = bb_upper - bb_lower
        if bb_width > 0:
            # Position within bands: 0 = lower, 1 = upper
            bb_position = (current_close - bb_lower) / bb_width
            # Mid-band is healthiest; near upper = stretched; near lower = weak
            vol_parts.append((_clip(bb_position), 0.60))
        else:
            vol_parts.append((0.5, 0.60))  # flat band
    else:
        notes.append("TA: bollinger bands unavailable")

    atr_val = ta_indicators.get("atr_14")
    if atr_val is not None and current_close is not None and current_close > 0:
        # ATR as % of price — low vol environment scores higher for trend-following
        atr_pct = atr_val / current_close
        # atr_pct typically 1-5%. Lower = calmer = better for trend continuation
        vol_score = _clip(1.0 - atr_pct * 10.0)  # 0%→1.0, 5%→0.5, 10%→0.0
        vol_parts.append((vol_score, 0.40))
    else:
        notes.append("TA: atr_14 unavailable")

    vol_score_final: float | None = None
    if vol_parts:
        vw = sum(w for _, w in vol_parts)
        vol_score_final = sum(s * w / vw for s, w in vol_parts)

    # --- Blend all components ---
    final_parts: list[tuple[float, float]] = []
    if return_score is not None:
        final_parts.append((return_score, 0.40))
    if trend_score is not None:
        final_parts.append((trend_score, 0.25))
    if mq_score is not None:
        final_parts.append((mq_score, 0.20))
    if vol_score_final is not None:
        final_parts.append((vol_score_final, 0.15))

    if not final_parts:
        return 0.5, notes

    total_w = sum(w for _, w in final_parts)
    blended = sum(s * w / total_w for s, w in final_parts)
    return _clip(blended), notes
```

- [ ] **Step 4: Update the data loading to pass TA indicators to momentum score**

Modify the caller of `_momentum_score` in `scoring.py`. In `_load_instrument_data`, extend the price_daily query:

```python
        # Latest price features
        cur.execute(
            """
            SELECT return_1m, return_3m, return_6m, close,
                   sma_200, macd_histogram, rsi_14,
                   stoch_k, stoch_d,
                   bb_upper, bb_lower, atr_14
            FROM price_daily
            WHERE instrument_id = %(id)s
              AND close IS NOT NULL
            ORDER BY price_date DESC
            LIMIT 1
            """,
            {"id": instrument_id},
        )
        price_row: dict[str, Any] | None = cur.fetchone()
```

Then in the scoring function where `_momentum_score` is called, build the `ta_indicators` dict from `price_row`:

```python
    # Build TA indicators dict for momentum score
    ta_indicators: dict[str, float | None] | None = None
    if price_row is not None:
        ta_keys = ["sma_200", "macd_histogram", "rsi_14", "stoch_k", "stoch_d", "bb_upper", "bb_lower", "atr_14"]
        ta_raw = {k: price_row.get(k) for k in ta_keys}
        if any(v is not None for v in ta_raw.values()):
            ta_indicators = {k: float(v) if v is not None else None for k, v in ta_raw.items()}
            ta_indicators["current_close"] = float(current_price) if current_price else None

    momentum, momentum_notes = _momentum_score(
        return_1m=float(price_row["return_1m"]) if price_row and price_row.get("return_1m") is not None else None,
        return_3m=float(price_row["return_3m"]) if price_row and price_row.get("return_3m") is not None else None,
        return_6m=float(price_row["return_6m"]) if price_row and price_row.get("return_6m") is not None else None,
        ta_indicators=ta_indicators,
    )
```

- [ ] **Step 5: Run all tests**

Run: `uv run pytest tests/test_scoring.py tests/test_technical_analysis.py tests/test_market_data.py -v`
Expected: All tests PASS

- [ ] **Step 6: Run full pre-push checklist**

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

Expected: All four pass.

- [ ] **Step 7: Commit**

```bash
git add app/services/scoring.py tests/test_scoring.py
git commit -m "feat(#200): enhance momentum score with TA subcomponents (trend, momentum quality, volatility regime)"
```

---

### Task 7: Smoke test — verify TA integration end-to-end

**Files:**
- Existing: `tests/smoke/test_app_boots.py` (verify no regressions)

- [ ] **Step 1: Run the smoke test to ensure the app still boots**

Run: `uv run pytest tests/smoke/test_app_boots.py -v`
Expected: PASS

- [ ] **Step 2: Run migration against dev DB**

Run: `psql $DATABASE_URL -f sql/025_technical_analysis_columns.sql`
Expected: All ALTER TABLE statements succeed.

- [ ] **Step 3: Verify full test suite**

Run: `uv run pytest -v`
Expected: All tests PASS

- [ ] **Step 4: Run pre-push checklist**

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

Expected: All four pass.

---

### Task 8: Final review and push

This task follows the branch/PR workflow from CLAUDE.md.

- [ ] **Step 1: Review the full diff**

Run: `git diff main --stat` and `git diff main` to review all changes.

- [ ] **Step 2: Run Codex review before push**

```bash
codex.cmd review --base main
```

Address any real findings. If deferring, ask Codex to confirm.

- [ ] **Step 3: Push and open PR**

```bash
git push -u origin feature/200-technical-analysis-engine
gh pr create --title "feat(#200): technical analysis engine" --body "..."
```

- [ ] **Step 4: Poll review and CI**

Poll `gh pr view <n> --comments` and `gh pr checks <n>` until review has posted on the latest commit SHA and CI is green.

- [ ] **Step 5: Resolve review comments**

Each comment resolved as `FIXED {sha}`, `DEFERRED #{issue}`, or `REBUTTED {reason}`.
Each PREVENTION comment resolved as `EXTRACTED {file}`, `ALREADY_COVERED {file}`, or `REBUTTED {reason}`.

- [ ] **Step 6: Codex review before merge**

```bash
codex.cmd review --base main
```

Merge only after Codex confirms + CI green on latest SHA.

- [ ] **Step 7: Merge, clean up**

```bash
gh pr merge <n> --squash --delete-branch
git branch -d feature/200-technical-analysis-engine
```

Close linked issue #200.
