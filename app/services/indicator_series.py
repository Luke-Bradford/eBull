"""Phase 2 — streaming, strictly causal indicator SERIES over stored OHLCV.

Spec: ``docs/proposals/ta/2026-08-05-historical-indicator-recompute.md``.
Ticket 2a. Refs #2240, #2260, #2279.

WHY THIS EXISTS
---------------
``technical_analysis.py`` returns ONE value for the latest bar and is O(n) per
call, so building a historical series with it is O(n²). Measured on the five
deepest real corpus series (16,236 bars each): **28.2 s naive vs 0.0026 s
streaming, ~10,000x, with zero mismatches across 81,107 compared values.**
Corpus-wide ``sum(n²)/sum(n) = 7,480``, i.e. ~5.7 hours against ~4 seconds.

⚠ NOTHING IS PERSISTED. Not a table, not a column, not a cache. That is settled
twice over — sql/249 declines indicator columns on ``research_price_daily``
("storage and drift for no read"), and #2279 §6 declines persisted
swings/levels on measured cost. A phase-3 *signal* is what gets stored, and it
carries ``rule_set_version`` so it can be invalidated against these rules.

⚠ CAUSALITY IS THE PRIMARY INVARIANT, NOT A PREFERENCE. #2260 recorded
RSI<30 → 76.8% 20-day hit rate. A causal recompute measures 51.8% / 50.4% on
the two full corpora: a non-causal recompute manufactured a 27-point phantom
edge that survived every arithmetic check for months. Every value at index i
here depends on inputs 0..i and nothing later.

WHAT THIS MODULE DOES NOT GUARD
-------------------------------
⚠ Quarantine and adjustment basis are the CALLER's gate. These functions have
no database access and compute over whatever bars they are handed, so a caller
can feed them quarantined or unadjusted bars and get numbers back. ``universe``
alone is therefore insufficient provenance — phase 3 must also record the
eligibility predicate it applied. Stated here rather than silently assumed,
because ``price_structure._atr_at`` fails CLOSED on masked bars and this module
structurally cannot.

⚠ Look-ahead is broader than indicator causality. Fill timing (a signal on
close(t) fills at open(t+1)) and as-of eligibility belong to phases 3-5. Nothing
here closes those.
"""

from __future__ import annotations

import hashlib
from collections import deque
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Literal

from app.services.technical_analysis import OHLCVRow

# ---------------------------------------------------------------------------
# Rule-set version
# ---------------------------------------------------------------------------
# Same construction as price_quarantine.py and price_structure.py: a stable id
# plus a hash of THIS MODULE'S SOURCE. Over-invalidation is the deliberate,
# inherited trade — a comment edit changes the string, which makes every
# previously stored signal visibly stale rather than silently mixed.
RULE_SET_ID = "indicator-series-v1"


def _code_hash() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


RULE_SET_VERSION = f"{RULE_SET_ID}+{_code_hash()}"

#: Inherited verbatim from price_structure. REQUIRED on every entry point with
#: NO DEFAULT — a caller cannot construct a result without stating which
#: universe the bars came from (#2288's survivor-labelling contract). A field
#: with a default is a field a consumer can bypass.
Universe = Literal["survivor_only", "survivorship_free"]


# ---------------------------------------------------------------------------
# Input
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BarSeries:
    """Date-stamped bars, validated at construction.

    ⚠ THE SPEC ASSERTED ORDERING AS AN INVARIANT AND THAT WAS NOT ENFORCEABLE
    ON THE SIGNATURES IT PROPOSED. ``OHLCVRow`` carries no date, so
    ``rsi_series(closes)`` had no way to know whether it had been handed bars
    newest-first — which passes every value-equality fixture while inverting
    time in production, the one look-ahead no amount of causality testing on a
    correctly-ordered fixture can catch.

    Making it a validated input type moves the guard from "the caller should"
    to "the caller cannot not". Ordering and duplicate-date rejection happen
    here, once, rather than in seven functions.

    ⚠ Calendar GAPS are allowed and never interpolated. A missing session is
    normal (holidays, halts, an instrument that had not listed yet); a
    fabricated bar to fill it would be an invented observation, which is a
    worse failure than a gap.
    """

    dates: tuple[date, ...]
    rows: tuple[OHLCVRow, ...]

    def __post_init__(self) -> None:
        if len(self.dates) != len(self.rows):
            raise ValueError(f"BarSeries length mismatch: {len(self.dates)} dates, {len(self.rows)} rows")
        for i in range(1, len(self.dates)):
            if self.dates[i] == self.dates[i - 1]:
                raise ValueError(f"BarSeries duplicate date at index {i}: {self.dates[i]}")
            if self.dates[i] < self.dates[i - 1]:
                raise ValueError(
                    f"BarSeries not ascending at index {i}: {self.dates[i - 1]} then {self.dates[i]}. "
                    "Bars must be oldest-first — reversed input silently inverts time."
                )

    def __len__(self) -> int:
        return len(self.rows)

    @property
    def closes(self) -> list[Decimal | None]:
        return [row.get("close") for row in self.rows]

    # ⚠ Decimal -> float conversion, done ONCE per series and cached.
    #
    # Measured before this existed: stochastic ran at 5,329 ns/bar because it
    # converted 14 highs and 14 lows per bar — 28 Decimal->float conversions to
    # produce one value. Across the corpus the seven indicators together came
    # to 305.6 s against the spec's < 60 s acceptance, and conversion was the
    # dominant term, not the arithmetic.
    #
    # `object.__setattr__` because the dataclass is frozen: the cache is
    # derived state, not mutable content, and the alternative (dropping
    # frozen) would give up the immutability the contract depends on.

    def _floats(self, field: str) -> list[float | None]:
        cache: dict[str, list[float | None]] | None = getattr(self, "_float_cache", None)
        if cache is None:
            cache = {}
            object.__setattr__(self, "_float_cache", cache)
        cached = cache.get(field)
        if cached is None:
            value = self.rows[0].get(field) if self.rows else None
            del value
            cached = [None if (v := row.get(field)) is None else float(v) for row in self.rows]
            cache[field] = cached
        return cached

    @property
    def float_closes(self) -> list[float | None]:
        return self._floats("close")

    @property
    def float_highs(self) -> list[float | None]:
        return self._floats("high")

    @property
    def float_lows(self) -> list[float | None]:
        return self._floats("low")


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class IndicatorSeries:
    """One indicator over one BarSeries.

    ``len(values) == len(input)`` ALWAYS. An offset series is how an off-by-one
    enters a backtest, so an index into ``values`` is always an index into the
    bars.

    ⚠ ``None`` in ``values`` is ambiguous on its own and that ambiguity is the
    vacuous-truth class the prevention log already carries (design-doc decision
    5: a rule returns fired / not_fired / **not_evaluable**, never a bare
    boolean). ``not_evaluable_indices`` disambiguates: a ``None`` at an index
    listed there is "could not compute from this input", and a ``None`` not
    listed is warm-up.
    """

    values: tuple[float | None, ...]
    universe: Universe
    #: Indices where the input could not support a value — a NULL OHLC field,
    #: or a window containing one. NOT warm-up.
    not_evaluable_indices: tuple[int, ...] = ()
    rule_set_version: str = RULE_SET_VERSION

    def __len__(self) -> int:
        return len(self.values)


@dataclass(frozen=True)
class MultiIndicatorSeries:
    """For indicators that emit several aligned components (MACD, Bollinger,
    stochastic). Kept as named series rather than a list of tuples so a
    consumer indexes by name and cannot transpose the components.

    ⚠ ``frozen=True`` here buys immutability, NOT hashability — ``components``
    is a dict, so hashing raises at runtime despite the generated ``__hash__``.
    Same for ``BarSeries``, whose rows are ``OHLCVRow`` TypedDicts. Neither is
    intended to be a set member or a dict key; if that is ever needed, convert
    to a tuple of pairs rather than removing the ergonomics for every caller.
    """

    components: dict[str, tuple[float | None, ...]]
    universe: Universe
    not_evaluable_indices: tuple[int, ...] = ()
    rule_set_version: str = RULE_SET_VERSION

    def __post_init__(self) -> None:
        # ⚠ Components must be mutually aligned, and the length is DERIVED from
        # them rather than passed in. The first draft carried a `_length` field
        # that nothing validated, so a caller constructing this directly could
        # get a `__len__` disagreeing with the data — an alignment bug in the
        # one object whose entire job is alignment.
        lengths = {len(values) for values in self.components.values()}
        if len(lengths) > 1:
            raise ValueError(f"MultiIndicatorSeries components are not aligned: lengths {sorted(lengths)}")

    def __len__(self) -> int:
        for values in self.components.values():
            return len(values)
        return 0


# ---------------------------------------------------------------------------
# Parameter validation
# ---------------------------------------------------------------------------


def _check_period(period: int, name: str = "period") -> None:
    """⚠ Cheap, and the failure it prevents is silent. An inverted MACD pair
    produces a sign-flipped histogram that looks like a real signal."""
    if period <= 0:
        raise ValueError(f"{name} must be positive, got {period}")


def _usable_float(value: Decimal | None) -> float | None:
    return None if value is None else float(value)


# ---------------------------------------------------------------------------
# The indicators
# ---------------------------------------------------------------------------
#
# Each is a single forward pass. The shape is always the same: walk the bars
# once, carry the smoothing state, emit None until warm, emit None and record
# the index when the input cannot support a value.


def sma_series(series: BarSeries, *, universe: Universe, period: int) -> IndicatorSeries:
    """Arithmetic mean of the last ``period`` closes.

    ⚠ A NULL close inside the window makes the mean unevaluable for every index
    whose window contains it — not zero, and not the mean of what remains.
    Substituting would fabricate an observation.
    """
    _check_period(period)
    closes = series.float_closes
    values: list[float | None] = [None] * len(closes)
    unevaluable: list[int] = []
    # Running sum — O(1) per bar. `nulls` counts NULLs currently inside the
    # window so the guard stays exact without rescanning it.
    running = 0.0
    nulls = 0
    for i, value in enumerate(closes):
        if value is None:
            nulls += 1
        else:
            running += value
        if i >= period:
            dropped = closes[i - period]
            if dropped is None:
                nulls -= 1
            else:
                running -= dropped
        if i + 1 < period:
            continue
        if nulls:
            unevaluable.append(i)
            continue
        values[i] = running / period
    return IndicatorSeries(tuple(values), universe, tuple(unevaluable))


def ema_series(series: BarSeries, *, universe: Universe, period: int) -> IndicatorSeries:
    """EMA seeded from SMA(period), matching ``technical_analysis._ema_series``.

    ⚠ The seed convention is a data-treatment choice with competing forms (SMA
    seed vs first-close seed) and it is inherited here rather than re-decided —
    equivalence with the shipped batch function is an acceptance criterion, and
    changing the seed would break it silently.

    A NULL close anywhere at or before index i poisons the recursion from that
    point on: unlike SMA there is no window that rolls off. So the series
    becomes unevaluable from the first NULL onward rather than resuming.
    """
    _check_period(period)
    closes = series.float_closes
    values: list[float | None] = [None] * len(closes)
    unevaluable: list[int] = []

    first_null = next((i for i, v in enumerate(closes) if v is None), None)
    horizon = len(closes) if first_null is None else first_null
    if first_null is not None:
        unevaluable.extend(range(first_null, len(closes)))
    if horizon < period:
        return IndicatorSeries(tuple(values), universe, tuple(unevaluable))

    seed_window = closes[:period]
    current = sum(v for v in seed_window if v is not None) / period
    values[period - 1] = current
    mult = 2.0 / (period + 1)
    for i in range(period, horizon):
        close = closes[i]
        assert close is not None
        current = close * mult + current * (1.0 - mult)
        values[i] = current
    return IndicatorSeries(tuple(values), universe, tuple(unevaluable))


def rsi_series(series: BarSeries, *, universe: Universe, period: int = 14) -> IndicatorSeries:
    """Wilder RSI — seed = simple average of the first ``period`` deltas, then
    Wilder smoothing.

    ⚠ Flat-series convention inherited from ``technical_analysis.rsi``: 50.0
    when average gain and loss are both zero, 100.0 when there are no losses.
    Local convention, not from Wilder, and labelled as such.
    """
    _check_period(period)
    closes = series.float_closes
    values: list[float | None] = [None] * len(closes)
    unevaluable: list[int] = []

    first_null = next((i for i, v in enumerate(closes) if v is None), None)
    horizon = len(closes) if first_null is None else first_null
    if first_null is not None:
        unevaluable.extend(range(first_null, len(closes)))
    if horizon <= period:
        return IndicatorSeries(tuple(values), universe, tuple(unevaluable))

    gain = 0.0
    loss = 0.0
    for i in range(1, period + 1):
        a, b = closes[i], closes[i - 1]
        assert a is not None and b is not None
        delta = a - b
        gain += max(delta, 0.0)
        loss += max(-delta, 0.0)
    gain /= period
    loss /= period
    values[period] = _rsi_value(gain, loss)

    for i in range(period + 1, horizon):
        a, b = closes[i], closes[i - 1]
        assert a is not None and b is not None
        delta = a - b
        gain = (gain * (period - 1) + max(delta, 0.0)) / period
        loss = (loss * (period - 1) + max(-delta, 0.0)) / period
        values[i] = _rsi_value(gain, loss)
    return IndicatorSeries(tuple(values), universe, tuple(unevaluable))


def _rsi_value(gain: float, loss: float) -> float:
    if gain == 0.0 and loss == 0.0:
        return 50.0
    if loss == 0.0:
        return 100.0
    return 100.0 - 100.0 / (1.0 + gain / loss)


def atr_series(series: BarSeries, *, universe: Universe, period: int = 14) -> IndicatorSeries:
    """Wilder ATR — TR = max(H-L, |H-C_prev|, |L-C_prev|), then Wilder smoothing.

    ⚠ This is the rolling form #2279 §6 flagged and declined to build: its
    ``_atr_at`` recomputes a 15-bar Wilder window at EVERY bar, making the
    level scans O(bars x period) and costing 252.3 s / 253.1 s per level. It
    said "Phase 5 should budget for it or fix it; it should not discover it."
    Ticket 2b does the rewire — ⚠ conditional on proving equivalence under
    MASKED bars, because ``_atr_at`` fails closed on those and this cannot.
    """
    _check_period(period)
    rows = series.rows
    values: list[float | None] = [None] * len(rows)
    unevaluable: list[int] = []

    highs, lows, closes_f = series.float_highs, series.float_lows, series.float_closes
    trs: list[float | None] = [None] * len(rows)
    for i in range(1, len(rows)):
        high, low = highs[i], lows[i]
        prev_close = closes_f[i - 1]
        # ⚠ [C2] `close[i]` is deliberately in this guard even though TR does
        # not read it. TR needs high[i], low[i] and close[i-1] only, so a bar
        # with a NULL close still HAS a computable true range — and emitting it
        # would let a caller pair a real ATR with a missing close at the same
        # index, exactly the incomplete-bar-treated-as-valid case this module's
        # contract forbids. `price_structure._atr_at` fails closed on any
        # masked field for the same reason; over-conservative and consistent
        # beats computable and inconsistent.
        if high is None or low is None or prev_close is None or closes_f[i] is None:
            continue
        trs[i] = max(high - low, abs(high - prev_close), abs(low - prev_close))

    first_null = next((i for i in range(1, len(rows)) if trs[i] is None), None)
    horizon = len(rows) if first_null is None else first_null
    if first_null is not None:
        unevaluable.extend(range(first_null, len(rows)))
    if horizon <= period:
        return IndicatorSeries(tuple(values), universe, tuple(unevaluable))

    window = [t for t in trs[1 : period + 1] if t is not None]
    current = sum(window) / period
    values[period] = current
    for i in range(period + 1, horizon):
        tr = trs[i]
        assert tr is not None
        current = (current * (period - 1) + tr) / period
        values[i] = current
    return IndicatorSeries(tuple(values), universe, tuple(unevaluable))


def macd_series(
    series: BarSeries,
    *,
    universe: Universe,
    fast: int = 12,
    slow: int = 26,
    signal_period: int = 9,
) -> MultiIndicatorSeries:
    """MACD line, signal and histogram, aligned to the input.

    ⚠ ``fast < slow`` is checked. An inverted pair produces a sign-flipped
    histogram that reads as a real signal and is invisible in any single value.
    """
    _check_period(fast, "fast")
    _check_period(slow, "slow")
    _check_period(signal_period, "signal_period")
    if fast >= slow:
        raise ValueError(f"fast must be < slow, got fast={fast} slow={slow}")

    fast_ema = ema_series(series, universe=universe, period=fast)
    slow_ema = ema_series(series, universe=universe, period=slow)
    n = len(series)

    line: list[float | None] = [None] * n
    for i in range(n):
        f, s = fast_ema.values[i], slow_ema.values[i]
        if f is not None and s is not None:
            line[i] = f - s

    # Signal = EMA(signal_period) of the MACD line, seeded from the SMA of the
    # line's first `signal_period` defined values — the same seed convention
    # ema_series uses, applied one level up.
    signal: list[float | None] = [None] * n
    hist: list[float | None] = [None] * n
    defined = [i for i, v in enumerate(line) if v is not None]
    if len(defined) >= signal_period:
        seed_idx = defined[signal_period - 1]
        window = [line[j] for j in defined[:signal_period]]
        current = sum(v for v in window if v is not None) / signal_period
        signal[seed_idx] = current
        mult = 2.0 / (signal_period + 1)
        for i in defined[signal_period:]:
            value = line[i]
            assert value is not None
            current = value * mult + current * (1.0 - mult)
            signal[i] = current
        for i in range(n):
            if line[i] is not None and signal[i] is not None:
                hist[i] = line[i] - signal[i]  # type: ignore[operator]

    return MultiIndicatorSeries(
        components={"line": tuple(line), "signal": tuple(signal), "histogram": tuple(hist)},
        universe=universe,
        not_evaluable_indices=fast_ema.not_evaluable_indices,
    )


def bollinger_series(
    series: BarSeries,
    *,
    universe: Universe,
    period: int = 20,
    num_std: float = 2.0,
) -> MultiIndicatorSeries:
    """SMA(period) ± num_std * POPULATION sigma.

    ⚠ Population rather than sample sigma is inherited from
    ``technical_analysis.bollinger_bands`` (its test asserts variance over
    ``/ 20``). Local to this codebase, and equivalence with the batch function
    is an acceptance criterion — flagged in the spec as a citation still owed.
    """
    _check_period(period)
    if num_std < 0:
        raise ValueError(f"num_std must be >= 0, got {num_std}")
    closes = series.float_closes
    n = len(closes)
    upper: list[float | None] = [None] * n
    middle: list[float | None] = [None] * n
    lower: list[float | None] = [None] * n
    unevaluable: list[int] = []

    # ⚠ MEAN is a running sum (O(1)); VARIANCE deliberately walks the window.
    #
    # The O(1) one-pass form (`sumsq/n - mean^2`) was tried and REVERTED. It is
    # a cancellation hazard on price data — variance is tiny against mean^2, so
    # the subtraction eats the significant digits. A sample said it was fine:
    # 48,707 bars from the three deepest series gave a max band error of
    # 2.4e-11 against this module's 1e-9 tolerance, a 40x margin.
    #
    # The FULL-CORPUS sweep then failed it: **193 mismatches on each band**
    # across 7,354 series. The sample was three large caps; the corpus contains
    # high-priced low-volatility names where mean^2 dwarfs the variance and the
    # cancellation bites. Half the speed win, none of the wrongness — and a
    # reminder that a favourable sample is not evidence of safety, which is the
    # same defect #2260 exists because of.
    running = 0.0
    nulls = 0
    for i in range(n):
        value = closes[i]
        if value is None:
            nulls += 1
        else:
            running += value
        if i >= period:
            dropped = closes[i - period]
            if dropped is None:
                nulls -= 1
            else:
                running -= dropped
        if i + 1 < period:
            continue
        if nulls:
            unevaluable.append(i)
            continue
        mean = running / period
        acc = 0.0
        for j in range(i - period + 1, i + 1):
            centred = closes[j]
            assert centred is not None
            acc += (centred - mean) ** 2
        variance = acc / period
        std = variance**0.5
        middle[i] = mean
        upper[i] = mean + num_std * std
        lower[i] = mean - num_std * std

    return MultiIndicatorSeries(
        components={"upper": tuple(upper), "middle": tuple(middle), "lower": tuple(lower)},
        universe=universe,
        not_evaluable_indices=tuple(unevaluable),
    )


def stochastic_series(
    series: BarSeries,
    *,
    universe: Universe,
    period: int = 14,
    d_period: int = 3,
) -> MultiIndicatorSeries:
    """%K over the period high/low range, %D = SMA(d_period) of %K.

    ⚠ Flat-range convention inherited from ``technical_analysis.stochastic``:
    %K = 50.0 when the period high equals the period low. Local convention, not
    from Lane, and labelled as such.
    """
    _check_period(period)
    _check_period(d_period, "d_period")
    rows = series.rows
    n = len(rows)
    k: list[float | None] = [None] * n
    d: list[float | None] = [None] * n
    unevaluable: list[int] = []

    highs = series.float_highs
    lows = series.float_lows
    closes = series.float_closes

    # Monotonic deques — the standard sliding-window max/min. O(1) amortised
    # per bar instead of O(period), and EXACT: max and min introduce no
    # floating-point error, unlike a running-sum trick.
    max_dq: deque[int] = deque()
    min_dq: deque[int] = deque()
    nulls = 0
    for i in range(n):
        high_i, low_i = highs[i], lows[i]
        if high_i is None or low_i is None:
            nulls += 1
        else:
            while max_dq and (highs[max_dq[-1]] or 0.0) <= high_i:
                max_dq.pop()
            max_dq.append(i)
            while min_dq and (lows[min_dq[-1]] or 0.0) >= low_i:
                min_dq.pop()
            min_dq.append(i)
        if i >= period:
            j = i - period
            if highs[j] is None or lows[j] is None:
                nulls -= 1
            if max_dq and max_dq[0] == j:
                max_dq.popleft()
            if min_dq and min_dq[0] == j:
                min_dq.popleft()
        if i + 1 < period:
            continue
        close = closes[i]
        if nulls or close is None or not max_dq or not min_dq:
            unevaluable.append(i)
            continue
        hi = highs[max_dq[0]]
        lo = lows[min_dq[0]]
        assert hi is not None and lo is not None
        k[i] = 50.0 if hi == lo else (close - lo) / (hi - lo) * 100.0

    # ⚠ [C2] %D inherits %K's unevaluability. A NULL input makes %K unevaluable
    # at index j, and every %D window containing j is unevaluable too — for the
    # following d_period - 1 bars. The first draft left those as a bare None:
    # outside warm-up AND outside not_evaluable_indices, which is precisely the
    # warm-up/unevaluable collapse the result contract exists to prevent.
    unevaluable_k = set(unevaluable)
    for i in range(n):
        if i + 1 < d_period:
            continue
        window_indices = range(i - d_period + 1, i + 1)
        if any(j in unevaluable_k for j in window_indices):
            if i not in unevaluable_k:
                unevaluable.append(i)
            continue
        window_k = k[i - d_period + 1 : i + 1]
        if any(v is None for v in window_k):
            continue
        d[i] = sum(v for v in window_k if v is not None) / d_period
    unevaluable.sort()

    return MultiIndicatorSeries(
        components={"k": tuple(k), "d": tuple(d)},
        universe=universe,
        not_evaluable_indices=tuple(unevaluable),
    )


def atr_window_series(series: BarSeries, *, universe: Universe, period: int = 14) -> IndicatorSeries:
    """Rolling TRAILING-WINDOW ATR — the mean of the last ``period`` true
    ranges, recomputed in O(1) per bar rather than O(period).

    ⚠ THIS IS NOT ``atr_series``, AND THE DIFFERENCE IS THE WHOLE POINT.
    ``atr_series`` is Wilder-smoothed from the series START (recursive, infinite
    memory). This one has finite memory: it forgets everything before the
    window. They agree at the seed index and diverge everywhere after —
    measured on a 36-bar fixture, index 25 gives 3.9610 (Wilder) against 4.2500
    (window).

    It exists because ``price_structure._atr_at`` computes exactly this, by
    calling ``technical_analysis.atr`` with exactly ``period + 1`` bars: with
    that many bars there are exactly ``period`` true ranges, so
    ``atr_val = sum(trs[:period]) / period`` returns and the Wilder loop never
    executes. #2279 §6 measured that recompute-per-bar at 252.3 s / 253.1 s per
    level and said phase 5 should fix it rather than discover it — this is the
    fix that preserves the meaning.

    ⚠ Fails CLOSED on masking, exactly as ``_atr_at`` does: a single masked
    high/low/close anywhere in the ``period + 1`` bar window makes the value
    unevaluable. Substituting a fallback would silently widen or narrow a level
    tolerance at precisely the bars quarantine flagged as untrustworthy.
    """
    _check_period(period)
    rows = series.rows
    n = len(rows)
    values: list[float | None] = [None] * n
    unevaluable: list[int] = []

    highs, lows, closes_f = series.float_highs, series.float_lows, series.float_closes
    trs: list[float | None] = [None] * n
    for i in range(1, n):
        high, low = highs[i], lows[i]
        prev_close = closes_f[i - 1]
        if high is None or low is None or prev_close is None or closes_f[i] is None:
            continue
        trs[i] = max(high - low, abs(high - prev_close), abs(low - prev_close))

    for i in range(period, n):
        # The window spans bars[i - period .. i]; a masked field on ANY of them
        # kills it, which is what the None TRs below encode.
        window = trs[i - period + 1 : i + 1]
        if any(t is None for t in window):
            unevaluable.append(i)
            continue
        j = i - period
        if highs[j] is None or lows[j] is None or closes_f[j] is None:
            unevaluable.append(i)
            continue
        values[i] = sum(t for t in window if t is not None) / period

    return IndicatorSeries(tuple(values), universe, tuple(unevaluable))
