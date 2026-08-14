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
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from functools import cached_property
from pathlib import Path
from typing import Literal

import numpy as np
import numpy.typing as npt
from numpy.lib.stride_tricks import sliding_window_view

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
    # `cached_property` and not a hand-rolled dict: it writes straight into
    # `instance.__dict__` instead of going through `__setattr__`, which is the
    # method `frozen=True` overrides to raise — so it caches on a FROZEN
    # dataclass without giving up the immutability the contract depends on.
    #
    # ⚠ `slots=True` would break this: there would be no instance `__dict__` to
    # write into, and it fails at first access rather than at class definition.
    # The cached values live outside the declared fields, so `__eq__` and
    # `__hash__` still compare `(dates, rows)` alone.

    def _floats(self, field: str) -> list[float | None]:
        return [None if (v := row.get(field)) is None else float(v) for row in self.rows]

    @cached_property
    def float_closes(self) -> list[float | None]:
        return self._floats("close")

    @cached_property
    def float_highs(self) -> list[float | None]:
        return self._floats("high")

    @cached_property
    def float_lows(self) -> list[float | None]:
        return self._floats("low")

    # ⚠ NaN, not None, and that is the load-bearing part (#2311).
    #
    # The window indicators below are vectorised, and NaN is the only "missing"
    # marker that survives `max` / `min` / `mean` / `var` without a per-element
    # Python branch: any window containing one produces NaN, so the unevaluable
    # mask falls out of `isnan` instead of a hand-carried null counter. The
    # counter and the monotonic deques it replaced were correct but cost
    # 848-1,138 ns/bar in interpreter overhead, which is what put the corpus
    # sweep at 83.3 s against a < 60 s acceptance.
    #
    # Built from the float cache rather than the Decimals so the conversion
    # still happens exactly once per field per series.

    @cached_property
    def array_closes(self) -> npt.NDArray[np.float64]:
        return np.array(self.float_closes, dtype=float)

    @cached_property
    def array_highs(self) -> npt.NDArray[np.float64]:
        return np.array(self.float_highs, dtype=float)

    @cached_property
    def array_lows(self) -> npt.NDArray[np.float64]:
        return np.array(self.float_lows, dtype=float)


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


# ---------------------------------------------------------------------------
# The indicators
# ---------------------------------------------------------------------------
#
# Each is a single forward pass. The shape is always the same: walk the bars
# once, carry the smoothing state, emit None until warm, emit None and record
# the index when the input cannot support a value.
#
# ⚠ The RECURSIVE ones (EMA, RSI, ATR, MACD) stay Python loops on purpose.
# Each value depends on the previous one, so there is nothing to vectorise
# without changing the arithmetic, and at 51-222 ns/bar they are not the cost:
# the window indicators were 1,986 of the 2,662 ns/bar total (#2311).


def _to_optional(values: npt.NDArray[np.float64]) -> list[float | None]:
    """``NaN`` -> ``None``, everything else a Python float.

    ⚠ The contract is ``float | None``, never NaN — a NaN leaking into
    ``IndicatorSeries.values`` would compare falsey-ish in every direction
    (``nan > x`` and ``nan < x`` are both False), which is the vacuous-truth
    class the result contract exists to prevent. Converting whole and then
    patching the NaN positions is cheap because they are few: warm-up plus
    whatever NULLs the input carried.
    """
    out: list[float | None] = values.tolist()
    for i in np.flatnonzero(np.isnan(values)).tolist():
        out[i] = None
    return out


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
    # ⚠ NO RE-SEEDING, and the reason is measured rather than assumed.
    #
    # A running sum looks like the same class of numerical shortcut as the
    # reverted one-pass Bollinger variance, so it was checked the same way.
    # Against an exact `math.fsum` reference over 20,000 bars:
    #
    #     base 1e2   abs 1.4e-14   rel 1.4e-16
    #     base 1e5   abs 4.4e-11   rel 4.4e-16
    #     base 1e9   abs 2.4e-07   rel 2.4e-16
    #
    # The RELATIVE error is ~1-2 ULP at every magnitude — this accumulator is
    # as accurate as float64 permits, and the absolute figure only tracks the
    # magnitude, as it must. An earlier draft added periodic re-seeding on the
    # strength of that 2.4e-07 and it was cargo cult: `ta.sma` misses a 1e-9
    # ABSOLUTE tolerance at 1e9 too, because one ULP there is 1.2e-07. The
    # tolerance was the defect, not the accumulator (see the harness).
    #
    # Bollinger's VARIANCE is a genuinely different case and does not get a
    # running form; see the comment there.
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


def adx_series(series: BarSeries, *, universe: Universe, period: int = 14) -> IndicatorSeries:
    """Wilder's ADX — Average Directional Index.

    SOURCE RULE: J. Welles Wilder Jr., *New Concepts in Technical Trading
    Systems* (1978), ch. 4. Nothing here is ours to choose, and the steps are
    named so a reader can check them against the book rather than against this
    docstring:

    1. ``+DM`` = ``high[i] - high[i-1]`` when that exceeds both
       ``low[i-1] - low[i]`` and zero, else 0. ``-DM`` mirrors it. ⚠ A bar with
       BOTH moves larger keeps only the larger one; an inside bar contributes
       neither. Taking both — the obvious vectorisation — makes every bar
       directional and drives ADX up.
    2. ``TR`` exactly as ``atr_series`` computes it.
    3. Wilder-smooth all three over ``period``.
    4. ``+DI = 100 * smoothed(+DM) / smoothed(TR)``, ``-DI`` likewise.
    5. ``DX = 100 * |+DI - -DI| / (+DI + -DI)``.
    6. ``ADX`` = Wilder average of ``DX``, seeded with the mean of its first
       ``period`` values.

    ⚠ WILDER'S RUNNING-SUM FORM AND THE AVERAGE FORM ARE INTERCHANGEABLE HERE,
    and the average form is used to match ``atr_series``. The book smooths as
    ``S = S_prev - S_prev/period + current`` (a running SUM); this smooths as
    ``S = (S_prev*(period-1) + current)/period`` (its average, i.e. the sum over
    ``period``). ``+DI`` is a RATIO of two identically-smoothed series, so the
    constant factor cancels exactly and the DI values are the book's. Stated
    because the two forms differ by ``period``x and comparing one to the other
    looks like a bug.

    ⚠ FAIL-CLOSED FROM THE FIRST MASKED FIELD, matching ``atr_series`` rather
    than skipping the bar. Wilder smoothing is recursive: a gap does not affect
    one value, it shifts every value after it. Resuming past a hole would
    produce numbers that look valid and are not, so everything from the first
    unusable bar is ``not_evaluable``.

    ⚠ ``DX`` DENOMINATOR OF ZERO is possible — a stretch with no directional
    movement at all gives ``+DI = -DI = 0``. Wilder does not define DX there.
    It is reported as unevaluable rather than as ``DX = 0``: zero means "equal
    directional pressure", which is a measurement, and this is its absence.
    """
    _check_period(period)
    rows = series.rows
    n = len(rows)
    values: list[float | None] = [None] * n

    highs, lows, closes_f = series.float_highs, series.float_lows, series.float_closes
    trs: list[float | None] = [None] * n
    plus_dm: list[float] = [0.0] * n
    minus_dm: list[float] = [0.0] * n
    for i in range(1, n):
        high, low, prev_high, prev_low = highs[i], lows[i], highs[i - 1], lows[i - 1]
        prev_close = closes_f[i - 1]
        # Same guard as `atr_series`, plus the previous bar's range, which the
        # directional movement needs and the true range does not.
        if (
            high is None
            or low is None
            or prev_high is None
            or prev_low is None
            or prev_close is None
            or closes_f[i] is None
        ):
            continue
        trs[i] = max(high - low, abs(high - prev_close), abs(low - prev_close))
        up_move = high - prev_high
        down_move = prev_low - low
        if up_move > down_move and up_move > 0:
            plus_dm[i] = up_move
        if down_move > up_move and down_move > 0:
            minus_dm[i] = down_move

    first_null = next((i for i in range(1, n) if trs[i] is None), None)
    horizon = n if first_null is None else first_null
    unevaluable: list[int] = [] if first_null is None else list(range(first_null, n))
    # ADX needs `period` smoothed bars, then `period` DX values to seed its own
    # average: the first reading sits at index `2 * period - 1`.
    if horizon <= 2 * period - 1:
        return IndicatorSeries(tuple(values), universe, tuple(unevaluable))

    window = [t for t in trs[1 : period + 1] if t is not None]
    smooth_tr = sum(window) / period
    smooth_plus = sum(plus_dm[1 : period + 1]) / period
    smooth_minus = sum(minus_dm[1 : period + 1]) / period

    dx_values: list[float | None] = [None] * n

    def _dx(tr: float, plus: float, minus: float) -> float | None:
        if tr <= 0:
            return None
        plus_di = 100.0 * plus / tr
        minus_di = 100.0 * minus / tr
        total = plus_di + minus_di
        if total <= 0:
            return None
        return 100.0 * abs(plus_di - minus_di) / total

    dx_values[period] = _dx(smooth_tr, smooth_plus, smooth_minus)
    for i in range(period + 1, horizon):
        tr = trs[i]
        assert tr is not None
        smooth_tr = (smooth_tr * (period - 1) + tr) / period
        smooth_plus = (smooth_plus * (period - 1) + plus_dm[i]) / period
        smooth_minus = (smooth_minus * (period - 1) + minus_dm[i]) / period
        dx_values[i] = _dx(smooth_tr, smooth_plus, smooth_minus)

    seed = dx_values[period : 2 * period]
    if any(value is None for value in seed):
        # A flat stretch inside the seed window leaves ADX undefined from the
        # start. Refusing the whole series is the fail-closed reading and keeps
        # the recursion from being seeded with a fabricated zero.
        unevaluable = sorted(set(unevaluable) | set(range(2 * period - 1, n)))
        return IndicatorSeries(tuple(values), universe, tuple(unevaluable))

    current = sum(value for value in seed if value is not None) / period
    values[2 * period - 1] = current
    for i in range(2 * period, horizon):
        dx = dx_values[i]
        if dx is None:
            # An undefined DX mid-series breaks the recursion for everything
            # after it, for the same reason a masked bar does.
            unevaluable = sorted(set(unevaluable) | set(range(i, n)))
            return IndicatorSeries(tuple(values), universe, tuple(unevaluable))
        current = (current * (period - 1) + dx) / period
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
    closes = series.array_closes
    n = closes.size
    upper = np.full(n, np.nan)
    middle = np.full(n, np.nan)
    lower = np.full(n, np.nan)
    unevaluable: tuple[int, ...] = ()

    # ⚠ THE VARIANCE MUST STAY TWO-PASS, AND VECTORISING DOES NOT CHANGE THAT.
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
    #
    # `ndarray.var` is safe here for a reason that was CHECKED rather than
    # assumed: `numpy._core._methods._var` computes `arrmean` and then
    # `sum((x - arrmean)**2)` — the same two passes the Python loop did, in C.
    # ⚠ Do NOT "optimise" this to `(w**2).mean(axis=1) - means**2`. That is the
    # reverted form wearing a numpy hat, and `TestBollingerNumericalStability`
    # is what will tell you so.
    if n >= period:
        windows = sliding_window_view(closes, period)
        means = windows.mean(axis=1)
        stds = np.sqrt(windows.var(axis=1))
        middle[period - 1 :] = means
        upper[period - 1 :] = means + num_std * stds
        lower[period - 1 :] = means - num_std * stds
        # A NaN mean is a window containing a NULL close: unevaluable, not
        # warm-up. Warm-up is the untouched `period - 1` prefix, which is
        # exactly why the prefix is NOT in this list.
        unevaluable = tuple((np.flatnonzero(np.isnan(means)) + (period - 1)).tolist())

    return MultiIndicatorSeries(
        components={
            "upper": tuple(_to_optional(upper)),
            "middle": tuple(_to_optional(middle)),
            "lower": tuple(_to_optional(lower)),
        },
        universe=universe,
        not_evaluable_indices=unevaluable,
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
    n = len(series)
    highs = series.array_highs
    lows = series.array_lows
    closes = series.array_closes

    # Sliding window max/min. ⚠ This is O(n x period) work, NOT the O(n)
    # amortised monotonic deques it replaced — and it is ~60x faster anyway,
    # because the deques' cost was 848 ns/bar of interpreter overhead and this
    # is `period` float comparisons in C. Same exactness either way: max and
    # min introduce no floating-point error.
    k_values = np.full(n, np.nan)
    unevaluable_k = np.zeros(n, dtype=bool)
    if n >= period:
        window_high = sliding_window_view(highs, period).max(axis=1)
        window_low = sliding_window_view(lows, period).min(axis=1)
        close_at = closes[period - 1 :]
        span = window_high - window_low
        # A NULL high, low or close anywhere in the window makes the span or
        # the close NaN, which carries straight through the division.
        with np.errstate(divide="ignore", invalid="ignore"):
            k_at = (close_at - window_low) / span * 100.0
        # Flat-range convention, inherited from `technical_analysis.stochastic`
        # ⚠ but NOT applied when the close is missing: a flat window with no
        # close is unevaluable, not 50.0. `span == 0.0` is already False for a
        # NaN span, so only the close needs the explicit guard.
        k_values[period - 1 :] = np.where((span == 0.0) & ~np.isnan(close_at), 50.0, k_at)
        unevaluable_k[period - 1 :] = np.isnan(k_values[period - 1 :])

    # ⚠ [C2] %D inherits %K's unevaluability. A NULL input makes %K unevaluable
    # at index j, and every %D window containing j is unevaluable too — for the
    # following d_period - 1 bars. The first draft left those as a bare None:
    # outside warm-up AND outside not_evaluable_indices, which is precisely the
    # warm-up/unevaluable collapse the result contract exists to prevent.
    #
    # A NaN %K covers BOTH cases in the mean below, so `d` is None for warm-up
    # and for unevaluable alike — the boolean mask is what tells them apart,
    # and only the unevaluable one is listed.
    d_values = np.full(n, np.nan)
    unevaluable_d = np.zeros(n, dtype=bool)
    if n >= d_period:
        d_values[d_period - 1 :] = sliding_window_view(k_values, d_period).mean(axis=1)
        unevaluable_d[d_period - 1 :] = sliding_window_view(unevaluable_k, d_period).any(axis=1)

    unevaluable = tuple(np.flatnonzero(unevaluable_k | unevaluable_d).tolist())
    return MultiIndicatorSeries(
        components={"k": tuple(_to_optional(k_values)), "d": tuple(_to_optional(d_values))},
        universe=universe,
        not_evaluable_indices=unevaluable,
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
