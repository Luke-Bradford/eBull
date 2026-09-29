"""#3471 spec v6 §16.1-§16.2 — the pack's structure levels and setup detectors (pure).

Every level and every detector is a function of ONE §3.2 series (a single quarantine-masked
price segment of completed sessions), evaluated at its last bar ``t``. Nothing here reads a
bar after ``t``, so truncating the series after ``t`` cannot change any output (O-v6-3).

Arithmetic is exact: every stored value is taken as the rational its ``Decimal`` (or a float's
shortest round-trip ``repr``) denotes, and no float is ever compared (r2-10).

Provenance of every constant is in the spec's §16.1 tables; each is either published (cited
there) or labelled "by construction". They are frozen with the module's bytes in the
declaration's ``policy_modules``.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from fractions import Fraction
from typing import Final, Literal

from app.services.ai_trial_decision import exact
from app.services.indicator_series import BarSeries, atr_series, sma_series
from app.services.price_structure import StructureBar, Swing, detect_swings

# ---------------------------------------------------------------------------
# Closed vocabularies (§16.3 schema enums; their ORDER is the library's tie order)
# ---------------------------------------------------------------------------
SUPPORT_LEVEL_IDS: Final = (
    "swing_low_1",
    "swing_low_2",
    "swing_low_3",
    "donchian20_low",
    "donchian55_low",
    "sma20",
    "sma50",
    "sma200",
    "vwap20_proxy",
)
TARGET_LEVEL_IDS: Final = (
    "swing_high_1",
    "swing_high_2",
    "swing_high_3",
    "donchian20_high",
    "donchian55_high",
    "range20_projection",
    "mm_up",
)
LEVEL_IDS: Final = SUPPORT_LEVEL_IDS + TARGET_LEVEL_IDS
SetupType = Literal[
    "breakout_donchian20",
    "pullback_rising_sma20",
    "pullback_rising_sma50",
    "range_support_bounce",
    "trend_continuation_flag",
]
SETUP_TYPES: Final[tuple[SetupType, ...]] = (
    "breakout_donchian20",
    "pullback_rising_sma20",
    "pullback_rising_sma50",
    "range_support_bounce",
    "trend_continuation_flag",
)

# ---------------------------------------------------------------------------
# Constants (§16.1; provenance per the spec tables)
# ---------------------------------------------------------------------------
UNIVERSE: Final = "survivor_only"  # as `ai_trial_pack.indicators` passes it (r2-9)
FRACTAL_N: Final = 2  # Williams 5-bar fractal, strict (`_pivot_at`)
SWINGS_KEPT: Final = 3  # by construction
DONCHIAN_SHORT: Final = 20  # Turtle System 1
DONCHIAN_LONG: Final = 55  # Turtle System 2
ATR_PERIOD: Final = 14
# pullback_rising_sma (adapted from Connors & Raschke; every window/multiple by construction)
PULLBACK_SLOPE_LAG: Final = 5
PULLBACK_PRIOR_FIRST: Final = 10  # prior-above window [t-10, t-4]
PULLBACK_PRIOR_LAST: Final = 4
PULLBACK_TOUCH_BARS: Final = 3  # touch window [t-3, t]
PULLBACK_TOUCH_ABOVE_ATR: Final = Fraction(1, 2)
PULLBACK_TOUCH_BELOW_ATR: Final = Fraction(1)
# range_support_bounce (entirely by construction)
RANGE_MIN_WIDTH_ATR: Final = Fraction(3, 2)
RANGE_MAX_WIDTH_ATR: Final = Fraction(4)
RANGE_TOUCH_ATR: Final = Fraction(1, 2)
RANGE_MIN_TOUCHES: Final = 2
# trend_continuation_flag (morphology per E&M / Bulkowski; thresholds by construction)
FLAG_PEAK_FIRST: Final = 15  # h in [t-15, t-3]
FLAG_PEAK_LAST: Final = 3
FLAG_POLE_BARS: Final = 10  # pole low over [h-10, h-1]
FLAG_POLE_MIN_ATR: Final = Fraction(3)
FLAG_MAX_RETRACE: Final = Fraction(1, 2)


@dataclass(frozen=True)
class Level:
    """One computed level: an exact positive price and the bar it is anchored to (``None``
    for moving averages, the VWAP proxy and projections)."""

    price: Fraction
    origin_bar: int | None


@dataclass(frozen=True)
class SetupState:
    detected: bool
    inputs_missing: bool


_MISSING: Final = SetupState(detected=False, inputs_missing=True)


def _q(value: Decimal | float | None) -> Fraction | None:
    if value is None:
        return None
    number = value if isinstance(value, Decimal) else Decimal(repr(value))
    return exact(number) if number.is_finite() else None


def _positive(value: Fraction | None) -> Fraction | None:
    return value if value is not None and value > 0 else None


@dataclass(frozen=True)
class _Bars:
    """The series as exact rationals, plus the contemporaneous SMA/ATR series."""

    high: tuple[Fraction | None, ...]
    low: tuple[Fraction | None, ...]
    close: tuple[Fraction | None, ...]
    atr: tuple[Fraction | None, ...]
    sma: Mapping[int, tuple[Fraction | None, ...]]

    @property
    def t(self) -> int:
        return len(self.close) - 1


def _bars(series: BarSeries) -> _Bars:
    def series_of(values: Sequence[float | None]) -> tuple[Fraction | None, ...]:
        return tuple(_q(v) for v in values)

    return _Bars(
        high=tuple(_q(row["high"]) for row in series.rows),
        low=tuple(_q(row["low"]) for row in series.rows),
        close=tuple(_q(row["close"]) for row in series.rows),
        atr=series_of(atr_series(series, universe=UNIVERSE, period=ATR_PERIOD).values),
        sma={p: series_of(sma_series(series, universe=UNIVERSE, period=p).values) for p in (20, 50)},
    )


# ---------------------------------------------------------------------------
# Levels (§16.1)
# ---------------------------------------------------------------------------
def _donchian(bars: _Bars, n: int) -> tuple[Level | None, Level | None]:
    """min(low) / max(high) over bars [t-n, t-1]; ``None`` on a short or holed window (r2-7).
    The origin is the extreme bar, latest on ties."""
    t = bars.t
    if t - n < 0:
        return None, None
    idx = range(t - n, t)

    def side(values: tuple[Fraction | None, ...], pick: Callable[[list[Fraction]], Fraction]) -> Level | None:
        # Each side is independent: a hole in the lows does not null the high (Codex ckpt-2).
        window = [values[i] for i in idx]
        if any(v is None for v in window):
            return None
        extreme = pick([v for v in window if v is not None])
        if _positive(extreme) is None:
            return None
        return Level(extreme, max(i for i in idx if values[i] == extreme))

    return side(bars.low, min), side(bars.high, max)


def normalised_pivots(swings: Sequence[Swing]) -> list[Swing]:
    """§16.1 normalisation: merge by index, drop a bar that is both a high and a low pivot,
    collapse each same-kind run to its extreme (ties to the later bar)."""
    kinds_at: dict[int, set[str]] = {}
    for swing in swings:
        kinds_at.setdefault(swing.index, set()).add(swing.kind)
    ordered = sorted((s for s in swings if len(kinds_at[s.index]) == 1), key=lambda s: s.index)
    out: list[Swing] = []
    for swing in ordered:
        if out and out[-1].kind == swing.kind:
            prev = out[-1]
            better = (
                exact(swing.price) >= exact(prev.price)
                if swing.kind == "high"
                else exact(swing.price) <= exact(prev.price)
            )
            if better:
                out[-1] = swing
            continue
        out.append(swing)
    return out


def _structure_bars(series: BarSeries) -> list[StructureBar]:
    return [
        StructureBar(
            bar_date=d,
            open=row["open"],
            high=row["high"],
            low=row["low"],
            close=row["close"],
            volume=row["volume"],
        )
        for d, row in zip(series.dates, series.rows, strict=True)
    ]


def _pivot_levels(series: BarSeries, bars: _Bars) -> dict[str, Level | None]:
    out: dict[str, Level | None] = {f"swing_{k}_{i}": None for k in ("low", "high") for i in range(1, SWINGS_KEPT + 1)}
    out["mm_up"] = None
    result = detect_swings(_structure_bars(series), FRACTAL_N, universe=UNIVERSE)
    if result.not_evaluable_indices:
        return out  # unknown pivot state anywhere nulls every pivot-derived level (r2-6)
    # detect_swings emits only pivots confirmed within the series (index + n <= t).
    pivots = normalised_pivots([s for s in result.swings if s.confirmed_index <= bars.t])
    for kind in ("low", "high"):
        recent = [s for s in reversed(pivots) if s.kind == kind][:SWINGS_KEPT]
        for rank, swing in enumerate(recent, start=1):
            price = _positive(exact(swing.price))
            out[f"swing_{kind}_{rank}"] = None if price is None else Level(price, swing.index)
    out["mm_up"] = _measured_move(pivots, bars)
    return out


def _measured_move(pivots: Sequence[Swing], bars: _Bars) -> Level | None:
    """C + (B − A) over the LATEST three normalised pivots (no backward search), L-H-L with
    A < C < B; null when a raw low after C is <= C or a raw high after B is >= the target."""
    if len(pivots) < 3:
        return None
    a, b, c = pivots[-3:]
    if (a.kind, b.kind, c.kind) != ("low", "high", "low"):
        return None
    pa, pb, pc = exact(a.price), exact(b.price), exact(c.price)
    if not pa < pc < pb:
        return None
    target = pc + (pb - pa)
    after_c = bars.low[c.index + 1 : bars.t + 1]
    after_b = bars.high[b.index + 1 : bars.t + 1]
    if any(v is None for v in after_c) or any(v is None for v in after_b):
        return None
    if any(v is not None and v <= pc for v in after_c):
        return None
    if any(v is not None and v >= target for v in after_b):
        return None
    return Level(target, None)


def compute_levels(series: BarSeries, *, indicators: Mapping[str, float | None]) -> dict[str, Level | None]:
    """Every §16.1 level id → its ``Level`` or ``None``. ``indicators`` is the pack's own
    ``ai_trial_pack.indicators(series)`` output (the SMA and VWAP-proxy levels)."""
    bars = _bars(series)
    out: dict[str, Level | None] = dict.fromkeys(LEVEL_IDS)
    if not bars.close:
        return out
    out.update(_pivot_levels(series, bars))
    d20_low, d20_high = _donchian(bars, DONCHIAN_SHORT)
    d55_low, d55_high = _donchian(bars, DONCHIAN_LONG)
    out["donchian20_low"], out["donchian20_high"] = d20_low, d20_high
    out["donchian55_low"], out["donchian55_high"] = d55_low, d55_high
    if d20_low is not None and d20_high is not None:
        out["range20_projection"] = Level(d20_high.price + (d20_high.price - d20_low.price), None)
    for key in ("sma20", "sma50", "sma200", "vwap20_proxy"):
        price = _positive(_q(indicators.get(key)))
        out[key] = None if price is None else Level(price, None)
    return out


# ---------------------------------------------------------------------------
# Setup detectors (§16.1) — contemporaneous SMA/ATR at every tested bar
# ---------------------------------------------------------------------------
def _get(values: Sequence[Fraction | None], i: int) -> Fraction | None:
    return values[i] if 0 <= i < len(values) else None


def _breakout_donchian20(bars: _Bars, levels: Mapping[str, Level | None]) -> SetupState:
    close, high = _get(bars.close, bars.t), levels["donchian20_high"]
    if close is None or high is None:
        return _MISSING
    return SetupState(detected=close > high.price, inputs_missing=False)


def _pullback_rising_sma(bars: _Bars, period: int) -> SetupState:
    t, sma = bars.t, bars.sma[period]
    if t - PULLBACK_PRIOR_FIRST < 0:
        return _MISSING
    now, lagged, close = _get(sma, t), _get(sma, t - PULLBACK_SLOPE_LAG), _get(bars.close, t)
    prior = [(_get(bars.close, k), _get(sma, k)) for k in range(t - PULLBACK_PRIOR_FIRST, t - PULLBACK_PRIOR_LAST + 1)]
    touch = [(_get(bars.low, j), _get(sma, j), _get(bars.atr, j)) for j in range(t - PULLBACK_TOUCH_BARS, t + 1)]
    if now is None or lagged is None or close is None:
        return _MISSING
    if any(c is None or s is None for c, s in prior) or any(v is None for row in touch for v in row):
        return _MISSING
    rising = now > lagged
    prior_above = all(c > s for c, s in prior if c is not None and s is not None)
    touched = any(
        s - PULLBACK_TOUCH_BELOW_ATR * a <= low <= s + PULLBACK_TOUCH_ABOVE_ATR * a
        for low, s, a in touch
        if low is not None and s is not None and a is not None
    )
    return SetupState(detected=rising and prior_above and touched and close > now, inputs_missing=False)


def _non_adjacent_count(indices: Sequence[int]) -> int:
    """The most indices choosable with no two adjacent (greedy over ascending indices)."""
    count, last = 0, None
    for i in sorted(indices):
        if last is None or i - last >= 2:
            count, last = count + 1, i
    return count


def _range_support_bounce(bars: _Bars, levels: Mapping[str, Level | None]) -> SetupState:
    t = bars.t
    low_lvl, high_lvl = levels["donchian20_low"], levels["donchian20_high"]
    atr_t, close, prev_close = _get(bars.atr, t), _get(bars.close, t), _get(bars.close, t - 1)
    if low_lvl is None or high_lvl is None or atr_t is None or close is None or prev_close is None:
        return _MISSING
    window = range(t - DONCHIAN_SHORT, t)
    rows = [(j, bars.low[j], bars.high[j], bars.atr[j]) for j in window]
    recent = [(bars.low[j], bars.atr[j]) for j in (t - 1, t)]
    if any(v is None for row in rows for v in row[1:]) or any(v is None for row in recent for v in row):
        return _MISSING
    support, resistance = low_lvl.price, high_lvl.price
    width = resistance - support
    low_touches = [
        j for j, lo, _, a in rows if lo is not None and a is not None and lo <= support + RANGE_TOUCH_ATR * a
    ]
    high_touches = [
        j for j, _, hi, a in rows if hi is not None and a is not None and hi >= resistance - RANGE_TOUCH_ATR * a
    ]
    detected = (
        RANGE_MIN_WIDTH_ATR * atr_t <= width <= RANGE_MAX_WIDTH_ATR * atr_t
        and _non_adjacent_count(low_touches) >= RANGE_MIN_TOUCHES
        and _non_adjacent_count(high_touches) >= RANGE_MIN_TOUCHES
        and any(lo is not None and a is not None and lo <= support + RANGE_TOUCH_ATR * a for lo, a in recent)
        and close >= support
        and close > prev_close
    )
    return SetupState(detected=detected, inputs_missing=False)


def _trend_continuation_flag(bars: _Bars) -> SetupState:
    t = bars.t
    first = t - FLAG_PEAK_FIRST
    if first - FLAG_POLE_BARS < 0:
        return _MISSING
    peak_window = range(first, t - FLAG_PEAK_LAST + 1)
    peak_highs = [bars.high[i] for i in peak_window]
    if any(v is None for v in peak_highs):
        return _MISSING
    top = max(v for v in peak_highs if v is not None)
    h = max(i for i in peak_window if bars.high[i] == top)  # latest on ties
    pole_lows = [bars.low[i] for i in range(h - FLAG_POLE_BARS, h)]
    flag_highs = [bars.high[i] for i in range(h + 1, t + 1)]
    flag_lows = [bars.low[i] for i in range(h + 1, t + 1)]
    atr_h = bars.atr[h]
    if atr_h is None or any(v is None for v in pole_lows + flag_highs + flag_lows):
        return _MISSING
    pole_low = min(v for v in pole_lows if v is not None)
    pole = top - pole_low
    detected = (
        pole >= FLAG_POLE_MIN_ATR * atr_h
        and max(v for v in flag_highs if v is not None) < top
        and min(v for v in flag_lows if v is not None) >= top - FLAG_MAX_RETRACE * pole
    )
    return SetupState(detected=detected, inputs_missing=False)


def detect_setups(series: BarSeries, *, levels: Mapping[str, Level | None]) -> dict[SetupType, SetupState]:
    """Every §16.1 setup (except ``none``) → ``{detected, inputs_missing}`` at the last bar.
    ``levels`` is ``compute_levels`` over the same series."""
    bars = _bars(series)
    if not bars.close:
        return dict.fromkeys(SETUP_TYPES, _MISSING)
    detectors: dict[SetupType, Callable[[], SetupState]] = {
        "breakout_donchian20": lambda: _breakout_donchian20(bars, levels),
        "pullback_rising_sma20": lambda: _pullback_rising_sma(bars, 20),
        "pullback_rising_sma50": lambda: _pullback_rising_sma(bars, 50),
        "range_support_bounce": lambda: _range_support_bounce(bars, levels),
        "trend_continuation_flag": lambda: _trend_continuation_flag(bars),
    }
    return {name: detect() for name, detect in detectors.items()}


__all__ = [
    "LEVEL_IDS",
    "SETUP_TYPES",
    "SUPPORT_LEVEL_IDS",
    "TARGET_LEVEL_IDS",
    "Level",
    "SetupState",
    "SetupType",
    "compute_levels",
    "detect_setups",
    "normalised_pivots",
]
