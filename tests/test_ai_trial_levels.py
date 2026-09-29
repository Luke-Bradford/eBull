"""#3471 spec v6 §16.1 — structure levels and setup detectors (pure)."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from fractions import Fraction

import pytest

from app.services.ai_trial_levels import (
    LEVEL_IDS,
    SETUP_TYPES,
    Level,
    compute_levels,
    detect_setups,
    normalised_pivots,
)
from app.services.indicator_series import BarSeries, OHLCVRow
from app.services.price_structure import Swing

NO_INDICATORS: dict[str, float | None] = {}


def _series(bars: list[tuple[float, float, float]]) -> BarSeries:
    """(high, low, close) per bar; open = close."""
    start = date(2026, 1, 1)
    rows = [
        OHLCVRow(
            open=Decimal(repr(c)),
            high=Decimal(repr(h)),
            low=Decimal(repr(lo)),
            close=Decimal(repr(c)),
            volume=1000,
        )
        for h, lo, c in bars
    ]
    return BarSeries(dates=tuple(start + timedelta(days=i) for i in range(len(bars))), rows=tuple(rows))


def _flat(n: int, close: float = 100.0, spread: float = 1.0) -> list[tuple[float, float, float]]:
    return [(close + spread, close - spread, close)] * n


def _price(level: Level | None) -> Fraction | None:
    return None if level is None else level.price


def test_every_id_is_always_present() -> None:
    levels = compute_levels(_series(_flat(3)), indicators=NO_INDICATORS)
    assert tuple(levels) == LEVEL_IDS
    setups = detect_setups(_series(_flat(3)), levels=levels)
    assert tuple(setups) == SETUP_TYPES
    assert all(state.inputs_missing and not state.detected for state in setups.values())


def test_donchian_excludes_the_tested_bar_and_breakout_fires() -> None:
    bars = _flat(25)
    bars.append((110.0, 104.0, 109.0))  # t: closes above every prior high
    series = _series(bars)
    levels = compute_levels(series, indicators=NO_INDICATORS)
    assert _price(levels["donchian20_high"]) == 101  # bar t's 110 excluded
    assert _price(levels["donchian20_low"]) == 99
    assert _price(levels["range20_projection"]) == 103
    assert levels["donchian55_high"] is None  # short window
    assert levels["donchian20_high"] is not None and levels["donchian20_high"].origin_bar == 24  # latest tie
    assert detect_setups(series, levels=levels)["breakout_donchian20"].detected


def test_breakout_near_miss_at_the_channel_high() -> None:
    bars = _flat(25) + [(101.5, 99.0, 101.0)]  # close == prior high: not strictly above
    series = _series(bars)
    levels = compute_levels(series, indicators=NO_INDICATORS)
    assert not detect_setups(series, levels=levels)["breakout_donchian20"].detected


def test_a_pivot_is_not_a_level_until_confirmed() -> None:
    # O-v6-3: a local high at t-1 is not knowable at t (needs 2 bars after it).
    rising = [(100.0 + i + 1, 100.0 + i - 1, 100.0 + i) for i in range(10)]
    peak = [(120.0, 110.0, 115.0)]
    one_after = [(114.0, 108.0, 110.0)]
    at_t = compute_levels(_series(rising + peak + one_after), indicators=NO_INDICATORS)
    assert at_t["swing_high_1"] is None
    confirmed = compute_levels(_series(rising + peak + one_after + [(112.0, 106.0, 108.0)]), indicators=NO_INDICATORS)
    assert _price(confirmed["swing_high_1"]) == 120
    assert confirmed["swing_high_1"] is not None and confirmed["swing_high_1"].origin_bar == 10


def _swing(index: int, kind: str, price: float) -> Swing:
    return Swing(
        index=index,
        bar_date=date(2026, 1, 1),
        kind=kind,  # type: ignore[arg-type]
        price=price,
        n=2,
        confirmed_index=index + 2,
        confirmed_date=date(2026, 1, 1),
    )


def test_normalised_pivots_collapse_runs_and_drop_two_sided_bars() -> None:
    swings = [
        _swing(1, "low", 90.0),
        _swing(3, "high", 110.0),
        _swing(5, "high", 112.0),
        _swing(7, "high", 112.0),  # tie goes to the later bar
        _swing(9, "low", 95.0),
        _swing(9, "high", 120.0),  # both kinds on bar 9: BOTH dropped
        _swing(11, "low", 96.0),
        _swing(13, "low", 94.0),  # run of lows collapses to its extreme
    ]
    out = normalised_pivots(swings)
    assert [(s.index, s.kind) for s in out] == [(1, "low"), (7, "high"), (13, "low")]


def _measured_move_bars(*, breach_c: bool = False, tail_high: float = 118.0) -> list[tuple[float, float, float]]:
    """A (low 90) → B (high 110) → C (low 100) → drift up; mm_up = 100 + 20 = 120."""
    path = [100, 96, 92, 90, 93, 97, 101, 105, 108, 110, 107, 104, 101, 100, 102, 104, 106, 107]
    bars = [(p + 0.5, p - 0.5, float(p)) for p in path]
    bars[3] = (90.5, 90.0, 90.0)  # A
    bars[9] = (110.0, 109.5, 110.0)  # B
    bars[13] = (100.5, 100.0, 100.0)  # C
    if breach_c:
        bars[16] = (106.5, 99.0, 106.0)
    bars[-1] = (tail_high, 106.5, 107.0)
    return bars


def test_measured_move_projects_equal_legs() -> None:
    levels = compute_levels(_series(_measured_move_bars()), indicators=NO_INDICATORS)
    assert _price(levels["mm_up"]) == 120


def test_measured_move_null_when_c_breached_or_target_hit() -> None:
    assert compute_levels(_series(_measured_move_bars(breach_c=True)), indicators=NO_INDICATORS)["mm_up"] is None
    assert compute_levels(_series(_measured_move_bars(tail_high=120.0)), indicators=NO_INDICATORS)["mm_up"] is None


def test_indicator_levels_are_exact_and_positive_only() -> None:
    levels = compute_levels(
        _series(_flat(5)), indicators={"sma20": 99.1, "sma50": 0.0, "sma200": None, "vwap20_proxy": 98.7}
    )
    assert _price(levels["sma20"]) == Fraction(Decimal("99.1"))
    assert levels["sma50"] is None and levels["sma200"] is None
    assert _price(levels["vwap20_proxy"]) == Fraction(Decimal("98.7"))


def _pullback_bars(*, touch: bool) -> list[tuple[float, float, float]]:
    """A steady uptrend (+0.5/bar) whose last bars dip toward the 20-bar SMA."""
    closes = [100 + 0.5 * i for i in range(40)]
    bars = [(c + 0.5, c - 0.5, c) for c in closes]
    # SMA20 at the end sits ~4.75 below the trend; dip the low of t-1 to it.
    dip_low = closes[-2] - 4.5 if touch else closes[-2] - 0.5
    bars[-2] = (closes[-2] + 0.5, dip_low, closes[-2] - 1.0)
    return bars


@pytest.mark.parametrize(("touch", "expected"), [(True, True), (False, False)])
def test_pullback_rising_sma20(touch: bool, expected: bool) -> None:
    series = _series(_pullback_bars(touch=touch))
    levels = compute_levels(series, indicators=NO_INDICATORS)
    state = detect_setups(series, levels=levels)["pullback_rising_sma20"]
    assert not state.inputs_missing
    assert state.detected is expected


def _range_bars(*, bounce: bool) -> list[tuple[float, float, float]]:
    """Oscillation between ~97 and ~103 (width < 4 ATR), ending at support."""
    cycle = [100.0, 102.0, 103.0, 101.0, 99.0, 97.5, 98.0]
    closes = (cycle * 6)[:40]  # 40 bars: ATR warm-up covers the 20-bar window
    bars = [(c + 0.5, c - 0.5, c) for c in closes]
    bars[-2] = (98.0, 97.0, 97.3)
    bars[-1] = (98.5, 97.2, 98.2 if bounce else 97.2)
    return bars


@pytest.mark.parametrize(("bounce", "expected"), [(True, True), (False, False)])
def test_range_support_bounce(bounce: bool, expected: bool) -> None:
    series = _series(_range_bars(bounce=bounce))
    levels = compute_levels(series, indicators=NO_INDICATORS)
    state = detect_setups(series, levels=levels)["range_support_bounce"]
    assert not state.inputs_missing
    assert state.detected is expected


def _flag_bars(*, new_high: bool) -> list[tuple[float, float, float]]:
    base = _flat(20, close=100.0, spread=0.5)
    pole = [(100.0 + 2 * i + 0.5, 100.0 + 2 * i - 0.5, 100.0 + 2 * i) for i in range(1, 9)]  # to 116
    flag = [(115.5, 113.0, 114.0), (115.0, 112.5, 113.0), (114.5, 112.0, 113.5), (115.0, 112.5, 114.0)]
    if new_high:
        flag[-1] = (117.0, 113.0, 116.5)
    return base + pole + flag


@pytest.mark.parametrize(("new_high", "expected"), [(False, True), (True, False)])
def test_trend_continuation_flag(new_high: bool, expected: bool) -> None:
    series = _series(_flag_bars(new_high=new_high))
    levels = compute_levels(series, indicators=NO_INDICATORS)
    state = detect_setups(series, levels=levels)["trend_continuation_flag"]
    assert not state.inputs_missing
    assert state.detected is expected


def test_no_level_is_anchored_after_its_knowable_bar() -> None:
    # O-v6-3 look-ahead: at every cut t, a pivot level's origin is <= t - 2 (confirmation) and a
    # Donchian level's origin is <= t - 1 (bar t excluded).
    import math

    path = [
        (100 + 10 * math.sin(i / 5) + 0.6, 100 + 10 * math.sin(i / 5) - 0.6, 100 + 10 * math.sin(i / 5))
        for i in range(90)
    ]
    seen_pivot = False
    for t in range(30, 90):
        levels = compute_levels(_series(path[: t + 1]), indicators=NO_INDICATORS)
        for key, level in levels.items():
            if level is None or level.origin_bar is None:
                continue
            if key.startswith("swing_"):
                seen_pivot = True
                assert level.origin_bar <= t - 2, (t, key)
            else:
                assert level.origin_bar <= t - 1, (t, key)
    assert seen_pivot


def test_a_masked_low_does_not_null_the_donchian_high() -> None:
    bars = _flat(25) + [(110.0, 104.0, 109.0)]
    series = _series(bars)
    rows = list(series.rows)
    masked = dict(rows[10])
    masked["low"] = None
    rows[10] = masked  # type: ignore[call-overload]
    holed = BarSeries(dates=series.dates, rows=tuple(rows))  # type: ignore[arg-type]
    levels = compute_levels(holed, indicators=NO_INDICATORS)
    assert levels["donchian20_low"] is None
    assert _price(levels["donchian20_high"]) == 101
    assert detect_setups(holed, levels=levels)["breakout_donchian20"].detected
