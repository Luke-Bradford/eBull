"""Tests for the pure price-structure primitives (#2279).

Pure-logic only, no DB — the decisions here are all expressible as table tests,
which is the repo's default. The DB-facing half (``research_price_structure_store``)
is exercised by the acceptance script against the real corpus rather than by a
fixture, because what it must get right is fail-closed behaviour against real
coverage rows.

The tests that matter most are not the "does it find the pivot" ones. They are:

- ``not_evaluable`` is DISTINGUISHABLE from an empty result (the vacuous-truth
  class),
- masked bars suppress a pivot rather than producing one,
- every confirmation index is emitted and equals ``index + n``,
- the warm-up boundary is asserted off-by-one in BOTH directions.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.services.price_structure import (
    BANDWIDTH_LOOKBACK,
    BANDWIDTH_WINDOW,
    FIB_RATIOS,
    RULE_SET_ID,
    RULE_SET_VERSION,
    SWING_LADDER,
    Level,
    State,
    StructureBar,
    Swing,
    SwingSeries,
    anchored_vwap,
    classify_interaction,
    cluster_levels,
    detect_swings,
    fib_levels,
    find_break_and_retest,
    select_leg,
    volatility_regime,
)

BASE = date(2020, 1, 1)


def _bars(
    highs: Sequence[float | None],
    lows: Sequence[float | None] | None = None,
    closes: Sequence[float | None] | None = None,
    volumes: Sequence[int | None] | None = None,
) -> list[StructureBar]:
    """Bars from a high series; lows default to high - 1, closes to the midpoint."""
    n = len(highs)
    low_series: Sequence[float | None] = lows if lows is not None else [None if h is None else h - 1.0 for h in highs]
    close_series: Sequence[float | None] = (
        closes
        if closes is not None
        else [None if (h is None or lo is None) else (h + lo) / 2.0 for h, lo in zip(highs, low_series, strict=True)]
    )
    volume_series: Sequence[int | None] = volumes if volumes is not None else [1_000] * n
    out: list[StructureBar] = []
    for i in range(n):
        h, lo, c = highs[i], low_series[i], close_series[i]
        out.append(
            StructureBar(
                bar_date=BASE + timedelta(days=i),
                open=Decimal(str(lo if lo is not None else 1.0)),
                high=None if h is None else Decimal(str(h)),
                low=None if lo is None else Decimal(str(lo)),
                close=None if c is None else Decimal(str(c)),
                volume=volume_series[i],
            )
        )
    return out


def _swing(index: int, kind: str, price: float, n: int = 1) -> Swing:
    return Swing(
        index=index,
        bar_date=BASE + timedelta(days=index),
        kind=kind,  # type: ignore[arg-type]
        price=price,
        n=n,
        confirmed_index=index + n,
        confirmed_date=BASE + timedelta(days=index + n),
    )


def _series(swings: Sequence[Swing], *, n: int = 1, blinded: tuple[int, ...] = ()) -> SwingSeries:
    """Wrap hand-made swings so cluster_levels gets the upstream tri-state.

    cluster_levels takes the whole SwingSeries precisely so "found none" cannot
    be confused with "could not look" — so a test helper that fabricated a
    plausible-looking state would defeat the thing being tested. State here is
    derived from the same rule the detector uses.
    """
    return SwingSeries(
        state=_state_for_test(bool(swings), bool(blinded)),
        swings=tuple(swings),
        n=n,
        bars_evaluated=0,
        not_evaluable_indices=blinded,
        universe="survivor_only",
    )


def _state_for_test(found: bool, blinded: bool) -> State:
    if found:
        return "fired"
    return "not_evaluable" if blinded else "not_fired"


# ---------------------------------------------------------------------------
# Swings
# ---------------------------------------------------------------------------


def test_detects_a_simple_pivot_high_and_low() -> None:
    bars = _bars([1.0, 2.0, 5.0, 2.0, 1.0])
    result = detect_swings(bars, 2, universe="survivor_only")

    assert result.state == "fired"
    highs = [s for s in result.swings if s.kind == "high"]
    assert [s.index for s in highs] == [2]
    assert highs[0].price == 5.0


@pytest.mark.parametrize("n", sorted(SWING_LADDER.values()))
def test_confirmation_index_is_always_index_plus_n(n: int) -> None:
    """The whole look-ahead defence. If this drifts, backtests silently lie."""
    highs = [1.0] * (2 * n + 1)
    highs[n] = 9.0
    result = detect_swings(_bars(highs), n, universe="survivor_only")

    assert result.state == "fired"
    for swing in result.swings:
        assert swing.confirmed_index == swing.index + n
        assert swing.confirmed_date == BASE + timedelta(days=swing.index + n)


def test_too_short_is_not_evaluable_and_not_merely_empty() -> None:
    """`not_evaluable` vs `not_fired` is the distinction the whole tri-state exists for."""
    short = detect_swings(_bars([1.0, 2.0, 3.0]), 2, universe="survivor_only")
    assert short.state == "not_evaluable"
    assert short.swings == ()

    flat = detect_swings(_bars([1.0] * 11), 2, universe="survivor_only")
    assert flat.state == "not_fired"
    assert flat.swings == ()

    assert short.state != flat.state


@pytest.mark.parametrize(("n", "length", "expected"), [(2, 4, "not_evaluable"), (2, 5, "not_fired")])
def test_warmup_boundary_is_exact_in_both_directions(n: int, length: int, expected: str) -> None:
    result = detect_swings(_bars([1.0] * length), n, universe="survivor_only")
    assert result.state == expected


def test_plateau_yields_no_pivot() -> None:
    """Two equal highs in one window: the strict rule must emit neither.

    Emitting both would cluster into one level with an inflated touch count, and
    the count is the only thing a level asserts.
    """
    result = detect_swings(_bars([1.0, 5.0, 3.0, 5.0, 1.0]), 2, universe="survivor_only")
    assert [s for s in result.swings if s.kind == "high"] == []


def test_masked_bar_suppresses_the_pivot_rather_than_asserting_one() -> None:
    """A quarantined wick must not be able to create OR destroy a swing silently."""
    clean = detect_swings(_bars([1.0, 2.0, 5.0, 2.0, 1.0]), 2, universe="survivor_only")
    assert [s.index for s in clean.swings if s.kind == "high"] == [2]

    # Mask a NEIGHBOUR: it cannot refute the candidate, so the candidate becomes
    # undecidable rather than confirmed.
    masked = detect_swings(
        _bars([1.0, None, 5.0, 2.0, 1.0]),
        2,
        universe="survivor_only",
    )
    assert [s.index for s in masked.swings if s.kind == "high"] == []
    assert 2 in masked.not_evaluable_indices


def test_masked_candidate_bar_is_reported_not_evaluable() -> None:
    result = detect_swings(_bars([1.0, 2.0, None, 2.0, 1.0]), 2, universe="survivor_only")
    assert 2 in result.not_evaluable_indices


def test_universe_label_is_carried_through() -> None:
    result = detect_swings(_bars([1.0] * 11), 2, universe="survivor_only")
    assert result.universe == "survivor_only"
    assert result.rule_set_version == RULE_SET_VERSION


# ---------------------------------------------------------------------------
# Levels
# ---------------------------------------------------------------------------


def test_highs_and_lows_never_cluster_together() -> None:
    """A level asserts which side price approached from; merging destroys that."""
    bars = _bars([10.0] * 40)
    swings = [_swing(20, "high", 10.0), _swing(25, "low", 10.0)]
    result = cluster_levels(bars, _series(swings), universe="survivor_only")

    kinds = sorted(lv.kind for lv in result.levels)
    assert kinds == ["resistance", "support"]
    assert all(lv.touches == 1 for lv in result.levels)


def test_nearby_swings_cluster_and_count_touches() -> None:
    bars = _bars([10.0] * 40)
    swings = [_swing(20, "high", 10.0), _swing(25, "high", 10.05), _swing(30, "high", 30.0)]
    result = cluster_levels(bars, _series(swings), universe="survivor_only")

    resistance = sorted((lv for lv in result.levels if lv.kind == "resistance"), key=lambda lv: lv.price_mean)
    assert [lv.touches for lv in resistance] == [2, 1]
    assert resistance[0].price_mean == pytest.approx((10.0 + 10.05) / 2)


def test_swing_with_unusable_atr_window_is_reported_unclustered() -> None:
    """Fail-closed: no fallback tolerance, so no silently-merged level."""
    highs: list[float | None] = [10.0] * 40
    highs[18] = None  # inside the ATR(14) window ending at index 20
    bars = _bars(highs)
    result = cluster_levels(bars, _series([_swing(20, "high", 10.0)]), universe="survivor_only")

    assert result.levels == ()
    assert [s.index for s in result.unclustered] == [20]


def test_no_swings_is_not_fired_not_a_crash() -> None:
    result = cluster_levels(_bars([10.0] * 40), _series([]), universe="survivor_only")
    assert result.state == "not_fired"


# ---------------------------------------------------------------------------
# Level interaction
# ---------------------------------------------------------------------------


def _flat_bars(n: int = 40) -> list[StructureBar]:
    return _bars([10.5] * n, lows=[9.5] * n, closes=[10.0] * n)


def _level(low: float = 9.9, high: float = 10.1) -> Level:
    return Level(
        kind="resistance",
        price_low=low,
        price_high=high,
        price_mean=(low + high) / 2,
        touches=2,
        first_touch_date=BASE,
        last_touch_date=BASE + timedelta(days=5),
    )


def test_close_inside_the_band_is_a_touch() -> None:
    assert classify_interaction(_level(), _flat_bars(), 30) == "touch"


def test_break_requires_a_close_through_not_a_wick_through() -> None:
    """The wick is the part of a bar most likely to be a bad print."""
    bars = _flat_bars()
    bars[30] = StructureBar(
        bar_date=bars[30].bar_date,
        open=Decimal("10"),
        high=Decimal("50"),  # wick far above
        low=Decimal("9.5"),
        close=Decimal("10.0"),  # ... but closes inside
        volume=1_000,
    )
    assert classify_interaction(_level(), bars, 30) == "touch"


def test_break_direction_is_reported() -> None:
    bars = _flat_bars()
    up = list(bars)
    up[30] = StructureBar(bars[30].bar_date, Decimal("10"), Decimal("99"), Decimal("98"), Decimal("99"), 1_000)
    assert classify_interaction(_level(), up, 30) == "break_up"

    down = list(bars)
    down[30] = StructureBar(bars[30].bar_date, Decimal("1"), Decimal("1.2"), Decimal("0.9"), Decimal("1.0"), 1_000)
    assert classify_interaction(_level(), down, 30) == "break_down"


def test_unusable_atr_window_makes_the_interaction_not_evaluable() -> None:
    bars = _flat_bars()
    bars[25] = StructureBar(bars[25].bar_date, Decimal("10"), None, None, Decimal("10"), 1_000)
    assert classify_interaction(_level(), bars, 30) == "not_evaluable"


def _replace(bars: list[StructureBar], i: int, high: float, low: float, close: float) -> None:
    bars[i] = StructureBar(
        bars[i].bar_date, Decimal("10"), Decimal(str(high)), Decimal(str(low)), Decimal(str(close)), 1_000
    )


def test_break_and_retest_requires_all_three_legs() -> None:
    """Break at 30, gap-over at 31, retest at 32, confirm at 34.

    Bar 31 deliberately sits entirely above the band: a bar that gaps past the
    level is NOT a retest, and the window continues rather than resetting.
    """
    bars = _flat_bars(60)
    _replace(bars, 30, 20.0, 19.0, 20.0)  # break up
    _replace(bars, 31, 22.0, 21.0, 21.5)  # gap-over: no intersection with the band
    _replace(bars, 32, 10.2, 9.8, 10.0)  # retest into the band
    _replace(bars, 33, 20.5, 19.5, 10.0)  # still inside on the close: not a confirm
    _replace(bars, 34, 21.0, 20.0, 21.0)  # confirm

    result = find_break_and_retest(_level(), bars, universe="survivor_only", max_retest_bars=10)
    assert result.state == "fired"
    assert [(p.break_index, p.retest_index, p.confirm_index) for p in result.patterns] == [(30, 32, 34)]
    assert result.patterns[0].direction == "up"


def test_opposite_side_close_voids_the_pattern_and_rebreaks() -> None:
    """A close through the far side is itself a break, so it must not be dropped."""
    bars = _flat_bars(60)
    _replace(bars, 30, 20.0, 19.0, 20.0)  # break up
    _replace(bars, 31, 22.0, 21.0, 21.5)  # keep out of the band
    _replace(bars, 32, 1.2, 0.9, 1.0)  # close through the OTHER side

    result = find_break_and_retest(_level(), bars, universe="survivor_only", max_retest_bars=10)
    assert result.state == "not_fired"  # the up-pattern never confirmed


def test_break_without_retest_is_not_fired_not_not_evaluable() -> None:
    """The absence was observed, so it is a negative — not missing evidence."""
    bars = _flat_bars(60)
    bars[30] = StructureBar(bars[30].bar_date, Decimal("10"), Decimal("20"), Decimal("19"), Decimal("20"), 1_000)

    result = find_break_and_retest(_level(), bars, universe="survivor_only", max_retest_bars=3)
    assert result.state == "not_fired"


# ---------------------------------------------------------------------------
# Fibonacci
# ---------------------------------------------------------------------------


def test_leg_walks_back_past_same_kind_swings() -> None:
    """Fractals do not alternate; 'the last high and the last low' is ambiguous."""
    swings = [
        _swing(5, "low", 10.0),
        _swing(10, "high", 20.0),
        _swing(15, "high", 30.0),
    ]
    leg = select_leg(swings)
    assert leg is not None
    assert (leg.start.index, leg.end.index) == (5, 15)
    assert leg.direction == "up"


def test_leg_of_all_same_kind_is_none() -> None:
    assert select_leg([_swing(1, "high", 1.0), _swing(2, "high", 2.0)]) is None


def test_up_leg_measures_down_from_the_high() -> None:
    leg = select_leg([_swing(0, "low", 100.0), _swing(10, "high", 200.0)])
    assert leg is not None
    fib = fib_levels(leg, universe="survivor_only")

    assert fib.direction == "up"
    assert fib.levels[0.5] == pytest.approx(150.0)
    assert fib.levels[0.236] == pytest.approx(200.0 - 0.236 * 100.0)


def test_down_leg_measures_up_from_the_low() -> None:
    leg = select_leg([_swing(0, "high", 200.0), _swing(10, "low", 100.0)])
    assert leg is not None
    fib = fib_levels(leg, universe="survivor_only")

    assert fib.direction == "down"
    assert fib.levels[0.5] == pytest.approx(150.0)
    assert fib.levels[0.236] == pytest.approx(100.0 + 0.236 * 100.0)


def test_fib_is_unusable_before_the_later_anchor_is_confirmed() -> None:
    start = _swing(0, "low", 100.0, n=5)
    end = _swing(10, "high", 200.0, n=5)
    fib = fib_levels(select_leg([start, end]), universe="survivor_only")

    assert fib.usable_from_index is not None
    assert fib.usable_from_index == end.confirmed_index == 15
    assert fib.usable_from_index > end.index


def test_no_leg_is_not_evaluable() -> None:
    fib = fib_levels(None, universe="survivor_only")
    assert fib.state == "not_evaluable"
    assert fib.levels == {}


def test_all_published_ratios_are_returned() -> None:
    fib = fib_levels(select_leg([_swing(0, "low", 0.0), _swing(1, "high", 100.0)]), universe="survivor_only")
    assert sorted(fib.levels) == sorted(FIB_RATIOS)


# ---------------------------------------------------------------------------
# Anchored VWAP
# ---------------------------------------------------------------------------


def test_vwap_is_the_volume_weighted_typical_price() -> None:
    bars = _bars([12.0, 12.0], lows=[8.0, 8.0], closes=[10.0, 10.0], volumes=[100, 300])
    result = anchored_vwap(bars, 0, universe="survivor_only")

    assert result.state == "fired"
    assert result.value == pytest.approx(10.0)  # HLC3 = 10 on both bars
    assert result.bars_with_volume == 2


def test_zero_volume_window_is_not_evaluable_never_zero() -> None:
    """5.16% of corpus bars have zero volume; a 0.0 VWAP would be a fabricated price."""
    bars = _bars([12.0, 12.0], volumes=[0, 0])
    result = anchored_vwap(bars, 0, universe="survivor_only")

    assert result.state == "not_evaluable"
    assert result.value is None
    assert result.bars_with_volume == 0


def test_null_volume_is_treated_as_no_volume_not_as_zero_weight() -> None:
    bars = _bars([12.0, 12.0], lows=[8.0, 8.0], closes=[10.0, 10.0], volumes=[None, 300])
    result = anchored_vwap(bars, 0, universe="survivor_only")
    assert result.bars_with_volume == 1


def test_swing_anchor_exposes_a_usable_from_distinct_from_the_anchor() -> None:
    """The sum starts at the pivot; the CHOICE was not knowable until confirmation."""
    bars = _bars([12.0] * 20)
    anchor = _swing(5, "high", 12.0, n=5)
    result = anchored_vwap(bars, anchor, universe="survivor_only")

    assert result.anchor_index == 5
    assert result.usable_from_index == 10
    assert result.usable_from_index > result.anchor_index


def test_int_anchor_is_usable_immediately() -> None:
    """A calendar anchor (an earnings date) IS knowable at its own bar."""
    result = anchored_vwap(_bars([12.0] * 20), 5, universe="survivor_only")
    assert result.anchor_index == result.usable_from_index == 5


# ---------------------------------------------------------------------------
# Volatility regime
# ---------------------------------------------------------------------------


def _closes(values: list[float]) -> list[Decimal]:
    return [Decimal(str(v)) for v in values]


def test_regime_warmup_boundary_is_window_plus_lookback_minus_one() -> None:
    needed = BANDWIDTH_WINDOW + BANDWIDTH_LOOKBACK - 1
    assert needed == 145

    short = volatility_regime(_closes([10.0] * (needed - 1)), universe="survivor_only")
    assert short.regime == "not_evaluable"

    exact = volatility_regime(_closes([10.0 + i * 0.1 for i in range(needed)]), universe="survivor_only")
    assert exact.regime != "not_evaluable"


def test_narrowing_series_ends_in_compression() -> None:
    """Bollinger's Squeeze: BandWidth at its lowest in six months, not a percentile."""
    needed = BANDWIDTH_WINDOW + BANDWIDTH_LOOKBACK - 1
    # Oscillation whose amplitude decays to nothing: the last window is tightest.
    values = [100.0 + (needed - i) * 0.5 * (1 if i % 2 else -1) for i in range(needed)]
    result = volatility_regime(_closes(values), universe="survivor_only")

    assert result.regime == "compression"
    assert result.bandwidth_pct_rank == 0.0


def test_widening_series_ends_in_expansion() -> None:
    needed = BANDWIDTH_WINDOW + BANDWIDTH_LOOKBACK - 1
    values = [100.0 + i * 0.5 * (1 if i % 2 else -1) for i in range(needed)]
    result = volatility_regime(_closes(values), universe="survivor_only")

    assert result.regime == "expansion"
    assert result.bandwidth_pct_rank == pytest.approx(100.0 * (BANDWIDTH_LOOKBACK - 1) / BANDWIDTH_LOOKBACK)


def test_masked_close_makes_the_regime_not_evaluable() -> None:
    needed = BANDWIDTH_WINDOW + BANDWIDTH_LOOKBACK - 1
    values: list[Decimal | None] = [Decimal("10")] * needed
    values[-3] = None
    assert volatility_regime(values, universe="survivor_only").regime == "not_evaluable"


def test_non_positive_middle_band_is_not_evaluable_not_a_division() -> None:
    """2 corpus bars close at or below zero; dividing by that middle is nonsense."""
    needed = BANDWIDTH_WINDOW + BANDWIDTH_LOOKBACK - 1
    result = volatility_regime(_closes([0.0] * needed), universe="survivor_only")
    assert result.regime == "not_evaluable"


# ---------------------------------------------------------------------------
# Rule-set version
# ---------------------------------------------------------------------------


def test_version_is_derived_from_module_source() -> None:
    """Asserted as DERIVED, not as 'changes only when a rule changes'.

    The source hash over-invalidates — a comment edit bumps it. That is the
    inherited, deliberate trade from ``price_quarantine``: visibly stale beats
    silently mixed. Asserting the narrower property would be asserting something
    that is both untestable and false.
    """
    import hashlib
    from pathlib import Path

    import app.services.price_structure as module

    expected = hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()[:12]
    assert RULE_SET_VERSION == f"{RULE_SET_ID}+{expected}"


def test_version_is_distinct_from_the_quarantine_rule_set() -> None:
    from app.services.price_quarantine import RULE_SET_VERSION as QUARANTINE_VERSION

    assert RULE_SET_VERSION != QUARANTINE_VERSION


# ---------------------------------------------------------------------------
# Blinded tri-state — "found nothing while some evidence was hidden"
# ---------------------------------------------------------------------------
#
# Three separate places got this wrong on the first pass (caught by Codex at
# checkpoint 2). Finding nothing while a bar was masked is NOT a negative, and
# reporting it as `not_fired` feeds a quarantined bar into a win-rate
# denominator as an observed miss.


def test_masked_candidate_with_no_swing_is_not_evaluable_not_not_fired() -> None:
    result = detect_swings(_bars([1.0, None, 5.0, 2.0, 1.0]), 2, universe="survivor_only")
    assert result.swings == ()
    assert result.not_evaluable_indices != ()
    assert result.state == "not_evaluable"


def test_a_real_swing_still_fires_even_when_another_candidate_was_masked() -> None:
    """Partial blinding must not suppress structure that WAS observed."""
    highs: list[float | None] = [1.0, 2.0, 9.0, 2.0, 1.0, 2.0, None, 2.0, 1.0]
    result = detect_swings(_bars(highs), 2, universe="survivor_only")

    assert [s.index for s in result.swings if s.kind == "high"] == [2]
    assert result.not_evaluable_indices != ()
    assert result.state == "fired"


def test_all_swings_unclusterable_is_not_evaluable() -> None:
    highs: list[float | None] = [10.0] * 40
    highs[18] = None  # inside the ATR(14) window ending at index 20
    result = cluster_levels(_bars(highs), _series([_swing(20, "high", 10.0)]), universe="survivor_only")

    assert result.levels == ()
    assert result.unclustered != ()
    assert result.state == "not_evaluable"


def test_masked_bar_inside_a_sequence_voids_it_rather_than_spanning_it() -> None:
    """The worst failure direction: a positive signal emitted across hidden evidence.

    Without the reset, the break at 30 would survive the masked bar at 31 and
    pair with the retest/confirm after it — reporting a pattern over a bar the
    quarantine says is not a price, and which could have been the opposite-side
    close that invalidated the setup.
    """
    bars = _flat_bars(60)
    _replace(bars, 30, 20.0, 19.0, 20.0)  # break up
    bars[31] = StructureBar(bars[31].bar_date, Decimal("10"), None, None, None, 1_000)  # masked
    _replace(bars, 32, 10.2, 9.8, 10.0)  # would-be retest
    _replace(bars, 34, 21.0, 20.0, 21.0)  # would-be confirm

    result = find_break_and_retest(_level(), bars, universe="survivor_only", max_retest_bars=10)
    assert result.patterns == ()
    assert result.state == "not_evaluable"


def test_masked_bar_outside_any_sequence_does_not_blind_the_result() -> None:
    """Only a mask INSIDE an active sequence hides anything."""
    bars = _flat_bars(60)
    bars[5] = StructureBar(bars[5].bar_date, Decimal("10"), None, None, None, 1_000)
    _replace(bars, 30, 20.0, 19.0, 20.0)  # break, never retested

    result = find_break_and_retest(_level(), bars, universe="survivor_only", max_retest_bars=3)
    assert result.state == "not_fired"


def test_cluster_levels_inherits_upstream_not_evaluable() -> None:
    """The fourth instance of the same collapse, found by the review bot.

    A bare `Sequence[Swing]` cannot distinguish "the detector found none" from
    "the detector could not look" — both arrive as `()`. Taking the whole
    SwingSeries is what makes the collapse unrepresentable.
    """
    too_short = detect_swings(_bars([1.0, 2.0, 3.0]), 2, universe="survivor_only")
    assert too_short.state == "not_evaluable"

    result = cluster_levels(_bars([1.0, 2.0, 3.0]), too_short, universe="survivor_only")
    assert result.state == "not_evaluable"


def test_cluster_levels_inherits_partial_upstream_blinding() -> None:
    blinded = _series([], blinded=(7,))
    assert cluster_levels(_bars([10.0] * 40), blinded, universe="survivor_only").state == "not_evaluable"


def test_bars_evaluated_is_comparable_across_both_branches() -> None:
    """Both paths count bars at which a pivot could be DECIDED, so yields divide."""
    short = detect_swings(_bars([1.0] * 4), 2, universe="survivor_only")
    assert short.state == "not_evaluable"
    assert short.bars_evaluated == 0

    normal = detect_swings(_bars([1.0] * 11), 2, universe="survivor_only")
    assert normal.bars_evaluated == 11 - 2 * 2


def test_a_break_landing_on_the_timeout_bar_starts_a_fresh_sequence() -> None:
    """The expired sequence must not swallow the bar that outlives it.

    Previously the timeout branch reset to idle and `continue`d, dropping a
    same-direction break on that exact bar — while an OPPOSITE-direction break
    on the same bar was correctly kept. Two rules for one event, decided by
    branch order.
    """
    bars = _flat_bars(80)
    _replace(bars, 20, 20.0, 19.0, 20.0)  # break up
    # 21-25 sit entirely above the band, so nothing retests and the window runs
    # out. (The flat baseline closes INSIDE the band, so leaving them alone
    # would hand the first break a retest on the very next bar.)
    for i in range(21, 26):
        _replace(bars, i, 25.0, 24.0, 24.5)
    _replace(bars, 26, 30.0, 29.0, 30.0)  # break landing one bar past the window
    _replace(bars, 27, 25.0, 24.0, 24.5)  # still clear of the band
    _replace(bars, 28, 10.2, 9.8, 10.0)  # retest of the SECOND break
    _replace(bars, 30, 31.0, 30.0, 31.0)  # confirm

    result = find_break_and_retest(_level(), bars, universe="survivor_only", max_retest_bars=5)
    assert [(p.break_index, p.retest_index, p.confirm_index) for p in result.patterns] == [(26, 28, 30)]


def test_a_confirmation_after_the_retest_window_does_not_emit() -> None:
    """The window is enforced by expiry, so a late close cannot resurrect it."""
    bars = _flat_bars(80)
    _replace(bars, 20, 20.0, 19.0, 20.0)  # break up
    _replace(bars, 21, 10.2, 9.8, 10.0)  # retest
    _replace(bars, 40, 21.0, 20.0, 21.0)  # confirm, far outside the window

    result = find_break_and_retest(_level(), bars, universe="survivor_only", max_retest_bars=3)
    assert result.patterns == ()
