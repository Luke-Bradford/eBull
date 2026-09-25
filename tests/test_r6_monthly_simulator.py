"""#2901 PR B part 2a: ``simulate_monthly`` against the spec's reconciliation tests 1–11 and parity.

Spec: ``docs/proposals/ta/2026-09-25-2901-quality-declaration-spec.md`` ("Simulator", "Reconciliation tests").
Every fixture runs under the three programme policies at h = 0 and h = HALF_SPREAD, and every run is checked for
parity with the frozen ``simulate_portfolio`` and for telescoping. The expected monthly wealths are written out by
hand (test 8): parity cannot catch a return booked in the wrong month.

2013 NYSE sessions used: Jul 1 (first of July), Jul 31, Aug 30 (last of August), Sep 3 (first after Labor Day),
Sep 27, Sep 30, Oct 1, Oct 25. Jul 4 is a full closure (the closure-dated bar case).
"""

from __future__ import annotations

import math
from datetime import date, datetime
from typing import Any

import pytest

from app.services.r6_exclusion_trial import (
    HALF_SPREAD,
    PROGRAMME_POLICIES,
    ZERO_RECOVERY,
    PriceBar,
    PriceSeries,
    SeriesEvidence,
    TerminationPolicy,
    simulate_portfolio,
)
from app.services.r6_monthly_trial import (
    MonthlyResult,
    MonthlySchedule,
    ParityMismatch,
    Refused,
    SimulationError,
    check_parity,
    identity_gate,
    last_session_of_month,
    simulate_monthly,
)
from app.services.series_termination import TerminationClass, TerminationEvidence

JUL1, JUL31, AUG30 = date(2013, 7, 1), date(2013, 7, 31), date(2013, 8, 30)
SEP3, SEP27, SEP30, OCT1, OCT25 = (
    date(2013, 9, 3),
    date(2013, 9, 27),
    date(2013, 9, 30),
    date(2013, 10, 1),
    date(2013, 10, 25),
)
#: The annual simulator's formation for each x_date: the first bar after it is that x_date.
FORMATION = {JUL1: datetime(2013, 6, 28), SEP3: datetime(2013, 8, 30), OCT1: datetime(2013, 9, 30)}
H_VALUES = (0.0, HALF_SPREAD)
UNKNOWN = TerminationClass.UNKNOWN


def _series(symbol: str, *rows: tuple[date, float, float]) -> PriceSeries:
    return PriceSeries(symbol, tuple(PriceBar(day, o, c) for day, o, c in rows), 0)


def _evidence(prices: dict[str, PriceSeries]) -> dict[str, SeriesEvidence]:
    return {
        symbol: SeriesEvidence(
            index,
            series.bars[0].day,
            series.bars[-1].day,
            TerminationEvidence(linked=False, provision=None, q_suffix=False),
        )
        for index, (symbol, series) in enumerate(sorted(prices.items()), start=1)
    }


def _run(
    schedule: MonthlySchedule,
    prices: dict[str, PriceSeries],
    policy: TerminationPolicy,
    h: float,
    window_end: date,
) -> MonthlyResult:
    """The monthly path, with parity (test 6) and telescoping (test 1) asserted on every call."""
    evidence = _evidence(prices)
    monthly = simulate_monthly(
        schedule=schedule, prices=prices, policy=policy, half_spread=h, evidence=evidence, window_end=window_end
    )
    annual = simulate_portfolio(
        schedule=tuple((FORMATION[day], target) for day, target in schedule),
        prices=prices,
        policy=policy,
        half_spread=h,
        window_end=window_end,
        evidence=evidence,
    )
    check_parity(monthly, annual, window_end=window_end)
    telescoped = math.prod(monthly.factors.values()) * monthly.partial_factor
    assert telescoped == pytest.approx(monthly.terminal_wealth, rel=1e-12, abs=1e-15)
    _assert_bookkeeping(monthly, h)
    return monthly


def _assert_bookkeeping(result: MonthlyResult, h: float) -> None:
    """Test 2 (pre-cost W = n·target + cost; W = cash + holdings) and test 3 (recognised cash)."""
    for event, target in zip(result.events[:-1], result.target_values, strict=True):
        assert event.target_count * target + event.spread_cost == pytest.approx(event.pre_cost_wealth, rel=1e-10)
    for mark in result.marks:
        assert mark.wealth == pytest.approx(mark.cash + mark.held_value, rel=1e-12, abs=1e-15)
    at_marks = {mark.session for mark in result.marks}
    recognised = [r for r in result.realisations if r.session in at_marks]
    assert all(r.status == "terminated" for r in recognised)
    credited = sum(mark.recognised_cash for mark in result.marks)
    assert credited == pytest.approx(sum(r.realised_value * (1 - h) for r in recognised), rel=1e-12, abs=1e-15)
    terminated = [r.symbol for r in result.realisations if r.status == "terminated"]
    assert len(terminated) == len(set(terminated)), "a terminated symbol was valued again after recognition"


def _wealths(result: MonthlyResult) -> dict[tuple[int, int], float]:
    return {mark.month: mark.wealth for mark in result.marks}


def _approx(expected: dict[Any, float]) -> Any:
    return pytest.approx(expected, rel=1e-12, abs=1e-15)


# --- Test 4: the hand-computed equal-weight fixture ----------------------------------------------------------

PLAIN = {
    "AAA": _series("AAA", (JUL1, 10.0, 10.0), (JUL31, 11.0, 11.0), (AUG30, 12.0, 12.0), (SEP27, 13.0, 13.0)),
    "BBB": _series("BBB", (JUL1, 20.0, 20.0), (JUL31, 18.0, 18.0), (AUG30, 22.0, 22.0), (SEP27, 20.0, 20.0)),
}


@pytest.mark.parametrize("policy", PROGRAMME_POLICIES, ids=lambda p: p.label)
@pytest.mark.parametrize("h", H_VALUES)
def test_equal_weight_fixture_by_hand(policy: TerminationPolicy, h: float) -> None:
    result = _run(((JUL1, frozenset(PLAIN)),), PLAIN, policy, h, SEP27)
    t = 1 / (2 * (1 + h))  # 2t + h·2t = 1
    assert _wealths(result) == _approx({(2013, 6): 1.0, (2013, 7): t * 2.0, (2013, 8): t * (1.2 + 1.1)})
    assert result.terminal_wealth == pytest.approx(t * (1.3 + 1.0) * (1 - h), rel=1e-12)
    assert result.marks[-1].session == AUG30 and result.ruin_month is None
    if h == 0:
        assert dict(result.factors) == _approx({(2013, 7): 1.0, (2013, 8): 1.15})
        assert result.partial_return == pytest.approx(0.0, abs=1e-15)


# --- Test 5 + 8: one fixture per table cell, wealths pinned by hand ------------------------------------------

#: TRM's last bar is the July month-end: priced there, recognised at the August mark.
#: GAP misses the July mark and recovers. LIV (alive at capture) misses July and the final.
#: FIN terminates inside the partial final month. CLO's last bar is dated on the Jul 4 full closure.
CELLS = {
    "TRM": _series("TRM", (JUL1, 10.0, 10.0), (JUL31, 8.0, 8.0)),
    "GAP": _series("GAP", (JUL1, 10.0, 10.0), (AUG30, 12.0, 12.0), (SEP27, 11.0, 11.0)),
    "LIV": _series("LIV", (JUL1, 10.0, 10.0), (AUG30, 9.0, 9.0), (date(2024, 9, 26), 5.0, 5.0)),
    "FIN": _series("FIN", (JUL1, 10.0, 10.0), (JUL31, 10.0, 10.0), (AUG30, 10.0, 10.0), (date(2013, 9, 10), 6.0, 6.0)),
    "CLO": _series("CLO", (JUL1, 10.0, 10.0), (date(2013, 7, 4), 7.0, 7.0)),
}


@pytest.mark.parametrize("policy", PROGRAMME_POLICIES, ids=lambda p: p.label)
@pytest.mark.parametrize("h", H_VALUES)
def test_each_cell_books_its_loss_in_the_right_month(policy: TerminationPolicy, h: float) -> None:
    result = _run(((JUL1, frozenset(CELLS)),), CELLS, policy, h, SEP27)
    f = policy.terminal_fraction(UNKNOWN)
    t = 1 / (5 * (1 + h))
    s = t / 10  # every name opens at 10
    # July: TRM priced at 8; GAP held at 10; LIV held at 10; FIN 10; CLO recognised off its Jul 4 close (7).
    jul = s * (8 + 10 + 10 + 10) + s * 7 * f * (1 - h)
    # August: TRM recognised (8·f, spread h); GAP 12; LIV 9; FIN 10; CLO's cash carried.
    aug = s * 8 * f * (1 - h) + s * (12 + 9 + 10) + s * 7 * f * (1 - h)
    # Final: GAP 11; LIV alive at capture → last close 9 at fraction 1; FIN terminated → 6·f; cash carried.
    final = (s * 8 * f + s * 7 * f) * (1 - h) + s * (11 + 9 + 6 * f) * (1 - h)
    assert _wealths(result) == _approx({(2013, 6): 1.0, (2013, 7): jul, (2013, 8): aug})
    assert result.terminal_wealth == pytest.approx(final, rel=1e-12, abs=1e-15)
    statuses = {(r.symbol, r.session): r.status for r in result.realisations}
    assert statuses == {
        ("CLO", JUL31): "terminated",
        ("TRM", AUG30): "terminated",
        ("LIV", SEP27): "alive_at_capture",
        ("FIN", SEP27): "terminated",
    }
    clo = next(r for r in result.realisations if r.symbol == "CLO")
    assert clo.last_bar == date(2013, 7, 4) and clo.last_close_value == pytest.approx(s * 7)


# --- Tests 3, 5, 7, 8: rebalance cells and the multi-formation carry ------------------------------------------

#: STP terminates mid-July (recognised at the July mark; its cash carries through August and rejoins at Sep 3).
#: GAP misses the Sep 3 rebalance (sold at the gap bound) and is bought again at Oct 1.
#: RBL's last bar is the August month-end: priced there, recognised exactly at the Sep 3 rebalance.
CARRY = {
    "AAA": _series(
        "AAA",
        (JUL1, 10.0, 10.0),
        (JUL31, 10.0, 10.0),
        (AUG30, 10.0, 10.0),
        (SEP3, 10.0, 10.0),
        (SEP30, 10.0, 10.0),
        (OCT1, 10.0, 10.0),
        (OCT25, 10.0, 10.0),
    ),
    "STP": _series("STP", (JUL1, 10.0, 10.0), (date(2013, 7, 15), 4.0, 4.0)),
    "RBL": _series("RBL", (JUL1, 10.0, 10.0), (JUL31, 10.0, 10.0), (AUG30, 5.0, 5.0)),
    "GAP": _series(
        "GAP",
        (JUL1, 10.0, 10.0),
        (JUL31, 10.0, 10.0),
        (AUG30, 10.0, 10.0),
        (SEP30, 10.0, 10.0),
        (OCT1, 10.0, 10.0),
        (OCT25, 10.0, 10.0),
    ),
}
CARRY_SCHEDULE: MonthlySchedule = (
    (JUL1, frozenset({"AAA", "STP", "RBL", "GAP"})),
    (SEP3, frozenset({"AAA"})),
    (OCT1, frozenset({"AAA", "GAP"})),
)


@pytest.mark.parametrize("policy", PROGRAMME_POLICIES, ids=lambda p: p.label)
@pytest.mark.parametrize("h", H_VALUES)
def test_recognised_cash_carries_to_the_next_rebalance(policy: TerminationPolicy, h: float) -> None:
    result = _run(CARRY_SCHEDULE, CARRY, policy, h, OCT25)
    f, g = policy.terminal_fraction(UNKNOWN), policy.gap_fraction
    t1 = 1 / (4 * (1 + h))
    s = t1 / 10
    recognised = s * 4 * f * (1 - h)  # STP at the July mark
    jul = recognised + s * (10 + 10 + 10)
    aug = recognised + s * (10 + 5 + 10)
    # Sep 3: AAA held at 10 (target), RBL recognised at 5·f, GAP sold at g·10; all sold except AAA's target.
    current_aaa, rbl, gap = s * 10, s * 5 * f, s * 10 * g
    pre = recognised + current_aaa + rbl + gap
    # One target, T ≥ AAA always (the sold names and the cash only add): T + h(T − AAA + RBL + GAP) = pre.
    t2 = (pre + h * (current_aaa - rbl - gap)) / (1 + h)
    sep = t2  # AAA flat at 10
    t3 = _solve_two(t2, h)
    final = 2 * t3 * (1 - h)
    assert _wealths(result) == _approx({(2013, 6): 1.0, (2013, 7): jul, (2013, 8): aug, (2013, 9): sep})
    assert result.target_values == pytest.approx((t1, t2, t3), rel=1e-9)
    assert result.terminal_wealth == pytest.approx(final, rel=1e-9)
    gap_sale = next(r for r in result.realisations if r.symbol == "GAP")
    assert (gap_sale.session, gap_sale.status) == (SEP3, "gap")
    rebal = next(r for r in result.realisations if r.symbol == "RBL")
    assert (rebal.session, rebal.status) == (SEP3, "terminated")
    assert result.marks[1].recognised_cash == pytest.approx(recognised, abs=1e-15)


def _solve_two(wealth: float, h: float) -> float:
    """Oct 1: AAA is held at ``wealth``; buying GAP from nothing. 2T + h(|T − W| + T) = W with T ≤ W."""
    # T ≤ W: 2T + h(W − T) + hT = W → T = W(1 − h) / 2.
    return wealth * (1 - h) / 2


# --- Test 9: ruin --------------------------------------------------------------------------------------------

LONE = {"ONE": _series("ONE", (JUL1, 10.0, 10.0), (date(2013, 7, 15), 4.0, 4.0))}


@pytest.mark.parametrize("h", H_VALUES)
def test_a_ruin_inside_the_months_is_minus_one_then_zero(h: float) -> None:
    result = _run(((JUL1, frozenset(LONE)),), LONE, ZERO_RECOVERY, h, SEP27)
    assert result.ruin_month == (2013, 7)
    assert dict(result.factors) == {(2013, 7): 0.0, (2013, 8): 1.0}
    assert result.partial_return == 0.0 and result.terminal_wealth == 0.0 and result.total_return == -1.0
    assert result.ruined_within([(2013, 7), (2013, 8)]) and not result.ruined_in_partial


@pytest.mark.parametrize("h", H_VALUES)
def test_a_ruined_book_creates_no_targets_at_a_later_rebalance(h: float) -> None:
    prices = {**LONE, "AAA": CARRY["AAA"]}
    result = _run(((JUL1, frozenset(LONE)), (SEP3, frozenset({"AAA"}))), prices, ZERO_RECOVERY, h, OCT25)
    assert result.events[1].pre_cost_wealth == 0.0 and result.events[1].target_count == 0
    assert result.target_values[1] == 0.0 and result.terminal_wealth == 0.0
    assert result.ruin_month == (2013, 7) and dict(result.factors)[(2013, 9)] == 1.0


@pytest.mark.parametrize("h", H_VALUES)
def test_a_september_ruin_leaves_the_monthly_series_alone(h: float) -> None:
    late = {
        "ONE": _series(
            "ONE", (JUL1, 10.0, 10.0), (JUL31, 10.0, 10.0), (AUG30, 10.0, 10.0), (date(2013, 9, 10), 1.0, 1.0)
        )
    }
    result = _run(((JUL1, frozenset(late)),), late, ZERO_RECOVERY, h, SEP27)
    assert result.ruin_month is None and result.ruined_in_partial
    assert result.partial_return == -1.0
    assert all(factor > 0 for factor in result.factors.values())


def test_a_gate_leg_ruin_refuses_that_policy() -> None:
    result = simulate_monthly(
        schedule=((JUL1, frozenset(LONE)),),
        prices=LONE,
        policy=ZERO_RECOVERY,
        half_spread=0.0,
        evidence=_evidence(LONE),
        window_end=SEP27,
    )
    months = [(2013, 7), (2013, 8)]
    ours = {m: result.factors[m] - 1.0 for m in months}
    assert isinstance(identity_gate(ours, ours, ruined=result.ruined_within(months), months=months), Refused)


# --- Test 10: finiteness and underflow ------------------------------------------------------------------------


def test_an_underflowing_holding_value_raises() -> None:
    tiny = {"TNY": _series("TNY", (JUL1, 1e300, 1e300), (JUL31, 1e-30, 1e-30), (SEP27, 1.0, 1.0))}
    with pytest.raises(SimulationError, match="underflowed"):
        simulate_monthly(
            schedule=((JUL1, frozenset(tiny)),),
            prices=tiny,
            policy=ZERO_RECOVERY,
            half_spread=0.0,
            evidence=_evidence(tiny),
            window_end=SEP27,
        )


def test_an_underflowing_spread_cost_raises() -> None:
    # A positive notional at the smallest subnormal: h × notional rounds to 0, which is not a free trade.
    dust = {"DST": _series("DST", (JUL1, 1.0, 1.0), (JUL31, 1.0, 1.0), (AUG30, 1.0, 1.0), (SEP27, 5e-324, 5e-324))}
    with pytest.raises(SimulationError, match="final sale cost"):
        simulate_monthly(
            schedule=((JUL1, frozenset(dust)),),
            prices=dust,
            policy=ZERO_RECOVERY,
            half_spread=HALF_SPREAD,
            evidence=_evidence(dust),
            window_end=SEP27,
        )


def test_a_wealth_near_zero_is_not_a_ruin() -> None:
    near = {
        "NEA": _series(
            "NEA", (JUL1, 1.0, 1.0), (JUL31, 1e-320, 1e-320), (AUG30, 1e-310, 1e-310), (SEP27, 1e-310, 1e-310)
        )
    }
    result = _run(((JUL1, frozenset(near)),), near, ZERO_RECOVERY, 0.0, SEP27)
    assert result.ruin_month is None and 0 < result.marks[1].wealth < 1e-300


def test_a_non_finite_price_raises() -> None:
    bad = {"BAD": _series("BAD", (JUL1, 10.0, 10.0), (JUL31, math.nan, math.nan), (SEP27, 1.0, 1.0))}
    with pytest.raises(SimulationError, match="finite positive price"):
        simulate_monthly(
            schedule=((JUL1, frozenset(bad)),),
            prices=bad,
            policy=ZERO_RECOVERY,
            half_spread=0.0,
            evidence=_evidence(bad),
            window_end=SEP27,
        )


# --- Test 11 and the schedule contract ------------------------------------------------------------------------


def test_a_target_without_a_valid_bar_on_its_session_refuses() -> None:
    missing = {**PLAIN, "LAT": _series("LAT", (JUL31, 10.0, 10.0), (SEP27, 10.0, 10.0))}
    with pytest.raises(SimulationError, match="no bar on its execution session"):
        simulate_monthly(
            schedule=((JUL1, frozenset(missing)),),
            prices=missing,
            policy=ZERO_RECOVERY,
            half_spread=0.0,
            evidence=_evidence(missing),
            window_end=SEP27,
        )
    infinite = {**PLAIN, "INF": _series("INF", (JUL1, math.inf, 10.0), (SEP27, 10.0, 10.0))}
    with pytest.raises(SimulationError, match="adjusted open"):
        simulate_monthly(
            schedule=((JUL1, frozenset(infinite)),),
            prices=infinite,
            policy=ZERO_RECOVERY,
            half_spread=0.0,
            evidence=_evidence(infinite),
            window_end=SEP27,
        )


def test_the_schedule_contract() -> None:
    kwargs: dict[str, Any] = {"prices": PLAIN, "evidence": _evidence(PLAIN), "window_end": SEP27, "half_spread": 0.0}
    with pytest.raises(SimulationError, match="month-end"):
        simulate_monthly(schedule=((JUL31, frozenset(PLAIN)),), policy=ZERO_RECOVERY, **kwargs)
    with pytest.raises(SimulationError, match="legacy"):
        simulate_monthly(schedule=((JUL1, frozenset(PLAIN)),), policy=TerminationPolicy("x", 0.0, None), **kwargs)
    with pytest.raises(SimulationError, match="strictly increasing"):
        simulate_monthly(schedule=(), policy=ZERO_RECOVERY, **kwargs)


def test_last_session_of_month_follows_the_nyse_calendar() -> None:
    assert last_session_of_month((2013, 8)) == AUG30  # Aug 31 is a Saturday
    assert last_session_of_month((2024, 3)) == date(2024, 3, 28)  # Good Friday Mar 29, weekend after
    assert last_session_of_month((2024, 8)) == date(2024, 8, 30)


def test_parity_catches_a_different_terminal_wealth() -> None:
    evidence = _evidence(PLAIN)
    monthly = simulate_monthly(
        schedule=((JUL1, frozenset(PLAIN)),),
        prices=PLAIN,
        policy=ZERO_RECOVERY,
        half_spread=HALF_SPREAD,
        evidence=evidence,
        window_end=SEP27,
    )
    annual = simulate_portfolio(
        schedule=((FORMATION[JUL1], frozenset(PLAIN)),),
        prices=PLAIN,
        policy=ZERO_RECOVERY,
        half_spread=0.0,
        window_end=SEP27,
        evidence=evidence,
    )
    with pytest.raises(ParityMismatch):
        check_parity(monthly, annual, window_end=SEP27)
