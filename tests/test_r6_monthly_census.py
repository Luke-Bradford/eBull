"""#2901 PR B part 2b: the descriptive censuses over ``simulate_monthly``'s cells and holding periods.

Spec: ``docs/proposals/ta/2026-09-25-2901-quality-declaration-spec.md`` ("Turnover", "Stale-mark census"). The
censuses never gate. Expected values are written out by hand from the fixtures; every run still goes through the
part-2a harness, so parity with ``simulate_portfolio`` and telescoping are asserted too.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from app.services.market_calendar import us_market_status
from app.services.r6_exclusion_trial import (
    HALF_SPREAD,
    PROGRAMME_POLICIES,
    ZERO_RECOVERY,
    PriceBar,
    PriceSeries,
    TerminationPolicy,
)
from app.services.r6_monthly_trial import (
    GapEpisode,
    HeldSeriesQuality,
    HoldingPeriod,
    Unavailable,
    stale_mark_census,
    turnover_census,
)
from tests.test_r6_monthly_simulator import (
    AUG30,
    CARRY,
    CARRY_SCHEDULE,
    CELLS,
    H_VALUES,
    JUL1,
    JUL31,
    LONE,
    OCT1,
    OCT25,
    SEP3,
    SEP27,
    UNKNOWN,
    _evidence,
    _run,
    _solve_two,
)

JUL_TO_SEP = ((2013, 7), (2013, 8), (2013, 9))


def _dense(
    symbol: str, last: date, *, missing: tuple[date, ...] = (), extra: tuple[date, ...] = (), invalid: int = 0
) -> PriceSeries:
    """A bar at 10.0 on every NYSE session from Jul 1 2013 to ``last``, except ``missing``, plus ``extra`` days."""
    days: list[date] = []
    day = JUL1
    while day <= last:
        if us_market_status(day) != "closed" and day not in missing:
            days.append(day)
        day += timedelta(days=1)
    return PriceSeries(symbol, tuple(PriceBar(d, 10.0, 10.0) for d in sorted({*days, *extra})), invalid)


#: HLT misses Jul 3 and Jul 5 around a bar dated on the Jul 4 full closure (not a resumption), then Jul 30 – Aug 1
#: across the July mark. LIV (alive at capture) stops after Oct 18 and misses the final. TRM's last bar is Aug 15.
DENSE = {
    "HLT": _dense(
        "HLT",
        OCT25,
        missing=(date(2013, 7, 3), date(2013, 7, 5), date(2013, 7, 30), JUL31, date(2013, 8, 1)),
        extra=(date(2013, 7, 4),),
        invalid=2,
    ),
    "LIV": _dense("LIV", date(2013, 10, 18), extra=(date(2024, 9, 26),)),
    "TRM": _dense("TRM", date(2013, 8, 15)),
}


@pytest.mark.parametrize("policy", PROGRAMME_POLICIES, ids=lambda p: p.label)
@pytest.mark.parametrize("h", H_VALUES)
def test_stale_mark_census_by_hand(policy: TerminationPolicy, h: float) -> None:
    result = _run(((JUL1, frozenset(DENSE)),), DENSE, policy, h, OCT25)
    census = stale_mark_census(result, prices=DENSE, evidence=_evidence(DENSE))
    f = policy.terminal_fraction(UNKNOWN)
    t = 1 / (3 * (1 + h))  # every position is worth t at a price of 10
    assert census.cells == {
        ("mark", "bar", "priced"): 6,
        ("mark", "gap", "held_stale"): 1,
        ("mark", "terminated", "recognised"): 1,
        ("final", "bar", "priced"): 1,
        ("final", "alive_at_capture", "sold_last_close"): 1,
    }
    aug = 2 * t + f * t * (1 - h)
    assert census.stale_share == pytest.approx({(2013, 7): 1 / 3, (2013, 8): 0.0, (2013, 9): 0.0}, rel=1e-12)
    assert result.marks[2].wealth == pytest.approx(aug, rel=1e-12)
    assert census.gaps == (
        GapEpisode("HLT", date(2013, 7, 3), 2, censored=False),
        GapEpisode("HLT", date(2013, 7, 30), 3, censored=False),
        GapEpisode("LIV", date(2013, 10, 21), 5, censored=True),
    )
    assert census.held_series == (
        HeldSeriesQuality("HLT", invalid_rows=2, closure_dated_bars=1),
        HeldSeriesQuality("LIV", invalid_rows=0, closure_dated_bars=0),
        HeldSeriesQuality("TRM", invalid_rows=0, closure_dated_bars=0),
    )
    assert census.closure_dated_valuations == 0

    turnover = turnover_census(result, JUL_TO_SEP)
    # TRM is recognised at the August mark: a sale of R = f·t; the pre-loss value removed is t.
    assert turnover.turnover == pytest.approx({(2013, 7): 0.0, (2013, 8): f / 6, (2013, 9): 0.0}, abs=1e-15)
    assert turnover.mean_turnover == pytest.approx(f / 18, abs=1e-15)
    assert turnover.surviving_months == JUL_TO_SEP
    assert turnover.forced_exits == pytest.approx({(2013, 7): 0.0, (2013, 8): 1 / 3, (2013, 9): 0.0}, rel=1e-12)
    # The partial month: the final sale of HLT (priced) and LIV (alive at capture, last close).
    assert turnover.partial_traded == pytest.approx(2 * t, rel=1e-12)
    assert turnover.partial_forced_exits == 0.0
    assert turnover.window_traded == pytest.approx(3 * t + f * t + 2 * t, rel=1e-12)


@pytest.mark.parametrize("policy", PROGRAMME_POLICIES, ids=lambda p: p.label)
@pytest.mark.parametrize("h", H_VALUES)
def test_a_closure_dated_last_close_is_counted(policy: TerminationPolicy, h: float) -> None:
    result = _run(((JUL1, frozenset(CELLS)),), CELLS, policy, h, SEP27)
    census = stale_mark_census(result, prices=CELLS, evidence=_evidence(CELLS))
    assert census.closure_dated_valuations == 1  # CLO, recognised at the July mark off its Jul 4 close
    assert census.cells == {
        ("mark", "bar", "priced"): 5,
        ("mark", "gap", "held_stale"): 2,
        ("mark", "terminated", "recognised"): 2,
        ("final", "bar", "priced"): 1,
        ("final", "alive_at_capture", "sold_last_close"): 1,
        ("final", "terminated", "recognised"): 1,
    }
    assert sum(census.cells.values()) == 5 + 4 + 3  # held symbols at Jul 31, Aug 30 and the final
    assert {period.symbol: (period.start, period.end) for period in result.holding_periods} == {
        "CLO": (JUL1, JUL31),
        "TRM": (JUL1, AUG30),
        "FIN": (JUL1, SEP27),
        "GAP": (JUL1, SEP27),
        "LIV": (JUL1, SEP27),
    }


@pytest.mark.parametrize("policy", PROGRAMME_POLICIES, ids=lambda p: p.label)
@pytest.mark.parametrize("h", H_VALUES)
def test_turnover_counts_rebalances_and_mark_recognitions_once(policy: TerminationPolicy, h: float) -> None:
    result = _run(CARRY_SCHEDULE, CARRY, policy, h, OCT25)
    f, g = policy.terminal_fraction(UNKNOWN), policy.gap_fraction
    s = 1 / (4 * (1 + h)) / 10
    recognised = s * 4 * f * (1 - h)
    aug = recognised + s * (10 + 5 + 10)
    current_aaa, rbl, gap = s * 10, s * 5 * f, s * 10 * g
    t2 = (recognised + current_aaa + rbl + gap + h * (current_aaa - rbl - gap)) / (1 + h)
    t3 = _solve_two(t2, h)
    sep3_traded = (t2 - current_aaa) + rbl + gap  # RBL's recognition is inside the rebalance's trades
    census = turnover_census(result, JUL_TO_SEP)
    assert census.turnover == pytest.approx(
        {(2013, 7): s * 4 * f / 2, (2013, 8): 0.0, (2013, 9): sep3_traded / 2 / aug}, rel=1e-9, abs=1e-15
    )
    assert census.forced_exits == pytest.approx(
        {(2013, 7): s * 4, (2013, 8): 0.0, (2013, 9): s * 5 / aug}, rel=1e-9, abs=1e-15
    )
    # Oct 1 (after the last mark) sells AAA down to t3 and buys GAP; then the final sale of both.
    assert census.partial_traded == pytest.approx(t2 + 2 * t3, rel=1e-9)
    assert census.window_traded == pytest.approx(4 * s * 10 + s * 4 * f + sep3_traded + t2 + 2 * t3, rel=1e-9)

    cells = {(cell.session, cell.symbol): (cell.kind, cell.status, cell.action) for cell in result.cells}
    assert cells[(SEP3, "AAA")] == ("rebalance", "bar", "priced")
    assert cells[(SEP3, "RBL")] == ("rebalance", "terminated", "recognised")
    assert cells[(SEP3, "GAP")] == ("rebalance", "gap", "bounded_sold")
    assert cells[(OCT1, "AAA")] == ("rebalance", "bar", "priced")
    assert (OCT1, "GAP") not in cells  # bought at Oct 1, not held into it
    assert sorted(result.holding_periods, key=lambda p: (p.symbol, p.start)) == [
        HoldingPeriod("AAA", JUL1, OCT25),
        HoldingPeriod("GAP", JUL1, SEP3),
        HoldingPeriod("GAP", OCT1, OCT25),  # a gap-sold symbol bought again
        HoldingPeriod("RBL", JUL1, SEP3),
        HoldingPeriod("STP", JUL1, JUL31),
    ]


@pytest.mark.parametrize("h", H_VALUES)
def test_ratios_after_a_ruin_are_unavailable(h: float) -> None:
    result = _run(((JUL1, frozenset(LONE)),), LONE, ZERO_RECOVERY, h, SEP27)
    months = ((2013, 7), (2013, 8))
    census = turnover_census(result, months)
    assert census.turnover[(2013, 7)] == 0.0  # zero recovery: the recognition trades nothing
    assert isinstance(census.turnover[(2013, 8)], Unavailable)
    assert isinstance(census.mean_turnover, Unavailable)
    assert census.surviving_months == ((2013, 7),) and census.surviving_mean_turnover == 0.0
    assert census.forced_exits[(2013, 7)] == pytest.approx(4 / (10 * (1 + h)), rel=1e-12)
    assert isinstance(census.forced_exits[(2013, 8)], Unavailable)
    assert isinstance(census.partial_forced_exits, Unavailable)
    stale = stale_mark_census(result, prices=LONE, evidence=_evidence(LONE))
    assert isinstance(stale.stale_share[(2013, 8)], Unavailable)


@pytest.mark.parametrize("h", H_VALUES)
def test_a_ruined_book_has_no_cells_after_the_ruin(h: float) -> None:
    prices = {**LONE, "AAA": CARRY["AAA"]}
    result = _run(((JUL1, frozenset(LONE)), (SEP3, frozenset({"AAA"}))), prices, ZERO_RECOVERY, h, OCT25)
    assert [(cell.session, cell.symbol, cell.action) for cell in result.cells] == [(JUL31, "ONE", "recognised")]
    assert result.holding_periods == (HoldingPeriod("ONE", JUL1, JUL31),)


def test_turnover_refuses_a_result_without_the_census_months() -> None:
    result = _run(((JUL1, frozenset(LONE)),), LONE, ZERO_RECOVERY, HALF_SPREAD, SEP27)
    with pytest.raises(ValueError, match="census months"):
        turnover_census(result, ((2013, 7), (2013, 9)))
