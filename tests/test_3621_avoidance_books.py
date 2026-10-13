"""#3621 slice 2a: populations, target sets, reference books and the pair verdict, on synthetic inputs only."""

from __future__ import annotations

import math
from datetime import date

import pytest

from app.services.avoidance_filters import FILTER_SETS, Filter, MaxReading, NameFlags
from app.services.factor_book import BookRefusal
from app.services.factor_book_path import HoldingReturn, Month, PathResult
from app.services.factor_book_series import ARMS
from app.services.factor_panel_prices import HoldingStatus
from app.services.strategy_result import AmbiguityArm
from scripts.report_3609_step2 import PanelMonth, PanelName
from scripts.report_3621_books import (
    PairResult,
    PairVerdict,
    Population,
    book_paths,
    pair_result,
    populations,
    targets,
)

CUTOFFS = (100.0, 200.0, 300.0)
MAX, SUB5, YOUNG, SUB5_YOUNG, ALL3 = FILTER_SETS


def test_populations_partition_by_nyse_cutoffs_and_split_top_1000_from_rest() -> None:
    me = {i: 1000.0 + i for i in range(1005)}
    me.update({2000: 99.0, 2001: 100.0, 2002: 200.0, 2003: 300.0})
    got = populations(me, CUTOFFS)
    assert got[Population.MICRO] == {2000}
    assert got[Population.SMALL] == {2001}
    assert got[Population.LARGE] == {2002}
    assert got[Population.MEGA] == set(range(1005)) | {2003}
    assert got[Population.TOP1000] == set(range(5, 1005))
    assert got[Population.REST] == {0, 1, 2, 3, 4, 2000, 2001, 2002, 2003}
    assert got[Population.ALL] == set(me)


def test_top_1000_ties_break_by_name_key_and_bad_inputs_refuse() -> None:
    me = dict.fromkeys(range(1001), 500.0)
    assert populations(me, CUTOFFS)[Population.REST] == {1000}
    with pytest.raises(BookRefusal, match="UNIVERSE_SHORT"):
        populations(dict.fromkeys(range(999), 500.0), CUTOFFS)
    with pytest.raises(ValueError, match="ascending"):
        populations(me, (200.0, 100.0, 300.0))
    with pytest.raises(ValueError, match="ME not finite"):
        populations({**me, 5000: math.nan}, CUTOFFS)


def _flags(flagged: dict[int, set[Filter]]) -> dict[int, NameFlags]:
    reading = MaxReading(0.01, 20, 0, None)
    return {n: NameFlags(reading, frozenset(f)) for n, f in flagged.items()}


def test_targets_remove_any_member_of_the_set_and_report_what_they_removed() -> None:
    flags = _flags({1: set(), 2: {Filter.MAX}, 3: {Filter.SUB5}, 4: {Filter.YOUNG, Filter.SUB5}})
    population = frozenset({1, 2, 3, 4})
    got = targets(population, flags, SUB5_YOUNG)
    assert (got.unfiltered, got.filtered, got.flagged) == (population, {1, 2}, {3, 4})
    assert targets(population, flags, MAX).flagged == {2}
    with pytest.raises(ValueError, match="no flags"):
        targets(frozenset({9}), flags, MAX)


# --------------------------------------------------------------------------- the pair verdict

#: Formations 2021-03..2021-08 put return months 2021-04..2021-09 on both sides of stage B's start (2021-06).
FORMATIONS = [date(2021, m, 28) for m in range(3, 9)]
MONTHS: list[Month] = [(2021, m) for m in range(4, 10)]
PRE_B, IN_B = MONTHS[:2], MONTHS[2:]


def _path(returns: dict[Month, float], holdings: int = 20, nonpositive: Month | None = None) -> PathResult:
    return PathResult(
        returns=dict(returns),
        holdings={(f.year, f.month): holdings for f in FORMATIONS},
        nonpositive=nonpositive,
    )


Paths = dict[tuple[AmbiguityArm, str], PathResult]


def _paths(
    by: dict[Month, float],
    *,
    stress: dict[Month, float] | None = None,
    holdings: int = 20,
    nonpositive: Month | None = None,
) -> Paths:
    out: Paths = {}
    for arm in ARMS:
        out[(arm, "net")] = _path(by, holdings, nonpositive)
        out[(arm, "stress_2x")] = _path(stress or by, holdings, nonpositive)
    return out


FLAT = dict.fromkeys(MONTHS, 0.01)
EVERY = {f: 1 for f in FORMATIONS}


def _verdict(
    u: Paths, f: Paths, excluded: dict[date, int] = EVERY, *, has_max: bool = False, fidelity: bool | None = None
) -> PairResult:
    return pair_result(u, f, excluded, has_max=has_max, max_fidelity_passed=fidelity)


def test_eligible_when_every_delta_g_is_at_least_zero() -> None:
    better = {m: 0.02 for m in MONTHS}
    got = _verdict(_paths(FLAT), _paths(better))
    assert got.verdict is PairVerdict.ELIGIBLE and got.failed == ()
    assert len(got.delta_g) == 6
    assert got.delta_g[("whole", "best_case", "net")] == pytest.approx(12 * (math.log(1.02) - math.log(1.01)))
    assert _verdict(_paths(FLAT), _paths(FLAT)).verdict is PairVerdict.ELIGIBLE  # the margin is zero


def test_stress_cost_alone_can_fail_the_whole_path() -> None:
    worse_at_stress = {m: 0.005 for m in MONTHS}
    got = _verdict(_paths(FLAT), _paths(FLAT, stress=worse_at_stress))
    assert (got.verdict, got.failed) == (PairVerdict.NOT_ELIGIBLE, ("1 whole path",))


def test_stage_b_fails_on_its_own_months_even_when_the_whole_path_passes() -> None:
    filtered = {**{m: 0.10 for m in PRE_B}, **{m: 0.005 for m in IN_B}}
    got = _verdict(_paths(FLAT), _paths(filtered))
    assert got.delta_g[("whole", "worst_case", "stress_2x")] > 0
    assert (got.verdict, got.failed) == (PairVerdict.NOT_ELIGIBLE, ("2 stage B",))


def test_boundary_formation_cost_stays_in_stage_a() -> None:
    """Formation 2021-05's trades are booked in return month 2021-05, so a cost there cannot fail stage B."""
    unfiltered = {**FLAT, (2021, 5): 0.01}
    filtered = {**FLAT, (2021, 5): -0.05}
    got = _verdict(_paths(unfiltered), _paths(filtered))
    assert got.delta_g[("stage B", "best_case", "net")] == 0.0
    assert got.failed == ("1 whole path",)


def test_holdings_gate_and_max_fidelity_are_named() -> None:
    got = _verdict(_paths(FLAT), _paths(FLAT, holdings=9), has_max=True, fidelity=False)
    assert got.failed == ("3 holdings count", "4 MAX fidelity")
    assert (
        _verdict(_paths(FLAT), _paths(FLAT, holdings=10), has_max=True, fidelity=True).verdict is PairVerdict.ELIGIBLE
    )
    with pytest.raises(ValueError, match="fidelity verdict"):
        _verdict(_paths(FLAT), _paths(FLAT), has_max=True)


def test_no_effect_refused_and_the_stage_b_label() -> None:
    none = dict.fromkeys(FORMATIONS, 0)
    assert _verdict(_paths(FLAT), _paths(FLAT), none).verdict is PairVerdict.NO_EFFECT
    exhausted = _verdict(_paths(FLAT), _paths(FLAT, nonpositive=(2021, 7)), none)
    assert exhausted.verdict is PairVerdict.REFUSED
    assert {(e.book, e.month) for e in exhausted.exhausted} == {("U_F", (2021, 7))}
    assert len(exhausted.exhausted) == 4
    early_only = {f: int(f.month < 5) for f in FORMATIONS}
    assert _verdict(_paths(FLAT), _paths(FLAT), early_only).no_stage_b_exclusions
    assert not _verdict(_paths(FLAT), _paths(FLAT), {**early_only, date(2021, 5, 28): 1}).no_stage_b_exclusions


def test_mismatched_return_months_refuse() -> None:
    with pytest.raises(ValueError, match="share their return months"):
        _verdict(_paths(FLAT), _paths({m: 0.01 for m in MONTHS[1:]}))


# --------------------------------------------------------------------------- books from panel months


def _month(formation: date, returns: dict[int, float]) -> PanelMonth:
    def name(n: int, r: float) -> PanelName:
        return PanelName(n, 1000.0, "OTHER", {}, HoldingReturn(HoldingStatus.OBSERVED, dict.fromkeys(ARMS, r)), None)

    return PanelMonth(
        formation=formation,
        session=formation,
        admitted={n: name(n, r) for n, r in returns.items()},
        close=dict.fromkeys(returns, 20.0),
        first_bar=dict.fromkeys(returns, date(2010, 1, 1)),
    )


def test_books_end_to_end_excluding_a_loser_lifts_g_and_an_empty_target_holds_cash() -> None:
    names = range(12)
    months = [_month(f, {**dict.fromkeys(names, 0.01), 0: -0.5}) for f in FORMATIONS]
    flags = _flags({n: {Filter.MAX} if n == 0 else set() for n in names})
    sets = [targets(frozenset(names), flags, MAX) for _ in months]
    u = book_paths(months, [t.unfiltered for t in sets])
    f = book_paths(months, [t.filtered for t in sets])
    assert set(u) == {(arm, cost) for arm in ARMS for cost in ("net", "stress_2x")}
    got = pair_result(
        u,
        f,
        {m.formation: len(t.flagged) for m, t in zip(months, sets, strict=True)},
        has_max=True,
        max_fidelity_passed=True,
    )
    assert got.verdict is PairVerdict.ELIGIBLE
    assert got.delta_g[("whole", "worst_case", "stress_2x")] > 0

    every = frozenset(names)
    path = book_paths(months, [every, frozenset(), frozenset(), every, every, every])[("best_case", "net")]
    assert path.holdings[(2021, 4)] == path.holdings[(2021, 5)] == 0
    assert path.returns[(2021, 4)] < 0  # sells at s(2021-04): month 2021-04 carries the cost
    assert path.returns[(2021, 5)] == 0.0  # cash all of May and no trade at its closing formation
    assert path.returns[(2021, 6)] < 0  # cash all of June, re-entry cost at s(2021-06)
