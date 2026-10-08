"""#3621 slice 2a: the populations, target sets, reference books and the frozen pair verdict.

Spec: ``docs/research/2026-10-08-3621-avoidance-filters.md`` §"The books" and §"Decision rule" (PR #3724). Pure
functions over step 2's :class:`~scripts.report_3609_step2.PanelMonth` and the slice 1 flags
(:mod:`app.services.avoidance_filters`). The printed diagnostics are slice 2b.

* **Populations** at each formation: JKP's NYSE size segments (micro ME < p20 <= small < p50 <= large < p80 <=
  mega, the cutoffs in USD), step 2's book universe (:func:`~app.services.factor_book.universe`, the top 1,000 by ME,
  ties by ``name_key``), ``rest`` (every other admitted name) and ``all`` (the pooled diagnostic, no verdict).
* **Target sets:** U(P, M) is every admitted name in P; U_F removes every name filter set F flags; B_F is what it
  removed. None of step 2's entry or retention rules applies.
* **Books:** step 2's equal-weight reference (:func:`~app.services.factor_book_references.reference_decisions`) with
  the target set as the formation's universe, valued by :func:`~app.services.factor_book_path.value_path` in both
  termination arms at base and stress cost. An empty target sells everything and holds cash.
* **Windows** are slices of the one path by return month, as ``value_path`` books them: formation M's trades fall in
  month M's return. So the whole path is 2014-10..2024-08 and stage B is 2021-06..2024-08; the boundary formation's
  (2021-05) trading cost stays in stage A. This differs from step 2's G2, which folded that cost into stage B's first
  month (``report_3609_step2_verdict.stage_b_returns``): this spec allocates every window's costs as ``value_path``
  books them (§"The books", §"Diagnostics").
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from typing import Final

from app.services.avoidance_filters import Filter, NameFlags
from app.services.factor_book import bands, universe
from app.services.factor_book_path import MIN_HOLDINGS, Formation, Month, PathResult, month_of, value_path
from app.services.factor_book_references import reference_decisions
from app.services.factor_book_series import ARMS
from app.services.strategy_result import AmbiguityArm
from scripts.report_3609_baselines import COSTS
from scripts.report_3609_step2 import PanelMonth
from scripts.report_3609_step2_verdict import BASE, STAGE_B, STAGE_B_FORMATIONS, STRESS, log_growth

#: §"Decision rule": the cost scenarios the verdict reads.
SCENARIOS: Final = (BASE, STRESS)
#: The margin on every ΔG condition (§"Decision rule": "The margin is zero").
DELTA_G_MARGIN: Final = 0.0


class Population(StrEnum):
    MICRO = "micro"
    SMALL = "small"
    LARGE = "large"
    MEGA = "mega"
    TOP1000 = "top1000"
    REST = "rest"
    ALL = "all"


#: The six populations with verdicts; ``all`` is the pooled diagnostic.
VERDICT_POPULATIONS: Final = tuple(p for p in Population if p is not Population.ALL)


class PairVerdict(StrEnum):
    ELIGIBLE = "ELIGIBLE"
    NOT_ELIGIBLE = "NOT ELIGIBLE"
    NO_EFFECT = "NO_EFFECT"
    REFUSED = "REFUSED"


def populations(me: Mapping[int, float], cutoffs_usd: tuple[float, float, float]) -> dict[Population, frozenset[int]]:
    """Every population's admitted names at one formation; ``cutoffs_usd`` is (p20, p50, p80) in USD."""
    p20, p50, p80 = cutoffs_usd
    if not (all(math.isfinite(c) for c in cutoffs_usd) and 0 < p20 <= p50 <= p80):
        raise ValueError(f"NYSE cutoffs must be finite, positive and ascending: {cutoffs_usd}")
    out: dict[Population, set[int]] = {p: set() for p in Population}
    for name, value in me.items():
        if not (math.isfinite(value) and value > 0):
            raise ValueError(f"ME not finite and positive for name {name}: {value!r}")
        segment = (
            Population.MICRO
            if value < p20
            else Population.SMALL
            if value < p50
            else Population.LARGE
            if value < p80
            else Population.MEGA
        )
        out[segment].add(name)
    top = frozenset(universe(me))
    out[Population.TOP1000] = set(top)
    out[Population.REST] = set(me.keys() - top)
    out[Population.ALL] = set(me)
    return {p: frozenset(names) for p, names in out.items()}


@dataclass(frozen=True)
class Targets:
    unfiltered: frozenset[int]
    filtered: frozenset[int]
    flagged: frozenset[int]


def targets(population: frozenset[int], flags: Mapping[int, NameFlags], filter_set: frozenset[Filter]) -> Targets:
    """U, U_F and B_F of one population at one formation."""
    missing = sorted(population - flags.keys())
    if missing:
        raise ValueError(f"{len(missing)} population names have no flags: {missing[:5]}")
    flagged = frozenset(n for n in population if flags[n].removed_by(filter_set))
    return Targets(population, population - flagged, flagged)


def book_paths(
    months: Sequence[PanelMonth], target_sets: Sequence[frozenset[int]]
) -> dict[tuple[AmbiguityArm, str], PathResult]:
    """One equal-weight book holding ``target_sets[i]`` after formation i's trades, per arm and verdict scenario."""
    if len(months) != len(target_sets):
        raise ValueError(f"{len(target_sets)} target sets for {len(months)} formations")
    formations = []
    for month, target in zip(months, target_sets, strict=True):
        outside = sorted(target - month.admitted.keys())
        if outside:
            raise ValueError(f"{month.formation}: targets not admitted: {outside[:5]}")
        formations.append(
            Formation(
                formation=month.formation,
                session=month.session,
                universe=target,
                bands=bands({}),
                close=month.close,
                first_bar=month.first_bar,
                returns={name: month.admitted[name].holding for name in target},
            )
        )
    decisions = reference_decisions(formations)
    return {
        (arm, cost): value_path(decisions, arm=arm, cost_multiplier=COSTS[cost]) for arm in ARMS for cost in SCENARIOS
    }


def _window(returns: Mapping[Month, float], first: Month | None = None, last: Month | None = None) -> list[Month]:
    return [m for m in sorted(returns) if (first is None or m >= first) and (last is None or m <= last)]


@dataclass(frozen=True)
class Exhaustion:
    book: str
    arm: AmbiguityArm
    cost: str
    month: Month


@dataclass(frozen=True)
class PairResult:
    verdict: PairVerdict
    #: The failed conditions of a NOT ELIGIBLE pair, in the spec's order.
    failed: tuple[str, ...] = ()
    #: Every book, arm and scenario of a REFUSED pair that reached non-positive wealth.
    exhausted: tuple[Exhaustion, ...] = ()
    #: ΔG by (window, arm, cost): ``whole`` at base and stress, ``stage B`` at base.
    delta_g: Mapping[tuple[str, AmbiguityArm, str], float] = field(default_factory=dict)
    #: Formations (path keys) at which F flagged a name in U.
    exclusion_formations: tuple[Month, ...] = ()

    @property
    def no_stage_b_exclusions(self) -> bool:
        first, last = STAGE_B_FORMATIONS
        return not any(first <= m <= last for m in self.exclusion_formations)


def pair_result(
    unfiltered: Mapping[tuple[AmbiguityArm, str], PathResult],
    filtered: Mapping[tuple[AmbiguityArm, str], PathResult],
    excluded: Mapping[date, int],
    *,
    has_max: bool,
    max_fidelity_passed: bool | None,
) -> PairResult:
    """§"Decision rule" for one (F, P): ``excluded`` maps each formation to |B_F|."""
    if has_max and max_fidelity_passed is None:
        raise ValueError("a set containing MAX needs the MAX fidelity verdict")
    exclusions = tuple(sorted(month_of(f) for f, n in excluded.items() if n > 0))
    exhausted = tuple(
        Exhaustion(book, arm, cost, path.nonpositive)
        for book, paths in (("U", unfiltered), ("U_F", filtered))
        for (arm, cost), path in sorted(paths.items())
        if path.nonpositive is not None
    )
    if exhausted:
        return PairResult(PairVerdict.REFUSED, exhausted=exhausted, exclusion_formations=exclusions)
    if not exclusions:
        return PairResult(PairVerdict.NO_EFFECT)

    delta: dict[tuple[str, AmbiguityArm, str], float] = {}
    for arm in ARMS:
        for cost in SCENARIOS:
            u, f = unfiltered[(arm, cost)].returns, filtered[(arm, cost)].returns
            whole = _window(u)
            if whole != _window(f):
                raise ValueError(f"U and U_F do not share their return months ({arm}, {cost})")
            label = f"{arm} {cost}"
            delta[("whole", arm, cost)] = log_growth(f, whole, f"U_F {label}") - log_growth(u, whole, f"U {label}")
        u, f = unfiltered[(arm, BASE)].returns, filtered[(arm, BASE)].returns
        stage_b = _window(u, *STAGE_B)
        delta[("stage B", arm, BASE)] = log_growth(f, stage_b, f"U_F stage B {arm}") - log_growth(
            u, stage_b, f"U stage B {arm}"
        )

    failed = []
    if any(delta[("whole", arm, cost)] < DELTA_G_MARGIN for arm in ARMS for cost in SCENARIOS):
        failed.append("1 whole path")
    if any(delta[("stage B", arm, BASE)] < DELTA_G_MARGIN for arm in ARMS):
        failed.append("2 stage B")
    if min(filtered[(ARMS[0], BASE)].holdings.values()) < MIN_HOLDINGS:
        failed.append("3 holdings count")
    if has_max and not max_fidelity_passed:
        failed.append("4 MAX fidelity")
    verdict = PairVerdict.NOT_ELIGIBLE if failed else PairVerdict.ELIGIBLE
    return PairResult(verdict, tuple(failed), delta_g=delta, exclusion_formations=exclusions)
