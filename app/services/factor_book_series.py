"""The #3609 step 2 series: every compared path, valued under each termination arm and cost scenario.

Spec: ``docs/research/2026-10-06-3609-step2-factor-book.md`` §"Dates, samples and the hold-out" (the stage boundary
state), §"References and the control", §"Decision rule" (completeness, non-positive wealth and its precedence) and
§"Registration" (canonical JSON) (PR #3666). Pure functions over the report's :class:`Formation` inputs.

**Order.** Selections are computed first, once, because none depends on the arm or the cost scenario: the book's
decisions and the two references', then each control draw's. A selection refusal (``PRICE_INVALID``,
``CONTROL_SHORT``, ``ME_INVALID``) raises, then B1's ``COMPARATOR_INVALID``, then non-positive wealth, then
completeness. Each control draw is valued as soon as it is selected and summarised (returns, turnover, category
totals, boundary state), so 1,000 draws' decisions and trade lists are never held at once. That interleaving cannot
change which refusal is reported: valuation raises no data refusal, because every name it trades is a universe name
or a holding whose raw close selection already validated (``check_closes``), and wealth is judged only after
everything is valued. The book's and the references' full paths are kept for the diagnostics.

**Non-positive wealth** (§"Decision rule"). Each valued path stops at its own first non-positive month. The run's
stopping month is the earliest of those over every series, draw, arm and cost scenario; :func:`run_series` refuses
``WEALTH_NONPOSITIVE`` with every series that stopped in it, and returns nothing, so no month after the stopping
month is ever read by a statistic, gate or diagnostic.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any, Final, get_args

from app.services.factor_book import BookRefusal
from app.services.factor_book_control import DRAWS, Pool, control_decisions
from app.services.factor_book_path import (
    BoundaryState,
    Decision,
    Formation,
    Month,
    PathResult,
    TradeCategory,
    book_decisions,
    month_of,
    next_month,
    value_path,
)
from app.services.factor_book_references import B1Path, b1_path, reference_decisions
from app.services.strategy_result import AmbiguityArm

ARMS: Final[tuple[AmbiguityArm, ...]] = tuple(sorted(get_args(AmbiguityArm)))
#: The valued series other than B1, as named in the boundary payload and in refusals.
BOOK: Final = "book"
CONTROL: Final = "control"
EQUAL_WEIGHT: Final = "equal_weight"
CAP_WEIGHTED: Final = "cap_weighted"
B1: Final = "b1"

#: (termination arm, cost scenario name).
Scenario = tuple[AmbiguityArm, str]


@dataclass(frozen=True)
class Summary:
    """One valued path without its trade list: what the verdict and the control's diagnostics read."""

    returns: Mapping[Month, float]
    turnover: Mapping[Month, float]
    #: NAV after each month's trades (the stage-B slice starts from the boundary's pre-trade NAV).
    nav: Mapping[Month, float]
    #: Order notional and charged cost per trade category.
    categories: Mapping[TradeCategory, tuple[float, float]]
    nonpositive: Month | None
    boundary: BoundaryState | None


def summarise(path: PathResult) -> Summary:
    notional: dict[TradeCategory, list[float]] = {}
    cost: dict[TradeCategory, list[float]] = {}
    for trade in path.trades:
        notional.setdefault(trade.category, []).append(trade.notional)
        cost.setdefault(trade.category, []).append(trade.cost)
    categories = {c: (math.fsum(notional[c]), math.fsum(cost[c])) for c in notional}
    return Summary(dict(path.returns), dict(path.turnover), dict(path.nav), categories, path.nonpositive, path.boundary)


@dataclass(frozen=True)
class SeriesRun:
    #: The declared holding months, first to last.
    months: tuple[Month, ...]
    book: Mapping[Scenario, PathResult]
    equal_weight: Mapping[Scenario, PathResult]
    cap_weighted: Mapping[Scenario, PathResult]
    #: Per scenario, one summary per draw, indexed by draw.
    control: Mapping[Scenario, tuple[Summary, ...]]
    #: Per cost scenario.
    b1: Mapping[str, B1Path]
    #: Per formation, the smallest purchase pool any draw met and the purchases that draw needed (§"Shortage
    #: refuses"); the first such draw on a tie.
    pools: tuple[Pool, ...]
    #: The book's formations with fewer than ``MIN_HOLDINGS`` holdings (§"Insufficient book").
    insufficient: tuple[date, ...]


def holding_months(formations: Sequence[Formation]) -> tuple[Month, ...]:
    first, last = next_month(month_of(formations[0].formation)), next_month(month_of(formations[-1].formation))
    months = [first]
    while months[-1] < last:
        months.append(next_month(months[-1]))
    return tuple(months)


def _smallest_pools(draws: Sequence[Sequence[Pool]]) -> tuple[Pool, ...]:
    return tuple(min(column, key=lambda pool: pool.pool) for column in zip(*draws, strict=True))


def run_series(
    formations: Sequence[Formation],
    me: Sequence[Mapping[int, float]],
    *,
    b1_saved: Mapping[str, Sequence[object]],
    b1_close: float,
    costs: Mapping[str, float],
    draws: int = DRAWS,
    boundary: date | None = None,
) -> SeriesRun:
    """Every series over ``formations`` (consecutive months), in the module's order; refuses as it states.

    ``me`` is one ME map per formation (the cap-weighted reference); ``b1_saved`` is step 0's ``"B1 SPY"`` path and
    ``b1_close`` SPY's raw close at the first formation's session; ``costs`` is step 0's scenario map; ``boundary``
    names the formation whose pre-trade state every valued path captures."""
    if not formations:
        raise ValueError("a run needs at least one formation")
    months = holding_months(formations)
    book = book_decisions(formations)
    equal_weight = reference_decisions(formations)
    cap_weighted = reference_decisions(formations, me)
    scenarios: list[Scenario] = [(arm, name) for arm in ARMS for name in costs]

    def value(decisions: Sequence[Decision], scenario: Scenario) -> PathResult:
        arm, name = scenario
        return value_path(decisions, arm=arm, cost_multiplier=costs[name], boundary=boundary)

    control: dict[Scenario, list[Summary]] = {s: [] for s in scenarios}
    pools: list[tuple[Pool, ...]] = []
    for d in range(draws):
        draw = control_decisions(formations, book, d)
        pools.append(draw.pools)
        for s in scenarios:
            control[s].append(summarise(value(draw.decisions, s)))
    b1 = {
        name: b1_path(b1_saved, first=months[0], last=months[-1], close=b1_close, cost_multiplier=k)
        for name, k in costs.items()
    }
    run = SeriesRun(
        months=months,
        book={s: value(book.decisions, s) for s in scenarios},
        equal_weight={s: value(equal_weight, s) for s in scenarios},
        cap_weighted={s: value(cap_weighted, s) for s in scenarios},
        control={s: tuple(summaries) for s, summaries in control.items()},
        b1=b1,
        pools=_smallest_pools(pools) if pools else (),
        insufficient=book.insufficient,
    )
    refusals = wealth_refusals(run)
    if refusals:
        raise WealthNonpositive(refusals)
    check_complete(run)
    return run


# --------------------------------------------------------------------------- refusals


@dataclass(frozen=True, order=True)
class WealthRefusal:
    """One series whose wealth factor was not finite and positive in the run's stopping month."""

    month: Month
    series: str
    #: ``None`` for B1, whose returns do not depend on the arm.
    arm: str | None
    cost: str
    draw: int | None = None


class WealthNonpositive(BookRefusal):
    """§"Decision rule": the run stopped; ``refusals`` holds every series that stopped in the stopping month."""

    def __init__(self, refusals: tuple[WealthRefusal, ...]) -> None:
        super().__init__("WEALTH_NONPOSITIVE", f"{len(refusals)} series stopped in {refusals[0].month}")
        self.refusals = refusals


def _b1_nonpositive(path: B1Path) -> Month | None:
    for month in sorted(path.returns):
        factor = 1.0 + path.returns[month]
        if not (math.isfinite(factor) and factor > 0):
            return month
    return None


def _stops(run: SeriesRun) -> list[WealthRefusal]:
    found: list[WealthRefusal] = []
    for series, paths in ((BOOK, run.book), (EQUAL_WEIGHT, run.equal_weight), (CAP_WEIGHTED, run.cap_weighted)):
        for (arm, cost), path in paths.items():
            if path.nonpositive is not None:
                found.append(WealthRefusal(path.nonpositive, series, arm, cost))
    for (arm, cost), summaries in run.control.items():
        for draw, summary in enumerate(summaries):
            if summary.nonpositive is not None:
                found.append(WealthRefusal(summary.nonpositive, CONTROL, arm, cost, draw))
    for cost, b1 in run.b1.items():
        if (month := _b1_nonpositive(b1)) is not None:
            found.append(WealthRefusal(month, B1, None, cost))
    return found


def wealth_refusals(run: SeriesRun) -> tuple[WealthRefusal, ...]:
    """§"Decision rule", non-positive wealth: every series that stopped in the run's stopping month, sorted.

    Empty when no series stopped. No series stops before the stopping month, by its definition."""
    found = _stops(run)
    if not found:
        return ()
    stop = min(refusal.month for refusal in found)
    return tuple(sorted(refusal for refusal in found if refusal.month == stop))


def check_complete(run: SeriesRun) -> None:
    """§"Decision rule": every compared series has exactly one finite return for each declared month.

    Run after :func:`wealth_refusals` is empty (a stopped path is incomplete by design). Refuses
    ``COMPARATOR_INVALID`` with the first incomplete series."""
    declared = set(run.months)
    series: list[tuple[str, Mapping[Month, float]]] = []
    for name, paths in ((BOOK, run.book), (EQUAL_WEIGHT, run.equal_weight), (CAP_WEIGHTED, run.cap_weighted)):
        series += [(f"{name} {arm} {cost}", path.returns) for (arm, cost), path in paths.items()]
    for (arm, cost), summaries in run.control.items():
        series += [(f"control draw {d} {arm} {cost}", s.returns) for d, s in enumerate(summaries)]
    series += [(f"b1 {cost}", path.returns) for cost, path in run.b1.items()]
    for label, returns in series:
        if returns.keys() != declared or not all(math.isfinite(r) for r in returns.values()):
            raise BookRefusal("COMPARATOR_INVALID", f"{label} lacks one finite return for each declared month")


# --------------------------------------------------------------------------- the boundary payload


def canonical_json(obj: object) -> bytes:
    """§"Registration", canonical JSON: sorted keys, compact separators, ASCII, no NaN; floats by shortest
    round-trip ``repr``."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("utf-8")


def _record(state: BoundaryState | None, label: str) -> dict[str, Any]:
    if state is None:
        raise ValueError(f"{label} captured no boundary state")
    return state.record()


def boundary_payload(run: SeriesRun) -> bytes:
    """§"Dates, samples and the hold-out": the stage boundary state of every valued path, as canonical JSON.

    Keyed series → arm → cost scenario; the control's draws are a list sorted by draw, each record's positions by
    ``name_key``. B1 holds no positions and is not included. A path without a captured state is a caller error."""
    payload: dict[str, Any] = {}
    for series, paths in ((BOOK, run.book), (EQUAL_WEIGHT, run.equal_weight), (CAP_WEIGHTED, run.cap_weighted)):
        for (arm, cost), path in paths.items():
            payload.setdefault(series, {}).setdefault(arm, {})[cost] = _record(path.boundary, f"{series} {arm} {cost}")
    for (arm, cost), summaries in run.control.items():
        payload.setdefault(CONTROL, {}).setdefault(arm, {})[cost] = [
            {"draw": d, **_record(s.boundary, f"control draw {d} {arm} {cost}")} for d, s in enumerate(summaries)
        ]
    return canonical_json(payload)


def boundary_sha256(run: SeriesRun) -> str:
    return hashlib.sha256(boundary_payload(run)).hexdigest()


__all__ = [
    "ARMS",
    "B1",
    "BOOK",
    "CAP_WEIGHTED",
    "CONTROL",
    "EQUAL_WEIGHT",
    "Scenario",
    "SeriesRun",
    "Summary",
    "WealthNonpositive",
    "WealthRefusal",
    "boundary_payload",
    "boundary_sha256",
    "canonical_json",
    "check_complete",
    "holding_months",
    "run_series",
    "summarise",
    "wealth_refusals",
]
