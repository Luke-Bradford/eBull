"""#3609 step 2's verdict over the valued series (slice 3c-iv(d)).

Spec: ``docs/research/2026-10-06-3609-step2-factor-book.md`` §"Decision rule" (PR #3666): G1, G2, the verdict order,
the turnover veto and the two ``PASS`` annotations. Pure functions over
:class:`~app.services.factor_book_series.SeriesRun`, which has already refused every data, comparator and
non-positive-wealth condition (verdict-order step 1, ``REFUSED``), so every monthly wealth factor here is finite and
positive. The statistics are step 0's (``scripts/report_3609_baselines.py``): its Newey-West regression and lag
rule, its factor names and its nearest-rank percentile.

* **G1** (whole path, base cost, per arm): the book's monthly return in excess of RF on FF5 plus momentum; HML, RMW
  and CMA must each load positively with a one-sided t above z(1 − 0.05/3). Its numerical refusals are checked before
  any coefficient is read.
* **G2** (stage B, base cost, per arm): the book's annualised log growth G = (12/n) Σ ln(1 + r) above B1's and above
  the nearest-rank median of the control draws'.
* **Turnover veto:** any stage-B month with the book's one-way turnover above 50% at base cost, in either arm.
* **Annotations,** computed only for a pass candidate: "fails at stress cost" and "depends on <year>".

**Month labels.** Months are ``(year, month)``. The path charges formation M's trades in return month M, the month
whose end they trade at, and month M + 1 holds their result. So:

* **Stage B starts from the boundary's pre-trade NAV** (§"Dates, samples and the hold-out": stage-B statistics are
  slices of the path after that state). Return months 2021-06..2024-08 alone would miss the boundary formation's
  (2021-05) trading cost, which sits in the stage-A month 2021-05. :func:`stage_b_returns` folds that cost factor,
  post-trade over pre-trade NAV at the boundary, into the first stage-B month. B1 holds through and has none.
* **The turnover veto reads stage B's formations,** 2021-05..2024-07, which is how the path keys turnover.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from statistics import NormalDist
from typing import Final

import numpy as np

from app.services.factor_book import BookRefusal
from app.services.factor_book_path import BoundaryState, Month, month_of, next_month
from app.services.factor_book_series import ARMS, SeriesRun
from app.services.strategy_result import AmbiguityArm
from scripts.report_3609_baselines import FACTOR_REGRESSORS, nearest_rank, ols_newey_west

#: §"Dates, samples and the hold-out": stage B's holding months, and its formations (the path's turnover keys).
STAGE_B: Final[tuple[Month, Month]] = ((2021, 6), (2024, 8))
STAGE_B_FORMATIONS: Final[tuple[Month, Month]] = ((2021, 5), (2024, 7))
#: Step 0's cost scenario names (``COSTS``): the base case and the stress.
BASE: Final = "net"
STRESS: Final = "stress_2x"
#: G1's intended loadings: HML for value, RMW for GP/A, CMA for investment.
G1_LOADINGS: Final = ("HML", "RMW", "CMA")
#: z(1 − 0.05/3), one-sided, Bonferroni over the three loadings (the spec prints 2.128).
G1_CRITICAL: Final = NormalDist().inv_cdf(1.0 - 0.05 / len(G1_LOADINGS))
#: ``research-process.md``: above this one-way monthly turnover the net expected benefit must be shown.
TURNOVER_VETO: Final = 0.5


# --------------------------------------------------------------------------- G1


@dataclass(frozen=True)
class G1Result:
    #: The §"Decision rule" refusal that held, if any; then nothing else is set.
    refusal: str | None = None
    #: ``alpha`` and each regressor.
    coefficients: Mapping[str, float] = field(default_factory=dict)
    t_stats: Mapping[str, float] = field(default_factory=dict)
    lag: int | None = None

    @property
    def passed(self) -> bool:
        # A t above z > 0 over a positive standard error (a non-positive one refused) is a positive loading.
        return self.refusal is None and all(self.t_stats[name] > G1_CRITICAL for name in G1_LOADINGS)


def g1(
    returns: Mapping[Month, float], factors: Mapping[str, Mapping[Month, float]], months: Sequence[Month]
) -> G1Result:
    """G1 on ``months``. Refusals, in the spec's order: a missing factor month; a non-finite value; anything but
    exactly one book observation per declared month; a rank-deficient design; zero residual variance; a Newey-West
    standard error that is not finite and positive."""
    names = (*FACTOR_REGRESSORS, "RF")
    missing = [(n, m) for n in names for m in months if m not in factors.get(n, {})]
    if missing:
        return G1Result(f"missing_factor_month: {len(missing)}, first {missing[0]}")
    values = [*returns.values(), *(factors[n][m] for n in names for m in months)]
    if not all(math.isfinite(v) for v in values):
        return G1Result("non_finite: a book or factor value is not finite")
    if len(set(months)) != len(months) or returns.keys() != set(months):
        return G1Result("misaligned: the book needs exactly one return for each declared month")
    raw = np.array([[returns[m], *(factors[n][m] for n in names)] for m in months], dtype=float)
    x = raw[:, 1 : 1 + len(FACTOR_REGRESSORS)]
    y = raw[:, 0] - raw[:, -1]
    design = np.column_stack([np.ones(len(y)), x])
    if np.linalg.matrix_rank(design) < design.shape[1]:
        return G1Result("rank_deficient: the design matrix's rank is below its column count")
    fit = ols_newey_west(y, x)
    residual = y - design @ fit.coefficients
    if not float(residual @ residual) > 0.0:
        return G1Result("zero_residual_variance")
    if not all(math.isfinite(e) and e > 0 for e in fit.standard_errors):
        return G1Result("newey_west_se_invalid: a standard error is not finite and positive")
    labels = ("alpha", *FACTOR_REGRESSORS)
    return G1Result(
        None,
        {n: float(c) for n, c in zip(labels, fit.coefficients, strict=True)},
        {n: float(t) for n, t in zip(labels, fit.t_stats, strict=True)},
        fit.lag,
    )


# --------------------------------------------------------------------------- G2


def log_growth(returns: Mapping[Month, float], months: Sequence[Month], label: str) -> float:
    """G = (12/n) Σ ln(1 + r) over ``months``; a non-finite G refuses ``COMPARATOR_INVALID``."""
    if not months:
        raise ValueError(f"{label}: G needs at least one month")
    g = 12.0 / len(months) * math.fsum(math.log(1.0 + returns[m]) for m in months)
    if not math.isfinite(g):
        raise BookRefusal("COMPARATOR_INVALID", f"{label}: G is not finite")
    return g


@dataclass(frozen=True)
class G2Result:
    book: float
    b1: float
    control_median: float

    @property
    def failing(self) -> tuple[str, ...]:
        """The comparisons the book does not beat."""
        return tuple(
            name for name, value in (("B1", self.b1), ("control median", self.control_median)) if self.book <= value
        )

    @property
    def passed(self) -> bool:
        return not self.failing


def stage_b_returns(
    returns: Mapping[Month, float], nav: Mapping[Month, float], boundary: BoundaryState | None, window: Sequence[Month]
) -> dict[Month, float]:
    """``window``'s returns from the boundary's pre-trade NAV: the first month's factor times the boundary
    formation's cost factor (post-trade NAV over pre-trade NAV)."""
    if boundary is None:
        raise ValueError("a stage-B slice needs the path's boundary state")
    month = month_of(boundary.formation)
    if next_month(month) != window[0]:
        raise ValueError(f"the boundary formation {boundary.formation} does not precede stage B's first month")
    out = {m: returns[m] for m in window}
    out[window[0]] = (1.0 + out[window[0]]) * (nav[month] / boundary.nav) - 1.0
    return out


def g2(
    run: SeriesRun, arm: AmbiguityArm, cost: str, window: Sequence[Month], months: Sequence[Month] | None = None
) -> G2Result:
    """G2's three G values over ``months`` (default: all of ``window``, stage B) of the stage-B slices."""
    months = window if months is None else months
    label = f"{arm} {cost}"
    book = run.book[(arm, cost)]
    draws = [
        log_growth(stage_b_returns(s.returns, s.nav, s.boundary, window), months, f"control draw {d} {label}")
        for d, s in enumerate(run.control[(arm, cost)])
    ]
    return G2Result(
        log_growth(stage_b_returns(book.returns, book.nav, book.boundary, window), months, f"book {label}"),
        log_growth(run.b1[cost].returns, months, f"b1 {cost}"),
        nearest_rank(draws, 50),
    )


# --------------------------------------------------------------------------- the verdict


def stage_b_months(months: Sequence[Month]) -> list[Month]:
    """Stage B's months of the run, which must hold all of them."""
    first, last = STAGE_B
    window = [m for m in months if first <= m <= last]
    if not window or window[0] != first or window[-1] != last:
        raise ValueError(f"the run's months do not cover stage B {first}..{last}")
    return window


def turnover_exceedances(run: SeriesRun) -> dict[str, tuple[Month, ...]]:
    """Per arm, stage B's formations whose base-cost one-way turnover exceeds ``TURNOVER_VETO``."""
    first, last = STAGE_B_FORMATIONS
    out: dict[str, tuple[Month, ...]] = {}
    for arm in ARMS:
        turnover = run.book[(arm, BASE)].turnover
        out[arm] = tuple(m for m in sorted(turnover) if first <= m <= last and turnover[m] > TURNOVER_VETO)
    return out


def pass_annotations(run: SeriesRun, window: Sequence[Month]) -> tuple[str, ...]:
    """§"Decision rule": the pass candidate's two annotations, each naming its failing arm and comparison."""
    out: list[str] = []
    stress = [f"{arm} vs {', '.join(r.failing)}" for arm in ARMS if not (r := g2(run, arm, STRESS, window)).passed]
    if stress:
        out.append(f"fails at stress cost ({'; '.join(stress)})")
    for year in sorted({m[0] for m in window}):
        remaining = [m for m in window if m[0] != year]
        failed = [
            f"{arm} vs {', '.join(r.failing)}"
            for arm in ARMS
            if not (r := g2(run, arm, BASE, window, remaining)).passed
        ]
        if failed:
            out.append(f"depends on {year} ({'; '.join(failed)})")
    return tuple(out)


@dataclass(frozen=True)
class Verdict:
    """``INSUFFICIENT``, ``G1_REFUSED``, ``FAIL`` or ``PASS``; ``REFUSED`` is raised, never returned."""

    status: str
    reason: str | None = None
    insufficient: tuple[date, ...] = ()
    g1: Mapping[str, G1Result] | None = None
    g2: Mapping[str, G2Result] | None = None
    turnover: Mapping[str, tuple[Month, ...]] | None = None
    annotations: tuple[str, ...] = ()


def verdict(run: SeriesRun, factors: Mapping[str, Mapping[Month, float]]) -> Verdict:
    """§"Decision rule", verdict order steps 2-6; step 1 (``REFUSED``) was raised by ``run_series`` or is raised
    here (``COMPARATOR_INVALID`` from a G)."""
    window = stage_b_months(run.months)
    if run.insufficient:
        return Verdict("INSUFFICIENT", insufficient=run.insufficient)
    g1s = {arm: g1(run.book[(arm, BASE)].returns, factors, run.months) for arm in ARMS}
    refused = [f"{arm}: {r.refusal}" for arm, r in g1s.items() if r.refusal is not None]
    if refused:
        return Verdict("G1_REFUSED", "; ".join(refused), g1=g1s)
    g2s = {arm: g2(run, arm, BASE, window) for arm in ARMS}
    failing = [f"G1 {arm}" for arm in ARMS if not g1s[arm].passed]
    failing += [f"G2 {arm} vs {', '.join(g2s[arm].failing)}" for arm in ARMS if not g2s[arm].passed]
    if failing:
        return Verdict("FAIL", "; ".join(failing), g1=g1s, g2=g2s)
    exceeded = turnover_exceedances(run)
    if any(exceeded.values()):
        return Verdict("FAIL", "TURNOVER_BENEFIT_UNSHOWN", g1=g1s, g2=g2s, turnover=exceeded)
    return Verdict("PASS", g1=g1s, g2=g2s, turnover=exceeded, annotations=pass_annotations(run, window))


__all__ = [
    "BASE",
    "G1_CRITICAL",
    "G1_LOADINGS",
    "STAGE_B",
    "STAGE_B_FORMATIONS",
    "STRESS",
    "TURNOVER_VETO",
    "G1Result",
    "G2Result",
    "Verdict",
    "pass_annotations",
    "g1",
    "g2",
    "log_growth",
    "stage_b_months",
    "stage_b_returns",
    "turnover_exceedances",
    "verdict",
]
