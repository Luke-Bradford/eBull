"""#3740 (#3609 step 3): premise 5's planner, the G1 bar and power for each candidate family set.

Spec: ``docs/research/2026-10-10-3740-step3-vw-book.md`` premise 5 and §"Decision rule" condition 3 (PR #3755). Pure
functions only. The historical run that feeds them (stage A and stage B, 119 holding months, both termination arms
at base cost) is slice 3, which waits on slice F's spec closing its checkpoint 1 (§"Slices" 1). Nothing here reads
an artefact, the database or a book return; the caller supplies each arm's 119 months.

* **Candidate sets** (:func:`candidate_sets`): the preference order filtered to the supported families (§"Condition
  1"), evaluated in order by :func:`plan`, which stops at the first set whose criterion holds. Fits are requested
  lazily, so a set after the chosen one is never fitted and never counts as a search.
* **The historical fit** (:func:`arm_fit`): G1's regression (step 2's ``g1``, so its refusals) over the 119 months,
  keeping the joint rows of factors and RF, the coefficients, the Newey-West standard errors and the residuals.
* **The bootstrap** (:func:`draws`, :func:`resample`): a 24-month circular block bootstrap (Politis & Romano 1992;
  block length 3), eight block starts drawn uniformly from the 119 months by ``randrange``, each block wrapping from
  the last month to the first. One ``random.Random`` per (set, arm, kind), seeded
  ``3740-step3-planning:{set}:{arm}:{kind}``, draws each replication's eight starts in order.
* **A replication** (:func:`replicate`) rebuilds the book's return from the resampled rows as RF + X b + e, with the
  intercept at zero and the intended loadings at zero (``null``) or at their historical estimates (``power``), the
  other loadings at theirs, and runs G1 on it. A replication G1 refuses records t = +inf for the null and does not
  clear for power; the denominator is always the replication count.
* **Bars** (:func:`bars`): per intended loading, the larger of z(1 - 0.05/k) and the nearest-rank (1 - 0.05/k)
  quantile of its null t in each arm, k the number of intended loadings.
* **Power** (:func:`arm_power`): the fraction of replications in which every intended loading's t is above its bar.
  **Criterion:** the conjunction bound over the two arms, 1 - sum(1 - arm power), at least ``TARGET_POWER``.
* **Printed beside it, not gated:** the analytic planned t b / (s sqrt(119/24)) with its normal power against the
  bar, and the bootstrap power with every intended loading at its unadjusted lower bound b - z(0.95) s. The
  lower-bound replications reuse the power run's draws (common random numbers), so the spec's one seed per
  (set, arm, null or power) run is unchanged and the two powers differ only through the loadings.
"""

from __future__ import annotations

import math
import random
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass
from fractions import Fraction
from statistics import NormalDist
from typing import Final, Literal

import numpy as np

from app.services.factor_book_path import Month, next_month
from app.services.factor_book_series import ARMS
from app.services.vw_factor_book import FAMILIES
from scripts.report_3609_baselines import FACTOR_REGRESSORS, ols_newey_west
from scripts.report_3609_step2_verdict import g1

Kind = Literal["null", "power"]

#: Premise 1's table order; set labels and seeds join a set's families in this order.
FAMILY_ORDER: Final[tuple[str, ...]] = tuple(FAMILIES)
#: Premise 1: each family's intended loading.
INTENDED: Final[Mapping[str, str]] = {"value": "HML", "GP/A": "RMW", "investment": "CMA"}
#: Premise 5: the largest subset, preferring value, then GP/A, then investment.
PREFERENCE: Final[tuple[tuple[str, ...], ...]] = (
    ("value", "GP/A", "investment"),
    ("value", "GP/A"),
    ("value", "investment"),
    ("GP/A", "investment"),
    ("value",),
    ("GP/A",),
    ("investment",),
)
#: Holding months 2014-10..2024-08: stage A's 80 and stage B's 39.
HISTORICAL_FIRST: Final[Month] = (2014, 10)
HISTORICAL_LAST: Final[Month] = (2024, 8)
HISTORICAL_MONTHS: Final = 119
FORWARD_MONTHS: Final = 24
#: By construction (premise 5); eight blocks make the 24 forward months.
BLOCK_LENGTH: Final = 3
BLOCKS: Final = FORWARD_MONTHS // BLOCK_LENGTH
REPLICATIONS: Final = 10_000
#: The family-wise one-sided level, split over the k intended loadings.
FAMILYWISE_ALPHA: Final = Fraction(5, 100)
#: The declared criterion on the conjunction bound over the two arms, compared exactly: powers are counts over the
#: replication count, so a bound of exactly 0.80 passes rather than landing one ulp below it in floating point.
TARGET_POWER: Final = Fraction(80, 100)
Z_95: Final = NormalDist().inv_cdf(0.95)
KINDS: Final[tuple[Kind, ...]] = ("null", "power")
#: G1's columns after the intercept, then RF: the joint row the bootstrap resamples.
_COLUMNS: Final = (*FACTOR_REGRESSORS, "RF")


class PlanError(RuntimeError):
    """An input the plan cannot be computed from (premise 5's refusals)."""


def _grid(first: Month, count: int) -> tuple[Month, ...]:
    out: list[Month] = [first]
    while len(out) < count:
        out.append(next_month(out[-1]))
    return tuple(out)


HISTORICAL_GRID: Final = _grid(HISTORICAL_FIRST, HISTORICAL_MONTHS)
if HISTORICAL_GRID[-1] != HISTORICAL_LAST:
    raise PlanError(f"{HISTORICAL_MONTHS} months from {HISTORICAL_FIRST} do not end at {HISTORICAL_LAST}")
#: Labels only: G1 keys each observation by month, and a replication's rows take these in order.
_GRID: Final = _grid((1, 1), FORWARD_MONTHS)


def candidate_sets(supported: Collection[str]) -> tuple[tuple[str, ...], ...]:
    """The preference order, keeping only the sets contained in the supported families."""
    unknown = set(supported) - set(FAMILY_ORDER)
    if unknown:
        raise PlanError(f"unknown families {sorted(unknown)}")
    return tuple(s for s in PREFERENCE if set(s) <= set(supported))


def set_label(chosen: Sequence[str]) -> str:
    if tuple(chosen) not in PREFERENCE:
        raise PlanError(f"{tuple(chosen)} is not a candidate set in the table's order")
    return "+".join(chosen)


def seed(chosen: Sequence[str], arm: str, kind: Kind) -> str:
    if arm not in ARMS or kind not in KINDS:
        raise PlanError(f"no planning run ({arm!r}, {kind!r})")
    return f"3740-step3-planning:{set_label(chosen)}:{arm}:{kind}"


def draws(chosen: Sequence[str], arm: str, kind: Kind, replications: int = REPLICATIONS) -> list[tuple[int, ...]]:
    """Each replication's eight block starts, drawn in order from the run's own generator."""
    rng = random.Random(seed(chosen, arm, kind))
    return [tuple(rng.randrange(HISTORICAL_MONTHS) for _ in range(BLOCKS)) for _ in range(replications)]


def resample(starts: Sequence[int]) -> tuple[int, ...]:
    """The 24 historical row indices: each start's block of three, wrapping from the last month to the first."""
    return tuple((s + j) % HISTORICAL_MONTHS for s in starts for j in range(BLOCK_LENGTH))


@dataclass(frozen=True)
class ArmFit:
    """One arm's 119-month regression: the joint rows (``_COLUMNS``), coefficients, standard errors and residuals."""

    rows: np.ndarray
    coefficients: Mapping[str, float]
    standard_errors: Mapping[str, float]
    residuals: np.ndarray


def arm_fit(
    returns: Mapping[Month, float], factors: Mapping[str, Mapping[Month, float]], months: Sequence[Month]
) -> ArmFit:
    """G1's regression on the historical path; G1's refusals, and a path off its 119 months, refuse the plan."""
    if tuple(months) != HISTORICAL_GRID:
        raise PlanError(
            f"the historical path has {len(months)} months, not the {HISTORICAL_MONTHS} of "
            f"{HISTORICAL_FIRST}..{HISTORICAL_LAST} in order"
        )
    result = g1(returns, factors, months)
    if result.refusal is not None:
        raise PlanError(f"historical G1 refused: {result.refusal}")
    rows = np.array([[factors[n][m] for n in _COLUMNS] for m in months], dtype=float)
    y = np.array([returns[m] for m in months], dtype=float) - rows[:, -1]
    x = rows[:, :-1]
    fit = ols_newey_west(y, x)
    labels = ("alpha", *FACTOR_REGRESSORS)
    return ArmFit(
        rows=rows,
        coefficients={n: float(c) for n, c in zip(labels, fit.coefficients, strict=True)},
        standard_errors={n: float(e) for n, e in zip(labels, fit.standard_errors, strict=True)},
        residuals=y - np.column_stack([np.ones(len(y)), x]) @ fit.coefficients,
    )


def simulated_coefficients(fit: ArmFit, intended: Mapping[str, float]) -> np.ndarray:
    """Intercept zero, the intended loadings as given, every other loading at its historical estimate."""
    unknown = set(intended) - set(FACTOR_REGRESSORS)
    if unknown:
        raise PlanError(f"unknown loadings {sorted(unknown)}")
    return np.array([0.0, *(intended.get(n, fit.coefficients[n]) for n in FACTOR_REGRESSORS)])


def replicate(fit: ArmFit, coefficients: np.ndarray, indices: Sequence[int]) -> Mapping[str, float] | None:
    """G1's t per regressor on one resample of the book rebuilt as RF + X b + e; ``None`` when G1 refuses."""
    sample = fit.rows[list(indices)]
    book = sample[:, -1] + coefficients[0] + sample[:, :-1] @ coefficients[1:] + fit.residuals[list(indices)]
    returns = dict(zip(_GRID, book.tolist(), strict=True))
    factors = {n: dict(zip(_GRID, sample[:, i].tolist(), strict=True)) for i, n in enumerate(_COLUMNS)}
    result = g1(returns, factors, _GRID)
    return None if result.refusal is not None else result.t_stats


def loading_ts(
    fit: ArmFit, chosen: Sequence[str], coefficients: np.ndarray, starts: Sequence[Sequence[int]]
) -> list[tuple[float, ...] | None]:
    """Per replication, the intended loadings' t in the set's order, or ``None`` when G1 refused it."""
    names = [INTENDED[f] for f in chosen]
    out: list[tuple[float, ...] | None] = []
    for s in starts:
        t = replicate(fit, coefficients, resample(s))
        out.append(None if t is None else tuple(t[n] for n in names))
    return out


def quantile_level(k: int) -> Fraction:
    return 1 - FAMILYWISE_ALPHA / k


def nearest_rank(values: Sequence[float], q: Fraction) -> float:
    """The ceil(q n)-th order statistic, with the rank computed exactly."""
    if not values or not 0 < q <= 1:
        raise PlanError("a nearest-rank quantile needs values and 0 < q <= 1")
    rank = math.ceil(q * len(values))
    return sorted(values)[rank - 1]


def bars(null_ts: Mapping[str, Sequence[tuple[float, ...] | None]], chosen: Sequence[str]) -> dict[str, float]:
    """Per intended loading, the larger of z(1 - 0.05/k) and its null t's quantile in each arm (refused = +inf)."""
    q = quantile_level(len(chosen))
    z = NormalDist().inv_cdf(float(q))
    out: dict[str, float] = {}
    for i, family in enumerate(chosen):
        quantiles = [nearest_rank([math.inf if r is None else r[i] for r in null_ts[arm]], q) for arm in ARMS]
        out[INTENDED[family]] = max(z, *quantiles)
    return out


def arm_power(ts: Sequence[tuple[float, ...] | None], chosen: Sequence[str], bar: Mapping[str, float]) -> Fraction:
    """The fraction of replications in which every intended loading's t is above its bar; refused never clears."""
    limits = [bar[INTENDED[f]] for f in chosen]
    cleared = sum(1 for r in ts if r is not None and all(t > b for t, b in zip(r, limits, strict=True)))
    return Fraction(cleared, len(ts))


def conjunction_bound(powers: Mapping[str, Fraction]) -> Fraction:
    return 1 - sum((1 - p for p in powers.values()), Fraction(0))


def analytic_power(fit: ArmFit, loading: str, bar: float) -> tuple[float, float]:
    """Printed only: the planned 24-month t b / (s sqrt(119/24)) and its normal power against the bar."""
    planned = fit.coefficients[loading] / (fit.standard_errors[loading] * math.sqrt(HISTORICAL_MONTHS / FORWARD_MONTHS))
    return planned, NormalDist().cdf(planned - bar)


@dataclass(frozen=True)
class SetPlan:
    label: str
    bars: Mapping[str, float]
    #: Per arm: the gated bootstrap power, and the printed powers (analytic per loading, bootstrap at lower bounds).
    power: Mapping[str, Fraction]
    conjunction_bound: Fraction
    passes: bool
    analytic: Mapping[str, Mapping[str, tuple[float, float]]]
    power_at_lower_bounds: Mapping[str, Fraction]
    #: Per arm, replications G1 refused in the null and the power run.
    refused: Mapping[str, tuple[int, int]]


def plan_set(fits: Mapping[str, ArmFit], chosen: Sequence[str], replications: int = REPLICATIONS) -> SetPlan:
    """One evaluated set: the bars from both arms' nulls, each arm's power, and the criterion."""
    if set(fits) != set(ARMS):
        raise PlanError(f"a set needs one fit per arm {ARMS}, got {sorted(fits)}")
    if replications <= 0:
        raise PlanError(f"a run needs at least one replication, got {replications}")
    names = [INTENDED[f] for f in chosen]
    null: dict[str, list[tuple[float, ...] | None]] = {}
    alternative: dict[str, list[tuple[float, ...] | None]] = {}
    lower: dict[str, list[tuple[float, ...] | None]] = {}
    for arm in ARMS:
        fit = fits[arm]
        null[arm] = loading_ts(
            fit,
            chosen,
            simulated_coefficients(fit, dict.fromkeys(names, 0.0)),
            draws(chosen, arm, "null", replications),
        )
        starts = draws(chosen, arm, "power", replications)
        historical = {n: fit.coefficients[n] for n in names}
        alternative[arm] = loading_ts(fit, chosen, simulated_coefficients(fit, historical), starts)
        bounds = {n: fit.coefficients[n] - Z_95 * fit.standard_errors[n] for n in names}
        lower[arm] = loading_ts(fit, chosen, simulated_coefficients(fit, bounds), starts)
    bar = bars(null, chosen)
    power = {arm: arm_power(alternative[arm], chosen, bar) for arm in ARMS}
    bound = conjunction_bound(power)
    return SetPlan(
        label=set_label(chosen),
        bars=bar,
        power=power,
        conjunction_bound=bound,
        passes=bound >= TARGET_POWER,
        analytic={arm: {n: analytic_power(fits[arm], n, bar[n]) for n in names} for arm in ARMS},
        power_at_lower_bounds={arm: arm_power(lower[arm], chosen, bar) for arm in ARMS},
        refused={arm: (null[arm].count(None), alternative[arm].count(None)) for arm in ARMS},
    )


@dataclass(frozen=True)
class Plan:
    #: Every evaluated set in order; these are the planning searches the register counts.
    evaluated: tuple[SetPlan, ...]
    #: The first set whose criterion holds, or ``None``: "condition 3 cannot be powered on 24 months".
    chosen: tuple[str, ...] | None


def plan(
    supported: Collection[str],
    fits_for: Callable[[tuple[str, ...]], Mapping[str, ArmFit]],
    replications: int = REPLICATIONS,
) -> Plan:
    """Evaluate the candidate sets in order, fitting each only when it is reached, and stop at the first pass."""
    evaluated: list[SetPlan] = []
    for chosen in candidate_sets(supported):
        result = plan_set(fits_for(chosen), chosen, replications)
        evaluated.append(result)
        if result.passes:
            return Plan(tuple(evaluated), chosen)
    return Plan(tuple(evaluated), None)
