"""#2901 quality arm: the monthly trial's identity gate, statistics and verdict (PR B, part 1).

Spec: ``docs/proposals/ta/2026-09-25-2901-quality-declaration-spec.md`` ("Identity gate", "Statistics",
"Composition with termination policies", "Verdict"). Everything here is pure and consumes monthly series keyed by
``(year, month)``, so it is fixed and tested before any return exists. The simulator (``simulate_monthly``) and the
sealed runner are part 2.

Every statistic returns a value or a :class:`Refused` with a named reason. Conditions over them are three-valued
(:class:`Tri`) and compose by the weak Kleene conjunction, so a refusal is carried to the verdict boundary instead of
being read as a failure or a pass.

``r6_exclusion_trial`` is NOT edited: its sha256 is in #2908's result identity and in #3362's
``termination_identity``. Its public primitives are imported, never re-implemented.
"""

from __future__ import annotations

import csv
import io
import math
import statistics
import zipfile
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Final

from app.services.r6_exclusion_trial import ZERO_RECOVERY, haircut_net_return, month_pairs
from app.services.r6_exclusion_trial import validate_factor as _validate_factor

Month = tuple[int, int]

REFERENCE_MEMBER: Final = "prof_monthly_2025/portf_gpa_monthly_2025.csv"
REFERENCE_HEADER: Final = ["year", "month", "rank_GPA", "nstocks", "ret_vw"]
REFERENCE_LEGS: Final = (1, 10)

#: 2013-07 … 2024-08: the 134 full months. The partial 2024-09 enters total returns only.
STATISTIC_MONTHS: Final = month_pairs((2013, 7), (2024, 8))

#: Harvey/Liu/Zhu's hurdle, inherited from #2908. Also the |z| whose upper tail sets the cohort bar q.
HAC_T_BAR: Final = 3.0
#: Harvey, Liu & Zhu (2016) §4, Bonferroni's adjustment, at their level.
FAMILY_ALPHA: Final = 0.05
#: #2901's headline (A − C) and diagnostic (A − C′) rows of the first run.
FIRST_RUN_ROWS: Final = 2
#: Two reserved rows for each of the three permitted corrections, counted whether used or not.
RESERVED_CORRECTION_ROWS: Final = 6
ROBUST_HAIRCUT: Final = 0.58
CONTINGENT_HAIRCUT: Final = 0.15
HAIRCUTS: Final = (CONTINGENT_HAIRCUT, ROBUST_HAIRCUT)
#: The cohort bar's bisection bracket and tolerance.
QUANTILE_BRACKET: Final = 100.0
QUANTILE_TOLERANCE: Final = 1e-12


class GateRefusal(RuntimeError):
    """The reference file could not be read under its declared contract. Every policy's gate is refused."""


@dataclass(frozen=True)
class Refused:
    """A statistic or gate value that could not be computed. It is never a number and never a verdict."""

    reason: str


class Tri(StrEnum):
    """A three-valued condition: true, false or refused."""

    TRUE = "T"
    FALSE = "F"
    REFUSED = "R"


def weak_kleene_and(values: Iterable[Tri]) -> Tri:
    """Weak Kleene (Bochvar) conjunction: R if any input is R; otherwise T if every input is T; otherwise F."""
    inputs = list(values)
    if not inputs:
        raise ValueError("a conjunction needs at least one input")
    if Tri.REFUSED in inputs:
        return Tri.REFUSED
    return Tri.TRUE if all(value is Tri.TRUE for value in inputs) else Tri.FALSE


def _tri(holds: bool) -> Tri:
    return Tri.TRUE if holds else Tri.FALSE


class Portfolio(StrEnum):
    """The books whose 134-month ruin refuses a statistic."""

    ARM = "A"
    CONTROL = "C"
    COMPLETE_CASE = "C′"
    GATE_CONTROL = "D₀"


class Comparator(StrEnum):
    CONTROL = "C"
    COMPLETE_CASE = "C′"


# --- Identity gate -------------------------------------------------------------------------------------------


def read_global_q_gpa(path: Path, months: Sequence[Month] = STATISTIC_MONTHS) -> dict[Month, float]:
    """Rank 10 minus rank 1 ``ret_vw`` per month of ``months``, as a fraction.

    Raw rows are read before any dictionary is built: a malformed key or a duplicate (year, month, rank) anywhere
    refuses. Each rank-1 and rank-10 row inside ``months`` must have an integer ``nstocks`` ≥ 1 and a finite
    ``ret_vw`` ≥ −100 (exactly −100 is a total loss), and each leg must cover ``months`` exactly. No magnitude bound
    is imposed: a uniform unit error cannot move the gate's correlation, beta sign or lead/lag test.
    """
    with zipfile.ZipFile(path) as archive, archive.open(REFERENCE_MEMBER) as raw:
        reader = csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8-sig"))
        if reader.fieldnames != REFERENCE_HEADER:
            raise GateRefusal(f"unexpected global-q GP/A header: {reader.fieldnames}")
        rows = list(reader)
    window = set(months)
    seen: set[tuple[int, int, int]] = set()
    legs: dict[int, dict[Month, float]] = {rank: {} for rank in REFERENCE_LEGS}
    for row in rows:
        try:
            key = (int(row["year"]), int(row["month"]), int(row["rank_GPA"]))
        except (TypeError, ValueError) as exc:
            raise GateRefusal(f"malformed global-q GP/A key in {row}") from exc
        if key in seen:
            raise GateRefusal(f"duplicate global-q GP/A row {key}")
        seen.add(key)
        month, rank = key[:2], key[2]
        if rank not in legs or month not in window:
            continue
        try:
            names = int(row["nstocks"])
            value = float(row["ret_vw"])
        except (TypeError, ValueError) as exc:
            raise GateRefusal(f"global-q GP/A row {key} is outside its domain") from exc
        if names < 1 or not math.isfinite(value) or value < -100.0:
            raise GateRefusal(f"global-q GP/A row {key} is outside its domain")
        legs[rank][month] = value / 100.0
    for rank, series in legs.items():
        if set(series) != window:
            raise GateRefusal(f"global-q GP/A rank {rank} does not cover the {len(window)} statistic months")
    low, high = (legs[rank] for rank in REFERENCE_LEGS)
    return {month: high[month] - low[month] for month in months}


@dataclass(frozen=True)
class GateReadout:
    """What the gate publishes. ⚠ No leg mean, beta magnitude, alpha or cumulative figure: those carry arm
    information."""

    months: int
    correlation: float
    beta_positive: bool
    lag_correlation: float
    lead_correlation: float
    passed: bool


def identity_gate(
    ours: Mapping[Month, float],
    reference: Mapping[Month, float],
    *,
    ruined: bool,
    months: Sequence[Month] = STATISTIC_MONTHS,
) -> GateReadout | Refused:
    """#2908's frozen rule for one policy, on an exactly aligned calendar.

    ``ours`` is the gross spread r(A) − r(D₀); ``ruined`` is a gross ruin of A or D₀ inside ``months``. Both key
    sets must equal ``months`` exactly, so ``validate_factor``'s key intersection is the identity and its positional
    lag/lead shift lines up with the months. A misaligned calendar, a non-finite month, a ruin or a zero-variance
    raise from ``_pearson`` refuses this policy's gate.
    """
    expected = tuple(months)
    if len(set(expected)) != len(expected) or list(expected) != sorted(expected):
        raise ValueError("the gate calendar must be strictly increasing with no duplicate month")
    if ruined:
        return Refused("a gross ruin of A or D₀ inside the statistic months")
    window = set(expected)
    for side, series in (("our", ours), ("reference", reference)):
        if set(series) != window:
            return Refused(
                f"the {side} spread months differ from the calendar: missing {sorted(window - set(series))[:3]}, "
                f"extra {sorted(set(series) - window)[:3]}"
            )
        if not all(math.isfinite(series[key]) for key in expected):
            return Refused(f"the {side} spread has a non-finite month")
    try:
        validation = _validate_factor(dict(ours), dict(reference))
    except RuntimeError as exc:
        return Refused(f"the gate correlations could not be computed: {exc}")
    return GateReadout(
        months=validation.months,
        correlation=validation.correlation,
        beta_positive=validation.beta > 0,
        lag_correlation=validation.lag_correlation,
        lead_correlation=validation.lead_correlation,
        passed=validation.passed,
    )


def compose_gate(readouts: Mapping[str, GateReadout | Refused]) -> Tri:
    """The gate under every programme policy, weak-Kleene composed. ``zero_recovery`` must be present."""
    if ZERO_RECOVERY.label not in readouts:
        raise ValueError("the governing zero_recovery gate readout is missing")
    return weak_kleene_and(
        Tri.REFUSED if isinstance(readout, Refused) else _tri(readout.passed) for readout in readouts.values()
    )


# --- Mean and HAC t (headline) -------------------------------------------------------------------------------


def mean_active(active: Sequence[float], *, ruined: bool) -> float | Refused:
    """mean(a). ``ruined`` is a ruin of A or of the comparator inside the 134 months."""
    if len(active) < 2:
        return Refused(f"a mean needs at least 2 observations, got {len(active)}")
    if not all(math.isfinite(value) for value in active):
        return Refused("a non-finite monthly active return")
    if ruined:
        return Refused("a ruin of A or of the comparator inside the statistic months")
    return math.fsum(active) / len(active)


def newey_west_lag(observations: int) -> int:
    """⌊4(T/100)^{2/9}⌋, the rule-of-thumb truncation of Newey & West (1994). 4 at T = 134."""
    if observations < 2:
        raise ValueError(f"a HAC estimate needs at least 2 observations, got {observations}")
    return math.floor(4.0 * (observations / 100.0) ** (2.0 / 9.0))


@dataclass(frozen=True)
class HacEstimate:
    observations: int
    lag: int
    mean: float
    #: γ̂₀, 1/T normalisation.
    variance: float
    #: Ω̂ = γ̂₀ + 2 Σ (1 − l/(L+1)) γ̂_l, Bartlett kernel.
    long_run_variance: float
    standard_error: float
    t_stat: float


def hac_t(active: Sequence[float], *, ruined: bool) -> HacEstimate | Refused:
    """mean / Newey–West SE: Bartlett kernel, 1/T autocovariances centred on the mean, no prewhitening."""
    mean = mean_active(active, ruined=ruined)
    if isinstance(mean, Refused):
        return mean
    count = len(active)
    lag = newey_west_lag(count)
    centred = [value - mean for value in active]

    def autocovariance(shift: int) -> float:
        return math.fsum(centred[t] * centred[t - shift] for t in range(shift, count)) / count

    variance = autocovariance(0)
    if variance == 0.0:
        return Refused("γ̂₀ = 0: the active series is constant")
    long_run = variance + 2.0 * math.fsum(
        (1.0 - shift / (lag + 1)) * autocovariance(shift) for shift in range(1, lag + 1)
    )
    if not (math.isfinite(long_run) and long_run > 0.0):
        return Refused(f"the HAC variance is not positive and finite: {long_run}")
    standard_error = math.sqrt(long_run / count)
    return HacEstimate(
        observations=count,
        lag=lag,
        mean=mean,
        variance=variance,
        long_run_variance=long_run,
        standard_error=standard_error,
        t_stat=mean / standard_error,
    )


# --- Bonferroni over the frozen family -----------------------------------------------------------------------


def family_size(history_rows: int) -> int:
    """M = |H| + 2 + 6, frozen in the declaration before the run. Corrections never raise it afterwards."""
    if history_rows < 0:
        raise ValueError(f"the history row count cannot be negative, got {history_rows}")
    return history_rows + FIRST_RUN_ROWS + RESERVED_CORRECTION_ROWS


def two_sided_p(t_stat: float) -> float:
    """p = 2Φ(−|t|) = erfc(|t|/√2), evaluated as a survival function (never 1 − Φ, which loses the upper tail)."""
    return math.erfc(abs(t_stat) / math.sqrt(2.0))


def _valid_family(size: object) -> bool:
    return isinstance(size, int) and not isinstance(size, bool) and size >= 1


def bonferroni_t(size: int) -> float:
    """t_M = max(3, the t whose two-sided p is α/M), the root found by bisection on erfc. Reported, not used to
    decide: the conditions evaluate t > 3 and p ≤ α/M literally."""
    if not _valid_family(size):
        raise ValueError(f"the family size must be a positive integer, got {size!r}")
    target = FAMILY_ALPHA / size
    low, high = 0.0, 1.0
    while two_sided_p(high) > target:
        high *= 2.0
    while high - low > QUANTILE_TOLERANCE:
        middle = 0.5 * (low + high)
        if two_sided_p(middle) > target:
            low = middle
        else:
            high = middle
    return max(HAC_T_BAR, high)


# --- Finite-sample cohort t ----------------------------------------------------------------------------------


def student_t_cdf(value: float, df: int) -> float:
    """Student t CDF for integer ν, from the closed forms of Abramowitz & Stegun 26.7.3/26.7.4.

    With θ = arctan(t/√ν), A(t|ν) = P(|T| < t) is
    - ν odd:  (2/π)[θ + sinθ cosθ (1 + (2/3)cos²θ + … + (2·4···(ν−3))/(1·3···(ν−2)) cos^{ν−3}θ)], the bracket
      absent at ν = 1;
    - ν even: sinθ (1 + (1/2)cos²θ + … + (1·3···(ν−3))/(2·4···(ν−2)) cos^{ν−2}θ).
    """
    if df < 1:
        raise ValueError(f"degrees of freedom must be a positive integer, got {df}")
    theta = math.atan(abs(value) / math.sqrt(df))
    cos_sq = math.cos(theta) ** 2
    if df % 2:
        series = 0.0
        if df > 1:
            term = series = 1.0
            for k in range(1, (df - 3) // 2 + 1):
                term *= (2 * k) / (2 * k + 1) * cos_sq
                series += term
        within = (2.0 / math.pi) * (theta + math.sin(theta) * math.cos(theta) * series)
    else:
        term = series = 1.0
        for k in range(1, (df - 2) // 2 + 1):
            term *= (2 * k - 1) / (2 * k) * cos_sq
            series += term
        within = math.sin(theta) * series
    half = 0.5 * within
    return 0.5 + half if value >= 0 else 0.5 - half


def student_t_quantile(probability: float, df: int) -> float:
    """Inverse of :func:`student_t_cdf` by bisection on [0, 100] to 1e-12."""
    if not 0.0 < probability < 1.0:
        raise ValueError(f"a quantile needs a probability in (0, 1), got {probability}")
    if probability < 0.5:
        return -student_t_quantile(1.0 - probability, df)
    if student_t_cdf(QUANTILE_BRACKET, df) < probability:
        raise ValueError(f"the t_{df} quantile at {probability} lies above {QUANTILE_BRACKET}")
    low, high = 0.0, QUANTILE_BRACKET
    while high - low > QUANTILE_TOLERANCE:
        middle = 0.5 * (low + high)
        if student_t_cdf(middle, df) < probability:
            low = middle
        else:
            high = middle
    return 0.5 * (low + high)


def cohort_t_bar(df: int) -> float:
    """q = F⁻¹_tν(Φ(3)): the t_ν quantile at the upper-tail probability of |z| = 3."""
    return student_t_quantile(1.0 - 0.5 * math.erfc(HAC_T_BAR / math.sqrt(2.0)), df)


def holding_year(month: Month) -> int:
    """The formation year D of the holding-year cohort July(D) … June(D + 1) containing ``month``."""
    year, number = month
    return year if number >= 7 else year - 1


def cohort_active_returns(
    arm: Mapping[Month, float],
    comparator: Mapping[Month, float],
    months: Sequence[Month] = STATISTIC_MONTHS,
) -> dict[int, float]:
    """∏(1 + r(A)) − ∏(1 + r(C)) per COMPLETE (12-month) holding-year cohort. Over 134 months: 2013 … 2023."""
    grouped: dict[int, list[Month]] = defaultdict(list)
    for month in months:
        grouped[holding_year(month)].append(month)
    return {
        year: math.prod(1.0 + arm[m] for m in members) - math.prod(1.0 + comparator[m] for m in members)
        for year, members in sorted(grouped.items())
        if len(members) == 12
    }


@dataclass(frozen=True)
class CohortTest:
    cohorts: int
    df: int
    mean: float
    t_stat: float
    bar: float


def cohort_t(active: Mapping[int, float], *, ruined: bool) -> CohortTest | Refused:
    """mean / (sd / √G), ddof = 1, against t_{G−1} (Bester/Conley/Hansen 2011; Cameron & Miller 2015 §VI)."""
    values = list(active.values())
    if len(values) < 2:
        return Refused(f"a cohort t needs at least 2 complete cohorts, got {len(values)}")
    if not all(math.isfinite(value) for value in values):
        return Refused("a non-finite cohort active return")
    if ruined:
        return Refused("a ruin of A or of the comparator inside the statistic months")
    spread = statistics.stdev(values)
    if spread == 0.0:
        return Refused("the cohort active returns have zero spread")
    mean = statistics.fmean(values)
    df = len(values) - 1
    return CohortTest(len(values), df, mean, mean / (spread / math.sqrt(len(values))), cohort_t_bar(df))


# --- Haircut margins -----------------------------------------------------------------------------------------


@dataclass(frozen=True)
class TotalReturns:
    """Whole-window total returns, the partial final month included."""

    arm_gross: float
    arm_net: float
    control_gross: float
    control_net: float
    complete_case_gross: float
    complete_case_net: float

    def comparator(self, which: Comparator) -> tuple[float, float]:
        if which is Comparator.CONTROL:
            return self.control_gross, self.control_net
        return self.complete_case_gross, self.complete_case_net


@dataclass(frozen=True)
class HaircutMargins:
    #: adjusted, which must be > 0.
    positive: float
    #: adjusted − the comparator's net total return, which must be > 0.
    beats_comparator: float


def haircut_margins(
    *, arm_gross: float, arm_net: float, comparator_gross: float, comparator_net: float, haircut: float
) -> HaircutMargins | Refused:
    """#2908's rule against a comparator. Ruin does not refuse it: a total return of −1 is an observed outcome."""
    if not all(math.isfinite(value) for value in (arm_gross, arm_net, comparator_gross, comparator_net)):
        return Refused("a non-finite total return")
    adjusted = haircut_net_return(
        strategy_gross=arm_gross, strategy_net=arm_net, buy_hold_gross=comparator_gross, haircut=haircut
    )
    return HaircutMargins(positive=adjusted, beats_comparator=adjusted - comparator_net)


# --- Per-policy statistics -----------------------------------------------------------------------------------


@dataclass(frozen=True)
class PolicyStatistics:
    #: mean(a), a = r(A) − r(C).
    mean_active: float | Refused
    hac: HacEstimate | Refused
    cohort: CohortTest | Refused
    #: mean(a′), a′ = r(A) − r(C′).
    diagnostic_mean: float | Refused
    margins: Mapping[tuple[Comparator, float], HaircutMargins | Refused]


def summarise_policy(
    *,
    arm: Mapping[Month, float],
    control: Mapping[Month, float],
    complete_case: Mapping[Month, float],
    totals: TotalReturns,
    ruined: frozenset[Portfolio],
    months: Sequence[Month] = STATISTIC_MONTHS,
) -> PolicyStatistics:
    """Every statistic of the spec for one termination policy, on net monthly returns.

    ``ruined`` names the books ruined inside ``months``. Each series must cover exactly ``months``: anything else is
    a runner defect and raises.
    """
    window = set(months)
    for name, series in (("arm", arm), ("control", control), ("complete_case", complete_case)):
        if set(series) != window:
            raise ValueError(f"the {name} monthly series does not cover exactly the statistic months")
    headline_ruin = bool(ruined & {Portfolio.ARM, Portfolio.CONTROL})
    diagnostic_ruin = bool(ruined & {Portfolio.ARM, Portfolio.COMPLETE_CASE})
    active = [arm[m] - control[m] for m in months]
    diagnostic = [arm[m] - complete_case[m] for m in months]
    margins: dict[tuple[Comparator, float], HaircutMargins | Refused] = {}
    for which in Comparator:
        gross, net = totals.comparator(which)
        for d in HAIRCUTS:
            margins[(which, d)] = haircut_margins(
                arm_gross=totals.arm_gross,
                arm_net=totals.arm_net,
                comparator_gross=gross,
                comparator_net=net,
                haircut=d,
            )
    return PolicyStatistics(
        mean_active=mean_active(active, ruined=headline_ruin),
        hac=hac_t(active, ruined=headline_ruin),
        cohort=cohort_t(cohort_active_returns(arm, control, months), ruined=headline_ruin),
        diagnostic_mean=mean_active(diagnostic, ruined=diagnostic_ruin),
        margins=margins,
    )


# --- Conditions, composed across policies --------------------------------------------------------------------


class Direction(StrEnum):
    #: A lower-bound predicate (> or ≥ a bar): the minimum binds.
    LOWER = "lower"
    #: An upper-bound predicate (< or ≤ a bar): the maximum binds.
    UPPER = "upper"


@dataclass(frozen=True)
class ConditionReadout:
    state: Tri
    by_policy: Mapping[str, Tri]
    #: (policy, least favourable value in the predicate's direction) over the numeric policies only; ``None`` is
    #: ``unavailable``: every policy refused.
    binding: tuple[str, float] | None
    #: policy → refusal reason.
    refusals: Mapping[str, str]


def _least_favourable(values: Mapping[str, float], direction: Direction) -> tuple[str, float]:
    """The extremum in the predicate's direction, ties to the alphabetically first policy.

    Same ordering as ``binding_policy``, which cannot be called here: it requires ``zero_recovery`` among its
    inputs, and a refused governing policy is excluded from every extremum.
    """
    sign = 1.0 if direction is Direction.LOWER else -1.0
    label = min(values, key=lambda key: (sign * values[key], key))
    return label, values[label]


def compose_condition(
    by_policy: Mapping[str, PolicyStatistics],
    evaluate: Callable[[PolicyStatistics], tuple[float, bool] | Refused],
    direction: Direction,
) -> ConditionReadout:
    """One condition under every policy: per-policy T/F/R, weak-Kleene composed, with its binding value.

    ``evaluate`` returns the policy's value and whether the predicate holds on it, or a refusal.
    """
    if ZERO_RECOVERY.label not in by_policy:
        raise ValueError("the governing zero_recovery statistics are missing")
    states: dict[str, Tri] = {}
    numeric: dict[str, float] = {}
    refusals: dict[str, str] = {}
    for label, stats in by_policy.items():
        result = evaluate(stats)
        if isinstance(result, Refused):
            states[label] = Tri.REFUSED
            refusals[label] = result.reason
        else:
            numeric[label], holds = result
            states[label] = _tri(holds)
    return ConditionReadout(
        state=weak_kleene_and(states.values()),
        by_policy=states,
        binding=_least_favourable(numeric, direction) if numeric else None,
        refusals=refusals,
    )


def _predicate(
    value: Callable[[PolicyStatistics], float | Refused], holds: Callable[[float], bool]
) -> Callable[[PolicyStatistics], tuple[float, bool] | Refused]:
    def evaluate(stats: PolicyStatistics) -> tuple[float, bool] | Refused:
        result = value(stats)
        return result if isinstance(result, Refused) else (result, holds(result))

    return evaluate


def _cohort_beats_bar(stats: PolicyStatistics) -> tuple[float, bool] | Refused:
    cohort = stats.cohort
    return cohort if isinstance(cohort, Refused) else (cohort.t_stat, cohort.t_stat > cohort.bar)


def _margin(which: Comparator, haircut: float, field: str) -> Callable[[PolicyStatistics], float | Refused]:
    def value(stats: PolicyStatistics) -> float | Refused:
        margins = stats.margins[(which, haircut)]
        return margins if isinstance(margins, Refused) else getattr(margins, field)

    return value


class Condition(StrEnum):
    BONFERRONI = "bonferroni"
    COHORT = "cohort_t"
    DIAGNOSTIC_MEAN = "diagnostic_mean"
    UNDERPERFORMS = "underperforms_monthly_hac"
    MEAN_NOT_POSITIVE = "mean_active_not_positive"


MARGIN_FIELDS: Final = ("positive", "beats_comparator")


def margin_condition(which: Comparator, haircut: float, field: str) -> str:
    return f"haircut_{haircut}_{field}_{which.name.lower()}"


def pass_conditions(haircut: float) -> tuple[str, ...]:
    """P(d): pass(d)'s two margins for C and for C′."""
    return tuple(margin_condition(which, haircut, field) for which in Comparator for field in MARGIN_FIELDS)


#: Conditions whose failure or refusal withholds a pass. The label conditions (underperformance, mean ≤ 0) are not.
GATING_CONDITIONS: Final = (
    Condition.BONFERRONI,
    Condition.COHORT,
    Condition.DIAGNOSTIC_MEAN,
    *pass_conditions(CONTINGENT_HAIRCUT),
    *pass_conditions(ROBUST_HAIRCUT),
)


def evaluate_conditions(by_policy: Mapping[str, PolicyStatistics], family: int) -> dict[str, ConditionReadout]:
    """Every verdict condition across the programme policies.

    An invalid ``family`` refuses both Bonferroni conditions under every policy (the table's "M is not a positive
    integer"). Each is evaluated literally as t > 3 and p ≤ α/M (or t < −3 and p ≤ α/M), never against a rounded t_M.
    """
    valid = _valid_family(family)
    level = FAMILY_ALPHA / family if valid else math.nan

    def t_stat(stats: PolicyStatistics) -> float | Refused:
        if not valid:
            return Refused(f"the family size is not a positive integer: {family!r}")
        return stats.hac if isinstance(stats.hac, Refused) else stats.hac.t_stat

    conditions: dict[str, ConditionReadout] = {
        Condition.BONFERRONI: compose_condition(
            by_policy, _predicate(t_stat, lambda t: t > HAC_T_BAR and two_sided_p(t) <= level), Direction.LOWER
        ),
        Condition.COHORT: compose_condition(by_policy, _cohort_beats_bar, Direction.LOWER),
        Condition.DIAGNOSTIC_MEAN: compose_condition(
            by_policy, _predicate(lambda s: s.diagnostic_mean, lambda m: m > 0.0), Direction.LOWER
        ),
        Condition.UNDERPERFORMS: compose_condition(
            by_policy, _predicate(t_stat, lambda t: t < -HAC_T_BAR and two_sided_p(t) <= level), Direction.UPPER
        ),
        Condition.MEAN_NOT_POSITIVE: compose_condition(
            by_policy, _predicate(lambda s: s.mean_active, lambda m: m <= 0.0), Direction.UPPER
        ),
    }
    for which in Comparator:
        for d in HAIRCUTS:
            for field in MARGIN_FIELDS:
                conditions[margin_condition(which, d, field)] = compose_condition(
                    by_policy, _predicate(_margin(which, d, field), lambda margin: margin > 0.0), Direction.LOWER
                )
    return conditions


# --- Verdict -------------------------------------------------------------------------------------------------


class Verdict(StrEnum):
    REFUSED_PRE_GATE = "REFUSED_PRE_GATE"
    GATE_FAIL = "GATE_FAIL"
    SIMULATOR_INVARIANT = "FAIL(simulator_invariant)"
    PASS_ROBUST = "PASS_ROBUST"
    PASS_CONTINGENT = "PASS_CONTINGENT"
    NOT_PASS_REFUSED = "NOT_PASS_REFUSED"
    UNDERPERFORMS_CONTROL_MONTHLY_HAC = "UNDERPERFORMS_CONTROL_MONTHLY_HAC"
    FAIL_CONTROL_MONTHLY = "FAIL_CONTROL_MONTHLY"
    FAIL_ECONOMIC = "FAIL_ECONOMIC"
    FAIL_DIAGNOSTIC = "FAIL_DIAGNOSTIC"
    UNDETERMINED_AT_THIS_POWER = "UNDETERMINED_AT_THIS_POWER"
    NOT_PASS_MIXED = "NOT_PASS_MIXED"


@dataclass(frozen=True)
class VerdictResult:
    verdict: Verdict
    conditions: Mapping[str, ConditionReadout]
    #: Gating conditions that are false, then those refused, each in ``GATING_CONDITIONS`` order.
    failed: tuple[str, ...]
    refused: tuple[str, ...]


def _state(conditions: Mapping[str, ConditionReadout], names: Iterable[str]) -> Tri:
    return weak_kleene_and(conditions[name].state for name in names)


def decide_verdict(
    conditions: Mapping[str, ConditionReadout] | None,
    *,
    refused_pre_gate: bool,
    gate: Tri,
    simulator_failed: bool,
) -> VerdictResult:
    """The spec's verdict lines in order; the first match is the verdict.

    ``refused_pre_gate``, ``gate`` and ``simulator_failed`` describe the run AFTER the correction rule was applied
    (a correction unavailable, declined or used up). At this boundary a refused gate is a failed one.
    """
    if refused_pre_gate:
        return VerdictResult(Verdict.REFUSED_PRE_GATE, {}, (), ())
    if gate is not Tri.TRUE:
        return VerdictResult(Verdict.GATE_FAIL, {}, (), ())
    if simulator_failed:
        return VerdictResult(Verdict.SIMULATOR_INVARIANT, {}, (), ())
    if conditions is None:
        raise ValueError("a passed gate and a clean run must supply the evaluated conditions")

    failed = tuple(name for name in GATING_CONDITIONS if conditions[name].state is Tri.FALSE)
    refused = tuple(name for name in GATING_CONDITIONS if conditions[name].state is Tri.REFUSED)

    def result(verdict: Verdict) -> VerdictResult:
        return VerdictResult(verdict, conditions, failed, refused)

    significance = (Condition.BONFERRONI, Condition.COHORT)
    base = _state(conditions, (*significance, Condition.DIAGNOSTIC_MEAN))
    contingent = _state(conditions, pass_conditions(CONTINGENT_HAIRCUT))
    robust = _state(conditions, pass_conditions(ROBUST_HAIRCUT))
    if base is Tri.TRUE and robust is Tri.TRUE:
        return result(Verdict.PASS_ROBUST)
    if base is Tri.TRUE and contingent is Tri.TRUE and robust is Tri.FALSE:
        return result(Verdict.PASS_CONTINGENT)
    if refused:
        return result(Verdict.NOT_PASS_REFUSED)
    if conditions[Condition.UNDERPERFORMS].state is Tri.TRUE:
        return result(Verdict.UNDERPERFORMS_CONTROL_MONTHLY_HAC)
    if conditions[Condition.MEAN_NOT_POSITIVE].state is Tri.TRUE:
        return result(Verdict.FAIL_CONTROL_MONTHLY)
    if base is Tri.TRUE and contingent is Tri.FALSE:
        return result(Verdict.FAIL_ECONOMIC)
    if _state(conditions, significance) is Tri.TRUE and conditions[Condition.DIAGNOSTIC_MEAN].state is Tri.FALSE:
        return result(Verdict.FAIL_DIAGNOSTIC)
    # Literal: P(0.58) is a gating condition too, so a significance failure beside a failed 0.58 haircut is mixed.
    if set(failed) <= set(significance):
        return result(Verdict.UNDETERMINED_AT_THIS_POWER)
    return result(Verdict.NOT_PASS_MIXED)


__all__ = [
    "CONTINGENT_HAIRCUT",
    "FAMILY_ALPHA",
    "FIRST_RUN_ROWS",
    "GATING_CONDITIONS",
    "HAC_T_BAR",
    "HAIRCUTS",
    "REFERENCE_MEMBER",
    "RESERVED_CORRECTION_ROWS",
    "ROBUST_HAIRCUT",
    "STATISTIC_MONTHS",
    "CohortTest",
    "Comparator",
    "Condition",
    "ConditionReadout",
    "Direction",
    "GateReadout",
    "GateRefusal",
    "HacEstimate",
    "HaircutMargins",
    "PolicyStatistics",
    "Portfolio",
    "Refused",
    "TotalReturns",
    "Tri",
    "Verdict",
    "VerdictResult",
    "bonferroni_t",
    "cohort_active_returns",
    "cohort_t",
    "cohort_t_bar",
    "compose_condition",
    "compose_gate",
    "decide_verdict",
    "evaluate_conditions",
    "family_size",
    "hac_t",
    "haircut_margins",
    "holding_year",
    "identity_gate",
    "margin_condition",
    "mean_active",
    "newey_west_lag",
    "pass_conditions",
    "read_global_q_gpa",
    "student_t_cdf",
    "student_t_quantile",
    "summarise_policy",
    "two_sided_p",
]
