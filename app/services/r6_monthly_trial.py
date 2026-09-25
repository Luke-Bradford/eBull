"""#2901 quality arm: the monthly trial's identity gate, statistics and verdict (PR B, part 1).

Spec: ``docs/proposals/ta/2026-09-25-2901-quality-declaration-spec.md`` ("Identity gate", "Statistics", "Verdict").
Everything here is pure and consumes monthly series keyed by ``(year, month)``, so it is fixed and tested before
any return exists. The simulator (``simulate_monthly``) and the sealed runner are part 2.

``r6_exclusion_trial`` is NOT edited: its sha256 is in #2908's result identity and in #3362's
``termination_identity``. Its public primitives are imported, never re-implemented.
"""

from __future__ import annotations

import csv
import dataclasses
import io
import math
import statistics
import zipfile
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Final

from app.services.deflated_sharpe import MIN_MEASURED_TRIALS, DeflatedSharpeResult, deflated_sharpe, trade_moments
from app.services.r6_exclusion_trial import ZERO_RECOVERY, binding_policy, haircut_net_return, month_pairs
from app.services.r6_exclusion_trial import validate_factor as _validate_factor
from app.services.strategy_result import DSR_PROMOTION_THRESHOLD

Month = tuple[int, int]

REFERENCE_MEMBER: Final = "prof_monthly_2025/portf_gpa_monthly_2025.csv"
REFERENCE_HEADER: Final = ["year", "month", "rank_GPA", "nstocks", "ret_vw"]

#: 2013-07 … 2024-08: the 134 full months. The partial 2024-09 enters total returns only.
STATISTIC_MONTHS: Final = month_pairs((2013, 7), (2024, 8))

#: Harvey/Liu/Zhu. Also the |z| whose two-sided tail probability the cohort t₁₀ bar matches.
HAC_T_BAR: Final = 3.0
ROBUST_HAIRCUT: Final = 0.58
CONTINGENT_HAIRCUT: Final = 0.15
HAIRCUTS: Final = (CONTINGENT_HAIRCUT, ROBUST_HAIRCUT)

#: #2908's three arms (dilution, filing-risk, union) + #2901's headline and C′ diagnostic.
BASE_DECLARED_TRIALS: Final = 5
#: Monthly axis, distinct from ``c6-deflated-sharpe-v1`` (trade axis).
MONTHLY_DSR_MODEL_ID: Final = "quality-2901-monthly-dsr-v1"
DSR_BAR: Final = float(DSR_PROMOTION_THRESHOLD)

_NORMAL: Final = statistics.NormalDist()


class GateRefusal(RuntimeError):
    """The identity gate could not be evaluated: calendar, key or unit misalignment. Arm outcomes stay sealed."""


# --- Identity gate -------------------------------------------------------------------------------------------


def read_global_q_gpa(path: Path) -> dict[Month, float]:
    """Rank 10 minus rank 1 ``ret_vw`` per month, as a fraction.

    Refuses a wrong header, a duplicate (month, rank), and any rank-1 or rank-10 value that is non-finite or has
    ``|ret| >= 1`` after ÷ 100 — a percent/fraction confusion fails that check.
    """
    legs: dict[Month, dict[int, float]] = defaultdict(dict)
    with zipfile.ZipFile(path) as archive, archive.open(REFERENCE_MEMBER) as raw:
        reader = csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8-sig"))
        if reader.fieldnames != REFERENCE_HEADER:
            raise GateRefusal(f"unexpected global-q GP/A header: {reader.fieldnames}")
        for row in reader:
            key = (int(row["year"]), int(row["month"]))
            rank = int(row["rank_GPA"])
            if rank in legs[key]:
                raise GateRefusal(f"duplicate global-q GP/A rank {rank} at {key}")
            value = float(row["ret_vw"]) / 100.0
            if rank in (1, 10) and not (math.isfinite(value) and abs(value) < 1.0):
                raise GateRefusal(f"global-q GP/A rank {rank} at {key} is {value} after ÷ 100 — not a monthly return")
            legs[key][rank] = value
    return {key: ranks[10] - ranks[1] for key, ranks in legs.items() if 1 in ranks and 10 in ranks}


@dataclass(frozen=True)
class GateReadout:
    """What the gate publishes. ⚠ No leg mean, alpha or cumulative figure: those carry arm information."""

    months: int
    correlation: float
    beta: float
    lag_correlation: float
    lead_correlation: float
    passed: bool


def identity_gate(
    ours: Mapping[Month, float],
    reference: Mapping[Month, float],
    months: Sequence[Month] = STATISTIC_MONTHS,
) -> GateReadout:
    """#2908's frozen rule on an exactly aligned calendar.

    ``ours`` must cover exactly ``months``, and ``reference`` restricted to the window must too: a missing or
    extra key refuses. Only then is ``validate_factor`` called, whose key intersection is then the identity and
    whose positional lag/lead shift therefore lines up with the months.
    """
    expected = tuple(months)
    if len(set(expected)) != len(expected) or list(expected) != sorted(expected):
        raise GateRefusal("the gate calendar must be strictly increasing with no duplicate month")
    window = set(expected)
    if set(ours) != window:
        raise GateRefusal(
            f"our spread months differ from the calendar: missing {sorted(window - set(ours))[:3]}, "
            f"extra {sorted(set(ours) - window)[:3]}"
        )
    in_window = {key for key in reference if expected[0] <= key <= expected[-1]}
    if in_window != window:
        raise GateRefusal(
            f"reference months differ from the calendar: missing {sorted(window - in_window)[:3]}, "
            f"extra {sorted(in_window - window)[:3]}"
        )
    for side, series in (("our", ours), ("reference", reference)):
        if not all(math.isfinite(series[key]) for key in expected):
            raise GateRefusal(f"the {side} spread has a non-finite month")
    validation = _validate_factor({key: ours[key] for key in expected}, {key: reference[key] for key in expected})
    return GateReadout(
        months=validation.months,
        correlation=validation.correlation,
        beta=validation.beta,
        lag_correlation=validation.lag_correlation,
        lead_correlation=validation.lead_correlation,
        passed=validation.passed,
    )


def gate_passed(readouts: Mapping[str, GateReadout]) -> bool:
    """A conjunct over every policy: any policy can only refuse. ``zero_recovery`` must be present."""
    if ZERO_RECOVERY.label not in readouts:
        raise ValueError("the governing zero_recovery gate readout is missing")
    return all(readout.passed for readout in readouts.values())


# --- HAC t (headline) ----------------------------------------------------------------------------------------


def newey_west_lag(observations: int) -> int:
    """Newey & West (1994) rule of thumb ⌊4(T/100)^{2/9}⌋, R ``sandwich::NeweyWest`` "NW1994". 4 at T = 134."""
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
    #: T·γ̂₀/Ω̂, capped at T. The DSR's T, so dependence is counted once, from the same estimate.
    effective_size: float


def hac_t(series: Sequence[float]) -> HacEstimate | None:
    """mean / Newey–West SE. ``None`` when γ̂₀ or Ω̂ is not positive (a constant series): no t exists."""
    count = len(series)
    lag = newey_west_lag(count)
    mean = math.fsum(series) / count
    centred = [value - mean for value in series]

    def autocovariance(shift: int) -> float:
        return math.fsum(centred[t] * centred[t - shift] for t in range(shift, count)) / count

    variance = autocovariance(0)
    long_run = variance + 2.0 * math.fsum(
        (1.0 - shift / (lag + 1)) * autocovariance(shift) for shift in range(1, lag + 1)
    )
    if variance <= 0.0 or long_run <= 0.0:
        return None
    standard_error = math.sqrt(long_run / count)
    return HacEstimate(
        observations=count,
        lag=lag,
        mean=mean,
        variance=variance,
        long_run_variance=long_run,
        standard_error=standard_error,
        t_stat=mean / standard_error,
        effective_size=min(float(count), count * variance / long_run),
    )


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
    """Inverse of :func:`student_t_cdf` by bisection."""
    if not 0.0 < probability < 1.0:
        raise ValueError(f"a quantile needs a probability in (0, 1), got {probability}")
    if probability < 0.5:
        return -student_t_quantile(1.0 - probability, df)
    low, high = 0.0, 1.0
    while student_t_cdf(high, df) < probability:
        high *= 2.0
    for _ in range(200):
        middle = 0.5 * (low + high)
        if student_t_cdf(middle, df) < probability:
            low = middle
        else:
            high = middle
    return 0.5 * (low + high)


def cohort_t_bar(df: int) -> float:
    """The t_ν quantile at the same two-sided tail probability as |z| = 3, i.e. p = 2(1 − Φ(3))."""
    return student_t_quantile(_NORMAL.cdf(HAC_T_BAR), df)


def holding_year(month: Month) -> int:
    """The formation year D of the holding-year cohort July(D) … June(D + 1) containing ``month``."""
    year, number = month
    return year if number >= 7 else year - 1


def cohort_active_returns(
    arm: Mapping[Month, float],
    control: Mapping[Month, float],
    months: Sequence[Month] = STATISTIC_MONTHS,
) -> dict[int, float]:
    """∏(1 + r(A)) − ∏(1 + r(C)) per COMPLETE (12-month) holding-year cohort. Over 134 months: 2013 … 2023."""
    grouped: dict[int, list[Month]] = defaultdict(list)
    for month in months:
        grouped[holding_year(month)].append(month)
    return {
        year: math.prod(1.0 + arm[m] for m in members) - math.prod(1.0 + control[m] for m in members)
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
    passed: bool


def cohort_t(active: Mapping[int, float]) -> CohortTest | None:
    """mean / (sd / √G), ddof = 1, against t_{G−1} (Bester/Conley/Hansen 2011; Cameron & Miller 2015 §VI).

    ``None`` when the cohorts have zero spread: no t exists, and the conjunct refuses.
    """
    values = list(active.values())
    if len(values) < 2:
        raise ValueError(f"a cohort t needs at least 2 cohorts, got {len(values)}")
    spread = statistics.stdev(values)
    if spread <= 0.0:
        return None
    mean = statistics.fmean(values)
    t_stat = mean / (spread / math.sqrt(len(values)))
    bar = cohort_t_bar(len(values) - 1)
    return CohortTest(len(values), len(values) - 1, mean, t_stat, bar, t_stat > bar)


# --- Deflated Sharpe (monthly axis) --------------------------------------------------------------------------


def declared_trials(corrections: int) -> int:
    """M = 5 + corrections. Each identity-gate or construction correction adds one trial."""
    if corrections < 0:
        raise ValueError(f"corrections cannot be negative, got {corrections}")
    return BASE_DECLARED_TRIALS + corrections


def monthly_dsr(
    active: Sequence[float],
    *,
    effective_size: float,
    same_window_sharpes: Sequence[float],
    declared_trials: int,
    trial_register_version: str,
) -> DeflatedSharpeResult | None:
    """Bailey & López de Prado (2014) on the monthly active series, ρ = 0 (so N = M).

    V[{SR_n}] = max(1/(T − 1), ddof = 1 variance of the same-window measured SRs), with T the HAC effective size.
    The floor is equation (2)'s own sampling variance of an SR under H0, so a near-identical pair of measured
    trials cannot collapse SR₀ toward zero. ``None`` in any of ``deflated_sharpe``'s refusal states — never a pass.
    """
    if len(same_window_sharpes) < MIN_MEASURED_TRIALS or not effective_size > 1.0:
        return None
    moments = trade_moments([100.0 * value for value in active])
    if moments is None:
        return None
    variance = max(1.0 / (effective_size - 1.0), statistics.variance(same_window_sharpes))
    result = deflated_sharpe(
        moments,
        effective_sample_size=effective_size,
        trial_sharpe_variance=variance,
        declared_trials=declared_trials,
        average_correlation=0.0,
        measured_trials=len(same_window_sharpes),
        trial_register_version=trial_register_version,
    )
    return None if result is None else dataclasses.replace(result, model_id=MONTHLY_DSR_MODEL_ID)


# --- Haircut bars --------------------------------------------------------------------------------------------


def haircut_pass(*, arm_gross: float, arm_net: float, control_gross: float, control_net: float, haircut: float) -> bool:
    """#2908's rule against the control: adjusted > 0 and adjusted > the control's net total return."""
    adjusted = haircut_net_return(
        strategy_gross=arm_gross, strategy_net=arm_net, buy_hold_gross=control_gross, haircut=haircut
    )
    return adjusted > 0.0 and adjusted > control_net


# --- Per-policy summary and verdict --------------------------------------------------------------------------


@dataclass(frozen=True)
class TotalReturns:
    """Whole-window total returns, the partial final month included."""

    arm_gross: float
    arm_net: float
    control_gross: float
    control_net: float
    complete_case_gross: float
    complete_case_net: float


@dataclass(frozen=True)
class PolicyStatistics:
    mean_active: float
    hac: HacEstimate | None
    cohort: CohortTest | None
    dsr: DeflatedSharpeResult | None
    haircut_pass: Mapping[float, bool]
    complete_case_mean_active: float
    complete_case_haircut_pass: Mapping[float, bool]


def summarise_policy(
    *,
    arm: Mapping[Month, float],
    control: Mapping[Month, float],
    complete_case: Mapping[Month, float],
    totals: TotalReturns,
    corrections: int,
    trial_register_version: str,
    months: Sequence[Month] = STATISTIC_MONTHS,
) -> PolicyStatistics:
    """Every statistic of the spec for one termination policy. Each series must cover exactly ``months``."""
    window = set(months)
    for name, series in (("arm", arm), ("control", control), ("complete_case", complete_case)):
        if set(series) != window:
            raise ValueError(f"the {name} monthly series does not cover exactly the statistic months")
    active = [arm[m] - control[m] for m in months]
    diagnostic = [arm[m] - complete_case[m] for m in months]
    hac = hac_t(active)
    headline_moments = trade_moments([100.0 * value for value in active])
    diagnostic_moments = trade_moments([100.0 * value for value in diagnostic])
    dsr = None
    if hac is not None and headline_moments is not None and diagnostic_moments is not None:
        dsr = monthly_dsr(
            active,
            effective_size=hac.effective_size,
            same_window_sharpes=(headline_moments.sharpe, diagnostic_moments.sharpe),
            declared_trials=declared_trials(corrections),
            trial_register_version=trial_register_version,
        )
    return PolicyStatistics(
        mean_active=statistics.fmean(active),
        hac=hac,
        cohort=cohort_t(cohort_active_returns(arm, control, months)),
        dsr=dsr,
        haircut_pass={
            d: haircut_pass(
                arm_gross=totals.arm_gross,
                arm_net=totals.arm_net,
                control_gross=totals.control_gross,
                control_net=totals.control_net,
                haircut=d,
            )
            for d in HAIRCUTS
        },
        complete_case_mean_active=statistics.fmean(diagnostic),
        complete_case_haircut_pass={
            d: haircut_pass(
                arm_gross=totals.arm_gross,
                arm_net=totals.arm_net,
                control_gross=totals.complete_case_gross,
                control_net=totals.complete_case_net,
                haircut=d,
            )
            for d in HAIRCUTS
        },
    )


class Verdict(StrEnum):
    GATE_FAIL = "GATE_FAIL"
    SIMULATOR_INVARIANT = "FAIL(simulator_invariant)"
    PASS_ROBUST = "PASS_ROBUST"
    PASS_CONTINGENT = "PASS_CONTINGENT"
    UNDERPERFORMS_CONTROL = "UNDERPERFORMS_CONTROL"
    FAIL_CONTROL = "FAIL_CONTROL"
    UNDETERMINED_AT_THIS_POWER = "UNDETERMINED_AT_THIS_POWER"


@dataclass(frozen=True)
class VerdictResult:
    verdict: Verdict
    #: statistic → (binding policy, least favourable value). ``None`` value = a refusal under that policy.
    binding: Mapping[str, tuple[str, float | None]]


def _binding(values: Mapping[str, float | None]) -> tuple[str, float | None]:
    """The least favourable value across policies; a refusal (``None``) under any policy binds.

    Refusals carry no magnitude to rank, so among several the reported label is the alphabetically first. That
    choice only names a provenance; the verdict is the same whichever refused policy is named.
    """
    if ZERO_RECOVERY.label not in values:
        raise ValueError("the governing zero_recovery statistics are missing")
    refused = sorted(label for label, value in values.items() if value is None)
    if refused:
        return refused[0], None
    return binding_policy({label: value for label, value in values.items() if value is not None})


def decide_verdict(
    by_policy: Mapping[str, PolicyStatistics] | None,
    *,
    gate_passed: bool,
    simulator_failed: bool,
) -> VerdictResult:
    """The spec's verdict lines in order; the first match is the verdict. A policy can only turn a pass into a fail."""
    if not gate_passed:
        return VerdictResult(Verdict.GATE_FAIL, {})
    if simulator_failed:
        return VerdictResult(Verdict.SIMULATOR_INVARIANT, {})
    if by_policy is None:
        raise ValueError("a passed gate and a clean run must supply per-policy statistics")

    binding = {
        "hac_t": _binding({k: None if s.hac is None else s.hac.t_stat for k, s in by_policy.items()}),
        "cohort_t": _binding({k: None if s.cohort is None else s.cohort.t_stat for k, s in by_policy.items()}),
        "dsr": _binding({k: None if s.dsr is None else s.dsr.deflated_sharpe for k, s in by_policy.items()}),
        "mean_active": _binding({k: s.mean_active for k, s in by_policy.items()}),
        "complete_case_mean_active": _binding({k: s.complete_case_mean_active for k, s in by_policy.items()}),
    }
    policies = list(by_policy.values())
    core = (
        all(s.hac is not None and s.hac.t_stat > HAC_T_BAR for s in policies)
        and all(s.cohort is not None and s.cohort.passed for s in policies)
        and all(s.dsr is not None and s.dsr.deflated_sharpe > DSR_BAR for s in policies)
        and all(s.complete_case_mean_active > 0.0 for s in policies)
    )

    def haircuts_hold(d: float) -> bool:
        return all(s.haircut_pass[d] and s.complete_case_haircut_pass[d] for s in policies)

    if core and haircuts_hold(ROBUST_HAIRCUT):
        return VerdictResult(Verdict.PASS_ROBUST, binding)
    if core and haircuts_hold(CONTINGENT_HAIRCUT):
        return VerdictResult(Verdict.PASS_CONTINGENT, binding)
    hac_value = binding["hac_t"][1]
    if hac_value is not None and hac_value < -HAC_T_BAR:
        return VerdictResult(Verdict.UNDERPERFORMS_CONTROL, binding)
    mean_value = binding["mean_active"][1]
    if mean_value is not None and mean_value <= 0.0:
        return VerdictResult(Verdict.FAIL_CONTROL, binding)
    return VerdictResult(Verdict.UNDETERMINED_AT_THIS_POWER, binding)


__all__ = [
    "BASE_DECLARED_TRIALS",
    "CONTINGENT_HAIRCUT",
    "DSR_BAR",
    "HAC_T_BAR",
    "HAIRCUTS",
    "MONTHLY_DSR_MODEL_ID",
    "REFERENCE_MEMBER",
    "ROBUST_HAIRCUT",
    "STATISTIC_MONTHS",
    "CohortTest",
    "GateReadout",
    "GateRefusal",
    "HacEstimate",
    "PolicyStatistics",
    "TotalReturns",
    "Verdict",
    "VerdictResult",
    "cohort_active_returns",
    "cohort_t",
    "cohort_t_bar",
    "declared_trials",
    "decide_verdict",
    "gate_passed",
    "hac_t",
    "haircut_pass",
    "holding_year",
    "identity_gate",
    "monthly_dsr",
    "newey_west_lag",
    "read_global_q_gpa",
    "student_t_cdf",
    "student_t_quantile",
    "summarise_policy",
]
