"""Pure evaluator for the frozen MT-1 four-arm controlled trial (#2437).

This module does not load prices, query the result ledger, or authorise an
outcome read.  It accepts four already-built, after-cost monthly return books
and applies the estimand frozen in
``docs/proposals/ta/2026-08-15-mt1-volatility-managed-relative-strength-
preregistration.md``.  Keeping the statistic here allows the implementation to
be reviewed before the trial register and sealed-outcome gate are opened.

The primary comparison is a difference in differences: volatility scaling's
CER improvement on MT-1 less the same improvement on the S-8 negative control.
All four arms use identical resampled month indices in every replication.
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Final, cast

import numpy as np
import numpy.typing as npt

TRIAL_ID: Final = "mt1-capped-volatility-managed-relative-strength-v1"
NEGATIVE_CONTROL_TRIAL_ID: Final = "mt1-s8-capped-volatility-negative-control-v1"
RISK_AVERSION: Final = 5.0
BLOCK_LENGTH_MONTHS: Final = 12
BOOTSTRAP_RESAMPLES: Final = 10_000
BOOTSTRAP_SEED: Final = 243_715_082_026
CONFIDENCE: Final = 0.95
MIN_COMMON_MONTHS: Final = 120
MAX_ANNUALISED_TURNOVER: Final = 6.0
EXPECTED_SHORTFALL_TAIL: Final = 0.05
_BOOTSTRAP_BATCH: Final = 200


def _code_hash() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


TRIAL_EVALUATOR_VERSION: Final = f"mt1-four-arm-cer-mbb-v1+{_code_hash()}"


class MT1TrialRefused(ValueError):
    """The frozen trial cannot be evaluated from the supplied evidence."""


@dataclass(frozen=True)
class MonthlyReturn:
    """One complete calendar month's after-cost decimal portfolio return."""

    month: date
    value: float


@dataclass(frozen=True)
class PortfolioArm:
    """One of the four frozen arms, on its complete-month outcome axis."""

    monthly_returns: tuple[MonthlyReturn, ...]


@dataclass(frozen=True)
class ScaledBookStructuralAudit:
    """Outcome-free proof required for each scaled book before statistics.

    ``decision_dates`` are the dates on which exposure changed or was
    reaffirmed. They must equal the evaluator-supplied frozen first-session
    monthly clock;
    a subset is not accepted because silently omitting a decision can suppress
    turnover. ``annualised_turnover`` is a decimal multiple (``6`` = 600%).
    """

    decision_dates: tuple[date, ...]
    expected_decision_dates: tuple[date, ...]
    annualised_turnover: float
    traded_notional: float
    exposure_reconciled: bool


@dataclass(frozen=True)
class StructuralGateReport:
    mt1_decision_dates: int
    s8_decision_dates: int
    mt1_annualised_turnover: float
    s8_annualised_turnover: float
    mt1_traded_notional: float
    s8_traded_notional: float


@dataclass(frozen=True)
class PercentileInterval:
    low: float
    high: float


@dataclass(frozen=True)
class ArmRiskReport:
    certainty_equivalent: float
    maximum_drawdown: float
    expected_shortfall_5: float


@dataclass(frozen=True)
class MT1TrialResult:
    """The frozen historical statistic, not a promotion or capital decision."""

    common_months: tuple[date, ...]
    excluded_months_by_arm: tuple[int, int, int, int]
    structural: StructuralGateReport
    mt1_scaled: ArmRiskReport
    mt1_unscaled: ArmRiskReport
    s8_scaled: ArmRiskReport
    s8_unscaled: ArmRiskReport
    mt1_delta_cer: float
    s8_delta_cer: float
    primary_difference_in_differences: float
    mt1_delta_interval: PercentileInterval
    primary_interval: PercentileInterval
    primary_lower_bound_positive: bool
    mt1_lower_bound_positive: bool
    mt1_drawdown_improved: bool
    mt1_expected_shortfall_improved: bool
    historical_statistical_conjuncts_pass: bool
    evaluator_version: str = TRIAL_EVALUATOR_VERSION
    bootstrap_block_length: int = BLOCK_LENGTH_MONTHS
    bootstrap_resamples: int = BOOTSTRAP_RESAMPLES
    bootstrap_seed: int = BOOTSTRAP_SEED


def _validate_structural_audit(label: str, audit: ScaledBookStructuralAudit) -> None:
    if not audit.expected_decision_dates:
        raise MT1TrialRefused(f"{label} frozen first-session monthly decision clock is empty")
    if tuple(sorted(set(audit.expected_decision_dates))) != audit.expected_decision_dates:
        raise MT1TrialRefused(f"{label} expected decision dates must be sorted and unique")
    if tuple(sorted(set(audit.decision_dates))) != audit.decision_dates:
        raise MT1TrialRefused(f"{label} decision dates must be sorted and unique")
    if audit.decision_dates != audit.expected_decision_dates:
        raise MT1TrialRefused(f"{label} decisions do not equal the frozen first-session monthly clock")
    if not math.isfinite(audit.annualised_turnover) or audit.annualised_turnover < 0:
        raise MT1TrialRefused(f"{label} annualised turnover is not finite and non-negative")
    if audit.annualised_turnover > MAX_ANNUALISED_TURNOVER:
        raise MT1TrialRefused(f"{label} annualised turnover exceeds 600%")
    if not math.isfinite(audit.traded_notional) or audit.traded_notional < 0:
        raise MT1TrialRefused(f"{label} traded notional is not finite and non-negative")
    if not audit.exposure_reconciled:
        raise MT1TrialRefused(f"{label} exposure changes do not reconcile to the frozen formula")


def structural_gate(
    mt1_scaled: ScaledBookStructuralAudit,
    s8_scaled: ScaledBookStructuralAudit,
) -> StructuralGateReport:
    """Apply the turnover/clock/reconciliation gate without return inputs."""
    _validate_structural_audit("MT-1 scaled", mt1_scaled)
    _validate_structural_audit("S-8 scaled", s8_scaled)
    if mt1_scaled.expected_decision_dates != s8_scaled.expected_decision_dates:
        raise MT1TrialRefused("MT-1 and S-8 do not share the same frozen first-session monthly exposure clock")
    return StructuralGateReport(
        mt1_decision_dates=len(mt1_scaled.decision_dates),
        s8_decision_dates=len(s8_scaled.decision_dates),
        mt1_annualised_turnover=mt1_scaled.annualised_turnover,
        s8_annualised_turnover=s8_scaled.annualised_turnover,
        mt1_traded_notional=mt1_scaled.traded_notional,
        s8_traded_notional=s8_scaled.traded_notional,
    )


def _arm_map(label: str, arm: PortfolioArm) -> dict[date, float]:
    months = tuple(point.month for point in arm.monthly_returns)
    if tuple(sorted(set(months))) != months:
        raise MT1TrialRefused(f"{label} months must be sorted and unique")
    values: dict[date, float] = {}
    for point in arm.monthly_returns:
        if point.month.day != 1:
            raise MT1TrialRefused(f"{label} month keys must be first calendar days")
        if not math.isfinite(point.value) or point.value < -1.0:
            raise MT1TrialRefused(f"{label} contains a non-finite or below-minus-one monthly return")
        values[point.month] = point.value
    return values


def certainty_equivalent(values: npt.NDArray[np.float64]) -> float:
    if values.ndim != 1 or len(values) < 2:
        raise MT1TrialRefused("certainty equivalent needs at least two monthly returns")
    with np.errstate(over="ignore", invalid="ignore"):
        result = float(np.mean(values) - (RISK_AVERSION / 2.0) * np.var(values, ddof=1))
    if not math.isfinite(result):
        raise MT1TrialRefused("certainty equivalent is not finite")
    return result


def maximum_drawdown(values: npt.NDArray[np.float64]) -> float:
    """Return conventional non-negative peak-to-trough drawdown magnitude."""
    with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
        wealth = np.cumprod(1.0 + values)
    if not np.all(np.isfinite(wealth)):
        raise MT1TrialRefused("monthly return path produces non-finite wealth")
    peaks = np.maximum.accumulate(np.concatenate((np.asarray([1.0]), wealth)))
    drawdowns = 1.0 - wealth / peaks[1:]
    return max(0.0, float(np.max(drawdowns, initial=0.0)))


def expected_shortfall_5(values: npt.NDArray[np.float64]) -> float:
    if not len(values):
        raise MT1TrialRefused("expected shortfall needs at least one monthly return")
    tail_count = max(1, math.ceil(len(values) * EXPECTED_SHORTFALL_TAIL))
    return float(np.mean(np.sort(values)[:tail_count]))


def _risk_report(values: npt.NDArray[np.float64]) -> ArmRiskReport:
    return ArmRiskReport(
        certainty_equivalent=certainty_equivalent(values),
        maximum_drawdown=maximum_drawdown(values),
        expected_shortfall_5=expected_shortfall_5(values),
    )


def _paired_bootstrap(
    arms: npt.NDArray[np.float64],
) -> tuple[PercentileInterval, PercentileInterval]:
    month_count = arms.shape[1]
    blocks_per_sample = math.ceil(month_count / BLOCK_LENGTH_MONTHS)
    possible_starts = month_count - BLOCK_LENGTH_MONTHS + 1
    if possible_starts < 1:  # pragma: no cover - guarded by MIN_COMMON_MONTHS
        raise MT1TrialRefused("common month axis is shorter than the frozen block length")

    generator = np.random.default_rng(BOOTSTRAP_SEED)
    mt1_statistics = np.empty(BOOTSTRAP_RESAMPLES, dtype=np.float64)
    primary_statistics = np.empty(BOOTSTRAP_RESAMPLES, dtype=np.float64)
    offsets = np.arange(BLOCK_LENGTH_MONTHS, dtype=np.int64)
    written = 0
    while written < BOOTSTRAP_RESAMPLES:
        batch_size = min(_BOOTSTRAP_BATCH, BOOTSTRAP_RESAMPLES - written)
        starts = generator.integers(0, possible_starts, size=(batch_size, blocks_per_sample), dtype=np.int64)
        indices = (starts[:, :, None] + offsets[None, None, :]).reshape(batch_size, -1)[:, :month_count]
        sampled = arms[:, indices]
        cer = np.mean(sampled, axis=2) - (RISK_AVERSION / 2.0) * np.var(sampled, axis=2, ddof=1)
        mt1_delta = cer[0] - cer[1]
        s8_delta = cer[2] - cer[3]
        mt1_statistics[written : written + batch_size] = mt1_delta
        primary_statistics[written : written + batch_size] = mt1_delta - s8_delta
        written += batch_size

    if not np.all(np.isfinite(mt1_statistics)) or not np.all(np.isfinite(primary_statistics)):
        raise MT1TrialRefused("paired bootstrap produced a non-finite statistic")

    tail = (1.0 - CONFIDENCE) * 50.0
    mt1_low, mt1_high = np.percentile(mt1_statistics, [tail, 100.0 - tail], method="linear")
    primary_low, primary_high = np.percentile(primary_statistics, [tail, 100.0 - tail], method="linear")
    return (
        PercentileInterval(float(mt1_low), float(mt1_high)),
        PercentileInterval(float(primary_low), float(primary_high)),
    )


def evaluate_mt1_trial(
    *,
    mt1_scaled: PortfolioArm,
    mt1_unscaled: PortfolioArm,
    s8_scaled: PortfolioArm,
    s8_unscaled: PortfolioArm,
    mt1_structural: ScaledBookStructuralAudit,
    s8_structural: ScaledBookStructuralAudit,
) -> MT1TrialResult:
    """Evaluate all four arms under the preregistered paired statistic.

    Structural audits are checked before any outcome statistic is calculated.
    Passing this function's conjuncts is deliberately named *historical
    statistical* evidence: the standard promotion, recent-window, cost,
    synthetic-control and DSR gates remain separate and mandatory.
    """
    structural = structural_gate(mt1_structural, s8_structural)
    labelled_arms = (
        ("MT-1 scaled", mt1_scaled),
        ("MT-1 unscaled", mt1_unscaled),
        ("S-8 scaled", s8_scaled),
        ("S-8 unscaled", s8_unscaled),
    )
    mappings = tuple(_arm_map(label, arm) for label, arm in labelled_arms)
    common = tuple(sorted(set.intersection(*(set(mapping) for mapping in mappings))))
    if len(common) < MIN_COMMON_MONTHS:
        raise MT1TrialRefused(f"paired trial needs {MIN_COMMON_MONTHS} common complete months; received {len(common)}")
    excluded = tuple(len(mapping) - len(common) for mapping in mappings)
    matrix = np.asarray([[mapping[month] for month in common] for mapping in mappings], dtype=np.float64)
    reports = tuple(_risk_report(row) for row in matrix)
    mt1_delta = reports[0].certainty_equivalent - reports[1].certainty_equivalent
    s8_delta = reports[2].certainty_equivalent - reports[3].certainty_equivalent
    primary = mt1_delta - s8_delta
    mt1_interval, primary_interval = _paired_bootstrap(matrix)
    primary_positive = primary_interval.low > 0.0
    mt1_positive = mt1_interval.low > 0.0
    drawdown_improved = reports[0].maximum_drawdown < reports[1].maximum_drawdown
    expected_shortfall_improved = reports[0].expected_shortfall_5 > reports[1].expected_shortfall_5
    conjuncts = primary_positive and mt1_positive and drawdown_improved and expected_shortfall_improved
    return MT1TrialResult(
        common_months=common,
        excluded_months_by_arm=cast(tuple[int, int, int, int], excluded),
        structural=structural,
        mt1_scaled=reports[0],
        mt1_unscaled=reports[1],
        s8_scaled=reports[2],
        s8_unscaled=reports[3],
        mt1_delta_cer=mt1_delta,
        s8_delta_cer=s8_delta,
        primary_difference_in_differences=primary,
        mt1_delta_interval=mt1_interval,
        primary_interval=primary_interval,
        primary_lower_bound_positive=primary_positive,
        mt1_lower_bound_positive=mt1_positive,
        mt1_drawdown_improved=drawdown_improved,
        mt1_expected_shortfall_improved=expected_shortfall_improved,
        historical_statistical_conjuncts_pass=conjuncts,
    )


__all__ = [
    "BLOCK_LENGTH_MONTHS",
    "BOOTSTRAP_RESAMPLES",
    "BOOTSTRAP_SEED",
    "MAX_ANNUALISED_TURNOVER",
    "MIN_COMMON_MONTHS",
    "NEGATIVE_CONTROL_TRIAL_ID",
    "TRIAL_EVALUATOR_VERSION",
    "TRIAL_ID",
    "ArmRiskReport",
    "MT1TrialRefused",
    "MT1TrialResult",
    "MonthlyReturn",
    "PercentileInterval",
    "PortfolioArm",
    "ScaledBookStructuralAudit",
    "StructuralGateReport",
    "certainty_equivalent",
    "evaluate_mt1_trial",
    "expected_shortfall_5",
    "maximum_drawdown",
    "structural_gate",
]
