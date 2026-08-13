"""Compact, immutable edge-attribution evidence required by #2505.

The result ledger already proves how a backtest was produced.  This contract
proves why a candidate is economically actionable: it survives costs and tails,
ranks later outcomes, and beats predeclared simpler explanations on the same
observations and fills.  It deliberately stores aggregates rather than feature
histories or polling snapshots.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, DecimalException
from typing import Final, Literal

EVIDENCE_VERSION: Final = "promotion-edge-evidence-v1"

ChallengerRole = Literal[
    "raw_instrument_shock",
    "market_residual",
    "market_sector_residual",
    "matched_random_entries",
    "unfiltered_eligible_signals",
]

REQUIRED_CHALLENGERS: Final[frozenset[ChallengerRole]] = frozenset(
    {
        "raw_instrument_shock",
        "market_residual",
        "market_sector_residual",
        "matched_random_entries",
        "unfiltered_eligible_signals",
    }
)

CostInput = Literal["spread", "slippage", "financing", "fx", "broker_eligibility"]
REQUIRED_COST_INPUTS: Final[frozenset[CostInput]] = frozenset(
    {"spread", "slippage", "financing", "fx", "broker_eligibility"}
)

ContrastDimension = Literal[
    "feature_score",
    "execution_cost",
    "entry_gap",
    "liquidity",
    "market_stress",
    "sector_concentration",
    "date_concentration",
    "instrument_concentration",
]
REQUIRED_CONTRASTS: Final[frozenset[ContrastDimension]] = frozenset(
    {
        "feature_score",
        "execution_cost",
        "entry_gap",
        "liquidity",
        "market_stress",
        "sector_concentration",
        "date_concentration",
        "instrument_concentration",
    }
)


@dataclass(frozen=True)
class ChallengerEvidence:
    """One frozen simpler explanation measured on the candidate's exact path."""

    role: ChallengerRole
    observation_count: int
    expectancy_pct: Decimal
    candidate_minus_challenger_pct: Decimal
    same_observations_and_fills: bool
    causal_observation_rule_version: str
    fill_rule_version: str
    overlap_rule_version: str

    def __post_init__(self) -> None:
        if self.role not in REQUIRED_CHALLENGERS:
            raise ValueError(f"unknown challenger role {self.role!r}")
        if type(self.observation_count) is not int or self.observation_count < 1:
            raise ValueError("challenger observation_count must be positive")
        if type(self.same_observations_and_fills) is not bool:
            raise ValueError("challenger same_observations_and_fills must be a boolean")
        if not self.causal_observation_rule_version or not self.fill_rule_version or not self.overlap_rule_version:
            raise ValueError("challenger causal observation, fill and overlap rule versions must be non-empty")
        if not self.expectancy_pct.is_finite() or not self.candidate_minus_challenger_pct.is_finite():
            raise ValueError("challenger metrics must be finite")


@dataclass(frozen=True)
class ExpectedValueBucket:
    """One predeclared forecast rank and its later realised expectancy."""

    rank: int
    observation_count: int
    forecast_ev_pct: Decimal
    realised_expectancy_pct: Decimal

    def __post_init__(self) -> None:
        if type(self.rank) is not int or self.rank < 1:
            raise ValueError("EV bucket rank must be positive")
        if type(self.observation_count) is not int or self.observation_count < 1:
            raise ValueError("EV bucket observation_count must be positive")
        if not self.forecast_ev_pct.is_finite() or not self.realised_expectancy_pct.is_finite():
            raise ValueError("EV bucket metrics must be finite")


@dataclass(frozen=True)
class OutcomeContrast:
    """Bounded aggregate difference between profitable and losing outcomes."""

    dimension: ContrastDimension
    profitable_count: int
    losing_count: int
    profitable_mean: Decimal
    losing_mean: Decimal
    profitable_minus_losing: Decimal

    def __post_init__(self) -> None:
        if self.dimension not in REQUIRED_CONTRASTS:
            raise ValueError(f"unknown outcome contrast dimension {self.dimension!r}")
        if type(self.profitable_count) is not int or type(self.losing_count) is not int:
            raise ValueError("outcome contrast counts must be integers")
        if self.profitable_count < 1 or self.losing_count < 1:
            raise ValueError("outcome contrast requires profitable and losing observations")
        if any(
            not value.is_finite() for value in (self.profitable_mean, self.losing_mean, self.profitable_minus_losing)
        ):
            raise ValueError("outcome contrast metrics must be finite")
        if self.profitable_mean - self.losing_mean != self.profitable_minus_losing:
            raise ValueError("outcome contrast difference must equal profitable_mean - losing_mean")


@dataclass(frozen=True)
class RecentYearEvidence:
    """One recent calendar year's bounded after-cost stability evidence."""

    year: int
    observation_count: int
    after_cost_expectancy_pct: Decimal
    expectancy_ci_low_pct: Decimal
    risk_limits_passed: bool

    def __post_init__(self) -> None:
        if self.year < 2000 or self.year > 9999:
            raise ValueError("recent evidence year must be four digits and post-1999")
        if type(self.observation_count) is not int or self.observation_count < 1:
            raise ValueError("recent year observation_count must be a positive integer")
        if type(self.risk_limits_passed) is not bool:
            raise ValueError("recent year risk_limits_passed must be a boolean")
        if not self.after_cost_expectancy_pct.is_finite() or not self.expectancy_ci_low_pct.is_finite():
            raise ValueError("recent year expectancy metrics must be finite")
        if self.expectancy_ci_low_pct > self.after_cost_expectancy_pct:
            raise ValueError("recent year expectancy lower bound cannot exceed its point estimate")


@dataclass(frozen=True)
class PromotionEvidence:
    """The bounded #2505 evidence record paired one-to-one with a result row."""

    evidence_version: str
    causal_observation_rule_version: str
    fill_rule_version: str
    overlap_rule_version: str
    after_cost_expectancy_ci_low_pct: Decimal
    max_drawdown_pct: Decimal
    expected_shortfall_5_pct: Decimal
    worst_gap_pct: Decimal
    excluding_best_1_expectancy_pct: Decimal
    recent_year_stable: bool
    recent_years_evaluated: int
    recent_year_evidence: tuple[RecentYearEvidence, ...]
    max_date_contribution_pct: Decimal
    max_name_contribution_pct: Decimal
    max_sector_contribution_pct: Decimal
    max_concurrency: int
    capacity_usd: Decimal
    risk_limits_version: str
    risk_limits_passed: bool
    probability_calibration_passed: bool
    path_diagnostics_complete: bool
    outcome_count: int
    profitable_outcome_count: int
    losing_outcome_count: int
    flat_outcome_count: int
    target_first_count: int
    stop_first_count: int
    timeout_count: int
    ambiguous_path_count: int
    observed_cost_inputs: frozenset[CostInput]
    cost_observed_on: date
    cost_valid_through: date
    cost_source_version: str
    spread_bps: Decimal
    slippage_bps: Decimal
    financing_bps_per_day: Decimal
    fx_bps: Decimal
    broker_eligible: bool
    challengers: tuple[ChallengerEvidence, ...]
    ev_buckets: tuple[ExpectedValueBucket, ...]
    outcome_contrasts: tuple[OutcomeContrast, ...]

    def __post_init__(self) -> None:
        if self.evidence_version != EVIDENCE_VERSION:
            raise ValueError(f"evidence_version must be {EVIDENCE_VERSION!r}")
        if not self.risk_limits_version:
            raise ValueError("risk_limits_version must be non-empty")
        if not self.causal_observation_rule_version or not self.fill_rule_version or not self.overlap_rule_version:
            raise ValueError("candidate causal observation, fill and overlap rule versions must be non-empty")
        for name in (
            "recent_year_stable",
            "risk_limits_passed",
            "probability_calibration_passed",
            "path_diagnostics_complete",
            "broker_eligible",
        ):
            if type(getattr(self, name)) is not bool:
                raise ValueError(f"{name} must be a boolean")
        for name in (
            "recent_years_evaluated",
            "max_concurrency",
            "outcome_count",
            "profitable_outcome_count",
            "losing_outcome_count",
            "flat_outcome_count",
            "target_first_count",
            "stop_first_count",
            "timeout_count",
            "ambiguous_path_count",
        ):
            if type(getattr(self, name)) is not int:
                raise ValueError(f"{name} must be an integer")
        numeric = {
            "after_cost_expectancy_ci_low_pct": self.after_cost_expectancy_ci_low_pct,
            "max_drawdown_pct": self.max_drawdown_pct,
            "expected_shortfall_5_pct": self.expected_shortfall_5_pct,
            "worst_gap_pct": self.worst_gap_pct,
            "excluding_best_1_expectancy_pct": self.excluding_best_1_expectancy_pct,
            "max_date_contribution_pct": self.max_date_contribution_pct,
            "max_name_contribution_pct": self.max_name_contribution_pct,
            "max_sector_contribution_pct": self.max_sector_contribution_pct,
            "capacity_usd": self.capacity_usd,
            "spread_bps": self.spread_bps,
            "slippage_bps": self.slippage_bps,
            "financing_bps_per_day": self.financing_bps_per_day,
            "fx_bps": self.fx_bps,
        }
        if any(not value.is_finite() for value in numeric.values()):
            raise ValueError("promotion evidence metrics must be finite")
        for name in ("max_date_contribution_pct", "max_name_contribution_pct", "max_sector_contribution_pct"):
            value = numeric[name]
            if value < 0 or value > 100:
                raise ValueError(f"{name} must lie in [0, 100]")
        if self.max_drawdown_pct > 0 or self.expected_shortfall_5_pct > 0 or self.worst_gap_pct > 0:
            raise ValueError("tail losses must be reported as non-positive percentages")
        if self.max_concurrency < 1:
            raise ValueError("max_concurrency must be positive")
        if self.recent_years_evaluated < 0:
            raise ValueError("recent_years_evaluated must be non-negative")
        if self.capacity_usd <= 0:
            raise ValueError("capacity_usd must be positive")
        if min(self.spread_bps, self.slippage_bps, self.fx_bps) < 0:
            raise ValueError("spread, slippage and FX costs must be non-negative")
        if not self.cost_source_version:
            raise ValueError("cost_source_version must be non-empty")
        if self.cost_valid_through < self.cost_observed_on:
            raise ValueError("cost_valid_through cannot precede cost_observed_on")
        path_counts = (self.outcome_count, self.target_first_count, self.stop_first_count, self.timeout_count)
        if any(value < 0 for value in (*path_counts, self.ambiguous_path_count)):
            raise ValueError("path counts must be non-negative")
        if self.target_first_count + self.stop_first_count + self.timeout_count != self.outcome_count:
            raise ValueError("target/stop/timeout counts must partition outcome_count")
        if self.profitable_outcome_count + self.losing_outcome_count + self.flat_outcome_count != self.outcome_count:
            raise ValueError("profitable/losing/flat counts must partition outcome_count")
        if self.ambiguous_path_count > self.outcome_count:
            raise ValueError("ambiguous_path_count cannot exceed outcome_count")
        if not self.observed_cost_inputs <= REQUIRED_COST_INPUTS:
            raise ValueError("observed_cost_inputs contains an unknown cost input")
        roles = [item.role for item in self.challengers]
        if len(roles) != len(set(roles)):
            raise ValueError("challenger roles must be unique")
        dimensions = [item.dimension for item in self.outcome_contrasts]
        if len(dimensions) != len(set(dimensions)):
            raise ValueError("outcome contrast dimensions must be unique")
        years = [item.year for item in self.recent_year_evidence]
        if years != sorted(set(years)):
            raise ValueError("recent year evidence must have unique ascending years")
        if len(years) > 5:
            raise ValueError("recent year evidence is capped at five aggregate years")
        if self.recent_years_evaluated != len(years):
            raise ValueError("recent_years_evaluated must equal the stored recent-year evidence count")
        ranks = [item.rank for item in self.ev_buckets]
        if len(ranks) > 10:
            raise ValueError("EV bucket evidence is capped at ten aggregate buckets")
        if ranks != list(range(1, len(ranks) + 1)):
            raise ValueError("EV bucket ranks must be contiguous and ascending from one")


def evidence_refusals(
    evidence: PromotionEvidence,
    *,
    profit_factor: Decimal | float | None,
    as_of: date,
) -> tuple[str, ...]:
    """Return every #2505 refusal; an empty tuple is necessary, not sufficient."""

    refusals: list[str] = []
    if evidence.after_cost_expectancy_ci_low_pct <= 0:
        refusals.append("expectancy_lower_bound_not_positive")
    if profit_factor is None:
        refusals.append("profit_factor_not_computed")
    else:
        try:
            finite_profit_factor = profit_factor if isinstance(profit_factor, Decimal) else Decimal(str(profit_factor))
        except DecimalException, ValueError:
            refusals.append("profit_factor_invalid")
        else:
            if not finite_profit_factor.is_finite():
                refusals.append("profit_factor_invalid")
            elif finite_profit_factor <= 1:
                refusals.append("profit_factor_not_above_one")
    if not evidence.recent_year_stable or any(
        item.after_cost_expectancy_pct <= 0 or not item.risk_limits_passed for item in evidence.recent_year_evidence
    ):
        refusals.append("recent_year_instability")
    if (
        evidence.recent_years_evaluated < 2
        or not evidence.recent_year_evidence
        or evidence.recent_year_evidence[-1].year < as_of.year - 1
        or evidence.recent_year_evidence[-1].year > as_of.year
        or sum(item.observation_count for item in evidence.recent_year_evidence) > evidence.outcome_count
    ):
        refusals.append("recent_year_evidence_incomplete")
    if evidence.excluding_best_1_expectancy_pct <= 0:
        refusals.append("excluding_best_1_not_positive")
    if not evidence.risk_limits_passed:
        refusals.append("tail_or_concentration_limits_failed")
    if not evidence.probability_calibration_passed:
        refusals.append("probability_calibration_failed")
    if not evidence.path_diagnostics_complete or evidence.outcome_count < 1:
        refusals.append("path_diagnostics_incomplete")
    if evidence.observed_cost_inputs != REQUIRED_COST_INPUTS:
        refusals.append("executable_cost_inputs_missing")
    if evidence.cost_observed_on > as_of or as_of > evidence.cost_valid_through:
        refusals.append("executable_cost_inputs_stale")
    if not evidence.broker_eligible:
        refusals.append("broker_ineligible")

    by_role = {item.role: item for item in evidence.challengers}
    if set(by_role) != REQUIRED_CHALLENGERS:
        refusals.append("challenger_evidence_incomplete")
    else:
        if any(
            not item.same_observations_and_fills
            or item.causal_observation_rule_version != evidence.causal_observation_rule_version
            or item.fill_rule_version != evidence.fill_rule_version
            or item.overlap_rule_version != evidence.overlap_rule_version
            or (
                item.observation_count != evidence.outcome_count
                if item.role != "unfiltered_eligible_signals"
                else item.observation_count < evidence.outcome_count
            )
            for item in by_role.values()
        ):
            refusals.append("challenger_population_not_comparable")
        if any(item.candidate_minus_challenger_pct <= 0 for item in by_role.values()):
            refusals.append("candidate_does_not_beat_challengers")

    if (
        len(evidence.ev_buckets) < 3
        or sum(item.observation_count for item in evidence.ev_buckets) != evidence.outcome_count
    ):
        refusals.append("ev_bucket_evidence_incomplete")
    else:
        forecasts = [item.forecast_ev_pct for item in evidence.ev_buckets]
        realised = [item.realised_expectancy_pct for item in evidence.ev_buckets]
        forecast_monotonic = all(later >= earlier for earlier, later in zip(forecasts, forecasts[1:], strict=False))
        realised_monotonic = all(later >= earlier for earlier, later in zip(realised, realised[1:], strict=False))
        discriminating = realised[-1] > realised[0] and forecasts[-1] > forecasts[0]
        if not forecast_monotonic or not realised_monotonic or not discriminating:
            refusals.append("ev_bucket_ranking_not_monotonic")

    contrasts = {item.dimension: item for item in evidence.outcome_contrasts}
    if set(contrasts) != REQUIRED_CONTRASTS:
        refusals.append("outcome_contrast_evidence_incomplete")
    elif any(
        item.profitable_count != evidence.profitable_outcome_count or item.losing_count != evidence.losing_outcome_count
        for item in contrasts.values()
    ):
        refusals.append("outcome_contrast_population_not_comparable")
    return tuple(refusals)


__all__ = [
    "EVIDENCE_VERSION",
    "REQUIRED_CHALLENGERS",
    "REQUIRED_COST_INPUTS",
    "REQUIRED_CONTRASTS",
    "ChallengerEvidence",
    "ChallengerRole",
    "CostInput",
    "ContrastDimension",
    "ExpectedValueBucket",
    "OutcomeContrast",
    "PromotionEvidence",
    "RecentYearEvidence",
    "evidence_refusals",
]
