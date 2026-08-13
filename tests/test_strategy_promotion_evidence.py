"""#2505's bounded viability and edge-attribution contract."""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from decimal import Decimal

import pytest

from app.services.strategy_promotion_evidence import (
    EVIDENCE_VERSION,
    REQUIRED_CHALLENGERS,
    REQUIRED_CONTRASTS,
    REQUIRED_COST_INPUTS,
    ChallengerEvidence,
    ExpectedValueBucket,
    OutcomeContrast,
    PromotionEvidence,
    RecentYearEvidence,
    evidence_refusals,
)

_AS_OF = date(2026, 8, 12)


def _challenger(role: str, **overrides: object) -> ChallengerEvidence:
    values: dict[str, object] = {
        "role": role,
        "observation_count": 50,
        "expectancy_pct": Decimal("0.1"),
        "candidate_minus_challenger_pct": Decimal("0.2"),
        "same_observations_and_fills": True,
        "causal_observation_rule_version": "causal-v1",
        "fill_rule_version": "fills-v1",
        "overlap_rule_version": "overlap-v1",
    }
    values.update(overrides)
    return ChallengerEvidence(**values)  # type: ignore[arg-type]


def _evidence(**overrides: object) -> PromotionEvidence:
    values: dict[str, object] = {
        "evidence_version": EVIDENCE_VERSION,
        "causal_observation_rule_version": "causal-v1",
        "fill_rule_version": "fills-v1",
        "overlap_rule_version": "overlap-v1",
        "after_cost_expectancy_ci_low_pct": Decimal("0.1"),
        "max_drawdown_pct": Decimal("-8"),
        "expected_shortfall_5_pct": Decimal("-3"),
        "worst_gap_pct": Decimal("-5"),
        "excluding_best_1_expectancy_pct": Decimal("0.2"),
        "recent_year_stable": True,
        "recent_years_evaluated": 3,
        "recent_year_evidence": (
            RecentYearEvidence(2024, 50, Decimal("0.2"), Decimal("-0.1"), True),
            RecentYearEvidence(2025, 50, Decimal("0.3"), Decimal("0.0"), True),
            RecentYearEvidence(2026, 50, Decimal("0.4"), Decimal("0.1"), True),
        ),
        "max_date_contribution_pct": Decimal("8"),
        "max_name_contribution_pct": Decimal("7"),
        "max_sector_contribution_pct": Decimal("20"),
        "max_concurrency": 12,
        "capacity_usd": Decimal("100000"),
        "risk_limits_version": "test-risk-v1",
        "risk_limits_passed": True,
        "probability_calibration_passed": True,
        "path_diagnostics_complete": True,
        "outcome_count": 150,
        "profitable_outcome_count": 80,
        "losing_outcome_count": 70,
        "flat_outcome_count": 0,
        "target_first_count": 60,
        "stop_first_count": 45,
        "timeout_count": 45,
        "ambiguous_path_count": 2,
        "observed_cost_inputs": REQUIRED_COST_INPUTS,
        "cost_observed_on": date(2026, 8, 11),
        "cost_valid_through": date(2026, 8, 13),
        "cost_source_version": "etoro-quote-v1",
        "spread_bps": Decimal("8"),
        "slippage_bps": Decimal("5"),
        "financing_bps_per_day": Decimal("1"),
        "fx_bps": Decimal("2"),
        "broker_eligible": True,
        "challengers": tuple(_challenger(role, observation_count=150) for role in sorted(REQUIRED_CHALLENGERS)),
        "ev_buckets": (
            ExpectedValueBucket(1, 50, Decimal("-0.2"), Decimal("-0.1")),
            ExpectedValueBucket(2, 50, Decimal("0.1"), Decimal("0.2")),
            ExpectedValueBucket(3, 50, Decimal("0.4"), Decimal("0.5")),
        ),
        "outcome_contrasts": tuple(
            OutcomeContrast(role, 80, 70, Decimal("1"), Decimal("0"), Decimal("1"))
            for role in sorted(REQUIRED_CONTRASTS)
        ),
    }
    values.update(overrides)
    return PromotionEvidence(**values)  # type: ignore[arg-type]


def test_complete_evidence_clears_its_own_gate() -> None:
    assert evidence_refusals(_evidence(), profit_factor=1.2, as_of=_AS_OF) == ()


@pytest.mark.parametrize(
    ("override", "refusal"),
    [
        ({"after_cost_expectancy_ci_low_pct": Decimal("0")}, "expectancy_lower_bound_not_positive"),
        ({"recent_year_stable": False}, "recent_year_instability"),
        (
            {
                "recent_years_evaluated": 1,
                "recent_year_evidence": (RecentYearEvidence(2026, 50, Decimal("0.4"), Decimal("0.1"), True),),
            },
            "recent_year_evidence_incomplete",
        ),
        ({"excluding_best_1_expectancy_pct": Decimal("0")}, "excluding_best_1_not_positive"),
        ({"risk_limits_passed": False}, "tail_or_concentration_limits_failed"),
        ({"probability_calibration_passed": False}, "probability_calibration_failed"),
        ({"path_diagnostics_complete": False}, "path_diagnostics_incomplete"),
        ({"observed_cost_inputs": frozenset({"spread"})}, "executable_cost_inputs_missing"),
    ],
)
def test_each_missing_viability_dimension_fails_closed(override: dict[str, object], refusal: str) -> None:
    assert refusal in evidence_refusals(_evidence(**override), profit_factor=1.2, as_of=_AS_OF)


def test_profit_factor_must_be_measured_and_above_break_even() -> None:
    assert "profit_factor_not_computed" in evidence_refusals(_evidence(), profit_factor=None, as_of=_AS_OF)
    assert "profit_factor_not_above_one" in evidence_refusals(_evidence(), profit_factor=1.0, as_of=_AS_OF)
    assert "profit_factor_invalid" in evidence_refusals(_evidence(), profit_factor=float("nan"), as_of=_AS_OF)
    assert "profit_factor_invalid" in evidence_refusals(_evidence(), profit_factor=float("inf"), as_of=_AS_OF)


def test_recent_stability_uses_the_stored_year_values_not_only_a_boolean() -> None:
    evidence = _evidence()
    unstable = replace(
        evidence,
        recent_year_evidence=(
            *evidence.recent_year_evidence[:-1],
            replace(
                evidence.recent_year_evidence[-1],
                after_cost_expectancy_pct=Decimal("-0.01"),
                expectancy_ci_low_pct=Decimal("-0.1"),
            ),
        ),
    )
    assert "recent_year_instability" in evidence_refusals(unstable, profit_factor=1.2, as_of=_AS_OF)


def test_all_five_challengers_are_required_on_the_same_path() -> None:
    evidence = _evidence()
    incomplete = replace(evidence, challengers=evidence.challengers[:-1])
    assert "challenger_evidence_incomplete" in evidence_refusals(incomplete, profit_factor=1.2, as_of=_AS_OF)

    incomparable = replace(
        evidence,
        challengers=(replace(evidence.challengers[0], same_observations_and_fills=False), *evidence.challengers[1:]),
    )
    assert "challenger_population_not_comparable" in evidence_refusals(incomparable, profit_factor=1.2, as_of=_AS_OF)

    wrong_population = replace(
        evidence,
        challengers=(replace(evidence.challengers[0], observation_count=149), *evidence.challengers[1:]),
    )
    assert "challenger_population_not_comparable" in evidence_refusals(
        wrong_population, profit_factor=1.2, as_of=_AS_OF
    )

    wrong_rules = replace(
        evidence,
        challengers=(
            replace(evidence.challengers[0], overlap_rule_version="overlap-v2"),
            *evidence.challengers[1:],
        ),
    )
    assert "challenger_population_not_comparable" in evidence_refusals(wrong_rules, profit_factor=1.2, as_of=_AS_OF)


def test_candidate_must_beat_every_predeclared_challenger() -> None:
    evidence = _evidence()
    challengers = (
        replace(evidence.challengers[0], candidate_minus_challenger_pct=Decimal("0")),
        *evidence.challengers[1:],
    )
    assert "candidate_does_not_beat_challengers" in evidence_refusals(
        replace(evidence, challengers=challengers), profit_factor=1.2, as_of=_AS_OF
    )


def test_ev_buckets_must_rank_realised_outcomes_not_only_forecasts() -> None:
    reversed_realisation = replace(
        _evidence(),
        ev_buckets=(
            ExpectedValueBucket(1, 50, Decimal("-0.2"), Decimal("0.3")),
            ExpectedValueBucket(2, 50, Decimal("0.1"), Decimal("0.2")),
            ExpectedValueBucket(3, 50, Decimal("0.4"), Decimal("0.1")),
        ),
    )
    assert "ev_bucket_ranking_not_monotonic" in evidence_refusals(reversed_realisation, profit_factor=1.2, as_of=_AS_OF)

    malformed_forecast_ranks = replace(
        _evidence(),
        ev_buckets=(
            ExpectedValueBucket(1, 50, Decimal("0"), Decimal("-0.1")),
            ExpectedValueBucket(2, 50, Decimal("100"), Decimal("0.2")),
            ExpectedValueBucket(3, 50, Decimal("1"), Decimal("0.5")),
        ),
    )
    assert "ev_bucket_ranking_not_monotonic" in evidence_refusals(
        malformed_forecast_ranks, profit_factor=1.2, as_of=_AS_OF
    )

    partial_population = replace(_evidence(), ev_buckets=_evidence().ev_buckets[:-1])
    assert "ev_bucket_evidence_incomplete" in evidence_refusals(partial_population, profit_factor=1.2, as_of=_AS_OF)


def test_cost_values_must_be_current_and_broker_eligible() -> None:
    stale = evidence_refusals(_evidence(cost_valid_through=date(2026, 8, 11)), profit_factor=1.2, as_of=_AS_OF)
    assert "executable_cost_inputs_stale" in stale
    assert "broker_ineligible" in evidence_refusals(_evidence(broker_eligible=False), profit_factor=1.2, as_of=_AS_OF)


def test_all_outcome_contrasts_cover_the_same_profit_and_loss_populations() -> None:
    evidence = _evidence()
    assert "outcome_contrast_evidence_incomplete" in evidence_refusals(
        replace(evidence, outcome_contrasts=evidence.outcome_contrasts[:-1]),
        profit_factor=1.2,
        as_of=_AS_OF,
    )
    wrong_population = replace(
        evidence,
        outcome_contrasts=(
            replace(evidence.outcome_contrasts[0], profitable_count=79),
            *evidence.outcome_contrasts[1:],
        ),
    )
    assert "outcome_contrast_population_not_comparable" in evidence_refusals(
        wrong_population, profit_factor=1.2, as_of=_AS_OF
    )


def test_constructor_refuses_unbounded_or_ambiguous_shapes() -> None:
    with pytest.raises(ValueError, match="contiguous"):
        _evidence(ev_buckets=(ExpectedValueBucket(2, 50, Decimal("0"), Decimal("0")),))
    with pytest.raises(ValueError, match="unique"):
        first = _evidence().challengers[0]
        _evidence(challengers=(first, first))
    with pytest.raises(ValueError, match="non-positive"):
        _evidence(worst_gap_pct=Decimal("1"))
    with pytest.raises(ValueError, match="must be a boolean"):
        _evidence(recent_year_stable="false")
    with pytest.raises(ValueError, match="same_observations_and_fills must be a boolean"):
        _challenger("raw_instrument_shock", same_observations_and_fills="false")
