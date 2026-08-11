"""Recent prospective forecast assessment: proper scores, explicit refusals."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import psycopg
import pytest

from app.services.price_masked_bars import QUARANTINE_RULE_SET_VERSION
from app.services.strategy_forecast_assessment import (
    ForecastAssessmentError,
    ForecastAssessmentPolicy,
    ForecastObservation,
    ForecastScope,
    calculate_assessment,
    register_assessment_policy,
    run_forecast_assessments,
)
from app.services.strategy_forecast_outcome_resolution import RESOLVER_VERSION
from tests.fixtures.ebull_test_db import test_database_url

_SCOPE = ForecastScope(
    "S-ASSESS",
    "strategy-v1",
    "opportunity-forecast-v1",
    "model-v1",
    "calibration-v1",
    "setup-v1",
    "exit-v1",
)


def _policy(**overrides: object) -> ForecastAssessmentPolicy:
    values = {
        "policy_id": "assessment-policy-v1",
        "effective_from": datetime(2026, 8, 1, tzinfo=UTC),
        "recent_window_days": 90,
        "minimum_resolved_forecasts": 30,
        "adaptive_calibration_bins": 5,
        "max_normalized_brier_score": Decimal("0.20"),
        "min_brier_skill_score": Decimal("0.01"),
        "max_classwise_calibration_error": Decimal("0.10"),
        "max_ambiguous_rate": Decimal("0.05"),
        "max_unresolved_rate": Decimal("0.05"),
        "max_pending_rate": Decimal("0.20"),
        "max_assessment_age_days": 2,
        "evidence_ref": "ticket:2555",
    }
    values.update(overrides)
    return ForecastAssessmentPolicy(**values)  # type: ignore[arg-type]


def _observation(
    forecast_id: int,
    *,
    probabilities: tuple[str, str, str] = ("1", "0", "0"),
    outcome: str | None = "target_first",
    terminal_state: str = "resolved",
) -> ForecastObservation:
    return ForecastObservation(
        forecast_id=forecast_id,
        target_probability=Decimal(probabilities[0]),
        stop_probability=Decimal(probabilities[1]),
        timeout_probability=Decimal(probabilities[2]),
        outcome=outcome,  # type: ignore[arg-type]
        terminal_state=terminal_state,  # type: ignore[arg-type]
        outcome_id=forecast_id if terminal_state != "pending" else None,
    )


def _calculate(observations: list[ForecastObservation], policy: ForecastAssessmentPolicy | None = None):
    return calculate_assessment(
        scope=_SCOPE,
        observations=observations,
        policy=policy or _policy(),
        window_start=date(2026, 5, 14),
        window_end=date(2026, 8, 11),
    )


def _perfect_observations(count: int) -> list[ForecastObservation]:
    classes = (
        (("1", "0", "0"), "target_first"),
        (("0", "1", "0"), "stop_first"),
        (("0", "0", "1"), "timeout"),
    )
    return [
        _observation(index, probabilities=classes[(index - 1) % 3][0], outcome=classes[(index - 1) % 3][1])
        for index in range(1, count + 1)
    ]


def test_perfect_probabilities_pass_a_sufficient_recent_sample() -> None:
    assessment = _calculate(_perfect_observations(30))
    assert assessment.passed
    assert assessment.reason_codes == ()
    assert assessment.normalized_brier_score == 0
    assert assessment.baseline_normalized_brier_score is not None
    assert assessment.baseline_normalized_brier_score.quantize(Decimal("0.00000001")) == Decimal("0.33333333")
    assert assessment.brier_skill_score == 1
    assert assessment.max_classwise_calibration_error == 0
    assert (assessment.resolved_forecasts, assessment.target_first_count) == (30, 10)


def test_normalized_multiclass_brier_penalises_confident_wrong_forecasts() -> None:
    observations = [
        _observation(
            index,
            probabilities=("0.95", "0.05", "0"),
            outcome="stop_first",
        )
        for index in range(1, 31)
    ]
    assessment = _calculate(observations)
    assert assessment.normalized_brier_score == Decimal("0.9025")
    assert "normalized_brier_score_high" in assessment.reason_codes
    assert "classwise_calibration_error_high" in assessment.reason_codes
    assert not assessment.passed


def test_brier_skill_preserves_large_negative_evidence_without_clipping() -> None:
    observations = [
        _observation(index, probabilities=("0", "1", "0"), outcome="target_first") for index in range(1, 20_001)
    ]
    observations.append(_observation(20_001, probabilities=("1", "0", "0"), outcome="stop_first"))

    assessment = _calculate(observations)

    assert assessment.brier_skill_score is not None
    assert assessment.brier_skill_score < Decimal("-10000")


def test_small_apparently_perfect_sample_has_no_authority() -> None:
    assessment = _calculate(_perfect_observations(5))
    assert assessment.normalized_brier_score == 0
    assert assessment.reason_codes == ("insufficient_resolved_forecasts",)
    assert not assessment.passed


def test_ambiguous_unresolved_and_pending_are_rates_not_synthetic_classes() -> None:
    observations = _perfect_observations(30)
    observations.extend(
        (
            _observation(31, outcome=None, terminal_state="ambiguous"),
            _observation(32, outcome=None, terminal_state="unresolved"),
            _observation(33, outcome=None, terminal_state="pending"),
        )
    )
    assessment = _calculate(
        observations,
        _policy(max_ambiguous_rate=Decimal("0.02"), max_unresolved_rate=Decimal("0.02")),
    )
    assert assessment.resolved_forecasts == 30
    assert assessment.total_forecasts == 33
    assert assessment.ambiguous_rate == Decimal("0.03030303")
    assert assessment.unresolved_rate == Decimal("0.03030303")
    assert assessment.pending_rate == Decimal("0.03030303")
    assert assessment.reason_codes == ("ambiguous_rate_high", "unresolved_rate_high")


def test_evidence_identity_is_order_independent_but_changes_with_resolution() -> None:
    pending = _observation(2, outcome=None, terminal_state="pending")
    first = _calculate([_observation(1), pending])
    reordered = _calculate([pending, _observation(1)])
    resolved = _calculate([_observation(1), _observation(2)])
    assert first.evidence_hash == reordered.evidence_hash
    assert first.evidence_hash != resolved.evidence_hash


def test_observation_rejects_probability_or_state_shape_errors() -> None:
    with pytest.raises(ForecastAssessmentError, match="sum to one"):
        _observation(1, probabilities=("0.8", "0.3", "0"))
    with pytest.raises(ForecastAssessmentError, match="only resolved"):
        _observation(1, outcome="target_first", terminal_state="ambiguous")
    with pytest.raises(ForecastAssessmentError, match="min_brier_skill_score"):
        _calculate(_perfect_observations(30), _policy(min_brier_skill_score=Decimal("0")))


def test_policy_is_immutable_and_no_policy_means_no_assessment_authority(
    ebull_test_conn: psycopg.Connection[object],
) -> None:
    assert ebull_test_conn.execute("SELECT count(*) FROM strategy_forecast_assessment_policies").fetchone() == (0,)
    with psycopg.connect(test_database_url(), autocommit=True) as conn:
        assert run_forecast_assessments(conn, now=datetime(2026, 8, 11, tzinfo=UTC)).policy_id is None
        register_assessment_policy(conn, _policy())
        with pytest.raises(ForecastAssessmentError, match="already exists"):
            register_assessment_policy(conn, _policy(max_pending_rate=Decimal("0.5")))
        report = run_forecast_assessments(conn, now=datetime(2026, 8, 11, tzinfo=UTC))
    assert (report.policy_id, report.scopes_selected, report.evidence_rows_written) == (
        "assessment-policy-v1",
        0,
        0,
    )


def test_run_scores_a_real_recent_cohort_once_then_refreshes_only_current_pointer(
    ebull_test_conn: psycopg.Connection[object],
) -> None:
    assert ebull_test_conn.execute("SELECT count(*) FROM strategy_forecast_assessment_policies").fetchone() == (0,)
    with psycopg.connect(test_database_url(), autocommit=True) as conn:
        conn.execute("INSERT INTO exchanges (exchange_id,country,asset_class) VALUES ('2555','US','us_equity')")
        conn.execute(
            "INSERT INTO instruments (instrument_id,symbol,company_name,exchange,currency,is_tradable) "
            "VALUES (2555001,'FCAL','Forecast calibration test','2555','USD',true)"
        )
        conn.execute(
            """
            INSERT INTO strategy_forecast_calibrations (
                calibration_id,model_version,holdout_start,holdout_end,sample_size,
                brier_score,calibration_error,passed,evidence_ref
            ) VALUES ('cal-2555','model-2555','2026-01-01','2026-06-30',100,0.18,0.04,true,'test')
            """
        )
        conn.execute(
            """
            INSERT INTO strategy_signals (
                strategy_id,strategy_version,instrument_id,signal_bar_date,signal_kind,
                verdict,fill_bar_date,fill_price,universe,input_rule_set_versions
            )
            SELECT 'S-FCAL','v1',2555001,DATE '2026-07-01'+n,'entry','fired',
                   DATE '2026-07-02'+n,100,'survivor_only','{"indicator_series":"rules-v1"}'::jsonb
            FROM generate_series(0,29) n
            """
        )
        conn.execute(
            """
            INSERT INTO strategy_opportunity_forecasts (
                signal_id,forecast_policy_version,decided_at,valid_through,side,
                horizon_market_days,target_barrier_pct,stop_barrier_pct,setup_version,
                exit_policy_version,calibration_id,target_probability,stop_probability,
                timeout_probability,target_net_return_pct,stop_net_return_pct,
                timeout_net_return_pct,expected_duration_hours,uncertainty_penalty_pct,
                tail_penalty_pct,correlation_penalty_pct,cost_stress_penalty_pct,
                conservative_net_expectancy_pct,cost_model_id
            )
            SELECT signal_id,'opportunity-forecast-v1',signal_bar_date::timestamp + interval '20 hours',
                   signal_bar_date::timestamp + interval '7 days','long',5,10,5,'setup-2555',
                   'exit-2555','cal-2555',
                   CASE WHEN class_no=0 THEN 1 ELSE 0 END,
                   CASE WHEN class_no=1 THEN 1 ELSE 0 END,
                   CASE WHEN class_no=2 THEN 1 ELSE 0 END,
                   4,-2,0,24,0.2,0.1,0.1,0.1,
                   CASE WHEN class_no=0 THEN 3.5 WHEN class_no=1 THEN -2.5 ELSE -0.5 END,
                   'cost-v1'
            FROM (
                SELECT s.*, (row_number() OVER (ORDER BY signal_id)-1) % 3 AS class_no
                FROM strategy_signals s WHERE strategy_id='S-FCAL'
            ) ranked
            """
        )
        conn.execute(
            """
            INSERT INTO strategy_opportunity_forecast_outcomes (
                forecast_id,resolver_version,input_rule_set_version,outcome,exit_bar_date,
                exit_price,market_bars_held,gross_return_pct
            )
            SELECT forecast_id,%s,%s,
                   CASE class_no WHEN 0 THEN 'target_first' WHEN 1 THEN 'stop_first' ELSE 'timeout' END,
                   s.fill_bar_date,
                   CASE class_no WHEN 0 THEN 110 WHEN 1 THEN 95 ELSE 100 END,
                   0,CASE class_no WHEN 0 THEN 0.1 WHEN 1 THEN -0.05 ELSE 0 END
            FROM (
                SELECT f.*, (row_number() OVER (ORDER BY forecast_id)-1) %% 3 AS class_no
                FROM strategy_opportunity_forecasts f
            ) f
            JOIN strategy_signals s ON s.signal_id=f.signal_id
            """,
            (RESOLVER_VERSION, QUARANTINE_RULE_SET_VERSION),
        )
        register_assessment_policy(
            conn,
            _policy(),
        )

        first = run_forecast_assessments(conn, now=datetime(2026, 8, 11, tzinfo=UTC))
        second = run_forecast_assessments(conn, now=datetime(2026, 8, 12, tzinfo=UTC))

        assert (first.scopes_selected, first.evidence_rows_written, first.passed_scopes) == (1, 1, 1)
        assert (second.scopes_selected, second.evidence_rows_written, second.current_rows_refreshed) == (1, 0, 1)
        assert conn.execute("SELECT count(*) FROM strategy_forecast_assessments").fetchone() == (1,)
        assert conn.execute(
            "SELECT resolved_forecasts,normalized_brier_score,passed FROM strategy_forecast_assessments"
        ).fetchone() == (30, Decimal("0E-8"), True)
        assert conn.execute("SELECT checked_at FROM strategy_forecast_assessment_current").fetchone() == (
            datetime(2026, 8, 12, tzinfo=UTC),
        )
