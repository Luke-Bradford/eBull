"""#2843 — the autonomy flag and its approver, against a real database.

The pure half lives in `tests/test_2843_autonomy_approval_mode.py`; see its header
for why the split is load-bearing rather than tidy.

⚠ EVERY ASSERTION HERE IS AGAINST A STORED ROW, never against arguments handed to a
mock. An argument-equality test would prove the call shape and nothing about the
evidence `advance_strategy` actually assembled — which is the only thing worth
proving about a caller whose entire job is to be indistinguishable from an operator
except in WHO it stamps.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import psycopg
import pytest

from app.services.price_masked_bars import QUARANTINE_RULE_SET_VERSION
from app.services.strategy_control_plane import (
    StrategyControlError,
    configure_paper_pool,
    load_paper_pool,
)
from app.services.strategy_forecast_assessment import FORECAST_POLICY_VERSION
from app.services.strategy_forecast_outcome_resolution import RESOLVER_VERSION as FORECAST_OUTCOME_RESOLVER_VERSION

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("registered_strategy_test_candidates")]

_STRATEGY_ID = "S-GOV"
_VERSION = "autonomy-flag-v1"
_NOW = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _only_this_strategy(monkeypatch: pytest.MonkeyPatch) -> None:
    """The cycle's POPULATION is manifest-derived config, so it is the one thing
    stubbed here. Everything downstream of it -- stage read, purpose check, evidence
    assembly, promotion write -- runs for real against the database."""
    from app.services import strategy_autonomous_promotion, strategy_operator_promotion

    for module in (strategy_autonomous_promotion, strategy_operator_promotion):
        # ⚠ BOTH. `advance_strategy` resolves the version from its OWN module-level
        # import, so patching only the cycle's copy leaves it raising "unknown
        # strategy" -- which passes a naive "did it refuse?" assertion for entirely
        # the wrong reason.
        monkeypatch.setattr(module, "current_result_versions", lambda: {_STRATEGY_ID: _VERSION})


def _seed_forward_observation(conn: psycopg.Connection[Any]) -> None:
    """Seed the lifecycle directly, as `tests/test_2612_...` does and for the same
    reason: reaching `forward_observation` through `promote_strategy` needs pinned
    results carrying #2505 edge evidence and #2621 frozen-universe records, none of
    which the flag depends on. The LAST hop -- the one under test -- is taken for
    real."""
    conn.execute(
        """
        INSERT INTO strategy_promotions (
            strategy_id,strategy_version,from_stage,to_stage,gate_version,
            evidence_ref,promoted_by,reason,promoted_at
        ) VALUES
          (%(s)s,%(v)s,NULL,'research_candidate','test-v1',NULL,'operator','registered',
           %(now)s - interval '40 days'),
          (%(s)s,%(v)s,'research_candidate','historical_validated','test-v1','hist','operator','history',
           %(now)s - interval '39 days'),
          (%(s)s,%(v)s,'historical_validated','forward_observation','test-v1','fwd','operator','observe',
           %(now)s - interval '38 days')
        """,
        {"s": _STRATEGY_ID, "v": _VERSION, "now": _NOW},
    )


def _seed_passing_assessment(conn: psycopg.Connection[Any], *, checked_at: datetime = _NOW) -> None:
    """One fresh, passing, post-forward-observation prospective assessment.

    Shape copied from `tests/test_strategy_paper_executor.py::_authorise_forecast_scope`
    rather than reinvented -- two spellings of "a passing assessment" is how the two
    drift.
    """
    conn.execute(
        """
        INSERT INTO strategy_forecast_calibrations (
            calibration_id,model_version,holdout_start,holdout_end,sample_size,
            brier_score,calibration_error,passed,evidence_ref
        ) VALUES ('test-calibration-v1','test-model-v1','2024-01-01','2024-06-30',100,
                  0.1,0.05,true,'synthetic autonomy fixture only')
        ON CONFLICT (calibration_id) DO NOTHING
        """
    )
    conn.execute(
        """
        INSERT INTO strategy_forecast_assessment_policies (
            policy_id,effective_from,recent_window_days,minimum_resolved_forecasts,
            adaptive_calibration_bins,max_normalized_brier_score,min_brier_skill_score,
            max_classwise_calibration_error,max_ambiguous_rate,max_unresolved_rate,
            max_pending_rate,max_assessment_age_days,evidence_ref
        ) VALUES ('test-autonomy-policy-v1',%s - interval '1 day',90,30,5,0.2,0.01,0.1,0.05,0.05,0.2,2,
                  'synthetic autonomy fixture only')
        ON CONFLICT (policy_id) DO NOTHING
        """,
        (checked_at,),
    )
    assessment = conn.execute(
        """
        INSERT INTO strategy_forecast_assessments (
            policy_id,strategy_id,strategy_version,forecast_policy_version,model_version,
            calibration_id,setup_version,exit_policy_version,resolver_version,
            input_rule_set_version,window_start,window_end,evidence_hash,
            total_forecasts,resolved_forecasts,target_first_count,stop_first_count,timeout_count,
            ambiguous_count,unresolved_count,pending_count,normalized_brier_score,
            baseline_normalized_brier_score,brier_skill_score,max_classwise_calibration_error,
            ambiguous_rate,unresolved_rate,pending_rate,passed,reason_codes
        ) VALUES (
            'test-autonomy-policy-v1',%s,%s,%s,'test-model-v1','test-calibration-v1','test-setup-v1',
            'test-exit-v1',%s,%s,%s::date-89,%s::date,'synthetic-autonomy',
            30,30,10,10,10,0,0,0,0,0.33333333,1,0,0,0,0,true,'[]'::jsonb
        ) RETURNING assessment_id
        """,
        (
            _STRATEGY_ID,
            _VERSION,
            FORECAST_POLICY_VERSION,
            FORECAST_OUTCOME_RESOLVER_VERSION,
            QUARANTINE_RULE_SET_VERSION,
            checked_at,
            checked_at,
        ),
    ).fetchone()
    assert assessment is not None
    conn.execute(
        """
        INSERT INTO strategy_forecast_assessment_current (
            policy_id,strategy_id,strategy_version,forecast_policy_version,model_version,
            calibration_id,setup_version,exit_policy_version,resolver_version,
            input_rule_set_version,assessment_id,checked_at
        ) VALUES (
            'test-autonomy-policy-v1',%s,%s,%s,'test-model-v1','test-calibration-v1','test-setup-v1',
            'test-exit-v1',%s,%s,%s,%s
        )
        """,
        (
            _STRATEGY_ID,
            _VERSION,
            FORECAST_POLICY_VERSION,
            FORECAST_OUTCOME_RESOLVER_VERSION,
            QUARANTINE_RULE_SET_VERSION,
            int(assessment[0]),
            checked_at,
        ),
    )


def _configure(conn: psycopg.Connection[Any], *, approval_mode: str, enabled: bool = True) -> None:
    configure_paper_pool(
        conn,
        enabled=enabled,
        capital_limit=Decimal("1000"),
        capital_mode="fixed",
        risk_profile="balanced",
        approval_mode=approval_mode,  # type: ignore[arg-type]
        changed_by="operator",
        reason="autonomy flag test",
    )


def _stage_rows(conn: psycopg.Connection[Any]) -> list[tuple[Any, ...]]:
    return conn.execute(
        "SELECT to_stage,promoted_by FROM strategy_promotions "
        "WHERE strategy_id=%s AND strategy_version=%s ORDER BY promotion_id",
        (_STRATEGY_ID, _VERSION),
    ).fetchall()


# --------------------------------------------------------------------------- flag


def test_the_flag_round_trips_through_the_event_table(ebull_test_conn: psycopg.Connection[Any]) -> None:
    _configure(ebull_test_conn, approval_mode="autonomous")
    assert load_paper_pool(ebull_test_conn).approval_mode == "autonomous"


def test_autonomous_is_refused_without_a_configured_mandate(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """The service refuses before sql/365's CHECK sees it, so the operator gets a
    named error rather than a raw constraint violation."""
    with pytest.raises(StrategyControlError, match="autonomous approval requires a configured"):
        configure_paper_pool(
            ebull_test_conn,
            enabled=False,
            capital_limit=Decimal("0"),
            capital_mode="fixed",
            risk_profile="unconfigured",
            approval_mode="autonomous",
            changed_by="operator",
            reason="should refuse",
        )


def test_flipping_only_the_flag_is_a_material_change(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """Without `approval_mode` in the change comparison, turning autonomy on while
    changing nothing else would raise "must alter ..." and be unreachable."""
    _configure(ebull_test_conn, approval_mode="manual")
    _configure(ebull_test_conn, approval_mode="autonomous")
    assert load_paper_pool(ebull_test_conn).approval_mode == "autonomous"


# ------------------------------------------------------------------------- cycle


@pytest.mark.parametrize(
    ("approval_mode", "enabled", "expected"),
    [
        ("manual", True, "approval_mode_manual"),
        ("autonomous", False, "paper_pool_disabled"),
    ],
)
def test_the_cycle_skips_whole_and_writes_nothing(
    ebull_test_conn: psycopg.Connection[Any], approval_mode: str, enabled: bool, expected: str
) -> None:
    """Every precondition is exercised against a strategy that WOULD otherwise
    advance — the assessment is seeded and passing — so a skip here is the flag
    refusing, not the evidence."""
    from app.services.strategy_autonomous_promotion import run_autonomous_promotion_cycle

    _seed_forward_observation(ebull_test_conn)
    _seed_passing_assessment(ebull_test_conn)
    _configure(ebull_test_conn, approval_mode=approval_mode, enabled=enabled)
    before = _stage_rows(ebull_test_conn)

    report = run_autonomous_promotion_cycle(ebull_test_conn, as_of=_NOW)

    assert report.skipped_reason == expected
    assert report.advanced == ()
    # ⚠ Both halves. "Reported a skip" and "wrote nothing" are different claims, and
    # the flag is only a control if the second one holds.
    assert _stage_rows(ebull_test_conn) == before


def test_an_autonomous_cycle_advances_one_stage_and_stamps_the_policy(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The whole ticket, asserted on the stored row.

    `paper_enabled` is reached through the real `advance_strategy` -> `promote_strategy`
    path, so every gate those apply ran. What differs from an operator click is one
    column.
    """
    from app.services.strategy_autonomous_promotion import AUTONOMOUS_APPROVER, run_autonomous_promotion_cycle

    _seed_forward_observation(ebull_test_conn)
    _seed_passing_assessment(ebull_test_conn)
    _configure(ebull_test_conn, approval_mode="autonomous")

    report = run_autonomous_promotion_cycle(ebull_test_conn, as_of=_NOW)

    assert report.skipped_reason is None
    assert [advance.stage for advance in report.advanced] == ["paper_enabled"]
    rows = _stage_rows(ebull_test_conn)
    assert rows[-1] == ("paper_enabled", AUTONOMOUS_APPROVER)
    assert AUTONOMOUS_APPROVER == "policy@autonomy-v1"
    # Every earlier row is still the operator's. The flag adds an approver; it does
    # not rewrite history or reattribute anything.
    assert {row[1] for row in rows[:-1]} == {"operator"}


def test_a_second_cycle_takes_no_further_step(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """At most one step per strategy per cycle, and `paper_enabled` is terminal for
    this approver -- `live_enabled` belongs to the measured live gate."""
    from app.services.strategy_autonomous_promotion import run_autonomous_promotion_cycle

    _seed_forward_observation(ebull_test_conn)
    _seed_passing_assessment(ebull_test_conn)
    _configure(ebull_test_conn, approval_mode="autonomous")
    run_autonomous_promotion_cycle(ebull_test_conn, as_of=_NOW)
    after_first = _stage_rows(ebull_test_conn)

    report = run_autonomous_promotion_cycle(ebull_test_conn, as_of=_NOW + timedelta(days=1))

    assert report.advanced == ()
    assert report.refusals == ((_STRATEGY_ID, "stage_terminal"),)
    assert _stage_rows(ebull_test_conn) == after_first


def test_a_refused_strategy_is_recorded_not_raised_and_writes_nothing(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """No assessment exists, so `enable_paper` refuses. A refusal is this job's
    normal output; it must neither raise nor leave a partial row behind."""
    from app.services.strategy_autonomous_promotion import run_autonomous_promotion_cycle

    _seed_forward_observation(ebull_test_conn)
    _configure(ebull_test_conn, approval_mode="autonomous")
    before = _stage_rows(ebull_test_conn)

    report = run_autonomous_promotion_cycle(ebull_test_conn, as_of=_NOW)

    assert report.skipped_reason is None
    assert report.advanced == ()
    assert len(report.refusals) == 1
    assert "prospective_assessment" in report.refusals[0][1]
    assert _stage_rows(ebull_test_conn) == before


def test_a_stale_assessment_still_refuses_under_autonomy(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """⚠ The load-bearing negative. The flag flips WHO approves, never WHAT
    qualifies: an assessment the operator path would be refused on must refuse the
    policy path identically."""
    from app.services.strategy_autonomous_promotion import run_autonomous_promotion_cycle

    _seed_forward_observation(ebull_test_conn)
    # max_assessment_age_days = 2 on the seeded policy.
    _seed_passing_assessment(ebull_test_conn, checked_at=_NOW - timedelta(days=5))
    _configure(ebull_test_conn, approval_mode="autonomous")

    report = run_autonomous_promotion_cycle(ebull_test_conn, as_of=_NOW)

    assert report.advanced == ()
    assert report.refusals == ((_STRATEGY_ID, "prospective_assessment_stale"),)
