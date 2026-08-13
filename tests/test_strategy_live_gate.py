"""#2450 fail-closed live promotion and kill-drill integration tests."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import uuid4

import psycopg
import pytest

from app.api.strategies import (
    KillDrillRequest,
    LivePromotionAttemptRequest,
    StrategyLifecycleRequest,
    attempt_live_promotion,
    change_strategy_lifecycle,
    execute_live_kill_drill,
)
from app.security.sessions import SessionRow
from app.services import prereg_contract
from app.services.outcome_resolver import RULE_SET_VERSION as OUTCOME_RULE_SET_VERSION
from app.services.prereg_contract import ForwardShadowFloor, PreregDeclaration, Supersession
from app.services.price_quarantine import RULE_SET_VERSION as QUARANTINE_RULE_SET_VERSION
from app.services.result_ledger import (
    freeze_preregistration,
    load_preregistration,
    supersede_preregistration,
)
from app.services.strategy_control_plane import (
    StrategyControlError,
    configure_deployment,
    promote_strategy,
)
from app.services.strategy_live_gate import (
    REQUIRED_KILL_DRILLS,
    assess_live_gate,
    record_live_promotion_attempt,
    register_live_gate_policy,
    run_kill_drill,
)
from app.services.strategy_result import STRUCTURAL_REFUSAL_POLICY_VERSION
from tests.test_strategy_position_manager import _opened_trade

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("registered_strategy_test_candidates")]

_STRATEGY_ID = "S-LIVE-GATE"
_VERSION = "live-gate-v1"


def _session() -> SessionRow:
    now = datetime.now(UTC)
    return SessionRow("live-gate-session", uuid4(), "operator", now + timedelta(hours=1), now)


def _forward_stage(conn: psycopg.Connection[Any]) -> None:
    conn.execute(
        """
        INSERT INTO strategy_promotions (
            strategy_id,strategy_version,from_stage,to_stage,gate_version,
            evidence_ref,promoted_by,reason,promoted_at
        ) VALUES
          (%s,%s,NULL,'research_candidate','test-v1',NULL,'test','registered',now()-interval '40 days'),
          (%s,%s,'research_candidate','historical_validated','test-v1',
           'hist','test','history',now()-interval '39 days'),
          (%s,%s,'historical_validated','forward_observation','test-v1',
           'forward','test','observe',now()-interval '38 days')
        """,
        (_STRATEGY_ID, _VERSION, _STRATEGY_ID, _VERSION, _STRATEGY_ID, _VERSION),
    )


def _freeze_declaration(conn: psycopg.Connection[Any]) -> int:
    """#2599 — a live-gate policy binds a frozen forward-shadow floor.

    ⚠ The declaration must be `capital_candidate` AND structurally clean, so it
    declares survivorship-free, carry-modelled stamps. A falsification-only
    trial has no live gate to register, which is its own test below.
    """
    # ⚠ Idempotent: two tests call `_policy` twice to assert the POLICY is
    # immutable, and a second freeze would fail on sql/333's UNIQUE first —
    # masking the refusal they are actually asserting.
    existing = load_preregistration(conn, _STRATEGY_ID, _VERSION)
    if existing is not None:
        return existing.declaration_id
    return freeze_preregistration(
        conn,
        PreregDeclaration(
            strategy_id=_STRATEGY_ID,
            strategy_version=_VERSION,
            contract_version="live-gate-test-contract-v1",
            prereg_purpose="capital_candidate",
            structural_refusal_policy_version=STRUCTURAL_REFUSAL_POLICY_VERSION,
            declared_universe_basis="survivorship_free",
            declared_carry_unmodelled=False,
            declared_fx_unmodelled=False,
            expected_structural_refusals=(),
            forward_shadow=ForwardShadowFloor(
                min_independent_decision_dates=2,
                min_calendar_weeks=1,
                derivation="live-gate test fixture",
            ),
            declared_by="operator",
        ),
    )


def _policy(conn: psycopg.Connection[Any]) -> int:
    _freeze_declaration(conn)
    policy = register_live_gate_policy(
        conn,
        strategy_id=_STRATEGY_ID,
        strategy_version=_VERSION,
        min_forward_resolved_signals=2,
        min_forward_days=10,
        min_paper_closed_trades=2,
        min_paper_days=10,
        max_reconciliation_age_seconds=60,
        min_shadow_alpha_pct=Decimal("0"),
        max_cost_drift_pct=Decimal("0.25"),
        max_average_slippage_pct=Decimal("0.50"),
        max_drawdown_pct=Decimal("5"),
        max_scan_age_seconds=300,
        max_quote_age_seconds=60,
        max_broker_health_age_seconds=60,
        max_live_capital=Decimal("250"),
        registered_by="operator",
        reason="preregister before paper",
    )
    return policy.live_gate_policy_id


def test_generic_promotion_cannot_bypass_dedicated_live_gate(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    _forward_stage(ebull_test_conn)
    promote_strategy(
        ebull_test_conn,
        strategy_id=_STRATEGY_ID,
        strategy_version=_VERSION,
        to_stage="paper_enabled",
        promoted_by="operator",
        reason="paper",
        evidence_ref="paper:evidence",
    )

    with pytest.raises(StrategyControlError, match="dedicated measured"):
        promote_strategy(
            ebull_test_conn,
            strategy_id=_STRATEGY_ID,
            strategy_version=_VERSION,
            to_stage="live_enabled",
            promoted_by="operator",
            reason="must not bypass",
            evidence_ref="made-up",
        )

    assert ebull_test_conn.execute(
        "SELECT count(*) FROM strategy_promotions WHERE to_stage='live_enabled'"
    ).fetchone() == (0,)


def test_policy_is_immutable_and_must_precede_paper_observation(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    _forward_stage(conn)
    _policy(conn)
    with pytest.raises(StrategyControlError, match="immutable"):
        _policy(conn)
    promote_strategy(
        conn,
        strategy_id=_STRATEGY_ID,
        strategy_version=_VERSION,
        to_stage="paper_enabled",
        promoted_by="operator",
        reason="begin untouched paper",
        evidence_ref="paper:start",
    )

    with pytest.raises(StrategyControlError, match="forward_observation"):
        _policy(conn)

    assert conn.execute("SELECT count(*) FROM strategy_live_gate_policies").fetchone() == (1,)


def test_report_names_every_missing_measured_prerequisite_and_live_writer_block(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    _forward_stage(conn)
    _policy(conn)
    promote_strategy(
        conn,
        strategy_id=_STRATEGY_ID,
        strategy_version=_VERSION,
        to_stage="paper_enabled",
        promoted_by="operator",
        reason="paper",
        evidence_ref="paper:start",
    )
    conn.execute("INSERT INTO exchanges (exchange_id,country,asset_class) VALUES ('2450','US','us_equity')")
    conn.execute(
        "INSERT INTO instruments (instrument_id,symbol,company_name,exchange,currency,is_tradable) "
        "VALUES (2450001,'UNGATED','Unresolved forward fixture','2450','USD',true)"
    )
    conn.execute(
        """
        INSERT INTO strategy_signals (
          strategy_id,strategy_version,instrument_id,signal_bar_date,
          signal_kind,verdict,fill_bar_date,fill_price,universe,
          input_rule_set_versions,created_at
        ) VALUES (%s,%s,2450001,current_date-20,'entry','fired',current_date-19,
                  100,'survivor_only','{"indicator_series":"rules-v1"}'::jsonb,now()-interval '20 days')
        """,
        (_STRATEGY_ID, _VERSION),
    )
    conn.execute(
        """
        INSERT INTO strategy_outcomes (
          signal_id,rule_set_version,input_rule_set_version,outcome,
          resolution_method,exit_bar_date,bars_held
        )
        SELECT signal_id,%s,%s,'ambiguous','daily_bar',fill_bar_date,0
        FROM strategy_signals
        WHERE strategy_id=%s AND strategy_version=%s
        """,
        (OUTCOME_RULE_SET_VERSION, QUARANTINE_RULE_SET_VERSION, _STRATEGY_ID, _VERSION),
    )

    report = assess_live_gate(
        conn,
        strategy_id=_STRATEGY_ID,
        strategy_version=_VERSION,
        requested_capital=Decimal("251"),
        now=datetime.now(UTC),
    )

    assert not report.passed
    assert {
        "forward_sample_insufficient",
        "paper_sample_insufficient",
        "shadow_alpha_below_policy",
        "reconciliation_slo_failed",
        "kill_drills_incomplete",
        "live_capital_exceeds_preregistered_cap",
        "live_strategy_broker_contract_not_validated",
    }.issubset(report.refusal_codes)
    # A both-touch daily bar has no known P&L and must not satisfy the resolved
    # performance sample merely because it has a terminal audit row.
    assert report.facts.forward_resolved_signals == 0
    assert report.facts.paper_closed_trades == 0
    assert report.facts.active_owned_instrument_count == 0
    assert "quote_health_stale" not in report.refusal_codes
    assessment_id = record_live_promotion_attempt(
        conn,
        report=report,
        assessed_by="operator",
        reason="record the measured refusal",
    )
    assert conn.execute(
        """
        SELECT passed,promotion_id,refusal_codes @> ARRAY['live_strategy_broker_contract_not_validated'],
               char_length(evidence_sha256)
        FROM strategy_live_gate_assessments
        WHERE live_gate_assessment_id=%s
        """,
        (assessment_id,),
    ).fetchone() == (False, None, True, 64)


def test_each_kill_drill_commits_an_entry_block_then_restores_without_heartbeat_heap(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    _forward_stage(conn)
    policy_id = _policy(conn)
    conn.commit()

    for drill_kind in REQUIRED_KILL_DRILLS:
        event_id = run_kill_drill(
            conn,
            strategy_id=_STRATEGY_ID,
            strategy_version=_VERSION,
            drill_kind=drill_kind,
            run_by="operator",
            reason=f"exercise {drill_kind}",
        )
        assert event_id > 0

    assert conn.execute(
        "SELECT count(*),bool_and(passed) FROM strategy_kill_drill_events WHERE live_gate_policy_id=%s",
        (policy_id,),
    ).fetchone() == (len(REQUIRED_KILL_DRILLS), True)
    assert conn.execute("SELECT count(*) FROM strategy_execution_blocks WHERE source LIKE 'drill:%'").fetchone() == (0,)


def test_policyless_live_attempt_is_refused_and_audited(
    ebull_test_conn: psycopg.Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _forward_stage(conn)
    conn.commit()
    monkeypatch.setattr("app.api.strategies._current_versions", lambda: {_STRATEGY_ID: _VERSION})

    response = attempt_live_promotion(
        _STRATEGY_ID,
        LivePromotionAttemptRequest(
            strategy_version=_VERSION,
            requested_capital=Decimal("10"),
            reason="prove policy-less refusal audit",
        ),
        _session(),
        conn,
    )

    assert "live_gate_policy_missing" in response.report.refusal_codes
    assert conn.execute(
        """
        SELECT live_gate_policy_id,strategy_id,strategy_version,passed,
               refusal_codes @> ARRAY['live_gate_policy_missing']
        FROM strategy_live_gate_assessments
        WHERE live_gate_assessment_id=%s
        """,
        (response.assessment_id,),
    ).fetchone() == (None, _STRATEGY_ID, _VERSION, False, True)


def test_failed_kill_drill_assertion_still_restores_synthetic_source(
    ebull_test_conn: psycopg.Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _forward_stage(conn)
    _policy(conn)
    conn.commit()

    def fail_gate_read(_conn: psycopg.Connection[Any]) -> None:
        raise RuntimeError("simulated assertion failure")

    monkeypatch.setattr("app.services.strategy_live_gate.load_entry_block_state", fail_gate_read)
    with pytest.raises(RuntimeError, match="simulated assertion failure"):
        run_kill_drill(
            conn,
            strategy_id=_STRATEGY_ID,
            strategy_version=_VERSION,
            drill_kind="quote_lag",
            run_by="operator",
            reason="prove cleanup",
        )

    assert conn.execute("SELECT count(*) FROM strategy_execution_blocks WHERE source='drill:quote_lag'").fetchone() == (
        0,
    )
    assert conn.execute("SELECT count(*) FROM strategy_kill_drill_events").fetchone() == (0,)


def test_authenticated_drill_endpoint_ends_any_request_read_transaction(
    ebull_test_conn: psycopg.Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _forward_stage(conn)
    _policy(conn)
    conn.commit()
    monkeypatch.setattr("app.api.strategies._current_versions", lambda: {_STRATEGY_ID: _VERSION})

    class UnrelatedBlockState:
        new_entries_blocked = True
        execution_block_reasons = ("an unrelated block is already active",)

    monkeypatch.setattr(
        "app.services.strategy_live_gate.load_entry_block_state",
        lambda _conn: UnrelatedBlockState(),
    )
    conn.execute("SELECT 1")

    response = execute_live_kill_drill(
        _STRATEGY_ID,
        "quote_lag",
        KillDrillRequest(strategy_version=_VERSION, reason="authenticated drill"),
        _session(),
        conn,
    )

    stored = conn.execute(
        "SELECT passed FROM strategy_kill_drill_events WHERE kill_drill_event_id=%s",
        (response.kill_drill_event_id,),
    ).fetchone()
    assert stored is not None
    assert response.passed is stored[0] is False


def test_missing_order_reconciliation_state_is_a_live_gate_breach(
    ebull_test_conn: psycopg.Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _opened_trade(conn, monkeypatch)
    conn.execute("DELETE FROM strategy_order_reconciliation_state")
    conn.commit()

    report = assess_live_gate(
        conn,
        strategy_id="S-ALLOC",
        strategy_version="v1",
        requested_capital=Decimal("1"),
    )

    assert report.facts.reconciliation_order_count == 1
    assert report.facts.reconciliation_breach_count == 1


def test_paper_duration_uses_observation_time_not_pre_2000_signal_bar(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    _forward_stage(conn)
    _policy(conn)
    conn.execute(
        """
        UPDATE strategy_promotions SET promoted_at=now()-interval '5 days'
        WHERE strategy_id=%s AND to_stage='forward_observation'
        """,
        (_STRATEGY_ID,),
    )
    promote_strategy(
        conn,
        strategy_id=_STRATEGY_ID,
        strategy_version=_VERSION,
        to_stage="paper_enabled",
        promoted_by="operator",
        reason="paper now",
        evidence_ref="paper:start",
    )

    report = assess_live_gate(
        conn,
        strategy_id=_STRATEGY_ID,
        strategy_version=_VERSION,
        requested_capital=Decimal("1"),
        now=datetime.now(UTC) + timedelta(days=2),
    )

    assert report.facts.forward_days == 5
    assert report.facts.paper_days == 2


def test_pause_disables_allocations_and_retirement_is_separately_audited(
    ebull_test_conn: psycopg.Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _forward_stage(conn)
    promote_strategy(
        conn,
        strategy_id=_STRATEGY_ID,
        strategy_version=_VERSION,
        to_stage="paper_enabled",
        promoted_by="operator",
        reason="paper",
        evidence_ref="paper:start",
    )
    deployment = configure_deployment(
        conn,
        strategy_id=_STRATEGY_ID,
        strategy_version=_VERSION,
        mode="paper",
        capital_limit=Decimal("100"),
        enabled=True,
        changed_by="operator",
        reason="paper sleeve",
    )
    conn.commit()
    monkeypatch.setattr("app.api.strategies._current_versions", lambda: {_STRATEGY_ID: _VERSION})

    paused = change_strategy_lifecycle(
        _STRATEGY_ID,
        StrategyLifecycleRequest(strategy_version=_VERSION, action="pause", reason="evidence drift"),
        _session(),
        conn,
    )
    retired = change_strategy_lifecycle(
        _STRATEGY_ID,
        StrategyLifecycleRequest(strategy_version=_VERSION, action="retire", reason="end strategy"),
        _session(),
        conn,
    )

    assert paused.stage == "paused"
    assert retired.stage == "retired"
    assert conn.execute(
        "SELECT enabled,revision FROM strategy_deployments WHERE deployment_id=%s", (deployment.deployment_id,)
    ).fetchone() == (False, 2)
    assert conn.execute(
        "SELECT to_stage FROM strategy_promotions WHERE strategy_id=%s ORDER BY promotion_id DESC LIMIT 2",
        (_STRATEGY_ID,),
    ).fetchall() == [("retired",), ("paused",)]


def test_registration_refuses_a_trial_that_never_froze_a_declaration(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """#2599 — there is no forward-shadow floor to bind, so there is no policy.

    ⚠ This is the reason the floor is NOT a parameter of
    ``register_live_gate_policy``: a caller who could type the numbers could
    type different ones from the frozen contract, and the declaration would be
    decorative.
    """
    _forward_stage(ebull_test_conn)
    with pytest.raises(StrategyControlError, match="no frozen preregistration declaration"):
        register_live_gate_policy(
            ebull_test_conn,
            strategy_id=_STRATEGY_ID,
            strategy_version=_VERSION,
            min_forward_resolved_signals=2,
            min_forward_days=10,
            min_paper_closed_trades=2,
            min_paper_days=10,
            max_reconciliation_age_seconds=60,
            min_shadow_alpha_pct=Decimal("0"),
            max_cost_drift_pct=Decimal("0.25"),
            max_average_slippage_pct=Decimal("0.50"),
            max_drawdown_pct=Decimal("5"),
            max_scan_age_seconds=300,
            max_quote_age_seconds=60,
            max_broker_health_age_seconds=60,
            max_live_capital=Decimal("250"),
            registered_by="operator",
            reason="preregister before paper",
        )


def test_registration_refuses_a_falsification_only_trial(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """A trial that declared it cannot promote capital has no live gate.

    ⚠ Registering one would be the declaration being quietly walked back — the
    exact move #2599 exists to make impossible.
    """
    _forward_stage(ebull_test_conn)
    freeze_preregistration(
        ebull_test_conn,
        PreregDeclaration(
            strategy_id=_STRATEGY_ID,
            strategy_version=_VERSION,
            contract_version="live-gate-test-contract-v1",
            prereg_purpose="falsification_only",
            structural_refusal_policy_version=STRUCTURAL_REFUSAL_POLICY_VERSION,
            declared_universe_basis="survivor_only",
            declared_carry_unmodelled=True,
            declared_fx_unmodelled=True,
            expected_structural_refusals=(
                "universe_basis_not_survivorship_free",
                "carry_unmodelled",
                "fx_unmodelled",
            ),
            forward_shadow=ForwardShadowFloor(2, 1, "live-gate test fixture"),
            declared_by="operator",
        ),
    )
    with pytest.raises(StrategyControlError, match="falsification_only"):
        register_live_gate_policy(
            ebull_test_conn,
            strategy_id=_STRATEGY_ID,
            strategy_version=_VERSION,
            min_forward_resolved_signals=2,
            min_forward_days=10,
            min_paper_closed_trades=2,
            min_paper_days=10,
            max_reconciliation_age_seconds=60,
            min_shadow_alpha_pct=Decimal("0"),
            max_cost_drift_pct=Decimal("0.25"),
            max_average_slippage_pct=Decimal("0.50"),
            max_drawdown_pct=Decimal("5"),
            max_scan_age_seconds=300,
            max_quote_age_seconds=60,
            max_broker_health_age_seconds=60,
            max_live_capital=Decimal("250"),
            registered_by="operator",
            reason="preregister before paper",
        )


def test_assessment_stops_honouring_a_declaration_whose_policy_was_superseded(
    ebull_test_conn: psycopg.Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """⚠ THE WIRING, not the rule — the pure function is covered elsewhere.

    A revert-probe that removed the `declaration_refusals` call from
    `assess_live_gate` passed every DB test in this file, which is exactly the
    divergence the review bot named: the research side refuses a superseded
    declaration on every look while the capital side kept honouring its frozen
    floor. The policy row is immutable; the POLICY VERSION is not.

    ⚠ SUPERSESSION IS SIMULATED BY MOVING THE CONSTANT, not by editing the row.
    Editing the row changes its digest, so `declaration_digest_mismatch` fires
    first and the coherence branch is never reached — the first draft of this
    test asserted the wrong code for that reason. In production the row never
    changes; `STRUCTURAL_REFUSAL_POLICY_VERSION` does.
    """
    _forward_stage(ebull_test_conn)
    _policy(ebull_test_conn)

    before = assess_live_gate(
        ebull_test_conn,
        strategy_id=_STRATEGY_ID,
        strategy_version=_VERSION,
        requested_capital=Decimal("100"),
    )
    assert before.forward_shadow_floor is not None
    assert "declaration_no_longer_coherent" not in before.refusal_codes

    monkeypatch.setattr(
        prereg_contract,
        "STRUCTURAL_REFUSAL_POLICY_VERSION",
        "structural-refusal-policy-2099-01-01-v9",
    )

    after = assess_live_gate(
        ebull_test_conn,
        strategy_id=_STRATEGY_ID,
        strategy_version=_VERSION,
        requested_capital=Decimal("100"),
    )
    assert "declaration_no_longer_coherent" in after.refusal_codes
    assert "declaration_digest_mismatch" not in after.refusal_codes
    assert after.forward_shadow_floor is None
    assert not after.passed


def test_a_superseded_declaration_still_supplies_the_floor_to_its_policy(
    ebull_test_conn: psycopg.Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#2634 — the wedge one level up, and the reason the identity test widened.

    A live-gate policy binds ONE ``declaration_id`` and is itself immutable. So
    when #2634's supersession inserts a new revision, the old
    ``policy.declaration_id == frozen.declaration_id`` test stops matching and
    the trial sits at ``forward_shadow_floor_missing`` forever — the same
    permanent wedge #2634 exists to remove, moved from the research side to the
    capital side.

    ⚠ HONOURING AN ANCESTOR CANNOT LOOSEN ANYTHING, which is the whole licence
    for widening it. A supersession may not change the purpose, the stamps or
    either floor, so every revision in a chain carries identical terms; a
    genuinely laxer declaration is not a supersession at all, it is a new
    ``strategy_version`` and therefore a different policy.
    """
    _forward_stage(ebull_test_conn)
    _policy(ebull_test_conn)
    stranded = load_preregistration(ebull_test_conn, _STRATEGY_ID, _VERSION)
    assert stranded is not None

    bumped = "structural-refusal-policy-2099-01-01-v9"
    monkeypatch.setattr(prereg_contract, "STRUCTURAL_REFUSAL_POLICY_VERSION", bumped)

    stranded_report = assess_live_gate(
        ebull_test_conn,
        strategy_id=_STRATEGY_ID,
        strategy_version=_VERSION,
        requested_capital=Decimal("100"),
    )
    assert "declaration_no_longer_coherent" in stranded_report.refusal_codes

    successor_id = supersede_preregistration(
        ebull_test_conn,
        replace(stranded.declaration, structural_refusal_policy_version=bumped, declared_by="operator-repair"),
        Supersession(
            reason="structural_refusal_policy_superseded",
            attestation="no outcome of this trial has been opened; repairing the policy version only",
        ),
    )

    repaired = assess_live_gate(
        ebull_test_conn,
        strategy_id=_STRATEGY_ID,
        strategy_version=_VERSION,
        requested_capital=Decimal("100"),
    )
    # The policy still points at the PREDECESSOR — that is the whole point.
    assert successor_id != stranded.declaration_id
    assert "declaration_no_longer_coherent" not in repaired.refusal_codes
    assert repaired.forward_shadow_floor is not None
    assert repaired.forward_shadow_floor.min_independent_decision_dates == 2
