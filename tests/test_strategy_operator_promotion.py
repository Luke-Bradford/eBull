"""Operator promotion selects complete evidence and cannot accept cherry-picked ids."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from threading import Barrier
from types import SimpleNamespace
from uuid import uuid4

import psycopg
import pytest
from pydantic import ValidationError

from app.api.strategies import (
    StrategyInitialPaperSetupRequest,
    StrategyPromotionRequest,
    create_strategy_paper_setup,
)
from app.security.sessions import SessionRow
from app.services.backtest_run import BACKTEST_UNIVERSE, corpus_version_for
from app.services.cost_model import COST_MODEL_ID
from app.services.equity_curve import BENCHMARK_RULE_ID, SIZING_RULE_ID
from app.services.outcome_resolver import RULE_SET_VERSION as OUTCOME_RULE_SET_VERSION
from app.services.position_builder import RULE_SET_VERSION as POSITION_RULE_SET_VERSION
from app.services.prereg_contract import ForwardShadowFloor, PreregDeclaration
from app.services.research_price_structure_store import QUARANTINE_RULE_SET_VERSION
from app.services.result_ledger import freeze_preregistration, store_holdout_result
from app.services.strategy_ambiguity_policy import AMBIGUITY_RULE_VERSION
from app.services.strategy_control_plane import (
    StrategyControlError,
    configure_execution_policy,
    configure_paper_pool,
)
from app.services.strategy_operator_promotion import (
    _load_current_prospective_evidence,
    advance_strategy_for_operator,
    load_forward_floor_evidence,
)
from app.services.strategy_recent_evidence import RECENT_EVIDENCE_WINDOWS
from app.services.strategy_result import (
    METRIC_AXIS_RULE_VERSION,
    STRUCTURAL_REFUSAL_POLICY_VERSION,
    TOTAL_RETURN_BASIS,
    metric_axis_sha256,
)
from app.services.strategy_statistics import periods_per_year
from tests.fixtures.ebull_test_db import test_database_url
from tests.test_result_ledger import build_metrics, build_result

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("registered_strategy_test_candidates")]

_STRATEGY_ID = "S-GOV"
_VERSION = "operator-promotion-v1"


def _store_matrix(conn: psycopg.Connection[tuple], *, omit_last: bool = False, duplicate_first: bool = False) -> None:
    version = _VERSION
    cells = [
        (item.window.start, item.window.end, item.window_id, ambiguity, quarantine)
        for item in RECENT_EVIDENCE_WINDOWS.values()
        for ambiguity in ("best_case", "worst_case")
        for quarantine in ("masked", "admitted")
    ]
    if omit_last:
        cells.pop()
    for index, (window_start, window_end, window_id, ambiguity, quarantine) in enumerate(cells):
        axis = (window_start, window_end)
        store_holdout_result(
            conn,
            build_result(
                strategy_id=_STRATEGY_ID,
                strategy_version=version,
                result_scope="sleeve",
                namespace="hold_out",
                ambiguity_arm=ambiguity,
                quarantine_arm=quarantine,
                window_start=window_start,
                window_end=window_end,
                evidence_window_id=window_id,
                metric_axis_rule_version=METRIC_AXIS_RULE_VERSION,
                metric_axis_dates=axis,
                metric_axis_start=axis[0],
                metric_axis_end=axis[-1],
                metric_axis_digest=metric_axis_sha256(axis),
                opportunity_set_digest="0" * 64,
                metrics=build_metrics(cagr_pct=-100, periods_per_year=periods_per_year(axis)),
                corpus_version=corpus_version_for(BACKTEST_UNIVERSE),
                cost_model_id=COST_MODEL_ID,
                sizing_rule=SIZING_RULE_ID,
                benchmark_rule=BENCHMARK_RULE_ID,
                return_basis=TOTAL_RETURN_BASIS,
                ambiguity_rule_version=AMBIGUITY_RULE_VERSION,
                position_rule_set_version=POSITION_RULE_SET_VERSION,
                outcome_rule_set_version=OUTCOME_RULE_SET_VERSION,
                input_rule_set_version=QUARANTINE_RULE_SET_VERSION,
            ),
            accessed_by="tests/test_strategy_operator_promotion.py",
            purpose=f"authoritative matrix cell {index}",
        )
    if duplicate_first:
        window_start, window_end, window_id, ambiguity, quarantine = cells[0]
        axis = (window_start, window_start + timedelta(days=1), window_end)
        store_holdout_result(
            conn,
            build_result(
                strategy_id=_STRATEGY_ID,
                strategy_version=version,
                result_scope="sleeve",
                namespace="hold_out",
                ambiguity_arm=ambiguity,
                quarantine_arm=quarantine,
                window_start=window_start,
                window_end=window_end,
                evidence_window_id=window_id,
                metric_axis_rule_version=METRIC_AXIS_RULE_VERSION,
                metric_axis_dates=axis,
                metric_axis_start=axis[0],
                metric_axis_end=axis[-1],
                metric_axis_digest=metric_axis_sha256(axis),
                opportunity_set_digest="0" * 64,
                metrics=build_metrics(cagr_pct=-100, periods_per_year=periods_per_year(axis)),
                corpus_version=corpus_version_for(BACKTEST_UNIVERSE),
                cost_model_id=COST_MODEL_ID,
                sizing_rule=SIZING_RULE_ID,
                benchmark_rule=BENCHMARK_RULE_ID,
                return_basis=TOTAL_RETURN_BASIS,
                ambiguity_rule_version=AMBIGUITY_RULE_VERSION,
                position_rule_set_version=POSITION_RULE_SET_VERSION,
                outcome_rule_set_version=OUTCOME_RULE_SET_VERSION,
                input_rule_set_version=QUARANTINE_RULE_SET_VERSION,
            ),
            accessed_by="tests/test_strategy_operator_promotion.py",
            purpose="duplicate matrix cell under a distinct authenticated axis",
        )


def _register(conn: psycopg.Connection[tuple]) -> None:
    advance_strategy_for_operator(
        conn,
        strategy_id=_STRATEGY_ID,
        strategy_version=_VERSION,
        action="register_candidate",
        promoted_by="operator",
        reason="register preregistered candidate",
    )


def _session() -> SessionRow:
    now = datetime.now(UTC)
    return SessionRow("operator-promotion-session", uuid4(), "operator", now + timedelta(hours=1), now)


def _seed_legacy_unlinked_paper_chain(conn: psycopg.Connection[tuple]) -> None:
    conn.execute(
        """
        INSERT INTO strategy_promotions (
            strategy_id,strategy_version,from_stage,to_stage,gate_version,
            evidence_ref,promoted_by,reason
        ) VALUES
          (%s,%s,NULL,'research_candidate','test-v1',NULL,'test','registered'),
          (%s,%s,'research_candidate','historical_validated','test-v1','hist','test','history'),
          (%s,%s,'historical_validated','forward_observation','test-v1','forward','test','observe'),
          (%s,%s,'forward_observation','paper_enabled','test-v1','paper','test','paper')
        """,
        (_STRATEGY_ID, _VERSION, _STRATEGY_ID, _VERSION, _STRATEGY_ID, _VERSION, _STRATEGY_ID, _VERSION),
    )


def _seed_prospective_assessment(
    conn: psycopg.Connection[tuple], *, forward_started_at: datetime, window_start: date
) -> None:
    checked_at = datetime(2026, 8, 15, 12, tzinfo=UTC)
    conn.execute(
        """
        INSERT INTO strategy_promotions (
            strategy_id,strategy_version,from_stage,to_stage,gate_version,
            evidence_ref,promoted_by,reason,promoted_at
        ) VALUES
          (%s,%s,NULL,'research_candidate','test-v1',NULL,'test','registered',%s-interval '2 days'),
          (%s,%s,'research_candidate','historical_validated','test-v1','hist','test','history',%s-interval '1 day'),
          (%s,%s,'historical_validated','forward_observation','test-v1','forward','test','observe',%s)
        """,
        (
            _STRATEGY_ID,
            _VERSION,
            forward_started_at,
            _STRATEGY_ID,
            _VERSION,
            forward_started_at,
            _STRATEGY_ID,
            _VERSION,
            forward_started_at,
        ),
    )
    conn.execute(
        """
        INSERT INTO strategy_forecast_calibrations (
            calibration_id,model_version,holdout_start,holdout_end,sample_size,
            brier_score,calibration_error,passed,evidence_ref
        ) VALUES ('operator-cal','operator-model','2026-01-01','2026-06-30',100,0.18,0.04,true,'calibration:test')
        """
    )
    conn.execute(
        """
        INSERT INTO strategy_forecast_assessment_policies (
            policy_id,effective_from,recent_window_days,minimum_resolved_forecasts,
            adaptive_calibration_bins,max_normalized_brier_score,min_brier_skill_score,
            max_classwise_calibration_error,max_ambiguous_rate,max_unresolved_rate,
            max_pending_rate,max_assessment_age_days,evidence_ref
        ) VALUES ('operator-policy','2026-01-01',30,30,5,0.5,0.01,0.2,0.1,0.1,0.1,7,'policy:test')
        """
    )
    assessment_row = conn.execute(
        """
        INSERT INTO strategy_forecast_assessments (
            policy_id,strategy_id,strategy_version,forecast_policy_version,model_version,
            calibration_id,setup_version,exit_policy_version,resolver_version,input_rule_set_version,
            window_start,window_end,evidence_hash,total_forecasts,resolved_forecasts,
            target_first_count,stop_first_count,timeout_count,ambiguous_count,unresolved_count,
            pending_count,normalized_brier_score,baseline_normalized_brier_score,brier_skill_score,
            max_classwise_calibration_error,ambiguous_rate,unresolved_rate,pending_rate,passed,reason_codes
        ) VALUES (
            'operator-policy',%s,%s,'forecast-v1','operator-model','operator-cal','setup-v1','exit-v1',
            'resolver-v1','input-v1',%s,'2026-08-14','evidence-hash',30,30,30,0,0,0,0,0,
            0.1,0.3,0.2,0.05,0,0,0,true,'[]'::jsonb
        ) RETURNING assessment_id
        """,
        (_STRATEGY_ID, _VERSION, window_start),
    ).fetchone()
    assert assessment_row is not None
    assessment_id = int(assessment_row[0])
    conn.execute(
        """
        INSERT INTO strategy_forecast_assessment_current (
            policy_id,strategy_id,strategy_version,forecast_policy_version,model_version,
            calibration_id,setup_version,exit_policy_version,resolver_version,input_rule_set_version,
            assessment_id,checked_at
        ) VALUES ('operator-policy',%s,%s,'forecast-v1','operator-model','operator-cal','setup-v1',
                  'exit-v1','resolver-v1','input-v1',%s,%s)
        """,
        (_STRATEGY_ID, _VERSION, assessment_id, checked_at),
    )


def _freeze_capital_declaration(conn: psycopg.Connection[tuple]) -> int:
    return freeze_preregistration(
        conn,
        PreregDeclaration(
            strategy_id=_STRATEGY_ID,
            strategy_version=_VERSION,
            contract_version="operator-promotion-contract-v1",
            prereg_purpose="capital_candidate",
            structural_refusal_policy_version=STRUCTURAL_REFUSAL_POLICY_VERSION,
            declared_universe_basis="survivorship_free",
            declared_carry_unmodelled=False,
            declared_fx_unmodelled=False,
            expected_structural_refusals=(),
            forward_shadow=ForwardShadowFloor(
                min_independent_decision_dates=1,
                min_calendar_weeks=1,
                derivation="independently fixed one-date test floor",
            ),
            declared_by="operator",
        ),
    )


def test_registration_is_ordered_and_idempotent(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    version = _VERSION
    first = advance_strategy_for_operator(
        ebull_test_conn,
        strategy_id=_STRATEGY_ID,
        strategy_version=version,
        action="register_candidate",
        promoted_by="operator",
        reason="register preregistered candidate",
    )
    retry = advance_strategy_for_operator(
        ebull_test_conn,
        strategy_id=_STRATEGY_ID,
        strategy_version=version,
        action="register_candidate",
        promoted_by="operator",
        reason="browser retry",
    )

    assert first.created is True
    assert retry.created is False
    assert retry.promotion.promotion_id == first.promotion.promotion_id
    assert ebull_test_conn.execute(
        "SELECT count(*) FROM strategy_promotions WHERE strategy_id=%s AND strategy_version=%s",
        (_STRATEGY_ID, version),
    ).fetchone() == (1,)


def test_concurrent_registration_serializes_to_one_event(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    ebull_test_conn.commit()
    barrier = Barrier(2)

    def register(reason: str) -> tuple[int, bool]:
        with psycopg.connect(test_database_url()) as conn:
            barrier.wait(timeout=5)
            with conn.transaction():
                result = advance_strategy_for_operator(
                    conn,
                    strategy_id=_STRATEGY_ID,
                    strategy_version=_VERSION,
                    action="register_candidate",
                    promoted_by="operator",
                    reason=reason,
                )
            return result.promotion.promotion_id, result.created

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(register, ("first submit", "concurrent retry")))

    assert results[0][0] == results[1][0]
    assert sorted(created for _promotion_id, created in results) == [False, True]
    assert ebull_test_conn.execute(
        "SELECT count(*) FROM strategy_promotions WHERE strategy_id=%s AND strategy_version=%s",
        (_STRATEGY_ID, _VERSION),
    ).fetchone() == (1,)


def test_historical_action_refuses_an_incomplete_denominator(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _register(ebull_test_conn)
    _store_matrix(ebull_test_conn, omit_last=True)

    with pytest.raises(StrategyControlError, match="expected=24, found=23"):
        advance_strategy_for_operator(
            ebull_test_conn,
            strategy_id=_STRATEGY_ID,
            strategy_version=_VERSION,
            action="validate_historical",
            promoted_by="operator",
            reason="must bind every declared cell",
        )


def test_historical_action_refuses_duplicate_cell_even_when_every_label_exists(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _register(ebull_test_conn)
    _store_matrix(ebull_test_conn, duplicate_first=True)

    with pytest.raises(StrategyControlError, match="expected=24, found=25.*duplicate=1"):
        advance_strategy_for_operator(
            ebull_test_conn,
            strategy_id=_STRATEGY_ID,
            strategy_version=_VERSION,
            action="validate_historical",
            promoted_by="operator",
            reason="duplicates are not completeness",
        )


def test_complete_matrix_is_replayed_and_bare_rows_still_cannot_promote(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _register(ebull_test_conn)
    _store_matrix(ebull_test_conn)

    with pytest.raises(StrategyControlError, match="fails promotion evidence"):
        advance_strategy_for_operator(
            ebull_test_conn,
            strategy_id=_STRATEGY_ID,
            strategy_version=_VERSION,
            action="validate_historical",
            promoted_by="operator",
            reason="complete shape is necessary but not sufficient",
        )
    assert ebull_test_conn.execute(
        "SELECT count(*) FROM strategy_promotions WHERE strategy_id=%s AND to_stage='historical_validated'",
        (_STRATEGY_ID,),
    ).fetchone() == (0,)


def test_prospective_assessment_cannot_reuse_pre_forward_observations(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_prospective_assessment(
        ebull_test_conn,
        forward_started_at=datetime(2026, 8, 1, 12, tzinfo=UTC),
        window_start=date(2026, 8, 1),
    )
    with pytest.raises(StrategyControlError, match="includes pre-forward observations"):
        _load_current_prospective_evidence(
            ebull_test_conn,
            strategy_id=_STRATEGY_ID,
            strategy_version=_VERSION,
            now=datetime(2026, 8, 15, 12, tzinfo=UTC),
        )


def test_prospective_assessment_accepts_only_a_wholly_post_forward_window(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_prospective_assessment(
        ebull_test_conn,
        forward_started_at=datetime(2026, 7, 1, 12, tzinfo=UTC),
        window_start=date(2026, 7, 2),
    )
    evidence = _load_current_prospective_evidence(
        ebull_test_conn,
        strategy_id=_STRATEGY_ID,
        strategy_version=_VERSION,
        now=datetime(2026, 8, 15, 12, tzinfo=UTC),
    )

    assert evidence.assessment_id > 0
    assert evidence.evidence_ref.startswith("strategy-prospective-assessment:sha256:")


def test_paper_forward_floor_reads_the_frozen_declaration_and_resolved_dates(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_prospective_assessment(
        ebull_test_conn,
        forward_started_at=datetime(2026, 7, 1, 12, tzinfo=UTC),
        window_start=date(2026, 7, 2),
    )
    declaration_id = _freeze_capital_declaration(ebull_test_conn)
    ebull_test_conn.execute("INSERT INTO exchanges (exchange_id,country,asset_class) VALUES ('2770','US','us_equity')")
    ebull_test_conn.execute(
        "INSERT INTO instruments (instrument_id,symbol,company_name,exchange,currency,is_tradable) "
        "VALUES (2770001,'FWD','Forward floor fixture','2770','USD',true)"
    )
    signal_id = ebull_test_conn.execute(
        """
        INSERT INTO strategy_signals (
            strategy_id,strategy_version,instrument_id,signal_bar_date,signal_kind,
            verdict,fill_bar_date,fill_price,universe,input_rule_set_versions,created_at
        ) VALUES (%s,%s,2770001,'2026-07-02','entry','fired','2026-07-03',100,
                  'survivor_only','{"indicator_series":"rules-v1"}'::jsonb,'2026-07-02 12:00+00')
        RETURNING signal_id
        """,
        (_STRATEGY_ID, _VERSION),
    ).fetchone()
    assert signal_id is not None
    ebull_test_conn.execute(
        """
        INSERT INTO strategy_outcomes (
            signal_id,rule_set_version,input_rule_set_version,outcome,resolution_method,
            exit_bar_date,exit_price,bars_held,gross_return_pct
        ) VALUES (%s,%s,%s,'expired','daily_bar','2026-07-08',101,3,1)
        """,
        (signal_id[0], OUTCOME_RULE_SET_VERSION, QUARANTINE_RULE_SET_VERSION),
    )

    evidence = load_forward_floor_evidence(
        ebull_test_conn,
        strategy_id=_STRATEGY_ID,
        strategy_version=_VERSION,
        now=datetime(2026, 8, 15, 12, tzinfo=UTC),
    )

    assert evidence.declaration_id == declaration_id
    assert (evidence.resolved_signals, evidence.decision_dates) == (1, 1)
    assert evidence.elapsed_days == 45


def test_paper_forward_floor_refuses_zero_resolved_decision_dates(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_prospective_assessment(
        ebull_test_conn,
        forward_started_at=datetime(2026, 7, 1, 12, tzinfo=UTC),
        window_start=date(2026, 7, 2),
    )
    _freeze_capital_declaration(ebull_test_conn)

    with pytest.raises(StrategyControlError, match="decision-date floor"):
        load_forward_floor_evidence(
            ebull_test_conn,
            strategy_id=_STRATEGY_ID,
            strategy_version=_VERSION,
            now=datetime(2026, 8, 15, 12, tzinfo=UTC),
        )


def test_request_vocabulary_has_no_live_or_arbitrary_stage() -> None:
    with pytest.raises(ValidationError):
        StrategyPromotionRequest(
            strategy_version="v1",
            action="live_enabled",  # type: ignore[arg-type]
            reason="must be impossible",
        )
    with pytest.raises(ValidationError):
        StrategyPromotionRequest(
            strategy_version="v1",
            action="paper_enabled",  # type: ignore[arg-type]
            reason="destinations are not request actions",
        )


def test_execution_policy_rejects_non_finite_limits_before_database_access(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    with pytest.raises(StrategyControlError, match="must be finite"):
        configure_execution_policy(
            ebull_test_conn,
            deployment_id=1,
            ticket_sizing_mode="percent",
            ticket_fraction=Decimal("0.1"),
            fixed_ticket_amount=None,
            max_ticket_amount=Decimal("10"),
            stop_loss_pct=Decimal("5"),
            take_profit_pct=Decimal("10"),
            max_quote_age_seconds=30,
            max_scan_age_seconds=300,
            max_halt_feed_age_seconds=300,
            max_cost_age_seconds=3600,
            max_reconciliation_age_seconds=60,
            max_instrument_exposure_pct=Decimal("20"),
            max_portfolio_exposure_pct=Decimal("50"),
            max_drawdown_pct=Decimal("10"),
            min_net_expectancy_pct=Decimal("NaN"),
            cost_stress_multiplier=Decimal("2"),
            changed_by="operator",
            reason="must reject NaN",
        )


def test_paper_retry_refuses_an_unresolvable_prospective_evidence_reference(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_legacy_unlinked_paper_chain(ebull_test_conn)
    with pytest.raises(StrategyControlError, match="prospective evidence link is missing"):
        advance_strategy_for_operator(
            ebull_test_conn,
            strategy_id=_STRATEGY_ID,
            strategy_version=_VERSION,
            action="approve_paper",
            promoted_by="operator",
            reason="a hash without its evidence row is not auditable",
        )


def test_promotion_assessment_link_has_restricting_foreign_keys(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    rows = ebull_test_conn.execute(
        """
        SELECT confrelid::regclass::text,confdeltype,pg_get_constraintdef(oid)
        FROM pg_constraint
        WHERE conrelid='strategy_promotion_forward_evidence'::regclass
          AND contype='f'
        ORDER BY confrelid::regclass::text
        """
    ).fetchall()
    assert [(row[0], row[1]) for row in rows] == [
        ("strategy_forecast_assessments", "r"),
        ("strategy_preregistration_declarations", "r"),
        ("strategy_promotions", "r"),
    ]
    definitions = "\n".join(str(row[2]) for row in rows)
    assert "FOREIGN KEY (assessment_id, strategy_id, strategy_version)" in definitions
    assert "FOREIGN KEY (declaration_id, strategy_id, strategy_version)" in definitions
    assert "FOREIGN KEY (promotion_id, strategy_id, strategy_version, promotion_stage)" in definitions


def test_promotion_sources_and_forward_evidence_are_immutable(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    trigger_tables = ebull_test_conn.execute(
        """
        SELECT event_object_table
        FROM information_schema.triggers
        WHERE trigger_name IN (
            'trg_strategy_promotions_immutable',
            'trg_strategy_forecast_assessments_immutable',
            'trg_strategy_promotion_forward_evidence_immutable'
        )
        ORDER BY event_object_table
        """
    ).fetchall()
    assert trigger_tables == [
        ("strategy_forecast_assessments",),
        ("strategy_forecast_assessments",),
        ("strategy_promotion_forward_evidence",),
        ("strategy_promotion_forward_evidence",),
        ("strategy_promotions",),
        ("strategy_promotions",),
    ]


def test_first_paper_setup_creates_disabled_deployment_and_explicit_policy_atomically(
    ebull_test_conn: psycopg.Connection[tuple], monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed_legacy_unlinked_paper_chain(ebull_test_conn)
    configure_paper_pool(
        ebull_test_conn,
        enabled=False,
        capital_limit=Decimal("1000"),
        risk_profile="unconfigured",
        changed_by="operator",
        reason="bound the demo pool",
    )
    monkeypatch.setattr(
        "app.api.strategies._current_versions",
        lambda: {_STRATEGY_ID: _VERSION},
    )
    monkeypatch.setattr(
        "app.api.strategies.get_strategy_overview",
        lambda _conn: SimpleNamespace(
            strategies=[
                SimpleNamespace(
                    strategy_id=_STRATEGY_ID,
                    allocation=SimpleNamespace(policy_configured=False),
                    allocation_refusals=["execution_policy_missing"],
                )
            ]
        ),
    )
    response = create_strategy_paper_setup(
        _STRATEGY_ID,
        StrategyInitialPaperSetupRequest(
            strategy_version=_VERSION,
            capital_limit=Decimal("100"),
            ticket_sizing_mode="percent",
            ticket_value=Decimal("10"),
            max_ticket_amount=Decimal("25"),
            stop_loss_pct=Decimal("5"),
            take_profit_pct=Decimal("10"),
            max_quote_age_seconds=30,
            max_scan_age_seconds=300,
            max_halt_feed_age_seconds=300,
            max_cost_age_seconds=3600,
            max_reconciliation_age_seconds=60,
            max_instrument_exposure_pct=Decimal("20"),
            max_portfolio_exposure_pct=Decimal("50"),
            max_drawdown_pct=Decimal("10"),
            min_net_expectancy_pct=Decimal("0"),
            cost_stress_multiplier=Decimal("2"),
            reason="explicit first paper limits",
        ),
        _session(),
        ebull_test_conn,
    )

    assert response.enabled is False
    assert response.capital_limit == Decimal("100")
    assert ebull_test_conn.execute(
        "SELECT capital_limit,enabled FROM strategy_deployments WHERE deployment_id=%s",
        (response.deployment_id,),
    ).fetchone() == (Decimal("100"), False)
    assert ebull_test_conn.execute(
        "SELECT ticket_fraction,max_ticket_amount,cost_stress_multiplier "
        "FROM strategy_execution_policies WHERE deployment_id=%s",
        (response.deployment_id,),
    ).fetchone() == (Decimal("0.1"), Decimal("25"), Decimal("2"))
