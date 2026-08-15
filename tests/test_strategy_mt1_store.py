"""Two-phase persistence tests for the paved MT-1 runner (#2769)."""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from types import SimpleNamespace
from typing import Any, cast

import psycopg
import pytest

import app.services.strategy_mt1_store as store
from app.services.result_ledger import freeze_preregistration
from app.services.strategy_mt1_books import BOOK_RULE_VERSION, MT1FourArmBooks
from app.services.strategy_mt1_preregistration import build_declarations
from app.services.strategy_mt1_read_model import load_mt1_controlled_trial_state
from app.services.strategy_mt1_runner import (
    MT1HistoricalBundle,
    MT1InSamplePreparation,
    MT1InSampleStructuralRefusal,
    MT1InSampleStructuralRefused,
    MT1PreparedBundle,
    MT1PreparedCell,
    MT1PreregistrationAuthority,
    MT1RobustnessCell,
    MT1RunnerRefused,
    validate_mt1_preregistrations,
)
from app.services.strategy_mt1_trial import (
    ArmRiskReport,
    MT1TrialResult,
    PercentileInterval,
    ScaledBookStructuralAudit,
    StructuralGateReport,
)
from app.services.strategy_result_universe import ResultUniverseRecord

_KEYS = (
    ("best_case", "admitted"),
    ("best_case", "masked"),
    ("worst_case", "admitted"),
    ("worst_case", "masked"),
)
_RUNNER_HEAD = "a" * 40
_COMMON_MONTHS = tuple(date(2010 + offset // 12, offset % 12 + 1, 1) for offset in range(120))
_DECISION_DATES = (date(2020, 1, 2), date(2020, 2, 3))
_AXIS = (*_COMMON_MONTHS, *_DECISION_DATES)


def _months() -> tuple[date, ...]:
    return _COMMON_MONTHS


def _book() -> MT1FourArmBooks:
    audit = ScaledBookStructuralAudit(
        decision_dates=_DECISION_DATES,
        expected_decision_dates=_DECISION_DATES,
        annualised_turnover=1.0,
        traded_notional=100.0,
        exposure_reconciled=True,
    )
    source = SimpleNamespace(structural=audit)
    return cast(
        MT1FourArmBooks,
        SimpleNamespace(mt1=source, s8=source, rule_version=BOOK_RULE_VERSION),
    )


def _result(*, passed: bool = True) -> MT1TrialResult:
    risk = ArmRiskReport(certainty_equivalent=0.1, maximum_drawdown=0.1, expected_shortfall_5=-0.1)
    return MT1TrialResult(
        common_months=_months(),
        excluded_months_by_arm=(0, 0, 0, 0),
        structural=StructuralGateReport(2, 2, 1.0, 1.0, 100.0, 100.0),
        mt1_scaled=risk,
        mt1_unscaled=risk,
        s8_scaled=risk,
        s8_unscaled=risk,
        mt1_delta_cer=0.0,
        s8_delta_cer=0.0,
        primary_difference_in_differences=0.0,
        mt1_delta_interval=PercentileInterval(-0.1, 0.1),
        primary_interval=PercentileInterval(-0.1, 0.1),
        primary_lower_bound_positive=passed,
        mt1_lower_bound_positive=passed,
        mt1_drawdown_improved=passed,
        mt1_expected_shortfall_improved=passed,
        historical_statistical_conjuncts_pass=passed,
    )


def _preparation(conn: psycopg.Connection[tuple]) -> MT1InSamplePreparation:
    mt1, s8 = build_declarations()
    mt1_id = freeze_preregistration(conn, mt1)
    s8_id = freeze_preregistration(conn, s8)
    conn.commit()
    authorities = (
        MT1PreregistrationAuthority(mt1.strategy_id, mt1.strategy_version, mt1_id, mt1.sha256),
        MT1PreregistrationAuthority(s8.strategy_id, s8.strategy_version, s8_id, s8.sha256),
    )
    prepared = MT1PreparedBundle(
        cells=tuple(
            MT1PreparedCell(cast(object, ambiguity), cast(object, quarantine), _book())  # type: ignore[arg-type]
            for ambiguity, quarantine in _KEYS
        ),
        axis_dates=_AXIS,
        opportunity_record=ResultUniverseRecord(
            universe_rule_version="test-universe-v1",
            evaluated_instrument_ids=frozenset({1, 2}),
            validated_universe_ids=frozenset({1, 2}),
        ),
    )
    return MT1InSamplePreparation(
        authorities=authorities,
        prepared=prepared,
        mt1_strategy_version=mt1.strategy_version,
        s8_control_strategy_version=s8.strategy_version,
        mt1_source_strategy_version="mt1-source-v1",
        s8_source_strategy_version="s8-source-v1",
        corpus_version="corpus-v1",
        runner_source_head=_RUNNER_HEAD,
    )


def _bundle(preparation: MT1InSamplePreparation, *, passed: bool = True) -> MT1HistoricalBundle:
    return MT1HistoricalBundle(
        cells=tuple(
            MT1RobustnessCell(cell.ambiguity_arm, cell.quarantine_arm, cell.books, _result(passed=passed))
            for cell in preparation.prepared.cells
        ),
        axis_dates=preparation.prepared.axis_dates,
        opportunity_record=preparation.prepared.opportunity_record,
    )


def _refusal(preparation: MT1InSamplePreparation) -> MT1InSampleStructuralRefusal:
    return MT1InSampleStructuralRefusal(
        authorities=preparation.authorities,
        axis_dates=preparation.prepared.axis_dates,
        opportunity_record=preparation.prepared.opportunity_record,
        detail="turnover exceeded the frozen ceiling",
        mt1_strategy_version=preparation.mt1_strategy_version,
        s8_control_strategy_version=preparation.s8_control_strategy_version,
        mt1_source_strategy_version=preparation.mt1_source_strategy_version,
        s8_source_strategy_version=preparation.s8_source_strategy_version,
        corpus_version=preparation.corpus_version,
        runner_source_head=preparation.runner_source_head,
    )


def test_complete_structural_fan_commits_before_atomic_result_and_retries_read_back(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    preparation = _preparation(ebull_test_conn)
    attempt_id = store.store_mt1_structural_preparation(ebull_test_conn, preparation)
    assert ebull_test_conn.execute(
        "SELECT count(*) FROM strategy_mt1_structural_cells WHERE structural_attempt_id = %s", (attempt_id,)
    ).fetchone() == (4,)
    assert ebull_test_conn.execute(
        "SELECT runner_source_head,structural_evidence_json->>'runner_source_head' "
        "FROM strategy_mt1_structural_attempts WHERE structural_attempt_id=%s",
        (attempt_id,),
    ).fetchone() == (_RUNNER_HEAD, _RUNNER_HEAD)

    bundle = _bundle(preparation)
    result_id = store.store_mt1_trial_bundle(ebull_test_conn, structural_attempt_id=attempt_id, bundle=bundle)
    assert ebull_test_conn.execute(
        "SELECT count(*) FROM strategy_mt1_trial_result_cells WHERE mt1_trial_result_id = %s", (result_id,)
    ).fetchone() == (4,)
    assert store.store_mt1_structural_preparation(ebull_test_conn, preparation) == attempt_id
    assert store.store_mt1_trial_bundle(ebull_test_conn, structural_attempt_id=attempt_id, bundle=bundle) == result_id
    read_model = load_mt1_controlled_trial_state(ebull_test_conn)
    assert read_model.state == "historical_conjuncts_passed"
    assert len(read_model.result_cells) == 4
    assert (read_model.holdout_evaluations, read_model.holdout_accesses) == (0, 0)
    assert read_model.promotion_authority is False
    assert read_model.paper_activation_reachable is False
    assert read_model.live_activation_reachable is False


def test_same_versions_cannot_be_rewritten_with_different_evidence(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    preparation = _preparation(ebull_test_conn)
    store.store_mt1_structural_preparation(ebull_test_conn, preparation)
    changed = MT1InSamplePreparation(
        **{**preparation.__dict__, "corpus_version": "different-corpus"},
    )
    with pytest.raises(store.MT1EvidenceConflict, match="different structural evidence"):
        store.store_mt1_structural_preparation(ebull_test_conn, changed)
    ebull_test_conn.rollback()


def test_idempotent_child_match_refuses_digest_strings_that_do_not_authenticate_json() -> None:
    payload: dict[str, object] = {"canonical": "payload"}
    digest = store._digest(payload)
    expected = [("best_case", "admitted", digest, payload)]
    assert store._stored_cells_match(
        [("best_case", "admitted", digest, payload)],
        expected,
    )
    assert not store._stored_cells_match(
        [("best_case", "admitted", digest, {"canonical": "tampered"})],
        expected,
    )
    assert not store._stored_cells_match(
        [("best_case", "admitted", "0" * 64, payload)],
        expected,
    )


def test_database_frozen_declaration_term_mismatch_refuses_before_evaluation(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    mt1, s8 = build_declarations()
    changed = replace(
        mt1,
        forward_shadow=replace(mt1.forward_shadow, min_calendar_weeks=mt1.forward_shadow.min_calendar_weeks + 1),
    )
    freeze_preregistration(ebull_test_conn, changed)
    freeze_preregistration(ebull_test_conn, s8)
    ebull_test_conn.commit()
    with pytest.raises(MT1RunnerRefused, match="declaration_terms_changed=forward_shadow"):
        validate_mt1_preregistrations(ebull_test_conn)


def test_database_retry_refuses_a_metric_axis_derivation_drift(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    preparation = _preparation(ebull_test_conn)
    store.store_mt1_structural_preparation(ebull_test_conn, preparation)
    changed_axis = (*_COMMON_MONTHS, date(2019, 12, 15), *_DECISION_DATES)
    drifted = replace(preparation, prepared=replace(preparation.prepared, axis_dates=changed_axis))
    with pytest.raises(store.MT1EvidenceConflict, match="different structural evidence"):
        store.store_mt1_structural_preparation(ebull_test_conn, drifted)
    ebull_test_conn.rollback()


def test_structural_refusal_commits_no_cells_and_is_idempotent(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    refusal = _refusal(_preparation(ebull_test_conn))
    attempt_id = store.store_mt1_structural_refusal(ebull_test_conn, refusal)
    assert ebull_test_conn.execute(
        """
        SELECT passed, refusal_code,
               (SELECT count(*) FROM strategy_mt1_structural_cells c
                 WHERE c.structural_attempt_id = a.structural_attempt_id)
          FROM strategy_mt1_structural_attempts a WHERE structural_attempt_id = %s
        """,
        (attempt_id,),
    ).fetchone() == (False, "structural_gate_refused", 0)
    assert store.store_mt1_structural_refusal(ebull_test_conn, refusal) == attempt_id
    read_model = load_mt1_controlled_trial_state(ebull_test_conn)
    assert read_model.state == "structural_refused"
    assert read_model.refusal_detail == refusal.detail
    assert read_model.result_cells == ()


def test_failed_all_cell_conjunction_is_terminal_and_operator_visible(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    preparation = _preparation(ebull_test_conn)
    attempt_id = store.store_mt1_structural_preparation(ebull_test_conn, preparation)
    store.store_mt1_trial_bundle(
        ebull_test_conn,
        structural_attempt_id=attempt_id,
        bundle=_bundle(preparation, passed=False),
    )
    read_model = load_mt1_controlled_trial_state(ebull_test_conn)
    assert read_model.state == "historical_conjuncts_failed"
    assert read_model.historical_conjuncts_pass is False
    assert all(cell.historical_conjuncts_pass is False for cell in read_model.result_cells)


def test_paved_runner_commits_structure_before_evaluating_any_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    preparation = cast(
        MT1InSamplePreparation,
        SimpleNamespace(
            prepared=object(),
            authorities=(),
            mt1_strategy_version="mt1-v1",
            s8_control_strategy_version="s8-v1",
            mt1_source_strategy_version="source-mt1-v1",
            s8_source_strategy_version="source-s8-v1",
            corpus_version="corpus-v1",
            runner_source_head=_RUNNER_HEAD,
        ),
    )
    bundle = cast(MT1HistoricalBundle, object())
    events: list[str] = []
    monkeypatch.setattr(store, "prepare_mt1_in_sample_evaluation", lambda *_args, **_kwargs: preparation)

    def store_structure(*_args: object, **_kwargs: object) -> int:
        events.append("structural_commit")
        return 10

    def evaluate(_prepared: object) -> MT1HistoricalBundle:
        assert events == ["structural_commit"]
        events.append("evaluate")
        return bundle

    def store_result(*_args: object, **_kwargs: object) -> int:
        assert events == ["structural_commit", "evaluate"]
        events.append("result_commit")
        return 20

    monkeypatch.setattr(store, "store_mt1_structural_preparation", store_structure)
    monkeypatch.setattr(store, "evaluate_mt1_prepared_bundle", evaluate)
    monkeypatch.setattr(store, "store_mt1_trial_bundle", store_result)
    monkeypatch.setattr(store, "MT1InSampleEvaluation", lambda **_kwargs: cast(object, object()))

    result = store.run_and_store_mt1_in_sample_evaluation(  # type: ignore[arg-type]
        cast(Any, object()), runner_source_head=_RUNNER_HEAD
    )

    assert events == ["structural_commit", "evaluate", "result_commit"]
    assert isinstance(result, store.MT1StoredEvaluation)
    assert (result.structural_attempt_id, result.trial_result_id) == (10, 20)


def test_paved_runner_persists_structural_refusal_without_evaluating(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    refusal = cast(MT1InSampleStructuralRefusal, SimpleNamespace(detail="refused"))
    monkeypatch.setattr(
        store,
        "prepare_mt1_in_sample_evaluation",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(MT1InSampleStructuralRefused(refusal)),
    )
    monkeypatch.setattr(store, "store_mt1_structural_refusal", lambda _conn, evidence: 42)
    monkeypatch.setattr(
        store,
        "evaluate_mt1_prepared_bundle",
        lambda *_args: (_ for _ in ()).throw(AssertionError("outcomes must remain unevaluated")),
    )
    result = store.run_and_store_mt1_in_sample_evaluation(  # type: ignore[arg-type]
        cast(Any, object()), runner_source_head=_RUNNER_HEAD
    )

    assert isinstance(result, store.MT1StoredRefusal)
    assert (result.structural_attempt_id, result.detail) == (42, "refused")
