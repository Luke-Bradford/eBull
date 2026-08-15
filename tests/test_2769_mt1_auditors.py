"""Outcome-free verifier/auditor contracts for #2769."""

from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path

from app.services.backtest_run import BACKTEST_UNIVERSE, corpus_version_for
from app.services.cost_model import COST_MODEL_ID
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_mt1_books import BOOK_RULE_VERSION
from app.services.strategy_mt1_identity import mt1_identity, s8_control_identity
from app.services.strategy_mt1_runner import MT1_SOURCE_STRATEGY_ID, S8_SOURCE_STRATEGY_ID
from app.services.strategy_mt1_trial import TRIAL_CONTRACT_VERSION, TRIAL_EVALUATOR_VERSION
from app.services.strategy_result import METRIC_AXIS_RULE_VERSION, metric_axis_sha256
from app.services.strategy_result_universe import ResultUniverseRecord, record_sha256
from app.services.trial_register import TRIAL_REGISTER_VERSION
from scripts.verify_2769_mt1_derivation import verify_persisted_inputs

_ROOT = Path(__file__).resolve().parents[1]


def _sha(payload: dict[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode()
    ).hexdigest()


def test_derivation_replay_requires_every_source_dependent_input_pin() -> None:
    axis = (date(2020, 1, 2), date(2020, 1, 3))
    opportunity = ResultUniverseRecord(
        universe_rule_version="test-universe-v1",
        evaluated_instrument_ids=frozenset({1}),
        validated_universe_ids=frozenset({1}),
    )
    mt1_source = STRATEGY_MANIFEST[MT1_SOURCE_STRATEGY_ID].identity(
        universe=BACKTEST_UNIVERSE, cost_model_id=COST_MODEL_ID
    )
    s8_source = STRATEGY_MANIFEST[S8_SOURCE_STRATEGY_ID].identity(
        universe=BACKTEST_UNIVERSE, cost_model_id=COST_MODEL_ID
    )
    mt1 = mt1_identity(universe=BACKTEST_UNIVERSE, cost_model_id=COST_MODEL_ID)
    s8 = s8_control_identity(universe=BACKTEST_UNIVERSE, cost_model_id=COST_MODEL_ID)
    payload: dict[str, object] = {"structural": "header"}
    row: dict[str, object] = {
        "book_rule_version": BOOK_RULE_VERSION,
        "corpus_version": corpus_version_for(BACKTEST_UNIVERSE),
        "cost_model_id": COST_MODEL_ID,
        "evaluator_version": TRIAL_EVALUATOR_VERSION,
        "metric_axis_dates": list(axis),
        "metric_axis_digest": metric_axis_sha256(axis),
        "metric_axis_rule_version": METRIC_AXIS_RULE_VERSION,
        "mt1_source_strategy_version": mt1_source.version,
        "mt1_strategy_version": mt1.version,
        "opportunity_set_digest": record_sha256(opportunity),
        "s8_source_strategy_version": s8_source.version,
        "s8_strategy_version": s8.version,
        "structural_evidence_json": payload,
        "structural_evidence_sha256": _sha(payload),
        "trial_contract_version": TRIAL_CONTRACT_VERSION,
        "trial_register_version": TRIAL_REGISTER_VERSION,
        "universe_basis": "survivorship_free",
    }
    assert verify_persisted_inputs(row, derived_axis=axis, derived_opportunity=opportunity) == []

    row["metric_axis_digest"] = "0" * 64
    assert verify_persisted_inputs(row, derived_axis=axis, derived_opportunity=opportunity) == ["metric_axis_digest"]


def test_structural_auditor_cannot_query_the_result_tables() -> None:
    source = (_ROOT / "scripts" / "audit_2769_mt1_structural.py").read_text()
    assert "FROM strategy_mt1_trial_result" not in source
    assert "JOIN strategy_mt1_trial_result" not in source
    assert "certainty_equivalent" not in source
    assert "maximum_drawdown" not in source
    assert "expected_shortfall" not in source
