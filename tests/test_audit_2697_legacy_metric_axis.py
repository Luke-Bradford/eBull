from __future__ import annotations

from datetime import date
from typing import Any

from app.services.strategy_result import ResultIdentity
from scripts.audit_2697_legacy_metric_axis import _AXIS_FIELDS, audit_rows
from scripts.audit_2745_in_sample_run import (
    AMBIGUITY_ARMS,
    EXPECTED_IDENTITY,
    EXPECTED_STRATEGY_VERSIONS,
    EXPECTED_TRIAL_COUNT,
    QUARANTINE_ARMS,
)


def _row(strategy_id: str, ambiguity: str, quarantine: str, result_id: int) -> dict[str, Any]:
    row: dict[str, Any] = {
        "result_id": result_id,
        "strategy_id": strategy_id,
        "strategy_version": EXPECTED_STRATEGY_VERSIONS[strategy_id],
        "result_scope": "sleeve",
        "namespace": "in_sample",
        "ambiguity_arm": ambiguity,
        "quarantine_arm": quarantine,
        "purpose": "harness_validation",
        "carry_unmodelled": False,
        "fx_unmodelled": False,
        "trial_count": EXPECTED_TRIAL_COUNT,
    }
    row.update(EXPECTED_IDENTITY)
    row["window_start"] = date.fromisoformat(str(row["window_start"]))
    row["window_end"] = date.fromisoformat(str(row["window_end"]))
    for field in _AXIS_FIELDS:
        row[field] = None
    identity = ResultIdentity(
        strategy_id=strategy_id,
        strategy_version=str(row["strategy_version"]),
        result_scope="sleeve",
        namespace="in_sample",
        ambiguity_arm=ambiguity,  # type: ignore[arg-type]
        quarantine_arm=quarantine,  # type: ignore[arg-type]
        sizing_rule=str(row["sizing_rule"]),
        benchmark_rule=str(row["benchmark_rule"]),
        cost_model_id=str(row["cost_model_id"]),
        corpus_version=str(row["corpus_version"]),
        window_start=row["window_start"],
        window_end=row["window_end"],
        position_rule_set_version=str(row["position_rule_set_version"]),
        outcome_rule_set_version=str(row["outcome_rule_set_version"]),
        input_rule_set_version=str(row["input_rule_set_version"]),
        return_basis=str(row["return_basis"]),
        ambiguity_rule_version=str(row["ambiguity_rule_version"]),
    )
    row["result_version"] = identity.version
    return row


def _complete() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for strategy in EXPECTED_STRATEGY_VERSIONS:
        for ambiguity in AMBIGUITY_ARMS:
            for quarantine in QUARANTINE_ARMS:
                rows.append(_row(strategy, ambiguity, quarantine, len(rows) + 1))
    return rows


def test_complete_legacy_population_is_structurally_refused_without_metrics() -> None:
    report = audit_rows(_complete())
    assert report.failures == ()
    assert len(report.result_ids) == 40


def test_one_invented_axis_field_fails_the_structural_audit() -> None:
    rows = _complete()
    rows[0]["metric_axis_rule_version"] = "full-namespace-panel-v1"
    report = audit_rows(rows)
    assert any("invented metric-axis fields" in failure for failure in report.failures)


def test_missing_arm_is_an_integrity_failure() -> None:
    report = audit_rows(_complete()[:-1])
    assert any("published 39 rows" in failure for failure in report.failures)
    assert any("missing result keys" in failure for failure in report.failures)
