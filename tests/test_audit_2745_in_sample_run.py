from __future__ import annotations

from copy import deepcopy
from datetime import date
from typing import Any

from app.services.strategy_result import ResultIdentity
from scripts.audit_2745_in_sample_run import (
    AMBIGUITY_ARMS,
    EXPECTED_CONTROL_MODEL,
    EXPECTED_CONTROL_SEED,
    EXPECTED_CONTROL_SIZE,
    EXPECTED_IDENTITY,
    EXPECTED_STRATEGY_VERSIONS,
    EXPECTED_WALK_FORWARD_MODEL,
    QUARANTINE_ARMS,
    Attachments,
    audit_rows,
)


def _row(strategy_id: str, ambiguity: str, quarantine: str, result_id: int) -> dict[str, Any]:
    row: dict[str, Any] = {
        "result_id": result_id,
        "strategy_id": strategy_id,
        "strategy_version": EXPECTED_STRATEGY_VERSIONS[strategy_id],
        "ambiguity_arm": ambiguity,
        "quarantine_arm": quarantine,
        "purpose": "harness_validation",
        "carry_unmodelled": False,
        "fx_unmodelled": False,
        "evaluated_instrument_count": 100,
        "effective_sample_size": 80,
        "bootstrap_block_length": 5,
        "bootstrap_cluster_count": 40,
        "bootstrap_resamples": 2000,
        "bootstrap_seed": 20260808,
        "bootstrap_design_effect": 1.2,
        "bootstrap_model_id": "c3-block-bootstrap-v1",
        "deflated_sharpe": 0.8,
        "dsr_measured_trials": 10,
        "dsr_model_id": "c6-deflated-sharpe-v1",
        "trial_count": 272,
        "expectancy_per_trade_pct": 1,
        "expectancy_ci_low_pct": 0.1,
        "profit_factor": 1.2,
        "return_vs_buy_and_hold_pct": 0.2,
        "sharpe": 1.1,
        "trade_count": 50,
        "synthetic_control_model_id": EXPECTED_CONTROL_MODEL,
        "synthetic_control_size": EXPECTED_CONTROL_SIZE,
        "synthetic_control_root_seed": EXPECTED_CONTROL_SEED,
        "synthetic_control_mean_return_ci_low_pct": -0.1,
        "synthetic_control_mean_return_ci_high_pct": 0.1,
        "synthetic_control_sharpe_threshold": 1.0,
        "synthetic_control_passed": True,
    }
    row.update(EXPECTED_IDENTITY)
    row["window_start"] = date.fromisoformat(str(EXPECTED_IDENTITY["window_start"]))
    row["window_end"] = date.fromisoformat(str(EXPECTED_IDENTITY["window_end"]))
    row["result_version"] = ResultIdentity(
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
    ).version
    return row


def _attachments() -> Attachments:
    return Attachments(
        fold_count=4,
        walk_forward_model_id=EXPECTED_WALK_FORWARD_MODEL,
        universe_refusals=(),
        ambiguity_verdict=False,
        termination_census={
            "universe_admitted_total": 90,
            "universe_unlinked_alive_excluded": 5,
            "universe_unharvested_excluded": 5,
            "universe_vendor_series_total": 100,
        },
        regime_trade_count=50,
        regime_row_count=2,
    )


def _complete() -> tuple[list[dict[str, Any]], dict[int, Attachments]]:
    rows: list[dict[str, Any]] = []
    attached: dict[int, Attachments] = {}
    result_id = 1
    for strategy_id in EXPECTED_STRATEGY_VERSIONS:
        for ambiguity in AMBIGUITY_ARMS:
            for quarantine in QUARANTINE_ARMS:
                rows.append(_row(strategy_id, ambiguity, quarantine, result_id))
                attached[result_id] = _attachments()
                result_id += 1
    return rows, attached


def test_complete_conjunctive_population_can_survive() -> None:
    rows, attached = _complete()
    report = audit_rows(rows, attached)
    assert report.integrity_failures == ()
    assert report.survivors == tuple(sorted(EXPECTED_STRATEGY_VERSIONS))


def test_one_bad_arm_rejects_the_whole_strategy_without_ranking_arms() -> None:
    rows, attached = _complete()
    target = rows[0]
    target["expectancy_ci_low_pct"] = -0.01
    report = audit_rows(rows, attached)
    strategy_id = str(target["strategy_id"])
    assert strategy_id not in report.survivors
    assert any(
        reason.endswith(":expectancy_lower_bound_not_positive") for reason in report.strategy_refusals[strategy_id]
    )


def test_missing_parent_row_is_an_integrity_failure_not_a_strategy_result() -> None:
    rows, attached = _complete()
    removed = rows.pop()
    attached.pop(int(removed["result_id"]))
    report = audit_rows(rows, attached)
    assert any("published 39 parent rows" in failure for failure in report.integrity_failures)
    assert any("missing result keys" in failure for failure in report.integrity_failures)
    assert report.survivors == ()


def test_stored_synthetic_control_verdict_is_recomputed() -> None:
    rows, attached = _complete()
    target = rows[0]
    target["synthetic_control_mean_return_ci_low_pct"] = 0.01
    target["synthetic_control_passed"] = True
    report = audit_rows(deepcopy(rows), attached)
    reasons = report.strategy_refusals[str(target["strategy_id"])]
    assert any(reason.endswith(":synthetic_control_cohort_shows_edge") for reason in reasons)
    assert any(reason.endswith(":synthetic_control_stored_verdict_mismatch") for reason in reasons)
