from __future__ import annotations

from copy import deepcopy
from itertools import product
from typing import Any

from app.services.backtest_run import AMBIGUITY_ARM_ORDER, QUARANTINE_ARM_ORDER
from app.services.strategy_manifest import STRATEGY_MANIFEST
from scripts.audit_2697_metric_axis_ab import _CONTROL, _CONTROL_DELTA, _METRICS, audit_records

_HEAD = "a" * 40


def _numbers(keys: frozenset[str]) -> dict[str, object]:
    return {key: float(index + 1) for index, key in enumerate(sorted(keys))}


def _comparison(strategy: str, ambiguity: str, quarantine: str) -> dict[str, Any]:
    legacy_control: dict[str, object] = _numbers(_CONTROL - {"passed"})
    legacy_control["passed"] = False
    current_control: dict[str, object] = _numbers(_CONTROL - {"passed"})
    current_control["passed"] = True
    return {
        "record_type": "comparison",
        "candidate_head": _HEAD,
        "population": "full",
        "strategy_id": strategy,
        "ambiguity_arm": ambiguity,
        "quarantine_arm": quarantine,
        "legacy_axis": ["2000-01-03", "2021-06-28"],
        "current_axis": ["1962-01-02", "2021-06-28"],
        "legacy_comparator_population": 3,
        "current_opportunity_population": 10,
        "current_comparator_population": 10,
        "legacy": _numbers(_METRICS),
        "current": _numbers(_METRICS),
        "delta_current_minus_legacy": _numbers(_METRICS),
        "legacy_synthetic_thresholds": legacy_control,
        "current_synthetic_thresholds": current_control,
        "synthetic_threshold_delta_current_minus_legacy": _numbers(_CONTROL_DELTA),
    }


def _complete() -> list[dict[str, Any]]:
    rows = [
        _comparison(strategy, ambiguity, quarantine)
        for strategy, ambiguity, quarantine in product(
            STRATEGY_MANIFEST,
            AMBIGUITY_ARM_ORDER,
            QUARANTINE_ARM_ORDER,
        )
    ]
    rows.append(
        {
            "record_type": "acceptance_summary",
            "candidate_head": _HEAD,
            "population": "full",
            "expected_rows": len(rows),
            "observed_rows": len(rows),
            "complete": True,
        }
    )
    return rows


def test_complete_exact_head_population_passes_without_reporting_values() -> None:
    report = audit_records(_complete(), candidate_head=_HEAD)
    assert report.failures == ()
    assert report.comparison_count == 40
    assert report.as_dict()["performance_values_reported"] is False


def test_partial_file_without_final_summary_refuses() -> None:
    report = audit_records(_complete()[:-1], candidate_head=_HEAD)
    assert any("acceptance summary" in failure for failure in report.failures)


def test_same_count_but_duplicate_and_missing_key_refuses() -> None:
    rows = _complete()
    rows[1] = deepcopy(rows[0])
    report = audit_records(rows, candidate_head=_HEAD)
    assert any("duplicate comparison key" in failure for failure in report.failures)
    assert any("missing comparison keys" in failure for failure in report.failures)


def test_comparator_population_drift_refuses() -> None:
    rows = _complete()
    rows[0]["current_comparator_population"] = 9
    report = audit_records(rows, candidate_head=_HEAD)
    assert any("comparator population differs" in failure for failure in report.failures)


def test_non_finite_metric_refuses() -> None:
    rows = _complete()
    rows[0]["current"]["sharpe"] = float("nan")
    report = audit_records(rows, candidate_head=_HEAD)
    assert any("current metric payload" in failure for failure in report.failures)


def test_a_missing_current_result_never_becomes_acceptable_by_explaining_its_absence() -> None:
    rows = _complete()
    row = rows[0]
    row.pop("current")
    row.pop("current_axis")
    row["result_note"] = "current fixed-axis result is unexpectedly absent"

    report = audit_records(rows, candidate_head=_HEAD)

    assert any("current fixed-axis result is absent" in failure for failure in report.failures)


def test_legacy_axis_and_comparator_population_are_structural_evidence() -> None:
    malformed_axis = _complete()
    malformed_axis[0]["legacy_axis"] = ["2000-01-03"]
    assert any(
        "legacy axis endpoints are malformed" in failure
        for failure in audit_records(malformed_axis, candidate_head=_HEAD).failures
    )

    impossible_population = _complete()
    impossible_population[0]["legacy_comparator_population"] = 11
    assert any(
        "legacy comparator population exceeds" in failure
        for failure in audit_records(impossible_population, candidate_head=_HEAD).failures
    )


def test_declared_sortino_null_is_structurally_complete() -> None:
    rows = _complete()
    rows[0]["legacy"]["sortino"] = None
    rows[0]["current"]["sortino"] = None
    rows[0]["delta_current_minus_legacy"]["sortino"] = None
    assert audit_records(rows, candidate_head=_HEAD).failures == ()


def test_explicit_all_cash_legacy_absence_is_structurally_complete() -> None:
    rows = _complete()
    row = rows[0]
    for field in (
        "legacy_axis",
        "legacy_comparator_population",
        "legacy",
        "delta_current_minus_legacy",
        "legacy_synthetic_thresholds",
        "current_synthetic_thresholds",
    ):
        row.pop(field)
    row.update(
        {
            "legacy_result": False,
            "current_result": True,
            "result_note": "legacy position-selected span is undefined for an all-cash strategy",
            "legacy_synthetic_control": False,
            "current_synthetic_control": False,
            "synthetic_threshold_delta_current_minus_legacy": None,
            "synthetic_threshold_note": "no control is defined for an all-cash or absent result",
        }
    )
    assert audit_records(rows, candidate_head=_HEAD).failures == ()
