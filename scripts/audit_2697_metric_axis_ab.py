#!/usr/bin/env python3
"""Structurally audit #2697's full-population A/B JSONL without ranking outcomes."""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from itertools import product
from pathlib import Path
from typing import Any, Final

from app.services.backtest_run import AMBIGUITY_ARM_ORDER, QUARANTINE_ARM_ORDER
from app.services.strategy_manifest import STRATEGY_MANIFEST
from scripts.verify_2697_metric_axis_ab import _exact_candidate_head

_METRICS: Final = frozenset(
    {
        "total_return_pct",
        "cagr_pct",
        "periods_per_year",
        "annualised_volatility_pct",
        "sharpe",
        "sortino",
        "max_drawdown_pct",
        "exposure_time_pct",
        "turnover_annualised",
        "buy_and_hold_return_pct",
        "return_vs_buy_and_hold_pct",
    }
)
_CONTROL: Final = frozenset({"cohort_sharpe_threshold", "cohort_return_threshold_pct", "passed"})
_CONTROL_DELTA: Final = frozenset({"cohort_sharpe_threshold", "cohort_return_threshold_pct"})
_EXPECTED_KEYS: Final = frozenset(product(STRATEGY_MANIFEST, AMBIGUITY_ARM_ORDER, QUARANTINE_ARM_ORDER))


@dataclass(frozen=True)
class AuditReport:
    candidate_head: str
    failures: tuple[str, ...]
    comparison_count: int

    def as_dict(self) -> dict[str, object]:
        return {
            "audit": "metric-axis-ab-structural-only",
            "candidate_head": self.candidate_head,
            "performance_values_reported": False,
            "expected_comparisons": len(_EXPECTED_KEYS),
            "observed_comparisons": self.comparison_count,
            "complete": not self.failures,
            "failures": list(self.failures),
        }


def _finite_payload(value: object, *, keys: frozenset[str]) -> bool:
    if not isinstance(value, dict) or set(value) != keys:
        return False
    return all(
        isinstance(item, (int, float)) and not isinstance(item, bool) and math.isfinite(float(item))
        for item in value.values()
    )


def _valid_metric_payload(value: object) -> bool:
    """Require every metric key; Sortino alone has a declared semantic null."""
    if not isinstance(value, dict) or set(value) != _METRICS:
        return False
    return all(
        (key == "sortino" and item is None)
        or (isinstance(item, (int, float)) and not isinstance(item, bool) and math.isfinite(float(item)))
        for key, item in value.items()
    )


def _valid_control(value: object) -> bool:
    return (
        isinstance(value, dict)
        and set(value) == _CONTROL
        and isinstance(value.get("passed"), bool)
        and _finite_payload(
            {key: item for key, item in value.items() if key != "passed"},
            keys=_CONTROL - {"passed"},
        )
    )


def audit_records(records: list[dict[str, Any]], *, candidate_head: str) -> AuditReport:
    failures: list[str] = []
    if not records:
        return AuditReport(candidate_head, ("evidence file is empty",), 0)
    summaries = [item for item in records if item.get("record_type") == "acceptance_summary"]
    comparisons = [item for item in records if item.get("record_type") == "comparison"]
    unknown = [
        item.get("record_type")
        for item in records
        if item.get("record_type") not in {"comparison", "acceptance_summary"}
    ]
    if unknown:
        failures.append(f"unknown record types: {unknown}")
    if len(summaries) != 1 or records[-1].get("record_type") != "acceptance_summary":
        failures.append("exactly one acceptance summary must be the final record")
    else:
        summary = summaries[0]
        if summary.get("candidate_head") != candidate_head:
            failures.append("summary candidate head does not match the audited source head")
        if summary.get("population") != "full":
            failures.append("summary is not labelled full population")
        if summary.get("complete") is not True:
            failures.append("summary does not declare completion")
        if summary.get("expected_rows") != len(_EXPECTED_KEYS) or summary.get("observed_rows") != len(comparisons):
            failures.append("summary counts do not reconcile to the declared comparison population")

    keyed: dict[tuple[str, str, str], dict[str, Any]] = {}
    current_axes: set[tuple[str, str]] = set()
    for row in comparisons:
        key = (str(row.get("strategy_id")), str(row.get("ambiguity_arm")), str(row.get("quarantine_arm")))
        if key in keyed:
            failures.append(f"duplicate comparison key {key}")
        keyed[key] = row
        label = "/".join(key)
        if row.get("candidate_head") != candidate_head:
            failures.append(f"{label}: candidate head mismatch")
        if row.get("population") != "full":
            failures.append(f"{label}: not labelled full population")
        if row.get("current_result") is False:
            failures.append(f"{label}: current fixed-axis result is absent")

        legacy_present = "legacy" in row
        current_present = "current" in row
        if legacy_present != ("legacy_axis" in row):
            failures.append(f"{label}: legacy result/axis presence disagrees")
        if current_present != ("current_axis" in row):
            failures.append(f"{label}: current result/axis presence disagrees")
        if not current_present:
            failures.append(f"{label}: current fixed-axis result is absent")
            continue
        if not legacy_present:
            if row.get("legacy_result") is not False or row.get("current_result") is not True:
                failures.append(f"{label}: absent legacy result flags are inconsistent")
            if row.get("result_note") != "legacy position-selected span is undefined for an all-cash strategy":
                failures.append(f"{label}: absent legacy result is not explicitly identified as all-cash")
        elif not _valid_metric_payload(row.get("legacy")):
            failures.append(f"{label}: legacy metric payload is incomplete or non-finite")
        if legacy_present:
            legacy_axis = row.get("legacy_axis")
            if (
                not isinstance(legacy_axis, list)
                or len(legacy_axis) != 2
                or not all(isinstance(item, str) for item in legacy_axis)
            ):
                failures.append(f"{label}: legacy axis endpoints are malformed")
            legacy_population = row.get("legacy_comparator_population")
            if (
                not isinstance(legacy_population, int)
                or isinstance(legacy_population, bool)
                or legacy_population < 1
            ):
                failures.append(f"{label}: legacy comparator population is not positive")
        if not _valid_metric_payload(row.get("current")):
            failures.append(f"{label}: current metric payload is incomplete or non-finite")
        if not _valid_metric_payload(row.get("delta_current_minus_legacy")) and legacy_present:
            failures.append(f"{label}: metric delta payload is incomplete or non-finite")

        axis = row.get("current_axis")
        if not isinstance(axis, list) or len(axis) != 2 or not all(isinstance(item, str) for item in axis):
            failures.append(f"{label}: current axis endpoints are malformed")
        else:
            current_axes.add((axis[0], axis[1]))
        opportunity = row.get("current_opportunity_population")
        comparator = row.get("current_comparator_population")
        if not isinstance(opportunity, int) or isinstance(opportunity, bool) or opportunity < 1:
            failures.append(f"{label}: opportunity population is not positive")
        if comparator != opportunity:
            failures.append(f"{label}: comparator population differs from opportunity population")
        if (
            legacy_present
            and isinstance(row.get("legacy_comparator_population"), int)
            and not isinstance(row.get("legacy_comparator_population"), bool)
            and isinstance(opportunity, int)
            and not isinstance(opportunity, bool)
            and row["legacy_comparator_population"] > opportunity
        ):
            failures.append(f"{label}: legacy comparator population exceeds the frozen opportunity population")

        legacy_control = row.get("legacy_synthetic_thresholds")
        current_control = row.get("current_synthetic_thresholds")
        delta = row.get("synthetic_threshold_delta_current_minus_legacy")
        if legacy_present:
            if not _valid_control(legacy_control):
                failures.append(f"{label}: legacy control payload is incomplete or non-finite")
            if not _valid_control(current_control):
                failures.append(f"{label}: current control payload is incomplete or non-finite")
            if not _finite_payload(delta, keys=_CONTROL_DELTA):
                failures.append(f"{label}: control delta payload is incomplete or non-finite")
        elif not (
            row.get("legacy_synthetic_control") is False
            and row.get("current_synthetic_control") is False
            and delta is None
            and row.get("synthetic_threshold_note") == "no control is defined for an all-cash or absent result"
        ):
            failures.append(f"{label}: absent all-cash controls are not represented consistently")

    missing = sorted(_EXPECTED_KEYS - keyed.keys())
    extra = sorted(keyed.keys() - _EXPECTED_KEYS)
    if len(comparisons) != len(_EXPECTED_KEYS):
        failures.append(f"observed {len(comparisons)} comparisons, expected {len(_EXPECTED_KEYS)}")
    if missing:
        failures.append(f"missing comparison keys: {missing}")
    if extra:
        failures.append(f"unexpected comparison keys: {extra}")
    if len(current_axes) != 1:
        failures.append(f"current comparisons carry {len(current_axes)} distinct fixed axes, expected one")
    return AuditReport(candidate_head, tuple(sorted(set(failures))), len(comparisons))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("evidence", type=Path)
    args = parser.parse_args()
    candidate_head = _exact_candidate_head()
    with args.evidence.open() as source:
        records = [json.loads(line) for line in source if line.strip()]
    report = audit_records(records, candidate_head=candidate_head)
    print(json.dumps(report.as_dict(), indent=2, sort_keys=True))
    return 1 if report.failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
