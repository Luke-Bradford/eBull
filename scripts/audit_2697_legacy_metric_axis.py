#!/usr/bin/env python3
"""Outcome-blind structural audit of an explicitly named legacy run for #2697.

This script deliberately never selects a performance, trade, comparator,
bootstrap, deflation, or synthetic-control value. It establishes only that the
legacy invocation completed atomically, produced its exact declared identity
population, carries no invented metric-axis provenance, and is refused by the
current promotion rule as ``metric_axis_unproven``.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Final, cast

import psycopg
from psycopg.rows import dict_row

from app.config import settings
from app.services.strategy_result import (
    AmbiguityArm,
    ResultIdentity,
    ResultNamespace,
    ResultScope,
    metric_axis_promotion_refusals,
)
from scripts.audit_2745_in_sample_run import (
    EXPECTED_IDENTITY,
    EXPECTED_KEYS,
    EXPECTED_STRATEGY_VERSIONS,
    EXPECTED_TRIAL_COUNT,
)

_EXPECTED_PARAMS: Final = {
    "synthetic_control": True,
    "trial_register_version": "trial-register-2026-08-15-r6",
}

_AXIS_FIELDS: Final = (
    "metric_axis_rule_version",
    "metric_axis_dates",
    "metric_axis_start",
    "metric_axis_end",
    "metric_axis_digest",
    "opportunity_set_digest",
    "evidence_window_id",
)


class _NoMetricAccess:
    """Raise if the legacy refusal ever starts consulting an outcome value."""

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"legacy metric-axis refusal unexpectedly read performance field {name!r}")


@dataclass(frozen=True)
class StructuralAudit:
    run_id: int
    failures: tuple[str, ...]
    result_ids: tuple[int, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "audit": "metric-axis-legacy-structural-only",
            "performance_fields_read": False,
            "result_ids": list(self.result_ids),
            "row_count": len(self.result_ids),
            "all_refused_as": "metric_axis_unproven" if not self.failures else None,
            "failures": list(self.failures),
        }


def _identity(row: dict[str, Any]) -> ResultIdentity:
    return ResultIdentity(
        strategy_id=str(row["strategy_id"]),
        strategy_version=str(row["strategy_version"]),
        result_scope=cast(ResultScope, row["result_scope"]),
        namespace=cast(ResultNamespace, row["namespace"]),
        ambiguity_arm=cast(AmbiguityArm, row["ambiguity_arm"]),
        quarantine_arm=cast(Any, row["quarantine_arm"]),
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


def audit_rows(rows: list[dict[str, Any]], *, run_id: int) -> StructuralAudit:
    failures: list[str] = []
    keyed: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (str(row["strategy_id"]), str(row["ambiguity_arm"]), str(row["quarantine_arm"]))
        if key in keyed:
            failures.append(f"duplicate result key {key}")
        keyed[key] = row
        expected_version = EXPECTED_STRATEGY_VERSIONS.get(key[0])
        if expected_version is None or row["strategy_version"] != expected_version:
            failures.append(f"{key}: unexpected strategy version {row['strategy_version']!r}")
        for field, expected in EXPECTED_IDENTITY.items():
            actual = row[field]
            if field in {"window_start", "window_end"}:
                actual = actual.isoformat()
            if actual != expected:
                failures.append(f"{key}: {field} {actual!r} != {expected!r}")
        if row["purpose"] != "harness_validation":
            failures.append(f"{key}: purpose {row['purpose']!r} is not harness_validation")
        if row["carry_unmodelled"] is not False or row["fx_unmodelled"] is not False:
            failures.append(f"{key}: carry/fx provenance is not false/false")
        if row["trial_count"] != EXPECTED_TRIAL_COUNT:
            failures.append(f"{key}: trial_count {row['trial_count']!r} != {EXPECTED_TRIAL_COUNT}")
        non_null_axis = [field for field in _AXIS_FIELDS if row[field] is not None]
        if non_null_axis:
            failures.append(f"{key}: legacy row invented metric-axis fields {non_null_axis}")
        identity = _identity(row)
        if row["result_version"] != identity.version:
            failures.append(f"{key}: result-version digest does not reconstruct")
        refusal = metric_axis_promotion_refusals(
            cast(Any, SimpleNamespace(identity=identity, metrics=_NoMetricAccess()))
        )
        if refusal != ("metric_axis_unproven",):
            failures.append(f"{key}: current promotion refusal is {refusal!r}")

    missing = sorted(EXPECTED_KEYS - keyed.keys())
    extra = sorted(keyed.keys() - EXPECTED_KEYS)
    if len(rows) != len(EXPECTED_KEYS):
        failures.append(f"published {len(rows)} rows, expected {len(EXPECTED_KEYS)}")
    if missing:
        failures.append(f"missing result keys: {missing}")
    if extra:
        failures.append(f"unexpected result keys: {extra}")
    return StructuralAudit(
        run_id=run_id,
        failures=tuple(sorted(set(failures))),
        result_ids=tuple(sorted(int(row["result_id"]) for row in rows)),
    )


_RESULT_SQL: Final = """
SELECT result_id, strategy_id, strategy_version, result_version, result_scope,
       namespace, ambiguity_arm, quarantine_arm, window_start, window_end,
       purpose, universe_basis, corpus_version, cost_model_id,
       carry_unmodelled, fx_unmodelled, sizing_rule,
       benchmark_rule, position_rule_set_version, outcome_rule_set_version,
       input_rule_set_version, return_basis, ambiguity_rule_version,
       metric_set_id, trial_count, trial_register_version,
       metric_axis_rule_version, metric_axis_dates, metric_axis_start,
       metric_axis_end, metric_axis_digest, opportunity_set_digest,
       evidence_window_id
FROM strategy_results_store
WHERE created_at >= %(started_at)s
  AND created_at <= %(finished_at)s
ORDER BY strategy_id, ambiguity_arm, quarantine_arm, result_id
"""


def invocation_failures(
    *,
    job_name: object,
    params: object,
    request_id: object,
    request: tuple[object, object] | None,
) -> tuple[str, ...]:
    """Validate exact run/request provenance without consulting result relations."""
    failures: list[str] = []
    if job_name != "strategy_backtest_run":
        failures.append(f"unexpected job name {job_name!r}")
    if params != _EXPECTED_PARAMS:
        failures.append("job params_snapshot is not the exact declared r6 payload")
    if request is None:
        failures.append(f"linked request {request_id!r} is missing")
    else:
        request_job_name, payload = request
        if request_job_name != job_name:
            failures.append("linked request job name differs from run")
        if payload != {"control": {}, "params": _EXPECTED_PARAMS}:
            failures.append("linked request is not the exact declared r6 payload")
    return tuple(failures)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", type=int, required=True)
    args = parser.parse_args()
    if args.run_id <= 0:
        parser.error("--run-id must be positive")

    with psycopg.connect(settings.database_url) as conn:
        job = conn.execute(
            """SELECT job_name, status, started_at, finished_at, row_count,
                      error_msg, linked_request_id, params_snapshot
               FROM job_runs WHERE run_id = %s""",
            (args.run_id,),
        ).fetchone()
        if job is None:
            print(json.dumps({"run_id": args.run_id, "state": "missing"}, sort_keys=True))
            return 2
        job_name, status, started_at, finished_at, job_row_count, error, request_id, params = job
        if status != "success" or finished_at is None:
            print(
                json.dumps(
                    {"run_id": args.run_id, "state": status, "error": error},
                    sort_keys=True,
                    default=str,
                )
            )
            return 2
        request = conn.execute(
            "SELECT job_name, payload FROM pending_job_requests WHERE request_id = %s",
            (request_id,),
        ).fetchone()
        provenance_failures = invocation_failures(
            job_name=job_name,
            params=params,
            request_id=request_id,
            request=request,
        )
        if provenance_failures:
            print(
                json.dumps(
                    {
                        "run_id": args.run_id,
                        "audit": "metric-axis-legacy-structural-only",
                        "performance_fields_read": False,
                        "failures": provenance_failures,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            return 1
        with conn.cursor(row_factory=dict_row) as cursor:
            cursor.execute(_RESULT_SQL, {"started_at": started_at, "finished_at": finished_at})
            rows = list(cursor.fetchall())
        report = audit_rows(rows, run_id=args.run_id)
        failures = list(report.failures)
        if job_row_count != len(EXPECTED_KEYS):
            failures.append(f"job row_count {job_row_count!r} != {len(EXPECTED_KEYS)}")
            report = StructuralAudit(args.run_id, tuple(sorted(set(failures))), report.result_ids)
        print(json.dumps(report.as_dict(), indent=2, sort_keys=True))
        return 1 if report.failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
