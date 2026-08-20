#!/usr/bin/env python3
"""Read-only, outcome-gated audit of run 98349 against the #2745 protocol.

The job status is read before any result metric. A non-successful job exits
without opening the result relations, preserving the invocation's atomic
evidence boundary. Once successful, this script DOES read outcome metrics, then
judges every strategy conjunctively across best/worst ambiguity and
masked/admitted quarantine arms. It never ranks rows or chooses a favourable
arm. Use ``audit_2697_legacy_metric_axis.py`` for the structural-only audit that
must precede any interpretation of this run.
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from decimal import Decimal
from itertools import product
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final, cast

import psycopg
from psycopg.rows import dict_row

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import settings  # noqa: E402
from app.services.result_ledger import read_walk_forward_folds  # noqa: E402
from app.services.strategy_result import (  # noqa: E402
    AmbiguityArm,
    ResultIdentity,
    ResultNamespace,
    ResultScope,
)
from app.services.strategy_result_ambiguity import (  # noqa: E402
    ambiguity_verdict,
    load_result_ambiguity,
)
from app.services.strategy_result_universe import (  # noqa: E402
    load_result_universe,
    load_termination_census,
    universe_promotion_refusals,
)

RUN_ID: Final = 98349
EXPECTED_STRATEGY_VERSIONS: Final = MappingProxyType(
    {
        "s1-time-series-momentum": "strategy-registry-v1+cd8a60d57047",
        "s2-cross-sectional-momentum": "strategy-registry-v1+b06b3560c9e3",
        "s3-mean-reversion-in-trend": "strategy-registry-v1+4b466c368e9c",
        "s4-volatility-compression-breakout": "strategy-registry-v1+3b9ed4d738fa",
        "s5-support-bounce": "strategy-registry-v1+878c7474c928",
        "s6-resistance-breakout": "strategy-registry-v1+61ff19557688",
        "s7-trend-pullback": "strategy-registry-v1+9240e675a1ed",
        "s8-range-mean-reversion": "strategy-registry-v1+9052ecd5fb62",
        "s9-squeeze-expansion": "strategy-registry-v1+a1af530c6fbb",
        "s10-relative-strength-leader": "strategy-registry-v1+42e74c7725a1",
    }
)
AMBIGUITY_ARMS: Final = ("best_case", "worst_case")
QUARANTINE_ARMS: Final = ("masked", "admitted")
EXPECTED_KEYS: Final = frozenset(product(EXPECTED_STRATEGY_VERSIONS, AMBIGUITY_ARMS, QUARANTINE_ARMS))

EXPECTED_IDENTITY: Final = MappingProxyType(
    {
        "result_scope": "sleeve",
        "namespace": "in_sample",
        "window_start": "1962-01-02",
        "window_end": "2024-09-27",
        "universe_basis": "survivorship_free",
        "corpus_version": "icyDenev/Intrader@2024-09-27",
        "cost_model_id": "static-p75-insession-v3+split-adjusted-max+carry-fx-structural-zero-long-x1-real-usd",
        "sizing_rule": "equal_weight_concurrent_v1",
        "benchmark_rule": "equal_weight_buy_and_hold_v1",
        "position_rule_set_version": "position-builder-v1+f46b7fede3d1",
        "outcome_rule_set_version": "outcome-resolver-v1+54aa83427048",
        "input_rule_set_version": "price-quarantine-v1+d0423dbd9cb5",
        "return_basis": "split-dividend-adjusted-wealth-v1",
        "ambiguity_rule_version": "ambiguity-verdict-2026-08-13-v1-no-cohort-threshold",
        "metric_set_id": "criterion7-v2",
        "trial_register_version": "trial-register-2026-08-15-r6",
    }
)
EXPECTED_TRIAL_COUNT: Final = 272
EXPECTED_FOLD_COUNT: Final = 4
EXPECTED_WALK_FORWARD_MODEL: Final = "c5-purged-walk-forward-v1"
EXPECTED_CONTROL_MODEL: Final = "permuted-entry-uniform-gap-v1"
EXPECTED_CONTROL_SIZE: Final = 1000
EXPECTED_CONTROL_SEED: Final = 20260808
EXPECTED_BOOTSTRAP_MODEL: Final = "c3-block-bootstrap-v1"
EXPECTED_BOOTSTRAP_RESAMPLES: Final = 2000
EXPECTED_BOOTSTRAP_SEED: Final = 20260808
EXPECTED_DSR_MODEL: Final = "c6-deflated-sharpe-v1"
REQUIRED_TERMINATION_STRATA: Final = frozenset(
    {
        "universe_admitted_total",
        "universe_unlinked_alive_excluded",
        "universe_unharvested_excluded",
        "universe_vendor_series_total",
    }
)


@dataclass(frozen=True)
class Attachments:
    fold_count: int
    walk_forward_model_id: str | None
    universe_refusals: tuple[str, ...]
    ambiguity_verdict: bool | None
    termination_census: dict[str, int]
    regime_trade_count: int
    regime_row_count: int


@dataclass(frozen=True)
class AuditReport:
    integrity_failures: tuple[str, ...]
    strategy_refusals: dict[str, tuple[str, ...]]

    @property
    def survivors(self) -> tuple[str, ...]:
        if self.integrity_failures:
            return ()
        return tuple(sorted(strategy for strategy, refusals in self.strategy_refusals.items() if not refusals))

    def as_dict(self) -> dict[str, object]:
        return {
            "run_id": RUN_ID,
            "integrity_failures": list(self.integrity_failures),
            "survivors": list(self.survivors),
            "strategy_refusals": {key: list(value) for key, value in sorted(self.strategy_refusals.items())},
        }


def _strictly_positive(value: object) -> bool:
    return value is not None and Decimal(str(value)).is_finite() and Decimal(str(value)) > 0


def _above_one(value: object) -> bool:
    return value is not None and Decimal(str(value)).is_finite() and Decimal(str(value)) > 1


def _row_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (str(row["strategy_id"]), str(row["ambiguity_arm"]), str(row["quarantine_arm"]))


def _identity_failures(row: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    strategy_id = str(row["strategy_id"])
    expected_version = EXPECTED_STRATEGY_VERSIONS.get(strategy_id)
    if expected_version is None:
        failures.append(f"unexpected strategy {strategy_id}")
    elif row["strategy_version"] != expected_version:
        failures.append(f"{strategy_id}: strategy_version {row['strategy_version']!r} != {expected_version!r}")
    for field, expected in EXPECTED_IDENTITY.items():
        actual = row[field]
        if field in {"window_start", "window_end"}:
            actual = actual.isoformat() if hasattr(actual, "isoformat") else str(actual)
        if actual != expected:
            failures.append(f"{strategy_id}: {field} {actual!r} != {expected!r}")
    if row["purpose"] != "harness_validation":
        failures.append(f"{strategy_id}: purpose is {row['purpose']!r}, not the frozen harness_validation run")
    if row["carry_unmodelled"] is not False or row["fx_unmodelled"] is not False:
        failures.append(f"{strategy_id}: carry/fx stamps are not false/false")
    recomputed = ResultIdentity(
        strategy_id=strategy_id,
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
    ).version
    if row["result_version"] != recomputed:
        failures.append(f"{strategy_id}: result_version {row['result_version']!r} != recomputed {recomputed!r}")
    return failures


def _row_refusals(row: dict[str, Any], attached: Attachments) -> list[str]:
    refusals: list[str] = []
    if row["evaluated_instrument_count"] is None or int(row["evaluated_instrument_count"]) < 1:
        refusals.append("no_evaluated_population")
    refusals.extend(attached.universe_refusals)
    if row["effective_sample_size"] is None:
        refusals.append("effective_sample_size_not_computed")
    bootstrap_fields = (
        "bootstrap_block_length",
        "bootstrap_cluster_count",
        "bootstrap_design_effect",
    )
    if (
        row["bootstrap_model_id"] != EXPECTED_BOOTSTRAP_MODEL
        or row["bootstrap_resamples"] != EXPECTED_BOOTSTRAP_RESAMPLES
        or row["bootstrap_seed"] != EXPECTED_BOOTSTRAP_SEED
        or any(row[field] is None for field in bootstrap_fields)
    ):
        refusals.append("bootstrap_provenance_incomplete")
    if row["deflated_sharpe"] is None:
        refusals.append("deflated_sharpe_not_computed")
    if row["dsr_model_id"] != EXPECTED_DSR_MODEL or row["dsr_measured_trials"] is None:
        refusals.append("deflated_sharpe_provenance_incomplete")
    if row["trial_count"] != EXPECTED_TRIAL_COUNT:
        refusals.append("trial_count_mismatch")
    if row["trial_register_version"] != EXPECTED_IDENTITY["trial_register_version"]:
        refusals.append("trial_register_superseded")
    if not _strictly_positive(row["expectancy_per_trade_pct"]):
        refusals.append("expectancy_not_positive")
    if not _strictly_positive(row["expectancy_ci_low_pct"]):
        refusals.append("expectancy_lower_bound_not_positive")
    if not _above_one(row["profit_factor"]):
        refusals.append("profit_factor_not_above_one")
    if not _strictly_positive(row["return_vs_buy_and_hold_pct"]):
        refusals.append("point_comparator_excess_not_positive")

    if row["synthetic_control_model_id"] != EXPECTED_CONTROL_MODEL:
        refusals.append("synthetic_control_model_mismatch")
    if row["synthetic_control_size"] != EXPECTED_CONTROL_SIZE:
        refusals.append("synthetic_control_size_mismatch")
    if row["synthetic_control_root_seed"] != EXPECTED_CONTROL_SEED:
        refusals.append("synthetic_control_seed_mismatch")
    low = row["synthetic_control_mean_return_ci_low_pct"]
    high = row["synthetic_control_mean_return_ci_high_pct"]
    if low is None or high is None or not (Decimal(str(low)) <= 0 <= Decimal(str(high))):
        refusals.append("synthetic_control_cohort_shows_edge")
    sharpe = row["sharpe"]
    threshold = row["synthetic_control_sharpe_threshold"]
    if sharpe is None or threshold is None or Decimal(str(sharpe)) <= Decimal(str(threshold)):
        refusals.append("synthetic_control_sharpe_below_cohort")
    recomputed_control_pass = not {
        "synthetic_control_cohort_shows_edge",
        "synthetic_control_sharpe_below_cohort",
    }.intersection(refusals)
    if row["synthetic_control_passed"] is not recomputed_control_pass:
        refusals.append("synthetic_control_stored_verdict_mismatch")

    if attached.ambiguity_verdict is None:
        refusals.append("ambiguity_arms_not_compared")
    elif attached.ambiguity_verdict:
        refusals.append("ambiguity_material")
    if attached.fold_count != EXPECTED_FOLD_COUNT:
        refusals.append("walk_forward_fold_count_mismatch")
    if attached.walk_forward_model_id != EXPECTED_WALK_FORWARD_MODEL:
        refusals.append("walk_forward_model_mismatch")
    census = attached.termination_census
    if not REQUIRED_TERMINATION_STRATA.issubset(census):
        refusals.append("termination_census_incomplete")
    elif (
        census["universe_admitted_total"]
        + census["universe_unlinked_alive_excluded"]
        + census["universe_unharvested_excluded"]
        != census["universe_vendor_series_total"]
    ):
        refusals.append("termination_census_not_reconciled")
    if row["trade_count"] is None:
        refusals.append("trade_count_missing")
        trade_count = 0
    else:
        trade_count = int(row["trade_count"])
    if attached.regime_trade_count != trade_count:
        refusals.append("regime_cohort_trade_count_mismatch")
    if (trade_count == 0) != (attached.regime_row_count == 0):
        refusals.append("regime_cohort_presence_mismatch")
    return refusals


def audit_rows(rows: list[dict[str, Any]], attachments: dict[int, Attachments]) -> AuditReport:
    integrity: list[str] = []
    keyed: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = _row_key(row)
        if key in keyed:
            integrity.append(f"duplicate result key {key}")
        keyed[key] = row
        integrity.extend(_identity_failures(row))
    missing = sorted(EXPECTED_KEYS - keyed.keys())
    extra = sorted(keyed.keys() - EXPECTED_KEYS)
    if len(rows) != len(EXPECTED_KEYS):
        integrity.append(f"published {len(rows)} parent rows, expected {len(EXPECTED_KEYS)}")
    if missing:
        integrity.append(f"missing result keys: {missing}")
    if extra:
        integrity.append(f"unexpected result keys: {extra}")

    per_strategy: dict[str, list[str]] = {strategy_id: [] for strategy_id in EXPECTED_STRATEGY_VERSIONS}
    for key in sorted(EXPECTED_KEYS & keyed.keys()):
        row = keyed[key]
        result_id = int(row["result_id"])
        attached = attachments.get(result_id)
        if attached is None:
            integrity.append(f"result {result_id} has no audited attachment bundle")
            continue
        per_strategy[key[0]].extend(f"{key[1]}/{key[2]}:{reason}" for reason in _row_refusals(row, attached))
    return AuditReport(
        integrity_failures=tuple(sorted(set(integrity))),
        strategy_refusals={key: tuple(sorted(set(value))) for key, value in per_strategy.items()},
    )


_RESULT_SQL: Final = """
SELECT result_id, strategy_id, strategy_version, result_version, result_scope, namespace,
       ambiguity_arm, quarantine_arm, window_start, window_end, universe_basis,
       corpus_version, cost_model_id, carry_unmodelled, fx_unmodelled, sizing_rule,
       position_rule_set_version, outcome_rule_set_version, input_rule_set_version,
       evaluated_instrument_count, trial_count, deflated_sharpe,
       expectancy_per_trade_pct, profit_factor, sharpe, trade_count,
       effective_sample_size, return_vs_buy_and_hold_pct, metric_set_id,
       expectancy_ci_low_pct, bootstrap_block_length, bootstrap_cluster_count,
       bootstrap_resamples, bootstrap_seed, bootstrap_design_effect, bootstrap_model_id,
       dsr_measured_trials, dsr_model_id, trial_register_version,
       synthetic_control_model_id, synthetic_control_size, synthetic_control_root_seed,
       synthetic_control_mean_return_ci_low_pct, synthetic_control_mean_return_ci_high_pct,
       synthetic_control_sharpe_threshold, synthetic_control_passed,
       benchmark_rule, purpose, return_basis, ambiguity_rule_version
FROM strategy_results
WHERE strategy_id = ANY(%(strategy_ids)s)
  AND namespace = 'in_sample'
  AND window_start = DATE '1962-01-02'
  AND window_end = DATE '2024-09-27'
  AND universe_basis = 'survivorship_free'
ORDER BY strategy_id, ambiguity_arm, quarantine_arm
"""


def _load_attachments(conn: psycopg.Connection[tuple[Any, ...]], rows: list[dict[str, Any]]) -> dict[int, Attachments]:
    attached: dict[int, Attachments] = {}
    for row in rows:
        result_id = int(row["result_id"])
        folds = read_walk_forward_folds(conn, result_id)
        universe = load_result_universe(conn, result_id)
        ambiguity = load_result_ambiguity(conn, result_id)
        census = load_termination_census(conn, result_id)
        cohort = conn.execute(
            "SELECT count(*), coalesce(sum(trade_count), 0) FROM strategy_result_regime_cohorts WHERE result_id = %s",
            (result_id,),
        ).fetchone()
        assert cohort is not None
        attached[result_id] = Attachments(
            fold_count=0 if folds is None else len(folds.folds),
            walk_forward_model_id=None if folds is None else folds.model_id,
            universe_refusals=universe_promotion_refusals(
                universe,
                evaluated_instrument_count=(
                    0 if row["evaluated_instrument_count"] is None else int(row["evaluated_instrument_count"])
                ),
            ),
            ambiguity_verdict=None if ambiguity is None else ambiguity_verdict(ambiguity),
            termination_census=census,
            regime_trade_count=int(cohort[1]),
            regime_row_count=int(cohort[0]),
        )
    return attached


def main() -> int:
    with psycopg.connect(settings.database_url) as conn:
        job = conn.execute("SELECT status, error_msg, row_count FROM job_runs WHERE run_id = %s", (RUN_ID,)).fetchone()
        if job is None:
            print(json.dumps({"run_id": RUN_ID, "state": "missing"}, sort_keys=True))
            return 2
        status, error, job_row_count = str(job[0]), job[1], job[2]
        if status != "success":
            print(json.dumps({"run_id": RUN_ID, "state": status, "error": error}, sort_keys=True, default=str))
            return 2
        with conn.cursor(row_factory=dict_row) as cursor:
            cursor.execute(_RESULT_SQL, {"strategy_ids": list(EXPECTED_STRATEGY_VERSIONS)})
            rows = list(cursor.fetchall())
        report = audit_rows(rows, _load_attachments(conn, rows))
        if job_row_count != len(EXPECTED_KEYS):
            report = AuditReport(
                integrity_failures=tuple(
                    sorted((*report.integrity_failures, f"job row_count {job_row_count!r} != {len(EXPECTED_KEYS)}"))
                ),
                strategy_refusals=report.strategy_refusals,
            )
        print(json.dumps(report.as_dict(), indent=2, sort_keys=True, default=str))
        return 1 if report.integrity_failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
