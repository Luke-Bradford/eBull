"""Reproduce #2488's S-4 equivalence and full-window timing evidence.

Read-only in ``compare-s4`` mode. ``full-run`` lets the production writer
execute inside one outer transaction, hashes every resulting store field, then
rolls the transaction back and verifies that the physical result count is
unchanged. PostgreSQL sequences are non-transactional, so the latter can leave
harmless identifier gaps even though it retains no evidence rows.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import resource
import time
from array import array
from dataclasses import replace
from decimal import Decimal
from typing import Any

import psycopg
from psycopg.rows import dict_row

from app.config import settings
from app.services.backtest_run import (
    AMBIGUITY_ARM_ORDER,
    BACKTEST_UNIVERSE,
    evaluate_arm,
    evaluate_level_arms,
    load_corpus,
    run_backtest,
)
from app.services.cost_model import COST_MODEL_ID
from app.services.research_price_structure_store import QuarantineArm
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_recent_evidence import recent_evidence_window

_S4 = "s4-volatility-compression-breakout"


def _jsonable(value: object) -> object:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {field.name: _jsonable(getattr(value, field.name)) for field in dataclasses.fields(value)}
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, array):
        return [_jsonable(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted(_jsonable(item) for item in value)  # type: ignore[type-var]
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()  # type: ignore[union-attr]
    return value


def _digest(value: object) -> str:
    payload = json.dumps(_jsonable(value), sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(payload.encode()).hexdigest()


def _resources(started_wall: float, started_cpu: float) -> dict[str, object]:
    return {
        "wall_seconds": time.monotonic() - started_wall,
        "cpu_seconds": time.process_time() - started_cpu,
        "peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
    }


def compare_s4(*, window_id: str, quarantine_arm: QuarantineArm, limit: int | None) -> dict[str, object]:
    window = recent_evidence_window(window_id).window
    entry = STRATEGY_MANIFEST[_S4]
    scalar_entry = replace(entry, exit_levels_batch=None)
    identity = entry.identity(universe=BACKTEST_UNIVERSE, cost_model_id=COST_MODEL_ID)
    with psycopg.connect(settings.database_url) as conn:
        corpus = load_corpus(conn, limit=limit, evaluation_window=window)
        reference_started_wall = time.monotonic()
        reference_started_cpu = time.process_time()
        reference = tuple(
            evaluate_arm(
                conn,
                scalar_entry,
                corpus=corpus,
                quarantine_arm=quarantine_arm,
                ambiguity_arm=ambiguity,
                identity=identity,
                namespaces=("hold_out",),
            )
            for ambiguity in AMBIGUITY_ARM_ORDER
        )
        reference_resources = _resources(reference_started_wall, reference_started_cpu)

        optimized_started_wall = time.monotonic()
        optimized_started_cpu = time.process_time()
        optimized = evaluate_level_arms(
            conn,
            entry,
            corpus=corpus,
            quarantine_arm=quarantine_arm,
            identity=identity,
            namespaces=("hold_out",),
        )
        optimized_resources = _resources(optimized_started_wall, optimized_started_cpu)

    reference_exact = tuple(replace(item, elapsed_s=0.0) for item in reference)
    optimized_exact = tuple(replace(item, elapsed_s=0.0) for item in optimized)
    return {
        "mode": "compare-s4",
        "window_id": window_id,
        "quarantine_arm": quarantine_arm,
        "series": len(corpus.pairs),
        "limit": limit,
        "reference": {**reference_resources, "measurement_sha256": _digest(reference_exact)},
        "optimized": {**optimized_resources, "measurement_sha256": _digest(optimized_exact)},
        "exact_equal_excluding_elapsed": reference_exact == optimized_exact,
    }


def full_run(*, window_id: str) -> dict[str, object]:
    window = recent_evidence_window(window_id).window
    started_wall = time.monotonic()
    started_cpu = time.process_time()
    with psycopg.connect(settings.database_url) as conn:
        with conn.cursor(row_factory=dict_row) as cursor:
            before_row = cursor.execute("SELECT count(*) AS count FROM strategy_results_store").fetchone()
        if before_row is None:  # pragma: no cover - aggregate always returns one row
            raise RuntimeError("strategy result count query returned no row")
        before = int(before_row["count"])
        report = run_backtest(
            conn,
            holdout_purpose="issue #2488 benchmark; transaction rolled back",
            holdout_accessed_by="codex benchmark",
            evaluation_window=window,
        )
        result_versions = [row.result_version for row in report.rows]
        with conn.cursor(row_factory=dict_row) as cursor:
            rows = cursor.execute(
                """
                SELECT *
                  FROM strategy_results_store
                 WHERE result_version = ANY(%(result_versions)s)
                 ORDER BY strategy_id, namespace, ambiguity_arm, quarantine_arm
                """,
                {"result_versions": result_versions},
            ).fetchall()
        if len(rows) != report.rows_written:
            raise RuntimeError(
                f"production report built {report.rows_written} rows but the exact result identities read back "
                f"{len(rows)} rows"
            )
        row_digest = _digest([dict(row) for row in rows])
        conn.rollback()
        with conn.cursor(row_factory=dict_row) as cursor:
            after_row = cursor.execute("SELECT count(*) AS count FROM strategy_results_store").fetchone()
        if after_row is None:  # pragma: no cover - aggregate always returns one row
            raise RuntimeError("strategy result count query returned no row after rollback")
        after = int(after_row["count"])
    return {
        "mode": "full-run",
        "window_id": window_id,
        "rows_built": report.rows_written,
        "store_rows_hashed": len(rows),
        "store_rows_sha256": row_digest,
        "physical_rows_before": before,
        "physical_rows_after_rollback": after,
        "rollback_preserved_store": before == after,
        "nontransactional_sequence_ids_may_have_advanced": True,
        **_resources(started_wall, started_cpu),
        "stage_seconds": {
            f"{arm.strategy_id}/{arm.ambiguity_arm or 'shared'}/{arm.quarantine_arm}": arm.elapsed_s
            for arm in report.arms
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("compare-s4", "full-run"))
    parser.add_argument("--window", default="year-2026-ytd")
    parser.add_argument("--arm", choices=("admitted", "masked"), default="admitted")
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()
    result: dict[str, Any]
    if args.mode == "compare-s4":
        result = compare_s4(window_id=args.window, quarantine_arm=args.arm, limit=args.limit)
    else:
        result = full_run(window_id=args.window)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
