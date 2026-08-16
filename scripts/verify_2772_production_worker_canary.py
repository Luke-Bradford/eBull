#!/usr/bin/env python3
"""Measure a real production collector, then stop before cohort fan-out.

This runs the unchanged S-1 evaluation over a hard-capped survivor-only corpus
slice. At the exact ``run_cohort`` boundary it diverts the populated production
``CohortCollector`` into #2772's fixed 1/2/4-worker canary and raises an internal
stop signal. No cohort result is built and no result row is written.

    uv run python -m scripts.verify_2772_production_worker_canary \
      --series-limit 32 --output /tmp/ebull-2772-worker-canary.json
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any
from unittest.mock import patch

import psycopg

from app.config import settings
from app.services import backtest_run
from app.services.backtest_run import CONTROL_NAMESPACE, evaluate_arm, load_corpus
from app.services.cost_model import COST_MODEL_ID
from app.services.random_entry_cohort import SPEC_COHORT_SIZE
from app.services.strategies.s1_time_series_momentum import S1_STRATEGY_ID
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.synthetic_control_run import CohortCollector, WorkerCanaryReport, run_worker_canary

MAX_CANARY_SERIES = 100


class _CanaryComplete(RuntimeError):
    def __init__(self, report: WorkerCanaryReport) -> None:
        super().__init__("production worker canary completed and stopped before cohort fan-out")
        self.report = report


def _canary_only(
    collector: CohortCollector,
    *,
    axis: Any,
    benchmark: Any,
    cohort_size: int,
    **_unused: Any,
) -> None:
    if cohort_size != SPEC_COHORT_SIZE:
        raise RuntimeError(f"production boundary supplied cohort size {cohort_size}, expected {SPEC_COHORT_SIZE}")
    raise _CanaryComplete(run_worker_canary(collector, axis=axis, benchmark=benchmark))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--series-limit", type=int, default=32)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not 2 <= args.series_limit <= MAX_CANARY_SERIES:
        parser.error(f"--series-limit must be between 2 and {MAX_CANARY_SERIES}")

    universe = "survivor_only"
    entry = STRATEGY_MANIFEST[S1_STRATEGY_ID]
    identity = entry.identity(universe=universe, cost_model_id=COST_MODEL_ID)
    with psycopg.connect(
        settings.database_url,
        options="-c statement_timeout=30000 -c default_transaction_read_only=on",
    ) as conn:
        corpus = load_corpus(conn, universe_basis=universe, limit=args.series_limit)
        try:
            with patch.object(backtest_run, "run_cohort", _canary_only):
                evaluate_arm(
                    conn,
                    entry,
                    corpus=corpus,
                    quarantine_arm="masked",
                    ambiguity_arm=None,
                    identity=identity,
                    namespaces=(CONTROL_NAMESPACE,),
                    cohort_size=SPEC_COHORT_SIZE,
                )
        except _CanaryComplete as completed:
            payload = {
                "canary_id": "production-collector-worker-canary-v1",
                "strategy_id": S1_STRATEGY_ID,
                "universe": universe,
                "quarantine_arm": "masked",
                "series_limit": args.series_limit,
                "outcome_fields_emitted": False,
                "database_writes": 0,
                "report": asdict(completed.report),
            }
        else:  # pragma: no cover - the interceptor is the safety boundary
            raise RuntimeError("evaluation passed the worker-canary boundary instead of stopping")

    rendered = json.dumps(payload, indent=2, sort_keys=True)
    if args.output is None:
        print(rendered)
    else:
        args.output.write_text(rendered + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
