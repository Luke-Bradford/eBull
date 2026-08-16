#!/usr/bin/env python3
"""Measure a real production collector, then stop before cohort fan-out.

This runs the unchanged S-1 evaluation over either a hard-capped survivor-only
slice or the full production universe. At the exact ``run_cohort`` boundary it
diverts the populated production ``CohortCollector`` into #2772's fixed worker
canary or three-member launch pilot and raises an internal stop signal. No
cohort result is built and no result row is written.

    uv run python -m scripts.verify_2772_production_worker_canary \
      --series-limit 32 --output /tmp/ebull-2772-worker-canary.json

    uv run python -m scripts.verify_2772_production_worker_canary \
      --full-launch-pilot --output /tmp/ebull-2772-full-launch-pilot.json
"""

from __future__ import annotations

import argparse
import cProfile
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any
from unittest.mock import patch

import psycopg

from app.config import settings
from app.services import backtest_run
from app.services.backtest_run import (
    BACKTEST_UNIVERSE,
    CONTROL_NAMESPACE,
    evaluate_arm,
    evaluate_level_arms,
    load_corpus,
)
from app.services.cost_model import COST_MODEL_ID
from app.services.random_entry_cohort import SPEC_COHORT_SIZE
from app.services.strategies.s1_time_series_momentum import S1_STRATEGY_ID
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.synthetic_control_run import (
    CohortCollector,
    LaunchPilotReport,
    WorkerCanaryReport,
    _measure_member,
    _MemberInputs,
    run_launch_pilot,
    run_worker_canary,
)

MAX_CANARY_SERIES = 100
_PROFILE_OUTPUT: Path | None = None
_FULL_LAUNCH_PILOT = False


class _CanaryComplete(RuntimeError):
    def __init__(self, report: WorkerCanaryReport | LaunchPilotReport) -> None:
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
    if _PROFILE_OUTPUT is not None:
        inputs = _MemberInputs(
            placements=tuple(collector.placements),
            axis=tuple(axis),
            benchmark=benchmark,
            expected_trade_count=collector.matchable_trade_count,
        )
        profiler = cProfile.Profile()
        profiler.runcall(_measure_member, 0, inputs)
        profiler.dump_stats(_PROFILE_OUTPUT)
    report = (
        run_launch_pilot(collector, axis=axis, benchmark=benchmark)
        if _FULL_LAUNCH_PILOT
        else run_worker_canary(collector, axis=axis, benchmark=benchmark)
    )
    raise _CanaryComplete(report)


def main() -> int:
    global _FULL_LAUNCH_PILOT, _PROFILE_OUTPUT
    parser = argparse.ArgumentParser()
    parser.add_argument("--series-limit", type=int, default=32)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--profile-output", type=Path, help="optional cProfile data for fixed member 0; no outcomes")
    parser.add_argument("--full-launch-pilot", action="store_true")
    args = parser.parse_args()
    if not args.full_launch_pilot and not 2 <= args.series_limit <= MAX_CANARY_SERIES:
        parser.error(f"--series-limit must be between 2 and {MAX_CANARY_SERIES}")
    if args.full_launch_pilot and args.profile_output is not None:
        parser.error("--profile-output cannot add work to the fixed full launch pilot")
    _PROFILE_OUTPUT = args.profile_output
    _FULL_LAUNCH_PILOT = args.full_launch_pilot

    universe = BACKTEST_UNIVERSE if args.full_launch_pilot else "survivor_only"
    series_limit = None if args.full_launch_pilot else args.series_limit
    entry = STRATEGY_MANIFEST[S1_STRATEGY_ID]
    identity = entry.identity(universe=universe, cost_model_id=COST_MODEL_ID)
    with psycopg.connect(
        settings.database_url,
        options="-c statement_timeout=30000 -c default_transaction_read_only=on",
    ) as conn:
        corpus = load_corpus(conn, universe_basis=universe, limit=series_limit)
        try:
            with patch.object(backtest_run, "run_cohort", _canary_only):
                common = {
                    "corpus": corpus,
                    "quarantine_arm": "masked",
                    "identity": identity,
                    "namespaces": (CONTROL_NAMESPACE,),
                    "cohort_size": SPEC_COHORT_SIZE,
                }
                if corpus.termination:
                    evaluate_level_arms(conn, entry, **common)
                else:
                    evaluate_arm(conn, entry, ambiguity_arm=None, **common)
        except _CanaryComplete as completed:
            payload = {
                "canary_id": (
                    "full-production-launch-pilot-v1"
                    if args.full_launch_pilot
                    else "production-collector-worker-canary-v1"
                ),
                "strategy_id": S1_STRATEGY_ID,
                "universe": universe,
                "quarantine_arm": "masked",
                "series_limit": series_limit,
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
