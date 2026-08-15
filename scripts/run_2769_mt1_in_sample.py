"""Run the preregistered, in-sample-only MT-1 controlled trial (#2769).

Without ``--execute`` this command validates the exact current declarations
and confirms that neither holdout outcomes nor holdout access records exist.
The execution path is deliberately two phase: it commits the complete
outcome-free structural fan before calculating any return statistic, then
commits all four robustness cells atomically.  It never loads a post-boundary
bar and it never selects a favourable ambiguity/quarantine cell.
"""

from __future__ import annotations

import argparse
import json
import sys

import psycopg

from app.config import settings
from app.services.backtest_run import BacktestProgressEvent
from app.services.strategy_mt1_runner import validate_mt1_preregistrations
from app.services.strategy_mt1_store import (
    MT1StoredEvaluation,
    MT1StoredRefusal,
    run_and_store_mt1_in_sample_evaluation,
)
from scripts._prereg_freeze_guard import assert_policy_version_merged

ACKNOWLEDGEMENT = "RUN-2769-PREREGISTERED-IN-SAMPLE"


def _progress(event: BacktestProgressEvent) -> None:
    sys.stderr.write(
        json.dumps(
            {
                "ambiguity_arm": event.ambiguity_arm,
                "phase": event.phase,
                "quarantine_arm": event.quarantine_arm,
                "series_seen": event.series_seen,
                "series_total": event.series_total,
                "strategy_id": event.strategy_id,
                "type": "progress",
            },
            sort_keys=True,
        )
        + "\n"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute",
        metavar="ACKNOWLEDGEMENT",
        help=f"run the one durable in-sample trial; required literal: {ACKNOWLEDGEMENT}",
    )
    args = parser.parse_args(argv)
    if args.execute is not None and args.execute != ACKNOWLEDGEMENT:
        parser.error(f"--execute requires the exact acknowledgement {ACKNOWLEDGEMENT!r}")

    policy = assert_policy_version_merged()
    with psycopg.connect(settings.database_url) as conn:
        if args.execute is None:
            authorities = validate_mt1_preregistrations(conn)
            sys.stdout.write(
                json.dumps(
                    {
                        **policy,
                        "declarations": [
                            {
                                "declaration_id": item.declaration_id,
                                "declaration_sha256": item.declaration_sha256,
                                "strategy_id": item.strategy_id,
                                "strategy_version": item.strategy_version,
                            }
                            for item in authorities
                        ],
                        "outcome": "authority_ready_no_outcomes_evaluated",
                    },
                    sort_keys=True,
                )
                + "\n"
            )
            return 0

        stored = run_and_store_mt1_in_sample_evaluation(conn, progress=_progress)
    if isinstance(stored, MT1StoredRefusal):
        sys.stdout.write(
            json.dumps(
                {
                    **policy,
                    "detail": stored.detail,
                    "outcome": "structural_gate_refused_no_performance_stored",
                    "structural_attempt_id": stored.structural_attempt_id,
                },
                sort_keys=True,
            )
            + "\n"
        )
        return 2
    assert isinstance(stored, MT1StoredEvaluation)
    sys.stdout.write(
        json.dumps(
            {
                **policy,
                "all_four_historical_conjuncts_pass": (stored.evaluation.bundle.historical_statistical_conjuncts_pass),
                "outcome": "complete_four_cell_result_stored",
                "structural_attempt_id": stored.structural_attempt_id,
                "trial_result_id": stored.trial_result_id,
            },
            sort_keys=True,
        )
        + "\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["ACKNOWLEDGEMENT", "main"]
