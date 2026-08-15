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
import subprocess
import sys
from pathlib import Path

import psycopg

from app.config import settings
from app.services.backtest_run import BacktestProgressEvent
from app.services.strategy_mt1_runner import validate_mt1_preregistrations
from app.services.strategy_mt1_store import (
    MT1StoredEvaluation,
    MT1StoredRefusal,
    run_and_store_mt1_in_sample_evaluation,
)
from scripts._prereg_freeze_guard import assert_policy_version_merged, refresh_main_ref

ACKNOWLEDGEMENT = "RUN-2769-PREREGISTERED-IN-SAMPLE"
_REPO_ROOT = Path(__file__).resolve().parents[1]


def _git_output(*args: str) -> str | None:
    try:
        completed = subprocess.run(  # noqa: S603 - fixed executable and internal arguments
            ["git", *args],
            cwd=_REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return None
    return completed.stdout if completed.returncode == 0 else None


def assert_exact_clean_main_source() -> dict[str, object]:
    """Refuse immutable evidence unless this exact clean runner is on main."""
    if not refresh_main_ref():
        raise SystemExit("refusing MT-1: origin/main could not be refreshed")
    status = _git_output("status", "--porcelain")
    head = _git_output("rev-parse", "HEAD")
    main = _git_output("rev-parse", "origin/main")
    if status is None or head is None or main is None:
        raise SystemExit("refusing MT-1: the exact source head could not be established")
    if status.strip():
        raise SystemExit("refusing MT-1: the runner worktree is not clean")
    source_head = head.strip()
    main_head = main.strip()
    if len(source_head) != 40 or any(character not in "0123456789abcdef" for character in source_head):
        raise SystemExit("refusing MT-1: runner head is not an exact lower-case Git object ID")
    if source_head != main_head:
        raise SystemExit(
            f"refusing MT-1: runner head {source_head} is not exact origin/main {main_head}; merge and update first"
        )
    return {
        "runner_source_clean": True,
        "runner_source_head": source_head,
        "runner_source_matches_main": True,
    }


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

    source = assert_exact_clean_main_source()
    policy = assert_policy_version_merged()
    with psycopg.connect(settings.database_url) as conn:
        if args.execute is None:
            authorities = validate_mt1_preregistrations(conn)
            sys.stdout.write(
                json.dumps(
                    {
                        **policy,
                        **source,
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

        stored = run_and_store_mt1_in_sample_evaluation(
            conn,
            runner_source_head=str(source["runner_source_head"]),
            progress=_progress,
        )
    if isinstance(stored, MT1StoredRefusal):
        sys.stdout.write(
            json.dumps(
                {
                    **policy,
                    **source,
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
                **source,
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


__all__ = ["ACKNOWLEDGEMENT", "assert_exact_clean_main_source", "main"]
