"""Operator CLI — run the reconciliation spot-check.

Usage::

    uv run python scripts/run_reconciliation.py --sample-size 25
    uv run python scripts/run_reconciliation.py --sample-size 25 --seed 1234

Picks N random instruments and runs every registered reconciliation
check against them, comparing what we have in SQL against live SEC
EDGAR. Drift findings land in ``data_reconciliation_findings`` for
operator triage.

``--seed`` reproduces the exact instrument cohort from a prior run —
useful for "is that finding still there?" workflows after a fix.

Operator audit 2026-05-03 made the case for a self-healing layer:
the system should know when something isn't right and flag it
without operator hand-curation. This is the mechanism. UI surface
on the ingest-health page lands as a follow-up PR.
"""

from __future__ import annotations

import argparse
import logging
import sys

import psycopg

from app.config import settings
from app.services.reconciliation import run_spot_check


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run a reconciliation spot-check.")
    p.add_argument(
        "--sample-size",
        type=int,
        default=25,
        help="Number of random instruments to spot-check (default 25).",
    )
    p.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Reproducible seed for the instrument selection. Omit to pick a fresh random cohort.",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _parse_args(argv if argv is not None else sys.argv[1:])

    with psycopg.connect(settings.database_url) as conn:
        summary = run_spot_check(
            conn,
            sample_size=args.sample_size,
            sample_seed=args.seed,
            triggered_by="operator",
        )

    print(
        f"Reconciliation run {summary.run_id}: "
        f"checked={summary.instruments_checked} "
        f"findings={summary.findings_emitted}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
