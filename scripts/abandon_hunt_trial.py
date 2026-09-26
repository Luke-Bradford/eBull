"""Abandon a hunt trial whose infrastructure error keeps recurring (#3385, obligation 131).

Run ONLY from a reviewed PR that names the trial and the reason (spec "The audited door":
``abandoned`` is written only by a reviewed script). ``hunt_harness.abandon_trial``
refuses unless the trial is a registration with no outcome and its last
``ABANDON_MIN_FAILURES`` recorded failures share one error class. It prints the stored
``outcome_sha256`` and nothing else: an abandonment computes and releases no number.

    PYTHONPATH=. uv run python -m scripts.abandon_hunt_trial --trial 42 \\
        --reason "price read times out on series 1234 (#NNNN)" --by "<operator or session>"
"""

from __future__ import annotations

import argparse
import sys

import psycopg

from app.config import settings
from app.services import hunt_harness


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Abandon a hunt trial whose infrastructure error keeps recurring.")
    parser.add_argument("--trial", type=int, required=True, help="hunt_trials.hunt_trial_id")
    parser.add_argument("--reason", required=True, help="why, citing the reviewed PR or issue")
    parser.add_argument("--by", required=True, help="who is abandoning it")
    args = parser.parse_args(argv)
    with psycopg.connect(settings.database_url) as conn:
        try:
            outcome = hunt_harness.abandon_trial(conn, args.trial, reason=args.reason, abandoned_by=args.by)
        except hunt_harness.AbandonmentRefused as refused:
            print(f"refused: {refused}", file=sys.stderr)
            return 1
    print(f"hunt trial {args.trial} abandoned: outcome_sha256 {outcome}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
