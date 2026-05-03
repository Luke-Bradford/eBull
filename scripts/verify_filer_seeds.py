"""Operator CLI — verify every active institutional_filer_seeds row
against SEC's live submissions.json.

Usage::

    uv run python -m scripts.verify_filer_seeds

Walks every ``active=TRUE`` seed, fetches SEC submissions.json
(write-through-cached via cik_raw_documents), and compares the
recorded ``expected_name`` against SEC's authoritative entity
``name`` field. Output groups results by status so an operator can
triage drift in one read.

Operator audit 2026-05-03 (issue #807) found a 6-of-10 mis-label
rate on the prior hand-curated seed list. At a 150-row scale, a
similar rate would silently mis-attribute thousands of 13F
holdings. This sweep is the gate.
"""

from __future__ import annotations

import logging
import sys
from collections import Counter

import psycopg

from app.config import settings
from app.services.filer_seed_verification import verify_all_active


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    counts: Counter[str] = Counter()
    drifts: list[str] = []
    fetch_errors: list[str] = []

    with psycopg.connect(settings.database_url) as conn:
        for result in verify_all_active(conn):
            counts[result.status] += 1
            if result.status == "drift":
                drifts.append(f"  {result.cik}: expected={result.expected_name!r} sec={result.sec_name!r}")
            elif result.status == "fetch_error":
                fetch_errors.append(f"  {result.cik}: {result.detail}")

    print(f"Filer seed verification: {sum(counts.values())} active seeds")
    for status in ("match", "drift", "missing", "fetch_error"):
        print(f"  {status}: {counts.get(status, 0)}")

    if drifts:
        print("\nDrift findings (operator triage):")
        for line in drifts:
            print(line)

    if fetch_errors:
        print("\nFetch errors (transient — re-run later):")
        for line in fetch_errors:
            print(line)

    # Exit non-zero unless EVERY active seed verified clean.
    # Downstream automation (a future CI gate, scheduler check)
    # treats anything other than a fully-matching cohort as
    # unverified — drift, missing, AND fetch_error all block.
    # Codex pre-push review caught the prior "drift only" exit
    # rule, which would have let a sweep with all 14 seeds in
    # fetch_error pass silently.
    total = sum(counts.values())
    matched = counts.get("match", 0)
    return 0 if total > 0 and matched == total else 1


if __name__ == "__main__":
    sys.exit(main())
