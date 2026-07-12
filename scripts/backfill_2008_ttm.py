"""One-shot #2008 TTM reconciliation backfill.

Two steps, in order:

1. Purge ``fundamentals_snapshot`` rows for instruments with NO
   ``financial_facts_raw`` facts — provider-era garbage with no periods
   backing and, post-#2008, no writer that would ever refresh them
   (the write-through only fires for instruments the normalizer visits).
2. ``normalize_financial_periods(conn)`` over every instrument with raw
   facts: re-derives ``financial_periods`` under the post-#1835
   duration-window rules (heals the frame-filter rot this issue found on
   CAT/AMZN/IMAX/…) and rewashes ``fundamentals_snapshot`` from the
   normalized rows via the Section-1 write-through (replaces the
   first-tag-wins garbage on NVDA/AMSC/TER/…).

Idempotent: both the periods rewash and the snapshot rewash are
DELETE-then-INSERT per instrument; re-running converges to the same
state. One-shot: the daily ``daily_financial_facts`` touched-CIK
normalize keeps everything current afterwards, and a fresh environment
gets the same result from ``fundamentals/bootstrap.py``.

Run from repo root:

    uv run python -m scripts.backfill_2008_ttm            # dry-run counts
    uv run python -m scripts.backfill_2008_ttm --apply

No SEC HTTP calls — pure DB-side re-derivation (~50-200 ms/instrument;
full dev universe ≈ 4,700 instruments).
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

import psycopg

from app.config import settings
from app.services.fundamentals import normalize_financial_periods
from scripts._dev_guard import assert_dev_environment

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_ORPHAN_SNAPSHOT_SQL = """
    SELECT COUNT(DISTINCT fs.instrument_id)
    FROM fundamentals_snapshot fs
    WHERE NOT EXISTS (
        SELECT 1 FROM financial_facts_raw f
        WHERE f.instrument_id = fs.instrument_id
    )
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="write; default is dry-run counts")
    args = parser.parse_args()

    assert_dev_environment()

    with psycopg.connect(settings.database_url) as conn:
        orphans = conn.execute(_ORPHAN_SNAPSHOT_SQL).fetchone()
        orphan_count = int(orphans[0]) if orphans else 0
        cohort = conn.execute("SELECT COUNT(DISTINCT instrument_id) FROM financial_facts_raw").fetchone()
        cohort_count = int(cohort[0]) if cohort else 0
        logger.info(
            "cohort: %d instruments with raw facts; %d orphan-snapshot instruments to purge",
            cohort_count,
            orphan_count,
        )
        if not args.apply:
            logger.info("dry-run — pass --apply to execute")
            return 0

        cur = conn.execute(
            """
            DELETE FROM fundamentals_snapshot fs
            WHERE NOT EXISTS (
                SELECT 1 FROM financial_facts_raw f
                WHERE f.instrument_id = fs.instrument_id
            )
            """
        )
        logger.info("purged %d orphan snapshot rows", cur.rowcount)
        conn.commit()

        started = time.monotonic()
        summary = normalize_financial_periods(conn)
        conn.commit()
        logger.info(
            "normalized %d instruments in %.0fs: %d raw periods, %d canonical",
            summary.instruments_processed,
            time.monotonic() - started,
            summary.periods_raw_upserted,
            summary.periods_canonical_upserted,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
