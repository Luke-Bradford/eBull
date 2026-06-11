"""One-shot standalone drain of pending sec_10k manifest rows (#554 backfill).

S7-precedent recipe: the manifest worker drains only 100 rows per
5-minute tick, so a 3,175-row rebuild drains standalone — iter_pending
chunks of 50 through ``_dispatch_rows``, commit per chunk. Run from
the feature branch checkout so the v2 parser (dimensional step) is the
one dispatching.

Usage: uv run python scripts/drain_554_sec10k.py
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime

import psycopg

from app.config import settings
from app.jobs.sec_manifest_worker import _dispatch_rows
from app.services.manifest_parsers import register_all_parsers
from app.services.sec_manifest import iter_pending

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("drain_554")


def main() -> None:
    register_all_parsers()
    totals = {"parsed": 0, "tombstoned": 0, "failed": 0, "skipped": 0}
    chunks = 0
    started = time.monotonic()
    with psycopg.connect(settings.database_url, application_name="drain-554-sec10k") as conn:
        while True:
            rows = list(iter_pending(conn, source="sec_10k", limit=50))
            if not rows:
                break
            stats = _dispatch_rows(conn, rows, now=datetime.now(tz=UTC))
            conn.commit()
            chunks += 1
            totals["parsed"] += stats.parsed
            totals["tombstoned"] += stats.tombstoned
            totals["failed"] += stats.failed
            totals["skipped"] += stats.skipped_no_parser
            logger.info(
                "chunk %d done: %s (elapsed %.0fs)",
                chunks,
                totals,
                time.monotonic() - started,
            )
    logger.info("DRAIN COMPLETE: %s in %.0fs", totals, time.monotonic() - started)


if __name__ == "__main__":
    main()
