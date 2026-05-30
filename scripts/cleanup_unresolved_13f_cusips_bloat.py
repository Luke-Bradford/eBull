#!/usr/bin/env python
"""One-shot cleanup for the `unresolved_13f_cusips` bulk-partition bloat (#1349).

The bulk partition (`source IN ('bulk_13f_dataset','bulk_nport_dataset')`)
accumulated 1.3 GB / 6.7 M rows because nothing deleted resolved/aged rows
(both sweeps only UPDATE `resolution_status`; the only DELETE is legacy-
scoped). This script drains the **provably-dead** subset — bulk rows whose
`period_end` is outside the per-source ingest retention floor — then reclaims
the space.

Safety: a `period_end < cutoff` bulk row is a marker for a period no pipeline
will ever materialise (the bulk ingest rejects it at its retention gate), so
the observation is permanently unrecoverable and the marker is pure dead
weight. The period-based predicate is the only grain-safe cleanup — see
`docs/proposals/etl/1349-unresolved-13f-cusips-bloat.md` §2a/§3. In-retention
rows (the genuine pending work-queue) are KEPT.

Dry-run by default; pass ``--apply`` to delete + ``VACUUM (FULL, ANALYZE)``.
VACUUM FULL takes ACCESS EXCLUSIVE on the table (blocks ALL readers/writers)
— run in a maintenance window.

    uv run python -m scripts.cleanup_unresolved_13f_cusips_bloat            # report only
    uv run python -m scripts.cleanup_unresolved_13f_cusips_bloat --apply    # delete + vacuum
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable
from datetime import date

import psycopg

from app.config import settings
from app.services.cusip_resolver import BulkCusipSource, purge_unresolved_bulk_rows_outside_retention
from app.services.institutional_holdings import thirteen_f_retention_cutoff
from app.services.n_port_ingest import n_port_retention_cutoff

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("cleanup_unresolved_13f_cusips_bloat")

_BATCH = 100_000
_MAX_PASSES = 10_000  # backstop; the delete is monotonic so it always drains

# (source, cutoff-callable) — N-PORT and 13F have distinct retention floors.
_SOURCES: tuple[tuple[BulkCusipSource, Callable[[], date]], ...] = (
    ("bulk_13f_dataset", thirteen_f_retention_cutoff),
    ("bulk_nport_dataset", n_port_retention_cutoff),
)


def _report(conn: psycopg.Connection[tuple], *, label: str) -> None:
    with conn.cursor() as cur:
        total = cur.execute("SELECT count(*) FROM unresolved_13f_cusips").fetchone()
        distinct = cur.execute("SELECT count(DISTINCT cusip) FROM unresolved_13f_cusips").fetchone()
        size = cur.execute("SELECT pg_size_pretty(pg_total_relation_size('unresolved_13f_cusips'))").fetchone()
        logger.info(
            "[%s] total_rows=%s distinct_cusips=%s size=%s",
            label,
            total[0] if total else "?",
            distinct[0] if distinct else "?",
            size[0] if size else "?",
        )
        # Per-source reclaimable (< cutoff) vs kept (>= cutoff) breakdown.
        for source, cutoff_fn in _SOURCES:
            cutoff = cutoff_fn()
            row = cur.execute(
                """
                SELECT
                    count(*) FILTER (WHERE period_end < %(cutoff)s)  AS reclaimable,
                    count(*) FILTER (WHERE period_end >= %(cutoff)s) AS kept
                  FROM unresolved_13f_cusips
                 WHERE source = %(source)s
                """,
                {"source": source, "cutoff": cutoff},
            ).fetchone()
            logger.info(
                "[%s]   %s cutoff=%s reclaimable(<cutoff)=%s kept(>=cutoff)=%s",
                label,
                source,
                cutoff.isoformat(),
                row[0] if row else "?",
                row[1] if row else "?",
            )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Delete out-of-retention bulk rows + VACUUM FULL (default: report only).",
    )
    args = parser.parse_args()

    logger.info("cleanup_unresolved_13f_cusips_bloat: apply=%s db=%s", args.apply, settings.database_url[:40])

    with psycopg.connect(settings.database_url) as conn:
        _report(conn, label="before")

        if not args.apply:
            logger.info("dry-run: no rows deleted. Re-run with --apply to delete + VACUUM FULL.")
            return 0

        purged_total = 0
        for source, cutoff_fn in _SOURCES:
            cutoff = cutoff_fn()
            source_purged = 0
            for _ in range(_MAX_PASSES):
                deleted = purge_unresolved_bulk_rows_outside_retention(conn, source=source, cutoff=cutoff, limit=_BATCH)
                conn.commit()
                source_purged += deleted
                if deleted == 0:
                    break
            logger.info("purged %s: %d rows (cutoff=%s)", source, source_purged, cutoff.isoformat())
            purged_total += source_purged

        logger.info("purged %d total out-of-retention bulk rows", purged_total)

        # VACUUM FULL cannot run inside a transaction block. psycopg opens an
        # implicit txn per statement, so flip autocommit before issuing it —
        # but the autocommit setter raises if a txn is already in progress,
        # and the _report() SELECTs above just opened one. Commit to close it
        # first.
        _report(conn, label="after-delete")
        conn.commit()
        conn.autocommit = True
        logger.info("VACUUM (FULL, ANALYZE) unresolved_13f_cusips — ACCESS EXCLUSIVE, may take a while …")
        conn.execute("VACUUM (FULL, ANALYZE) unresolved_13f_cusips")
        _report(conn, label="after-vacuum")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
