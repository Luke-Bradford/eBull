"""Backfill historical per-class shares-outstanding from SEC DERA FSDS (#1745).

`instrument_class_shares_outstanding` (sql/200) is keyed `(instrument_id,
period_end)` and already holds history — but only `last_n_quarters(4)` are
ever downloaded by the bootstrap, so dev has just one quarter per issuer.
The per-period dual-class FCF-yield trend (#1745) needs per-class shares at
each historical period_end. This script downloads the last N FSDS quarters
and runs the existing, settled parser
(`fsds_class_shares.ingest_fsds_class_shares_archive`) over each — no new
parser, no schema change. The no-demotion upsert handles quarter overlap and
restatement.

Run from the repo root::

    uv run python scripts/backfill_fsds_class_shares_history.py --quarters 20 --apply

Dry-run by default (lists the quarters + whether each ZIP is already cached).
`--apply` downloads (each ~530 MB) + ingests. `--keep` retains the ZIPs
(default: delete each after a successful ingest to bound disk). Resumable —
a cached ZIP is re-used, and ingest is idempotent.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

import psycopg

from app.config import settings
from app.security.master_key import resolve_data_dir
from app.services.fsds_class_shares import ingest_fsds_class_shares_archive
from app.services.sec_bulk_download import (
    SEC_BASE_URL,
    BulkArchive,
    download_bulk_archives,
    last_n_quarters,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _bulk_dir() -> Path:
    # Mirrors app.services.sec_bulk_orchestrator_jobs._bulk_dir without importing
    # that module (it pulls in the insider-ingest import cycle).
    return resolve_data_dir() / "sec" / "bulk"


def _archive_path(name: str) -> Path:
    return _bulk_dir() / name


def _fsds_archives(quarters: list[str]) -> list[BulkArchive]:
    return [
        BulkArchive(
            name=f"fsds_{q}.zip",
            url=f"{SEC_BASE_URL}/files/dera/data/financial-statement-data-sets/{q}.zip",
            # The newest quarter is published weeks after it closes — optional so
            # an expected-404 right after a boundary doesn't fatal the batch.
            optional=(idx == 0),
        )
        for idx, q in enumerate(quarters)
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quarters", type=int, default=20, help="How many recent FSDS quarters (default 20 ≈ 5y).")
    parser.add_argument("--apply", action="store_true", help="Download + ingest (default: dry-run plan).")
    parser.add_argument("--keep", action="store_true", help="Keep ZIPs after ingest (default: delete each).")
    args = parser.parse_args(argv)

    quarters = last_n_quarters(int(args.quarters))
    logger.info("FSDS history backfill: %d quarters %s..%s", len(quarters), quarters[-1], quarters[0])

    if not args.apply:
        for q in quarters:
            cached = _archive_path(f"fsds_{q}.zip").exists()
            logger.info("  %s — %s", q, "cached" if cached else "would download")
        logger.info("DRY RUN — pass --apply to download + ingest.")
        return 0

    archives = _fsds_archives(quarters)
    # bandwidth_threshold_mbps=0 so a slow dev link still proceeds (no fallback
    # skip); the orchestrator's probe is for the live bootstrap, not a backfill.
    result = asyncio.run(
        download_bulk_archives(
            target_dir=_bulk_dir(),
            user_agent=settings.sec_user_agent,
            archives=archives,
            bandwidth_threshold_mbps=0,
        )
    )
    logger.info("download: mode=%s", result.mode)

    total_written = 0
    ingested = 0
    for q in quarters:
        path = _archive_path(f"fsds_{q}.zip")
        if not path.exists():
            logger.info("  %s — absent after download (expected for the newest optional quarter), skipping", q)
            continue
        with psycopg.connect(settings.database_url) as conn:
            try:
                res = ingest_fsds_class_shares_archive(conn=conn, archive_path=path, fsds_qtr=q)
                conn.commit()
            except Exception:
                conn.rollback()
                logger.exception("  %s — ingest failed", q)
                continue
        ingested += 1
        total_written += res.rows_written
        logger.info("  %s — written=%d no_row=%s", q, res.rows_written, res.curated_pairs_without_row)
        if not args.keep:
            path.unlink(missing_ok=True)

    logger.info("FSDS history backfill: complete. quarters_ingested=%d rows_written=%d", ingested, total_written)
    return 0


if __name__ == "__main__":
    sys.exit(main())
