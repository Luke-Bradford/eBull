"""Seed the expected-filings watchlist (#1788 / #677 Part B) — CLI wrapper.

Derives the next expected periodic filing (10-Q / 10-K) per high-value
instrument (watchlist + open positions) from ``financial_periods`` and
upserts ``expected_filings``. The ``expected_filings_poller`` then
force-refreshes fundamentals the moment that filing appears.

Run from the repo root via the module form so ``app`` imports resolve:

    uv run python -m scripts.seed_expected_filings --dry-run
    uv run python -m scripts.seed_expected_filings --symbol AAPL

``--dry-run`` prints the derived rows without writing. ``--symbol SYM``
force-seeds one instrument regardless of watchlist/position membership
(operator ad-hoc declaration + dev verification). With neither flag, the
full high-value scope is upserted.

The derive -> upsert core lives in ``app.jobs.expected_filings_poller`` so
the CLI, the daily seed job, and the tests share one implementation.
"""

from __future__ import annotations

import argparse
import logging
import sys

import psycopg

from app.config import settings
from app.jobs.expected_filings_poller import derive_seed_rows, seed_expected_filings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--symbol",
        default=None,
        help="Force-seed a single symbol (e.g. AAPL), bypassing watchlist/position scope.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the derived (instrument, form, window, baseline) rows without writing.",
    )
    args = parser.parse_args(argv)

    with psycopg.connect(settings.database_url) as conn:
        if args.dry_run:
            rows = derive_seed_rows(conn, only_symbol=args.symbol)
            conn.commit()
            if not rows:
                logger.info("seed_expected_filings: scope is empty — nothing to seed")
                return 0
            for r in rows:
                logger.info(
                    "instrument_id=%d %s window=[%s..%s] anchor=%s baseline=%s",
                    r.instrument_id,
                    r.expected_filing_type,
                    r.expected_window_start,
                    r.expected_window_end,
                    r.anchor_period_end,
                    r.baseline_accession,
                )
            logger.info("seed_expected_filings: DRY-RUN — %d row(s); omit --dry-run to write", len(rows))
            return 0

        stats = seed_expected_filings(conn, only_symbol=args.symbol)

    logger.info(
        "seed_expected_filings: scoped=%d upserted=%d",
        stats.instruments_scoped,
        stats.rows_upserted,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
