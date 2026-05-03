"""Run the CIK discovery sweep against SEC's company_tickers.json.

Usage::

    uv run python scripts/discover_ciks.py

Walks every no-CIK instrument, looks up the ticker in SEC's curated
map, writes ``external_identifiers`` rows for matches. Idempotent —
re-running produces the same set of inserts (zero on the second
pass).

Operator audit 2026-05-03 found 7,281 of 12,379 instruments had no
SEC CIK row. This script is the cleanup. After it lands, the SEC
ingesters (13F, Form 4, fundamentals etc.) reach a wider universe
on every subsequent run.
"""

from __future__ import annotations

import logging
import sys

import psycopg

from app.config import settings
from app.services.cik_discovery import discover_ciks


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    with psycopg.connect(settings.database_url) as conn:
        result = discover_ciks(conn)
    print(
        f"CIK discovery complete: scanned={result.instruments_scanned} "
        f"matches={result.matches_found} inserted={result.rows_inserted} "
        f"misses={result.misses}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
