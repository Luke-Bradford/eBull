"""Operator CLI — re-wash stored raw filing bodies under the current
parser version.

Usage::

    uv run python scripts/rewash.py --kind form4_xml
    uv run python scripts/rewash.py --kind form4_xml --since 2024-01-01
    uv run python scripts/rewash.py --kind form4_xml --dry-run

Walks every ``filing_raw_documents`` row of the given kind whose
``parser_version`` is below the current version and re-applies the
parser against the stored body. No SEC re-fetch.

Operator audit 2026-05-03 motivated this. The raw store (#808) +
per-ingester wiring (#810/#811) make local re-wash possible — this
CLI is the operator-visible surface.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

import psycopg

from app.config import settings
from app.services.raw_filings import DocumentKind
from app.services.rewash_filings import registered_specs, run_rewash


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Re-wash stored raw filings under the current parser.")
    p.add_argument(
        "--kind",
        required=True,
        choices=sorted(registered_specs().keys()),
        help="Document kind to re-wash (only registered kinds available).",
    )
    p.add_argument(
        "--since",
        type=date.fromisoformat,
        default=None,
        help="Only re-wash rows fetched on/after this date (YYYY-MM-DD).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Walk and count without writing anything.",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    kind: DocumentKind = args.kind  # argparse validated against registered keys

    with psycopg.connect(settings.database_url) as conn:
        result = run_rewash(
            conn,
            document_kind=kind,
            since=args.since,
            dry_run=args.dry_run,
        )

    print(
        f"Rewash {kind} (dry_run={args.dry_run}): "
        f"scanned={result.rows_scanned} "
        f"reparsed={result.rows_reparsed} "
        f"skipped={result.rows_skipped} "
        f"failed={result.rows_failed}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
