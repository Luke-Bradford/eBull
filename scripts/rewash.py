"""Operator CLI — re-wash stored raw filing bodies under the current
parser version.

Usage::

    uv run python scripts/rewash.py --kind form4_xml
    uv run python scripts/rewash.py --kind form4_xml --since 2024-01-01
    uv run python scripts/rewash.py --kind form4_xml --dry-run

Walks every ``filing_raw_documents`` row of the given kind whose
``parser_version`` is below the current version and re-applies the
parser against the stored body. No SEC re-fetch — except for a
fetch-backed kind (#3899, e.g. ``def14a_body``), whose swept bodies are
re-fetched and hash-verified through the cross-process SEC rate gate::

    uv run python scripts/rewash.py --kind def14a_body --accession 0000320193-25-000008

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
from app.services.rewash_filings import RewashResult, fetch_backed_specs, registered_specs, run_rewash

# NOTE (#2110): this CLI previously needed an isort-pinned side-effect
# pre-import of ``app.services.manifest_parsers`` before rewash_filings —
# the insider modules' ``manifest_parsers._classify`` import fired the
# package init mid-initialisation and re-entered them. Dead since
# ``_classify`` moved to ``app.services.upsert_classify``; import order
# is now free. Guarded by tests/test_import_order_regression.py.


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Re-wash stored raw filings under the current parser.")
    p.add_argument(
        "--kind",
        required=True,
        choices=sorted({*registered_specs(), *fetch_backed_specs()}),
        help="Document kind to re-wash (only registered kinds available).",
    )
    p.add_argument(
        "--accession",
        action="append",
        default=None,
        help="Restrict the stale cohort to this accession (repeatable).",
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

    if kind in fetch_backed_specs() and not args.dry_run:
        result = _run_fetch_backed(kind, args)
    else:
        with psycopg.connect(settings.database_url) as conn:
            result = run_rewash(
                conn, document_kind=kind, since=args.since, dry_run=args.dry_run, accessions=args.accession
            )

    print(
        f"Rewash {kind} (dry_run={args.dry_run}): "
        f"scanned={result.rows_scanned} "
        f"reparsed={result.rows_reparsed} "
        f"skipped={result.rows_skipped} "
        f"failed={result.rows_failed} "
        f"raced={result.rows_raced}"
    )
    return 0


def _run_fetch_backed(kind: DocumentKind, args: argparse.Namespace) -> RewashResult:
    """A fetch-backed rewash fetches from SEC, so it first installs the
    cross-process rate gate the jobs process uses (``app/jobs/__main__.py``):
    a per-process floor here would add a second 10 req/s client beside the
    manifest worker."""
    from psycopg_pool import ConnectionPool

    from app.providers.implementations.sec_edgar import SecFilingsProvider
    from app.providers.postgres_rate_gate import PostgresFloorGate
    from app.providers.rate_gate import SEC_MIN_REQUEST_INTERVAL_S
    from app.providers.sec_rate_gate_holder import set_sec_rate_gate

    with ConnectionPool(
        settings.database_url, min_size=1, max_size=1, kwargs={"application_name": "rewash-gate"}
    ) as pool:
        set_sec_rate_gate(PostgresFloorGate(pool, budget="sec", floor_s=SEC_MIN_REQUEST_INTERVAL_S))
        with (
            SecFilingsProvider(user_agent=settings.sec_user_agent) as provider,
            psycopg.connect(settings.database_url) as conn,
        ):
            return run_rewash(
                conn,
                document_kind=kind,
                since=args.since,
                fetch_text=provider.fetch_document_text,
                accessions=args.accession,
            )


if __name__ == "__main__":
    sys.exit(main())
