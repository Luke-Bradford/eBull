"""Freeze and verify the bounded recent eToro comparator snapshot (#2482).

This reads existing structured eToro ``price_daily`` facts; it performs no API
requests and stores no raw payload or derived indicator series.

Run from the repository root::

    PYTHONPATH=. uv run python scripts/ingest_2482_etoro_comparators.py --probe
    PYTHONPATH=. uv run python scripts/ingest_2482_etoro_comparators.py --load --verify
"""

from __future__ import annotations

import argparse
import sys

import psycopg

from app.config import settings
from app.services.research_comparator_snapshot import (
    FROZEN_FRONTIER,
    SNAPSHOT_ID,
    ComparatorSnapshot,
    load_source_snapshot,
    store_snapshot,
    verify_stored_snapshot,
)


def _print_snapshot(label: str, snapshot: ComparatorSnapshot) -> None:
    members = snapshot.members
    row_count = snapshot.row_count
    sha256 = snapshot.sha256
    print(f"{label}: {SNAPSHOT_ID}")
    print(f"frontier: {FROZEN_FRONTIER}")
    print(f"members: {len(members)}  rows: {row_count:,}  sha256: {sha256}")
    for member in members:
        print(
            f"  {member.symbol:<5} source_id={member.instrument_id:<5} "
            f"bars={len(member.bars):>4} {member.first_bar}..{member.last_bar} "
            f"sha256={member.sha256}"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--probe", action="store_true", help="read and fingerprint price_daily without writing")
    parser.add_argument("--load", action="store_true", help="store the immutable snapshot in one transaction")
    parser.add_argument("--verify", action="store_true", help="re-hash the stored snapshot and its exclusion guards")
    args = parser.parse_args(argv)
    if not (args.probe or args.load or args.verify):
        parser.error("pass --probe, --load and/or --verify")

    with psycopg.connect(settings.database_url) as conn:
        source = None
        if args.probe or args.load:
            source = load_source_snapshot(conn)
            _print_snapshot("source", source)
        if args.load:
            assert source is not None
            written = store_snapshot(conn, source)
            conn.commit()
            print(f"stored/verified {written:,} bounded daily bars")
        if args.verify:
            stored = verify_stored_snapshot(conn)
            _print_snapshot("stored", stored)
    return 0


if __name__ == "__main__":
    sys.exit(main())
