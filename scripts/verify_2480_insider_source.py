"""Build #2480's source-only population without reading any outcome price."""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path

import psycopg

from app.config import settings
from app.services.insider_purchase_candidate import PRIMARY_START, build_source


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive-dir", type=Path, required=True)
    parser.add_argument("--last-quarter", required=True, help="pinned latest available archive, YYYYqN")
    args = parser.parse_args(argv)
    archives = sorted(args.archive_dir.glob("*_form345.zip"))
    if not archives:
        parser.error("archive directory contains no *_form345.zip files")

    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET TRANSACTION READ ONLY")
        build = build_source(conn, archives, expected_last_quarter=args.last_quarter)

    recent = [item for item in build.classified if item.observation.filed_date >= PRIMARY_START]
    classes = Counter(item.insider_class for item in recent)
    months = {item.signal_month for item in recent}
    print("#2480 source-only census — no outcome prices read")
    print(f"archives: {archives[0].name} .. {archives[-1].name} ({len(archives)})")
    print(f"archive manifest SHA-256: {build.archive_manifest_sha256}")
    print(f"eligible source purchase observations: {len(build.purchases):,}")
    print(f"classified observations before universe resolution: {len(build.source_classified):,}")
    print(f"classified observations with research series: {len(build.classified):,}")
    print(f"classified since 2022: {len(recent):,} across {len(months):,} filing months")
    print(f"recent routine={classes['routine']:,} opportunistic={classes['opportunistic']:,}")
    print("\nsource construction counters")
    for reason, count in build.refusals.items():
        print(f"  {reason:<48} {count:>12,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
