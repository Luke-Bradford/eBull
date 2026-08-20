"""Build the preregistered #2476 point-in-time source and print its census.

This command does not load any outcome price and cannot inspect the sealed
recent return interval.  It is safe to run while the source construction is
being reviewed.
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path

import psycopg

from app.config import settings
from app.security.master_key import resolve_data_dir
from app.services.pead_candidate import build_archive_source, build_source
from app.services.strategy_result import CORPUS_VENDORS


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--operational-retained-only",
        action="store_true",
        help="diagnose the hot-table retention horizon instead of the deep bulk archive",
    )
    parser.add_argument(
        "--archive",
        type=Path,
        default=resolve_data_dir() / "sec" / "bulk" / "companyfacts.zip",
    )
    args = parser.parse_args(argv)
    with psycopg.connect(settings.database_url) as conn:
        population = conn.execute(
            """
            SELECT count(*), count(DISTINCT m.instrument_id),
                   count(*) FILTER (WHERE m.accepted_at IS NOT NULL)
            FROM sec_filing_manifest m
            JOIN research_price_series s
              ON s.instrument_id = m.instrument_id
             AND s.vendor = %s
            WHERE m.form IN ('10-Q', '10-K')
              AND m.filed_at >= TIMESTAMPTZ '2022-01-01 00:00:00+00'
            """,
            (CORPUS_VENDORS[0],),
        ).fetchone()
        if population is None:
            raise RuntimeError("SEC filing population census returned no row")
        if args.operational_retained_only:
            build = build_source(conn)
            archive_sha256 = None
        else:
            build, loaded = build_archive_source(conn, args.archive)
            archive_sha256 = loaded.archive_sha256

    recent_observations = [item for item in build.observations if item.filed_date.year >= 2022]
    recent_events = [item for item in build.sue_events if item.observation.filed_date.year >= 2022]
    recent_classified = [item for item in build.triggers if item.event.observation.filed_date.year >= 2022]
    recent_sides = Counter(item.side or "control" for item in recent_classified)

    print("#2476 source-only census — no outcomes read")
    print(f"source: {'operational retained tables' if archive_sha256 is None else args.archive}")
    if archive_sha256 is not None:
        print(f"companyfacts archive SHA-256:             {archive_sha256}")
    print(f"manifest original 10-Q/10-K since 2022: {int(population[0]):,}")
    print(f"manifest instruments since 2022:         {int(population[1]):,}")
    print(f"exact accepted_at present:               {int(population[2]):,}")
    print(f"as-filed quarter observations since 2022:{len(recent_observations):>10,}")
    print(f"21-difference SUE events since 2022:     {len(recent_events):>10,}")
    print(f"causally classified events since 2022:   {len(recent_classified):>10,}")
    print(
        "classified sides since 2022:            "
        f"long={recent_sides['long']:,} short={recent_sides['short']:,} control={recent_sides['control']:,}"
    )
    print("\nall source construction counters")
    for reason, count in build.refusals.items():
        print(f"  {reason:<42} {count:>10,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
