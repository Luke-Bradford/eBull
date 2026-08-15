"""Measure #2493 PEAD arrival/dependence without reading an outcome price."""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from datetime import date
from pathlib import Path

import psycopg

from app.config import settings
from app.security.master_key import resolve_data_dir
from app.services.pead_candidate import build_archive_source, expand_instrument_alternatives
from app.services.pead_feasibility import (
    eligible_events,
    load_pre_outcome_windows,
    market_session_dates,
    purged_date_count,
)
from app.services.strategy_result import CORPUS_FROZEN_LAST_BAR

PRIOR_TRIAL_ARCHIVE_SHA256 = "126056a91f8d0446bd0f9c04f7db84da7e405d171c541fe72c7aae70d5b6c02b"


def _two_years_before(value: date) -> date:
    try:
        return value.replace(year=value.year - 2)
    except ValueError:  # February 29
        return value.replace(year=value.year - 2, day=28)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--archive",
        type=Path,
        default=resolve_data_dir() / "sec" / "bulk" / "companyfacts.zip",
    )
    args = parser.parse_args(argv)

    with psycopg.connect(settings.database_url) as conn:
        build, loaded = build_archive_source(conn, args.archive)
        expanded = expand_instrument_alternatives(build.triggers, build.instrument_alternatives)
        windows = load_pre_outcome_windows(conn, expanded)
        events, refusals = eligible_events(windows)

    entry_dates = tuple(item.entry_date for item in events)
    distinct_dates = tuple(sorted(set(entry_dates)))
    if not distinct_dates:
        raise RuntimeError("the frozen PEAD rule produced no eligible long entries")
    session_dates = market_session_dates(distinct_dates[0], CORPUS_FROZEN_LAST_BAR)
    trailing_start = _two_years_before(CORPUS_FROZEN_LAST_BAR)
    recent_dates = tuple(item for item in entry_dates if trailing_start <= item <= CORPUS_FROZEN_LAST_BAR)
    yearly = Counter(item.year for item in entry_dates)

    print("#2493 PEAD feasibility census — entry and pre-entry data only")
    print(f"companyfacts archive SHA-256: {loaded.archive_sha256}")
    source_relation = (
        "exact #2476 archive"
        if loaded.archive_sha256 == PRIOR_TRIAL_ARCHIVE_SHA256
        else "refreshed capture; not an exact #2476 archive reproduction"
    )
    print(f"source relation:             {source_relation}")
    print(f"frozen price frontier:        {CORPUS_FROZEN_LAST_BAR}")
    print(f"eligible long issuer-events:  {len(events):,}")
    print(f"distinct entry dates:         {len(distinct_dates):,}")
    print(f"purged 62-session dates:      {purged_date_count(entry_dates, session_dates):,}")
    print(f"trailing-24-month start:      {trailing_start}")
    print(f"trailing-24-month events:     {len(recent_dates):,}")
    print(f"trailing-24-month dates:      {len(set(recent_dates)):,}")
    print(f"trailing-24-month purged:     {purged_date_count(recent_dates, session_dates):,}")
    print("yearly eligible events:")
    for year, count in sorted(yearly.items()):
        print(f"  {year}: {count:,}")
    print("entry-known refusal census:")
    for reason, count in refusals.items():
        print(f"  {reason:<42} {count:>10,}")
    print("power verdict: refused — minimum worthwhile net effect and compatible planning dispersion are not frozen")
    return 0


if __name__ == "__main__":
    sys.exit(main())
