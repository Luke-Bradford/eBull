"""Full-population scan of FINRA's DESIGNATED bimonthly settlement calendar (#2234).

Rule 4560(a) requires short-interest reports "no later than the second
business day after the reporting settlement date designated by FINRA" —
the settlement calendar is designated and published by FINRA as per-year
tables, NOT derived from a formula. This script measures how far the
derived calendar in ``app/jobs/finra_short_interest_refresh`` diverges
from the designated one, over every anchor the CDN can be asked about.

For each half-month anchor it probes the CDN, walking back one calendar
day at a time up to the job's own bound, and reads the resolved file's
own ``settlementDate`` column back to confirm the file agrees with the
date it was fetched under.

Usage:

    PYTHONPATH=. uv run python scripts/audit_finra_settlement_calendar.py \\
        --start 2021-07 --end 2026-07 --out /tmp/finra_calendar.txt

Runs at the provider's 1 req/s floor. ~120 anchors ≈ 2-3 minutes.
"""

from __future__ import annotations

import argparse
import calendar
import csv
import io
import sys
from datetime import date, timedelta

from app.jobs.finra_short_interest_refresh import (
    _MAX_ANCHOR_WALKBACK_DAYS,
    _walk_back_to_weekday,
)
from app.providers.implementations.finra_short_interest import (
    FinraNotFound,
    FinraShortInterestProvider,
)
from app.services.finra_short_interest_ingest import parse_body_settlement_date


def _anchors(start: date, end: date) -> list[date]:
    """Weekend-adjusted (15th, last-calendar-day) anchors in ``[start, end]``."""
    out: list[date] = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        for day in (15, calendar.monthrange(y, m)[1]):
            anchor = _walk_back_to_weekday(date(y, m, day))
            if start <= anchor <= end:
                out.append(anchor)
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return sorted(set(out))


def _body_dates(payload: bytes) -> set[date | None]:
    reader = csv.DictReader(io.StringIO(payload.decode("utf-8")), delimiter="|")
    return {parse_body_settlement_date(row.get("settlementDate")) for row in reader}


def _month_arg(text: str) -> date:
    year, month = text.split("-")
    return date(int(year), int(month), 1)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", type=_month_arg, required=True, help="YYYY-MM inclusive")
    ap.add_argument("--end", type=_month_arg, required=True, help="YYYY-MM inclusive")
    ap.add_argument("--out", default="-", help="output file, or - for stdout")
    args = ap.parse_args()

    end_of_end_month = date(args.end.year, args.end.month, calendar.monthrange(args.end.year, args.end.month)[1])
    anchors = _anchors(args.start, end_of_end_month)
    provider = FinraShortInterestProvider()
    sink = sys.stdout if args.out == "-" else open(args.out, "w")  # noqa: SIM115

    exact = 0
    shifted: list[tuple[date, date]] = []
    unresolved: list[date] = []
    body_mismatch: list[tuple[date, object]] = []

    try:
        for anchor in anchors:
            probe = anchor
            payload: bytes | None = None
            for _ in range(_MAX_ANCHOR_WALKBACK_DAYS + 1):
                try:
                    payload = provider.fetch_settlement_file(probe)
                    break
                except FinraNotFound:
                    probe -= timedelta(days=1)
            if payload is None:
                unresolved.append(anchor)
                print(f"{anchor}  UNRESOLVED (no file within {_MAX_ANCHOR_WALKBACK_DAYS} days)", file=sink)
                sink.flush()
                continue

            seen = _body_dates(payload)
            if seen != {probe}:
                body_mismatch.append((probe, sorted(str(s) for s in seen)[:5]))
            if probe == anchor:
                exact += 1
            else:
                shifted.append((anchor, probe))
            print(
                f"{anchor}  designated={probe}  shift={(anchor - probe).days}d  body_dates_ok={seen == {probe}}",
                file=sink,
            )
            sink.flush()

        print("", file=sink)
        print(f"anchors probed:            {len(anchors)}", file=sink)
        print(f"derived == designated:     {exact}", file=sink)
        print(f"holiday-shifted:           {len(shifted)}  {[(str(a), str(d)) for a, d in shifted]}", file=sink)
        print(f"unresolved within bound:   {len(unresolved)}  {[str(a) for a in unresolved]}", file=sink)
        print(f"body settlementDate wrong: {len(body_mismatch)}  {body_mismatch}", file=sink)
        sink.flush()
    finally:
        if sink is not sys.stdout:
            sink.close()

    return 1 if body_mismatch else 0


if __name__ == "__main__":
    raise SystemExit(main())
