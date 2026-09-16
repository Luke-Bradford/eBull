"""#3104 slice 9 — full-population boundary census for the session-gap statistic.

    PYTHONPATH=. uv run python -m scripts.census_3104_exit_gap

⚠⚠ IT CALLS ``boundary_gaps`` RATHER THAN RE-EXPRESSING ITS RULES IN SQL.  A
census that restates the exclusion vocabulary is a second copy of a closed
vocabulary, and the two drift; an earlier SQL-only version of this census
reported a "measurable" population that was not the module's, because it had no
hole rule, no break rule and no provisional cutoff.

⚠ WHAT IT BOUNDS AND WHAT IT DOES NOT.  This is PER BOUNDARY over the whole
archive.  The run's own population is narrower — realised legs only, one
quarantine arm, and a panel-axis mapping this census does not apply (every
series is censused on its own dense axis, which is the most favourable case for
``off_axis`` and is stated rather than hidden).  Legs are not uniformly
distributed over bars, so nothing here predicts the per-leg histogram.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

import psycopg

from app.config import settings
from app.services.price_quarantine import PROVISIONAL_WINDOW_DAYS
from app.services.research_corpus_ingest import RESEARCH_ARCHIVES
from app.services.strategy_exit_gap import EXIT_GAP_RULE_VERSION, boundary_gaps

_SERIES = """
SELECT s.series_id, s.instrument_id
FROM research_price_series s
WHERE s.vendor = %(vendor)s
ORDER BY s.series_id
"""

_BARS = """
SELECT bar_date, open, close, adj_close
FROM research_price_daily
WHERE series_id = %(series_id)s
ORDER BY bar_date
"""

_BREAKS = """
SELECT instrument_id, break_date
FROM price_series_break
WHERE resolved_by IS NULL
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vendor", default="icyDenev/Intrader")
    args = parser.parse_args()

    archive = next((item for item in RESEARCH_ARCHIVES if item.vendor == args.vendor), None)
    if archive is None:
        raise SystemExit(f"no archive declares vendor {args.vendor!r}")
    provisional_from = archive.quarantine_as_of - timedelta(days=PROVISIONAL_WINDOW_DAYS)

    verdicts: Counter[str] = Counter()
    measurable = 0
    adverse = 0
    lowest: tuple[float, int, date] | None = None
    highest: tuple[float, int, date] | None = None
    spans: Counter[str] = Counter()
    series_count = 0

    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET statement_timeout = '30min'")
        breaks: dict[int, list[date]] = {}
        with conn.cursor() as cur:
            cur.execute(_BREAKS)
            for instrument_id, break_date in cur.fetchall():
                breaks.setdefault(int(instrument_id), []).append(break_date)
        print(f"rule            {EXIT_GAP_RULE_VERSION}", flush=True)
        print(f"vendor          {args.vendor}", flush=True)
        print(f"basis           {archive.adjustment_basis}", flush=True)
        print(f"provisional_from {provisional_from}  (quarantine_as_of {archive.quarantine_as_of})", flush=True)
        print(f"unresolved breaks on {len(breaks)} linked instruments", flush=True)

        with conn.cursor() as series_cur:
            series_cur.execute(_SERIES, {"vendor": args.vendor})
            rows = series_cur.fetchall()
        for series_id, instrument_id in rows:
            series_count += 1
            with conn.cursor() as cur:
                cur.execute(_BARS, {"series_id": series_id})
                bars = cur.fetchall()
            if not bars:
                continue
            dates = [row[0] for row in bars]
            opens: list[Decimal | float | None] = [row[1] for row in bars]
            closes = [_float(row[2]) for row in bars]
            wealth = [_float(row[3]) for row in bars]
            values, reasons = boundary_gaps(
                dates=dates,
                opens=opens,
                offsets=list(range(len(dates))),
                raw_closes=closes,
                wealth_closes=wealth,
                provisional_from=provisional_from,
                unresolved_breaks=breaks.get(int(instrument_id), ()) if instrument_id is not None else (),
            )
            for index, reason in enumerate(reasons):
                if reason is not None:
                    verdicts[reason] += 1
                    continue
                verdicts["measurable"] += 1
                measurable += 1
                value = values[index]
                if value < 0.0:
                    adverse += 1
                if lowest is None or value < lowest[0]:
                    lowest = (value, int(series_id), dates[index])
                if highest is None or value > highest[0]:
                    highest = (value, int(series_id), dates[index])
                spans[_span_bucket((dates[index] - dates[index - 1]).days)] += 1
            if series_count % 2000 == 0:
                print(f"  {series_count}/{len(rows)} series", flush=True)

    print(f"\nseries          {series_count}", flush=True)
    total = sum(verdicts.values())
    print(f"boundaries      {total}", flush=True)
    for reason, count in verdicts.most_common():
        print(f"  {reason:<22}{count:>12,}  {100.0 * count / total:6.3f}%", flush=True)
    print(f"\nadverse (< 0)   {adverse:,}  {100.0 * adverse / measurable:6.3f}% of measurable", flush=True)
    # ⚠ PRINTED UNROUNDED. A rounded extremum cannot establish where the floor
    # is, and "exactly -100%" was claimed off a `round(...,2)` in an earlier
    # version of this census.
    print(f"minimum         {lowest!r}", flush=True)
    print(f"maximum         {highest!r}", flush=True)
    print("\nmeasured boundary calendar spans:", flush=True)
    for bucket, count in sorted(spans.items()):
        print(f"  {bucket:<22}{count:>12,}", flush=True)


def _span_bucket(days: int) -> str:
    if days == 1:
        return "1 day"
    if days <= 3:
        return "2-3 days"
    if days <= 5:
        return "4-5 days"
    return "6+ days"


def _float(value: Any) -> float:
    if value is None:
        return float("nan")
    return float(value)


if __name__ == "__main__":
    main()
