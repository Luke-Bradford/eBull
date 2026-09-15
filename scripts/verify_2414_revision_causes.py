"""#2414 — what the stored `daily_candle_refresh` telemetry can and cannot say about
bar revisions.

Read-only. Two modes:

``--census``
    Aggregate every run's ``job_runs.progress_json -> 'context'`` and print the
    revision population WITH ITS OWN WINDOW, so no figure in the spec is hand-written.
    ⚠ The three counters shipped on different days, so a run carrying ``bars_revised``
    may carry ``bars_revised_age_days`` as ``None`` (not present) rather than ``{}``
    (present and empty). Those two are counted separately and never summed — reading
    them as one population reports a contradiction that does not exist.

``--causes``
    The same aggregation over the new ``bars_revised_by_cause`` /
    ``bars_revised_max_age_days_by_cause`` keys, plus the conservation check
    ``sum(by_cause) == bars_revised`` per run. Prints nothing but the header until the
    producer has run at least once after deploy.
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from typing import Any

import psycopg

from app.config import settings

_SQL = """
SELECT run_id, started_at, status, progress_json -> 'context' AS ctx
  FROM job_runs
 WHERE job_name = 'daily_candle_refresh'
   AND progress_json -> 'context' IS NOT NULL
 ORDER BY started_at
"""


def _rows(conn: psycopg.Connection[Any]) -> list[tuple[int, Any, str, dict[str, Any]]]:
    return [(r[0], r[1], r[2], r[3] or {}) for r in conn.execute(_SQL).fetchall()]


def census(conn: psycopg.Connection[Any]) -> int:
    rows = _rows(conn)
    with_counter = [r for r in rows if "bars_revised" in r[3]]
    # ⚠ PRESENT-AND-EMPTY is not ABSENT. `{}` means the run carried the histogram and
    # revised nothing; `None` means the run predates the histogram entirely.
    with_hist = [r for r in with_counter if r[3].get("bars_revised_age_days") is not None]

    total = sum(int(r[3].get("bars_revised") or 0) for r in with_counter)
    hist_total = sum(int(r[3].get("bars_revised") or 0) for r in with_hist)
    with_any = [r for r in with_counter if int(r[3].get("bars_revised") or 0) > 0]

    buckets: Counter[str] = Counter()
    deepest: int | None = None
    for _, _, _, ctx in with_hist:
        for bucket, count in (ctx.get("bars_revised_age_days") or {}).items():
            buckets[bucket] += int(count)
        age = ctx.get("bars_revised_max_age_days")
        if age is not None:
            deepest = int(age) if deepest is None else max(deepest, int(age))

    print("=== #2414 revision census (daily_candle_refresh) ===")
    print(f"runs with a 'context' payload            : {len(rows)}")
    print(f"runs carrying `bars_revised`             : {len(with_counter)}")
    if with_counter:
        print(f"window (first -> last such run)          : {with_counter[0][1]} -> {with_counter[-1][1]}")
    print(f"runs with >= 1 revision                  : {len(with_any)}")
    print(f"bars revised, total                      : {total}")
    print(f"runs ALSO carrying `bars_revised_age_days`: {len(with_hist)}")
    print(f"bars revised inside that window          : {hist_total}")
    print(f"bars revised BEFORE the histogram shipped : {total - hist_total}")
    print(f"deepest revision (calendar days)         : {deepest}")
    print(f"age buckets (histogram window only)      : {dict(sorted(buckets.items()))}")
    # The histogram's own conservation: every revision inside the instrumented window
    # must land in exactly one bucket. A mismatch means a bar was revised and not aged.
    print(f"histogram conservation (sum == revised)  : {sum(buckets.values())} == {hist_total}")
    return 0


def causes(conn: psycopg.Connection[Any]) -> int:
    rows = [r for r in _rows(conn) if "bars_revised_by_cause" in r[3]]
    print("=== #2414 revision causes ===")
    print(f"runs carrying `bars_revised_by_cause`    : {len(rows)}")
    if not rows:
        print("(the producer has not run since deploy — nothing to report)")
        return 0
    print(f"window (first -> last such run)          : {rows[0][1]} -> {rows[-1][1]}")

    by_cause: Counter[str] = Counter()
    max_by_cause: dict[str, int] = {}
    refetches = 0
    breaches = 0
    for run_id, _, _, ctx in rows:
        counts = {str(k): int(v) for k, v in (ctx.get("bars_revised_by_cause") or {}).items()}
        by_cause.update(counts)
        for cause, age in (ctx.get("bars_revised_max_age_days_by_cause") or {}).items():
            cause = str(cause)
            max_by_cause[cause] = max(int(age), max_by_cause.get(cause, int(age)))
        refetches += int(ctx.get("adjustment_refetches") or 0)
        revised = int(ctx.get("bars_revised") or 0)
        if sum(counts.values()) != revised:
            breaches += 1
            print(f"  ⚠ run {run_id}: sum(by_cause)={sum(counts.values())} != bars_revised={revised}")

    print(f"revisions by cause                       : {dict(sorted(by_cause.items()))}")
    print(f"deepest revision by cause (days)         : {dict(sorted(max_by_cause.items()))}")
    print(f"adjustment heal re-fetches (committed)   : {refetches}")
    print(f"conservation breaches                    : {breaches}")
    return 1 if breaches else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--census", action="store_true", help="the pre-cause revision population")
    parser.add_argument("--causes", action="store_true", help="the by-cause attribution")
    args = parser.parse_args()
    if not (args.census or args.causes):
        parser.error("choose --census and/or --causes")
    with psycopg.connect(settings.database_url) as conn:
        rc = 0
        if args.census:
            rc |= census(conn)
        if args.causes:
            rc |= causes(conn)
    return rc


if __name__ == "__main__":
    sys.exit(main())
