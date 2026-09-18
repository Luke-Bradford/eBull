"""#3159 — measure the halt feed's execution-lane starvation, before and after.

Read-only.  Every figure quoted in ``_CORE_PREFLIGHT_FRESHNESS_PRODUCERS`` and in
the #3159 PR description is produced here rather than written by hand, so a
re-measurement cannot leave a stale number in a comment (repo rule: never hardcode
a derived statistic into prose).

Usage::

    PYTHONPATH=. uv run python -m scripts.measure_3159_halt_feed_lane
    PYTHONPATH=. uv run python -m scripts.measure_3159_halt_feed_lane --days 14

The acceptance figure is ``in-session minutes over bound``: the fraction of open-session
minutes in which ``strategy_halt_feed_state.fetched_at`` would have been older than
``CORE_MAX_HALT_FEED_AGE_SECONDS``, walked against the job's own successful runs.  It is
the number that must FALL after the lane move.

⚠ The walk uses successful ``job_runs`` rows as the proxy for ``fetched_at``.  That proxy
is validated in #3159 (the two agree to within one run's duration, ~0.45 s) and is what
makes a BEFORE measurement possible at all -- ``strategy_halt_feed_state`` keeps only the
current row, so history exists nowhere else.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta

import psycopg

from app.config import settings
from app.jobs.runtime import EXECUTION_LANE_GENERAL, execution_lane_for
from app.services.strategy_core_preflight import CORE_MAX_HALT_FEED_AGE_SECONDS
from app.workers.scheduler import JOB_STRATEGY_HALT_FEED_REFRESH, SCHEDULED_JOBS

#: The job's own collection window, from ``_strategy_halt_collection_due``: 09:00 ET
#: through the regular close plus 15 minutes.  Expressed in UTC on standard time; the
#: walk is a diagnostic, so a DST-shifted hour changes the denominator slightly and
#: not the comparison, which uses the same window on both sides.
_SESSION_START_UTC = "13:30"
_SESSION_END_UTC = "20:15"


def _general_lane_jobs() -> list[str]:
    return [j.name for j in SCHEDULED_JOBS if execution_lane_for(j.name) == EXECUTION_LANE_GENERAL]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=7, help="lookback window in days (default 7)")
    args = parser.parse_args()
    since = timedelta(days=args.days)

    general = _general_lane_jobs()
    print(f"measured_at            : {datetime.now(tz=UTC).isoformat()}")
    print(f"lookback               : {args.days} days")
    print(f"halt-feed bound        : CORE_MAX_HALT_FEED_AGE_SECONDS = {CORE_MAX_HALT_FEED_AGE_SECONDS}s")
    print(f"halt-feed lane NOW     : {execution_lane_for(JOB_STRATEGY_HALT_FEED_REFRESH)}")
    print(f"general-lane members   : {len(general)} jobs share 1 permit")

    with psycopg.connect(settings.database_url) as conn:
        skips = conn.execute(
            """
            SELECT count(*) FROM job_runs
             WHERE job_name = %(job)s AND status = 'skipped'
               AND error_msg = 'max_instances_active'
               AND started_at > now() - %(since)s
            """,
            {"job": JOB_STRATEGY_HALT_FEED_REFRESH, "since": since},
        ).fetchone()
        assert skips is not None
        print(f"\nmax_instances_active skips : {skips[0]}")

        spanned = conn.execute(
            """
            WITH skips AS (
              SELECT started_at AS t FROM job_runs
               WHERE job_name = %(job)s AND status = 'skipped'
                 AND error_msg = 'max_instances_active'
                 AND started_at > now() - %(since)s
            )
            SELECT count(*) FILTER (WHERE EXISTS (
                     SELECT 1 FROM job_runs r
                      WHERE r.job_name = ANY(%(gen)s) AND r.job_name <> %(job)s
                        AND r.started_at <= s.t
                        AND coalesce(r.finished_at, now()) >= s.t)),
                   count(*)
              FROM skips s
            """,
            {"job": JOB_STRATEGY_HALT_FEED_REFRESH, "since": since, "gen": general},
        ).fetchone()
        assert spanned is not None
        print(f"  ... spanned by a concurrent general-lane run : {spanned[0]} / {spanned[1]}")

        # ⚠ A NULL finished_at would make `coalesce(..., now())` span everything and
        # inflate the line above, so the check runs beside it rather than in a comment.
        orphans = conn.execute(
            """
            SELECT count(*) FROM job_runs
             WHERE job_name = ANY(%(gen)s) AND finished_at IS NULL
               AND started_at > now() - %(since)s
            """,
            {"gen": general, "since": since},
        ).fetchone()
        assert orphans is not None
        print(f"  ... unterminated general-lane runs (inflate the join if > 0) : {orphans[0]}")

        print("\ngeneral-lane occupants spanning a skip instant:")
        for name, count in conn.execute(
            """
            WITH skips AS (
              SELECT started_at AS t FROM job_runs
               WHERE job_name = %(job)s AND status = 'skipped'
                 AND error_msg = 'max_instances_active'
                 AND started_at > now() - %(since)s
            )
            SELECT r.job_name, count(DISTINCT s.t)
              FROM skips s
              JOIN job_runs r
                ON r.job_name = ANY(%(gen)s) AND r.job_name <> %(job)s
               AND r.started_at <= s.t AND coalesce(r.finished_at, now()) >= s.t
             GROUP BY 1 ORDER BY 2 DESC LIMIT 10
            """,
            {"job": JOB_STRATEGY_HALT_FEED_REFRESH, "since": since, "gen": general},
        ).fetchall():
            print(f"  {count:5d}  {name}")

        print("\nlane-member body durations (the wait a colliding fire can inherit):")
        for row in conn.execute(
            """
            SELECT job_name, count(*),
                   round(avg(extract(epoch FROM finished_at - started_at))::numeric, 2),
                   round(max(extract(epoch FROM finished_at - started_at))::numeric, 2)
              FROM job_runs
             WHERE job_name = ANY(%(names)s) AND status = 'success'
               AND started_at > now() - %(since)s AND finished_at IS NOT NULL
             GROUP BY 1 ORDER BY 4 DESC
            """,
            {"names": sorted(_lane_peers()), "since": since},
        ).fetchall():
            print(f"  {row[0]:32s} runs={row[1]:5d} mean={row[2]}s max={row[3]}s")

        # THE ACCEPTANCE FIGURE.  Walk every in-session minute and ask how old the feed
        # was, using the most recent successful run at or before that minute.
        over, total, worst = conn.execute(
            """
            WITH runs AS (
              SELECT started_at FROM job_runs
               WHERE job_name = %(job)s AND status = 'success'
                 AND started_at > now() - %(since)s
            ),
            minutes AS (
              SELECT generate_series(
                       date_trunc('minute', now() - %(since)s),
                       date_trunc('minute', now()),
                       interval '1 minute') AS m
            ),
            in_session AS (
              SELECT m FROM minutes
               WHERE extract(dow FROM m) BETWEEN 1 AND 5
                 AND m::time BETWEEN %(start)s::time AND %(end)s::time
            ),
            aged AS (
              SELECT i.m,
                     extract(epoch FROM i.m - (SELECT max(started_at) FROM runs WHERE started_at <= i.m)) AS age
                FROM in_session i
            )
            SELECT count(*) FILTER (WHERE age > %(bound)s), count(*) FILTER (WHERE age IS NOT NULL),
                   round(max(age)::numeric, 0)
              FROM aged
            """,
            {
                "job": JOB_STRATEGY_HALT_FEED_REFRESH,
                "since": since,
                "start": _SESSION_START_UTC,
                "end": _SESSION_END_UTC,
                "bound": CORE_MAX_HALT_FEED_AGE_SECONDS,
            },
        ).fetchone() or (0, 0, None)

        pct = (100.0 * over / total) if total else 0.0
        print("\n=== ACCEPTANCE FIGURE (must FALL after the lane move) ===")
        print(f"  in-session minutes examined : {total}")
        print(f"  minutes over the {CORE_MAX_HALT_FEED_AGE_SECONDS}s bound   : {over}  ({pct:.1f}%)")
        print(f"  worst observed age          : {worst}s")
    return 0


def _lane_peers() -> list[str]:
    lane = execution_lane_for(JOB_STRATEGY_HALT_FEED_REFRESH)
    return [j.name for j in SCHEDULED_JOBS if execution_lane_for(j.name) == lane]


if __name__ == "__main__":  # pragma: no cover - operator entry point
    raise SystemExit(main())
