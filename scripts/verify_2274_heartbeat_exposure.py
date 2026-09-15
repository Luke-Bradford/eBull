"""#2274 — what installing the ``job_runs`` heartbeat exposes, measured.

Read-only. One ``REPEATABLE READ READ ONLY`` transaction, so every figure
below describes the same snapshot.

    PYTHONPATH=. uv run python -m scripts.verify_2274_heartbeat_exposure

Answers two questions the spec must not assert:

1. **Do the covered jobs heartbeat today?** They have the tick sites; the
   listener was missing, so the expected answer is zero. Printed per job
   rather than claimed, and with the window named — a bounded query cannot
   establish "never".
2. **How much of the clock does a heartbeating job occupy?** This is what
   decides how often ``dev_reload`` will DEFER a deploy rather than
   preempt the job.

   ⚠ A true union (``range_agg``), not a sum of spans. Summing
   double-counts overlap, and these jobs overlap.

   ⚠⚠ Occupancy is NOT a deploy-deferral probability, and this script
   deliberately does not print one. Deploys are not uniformly distributed
   in time — the loop's own deploy step is scheduled outside the #2833
   evidence window on purpose — so converting occupancy into "one deploy
   in N" would assume exactly the thing that is false.

⚠⚠ THE TWO QUESTIONS WANT DIFFERENT WINDOWS, AND ONE QUERY FOR BOTH IS
WRONG. Question 1 is historical — "has this column EVER been written" —
and wants the long window. Question 2 is about what the change does NOW,
and a long window averages over regimes that have since ended.

Measured the hard way on 2026-09-15: the 90-day answer was **32.2%** and
the 7-day answer was **4.1%**, because `thesis_refresh` contributes 3.0M
of the 90-day seconds and has been **100% `skipped` in 0.0s for a week**
under #2855's `llm_model_writer='parked-2855'` park. A skipped run is
terminal on entry, so it never sits `running` and defers nothing. The
32.2% was reported as the operational consequence of a change shipping
that day; it described a regime that no longer existed.

So both windows print, and so does a per-status SKIP count — the column
that makes a parked job visible at a glance instead of an hour later.

Not measured here, because it cannot be: the inter-tick GAP distribution.
No run had ever emitted a second tick before the producer shipped, so the
gap has no observations yet. It is a dev-verify follow-up, not a
precondition.
"""

from __future__ import annotations

import psycopg

from app.config import settings

# The jobs whose bodies reach a ``report_progress`` tick site, resolved to
# the ``_tracked_job`` that owns the row they would write. Derived by
# inspection (spec §3), NOT by pattern-matching a name — two of the four
# arrive through a call chain rather than their own loop.
COVERED_JOBS = (
    "daily_candle_refresh",  # market_data.refresh_market_data
    "daily_financial_facts",  # fundamentals.execute_refresh
    "thesis_refresh",  # scheduler thesis batch loop
    "expected_filings_poller",  # -> run_force_refresh -> refresh_financial_facts
)

# The parent in the one confirmed nesting (fundamentals_sync ->
# daily_financial_facts). Included so its zero is visible: §4.4 gives a
# parent no heartbeat on purpose, and a reader should be able to see that
# rather than take it on the spec's word.
CONTEXT_JOBS = ("fundamentals_sync", "strategy_backtest_run")

# Long window answers "has this column ever been written"; short window
# answers "what will the change do now". See the module docstring — using
# one for both is the error this script now exists to prevent.
HISTORY_DAYS = 90
CURRENT_DAYS = 7

_PER_JOB = """
SELECT job_name,
       count(*)                                                        AS runs,
       count(*) FILTER (WHERE status = 'success')                      AS successes,
       count(*) FILTER (WHERE status = 'skipped')                      AS skipped,
       count(*) FILTER (WHERE last_progress_at IS NOT NULL)            AS with_heartbeat,
       round(percentile_cont(0.5) WITHIN GROUP (
           ORDER BY EXTRACT(EPOCH FROM (finished_at - started_at)))::numeric, 1) AS p50_s,
       round(percentile_cont(0.95) WITHIN GROUP (
           ORDER BY EXTRACT(EPOCH FROM (finished_at - started_at)))::numeric, 1) AS p95_s,
       round(max(EXTRACT(EPOCH FROM (finished_at - started_at)))::numeric, 1)    AS max_s
  FROM job_runs
 WHERE started_at > now() - make_interval(days => %(days)s)
   AND job_name = ANY(%(jobs)s)
 GROUP BY 1
 ORDER BY 1
"""

# ⚠ Durations are over runs of EVERY status, not successes only. A ceiling
# or a deferral acts on runs that never succeeded just as much as on ones
# that did, so filtering to successes would measure the wrong population
# (the error Codex caught on the rule-5 constant in 7f8d2b99). The
# per-status split is printed above so the mix is visible.

_OCCUPANCY = """
WITH spans AS (
    SELECT tstzrange(started_at, COALESCE(finished_at, now())) AS span
      FROM job_runs
     WHERE started_at > now() - make_interval(days => %(days)s)
       AND job_name = ANY(%(jobs)s)
       AND COALESCE(finished_at, now()) > started_at
), merged AS (
    SELECT unnest(range_agg(span)) AS span FROM spans
)
SELECT round((
           EXTRACT(EPOCH FROM sum(upper(span) - lower(span)))
           / (%(days)s * 86400.0) * 100
       )::numeric, 1) AS pct_of_wallclock
  FROM merged
"""


def main() -> None:
    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        with conn.transaction():
            now = conn.execute("SELECT now()").fetchone()
            assert now is not None
            print(f"snapshot: {now[0]:%Y-%m-%d %H:%M:%SZ}\n")

            jobs = list(COVERED_JOBS + CONTEXT_JOBS)
            for days, purpose in ((HISTORY_DAYS, "has it EVER been written"), (CURRENT_DAYS, "what happens NOW")):
                print(f"=== last {days} days — {purpose} ===")
                header = f"{'job':<26} {'runs':>6} {'ok':>6} {'skip':>6} {'with_hb':>8}"
                print(f"{header} {'p50_s':>10} {'p95_s':>10} {'max_s':>10}")
                for name, runs, ok, skip, hb, p50, p95, mx in conn.execute(
                    _PER_JOB, {"days": days, "jobs": jobs}
                ).fetchall():
                    tag = "" if name in COVERED_JOBS else "   (context)"
                    # ⚠ skip == runs means the job is PARKED and contributes
                    # nothing — a skipped run is terminal on entry, so it never
                    # sits ``running`` and defers nothing. That is what made the
                    # 90-day occupancy figure describe a dead regime (#2855).
                    flag = "  ⚠ PARKED (all skipped)" if runs and skip == runs else ""
                    print(
                        f"{name:<26} {runs:>6} {ok:>6} {skip:>6} {hb:>8} {p50!s:>10} {p95!s:>10} {mx!s:>10}{tag}{flag}"
                    )

                row = conn.execute(_OCCUPANCY, {"days": days, "jobs": list(COVERED_JOBS)}).fetchone()
                pct = None if row is None else row[0]
                print(f"covered-job occupancy (true union of in-flight spans): {pct}% of wall clock\n")

            print("⚠ occupancy, NOT a deploy-deferral probability — see this module's docstring.")
            print(f"⚠ quote the {CURRENT_DAYS}-day figure for operational consequence; the")
            print(f"  {HISTORY_DAYS}-day one answers a different question and averages over ended regimes.")


if __name__ == "__main__":
    main()
