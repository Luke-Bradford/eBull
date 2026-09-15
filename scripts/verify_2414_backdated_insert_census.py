"""#2414 item 2 — the backdated-insert record, measured rather than asserted.

Two modes, and they answer different questions:

``--gap-census``
    How many session-dates are MISSING from the interior of a US-equity
    instrument's stored span? That is the population a gap closure could turn
    into backdated inserts, so it is the table's sizing input.

``--census``
    What does ``price_daily_backdated_insert`` actually hold, and is every
    stored row self-consistent with the frontier it recorded?

⚠⚠ WHY THIS SCRIPT EXISTS RATHER THAN A FIGURE IN A DOCUMENT. The first version
of the gap census was written inline as ``(count of calendar dates in span) -
(count of bars)`` against a ">= 500 instruments have a bar" derived calendar. It
reported 449,533 and was quoted as an UPPER BOUND. Both halves were wrong, and
Codex checkpoint 1 caught them:

  * **The subtraction is not a gap count.** ``price_daily`` holds bars on dates
    that are not sessions at all — 3,669 weekend bars across 389 instruments are
    already in the prevention log — and every such bar CANCELS a genuinely
    missing session date. The result is therefore neither an upper nor a lower
    bound. A gap count needs an ANTI-JOIN, which is what this script does.
  * **">= 500 instruments" is an invented calendar.** The repo already owns the
    documented rule (``app/services/market_calendar.py``, NYSE holiday calendar
    + observed extraordinary closures, sourced to nyse.com), and the instrument
    side is resolvable through ``exchanges.asset_class`` — the join
    ``exchanges.exchange_id = instruments.exchange`` is used by
    ``scripts/verify_3046_weekend_bar_census.py`` already.

So the numbers are computed here, at run time, and no caller should ever copy one
into prose. (Repo rule: "never hardcode a derived statistic into prose".)

⚠ NAMED EXCLUSIONS, because a bound that hides them is not a bound:

  * **Below-minimum deepening is NOT counted.** ``force_backfill`` takes an
    instrument from 400 stored bars to 1000 — every one of those 600 lands below
    the stored minimum and is backdated. How much history the provider holds
    beyond our oldest bar is not knowable from stored state, so this census
    measures the INTERIOR only and the real population is strictly larger.
  * **Non-US-equity instruments are out of scope**, because we hold no
    authoritative session calendar for LSE / Euronext / Tokyo / crypto. Their
    interior gaps are real; this script declines to guess at them rather than
    inflating the figure with foreign holidays, which is exactly what the
    superseded version did.
  * A missing session date is capacity, **not** a pending write. The provider may
    genuinely have no bar (halt, IPO mid-span, delisting-and-relisting), in which
    case that date never becomes a row.

Usage::

    PYTHONPATH=. uv run python scripts/verify_2414_backdated_insert_census.py --gap-census
    PYTHONPATH=. uv run python scripts/verify_2414_backdated_insert_census.py --census
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta

import psycopg

from app.config import settings
from app.services.market_calendar import us_market_status

#: ``exchanges.asset_class`` value whose sessions the NYSE calendar governs.
#: Includes OTC Markets, which follows the same session dates.
_US_EQUITY = "us_equity"

#: The gap census joins every in-scope instrument against every session in the
#: corpus range, so it is a large but indexed anti-join. Generous, and explicit
#: rather than left to the server default (the repo's default is no timeout, and
#: an unbounded census on a box running a 6,000-second sweep is a hang).
_STATEMENT_TIMEOUT = "2700s"


def _sessions(lo: date, hi: date) -> list[date]:
    """Every NYSE regular session in ``[lo, hi]``, per ``market_calendar``.

    ``half_day`` counts — a 13:00 ET early close is a session that trades and
    therefore produces a bar. Only ``closed`` is excluded.
    """
    out: list[date] = []
    d = lo
    while d <= hi:
        if us_market_status(d) != "closed":
            out.append(d)
        d += timedelta(days=1)
    return out


def _gap_census(conn: psycopg.Connection) -> None:  # type: ignore[type-arg]
    row = conn.execute(
        """
        SELECT min(p.price_date), max(p.price_date), count(*), count(DISTINCT p.instrument_id)
          FROM price_daily p
          JOIN instruments i ON i.instrument_id = p.instrument_id
          JOIN exchanges  e ON e.exchange_id   = i.exchange
         WHERE e.asset_class = %(cls)s
        """,
        {"cls": _US_EQUITY},
    ).fetchone()
    assert row is not None
    lo, hi, bars, instruments = row
    if lo is None:
        print("scope: 0 us_equity bars — nothing to measure")
        return
    print(f"scope           : asset_class={_US_EQUITY}")
    print(f"corpus range    : {lo} .. {hi}")
    print(f"bars in scope   : {bars:,} across {instruments:,} instruments")

    sessions = _sessions(lo, hi)
    print(f"NYSE sessions   : {len(sessions):,} (market_calendar.us_market_status != 'closed')")

    conn.execute("CREATE TEMP TABLE _sessions (d DATE PRIMARY KEY)")
    with conn.cursor() as cur:
        with cur.copy("COPY _sessions (d) FROM STDIN") as copy:
            for d in sessions:
                copy.write_row((d,))
    conn.execute("ANALYZE _sessions")

    # ANTI-JOIN, not a subtraction: a bar on a non-session date (weekend rows
    # exist) must not cancel a missing session date.
    agg = conn.execute(
        """
        WITH scope AS (
            SELECT p.instrument_id, min(p.price_date) AS lo, max(p.price_date) AS hi
              FROM price_daily p
              JOIN instruments i ON i.instrument_id = p.instrument_id
              JOIN exchanges  e ON e.exchange_id   = i.exchange
             WHERE e.asset_class = %(cls)s
             GROUP BY p.instrument_id
        ), missing AS (
            SELECT s.instrument_id, count(*) AS gaps
              FROM scope s
              JOIN _sessions t ON t.d > s.lo AND t.d < s.hi
             WHERE NOT EXISTS (
                     SELECT 1 FROM price_daily p
                      WHERE p.instrument_id = s.instrument_id
                        AND p.price_date    = t.d
                   )
             GROUP BY s.instrument_id
        )
        SELECT (SELECT count(*) FROM scope)                                AS instruments,
               (SELECT count(*) FROM missing)                              AS with_gaps,
               (SELECT coalesce(sum(gaps), 0) FROM missing)                AS total_gaps,
               (SELECT coalesce(max(gaps), 0) FROM missing)                AS worst,
               (SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY gaps)
                  FROM missing)                                           AS median_of_affected
        """,
        {"cls": _US_EQUITY},
    ).fetchone()
    assert agg is not None
    n, with_gaps, total, worst, median = agg
    pct = (with_gaps / n * 100) if n else 0.0
    print(f"instruments     : {n:,}")
    print(f"with >=1 gap    : {with_gaps:,} ({pct:.1f}%)")
    print(f"interior gaps   : {total:,}   worst instrument {worst:,}   median of affected {median}")
    print()
    print("⚠ INTERIOR ONLY. Below-minimum deepening (force_backfill 400 -> 1000 bars) is")
    print("  backdated too and is NOT counted here — provider reach beyond our oldest bar")
    print("  is not knowable from stored state, so the real population is strictly larger.")
    print("⚠ Non-US-equity instruments are excluded: no authoritative session calendar.")
    print("⚠ A missing session date is CAPACITY, not a pending write.")


def _census(conn: psycopg.Connection) -> None:  # type: ignore[type-arg]
    row = conn.execute(
        """
        SELECT count(*),
               count(DISTINCT instrument_id),
               min(inserted_at),
               max(inserted_at),
               count(*) FILTER (WHERE price_date >= frontier_before)
          FROM price_daily_backdated_insert
        """
    ).fetchone()
    assert row is not None
    rows, instruments, first, last, inconsistent = row
    print(f"rows            : {rows:,} across {instruments:,} instruments")
    if rows == 0:
        print()
        print("⚠⚠ UNINTERPRETABLE. An empty table cannot distinguish 'no bar appeared behind a")
        print("   frontier' from 'the writer has not run'. Backdated inserts come from gap")
        print("   closure and from force_backfill deepening, neither of which an hourly")
        print("   ~9-second daily_candle_refresh pass with items_done=0 performs. The")
        print("   acceptance is A BIG SWEEP COMPLETED ON CODE CARRYING THE WRITER, not 'a run")
        print("   completed'. Check:")
        print("     SELECT max(started_at) FROM job_runs")
        print("      WHERE job_name='daily_candle_refresh' AND status='success';")
        return
    print(f"observed window : {first} .. {last}")
    # Self-consistency: the writer classified against `frontier_before`, which it
    # STORES, so every row must satisfy the predicate that put it here. This is
    # the one check that does not depend on knowing what the provider sent.
    print(f"rows violating price_date < frontier_before : {inconsistent}  (expect 0)")
    for kind_row in conn.execute(
        """
        SELECT cause, count(*) FROM price_daily_backdated_insert GROUP BY cause ORDER BY 2 DESC
        """
    ).fetchall():
        print(f"  cause {kind_row[0]:<22} {kind_row[1]:,}")
    print()
    print("⚠ `cause` is the WRITE BRANCH, not an economic cause — one fetch can insert")
    print("  bars for more than one underlying reason. It is an upper bound on attribution.")
    print("⚠ `initial_backfill` here is NOT impossible, it is a RACE: a concurrent writer")
    print("  can commit history between this run's fetch-size decision and its frontier")
    print("  read. A nonzero count is a concurrency observation, not a classifier bug.")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--gap-census", action="store_true", help="interior missing-session population")
    ap.add_argument("--census", action="store_true", help="stored backdated-insert rows")
    args = ap.parse_args()
    if not (args.gap_census or args.census):
        ap.error("pick --gap-census or --census")
    with psycopg.connect(settings.database_url) as conn:
        conn.execute(f"SET statement_timeout = '{_STATEMENT_TIMEOUT}'")
        if args.gap_census:
            _gap_census(conn)
        if args.census:
            if args.gap_census:
                print()
            _census(conn)


if __name__ == "__main__":
    main()
