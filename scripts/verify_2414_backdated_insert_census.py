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


#: The migration that created ``price_daily_backdated_insert``. Its ledger
#: ``applied_at`` is the earliest instant at which the insert writer could have
#: been present, and it is read from the DB rather than written down here —
#: repo rule, "never hardcode a derived statistic".
_INSERT_TABLE_MIGRATION = "388_price_daily_backdated_insert.sql"

#: ONE statement, deliberately. Under READ COMMITTED each statement takes its own
#: snapshot, so asking for the census and the witness separately lets a deepening
#: commit land between them: query 1 sees zero inserts, query 2 sees the
#: revisions that same transaction wrote, and the script certifies "zero" over a
#: state that never existed. Codex checkpoint 2 (#2414). Both aggregates here are
#: scalar subqueries of a single SELECT, so they describe one snapshot.
_CENSUS_SQL = """
WITH writer_since AS (
    SELECT applied_at FROM schema_migrations WHERE filename = %(migration)s
)
SELECT (SELECT count(*)                  FROM price_daily_backdated_insert),
       (SELECT count(DISTINCT instrument_id) FROM price_daily_backdated_insert),
       (SELECT min(inserted_at)          FROM price_daily_backdated_insert),
       (SELECT max(inserted_at)          FROM price_daily_backdated_insert),
       (SELECT count(*) FILTER (WHERE price_date >= frontier_before)
          FROM price_daily_backdated_insert),
       (SELECT count(*)            FROM price_daily_revision r, writer_since w
         WHERE r.cause = 'force_backfill' AND r.revised_at >= w.applied_at),
       (SELECT min(r.revised_at)::date FROM price_daily_revision r, writer_since w
         WHERE r.cause = 'force_backfill' AND r.revised_at >= w.applied_at),
       (SELECT max(r.revised_at)::date FROM price_daily_revision r, writer_since w
         WHERE r.cause = 'force_backfill' AND r.revised_at >= w.applied_at),
       (SELECT applied_at FROM writer_since)
"""


def _census(conn: psycopg.Connection) -> None:  # type: ignore[type-arg]
    """Report the stored backdated-insert rows, and whether a zero is READABLE.

    The witness for an empty table is a ``price_daily_revision`` row with
    ``cause='force_backfill'``. ``_record_bar_revisions`` and
    ``_record_backdated_inserts`` are invoked from ONE transaction block in
    ``refresh_market_data`` (``market_data.py:817`` and ``:851``) under one
    resolved ``write_branch``, so such a row is evidence that block executed on a
    run where ``force_backfill=True`` — the only caller that can push a bar below
    an instrument's stored minimum.

    ⚠⚠ LIVENESS WITNESS, NOT A CROSS-CHECK OF THE COUNT. The two tables share a
    writer, so neither can corroborate the other's CONTENT — the #3109 "two
    records written by one code path" tautology. The question asked here is
    strictly "did the code run", and a shared path is the right witness for that
    rather than a disqualifying one.

    ⚠ BOUNDED AT ``388``'s ``applied_at``. ``sql/387`` (revisions) was applied
    before ``sql/388`` (backdated inserts) — 100 minutes apart on the dev DB — so
    an unbounded witness would let a revision written in that window attest to a
    block that did not yet contain the insert writer. Migrations run in-process
    at lifespan startup, so the process that applied 388 already carried the
    writer. ⚠ Residual, stated rather than closed: a SEPARATE process still on
    pre-``a33176b0`` code could write a revision after 388 applied. It shrinks to
    nothing once any deploy has followed the migration.

    ⚠ Sufficient, not necessary: a deepening pass that revised no existing bar
    writes no revision row, so an absent witness means "cannot tell", never
    "never ran". ``incremental`` and ``adjustment_heal`` are excluded — the
    hourly job writes those and it cannot deepen.
    """
    row = conn.execute(_CENSUS_SQL, {"migration": _INSERT_TABLE_MIGRATION}).fetchone()
    assert row is not None
    rows, instruments, first, last, inconsistent = row[:5]
    n_witness, first_witness, last_witness, writer_since = row[5:]
    print(f"rows            : {rows:,} across {instruments:,} instruments")
    if rows == 0:
        print()
        if writer_since is None:
            print(f"⚠⚠ UNINTERPRETABLE. {_INSERT_TABLE_MIGRATION} is not in schema_migrations, so")
            print("   this database has no backdated-insert writer to have run.")
            return
        if not n_witness:
            print("⚠⚠ UNINTERPRETABLE. An empty table cannot distinguish 'no bar appeared behind a")
            print("   frontier' from 'the writer has not run'.")
            print()
            print("   ⚠ The trigger is NOT 'a big sweep'. An 11k-instrument daily_candle_refresh")
            print("     runs the INCREMENTAL branch; `force_backfill=True` is a separate path")
            print("     with a single caller. Run it, bounded, and re-run this census:")
            print("       PYTHONPATH=. uv run python scripts/rebackfill_candles_5y.py --apply --limit 12")
            print()
            print(f"   ⚠ Witness bounded at {writer_since} ({_INSERT_TABLE_MIGRATION} applied).")
            print("     force_backfill revisions OLDER than that attest to a block predating")
            print("     the insert writer, so they are deliberately not counted.")
            return
        print(f"deepening witness: {n_witness:,} price_daily_revision row(s) with cause='force_backfill'")
        print(f"                   {first_witness} .. {last_witness}  (>= {writer_since})")
        print()
        print("✅ INTERPRETABLE, and the reading is ZERO BACKDATED INSERTS — a measurement,")
        print("   not an absence of coverage. `_record_bar_revisions` and")
        print("   `_record_backdated_inserts` are called from the SAME transaction block in")
        print("   `refresh_market_data`, so a force_backfill revision row proves that block")
        print("   executed on a deepening run with `frontier_before` resolved.")
        print()
        print("   ⚠ THIS WITNESSES EXECUTION, NOT CONTENT. Using one of two writers to")
        print("     corroborate the other's NUMBER would be circular — they share a code")
        print("     path, so they cannot be independent observations of the same fact")
        print("     (the #3109 tautology). The claim made here is only 'the block ran'.")
        print("   ⚠ SUFFICIENT, NOT NECESSARY. A deepening run that revised no existing bar")
        print("     leaves no witness, so ABSENCE of one proves nothing either way.")
        print("   ⚠ Covers the DEEPENING class only. Gap closure on the incremental branch")
        print("     also produces backdated inserts and has no witness here.")
        print("   ⚠ A separate process still on pre-writer code could have written a witness")
        print(f"     after {writer_since}. Residual only until a deploy follows the migration.")
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
