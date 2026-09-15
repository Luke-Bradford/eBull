"""#2414 — census of `price_daily_revision`, the per-bar overwrite log.

Read-only, one snapshot. Prints the table's own state and NOTHING derived from the
strategy ledger.

⚠⚠ THERE IS DELIBERATELY NO LEDGER-EXPOSURE ARM. The obvious join —
``strategy_signals s JOIN price_daily_revision r ON r.instrument_id = s.instrument_id
AND s.signal_bar_date >= r.price_date AND s.created_at < r.revised_at`` — was specced,
taken to Codex checkpoint 1, and killed: it OVERCOUNTS (segmented evaluation restarts
indicator state; masked fields change no consumed input; a volume-only revision is
indistinguishable here; the bar need not have existed when the signal ran; not every
stored strategy id has unbounded memory) and UNDERCOUNTS (the regime is computed on the
BENCHMARK, so a benchmark revision reaches every regime-gated instrument;
cross-sectional ranking couples instruments; ``resolve_fills`` reads the NEXT bar's
open). And ``created_at`` is the ledger WRITE time, not the bar READ time, so the
predicate is unsound in both orderings regardless. A number that is neither an upper nor
a lower bound is worse than no number. Full enumeration in
``sql/387_price_daily_revision.sql``'s header.

⚠ AN EMPTY TABLE PROVES NOTHING. Before the first post-deploy sweep the window is
undefined, and an empty table cannot distinguish "no bar was revised" from "the writer
is not running" — the recorded #2274 lesson, applied to the surface it came from. The
window is therefore always printed, and a zero is reported as uninterpretable rather
than as evidence of non-occurrence.
"""

from __future__ import annotations

import sys
from typing import Any

import psycopg

from app.config import settings

_TOTALS = """
SELECT count(*)                                        AS rows,
       count(DISTINCT instrument_id)                   AS instruments,
       count(DISTINCT (instrument_id, price_date))     AS bars,
       min(revised_at)                                 AS first_at,
       max(revised_at)                                 AS last_at,
       min(price_date)                                 AS first_bar,
       max(price_date)                                 AS last_bar
  FROM price_daily_revision
"""

_BY_CAUSE = """
SELECT cause, count(*) AS rows, count(DISTINCT instrument_id) AS instruments
  FROM price_daily_revision
 GROUP BY cause
 ORDER BY count(*) DESC, cause
"""

#: Bars overwritten more than once. Reported separately because repeats inflate the
#: row count without widening the population, and because the same date CAN legitimately
#: appear twice from ONE call (`_normalise_candles` does not dedup dates).
_REPEATS = """
SELECT count(*) AS bars, coalesce(max(n), 0) AS worst, coalesce(sum(n), 0) AS rows
  FROM (SELECT count(*) AS n FROM price_daily_revision
         GROUP BY instrument_id, price_date HAVING count(*) > 1) t
"""


def census(conn: psycopg.Connection[Any]) -> int:
    row = conn.execute(_TOTALS).fetchone()
    assert row is not None
    rows, instruments, bars, first_at, last_at, first_bar, last_bar = row

    print("price_daily_revision — census (#2414)")
    if not rows:
        print("  rows: 0")
        print("  ⚠ UNINTERPRETABLE. An empty table does not distinguish 'no bar has been")
        print("    revised' from 'the writer has not run'. Confirm a `daily_candle_refresh`")
        print("    run completed on code carrying `_record_bar_revisions` before reading")
        print("    anything into this zero.")
        return 0

    span_days = (last_at - first_at).total_seconds() / 86400.0
    print(f"  rows (overwrite events)      : {rows}")
    print(f"  distinct instruments         : {instruments}")
    print(f"  distinct (instrument, bar)   : {bars}")
    print(f"  observed window (revised_at) : {first_at:%Y-%m-%d %H:%M:%SZ} -> {last_at:%Y-%m-%d %H:%M:%SZ}")
    print(f"  window span (days)           : {span_days:.3f}")
    # Computed from the two endpoints above, never quoted: a rate written by hand goes
    # stale silently the moment the corpus moves.
    if span_days > 0:
        print(f"  rows per day (over window)   : {rows / span_days:.1f}")
    else:
        print("  rows per day (over window)   : n/a — window shorter than one sample")
    print(f"  revised bar dates span       : {first_bar} -> {last_bar}")

    repeats = conn.execute(_REPEATS).fetchone()
    assert repeats is not None
    repeat_bars, worst, repeat_rows = repeats
    print(f"  bars revised more than once  : {repeat_bars} ({repeat_rows} rows, worst {worst}x)")

    print("\n  by cause — ⚠ a write BRANCH, an UPPER BOUND on attribution, never")
    print("  an economic cause. `adjustment_heal` does not mean 'split'.")
    print(f"  {'cause':<22} {'rows':>8} {'instruments':>12}")
    for cause, cause_rows, cause_instruments in conn.execute(_BY_CAUSE).fetchall():
        print(f"  {cause:<22} {cause_rows:>8} {cause_instruments:>12}")

    print("\n⚠ This is ONE mutation class — an OHLCV overwrite. Historical INSERTs,")
    print("  quarantine verdict changes and segmentation changes all move a decision's")
    print("  consumed inputs and produce NO row here.")
    print("⚠ It does NOT locate affected strategy_signals rows; see this script's")
    print("  docstring and sql/387's header for why that join is neither bound.")
    return 0


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] not in {"--census"}:
        print(f"usage: {sys.argv[0]} [--census]", file=sys.stderr)
        return 2
    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        # One snapshot, so the totals, the repeat count and the by-cause split cannot
        # describe three different states of a table a live sweep is writing to.
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        with conn.transaction():
            return census(conn)


if __name__ == "__main__":
    raise SystemExit(main())
