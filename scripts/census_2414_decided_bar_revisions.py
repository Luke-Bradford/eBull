"""#2414 — how many stored verdicts had their own bars overwritten afterwards.

    PYTHONPATH=. uv run python -m scripts.census_2414_decided_bar_revisions

Read-only. Spec: ``docs/proposals/ta/2026-09-18-decided-bar-revision-census.md``.

Discharges the clause #2414 marked *"Not yet measured"* — *"How often a
``price_daily`` bar is actually revised after first write, and by how much […] a
revision rate of zero would change the priority, and a non-trivial one bounds how
much of a stored track record is silently stale"* — now that ``strategy_signals``
is no longer the empty table it was when the ticket was filed.

TWO INSTRUMENTS, and they are complementary rather than redundant.

**Part 1 — the DIRECT arm.** ``strategy_signals.fill_price`` is a STORED COPY of
one bar value: ``resolve_fills`` prices every fill at ``open(signal_index + 1)``
from the masked series, and ``load_masked_bars`` masks fields to ``None`` without
rescaling anything (``app/services/price_masked_bars.py:174``). So
``fill_price = price_daily.open`` at ``(instrument_id, fill_bar_date)`` is an
INVARIANT, and any inequality is proof that the bar moved after the verdict was
stored. ⚠⚠ This needs NO audit relation, so it covers the **whole ledger** with no
telemetry floor and no denominator caveat. It is exact.

**Part 2 — the AUDIT arms.** The direct arm can only see the one bar the ledger
keeps a copy of. A revision to the DECISION bar, or anywhere in the segment prefix,
leaves no stored copy to compare against — for those, ``price_daily_revision``
(sql/387) and ``price_daily_backdated_insert`` (sql/388) are the only witnesses,
and they record nothing before their own first row.

⚠⚠ WHY A REVISION ROW MEANS THE VALUE REALLY MOVED, which is what makes the audit
arms' claims exact rather than inferred. ``_upsert_candles``' ``ON CONFLICT DO
UPDATE`` carries ``WHERE price_daily.open IS DISTINCT FROM EXCLUDED.open OR … volume
IS DISTINCT FROM EXCLUDED.volume`` (``app/services/market_data.py:1439-1443``), so a
re-fetch that changes nothing writes nothing; ``_record_bar_revisions`` (``:1483``)
then appends one row per bar the UPDATE actually touched, INSIDE the same
transaction as the bar write.

⚠⚠ WHAT NEITHER PART ESTABLISHES, and none of it is repairable here.

* **Not a count of affected VERDICTS.** ``sql/387``'s header is the governing
  document and it kills that inference, listing five overcounts (segmented
  evaluation restarts indicator state; ``load_masked_bars`` masks fields; a
  volume-only revision is indistinguishable from a close revision;
  ``signal_bar_date >= price_date`` does not establish the bar existed at scan
  time; not every strategy has unbounded memory) and three undercounts (the regime
  is computed on the BENCHMARK, so instrument equality excludes every regime-gated
  name; cross-sectional ranking evaluates instruments together; ``resolve_fills``
  reads the NEXT bar's open). Every arm here is a claim about BARS.
* **The audit arms are not a rate over the whole ledger.** ``price_daily`` keeps no
  prior value, so a bar overwritten before the audit floor left no trace anywhere.
  The observable denominator is signals written at or after the floor; the
  remainder is UNMEASURABLE, printed as such and never as zero. ⚠ The direct arm
  does not have this problem and is the reason both are here.
* **The audit arms are FLOORS.** ``strategy_signals.created_at`` is the ledger
  WRITE time, not the bar READ time (sql/387), and the scan loads and computes
  before opening its write transaction. A revision committed inside that gap was
  not seen by the scan and is still counted here as "read in its corrected form".
  Closing it needs a read-set record.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

import psycopg
from psycopg import IsolationLevel

from app.config import settings

#: The two append-only audit relations, with the stamp each one dates its events by.
REVISION_SOURCE = ("price_daily_revision", "revised_at")
INSERT_SOURCE = ("price_daily_backdated_insert", "inserted_at")

#: Part 1. ⚠ Compared against the RAW ``price_daily.open``, not the masked value:
#: the question is whether the stored BAR moved, and a bar that became masked after
#: the fact is a different (also real) anomaly that this predicate would hide.
#: ⚠ ``verdict = 'fired'`` because ``strategy_signals_fill_matches_verdict``
#: guarantees ``fill_price`` is NULL on every other verdict — without it the
#: "agrees" bucket would silently absorb every non-fired row.
DIRECT_ARM_SQL = """
    SELECT COUNT(*)                                                          AS fired,
           COUNT(*) FILTER (WHERE p.open IS NULL)                            AS bar_gone,
           COUNT(*) FILTER (WHERE p.open IS NOT NULL AND p.open =  s.fill_price) AS agrees,
           COUNT(*) FILTER (WHERE p.open IS NOT NULL AND p.open <> s.fill_price) AS differs,
           COUNT(DISTINCT s.instrument_id) FILTER (WHERE p.open IS NOT NULL
                                                    AND p.open <> s.fill_price) AS instruments
    FROM strategy_signals s
    LEFT JOIN price_daily p
      ON p.instrument_id = s.instrument_id AND p.price_date = s.fill_bar_date
    WHERE s.verdict = 'fired'
"""

#: The shape of the disagreement, not just its size. A stored fill that differs by
#: an EXACT ratio shared across an instrument's rows is a corporate action; one that
#: differs by a few basis points is a late provider correction. The distinction
#: decides how much of a track record is wrong and by how much, which is the second
#: half of #2414's unmeasured clause ("and by how much").
DIRECT_ARM_RATIOS_SQL = """
    SELECT round(p.open / nullif(s.fill_price, 0), 4) AS ratio,
           COUNT(*), COUNT(DISTINCT s.instrument_id)
    FROM strategy_signals s
    JOIN price_daily p
      ON p.instrument_id = s.instrument_id AND p.price_date = s.fill_bar_date
    WHERE s.verdict = 'fired' AND p.open <> s.fill_price
    GROUP BY 1 ORDER BY 2 DESC
"""

DIRECT_ARM_SPAN_SQL = """
    SELECT MIN(s.signal_bar_date), MAX(s.signal_bar_date),
           MIN(s.created_at)::date, MAX(s.created_at)::date
    FROM strategy_signals s
    JOIN price_daily p
      ON p.instrument_id = s.instrument_id AND p.price_date = s.fill_bar_date
    WHERE s.verdict = 'fired' AND p.open <> s.fill_price
"""


#: Reconciles part 2's ``fill_bar`` arm against part 1, on the rows both can see.
#: ⚠⚠ THIS MEASURES sql/387's FIELD-BLINDNESS OVERCOUNT rather than asserting it.
#: ``price_daily_revision`` records that a bar was overwritten, not WHICH field
#: moved — so a same-day close/high/low/volume settle produces a revision row on a
#: bar whose OPEN never changed, and the fill price it priced is not stale at all.
#: The two parts are expected to disagree here; the size of the disagreement is the
#: overcount, and printing it is the only honest way to read the arm.
CROSS_CHECK_SQL = """
    SELECT COUNT(*) FILTER (WHERE p.open =  s.fill_price) AS open_unchanged,
           COUNT(*) FILTER (WHERE p.open <> s.fill_price) AS open_moved
    FROM strategy_signals s
    JOIN price_daily p
      ON p.instrument_id = s.instrument_id AND p.price_date = s.fill_bar_date
    WHERE s.verdict = 'fired'
      AND s.created_at >= %(floor)s
      AND EXISTS (SELECT 1 FROM price_daily_revision r
                  WHERE r.instrument_id = s.instrument_id
                    AND r.price_date = s.fill_bar_date
                    AND r.revised_at > s.created_at)
"""


@dataclass(frozen=True)
class Arm:
    """One claim about bars, and the predicate that is exactly as strong as it.

    ⚠ THE DATE PREDICATES ARE THE DECLARATION. ``decision_bar`` is
    ``= signal_bar_date``, ``fill_bar`` is ``= fill_bar_date``, ``prefix`` is
    ``< signal_bar_date`` — and ``strategy_signals_fill_after_signal`` guarantees
    ``fill_bar_date > signal_bar_date``, so no (signal, event) PAIR can satisfy two
    of them. ⚠ That does NOT make the arms additive per signal: one signal may
    appear in several, which is why the union is reported separately rather than
    left to a reader who would sum the columns.
    """

    key: str
    date_predicate: str
    fired_only: bool
    claim: str


ARMS: Sequence[Arm] = (
    Arm(
        key="decision_bar",
        date_predicate="e.price_date = s.signal_bar_date",
        fired_only=False,
        claim="the bar this verdict is ABOUT no longer holds the values it was decided on",
    ),
    Arm(
        key="fill_bar",
        date_predicate="e.price_date = s.fill_bar_date",
        fired_only=True,
        claim="the bar whose OPEN priced this fill was overwritten — the one arm the direct "
        "comparison in part 1 also covers, so the two should agree where their windows overlap",
    ),
    Arm(
        key="prefix",
        date_predicate="e.price_date < s.signal_bar_date",
        fired_only=False,
        claim="NEITHER BOUND on affected verdicts (sql/387) — reported because for a "
        "Wilder-recursive strategy the segment prefix IS the read set",
    ),
)


def arm_sql(arm: Arm, *, table: str, stamp: str, by_cause: bool) -> str:
    """The arm's count over one audit relation, on the caller's snapshot.

    ⚠ ``COUNT(DISTINCT s.signal_id)``, never pairs: ``_record_bar_revisions``
    documents that ``revised_bar_dates`` MAY REPEAT A DATE, so one signal can match
    through several rows and a pair count would report the same staleness twice.

    ⚠ ``e.{stamp} > s.created_at`` is on every arm. Without it the arm would count
    bars overwritten BEFORE the verdict was stored — which the scan read in their
    corrected form, and which are not staleness at all.
    """
    cause = "e.cause," if by_cause else ""
    group = "GROUP BY e.cause ORDER BY 2 DESC" if by_cause else ""
    return f"""
        SELECT {cause} COUNT(DISTINCT s.signal_id)
        FROM strategy_signals s
        JOIN {table} e
          ON e.instrument_id = s.instrument_id
         AND {arm.date_predicate}
         AND e.{stamp} > s.created_at
        WHERE s.created_at >= %(floor)s
          {_fired_clause(arm)}
        {group}
    """


def union_sql(*, table: str, stamp: str) -> str:
    """Signals matching ANY arm — the number a reader would otherwise get by summing."""
    clauses = " OR ".join(f"({arm.date_predicate}{_fired_clause(arm)})" for arm in ARMS)
    return f"""
        SELECT COUNT(DISTINCT s.signal_id)
        FROM strategy_signals s
        JOIN {table} e
          ON e.instrument_id = s.instrument_id
         AND e.{stamp} > s.created_at
        WHERE s.created_at >= %(floor)s AND ({clauses})
    """


def _fired_clause(arm: Arm) -> str:
    return " AND s.verdict = 'fired'" if arm.fired_only else ""


def observable_split(total: int, before_floor: int) -> tuple[int, int]:
    """Split the ledger into what the audit relation could have seen, and what it could not.

    ⚠ A relation with no rows has no floor, and the caller passes
    ``before_floor == total`` for that case: reporting ``(total, 0)`` would be a
    measured zero over a population no instrument was watching.
    """
    if before_floor > total:
        raise ValueError(f"before_floor {before_floor} exceeds total {total}")
    if total < 0 or before_floor < 0:
        raise ValueError(f"counts must be non-negative, got total={total} before_floor={before_floor}")
    return total - before_floor, before_floor


def _scalar(cur: psycopg.Cursor, sql: str, params: dict[str, object] | None = None) -> int:
    row = cur.execute(sql, params or {}).fetchone()
    return 0 if row is None or row[0] is None else int(row[0])


def _report_direct_arm(cur: psycopg.Cursor) -> None:
    print(f"\n{'=' * 78}\nPART 1 — DIRECT: stored fill_price vs the bar it was copied from")
    print(f"{'=' * 78}")
    print("  Full ledger. No telemetry floor, no audit relation, exact.")

    row = cur.execute(DIRECT_ARM_SQL).fetchone()
    assert row is not None
    fired, bar_gone, agrees, differs, instruments = row
    if fired == 0:
        print("\n  0 fired signals — nothing stores a fill price, so this arm is vacuous.")
        return

    print(f"\n  fired signals            : {fired:,}")
    print(f"  fill bar no longer present: {bar_gone:,}")
    print(f"  stored price still agrees : {agrees:,}")
    print(f"  ⚠ STORED PRICE IS STALE   : {differs:,}  ({100.0 * differs / fired:.4f}% of fired)")
    print(f"    across instruments      : {instruments:,}")

    if differs == 0:
        return

    span = cur.execute(DIRECT_ARM_SPAN_SQL).fetchone()
    assert span is not None
    print(f"    signal bar dates        : {span[0]} .. {span[1]}")
    print(f"    written                 : {span[2]} .. {span[3]}")

    print("\n  by ratio (current open / stored fill_price) — an EXACT shared ratio is a")
    print("  corporate action; a few basis points is a late provider correction:")
    for ratio, n, insts in cur.execute(DIRECT_ARM_RATIOS_SQL).fetchall():
        print(f"    {str(ratio):>12}  {n:>6,} signal(s)  {insts:>4} instrument(s)")


def _report_audit_source(cur: psycopg.Cursor, *, table: str, stamp: str, total_signals: int) -> None:
    print(f"\n{'=' * 78}\nPART 2 — AUDIT: {table}\n{'=' * 78}")

    events = _scalar(cur, f"SELECT COUNT(*) FROM {table}")
    if events == 0:
        observable, unmeasurable = observable_split(total_signals, total_signals)
        print(f"  0 rows — no floor exists, so NOTHING is observable through this relation.")
        print(f"  observable {observable:,} / unmeasurable {unmeasurable:,}. Arms not reported.")
        return

    floor_row = cur.execute(f"SELECT MIN({stamp}), MAX({stamp}) FROM {table}").fetchone()
    assert floor_row is not None
    floor, ceiling = floor_row
    print(f"  rows            : {events:,}")
    print(f"  telemetry window: {floor.isoformat()} .. {ceiling.isoformat()}")

    print("\n  per cause (events / instruments / distinct bar dates / bar-date span):")
    for cause, n, insts, dates, lo, hi in cur.execute(
        f"""
        SELECT cause, COUNT(*), COUNT(DISTINCT instrument_id), COUNT(DISTINCT price_date),
               MIN(price_date), MAX(price_date)
        FROM {table} GROUP BY cause ORDER BY 2 DESC
        """
    ).fetchall():
        print(f"    {cause:<20} {n:>7,}  {insts:>6,}  {dates:>6,}  {lo} .. {hi}")

    before = _scalar(
        cur, "SELECT COUNT(*) FROM strategy_signals WHERE created_at < %(floor)s", {"floor": floor}
    )
    observable, unmeasurable = observable_split(total_signals, before)
    print(f"\n  DENOMINATOR — signals written at or after the floor : {observable:,}")
    print(f"  UNMEASURABLE — signals written before it            : {unmeasurable:,}")
    print("    ⚠ unmeasurable is NOT zero-affected: price_daily keeps no prior value, so a")
    print("      bar overwritten before the floor left no trace in this relation. Part 1 is")
    print("      the arm that reaches them, and only for the fill bar.")

    if observable == 0:
        print("\n  No observable signals — every arm below would be a vacuous 0. Not reported.")
        return

    print("\n  arms (DISTINCT signals; disjoint per pair, NOT additive per signal):")
    for arm in ARMS:
        hits = _scalar(cur, arm_sql(arm, table=table, stamp=stamp, by_cause=False), {"floor": floor})
        print(f"\n    {arm.key:<14} {hits:>7,} of {observable:,}  ({100.0 * hits / observable:.4f}%)")
        print(f"      claim: {arm.claim}")
        if hits:
            for cause, n in cur.execute(
                arm_sql(arm, table=table, stamp=stamp, by_cause=True), {"floor": floor}
            ).fetchall():
                print(f"        {cause:<20} {n:>7,}")

    union = _scalar(cur, union_sql(table=table, stamp=stamp), {"floor": floor})
    print(f"\n    ANY ARM        {union:>7,} of {observable:,}  ({100.0 * union / observable:.4f}%)")

    if table == REVISION_SOURCE[0]:
        row = cur.execute(CROSS_CHECK_SQL, {"floor": floor}).fetchone()
        assert row is not None
        unchanged, moved = row
        print("\n  RECONCILIATION with part 1, on the fill_bar arm's own rows —")
        print("  sql/387 records THAT a bar was overwritten, not WHICH field moved:")
        print(f"    open never moved (overcount): {unchanged:,}")
        print(f"    open really moved           : {moved:,}")
        if unchanged and not moved:
            print("    ⚠ EVERY hit is an overcount on this window: the revisions moved some")
            print("      other OHLCV field, so no fill price these arms flag is actually stale.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()

    # ⚠ ONE SNAPSHOT for the whole census. sql/387's checkpoint 2: under READ
    # COMMITTED a deepening committing between two statements lets a script certify
    # a state that never existed. Set on the CONNECTION, before the first execute —
    # `SET TRANSACTION ISOLATION LEVEL` is only legal as a transaction's first
    # statement, so issuing it through `conn.execute` would depend on psycopg not
    # having opened one already. Both audit relations are append-only, so there is
    # no serialisation failure to retry.
    with psycopg.connect(settings.database_url) as conn:
        conn.isolation_level = IsolationLevel.REPEATABLE_READ
        with conn.cursor() as cur:
            total = _scalar(cur, "SELECT COUNT(*) FROM strategy_signals")
            print(f"strategy_signals: {total:,} rows")
            if total == 0:
                print("  empty — #2414 is still academic and nothing below can be non-zero.")
                conn.rollback()
                return
            span = cur.execute(
                "SELECT MIN(created_at), MAX(created_at), COUNT(DISTINCT strategy_id), "
                "COUNT(DISTINCT instrument_id) FROM strategy_signals"
            ).fetchone()
            assert span is not None
            lo, hi, strategies, instruments = span
            print(f"  written {lo.isoformat()} .. {hi.isoformat()}")
            print(f"  {strategies} strategy id(s), {instruments:,} instrument(s)")

            _report_direct_arm(cur)
            for table, stamp in (REVISION_SOURCE, INSERT_SOURCE):
                _report_audit_source(cur, table=table, stamp=stamp, total_signals=total)

        conn.rollback()

    print(f"\n{'=' * 78}")
    print("Read-only. Every arm is a claim about BARS, never about affected verdicts —")
    print("sql/387's header is the governing document and kills that inference.")
    print(f"Run at {datetime.now().astimezone().isoformat()}")


if __name__ == "__main__":
    main()
