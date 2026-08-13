"""Time the accession-keyed lookups on ``def14a_beneficial_holdings``.

Issue: #2171.

Every rewashed DEF 14A row and every live DEF 14A ingest runs several lookups
keyed on ``accession_number`` alone. None of the four indexes present leads with
that column, so each one is a sequential scan whose cost grows with the table.

The three shapes timed here are the read-side ones, at their call sites:

    A. ``app/services/rewash_filings.py:518``   cover-label guard
       ``SELECT holder_name ... WHERE accession_number = %s``
    B. ``app/services/rewash_filings.py:587``   existing-rows probe
       ``SELECT issuer_cik, instrument_id ... WHERE accession_number = %s LIMIT 1``
    C. ``app/services/rewash_filings.py:851``   sibling-instrument union
       ``SELECT DISTINCT instrument_id ... WHERE accession_number = %s
         AND instrument_id IS NOT NULL``

Two write shapes are NOT timed, because timing them means executing them:

    D. ``app/services/rewash_filings.py:720``  accession-wide DELETE — same
       predicate as A, so A's plan is its plan. Confirm with ``--explain``.
    E. ``app/services/def14a_ingest.py:743``   DELETE carrying
       ``instrument_id = ANY(...)``, which already leads with
       ``uq_def14a_holdings_instrument_accession_holder``. Not a seq scan today;
       included in ``--explain`` so a regression there is visible.

FULL POPULATION, not a sample: each shape runs once per accession over every
distinct ``accession_number`` in the table. A single hot accession measures the
buffer cache, not the scan.

Read-only. Runs against whatever ``settings.database_url`` points at, and never
writes. Index variants are created and dropped OUTSIDE this script so the same
binary times every arm.

Usage:
    uv run python scripts/bench_2171_def14a_accession_lookup.py --label pre
    uv run python scripts/bench_2171_def14a_accession_lookup.py --explain
"""

from __future__ import annotations

import argparse
import sys
import time
from typing import Any, LiteralString

import psycopg

from app.config import settings

# Verbatim from the call sites named in the module docstring. Kept as literals
# rather than imported: the point is to time the SQL, and importing the callers
# would drag in the rewash runner's transaction and locking.
SHAPES: dict[str, LiteralString] = {
    "A_cover_label_guard": ("SELECT holder_name FROM def14a_beneficial_holdings WHERE accession_number = %s"),
    "B_existing_rows_probe": (
        "SELECT issuer_cik, instrument_id FROM def14a_beneficial_holdings WHERE accession_number = %s LIMIT 1"
    ),
    "C_sibling_instrument_union": (
        "SELECT DISTINCT instrument_id FROM def14a_beneficial_holdings "
        "WHERE accession_number = %s AND instrument_id IS NOT NULL"
    ),
}

EXPLAIN_ONLY: dict[str, tuple[LiteralString, tuple[object, ...]]] = {
    "D_accession_wide_delete": (
        "DELETE FROM def14a_beneficial_holdings WHERE accession_number = %s",
        (),
    ),
    "E_ingest_supersede_delete": (
        "DELETE FROM def14a_beneficial_holdings WHERE accession_number = %s "
        "AND instrument_id = ANY(%s::bigint[]) AND holder_name <> ALL(%s::text[])",
        ([1], ["placeholder"]),
    ),
}


def _accessions(conn: psycopg.Connection[Any]) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT accession_number FROM def14a_beneficial_holdings ORDER BY accession_number")
        return [row[0] for row in cur.fetchall()]


def _census(conn: psycopg.Connection[Any], when: str) -> None:
    """Table size at arm start and end.

    The dev jobs daemon writes this table while the bench runs, and the arms are
    sequential — the prevention-log rule that an A/B is invalid across sequential
    arms under a concurrent writer applies directly. A seq scan's cost is a
    function of table size, so drift between arms is the contamination that
    matters. Printed rather than guarded: the arms cannot be serialised against
    the daemon from here, so the honest move is to publish the drift alongside
    the timings and let the reader judge it against the effect size.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*), count(DISTINCT accession_number), "
            "pg_size_pretty(pg_relation_size('def14a_beneficial_holdings')) "
            "FROM def14a_beneficial_holdings"
        )
        row = cur.fetchone()
    assert row is not None
    print(f"  census[{when}] rows={row[0]} accessions={row[1]} heap={row[2]}")


def _time_shapes(conn: psycopg.Connection[Any], accessions: list[str], label: str) -> int:
    print(f"label={label}  accessions={len(accessions)}")
    _census(conn, "start")
    for name, sql in SHAPES.items():
        started = time.perf_counter()
        rows = 0
        with conn.cursor() as cur:
            for acc in accessions:
                cur.execute(sql, (acc,))
                rows += len(cur.fetchall())
        elapsed = time.perf_counter() - started
        per_call_ms = elapsed * 1000.0 / len(accessions)
        print(f"  {name:<28} total={elapsed:8.2f}s  per_call={per_call_ms:7.3f}ms  rows={rows}", flush=True)
    _census(conn, "end")
    return 0


def _explain(conn: psycopg.Connection[Any], accession: str) -> int:
    for name, sql in SHAPES.items():
        print(f"--- {name}")
        with conn.cursor() as cur:
            # EXPLAIN ANALYZE is safe on the read shapes.
            cur.execute(f"EXPLAIN (ANALYZE, BUFFERS) {sql}", (accession,))
            for row in cur.fetchall():
                print(f"    {row[0]}")
    for name, (sql, extra) in EXPLAIN_ONLY.items():
        print(f"--- {name}  (plan only — EXPLAIN without ANALYZE, it is a DELETE)")
        with conn.cursor() as cur:
            cur.execute(f"EXPLAIN {sql}", (accession, *extra))
            for row in cur.fetchall():
                print(f"    {row[0]}")
    return 0


def _verify(conn: psycopg.Connection[Any], accessions: list[str]) -> int:
    """Full-population equivalence: indexed plan vs forced seq-scan plan.

    An index is only safe if it changes no answer. The A/B that establishes that
    is not a timing — it is running every shape over every accession twice, once
    with the index available and once with the planner forced back onto the
    pre-migration plan, and requiring the result sets to be identical.

    Forcing is per-transaction (``SET LOCAL``) and read-only.

    ``enable_indexonlyscan`` is turned off in the control arm alongside
    ``enable_indexscan``. Empirically it is redundant — Postgres will not choose
    an index-only scan with ``enable_indexscan = off``, verified by EXPLAINing
    shape C's control plan on the dev corpus and getting a Seq Scan — but the
    dependency is undocumented at the call site and shape C is precisely the one
    the migration covers index-only, so the control is stated explicitly rather
    than relying on it (Codex checkpoint 2, P3). The assertion below turns the
    whole question into a check instead of a claim.
    """
    _census(conn, "verify")
    mismatches = 0
    for name, sql in SHAPES.items():
        differing = 0
        with conn.cursor() as cur:
            # The control arm is only a control if it really is the pre-migration
            # plan. Assert that once per shape rather than assuming it.
            cur.execute("SET LOCAL enable_indexscan = off")
            cur.execute("SET LOCAL enable_bitmapscan = off")
            cur.execute("SET LOCAL enable_indexonlyscan = off")
            cur.execute(f"EXPLAIN {sql}", (accessions[0],))
            control_plan = "\n".join(str(r[0]) for r in cur.fetchall())
            if "Seq Scan" not in control_plan:
                print(f"  CONTROL NOT A SEQ SCAN for {name}:\n{control_plan}")
                return 1
            for acc in accessions:
                cur.execute("SET LOCAL enable_indexscan = on")
                cur.execute("SET LOCAL enable_bitmapscan = on")
                cur.execute("SET LOCAL enable_indexonlyscan = on")
                cur.execute("SET LOCAL enable_seqscan = on")
                indexed = sorted(map(repr, cur.execute(sql, (acc,)).fetchall()))
                # Force the pre-migration plan.
                cur.execute("SET LOCAL enable_indexscan = off")
                cur.execute("SET LOCAL enable_bitmapscan = off")
                cur.execute("SET LOCAL enable_indexonlyscan = off")
                control = sorted(map(repr, cur.execute(sql, (acc,)).fetchall()))
                if indexed != control:
                    differing += 1
                    if differing <= 3:
                        print(f"    MISMATCH {name} {acc}: indexed={indexed!r} control={control!r}")
        print(f"  {name:<28} accessions={len(accessions)} mismatches={differing}", flush=True)
        mismatches += differing
    print(f"  TOTAL mismatches={mismatches}")
    return 0 if mismatches == 0 else 1


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--label", default="unlabelled", help="arm name recorded in the output")
    ap.add_argument("--explain", action="store_true", help="print plans for one accession instead of timing")
    ap.add_argument("--verify", action="store_true", help="full-population equivalence: indexed vs forced seq scan")
    args = ap.parse_args(argv)

    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET statement_timeout = 0")
        accessions = _accessions(conn)
        if not accessions:
            print("no accessions in def14a_beneficial_holdings — nothing to measure", file=sys.stderr)
            return 1
        if args.explain:
            return _explain(conn, accessions[0])
        if args.verify:
            return _verify(conn, accessions)
        return _time_shapes(conn, accessions, args.label)


if __name__ == "__main__":
    raise SystemExit(main())
