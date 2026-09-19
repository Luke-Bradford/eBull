"""#3146 backfill — re-project ``ownership_insiders_current`` under the corrected line order.

#3146 changes only the projection's ORDER BY tail, so no filing is re-parsed and no
observation row changes. That means the daily drift-repair sweep will NOT pick the change up:
``app/jobs/ownership_observations_repair.py`` selects instruments whose
``ownership_refresh_state.last_drained_observations_max_ingested_at`` differs from their
observations' ``max(ingested_at)``, and a pure rule change moves neither. The repair has to be
driven once, explicitly.

Scope — instruments holding at least one live ``:NDT:`` observation, and no others. The new
tail only discriminates between ``:NDT:`` documents; with none present it reduces to
``split_part(...) ASC, NULL DESC, source_document_id ASC``, which is the pre-#3146 order.
So this scope is provably complete rather than merely conservative.

Usage::

    PYTHONPATH=. uv run python -m scripts.backfill_3146_insiders_current --plan
    PYTHONPATH=. uv run python -m scripts.backfill_3146_insiders_current --apply

``--plan`` is read-only. ``--apply`` writes ONLY ``ownership_insiders_current`` +
``ownership_refresh_state``, through the shipped ``refresh_insiders_current_batch`` writer —
never raw SQL against the target, so the diff-aware MERGE, the advisory locks and the
scope-clamped prune all apply exactly as they do in production.

Exits 1 if any instrument fails even the per-instrument fallback, and if the post-run
verification still finds a key whose winner disagrees with the shipped ordering.
"""

from __future__ import annotations

import argparse
import sys
import time
from typing import Any, LiteralString, cast

import psycopg

from app.config import settings
from app.services.ownership_observations import (
    INSIDER_WINNER_ORDER_TAIL,
    refresh_current_with_batch_fallback,
    refresh_insiders_current,
    refresh_insiders_current_batch,
)

_CHUNK = 500

# The ORDER BY head is stable and re-spelled here; the TAIL — the only part #3146 changes — is
# IMPORTED, so this verifier cannot drift into checking an ordering the code no longer ships
# (prevention-log: a verifier pinned to its own copy of the rule under test proves nothing).
_VERIFY_ORDER_HEAD = """
                 ORDER BY instrument_id, holder_identity_key, ownership_nature,
                          CASE source
                              WHEN 'form4' THEN 1 WHEN 'form3' THEN 2
                              WHEN '13d' THEN 3 WHEN '13g' THEN 3
                              WHEN 'def14a' THEN 4 WHEN '13f' THEN 5
                              WHEN 'nport' THEN 6 WHEN 'ncsr' THEN 6
                              WHEN 'xbrl_dei' THEN 7 WHEN '10k_note' THEN 8
                              WHEN 'finra_si' THEN 9 ELSE 10 END ASC,
                          period_end DESC, filed_at DESC, source ASC,
"""

_SCOPE_SQL = """
    SELECT DISTINCT instrument_id
      FROM ownership_insiders_observations
     WHERE known_to IS NULL AND source_document_id ~ ':NDT:'
     ORDER BY instrument_id
"""


def _scope(conn: psycopg.Connection[Any]) -> list[int]:
    with conn.cursor() as cur:
        cur.execute(_SCOPE_SQL)
        return [int(r[0]) for r in cur.fetchall()]


def _verify(conn: psycopg.Connection[Any]) -> int:
    """Count live ``_current`` rows the shipped ordering would NOT have chosen.

    Re-derives the winner per stored row from the observations and compares. A backfill that
    reports success while leaving rows on the old rule is the failure mode worth catching, so
    this is a release gate rather than a print.

    ⚠ Scope of what it can see: it JOINs stored ``_current`` to the derived winners, so it
    detects a stored row carrying the WRONG balance. It cannot detect a key the #1805
    de-collision drops, because such a key has no ``_current`` row to join to — that case is
    covered separately by the A/B's FULL OUTER "deleted" count
    (``scripts/audit_3146_insider_line_order.py --ab``), which is 0.
    """
    with conn.cursor() as cur:
        cur.execute(
            cast(
                LiteralString,
                f"""
            WITH derived AS (
                SELECT DISTINCT ON (instrument_id, holder_identity_key, ownership_nature)
                       instrument_id, holder_identity_key, ownership_nature,
                       source_document_id, shares
                  FROM ownership_insiders_observations
                 WHERE known_to IS NULL
                 {_VERIFY_ORDER_HEAD} {INSIDER_WINNER_ORDER_TAIL}
            )
            SELECT count(*)
              FROM ownership_insiders_current c
              JOIN derived d USING (instrument_id, holder_identity_key, ownership_nature)
             WHERE c.source_document_id ~ ':NDT:'
               AND c.shares IS DISTINCT FROM d.shares
        """,
            )
        )
        return int(cur.fetchone()[0])  # type: ignore[index]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", action="store_true", help="read-only: report the scope")
    parser.add_argument("--apply", action="store_true", help="re-project the scope")
    args = parser.parse_args()
    if not (args.plan or args.apply):
        parser.error("choose --plan or --apply")

    conn = psycopg.connect(settings.database_url)
    try:
        ids = _scope(conn)
        print(f"scope: {len(ids):,} instruments holding at least one live :NDT: observation")
        if not ids:
            print("nothing in scope — refusing to report success on an empty run")
            sys.exit(1)
        if args.plan:
            stale = _verify(conn)
            print(f"rows whose stored winner disagrees with the shipped ordering: {stale:,}")
            return

        t0 = time.time()
        done = 0
        failures: list[tuple[int, Exception]] = []
        for start in range(0, len(ids), _CHUNK):
            chunk = ids[start : start + _CHUNK]
            refreshed, chunk_failures = refresh_current_with_batch_fallback(
                conn,
                instrument_ids=chunk,
                refresh_batch_fn=refresh_insiders_current_batch,
                refresh_one_fn=refresh_insiders_current,
            )
            conn.commit()
            done += refreshed
            failures.extend(chunk_failures)
            print(f"  {done:,}/{len(ids):,} instruments  {time.time() - t0:.0f}s", flush=True)

        print(f"\nrefreshed {done:,} instruments in {time.time() - t0:.0f}s; failures {len(failures)}")
        for iid, exc in failures[:10]:
            print(f"  FAILED {iid}: {exc}")
        stale = _verify(conn)
        print(f"post-run rows still on the old winner: {stale:,}   ← must be 0")
        if failures:
            sys.exit(1)
        if stale:
            sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
