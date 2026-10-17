"""#3227 — full-population A/B of ``INSIDER_SUPERSEDED_BY_AMENDMENT_FILTER`` in the insiders projection.

Control = ``refresh_insiders_current_batch`` as it stands on a git ref (default ``origin/main``),
loaded from ``git show`` so the two arms run the SAME database state. Treatment = the working
tree. Both arms run inside one transaction per batch that is ROLLED BACK, so nothing is written:
the comparison is against a RECOMPUTED baseline, never against whatever ``_current`` held.

Stage 0 counts the observations the filter excludes. Stage 1 diffs ``_current`` for every
instrument carrying a Form 3/A or 4/A — the only instruments the filter can reach — by key:
removed, added, changed. Stage 2 renders the ownership rollup under both arms for every
instrument stage 1 found changed and reports the operator-visible insider figure, gain side too.

    PYTHONPATH=. uv run python -m scripts.ab_3227_amendment_restatement [--ref origin/main]

``--apply`` is the backfill, run once AFTER the change is deployed: it refreshes ``_current``
with the working-tree projection for the same instrument set and COMMITS. A projection change
moves no observation watermark, so the repair sweep never re-runs these instruments by itself.
"""

from __future__ import annotations

import argparse
import sys
import time
from decimal import Decimal
from typing import Any

import psycopg

from app.config import settings
from app.services import ownership_observations as new_obs
from scripts.ab_3227_holding_line_sum import _insider_view, _load_control, _snapshot

_BATCH = 200

# Every instrument carrying an observation from a 3/A or 4/A — not ``insider_filings.instrument_id``,
# which names one canonical instrument while the writers fan the same observations out to each
# share-class sibling (Codex ckpt-2). The filter can only exclude a row whose holder has an
# amendment observation on the same instrument, so this set is complete.
_INSTRUMENTS_SQL = """
SELECT DISTINCT o.instrument_id
  FROM ownership_insiders_observations o
  JOIN insider_filings f ON f.accession_number = o.source_accession
 WHERE f.document_type IN ('3/A', '4/A') AND o.known_to IS NULL
 ORDER BY 1
"""

# The observations the filter excludes, over the reachable instruments.
_EXCLUDED_SQL = (
    """
SELECT count(*), count(DISTINCT source_accession), count(DISTINCT instrument_id)
  FROM ownership_insiders_observations
 WHERE instrument_id = ANY(%(ids)s::bigint[]) AND known_to IS NULL
   AND NOT (TRUE
"""
    + new_obs.INSIDER_SUPERSEDED_BY_AMENDMENT_FILTER
    + ")"
)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ref", default="origin/main", help="control ref; its code is EXECUTED — trusted refs only")
    ap.add_argument("--apply", action="store_true", help="refresh _current with the new projection and commit")
    args = ap.parse_args()

    with psycopg.connect(settings.database_url) as conn:
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ  # one snapshot per arm
        conn.execute("SET jit = off")
        ids = [int(r[0]) for r in conn.execute(_INSTRUMENTS_SQL).fetchall()]
        print(f"instruments with a 3/A or 4/A observation      {len(ids):>8,}")
        if args.apply:
            conn.rollback()
            for i in range(0, len(ids), _BATCH):
                new_obs.refresh_insiders_current_batch(conn, instrument_ids=ids[i : i + _BATCH])
                conn.commit()
            print(f"applied: refreshed {len(ids):,} instruments")
            return 0
        n_obs, n_acc, n_iid = conn.execute(_EXCLUDED_SQL, {"ids": ids}).fetchone() or (0, 0, 0)
        conn.rollback()
        print(f"observations excluded (superseded)               {n_obs:>8,}")
        print(f"  original accessions                            {n_acc:>8,}")
        print(f"  instruments                                    {n_iid:>8,}")
        control = _load_control(args.ref)

        removed: list[tuple[Any, ...]] = []
        added: list[tuple[Any, ...]] = []
        changed: list[tuple[Any, ...]] = []
        t_control = t_treat = 0.0
        for i in range(0, len(ids), _BATCH):
            batch = ids[i : i + _BATCH]
            conn.execute("SELECT 1")  # open the outer transaction; the refreshers nest as savepoints
            t0 = time.monotonic()
            control.refresh_insiders_current_batch(conn, instrument_ids=batch)
            t_control += time.monotonic() - t0
            a = _snapshot(conn, batch)
            t0 = time.monotonic()
            new_obs.refresh_insiders_current_batch(conn, instrument_ids=batch)
            t_treat += time.monotonic() - t0
            b = _snapshot(conn, batch)
            conn.rollback()
            removed += [(k, a[k]) for k in a.keys() - b.keys()]
            added += [(k, b[k]) for k in b.keys() - a.keys()]
            changed += [(k, a[k], b[k]) for k in a.keys() & b.keys() if a[k] != b[k]]

        print(f"refresh time control / treatment                 {t_control:>7.1f}s / {t_treat:.1f}s")
        print(f"_current keys removed                            {len(removed):>8,}")
        print(f"_current keys added                              {len(added):>8,}")
        print(f"_current keys changed                            {len(changed):>8,}")
        # value tuple = (holder_cik, holder_name, source, source_document_id, source_accession,
        # source_url, filed_at, period_start, period_end, shares). A changed key either passes to a
        # filing at least as new (the amendment, typically filed the same day as its original, which
        # used to lose the accession-order tie), or FALLS BACK to an older filing on that key.
        newer = [(k, va, vb) for k, va, vb in changed if vb[6] >= va[6]]
        older = [(k, va, vb) for k, va, vb in changed if vb[6] < va[6]]
        print(f"  to a filing at least as new                    {len(newer):>8,}")
        print(f"    of which shares unchanged                    {sum(va[-1] == vb[-1] for _, va, vb in newer):>8,}")
        print(f"  fell back to an older filing                   {len(older):>8,}")
        for k, va, vb in older:
            print(f"    fallback {k}: {va[4]} {va[8]} {va[-1]} -> {vb[4]} {vb[8]} {vb[-1]}")
        for k, va in sorted(removed):
            print(f"  removed {k}: {va[4]} {va[-1]}")

        changed_iids = sorted({k[0] for k, *_ in removed + added + changed})
        print(f"distinct instruments changed                     {len(changed_iids):>8,}")
        symbols = dict(
            conn.execute(
                "SELECT instrument_id, symbol FROM instruments WHERE instrument_id = ANY(%(ids)s::bigint[])",
                {"ids": changed_iids},
            ).fetchall()
        )
        conn.rollback()
        rows: list[tuple[str, Decimal, Decimal, bool, bool, str]] = []
        for iid in changed_iids:
            sym = symbols.get(iid, "?")
            conn.execute("SELECT 1")
            control.refresh_insiders_current(conn, instrument_id=iid)
            ta, _pa, oa, ha = _insider_view(conn, sym, iid)
            new_obs.refresh_insiders_current(conn, instrument_id=iid)
            tb, _pb, ob, hb = _insider_view(conn, sym, iid)
            conn.rollback()
            moved = sorted(
                ((n, ha.get(n, Decimal(0)), hb.get(n, Decimal(0))) for n in ha.keys() | hb.keys()),
                key=lambda t: -(abs(t[2] - t[1])),
            )
            top = next((f"{n}: {x:,.0f} -> {y:,.0f}" for n, x, y in moved if x != y), "")
            rows.append((sym, ta, tb, oa, ob, top))

        print("\n=== rollup, instruments whose _current changed ===")
        print(f"insider slice total moved                        {sum(r[1] != r[2] for r in rows):>8,}")
        print(f"  up                                             {sum(r[2] > r[1] for r in rows):>8,}")
        print(f"  down                                           {sum(r[2] < r[1] for r in rows):>8,}")
        print(f"newly oversubscribed (was not, now is)           {sum((not r[3]) and r[4] for r in rows):>8,}")
        print(f"no longer oversubscribed                         {sum(r[3] and not r[4] for r in rows):>8,}")
        print("\n=== every moved instrument, largest move first ===")
        for sym, ta, tb, _oa, ob, top in sorted(rows, key=lambda r: -abs(r[2] - r[1])):
            if ta == tb:
                continue
            flag = " OVERSUBSCRIBED" if ob else ""
            print(f"{sym:<10} {ta:>16,.0f} -> {tb:>16,.0f}{flag}  | {top}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
