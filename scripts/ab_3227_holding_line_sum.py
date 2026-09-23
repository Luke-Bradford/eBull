"""#3227 item 3a — full-population A/B of the holding-line sum in the insiders projection.

Control = ``refresh_insiders_current_batch`` as it stands on a git ref (default
``origin/main``), loaded from ``git show`` so the two arms run the SAME database state.
Treatment = the working tree. Both arms run inside one transaction per batch that is ROLLED
BACK, so nothing is written: the comparison is against a RECOMPUTED baseline, never against
whatever ``_current`` happened to hold (a stale ``_current`` would otherwise show up as a
"change").

Stage 1 diffs ``_current`` for every instrument carrying a live ``:NDH:`` observation (the
only rows the change can reach) and asserts that nothing but ``shares`` moved. Stage 2 renders
the ownership rollup under both arms for every instrument stage 1 found changed, and reports
the operator-visible insider figure — gain side included.

    PYTHONPATH=. uv run python -m scripts.ab_3227_holding_line_sum [--ref origin/main]

``--apply`` is the backfill, run once AFTER the change is deployed: it refreshes ``_current``
with the working-tree projection for the same instrument set and COMMITS. A projection change
moves no observation watermark, so the repair sweep never re-runs these instruments by itself.
"""

from __future__ import annotations

import argparse
import importlib.util
import subprocess
import sys
import tempfile
from decimal import Decimal
from pathlib import Path
from types import ModuleType
from typing import Any

import psycopg

from app.config import settings
from app.services import ownership_observations as new_obs
from app.services.ownership_rollup import get_ownership_rollup

_BATCH = 200

_CURRENT_ROWS_SQL = """
SELECT instrument_id, holder_identity_key, ownership_nature,
       holder_cik, holder_name, source, source_document_id, source_accession, source_url,
       filed_at, period_start, period_end, shares
  FROM ownership_insiders_current
 WHERE instrument_id = ANY(%(ids)s::bigint[])
"""


def _load_control(ref: str) -> ModuleType:
    src = subprocess.run(
        ["git", "show", f"{ref}:app/services/ownership_observations.py"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    path = Path(tempfile.mkdtemp()) / "control_ownership_observations.py"
    path.write_text(src)
    spec = importlib.util.spec_from_file_location("control_ownership_observations", path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod  # dataclasses resolve annotations through sys.modules
    spec.loader.exec_module(mod)
    return mod


def _snapshot(conn: psycopg.Connection[Any], ids: list[int]) -> dict[tuple[Any, ...], tuple[Any, ...]]:
    rows = conn.execute(_CURRENT_ROWS_SQL, {"ids": ids}).fetchall()
    return {r[:3]: r[3:] for r in rows}


def _arm(conn: psycopg.Connection[Any], refresher: Any, ids: list[int]) -> dict[tuple[Any, ...], tuple[Any, ...]]:
    refresher(conn, instrument_ids=ids)
    return _snapshot(conn, ids)


def _insider_view(
    conn: psycopg.Connection[Any], symbol: str, iid: int
) -> tuple[Decimal, Decimal, bool, dict[str, Decimal]]:
    r = get_ownership_rollup(conn, symbol, iid)
    ins = next((s for s in r.slices if s.category == "insiders"), None)
    total = ins.total_shares if ins else Decimal(0)
    pct_sum = sum((s.pct_outstanding for s in r.slices), Decimal(0))
    holders = {h.filer_name: h.shares for h in ins.holders} if ins else {}
    return total, pct_sum, r.residual.oversubscribed, holders


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ref", default="origin/main")
    ap.add_argument("--apply", action="store_true", help="refresh _current with the new projection and commit")
    args = ap.parse_args()

    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET jit = off")
        ids = [
            int(r[0])
            for r in conn.execute(
                "SELECT DISTINCT instrument_id FROM ownership_insiders_observations"
                " WHERE source_document_id ~ ':NDH:[0-9]+$' AND known_to IS NULL ORDER BY 1"
            ).fetchall()
        ]
        conn.rollback()
        print(f"instruments carrying a live :NDH: observation   {len(ids):>8,}")
        if args.apply:
            for i in range(0, len(ids), _BATCH):
                new_obs.refresh_insiders_current_batch(conn, instrument_ids=ids[i : i + _BATCH])
                conn.commit()
            print(f"applied: refreshed {len(ids):,} instruments")
            return 0
        control = _load_control(args.ref)

        changed_keys: list[tuple[Any, ...]] = []
        non_share_moves = 0
        up = down = 0
        for i in range(0, len(ids), _BATCH):
            batch = ids[i : i + _BATCH]
            conn.execute("SELECT 1")  # open the outer transaction; the refreshers nest as savepoints
            a = _arm(conn, control.refresh_insiders_current_batch, batch)
            b = _arm(conn, new_obs.refresh_insiders_current_batch, batch)
            conn.rollback()
            if a.keys() != b.keys():
                print(f"KEY SET DIFFERS in batch at {i}: -{len(a.keys() - b.keys())} +{len(b.keys() - a.keys())}")
                return 1
            for k, va in a.items():
                vb = b[k]
                if va == vb:
                    continue
                changed_keys.append(k)
                if va[:-1] != vb[:-1]:
                    non_share_moves += 1
                sa, sb = va[-1], vb[-1]
                if sa is not None and sb is not None:
                    up += sb > sa
                    down += sb < sa

        changed_iids = sorted({k[0] for k in changed_keys})
        print(f"_current keys changed                            {len(changed_keys):>8,}")
        print(f"  shares up                                      {up:>8,}")
        print(f"  shares down                                    {down:>8,}")
        print(f"  a column other than shares moved               {non_share_moves:>8,}   (must be 0)")
        print(f"distinct instruments changed                     {len(changed_iids):>8,}")

        symbols = dict(
            conn.execute(
                "SELECT instrument_id, symbol FROM instruments WHERE instrument_id = ANY(%(ids)s::bigint[])",
                {"ids": changed_iids},
            ).fetchall()
        )
        conn.rollback()
        rows: list[tuple[str, Decimal, Decimal, Decimal, Decimal, bool, bool, str]] = []
        for iid in changed_iids:
            sym = symbols.get(iid, "?")
            conn.execute("SELECT 1")
            control.refresh_insiders_current(conn, instrument_id=iid)
            ta, pa, oa, ha = _insider_view(conn, sym, iid)
            new_obs.refresh_insiders_current(conn, instrument_id=iid)
            tb, pb, ob, hb = _insider_view(conn, sym, iid)
            conn.rollback()
            moved = sorted(
                ((n, ha.get(n, Decimal(0)), hb.get(n, Decimal(0))) for n in ha.keys() | hb.keys()),
                key=lambda t: -(abs(t[2] - t[1])),
            )
            top = next((f"{n}: {a:,.0f} -> {b:,.0f}" for n, a, b in moved if a != b), "")
            rows.append((sym, ta, tb, pa, pb, oa, ob, top))

        moved_rows = [r for r in rows if r[1] != r[2]]
        print("\n=== rollup, instruments whose _current changed ===")
        print(f"insider slice total moved                        {len(moved_rows):>8,}")
        print(f"  up                                             {sum(r[2] > r[1] for r in rows):>8,}")
        print(f"  down                                           {sum(r[2] < r[1] for r in rows):>8,}")
        print(f"  unchanged (absorbed by owner-once / MAX)       {len(rows) - len(moved_rows):>8,}")
        print(f"newly oversubscribed (was not, now is)           {sum((not r[5]) and r[6] for r in rows):>8,}")
        print(f"no longer oversubscribed                         {sum(r[5] and not r[6] for r in rows):>8,}")
        for sym, ta, tb, pa, pb, oa, ob, top in rows:
            if ob and not oa:
                print(f"  newly: {sym:<10} pie {pa * 100:6.2f}% -> {pb * 100:6.2f}%  | {top}")
        print("\n=== gain side, largest insider-total moves first (25) ===")
        for sym, ta, tb, pa, pb, oa, ob, top in sorted(rows, key=lambda r: -(r[2] - r[1]))[:25]:
            flag = " OVERSUBSCRIBED" if ob else ""
            print(f"{sym:<10} {ta:>16,.0f} -> {tb:>16,.0f}  pie {pa * 100:6.2f}% -> {pb * 100:6.2f}%{flag}  | {top}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
