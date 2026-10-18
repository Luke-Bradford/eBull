"""#3227 — full-population A/B of the Form 4 vehicle-balance sum (``form4_observation_shares``).

Spec: docs/specs/ownership/2026-10-17-3227-form4-vehicle-balance.md, Acceptance 2.

Control = each ``I`` observation's latest-line balance (the pre-change value). Treatment = the
helper's value. Both come from ONE pass of ``derive_form4_observations`` — the weekly writer's own
derivation, no ``since``, no ``LIMIT`` — so the comparison is against a RECOMPUTED baseline, never
against whatever the observation currently holds.

Stage 0 runs the census (``scripts.census_3227_form4_vehicle_balance``) in the same snapshot and
checks that the helper's verdict equals the census verdict for every candidate the weekly writer
reaches, counting the census candidates it does not reach. Stage 1 counts observations up /
unchanged / down and what the stored observations hold. Stage 2 writes each arm's value onto the
changed observations inside a transaction that is ROLLED BACK, refreshes ``_current`` (the
per-instrument refresher for both arms, and the batch refresher for the treatment, which must
agree), and reports ``_current`` keys, the operator-visible insider total, oversubscription, and
each changed holder's ``ownership_history`` points.

    PYTHONPATH=. uv run python -m scripts.ab_3227_form4_vehicle_balance

``--apply`` is the backfill, run once AFTER the change is deployed: ``sync_insiders`` with no
``since`` and no ``LIMIT`` (it commits), then the batch ``_current`` refresh of every instrument
with a form4 observation. ``--verify`` then checks both acceptance equalities: every stored
observation the writer reaches equals its recompute, and stored ``_current`` equals a fresh one.
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter, defaultdict
from decimal import Decimal
from typing import Any
from uuid import uuid4

import psycopg

from app.config import settings
from app.services.ownership_history import get_ownership_history
from app.services.ownership_observations import (
    record_insider_observation,
    refresh_insiders_current,
    refresh_insiders_current_batch,
)
from app.services.ownership_observations_sync import Form4Observation, derive_form4_observations, sync_insiders
from scripts import census_3227_form4_vehicle_balance as census
from scripts.ab_3227_holding_line_sum import _insider_view, _snapshot

_BATCH = 200
_SMOKE = ("AAPL", "GME", "MSFT", "JPM", "HD")

_STORED_SQL = """
SELECT instrument_id, source_document_id, holder_cik, ownership_nature, shares
  FROM ownership_insiders_observations
 WHERE source = 'form4' AND known_to IS NULL AND source_document_id !~ ':'
"""

# Holders who also carry a DERA (``:NDT:``/``:NDH:``) ``_current`` row on the instrument.
_DERA_SQL = """
SELECT DISTINCT instrument_id, holder_cik FROM ownership_insiders_current
 WHERE source_document_id ~ ':(NDT|NDH):'
"""


def _key(o: Form4Observation) -> tuple[int, str, str | None, str]:
    return (o.instrument_id, o.accession_number, o.holder_cik, o.ownership_nature)


def _write(conn: psycopg.Connection[Any], o: Form4Observation, shares: Decimal) -> None:
    assert o.sec_filed_at is not None
    record_insider_observation(
        conn,
        instrument_id=o.instrument_id,
        holder_cik=o.holder_cik,
        holder_name=o.holder_name or (o.holder_cik or "UNKNOWN"),
        ownership_nature=o.ownership_nature,  # type: ignore[arg-type]
        source="form4",
        source_document_id=o.accession_number,
        source_accession=o.accession_number,
        source_field=None,
        source_url=o.source_url,
        filed_at=o.sec_filed_at,
        period_start=None,
        period_end=o.period_end,
        ingest_run_id=uuid4(),
        shares=shares,
    )


def _stage0(conn: psycopg.Connection[Any], obs: list[Form4Observation]) -> None:
    for stmt in (census._LINES, census._CANDIDATES, census._CONDITIONS, census._HIST, census._CROSS_FILING):
        conn.execute(stmt)
    verdicts = {
        (int(r[0]), r[1], r[2]): r[3]
        for r in conn.execute(
            f"SELECT instrument_id, accession_number, filer_cik, {census._VERDICT} FROM c_eval e"
        ).fetchall()
    }
    helper = {
        (o.instrument_id, o.accession_number, o.holder_cik): o.verdict
        for o in obs
        if o.verdict is not None and o.verdict not in ("out_of_scope", "1 one_form")
    }
    agree = sum(1 for k, v in helper.items() if verdicts.get(k) == v)
    print("=== stage 0: helper verdicts vs census ===")
    print(f"census candidates                               {len(verdicts):>10,}")
    print(f"helper candidates (weekly writer reaches)       {len(helper):>10,}")
    print(f"  verdict equal                                 {agree:>10,}")
    differs = sum(1 for k, v in helper.items() if k in verdicts and verdicts[k] != v)
    print(f"  verdict differs                                 {differs:>10,}")
    print(f"  not a census candidate                        {sum(1 for k in helper if k not in verdicts):>10,}")
    for k, v in list(helper.items()):
        if verdicts.get(k) != v:
            print(f"    {k}: helper {v!r}, census {verdicts.get(k)!r}")
    unreached = Counter(v for k, v in verdicts.items() if k not in helper)
    print(f"census candidates the writer does not reach      {sum(unreached.values()):>10,}   (retention / no I line)")
    for v, n in sorted(unreached.items()):
        print(f"    {v:<34} {n:>8,}")
    by_verdict = Counter(helper.values())
    print("helper verdicts:")
    for v, n in sorted(by_verdict.items()):
        print(f"    {v:<34} {n:>8,}")


def _verify(conn: psycopg.Connection[Any]) -> int:
    stored = {(int(r[0]), r[1], r[2], r[3]): r[4] for r in conn.execute(_STORED_SQL).fetchall()}
    diffs = [o for o in derive_form4_observations(conn) if stored.get(_key(o)) != o.shares]
    print(f"stored observations differing from recompute    {len(diffs):>10,}")
    for o in diffs[:20]:
        print(f"  {_key(o)}: stored {stored.get(_key(o))}, recompute {o.shares}")
    ids = [
        int(r[0])
        for r in conn.execute(
            "SELECT DISTINCT instrument_id FROM ownership_insiders_observations WHERE source = 'form4' ORDER BY 1"
        ).fetchall()
    ]
    cur_diff = 0
    for i in range(0, len(ids), _BATCH):
        batch = ids[i : i + _BATCH]
        before = _snapshot(conn, batch)
        refresh_insiders_current_batch(conn, instrument_ids=batch)
        after = _snapshot(conn, batch)
        cur_diff += sum(1 for k in before.keys() | after.keys() if before.get(k) != after.get(k))
        conn.rollback()
    print(f"_current keys differing from a fresh refresh     {cur_diff:>10,}")
    return 0 if not diffs and not cur_diff else 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="run sync_insiders in full + batch refresh, and commit")
    ap.add_argument("--verify", action="store_true", help="check stored == recompute, for observations and _current")
    args = ap.parse_args()

    with psycopg.connect(settings.database_url) as conn:
        if args.apply:
            summary = sync_insiders(conn)
            conn.commit()
            print(f"sync_insiders: {summary.observations_recorded:,} recorded, {len(summary.orphans):,} orphans")
            return 0
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ  # one snapshot for stage 0 + 1
        conn.execute("SET jit = off")
        conn.execute("SET LOCAL statement_timeout = '1800s'")
        if args.verify:
            return _verify(conn)

        obs = list(derive_form4_observations(conn))
        _stage0(conn, obs)

        stored = {(int(r[0]), r[1], r[2], r[3]): r[4] for r in conn.execute(_STORED_SQL).fetchall()}
        dera = {(int(r[0]), r[1]) for r in conn.execute(_DERA_SQL).fetchall()}
        symbols = dict(conn.execute("SELECT instrument_id, symbol FROM instruments").fetchall())
        conn.rollback()

        changed = [o for o in obs if o.shares != o.latest_line_shares]
        print("\n=== stage 1: observations ===")
        print(f"observations the weekly writer derives          {len(obs):>10,}")
        up = sum(o.shares > o.latest_line_shares for o in obs)
        down = sum(o.shares < o.latest_line_shares for o in obs)
        print(f"  up / unchanged / down (down must be 0)        {up:>6,} / {len(obs) - up - down:,} / {down:,}")
        holds = Counter(
            "treatment"
            if stored.get(_key(o)) == o.shares
            else "control"
            if stored.get(_key(o)) == o.latest_line_shares
            else "missing"
            if _key(o) not in stored
            else "other"
            for o in changed
        )
        print(
            f"  changed: stored holds control / treatment / other / missing   "
            f"{holds['control']:,} / {holds['treatment']:,} / {holds['other']:,} / {holds['missing']:,}"
        )
        by_iid: dict[int, list[Form4Observation]] = defaultdict(list)
        for o in changed:
            by_iid[o.instrument_id].append(o)
        print(f"  changed instruments                           {len(by_iid):>10,}")
        n_dera = sum((o.instrument_id, o.holder_cik) in dera for o in changed)
        print(f"  changed keys whose holder has a DERA _current row  {n_dera:,}")
        smoke = [symbols.get(i) for i in by_iid if symbols.get(i) in _SMOKE]
        print(f"  smoke panel instruments touched               {smoke or 'none'}")

        print("\n=== stage 2: _current, rollup, history (rolled back) ===")
        rows: list[tuple[str, Decimal, Decimal, bool, bool, int, bool, bool]] = []
        cur_keys = hist_points = refresher_mismatch = 0
        for iid, group in by_iid.items():
            sym = symbols.get(iid, "?")
            arms: list[Any] = []
            for arm in ("control", "treatment", "batch"):
                for o in group:
                    _write(conn, o, o.latest_line_shares if arm == "control" else o.shares)
                if arm == "batch":
                    refresh_insiders_current_batch(conn, instrument_ids=[iid])
                else:
                    refresh_insiders_current(conn, instrument_id=iid)
                snap = _snapshot(conn, [iid])
                view = _insider_view(conn, sym, iid)
                hist = {
                    (o.holder_cik, p.period_end, p.ownership_nature): p.shares
                    for o in group
                    for p in get_ownership_history(conn, instrument_id=iid, category="insiders", holder_id=o.holder_cik)
                }
                arms.append((snap, view, hist))
                conn.rollback()
            (sc, vc, hc), (st, vt, ht), (sb, _vb, _hb) = arms
            refresher_mismatch += st != sb
            moved_keys = sum(1 for k in sc.keys() | st.keys() if sc.get(k) != st.get(k))
            cur_keys += moved_keys
            hist_points += sum(1 for k in hc.keys() | ht.keys() if hc.get(k) != ht.get(k))
            dera_hit = any((o.instrument_id, o.holder_cik) in dera for o in group)
            rows.append((sym, vc[0], vt[0], vc[2], vt[2], moved_keys, dera_hit, sym in _SMOKE))

        print(f"_current keys changed                           {cur_keys:>10,}")
        print(f"ownership_history points changed                {hist_points:>10,}")
        print(f"instruments where batch != per-instrument       {refresher_mismatch:>10,}")
        moved = [r for r in rows if r[1] != r[2]]
        print(f"insider total moved                             {len(moved):>10,}")
        n_up, n_down = sum(r[2] > r[1] for r in rows), sum(r[2] < r[1] for r in rows)
        print(f"  up / down                                     {n_up:>6,} / {n_down:,}")
        print(f"newly oversubscribed (gate 0, or inspect each)  {sum((not r[3]) and r[4] for r in rows):>10,}")
        for r in rows:
            if r[4] and not r[3]:
                print(f"  newly: {r[0]:<10} {r[1]:>18,.0f} -> {r[2]:>18,.0f}")
        print("\nDERA co-presence instruments (rollup before -> after):")
        for r in rows:
            if r[6]:
                print(f"  {r[0]:<10} {r[1]:>18,.0f} -> {r[2]:>18,.0f}{' OVERSUBSCRIBED' if r[4] else ''}")
        print("\nlargest insider-total moves (25):")
        for r in sorted(rows, key=lambda r: -(r[2] - r[1]))[:25]:
            print(f"  {r[0]:<10} {r[1]:>18,.0f} -> {r[2]:>18,.0f}{' OVERSUBSCRIBED' if r[4] else ''}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
