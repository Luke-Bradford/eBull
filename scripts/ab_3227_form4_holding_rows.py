"""#3227 — full-population A/B and backfill of a Form 4's holding-line observations.

Spec: docs/specs/ownership/2026-10-18-3227-form4-holding-rows.md, Change 4 and Acceptance 2/5.

    PYTHONPATH=. uv run python -m scripts.ab_3227_form4_holding_rows            # A/B, writes nothing
    PYTHONPATH=. uv run python -m scripts.ab_3227_form4_holding_rows --apply    # backfill (commits)
    PYTHONPATH=. uv run python -m scripts.ab_3227_form4_holding_rows --verify   # after --apply

The cohort is every stored ``form4_xml`` payload containing ``erivativeHolding`` that has an
``insider_filings`` row and a manifest ``filed_at`` inside the Form 4 retention window on its UTC
date — the manifest parser's own pre-fetch gates, so the replay cannot write a filing the weekly
writer would exclude.

A/B (control = what is stored now; treatment = the live writer's holding observations, computed
from each payload with the production parser and ``form4_holding_observations``):

* stage 0 — the helper's verdict, value and ``period_end`` per group against
  ``scripts.census_3227_form4_holding_rows``'s, and the parser's transactions against the stored
  ``insider_transactions`` rows (a replay must not move a transaction observation);
* stage 1 — observations added / changed / unchanged against the stored ones, per sibling the live
  writer fans out to;
* stage 2 — inside transactions that are ROLLED BACK, the treatment observations are written and
  ``_current`` refreshed (per-instrument and batch, which must agree); reports keys added, removed
  and changed by class (DERA winners dropped or replaced, source rank over a later ``form3``, ties,
  4/A keys), the insider rollup, newly oversubscribed instruments and ``ownership_history`` points.

``--apply`` replays the cohort through ``parse_form_4_xml`` + ``upsert_filing`` (stored bodies, no
refetch; SEC filings are immutable per accession), committing per accession, then runs
``sync_insiders`` with no ``since`` and no ``LIMIT``, then batch-refreshes ``_current`` for every
instrument with a form4 observation (the replay skips its per-accession refresh). ``--verify`` checks every stored
Form 4/4A observation the weekly writer reaches equals its recompute, and stored ``_current`` equals
a fresh batch refresh.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter, defaultdict
from datetime import UTC
from decimal import Decimal
from pathlib import Path
from typing import Any, Final, LiteralString
from uuid import uuid4

import psycopg
from psycopg.types.json import Jsonb

from app.config import settings
from app.services.insider_transactions import (
    FORM4_HOLDING_NATURE,
    ParsedFiling,
    form4_holding_observations,
    form4_within_retention,
    parse_form_4_xml,
    upsert_filing,
)
from app.services.ownership_history import get_ownership_history
from app.services.ownership_observations import (
    record_insider_observation,
    refresh_insiders_current,
    refresh_insiders_current_batch,
)
from app.services.ownership_observations_sync import derive_form4_observations, sync_insiders
from app.services.sec_identity import resolve_insider_writer_routing
from scripts import census_3227_form4_holding_rows as census
from scripts.ab_3227_holding_line_sum import _insider_view, _snapshot

_BATCH = 200
_SMOKE = ("AAPL", "GME", "MSFT", "JPM", "HD")

_COHORT: Final[LiteralString] = """
SELECT r.accession_number, f.instrument_id, f.primary_document_url, f.is_tombstone, f.document_type,
       f.period_of_report, m.filed_at, r.payload
  FROM filing_raw_documents r
  JOIN insider_filings f ON f.accession_number = r.accession_number
  JOIN sec_filing_manifest m ON m.accession_number = r.accession_number
 WHERE r.document_kind = 'form4_xml'
   AND r.payload LIKE '%%erivativeHolding%%'
"""

_STORED_TXNS: Final[LiteralString] = """
SELECT accession_number, txn_row_num, is_derivative, security_title, txn_date, txn_code, shares,
       post_transaction_shares, direct_indirect, nature_of_ownership
  FROM insider_transactions
 WHERE accession_number = ANY(%(accs)s::text[])
"""

_STORED_OBS: Final[LiteralString] = """
SELECT instrument_id, source_accession, holder_cik, ownership_nature, shares
  FROM ownership_insiders_observations
 WHERE source = 'form4' AND known_to IS NULL AND source_document_id !~ ':'
   AND source_accession = ANY(%(accs)s::text[])
"""

_MIGRATION = Path(__file__).resolve().parent.parent / "sql" / "503_insider_form4_holdings.sql"

_INSERT_HOLDING: Final[LiteralString] = """
INSERT INTO insider_form4_holdings (instrument_id, accession_number, row_num, filer_cik, filer_name,
       security_title, shares, is_derivative, direct_indirect, nature_of_ownership, footnote_refs)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

_VERIFY_STORED: Final[LiteralString] = """
SELECT instrument_id, source_document_id, holder_cik, ownership_nature, shares
  FROM ownership_insiders_observations
 WHERE source = 'form4' AND known_to IS NULL AND source_document_id !~ ':'
"""


def _cohort(conn: psycopg.Connection[Any]) -> list[tuple[Any, ...]]:
    """The cohort rows minus their payloads' failed gates (filed date taken in UTC)."""
    out = []
    with conn.cursor(name="cohort") as cur:
        cur.itersize = 2000
        cur.execute(_COHORT)
        for row in cur:
            filed_at = row[6]
            if filed_at is not None and form4_within_retention(filed_at.astimezone(UTC).date()):
                out.append(row)
    return out


def _txn_key(t: Any) -> tuple[Any, ...]:
    return (
        t.txn_row_num,
        t.is_derivative,
        t.security_title,
        t.txn_date,
        t.txn_code,
        None if t.shares is None else Decimal(t.shares),
        None if t.post_transaction_shares is None else Decimal(t.post_transaction_shares),
        t.direct_indirect,
        t.nature_of_ownership,
    )


class _Row:
    def __init__(self, r: tuple[Any, ...]) -> None:
        (
            _acc,
            self.txn_row_num,
            self.is_derivative,
            self.security_title,
            self.txn_date,
            self.txn_code,
            self.shares,
            self.post_transaction_shares,
            self.direct_indirect,
            self.nature_of_ownership,
        ) = r


def _apply(conn: psycopg.Connection[Any]) -> int:
    rows = _cohort(conn)
    conn.commit()
    print(f"cohort (gated) {len(rows):,}")
    t0, done, none, failed = time.monotonic(), 0, 0, 0
    for accession, iid, url, _tomb, _dt, _por, filed_at, payload in rows:
        parsed = parse_form_4_xml(payload)
        if parsed is None:
            none += 1
            continue
        try:
            upsert_filing(
                conn,
                instrument_id=int(iid),
                accession_number=accession,
                primary_document_url=str(url) if url else "",
                parsed=parsed,
                is_rewash=True,
                filed_at=filed_at,
                refresh_current=False,  # one batch refresh below, not ~0.45s per accession
            )
            conn.commit()
            done += 1
        except Exception as exc:  # noqa: BLE001 — one accession must not stop the replay; counted
            conn.rollback()
            failed += 1
            print(f"  FAILED {accession}: {exc}")
        if (done + none + failed) % 5000 == 0:
            print(f"  {done + none + failed:,} / {len(rows):,}  {time.monotonic() - t0:,.0f}s", flush=True)
    print(f"replayed {done:,}, parser None {none:,}, failed {failed:,}, {time.monotonic() - t0:,.0f}s")
    summary = sync_insiders(conn)
    conn.commit()
    print(f"sync_insiders: {summary.observations_recorded:,} recorded, {len(summary.orphans):,} orphans")
    for o in summary.orphans[:20]:
        print(f"  orphan: {o}")
    # The replay wrote observations on every share-class sibling without refreshing them, and
    # sync_insiders refreshes the instruments it wrote (entity instruments) only.
    ids = [
        int(r[0])
        for r in conn.execute(
            "SELECT DISTINCT instrument_id FROM ownership_insiders_observations WHERE source = 'form4' ORDER BY 1"
        ).fetchall()
    ]
    for i in range(0, len(ids), _BATCH):
        refresh_insiders_current_batch(conn, instrument_ids=ids[i : i + _BATCH])
        conn.commit()
    print(f"batch-refreshed _current for {len(ids):,} instruments")
    return 1 if failed else 0


def _verify(conn: psycopg.Connection[Any]) -> int:
    stored = {(int(r[0]), r[1], r[2], r[3]): r[4] for r in conn.execute(_VERIFY_STORED).fetchall()}
    obs = list(derive_form4_observations(conn))
    diffs = [
        o
        for o in obs
        if stored.get((o.instrument_id, o.accession_number, o.holder_cik, o.ownership_nature)) != o.shares
    ]
    holding = sum(o.kind == "holding" for o in obs)
    print(f"weekly-writer observations {len(obs):,} (holding {holding:,})")
    print(f"stored observations differing from recompute    {len(diffs):>10,}")
    for o in diffs[:20]:
        k = (o.instrument_id, o.accession_number, o.holder_cik, o.ownership_nature)
        print(f"  {k} {o.kind}: stored {stored.get(k)}, recompute {o.shares}")
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


def main() -> int:  # noqa: C901, PLR0912, PLR0915 — one linear report
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="replay the cohort + full sync_insiders, and commit")
    ap.add_argument("--verify", action="store_true", help="check stored == recompute, for observations and _current")
    args = ap.parse_args()

    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET jit = off")
        conn.commit()
        if args.apply:
            return _apply(conn)
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        if args.verify:
            return _verify(conn)

        # ---- stage 0: one snapshot for the census, the helper and the stored state
        census_groups, _counts, _seen = census.collect(conn)
        census_by = {(g.instrument_id, g.accession, g.filer_cik, g.di): g for g in census_groups}
        rows = _cohort(conn)
        accs = [r[0] for r in rows]
        stored_txns: dict[str, set[tuple[Any, ...]]] = defaultdict(set)
        stored_obs: dict[tuple[int, str, str | None, str], Decimal] = {}
        for i in range(0, len(accs), 5000):
            chunk = accs[i : i + 5000]
            for r in conn.execute(_STORED_TXNS, {"accs": chunk}).fetchall():
                stored_txns[r[0]].add(_txn_key(_Row(r)))
            for r in conn.execute(_STORED_OBS, {"accs": chunk}).fetchall():
                stored_obs[(int(r[0]), r[1], r[2], r[3])] = r[4]

        helper: dict[tuple[int, str, str, str], tuple[str, Decimal | None, Any]] = {}
        treatment: list[dict[str, Any]] = []
        drift: list[str] = []
        parsed_none = 0
        siblings_of: dict[tuple[int, str | None], list[int]] = {}
        entity_of: dict[tuple[int, str | None], int] = {}
        holding_rows: list[tuple[Any, ...]] = []
        evidence_iids: set[int] = set()
        for accession, iid, url, tomb, _dt, _por, filed_at, payload in rows:
            parsed: ParsedFiling | None = parse_form_4_xml(payload)
            if parsed is None:
                parsed_none += 1
                continue
            if {_txn_key(t) for t in parsed.transactions} != stored_txns.get(accession, set()):
                drift.append(accession)
            cik = parsed.filers[0].filer_cik
            groups = form4_holding_observations(
                [h for h in parsed.holdings if h.filer_cik == cik],
                [t for t in parsed.transactions if t.filer_cik == cik],
                document_type=parsed.document_type,
                owner_ciks=[f.filer_cik for f in parsed.filers],
                signature_date=parsed.signature_date,
                period_of_report=parsed.period_of_report,
                manifest_filed_at=filed_at,
            )
            rkey = (int(iid), parsed.issuer_cik)
            if rkey not in siblings_of:
                routing = resolve_insider_writer_routing(
                    conn, discovery_instrument_id=int(iid), issuer_cik=parsed.issuer_cik
                )
                siblings_of[rkey] = routing.sibling_instrument_ids or [routing.entity_instrument_id]
                entity_of[rkey] = routing.entity_instrument_id
            for h in parsed.holdings:
                holding_rows.append(
                    (
                        entity_of[rkey],
                        accession,
                        h.row_num,
                        h.filer_cik,
                        parsed.filers[0].filer_name,
                        h.security_title,
                        h.shares,
                        h.is_derivative,
                        h.direct_indirect,
                        h.nature_of_ownership,
                        Jsonb([{"footnote_id": r.footnote_id, "field": r.field} for r in h.footnote_refs]),
                    )
                )
                if h.direct_indirect == "I" and not h.is_derivative:
                    evidence_iids.update(siblings_of[rkey])
            for g in groups:
                helper[(int(iid), accession, cik, g.direct_indirect)] = (g.verdict, g.shares, g.period_end)
                if g.shares is None or g.period_end is None:
                    continue
                for sib in siblings_of[rkey]:
                    treatment.append(
                        {
                            "instrument_id": sib,
                            "accession": accession,
                            "holder_cik": cik,
                            "holder_name": parsed.filers[0].filer_name.strip() or cik,
                            "nature": FORM4_HOLDING_NATURE[g.direct_indirect],
                            "url": str(url) if url else None,
                            "filed_at": filed_at,
                            "period_end": g.period_end,
                            "shares": g.shares,
                            "doc_type": parsed.document_type,
                            "tombstoned": bool(tomb),
                        }
                    )
        symbols = dict(conn.execute("SELECT instrument_id, symbol FROM instruments").fetchall())

        print("=== stage 0: helper vs census, parser vs stored transactions ===")
        print(f"cohort payloads (gated)                         {len(rows):>10,}")
        print(f"  parser returned None                          {parsed_none:>10,}")
        print(f"  transaction drift (parse != stored rows)      {len(drift):>10,}")
        for a in drift[:20]:
            print(f"    {a}")
        census_keys = {k: (g.verdict, g.value, g.period_end) for k, g in census_by.items()}
        agree = sum(1 for k, v in helper.items() if census_keys.get(k) == v)
        print(f"helper groups                                   {len(helper):>10,}")
        print(f"census groups                                   {len(census_keys):>10,}")
        print(f"  equal (verdict, value, period_end)            {agree:>10,}")
        for k in sorted(helper.keys() ^ census_keys.keys())[:20]:
            print(f"    only in {'helper' if k in helper else 'census'}: {k} {helper.get(k) or census_keys.get(k)}")
        for k in sorted(k for k in helper.keys() & census_keys.keys() if helper[k] != census_keys[k])[:20]:
            print(f"    differs {k}: helper {helper[k]}, census {census_keys[k]}")
        print("helper verdicts:")
        for v, n in sorted(Counter(v[0] for v in helper.values()).items()):
            print(f"    {v:<40} {n:>8,}")

        print("\n=== stage 1: observations ===")
        verdict: Counter[str] = Counter()
        for o in treatment:
            prior = stored_obs.get((o["instrument_id"], o["accession"], o["holder_cik"], o["nature"]))
            verdict["added" if prior is None else "unchanged" if prior == o["shares"] else "changed"] += 1
        print(f"treatment observations (siblings included)      {len(treatment):>10,}")
        print(
            f"  added / unchanged / changed (changed must be 0)  "
            f"{verdict['added']:,} / {verdict['unchanged']:,} / {verdict['changed']:,}"
        )
        print("  removed: 0 by construction (record_insider_observation only upserts)")
        print(f"  from formerly tombstoned filings              {sum(o['tombstoned'] for o in treatment):>10,}")
        by_iid: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for o in treatment:
            by_iid[o["instrument_id"]].append(o)
        print(f"  instruments                                   {len(by_iid):>10,}")
        smoke = [symbols.get(i) for i in by_iid if symbols.get(i) in _SMOKE]
        print(f"  smoke panel touched                           {smoke or 'none'}")
        amend_keys = defaultdict(set)
        for o in treatment:
            amend_keys[(o["instrument_id"], o["holder_cik"], o["nature"])].add(o["doc_type"])
        four_a = [k for k, types in amend_keys.items() if {"4", "4/A"} <= types]
        print(f"  keys with holding observations from a 4 AND a 4/A  {len(four_a):,}")
        for k in four_a:
            print(f"    {symbols.get(k[0])} {k}")
        conn.rollback()

        # ---- stage 2: ONE transaction, rolled back at the end; each arm in a savepoint. The rollup
        # reads ``insider_form4_holdings`` (record-holder evidence, the name resolver), so the
        # control arm runs against the table empty and the treatment arm with the cohort's rows.
        print("\n=== stage 2: _current, rollup, history (rolled back) ===")
        classes: Counter[str] = Counter()
        listing: dict[str, list[str]] = defaultdict(list)
        moved_total = up = down = newly = mismatch = hist_points = 0
        newly_rows: list[str] = []
        ids = sorted(set(by_iid) | evidence_iids)
        print(f"instruments (holding observations or I holding lines)  {len(ids):,}")
        conn.execute(_MIGRATION.read_text())  # type: ignore[arg-type]  # no-op once applied
        if conn.execute("SELECT count(*) FROM insider_form4_holdings").fetchone() != (0,):
            print("insider_form4_holdings is not empty: the control arm would not be today's state")
            return 1

        def _arm(iid: int, group: list[dict[str, Any]], refresher: str) -> tuple[Any, Any, Any]:
            sym = symbols.get(iid, "?")
            holders = {o["holder_cik"] for o in group}
            snap: Any = None
            view: Any = None
            hist: Any = None
            with conn.transaction():
                run = uuid4()
                for o in group:
                    record_insider_observation(
                        conn,
                        instrument_id=iid,
                        holder_cik=o["holder_cik"],
                        holder_name=o["holder_name"],
                        ownership_nature=o["nature"],
                        source="form4",
                        source_document_id=o["accession"],
                        source_accession=o["accession"],
                        source_field=None,
                        source_url=o["url"],
                        filed_at=o["filed_at"],
                        period_start=None,
                        period_end=o["period_end"],
                        ingest_run_id=run,
                        shares=o["shares"],
                    )
                if refresher == "batch":
                    refresh_insiders_current_batch(conn, instrument_ids=[iid])
                else:
                    refresh_insiders_current(conn, instrument_id=iid)
                snap = _snapshot(conn, [iid])
                if refresher == "one":
                    view = _insider_view(conn, sym, iid)
                    hist = {
                        (h, p.period_end, p.ownership_nature): p.shares
                        for h in holders
                        for p in get_ownership_history(conn, instrument_id=iid, category="insiders", holder_id=h)
                    }
                raise psycopg.Rollback()
            return snap, view, hist

        control = {}
        for n, iid in enumerate(ids, 1):
            # Control: today's observations, freshly refreshed; history read for the same holders.
            snap, view, _ = _arm(iid, [], "one")
            holders = {o["holder_cik"] for o in by_iid.get(iid, [])}
            with conn.transaction():
                refresh_insiders_current(conn, instrument_id=iid)
                hist = {
                    (h, p.period_end, p.ownership_nature): p.shares
                    for h in holders
                    for p in get_ownership_history(conn, instrument_id=iid, category="insiders", holder_id=h)
                }
                raise psycopg.Rollback()
            control[iid] = (snap, view, hist)
            if n % 1000 == 0:
                print(f"  control {n:,} / {len(ids):,}", flush=True)
        with conn.cursor() as cur:
            cur.executemany(_INSERT_HOLDING, holding_rows)
        print(f"  treatment: {len(holding_rows):,} holding rows inserted (rolled back at the end)", flush=True)

        for n, iid in enumerate(ids, 1):
            sym = symbols.get(iid, "?")
            group = by_iid.get(iid, [])
            holding_docs = {(o["holder_cik"], o["nature"], o["accession"]) for o in group}
            before, view_b, hist_b = control[iid]
            after, view_a, hist_a = _arm(iid, group, "one")
            after_batch, _, _ = _arm(iid, group, "batch")
            mismatch += after != after_batch
            hist_points += sum(1 for k in hist_b.keys() | hist_a.keys() if hist_b.get(k) != hist_a.get(k))
            for key in before.keys() | after.keys():
                b, a = before.get(key), after.get(key)
                if b == a:
                    continue
                # value tuple: holder_cik, holder_name, source, source_document_id, source_accession,
                # source_url, filed_at, period_start, period_end, shares
                a_is_holding = a is not None and (a[0], key[2], a[3]) in holding_docs
                if b is None:
                    cls = "added (holding)" if a_is_holding else "added (other)"
                elif a is None:
                    cls = "removed: DERA winner dropped (clause 1)" if _is_dera(b[3]) else "removed: other"
                elif a_is_holding:
                    if _is_dera(b[3]):
                        cls = "changed: holding replaces a DERA winner"
                    elif b[2] == "form3" and b[8] is not None and b[8] > a[8]:
                        cls = "changed: holding wins by source rank over a later form3"
                    elif b[2] == "form4" and b[8] == a[8] and b[6] == a[6]:
                        cls = "changed: holding ties the winner (tail decides)"
                    elif b[9] == a[9]:
                        cls = "changed: holding wins, same value"
                    else:
                        cls = "changed: holding wins, value differs"
                else:
                    cls = "changed: other"
                classes[cls] += 1
                if not cls.startswith(("added (holding)", "changed: holding wins")):
                    listing[cls].append(f"{sym} {key[1]} {key[2]}: {b and (b[3], b[9])} -> {a and (a[3], a[9])}")
            if view_b[0] != view_a[0]:
                moved_total += 1
                up += view_a[0] > view_b[0]
                down += view_a[0] < view_b[0]
                if not group:
                    listing["rollup moved with no holding observation (evidence only)"].append(
                        f"{sym} {view_b[0]:,.0f} -> {view_a[0]:,.0f}"
                    )
            if view_b[3] != view_a[3] and not group:
                listing["insider holder names changed with no holding observation"].append(sym)
            if view_a[2] and not view_b[2]:
                newly += 1
                newly_rows.append(f"{sym} {view_b[0]:,.0f} -> {view_a[0]:,.0f}")
            if n % 1000 == 0:
                print(f"  treatment {n:,} / {len(ids):,}", flush=True)
        conn.rollback()

        for cls, k in sorted(classes.items()):
            print(f"  {cls:<58} {k:>8,}")
        print(f"instruments where batch != per-instrument       {mismatch:>10,}")
        print(f"ownership_history points changed                {hist_points:>10,}")
        print(f"insider rollup total moved                      {moved_total:>10,}  (up {up:,} / down {down:,})")
        print(f"newly oversubscribed (gate 0, or inspect each)  {newly:>10,}")
        for r in newly_rows:
            print(f"  {r}")
        for cls, items in sorted(listing.items()):
            print(f"\n[{cls}] {len(items):,}")
            for item in items:
                print(f"  {item}")
    return 0


def _is_dera(source_document_id: str | None) -> bool:
    doc = str(source_document_id or "")
    return ":NDT:" in doc or ":NDH:" in doc


if __name__ == "__main__":
    sys.exit(main())
