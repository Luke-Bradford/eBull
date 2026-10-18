"""#3227 — full-population A/B and backfill of a Form 4's mixed holding groups.

Spec: docs/specs/ownership/2026-10-18-3227-form4-mixed-groups.md, Change 3 and Acceptance 2/5.

    PYTHONPATH=. uv run python -m scripts.ab_3227_form4_mixed_groups            # A/B, writes nothing
    PYTHONPATH=. uv run python -m scripts.ab_3227_form4_mixed_groups --apply    # backfill (commits)
    PYTHONPATH=. uv run python -m scripts.ab_3227_form4_mixed_groups --verify   # after --apply

The cohort is every accession with a mixed key in ``scripts.census_3227_form4_mixed_groups``.

A/B (control = what is stored now):

* stage 0, one REPEATABLE READ snapshot — the weekly writer (``derive_form4_observations``, every
  Form 4/4A observation it reaches) against the stored observations and the census: the fold
  verdict per mixed key equals the census's; a ``sums`` key's shares equal the census value
  exactly; a refused key's equal the stored ones; observations up / unchanged / down (down must be
  0, and a key that is not mixed must not move); every field other than ``shares`` unchanged; every
  4/A ``sums`` key beside its base;
* stage 1, ONE transaction ROLLED BACK at the end — the live writer replays every cohort accession
  from its stored ``form4_xml`` payload (``upsert_filing``, as ``--apply`` does), which rewrites
  the share-class siblings too. Reports observations up / unchanged / down / added / removed and
  field changes over the entity AND sibling instruments, then ``_current`` keys changed by both
  refreshers (per-instrument and batch, which must agree), DERA winners dropped, the insider
  rollup, newly oversubscribed instruments, ``ownership_history`` points and the smoke panel.

``--apply`` replays the cohort through ``parse_form_4_xml`` + ``upsert_filing`` with the
per-accession ``_current`` refresh off, committing per accession, then runs ``sync_insiders`` with
no ``since`` and no ``LIMIT``, then batch-refreshes ``_current`` for every instrument with a form4
observation — ``sync_insiders`` alone never rewrites the share-class siblings (prevention log,
#3885). ``--verify`` checks every stored Form 4/4A observation the weekly writer reaches equals its
recompute, every sibling observation on a cohort accession equals its entity's, and stored
``_current`` equals a fresh batch refresh.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter, defaultdict
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Final, LiteralString

import psycopg

from app.config import settings
from app.services.insider_transactions import (
    FORM4_FOLD_NOT_MIXED,
    FORM4_VEHICLE_SUMS,
    parse_form_4_xml,
    upsert_filing,
)
from app.services.ownership_history import get_ownership_history
from app.services.ownership_observations import refresh_insiders_current, refresh_insiders_current_batch
from app.services.ownership_observations_sync import derive_form4_observations, sync_insiders
from app.services.ownership_rollup import get_ownership_rollup
from scripts import census_3227_form4_mixed_groups as census
from scripts.ab_3227_holding_line_sum import _snapshot

_BATCH = 200
_SMOKE = ("AAPL", "GME", "MSFT", "JPM", "HD")
_NATURE_DI: Final = {"direct": "D", "indirect": "I"}

# The payload and the filed_at the weekly writer uses (manifest first, else the earliest
# filing_events date on the filing's own instrument), so the replay's filed_at matches.
_PAYLOADS: Final[LiteralString] = """
SELECT r.accession_number, f.instrument_id, f.primary_document_url,
       COALESCE(m.filed_at,
                (SELECT min(fe.filing_date) FROM filing_events fe
                  WHERE fe.provider = 'sec' AND fe.provider_filing_id = f.accession_number
                    AND fe.instrument_id = f.instrument_id)::timestamp AT TIME ZONE 'UTC') AS filed_at,
       r.payload
  FROM filing_raw_documents r
  JOIN insider_filings f ON f.accession_number = r.accession_number
  LEFT JOIN sec_filing_manifest m ON m.accession_number = r.accession_number
 WHERE r.document_kind = 'form4_xml' AND r.accession_number = ANY(%(accs)s::text[])
"""

_STORED_OBS: Final[LiteralString] = """
SELECT instrument_id, source_document_id, holder_cik, ownership_nature,
       shares, period_end, filed_at, source_url, holder_name
  FROM ownership_insiders_observations
 WHERE source = 'form4' AND known_to IS NULL AND source_document_id !~ ':'
   {scope}
"""

_ObsKey = tuple[int, str, str | None, str]
_ObsVal = tuple[Any, ...]  # shares, period_end, filed_at, source_url, holder_name


def _insider_view(conn: psycopg.Connection[Any], symbol: str, iid: int) -> tuple[Any, ...]:
    """``scripts.ab_3227_holding_line_sum._insider_view`` plus the shares the rollup's Section-16
    control-group collapse (#1652/#2230) removed: a fold that breaks an exact-value match between
    two insiders' lots can release a block the collapse counted once."""
    # ownership-rollup-snapshot: caller-owned — each arm is one transaction, rolled back
    r = get_ownership_rollup(conn, symbol, iid)
    ins = next((s for s in r.slices if s.category == "insiders"), None)
    total = ins.total_shares if ins else Decimal(0)
    pct_sum = sum((s.pct_outstanding for s in r.slices), Decimal(0))
    holders = {h.filer_name: h.shares for h in ins.holders} if ins else {}
    collapsed = sum(
        (c.shares_removed for c in r.corrections_applied if c.kind == "insider_control_group_collapse"), Decimal(0)
    )
    return total, pct_sum, r.residual.oversubscribed, holders, collapsed


def _stored(conn: psycopg.Connection[Any], accs: list[str] | None = None) -> dict[_ObsKey, _ObsVal]:
    out: dict[_ObsKey, _ObsVal] = {}
    if accs is None:
        rows = conn.execute(_STORED_OBS.format(scope="")).fetchall()
    else:
        sql = _STORED_OBS.format(scope="AND source_document_id = ANY(%(accs)s::text[])")
        rows = []
        for i in range(0, len(accs), 5000):
            rows.extend(conn.execute(sql, {"accs": accs[i : i + 5000]}).fetchall())  # type: ignore[arg-type]
    for r in rows:
        out[(int(r[0]), r[1], r[2], r[3])] = tuple(r[4:])
    return out


def _cohort(conn: psycopg.Connection[Any]) -> tuple[list[census.Result], list[str]]:
    results, _counts = census.collect(conn)
    return results, sorted({r.accession for r in results})


def _payloads(conn: psycopg.Connection[Any], accs: list[str]) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for i in range(0, len(accs), 2000):
        rows.extend(conn.execute(_PAYLOADS, {"accs": accs[i : i + 2000]}).fetchall())
    return rows


def _replay(conn: psycopg.Connection[Any], rows: list[tuple[Any, ...]], *, commit: bool) -> tuple[int, int, int]:
    done = none = failed = 0
    t0 = time.monotonic()
    for accession, iid, url, filed_at, payload in rows:
        parsed = parse_form_4_xml(payload)
        if parsed is None:
            none += 1
            continue
        try:
            with conn.transaction():  # a savepoint inside stage 1's transaction; a commit unit under --apply
                upsert_filing(
                    conn,
                    instrument_id=int(iid),
                    accession_number=accession,
                    primary_document_url=str(url) if url else "",
                    parsed=parsed,
                    is_rewash=True,
                    filed_at=filed_at,
                    refresh_current=False,
                )
            if commit:
                conn.commit()
            done += 1
        except Exception as exc:  # noqa: BLE001 — one accession must not stop the replay; counted
            failed += 1
            print(f"  FAILED {accession}: {exc}")
        if (done + none + failed) % 2000 == 0:
            print(f"  {done + none + failed:,} / {len(rows):,}  {time.monotonic() - t0:,.0f}s", flush=True)
    print(f"replayed {done:,}, parser None {none:,}, failed {failed:,}, {time.monotonic() - t0:,.0f}s")
    return done, none, failed


def _stage0(conn: psycopg.Connection[Any]) -> list[census.Result]:  # noqa: C901, PLR0912, PLR0915
    results, accs = _cohort(conn)
    by_key = {(r.instrument_id, r.accession, r.filer_cik, r.di): r for r in results}
    stored = _stored(conn)
    weekly = list(derive_form4_observations(conn))
    print("=== stage 0: weekly writer vs stored observations and the census ===")
    print(f"cohort: mixed keys {len(results):,}, accessions {len(accs):,}")
    print(f"weekly-writer observations {len(weekly):,}")

    moves: Counter[str] = Counter()
    field_diffs: list[str] = []
    verdict_diffs: list[str] = []
    sums_diffs: list[str] = []
    refused_diffs: list[str] = []
    seen: set[tuple[int, str, str, str]] = set()
    fold_verdicts: Counter[tuple[str, str]] = Counter()
    for o in weekly:
        k = (o.instrument_id, o.accession_number, o.holder_cik, o.ownership_nature)
        prior = stored.get(k)
        if prior is None:
            moves["absent from stored"] += 1
            field_diffs.append(f"absent {k}")
            continue
        # The fields sync_insiders records beside ``shares``.
        filed = o.sec_filed_at or datetime.combine(o.period_end, datetime.min.time(), tzinfo=UTC)
        fields = (o.period_end, filed, o.source_url, o.holder_name or (o.holder_cik or "UNKNOWN"))
        if prior[1:] != fields:
            field_diffs.append(f"{k}: stored {prior[1:]}, recompute {fields}")
        mixed = o.kind == "transaction" and o.fold_verdict not in (None, FORM4_FOLD_NOT_MIXED, "out_of_scope")
        cls = (
            "holding-only"
            if o.kind == "holding"
            else f"mixed {'sums' if o.fold_verdict == FORM4_VEHICLE_SUMS else 'refused'}"
            if mixed
            else "not mixed"
        )
        moves[f"{cls}: {'up' if o.shares > prior[0] else 'down' if o.shares < prior[0] else 'unchanged'}"] += 1
        if not mixed:
            continue
        di = _NATURE_DI[o.ownership_nature]
        ck = (o.instrument_id, o.accession_number, o.holder_cik or "", di)
        seen.add(ck)
        fold_verdicts[(di, str(o.fold_verdict))] += 1
        c = by_key.get(ck)
        if c is None or c.verdict != o.fold_verdict:
            verdict_diffs.append(f"{ck}: helper {o.fold_verdict}, census {c and c.verdict}")
        if o.fold_verdict == FORM4_VEHICLE_SUMS:
            if c is None or c.value != o.shares:
                sums_diffs.append(f"{ck}: helper {o.shares}, census {c and c.value}")
        elif o.shares != prior[0]:
            refused_diffs.append(f"{ck}: recompute {o.shares}, stored {prior[0]}")
    census_only = [r for k, r in by_key.items() if k not in seen]

    for k, n in sorted(moves.items()):
        print(f"  {k:<40} {n:>10,}")
    print("fold verdicts (weekly writer):")
    for (di, v), n in sorted(fold_verdicts.items()):
        print(f"  {di} {v:<40} {n:>8,}")
    print(f"helper fold verdict != census                   {len(verdict_diffs):>10,}")
    print(f"sums value != census value (exact Decimal)      {len(sums_diffs):>10,}")
    print(f"refused value != stored                         {len(refused_diffs):>10,}")
    print(f"non-shares field differences / absent           {len(field_diffs):>10,}")
    for label, items in (
        ("verdict", verdict_diffs),
        ("sums", sums_diffs),
        ("refused", refused_diffs),
        ("fields", field_diffs),
    ):
        for item in items[:20]:
            print(f"    [{label}] {item}")
    print(f"census mixed keys with no weekly mixed observation  {len(census_only):,}")
    for r in census_only:
        print(f"    iid={r.instrument_id} {r.accession} CIK:{r.filer_cik} {r.di} {r.verdict} today={r.today}")

    print("\n4/A sums keys beside their base (same instrument, filer, D/I and stored period_end, Form 4):")
    period = {(k[0], k[1], k[2], k[3]): v[1] for k, v in stored.items()}
    by_filer: dict[tuple[int, str, str], list[census.Result]] = defaultdict(list)
    for r in results:
        by_filer[(r.instrument_id, r.filer_cik, r.di)].append(r)
    for r in sorted(results, key=lambda r: (r.instrument_id, r.accession)):
        if r.document_type != "4/A" or r.verdict != census.SUMS:
            continue
        pe = period.get((r.instrument_id, r.accession, r.filer_cik, census._NATURE[r.di]))
        bases = [
            b
            for b in by_filer[(r.instrument_id, r.filer_cik, r.di)]
            if b.document_type == "4"
            and period.get((b.instrument_id, b.accession, b.filer_cik, census._NATURE[b.di])) == pe
        ]
        base = "; ".join(f"{b.accession} {b.verdict} today={b.today} -> {b.value}" for b in bases) or "no mixed base"
        print(f"  iid={r.instrument_id} {r.accession} {r.di} today={r.today} -> {r.value} | base: {base}")
    conn.rollback()
    return results


def _stage1(conn: psycopg.Connection[Any], results: list[census.Result]) -> None:  # noqa: C901, PLR0912, PLR0915
    accs = sorted({r.accession for r in results})
    print("\n=== stage 1: live-writer replay, _current, rollup, history (rolled back) ===")
    before = _stored(conn, accs)
    ids = sorted({k[0] for k in before})
    symbols: dict[int, str] = dict(conn.execute("SELECT instrument_id, symbol FROM instruments").fetchall())
    cur_before: dict[tuple[Any, ...], tuple[Any, ...]] = {}
    for i in range(0, len(ids), _BATCH):
        cur_before.update(_snapshot(conn, ids[i : i + _BATCH]))
    rows = _payloads(conn, accs)
    print(f"cohort accessions {len(accs):,}; with a form4_xml payload {len(rows):,}; instruments {len(ids):,}")
    _, _, failed = _replay(conn, rows, commit=False)
    after = _stored(conn, accs)

    moves: Counter[str] = Counter()
    field_changes: list[str] = []
    changed_by_iid: dict[int, set[str | None]] = defaultdict(set)
    for k in before.keys() | after.keys():
        b, a = before.get(k), after.get(k)
        if b is None or a is None:
            moves["added" if b is None else "removed"] += 1
            field_changes.append(f"{'added' if b is None else 'removed'} {k}")
            continue
        if b[1:] != a[1:]:
            field_changes.append(f"{k}: {b[1:]} -> {a[1:]}")
        if a[0] != b[0]:
            changed_by_iid[k[0]].add(k[2])
        moves["up" if a[0] > b[0] else "down" if a[0] < b[0] else "unchanged"] += 1
    entity = {(r.instrument_id, r.accession) for r in results}
    sibling_keys = [k for k in after if (k[0], k[1]) not in entity]
    sib_moves = Counter(
        "up" if after[k][0] > before[k][0] else "down" if after[k][0] < before[k][0] else "unchanged"
        for k in sibling_keys
        if k in before
    )
    print(f"observations on cohort accessions (entity + siblings): {len(after):,}")
    for m, n in sorted(moves.items()):
        print(f"  {m:<12} {n:>10,}")
    print(f"  sibling observations {len(sibling_keys):,}: {dict(sib_moves)}")
    print(f"  field changes other than shares / presence     {len(field_changes):>10,}")
    for f in field_changes[:20]:
        print(f"    {f}")

    # _current: the per-instrument refresher (the live writer's), each rolled back, then the batch.
    per: dict[tuple[Any, ...], tuple[Any, ...]] = {}
    for iid in ids:
        with conn.transaction():
            refresh_insiders_current(conn, instrument_id=iid)
            per.update(_snapshot(conn, [iid]))
            raise psycopg.Rollback()
    batch: dict[tuple[Any, ...], tuple[Any, ...]] = {}
    for i in range(0, len(ids), _BATCH):
        refresh_insiders_current_batch(conn, instrument_ids=ids[i : i + _BATCH])
        batch.update(_snapshot(conn, ids[i : i + _BATCH]))
    disagree = sum(1 for k in per.keys() | batch.keys() if per.get(k) != batch.get(k))
    classes: Counter[str] = Counter()
    listing: list[str] = []
    for k in cur_before.keys() | batch.keys():
        b, a = cur_before.get(k), batch.get(k)
        if b == a:
            continue
        if b is None:
            cls = "added"
        elif a is None:
            cls = "removed: DERA winner dropped" if ":" in str(b[3]) else "removed: other"
        elif b[3] != a[3]:
            cls = "winner changed"
        elif b[9] != a[9]:
            cls = f"same winner, shares {'up' if a[9] > b[9] else 'down'}"
        else:
            cls = "same winner, other field"
        classes[cls] += 1
        if not cls.startswith("same winner, shares up"):
            listing.append(f"{cls}: {symbols.get(k[0])} {k} {b and (b[3], b[9])} -> {a and (a[3], a[9])}")
    print(f"_current: per-instrument vs batch disagreements  {disagree:>10,}")
    for c, n in sorted(classes.items()):
        print(f"  {c:<40} {n:>10,}")
    moved_iids = {k[0] for k in cur_before.keys() | batch.keys() if cur_before.get(k) != batch.get(k)}
    print(f"  _current instruments moved {len(moved_iids):,}")
    for item in listing[:40]:
        print(f"    {item}")

    # Rollup and history, over the instruments whose observations moved: the after-state is read
    # here, the before-state inside a savepoint that restores the stored observations' _current.
    moved = sorted(changed_by_iid)
    after_view = {iid: _insider_view(conn, symbols.get(iid, "?"), iid) for iid in moved}
    after_hist = {
        iid: {
            (h, p.period_end, p.ownership_nature): p.shares
            for h in changed_by_iid[iid]
            for p in get_ownership_history(conn, instrument_id=iid, category="insiders", holder_id=h)
        }
        for iid in moved
    }
    print(f"instruments with a moved observation {len(moved):,}")
    return_state = (moved, after_view, after_hist, symbols, changed_by_iid, failed)
    conn.rollback()
    _stage1_before(conn, *return_state)


def _stage1_before(
    conn: psycopg.Connection[Any],
    moved: list[int],
    after_view: dict[int, Any],
    after_hist: dict[int, dict[Any, Any]],
    symbols: dict[int, str],
    changed_by_iid: dict[int, set[str | None]],
    failed: int,
) -> None:
    """The control reads for stage 1's moved instruments, after its rollback restored today."""
    up = down = newly = hist_points = released = 0
    released_shares = Decimal(0)
    rows: list[str] = []
    for iid in moved:
        view = _insider_view(conn, symbols.get(iid, "?"), iid)
        hist = {
            (h, p.period_end, p.ownership_nature): p.shares
            for h in changed_by_iid[iid]
            for p in get_ownership_history(conn, instrument_id=iid, category="insiders", holder_id=h)
        }
        a = after_view[iid]
        up += a[0] > view[0]
        down += a[0] < view[0]
        hist_points += sum(1 for k in hist.keys() | after_hist[iid].keys() if hist.get(k) != after_hist[iid].get(k))
        holders = {
            n: (view[3].get(n), a[3].get(n)) for n in view[3].keys() | a[3].keys() if view[3].get(n) != a[3].get(n)
        }
        if a[4] < view[4]:
            released += 1
            released_shares += view[4] - a[4]
            rows.append(
                f"{symbols.get(iid)} control-group collapse RELEASED {view[4] - a[4]:,.0f}"
                f" (insiders {view[0]:,.0f} -> {a[0]:,.0f}); holders moved {holders}"
            )
        if a[0] < view[0]:
            rows.append(f"{symbols.get(iid)} rollup DOWN {view[0]:,.0f} -> {a[0]:,.0f}; holders moved {holders}")
        if a[2] and not view[2]:
            newly += 1
            rows.append(
                f"{symbols.get(iid)} newly oversubscribed: insiders {view[0]:,.0f} -> {a[0]:,.0f}"
                f" (pct sum {view[1]:.2f} -> {a[1]:.2f}); holders moved {holders}"
            )
    conn.rollback()
    print(f"insider rollup total moved: up {up:,} / down {down:,}")
    print(f"ownership_history points changed {hist_points:,}")
    print(f"instruments whose control-group collapse removes less: {released:,} ({released_shares:,.0f} shares)")
    print(f"newly oversubscribed (gate 0, or inspect each) {newly:,}; rollup-down and oversubscribed rows:")
    for r in rows:
        print(f"  {r}")
    smoke = sorted(symbols[i] for i in moved if symbols.get(i) in _SMOKE)
    print(f"smoke panel instruments moved: {smoke or 'none'}")
    print(f"replay failures {failed:,}")


def _apply(conn: psycopg.Connection[Any]) -> int:
    _results, accs = _cohort(conn)
    rows = _payloads(conn, accs)
    conn.commit()
    print(f"cohort accessions {len(accs):,}; with a form4_xml payload {len(rows):,}")
    _, _, failed = _replay(conn, rows, commit=True)
    summary = sync_insiders(conn)
    conn.commit()
    print(f"sync_insiders: {summary.observations_recorded:,} recorded, {len(summary.orphans):,} orphans")
    for o in summary.orphans[:20]:
        print(f"  orphan: {o}")
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
    results, accs = _cohort(conn)
    stored = _stored(conn)
    obs = list(derive_form4_observations(conn))
    diffs = [
        o
        for o in obs
        if stored.get((o.instrument_id, o.accession_number, o.holder_cik, o.ownership_nature), (None,))[0] != o.shares
    ]
    print(f"weekly-writer observations {len(obs):,}")
    print(f"stored observations differing from recompute    {len(diffs):>10,}")
    for o in diffs[:20]:
        k = (o.instrument_id, o.accession_number, o.holder_cik, o.ownership_nature)
        print(f"  {k} {o.kind}: stored {stored.get(k)}, recompute {o.shares}")
    entity = {(r.instrument_id, r.accession) for r in results}
    entity_iid = {r.accession: r.instrument_id for r in results}
    on_cohort = _stored(conn, accs)
    sib_diff = sib = 0
    for k, v in on_cohort.items():
        if (k[0], k[1]) in entity:
            continue
        sib += 1
        ent = on_cohort.get((entity_iid[k[1]], k[1], k[2], k[3]))
        sib_diff += ent is None or ent[0] != v[0]
    print(f"sibling observations on cohort accessions {sib:,}; differing from their entity's {sib_diff:,}")
    ids = [
        int(r[0])
        for r in conn.execute(
            "SELECT DISTINCT instrument_id FROM ownership_insiders_observations WHERE source = 'form4' ORDER BY 1"
        ).fetchall()
    ]
    cur_diff = 0
    for i in range(0, len(ids), _BATCH):
        batch = ids[i : i + _BATCH]
        with conn.transaction():
            before = _snapshot(conn, batch)
            refresh_insiders_current_batch(conn, instrument_ids=batch)
            after = _snapshot(conn, batch)
            cur_diff += sum(1 for k in before.keys() | after.keys() if before.get(k) != after.get(k))
            raise psycopg.Rollback()
    print(f"_current keys differing from a fresh refresh     {cur_diff:>10,}")
    conn.rollback()
    return 0 if not diffs and not sib_diff and not cur_diff else 1


def main() -> int:
    sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]  # interleave with log lines
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="replay the cohort + full sync_insiders, and commit")
    ap.add_argument("--verify", action="store_true", help="check stored == recompute, siblings, and _current")
    ap.add_argument("--stage0-only", action="store_true", help="skip stage 1 (the rolled-back replay)")
    ap.add_argument("--stage1-only", action="store_true", help="skip stage 0's comparisons (the cohort is still read)")
    args = ap.parse_args()

    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET jit = off")
        conn.commit()
        if args.apply:
            return _apply(conn)
        if args.verify:
            conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
            return _verify(conn)
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ  # one snapshot for stage 0
        results = _cohort(conn)[0] if args.stage1_only else _stage0(conn)
        conn.rollback()
        if args.stage0_only:
            return 0
        # READ COMMITTED: a REPEATABLE READ transaction this long fails on live jobs' concurrent
        # ``_current`` updates. Stage 1 is one transaction, rolled back.
        conn.isolation_level = psycopg.IsolationLevel.READ_COMMITTED
        _stage1(conn, results)
    return 0


if __name__ == "__main__":
    sys.exit(main())
