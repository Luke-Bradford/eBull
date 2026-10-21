"""Full-population A/B for the #3227 13F-duplicate fold (read-only).

Spec: docs/specs/ownership/2026-10-21-3227-13f-position-scope.md (Acceptance).

Builds every rollup on THIS checkout inside one REPEATABLE READ snapshot (session time zone UTC), with the fold,
the cover read, the co-filer read and the co-filer pass wrapped so their real inputs, outputs and costs are
recorded. The base arm is main's behaviour on the same input: the real ``_reconcile_cofiler_groups`` run on the
fold's INPUT (the fold is the only change between them). An independent ORACLE, this file's own copy of the spec's
rule and Change 2, re-reads cover rows (unbounded, per instrument) and co-filers itself.

Gates (non-zero exit on any failure):
  * population: selected = reached the pass + no_data (by reason) + errors; errors = 0;
  * the oracle's duplicate set equals the real fold's removed identities member for member, and the real
    corrections equal the oracle's (k, T) list; the real fold output equals the oracle's expected rows exactly,
    the dropped-source union included;
  * the oracle's co-filer read equals the real one;
  * per instrument: base total − treated total = Σ T; #3898's edges and ordered collapse list unchanged;
    newly oversubscribed = 0; input − output = Σ ``shares_removed`` over both passes;
  * #3898's input invariants (no ``insiders``/``blockholders`` row with a ``13f`` source; no ``institutions``
    identity with a ``13f`` row and another row) = 0;
  * no duplicate's k or M is a curated family-member CIK;
  * latency: (co-filer read + cover read + fold + co-filer pass) / (the rest of ``get_ownership_rollup``), p95 ≤
    10%, on the full population and on the affected instruments.

Reported, not gated: the census reconciliation (spec: 19 instruments, −346,612,691, 3 fixed) and the case review
(every duplicate with M, k, T, C, its filing date, P and every kept row of k and M).

Usage: ``PYTHONPATH=. uv run python -m scripts.ab_3227_13f_duplicates [--out PATH] [--limit N]``
"""

from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import sys
import time
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import psycopg

import app.services.ownership_rollup as rollup_mod
from app.config import settings
from app.db.snapshot import snapshot_read
from scripts.ab_3227_cofiler_links import _ORACLE_SQL as _COFILERS_ORACLE_SQL

_CATS = ("insiders", "blockholders", "institutions")
_COVERS_SQL = """
    SELECT reporter_cik, accession_number, filed_at, sole_dispositive_power, shared_dispositive_power,
           aggregate_amount_owned, filing_id
      FROM blockholder_filings WHERE instrument_id = %(iid)s AND filed_at IS NOT NULL
"""
_NAMED = {"GETY": Decimal(-115_259_246), "CVI.US": Decimal(-71_201_875), "RXT": Decimal(0), "MNRO": Decimal(0)}
_PANEL = ("AAPL", "GME", "MSFT", "JPM", "HD")


def _key(h: Any) -> str:
    return rollup_mod._identity_key(h.filer_cik, h.filer_name)


def _canon(raw: str | None) -> str | None:
    s = (raw or "").strip().lstrip("0")
    return s or None


def _digits(raw: str | None) -> str | None:
    s = _canon(raw)
    return s if s is not None and s.isascii() and s.isdigit() else None


def _key_canon(k: str) -> str | None:
    return _canon(k[4:]) if k.startswith("CIK:") else None


def _reports(row: tuple[Any, ...], t: Decimal) -> bool:
    _, _, _, sole, shared, agg, _ = row
    return sole is not None and shared is not None and sole + shared == t and agg == t


def oracle(by_category: Mapping[str, Sequence[Any]], covers: list[tuple[Any, ...]]) -> dict[str, Any]:
    """The spec's rule, written independently of ``_fold_13f_duplicates``."""
    rows: dict[str, list[Any]] = defaultdict(list)
    cat: dict[str, str] = {}
    for c, hs in by_category.items():
        for h in hs:
            cat[_key(h)] = c
            if c in _CATS:
                rows[_key(h)].append(h)
    tot = {k: sum((h.shares for h in hs), Decimal(0)) for k, hs in rows.items()}
    nodes: dict[str, list[str]] = defaultdict(list)
    for k, hs in rows.items():
        if tot[k] > 0 and _key_canon(k) and not any(h.winning_source in ("def14a", "13f") for h in hs):
            nodes[_key_canon(k) or ""].append(k)
    cand: dict[str, date] = {}
    for k, hs in rows.items():
        if cat[k] != "institutions" or _digits(k[4:] if k.startswith("CIK:") else None) is None or tot[k] <= 0:
            continue
        if any(h.winning_source != "13f" for h in hs):
            continue
        ps = {h.as_of_date for h in hs}
        if len(ps) == 1 and None not in ps:
            cand[k] = next(iter(ps))
    mine: dict[str, list[tuple[Any, ...]]] = defaultdict(list)
    on_acc: dict[str, list[tuple[str, tuple[Any, ...]]]] = defaultdict(list)
    for r in covers:
        c = _digits(r[0])
        if c:
            mine[c].append(r)
            on_acc[r[1]].append((c, r))
    twins: dict[str, tuple[list[str], tuple[Any, ...]]] = {}
    for m, p in cand.items():
        mc, t = _digits(m[4:]), tot[m]
        bound = datetime(p.year, p.month, p.day, tzinfo=UTC) + timedelta(days=1)
        filed = sorted((r for r in mine.get(mc or "", []) if r[2] < bound), key=lambda r: (r[2], r[1], r[6]))
        if not filed:
            continue
        # The latest (filed_at, accession) wins; among M's own rows on that accession the real read returns them by
        # filing_id and ``max`` keeps the first.
        top = next(r for r in filed if (r[2], r[1]) == (filed[-1][2], filed[-1][1]))
        if not _reports(top, t):
            continue
        ks = sorted(
            {k for c, r in on_acc[top[1]] if c != mc and _reports(r, t) for k in nodes.get(c, []) if tot[k] == t}
        )
        if ks:
            twins[m] = (ks, top)
    count = Counter(k for ks, _ in twins.values() for k in ks)
    dups = {m: (ks[0], c) for m, (ks, c) in twins.items() if len(ks) == 1 and count[ks[0]] == 1}
    return {"rows": rows, "tot": tot, "cand": cand, "dups": dups}


def expected_fold(by_category: Mapping[str, Sequence[Any]], o: dict[str, Any]) -> dict[str, list[Any]]:
    repl: dict[int, Any] = {}
    for m, (k, _) in o["dups"].items():
        ranked = sorted(enumerate(o["rows"][k]), key=lambda ih: (-ih[1].shares, ih[1].winning_accession, ih[0]))
        primary = ranked[0][1]
        dropped = list(primary.dropped_sources)
        seen = {(d.filer_cik, d.filer_name, d.source, d.accession_number, d.shares) for d in dropped}
        for h in o["rows"][m]:
            own = rollup_mod.DroppedSource(
                source=h.winning_source,
                accession_number=h.winning_accession,
                shares=h.shares,
                as_of_date=h.as_of_date,
                edgar_url=h.winning_edgar_url,
                filer_cik=h.filer_cik,
                filer_name=h.filer_name,
            )
            for d in (own, *h.dropped_sources):
                dk = (d.filer_cik, d.filer_name, d.source, d.accession_number, d.shares)
                if dk not in seen:
                    seen.add(dk)
                    dropped.append(d)
        repl[id(primary)] = replace(primary, dropped_sources=tuple(dropped))
    return {
        c: [repl.get(id(h), h) for h in hs if c != "institutions" or _key(h) not in o["dups"]]
        for c, hs in by_category.items()
    }


def _total(by_category: Mapping[Any, Sequence[Any]]) -> Decimal:
    return sum((h.shares for hs in by_category.values() for h in hs), Decimal(0))


def _row(h: Any) -> list[str]:
    return [h.winning_source, str(h.shares), h.winning_accession, str(h.as_of_date)]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="/tmp/ab_3227_13f_duplicates.json")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    head = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    base_sha = subprocess.run(["git", "merge-base", "HEAD", "origin/main"], capture_output=True, text=True).stdout
    print(f"HEAD {head}; base (merge-base with origin/main) {base_sha.strip()}")

    cap: dict[str, Any] = {}
    real_fold, real_pass = rollup_mod._fold_13f_duplicates, rollup_mod._reconcile_cofiler_groups
    real_covers, real_cofilers = rollup_mod._read_13f_covers, rollup_mod._read_cofilers

    def timed(name: str, fn: Any) -> Any:
        def wrapper(*a: Any) -> Any:
            t0 = time.perf_counter()
            out = fn(*a)
            cap[f"t_{name}"] = time.perf_counter() - t0
            # The copies are bookkeeping the real rollup does not pay: timed apart and taken off ``elapsed``.
            t1 = time.perf_counter()
            cap[f"{name}_in"], cap[f"{name}_out"] = a, out
            if name in ("fold", "pass"):
                # Copied: the caller appends family holders to the pass output's lists afterwards.
                cap[f"{name}_in"] = ({c: list(hs) for c, hs in a[0].items()}, a[1])
                cap[f"{name}_out"] = ({c: list(hs) for c, hs in out[0].items()}, out[1])
            cap["copy"] = cap.get("copy", 0.0) + time.perf_counter() - t1
            return out

        return wrapper

    rollup_mod._fold_13f_duplicates = timed("fold", real_fold)  # type: ignore[assignment]
    rollup_mod._reconcile_cofiler_groups = timed("pass", real_pass)  # type: ignore[assignment]
    rollup_mod._read_13f_covers = timed("covers", real_covers)  # type: ignore[assignment]
    rollup_mod._read_cofilers = timed("cofilers", real_cofilers)  # type: ignore[assignment]

    st: Counter[str] = Counter()
    fails: list[str] = []
    lat_all: list[float] = []
    lat_hit: list[float] = []
    parts: defaultdict[str, list[float]] = defaultdict(list)
    cases: list[dict[str, Any]] = []
    delta = Decimal(0)
    with psycopg.connect(settings.database_url) as conn, snapshot_read(conn):
        conn.execute("SET TIME ZONE 'UTC'")
        insts = conn.execute(
            """SELECT i.instrument_id, i.symbol FROM instruments i WHERE i.instrument_id IN (
                 SELECT instrument_id FROM ownership_insiders_current WHERE shares > 0 UNION
                 SELECT instrument_id FROM ownership_blockholders_current WHERE aggregate_amount_owned > 0 UNION
                 SELECT instrument_id FROM ownership_institutions_current WHERE shares > 0) ORDER BY 1"""
        ).fetchall()
        if args.limit:
            insts = insts[: args.limit]
        st["selected"] = len(insts)
        for n, (iid, symbol) in enumerate(insts):
            if n % 1000 == 0:
                print("progress", n, len(insts), flush=True)
            cap.clear()
            try:
                t0 = time.perf_counter()
                r = rollup_mod.get_ownership_rollup(conn, symbol, iid)
                elapsed = time.perf_counter() - t0 - cap.get("copy", 0.0)
            except Exception as exc:  # noqa: BLE001 — counted and gated at 0
                st["error"] += 1
                fails.append(f"{symbol}: {exc!r}")
                continue
            if "fold_in" not in cap:
                st[f"no_data: {r.no_data_reason}"] += 1
                if r.no_data_reason is None:
                    fails.append(f"{symbol}: did not reach the pass and has no no_data_reason")
                continue
            st["reached"] += 1
            cost = sum(cap.get(f"t_{x}", 0.0) for x in ("fold", "pass", "covers", "cofilers"))
            share = cost / max(elapsed - cost, 1e-9)
            lat_all.append(share)
            for x in ("fold", "pass", "covers", "cofilers"):
                parts[x].append(cap.get(f"t_{x}", 0.0) / max(elapsed - cost, 1e-9))
            (fold_in, _), (fold_out, f13) = cap["fold_in"], cap["fold_out"]
            (_, cofilers), (pass_out, cof_corr) = cap["pass_in"], cap["pass_out"]
            try:
                st["inv: s16/13DG rows with 13f"] += sum(
                    1 for c in ("insiders", "blockholders") for h in fold_in.get(c, []) if h.winning_source == "13f"
                )
                src: defaultdict[str, set[str]] = defaultdict(set)
                for h in fold_in.get("institutions", []):
                    src[_key(h)].add("13f" if h.winning_source == "13f" else "other")
                st["inv: institutions mixed 13f"] += sum(1 for v in src.values() if len(v) > 1)
                accs = sorted(
                    {h.winning_accession.strip() for c in _CATS for h in fold_in.get(c, []) if h.shares > 0} - {""}
                )
                cof = {a: frozenset(c) for a, c in conn.execute(_COFILERS_ORACLE_SQL, {"a": accs})} if accs else {}
                if cof != dict(cofilers):
                    fails.append(f"{symbol}: oracle co-filers != real")
                covers = conn.execute(_COVERS_SQL, {"iid": iid}).fetchall()
                o = oracle(fold_in, covers)
                st["candidates"] += len(o["cand"])
                real_gone = {_key(h) for h in fold_in.get("institutions", [])} - {
                    _key(h) for h in fold_out.get("institutions", [])
                }
                if real_gone != set(o["dups"]):
                    fails.append(f"{symbol}: real duplicates {sorted(real_gone)} != oracle {sorted(o['dups'])}")
                want = sorted(
                    (
                        min(enumerate(o["rows"][k]), key=lambda ih: (-ih[1].shares, ih[1].winning_accession, ih[0]))[
                            1
                        ].filer_cik,
                        o["tot"][m],
                    )
                    for m, (k, _) in o["dups"].items()
                )
                if sorted((c.filer_cik, c.shares_removed) for c in f13) != want:
                    fails.append(f"{symbol}: fold corrections != oracle")
                if expected_fold(fold_in, o) != fold_out:
                    fails.append(f"{symbol}: fold output rows != oracle expected rows")
                base_out, base_corr = real_pass(fold_in, cofilers)
                sum_t = sum((o["tot"][m] for m in o["dups"]), Decimal(0))
                d = _total(pass_out) - _total(base_out)
                if d != -sum_t:
                    fails.append(f"{symbol}: treated − base {d} != −Σ T {-sum_t}")
                if _total(fold_in) - _total(pass_out) != sum((c.shares_removed for c in (*f13, *cof_corr)), Decimal(0)):
                    fails.append(f"{symbol}: input − output != Σ shares_removed")
                g0, g1 = rollup_mod._cofiler_graph(fold_in, cofilers), rollup_mod._cofiler_graph(fold_out, cofilers)
                if g0.edges != g1.edges or g0.cliques != g1.cliques or g0.chains != g1.chains:
                    fails.append(f"{symbol}: #3898 edges or collapse list changed")
                if [(c.filer_cik, c.shares_removed) for c in base_corr] != [
                    (c.filer_cik, c.shares_removed) for c in cof_corr
                ]:
                    fails.append(f"{symbol}: #3898 corrections changed")
            except Exception as exc:  # noqa: BLE001 — counted and gated at 0
                st["error: oracle"] += 1
                fails.append(f"{symbol}: oracle {exc!r}")
                continue
            if symbol in _NAMED and d != _NAMED[symbol]:
                fails.append(f"{symbol}: named figure {d} != {_NAMED[symbol]}")
            if symbol in _NAMED or symbol in _PANEL:
                pie_ = sum((s.total_shares for s in r.slices if s.denominator_basis == "pie_wedge"), Decimal(0))
                print(f"named {symbol}: pie {pie_:,} (base {pie_ - d:,}), fold {d:,}")
            if not o["dups"]:
                continue
            if r.no_data_reason is not None:
                st[f"changed, then no_data: {r.no_data_reason}"] += 1
                continue
            st["changed"] += 1
            st["duplicates"] += len(o["dups"])
            lat_hit.append(share)
            delta += d
            pie = sum((s.total_shares for s in r.slices if s.denominator_basis == "pie_wedge"), Decimal(0))
            base_pie, out_ = pie - d, r.shares_outstanding
            if out_:
                st["over in main"] += base_pie > out_
                st["fixed"] += base_pie > out_ >= pie
                if pie > out_ >= base_pie:
                    fails.append(f"{symbol}: newly oversubscribed")
            fam = {
                (m.filer_cik or "").strip().lstrip("0") for s in r.slices for h in s.holders for m in h.family_members
            } - {""}
            in_clique = {k for m in g0.cliques for k in m}
            for m, (k, c) in o["dups"].items():
                if _key_canon(m) in fam or _key_canon(k) in fam:
                    fails.append(f"{symbol}: a duplicate's k or M is a family-member CIK")
                cases.append(
                    {
                        "symbol": symbol,
                        "M": [o["rows"][m][0].filer_name, m],
                        "k": [o["rows"][k][0].filer_name, k, "in #3898 clique" if k in in_clique else "singleton"],
                        "T": str(o["tot"][m]),
                        "P": str(o["cand"][m]),
                        "C": [c[1], str(c[2])],
                        "C_M": [str(c[3]), str(c[4]), str(c[5])],
                        "M_rows": [_row(h) for h in o["rows"][m]],
                        "k_rows": [_row(h) for h in o["rows"][k]],
                        "pie": str(pie),
                        "outstanding": str(out_),
                    }
                )
    accounted = st["reached"] + st["error"] + sum(v for k, v in st.items() if k.startswith("no_data"))
    if accounted != st["selected"]:
        fails.append(f"population: {accounted} accounted != {st['selected']} selected")
    for gate in ("inv: s16/13DG rows with 13f", "inv: institutions mixed 13f", "error: oracle"):
        if st[gate]:
            fails.append(f"{gate} = {st[gate]}")
    for name, xs in (("all", lat_all), ("affected", lat_hit)):
        if xs:
            p95 = statistics.quantiles(xs, n=20)[-1] if len(xs) >= 20 else max(xs)
            print(f"cost share {name}: n {len(xs)} p50 {statistics.median(xs):.2%} p95 {p95:.2%} max {max(xs):.2%}")
            if p95 > 0.10:
                fails.append(f"latency {name}: p95 cost share {p95:.1%} > 10%")
    for x, xs in parts.items():
        if len(xs) >= 20:
            print(f"cost share part {x}: p50 {statistics.median(xs):.2%} p95 {statistics.quantiles(xs, n=20)[-1]:.2%}")
    with open(args.out, "w") as fh:
        json.dump({"stats": st, "delta": str(delta), "cases": cases, "failures": fails}, fh, indent=1, default=str)
    for k, v in sorted(st.items()):
        print(f"{k}: {v}")
    print(f"delta {delta:,} (spec census: 19 instruments, -346,612,691, 3 fixed); written {args.out}")
    for c in sorted(cases, key=lambda c: -Decimal(c["T"])):
        print("CASE", json.dumps(c))
    for f in fails[:40]:
        print("FAIL", f)
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
