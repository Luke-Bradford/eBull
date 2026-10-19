"""Full-population A/B for the #3227 widened co-filer links (read-only).

Spec: docs/specs/ownership/2026-10-19-3227-cofiler-links-widened.md (Acceptance).

Builds every rollup on THIS checkout inside one REPEATABLE READ snapshot, with ``_reconcile_cofiler_groups``
wrapped so its real input and output are recorded. An independent ORACLE (this file's own copy of the rules)
re-reads co-filers itself over all three categories' winning accessions and derives both arms:

  * base  = #3895's rule: ``insiders``/``blockholders`` nodes without a ``def14a`` row; an edge only between
    the owner of a row on the accession and another owner it names; cliques collapse at MAX;
  * new   = the spec's Changes 1-3: ``institutions`` nodes too, no node with a ``def14a`` or ``13f`` row; every
    pair a node's own joint accession names links; a non-clique component falls back to base's cliques in it.

Gates (non-zero exit on any failure):
  * population: selected = reached the pass + no_data (by reason) + errors, errors = 0;
  * the real pass's collapses equal the oracle's new cliques member for member, with SUM − MAX each;
  * the real output equals the oracle's expected rows, the keeper's largest row's ``dropped_sources`` included;
  * per instrument: new pie ≤ base pie; newly oversubscribed = 0; input − output = Σ ``shares_removed``;
  * input invariants: no ``insiders``/``blockholders`` row with a ``13f`` source; no ``institutions`` identity
    with a ``13f`` row and another row;
  * family: no co-filer collapse holds a member CIK of an appended family holder (reported: every pass-input
    identity that does — a family member's Form 3/4 row is a person-side survivor of the family pre-pass);
  * treatment quality on the new cliques (``scripts.ab_3227_cofiler_groups.candidate_status``): = SUM ≤ 1,
    between-nearer-SUM ≤ 1, ≥ 4 in range;
  * latency: (co-filer read + pass) / (the rest of ``get_ownership_rollup``) per instrument, p95 ≤ 10%, on the
    full population and on the affected instruments. Paired base/new wall time was tried first: on a shared dev
    DB a single paired run measured p50 0.4% but p95 20.1% (253 instruments), i.e. noise, so it is not the gate.

Usage: ``PYTHONPATH=. uv run python -m scripts.ab_3227_cofiler_links [--out PATH] [--limit N]``
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import replace
from decimal import Decimal
from typing import Any

import psycopg

import app.services.ownership_rollup as rollup_mod
from app.config import settings
from app.db.snapshot import snapshot_read
from scripts.ab_3227_cofiler_groups import candidate_status

_CATS = ("insiders", "blockholders", "institutions")
_BASE_CATS = ("insiders", "blockholders")
# The oracle's own read of the reporting owners, independent of ``_read_cofilers``.
_ORACLE_SQL = """
    SELECT accession, array_agg(DISTINCT cik)
      FROM (SELECT accession_number AS accession, nullif(ltrim(btrim(filer_cik), '0'), '') AS cik
              FROM insider_filers WHERE accession_number = ANY(%(a)s::text[])
            UNION ALL
            SELECT accession_number, nullif(ltrim(btrim(reporter_cik), '0'), '')
              FROM blockholder_filings WHERE accession_number = ANY(%(a)s::text[])) x
     WHERE cik IS NOT NULL GROUP BY accession HAVING count(DISTINCT cik) >= 2
"""
_NAMED = frozenset({"DRVN", "BBAI", "HSIC", "RXT", "TMUS", "AAPL", "GME", "MSFT", "JPM", "HD"})


def _key(h: Any) -> str:
    return rollup_mod._identity_key(h.filer_cik, h.filer_name)


def _canon(k: str) -> str | None:
    return ((k[4:].strip().lstrip("0")) or None) if k.startswith("CIK:") else None


def oracle(by_category: Mapping[str, Sequence[Any]], cof: Mapping[str, frozenset[str]]) -> dict[str, Any]:
    """Both arms' collapses from the spec's rules, written independently of ``_cofiler_graph``."""
    rows: dict[str, list[Any]] = defaultdict(list)
    cat: dict[str, str] = {}
    for c in _CATS:
        for h in by_category.get(c, []):
            rows[_key(h)].append(h)
            cat[_key(h)] = c
    tot = {k: sum((h.shares for h in hs), Decimal(0)) for k, hs in rows.items()}

    def ok(k: str, excluded: set[str]) -> bool:
        return tot[k] > 0 and _canon(k) is not None and not any(h.winning_source in excluded for h in rows[k])

    base = [k for k in rows if cat[k] in _BASE_CATS and ok(k, {"def14a"})]
    new = [k for k in rows if ok(k, {"def14a", "13f"})]

    def edges(nodes: list[str], widened: bool) -> set[frozenset[str]]:
        by_c: dict[str, list[str]] = defaultdict(list)
        for k in nodes:
            by_c[_canon(k) or ""].append(k)
        out: set[frozenset[str]] = set()
        for k in nodes:
            for h in rows[k]:
                ciks = cof.get(h.winning_accession.strip()) if h.shares > 0 and h.winning_accession.strip() else None
                if not ciks or _canon(k) not in ciks:
                    continue
                named = {o for c in ciks for o in by_c.get(c, [])}
                if widened:
                    out |= {frozenset((a, b)) for a in named for b in named if a != b}
                else:
                    out |= {frozenset((k, o)) for o in named if o != k}
        return out

    def comps(nodes: list[str], e: set[frozenset[str]]) -> list[frozenset[str]]:
        seen: set[str] = set()
        adj: dict[str, set[str]] = defaultdict(set)
        for p in e:
            a, b = tuple(p)
            adj[a].add(b)
            adj[b].add(a)
        out = []
        for k in nodes:
            if k in seen:
                continue
            stack, comp = [k], set()
            while stack:
                x = stack.pop()
                if x not in comp:
                    comp.add(x)
                    stack.extend(adj[x] - comp)
            seen |= comp
            if len(comp) > 1:
                out.append(frozenset(comp))
        return out

    def clique(m: frozenset[str], e: set[frozenset[str]]) -> bool:
        return all(frozenset((a, b)) in e for a in m for b in m if a != b)

    be = edges(base, False)
    base_cl = [m for m in comps(base, be) if clique(m, be)]
    if not set(base) <= set(new):
        return {"rows": rows, "tot": tot, "cat": cat, "base": base_cl, "new": base_cl, "fallback_all": True}
    ne = edges(new, True)
    new_cl: list[frozenset[str]] = []
    for m in comps(new, ne):
        new_cl += [m] if clique(m, ne) else [b for b in base_cl if b <= m]
    return {"rows": rows, "tot": tot, "cat": cat, "base": base_cl, "new": new_cl, "fallback_all": False}


def keeper(m: frozenset[str], o: dict[str, Any]) -> str:
    return min(m, key=lambda k: (-o["tot"][k], o["cat"][k] != "insiders", int(_canon(k) or 0), k))


def removed(cliques: list[frozenset[str]], o: dict[str, Any]) -> Decimal:
    return sum((sum((o["tot"][k] for k in m), Decimal(0)) - o["tot"][keeper(m, o)] for m in cliques), Decimal(0))


def expected_output(by_category: Mapping[str, Sequence[Any]], o: dict[str, Any]) -> dict[str, list[Any]]:
    gone: set[str] = set()
    repl: dict[int, Any] = {}
    for m in o["new"]:
        kp = keeper(m, o)
        primary = max(o["rows"][kp], key=lambda h: h.shares)
        dropped = list(primary.dropped_sources)
        seen = {(d.filer_cik, d.filer_name, d.source, d.accession_number, d.shares) for d in dropped}
        for k in sorted(m - {kp}, key=lambda k: (-o["tot"][k], o["cat"][k] != "insiders", int(_canon(k) or 0), k)):
            gone.add(k)
            for h in o["rows"][k]:
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
        c: [repl.get(id(h), h) for h in hs if c not in _CATS or _key(h) not in gone] for c, hs in by_category.items()
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="/tmp/ab_3227_cofiler_links.json")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    cap: dict[str, Any] = {}
    real_pass, real_graph = rollup_mod._reconcile_cofiler_groups, rollup_mod._cofiler_graph

    def pass_wrapper(by_category: Any, cofilers: Any) -> Any:
        t0 = time.perf_counter()
        out = real_pass(by_category, cofilers)
        cap["t"] = time.perf_counter() - t0
        cap["input"] = {c: list(hs) for c, hs in by_category.items()}
        cap["output"] = {c: list(hs) for c, hs in out[0].items()}
        cap["cofilers"] = cofilers
        return out

    rollup_mod._reconcile_cofiler_groups = pass_wrapper  # type: ignore[assignment]

    real_read = rollup_mod._read_cofilers

    def read_wrapper(conn: Any, accessions: Any) -> Any:
        t0 = time.perf_counter()
        out = real_read(conn, accessions)
        cap["t_read"] = time.perf_counter() - t0
        return out

    rollup_mod._read_cofilers = read_wrapper  # type: ignore[assignment]

    st: Counter[str] = Counter()
    sh: defaultdict[str, Decimal] = defaultdict(Decimal)
    qual: Counter[str] = Counter()
    fails: list[str] = []
    lat_all: list[float] = []
    lat_hit: list[float] = []
    cases: list[dict[str, Any]] = []
    with psycopg.connect(settings.database_url) as conn, snapshot_read(conn):
        proxies: dict[int, list[tuple]] = {}
        for iid, doc, key, nm, s, pe, fa in conn.execute(
            """SELECT instrument_id, source_document_id, holder_name_key, holder_name, shares, period_end, filed_at
                 FROM ownership_def14a_current WHERE shares IS NOT NULL"""
        ):
            proxies.setdefault(iid, []).append(((doc, key, s), nm, s, pe or (fa.date() if fa else None)))
        insts = conn.execute(
            """SELECT i.instrument_id, i.symbol FROM instruments i WHERE i.instrument_id IN (
                 SELECT instrument_id FROM ownership_insiders_current WHERE shares > 0 UNION
                 SELECT instrument_id FROM ownership_blockholders_current WHERE aggregate_amount_owned > 0 UNION
                 SELECT instrument_id FROM ownership_institutions_current WHERE shares > 0) ORDER BY 1"""
        ).fetchall()
        if args.limit:
            insts = insts[: args.limit]
        st["selected"] = len(insts)
        for iid, symbol in insts:
            cap.clear()
            try:
                t0 = time.perf_counter()
                r = rollup_mod.get_ownership_rollup(conn, symbol, iid)
                elapsed = time.perf_counter() - t0
            except Exception as exc:  # noqa: BLE001 — counted and gated at 0
                st["error"] += 1
                fails.append(f"{symbol}: {exc!r}")
                continue
            if "input" not in cap:
                st[f"no_data: {r.no_data_reason}"] += 1
                if r.no_data_reason is None:
                    fails.append(f"{symbol}: did not reach the pass and has no no_data_reason")
                continue
            st["reached"] += 1
            # The capture's list copies are bookkeeping the real rollup does not pay; they sit inside
            # the wrapper's timed region only as a few list() calls, which this share includes.
            cost = cap["t"] + cap.get("t_read", 0.0)
            share = cost / max(elapsed - cost, 1e-9)
            lat_all.append(share)
            by_in, by_out, corr = (
                cap["input"],
                cap["output"],
                [c for c in r.corrections_applied if c.kind == "cofiler_group_collapse"],
            )
            st["inv: s16/13DG rows with 13f"] += sum(
                1 for c in _BASE_CATS for h in by_in.get(c, []) if h.winning_source == "13f"
            )
            inst_src: defaultdict[str, set[str]] = defaultdict(set)
            for h in by_in.get("institutions", []):
                inst_src[_key(h)].add("13f" if h.winning_source == "13f" else "other")
            st["inv: institutions mixed 13f"] += sum(1 for v in inst_src.values() if len(v) > 1)
            accs = sorted({h.winning_accession.strip() for c in _CATS for h in by_in.get(c, []) if h.shares > 0} - {""})
            cof = {a: frozenset(c) for a, c in conn.execute(_ORACLE_SQL, {"a": accs}).fetchall()} if accs else {}
            o = oracle(by_in, cof)
            st["oracle fallback_all"] += o["fallback_all"]
            real = {frozenset(m) for m in real_graph(by_in, cap["cofilers"]).cliques}
            if real != set(o["new"]):
                fails.append(f"{symbol}: real collapses != oracle")
            if expected_output(by_in, o) != by_out:
                fails.append(f"{symbol}: output rows != oracle expected rows")
            if r.no_data_reason is not None:
                # The pass ran, then the rollup returned a no-pie payload (``partial_class_denominator``
                # is decided after the pass), which carries no corrections or slices. The pass-level
                # checks above still ran; there is no pie to compare.
                st[f"reached, then no_data: {r.no_data_reason}"] += 1
                continue
            if len(corr) != len(o["new"]):
                fails.append(f"{symbol}: {len(corr)} corrections, oracle {len(o['new'])} cliques")
            want = sorted(sum((o["tot"][k] for k in m), Decimal(0)) - o["tot"][keeper(m, o)] for m in o["new"])
            if sorted(c.shares_removed for c in corr) != want:
                fails.append(f"{symbol}: shares_removed != oracle SUM - MAX")
            rm_new, rm_base = removed(o["new"], o), removed(o["base"], o)
            t_in = sum((h.shares for hs in by_in.values() for h in hs), Decimal(0))
            t_out = sum((h.shares for hs in by_out.values() for h in hs), Decimal(0))
            if t_in - t_out != sum((c.shares_removed for c in corr), Decimal(0)):
                fails.append(f"{symbol}: input - output != sum shares_removed")
            pie = sum((s.total_shares for s in r.slices if s.denominator_basis == "pie_wedge"), Decimal(0))
            base_pie = pie + rm_new - rm_base
            fam = {
                (m.filer_cik or "").strip().lstrip("0") for s in r.slices for h in s.holders for m in h.family_members
            } - {""}
            in_fam = {k for k in o["rows"] if _canon(k) in fam}
            st["pass-input identities with a family-member CIK"] += len(in_fam)
            for k in in_fam:
                st[f"family-member CIK in pass input: {o['cat'][k]} ({o['rows'][k][0].winning_source})"] += 1
            if any(m & in_fam for m in o["new"]):
                fails.append(f"{symbol}: a co-filer collapse holds a family-member CIK")
            if symbol in _NAMED:
                print(f"named {symbol}: pie {pie:,} base {base_pie:,} cliques {[sorted(m) for m in o['new']]}")
            if set(o["new"]) == set(o["base"]):
                continue
            st["changed"] += 1
            lat_hit.append(share)
            d = pie - base_pie
            sh["delta"] += d
            if d > 0:
                fails.append(f"{symbol}: pie up {d}")
            out_ = r.shares_outstanding
            if out_:
                st["changed with outstanding"] += 1
                st["fixed"] += base_pie > out_ >= pie
                if pie > out_ >= base_pie:
                    fails.append(f"{symbol}: newly oversubscribed")
            for m in set(o["new"]) - set(o["base"]):
                st["new cliques"] += 1
                kp = keeper(m, o)
                members = [(k, o["rows"][k], o["tot"][k]) for k in [kp, *sorted(m - {kp})]]
                status, proxy = candidate_status(members, out_, proxies.get(iid, []))
                stratum = "inst" if any(o["cat"][k] == "institutions" for k in m) else "s16/13DG"
                qual[f"{stratum} {status}"] += 1
                if not status.startswith("nd"):
                    cases.append(
                        {
                            "symbol": symbol,
                            "status": status,
                            "proxy": [str(x) for x in proxy or ()],
                            "members": [(k, o["cat"][k], str(o["tot"][k])) for k in sorted(m)],
                        }
                    )
                for k in m - {kp}:
                    for h in o["rows"][k]:
                        sh[f"removed {o['cat'][k]} ({h.winning_source})"] += h.shares
            for m in set(o["base"]) - set(o["new"]):
                kp = keeper(m, o)
                for k in m - {kp}:
                    for h in o["rows"][k]:
                        sh[f"removed {o['cat'][k]} ({h.winning_source})"] -= h.shares
    reached_sum = st["reached"] + st["error"] + sum(v for k, v in st.items() if k.startswith("no_data"))
    if reached_sum != st["selected"]:
        fails.append(f"population: {reached_sum} accounted != {st['selected']} selected")
    for inv in ("inv: s16/13DG rows with 13f", "inv: institutions mixed 13f"):
        if st[inv]:
            fails.append(f"{inv} = {st[inv]}")
    in_range = sum(
        v
        for k, v in qual.items()
        if k.split(" ", 1)[1] in ("=MAX", "=SUM", "ambiguous") or k.split(" ", 1)[1].startswith("between")
    )
    if in_range < 4:
        fails.append(f"quality: {in_range} in range < 4")
    if sum(v for k, v in qual.items() if k.endswith(" =SUM")) > 1:
        fails.append("quality: =SUM > 1")
    if sum(v for k, v in qual.items() if k.endswith("between_nearer_SUM")) > 1:
        fails.append("quality: between_nearer_SUM > 1")
    for name, xs in (("all", lat_all), ("affected", lat_hit)):
        if xs:
            p95 = statistics.quantiles(xs, n=20)[-1] if len(xs) >= 20 else max(xs)
            print(f"cost share {name}: n {len(xs)} p50 {statistics.median(xs):.2%} p95 {p95:.2%} max {max(xs):.2%}")
            if p95 > 0.10:
                fails.append(f"latency {name}: p95 cost share {p95:.1%} > 10%")
    with open(args.out, "w") as fh:
        json.dump(
            {
                "stats": st,
                "shares": {k: str(v) for k, v in sh.items()},
                "quality": qual,
                "cases": cases,
                "failures": fails,
            },
            fh,
            indent=1,
            default=str,
        )
    for k, v in sorted(st.items()):
        print(f"{k}: {v}")
    for k, v in sorted(sh.items()):
        print(f"shares {k}: {v:,}")
    for k, v in sorted(qual.items()):
        print(f"quality {k}: {v}")
    print(f"in range: {in_range}; written {args.out}")
    for f in fails[:40]:
        print("FAIL", f)
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
