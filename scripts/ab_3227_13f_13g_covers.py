"""Full-population A/B for the #3227 13F-duplicate fold's 13G extension (read-only).

Spec: docs/specs/ownership/2026-10-22-3227-13f-13g-covers.md (Acceptance).

Builds every rollup on THIS checkout inside one REPEATABLE READ snapshot (session time zone UTC), with the fold,
the cover reads, the co-filer read and the co-filer pass wrapped so their real inputs, outputs and costs are
recorded. The BASE arm is the real code at a pinned SHA (``--base``, default the merge-base with origin/main),
loaded from git as its own module: its ``_fold_13f_duplicates`` (the 13D arm only) runs on the fold's real input,
then the real co-filer pass. An independent ORACLE, this file's own frozen copy of the spec's name sources,
filer-of-record read and 13G rule, re-reads covers and observations per instrument (unbounded) and re-derives the
13D arm's matched set for reservations.

Gates (non-zero exit on any failure):
  combined pass:
  * the treated 13D-arm corrections equal the base's, member for member (field by field);
  * every treated correction is either a base correction or carries the "joint Schedule 13G" wording;
  increment:
  * the oracle's 13G duplicate set equals the treated pass's 13G corrections, member for member (M removed, k's
    CIK, T), and the treated fold output equals the base fold output with the oracle's folds applied;
  * per instrument: treated pie = base pie − Σ T over 13G duplicates only;
  * #3898's edges, cliques, chains and corrections unchanged;
  * newly oversubscribed = 0; input − output = Σ ``shares_removed`` over all corrections, both arms;
  * population accounted; base, treated and oracle errors each 0;
  * latency: (co-filer read + both cover reads + fold + co-filer pass) / (the rest of ``get_ownership_rollup``),
    p95 ≤ 10%, on the full population and on the affected instruments.

Reported, not gated: the census reconciliation (spec: 17 duplicates on 16 instruments, −235,291,509, 3 fixed) and
the case review (M, k, T, C, filer of record, P, route, the name-source row, every kept row of k and M, C's blocks).

Usage: ``PYTHONPATH=. uv run python -m scripts.ab_3227_13f_13g_covers [--base SHA] [--out PATH] [--limit N]``
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import re
import statistics
import subprocess
import sys
import time
import types
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import psycopg

import app.services.ownership_rollup as rollup_mod
from app.config import settings
from app.db.snapshot import snapshot_read
from scripts.ab_3227_13f_duplicates import _CATS, _digits, _key, _key_canon, _row, _total

_COVERS_SQL = """
    SELECT reporter_cik, accession_number, filed_at, sole_dispositive_power, shared_dispositive_power,
           aggregate_amount_owned, filing_id, reporter_name, submission_type
      FROM blockholder_filings WHERE instrument_id = %(iid)s AND filed_at IS NOT NULL
"""
_OBS_SQL = """
    SELECT source_accession, reporter_cik FROM ownership_blockholders_observations
     WHERE instrument_id = %(iid)s AND source = '13g' AND known_to IS NULL
"""
_13G = ("SCHEDULE 13G", "SCHEDULE 13G/A")
_NON_ALNUM = re.compile(r"[^A-Z0-9]+")
_NAMED = {"SARO": Decimal(-84_587_035), "RXT": Decimal(0)}
_PANEL = ("AAPL", "GME", "MSFT", "JPM", "HD")


def _name(s: str | None) -> str:
    """Frozen copy of the spec's name rule: case, punctuation and '&' folded; suffixes kept."""
    return _NON_ALNUM.sub(" ", (s or "").upper().replace("&", " AND ")).strip()


def _full(row: tuple[Any, ...], t: Decimal) -> bool:
    sole, shared, agg = row[3], row[4], row[5]
    return sole is not None and shared is not None and sole + shared == t and agg == t


def _bound(p: date) -> datetime:
    return datetime(p.year, p.month, p.day, tzinfo=UTC) + timedelta(days=1)


def oracle(
    by_category: Mapping[str, Sequence[Any]], covers: list[tuple[Any, ...]], obs: list[tuple[str, str | None]]
) -> dict[str, Any]:
    """The 13G-covers spec's rule, written independently of ``_fold_13f_duplicates``."""
    rows: dict[str, list[Any]] = defaultdict(list)
    cat: dict[str, str] = {}
    for c, hs in by_category.items():
        for h in hs:
            cat[_key(h)] = c
            if c in _CATS:
                rows[_key(h)].append(h)
    tot = {k: sum((h.shares for h in hs), Decimal(0)) for k, hs in rows.items()}
    nodes = [
        k
        for k, hs in rows.items()
        if tot[k] > 0 and _key_canon(k) and not any(h.winning_source in ("def14a", "13f") for h in hs)
    ]
    cand: dict[str, date] = {}
    for k, hs in rows.items():
        if cat[k] != "institutions" or _digits(k[4:] if k.startswith("CIK:") else None) is None or tot[k] <= 0:
            continue
        ps = {h.as_of_date for h in hs}
        if all(h.winning_source == "13f" for h in hs) and len(ps) == 1 and None not in ps:
            cand[k] = next(iter(ps))

    # The 13D arm's matched set (cover reports T and has a twin, before uniqueness): its twins are reserved.
    mine: dict[str, list[tuple[Any, ...]]] = defaultdict(list)
    on_acc: dict[str, list[tuple[str, tuple[Any, ...]]]] = defaultdict(list)
    for r in covers:
        c = _digits(r[0])
        if c:
            mine[c].append(r)
            on_acc[r[1]].append((c, r))
    matched_13d: dict[str, list[str]] = {}
    for m, p in cand.items():
        mc, t = _digits(m[4:]), tot[m]
        filed = sorted((r for r in mine.get(mc or "", []) if r[2] < _bound(p)), key=lambda r: (r[2], r[1], r[6]))
        if not filed:
            continue
        top = next(r for r in filed if (r[2], r[1]) == (filed[-1][2], filed[-1][1]))
        if not _full(top, t):
            continue
        ks = sorted(
            {
                k
                for c, r in on_acc[top[1]]
                if c != mc and _full(r, t)
                for k in nodes
                if _key_canon(k) == c and tot[k] == t
            }
        )
        if ks:
            matched_13d[m] = ks
    reserved = {k for ks in matched_13d.values() for k in ks}

    # 13G accessions, their blocks and their filer of record.
    blocks: dict[str, list[tuple[Any, ...]]] = defaultdict(list)
    for r in covers:
        if r[8] in _13G:
            blocks[r[1]].append(r)
    seen: dict[str, list[str | None]] = defaultdict(list)
    for acc, cik in obs:
        seen[acc].append(cik)
    filer: dict[str, str | None] = {}
    for acc, bs in blocks.items():
        vals = [_digits(v) for v in seen.get(acc, [])]
        ok = all(b[0] is None for b in bs) and vals and None not in vals and len(set(vals)) == 1
        filer[acc] = vals[0] if ok else None
    names_on = {acc: {_name(b[7]) for b in bs} - {""} for acc, bs in blocks.items()}

    def names(k: str, sources: tuple[str, ...]) -> set[str]:
        return {_name(h.filer_name) for h in rows[k] if h.winning_source in sources} - {""}

    found: dict[str, tuple[list[str], dict[str, Any]]] = {}
    selected: dict[str, str] = {}  # candidate → its C when C is a 13G (uniform or not), for tier (O)
    for m, p in cand.items():
        if m in matched_13d:
            continue
        mc, t, b = _digits(m[4:]), tot[m], _bound(p)
        stm = [(r[2], r[1], "cik") for r in mine.get(mc or "", []) if r[2] < b]
        mn = names(m, ("13f",))
        stm += [
            (r[2], acc, "13g")
            for acc, bs in blocks.items()
            if filer[acc] == mc or mn & names_on[acc]
            for r in bs
            if r[2] < b
        ]
        if not stm:
            continue
        latest = max(stm)
        if any(s[:2] == latest[:2] and s[2] == "cik" for s in stm):
            continue
        acc = latest[1]
        selected[m] = acc
        if not all(_full(r, t) for r in blocks[acc]):
            continue
        if filer[acc] is not None and filer[acc] == mc:
            route = "M filer, k name"
            ks = sorted(
                k for k in nodes if _key_canon(k) != mc and tot[k] == t and names(k, ("form3", "form4")) & names_on[acc]
            )
        else:
            route = "k filer, M name"
            ks = sorted(k for k in nodes if filer[acc] is not None and _key_canon(k) == filer[acc] and tot[k] == t)
        if ks:
            found[m] = (ks, {"C": acc, "filed": str(latest[0]), "filer": filer[acc], "route": route, "P": str(p)})
    count = Counter(k for ks, _ in found.values() for k in ks)
    dups = {
        m: (ks[0], ev) for m, (ks, ev) in found.items() if len(ks) == 1 and count[ks[0]] == 1 and ks[0] not in reserved
    }
    return {
        "rows": rows,
        "tot": tot,
        "cand": cand,
        "dups": dups,
        "nodes": nodes,
        "matched_13d": matched_13d,
        "found": found,
        "selected": selected,
        "filer": filer,
        "blocks": blocks,
        "names_on": names_on,
        "names": names,
    }


def apply_oracle(base_out: Mapping[str, Sequence[Any]], o: dict[str, Any]) -> dict[str, list[Any]]:
    repl: dict[int, Any] = {}
    for m, (k, _) in o["dups"].items():
        primary = min(enumerate(o["rows"][k]), key=lambda ih: (-ih[1].shares, ih[1].winning_accession, ih[0]))[1]
        dropped = list(primary.dropped_sources)
        seen = {(d.filer_cik, d.filer_name, d.source, d.accession_number, d.shares) for d in dropped}
        for h in o["rows"][m]:
            own = rollup_mod.DroppedSource(
                h.winning_source,
                h.winning_accession,
                h.shares,
                h.as_of_date,
                h.winning_edgar_url,
                h.filer_cik,
                h.filer_name,
            )
            for d in (own, *h.dropped_sources):
                dk = (d.filer_cik, d.filer_name, d.source, d.accession_number, d.shares)
                if dk not in seen:
                    seen.add(dk)
                    dropped.append(d)
        repl[id(primary)] = dataclasses.replace(primary, dropped_sources=tuple(dropped))
    return {
        c: [repl.get(id(h), h) for h in hs if c != "institutions" or _key(h) not in o["dups"]]
        for c, hs in base_out.items()
    }


def _plain(x: Any) -> Any:
    """Class-blind comparison form: the base module defines its own dataclasses."""
    if dataclasses.is_dataclass(x) and not isinstance(x, type):
        return tuple(_plain(getattr(x, f.name)) for f in dataclasses.fields(x))
    if isinstance(x, (list, tuple)):
        return tuple(_plain(v) for v in x)
    if isinstance(x, dict):
        return {k: _plain(v) for k, v in x.items()}
    return x


def _load_base(sha: str) -> types.ModuleType:
    src = subprocess.run(
        ["git", "show", f"{sha}:app/services/ownership_rollup.py"], capture_output=True, text=True, check=True
    ).stdout
    mod = types.ModuleType("_ab_base_ownership_rollup")
    path = f"<{sha}>/app/services/ownership_rollup.py"
    mod.__dict__["__file__"] = path
    sys.modules[mod.__name__] = mod  # dataclasses resolve annotations through sys.modules
    exec(compile(src, path, "exec"), mod.__dict__)
    return mod


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="")
    ap.add_argument("--out", default="/tmp/ab_3227_13f_13g_covers.json")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    head = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    base_sha = (
        args.base
        or subprocess.run(["git", "merge-base", "HEAD", "origin/main"], capture_output=True, text=True).stdout.strip()
    )
    print(f"HEAD {head}; base {base_sha}")
    base = _load_base(base_sha)
    if "_13G_SUBMISSION_TYPES" in base.__dict__:
        print("FAIL base already carries the 13G extension")
        return 1

    cap: dict[str, Any] = {}
    real_fold, real_pass = rollup_mod._fold_13f_duplicates, rollup_mod._reconcile_cofiler_groups
    reads = {n: getattr(rollup_mod, n) for n in ("_read_13f_covers", "_read_13g_filers", "_read_cofilers")}

    def timed(name: str, fn: Any) -> Any:
        def wrapper(*a: Any) -> Any:
            t0 = time.perf_counter()
            out = fn(*a)
            cap[f"t_{name}"] = time.perf_counter() - t0
            t1 = time.perf_counter()
            cap[f"{name}_in"], cap[f"{name}_out"] = a, out
            if name in ("fold", "pass"):
                # Copied: the caller appends family holders to the pass output's lists afterwards.
                cap[f"{name}_in"] = ({c: list(hs) for c, hs in a[0].items()}, *a[1:])
                cap[f"{name}_out"] = ({c: list(hs) for c, hs in out[0].items()}, out[1])
            cap["copy"] = cap.get("copy", 0.0) + time.perf_counter() - t1
            return out

        return wrapper

    rollup_mod._fold_13f_duplicates = timed("fold", real_fold)  # type: ignore[assignment]
    rollup_mod._reconcile_cofiler_groups = timed("pass", real_pass)  # type: ignore[assignment]
    for n, fn in reads.items():
        setattr(rollup_mod, n, timed(n, fn))
    timed_parts = ("fold", "pass", *reads)

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
                st["error: treated"] += 1
                fails.append(f"{symbol}: {exc!r}")
                continue
            if "fold_in" not in cap:
                st[f"no_data: {r.no_data_reason}"] += 1
                if r.no_data_reason is None:
                    fails.append(f"{symbol}: did not reach the pass and has no no_data_reason")
                continue
            st["reached"] += 1
            cost = sum(cap.get(f"t_{x}", 0.0) for x in timed_parts)
            share = cost / max(elapsed - cost, 1e-9)
            lat_all.append(share)
            for x in timed_parts:
                parts[x].append(cap.get(f"t_{x}", 0.0) / max(elapsed - cost, 1e-9))
            (fold_in, covers_in, _), (fold_out, f13) = cap["fold_in"], cap["fold_out"]
            (_, cofilers), (pass_out, cof_corr) = cap["pass_in"], cap["pass_out"]
            try:
                base_covers = [base._CoverRow(*dataclasses.astuple(c)[:6]) for c in covers_in]
                tb = time.perf_counter()
                base_fold_out, base_f13 = base._fold_13f_duplicates(fold_in, base_covers)
                parts["base fold (not in the gate)"].append((time.perf_counter() - tb) / max(elapsed - cost, 1e-9))
                base_out, base_cof = real_pass(base_fold_out, cofilers)
            except Exception as exc:  # noqa: BLE001
                st["error: base"] += 1
                fails.append(f"{symbol}: base {exc!r}")
                continue
            try:
                g13 = [c for c in f13 if "joint Schedule 13G " in c.detail]
                d13 = [c for c in f13 if "joint Schedule 13G " not in c.detail]
                if _plain(d13) != _plain(base_f13):
                    fails.append(f"{symbol}: treated 13D-arm corrections != base")
                o = oracle(
                    fold_in,
                    conn.execute(_COVERS_SQL, {"iid": iid}).fetchall(),
                    conn.execute(_OBS_SQL, {"iid": iid}).fetchall(),
                )
                st["candidates"] += len(o["cand"])
                base_kept = {_key(h) for h in base_fold_out.get("institutions", [])}
                gone = base_kept - {_key(h) for h in fold_out.get("institutions", [])}
                if gone != set(o["dups"]):
                    fails.append(f"{symbol}: 13G duplicates {sorted(gone)} != oracle {sorted(o['dups'])}")
                want = sorted(
                    (
                        min(enumerate(o["rows"][k]), key=lambda ih: (-ih[1].shares, ih[1].winning_accession, ih[0]))[
                            1
                        ].filer_cik,
                        o["tot"][m],
                    )
                    for m, (k, _) in o["dups"].items()
                )
                if sorted((c.filer_cik, c.shares_removed) for c in g13) != want:
                    fails.append(f"{symbol}: 13G corrections != oracle")
                if _plain(apply_oracle(base_fold_out, o)) != _plain(fold_out):
                    fails.append(f"{symbol}: fold output != base output with the oracle's folds")
                sum_t = sum((o["tot"][m] for m in o["dups"]), Decimal(0))
                d = _total(pass_out) - _total(base_out)
                if d != -sum_t:
                    fails.append(f"{symbol}: treated − base {d} != −Σ T {-sum_t}")
                if _total(fold_in) - _total(pass_out) != sum((c.shares_removed for c in (*f13, *cof_corr)), Decimal(0)):
                    fails.append(f"{symbol}: input − output != Σ shares_removed")
                g0 = rollup_mod._cofiler_graph(base_fold_out, cofilers)
                g1 = rollup_mod._cofiler_graph(fold_out, cofilers)
                if g0.edges != g1.edges or g0.cliques != g1.cliques or g0.chains != g1.chains:
                    fails.append(f"{symbol}: #3898 edges or collapse list changed")
                if [(c.filer_cik, c.shares_removed) for c in base_cof] != [
                    (c.filer_cik, c.shares_removed) for c in cof_corr
                ]:
                    fails.append(f"{symbol}: #3898 corrections changed")
            except Exception as exc:  # noqa: BLE001
                st["error: oracle"] += 1
                fails.append(f"{symbol}: oracle {exc!r}")
                continue
            if symbol in _NAMED and d != _NAMED[symbol]:
                fails.append(f"{symbol}: named figure {d} != {_NAMED[symbol]}")
            if symbol in _NAMED or symbol in _PANEL:
                pie_ = sum((s.total_shares for s in r.slices if s.denominator_basis == "pie_wedge"), Decimal(0))
                print(f"named {symbol}: pie {pie_:,} (base {pie_ - d:,}), 13G fold {d:,}")
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
            for m, (k, ev) in o["dups"].items():
                matched_by = "k" if ev["route"].startswith("M filer") else "M"
                who = k if matched_by == "k" else m
                srcs = ("form3", "form4") if matched_by == "k" else ("13f",)
                name_rows = [
                    [h.filer_name, *_row(h)]
                    for h in o["rows"][who]
                    if h.winning_source in srcs and _name(h.filer_name) in o["names_on"][ev["C"]]
                ]
                cases.append(
                    {
                        "symbol": symbol,
                        "M": [o["rows"][m][0].filer_name, m],
                        "k": [
                            o["rows"][k][0].filer_name,
                            k,
                            next(c for c, hs in fold_in.items() for h in hs if _key(h) == k),
                        ],
                        "T": str(o["tot"][m]),
                        **ev,
                        "name_source_rows": name_rows,
                        "C_blocks": [[b[7], str(b[3]), str(b[4]), str(b[5])] for b in o["blocks"][ev["C"]]],
                        "M_rows": [_row(h) for h in o["rows"][m]],
                        "k_rows": [_row(h) for h in o["rows"][k]],
                        "pie": str(pie),
                        "outstanding": str(out_),
                    }
                )
    accounted = st["reached"] + st["error: treated"] + sum(v for k, v in st.items() if k.startswith("no_data"))
    if accounted != st["selected"]:
        fails.append(f"population: {accounted} accounted != {st['selected']} selected")
    for gate in ("error: treated", "error: base", "error: oracle"):
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
        json.dump(
            {"head": head, "base": base_sha, "stats": st, "delta": str(delta), "cases": cases, "failures": fails},
            fh,
            indent=1,
            default=str,
        )
    for k, v in sorted(st.items()):
        print(f"{k}: {v}")
    print(f"delta {delta:,} (spec census: 17 duplicates on 16 instruments, -235,291,509, 3 fixed); written {args.out}")
    for c in sorted(cases, key=lambda c: -Decimal(c["T"])):
        print("CASE", json.dumps(c, default=str))
    for f in fails[:40]:
        print("FAIL", f)
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
