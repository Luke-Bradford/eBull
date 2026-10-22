"""Full-population A/B for tier (O) of the #3227 13F-duplicate fold (read-only).

Spec: docs/specs/ownership/2026-10-22-3227-13f-13g-observation-block.md (Acceptance).

Builds every rollup on THIS checkout inside one REPEATABLE READ snapshot (session time zone UTC), with the fold,
the cover reads, the co-filer read and the co-filer pass wrapped so their real inputs, outputs and costs are
recorded. The BASE arm is the real code at a pinned SHA (``--base``, default the merge-base with origin/main),
loaded from git as its own module: its ``_fold_13f_duplicates`` (the 13D arm and (U)) runs on the fold's real input,
with the observations reduced to the CIKs its read returned, then the real co-filer pass. An independent ORACLE
re-reads covers and observations per instrument (unbounded): tiers 1 and 2 are the 13G-covers A/B's frozen oracle
(``scripts.ab_3227_13f_13g_covers.oracle``); tier (O) is this file's own frozen copy of the spec's rule.

Gates (non-zero exit on any failure):
  combined pass:
  * the treated 13D-arm and (U) corrections equal the base's, member for member (field by field);
  * the oracle's (U) duplicates equal the base's (U) corrections (k's CIK, T);
  increment:
  * the oracle's (O) duplicate set equals the treated pass's (O) corrections ("counted here as"), member for member
    (M removed, k's CIK, T), and the treated fold output equals the base fold output with the oracle's (O) folds;
  * per instrument: treated pie = base pie − Σ T over (O) duplicates only;
  * #3898's edges, cliques, chains and corrections unchanged; newly oversubscribed = 0;
  * input − output = Σ ``shares_removed`` over all corrections;
  * population accounted; base, treated and oracle errors each 0;
  * latency: (co-filer read + both cover reads + fold + co-filer pass) / (the rest of ``get_ownership_rollup``),
    p95 ≤ 10%, on the full population and on the affected instruments.

Reported, not gated: the census reconciliation (spec: 14 duplicates on 14 instruments, −72,613,547, PRME fixed) and
the case review rows (M, k, T, C, filer of record, the observation, C's blocks, the blocks named as M, every kept
row of M and k).

Usage: ``PYTHONPATH=. uv run python -m scripts.ab_3227_13f_13g_observation_block [--base SHA] [--out PATH]
[--limit N]``
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import statistics
import subprocess
import sys
import time
from collections import Counter, defaultdict
from decimal import Decimal
from typing import Any

import psycopg

import app.services.ownership_rollup as rollup_mod
from app.config import settings
from app.db.snapshot import snapshot_read
from scripts.ab_3227_13f_13g_covers import _COVERS_SQL, _full, _load_base, _name, _plain, apply_oracle, oracle
from scripts.ab_3227_13f_duplicates import _digits, _key, _key_canon, _row, _total

_OBS_SQL = """
    SELECT source_accession, reporter_cik, reporter_name, aggregate_amount_owned
      FROM ownership_blockholders_observations
     WHERE instrument_id = %(iid)s AND source = '13g' AND known_to IS NULL
"""
_O_MARK = "counted here as"
_U_MARK = "every stored reporting-person block reports"
_NAMED = {"PRME": Decimal(-16_562_498), "DH": Decimal(0), "TDAY": Decimal(0)}
_PANEL = ("AAPL", "GME", "MSFT", "JPM", "HD")


def oracle_o(o: dict[str, Any], obs: list[tuple[Any, ...]], why: Counter[str]) -> dict[str, tuple[str, dict[str, Any]]]:
    """Tier (O), written independently of ``_fold_13f_duplicates``, over the 13G-covers oracle's output ``o``."""
    rows, tot, blocks, filer = o["rows"], o["tot"], o["blocks"], o["filer"]
    on: dict[str, list[tuple[Any, ...]]] = defaultdict(list)
    for r in obs:
        on[r[0]].append(r)
    found: dict[str, tuple[list[str], dict[str, Any]]] = {}
    for m, acc in o["selected"].items():
        if m in o["matched_13d"] or m in o["found"]:
            continue
        t, mc, bs, f = tot[m], _digits(m[4:]), blocks[acc], filer[acc]
        if all(_full(b, t) for b in bs) or f is None or f == mc:
            continue
        why["(O) evaluated"] += 1
        ob = on.get(acc, [])
        if len(ob) != 1 or ob[0][3] != t or ob[0][2] is None:
            why["observation count, name or aggregate"] += 1
            continue
        located = [b for b in bs if b[7] == ob[0][2]]
        if len(located) != 1 or not _full(located[0], t):
            why["observation's block"] += 1
            continue
        if any(b[5] is not None and b[5] > t for b in bs):
            why["a block above T"] += 1
            continue
        mn = o["names"](m, ("13f",))
        mb = [b for b in bs if _name(b[7]) in mn]
        if not mb or not all(_full(b, t) for b in mb):
            why["blocks named as M"] += 1
            continue
        ks = sorted(
            k
            for k in o["nodes"]
            if _key_canon(k) == f
            and tot[k] == t
            and all(h.winning_source == "13g" and h.winning_accession == acc for h in rows[k])
        )
        if not ks:
            why["no twin"] += 1
            continue
        found[m] = (
            ks,
            {
                "C": acc,
                "filed": str(bs[0][2]),
                "filer": f,
                "observation": [ob[0][1], ob[0][2], str(ob[0][3])],
                "M_blocks": sorted({b[7] for b in mb}),
                "same_block": ob[0][2] in {b[7] for b in mb},
            },
        )
    reserved = {k for ks in o["matched_13d"].values() for k in ks} | {k for ks, _ in o["found"].values() for k in ks}
    count = Counter(k for ks, _ in found.values() for k in ks)
    dups = {}
    for m, (ks, ev) in found.items():
        if len(ks) != 1:
            why["refused: >1 twin"] += 1
        elif ks[0] in reserved:
            why["refused: reserved"] += 1
        elif count[ks[0]] != 1:
            why["refused: shared twin"] += 1
        else:
            dups[m] = (ks[0], ev)
    return dups


def _primary_cik(rows: list[Any]) -> str | None:
    return min(enumerate(rows), key=lambda ih: (-ih[1].shares, ih[1].winning_accession, ih[0]))[1].filer_cik


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="")
    ap.add_argument("--out", default="/tmp/ab_3227_13f_13g_observation_block.json")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    head = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    base_sha = (
        args.base
        or subprocess.run(["git", "merge-base", "HEAD", "origin/main"], capture_output=True, text=True).stdout.strip()
    )
    print(f"HEAD {head}; base {base_sha}")
    base = _load_base(base_sha)
    if "_13G_SUBMISSION_TYPES" not in base.__dict__ or "_o_cover_blocks" in base.__dict__:
        print("FAIL base must carry (U) and not (O)")
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
    why: Counter[str] = Counter()
    fails: list[str] = []
    lat_all: list[float] = []
    lat_hit: list[float] = []
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
            (fold_in, covers_in, filers_in), (fold_out, f13) = cap["fold_in"], cap["fold_out"]
            (_, cofilers), (pass_out, cof_corr) = cap["pass_in"], cap["pass_out"]
            try:
                base_covers = [base._CoverRow(*dataclasses.astuple(c)) for c in covers_in]
                base_filers = {a: tuple(c for c, _, _ in rows) for a, rows in filers_in.items()}
                base_fold_out, base_f13 = base._fold_13f_duplicates(fold_in, base_covers, base_filers)
                base_out, base_cof = real_pass(base_fold_out, cofilers)
            except Exception as exc:  # noqa: BLE001
                st["error: base"] += 1
                fails.append(f"{symbol}: base {exc!r}")
                continue
            try:
                o_corr = [c for c in f13 if _O_MARK in c.detail]
                if _plain([c for c in f13 if _O_MARK not in c.detail]) != _plain(base_f13):
                    fails.append(f"{symbol}: treated 13D-arm and (U) corrections != base")
                obs = conn.execute(_OBS_SQL, {"iid": iid}).fetchall()
                o = oracle(fold_in, conn.execute(_COVERS_SQL, {"iid": iid}).fetchall(), [r_[:2] for r_ in obs])
                base_u = sorted((c.filer_cik, c.shares_removed) for c in base_f13 if _U_MARK in c.detail)
                if base_u != sorted((_primary_cik(o["rows"][k]), o["tot"][m]) for m, (k, _) in o["dups"].items()):
                    fails.append(f"{symbol}: oracle (U) duplicates != base (U) corrections")
                dups = oracle_o(o, obs, why)
                base_kept = {_key(h) for h in base_fold_out.get("institutions", [])}
                gone = base_kept - {_key(h) for h in fold_out.get("institutions", [])}
                if gone != set(dups):
                    fails.append(f"{symbol}: (O) duplicates {sorted(gone)} != oracle {sorted(dups)}")
                want = sorted((_primary_cik(o["rows"][k]), o["tot"][m]) for m, (k, _) in dups.items())
                if sorted((c.filer_cik, c.shares_removed) for c in o_corr) != want:
                    fails.append(f"{symbol}: (O) corrections != oracle")
                if _plain(apply_oracle(base_fold_out, {"dups": dups, "rows": o["rows"]})) != _plain(fold_out):
                    fails.append(f"{symbol}: fold output != base output with the oracle's (O) folds")
                sum_t = sum((o["tot"][m] for m in dups), Decimal(0))
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
            pie = sum((s.total_shares for s in r.slices if s.denominator_basis == "pie_wedge"), Decimal(0))
            if symbol in _NAMED or symbol in _PANEL:
                print(f"named {symbol}: pie {pie:,} (base {pie - d:,}), (O) fold {d:,}")
            if not dups:
                continue
            if r.no_data_reason is not None:
                st[f"changed, then no_data: {r.no_data_reason}"] += 1
                continue
            st["changed"] += 1
            st["duplicates"] += len(dups)
            lat_hit.append(share)
            delta += d
            base_pie, out_ = pie - d, r.shares_outstanding
            if out_:
                st["over in main"] += base_pie > out_
                st["fixed"] += base_pie > out_ >= pie
                if pie > out_ >= base_pie:
                    fails.append(f"{symbol}: newly oversubscribed")
            for m, (k, ev) in dups.items():
                cases.append(
                    {
                        "symbol": symbol,
                        "M": [o["rows"][m][0].filer_name, m],
                        "k": [o["rows"][k][0].filer_name, k],
                        "T": str(o["tot"][m]),
                        **ev,
                        "C_blocks": [[b[7], str(b[3]), str(b[4]), str(b[5])] for b in o["blocks"][ev["C"]]],
                        "M_rows": [[h.filer_name, *_row(h)] for h in o["rows"][m]],
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
    with open(args.out, "w") as fh:
        json.dump(
            {
                "head": head,
                "base": base_sha,
                "stats": st,
                "why": why,
                "delta": str(delta),
                "cases": cases,
                "failures": fails,
            },
            fh,
            indent=1,
            default=str,
        )
    for k, v in sorted(st.items()):
        print(f"{k}: {v}")
    print(dict(why))
    print(
        f"delta {delta:,} (spec census: 14 duplicates on 14 instruments, -72,613,547, PRME fixed); written {args.out}"
    )
    for c in sorted(cases, key=lambda c: -Decimal(c["T"])):
        print("CASE", json.dumps(c, default=str))
    for f in fails[:40]:
        print("FAIL", f)
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
