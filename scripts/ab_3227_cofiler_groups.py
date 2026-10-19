"""Full-population A/B for the #3227 co-filer group pass (read-only).

Spec: docs/specs/ownership/2026-10-19-3227-cofiler-group-identity.md (Acceptance).

Builds every instrument's rollup on THIS checkout inside one REPEATABLE READ snapshot, with
``_read_cofilers`` and ``_reconcile_cofiler_groups`` wrapped so their real input and output are
recorded. The pass only removes rows, so the base arm is exact without a second run: base pie =
treated pie + the pass's removed shares, and the script asserts that identity per instrument
(input total − output total == Σ ``shares_removed``).

Gates (non-zero exit on any failure):
  * pie up = 0, newly oversubscribed = 0;
  * every collapse is a clique of :func:`_cofiler_graph`, and no surviving identity changes category;
  * treatment quality, on the candidate DEF 14A test below: ≥ 15 in-range cliques, = SUM ≤ 1,
    between-nearer-SUM ≤ 2;
  * latency: the pass + its query add ≤ 10% to ``get_ownership_rollup`` at p95.

Candidate DEF 14A test (frozen here; the spec's Evidence 2): a clique's single proxy row counts
only when every member name tokenises, every member row's as-of is within [proxy as-of − 400d,
proxy as-of + 30d], exactly one distinct ``ownership_def14a_current`` row (keyed
``(source_document_id, holder_name_key, shares)``) matches any member, and a non-largest member
holds > 5% of outstanding. Classified against [MAX × 0.99, SUM × 1.01] first, then ±1% bands.

Usage: ``PYTHONPATH=. uv run python -m scripts.ab_3227_cofiler_groups [--out PATH] [--limit N]``
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
import sys
import time
from collections import Counter, defaultdict
from datetime import timedelta
from decimal import Decimal
from typing import Any

import psycopg

import app.services.ownership_rollup as rollup_mod
from app.config import settings
from app.db.snapshot import snapshot_read

_STOP = frozenset(
    "jr sr ii iii iv mr ms dr the llc lp inc ltd co fund capital management partners group holdings trust advisors "
    "llp corp and of et al entities affiliated with associated affiliates its investment investments company family "
    "revocable living foundation lllp plc limited master gp holding".split()
)
_WINDOW_BEFORE = timedelta(days=400)
_WINDOW_AFTER = timedelta(days=30)
_BAND = Decimal("0.01")
_MINOR_PCT = Decimal("0.05")
# The spec's named figures, then the corpus smoke panel.
_NAMED = frozenset({"TMUS", "IBRX", "INIO", "NEXT", "AAPL", "GME", "MSFT", "JPM", "HD"})


def tokens(name: str | None) -> frozenset[str]:
    """Lower-case alphanumeric tokens of length ≥ 2 outside the stop list."""
    words = re.sub(r"[^a-z0-9 ]", " ", (name or "").lower()).split()
    return frozenset(t for t in words if len(t) > 1 and t not in _STOP)


def names_match(a: frozenset[str], b: frozenset[str]) -> bool:
    common = a & b
    return len(common) >= 2 or (len(common) == 1 and (len(a) == 1 or len(b) == 1))


def classify(proxy: Decimal, mx: Decimal, sm: Decimal) -> str:
    """The candidate row against the admissible interval first, then the ±1% bands."""
    if proxy < mx * (1 - _BAND) or proxy > sm * (1 + _BAND):
        return "outside"
    at_max, at_sum = proxy <= mx * (1 + _BAND), proxy >= sm * (1 - _BAND)
    if at_max and at_sum:
        return "ambiguous"
    if at_max:
        return "=MAX"
    if at_sum:
        return "=SUM"
    return "between_nearer_MAX" if proxy - mx <= sm - proxy else "between_nearer_SUM"


def candidate_status(
    members: list[tuple[str, list[Any], Decimal]], outstanding: Decimal | None, proxies: list[tuple]
) -> tuple[str, tuple | None]:
    """``members`` = (identity, rows, owner total), keeper first; ``proxies`` = the instrument's
    (key, name, shares, as_of) rows."""
    names = [tokens(rows[0].filer_name) for _, rows, _ in members]
    dates = [h.as_of_date for _, rows, _ in members for h in rows]
    if not all(names):
        return "nd: a member name has no usable tokens", None
    if any(d is None for d in dates):
        return "nd: a member row has no date", None
    if outstanding is None or outstanding <= 0:
        return "nd: no outstanding", None
    if not any(total > outstanding * _MINOR_PCT for _, _, total in members[1:]):
        return "nd: every minor member <= 5%", None
    # One distinct proxy per declared key: the table's key also carries the ownership nature.
    matched = list({p[0]: p for p in proxies if any(names_match(n, tokens(p[1])) for n in names)}.values())
    if not matched:
        return "nd: no proxy row", None
    if len(matched) > 1:
        return "nd: >1 proxy rows", None
    proxy = matched[0]
    if proxy[3] is None or min(dates) < proxy[3] - _WINDOW_BEFORE or max(dates) > proxy[3] + _WINDOW_AFTER:  # type: ignore[operator]
        return "nd: member dates not aligned to proxy", proxy
    mx, sm = members[0][2], sum((t for _, _, t in members), Decimal(0))
    return classify(Decimal(proxy[2]), mx, sm), proxy


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="/tmp/ab_3227_cofiler_groups.json")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    calls: dict[str, Any] = {}
    real_read, real_pass = rollup_mod._read_cofilers, rollup_mod._reconcile_cofiler_groups

    def read_wrapper(conn: Any, accessions: Any) -> Any:
        t0 = time.perf_counter()
        out = real_read(conn, accessions)
        calls["t"] = calls.get("t", 0.0) + time.perf_counter() - t0
        return out

    def pass_wrapper(by_category: Any, cofilers: Any) -> Any:
        t0 = time.perf_counter()
        out = real_pass(by_category, cofilers)
        calls["t"] = calls.get("t", 0.0) + time.perf_counter() - t0
        # Copies: get_ownership_rollup extends these lists with the family holders afterwards. Their
        # cost is bookkeeping the real rollup does not pay, so it is timed and taken off ``elapsed``.
        t1 = time.perf_counter()
        calls["input"] = {c: list(hs) for c, hs in by_category.items()}
        calls["output"] = {c: list(hs) for c, hs in out[0].items()}
        calls["cofilers"] = cofilers
        calls["copy"] = time.perf_counter() - t1
        return out

    rollup_mod._read_cofilers = read_wrapper  # type: ignore[assignment]
    rollup_mod._reconcile_cofiler_groups = pass_wrapper  # type: ignore[assignment]

    stats: Counter[str] = Counter()
    shares: defaultdict[str, Decimal] = defaultdict(Decimal)
    failures: list[str] = []
    overhead: list[float] = []
    cliques_out: list[dict[str, Any]] = []
    with psycopg.connect(settings.database_url) as conn, snapshot_read(conn):
        insts = conn.execute(
            """SELECT i.instrument_id, i.symbol FROM instruments i
                WHERE i.instrument_id IN (SELECT instrument_id FROM ownership_insiders_current WHERE shares > 0
                                          UNION SELECT instrument_id FROM ownership_blockholders_current
                                                 WHERE aggregate_amount_owned > 0)
                ORDER BY 1"""
        ).fetchall()
        if args.limit:
            insts = insts[: args.limit]
        proxies_by_iid: dict[int, list[tuple]] = {}
        for iid, doc, key, nm, sh, pe, fa in conn.execute(
            """SELECT instrument_id, source_document_id, holder_name_key, holder_name, shares, period_end, filed_at
                 FROM ownership_def14a_current WHERE shares IS NOT NULL"""
        ):
            proxies_by_iid.setdefault(iid, []).append(((doc, key, sh), nm, sh, pe or (fa.date() if fa else None)))
        for iid, symbol in insts:
            calls.clear()
            t0 = time.perf_counter()
            r = rollup_mod.get_ownership_rollup(conn, symbol, iid)
            elapsed = time.perf_counter() - t0 - calls.get("copy", 0.0)
            if "input" not in calls:
                stats["rollup returned before the pass (no denominator)"] += 1
                continue
            overhead.append(calls["t"] / max(elapsed - calls["t"], 1e-9))
            pie = sum((s.total_shares for s in r.slices if s.denominator_basis == "pie_wedge"), Decimal(0))
            corr = [c for c in r.corrections_applied if c.kind == "cofiler_group_collapse"]
            removed = sum((c.shares_removed for c in corr), Decimal(0))
            stats["instruments"] += 1
            if symbol in _NAMED:
                keep = [
                    (s.category, h.filer_cik, h.filer_name, str(h.shares))
                    for s in r.slices
                    if s.category in ("insiders", "blockholders")
                    for h in s.holders
                    if h.shares > 1_000_000
                ]
                print(f"named {symbol}: pie {pie:,} (base {pie + removed:,}); {keep}", flush=True)
            by_in, by_out = calls["input"], calls["output"]
            graph = rollup_mod._cofiler_graph(by_in, calls["cofilers"])
            stats["chains_left_split"] += len(graph.chains)
            if not corr:
                if graph.cliques and r.slices:
                    failures.append(f"{symbol}: {len(graph.cliques)} cliques but no correction")
                elif graph.cliques:
                    # No denominator: the rollup returns an empty no-data payload after the pass ran.
                    stats["cliques on a no-pie rollup (no denominator)"] += len(graph.cliques)
                continue
            total_in = sum((h.shares for hs in by_in.values() for h in hs), Decimal(0))
            total_out = sum((h.shares for hs in by_out.values() for h in hs), Decimal(0))
            if total_in - total_out != removed:
                failures.append(f"{symbol}: input − output {total_in - total_out} != removed {removed}")
            cat_in = {rollup_mod._identity_key(h.filer_cik, h.filer_name): c for c, hs in by_in.items() for h in hs}
            for c, hs in by_out.items():
                for h in hs:
                    if cat_in.get(rollup_mod._identity_key(h.filer_cik, h.filer_name)) != c:
                        failures.append(f"{symbol}: {h.filer_name} changed category to {c}")
            if len(graph.cliques) != len(corr):
                failures.append(f"{symbol}: {len(graph.cliques)} cliques but {len(corr)} corrections")
            outstanding = r.shares_outstanding
            base = pie + removed
            stats["instruments_down"] += 1
            shares["removed"] += removed
            if outstanding:
                was, now = base > outstanding, pie > outstanding
                stats["oversubscribed_fixed"] += was and not now
                stats["newly_oversubscribed"] += now and not was
                if now and not was:
                    failures.append(f"{symbol}: newly oversubscribed")
            for members in graph.cliques:
                stats["cliques"] += 1
                rows = [(k, graph.rows_by_identity[k], graph.totals[k]) for k in members]
                mx, sm = rows[0][2], sum((t for _, _, t in rows), Decimal(0))
                status, proxy = candidate_status(rows, outstanding, proxies_by_iid.get(iid, []))
                stats[status] += 1
                if outstanding and sm > outstanding:
                    stats["E1 cliques SUM > outstanding"] += 1
                    shares["E1 SUM - outstanding"] += sm - outstanding
                    shares["E1 SUM - MAX"] += sm - mx
                cliques_out.append(
                    {
                        "symbol": symbol,
                        "outstanding": str(outstanding),
                        "max": str(mx),
                        "sum": str(sm),
                        "status": status,
                        "proxy": [str(x) for x in proxy] if proxy else None,
                        "members": [
                            {
                                "identity": k,
                                "category": graph.category_of[k],
                                "total": str(t),
                                "rows": [
                                    (h.filer_name, h.winning_source, h.winning_accession, str(h.as_of_date))
                                    + (str(h.shares),)
                                    for h in hs
                                ],
                            }
                            for k, hs, t in rows
                        ],
                        "links": [(sorted(p), sorted(a)) for p, a in graph.edges.items() if p <= set(members)],
                    }
                )

    in_range = sum(v for k, v in stats.items() if k in ("=MAX", "=SUM", "ambiguous") or k.startswith("between"))
    if in_range < 15:
        failures.append(f"quality: {in_range} in-range candidate cliques < 15")
    if stats["=SUM"] > 1:
        failures.append(f"quality: =SUM {stats['=SUM']} > 1")
    if stats["between_nearer_SUM"] > 2:
        failures.append(f"quality: between_nearer_SUM {stats['between_nearer_SUM']} > 2")
    p95 = statistics.quantiles(overhead, n=20)[-1] if len(overhead) >= 20 else max(overhead, default=0.0)
    if p95 > 0.10:
        failures.append(f"latency: p95 overhead {p95:.1%} > 10%")
    json.dump(
        {
            "stats": stats,
            "shares": {k: str(v) for k, v in shares.items()},
            "cliques": cliques_out,
            "failures": failures,
        },
        open(args.out, "w"),
        indent=1,
        default=str,
    )
    for k, v in sorted(stats.items()):
        print(f"{k}: {v}")
    for k, v in sorted(shares.items()):
        print(f"shares {k}: {v:,}")
    if overhead:
        print(f"overhead p50 {statistics.median(overhead):.2%} p95 {p95:.2%} max {max(overhead):.2%}")
    print(f"in-range candidate cliques: {in_range}; written {args.out}")
    for f in failures[:40]:
        print("FAIL", f)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
