"""Read-only census of the #2230 residual: instruments whose RENDERED insiders wedge
exceeds ``shares_outstanding``.

#2226's partition (2026-08-03) measured **378** such instruments and split them out as
#2230's headline. Everything that has landed since then touches that number — #2229
(staleness release), #1652/#2230's deemed-chain tier, #1764's same-accession tier, #2385's
representative key, #2386's overloaded-``ownership_nature`` fix, #2786's release-hazard
narrowing — and **none of those runs re-measured it**. This script does, so the residual is
stated as a figure computed today rather than carried from the issue body.

⚠ The measurement point is the RENDERED wedge, not the ``ownership_insiders_current`` sum.
Those are different populations in both directions and the difference is not a rounding
detail:

* the read path folds (control-group collapses, owner-once MAX across channels), which
  removes shares the raw sum carries; and
* ``_reconcile_owner_once`` routes a holder to the **insiders** slice whenever a
  Section-16 relationship wins the source priority, so a 13D/G or 13F row can arrive in
  the insiders wedge without ever appearing in ``ownership_insiders_current``.

So a candidate list built by pre-filtering on the raw table is neither a superset nor a
subset of the answer. The population here is therefore EVERY instrument, rendered.

Each line also carries the fields the classification needs, so the follow-up pass does not
have to re-render: every pie wedge, the pie total, and a counter of the read-time
corrections that fired.

Usage (sharded so several workers can share the population):

    PYTHONPATH=. uv run python -m scripts.audit_2230_insider_oversubscription \
        --shard 0 --shards 4 --out /tmp/audit2230_resid_0.jsonl

Then summarise every shard together:

    PYTHONPATH=. uv run python -m scripts.audit_2230_insider_oversubscription \
        --summarise /tmp/audit2230_resid_*.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from collections.abc import Sequence
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

import psycopg

from app.config import settings
from app.services.ownership_rollup import Holder, get_ownership_rollup

POPULATION_SQL = """
SELECT instrument_id, symbol
  FROM instruments
 ORDER BY instrument_id
"""


def _snapshot(conn: psycopg.Connection[Any], symbol: str, instrument_id: int) -> dict[str, Any]:
    rollup = get_ownership_rollup(conn, symbol, instrument_id)
    wedges = {s.category: s.total_shares for s in rollup.slices if s.denominator_basis == "pie_wedge"}
    pie_total = sum(wedges.values(), Decimal(0))
    outstanding = rollup.shares_outstanding
    insiders = wedges.get("insiders", Decimal(0))
    return {
        "instrument_id": instrument_id,
        "symbol": symbol,
        "outstanding": str(outstanding) if outstanding is not None else None,
        "outstanding_as_of": rollup.shares_outstanding_as_of.isoformat() if rollup.shares_outstanding_as_of else None,
        "wedges": {k: str(v) for k, v in wedges.items()},
        "pie_total": str(pie_total),
        "insiders_ratio": str(insiders / outstanding) if outstanding and outstanding > 0 else None,
        "pie_ratio": str(pie_total / outstanding) if outstanding and outstanding > 0 else None,
        "corrections": dict(Counter(c.kind for c in rollup.corrections_applied)),
    }


#: Counterfactual staleness bounds, in years. NOT proposed rules — see
#: :func:`partition_insiders`.
STALENESS_ARMS: tuple[int, ...] = (2, 5)


def partition_insiders(
    holders: Sequence[Holder],
    outstanding: Decimal | None,
    anchor: date | None,
) -> dict[str, Any]:
    """Attribute one instrument's insider overage to a mechanism. Pure.

    Two arms, each answering "would THIS mechanism, applied perfectly, bring the wedge
    back under ``shares_outstanding``". They are diagnostics, not designs: the point is to
    price #2230's own mechanism against the alternatives on the same cohort and the same
    criterion, because the ticket has never been measured that way.

    * ``clears_if_clusters_folded`` — #2230's mechanism. Every surviving equal-value
      cluster (≥2 distinct identities at one exact share value, i.e. one the read path's
      fold passes did NOT collapse) counted once instead of N times.
    * ``clears_if_bounded_{N}y`` — the staleness alternative. Holders whose ``as_of_date``
      predates the slice's own newest by more than N years are dropped.

    ⚠ The staleness arm is NOT a proposed rule and must not be lifted into one. It
    over-removes by construction: a long-held stake that the holder has had no occasion to
    restate is indistinguishable here from one they exited. A real expiry treatment needs
    its own source rule — Section 16's obligation ENDS when the person ceases to be an
    insider (Rule 16a-2), which is why the row simply stops moving rather than going to
    zero. The arm exists to size the axis, nothing more.

    ``single_holder_over`` is the load-bearing split and is checked FIRST: when one holder
    alone exceeds ``shares_outstanding`` there is nothing to de-duplicate, so no
    additivity treatment of any kind can fix that instrument. It is a bad row (a
    pre-reverse-split count, #2231) or a bad denominator (#2232). #2230's mechanism can
    only be at work on the complement.

    The anchor is the slice's own newest as-of rather than today, so an instrument whose
    whole insider book is old is not scored as entirely stale. A NULL as-of is KEPT on
    every arm (fail toward the status quo) and counted separately so the reader can price
    that choice.
    """
    live = [h for h in holders if h.shares > 0]
    by_value: dict[Decimal, list[Holder]] = {}
    for h in live:
        by_value.setdefault(h.shares, []).append(h)

    def _identities(members: Sequence[Holder]) -> set[str]:
        return {h.filer_cik or h.filer_name for h in members}

    clusters = [v for v in by_value.values() if len(_identities(v)) >= 2]
    total = sum((h.shares for h in holders), Decimal(0))
    top = max((h.shares for h in holders), default=Decimal(0))
    # Shares an equal-value cluster carries more than once: (n_identities - 1) x value.
    cluster_excess = sum(((len(_identities(v)) - 1) * v[0].shares for v in clusters), Decimal(0))
    has_denominator = outstanding is not None and outstanding > 0

    arms: dict[str, Decimal] = {}
    for years in STALENESS_ARMS:
        if anchor is None:
            arms[f"total_within_{years}y"] = total
            continue
        cutoff = anchor - timedelta(days=365 * years)
        arms[f"total_within_{years}y"] = sum(
            (h.shares for h in holders if h.as_of_date is None or h.as_of_date >= cutoff),
            Decimal(0),
        )

    out: dict[str, Any] = {
        "insiders_total": str(total),
        "insiders_ratio": str(total / outstanding) if has_denominator and outstanding else None,
        "n_holders": len(holders),
        "top_holder_shares": str(top),
        "top_holder_name": next((h.filer_name for h in holders if h.shares == top), None),
        "top_holder_as_of": next((h.as_of_date.isoformat() for h in holders if h.shares == top and h.as_of_date), None),
        "single_holder_over": bool(has_denominator and outstanding is not None and top > outstanding),
        "denominator_under_1m": bool(outstanding is not None and outstanding < 1_000_000),
        "equal_value_clusters": len(clusters),
        "max_cluster_members": max((len(_identities(v)) for v in clusters), default=0),
        "cluster_excess": str(cluster_excess),
        "clears_if_clusters_folded": bool(
            has_denominator and outstanding is not None and (total - cluster_excess) <= outstanding
        ),
        "holders_without_as_of": sum(1 for h in holders if h.as_of_date is None),
    }
    for key, value in arms.items():
        out[key] = str(value)
        out[f"clears_if_bounded_{key.rsplit('_', 1)[-1]}"] = bool(
            has_denominator and outstanding is not None and value <= outstanding
        )
    return out


def _classify(conn: psycopg.Connection[Any], symbol: str, instrument_id: int) -> dict[str, Any]:
    """Re-render ONE oversubscribed instrument and hand its insider holders to
    :func:`partition_insiders`.

    The partition is deliberately taken on the RENDERED insider holders rather than on
    ``ownership_insiders_current``, because the fold passes and owner-once both run between
    the two and the ticket is about what the operator sees.
    """
    rollup = get_ownership_rollup(conn, symbol, instrument_id)
    insiders = next((s for s in rollup.slices if s.category == "insiders"), None)
    holders = list(insiders.holders) if insiders else []
    return {
        "symbol": symbol,
        "instrument_id": instrument_id,
        "outstanding": str(rollup.shares_outstanding) if rollup.shares_outstanding is not None else None,
        **partition_insiders(holders, rollup.shares_outstanding, insiders.as_of_max if insiders else None),
        "as_of_min": insiders.as_of_min.isoformat() if insiders and insiders.as_of_min else None,
        "as_of_max": insiders.as_of_max.isoformat() if insiders and insiders.as_of_max else None,
        "corrections": dict(Counter(c.kind for c in rollup.corrections_applied)),
    }


def _run_classify(paths: list[str], out: str) -> int:
    over: list[tuple[int, str]] = []
    for path in paths:
        with open(path) as fh:
            for line in fh:
                rec = json.loads(line)
                if rec.get("insiders_ratio") and Decimal(rec["insiders_ratio"]) > 1:
                    over.append((rec["instrument_id"], rec["symbol"]))
    over.sort()
    errors = 0
    rows: list[dict[str, Any]] = []
    with psycopg.connect(settings.database_url) as conn, open(out, "w", buffering=1) as fh:
        for n, (instrument_id, symbol) in enumerate(over):
            try:
                rec = _classify(conn, symbol, instrument_id)
            except Exception as exc:
                errors += 1
                rec = {"instrument_id": instrument_id, "symbol": symbol, "error": repr(exc)}
            rows.append(rec)
            fh.write(json.dumps(rec) + "\n")
            if n % 50 == 0:
                print(f"classify {n}/{len(over)}", file=sys.stderr, flush=True)
    clean = [r for r in rows if "error" not in r]
    print(f"oversubscribed insiders wedges  : {len(over)}   harness errors {errors}")
    print(f"  one holder alone > outstanding: {sum(1 for r in clean if r['single_holder_over'])}")
    print(f"  denominator < 1e6             : {sum(1 for r in clean if r['denominator_under_1m'])}")
    additive = [r for r in clean if not r["single_holder_over"]]
    print(f"  additivity-eligible cohort    : {len(additive)}")
    print(f"    carrying an equal-value cluster : {sum(1 for r in additive if r['equal_value_clusters'])}")
    print(f"    folding them clears the wedge   : {sum(1 for r in additive if r['clears_if_clusters_folded'])}")
    print(f"    no equal-value cluster at all   : {sum(1 for r in additive if not r['equal_value_clusters'])}")
    print("counterfactual staleness arm (whole cohort, NOT a proposed rule):")
    for years in (2, 5):
        cleared = sum(1 for r in clean if r.get(f"clears_if_bounded_{years}y"))
        print(f"    bounding insider rows to {years}y clears : {cleared} of {len(clean)}")
    print(f"    holders carrying no as-of date        : {sum(r['holders_without_as_of'] for r in clean)}")
    return 1 if errors else 0


def _summarise(paths: list[str]) -> int:
    rows: list[dict[str, Any]] = []
    errors = 0
    for path in paths:
        with open(path) as fh:
            for line in fh:
                rec = json.loads(line)
                if "error" in rec:
                    errors += 1
                    print(f"HARNESS ERROR {rec['symbol']}: {rec['error']}", file=sys.stderr)
                    continue
                rows.append(rec)
    with_denominator = [r for r in rows if r["insiders_ratio"] is not None]
    over_insiders = [r for r in with_denominator if Decimal(r["insiders_ratio"]) > 1]
    over_pie = [r for r in with_denominator if Decimal(r["pie_ratio"]) > 1]
    print(f"instruments rendered            : {len(rows)}")
    print(f"harness errors                  : {errors}")
    print(f"  with a usable denominator     : {len(with_denominator)}")
    print(f"  insiders wedge > outstanding  : {len(over_insiders)}")
    print(f"  pie total    > outstanding    : {len(over_pie)}")
    buckets = Counter()
    for r in over_insiders:
        ratio = Decimal(r["insiders_ratio"])
        if ratio < Decimal("1.5"):
            buckets["1.0-1.5x"] += 1
        elif ratio < 5:
            buckets["1.5-5x"] += 1
        elif ratio < 100:
            buckets["5-100x"] += 1
        else:
            buckets[">=100x"] += 1
    print("insiders-over severity          :", dict(buckets))
    fired = Counter()
    for r in over_insiders:
        for kind, n in r["corrections"].items():
            fired[kind] += n
    print("corrections on the over cohort  :", dict(fired))
    worst = sorted(over_insiders, key=lambda r: Decimal(r["insiders_ratio"]), reverse=True)[:10]
    print("worst 10                        :", [(r["symbol"], r["insiders_ratio"][:8]) for r in worst])
    # Exit non-zero on a harness fault: a broken measurement and a clean population must
    # never be the same output (#2230 release-hazard census, 2026-08-20).
    return 1 if errors else 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--shard", type=int, default=0)
    ap.add_argument("--shards", type=int, default=1)
    ap.add_argument("--out")
    ap.add_argument("--summarise", nargs="*")
    ap.add_argument("--classify", nargs="*", help="census jsonl paths; re-renders the over cohort")
    args = ap.parse_args()

    if args.classify:
        if not args.out:
            ap.error("--out is required with --classify")
        return _run_classify(list(args.classify), args.out)
    if args.summarise:
        return _summarise(list(args.summarise))
    if not args.out:
        ap.error("--out is required unless --summarise is given")

    errors = 0
    with psycopg.connect(settings.database_url) as conn:
        population = conn.execute(POPULATION_SQL).fetchall()
        mine = [r for n, r in enumerate(population) if n % args.shards == args.shard]
        # Line-buffered: a buffered long run looks stalled when it is healthy
        # (prevention log, 2026-08-05 phase 2a).
        with open(args.out, "w", buffering=1) as fh:
            for n, (instrument_id, symbol) in enumerate(mine):
                try:
                    rec = _snapshot(conn, str(symbol), int(instrument_id))
                except Exception as exc:  # harness error — record, never silently drop
                    errors += 1
                    rec = {"instrument_id": int(instrument_id), "symbol": str(symbol), "error": repr(exc)}
                fh.write(json.dumps(rec) + "\n")
                if n % 200 == 0:
                    print(f"shard {args.shard}: {n}/{len(mine)}", file=sys.stderr, flush=True)
    print(f"shard {args.shard}: done {len(mine)} errors {errors}", file=sys.stderr, flush=True)
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
