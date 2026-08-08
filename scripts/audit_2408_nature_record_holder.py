"""Census + validation of the ``natureOfOwnership`` record-holder tier (#2408).

#2385 shipped a control-group representative rule that refuses every SAME-ACCESSION
cluster, and the refusal is a format fact rather than caution: ``<nonDerivativeTable>``
is a SIBLING of ``<reportingOwner>``, so a joint Form 3/4 does not attribute its Table I
lines to a co-filer, and ``insider_transactions._extract_holdings`` gives them all to
``filers[0]`` (``app/services/insider_transactions.py:449``). Within one accession
"Table I-attested" therefore means "listed first in the XML".

#2408 adds the evidence that DOES name a holder of record on a joint filing — the
``natureOfOwnership`` free text on the ``I`` lines, plus the footnotes those lines
reference. Two modes:

``--validate`` (the one that decides anything)
    Ground truth is unavailable for same-accession clusters BY CONSTRUCTION — that is the
    ticket. It IS available for CROSS-accession ones: each member files separately, so the
    Table I ``D`` line is attributed correctly and names the record holder. Score the text
    rule on those, against the incumbent key as the baseline. Runs in ~20 s (pure SQL, no
    rollup).

``--census``
    Run the REAL :func:`get_ownership_rollup` over the #2230 population and record which
    folds the tier moves, by route. Slower (DB-bound, ~15-40 min depending on contention).

⚠ Neither mode is the safety evidence on its own. The rep is the identity that survives
into ``_reconcile_owner_once``, so changing it re-parents the folded members' other-channel
rows (#2385: 108 instruments' pie totals moved). The paired full-population A/B against a
control worktree is what prices that, and it is a separate script
(``scripts/ab_2230_deemed_chain.py``).
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from decimal import Decimal
from typing import Any

import psycopg

from app.config import settings
from app.services import ownership_rollup as orl
from scripts.ab_2230_deemed_chain import POPULATION_SQL

# The labelled set: cross-accession Section 16 clusters carrying exactly ONE Table
# I-attested ``direct`` member. Per #2385 clause 1 that member IS the record holder, and
# the attribution is admissible precisely because the accessions differ. The provenance
# predicate is the same ``:(NDT|NDH):`` marker the read path uses (#2386) — spelled here
# rather than imported, so this stays an INDEPENDENT label rather than a restatement of
# the rule under test.
LABELLED_SQL = """
WITH rows AS (
  SELECT instrument_id, shares, holder_identity_key, holder_name, holder_cik, source_accession,
         (source_document_id !~ ':(NDT|NDH):') AS table_i, ownership_nature
    FROM ownership_insiders_current
   WHERE shares > 0 AND source IN ('form3','form4') AND coalesce(btrim(source_accession),'') <> ''
), clusters AS (
  SELECT instrument_id, shares FROM rows GROUP BY 1,2
  HAVING count(DISTINCT holder_identity_key) >= 2
     AND count(DISTINCT source_accession) >= 2
     AND count(*) FILTER (WHERE table_i AND ownership_nature = 'direct') = 1
)
SELECT r.instrument_id, r.shares, r.holder_identity_key, r.holder_name, r.holder_cik,
       r.source_accession, (r.table_i AND r.ownership_nature = 'direct') AS is_truth
  FROM rows r JOIN clusters c USING (instrument_id, shares)
"""


def _validate(conn: psycopg.Connection[Any]) -> int:
    """Score the text rule on the labelled set, against the incumbent key as baseline.

    ⚠ The baseline is the point. "92% correct" means nothing without "and the thing it
    replaces is 54% correct", which is what an arbitrary tie-break scores by construction.
    Both are printed, and so is the count of swaps that BREAK an already-correct incumbent
    — the regression direction, which a precision figure alone hides."""
    members: dict[tuple[int, Decimal], dict[str, str]] = defaultdict(dict)
    order: dict[tuple[int, Decimal], dict[str, tuple[str, str]]] = defaultdict(dict)
    accessions: dict[tuple[int, Decimal], set[str]] = defaultdict(set)
    truth: dict[tuple[int, Decimal], str] = {}
    with conn.cursor() as cur:
        cur.execute(LABELLED_SQL)
        for instrument_id, shares, key, name, cik, accession, is_truth in cur.fetchall():
            cluster = (int(instrument_id), Decimal(shares))
            members[cluster][str(key)] = str(name)
            order[cluster][str(key)] = (str(cik or ""), str(accession))
            accessions[cluster].add(str(accession))
            if is_truth:
                truth[cluster] = str(key)
        evidence = orl._read_record_holder_evidence(conn, sorted({a for accs in accessions.values() for a in accs}))

    stats: Counter[str] = Counter()
    wrong: list[tuple[str, str]] = []
    for cluster, names in members.items():
        _instrument_id, shares = cluster
        # Incumbent = the module's key past the source preference. Shares are equal inside
        # a cluster by construction and every member is an insider row here, so this
        # reduces to (filer_cik, accession) DESC — the arbitrary highest-CIK tie-break.
        #
        # ⚠ String comparison, matching ``_control_group_rep_key``, which also compares
        # ``filer_cik`` as text. That reproduces numeric CIK order only while every CIK is
        # fixed-width zero-padded, and the whole baseline column of this report hangs on
        # it, so it is asserted rather than assumed (review NITPICK on PR #2422). Measured:
        # ``select length(holder_cik), count(*) from ownership_insiders_current`` returns a
        # single row, ``10 | 93352``, and zero rows fail ``^[0-9]{10}$``.
        for cik, _accession in order[cluster].values():
            if len(cik) != 10 or not cik.isdigit():
                raise AssertionError(f"CIK {cik!r} is not 10-digit zero-padded; the incumbent tie-break is invalid")
        incumbent = max(names, key=lambda k: order[cluster][k])
        stats["clusters"] += 1
        if incumbent == truth[cluster]:
            stats["BASELINE incumbent already correct"] += 1
        texts: list[str] = []
        for accession in sorted(accessions[cluster]):
            texts.extend(evidence.get((accession, shares), ()))
        if not texts:
            stats["no evidence text at all"] += 1
            continue
        blobs = [orl._normalise_holder_text(t) for t in texts]
        named = [
            k for k, n in names.items() if (needle := orl._normalise_holder_text(n)) and any(needle in b for b in blobs)
        ]
        if len(named) != 1:
            stats["rule declines (0 or >=2 named)"] += 1
            continue
        stats["rule fires (exactly one named)"] += 1
        if named[0] == incumbent:
            stats["  ... agrees with the incumbent"] += 1
            continue
        stats["  ... SWAPS"] += 1
        if named[0] == truth[cluster]:
            stats["      swap CORRECT"] += 1
        else:
            stats["      swap WRONG"] += 1
            wrong.append((names[named[0]], names[truth[cluster]]))
            if incumbent == truth[cluster]:
                stats["      swap BROKE a correct incumbent"] += 1
    for key, value in stats.items():
        print(f"{key:38s} {value}")
    print("\nwrong picks (picked -> truth):")
    for picked, actual in wrong:
        print(f"  {picked[:44]:44s} -> {actual[:44]}")
    return 0


def _describe(h: orl.Holder) -> dict[str, Any]:
    return {"cik": h.filer_cik, "name": h.filer_name, "source": h.winning_source, "accession": h.winning_accession}


def _census(conn: psycopg.Connection[Any], out: str, limit: int, shard: int, shards: int) -> int:
    """Which folds the loaded tier actually moves, by route, over the real read path.

    ⚠ The control arm is recomputed in-harness from ``_control_group_rep_key`` rather than
    read off the loaded selector: a census that reads its control off the function under
    test reports a clean result whether or not the fix landed (#2386 prevention entry)."""
    folds: list[dict[str, Any]] = []
    current: dict[str, Any] = {}
    route = {"name": "unknown"}
    real_collapse = orl._collapse_insider_control_group
    real_same = orl._reconcile_same_accession_groups
    real_cross = orl._reconcile_insider_control_groups

    def _routed(name: str, fn: Any) -> Any:
        def wrapper(*a: Any, **kw: Any) -> Any:
            route["name"] = name
            try:
                return fn(*a, **kw)
            finally:
                route["name"] = "unknown"

        return wrapper

    def recording_collapse(cluster: list[orl.Holder], rows_by_identity: Any, evidence: Any = None) -> Any:
        # ``evidence`` defaults to None rather than to the module's empty mapping so this
        # wrapper still works against a checkout where ``_collapse_insider_control_group``
        # takes two arguments — the census is meant to be runnable at the control commit
        # too, and a TypeError there would read as "the tier does nothing".
        ev: Any = orl._NO_RECORD_HOLDER_EVIDENCE if evidence is None else evidence
        collapsed, correction = real_collapse(cluster, rows_by_identity, ev)
        incumbent = max(cluster, key=orl._control_group_rep_key)
        loaded = orl._select_control_group_rep(cluster, rows_by_identity, ev)
        named = orl._named_record_holder(cluster, ev)
        folds.append(
            {
                **current,
                "route": route["name"],
                "shares": str(cluster[0].shares),
                "cluster_size": len(cluster),
                "n_distinct_accessions": len({h.winning_accession for h in cluster}),
                "n_attested_direct": len(orl._attested_direct_holders(cluster)),
                "incumbent": _describe(incumbent),
                "loaded_rep": _describe(loaded),
                "named_record_holder": _describe(named) if named is not None else None,
                "moved": orl._identity_key(loaded.filer_cik, loaded.filer_name)
                != orl._identity_key(incumbent.filer_cik, incumbent.filer_name),
                "members": [_describe(h) for h in cluster],
            }
        )
        return collapsed, correction

    orl._collapse_insider_control_group = recording_collapse  # type: ignore[assignment]
    orl._reconcile_same_accession_groups = _routed("same_accession", real_same)  # type: ignore[assignment]
    orl._reconcile_insider_control_groups = _routed("cross_accession", real_cross)  # type: ignore[assignment]
    try:
        population = conn.execute(POPULATION_SQL).fetchall()
        if limit:
            population = population[:limit]
        population = [r for n, r in enumerate(population) if n % shards == shard]
        for n, (instrument_id, symbol) in enumerate(population):
            current = {"instrument_id": int(instrument_id), "symbol": str(symbol)}
            try:
                orl.get_ownership_rollup(conn, str(symbol), int(instrument_id))
            except Exception as exc:  # harness error — record, never silently drop
                folds.append({**current, "error": repr(exc)})
            if n % 100 == 0:
                print(f"{n}/{len(population)}", file=sys.stderr, flush=True)
    finally:
        orl._collapse_insider_control_group = real_collapse  # type: ignore[assignment]
        orl._reconcile_same_accession_groups = real_same  # type: ignore[assignment]
        orl._reconcile_insider_control_groups = real_cross  # type: ignore[assignment]

    with open(out, "w") as fh:
        for rec in folds:
            fh.write(json.dumps(rec) + "\n")
    good = [f for f in folds if "error" not in f]
    print(f"\ninstruments scanned : {len(population)}")
    print(f"harness errors      : {len(folds) - len(good)}")
    print(f"clusters folded     : {len(good)}")
    print("by route            :", dict(Counter(f["route"] for f in good)))
    named = [f for f in good if f["named_record_holder"]]
    moved = [f for f in good if f["moved"]]
    print(f"\ntext rule names a unique member : {len(named)}")
    print("   by route:", dict(Counter(f["route"] for f in named)))
    print(f"reps MOVED off the incumbent    : {len(moved)}")
    print("   by route:", dict(Counter(f["route"] for f in moved)))
    print("   instruments:", len({f["instrument_id"] for f in moved}))
    print("\nmovers (incumbent -> kept):")
    for f in moved:
        print(
            f"   {f['symbol']:6s} {f['route']:15s} {f['incumbent']['name'][:34]:34s} -> {f['loaded_rep']['name'][:40]}"
        )
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--validate", action="store_true", help="score the rule on the labelled cross-accession set")
    ap.add_argument("--census", action="store_true", help="run the real read path and record the folds it moves")
    ap.add_argument("--out", default="/tmp/audit2408.jsonl")
    ap.add_argument("--limit", type=int, default=0, help="0 = full population")
    ap.add_argument("--shard", type=int, default=0)
    ap.add_argument("--shards", type=int, default=1)
    args = ap.parse_args()
    if not (args.validate or args.census):
        ap.error("pass --validate and/or --census")
    with psycopg.connect(settings.database_url) as conn:
        if args.validate:
            _validate(conn)
        if args.census:
            _census(conn, args.out, args.limit, args.shard, args.shards)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
