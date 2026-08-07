"""Read-only census of the control-group representative choice (#2385).

Step 1 of #2385: the ticket proposes preferring the Rule 16a-1(a)(2) ``direct`` holder
as a folded cluster's representative, but states its own premise as UNMEASURED —
``_DEEMED_CHAIN_MAX_DIRECT`` is ``<= 1``, not ``== 1``, so an all-``indirect`` chain is
admissible and has no direct member to prefer. This script measures, over the full
population rather than the single ``XFOR`` case the ticket reasons from:

* how many clusters :func:`_reconcile_insider_control_groups` actually folds;
* the ``n_direct`` distribution over them (0 vs 1 vs >1 — the value-proxy tier does not
  gate on nature at all, so >1 is possible there);
* in how many the CURRENT representative differs from the direct member;
* whether the `direct` string can be trusted at all — see :func:`_ungated_rep_key`.

Method: wrap the module's own :func:`_collapse_insider_control_group` and run the REAL
:func:`get_ownership_rollup` over the same population the #2230 A/B uses. Nothing is
re-derived — the folds recorded are the folds the read path performs, and the tier is
recomputed from the module's own constants (:data:`_INSIDER_GROUP_MIN_SHARES`,
:func:`_is_group_block`) rather than a copy of the thresholds.

Usage:

    PYTHONPATH=. uv run python -m scripts.audit_2385_control_group_rep \
        --out /tmp/audit2385.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from decimal import Decimal
from typing import Any

import psycopg

from app.config import settings
from app.services import ownership_rollup as orl
from scripts.ab_2230_deemed_chain import POPULATION_SQL


def _ungated_rep_key(h: orl.Holder) -> tuple[bool, bool, Decimal, str, str]:
    """The key #2385 proposes **literally**: the existing key with
    ``ownership_nature == "direct"`` prepended below the insider-source preference,
    reading the string with no regard for where it came from.

    **Nothing here ships**, because the census falsified this key on two independent
    counts (both written up on #2385, 2026-08-07):

    1. ``ownership_nature`` is Form 4 Table I column 5 only on rows from the three XML
       ingest paths. ``sec_insider_dataset_ingest._map_relationship`` maps the DERA
       dataset's relationship flags onto the same column (officer/director → ``direct``,
       ten-percent-owner → ``beneficial``). This key moves **224** of 1,433 folds and
       **59** of them promote a role-derived row — an officer labelling a fund's block,
       the exact defect #2385 exists to remove. Gating on the row's provenance
       (``source_document_id !~ ':(NDT|NDH):'``) reduces it to 200.
    2. Even provenance-gated, the change is **not display-only**, which is what #2385
       assumed. The rep is exempt from the release-hazard check in
       ``_reconcile_insider_control_groups`` AND it is the identity that survives into
       ``_reconcile_owner_once``, so changing it re-parents the folded members'
       other-channel rows. Paired full-population A/B: **108 instruments' pie totals
       move, 1,308,852,857 shares net removed** — mostly right (``USBC`` insiders
       737,776,188 → 379,961,188) but wrong on 19 (``ACDC`` 225,951,558 → 298,774,575).

    ``rep_now`` is always the LOADED module's key, so running this script at any commit
    compares "what that commit does" against "what the ticket literally proposed"."""
    return (
        h.winning_source in orl._INSIDER_GROUP_SOURCES,
        h.ownership_nature == "direct",
        h.shares,
        h.filer_cik or "",
        h.winning_accession,
    )


def _tier(cluster: list[orl.Holder], route: str) -> str:
    """Which route admitted this cluster.

    ⚠ :func:`_collapse_insider_control_group` has TWO callers — the same-accession pass
    (#1764, ``app/services/ownership_rollup.py:2675``) and the cross-accession pass
    (#1652/#2230, ``:2820``) — and the same-accession one runs FIRST. A census that
    re-derives the tier from the value thresholds alone therefore mislabels every
    same-accession fold: measured on a 40-instrument probe, an all-``direct``,
    zero-``indirect`` AMR cluster was reported as ``deemed_chain`` when
    :func:`_is_deemed_chain` could not possibly have admitted it (it requires
    ``n_indirect >= 2``). ``route`` is captured from the caller instead; the value
    thresholds only split the cross-accession route's own two tiers."""
    if route != "cross_accession":
        return route
    distinct_ciks = {h.filer_cik for h in cluster}
    shares = cluster[0].shares
    value_proxies = shares >= orl._INSIDER_GROUP_MIN_SHARES and orl._is_group_block(shares)
    if len(distinct_ciks) >= 2 and value_proxies:
        return "value_proxy"
    return "deemed_chain"


def _describe(h: orl.Holder) -> dict[str, Any]:
    return {
        "cik": h.filer_cik,
        "name": h.filer_name,
        "nature": h.ownership_nature,
        "source": h.winning_source,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--limit", type=int, default=0, help="0 = full population")
    ap.add_argument("--shard", type=int, default=0)
    ap.add_argument("--shards", type=int, default=1)
    args = ap.parse_args()

    folds: list[dict[str, Any]] = []
    current: dict[str, Any] = {}
    route = {"name": "unknown"}
    real_collapse = orl._collapse_insider_control_group
    real_same_accession = orl._reconcile_same_accession_groups
    real_cross_accession = orl._reconcile_insider_control_groups

    def _routed(name: str, fn: Any) -> Any:
        def wrapper(*a: Any, **kw: Any) -> Any:
            route["name"] = name
            try:
                return fn(*a, **kw)
            finally:
                route["name"] = "unknown"

        return wrapper

    def recording_collapse(cluster: list[orl.Holder]) -> tuple[orl.Holder, orl.CorrectionApplied]:
        collapsed, correction = real_collapse(cluster)
        insiders = [h for h in cluster if h.winning_source in orl._INSIDER_GROUP_SOURCES]
        rep_now = max(cluster, key=orl._control_group_rep_key)
        rep_ungated = max(cluster, key=_ungated_rep_key)
        directs = [h for h in cluster if h.ownership_nature == "direct"]
        folds.append(
            {
                **current,
                "shares": str(cluster[0].shares),
                "tier": _tier(cluster, route["name"]),
                "cluster_size": len(cluster),
                "n_insider_members": len(insiders),
                "n_direct": sum(1 for h in cluster if h.ownership_nature == "direct"),
                "n_indirect": sum(1 for h in cluster if h.ownership_nature == "indirect"),
                "n_other_nature": sum(1 for h in cluster if h.ownership_nature not in ("direct", "indirect")),
                "n_direct_insider_only": sum(1 for h in insiders if h.ownership_nature == "direct"),
                "rep_now": _describe(rep_now),
                "rep_ungated": _describe(rep_ungated),
                "rep_changes": orl._identity_key(rep_now.filer_cik, rep_now.filer_name)
                != orl._identity_key(rep_ungated.filer_cik, rep_ungated.filer_name),
                "directs": [_describe(h) for h in directs],
            }
        )
        return collapsed, correction

    orl._collapse_insider_control_group = recording_collapse  # type: ignore[assignment]
    orl._reconcile_same_accession_groups = _routed("same_accession", real_same_accession)  # type: ignore[assignment]
    orl._reconcile_insider_control_groups = _routed("cross_accession", real_cross_accession)  # type: ignore[assignment]
    try:
        with psycopg.connect(settings.database_url) as conn:
            population = conn.execute(POPULATION_SQL).fetchall()
            if args.limit:
                population = population[: args.limit]
            population = [r for n, r in enumerate(population) if n % args.shards == args.shard]
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
        orl._reconcile_same_accession_groups = real_same_accession  # type: ignore[assignment]
        orl._reconcile_insider_control_groups = real_cross_accession  # type: ignore[assignment]

    with open(args.out, "w") as fh:
        for rec in folds:
            fh.write(json.dumps(rec) + "\n")

    errors = [f for f in folds if "error" in f]
    good = [f for f in folds if "error" not in f]
    print(f"\ninstruments scanned      : {len(population)}")
    print(f"harness errors           : {len(errors)}")
    print(f"clusters folded          : {len(good)}")
    print(f"instruments with a fold  : {len({f['instrument_id'] for f in good})}")
    tiers = sorted({f["tier"] for f in good})
    print("\nby tier:", dict(Counter(f["tier"] for f in good)))
    # ``n_direct_insider_only`` is the figure the Rule 16a-1(a)(2) argument is about.
    # Nature is meaningful only on Section 16 rows; a 13D/G member in the same value
    # bucket carries its own nature and can never win the rep (insider source is the
    # key's first component), so the whole-cluster count is reported but not decisive.
    all_nd = Counter(f["n_direct_insider_only"] for f in good)
    print("n_direct (insider members only), all folds:", dict(sorted(all_nd.items())))
    for t in tiers:
        by_t = Counter(f["n_direct_insider_only"] for f in good if f["tier"] == t)
        print(f"   ... {t:16s}:", dict(sorted(by_t.items())))
    changed = [f for f in good if f["rep_changes"]]
    print(f"\nfolds the UNGATED key would move : {len(changed)} of {len(good)}")
    print("   by tier:", dict(Counter(f["tier"] for f in changed)))
    print("   rep_now nature:", dict(Counter(str(f["rep_now"]["nature"]) for f in changed)))
    print("   instruments affected:", len({f["instrument_id"] for f in changed}))
    have_direct = [f for f in good if f["n_direct_insider_only"] >= 1]
    print(f"\nfolds with >=1 direct insider member : {len(have_direct)}")
    already = sum(1 for f in have_direct if f["rep_now"]["nature"] == "direct")
    print(f"   of those, rep already direct       : {already}")
    print(f"folds with ZERO direct insider member: {len(good) - len(have_direct)}   (rep unchanged by construction)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
