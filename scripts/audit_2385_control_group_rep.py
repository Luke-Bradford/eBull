"""Read-only census of the control-group representative choice (#2385).

#2385 proposes preferring the Rule 16a-1(a)(2) ``direct`` holder as a folded cluster's
representative instead of the incumbent key's highest-CIK member. This script prices that
proposal and each guard the measurement forced onto it, over the full population rather
than the single ``XFOR`` case the ticket reasons from.

One arm per clause, so the summary says which guard does the work instead of reporting a
combined total:

* ``rep_now`` — the pre-#2385 rule, spelled from :func:`orl._control_group_rep_key`.
* ``rep_ungated`` — what the ticket proposed LITERALLY (see :func:`_ungated_rep_key`).
* ``rep_rung1`` — the SHIPPED rule: all three clauses.
* ``rep_rung1_ungoverned`` / ``_any_accession`` / ``_multi_direct`` — the shipped rule
  minus the release guard / the accession clause / the one-direct-holder clause.
* ``rep_rung2`` — attested-any, MEASURED BUT NOT SHIPPED (see :func:`_attested_any`).
* ``rep_loaded`` — what this checkout actually does, reconciled in the summary.

⚠ That last line is the point of the layout. A census that reads its control arm off the
loaded function reports a clean result whether or not the fix landed (#2386 prevention
entry), so every arm is computed in-harness from the module's constants and the loaded
function's verdict is printed as a separate agreement count.

Method: wrap the module's own :func:`_collapse_insider_control_group` and run the REAL
:func:`get_ownership_rollup` over the same population the #2230 A/B uses. Nothing is
re-derived — the folds recorded are the folds the read path performs, and the tier is
captured from the CALLER (:func:`_tier`) rather than re-derived from the thresholds.

Usage (~4 min sharded 3 ways over 2,179 instruments):

    PYTHONPATH=. uv run python -m scripts.audit_2385_control_group_rep \
        --out /tmp/audit2385.jsonl --shard 0 --shards 3
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
        "table_i": h.nature_from_table_i,
    }


def _attested_any(cluster: list[orl.Holder]) -> list[orl.Holder]:
    """RUNG 2, measured but NOT shipped unless the numbers justify it: any Section 16
    member whose ``ownership_nature`` came off a Table I line at all.

    The case for it is #2386's ``ACTU`` — a 24-CIK Bios chain where every Table I line in
    the filing reads ``I``, so `orl._attested_direct_holders` is empty and the incumbent key
    keeps a DERA role-derived ``:NDT:`` row that carries no Table I evidence whatsoever.
    The case against is that the source rule names no member here: an ``I`` filer is not
    the holder of record either, so this rung is a tie-break preference, not a source
    rule, and it must earn its place on measured impact."""
    return [h for h in cluster if h.winning_source in orl._INSIDER_GROUP_SOURCES and h.nature_from_table_i]


def _rung(
    cluster: list[orl.Holder],
    pool: list[orl.Holder],
    incumbent: orl.Holder,
    *,
    governed: bool,
    index: Any,
    unique: bool = False,
    same_accession_ok: bool = True,
) -> orl.Holder:
    """One arm, computed IN-HARNESS from the module's own key and predicates rather than
    read off the loaded function — otherwise the control arm silently becomes the
    treatment arm the moment the fix lands (#2386 prevention entry).

    ``unique`` requires exactly one candidate (Rule 16a-1(a)(2)'s one-direct-holder shape);
    ``same_accession_ok=False`` adds the accession clause. Each guard is a separate flag so
    the summary can price it rather than report only the combined rule."""
    if not pool or (unique and len(pool) != 1):
        return incumbent
    candidate = max(pool, key=orl._control_group_rep_key)
    if not same_accession_ok and candidate.winning_accession == incumbent.winning_accession:
        return incumbent
    if orl._identity_key(candidate.filer_cik, candidate.filer_name) == orl._identity_key(
        incumbent.filer_cik, incumbent.filer_name
    ):
        return incumbent
    if governed and orl._releases_other_rows(incumbent, cluster, index):
        return incumbent
    return candidate


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

    def recording_collapse(
        cluster: list[orl.Holder], rows_by_identity: Any
    ) -> tuple[orl.Holder, orl.CorrectionApplied]:
        collapsed, correction = real_collapse(cluster, rows_by_identity)
        insiders = [h for h in cluster if h.winning_source in orl._INSIDER_GROUP_SOURCES]
        # ARM 0 — the pre-#2385 rule, spelled from the module's key rather than read off
        # the loaded collapse, so this stays the CONTROL at every commit.
        rep_now = max(cluster, key=orl._control_group_rep_key)
        rep_ungated = max(cluster, key=_ungated_rep_key)
        directs = [h for h in cluster if h.ownership_nature == "direct"]
        attested_direct = orl._attested_direct_holders(cluster)
        attested_any = _attested_any(cluster)
        exposed = orl._releases_other_rows(rep_now, cluster, rows_by_identity)
        # ARM 1 = the SHIPPED rule (all three guards). ARMs 1u / 1a / 1m each drop exactly
        # one guard, so the summary prices each rather than reporting only the total.
        arm = dict(index=rows_by_identity)
        rep_r1 = _rung(cluster, attested_direct, rep_now, governed=True, unique=True, same_accession_ok=False, **arm)
        rep_r1u = _rung(cluster, attested_direct, rep_now, governed=False, unique=True, same_accession_ok=False, **arm)
        rep_r1a = _rung(cluster, attested_direct, rep_now, governed=True, unique=True, same_accession_ok=True, **arm)
        rep_r1m = _rung(cluster, attested_direct, rep_now, governed=True, unique=False, same_accession_ok=False, **arm)
        rep_r2 = _rung(cluster, attested_any, rep_now, governed=True, unique=True, same_accession_ok=False, **arm)
        # ARM 3 — what the LOADED module actually does. Reconciled in the summary so a
        # reader can tell which arm this checkout implements instead of assuming (#2386).
        rep_loaded = orl._select_control_group_rep(cluster, rows_by_identity)

        def _same(a: orl.Holder, b: orl.Holder) -> bool:
            return orl._identity_key(a.filer_cik, a.filer_name) == orl._identity_key(b.filer_cik, b.filer_name)

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
                "n_attested_direct": len(attested_direct),
                "n_attested_any": len(attested_any),
                "n_distinct_accessions": len({h.winning_accession for h in cluster}),
                "n_role_derived": sum(1 for h in insiders if not h.nature_from_table_i),
                "incumbent_release_exposed": exposed,
                "rep_now": _describe(rep_now),
                "rep_ungated": _describe(rep_ungated),
                "rep_rung1": _describe(rep_r1),
                "rep_rung1_ungoverned": _describe(rep_r1u),
                "rep_rung1_any_accession": _describe(rep_r1a),
                "rep_rung1_multi_direct": _describe(rep_r1m),
                "rep_rung2": _describe(rep_r2),
                "rep_loaded": _describe(rep_loaded),
                "rep_changes": not _same(rep_now, rep_ungated),
                "rung1_changes": not _same(rep_now, rep_r1),
                "rung1_ungoverned_changes": not _same(rep_now, rep_r1u),
                "rung1_declined": (not _same(rep_now, rep_r1u)) and _same(rep_now, rep_r1),
                "rung1_any_accession_changes": not _same(rep_now, rep_r1a),
                "accession_clause_blocks": (not _same(rep_now, rep_r1a)) and _same(rep_now, rep_r1),
                "rung1_multi_direct_changes": not _same(rep_now, rep_r1m),
                "unique_clause_blocks": (not _same(rep_now, rep_r1m)) and _same(rep_now, rep_r1),
                "rung2_changes": not _same(rep_now, rep_r2),
                "rung2_beyond_rung1": (not _same(rep_now, rep_r2)) and _same(rep_now, rep_r1),
                "loaded_is_rung1": _same(rep_loaded, rep_r1),
                "loaded_is_now": _same(rep_loaded, rep_now),
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

    print("\n--- rung structure (#2385 decline-on-release-exposure design) ---")
    print(f"folds with >=1 ATTESTED direct member : {sum(1 for f in good if f['n_attested_direct'] >= 1)}")
    print(f"folds with >=1 attested member at all : {sum(1 for f in good if f['n_attested_any'] >= 1)}")
    print(f"folds with a role-derived member      : {sum(1 for f in good if f['n_role_derived'] >= 1)}")
    print(f"folds whose incumbent is release-EXPOSED : {sum(1 for f in good if f['incumbent_release_exposed'])}")
    # The accession clause exists because Table I attestation is assigned to ``filers[0]``
    # (`insider_transactions.py:449`), so within one accession it ranks by XML listing
    # order. This is the number that shows the clause is not decorative.
    print(f"\nfolds spanning >1 accession : {sum(1 for f in good if f['n_distinct_accessions'] > 1)}")
    for t in tiers:
        sub = [f for f in good if f["tier"] == t]
        print(
            f"   ... {t:16s}: {len(sub):4d} folds, "
            f"{sum(1 for f in sub if f['n_attested_any'] >= 2):4d} with >=2 attested members"
        )
    r1u = [f for f in good if f["rung1_ungoverned_changes"]]
    r1 = [f for f in good if f["rung1_changes"]]
    r1a = [f for f in good if f["rung1_any_accession_changes"]]
    dec = [f for f in good if f["rung1_declined"]]
    blocked = [f for f in good if f["accession_clause_blocks"]]
    print(f"\nRUNG 1 without the ACCESSION clause moves                  : {len(r1a)}")
    print("   by tier:", dict(Counter(f["tier"] for f in r1a)))
    print(f"   swaps the accession clause BLOCKS                      : {len(blocked)}")
    print("   by tier:", dict(Counter(f["tier"] for f in blocked)))
    r1m = [f for f in good if f["rung1_multi_direct_changes"]]
    ublocked = [f for f in good if f["unique_clause_blocks"]]
    print(f"RUNG 1 without the ONE-DIRECT-HOLDER clause moves          : {len(r1m)}")
    print(f"   swaps the one-direct-holder clause BLOCKS              : {len(ublocked)}")
    print("   by tier:", dict(Counter(f["tier"] for f in ublocked)))
    print(f"RUNG 1 without the RELEASE guard moves                     : {len(r1u)}")
    print(f"RUNG 1 SHIPPED (accession clause + release guard)          : {len(r1)}")
    print("   by tier:", dict(Counter(f["tier"] for f in r1)))
    print(f"   swaps the release guard DECLINES                       : {len(dec)}")
    print("   instruments moved by the shipped rule:", len({f["instrument_id"] for f in r1}))
    r2 = [f for f in good if f["rung2_beyond_rung1"]]
    print(f"\nRUNG 2 adds beyond rung 1 (attested-any over role-derived) : {len(r2)}")
    print("   by tier:", dict(Counter(f["tier"] for f in r2)))
    print("   instruments:", len({f["instrument_id"] for f in r2}))
    print(
        "   rep_now nature/provenance:",
        dict(Counter(f"{f['rep_now']['nature']}/{f['rep_now']['table_i']}" for f in r2)),
    )
    # Which arm does THIS checkout implement? Printed, not assumed — a census that reads
    # its control off the loaded function reports a clean result either way (#2386).
    print(
        f"\nloaded _select_control_group_rep agrees with RUNG 1 GOVERNED : "
        f"{sum(1 for f in good if f['loaded_is_rung1'])}/{len(good)}"
    )
    print(
        f"loaded _select_control_group_rep agrees with the OLD key      : "
        f"{sum(1 for f in good if f['loaded_is_now'])}/{len(good)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
