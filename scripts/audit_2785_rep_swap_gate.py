"""Read-only census of the #2785 REP-SWAP refusals in :func:`_select_control_group_rep`.

#2230 narrowed the deemed-chain FOLD gate from "are any rows stranded" to "would the
stranded rows change the identity's wedge" (:func:`_releases_into_another_wedge`).
**Clause 4 of the rep selector still asks the wide predicate**
(:func:`_releases_other_rows`), so it declines rep swaps for the same non-hazard. #2785
asks for the arithmetic before the change, because a swap is not symmetric with a fold:
a fold removes members and can only shrink the identity's contribution, whereas a swap
changes WHICH identity survives into :func:`_reconcile_owner_once`.

⚠ **COMMIT-INDEPENDENT, and it has to be** — the #2230 census printed
``clusters reaching the release check: 0`` with exit 0 when its wrapper was bound to the
predicate the gate had stopped calling, which is indistinguishable from a clean
population. This script never reads a verdict off the loaded code. It runs the REAL
:func:`_select_control_group_rep` three times per cluster with the clause-4 predicate
rebound, and reads the answer off which ``Holder`` comes back:

* ``ungated`` — predicate forced ``False``: the candidate the clauses 1-3/5 chose, i.e.
  what the selector would return with clause 4 removed entirely. A cluster where this
  differs from the incumbent is one where **clause 4 was reached**.
* ``wide``    — the checkout's own clause-4 predicate. This is production behaviour.
* ``narrow``  — predicate rebound to :func:`_releases_into_another_wedge`, the #2230 rule.

Re-spelling the selector's clauses here instead would reintroduce exactly the drift the
PR #2384 review WARNING calls out; running the real function under each arm cannot drift.

Three cross-checks are printed rather than assumed, because each is a way the harness
could be silently measuring nothing:

* ``ungated == incumbent`` on every cluster would mean clause 4 is never reached;
* ``wide != ungated`` must imply ``wide == incumbent`` (clause 4 only ever refuses);
* a flip (``narrow != wide``) must have an incumbent whose stranded rows are ALL
  ``_INSIDER_SOURCES`` — that is the whole content of the narrowing, so a flip without
  one is a harness fault, not a finding.

⚠ **Route matters and is recorded.** ``_select_control_group_rep`` is reached from two
passes. :func:`_reconcile_insider_control_groups` gates the deemed-chain route on the
fold-release check, which exempts the rep — so changing the rep changes which member the
gate exempts. :func:`_reconcile_same_accession_groups` and the value-proxy route inside
the same reconcile have **no** release gate at all, so clause 4 is their only release
protection. The counts are reported per route.

This script measures; it does not judge. The pie arithmetic of the flips needs the paired
full-population A/B (``scripts.ab_2230_deemed_chain`` + ``scripts.ab_2230_compare``),
whose population SQL is the same equal-value-cluster cohort reused here.

Usage:

    PYTHONPATH=. uv run python -m scripts.audit_2785_rep_swap_gate \
        --out /tmp/audit2785_rep_swap.jsonl
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
from scripts.audit_2230_release_hazard import _cluster_key, _describe


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--limit", type=int, default=0, help="0 = full population")
    ap.add_argument("--shard", type=int, default=0)
    ap.add_argument("--shards", type=int, default=1)
    args = ap.parse_args()

    records: dict[str, dict[str, Any]] = {}
    current: dict[str, Any] = {}

    real_wide = orl._releases_other_rows
    real_narrow = orl._releases_into_another_wedge
    real_rep = orl._select_control_group_rep
    real_same_acc = orl._reconcile_same_accession_groups
    real_control_groups = orl._reconcile_insider_control_groups
    route = {"name": "unknown"}

    def _never(_h: orl.Holder, _c: Any, _r: Any) -> bool:
        return False

    def recording_rep(cluster: Any, rows_by_identity: Any, record_holder_evidence: Any) -> orl.Holder:
        members = list(cluster)
        # Production arm FIRST and returned unchanged — the two extra arms must not be
        # able to influence what the rollup actually computes.
        wide_rep = real_rep(members, rows_by_identity, record_holder_evidence)
        orl._releases_other_rows = _never  # type: ignore[assignment]
        try:
            ungated_rep = real_rep(members, rows_by_identity, record_holder_evidence)
        finally:
            orl._releases_other_rows = real_wide  # type: ignore[assignment]
        orl._releases_other_rows = real_narrow  # type: ignore[assignment]
        try:
            narrow_rep = real_rep(members, rows_by_identity, record_holder_evidence)
        finally:
            orl._releases_other_rows = real_wide  # type: ignore[assignment]

        key = _cluster_key(int(current["instrument_id"]), members)
        if key not in records:
            incumbent = max(members, key=orl._control_group_rep_key)
            inc_key = orl._identity_key(incumbent.filer_cik, incumbent.filer_name)
            in_cluster = {id(h) for h in members}
            stranded = [r for r in rows_by_identity.get(inc_key, ()) if id(r) not in in_cluster]
            # The narrowing's whole content: an identity retaining ANY insider row outside
            # the cluster is still Section-16 in ``_reconcile_owner_once``, so its 13F
            # stays a ``dropped_source`` and demoting it changes no wedge.
            keeps_insider = any(r.winning_source in orl._INSIDER_SOURCES for r in stranded)

            def ident(h: orl.Holder) -> str:
                return orl._identity_key(h.filer_cik, h.filer_name)

            reached = ident(ungated_rep) != inc_key
            records[key] = {
                **current,
                "route": route["name"],
                "shares": str(members[0].shares if members else 0),
                "n_members": len(members),
                "n_identities": len({ident(h) for h in members}),
                "incumbent": _describe(incumbent),
                "ungated_rep": _describe(ungated_rep),
                "wide_rep": _describe(wide_rep),
                "narrow_rep": _describe(narrow_rep),
                # Clause 4 was reached iff clauses 1-3/5 produced a candidate of a
                # different identity — which is exactly "ungated disagrees with incumbent".
                "clause4_reached": reached,
                "declined_today": reached and ident(wide_rep) == inc_key,
                "flips_under_narrow": ident(narrow_rep) != ident(wide_rep),
                "incumbent_stranded": [{"source": str(r.winning_source), "shares": str(r.shares)} for r in stranded],
                "incumbent_keeps_insider_row": keeps_insider,
                "wide_refused": reached and ident(wide_rep) == inc_key,
                "narrow_refused": reached and ident(narrow_rep) == inc_key,
            }
        return wide_rep

    def wrap_route(fn: Any, name: str) -> Any:
        def wrapped(*a: Any, **kw: Any) -> Any:
            prev, route["name"] = route["name"], name
            try:
                return fn(*a, **kw)
            finally:
                route["name"] = prev

        return wrapped

    orl._select_control_group_rep = recording_rep  # type: ignore[assignment]
    orl._reconcile_same_accession_groups = wrap_route(real_same_acc, "same_accession")  # type: ignore[assignment]
    orl._reconcile_insider_control_groups = wrap_route(real_control_groups, "value_bucket")  # type: ignore[assignment]
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
                    records[f"ERR|{instrument_id}"] = {**current, "error": repr(exc)}
                if n % 100 == 0:
                    print(f"{n}/{len(population)}", file=sys.stderr, flush=True)
    finally:
        orl._select_control_group_rep = real_rep  # type: ignore[assignment]
        orl._reconcile_same_accession_groups = real_same_acc  # type: ignore[assignment]
        orl._reconcile_insider_control_groups = real_control_groups  # type: ignore[assignment]
        orl._releases_other_rows = real_wide  # type: ignore[assignment]

    with open(args.out, "w") as fh:
        for rec in records.values():
            fh.write(json.dumps(rec) + "\n")

    errors = [e for e in records.values() if "error" in e]
    good = [e for e in records.values() if "error" not in e]
    reached = [e for e in good if e["clause4_reached"]]
    declined = [e for e in good if e["declined_today"]]
    flips = [e for e in good if e["flips_under_narrow"]]

    print(f"\ninstruments scanned                        : {len(population)}")
    print(f"harness errors                             : {len(errors)}")
    print(f"clusters reaching the rep selector         : {len(good)}")
    print(f"   … routes                                : {dict(Counter(str(e['route']) for e in good))}")
    print(f"   … clause 4 REACHED (candidate != incumbent): {len(reached)}")
    print(f"   … clause 4 DECLINES today (wide)        : {len(declined)}")
    print(
        f"   … of those, incumbent keeps an insider row outside the cluster: "
        f"{sum(1 for e in declined if e['incumbent_keeps_insider_row'])}"
    )
    print(f"   … FLIPS under the narrow predicate      : {len(flips)}")
    print(f"   … flip routes                           : {dict(Counter(str(e['route']) for e in flips))}")
    if flips:
        print("   … flip symbols:", sorted({str(e["symbol"]) for e in flips})[:40])
        print(f"   … flip instruments                      : {len({e['instrument_id'] for e in flips})}")
        moved = sum(Decimal(e["shares"]) for e in flips)
        print(f"   … block value under the flipped clusters: {moved:,} shares")

    # --- Cross-checks. Each is a way this harness could be measuring nothing. ---------
    never_reached = len(reached) == 0
    # Clause 4 only ever REFUSES a swap; it can never invent one the clauses above did not
    # produce. So a wide verdict differing from ungated must be the incumbent.
    bad_refusal = [
        e
        for e in good
        if e["wide_rep"]["cik"] != e["ungated_rep"]["cik"] and e["wide_rep"]["cik"] != e["incumbent"]["cik"]
    ]
    # A flip is the narrowing's whole content: it must rest on an incumbent whose stranded
    # rows are all insider-source rows. A flip without one is a harness fault.
    bad_flip = [e for e in flips if not e["incumbent_keeps_insider_row"]]
    print(f"\ncross-check — clause 4 never reached (harness blind)   : {never_reached}")
    print(f"cross-check — wide verdict neither ungated nor incumbent: {len(bad_refusal)}")
    print(f"cross-check — flip without a retained insider row       : {len(bad_flip)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
