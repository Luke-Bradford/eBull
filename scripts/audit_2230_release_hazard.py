"""Read-only census of the #2230 RELEASE-HAZARD refusals in the deemed-chain tier.

``_reconcile_insider_control_groups`` admits a Rule 16a-1(a)(2) control chain on the
structured Form 3/4 fields (#2230), then **fails closed** if folding any non-rep member
would strand that identity's rows in other channels — the release hazard documented
inline at ``app/services/ownership_rollup.py``. Three session summaries have carried the
residual as "gated on revisiting #1652's exact-value-only consumption rule, a bigger
decision than this ticket" without ever measuring what the gate refuses or what the
refusal costs. This script measures both.

⚠ **Instrumentation point.** #2386's prevention entry records that a census wrapped on
``_collapse_insider_control_group`` is structurally blind to refusals, because a refused
cluster never reaches the collapser. The same trap sits one layer lower: wrapping
``_is_deemed_chain`` is blind to a RELEASE refusal, because that check runs *after* the
gate has already returned ``True``. The release predicate is the only function that sees
both the refused cluster and the ``rows_by_identity`` index needed to price the refusal,
so that is what this script wraps.

⚠⚠ **COMMIT-INDEPENDENT, and it has to be.** The first draft wrapped
``_releases_other_rows`` alone, which is the predicate the fold gate called *before* the
fix. After the fix the gate calls ``_releases_into_another_wedge``, the old wrapper is
reached only from inside ``_select_control_group_rep`` (where recording is deliberately
suppressed), and the census printed ``clusters reaching the release check: 0`` — **exit 0,
no error, indistinguishable from a clean population.** That is the same shape as the
``audit_2386`` trap: a broken measurement and a clean result are the same output. Caught by
Codex at checkpoint 2, not by any gate.

So BOTH predicates are wrapped, and neither verdict is taken from the loaded function.
Each member is scored under both counting rules here:

* ``wide``   — any row stranded outside the cluster (the pre-fix predicate);
* ``narrow`` — stranded AND none of the stranded rows is an ``_INSIDER_SOURCES`` row, i.e.
  the identity is left with nothing keeping it Section-16 in ``_reconcile_owner_once``.

``loaded_predicate`` records which one the checkout's fold gate actually invoked and is
reconciled in the summary. That line is what tells the reader which arm the code
implements, instead of leaving it to be assumed.

The wrapper computes the WHOLE cluster verdict on its first call for that cluster, rather
than reading it off the call sequence: the production loop ``break``s on the first
releasing member, so the real call sequence stops early and only ever names one of them.

Two figures per refused cluster, and the comparison between them is the finding:

* ``residual_double_count`` — ``(n_members - 1) * shares``, the deemed shares the pie
  carries more than once because the fold was refused.
* ``released_shares`` — what folding WOULD add to the pie, summed over the rows the
  demoted non-rep members hold in other channels.

Usage:

    PYTHONPATH=. uv run python -m scripts.audit_2230_release_hazard \
        --out /tmp/audit2230_release.jsonl
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


def _describe(h: orl.Holder) -> dict[str, Any]:
    return {
        "cik": h.filer_cik,
        "name": h.filer_name,
        "shares": str(h.shares),
        "nature": h.ownership_nature,
        "table_i": h.nature_from_table_i,
        "source": h.winning_source,
        "ten_pct": h.is_ten_percent_owner,
    }


def _cluster_key(instrument_id: int, cluster: list[orl.Holder]) -> str:
    """Stable per-cluster identity for dedup across the production loop's repeated
    calls. Keyed on the value bucket plus the member identity set — object ids are not
    usable because the same cluster is re-examined per member."""
    ids = sorted({orl._identity_key(h.filer_cik, h.filer_name) for h in cluster})
    return f"{instrument_id}|{cluster[0].shares if cluster else ''}|{','.join(ids)}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--limit", type=int, default=0, help="0 = full population")
    ap.add_argument("--shard", type=int, default=0)
    ap.add_argument("--shards", type=int, default=1)
    args = ap.parse_args()

    records: dict[str, dict[str, Any]] = {}
    current: dict[str, Any] = {}
    admitted: list[dict[str, Any]] = []

    real_wide = orl._releases_other_rows
    # Absent on a pre-#2230-residual checkout; wrapping is then a no-op and the summary's
    # reconciliation line reports ``wide``, which is the truthful answer for that commit.
    real_narrow = getattr(orl, "_releases_into_another_wedge", None)
    real_gate = orl._is_deemed_chain
    real_rep = orl._select_control_group_rep
    last_rep: dict[str, Any] = {}
    # ⚠ The release predicate has TWO callers, and only one is this census's subject.
    # ``_select_control_group_rep`` clause 4 calls it to gate a rep SWAP; the deemed-chain
    # release hazard calls it to gate the FOLD. Recording both conflates a swap refusal
    # with a fold refusal, and additionally corrupts the rep capture — the selector's own
    # call arrives *before* it has returned, so ``last_rep`` still holds the previous
    # cluster's rep. The first smoke run reported 3 clusters, 0 of them gate-admitted and
    # 3/3 with a rep outside their own cluster, which is what that looks like.
    in_rep_selector = {"depth": 0}

    def recording_gate(insiders: list[orl.Holder]) -> bool:
        verdict = real_gate(insiders)
        if verdict:
            admitted.append(
                {
                    **current,
                    "shares": str(insiders[0].shares) if insiders else None,
                    "n_insiders": len(insiders),
                }
            )
        return verdict

    def recording_rep(holders: Any, rows_by_identity: Any, record_holder_evidence: Any) -> orl.Holder:
        in_rep_selector["depth"] += 1
        try:
            rep = real_rep(holders, rows_by_identity, record_holder_evidence)
        finally:
            in_rep_selector["depth"] -= 1
        last_rep["rep"] = rep
        return rep

    def _record(cluster: Any, rows_by_identity: Any, loaded: str) -> None:
        """Score the whole cluster under BOTH counting rules. Neither verdict comes from
        the loaded predicate — see the commit-independence note in the module docstring."""
        members = list(cluster)
        key = _cluster_key(int(current["instrument_id"]), members)
        if key in records:
            records[key].setdefault("loaded_predicate", loaded)
            return
        rep = last_rep.get("rep")
        rep_key = orl._identity_key(rep.filer_cik, rep.filer_name) if rep is not None else None
        in_cluster = {id(h) for h in members}
        shares = members[0].shares if members else Decimal(0)
        per_member: list[dict[str, Any]] = []
        released_shares = Decimal(0)
        released_by_source: Counter[str] = Counter()
        n_wide = n_narrow = 0
        for m in members:
            m_key = orl._identity_key(m.filer_cik, m.filer_name)
            if m_key == rep_key:
                continue
            stranded = [r for r in rows_by_identity.get(m_key, ()) if id(r) not in in_cluster]
            # WIDE: anything stranded. NARROW: stranded AND nothing left keeping this
            # identity Section-16 for ``_reconcile_owner_once``.
            wide = bool(stranded)
            narrow = wide and not any(r.winning_source in orl._INSIDER_SOURCES for r in stranded)
            n_wide += wide
            n_narrow += narrow
            for r in stranded:
                released_shares += r.shares
                released_by_source[str(r.winning_source)] += 1
            per_member.append(
                {
                    **_describe(m),
                    "releases": wide,
                    "releases_narrow": narrow,
                    "stranded": [{"source": str(r.winning_source), "shares": str(r.shares)} for r in stranded],
                }
            )
        # The pie carries the block once per member today; folding leaves one.
        residual_double_count = shares * (len(members) - 1)
        records[key] = {
            **current,
            "loaded_predicate": loaded,
            "shares": str(shares),
            "n_members": len(members),
            "rep": _describe(rep) if rep is not None else None,
            "rep_in_cluster": bool(rep is not None and any(id(h) == id(rep) for h in members)),
            "n_releasing_non_rep": n_wide,
            "n_releasing_non_rep_narrow": n_narrow,
            "refused": n_wide > 0,
            "refused_narrow": n_narrow > 0,
            "residual_double_count": str(residual_double_count),
            "released_shares": str(released_shares),
            "net_pie_delta_if_folded": str(released_shares - residual_double_count),
            "released_by_source": dict(released_by_source),
            "members": per_member,
        }

    def recording_wide(holder: orl.Holder, cluster: Any, rows_by_identity: Any) -> bool:
        verdict = real_wide(holder, cluster, rows_by_identity)
        if not in_rep_selector["depth"]:  # else: rep-swap gate, not the fold gate
            _record(cluster, rows_by_identity, "wide")
        return verdict

    def recording_narrow(holder: orl.Holder, cluster: Any, rows_by_identity: Any) -> bool:
        assert real_narrow is not None
        verdict = real_narrow(holder, cluster, rows_by_identity)
        if not in_rep_selector["depth"]:
            _record(cluster, rows_by_identity, "narrow")
        return verdict

    orl._releases_other_rows = recording_wide  # type: ignore[assignment]
    if real_narrow is not None:
        orl._releases_into_another_wedge = recording_narrow  # type: ignore[assignment]
    orl._is_deemed_chain = recording_gate  # type: ignore[assignment]
    orl._select_control_group_rep = recording_rep  # type: ignore[assignment]
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
        orl._releases_other_rows = real_wide  # type: ignore[assignment]
        if real_narrow is not None:
            orl._releases_into_another_wedge = real_narrow  # type: ignore[assignment]
        orl._is_deemed_chain = real_gate  # type: ignore[assignment]
        orl._select_control_group_rep = real_rep  # type: ignore[assignment]

    with open(args.out, "w") as fh:
        for rec in records.values():
            fh.write(json.dumps(rec) + "\n")

    errors = [e for e in records.values() if "error" in e]
    good = [e for e in records.values() if "error" not in e]
    refused = [e for e in good if e["refused"]]
    refused_narrow = [e for e in good if e["refused_narrow"]]
    # Which predicate the checkout's fold gate actually invoked. PRINTED, never inferred —
    # this is the line that distinguishes "the population is empty" from "the harness is
    # wrapping a function the gate no longer calls" (see the module docstring).
    loaded = Counter(str(e.get("loaded_predicate")) for e in good)
    print(f"\nfold gate invoked predicate              : {dict(loaded) or '{} (no cluster reached it)'}")
    print(f"instruments scanned                      : {len(population)}")
    print(f"harness errors                           : {len(errors)}")
    print(f"deemed-chain clusters ADMITTED by the gate: {len(admitted)}")
    print(f"   … instruments                         : {len({(e['instrument_id']) for e in admitted})}")
    print(f"clusters reaching the release check       : {len(good)}")
    print(f"   … REFUSED under the WIDE predicate     : {len(refused)}")
    print(f"   … REFUSED under the NARROW predicate   : {len(refused_narrow)}")
    print(f"   … flipped wide→admitted by NARROW      : {len(refused) - len(refused_narrow)}")
    print(f"   … instruments affected (wide)          : {len({e['instrument_id'] for e in refused})}")
    flipped = [e for e in refused if not e["refused_narrow"]]
    if flipped:
        unlocked = sum(Decimal(e["residual_double_count"]) for e in flipped)
        print(f"   … double-count the flips remove        : {unlocked:,} shares")
        print("   … flip symbols:", sorted({str(e["symbol"]) for e in flipped})[:40])
    if refused:
        resid = sum(Decimal(e["residual_double_count"]) for e in refused)
        rel = sum(Decimal(e["released_shares"]) for e in refused)
        print(f"\nresidual double-count left in place (shares): {resid:,}")
        print(f"shares that folding WOULD release           : {rel:,}")
        print(f"net pie delta if folded anyway              : {rel - resid:,}")
        shrink = [e for e in refused if Decimal(e["net_pie_delta_if_folded"]) < 0]
        grow = [e for e in refused if Decimal(e["net_pie_delta_if_folded"]) > 0]
        print(f"   clusters where folding SHRINKS the pie   : {len(shrink)}")
        print(f"   clusters where folding GROWS the pie     : {len(grow)}")
        by_source: Counter[str] = Counter()
        for e in refused:
            by_source.update(e["released_by_source"])
        print(f"\nstranded rows by source: {dict(by_source)}")
        print("   symbols:", sorted({str(e["symbol"]) for e in refused})[:40])
    bad_rep = [e for e in good if not e["rep_in_cluster"]]
    print(f"\nclusters where the captured rep is NOT a member (harness fault): {len(bad_rep)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
