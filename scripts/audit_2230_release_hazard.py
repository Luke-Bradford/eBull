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
gate has already returned ``True``. :func:`_releases_other_rows` is the only function
that sees both the refused cluster and the ``rows_by_identity`` index needed to price the
refusal, so it is what this script wraps.

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

    real_release = orl._releases_other_rows
    real_gate = orl._is_deemed_chain
    real_rep = orl._select_control_group_rep
    last_rep: dict[str, Any] = {}
    # ⚠ ``_releases_other_rows`` has TWO callers, and only one is this census's subject.
    # ``_select_control_group_rep`` clause 4 calls it to gate a rep SWAP
    # (``ownership_rollup.py:2939``); the deemed-chain release hazard calls it to gate the
    # FOLD (``:3258``). Recording both conflates a swap refusal with a fold refusal, and
    # additionally corrupts the rep capture — the selector's own call arrives *before* it
    # has returned, so ``last_rep`` still holds the previous cluster's rep. The first
    # smoke run reported 3 clusters, 0 of them gate-admitted and 3/3 with a rep outside
    # their own cluster, which is what that looks like.
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

    def recording_release(
        holder: orl.Holder,
        cluster: Any,
        rows_by_identity: Any,
    ) -> bool:
        verdict = real_release(holder, cluster, rows_by_identity)
        if in_rep_selector["depth"]:
            return verdict  # rep-swap gate, not the fold gate — see the note above
        members = list(cluster)
        key = _cluster_key(int(current["instrument_id"]), members)
        if key not in records:
            rep = last_rep.get("rep")
            rep_key = orl._identity_key(rep.filer_cik, rep.filer_name) if rep is not None else None
            in_cluster = {id(h) for h in members}
            shares = members[0].shares if members else Decimal(0)
            per_member: list[dict[str, Any]] = []
            released_shares = Decimal(0)
            released_by_source: Counter[str] = Counter()
            n_releasing = 0
            for m in members:
                m_key = orl._identity_key(m.filer_cik, m.filer_name)
                if m_key == rep_key:
                    continue
                stranded = [r for r in rows_by_identity.get(m_key, ()) if id(r) not in in_cluster]
                if stranded:
                    n_releasing += 1
                for r in stranded:
                    released_shares += r.shares
                    released_by_source[str(r.winning_source)] += 1
                per_member.append(
                    {
                        **_describe(m),
                        "releases": bool(stranded),
                        "stranded": [{"source": str(r.winning_source), "shares": str(r.shares)} for r in stranded],
                    }
                )
            # The pie carries the block once per member today; folding leaves one.
            residual_double_count = shares * (len(members) - 1)
            records[key] = {
                **current,
                "shares": str(shares),
                "n_members": len(members),
                "rep": _describe(rep) if rep is not None else None,
                "rep_in_cluster": bool(rep is not None and any(id(h) == id(rep) for h in members)),
                "n_releasing_non_rep": n_releasing,
                "refused": n_releasing > 0,
                "residual_double_count": str(residual_double_count),
                "released_shares": str(released_shares),
                "net_pie_delta_if_folded": str(released_shares - residual_double_count),
                "released_by_source": dict(released_by_source),
                "members": per_member,
            }
        return verdict

    orl._releases_other_rows = recording_release  # type: ignore[assignment]
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
        orl._releases_other_rows = real_release  # type: ignore[assignment]
        orl._is_deemed_chain = real_gate  # type: ignore[assignment]
        orl._select_control_group_rep = real_rep  # type: ignore[assignment]

    with open(args.out, "w") as fh:
        for rec in records.values():
            fh.write(json.dumps(rec) + "\n")

    errors = [e for e in records.values() if "error" in e]
    good = [e for e in records.values() if "error" not in e]
    refused = [e for e in good if e["refused"]]
    print(f"\ninstruments scanned                      : {len(population)}")
    print(f"harness errors                           : {len(errors)}")
    print(f"deemed-chain clusters ADMITTED by the gate: {len(admitted)}")
    print(f"   … instruments                         : {len({(e['instrument_id']) for e in admitted})}")
    print(f"clusters reaching the release check       : {len(good)}")
    print(f"   … REFUSED by the release hazard        : {len(refused)}")
    print(f"   … instruments affected                 : {len({e['instrument_id'] for e in refused})}")
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
