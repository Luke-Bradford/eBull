"""Read-only census of the #2230 deemed-chain GATE under nature-provenance gating (#2386).

Step 1 of #2386. ``_is_deemed_chain`` counts ``n_direct``/``n_indirect`` off
``ownership_nature``, a column with four writers that do not agree on its meaning:
three XML paths derive it from Form 4/3 Table I column 5 ``directOrIndirectOwnership``
(``.claude/skills/data-sources/sec-edgar.md`` §2.3), while
``sec_insider_dataset_ingest._map_relationship`` maps the DERA insider dataset's
RELATIONSHIP flags onto it (officer/director → ``direct``, ten-percent-owner →
``beneficial``). A role-derived ``direct`` therefore counts against
``_DEEMED_CHAIN_MAX_DIRECT`` and can refuse a genuine control chain.

⚠ The measurement has to instrument the **gate**, not the collapse. A census wrapped on
``_collapse_insider_control_group`` (the #2385 shape) is structurally blind here: a
REFUSED cluster never reaches the collapser, and refusals are the whole population this
ticket is about. So this script wraps :func:`_is_deemed_chain` itself and evaluates both
counting rules on the identical input, in one process, on one DB read:

* ``now``    — nature counted over every member, as ``_is_deemed_chain`` did before this
  ticket;
* ``gated``  — the same thresholds with ``n_direct``/``n_indirect`` counted only over
  members whose nature is Table I-attested (``Holder.nature_from_table_i``), so a
  role-derived row counts toward NEITHER side, matching the posture the docstring
  already takes for legacy ``beneficial``.

Both are computed here from the module's own constants and preconditions, so the census
is COMMIT-INDEPENDENT — see the note in ``recording_gate`` for why taking ``now`` from
the loaded function instead would make the fix erase its own evidence. The loaded
function's verdict is recorded alongside and reconciled against both arms in the summary,
which is what tells you which arm the checkout implements.

Usage:

    PYTHONPATH=. uv run python -m scripts.audit_2386_deemed_chain_gate \
        --out /tmp/audit2386.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from typing import Any

import psycopg

from app.config import settings
from app.services import ownership_rollup as orl
from scripts.ab_2230_deemed_chain import POPULATION_SQL


def _counts(insiders: list[orl.Holder], *, table_i_only: bool) -> tuple[int, int]:
    """``(n_direct, n_indirect)`` under one counting rule. ``table_i_only`` skips any
    member whose nature came from the DERA relationship flags rather than Table I."""
    members = [h for h in insiders if h.nature_from_table_i] if table_i_only else insiders
    return (
        sum(1 for h in members if h.ownership_nature == "direct"),
        sum(1 for h in members if h.ownership_nature == "indirect"),
    )


def _shape_admits(n_direct: int, n_indirect: int) -> bool:
    """The ownership-form half of the gate, using the module's own constants."""
    return n_direct <= orl._DEEMED_CHAIN_MAX_DIRECT and n_indirect >= orl._DEEMED_CHAIN_MIN_INDIRECT


def _describe(h: orl.Holder) -> dict[str, Any]:
    return {
        "cik": h.filer_cik,
        "name": h.filer_name,
        "nature": h.ownership_nature,
        "table_i": h.nature_from_table_i,
        "source": h.winning_source,
        "ten_pct": h.is_ten_percent_owner,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--limit", type=int, default=0, help="0 = full population")
    ap.add_argument("--shard", type=int, default=0)
    ap.add_argument("--shards", type=int, default=1)
    args = ap.parse_args()

    evals: list[dict[str, Any]] = []
    current: dict[str, Any] = {}
    real_gate = orl._is_deemed_chain

    def recording_gate(insiders: list[orl.Holder]) -> bool:
        # ⚠ Both verdicts are COMPUTED here, not taken from the loaded function. Reading
        # ``verdict_now`` off ``real_gate`` makes the census a function of the commit it
        # runs at: after the fix lands, the loaded gate IS the provenance-gated one, so
        # "flips" collapses to 0 and the run reads as "no change" — a false negative that
        # looks exactly like a clean result. ``loaded_gate`` is recorded separately and
        # reconciled at the end, which turns that trap into a printed statement of which
        # arm the module under test currently implements.
        loaded = real_gate(insiders)
        # The two preconditions are shared by both counting rules — a cluster failing
        # either is refused whatever the natures say, so record it and move on.
        enough_ciks = len({h.filer_cik for h in insiders}) >= orl._DEEMED_CHAIN_MIN_CIKS
        all_ten_pct = all(h.is_ten_percent_owner for h in insiders)
        nd_now, ni_now = _counts(insiders, table_i_only=False)
        nd_gated, ni_gated = _counts(insiders, table_i_only=True)
        verdict_now = enough_ciks and all_ten_pct and _shape_admits(nd_now, ni_now)
        verdict_gated = enough_ciks and all_ten_pct and _shape_admits(nd_gated, ni_gated)
        evals.append(
            {
                **current,
                "loaded_gate": loaded,
                "shares": str(insiders[0].shares) if insiders else None,
                "n_members": len(insiders),
                "enough_ciks": enough_ciks,
                "all_ten_pct": all_ten_pct,
                "n_direct_now": nd_now,
                "n_indirect_now": ni_now,
                "n_direct_gated": nd_gated,
                "n_indirect_gated": ni_gated,
                "n_role_derived": sum(1 for h in insiders if not h.nature_from_table_i),
                "verdict_now": verdict_now,
                "verdict_gated": verdict_gated,
                "flips": verdict_now != verdict_gated,
                "members": [_describe(h) for h in insiders],
            }
        )
        return loaded

    orl._is_deemed_chain = recording_gate  # type: ignore[assignment]
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
                    evals.append({**current, "error": repr(exc)})
                if n % 100 == 0:
                    print(f"{n}/{len(population)}", file=sys.stderr, flush=True)
    finally:
        orl._is_deemed_chain = real_gate  # type: ignore[assignment]

    with open(args.out, "w") as fh:
        for rec in evals:
            fh.write(json.dumps(rec) + "\n")

    errors = [e for e in evals if "error" in e]
    good = [e for e in evals if "error" not in e]
    admits_now = [e for e in good if e["verdict_now"]]
    admits_gated = [e for e in good if e["verdict_gated"]]
    flips = [e for e in good if e["flips"]]
    # Which arm is loaded — see the note in ``recording_gate``. Printed, never inferred.
    agrees_now = sum(1 for e in good if e["loaded_gate"] == e["verdict_now"])
    agrees_gated = sum(1 for e in good if e["loaded_gate"] == e["verdict_gated"])
    print(f"\nloaded gate agrees with UNGATED counting : {agrees_now}/{len(good)}")
    print(f"loaded gate agrees with PROVENANCE-gated : {agrees_gated}/{len(good)}")
    print(f"\ninstruments scanned          : {len(population)}")
    print(f"harness errors               : {len(errors)}")
    print(f"gate evaluations             : {len(good)}")
    print(f"   … carrying a role-derived member: {sum(1 for e in good if e['n_role_derived'])}")
    print(f"admitted NOW                 : {len(admits_now)}")
    print(f"admitted under PROVENANCE GATE: {len(admits_gated)}")
    print(f"\nverdict flips                : {len(flips)}")
    print("   refuse→admit:", sum(1 for e in flips if e["verdict_gated"]))
    print("   admit→refuse:", sum(1 for e in flips if e["verdict_now"]))
    print("   instruments affected:", len({e["instrument_id"] for e in flips}))
    # A flip can only come from a cluster that passed BOTH preconditions; anything else
    # is refused on an axis this change does not touch. Reported so the residual is
    # attributable rather than assumed.
    blocked = [e for e in good if not (e["enough_ciks"] and e["all_ten_pct"])]
    print(f"\nrefused on CIK-count / 10%-owner (untouched by this change): {len(blocked)}")
    print(
        "shape-only refusals now:",
        sum(1 for e in good if e["enough_ciks"] and e["all_ten_pct"] and not e["verdict_now"]),
    )
    if flips:
        print("\nflip detail (n_direct now → gated):")
        print("  ", dict(Counter(f"{e['n_direct_now']}->{e['n_direct_gated']}" for e in flips)))
        print("   symbols:", sorted({str(e["symbol"]) for e in flips})[:40])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
