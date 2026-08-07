"""Compare the two #2230 A/B arms.

Reads the control and treatment JSONL dumps produced by
:mod:`scripts.ab_2230_deemed_chain` and reports the DISTINCT-ENTITY delta per instrument.

Reports both sides deliberately. A selection change loses in a different row than it gains
in (#2176), so a net figure can hide a re-attribution entirely: an instrument whose
insiders wedge is unchanged in total but whose holder identity SET differs is a swap, not
a no-op, and is listed separately.
"""

from __future__ import annotations

import argparse
import glob
import json
from decimal import Decimal
from typing import Any


def _load(pattern: str) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    for path in sorted(glob.glob(pattern)):
        with open(path) as fh:
            for line in fh:
                rec = json.loads(line)
                out[int(rec["instrument_id"])] = rec
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--control", required=True)
    ap.add_argument("--treatment", required=True)
    args = ap.parse_args()

    ctrl, treat = _load(args.control), _load(args.treatment)
    common = sorted(set(ctrl) & set(treat))
    print(f"control rows {len(ctrl)}  treatment rows {len(treat)}  compared {len(common)}")
    print(f"only-in-control {len(set(ctrl) - set(treat))}  only-in-treatment {len(set(treat) - set(ctrl))}")

    errs = [i for i in common if "error" in ctrl[i] or "error" in treat[i]]
    print(f"harness errors {len(errs)}")
    for i in errs[:5]:
        print("   ", ctrl[i].get("symbol"), ctrl[i].get("error") or treat[i].get("error"))

    gained, lost, reattributed, collapse_delta = [], [], [], []
    for i in common:
        c, t = ctrl[i], treat[i]
        if "error" in c or "error" in t:
            continue
        cid, tid = set(c["insider_identities"]), set(t["insider_identities"])
        nc, nt = len(c["collapses"]), len(t["collapses"])
        if nt != nc:
            collapse_delta.append((c["symbol"], nc, nt))
        # A collapse REMOVES identities from the wedge. Anything appearing only in
        # treatment is a gain and needs explaining.
        if tid - cid:
            gained.append((c["symbol"], sorted(tid - cid)))
        if cid - tid:
            lost.append((c["symbol"], sorted(cid - tid), str(c["insiders_shares"]), str(t["insiders_shares"])))
        if cid == tid and c["insiders_shares"] != t["insiders_shares"]:
            reattributed.append((c["symbol"], c["insiders_shares"], t["insiders_shares"]))

    # Whole-pie view. The insiders wedge alone is not a safe metric: a holder this pass
    # removes from insiders can REAPPEAR in blockholders under the dedup priority (#2229,
    # VEON). Only the pie total says whether shares actually left the accounting.
    pie_moves, wedge_gains = [], []
    for i in common:
        c, t = ctrl[i], treat[i]
        if "error" in c or "error" in t:
            continue
        if c.get("pie_total") is None or t.get("pie_total") is None:
            continue
        dc, dt = Decimal(c["pie_total"]), Decimal(t["pie_total"])
        if dc != dt:
            pie_moves.append((c["symbol"], dc, dt, dc - dt))
        for cat in set(c["pie_wedges"]) | set(t["pie_wedges"]):
            before = Decimal(c["pie_wedges"].get(cat, "0"))
            after = Decimal(t["pie_wedges"].get(cat, "0"))
            if after > before:
                wedge_gains.append((c["symbol"], cat, before, after))

    print()
    print(f"instruments whose PIE TOTAL moved        : {len(pie_moves)}")
    print(f"wedge-level INCREASES (any slice, any instrument): {len(wedge_gains)}   <-- must be 0")
    for row in wedge_gains[:20]:
        print("   ", row)
    print("\nPIE TOTAL — every instrument that moved:")
    tot_pie = Decimal(0)
    for sym, before, after, delta in sorted(pie_moves, key=lambda r: r[0]):
        tot_pie += delta
        sign = "-" if delta > 0 else "+"
        print(f"   {sym:9s} {before} -> {after}   ({sign}{abs(delta)})")
    print(f"\ntotal shares removed from the PIE: {tot_pie}")

    print()
    print(f"instruments whose collapse COUNT changed : {len(collapse_delta)}")
    print(f"instruments that LOST insider identities : {len(lost)}   <-- the intended effect")
    print(f"instruments that GAINED an identity      : {len(gained)}   <-- must be 0")
    print(f"same identity set but different total    : {len(reattributed)}   <-- must be 0")

    if gained:
        print("\nGAINED (unexpected):")
        for s, ids in gained[:20]:
            print("   ", s, ids)
    if reattributed:
        print("\nRE-ATTRIBUTED (unexpected):")
        for row in reattributed[:20]:
            print("   ", row)

    tot_removed = Decimal(0)
    print("\nLOSS SIDE — every instrument, identities removed and the wedge before/after:")
    for sym, ids, before, after in sorted(lost, key=lambda r: r[0]):
        delta = Decimal(before) - Decimal(after) if before != "None" and after != "None" else Decimal(0)
        tot_removed += delta
        print(f"   {sym:9s} -{len(ids):2d} identities   {before} -> {after}   (-{delta})")
    print(f"\ntotal double-counted shares removed: {tot_removed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
