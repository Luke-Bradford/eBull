"""Compare the two #2232 A/B arms. Distinct-entity metric; both sides inspected.

    PYTHONPATH=. uv run python -m scripts.ab_2232_compare \
        --control '/tmp/ab2232_ctrl_*.jsonl' --treatment '/tmp/ab2232_treat_*.jsonl'
"""

from __future__ import annotations

import argparse
import glob
import json
from typing import Any


def _load(pattern: str) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    for path in sorted(glob.glob(pattern)):
        with open(path) as fh:
            for line in fh:
                rec = json.loads(line)
                out[int(rec["instrument_id"])] = rec
    return out


# Everything the rollup renders that this change could plausibly disturb. `pie_wedges`
# and `pie_total` are here because #2229 established a holder can move BETWEEN wedges,
# so a per-slice check alone can read clean while the pie changed.
COMPARED = (
    "banner_state",
    "no_data_reason",
    "shares_outstanding",
    "slice_count",
    "pie_wedges",
    "pie_total",
    "largest_single_holder_pct",
    "residual_pct",
    "oversubscribed",
)
PANEL = ("AAPL", "GME", "MSFT", "JPM", "HD")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--control", required=True)
    ap.add_argument("--treatment", required=True)
    args = ap.parse_args()

    ctrl, treat = _load(args.control), _load(args.treatment)
    print(f"control rows {len(ctrl)} · treatment rows {len(treat)}")
    only_c, only_t = set(ctrl) - set(treat), set(treat) - set(ctrl)
    if only_c or only_t:
        print(f"⚠ POPULATION MISMATCH — control-only {len(only_c)}, treatment-only {len(only_t)}")

    errors = [r for r in (*ctrl.values(), *treat.values()) if "error" in r]
    print(f"harness errors: {len(errors)}")
    for r in errors[:10]:
        print(f"  {r['symbol']}: {r['error'][:160]}")

    changed: list[tuple[dict[str, Any], dict[str, Any], list[str]]] = []
    for iid in sorted(set(ctrl) & set(treat)):
        c, t = ctrl[iid], treat[iid]
        if "error" in c or "error" in t:
            continue
        diff = [k for k in COMPARED if c.get(k) != t.get(k)]
        if diff:
            changed.append((c, t, diff))

    print(f"\ninstruments compared: {len(set(ctrl) & set(treat))}")
    print(f"instruments CHANGED : {len(changed)}")

    suppressed = [x for x in changed if x[1]["no_data_reason"] == "partial_class_denominator"]
    # `no_data_reason` does not exist on origin/main, so the control records it as None
    # for EVERY row. An instrument already on the `absent` / `stale_denominator` path
    # therefore "differs" on that key alone while nothing it renders moved. That is the
    # new field appearing, not a behaviour change — but it has to be shown separately
    # and counted, not waved away in prose.
    field_only = [
        x for x in changed if x not in suppressed and x[2] == ["no_data_reason"] and x[0]["no_data_reason"] is None
    ]
    other = [x for x in changed if x not in suppressed and x not in field_only]
    print(f"  → newly suppressed (partial_class_denominator): {len(suppressed)}")
    print(f"  → no_data_reason newly POPULATED, nothing else moved: {len(field_only)}")
    if field_only:
        tally: dict[str | None, int] = {}
        for _c, t, _d in field_only:
            tally[t["no_data_reason"]] = tally.get(t["no_data_reason"], 0) + 1
        print(f"      by reason: {tally}")
    print(f"  → changed for ANY OTHER reason (must be 0)    : {len(other)}")

    print("\n--- SUPPRESSED (the loss side: what the operator stops seeing) ---")
    print(f"{'symbol':<9}{'denominator':>16}{'largest holder %':>20}{'wedges lost':>13}  banner was")
    for c, t, _ in sorted(suppressed, key=lambda x: -float(x[0]["largest_single_holder_pct"])):
        print(
            f"{c['symbol']:<9}{float(c['shares_outstanding'] or 0):>16,.0f}"
            f"{float(c['largest_single_holder_pct']) * 100:>19,.0f}%"
            f"{c['slice_count']:>13}  {c['banner_state']}"
        )

    if other:
        print("\n--- ⚠ CHANGED FOR ANOTHER REASON (each one is a defect) ---")
        for c, t, diff in other[:40]:
            print(f"{c['symbol']}: {diff}")
            for k in diff:
                print(f"    {k}: {c.get(k)!r} -> {t.get(k)!r}")

    print("\n--- golden panel (must be byte-identical) ---")
    for sym in PANEL:
        cs = next((r for r in ctrl.values() if r["symbol"] == sym), None)
        ts = next((r for r in treat.values() if r["symbol"] == sym), None)
        if cs is None or ts is None:
            print(f"{sym:<7} ABSENT from population (control={cs is not None}, treatment={ts is not None})")
            continue
        same = all(cs.get(k) == ts.get(k) for k in COMPARED)
        print(
            f"{sym:<7} {'IDENTICAL' if same else '*** MOVED ***'} · "
            f"outstanding={cs['shares_outstanding']} · wedges={cs['slice_count']} · "
            f"largest_holder={float(cs['largest_single_holder_pct']) * 100:.2f}% · "
            f"reason={ts['no_data_reason']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
