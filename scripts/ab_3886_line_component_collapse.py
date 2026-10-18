"""#3886 — full-population A/B of the control-group collapse at Table I line grain.

Spec: docs/specs/ownership/2026-10-18-3886-control-group-line-components.md, Acceptance.

    PYTHONPATH=. uv run python -m scripts.ab_3886_line_component_collapse --shard 0/3 --out /tmp/ab0.jsonl
    PYTHONPATH=. uv run python -m scripts.ab_3886_line_component_collapse --report /tmp/ab0.jsonl /tmp/ab1.jsonl ...

Read-only. The population is every instrument with a positive ``form4``/``form3`` row in
``ownership_insiders_current``. Treatment = ``get_ownership_rollup`` as shipped. Control = the same
call with ``_reconcile_insider_line_components`` replaced by the identity, run only on instruments
where treatment applied ≥1 line-grain correction: with none the pass returns its inputs unchanged
(``test_no_bucket_returns_inputs_unchanged``), so the two arms are equal there by construction.

Each JSONL row is one instrument: insiders and pie-wedge totals per arm, every line-grain bucket
(value, keeper, folded members) and every other correction kind's shares per arm. ``--report``
prints the totals, every mover, the growers (a pie that grows means a consumed whole member's
identity released another channel's row), the #3886 issue panel against its released amounts,
and every bucket for inspection.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from decimal import Decimal
from typing import Any, Final

import psycopg

from app.config import settings
from app.services import ownership_rollup as rollup

_POPULATION_SQL: Final = """
SELECT DISTINCT oc.instrument_id, i.symbol
  FROM ownership_insiders_current oc
  JOIN instruments i ON i.instrument_id = oc.instrument_id
 WHERE oc.source IN ('form4', 'form3') AND oc.shares > 0
 ORDER BY oc.instrument_id
"""

_LINE_KIND_MARKER: Final = "at Table I line grain"

# The 13 instruments and released shares the #3227 mixed-groups A/B measured (issue #3886 body).
_ISSUE_PANEL: Final[dict[str, Decimal]] = {
    "SMMT": Decimal(573_883_879),
    "TNYA": Decimal(49_313_559),
    "AMKR": Decimal(33_384_459),
    "MAR": Decimal(20_027_118),
    "ATEC": Decimal(10_645_045),
    "AMRX": Decimal(6_381_734),
    "NMRA": Decimal(6_046_907),
    "BANF": Decimal(4_910_000),
    "HCA": Decimal(4_274_252),
    "WHF": Decimal(3_976_258),
    "KWY": Decimal(3_975_660),
    "EPSN": Decimal(2_869_560),
    "AAPG": Decimal(1_193_569),
}


def _identity(
    survivors: list[rollup.Holder],
    blockholders: list[rollup.Holder],
    record_holder_evidence: Any = None,
) -> tuple[list[rollup.Holder], list[rollup.Holder], list[rollup.CorrectionApplied]]:
    return survivors, blockholders, []


def _view(conn: psycopg.Connection[Any], symbol: str, iid: int) -> dict[str, Any]:
    r = rollup.get_ownership_rollup(conn, symbol, iid)
    insiders = next((s.total_shares for s in r.slices if s.category == "insiders"), Decimal(0))
    pie = sum((s.total_shares for s in r.slices if s.denominator_basis == "pie_wedge"), Decimal(0))
    by_kind: dict[str, Decimal] = defaultdict(Decimal)
    buckets = []
    for c in r.corrections_applied:
        if _LINE_KIND_MARKER in c.detail:
            buckets.append({"keeper": c.filer_name, "removed": str(c.shares_removed), "detail": c.detail})
        else:
            by_kind[c.kind] += c.shares_removed
    return {
        "insiders": str(insiders),
        "pie": str(pie),
        "by_kind": {k: str(v) for k, v in by_kind.items()},
        "buckets": buckets,
    }


def _run(shard: int, of: int, out: str) -> int:
    real = rollup._reconcile_insider_line_components
    t0 = time.monotonic()
    errors = 0
    with psycopg.connect(settings.database_url) as conn, open(out, "w") as fh:
        population = [row for k, row in enumerate(conn.execute(_POPULATION_SQL).fetchall()) if k % of == shard]
        for n, (iid, symbol) in enumerate(population, 1):
            try:
                rollup._reconcile_insider_line_components = real
                on = _view(conn, symbol, iid)
                off = None
                if on["buckets"]:
                    rollup._reconcile_insider_line_components = _identity
                    off = _view(conn, symbol, iid)
            except Exception as exc:  # noqa: BLE001 — a harness error is counted and printed, never hidden
                errors += 1
                fh.write(json.dumps({"iid": iid, "symbol": symbol, "error": repr(exc)}) + "\n")
                continue
            finally:
                rollup._reconcile_insider_line_components = real
            fh.write(json.dumps({"iid": iid, "symbol": symbol, "on": on, "off": off}) + "\n")
            if n % 250 == 0:
                print(f"shard {shard}/{of}: {n}/{len(population)} {time.monotonic() - t0:.0f}s", flush=True)
    print(f"shard {shard}/{of}: done {len(population)} instruments, {errors} harness errors")
    return 1 if errors else 0


def _report(paths: list[str]) -> int:  # noqa: C901, PLR0912
    rows = [json.loads(line) for p in paths for line in open(p)]
    errors = [r for r in rows if "error" in r]
    ok = [r for r in rows if "error" not in r]
    fired = [r for r in ok if r["off"] is not None]
    print(f"instruments: {len(rows)}  harness errors: {len(errors)}  pass fired: {len(fired)}")
    for r in errors:
        print("  ERROR", r["symbol"], r["error"])
    ins_delta = pie_delta = Decimal(0)
    grow, shrink, flat = [], [], []
    buckets = 0
    other_kind_moves = []
    for r in fired:
        d_ins = Decimal(r["on"]["insiders"]) - Decimal(r["off"]["insiders"])
        d_pie = Decimal(r["on"]["pie"]) - Decimal(r["off"]["pie"])
        ins_delta += d_ins
        pie_delta += d_pie
        buckets += len(r["on"]["buckets"])
        (grow if d_pie > 0 else shrink if d_pie < 0 else flat).append((r["symbol"], d_ins, d_pie))
        if r["on"]["by_kind"] != r["off"]["by_kind"]:
            other_kind_moves.append((r["symbol"], r["off"]["by_kind"], r["on"]["by_kind"]))
    print(f"line-grain buckets: {buckets}")
    print(f"insiders delta: {ins_delta:,.0f}  pie delta: {pie_delta:,.0f}")
    print(f"pie grows: {len(grow)}  shrinks: {len(shrink)}  unchanged: {len(flat)}")
    for sym, d_ins, d_pie in grow:
        print(f"  GROW {sym} insiders {d_ins:,.0f} pie {d_pie:,.0f}")
    print(f"instruments where another correction kind moved: {len(other_kind_moves)}")
    for sym, before, after in other_kind_moves:
        print(f"  KIND {sym} off={before} on={after}")
    print("issue panel (released by the #3227 fold → removed at line grain now):")
    by_symbol = {r["symbol"]: r for r in ok}
    for sym, released in _ISSUE_PANEL.items():
        r = by_symbol.get(sym)
        if r is None:
            print(f"  {sym}: NOT IN POPULATION")
            continue
        removed = sum((Decimal(b["removed"]) for b in r["on"]["buckets"]), Decimal(0))
        print(f"  {sym}: released {released:,.0f}  line-grain removed {removed:,.0f}  covers={removed >= released}")
    print("every bucket, by instrument (largest pie change first):")
    for r in sorted(fired, key=lambda r: Decimal(r["on"]["pie"]) - Decimal(r["off"]["pie"])):
        d_pie = Decimal(r["on"]["pie"]) - Decimal(r["off"]["pie"])
        print(f"## {r['symbol']} pie {d_pie:,.0f}")
        for b in r["on"]["buckets"]:
            print(f"   {b['detail']}")
    return 1 if errors else 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--shard", default="0/1")
    ap.add_argument("--out")
    ap.add_argument("--report", nargs="+")
    args = ap.parse_args()
    if args.report:
        return _report(args.report)
    shard, of = (int(x) for x in args.shard.split("/"))
    return _run(shard, of, args.out or f"/tmp/ab_3886_{shard}.jsonl")


if __name__ == "__main__":
    sys.exit(main())
