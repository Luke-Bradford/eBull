"""Per-holder detail for named instruments — the GAIN-side inspection for #2785.

``scripts.ab_2230_deemed_chain`` records the insiders wedge's identity SET and its total,
which is the right distinct-entity metric for a FOLD (a fold can only remove identities).
A rep SWAP moves shares between two identities that are both already present, so the set
is unchanged and the total moves — the exact shape the A/B comparator flags as
``same identity set but different total``. That flag is correct to raise and cannot be
resolved from the A/B dump, because the dump holds no per-holder figure.

This probe supplies it. Run it in BOTH worktrees (control at ``origin/main``, treatment on
the branch) and diff, so the claim "the incumbent fell by X and the candidate rose by Y"
is read off the two arms rather than argued:

    PYTHONPATH=. uv run python -m scripts.probe_2785_wedge_detail HCA WBD SGU > /tmp/x.txt

Prints one line per pie-wedge holder, so a wedge that moved can be attributed to the
identities that moved it.

⚠ **Exits NON-ZERO if any symbol resolves to nothing**, and this is load-bearing rather
than tidiness. Invoked as ``… $SYMS`` from zsh, an unquoted parameter is NOT word-split,
so all eight symbols arrive as one argument, every lookup misses, and both arms emit one
identical ``NOT FOUND`` line — a clean, empty diff that reads exactly like "the change
moved nothing". Same shape as the #2386 census trap: a broken measurement and a clean
result must not be the same output.
"""

from __future__ import annotations

import sys

import psycopg

from app.config import settings
from app.services.ownership_rollup import get_ownership_rollup


def main(symbols: list[str]) -> int:
    if not symbols:
        print("usage: probe_2785_wedge_detail SYMBOL [SYMBOL ...]", file=sys.stderr)
        return 2
    missing: list[str] = []
    with psycopg.connect(settings.database_url) as conn:
        for symbol in symbols:
            row = conn.execute("SELECT instrument_id FROM instruments WHERE symbol = %s", (symbol,)).fetchone()
            if row is None:
                print(f"{symbol}: NOT FOUND")
                missing.append(symbol)
                continue
            rollup = get_ownership_rollup(conn, symbol, int(row[0]))
            for sl in rollup.slices:
                if sl.denominator_basis != "pie_wedge":
                    continue
                print(f"{symbol} {sl.category} total={sl.total_shares} holders={sl.filer_count}")
                for h in sorted(sl.holders, key=lambda h: -h.shares):
                    print(f"    {symbol} {sl.category} {h.filer_cik or '-'} {h.filer_name} {h.shares}")
            for c in rollup.corrections_applied:
                if c.kind == "insider_control_group_collapse":
                    print(f"    {symbol} CORRECTION kept={c.filer_name} removed={c.shares_removed}")
    print(f"resolved {len(symbols) - len(missing)}/{len(symbols)} symbols", file=sys.stderr)
    if missing:
        print(f"UNRESOLVED SYMBOLS: {missing}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
