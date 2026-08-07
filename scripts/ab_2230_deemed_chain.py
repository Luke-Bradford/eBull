"""Full-population A/B for the #2230 deemed-chain insider collapse.

Runs the REAL :func:`app.services.ownership_rollup.get_ownership_rollup` over every
instrument that carries an equal-value insider cluster — the complete blast radius of
:func:`_reconcile_insider_control_groups` — and dumps one JSON line per instrument.

Both arms run this same script against the same dev DB; the CONTROL arm runs it from a
worktree checked out at ``origin/main``, so the control is real code and not a simulation
(``.claude/skills/engineering/full-population-ab.md``).

Metric is DISTINCT ENTITY, not row count: per instrument we record the insiders slice's
holder count and share total, plus every ``insider_control_group_collapse`` correction
with the identity it kept and the identities it folded. Comparing the folded-identity SETS
is what makes a re-attribution visible — a selection change loses in a different row than
it gains in (#2176).

Usage (sharded so several workers can share the population):

    PYTHONPATH=. uv run python -m scripts.ab_2230_deemed_chain \
        --shard 0 --shards 3 --out /tmp/ab2230_treat_0.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
from decimal import Decimal
from typing import Any

import psycopg

from app.config import settings
from app.services.ownership_rollup import get_ownership_rollup

# Every instrument holding at least one equal-value insider cluster (>=2 distinct holder
# identities at one exact share value). That is exactly the set on which the pass under
# test can fire, so it is the full population for this change rather than a sample.
POPULATION_SQL = """
SELECT DISTINCT oc.instrument_id, i.symbol
  FROM ownership_insiders_current oc
  JOIN instruments i USING (instrument_id)
 WHERE oc.instrument_id IN (
        SELECT instrument_id
          FROM ownership_insiders_current
         WHERE shares > 0
         GROUP BY instrument_id, shares
        HAVING count(DISTINCT holder_identity_key) >= 2
       )
 ORDER BY oc.instrument_id
"""


def _snapshot(conn: psycopg.Connection[Any], symbol: str, instrument_id: int) -> dict[str, Any]:
    """One instrument's comparable state: the insiders wedge plus the identity-level
    detail of every control-group collapse applied to it."""
    rollup = get_ownership_rollup(conn, symbol, instrument_id)
    insiders = next((s for s in rollup.slices if s.category == "insiders"), None)
    collapses = [
        {
            "kept_cik": c.filer_cik,
            "kept_name": c.filer_name,
            "shares_removed": str(c.shares_removed),
            "detail": c.detail,
        }
        for c in rollup.corrections_applied
        if c.kind == "insider_control_group_collapse"
    ]
    # #2229 established that a holder removed from one wedge can REAPPEAR in another
    # (the 13D/G disclosure becomes the winning source under the dedup priority), so an
    # invariant written on a single slice fires falsely. Capture every pie wedge and the
    # pie total, and judge the change on those.
    wedges = {s.category: str(s.total_shares) for s in rollup.slices if s.denominator_basis == "pie_wedge"}
    pie_total = sum(
        (s.total_shares for s in rollup.slices if s.denominator_basis == "pie_wedge"),
        Decimal(0),
    )
    return {
        "instrument_id": instrument_id,
        "symbol": symbol,
        "pie_wedges": wedges,
        "pie_total": str(pie_total),
        "insiders_shares": str(insiders.total_shares) if insiders else None,
        "insiders_pct": str(insiders.pct_outstanding) if insiders else None,
        "insiders_holders": insiders.filer_count if insiders else 0,
        # Holder identities in the insiders wedge — the distinct-entity metric. A
        # collapse must REMOVE identities; anything else is a re-attribution.
        "insider_identities": sorted(f"{h.filer_cik or h.filer_name}" for h in (insiders.holders if insiders else [])),
        "collapses": collapses,
        "corrections_by_kind": {
            k: sum(1 for c in rollup.corrections_applied if c.kind == k)
            for k in sorted({c.kind for c in rollup.corrections_applied})
        },
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--shard", type=int, default=0)
    ap.add_argument("--shards", type=int, default=1)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    with psycopg.connect(settings.database_url) as conn:
        population = conn.execute(POPULATION_SQL).fetchall()
        mine = [r for n, r in enumerate(population) if n % args.shards == args.shard]
        # Write unbuffered and line-at-a-time: a piped/buffered long run looks stalled
        # when it is healthy (prevention log, 2026-08-05 phase 2a).
        with open(args.out, "w", buffering=1) as fh:
            for n, (instrument_id, symbol) in enumerate(mine):
                try:
                    rec = _snapshot(conn, str(symbol), int(instrument_id))
                except Exception as exc:  # harness error — record, never silently drop
                    rec = {"instrument_id": int(instrument_id), "symbol": str(symbol), "error": repr(exc)}
                fh.write(json.dumps(rec) + "\n")
                if n % 25 == 0:
                    print(f"shard {args.shard}: {n}/{len(mine)}", file=sys.stderr, flush=True)
    print(f"shard {args.shard}: done {len(mine)}", file=sys.stderr, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
