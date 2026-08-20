"""Full-population A/B for the #2232 partial-class denominator guard.

Runs the REAL :func:`app.services.ownership_rollup.get_ownership_rollup` over EVERY
instrument that carries a shares-outstanding row and dumps one JSON line each. Both
arms run this same script against the same dev DB; the CONTROL arm runs it from a
worktree checked out at ``origin/main``, so the control is real code and not a
simulation (``.claude/skills/engineering/full-population-ab.md``).

⚠ **The population is bounded, and here is exactly what it excludes.** The guard has
three conditions, and this runs the union of ALL of their surfaces independently —
every instrument with no cover-page DEI fact (condition 1's surface, whatever its
holders look like) UNION every instrument where some holder exceeds the denominator
(condition 2's surface, whatever its facts look like) UNION every instrument with
more disclosed holders than shares (condition 3's surface, ditto). So if any part of
the reasoning is wrong, this A/B sees it; only an instrument that satisfies NONE of
them is excluded, and such an instrument cannot reach the new branch through any
argument except one this set would have already falsified.

That bound is a cost decision and is stated rather than hidden: the whole
denominator-carrying universe is 4,654 instruments and `get_ownership_rollup`
sustains ~11/min across three shards on this box, i.e. ~7 hours per arm. The script
prints the excluded count at startup — read it, do not assume it.

Metric is DISTINCT ENTITY (instruments whose rendered state changes), never row
count, and BOTH sides are inspected: the loss side is the point of the change, but
an unchanged pie on every other instrument is the claim that has to be proved.

Usage:

    PYTHONPATH=. uv run python -m scripts.ab_2232_partial_class_denominator \
        --shard 0 --shards 3 --out /tmp/ab2232_treat_0.jsonl
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

# Condition 1's surface UNION condition 2's surface — each taken WITHOUT the other, so
# a wrong assumption in either half still lands in the population. The golden panel is
# unioned in unconditionally as a control that must not move.
POPULATION_SQL = """
WITH denom AS (
    SELECT l.instrument_id, i.symbol, l.latest_shares
      FROM instrument_share_count_latest l
      JOIN instruments i USING (instrument_id)
),
no_cover_fact AS (          -- condition 1's surface, any holders
    SELECT instrument_id, symbol FROM denom d
     WHERE NOT EXISTS (
        SELECT 1 FROM financial_facts_raw f
         WHERE f.instrument_id = d.instrument_id
           AND f.taxonomy = 'dei'
           AND f.concept = 'EntityCommonStockSharesOutstanding'
           AND f.unit = 'shares'
           AND f.val IS NOT NULL)
),
holder_over_100 AS (        -- condition 2's surface, any facts
    SELECT d.instrument_id, d.symbol FROM denom d
     WHERE GREATEST(
             COALESCE((SELECT max(shares) FROM ownership_insiders_current o
                        WHERE o.instrument_id = d.instrument_id), 0),
             COALESCE((SELECT max(shares) FROM ownership_institutions_current o
                        WHERE o.instrument_id = d.instrument_id), 0),
             COALESCE((SELECT max(aggregate_amount_owned) FROM ownership_blockholders_current o
                        WHERE o.instrument_id = d.instrument_id), 0)
           ) > d.latest_shares
),
holder_count_over AS (      -- condition 3's surface (#2232 arm 3), any facts
    -- ⚠ DELIBERATELY WIDER than the guard. The guard counts 13F managers only
    -- (`count_additive_institutional_holders`); this counts every channel's raw
    -- `_current` rows, before `_reconcile_owner_once` collapses an owner appearing
    -- in two of them. Both differences point the same way — the surface is a strict
    -- SUPERSET of what arm 3 can fire on — so the A/B sees any instrument the guard
    -- touches AND the ones it declines to, which is where over-firing would show.
    SELECT d.instrument_id, d.symbol FROM denom d
     WHERE (
             COALESCE((SELECT count(DISTINCT filer_cik) FROM ownership_institutions_current o
                        WHERE o.instrument_id = d.instrument_id), 0)
           + COALESCE((SELECT count(*) FROM ownership_insiders_current o
                        WHERE o.instrument_id = d.instrument_id), 0)
           + COALESCE((SELECT count(*) FROM ownership_blockholders_current o
                        WHERE o.instrument_id = d.instrument_id), 0)
           ) > d.latest_shares
),
panel AS (                  -- golden panel: must be byte-identical across arms
    SELECT instrument_id, symbol FROM denom WHERE symbol IN ('AAPL','GME','MSFT','JPM','HD')
)
SELECT instrument_id, symbol FROM no_cover_fact
UNION SELECT instrument_id, symbol FROM holder_over_100
UNION SELECT instrument_id, symbol FROM holder_count_over
UNION SELECT instrument_id, symbol FROM panel
 ORDER BY instrument_id
"""

TOTAL_SQL = "SELECT count(*) FROM instrument_share_count_latest"


def _snapshot(conn: psycopg.Connection[Any], symbol: str, instrument_id: int) -> dict[str, Any]:
    """One instrument's comparable rendered state.

    ``pie_wedges`` + ``pie_total`` are carried because #2229 established that a
    holder removed from one wedge can REAPPEAR in another, so an invariant written
    on a single slice fires falsely. ``largest_single_holder_pct`` is carried
    because it is the guard's own input — a change there without a state change
    would mean the guard read something other than what it fired on.
    """
    rollup = get_ownership_rollup(conn, symbol, instrument_id)
    wedges = {s.category: str(s.total_shares) for s in rollup.slices if s.denominator_basis == "pie_wedge"}
    pie_total = sum(
        (s.total_shares for s in rollup.slices if s.denominator_basis == "pie_wedge"),
        Decimal(0),
    )
    return {
        "instrument_id": instrument_id,
        "symbol": symbol,
        "banner_state": rollup.banner.state,
        # ⚠ getattr, not attribute access: the CONTROL arm runs origin/main, where
        # `no_data_reason` does not exist yet. A bare `rollup.no_data_reason` makes
        # every control row an AttributeError — 443 of 443 on the first attempt —
        # and an arm that errors on every row cannot disagree with anything.
        "no_data_reason": getattr(rollup, "no_data_reason", None),
        "shares_outstanding": str(rollup.shares_outstanding) if rollup.shares_outstanding is not None else None,
        "shares_outstanding_as_of": (
            rollup.shares_outstanding_as_of.isoformat() if rollup.shares_outstanding_as_of else None
        ),
        "slice_count": len(rollup.slices),
        "pie_wedges": wedges,
        "pie_total": str(pie_total),
        "largest_single_holder_pct": str(rollup.sanity.largest_single_holder_pct),
        "residual_pct": str(rollup.residual.pct_outstanding),
        "oversubscribed": rollup.residual.oversubscribed,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--shard", type=int, default=0)
    ap.add_argument("--shards", type=int, default=1)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    with psycopg.connect(settings.database_url) as conn:
        population = conn.execute(POPULATION_SQL).fetchall()
        total_row = conn.execute(TOTAL_SQL).fetchone()
        total = int(total_row[0]) if total_row else 0
        # No silent caps: say what was left out, every run, in both arms.
        print(
            f"population {len(population)} of {total} denominator-carrying instruments; "
            f"{total - len(population)} excluded (neither guard condition's surface)",
            file=sys.stderr,
            flush=True,
        )
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
                if n % 100 == 0:
                    print(f"shard {args.shard}: {n}/{len(mine)}", file=sys.stderr, flush=True)
    print(f"shard {args.shard}: done {len(mine)}", file=sys.stderr, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
