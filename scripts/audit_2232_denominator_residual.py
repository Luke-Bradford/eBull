"""Re-runnable census of the #2232 residual — the denominators that are wrong while
a cover-page count EXISTS, so the shipped scope arm cannot reach them.

    PYTHONPATH=. uv run python -m scripts.audit_2232_denominator_residual

Read-only. Writes nothing, creates no tables. ~2 s.

Why this exists rather than a figure written into prose: every number the #2232
thread rests on is a full-population count that moves as filings land, and a
hand-copied one goes stale silently in the place a reader trusts most. The three
claims it reproduces are

  1. the ratio ``max_holder / denominator`` is BIMODAL with an empty decade — that
     gap, not a chosen cut, is what says the shipped guard already isolates the
     right cohort;
  2. above the gap, all but a handful are already suppressed by the scope arm, and
     the remainder IS the residual;
  3. the pigeonhole arm's floor never comes near a real small-float issuer.

⚠ Arm 3 counts 13F managers ONLY. The all-channel count is printed beside it so the
narrowing stays visible: on `HQ` the two differ (0 managers vs 9 holders) and that
difference is exactly why arm 3 does not reach it.

⚠ The counts here are RAW ``_current`` rows — taken before
``_reconcile_owner_once`` collapses an owner appearing in two channels into one.
That makes every holder figure an UPPER bound on what the rollup divides against,
so this census can over-state the cohort but cannot miss a member of it. The
authoritative per-instrument verdict is the paired full-population A/B
(``scripts/ab_2232_partial_class_denominator.py``), which runs the real rollup.

⚠ The policy is not restated here. Each candidate is scored by importing the
production function ``denominator_is_partial_class`` and calling it — a census that
re-implements the predicate it is calibrating can measure nothing and still exit 0
(#2230, 2026-08-20).
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import psycopg
from psycopg.rows import dict_row

from app.config import settings
from app.services.ownership_rollup import denominator_is_partial_class

# One row per instrument carrying a LIVE denominator, with the two independent
# observations the guard reasons over: the largest single disclosed position, and
# how many distinct holders disclosed one at all.
#
# 548 days is #1581's freshness bound — the same one `_denominator_too_stale`
# applies — so a staler instrument is already suppressed for another reason and
# would inflate this cohort with rows the guard never sees.
CENSUS_SQL = """
WITH denom AS (
    SELECT l.instrument_id, i.symbol, i.company_name, l.latest_shares,
           l.as_of_date, l.source_taxonomy
      FROM instrument_share_count_latest l
      JOIN instruments i USING (instrument_id)
     WHERE l.latest_shares IS NOT NULL
       AND l.latest_shares > 0
       AND (CURRENT_DATE - l.as_of_date) <= 548
)
SELECT d.symbol,
       d.company_name,
       d.latest_shares,
       d.as_of_date,
       d.source_taxonomy,
       GREATEST(
         COALESCE((SELECT max(shares) FROM ownership_insiders_current o
                    WHERE o.instrument_id = d.instrument_id), 0),
         COALESCE((SELECT max(shares) FROM ownership_institutions_current o
                    WHERE o.instrument_id = d.instrument_id), 0),
         COALESCE((SELECT max(aggregate_amount_owned) FROM ownership_blockholders_current o
                    WHERE o.instrument_id = d.instrument_id), 0)
       ) AS max_holder,
       -- Arm 3's count: 13F managers ONLY, and only whole-share positions. See
       -- `count_additive_institutional_holders` — Section 16 / 13D-G holders may
       -- restate ONE deemed block under many identities (Rule 13d-5(b)(1)) and may
       -- hold fractions, so neither channel can carry a pigeonhole argument.
       COALESCE((SELECT count(DISTINCT filer_cik) FROM ownership_institutions_current o
                  WHERE o.instrument_id = d.instrument_id AND o.shares >= 1), 0) AS n_managers,
       -- Every disclosed holder, ANY channel. Not the guard's input — carried so the
       -- narrowing is visible as a column rather than argued in prose.
       COALESCE((SELECT count(DISTINCT filer_cik) FROM ownership_institutions_current o
                  WHERE o.instrument_id = d.instrument_id), 0)
       + COALESCE((SELECT count(*) FROM ownership_insiders_current o
                    WHERE o.instrument_id = d.instrument_id), 0)
       + COALESCE((SELECT count(*) FROM ownership_blockholders_current o
                    WHERE o.instrument_id = d.instrument_id), 0) AS n_holders_all_channels,
       EXISTS (SELECT 1 FROM financial_facts_raw f
                WHERE f.instrument_id = d.instrument_id
                  AND f.taxonomy = 'dei'
                  AND f.concept = 'EntityCommonStockSharesOutstanding'
                  AND f.unit = 'shares'
                  AND f.val IS NOT NULL) AS has_cover
  FROM denom d
"""


def _decade(ratio: Decimal) -> int:
    """Which power-of-ten band a ratio > 1 falls in (1 → 1-10, 2 → 10-100, …)."""
    band, cur = 1, Decimal(10)
    while ratio >= cur:
        band, cur = band + 1, cur * 10
    return band


def main() -> int:
    with psycopg.connect(settings.database_url) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(CENSUS_SQL)
            rows: list[dict[str, Any]] = list(cur.fetchall())

    print(f"live denominators (as_of within 548d): {len(rows)}")

    over = [r for r in rows if r["max_holder"] > r["latest_shares"]]
    print(
        f"\narm 2 surface — some holder exceeds the denominator: {len(over)}"
        f"  (no cover fact {sum(1 for r in over if not r['has_cover'])}"
        f" · cover fact present {sum(1 for r in over if r['has_cover'])})"
    )

    # Claim 1 — the gap. Printed as a full histogram rather than as the gap's
    # endpoints, so a future run that FILLS the gap is visible rather than
    # silently contradicting a sentence written today.
    print("\n  max_holder / denominator, by decade (empty bands are the evidence):")
    bands: dict[int, list[dict[str, Any]]] = {}
    for r in over:
        bands.setdefault(_decade(Decimal(r["max_holder"]) / Decimal(r["latest_shares"])), []).append(r)
    for band in range(1, max(bands, default=1) + 1):
        members = bands.get(band, [])
        lo, hi = 10 ** (band - 1), 10**band
        marker = "" if members else "   <-- EMPTY"
        print(f"    {lo:>12,}x – {hi:>13,}x : {len(members):>4}{marker}")

    # Claim 2 — above the gap, who is left once the scope arm has done its work.
    top = max((b for b in bands if bands[b]), default=1)
    above = [r for band in bands for r in bands[band] if band >= top - 3 and band > 3]
    if above:
        print("\n  above the gap:")
        print(f"    {'symbol':<9}{'denominator':>14}{'max holder':>18}{'managers':>9}  cover  suppressed by arm 1+2")
        for r in sorted(above, key=lambda r: -(Decimal(r["max_holder"]) / Decimal(r["latest_shares"]))):
            scope_arm = denominator_is_partial_class(
                has_dei_cover_share_count=bool(r["has_cover"]),
                largest_single_holder_pct=Decimal(r["max_holder"]) / Decimal(r["latest_shares"]),
                per_class_denominator_applied=False,
                additive_institutional_holders=0,  # arm 3 pinned OFF: this column is arm 1+2 alone
                outstanding=Decimal(r["latest_shares"]),
            )
            print(
                f"    {r['symbol']:<9}{float(r['latest_shares']):>14,.0f}{float(r['max_holder']):>18,.0f}"
                f"{r['n_managers']:>9}  {str(bool(r['has_cover'])):<7}{scope_arm}"
            )

    # Claim 3 — the pigeonhole arm, scored through the production function.
    fires = [
        r
        for r in rows
        if denominator_is_partial_class(
            has_dei_cover_share_count=True,  # scope arm pinned OFF: arm 3 alone
            largest_single_holder_pct=Decimal(0),
            per_class_denominator_applied=False,
            additive_institutional_holders=int(r["n_managers"]),
            outstanding=Decimal(r["latest_shares"]),
        )
    ]
    print(f"\narm 3 surface — more 13F managers than shares: {len(fires)} of {len(rows)}")
    print(f"  {'symbol':<9}{'denominator':>14}{'managers':>10}{'all holders':>13}  cover  taxonomy")
    for r in sorted(fires, key=lambda r: -(Decimal(r["n_managers"]) / Decimal(r["latest_shares"]))):
        print(
            f"  {r['symbol']:<9}{float(r['latest_shares']):>14,.0f}{r['n_managers']:>10}"
            f"{r['n_holders_all_channels']:>13}  {str(bool(r['has_cover'])):<7}{r['source_taxonomy']}"
        )
    residual = [r for r in fires if r["has_cover"]]
    print(f"  → of those, NOT already reachable by arm 1+2 (a cover fact exists): {len(residual)}")

    # The margin. A guard whose nearest miss is one row away is a threshold in
    # disguise; state the distance rather than asserting there is one.
    misses = [r for r in rows if r["n_managers"] > 0 and r["n_managers"] <= r["latest_shares"] and r["has_cover"]]
    misses.sort(key=lambda r: -(Decimal(r["n_managers"]) / Decimal(r["latest_shares"])))
    print("\n  closest NON-firing instruments that arm 1+2 does not already suppress:")
    for r in misses[:5]:
        ratio = float(Decimal(r["n_managers"]) / Decimal(r["latest_shares"]))
        print(
            f"    {r['symbol']:<9}{float(r['latest_shares']):>14,.0f}{r['n_managers']:>10}  managers/shares={ratio:.6f}"
        )

    # ⚠ Named, not hidden: the known-bad denominators arm 3 still cannot reach.
    unreached = [
        r
        for r in rows
        if r["has_cover"] and r["n_managers"] <= r["latest_shares"] and r["max_holder"] > r["latest_shares"] * 1000
    ]
    print(f"\n  known-bad, cover fact present, NOT reached by any arm: {len(unreached)}")
    for r in sorted(unreached, key=lambda r: r["latest_shares"]):
        print(
            f"    {r['symbol']:<9}{float(r['latest_shares']):>14,.0f}"
            f"{r['n_managers']:>10} managers{r['n_holders_all_channels']:>6} holders (all channels)"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
