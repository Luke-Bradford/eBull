"""Is our short-horizon "reversal" a real effect, or just the bid-ask bounce?

    PYTHONPATH=. uv run python scripts/verify_2437_roll_bounce.py

⚠ NOTHING IS WRITTEN. Gate on the EXIT CODE. Never pipe into head/tail.

WHY THIS EXISTS
---------------
`verify_2437_autocorrelation_term_structure.py` found that after year-clustering,
**short-horizon reversal is the ONLY part of the term structure that survives** —
1-day and 5-day negative in every price band at t -5 to -11, while every momentum
cell collapsed. That is now the single finding our own data supports.

⚠⚠ **Roll (1984) says that finding may have no economic content at all.**
*"A Simple Implicit Measure of the Effective Bid-Ask Spread in an Efficient
Market"*, JF 39(4). In an efficient market with a spread, trades alternate
between bid and ask, so observed price changes carry **spurious negative serial
covariance** — literally what is called a short-term reversal pattern. Roll's
result is that the spread can be RECOVERED from it:

    effective spread  =  2 * sqrt( -cov(dP_t, dP_t-1) )

THE TEST
--------
If our measured reversal is bounce, the spread IMPLIED by the autocovariance
should be close to the spread we already believe those names have. We have an
independent estimate: `cost_model.BANDS`, calibrated per price band.

    implied  ~  actual   ->  the reversal is BOUNCE. No tradable signal;
                             a strategy trading it pays the spread that
                             created the appearance of the edge.
    implied  >> actual   ->  there is economic reversal ON TOP of the bounce,
                             and the excess is the part worth trading.
    implied  << actual   ->  something is wrong with one of the two estimates.

⚠ This is a decomposition, not a significance test. It says how much of a real
number is mechanical, which is the question the t-statistic cannot answer.

⚠ Log returns are used, so the implied spread comes out as a FRACTION of price
and is directly comparable to `PriceBand.half_spread * 2`.
"""

from __future__ import annotations

import sys
from collections import defaultdict
from decimal import Decimal

import numpy as np
import psycopg

from app.config import settings
from app.services.cost_model import BANDS, band_for

MAX_ADJ_SPAN = 1e6

_SERIES_SQL = "SELECT series_id FROM research_price_series ORDER BY series_id"
_BARS_SQL = """
    SELECT adj_close, bar_date
    FROM research_price_daily
    WHERE series_id = %(series_id)s AND adj_close IS NOT NULL AND adj_close > 0
    ORDER BY bar_date
"""
_MEDIAN_SQL = """
    SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY close)
    FROM research_price_daily WHERE series_id = %(series_id)s AND close > 0
"""


def main() -> int:
    with psycopg.connect(settings.database_url) as conn:
        series_ids = [r[0] for r in conn.execute(_SERIES_SQL).fetchall()]
        print(f"{len(series_ids)} series", flush=True)
        # band label -> per-series first-order autocovariance of daily log returns
        by_band: dict[str, list[float]] = defaultdict(list)
        used = 0
        for n, series_id in enumerate(series_ids, start=1):
            rows = conn.execute(_BARS_SQL, {"series_id": series_id}).fetchall()
            if len(rows) < 250:
                continue
            prices = np.asarray([float(r[0]) for r in rows], dtype=np.float64)
            if float(prices.max()) / float(prices.min()) > MAX_ADJ_SPAN:
                continue
            med = conn.execute(_MEDIAN_SQL, {"series_id": series_id}).fetchone()
            if med is None or med[0] is None or float(med[0]) <= 0:
                continue
            rets = np.diff(np.log(prices))
            if len(rets) < 100 or rets.std() == 0:
                continue
            # ⚠ PER SERIES, then averaged across series. Pooling first would let
            # one long noisy series dominate the band.
            cov = float(np.cov(rets[:-1], rets[1:], bias=True)[0, 1])
            by_band[band_for(Decimal(str(round(float(med[0]), 4)))).label].append(cov)
            used += 1
            if n % 1000 == 0:
                print(f"  {n}/{len(series_ids)}", flush=True)

    print(f"\nused {used} series\n")
    print("ROLL (1984) DECOMPOSITION OF THE 1-DAY REVERSAL")
    print("implied round trip = 2*sqrt(-cov), averaged per series then across the band\n")
    print(f"{'band':<12}{'mean cov':>14}{'implied RT':>12}{'our RT':>10}{'implied/ours':>14}   verdict")
    print("-" * 84)

    actual = {b.label: float(b.half_spread) * 2 * 100 for b in BANDS}
    failures = 0
    for band in [b.label for b in BANDS]:
        covs = by_band.get(band, [])
        if len(covs) < 20:
            print(f"{band:<12}{'insufficient series':>50}")
            continue
        arr = np.asarray(covs)
        negative = arr[arr < 0]
        mean_cov = float(arr.mean())
        # ⚠ Roll is undefined for a POSITIVE covariance — the model cannot
        # produce one. Series with cov >= 0 are counted, never coerced.
        implied = 2.0 * np.sqrt(-negative) * 100 if negative.size else np.array([])
        implied_mean = float(implied.mean()) if implied.size else float("nan")
        ours = actual[band]
        ratio = implied_mean / ours if ours > 0 else float("nan")
        if ratio > 1.5:
            verdict = "EXCESS reversal beyond the spread"
        elif ratio > 0.67:
            verdict = "~= the spread -> LOOKS LIKE BOUNCE"
            failures += 1
        else:
            verdict = "below the spread -> check estimates"
        print(
            f"{band:<12}{mean_cov:>14.2e}{implied_mean:>11.3f}%{ours:>9.3f}%{ratio:>13.2f}x   {verdict}"
            f"   ({len(negative)}/{len(arr)} series cov<0)"
        )

    print("\n⚠ Reading this table:")
    print("  'LOOKS LIKE BOUNCE' means a strategy trading that reversal would be")
    print("  paying the very spread that produced the appearance of the edge.")
    print("  Roll is a LOWER bound on the real spread -- it assumes no information")
    print("  in order flow -- so a ratio near 1.0 is already damning.")
    print(f"\n{failures} of {len([b for b in BANDS])} bands look like pure bounce.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
