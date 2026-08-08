"""Does the sign of return autocorrelation actually flip with horizon, on OUR data?

    PYTHONPATH=. uv run python scripts/verify_2437_autocorrelation_term_structure.py

⚠ NOTHING IS WRITTEN. Gate on the EXIT CODE. Never pipe into head/tail — a pipe
returns the pipe's status and buffers the progress lines (`.claude/CLAUDE.md`).

WHY THIS EXISTS
---------------
`.claude/skills/quant/strategy-evidence.md` §2.8 asserts the organising fact of
the whole strategy catalogue: autocorrelation is **negative at days, positive at
3-12 months, negative again at 3-5 years** (Jegadeesh 1990; Jegadeesh & Titman
1993; De Bondt & Thaler 1985). Every family we own is a bet on one segment of
that curve.

⚠⚠ It was taken from the literature and NOT verified here. This script is the
verification. If the curve does not appear on our corpus, the skill's framing is
wrong for our universe and the catalogue is built on a borrowed fact.

THE FOUR TRAPS THIS DESIGN AVOIDS, all of them ones this repo has already hit
-----------------------------------------------------------------------------
1. **OVERLAPPING WINDOWS.** A k-day return sampled daily shares k-1 days with its
   neighbour. `market-structure.md` records t falling **50.3 -> 17.7 -> 5.1**
   across exactly this correction. ⚠ Every window here is NON-OVERLAPPING:
   returns are cut into disjoint k-day blocks and consecutive blocks paired.
2. **PENNY-STOCK DOMINATION.** A corpus-wide mean is a micro-cap mean —
   measured here: intraday expectancy is negative ONLY below $5. ⚠ Everything is
   reported **stratified by price band**, and the pooled figure is shown only
   beside the strata so it cannot be quoted alone.
3. **THE SHARED-PRINT TRAP.** Sorting on a variable ending at price P and
   measuring an outcome starting at P manufactures the effect. ⚠ Consecutive
   non-overlapping blocks share the boundary print, so a single bad close enters
   block A positively and block B negatively — which biases the measured
   autocorrelation NEGATIVE. That is the honest direction to worry about at the
   1-day horizon and it is stated in the output rather than hidden.
4. **ADJUSTMENT-DISTORTED SERIES.** #2400: back-adjusted closes are meaningless
   as LEVELS. Returns are safe, levels are not — so the price-band cut uses the
   series' own MEDIAN close and series whose adjusted history spans an
   implausible range are excluded and counted.

⚠ Returns come from ``adj_close`` (split AND dividend adjusted), not ``close``.
The skill is explicit that `adj_close` is the return series; the backtester's use
of `close` is a separate defect (#2429).
"""

from __future__ import annotations

import sys
from collections import defaultdict

import numpy as np
import psycopg

from app.config import settings

#: Trading days. 1w, 1m, 3m, 6m, 1y, 3y — spanning the three regimes the skill claims.
HORIZONS = (1, 5, 21, 63, 126, 252, 756)

#: A series whose adjusted history spans more than this ratio is adjustment-distorted
#: (#2400: serial reverse-splitters inflate to 3e17). Counted, never silently dropped.
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


def _band(median_close: float) -> str:
    if median_close < 5:
        return "a <$5"
    if median_close < 20:
        return "b $5-20"
    if median_close < 100:
        return "c $20-100"
    return "d >=$100"


def _paired_blocks(prices: np.ndarray, k: int) -> tuple[np.ndarray, np.ndarray]:
    """Consecutive NON-OVERLAPPING k-day log returns, paired (r_t, r_t+1)."""
    n_blocks = len(prices) // k
    if n_blocks < 3:
        return np.empty(0), np.empty(0)
    edges = prices[: n_blocks * k + 1 : k] if len(prices) > n_blocks * k else prices[: n_blocks * k : k]
    if len(edges) < 3:
        return np.empty(0), np.empty(0)
    rets = np.diff(np.log(edges))
    return rets[:-1], rets[1:]


def main() -> int:
    with psycopg.connect(settings.database_url) as conn:
        series_ids = [r[0] for r in conn.execute(_SERIES_SQL).fetchall()]
        print(f"{len(series_ids)} series in the research corpus", flush=True)

        # (band, horizon) -> [x_values, y_values] accumulated across series
        pairs: dict[tuple[str, int], tuple[list[float], list[float], list[int]]] = defaultdict(lambda: ([], [], []))
        excluded_distorted = 0
        excluded_short = 0
        used = 0

        for n, series_id in enumerate(series_ids, start=1):
            rows = conn.execute(_BARS_SQL, {"series_id": series_id}).fetchall()
            if len(rows) < 100:
                excluded_short += 1
                continue
            prices = np.asarray([float(r[0]) for r in rows], dtype=np.float64)
            dates = [r[1] for r in rows]
            if float(prices.max()) / float(prices.min()) > MAX_ADJ_SPAN:
                excluded_distorted += 1
                continue
            median_close = conn.execute(_MEDIAN_SQL, {"series_id": series_id}).fetchone()
            if median_close is None or median_close[0] is None:
                excluded_short += 1
                continue
            band = _band(float(median_close[0]))
            used += 1
            for k in HORIZONS:
                x, y = _paired_blocks(prices, k)
                if x.size:
                    bucket = pairs[(band, k)]
                    bucket[0].extend(x.tolist())
                    bucket[1].extend(y.tolist())
                    # ⚠ The block's ordinal position in THIS series' history is a
                    # proxy for calendar period — series start at different dates,
                    # so it is imperfect, but it is what clusters observations that
                    # move together. See the clustered arm below.
                    starts = dates[: len(x) * k : k][: len(x)]
                    bucket[2].extend([d.year for d in starts])
            if n % 500 == 0:
                print(f"  {n}/{len(series_ids)} series", flush=True)

    print(f"\nused {used}   excluded: {excluded_short} too short, {excluded_distorted} adjustment-distorted\n")

    bands = sorted({b for b, _ in pairs})
    print("AUTOCORRELATION OF NON-OVERLAPPING RETURNS  (corr, t, n-pairs)")
    print("⚠ POSITIVE = momentum/continuation.  NEGATIVE = reversal.\n")
    header = f"{'horizon':>9} | " + " | ".join(f"{b:^22}" for b in bands)
    print(header)
    print("-" * len(header))

    table: dict[tuple[str, int], float] = {}
    for k in HORIZONS:
        cells = []
        for band in bands:
            xs, ys, _yr = pairs.get((band, k), ([], [], []))
            if len(xs) < 30:
                cells.append(f"{'insufficient':^22}")
                continue
            x = np.asarray(xs)
            y = np.asarray(ys)
            r = float(np.corrcoef(x, y)[0, 1])
            n = len(x)
            # t for a correlation coefficient, n-2 df.
            t = r * np.sqrt(max(n - 2, 1) / max(1 - r * r, 1e-12))
            table[(band, k)] = r
            cells.append(f"{r:+.4f} t{t:+7.1f} n{n:>7,}".rjust(22))
        label = f"{k}d" if k < 21 else (f"{k // 21}mo" if k < 252 else f"{k // 252}y")
        print(f"{label:>9} | " + " | ".join(cells))

    # ------------------------------------------------------------------
    # ⚠⚠ THE ARM THAT DECIDES WHETHER ANY OF THE ABOVE IS SIGNIFICANT.
    # The pooled t values printed above are ARTEFACTS: they treat every
    # (series, block) pair as an independent observation, when series in the
    # same calendar year move together. This repo has already paid for that
    # error once -- t fell 50.3 -> 17.7 -> 5.1 on one effect under exactly this
    # correction. Here the statistic is computed WITHIN each calendar year and
    # the sample is then the YEARS, so the degrees of freedom collapse to
    # something honest.
    # ------------------------------------------------------------------
    print("\n\nYEAR-CLUSTERED: correlation computed WITHIN each year, then averaged ACROSS years")
    print("⚠ n is now the number of YEARS, not the number of pairs. This is the honest inference.\n")
    header2 = f"{'horizon':>9} | " + " | ".join(f"{b:^24}" for b in bands)
    print(header2)
    print("-" * len(header2))
    clustered_t: dict[tuple[str, int], float] = {}
    for k in HORIZONS:
        cells = []
        for band in bands:
            xs, ys, yrs = pairs.get((band, k), ([], [], []))
            if len(xs) < 30:
                cells.append(f"{'insufficient':^24}")
                continue
            x, y, yr = np.asarray(xs), np.asarray(ys), np.asarray(yrs)
            per_year = []
            for year in np.unique(yr):
                m = yr == year
                if m.sum() < 20:
                    continue
                xa, ya = x[m], y[m]
                if xa.std() == 0 or ya.std() == 0:
                    continue
                per_year.append(float(np.corrcoef(xa, ya)[0, 1]))
            if len(per_year) < 5:
                cells.append(f"{'too few years':^24}")
                continue
            arr = np.asarray(per_year)
            mean = float(arr.mean())
            se = float(arr.std(ddof=1) / np.sqrt(len(arr)))
            t = mean / se if se > 0 else 0.0
            clustered_t[(band, k)] = t
            cells.append(f"{mean:+.4f} t{t:+6.1f} yrs{len(arr):>4}".rjust(24))
        label = f"{k}d" if k < 21 else (f"{k // 21}mo" if k < 252 else f"{k // 252}y")
        print(f"{label:>9} | " + " | ".join(cells))

    survivors = {kk: v for kk, v in clustered_t.items() if abs(v) >= 3.0}
    print(f"\n⚠ cells surviving |t| >= 3.0 after clustering: {len(survivors)} of {len(clustered_t)}")
    print("   (Harvey/Liu/Zhu's hurdle, applied to our own measurement)")
    for (band, k), t in sorted(survivors.items(), key=lambda kv: -abs(kv[1]))[:10]:
        label = f"{k}d" if k < 21 else (f"{k // 21}mo" if k < 252 else f"{k // 252}y")
        print(f"     {band:<12} {label:>5}  t {t:+.1f}")

    print("\n--- VERDICT against the skill's §2.8 claim ---")
    print("claim: NEGATIVE at 1d-1mo, POSITIVE at 3-12mo, NEGATIVE at 3y\n")
    failures = 0
    for band in bands:
        short = table.get((band, 5))
        mid = table.get((band, 252))
        long = table.get((band, 756))
        if short is None or mid is None:
            print(f"  {band:<12} insufficient data")
            continue
        short_ok = short < 0
        mid_ok = mid > 0
        long_txt = "n/a" if long is None else f"{long:+.4f} {'REVERSAL' if long < 0 else 'CONTINUATION'}"
        print(
            f"  {band:<12} 1w {short:+.4f} {'REVERSAL ok' if short_ok else 'CONTINUATION  <-- against claim':<32}"
            f" 1y {mid:+.4f} {'MOMENTUM ok' if mid_ok else 'REVERSAL  <-- against claim':<32} 3y {long_txt}"
        )
        if not short_ok or not mid_ok:
            failures += 1

    print(f"\n{len(bands) - failures} of {len(bands)} price bands match the claimed short/medium sign pattern.")
    print(
        "⚠ Boundary-print bias pushes the 1-day figure NEGATIVE by construction "
        "(consecutive blocks share a print), so treat 1d as the least trustworthy row."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
