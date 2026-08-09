"""Does CONFLUENCE pay? Forward returns by how many conditions align (#2437).

    PYTHONPATH=. uv run python scripts/verify_2437_confluence.py

⚠ NOTHING IS WRITTEN. Gate on the EXIT CODE. Never pipe into head/tail.

WHY THIS EXISTS -- THE INSTRUMENT, NOT THE IDEA
-----------------------------------------------
Every prior test in this research pass was UNIVARIATE: one condition, averaged
across the whole cross-section. Gap down >2%. One-day drop <=-5%. 12-month
momentum. All returned nothing usable after costs and clustering, and each time
the conclusion recorded was "the effect is dead".

⚠⚠ That conclusion does not follow, and the operator said so five times before it
landed. If an edge exists only where several conditions CO-OCCUR, then measuring
each condition alone returns ~zero BY CONSTRUCTION: the firings where the other
conditions were absent swamp the few where they were present. A univariate test
is structurally incapable of finding a conjunction-dependent effect. We were
running the instrument that cannot detect the thing, then reporting the null.

WHAT THIS DOES DIFFERENTLY
--------------------------
Eleven pre-specified conditions, each a plain binary read off the chart, all
computable at the close of day t. Then ONE measurement: forward return bucketed
by HOW MANY of them are true simultaneously.

⚠ That is deliberately a single degree of freedom, not a search. Testing all 2^11
subsets would manufacture a winner from noise on 6,700 stocks over six years.
Asking "does expectancy rise monotonically with alignment count" asks the
operator's actual question -- *do traders need several things to line up* -- and
a monotonic gradient across buckets is far harder to fake than one lucky cell.
The breadth split in `verify_2437_loser_reversal.py` is the precedent: the
gradient was the finding, not any single bucket.

⚠⚠ INCLUDES THE TWO FAMILIES I DISMISSED WITHOUT TESTING. Support proximity
(distance to a prior pivot low) and Fibonacci retracement (position within the
60-day swing near 38.2/50/61.8%) are conditions 8 and 9. Both are constructed
mechanically -- no hand-drawn lines, no discretion -- so "does price behave
differently near these levels" becomes a measurement rather than an opinion. If
they carry nothing, the per-condition table will say so; that is a result, not a
prior.

DISCIPLINE
----------
- Every condition uses data up to and including the close of day t. Entry is the
  OPEN of day t+1. No lookahead anywhere.
- Adjusted prices throughout, so dividends and splits cannot fake a signal.
- ⚠ DAY-CLUSTERED inference. Conditions like trend and breadth are shared across
  hundreds of names at once, so pooling observations would inflate t enormously.
- Costs deducted. Every bucket is reported against the unconditional baseline
  over the same days -- an alignment count that merely matches drift is not edge.
- Per-condition marginals are printed too, precisely so the univariate view and
  the conjunction view sit side by side and can be compared.
"""

from __future__ import annotations

import sys
from collections import defaultdict
from datetime import date

import numpy as np
import psycopg

from app.config import settings

START = "2019-06-01"  # 6mo warmup before the 2020+ measurement window
MEASURE_FROM = 2020
HOLDS = (5, 10)
MIN_PRICE = 20.0
MIN_DOLLAR_VOL = 10_000_000.0
COST_BPS = 50.0

CONDITIONS = (
    "1 uptrend c>50>200",
    "2 pullback 3-12%",
    "3 near 52w high",
    "4 vol regime calm",
    "5 rel strength +",
    "6 volume quiet",
    "7 strong close",
    "8 near support",
    "9 fib 38/50/62",
    "10 rsi 35-55",
    "11 breadth up day",
)

# ⚠⚠ PLACEBO LEVELS. Codex round 6: the only test that settles whether a level type
# is special is to compare it against ARBITRARY levels drawn from the same swing.
# A 2021 study found Fibonacci zones did not beat non-Fibonacci zones -- so a bare
# "price is near 38.2%" hit rate proves nothing, because price is often mid-range
# and any fraction would score. These two ride alongside conditions 8 and 9 and are
# reported separately; they are NOT counted in the alignment total.
#
# The read is a DIFFERENCE, not a level: if `fib` and `fib placebo` score the same,
# 38.2/50/61.8 carry nothing beyond "somewhere in the range". Same for support
# against a randomly displaced pseudo-level.
PLACEBOS = ("P1 fake fib 29/44/71", "P2 fake support")

_SERIES = "SELECT DISTINCT series_id FROM research_price_daily WHERE bar_date >= %(s)s ORDER BY series_id"
_BARS = """
    SELECT bar_date, open, high, low, close, adj_close, volume
    FROM research_price_daily
    WHERE series_id = %(sid)s AND bar_date >= %(s)s
      AND open > 0 AND high > 0 AND low > 0 AND close > 0 AND adj_close > 0
    ORDER BY bar_date
"""


def _sma(x: np.ndarray, n: int) -> np.ndarray:
    out = np.full_like(x, np.nan)
    if len(x) >= n:
        c = np.cumsum(np.insert(x, 0, 0.0))
        out[n - 1 :] = (c[n:] - c[:-n]) / n
    return out


def _rsi(x: np.ndarray, n: int = 14) -> np.ndarray:
    out = np.full_like(x, np.nan)
    d = np.diff(x)
    up = np.where(d > 0, d, 0.0)
    dn = np.where(d < 0, -d, 0.0)
    if len(d) < n:
        return out
    au, ad = up[:n].mean(), dn[:n].mean()
    for i in range(n, len(d)):
        au = (au * (n - 1) + up[i]) / n  # Wilder smoothing, not a plain mean
        ad = (ad * (n - 1) + dn[i]) / n
        out[i + 1] = 100.0 if ad == 0 else 100.0 - 100.0 / (1.0 + au / ad)
    return out


def _cluster(groups: dict[date, list[float]]) -> tuple[float, float, int]:
    means = [float(np.mean(v)) for v in groups.values() if v]
    if len(means) < 5:
        return float("nan"), float("nan"), len(means)
    a = np.asarray(means)
    m = float(a.mean())
    se = float(a.std(ddof=1) / np.sqrt(len(a)))
    return m, (m / se if se > 0 else 0.0), len(a)


def _market_context(
    signal_date: date,
    lookback_date: date,
    mkt: dict[date, float],
    mkt_cum: dict[date, float],
) -> tuple[float, bool] | None:
    """Return causal market context observable at the signal close.

    The trade is grouped by its next-open entry date, but market breadth and
    relative strength belong to the completed signal bar.  Using the entry
    date here leaks that session's return into a decision made the night before.
    """
    if signal_date not in mkt or signal_date not in mkt_cum or lookback_date not in mkt_cum:
        return None
    market_return_21d = mkt_cum[signal_date] / mkt_cum[lookback_date] - 1.0
    return market_return_21d, mkt[signal_date] > 0


def main() -> int:
    conn = psycopg.connect(settings.database_url)
    sids = [r[0] for r in conn.execute(_SERIES, {"s": START}).fetchall()]
    print(f"{len(sids):,} series", flush=True)

    # ---- pass 1: equal-weight market return per day, for breadth + rel strength
    mkt_sum: dict[date, float] = defaultdict(float)
    mkt_n: dict[date, int] = defaultdict(int)
    for n, sid in enumerate(sids, start=1):
        rows = conn.execute(_BARS, {"sid": sid, "s": START}).fetchall()
        if len(rows) < 40:
            continue
        adj = np.asarray([float(r[5]) for r in rows])
        cl = np.asarray([float(r[4]) for r in rows])
        for i in range(1, len(rows)):
            if cl[i] >= MIN_PRICE:
                mkt_sum[rows[i][0]] += adj[i] / adj[i - 1] - 1.0
                mkt_n[rows[i][0]] += 1
        if n % 1500 == 0:
            print(f"  pass1 {n}/{len(sids)}", flush=True)
    mkt = {d: mkt_sum[d] / mkt_n[d] for d in mkt_sum if mkt_n[d] >= 50}
    dates_sorted = sorted(mkt)
    mkt_cum = {}
    acc = 1.0
    for d in dates_sorted:
        acc *= 1.0 + mkt[d]
        mkt_cum[d] = acc
    print(f"market series: {len(mkt):,} days", flush=True)

    # ---- pass 2: conditions + forward returns
    by_count: dict[tuple[int, int], dict[date, list[float]]] = defaultdict(lambda: defaultdict(list))
    by_cond: dict[tuple[int, int], dict[date, list[float]]] = defaultdict(lambda: defaultdict(list))
    base: dict[int, dict[date, list[float]]] = defaultdict(lambda: defaultdict(list))
    by_placebo: dict[tuple[int, int], dict[date, list[float]]] = defaultdict(lambda: defaultdict(list))
    cond_hits = np.zeros(len(CONDITIONS), dtype=np.int64)
    placebo_hits = np.zeros(len(PLACEBOS), dtype=np.int64)
    total = 0

    for n, sid in enumerate(sids, start=1):
        rows = conn.execute(_BARS, {"sid": sid, "s": START}).fetchall()
        if len(rows) < 260:
            continue
        dts = [r[0] for r in rows]
        op = np.asarray([float(r[1]) for r in rows])
        hi = np.asarray([float(r[2]) for r in rows])
        lo = np.asarray([float(r[3]) for r in rows])
        cl = np.asarray([float(r[4]) for r in rows])
        adj = np.asarray([float(r[5]) for r in rows])
        vol = np.asarray([float(r[6] or 0.0) for r in rows])

        f = adj / cl
        adj_open, adj_hi, adj_lo = op * f, hi * f, lo * f
        sma50, sma200 = _sma(adj, 50), _sma(adj, 200)
        rsi = _rsi(adj)
        tr = np.maximum(
            adj_hi[1:] - adj_lo[1:], np.maximum(np.abs(adj_hi[1:] - adj[:-1]), np.abs(adj_lo[1:] - adj[:-1]))
        )
        atr = np.full_like(adj, np.nan)
        atr[20:] = np.convolve(tr, np.ones(20) / 20, mode="valid")[: len(adj) - 20]
        dvol = cl * vol
        rng = adj_hi - adj_lo
        cpos = np.where(rng > 0, (adj - adj_lo) / np.maximum(rng, 1e-12), 0.5)

        # pivot lows: a low that is the minimum of its +/-5 neighbourhood.
        # ⚠ uses only bars up to j+5, and is consulted at i > j+5, so no lookahead.
        pivots = [j for j in range(5, len(adj) - 5) if adj_lo[j] == adj_lo[j - 5 : j + 6].min()]

        for i in range(252, len(adj) - max(HOLDS) - 1):
            if dts[i].year < MEASURE_FROM or cl[i] < MIN_PRICE:
                continue
            if float(np.median(dvol[i - 20 : i])) < MIN_DOLLAR_VOL:
                continue
            if not np.isfinite(sma200[i]) or not np.isfinite(atr[i]) or atr[i] <= 0:
                continue
            entry = adj_open[i + 1]
            if not np.isfinite(entry) or entry <= 0:
                continue
            signal_date = dts[i]
            entry_date = dts[i + 1]
            market_context = _market_context(signal_date, dts[i - 21], mkt, mkt_cum)
            if market_context is None:
                continue
            m21, breadth_up = market_context

            w = adj[i - 251 : i + 1]
            hi252, lo60, hi60 = w.max(), adj_lo[i - 59 : i + 1].min(), adj_hi[i - 59 : i + 1].max()
            peak10 = adj[i - 10 : i + 1].max()
            pull = adj[i] / peak10 - 1.0
            prior = [p for p in pivots if p < i - 5 and adj_lo[p] < adj[i]]
            sup_d = min(((adj[i] - adj_lo[p]) / atr[i] for p in prior), default=99.0)
            fib = (adj[i] - lo60) / (hi60 - lo60) if hi60 > lo60 else -1.0
            r21 = adj[i] / adj[i - 21] - 1.0
            c = (
                adj[i] > sma50[i] > sma200[i],
                -0.12 <= pull <= -0.03,
                adj[i] / hi252 >= 0.85,
                0.010 <= atr[i] / adj[i] <= 0.035,
                r21 > m21,
                dvol[i] < float(np.median(dvol[i - 20 : i])),
                cpos[i] > 0.6,
                sup_d <= 0.5,
                any(abs(fib - lv) <= 0.03 for lv in (0.382, 0.5, 0.618)),
                35.0 <= rsi[i] <= 55.0,
                breadth_up,
            )
            # ⚠ placebos are matched in SHAPE to conditions 9 and 8: the same
            # tolerance, the same swing, the same ATR unit -- only the level itself
            # is arbitrary. Anything else and the comparison measures the tolerance
            # rather than the level.
            p = (
                any(abs(fib - lv) <= 0.03 for lv in (0.29, 0.44, 0.71)),
                abs(sup_d - 1.5) <= 0.25,
            )
            k = int(sum(c))
            cond_hits += np.asarray(c, dtype=np.int64)
            placebo_hits += np.asarray(p, dtype=np.int64)
            total += 1
            for h in HOLDS:
                r = (adj[i + h] / entry - 1.0) * 1e4
                base[h][entry_date].append(r)
                by_count[(k, h)][entry_date].append(r)
                for ci, on in enumerate(c):
                    if on:
                        by_cond[(ci, h)][entry_date].append(r)
                for pi, on in enumerate(p):
                    if on:
                        by_placebo[(pi, h)][entry_date].append(r)
        if n % 1000 == 0:
            print(f"  pass2 {n}/{len(sids)}", flush=True)
    conn.close()

    print(f"\nCONFLUENCE TEST, {MEASURE_FROM}+  ({total:,} stock-days)")
    print("entry = next bar's adjusted OPEN, exit = adjusted CLOSE h bars later")
    print(f"⚠ DAY-CLUSTERED t. ⚠ NET deducts {COST_BPS:.0f} bps.\n")

    for h in HOLDS:
        bm, bt, _ = _cluster(base[h])
        print(f"--- hold {h} bars --- unconditional baseline {bm:.2f} bps (t {bt:.2f})")
        print(f"{'aligned':>9}{'stock-days':>12}{'gross bps':>11}{'NET':>9}{'vs base':>10}{'t':>8}{'days':>7}")
        for k in range(len(CONDITIONS) + 1):
            m, t, nd = _cluster(by_count[(k, h)])
            if not np.isfinite(m):
                continue
            ev = sum(len(v) for v in by_count[(k, h)].values())
            print(f"{k:>9}{ev:>12,}{m:>11.2f}{m - COST_BPS:>9.2f}{m - bm:>10.2f}{t:>8.2f}{nd:>7}")
        print()

    print("PER-CONDITION MARGINALS (the univariate view, for comparison)")
    print(f"{'condition':>20}{'fire rate':>11}{'5d bps':>9}{'t':>8}{'10d bps':>10}{'t':>8}")
    for ci, name in enumerate(CONDITIONS):
        m5, t5, _ = _cluster(by_cond[(ci, 5)])
        m10, t10, _ = _cluster(by_cond[(ci, 10)])
        rate = cond_hits[ci] / max(total, 1)
        print(f"{name:>20}{rate:>10.1%}{m5:>9.2f}{t5:>8.2f}{m10:>10.2f}{t10:>8.2f}")

    print("\n⚠⚠ PLACEBO LEVELS -- compare against conditions 8 and 9 above.")
    print("   Same tolerance, same swing, arbitrary level. If these score the same,")
    print("   the level type carries nothing beyond 'price is somewhere in the range'.")
    for pi, name in enumerate(PLACEBOS):
        m5, t5, _ = _cluster(by_placebo[(pi, 5)])
        m10, t10, _ = _cluster(by_placebo[(pi, 10)])
        rate = placebo_hits[pi] / max(total, 1)
        print(f"{name:>20}{rate:>10.1%}{m5:>9.2f}{t5:>8.2f}{m10:>10.2f}{t10:>8.2f}")

    print("\n⚠ The question is the GRADIENT across alignment counts, not any one bucket.")
    print("⚠ If confluence is real, 'vs base' rises with the count. If it is folklore,")
    print("  the column is flat or noisy and the marginals already told the whole story.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
