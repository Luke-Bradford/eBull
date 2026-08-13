"""Short the isolated loser: does the edge survive its own tail? (#2437)

    PYTHONPATH=. uv run python scripts/verify_2437_short_continuation.py

⚠ NOTHING IS WRITTEN. Gate on the EXIT CODE. Never pipe into head/tail.

WHY
---
`verify_2437_loser_reversal.py` measured buying one-day losers and found it loses
money monotonically -- worse with a bigger drop, worse with a longer hold, worse
when fewer names fall alongside. Reported three times as a null.

⚠⚠ It was never a null. It is the strongest effect measured in this research pass,
pointing the other way: an isolated stock that drops hard KEEPS dropping. It read
as failure only because the project was long-only, so the finding had no
expression beyond "do not buy". The operator lifted that constraint on 2026-08-09
(shorting permitted for research and paper trading; leverage still barred until
validation), which makes the short leg testable for the first time.

⚠⚠ A MEAN IS NOT ENOUGH FOR A SHORT, and this is the whole reason this script
exists rather than a sign flip in the old one. A long's worst case is -100%; a
short's is unbounded. A name that falls 12% and then rebounds 60% costs more than
twenty good trades earn. So the mean is reported LAST here, after the things that
actually decide whether it is tradable:

  - win rate per trade, and the median, not just the mean
  - the worst trades, and what the mean looks like with the best 1% of shorts
    removed (i.e. strip the collapses and see if anything is left)
  - ⚠⚠ DELISTING ATTRIBUTION. Our corpus is survivorship-controlled, so it
    contains names that went to zero. A backtested short harvests those in full.
    In reality the position is bought in, halted, or force-closed at a price we
    never see. If the edge lives in series that terminate shortly after the
    signal, it is an artefact of holding a short through an event we could not
    have held through. This is the single most likely way the result is wrong.

⚠ COST MODEL IS NOT THE LONG ONE. An eToro short is a CFD. Easy-to-borrow costs
spread only; hard-to-borrow (>10% annual) accrues daily at 21:00 GMT and TRIPLE at
weekends -- and a stock that just fell 12% is the archetypal hard-to-borrow name.
`BORROW_BPS_PER_DAY` is a placeholder for the >10%/yr tier (~2.7 bps/day) and is
applied per calendar day held, not per trading day, because financing accrues at
weekends. ⚠ It is an ASSUMPTION, not a measurement -- eToro does not publish a
per-symbol borrow table via the API, so the honest treatment is to show the result
at several borrow tiers and let the reader see where it breaks.
"""

from __future__ import annotations

import sys
from collections import defaultdict
from datetime import date

import numpy as np
import psycopg

from app.config import settings

START = "2020-01-01"
DROPS = (-0.05, -0.08, -0.12)
HOLDS = (5, 10)
MIN_PRICE = 20.0
MIN_DOLLAR_VOL = 10_000_000.0
SPREAD_BPS = 50.0
BORROW_TIERS_BPS_PER_DAY = (0.0, 2.7, 8.2)  # 0%, ~10%/yr, ~30%/yr annualised
TERMINATION_LOOKAHEAD = 252  # a series ending within a year counts as "died"

_SERIES = """
    SELECT series_id, MAX(bar_date) AS last_bar
    FROM research_price_daily
    WHERE bar_date >= %(s)s
    GROUP BY series_id
    ORDER BY series_id
"""
_BARS = """
    SELECT bar_date, open, close, adj_close, volume
    FROM research_price_daily
    WHERE series_id = %(sid)s AND bar_date >= %(s)s
      AND open > 0 AND close > 0 AND adj_close > 0
    ORDER BY bar_date
"""


def _cluster(groups: dict[date, list[float]]) -> tuple[float, float, int]:
    means = [float(np.mean(v)) for v in groups.values() if v]
    if len(means) < 5:
        return float("nan"), float("nan"), len(means)
    a = np.asarray(means)
    m = float(a.mean())
    se = float(a.std(ddof=1) / np.sqrt(len(a)))
    return m, (m / se if se > 0 else 0.0), len(a)


def main() -> int:
    with psycopg.connect(settings.database_url) as conn:
        rows = conn.execute(_SERIES, {"s": START}).fetchall()
        last_bar = {r[0]: r[1] for r in rows}
        corpus_end = max(last_bar.values())
        sids = [r[0] for r in rows]
        print(f"{len(sids):,} series, corpus ends {corpus_end}", flush=True)

        # (drop, hold) -> day -> [short gross bps]; plus flat lists for tail stats
        by_day: dict[tuple[float, int], dict[date, list[float]]] = defaultdict(lambda: defaultdict(list))
        by_day_alive: dict[tuple[float, int], dict[date, list[float]]] = defaultdict(lambda: defaultdict(list))
        flat: dict[tuple[float, int], list[float]] = defaultdict(list)
        died: dict[tuple[float, int], list[float]] = defaultdict(list)
        held_days: dict[tuple[float, int], list[int]] = defaultdict(list)

        for n, sid in enumerate(sids, start=1):
            bars = conn.execute(_BARS, {"sid": sid, "s": START}).fetchall()
            if len(bars) < 60:
                continue
            dts = [b[0] for b in bars]
            op = np.asarray([float(b[1]) for b in bars])
            cl = np.asarray([float(b[2]) for b in bars])
            adj = np.asarray([float(b[3]) for b in bars])
            vol = np.asarray([float(b[4] or 0.0) for b in bars])
            adj_open = op * (adj / cl)
            ret1 = np.empty_like(adj)
            ret1[0] = np.nan
            ret1[1:] = adj[1:] / adj[:-1] - 1.0
            dvol = cl * vol
            # ⚠ a series whose last bar is well inside the corpus has stopped
            # trading -- delisted, acquired, or renamed. Shorting into that is the
            # case a backtest flatters most.
            terminated = (corpus_end - last_bar[sid]).days > 30

            for i in range(21, len(cl) - max(HOLDS) - 1):
                if cl[i] < MIN_PRICE or float(np.median(dvol[i - 20 : i])) < MIN_DOLLAR_VOL:
                    continue
                entry = adj_open[i + 1]
                if not np.isfinite(entry) or entry <= 0:
                    continue
                day = dts[i + 1]
                dies_soon = terminated and (last_bar[sid] - day).days <= TERMINATION_LOOKAHEAD
                for th in DROPS:
                    if ret1[i] > th:
                        continue
                    for h in HOLDS:
                        key = (th, h)
                        # ⚠ SHORT: we profit when the price FALLS, so the sign flips.
                        short_bps = -(adj[i + h] / entry - 1.0) * 1e4
                        by_day[key][day].append(short_bps)
                        flat[key].append(short_bps)
                        held_days[key].append((dts[i + h] - day).days)
                        if dies_soon:
                            died[key].append(short_bps)
                        else:
                            by_day_alive[key][day].append(short_bps)
            if n % 1000 == 0:
                print(f"  {n}/{len(sids)}", flush=True)

    print(f"\nSHORT THE ONE-DAY LOSER, {START} onward")
    print("entry = SHORT at next bar's adjusted OPEN, cover at adjusted CLOSE h bars later")
    print(f"universe: prior close >= ${MIN_PRICE:.0f}, 20d median dollar volume >= ${MIN_DOLLAR_VOL / 1e6:.0f}m")
    print(f"⚠ DAY-CLUSTERED t. ⚠ spread {SPREAD_BPS:.0f} bps round trip, borrow shown per tier.\n")

    hdr = f"{'signal':>12}{'hold':>6}{'trades':>9}{'gross':>9}{'median':>9}{'win%':>7}{'t':>7}{'days':>7}"
    hdr += "".join(f"{f'net@{b}':>10}" for b in BORROW_TIERS_BPS_PER_DAY)
    print(hdr)
    print("-" * len(hdr))
    for th in DROPS:
        for h in HOLDS:
            key = (th, h)
            m, t, nd = _cluster(by_day[key])
            if not np.isfinite(m):
                continue
            arr = np.asarray(flat[key])
            cal = float(np.mean(held_days[key])) if held_days[key] else h
            line = (
                f"{th:>11.0%}{h:>6}{len(arr):>9,}{m:>9.1f}{float(np.median(arr)):>9.1f}"
                f"{float((arr > 0).mean()) * 100:>7.1f}{t:>7.2f}{nd:>7}"
            )
            for b in BORROW_TIERS_BPS_PER_DAY:
                line += f"{m - SPREAD_BPS - b * cal:>10.1f}"
            print(line)
    print(f"\n⚠ borrow tiers are bps/CALENDAR day ({BORROW_TIERS_BPS_PER_DAY}) = 0%, ~10%/yr, ~30%/yr.")

    print("\n⚠⚠ TAIL -- a short's losses are unbounded, so the mean is not the story.")
    print(f"{'signal':>12}{'hold':>6}{'worst':>10}{'p1':>9}{'p5':>9}{'p95':>9}{'best':>10}{'mean ex-top1%':>15}")
    for th in DROPS:
        for h in HOLDS:
            arr = np.asarray(flat[(th, h)])
            if arr.size < 100:
                continue
            cut = float(np.percentile(arr, 99))
            ex = arr[arr <= cut]
            print(
                f"{th:>11.0%}{h:>6}{arr.min():>10.0f}{float(np.percentile(arr, 1)):>9.0f}"
                f"{float(np.percentile(arr, 5)):>9.0f}{float(np.percentile(arr, 95)):>9.0f}"
                f"{arr.max():>10.0f}{float(ex.mean()):>15.1f}"
            )
    print("⚠ 'mean ex-top1%' strips the biggest SHORT WINS (the collapses). If the")
    print("  edge vanishes there, it is a lottery on a handful of blow-ups, not a strategy.")

    print("\n⚠⚠ DELISTING ATTRIBUTION -- the most likely way this result is wrong.")
    print(f"a trade is 'dying' if its series stops trading within {TERMINATION_LOOKAHEAD} days of entry.")
    print(
        f"{'signal':>12}{'hold':>6}{'dying':>8}{'share':>8}{'dying mean':>12}"
        f"{'ALIVE mean':>12}{'alive t':>9}{'days':>7}"
    )
    for th in DROPS:
        for h in HOLDS:
            key = (th, h)
            arr = np.asarray(flat[key])
            d = np.asarray(died[key])
            am, at, ad = _cluster(by_day_alive[key])
            if arr.size < 100 or not np.isfinite(am):
                continue
            print(
                f"{th:>11.0%}{h:>6}{d.size:>8,}{d.size / arr.size * 100:>7.1f}%"
                f"{(d.mean() if d.size else float('nan')):>12.1f}{am:>12.1f}{at:>9.2f}{ad:>7}"
            )
    print("⚠ 'ALIVE mean' excludes every name that stopped trading within the year.")
    print("  That is the honest number: it is the only one we could actually have held.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
