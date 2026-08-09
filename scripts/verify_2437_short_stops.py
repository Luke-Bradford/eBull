"""The surviving short arm, with a stop attached (#2437).

    PYTHONPATH=. uv run python scripts/verify_2437_short_stops.py

⚠ NOTHING IS WRITTEN. Gate on the EXIT CODE. Never pipe into head/tail.

WHY
---
`verify_2437_short_continuation.py` left exactly one arm standing: short a stock
that fell >=12% in a day, cover 5 days later. It survived costs at every borrow
tier, survived deleting its best 1% of trades, survived excluding every name that
stopped trading, and carried a positive median with a 54.4% win rate over 1,364
independent event days.

⚠⚠ And it is still untradable as written, because the worst single trade was
-412% and 1% of trades lost 50% or more. A short's loss is unbounded, so that is
not a tail to note and move past -- one such trade erases roughly 190 average
winners. A hard stop is therefore STRUCTURAL, not a refinement.

⚠ Which means the stopped version is a DIFFERENT STRATEGY and the unstopped
numbers do not describe it. A stop truncates the left tail, which is the point,
but it also converts some eventual winners into realised losses -- the position
that spikes 12% on day two and then collapses pays nothing once you are out.
Whether that trade is worth making is exactly what this measures, and it cannot
be reasoned out.

HOW THE STOP IS FILLED -- the honest treatment
----------------------------------------------
⚠⚠ A stop is not a guaranteed price. Each day we check the bar in the order the
session actually delivers it:

  1. If the OPEN is already at or above the stop, we fill at the OPEN. This is
     the gap-through case, and it is where a short actually gets hurt -- pretending
     the stop filled at its level would invent money that a gap never offered.
  2. Otherwise, if the HIGH touched the stop, we fill AT the stop level.

⚠ This is still optimistic. Intraday we could be filled worse than the stop even
without a gap, and a halt can reopen far away. Read the stopped figures as an
upper bound, in the same way the unstopped ones were a lower bound on risk.
"""

from __future__ import annotations

import sys
from collections import defaultdict
from datetime import date

import numpy as np
import psycopg

from app.config import settings

START = "2020-01-01"
DROP = -0.12  # the surviving threshold
HOLD = 5  # the surviving hold
STOPS = (None, 0.20, 0.12, 0.08, 0.05)  # cover if price rises this far above entry
MIN_PRICE = 20.0
MIN_DOLLAR_VOL = 10_000_000.0
SPREAD_BPS = 50.0
BORROW_TIERS = (0.0, 2.7, 8.2)  # bps per financed day: 0%, ~10%/yr, ~30%/yr

# ⚠ How many days of borrow a HOLD-bar position actually pays for.
# eToro charges the fee at 21:00 GMT each day a position is open and TRIPLES it on
# Friday for stocks, to cover the weekend. So a 5-trading-day hold pays four ordinary
# nights plus one Friday at 3x = 4 + 3 = 7 day-equivalents.
#
# ⚠ It is a FLAT approximation and coarser than the docstring's prose: a real hold
# straddles a different number of Fridays depending on the weekday it opens (0 or 1
# here, more for longer holds), and holidays are ignored entirely. Every hold is
# charged the worst case of exactly one triple, which errs against the strategy --
# the right direction for a cost assumption, but it is an assumption, not a model.
BORROW_DAY_EQUIVALENTS = (HOLD - 1) + 3

_SERIES = "SELECT DISTINCT series_id FROM research_price_daily WHERE bar_date >= %(s)s ORDER BY series_id"
_BARS = """
    SELECT bar_date, open, high, low, close, adj_close, volume
    FROM research_price_daily
    WHERE series_id = %(sid)s AND bar_date >= %(s)s
      AND open > 0 AND high > 0 AND low > 0 AND close > 0 AND adj_close > 0
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
    by_day: dict[float, dict[date, list[float]]] = defaultdict(lambda: defaultdict(list))
    flat: dict[float, list[float]] = defaultdict(list)
    stopped: dict[float, int] = defaultdict(int)
    gapped: dict[float, int] = defaultdict(int)

    with psycopg.connect(settings.database_url) as conn:
        sids = [r[0] for r in conn.execute(_SERIES, {"s": START}).fetchall()]
        print(f"{len(sids):,} series", flush=True)
        for n, sid in enumerate(sids, start=1):
            bars = conn.execute(_BARS, {"sid": sid, "s": START}).fetchall()
            if len(bars) < 60:
                continue
            dts = [b[0] for b in bars]
            op = np.asarray([float(b[1]) for b in bars])
            hi = np.asarray([float(b[2]) for b in bars])
            cl = np.asarray([float(b[4]) for b in bars])
            adj = np.asarray([float(b[5]) for b in bars])
            vol = np.asarray([float(b[6] or 0.0) for b in bars])
            f = adj / cl
            adj_open, adj_hi = op * f, hi * f
            ret1 = np.empty_like(adj)
            ret1[0] = np.nan
            ret1[1:] = adj[1:] / adj[:-1] - 1.0
            dvol = cl * vol

            for i in range(21, len(cl) - HOLD - 1):
                if ret1[i] > DROP or cl[i] < MIN_PRICE:
                    continue
                if float(np.median(dvol[i - 20 : i])) < MIN_DOLLAR_VOL:
                    continue
                entry = adj_open[i + 1]
                if not np.isfinite(entry) or entry <= 0:
                    continue
                day = dts[i + 1]
                for s in STOPS:
                    k = -1.0 if s is None else s
                    exit_px = adj[i + HOLD]
                    if s is not None:
                        level = entry * (1.0 + s)
                        for j in range(i + 1, i + HOLD + 1):
                            if adj_open[j] >= level:  # gap-through: no stop protection
                                exit_px = adj_open[j]
                                stopped[k] += 1
                                gapped[k] += 1
                                break
                            if adj_hi[j] >= level:
                                exit_px = level
                                stopped[k] += 1
                                break
                    r = -(exit_px / entry - 1.0) * 1e4
                    by_day[k][day].append(r)
                    flat[k].append(r)
            if n % 1000 == 0:
                print(f"  {n}/{len(sids)}", flush=True)

    print(f"\nSHORT A >={abs(DROP):.0%} ONE-DAY DROP, COVER AFTER {HOLD} BARS, {START} onward")
    print("entry = SHORT at next bar's adjusted OPEN. stop = cover if price rises that far.")
    print("⚠ gap-through fills at the OPEN, not the stop level. ⚠ DAY-CLUSTERED t.\n")
    hdr = f"{'stop':>8}{'trades':>9}{'stopped':>9}{'gapped':>8}{'gross':>9}{'median':>9}{'win%':>7}{'t':>7}"
    hdr += f"{'ex-top1%':>10}{'worst':>9}" + "".join(f"{f'net@{b}':>9}" for b in BORROW_TIERS)
    print(hdr)
    print("-" * len(hdr))
    for s in STOPS:
        k = -1.0 if s is None else s
        m, t, _ = _cluster(by_day[k])
        arr = np.asarray(flat[k])
        if arr.size < 100 or not np.isfinite(m):
            continue
        ex = arr[arr <= float(np.percentile(arr, 99))]
        label = "none" if s is None else f"{s:.0%}"
        line = (
            f"{label:>8}{arr.size:>9,}{stopped[k]:>9,}{gapped[k]:>8,}{m:>9.1f}"
            f"{float(np.median(arr)):>9.1f}{float((arr > 0).mean()) * 100:>7.1f}{t:>7.2f}"
            f"{float(ex.mean()):>10.1f}{arr.min():>9.0f}"
        )
        for b in BORROW_TIERS:
            line += f"{m - SPREAD_BPS - b * BORROW_DAY_EQUIVALENTS:>9.1f}"
        print(line)

    print("\n⚠ 'gapped' counts stops that filled at an OPEN above the level -- the case")
    print("  where the stop gave no protection at all. It is the honest cost of the tail.")
    print("⚠⚠ Read across: a tighter stop should cut 'worst' hard. If it also cuts")
    print("   'gross' and 'ex-top1%' to nothing, the tail WAS the strategy and there is")
    print("   no tradable version -- truncating the loss also truncates the edge.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
