"""Extreme one-day loser reversal on OUR data, 2020-2026 only (#2437).

    PYTHONPATH=. uv run python scripts/verify_2437_loser_reversal.py

⚠ NOTHING IS WRITTEN. Gate on the EXIT CODE. Never pipe into head/tail.

WHY THIS TEST, AND WHY ONLY 2020+
---------------------------------
Codex round 5, briefed with SEC/fundamental data forbidden, found exactly ONE
short-horizon long-only candidate with post-2020 evidence: buy after a large
one-day drop, hold 1-10 days. Reported ~+1.1% over 10 days on CRSP common stocks
through 2022. Everything else it surfaced was pre-2020, non-US, long-short, or
needed order-book data we do not receive.

⚠⚠ The sample is 2020+ **by construction, not by convenience**. This session's
gap-fade result measured +156 bps at t 5.73 over 1962-2026 and +11.8 bps at
t 1.39 over 2020-2026 alone: the effect died at zero commissions. A full-sample
number here would be a statement about a market that no longer exists. See
`docs/proposals/ta/2026-08-09-gap-fade-and-the-decay-finding.md`.

THE FOUR TRAPS THIS CONTROLS FOR
--------------------------------
1. ⚠⚠ **Corporate actions.** Codex ranked ex-dividend/split contamination as the
   single most likely explanation of the gap-fade edge, and the gap test did not
   control for it. Here EVERY return -- signal and outcome -- is computed on
   adjusted prices. The entry open is adjusted with its own bar's factor
   (`adj_close / close`), so a dividend cannot masquerade as a one-day crash and
   cannot fake a reversal.
2. ⚠⚠ **Day clustering.** Year-clustering is not enough: crashes are exactly when
   hundreds of names fire at once, so "800 names on one day" is nowhere near 800
   independent bets. Primary inference averages WITHIN each calendar day, then
   takes t across days. Year-clustered is reported beside it for comparison.
3. **Lookahead.** The signal is known at the close of day t. Entry is the OPEN of
   day t+1 -- a price we could transact at. The liquidity screen uses the prior
   close and a trailing volume median, both known before the entry.
4. **Drift.** A positive number after a crash may just be "stocks go up". Every
   arm is reported against an unconditional baseline over the same horizon,
   same universe, same days.

⚠ NOT controlled here: delisting returns beyond what the archive carries, halted
/ late-opening names (a missing next open drops the event rather than filling it
adversely -- optimistic), and the long-short residual construction of the source
paper. This is long-only, which the literature says is the weaker leg.
"""

from __future__ import annotations

import sys
from collections import defaultdict

import numpy as np
import psycopg

from app.config import settings

START = "2020-01-01"
DROP_THRESHOLDS = (-0.05, -0.08, -0.12)  # one-day adjusted return
HOLDS = (1, 2, 3, 5, 10)  # trading days, exit at the close
MIN_PRICE = 20.0  # prior close, unadjusted -- the tradable price
MIN_DOLLAR_VOL = 10_000_000.0  # 20-day median
COST_BPS = 50.0  # round trip, deducted from every figure
BREADTH_SIGNAL = -0.05  # ⚠ must be one of DROP_THRESHOLDS; asserted in _breadth_report

_SERIES = """
    SELECT DISTINCT series_id
    FROM research_price_daily
    WHERE bar_date >= %(start)s
    ORDER BY series_id
"""
_BARS = """
    SELECT bar_date, open, close, adj_close, volume
    FROM research_price_daily
    WHERE series_id = %(sid)s AND bar_date >= %(start)s
      AND open > 0 AND close > 0 AND adj_close > 0
    ORDER BY bar_date
"""


def _cluster_t(groups: dict[object, list[float]], min_n: int = 1) -> tuple[float, float, int]:
    """Mean and t computed ACROSS cluster means, not across observations.

    The whole point: a cluster (a calendar day, or a year) contributes one
    number no matter how many events it contains, so a single crash day cannot
    manufacture significance out of correlated bets.
    """
    means = [float(np.mean(v)) for v in groups.values() if len(v) >= min_n]
    if len(means) < 5:
        return float("nan"), float("nan"), len(means)
    a = np.asarray(means)
    m = float(a.mean())
    se = float(a.std(ddof=1) / np.sqrt(len(a)))
    return m, (m / se if se > 0 else 0.0), len(a)


def _breadth_report(by_day: dict[tuple[float | None, int], dict[object, list[float]]]) -> None:
    """Split the drop signal by how MANY names dropped that day.

    ⚠⚠ Why this exists: on the first run, day-clustered and year-clustered t
    disagreed in SIGN (`1d <= -5%`, 10-day hold: t(day) -4.49 vs t(year) +1.81).
    Both can only be true if outcome covaries with how many events a day carries
    -- day-clustering weights every day equally, year-clustering pools events and
    so is dominated by the days where hundreds of names fired at once.

    That is a testable claim about mechanism rather than a statistical nuisance:
    a market-wide crash is forced/correlated selling that can revert, whereas a
    lone name dropping while the market is flat is firm-specific news that
    reprices and stays repriced. `breadth` -- the count of qualifying events on
    the entry day -- is the cheapest available proxy for which one happened, and
    unlike a sector or beta control it needs no extra data.

    ⚠ Breadth is known at the close of day t, before the next open we enter at,
    so conditioning on it is not lookahead.
    """
    # ⚠ `by_day` is a defaultdict, so an absent key yields an EMPTY bucket rather
    # than raising -- a future edit to DROP_THRESHOLDS would silently delete this
    # whole report and print nothing but headers. Fail loudly instead.
    if BREADTH_SIGNAL not in DROP_THRESHOLDS:
        raise ValueError(f"BREADTH_SIGNAL {BREADTH_SIGNAL} is not in DROP_THRESHOLDS {DROP_THRESHOLDS}")
    print(f"\nBREADTH SPLIT -- does the crowd matter? (signal `1d <= {BREADTH_SIGNAL:.0%}`)")
    print("breadth = number of qualifying events sharing that entry day.")
    print("⚠ each row is a mean ACROSS DAYS in the bucket, so days weigh equally.\n")
    header = f"{'breadth':>14}{'hold':>6}{'days':>7}{'events':>10}{'gross bps':>11}{'NET':>9}{'t':>8}"
    print(header)
    print("-" * len(header))
    buckets = ((1, 1), (2, 5), (6, 20), (21, 100), (101, 10**9))
    for lo, hi in buckets:
        label = f"{lo}" if lo == hi else (f"{lo}-{hi}" if hi < 10**9 else f"{lo}+")
        for k in HOLDS:
            day_map = by_day[(BREADTH_SIGNAL, k)]
            sel = {d: v for d, v in day_map.items() if lo <= len(v) <= hi}
            if len(sel) < 5:
                continue
            m, t, n_days = _cluster_t(sel)
            print(
                f"{label:>14}{k:>6}{n_days:>7}{sum(len(v) for v in sel.values()):>10,}"
                f"{m:>11.2f}{m - COST_BPS:>9.2f}{t:>8.2f}"
            )
        print()


def main() -> int:
    # threshold -> hold -> day -> [returns bps]; None threshold = unconditional
    by_day: dict[tuple[float | None, int], dict[object, list[float]]] = defaultdict(lambda: defaultdict(list))
    by_year: dict[tuple[float | None, int], dict[object, list[float]]] = defaultdict(lambda: defaultdict(list))
    events: dict[tuple[float | None, int], int] = defaultdict(int)

    with psycopg.connect(settings.database_url) as conn:
        sids = [r[0] for r in conn.execute(_SERIES, {"start": START}).fetchall()]
        print(f"{len(sids):,} series with bars since {START}", flush=True)

        for n, sid in enumerate(sids, start=1):
            rows = conn.execute(_BARS, {"sid": sid, "start": START}).fetchall()
            if len(rows) < 60:
                continue
            dates = [r[0] for r in rows]
            op = np.asarray([float(r[1]) for r in rows])
            cl = np.asarray([float(r[2]) for r in rows])
            adj = np.asarray([float(r[3]) for r in rows])
            vol = np.asarray([float(r[4] or 0.0) for r in rows])

            # ⚠ same-bar factor turns the raw open into an adjusted open, so entry
            # and exit live on one continuous corporate-action-free series.
            factor = adj / cl
            adj_open = op * factor

            ret1 = np.empty_like(adj)
            ret1[0] = np.nan
            ret1[1:] = adj[1:] / adj[:-1] - 1.0

            dollar_vol = cl * vol
            for i in range(21, len(cl) - max(HOLDS) - 1):
                if cl[i] < MIN_PRICE:
                    continue
                if float(np.median(dollar_vol[i - 20 : i])) < MIN_DOLLAR_VOL:
                    continue
                entry = adj_open[i + 1]
                if not np.isfinite(entry) or entry <= 0:
                    continue
                day, year = dates[i + 1], dates[i + 1].year
                for k in HOLDS:
                    r = (adj[i + k] / entry - 1.0) * 1e4
                    by_day[(None, k)][day].append(r)
                    by_year[(None, k)][year].append(r)
                    events[(None, k)] += 1
                for th in DROP_THRESHOLDS:
                    if ret1[i] <= th:
                        for k in HOLDS:
                            r = (adj[i + k] / entry - 1.0) * 1e4
                            by_day[(th, k)][day].append(r)
                            by_year[(th, k)][year].append(r)
                            events[(th, k)] += 1
            if n % 500 == 0:
                print(f"  {n}/{len(sids)} series", flush=True)

    print(f"\nEXTREME ONE-DAY LOSER REVERSAL, {START} onward")
    print("entry = NEXT bar's adjusted OPEN. exit = adjusted CLOSE k bars later.")
    print(f"universe: prior close >= ${MIN_PRICE:.0f}, 20d median dollar volume >= ${MIN_DOLLAR_VOL / 1e6:.0f}m")
    print(f"⚠ primary t is DAY-CLUSTERED. ⚠ NET deducts {COST_BPS:.0f} bps round trip.\n")
    header = f"{'signal':>14}{'hold':>6}{'events':>10}{'gross bps':>11}{'NET':>9}{'t(day)':>9}{'days':>7}{'t(year)':>9}"
    print(header)
    print("-" * len(header))

    _breadth_report(by_day)

    survivors = 0
    for th in (None, *DROP_THRESHOLDS):
        label = "unconditional" if th is None else f"1d <= {th:.0%}"
        for k in HOLDS:
            key = (th, k)
            m_day, t_day, n_days = _cluster_t(by_day[key])
            _, t_year, _ = _cluster_t(by_year[key])
            if not np.isfinite(m_day):
                continue
            net = m_day - COST_BPS
            print(f"{label:>14}{k:>6}{events[key]:>10,}{m_day:>11.2f}{net:>9.2f}{t_day:>9.2f}{n_days:>7}{t_year:>9.2f}")
            if th is not None and net > 0 and t_day >= 2.0:
                survivors += 1
        print()

    print("⚠ Compare every conditional row against the 'unconditional' block at the")
    print("  same hold. A conditional edge that merely matches it is market drift.")
    print(f"\n{survivors} conditional arms clear BOTH net > 0 and day-clustered t >= 2.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
