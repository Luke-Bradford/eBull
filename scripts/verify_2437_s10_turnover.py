"""S-10 turnover measurement — the gate that runs BEFORE any S-10 code exists.

Parent spec: ``docs/proposals/ta/2026-08-14-strategy-set-s5-s10.md`` §S-10:

    ⚠ Turnover check FIRST (Novy-Marx/Velikov: >50%/month rarely survives
    costs). Monthly rebalance on deciles is near that bound — if measured
    turnover exceeds it, S-10 is disqualified before any backtest, exactly as
    S-1 was at 56x/yr.

THE RULE BEING MEASURED, SPEC-LITERAL
-------------------------------------
- Setup: regime in {bull_quiet}; universe ranked by 63-bar return.
- Signal: enter the top decile that ALSO closes above its own 50-SMA;
  rebalance monthly.
- Exit: leaves the top three deciles, or closes below 50-SMA.

The exit BAND is load-bearing for this measurement: "enter top decile, hold
while in top three" is a hysteresis rule whose entire purpose is to damp
turnover, so measuring naive top-decile membership would over-reject the
strategy the spec actually wrote.

FROZEN BY CONSTRUCTION (no published formulation exists for any of these —
stated per the source-rule discipline, exactly as S-2's decile cut had to be):

- 63-bar return = ``close(t) / close(t-63) - 1`` on the instrument's OWN bar
  indices (per-series windows, S-2's reading);
- 50-SMA = trailing mean of the last 50 closes including t;
- top decile = first ``N // 10`` of the panel ordered by score descending,
  instrument id ascending (S-2's tie-break, verbatim);
- top three deciles = first ``3 * N // 10`` of the same ordering;
- a held name with NO bar on a rebalance date is CARRIED, not exited — no
  trade can occur on a bar that does not exist, and the first draft of this
  script that read absence as a forced exit measured the calendar, not the
  strategy (see THIN DATES below). Carried counts are reported;
- the rebalance calendar is the union of bar dates carrying at least
  ``THIN_PANEL`` participating instruments, with the first-bar-of-new-month
  rule applied AFTER that cut. ``price_daily`` holds weekend/holiday rows for
  a handful of instruments (~11 on a typical Sunday), and an unfiltered union
  calendar hands the month's one rebalance to such a date — a decile cut on
  11 names when the typical cross-section is 2,000-5,700 is a calendar hole,
  not the strategy, and because the FIRST qualifying bar takes the month, the
  real first trading day then never rebalances at all. (⚠ S-2's production
  ``rebalance_dates`` reads the unfiltered union calendar and rebalances on
  those dates whenever ≥10 members trade — observed panels of 243 on
  2023-12-01. Noted on #2437 rather than fixed here; this script measures
  S-10.) ``THIN_PANEL = 100`` is by construction and the cut is reported;
- entry needs ``close > sma50`` strictly; exit needs ``close < sma50``
  strictly; equality holds an existing position and admits no new one.

TWO ARMS, BECAUSE THE SPEC LEAVES THE EXIT CADENCE OPEN
-------------------------------------------------------
"Rebalance monthly" pins the entry cadence; whether the 50-SMA exit is checked
monthly (at rebalance) or daily is a reading. Deciles only exist when the panel
is ranked, so the decile-band exit is monthly in both arms.

- ``monthly`` arm: both exit legs evaluated at rebalance dates only —
  the turnover floor.
- ``daily-sma`` arm: the 50-SMA exit checked every bar the holding trades,
  decile band still monthly — the turnover ceiling of the two readings.

If the floor already exceeds the bar, S-10 is dead under every reading. If the
floor passes and the ceiling fails, the implementation must pin the monthly
reading and say so.

TURNOVER DEFINITION
-------------------
Equal-weight, Novy-Marx/Velikov-style one-sided monthly turnover:
``(buys + sells) / 2 / mean(holdings_before, holdings_after)`` per rebalance
interval, averaged over intervals where the portfolio is non-empty. The bar is
~50%/month. Raw buy/sell/hold counts are reported alongside so the summary
statistic cannot hide the distribution.

POPULATION AND WINDOW
---------------------
The §4.0 validated universe on ``price_daily`` through ``load_masked_bars`` —
the same loader, masking and regime provider the live scan uses, so the
measurement is of the strategy the scan would actually run. ``price_daily``
reaches back to 2022-05 and the SPY regime is classifiable from 2023-02-27
(the 200-SMA + 126-bar BandWidth warm-up), so entries exist only from the
first bull_quiet rebalance after that. ⚠ survivor_only population, and that
CUTS WITH the gate here: today's survivors under-count forced exits
(delistings), so measured turnover is a floor in that one respect — a FAIL on
this population is final; a marginal pass is re-examined at backtest time on
the research corpus.

Usage
-----
    uv run python scripts/verify_2437_s10_turnover.py --out /tmp/s10_turnover.json
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import date

import numpy as np
import psycopg

from app.config import settings
from app.services.market_regime import Regime
from app.services.market_regime_provider import MarketRegimeProvider
from app.services.price_masked_bars import load_masked_bars
from app.services.strategies.s2_cross_sectional_momentum import rebalance_dates
from app.services.strategies.validated_universe import load_validated_universe

LOOKBACK_BARS = 63
SMA_BARS = 50
ENTRY_DECILE = 10  # top N//10
EXIT_BAND_DECILES = 3  # hold while within first 3*N//10

#: Below this an instrument cannot produce a score anyway (63-bar return needs
#: index 63; the 50-SMA needs 50). Reported, never silently applied.
MIN_BARS = LOOKBACK_BARS + 1

#: See the module docstring — a month boundary the market did not trade.
#: Raised 100 → 1000 at Codex ckpt-1: 100 still admits a corpus-hole date
#: (243 rows on 2023-12-01, a normal Friday most of the corpus is missing),
#: and a retention band cut on a sliver of the book's formation panel is the
#: calendar hole wearing a rank. Every real panel measured 2,084–5,747; 1000
#: refuses every observed junk date and no observed real one.
THIN_PANEL = 1000

#: S-2's §9 Q3 adjusted-price floor, applied to the ENTRY panel only.
MIN_CLOSE = 1.0


@dataclass(frozen=True)
class MemberDay:
    score: float
    close: float
    sma: float


def _member_days(
    dates: tuple[date, ...], closes: list[float | None]
) -> tuple[dict[date, MemberDay], dict[date, tuple[float | None, float | None]]]:
    """Per-date panel contribution + per-date (close, sma) for daily exits.

    A date is rankable only when close(t), close(t-63) and every close in the
    trailing 50 window are present and positive — a masked bar anywhere in the
    windows refuses the date rather than interpolating through it.
    """
    n = len(closes)
    values = np.array([np.nan if c is None or c <= 0.0 else c for c in closes], dtype=np.float64)
    finite = np.isfinite(values)
    # Trailing 50-SMA where the whole window is finite.
    sma = np.full(n, np.nan)
    if n >= SMA_BARS:
        kernel = np.ones(SMA_BARS) / SMA_BARS
        means = np.convolve(np.where(finite, values, 0.0), kernel, mode="valid")
        whole = np.convolve(finite.astype(np.float64), kernel, mode="valid") == 1.0
        sma[SMA_BARS - 1 :] = np.where(whole, means, np.nan)
    rankable: dict[date, MemberDay] = {}
    daily: dict[date, tuple[float | None, float | None]] = {}
    for i in range(n):
        c = values[i] if finite[i] else None
        s = sma[i] if np.isfinite(sma[i]) else None
        daily[dates[i]] = (c, s)
        if c is None or s is None or i < LOOKBACK_BARS:
            continue
        past = values[i - LOOKBACK_BARS]
        if not np.isfinite(past):
            continue
        rankable[dates[i]] = MemberDay(score=float(c / past - 1.0), close=float(c), sma=float(s))
    return rankable, daily


def _rank(panel: dict[int, MemberDay]) -> tuple[frozenset[int], frozenset[int]]:
    """(top decile of the ENTRY panel, top-3-decile band of the EXIT panel).

    Implementation semantics, exactly as the module will ship them (Codex
    ckpt-1 made the divergence explicit, so it is measured rather than argued):

    - ENTRY panel = names with ``close >= MIN_CLOSE`` — S-2's §9 Q3 adjusted-
      price floor; the decile denominator is the floored panel.
    - EXIT panel = every rankable name, floor-free — a held name that fell
      under $1 must still be exitable, and the retention band is cut on the
      panel that contains it.
    """
    ordered = sorted(panel.items(), key=lambda item: (-item[1].score, item[0]))
    entry_ordered = [(key, member) for key, member in ordered if member.close >= MIN_CLOSE]
    top1 = frozenset(key for key, _ in entry_ordered[: len(entry_ordered) // ENTRY_DECILE])
    top3 = frozenset(key for key, _ in ordered[: EXIT_BAND_DECILES * len(ordered) // ENTRY_DECILE])
    return top1, top3


def simulate(
    panels: dict[date, dict[int, MemberDay]],
    daily_by_member: dict[int, dict[date, tuple[float | None, float | None]]],
    calendar: list[date],
    regime_at: dict[date, Regime | None],
    *,
    daily_sma_exit: bool,
) -> dict[str, object]:
    rebalances = sorted(panels)
    holdings: set[int] = set()
    months: list[dict[str, object]] = []
    for idx, when in enumerate(rebalances):
        panel = panels[when]
        before = len(holdings)
        thin = len(panel) < THIN_PANEL
        carried_absent = {h for h in holdings if h not in panel}
        rule_exits: set[int] = set()
        buys: set[int] = set()
        if not thin:
            top1, top3 = _rank(panel)
            rule_exits = {h for h in holdings - carried_absent if h not in top3 or panel[h].close < panel[h].sma}
            holdings -= rule_exits
            if regime_at.get(when) is Regime.BULL_QUIET:
                buys = {m for m in top1 if panel[m].close > panel[m].sma} - holdings
                holdings |= buys
        intramonth_exits: set[int] = set()
        if daily_sma_exit:
            until = rebalances[idx + 1] if idx + 1 < len(rebalances) else None
            span = [d for d in calendar if d > when and (until is None or d < until)]
            for day in span:
                for h in list(holdings):
                    close, sma_value = daily_by_member[h].get(day, (None, None))
                    if close is not None and sma_value is not None and close < sma_value:
                        holdings.discard(h)
                        intramonth_exits.add(h)
        sells = len(rule_exits) + len(intramonth_exits)
        after = len(holdings)
        avg = (before + after) / 2.0
        months.append(
            {
                "date": when.isoformat(),
                "regime": (current.value if (current := regime_at.get(when)) else None),
                "panel": len(panel),
                "thin_skipped": thin,
                "before": before,
                "buys": len(buys),
                "rule_exits": len(rule_exits),
                "carried_absent": len(carried_absent),
                "intramonth_exits": len(intramonth_exits),
                "after": after,
                "turnover": ((len(buys) + sells) / 2.0 / avg) if avg > 0 else None,
            }
        )
    active = [m for m in months if m["turnover"] is not None and not m["thin_skipped"]]
    turnovers = [float(m["turnover"]) for m in active]  # type: ignore[arg-type]
    # The first month a portfolio exists is a bootstrap (everything is a buy,
    # turnover 1.0 by construction) — steady state excludes months that START
    # empty, and is the figure comparable to the published ~50%/month bar.
    steady = [float(m["turnover"]) for m in active if int(m["before"]) > 0]  # type: ignore[arg-type]
    return {
        "months": months,
        "active_months": len(active),
        "mean_monthly_turnover": (sum(turnovers) / len(turnovers)) if turnovers else None,
        "median_monthly_turnover": (sorted(turnovers)[len(turnovers) // 2] if turnovers else None),
        "max_monthly_turnover": max(turnovers) if turnovers else None,
        "steady_months": len(steady),
        "steady_mean_monthly_turnover": (sum(steady) / len(steady)) if steady else None,
        "steady_max_monthly_turnover": max(steady) if steady else None,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, help="write the full JSON here")
    args = parser.parse_args()

    with psycopg.connect(settings.database_url) as conn:
        universe = sorted(load_validated_universe(conn))
        provider = MarketRegimeProvider.load(conn)
        panels: dict[date, dict[int, MemberDay]] = {}
        daily_by_member: dict[int, dict[date, tuple[float | None, float | None]]] = {}
        calendar_set: set[date] = set()
        loaded = skipped_short = 0
        # First pass collects every instrument's dates so the rebalance calendar
        # is the PANEL's union, not any single member's (S-2's reading).
        series_by_member: dict[int, tuple[tuple[date, ...], list[float | None]]] = {}
        for instrument_id in universe:
            series = load_masked_bars(conn, instrument_id).series
            if len(series) < MIN_BARS:
                skipped_short += 1
                continue
            loaded += 1
            series_by_member[instrument_id] = (series.dates, list(series.float_closes))
            calendar_set.update(series.dates)

    participation = Counter(when for dates, _ in series_by_member.values() for when in dates)
    calendar = sorted(when for when in calendar_set if participation[when] >= THIN_PANEL)
    thin_dates_cut = len(calendar_set) - len(calendar)
    rebalance_set = rebalance_dates(calendar)
    for instrument_id, (dates, closes) in series_by_member.items():
        rankable, daily = _member_days(dates, closes)
        daily_by_member[instrument_id] = daily
        for when, member in rankable.items():
            if when in rebalance_set:
                panels.setdefault(when, {})[instrument_id] = member

    ordered_rebalances = tuple(sorted(panels))
    regime_series = provider.for_dates(ordered_rebalances)
    regime_at = dict(zip(ordered_rebalances, regime_series.values, strict=True))

    result = {
        "population": {
            "universe": len(series_by_member) + skipped_short,
            "loaded": loaded,
            "skipped_below_min_bars": skipped_short,
            "min_bars": MIN_BARS,
            "thin_dates_cut_from_calendar": thin_dates_cut,
            "rebalance_dates": len(ordered_rebalances),
            "bull_quiet_rebalances": sum(1 for r in regime_at.values() if r is Regime.BULL_QUIET),
            "regime_distribution": {
                (r.value if r else "unclassifiable"): sum(1 for v in regime_at.values() if v is r)
                for r in set(regime_at.values())
            },
        },
        "monthly": simulate(panels, daily_by_member, calendar, regime_at, daily_sma_exit=False),
        "daily_sma": simulate(panels, daily_by_member, calendar, regime_at, daily_sma_exit=True),
    }
    with open(args.out, "w") as handle:
        json.dump(result, handle, indent=2)

    for arm in ("monthly", "daily_sma"):
        stats = result[arm]
        print(
            f"{arm}: active_months={stats['active_months']} "
            f"mean={stats['mean_monthly_turnover']} median={stats['median_monthly_turnover']} "
            f"max={stats['max_monthly_turnover']} | steady_months={stats['steady_months']} "
            f"steady_mean={stats['steady_mean_monthly_turnover']} "
            f"steady_max={stats['steady_max_monthly_turnover']}"
        )
    print(f"population: {result['population']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
