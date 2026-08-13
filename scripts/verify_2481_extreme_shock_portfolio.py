"""Portfolio/tail stress for frozen candidate C-2 (#2481).

Run from the repository root:

    PYTHONPATH=. uv run python scripts/verify_2481_extreme_shock_portfolio.py

This is NOT a new alpha test.  The >=12% drop, next-open short, five-bar hold,
20% gap-aware stop and eligibility floor are copied unchanged from the searched
development result.  The script asks whether that fixed event stream can coexist
inside one unleveraged portfolio under deterministic capital constraints.

Nothing is written.  Contemporary sector labels are incomplete and are not
point-in-time history, so the output reports their coverage and cannot prove the
historical sector-concentration gate.  Unknown sectors share one conservative
bucket in the sector-capped arm.
"""

from __future__ import annotations

import sys
from collections import Counter
from datetime import date

import numpy as np
import psycopg

from app.config import settings
from app.services.extreme_shock_portfolio import (
    PortfolioStressConfig,
    ShockTradePath,
    simulate_extreme_shock_portfolio,
)
from app.services.trial_register import TRIAL_REGISTER, TRIAL_REGISTER_VERSION

START = "2020-01-01"
DROP = -0.12
HOLD = 5
STOP = 0.20
MIN_PRICE = 20.0
MIN_DOLLAR_VOL = 10_000_000.0
ROUND_TRIP_COST = 0.005
# Existing adverse sensitivity: 8.2 bps x seven financed-day equivalents.
CARRY_COST = 0.00082 * 7
PER_NAME_CAPS = (0.005, 0.01, 0.02, 0.05)
SECTOR_CAP = 0.25

_SERIES = """
    SELECT s.series_id, i.sector
    FROM research_price_series s
    LEFT JOIN instruments i ON i.instrument_id = s.instrument_id
    WHERE s.comparator_snapshot_id IS NULL
      AND EXISTS (
          SELECT 1 FROM research_price_daily d
          WHERE d.series_id = s.series_id AND d.bar_date >= %(start)s
      )
    ORDER BY s.series_id
"""

_BARS = """
    SELECT bar_date, open, high, close, adj_close, volume
    FROM research_price_daily
    WHERE series_id = %(series_id)s AND bar_date >= %(start)s
      AND open > 0 AND high > 0 AND close > 0 AND adj_close > 0
    ORDER BY bar_date
"""


def _extract_paths(conn: psycopg.Connection[tuple]) -> tuple[list[ShockTradePath], int, int]:
    series = conn.execute(_SERIES, {"start": START}).fetchall()
    paths: list[ShockTradePath] = []
    classified_series = sum(row[1] is not None for row in series)

    for number, (series_id, sector) in enumerate(series, start=1):
        bars = conn.execute(_BARS, {"series_id": series_id, "start": START}).fetchall()
        if len(bars) < 60:
            continue
        dates = [row[0] for row in bars]
        opens = np.asarray([float(row[1]) for row in bars])
        highs = np.asarray([float(row[2]) for row in bars])
        closes = np.asarray([float(row[3]) for row in bars])
        adjusted = np.asarray([float(row[4]) for row in bars])
        volumes = np.asarray([float(row[5] or 0.0) for row in bars])
        factor = adjusted / closes
        adjusted_open = opens * factor
        adjusted_high = highs * factor
        returns = np.empty_like(adjusted)
        returns[0] = np.nan
        returns[1:] = adjusted[1:] / adjusted[:-1] - 1.0
        dollar_volume = closes * volumes

        for signal_index in range(21, len(closes) - HOLD - 1):
            if returns[signal_index] > DROP or closes[signal_index] < MIN_PRICE:
                continue
            if float(np.median(dollar_volume[signal_index - 20 : signal_index])) < MIN_DOLLAR_VOL:
                continue
            entry_index = signal_index + 1
            entry = adjusted_open[entry_index]
            if not np.isfinite(entry) or entry <= 0:
                continue
            stop_level = entry * (1.0 + STOP)
            path: list[tuple[date, float]] = []
            for index in range(entry_index, signal_index + HOLD + 1):
                exit_price = adjusted[index]
                terminal = index == signal_index + HOLD
                if adjusted_open[index] >= stop_level:
                    exit_price = adjusted_open[index]
                    terminal = True
                elif adjusted_high[index] >= stop_level:
                    exit_price = stop_level
                    terminal = True
                path.append((dates[index], -(exit_price / entry - 1.0)))
                if terminal:
                    break
            paths.append(
                ShockTradePath(
                    trade_id=f"{series_id}:{dates[entry_index].isoformat()}",
                    series_id=int(series_id),
                    entry_date=dates[entry_index],
                    exit_date=path[-1][0],
                    sector=str(sector) if sector is not None else None,
                    cumulative_returns=tuple(path),
                )
            )
        if number % 1000 == 0:
            print(f"  extracted {number:,}/{len(series):,} series", flush=True)
    return paths, len(series), classified_series


def _fmt_pct(value: float | None) -> str:
    return "—" if value is None else f"{value * 100:+.2f}%"


def main() -> int:
    with psycopg.connect(settings.database_url) as conn:
        paths, series_count, classified_series = _extract_paths(conn)
    if not paths:
        print("REFUSED: the frozen rule produced no paths", file=sys.stderr)
        return 2

    entry_counts = Counter(path.entry_date for path in paths)
    signal_sector_known = sum(path.sector is not None for path in paths)
    search_floor = sum(
        item.searches
        for item in TRIAL_REGISTER.trials
        if item.trial_id in {"short-horizon-search-session-2026-08-09", "extreme-shock-portfolio-sizing-stress-v1"}
    )

    print("\nC-2 FROZEN EXTREME-SHOCK PORTFOLIO STRESS")
    print(f"rule: drop <= {DROP:.0%}; next-open short; {HOLD}-bar timeout; {STOP:.0%} gap-aware stop; no leverage")
    print(f"cost sensitivity: {ROUND_TRIP_COST * 1e4:.0f} bps round trip + {CARRY_COST * 1e4:.1f} bps adverse carry")
    print(
        f"trial register: {TRIAL_REGISTER_VERSION}; C-2 searched/evaluated-family floor={search_floor}; "
        "this interval is NOT a holdout"
    )
    print(
        f"source: {series_count:,} non-comparator series; current sector coverage "
        f"{classified_series:,}/{series_count:,} ({classified_series / series_count:.1%})"
    )
    print(
        f"signals: {len(paths):,}; {len(entry_counts):,} entry days; max same-day batch "
        f"{max(entry_counts.values()):,}; signal sector coverage {signal_sector_known / len(paths):.1%}"
    )
    print("⚠ sector labels are current/incomplete, not point-in-time; they constrain stress only.")

    header = (
        f"{'name cap':>9}{'sector':>9}{'funded':>10}{'return':>11}{'ann.':>10}"
        f"{'wt trade':>10}{'max DD':>10}{'worst day':>12}{'ES5':>10}{'max n':>8}{'gross':>9}"
        f"{'sector':>9}{'unknown':>10}{'DD + wipe':>11}"
    )
    print("\n" + header)
    print("-" * len(header))
    primary_result = None
    for cap in PER_NAME_CAPS:
        for sector_cap in (SECTOR_CAP, None):
            result = simulate_extreme_shock_portfolio(
                paths,
                PortfolioStressConfig(
                    per_name_cap=cap,
                    sector_cap=sector_cap,
                    round_trip_cost=ROUND_TRIP_COST,
                    carry_cost=CARRY_COST,
                ),
            )
            if cap == 0.01 and sector_cap == SECTOR_CAP:
                primary_result = result
            label = "25%" if sector_cap is not None else "none"
            print(
                f"{cap:>8.1%}{label:>9}{result.funded_trades:>8,}/{len(paths):<1}"
                f"{_fmt_pct(result.ending_return):>11}{_fmt_pct(result.annualized_return):>10}"
                f"{_fmt_pct(result.capital_weighted_trade_return):>10}"
                f"{_fmt_pct(result.max_drawdown):>10}{_fmt_pct(result.worst_day):>12}"
                f"{_fmt_pct(result.expected_shortfall_5):>10}{result.max_concurrent:>8}"
                f"{result.max_gross_exposure:>8.1%}{_fmt_pct(result.max_sector_exposure):>9}"
                f"{result.unknown_sector_funded_pct:>9.1%}{_fmt_pct(result.one_name_loss_stressed_max_drawdown):>11}"
            )

    assert primary_result is not None
    annual = "  ".join(f"{year} {_fmt_pct(value)}" for year, value in primary_result.annual_returns)
    print(f"\n1% name / 25% sector diagnostic calendar returns: {annual}")

    print("\nINTERPRETATION CONTRACT")
    print("- The cap rows are declared sizing sensitivities, not alternatives from which to select a winner.")
    print("- 'DD + wipe' adds a simultaneous -100% loss to one largest position on peak-exposure day.")
    print("- Caps apply when an entry is funded; peak gross/sector ratios can drift above them after losses.")
    print("- Positive historical return cannot promote C-2: 109 family evaluations, no clean holdout, incomplete PIT")
    print("  identity, and unavailable/stale broker short cost/availability remain binding blockers.")
    print("- Negative or structurally ruinous rows may reject C-2 without prospective trading.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
