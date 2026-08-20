"""Verify the ``LevelScan`` hoist against the live corpus (#2437).

Arms
----
``--equivalence`` ``LevelScan.at`` against the scalar ``levels_at`` on real
                  series, including quarantine-masked ones and the
                  ``volumes=None`` arm. A single mismatch exits non-zero.
``--census``      the cost this exists to remove: levels at EVERY bar of every
                  scanned instrument in the validated universe, which is the
                  shape S-5 and S-6 actually run in.

⚠ THE TWO ARMS MEASURE DIFFERENT THINGS AND THEIR SPEEDUPS ARE NOT COMPARABLE.
``--equivalence`` times the hoisted form against the CURRENT ``levels_at``,
which builds a scan per call and therefore already benefits from the vectorised
detection — it reports ~2x. The 24x in the module docstring is against the
per-index Python loop this replaced. Both subjects are named so a reader cannot
quote one figure under the other's meaning.
"""

from __future__ import annotations

import argparse
import sys
import time

import numpy as np
import psycopg

from app.config import settings
from app.services.indicator_series import BarSeries, atr_series
from app.services.price_levels import LevelScan, levels_at
from app.services.price_masked_bars import load_masked_bars
from app.services.strategies.validated_universe import load_validated_universe
from app.services.strategy_signal_scan import SCAN_UNIVERSE

#: Bars below which S-5/S-6 cannot produce a decided verdict anyway. Reported,
#: never silently applied.
MIN_BARS = 250


def _volumes(series: BarSeries) -> np.ndarray:
    out = np.empty(len(series), dtype=float)
    for index, row in enumerate(series.rows):
        value = row.get("volume")
        out[index] = np.nan if value is None or value < 0 else float(value)
    return out


def equivalence_arm(conn: psycopg.Connection) -> None:
    """⚠ Prove the hoist changed no verdict, on real data including masked bars."""
    universe = sorted(load_validated_universe(conn))[:25]
    masked_rows = conn.execute(
        "SELECT instrument_id FROM price_bar_quarantine GROUP BY 1 ORDER BY count(*) DESC LIMIT 5"
    ).fetchall()
    targets = universe + [int(row[0]) for row in masked_rows]

    checked = mismatches = with_volume = without_volume = 0
    hoisted_seconds = scalar_seconds = 0.0
    for instrument_id in targets:
        series = load_masked_bars(conn, instrument_id).series
        if len(series) < MIN_BARS:
            continue
        atr = atr_series(series, universe=SCAN_UNIVERSE, period=14)
        volumes = _volumes(series)
        for arm, vector in (("volumes", volumes), ("no-volumes", None)):
            scan = LevelScan.build(highs=series.array_highs, lows=series.array_lows, volumes=vector)
            for index in range(len(series)):
                value = atr.values[index]
                if value is None:
                    continue
                mark = time.perf_counter()
                fast = scan.at(atr=value, index=index)
                hoisted_seconds += time.perf_counter() - mark
                mark = time.perf_counter()
                slow = levels_at(
                    highs=series.array_highs,
                    lows=series.array_lows,
                    volumes=vector,
                    atr=value,
                    index=index,
                )
                scalar_seconds += time.perf_counter() - mark
                checked += 1
                if arm == "volumes":
                    with_volume += 1
                else:
                    without_volume += 1
                if fast != slow:
                    mismatches += 1
                    if mismatches == 1:
                        print(f"  FIRST MISMATCH instrument {instrument_id} arm {arm} index {index}")

    print(f"levels: {checked:,} (instrument, bar, arm) comparisons, {mismatches} mismatches")
    print(f"  with volume {with_volume:,}   volumes=None {without_volume:,}")
    speedup = scalar_seconds / max(hoisted_seconds, 1e-9)
    print(f"  hoisted {hoisted_seconds:8.2f}s   scalar {scalar_seconds:8.2f}s   speedup x{speedup:.1f}")
    print("  ⚠ that speedup is vs the CURRENT levels_at, not vs the per-index loop this replaced")
    if mismatches:
        sys.exit(1)


def census_arm(conn: psycopg.Connection) -> None:
    """Levels at EVERY bar of every scanned instrument — S-5/S-6's real shape."""
    universe = sorted(load_validated_universe(conn))
    scanned = short = 0
    bars = levels_seen = 0
    started = time.perf_counter()
    for position, instrument_id in enumerate(universe, start=1):
        if position % 1000 == 0:
            print(f"  ... {position}/{len(universe)} {time.perf_counter() - started:.0f}s", flush=True)
        series = load_masked_bars(conn, instrument_id).series
        if len(series) < MIN_BARS:
            short += 1
            continue
        scanned += 1
        atr = atr_series(series, universe=SCAN_UNIVERSE, period=14)
        scan = LevelScan.build(highs=series.array_highs, lows=series.array_lows, volumes=_volumes(series))
        for index in range(len(series)):
            value = atr.values[index]
            if value is None:
                continue
            bars += 1
            levels_seen += len(scan.at(atr=value, index=index))
    elapsed = time.perf_counter() - started
    print(f"\nelapsed {elapsed:.0f}s")
    print(f"  instruments scanned  {scanned:>10,}   (short, <{MIN_BARS} bars: {short:,})")
    print(f"  bars levelled        {bars:>10,}")
    print(f"  levels produced      {levels_seen:>10,}   ({levels_seen / max(bars, 1):.2f} per bar)")
    print(f"  cost per bar         {elapsed / max(bars, 1) * 1000:>10.4f} ms")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--equivalence", action="store_true")
    parser.add_argument("--census", action="store_true")
    args = parser.parse_args()
    if not (args.equivalence or args.census):
        parser.error("choose at least one arm")
    with psycopg.connect(settings.database_url) as conn:
        if args.equivalence:
            equivalence_arm(conn)
        if args.census:
            census_arm(conn)


if __name__ == "__main__":
    main()
