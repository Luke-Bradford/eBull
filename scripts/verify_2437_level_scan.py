"""Verify the ``LevelScan`` hoist against the live corpus (#2437).

Arms
----
``--equivalence`` ``LevelScan.at`` against a TRANSCRIBED reference on real
                  series, including quarantine-masked ones and the
                  ``volumes=None`` arm. A single mismatch exits non-zero.
                  ⚠ It used to compare against ``levels_at``, which calls
                  ``at`` — one code path, so that arm could not fail. See
                  ``_reference_at``.
``--census``      the cost this exists to remove: levels at EVERY bar of every
                  scanned instrument in the validated universe, which is the
                  shape S-5 and S-6 actually run in.

⚠ THE TWO ARMS MEASURE DIFFERENT THINGS AND THEIR SPEEDUPS ARE NOT COMPARABLE.
``--equivalence`` times ``at`` against ``_reference_at``, which shares the
vectorised pivot detection and differs only in materialising every cluster
before filtering it — so it isolates #2780's filter hoist and nothing else. The
figure in ``price_levels.LevelScan``'s docstring is against the per-index Python
loop #2437 replaced. Both subjects are named so a reader cannot quote one figure
under the other's meaning.
"""

from __future__ import annotations

import argparse
import sys
import time

import numpy as np
import psycopg

from app.config import settings
from app.services.indicator_series import BarSeries, atr_series
from app.services.price_levels import (
    CLUSTER_ATR_TOLERANCE,
    MAX_TOUCH_AGE_BARS,
    MIN_TOUCHES,
    LevelScan,
    PriceLevel,
    _cluster,
)
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


def _reference_at(scan: LevelScan, *, atr: float, index: int) -> tuple[PriceLevel, ...]:
    """The pre-#2780-filter-hoist ``LevelScan.at``, transcribed.

    ⚠⚠ THIS EXISTS BECAUSE THE OLD EQUIVALENCE ARM WENT TAUTOLOGICAL. It compared
    ``scan.at`` with ``levels_at``, and ``levels_at`` builds a scan and calls
    ``at`` — one code path, so once ``at`` changed, both sides changed together
    and the arm would have reported a clean sweep over any rewrite at all.

    It is built on ``_cluster``, which ``at`` no longer calls and which
    ``tests/test_price_level_scan.py`` pins bit-for-bit against a hand-written
    transcription over 1,500 randomised cases. So the chain is: hand-written
    arithmetic → ``_cluster`` → this → the full-population sweep below.
    """
    if index < 0 or index >= scan.highs.size:
        return ()
    if not np.isfinite(atr) or atr <= 0:
        return ()
    tolerance = CLUSTER_ATR_TOLERANCE * atr
    last_confirmed = index - scan.pivots.half_window
    hi_idx = [i for i in scan.pivots.high_indices if i <= last_confirmed]
    lo_idx = [i for i in scan.pivots.low_indices if i <= last_confirmed]

    total = 0.0 if scan.volume_cumsum is None else float(scan.volume_cumsum[index])
    out: list[PriceLevel] = []
    for kind, idxs, prices in (("resistance", hi_idx, scan.highs), ("support", lo_idx, scan.lows)):
        for price, cluster in _cluster(idxs, prices, scan.volumes, tolerance=tolerance):
            touches = len(cluster)
            last_touch = max(cluster)
            if touches < MIN_TOUCHES:
                continue
            if index - last_touch > MAX_TOUCH_AGE_BARS:
                continue
            if scan.volumes is None:
                share = 1.0
            else:
                share = float(np.nansum([scan.volumes[i] for i in cluster])) / total if total > 0 else 0.0
            strength = touches * float(np.log1p(share))
            out.append(
                PriceLevel(
                    price=price,
                    kind=kind,  # type: ignore[arg-type]
                    touches=touches,
                    last_touch_index=last_touch,
                    strength=strength,
                )
            )
    return tuple(sorted(out, key=lambda level: level.strength, reverse=True))


def equivalence_arm(conn: psycopg.Connection, *, limit: int = 25) -> None:
    """⚠ Prove the filter hoist changed no verdict, on real data including masked bars.

    Compared with ``==`` on the whole ``PriceLevel`` tuple, never ``approx``: a
    level price feeds a threshold comparison, so a last-bit change flips which
    trades exist. Counts what the two filters REJECTED as well as what survived —
    a sweep in which nothing was ever filtered would pass while testing nothing,
    which is the #2780 "fixture that refuses everything" trap in reverse.
    """
    universe = sorted(load_validated_universe(conn))[:limit]
    masked_rows = conn.execute(
        "SELECT instrument_id FROM price_bar_quarantine GROUP BY 1 ORDER BY count(*) DESC LIMIT 5"
    ).fetchall()
    targets = universe + [int(row[0]) for row in masked_rows]

    checked = mismatches = with_volume = without_volume = 0
    levels_returned = thin_rejected = stale_rejected = 0
    hoisted_seconds = reference_seconds = 0.0
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
                slow = _reference_at(scan, atr=value, index=index)
                reference_seconds += time.perf_counter() - mark
                checked += 1
                levels_returned += len(fast)
                if arm == "volumes":
                    with_volume += 1
                else:
                    without_volume += 1
                if fast != slow:
                    mismatches += 1
                    if mismatches == 1:
                        print(f"  FIRST MISMATCH instrument {instrument_id} arm {arm} index {index}")
                # What the hoisted filters actually rejected, from the reference side.
                for _, cluster in _cluster(
                    [i for i in scan.pivots.high_indices if i <= index - scan.pivots.half_window],
                    scan.highs,
                    scan.volumes,
                    tolerance=CLUSTER_ATR_TOLERANCE * value,
                ):
                    if len(cluster) < MIN_TOUCHES:
                        thin_rejected += 1
                    elif index - max(cluster) > MAX_TOUCH_AGE_BARS:
                        stale_rejected += 1

    print(f"levels: {checked:,} (instrument, bar, arm) comparisons, {mismatches} mismatches")
    print(f"  with volume {with_volume:,}   volumes=None {without_volume:,}")
    print(f"  levels returned {levels_returned:,}")
    print(f"  resistance clusters rejected: {thin_rejected:,} too thin, {stale_rejected:,} stale")
    speedup = reference_seconds / max(hoisted_seconds, 1e-9)
    print(f"  hoisted {hoisted_seconds:8.2f}s   reference {reference_seconds:8.2f}s   speedup x{speedup:.1f}")
    print("  ⚠ that speedup is vs the materialising `at`, not vs the per-index loop #2437 replaced")
    if mismatches:
        sys.exit(1)
    if not levels_returned:
        sys.exit("no level survived anywhere — equality against the reference proves nothing")
    if not (thin_rejected and stale_rejected):
        sys.exit("a filter never rejected anything — the hoist it guards is unverified")


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
    parser.add_argument("--limit", type=int, default=25, help="validated-universe instruments to sweep")
    args = parser.parse_args()
    if not (args.equivalence or args.census):
        parser.error("choose at least one arm")
    with psycopg.connect(settings.database_url) as conn:
        if args.equivalence:
            equivalence_arm(conn, limit=args.limit)
        if args.census:
            census_arm(conn)


if __name__ == "__main__":
    main()
