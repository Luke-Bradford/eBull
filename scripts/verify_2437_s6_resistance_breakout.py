"""Verify S-6 against the live corpus (#2437).

Arms
----
``--census``      the full-population funnel: what every leg rejects, and how
                  often S-6 fires. Nothing here is hardcoded — every figure the
                  PR quotes is printed by this arm.
``--market``      the benchmark's classified regime, and the three-way split
                  the eleventh reason code exists for.
``--equivalence`` the ``LevelScan`` hoist against the scalar ``levels_at`` on
                  real series, plus the timing that motivated it.
``--scan``        run the real signal scan on a bounded slice and report S-6's
                  own census through the manifest and the ledger writer.

⚠ EVERY ARM MEASURES; NONE ASSERTS A THRESHOLD. The full-population rule binds
descriptive claims as well as gates, so the numbers in the PR come from here
rather than from a sample or from memory.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter

import numpy as np
import psycopg

from app.config import settings
from app.services.cost_model import COST_MODEL_ID
from app.services.indicator_series import atr_series
from app.services.market_context import load_market_context
from app.services.price_levels import LevelScan, levels_at
from app.services.price_masked_bars import load_masked_bars
from app.services.strategies.s6_resistance_breakout import (
    PERMITTED_REGIMES,
    S6_STRATEGY_ID,
    VOLUME_MULTIPLE,
    s6_identity,
    s6_signals,
)
from app.services.strategies.validated_universe import load_validated_universe
from app.services.strategy_signal_scan import SCAN_UNIVERSE

#: ⚠ The label the LIVE SCAN uses, imported rather than retyped — a verify
#: script measuring under a different universe string measures a different
#: strategy identity (criterion 11 puts the universe inside the identity).
UNIVERSE = SCAN_UNIVERSE
MASKED_REASON = "quarantined_bar"

#: Bars below which a series cannot support a 3-touch level plus the 200-bar
#: benchmark warm-up it will be gated against. Reported, never silently applied.
MIN_BARS = 250


def _volumes(series: object) -> np.ndarray:
    rows = series.rows  # type: ignore[attr-defined]
    out = np.empty(len(rows), dtype=float)
    for index, row in enumerate(rows):
        value = row.get("volume")
        out[index] = np.nan if value is None or value < 0 else float(value)
    return out


def market_arm(conn: psycopg.Connection) -> None:
    context = load_market_context(conn, universe=UNIVERSE)
    print(f"benchmark {context.benchmark_symbol} (instrument {context.benchmark_instrument_id})")
    print(f"  rule set        {context.rule_set_version}")
    print(f"  classified      {len(context.regime_by_date):,} sessions")
    print(f"  first / last    {context.first_classified} .. {max(context.regime_by_date)}")
    distribution = Counter(regime.value for regime in context.regime_by_date.values())
    for name, count in sorted(distribution.items(), key=lambda kv: -kv[1]):
        share = count / len(context.regime_by_date)
        marker = "  <- permitted" if name in {r.value for r in PERMITTED_REGIMES} else ""
        print(f"    {name:16s} {count:>6,}  {share:6.2%}{marker}")

    universe = load_validated_universe(conn)
    known = warm = gap = 0
    gap_dates: Counter = Counter()
    rows = conn.execute(
        """
        SELECT d.price_date, count(*)
        FROM price_daily d
        JOIN price_quarantine_coverage cov
          ON cov.instrument_id = d.instrument_id
         AND cov.rule_set_version = %(qv)s
         AND d.price_date BETWEEN cov.first_bar AND cov.last_bar
        WHERE d.instrument_id = ANY(%(ids)s)
        GROUP BY 1
        """,
        {"qv": _quarantine_version(), "ids": list(universe)},
    ).fetchall()
    for when, count in rows:
        if when in context.regime_by_date:
            known += count
        elif when < context.first_classified:
            warm += count
        else:
            gap += count
            gap_dates[when] = count
    total = known + warm + gap
    print(f"\nvalidated-universe loadable bars: {total:,}")
    print(f"  regime known                    {known:>10,}  {known / total:6.2%}")
    print(f"  benchmark warm-up               {warm:>10,}  {warm / total:6.2%}  -> insufficient_warmup")
    print(f"  BENCHMARK GAP                   {gap:>10,}  {gap / total:6.2%}  -> missing_market_context")
    print(f"    over {len(gap_dates)} dates; worst: {gap_dates.most_common(5)}")


def _quarantine_version() -> str:
    from app.services.price_quarantine import RULE_SET_VERSION

    return RULE_SET_VERSION


def equivalence_arm(conn: psycopg.Connection) -> None:
    """⚠ The hoist is a performance change; prove it changed no verdict."""
    universe = sorted(load_validated_universe(conn))[:25]
    checked = mismatches = 0
    hoisted_seconds = scalar_seconds = 0.0
    for instrument_id in universe:
        series = load_masked_bars(conn, instrument_id).series
        if len(series) < MIN_BARS:
            continue
        atr = atr_series(series, universe=UNIVERSE, period=14)
        volumes = _volumes(series)
        scan = LevelScan.build(highs=series.array_highs, lows=series.array_lows, volumes=volumes)
        for index in range(len(series)):
            value = atr.values[index]
            if value is None:
                continue
            mark = time.perf_counter()
            fast = scan.at(atr=value, index=index)
            hoisted_seconds += time.perf_counter() - mark
            mark = time.perf_counter()
            slow = levels_at(highs=series.array_highs, lows=series.array_lows, volumes=volumes, atr=value, index=index)
            scalar_seconds += time.perf_counter() - mark
            checked += 1
            if fast != slow:
                mismatches += 1
    print(f"levels: {checked:,} (instrument, bar) pairs compared, {mismatches} mismatches")
    speedup = scalar_seconds / max(hoisted_seconds, 1e-9)
    print(f"  hoisted {hoisted_seconds:8.2f}s   scalar {scalar_seconds:8.2f}s   speedup x{speedup:.1f}")
    if mismatches:
        sys.exit(1)


def census_arm(conn: psycopg.Connection) -> None:
    """The full-population funnel, straight through ``s6_signals``."""
    universe = sorted(load_validated_universe(conn))
    context = load_market_context(conn, universe=UNIVERSE)
    identity = s6_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID)
    print(f"strategy {S6_STRATEGY_ID} version {identity.version}")
    print(f"  volume multiple {VOLUME_MULTIPLE}  permitted {sorted(r.value for r in PERMITTED_REGIMES)}")

    verdicts: Counter = Counter()
    reasons: Counter = Counter()
    firing_instruments: set[int] = set()
    scanned = short = 0
    started = time.perf_counter()
    for position, instrument_id in enumerate(universe, start=1):
        if position % 500 == 0:
            print(f"  ... {position}/{len(universe)} {time.perf_counter() - started:.0f}s", flush=True)
        series = load_masked_bars(conn, instrument_id).series
        if len(series) < MIN_BARS:
            short += 1
            continue
        scanned += 1
        for signal in s6_signals(series, universe=UNIVERSE, masked_reason=MASKED_REASON, market=context):
            verdicts[signal.verdict] += 1
            if signal.reason is not None:
                reasons[signal.reason] += 1
            if signal.verdict == "fired":
                firing_instruments.add(instrument_id)

    total = sum(verdicts.values())
    decided = verdicts["fired"] + verdicts["not_fired"]
    print(f"\nelapsed {time.perf_counter() - started:.0f}s")
    print(f"  instruments scanned            {scanned:>10,}   (short, <{MIN_BARS} bars: {short:,})")
    print(f"  bar verdicts                   {total:>10,}")
    for verdict, count in sorted(verdicts.items()):
        print(f"    {verdict:22s} {count:>10,}  {count / total:6.2%}")
    print("  not_evaluable reasons:")
    for reason, count in sorted(reasons.items(), key=lambda kv: -kv[1]):
        print(f"    {reason:22s} {count:>10,}  {count / total:6.2%}")
    if decided:
        print(f"\n  fire rate per DECIDED bar      {verdicts['fired'] / decided:8.4%}")
    if scanned:
        print(
            f"  instruments that ever fire     {len(firing_instruments) / scanned:8.2%}  ({len(firing_instruments):,})"
        )


def scan_arm(conn: psycopg.Connection) -> None:
    """S-6 through the real runner, on the real manifest, writing nothing."""
    from app.services.strategy_manifest import STRATEGY_MANIFEST
    from app.services.strategy_segmented_evaluation import segmented_signals

    entry = STRATEGY_MANIFEST[S6_STRATEGY_ID]
    context = load_market_context(conn, universe=UNIVERSE)
    universe = sorted(load_validated_universe(conn))[:200]
    census: Counter = Counter()
    fired_examples: list[tuple[int, str]] = []
    for instrument_id in universe:
        series = load_masked_bars(conn, instrument_id).series
        if len(series) < MIN_BARS:
            continue
        signals = segmented_signals(
            entry,
            series,
            universe=UNIVERSE,
            masked_reason=MASKED_REASON,
            unresolved_breaks=(),
            market=context,
        )
        for signal in signals:
            census[(signal.verdict, signal.reason or "")] += 1
            if signal.verdict == "fired" and len(fired_examples) < 10:
                fired_examples.append((instrument_id, str(series.dates[signal.signal_index])))
    print(f"segmented_signals over {len(universe)} instruments:")
    for key, count in sorted(census.items(), key=lambda kv: -kv[1]):
        print(f"    {key!s:44s} {count:>10,}")
    print(f"  sample fires (instrument_id, signal date): {fired_examples}")

    print("\n  refusal without a context (must raise):")
    try:
        segmented_signals(
            entry,
            load_masked_bars(conn, universe[0]).series,
            universe=UNIVERSE,
            masked_reason=MASKED_REASON,
            unresolved_breaks=(),
            market=None,
        )
    except ValueError as exc:
        print(f"    REFUSED: {exc}")
    else:
        print("    NOT REFUSED — the fail-closed guard is not wired")
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--census", action="store_true")
    parser.add_argument("--market", action="store_true")
    parser.add_argument("--equivalence", action="store_true")
    parser.add_argument("--scan", action="store_true")
    args = parser.parse_args()
    if not any((args.census, args.market, args.equivalence, args.scan)):
        parser.error("choose at least one arm")

    with psycopg.connect(settings.database_url) as conn:
        if args.market:
            market_arm(conn)
        if args.equivalence:
            equivalence_arm(conn)
        if args.scan:
            scan_arm(conn)
        if args.census:
            census_arm(conn)


if __name__ == "__main__":
    main()
