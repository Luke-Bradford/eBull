"""Full-population A/B for the ``missing_market_context`` reason code (#2437).

WHY AN A/B AND NOT A COUNT
--------------------------
The change moves a verdict: a bar on a date the benchmark did not trade was
``not_fired`` and becomes ``not_evaluable / missing_market_context``. Two things
have to be true and only one of them is obvious:

1. the bars that move are exactly the benchmark holes, and
2. **no signal that fired stops firing, and no new signal starts** — the fix is
   about bookkeeping, and a change to the fired set would mean it is not.

(2) is the claim worth the run. It is asserted per (strategy, instrument) on the
fired INDEX SETS, not on counts, so an equal-sized substitution cannot hide.

⚠⚠ THE CONTROL ARM IS THE SHIPPED SOURCE, RUN COLD, NOT A SIMULATION.
There is no way to reproduce the old behaviour from inside the new code: passing
``not_evaluable_indices=()`` looks like it should, and does not — the regime is
now a DECLARED input, so an unclassifiable value takes the structural warm-up
path and yields ``insufficient_warmup`` where the old code yielded
``not_fired``. A simulated control would have understated the diff by the whole
benchmark warm-up population. So this script is designed to run UNCHANGED on
both arms (it imports nothing this branch adds) and is executed once in a
worktree at ``origin/main`` and once here.

Usage
-----
    uv run python scripts/verify_2437_missing_market_context.py --dump /tmp/control.json
    uv run python scripts/verify_2437_missing_market_context.py --compare /tmp/control.json /tmp/treatment.json
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter

import psycopg

from app.config import settings
from app.services.market_regime_provider import MarketRegimeProvider
from app.services.price_masked_bars import MASKED_REASON, load_masked_bars
from app.services.price_segments import load_unresolved_breaks
from app.services.strategies.validated_universe import load_validated_universe
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_segmented_evaluation import segmented_signals
from app.services.strategy_signal_scan import SCAN_UNIVERSE

#: The three regime-gated per-series strategies. S-10 does not exist yet and
#: S-1…S-4 take no regime, so neither arm can differ on them.
GATED = ("s5-support-bounce", "s6-resistance-breakout", "s9-squeeze-expansion")

#: Below this a strategy cannot produce a decided verdict anyway (200-SMA plus a
#: 126-bar BandWidth window). ⚠ Reported in the dump, never silently applied —
#: a bound that shrinks the population without saying so is how an A/B lies.
MIN_BARS = 250


def dump_arm(conn: psycopg.Connection, path: str) -> None:
    universe = sorted(load_validated_universe(conn))
    provider = MarketRegimeProvider.load(conn)
    # ⚠ The production scan segments on unresolved scale breaks, and the regime
    # is re-sliced per segment — which is exactly where this change could go
    # wrong. Loading them here rather than passing `()` keeps the arms running
    # the shape production runs.
    breaks_by_instrument = load_unresolved_breaks(conn, universe)

    per_strategy: dict[str, dict[str, object]] = {
        strategy_id: {"counts": Counter(), "fired": {}} for strategy_id in GATED
    }
    scanned = skipped_short = 0
    for position, instrument_id in enumerate(universe):
        loaded = load_masked_bars(conn, instrument_id)
        series = loaded.series
        if len(series) < MIN_BARS:
            skipped_short += 1
            continue
        scanned += 1
        regime = provider.for_dates(series.dates)
        breaks = tuple(breaks_by_instrument.get(instrument_id, ()))
        for strategy_id in GATED:
            signals = segmented_signals(
                STRATEGY_MANIFEST[strategy_id],
                series,
                universe=SCAN_UNIVERSE,
                masked_reason=MASKED_REASON,
                unresolved_breaks=breaks,
                regime=regime,
            )
            bucket = per_strategy[strategy_id]
            counts: Counter = bucket["counts"]  # type: ignore[assignment]
            fired: dict[str, list[str]] = bucket["fired"]  # type: ignore[assignment]
            for signal in signals:
                counts[f"{signal.verdict}/{signal.reason or ''}"] += 1
            # ⚠ Fired bars keyed by DATE, not index. Index equality across arms
            # would also be satisfied by a series that loaded differently; the
            # date is the thing a trade would actually be placed on.
            hits = [series.dates[s.signal_index].isoformat() for s in signals if s.verdict == "fired"]
            if hits:
                fired[str(instrument_id)] = hits
        if position % 250 == 0:
            print(f"  … {position}/{len(universe)} instruments", flush=True)

    payload = {
        "scanned_instruments": scanned,
        "skipped_short_of_min_bars": skipped_short,
        "min_bars": MIN_BARS,
        "universe_size": len(universe),
        "strategies": {
            strategy_id: {
                "counts": dict(bucket["counts"]),  # type: ignore[arg-type]
                "fired": bucket["fired"],
            }
            for strategy_id, bucket in per_strategy.items()
        },
    }
    with open(path, "w") as handle:
        json.dump(payload, handle, indent=1, sort_keys=True)
    print(f"wrote {path}: {scanned} instruments scanned, {skipped_short} below {MIN_BARS} bars")


def compare(control_path: str, treatment_path: str) -> int:
    with open(control_path) as handle:
        control = json.load(handle)
    with open(treatment_path) as handle:
        treatment = json.load(handle)

    failures: list[str] = []
    if control["scanned_instruments"] != treatment["scanned_instruments"]:
        failures.append(
            f"arms scanned different populations: {control['scanned_instruments']} vs "
            f"{treatment['scanned_instruments']} — the comparison is meaningless"
        )

    for strategy_id in sorted(set(control["strategies"]) | set(treatment["strategies"])):
        before = control["strategies"][strategy_id]
        after = treatment["strategies"][strategy_id]
        print(f"\n=== {strategy_id}")
        keys = sorted(set(before["counts"]) | set(after["counts"]))
        print(f"{'verdict/reason':<42}{'control':>12}{'treatment':>12}{'delta':>12}")
        for key in keys:
            lhs = before["counts"].get(key, 0)
            rhs = after["counts"].get(key, 0)
            print(f"{key:<42}{lhs:>12,}{rhs:>12,}{rhs - lhs:>+12,}")

        # ⚠⚠ THE CLAIM. Distinct instruments, and the exact date sets.
        moved_instruments = {
            key
            for key in set(before["fired"]) | set(after["fired"])
            if before["fired"].get(key, []) != after["fired"].get(key, [])
        }
        total_before = sum(len(v) for v in before["fired"].values())
        total_after = sum(len(v) for v in after["fired"].values())
        print(
            f"fired signals: {total_before:,} control / {total_after:,} treatment across "
            f"{len(before['fired']):,} / {len(after['fired']):,} distinct instruments"
        )
        if moved_instruments:
            failures.append(
                f"{strategy_id}: the FIRED set changed on {len(moved_instruments)} instruments "
                f"(first few: {sorted(moved_instruments)[:5]}) — this change must not move a signal"
            )

    if failures:
        print("\nFAILED:")
        for line in failures:
            print(f"  - {line}")
        return 1
    print("\nOK: no fired signal moved on any instrument in either arm.")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dump", metavar="PATH", help="run one arm and write its digest")
    parser.add_argument("--compare", nargs=2, metavar=("CONTROL", "TREATMENT"))
    args = parser.parse_args()

    if args.compare:
        sys.exit(compare(*args.compare))
    if not args.dump:
        parser.error("one of --dump or --compare is required")
    with psycopg.connect(settings.database_url) as conn:
        dump_arm(conn, args.dump)


if __name__ == "__main__":
    main()
