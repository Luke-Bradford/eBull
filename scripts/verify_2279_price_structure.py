"""Full-corpus acceptance run for #2279 price-structure primitives.

Spec: ``docs/proposals/ta/2026-08-05-price-structure-primitives.md`` §8.

Reports the four things the spec says must be measured on the FULL population
rather than a panel:

1. swing yield per ladder rung,
2. the EXACT count of pivots made ``not_evaluable`` by the quarantine mask —
   reported, not bounded by an arithmetic that double-counts overlapping windows,
3. agreement with ``scipy.signal.argrelextrema(order=N)``, the reference
   implementation the spec compares against,
4. cost for all six primitives, not swings alone — §6's no-persistence decision
   rests on the whole workload and the earlier benchmark covered only swings.

Plus the fail-closed reader checks (§8.8), which are the ones a unit test cannot
make honestly: what matters is behaviour against REAL coverage rows.

scipy is not a repo dependency and must not become one. Run with::

    uv run --with scipy python scripts/verify_2279_price_structure.py

Read-only. Nothing here writes to the database.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.price_quarantine import RULE_SET_VERSION as QUARANTINE_VERSION  # noqa: E402
from app.services.price_structure import (  # noqa: E402
    SWING_LADDER,
    StructureBar,
    anchored_vwap,
    classify_interaction,
    cluster_levels,
    detect_swings,
    fib_levels,
    find_break_and_retest,
    select_leg,
    volatility_regime,
)
from app.services.research_price_structure_store import load_masked_series  # noqa: E402

# One streaming pass over every bar with its verdicts attached, ordered so the
# consumer can group by series without holding the corpus in memory. Same
# fail-closed shape as research_price_structure_store._LOAD_SQL — coverage row,
# current version, bar inside the evaluated range, COALESCE for sparse absence.
_STREAM_SQL = """
    SELECT d.series_id,
           d.bar_date,
           d.open,
           d.high,
           d.low,
           d.close,
           d.volume,
           COALESCE(q.range_usable, TRUE)  AS range_usable,
           COALESCE(q.return_usable, TRUE) AS return_usable
    FROM research_price_daily d
    JOIN research_price_quarantine_coverage cov
      ON cov.series_id = d.series_id
     AND cov.rule_set_version = %(quarantine_version)s
     AND d.bar_date BETWEEN cov.first_bar AND cov.last_bar
    LEFT JOIN research_bar_quarantine q
      ON q.series_id = d.series_id
     AND q.bar_date = d.bar_date
     AND q.rule_set_version = %(quarantine_version)s
    ORDER BY d.series_id, d.bar_date
"""


def _database_url() -> str:
    for line in Path(".env").read_text().splitlines():
        if line.startswith("DATABASE_URL="):
            return line.split("=", 1)[1].strip()
    raise SystemExit("DATABASE_URL not found in .env")


@dataclass
class RungStats:
    swings: int = 0
    highs: int = 0
    lows: int = 0
    not_evaluable_pivots: int = 0
    series_fired: int = 0
    series_not_fired: int = 0
    series_not_evaluable: int = 0
    #: scipy agreement, restricted to series with NO masked bars — scipy cannot
    #: represent a masked bar, so including them would compare two different
    #: questions. Mask behaviour is measured separately, above.
    cmp_series: int = 0
    cmp_matched: int = 0
    cmp_ours_only: int = 0
    cmp_scipy_only: int = 0
    cmp_scipy_only_at_boundary: int = 0


@dataclass
class Totals:
    series: int = 0
    bars: int = 0
    range_masked: int = 0
    return_masked: int = 0
    series_with_mask: int = 0
    rungs: dict[str, RungStats] = field(default_factory=lambda: defaultdict(RungStats))
    timings: dict[str, float] = field(default_factory=lambda: defaultdict(float))


def _stream_series(conn: psycopg.Connection[Any]) -> Iterator[tuple[int, list[StructureBar], int, int]]:
    """Yield (series_id, masked bars, range_masked, return_masked) per series."""
    current: int | None = None
    bars: list[StructureBar] = []
    range_masked = 0
    return_masked = 0

    with conn.cursor(name="structure_stream") as cur:
        cur.itersize = 50_000
        cur.execute(_STREAM_SQL, {"quarantine_version": QUARANTINE_VERSION})
        for row in cur:
            series_id, bar_date, open_, high, low, close, volume, range_ok, return_ok = row
            if series_id != current:
                if current is not None:
                    yield current, bars, range_masked, return_masked
                current, bars, range_masked, return_masked = series_id, [], 0, 0
            if not range_ok:
                range_masked += 1
            if not return_ok:
                return_masked += 1
            bars.append(
                StructureBar(
                    bar_date=bar_date,
                    open=open_,
                    high=high if range_ok else None,
                    low=low if range_ok else None,
                    close=close if return_ok else None,
                    volume=volume,
                )
            )
    if current is not None:
        yield current, bars, range_masked, return_masked


def _scipy_pivots(bars: list[StructureBar], n: int) -> tuple[set[int], set[int]]:
    # Imported inside the function, and unresolvable to pyright, BOTH on
    # purpose: scipy is the reference implementation this run compares against
    # and must never become a repo dependency. It is supplied per-invocation by
    # `uv run --with scipy`. A module-level import would make the rest of this
    # script unimportable without it.
    import numpy as np
    from scipy.signal import argrelextrema  # pyright: ignore[reportMissingImports]

    highs = np.array([float(b.high) for b in bars])  # type: ignore[arg-type]
    lows = np.array([float(b.low) for b in bars])  # type: ignore[arg-type]
    return (
        set(argrelextrema(highs, np.greater, order=n)[0].tolist()),
        set(argrelextrema(lows, np.less, order=n)[0].tolist()),
    )


def _measure_series(
    bars: list[StructureBar],
    masked: bool,
    totals: Totals,
    *,
    compare: bool,
) -> None:
    for name, n in SWING_LADDER.items():
        stats = totals.rungs[name]
        t0 = time.perf_counter()
        result = detect_swings(bars, n, universe="survivor_only")
        totals.timings[f"swings:{name}"] += time.perf_counter() - t0

        stats.swings += len(result.swings)
        stats.highs += sum(1 for s in result.swings if s.kind == "high")
        stats.lows += sum(1 for s in result.swings if s.kind == "low")
        stats.not_evaluable_pivots += len(result.not_evaluable_indices)
        if result.state == "fired":
            stats.series_fired += 1
        elif result.state == "not_fired":
            stats.series_not_fired += 1
        else:
            stats.series_not_evaluable += 1

        if compare and not masked and len(bars) >= 2 * n + 1:
            ours_h = {s.index for s in result.swings if s.kind == "high"}
            ours_l = {s.index for s in result.swings if s.kind == "low"}
            their_h, their_l = _scipy_pivots(bars, n)
            stats.cmp_series += 1
            for ours, theirs in ((ours_h, their_h), (ours_l, their_l)):
                stats.cmp_matched += len(ours & theirs)
                stats.cmp_ours_only += len(ours - theirs)
                extra = theirs - ours
                stats.cmp_scipy_only += len(extra)
                stats.cmp_scipy_only_at_boundary += sum(1 for i in extra if i < n or i >= len(bars) - n)

        # Downstream primitives, timed on the medium rung only — they consume
        # swings, so running all three would triple-count the same workload.
        if name != "medium":
            continue
        t0 = time.perf_counter()
        levels = cluster_levels(bars, result, universe="survivor_only")
        totals.timings["levels"] += time.perf_counter() - t0

        t0 = time.perf_counter()
        fib_levels(select_leg(result.swings), universe="survivor_only")
        totals.timings["fib"] += time.perf_counter() - t0

        t0 = time.perf_counter()
        if result.swings:
            anchored_vwap(bars, result.swings[-1], universe="survivor_only")
        totals.timings["vwap"] += time.perf_counter() - t0

        t0 = time.perf_counter()
        volatility_regime([b.close for b in bars], universe="survivor_only")
        totals.timings["regime"] += time.perf_counter() - t0

        # Interaction + break/retest are per-level, so cost scales with level
        # count. Measure on the single most-touched level rather than all of
        # them: the per-level cost is what a strategy pays, not the sum.
        if levels.levels:
            busiest = max(levels.levels, key=lambda lv: lv.touches)
            t0 = time.perf_counter()
            for i in range(len(bars)):
                classify_interaction(busiest, bars, i)
            totals.timings["interaction"] += time.perf_counter() - t0

            t0 = time.perf_counter()
            find_break_and_retest(busiest, bars, universe="survivor_only", max_retest_bars=2 * n)
            totals.timings["break_retest"] += time.perf_counter() - t0


def _reader_checks(conn: psycopg.Connection[Any]) -> None:
    """§8.8 — fail-closed behaviour against REAL coverage rows."""
    print("\n=== fail-closed reader (§8.8) ===")

    sample = conn.execute(
        "SELECT series_id FROM research_price_series WHERE bar_count > 200 ORDER BY series_id LIMIT 3"
    ).fetchall()
    for (series_id,) in sample:
        loaded = load_masked_series(conn, series_id)
        print(
            f"  series {series_id:>6}  bars {len(loaded.bars):>7}  "
            f"range_masked {loaded.range_masked:>3}  return_masked {loaded.return_masked:>3}"
        )
        assert loaded.evaluated, "a covered series must return bars"

    missing = load_masked_series(conn, -1)
    print(f"  nonexistent series -> {len(missing.bars)} bars (expect 0)")
    assert not missing.evaluated

    # Stale rule-set version must read as UNUSABLE, not as clean. Exercised by
    # running the reader's own SQL with a version that does not exist — the
    # COALESCE alone would happily return every bar here, which is exactly the
    # failure the coverage join exists to prevent.
    stale = conn.execute(
        _STREAM_SQL.replace("ORDER BY d.series_id, d.bar_date", "LIMIT 1"),
        {"quarantine_version": "price-quarantine-v1+deadbeefdead"},
    ).fetchall()
    print(f"  stale rule_set_version -> {len(stale)} rows (expect 0)")
    assert not stale, "a stale rule-set version must read as unusable"

    covered = conn.execute(
        """
        SELECT count(*) FROM research_price_series s
        WHERE s.bar_count IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM research_price_quarantine_coverage c
              WHERE c.series_id = s.series_id AND c.rule_set_version = %(v)s
          )
        """,
        {"v": QUARANTINE_VERSION},
    ).fetchone()
    print(f"  series with bars but no CURRENT coverage row -> {covered[0] if covered else '?'} (expect 0)")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0, help="stop after N series (0 = full corpus)")
    parser.add_argument("--no-compare", action="store_true", help="skip the scipy agreement arm")
    args = parser.parse_args()

    started = time.perf_counter()
    totals = Totals()

    with psycopg.connect(_database_url()) as conn:
        _reader_checks(conn)

        print("\n=== full-corpus run ===", flush=True)
        for series_id, bars, range_masked, return_masked in _stream_series(conn):
            totals.series += 1
            totals.bars += len(bars)
            totals.range_masked += range_masked
            totals.return_masked += return_masked
            masked = bool(range_masked or return_masked)
            if masked:
                totals.series_with_mask += 1
            _measure_series(bars, masked, totals, compare=not args.no_compare)

            if args.limit and totals.series >= args.limit:
                break
            if totals.series % 500 == 0:
                print(
                    f"  {totals.series:>5} series / {totals.bars:>10,} bars ({time.perf_counter() - started:.0f}s)",
                    flush=True,
                )

    print(f"\nseries {totals.series:,}   bars {totals.bars:,}")
    print(
        f"masked: {totals.range_masked:,} range / {totals.return_masked:,} return "
        f"across {totals.series_with_mask:,} series"
    )

    print("\n=== swing yield + mask blast radius, per rung (§8.3) ===")
    print(f"{'rung':8}{'N':>4}{'swings':>12}{'highs':>11}{'lows':>11}{'not_eval':>10}{'%bars':>9}")
    for name, n in SWING_LADDER.items():
        s = totals.rungs[name]
        pct = 100.0 * s.not_evaluable_pivots / totals.bars if totals.bars else 0.0
        print(f"{name:8}{n:>4}{s.swings:>12,}{s.highs:>11,}{s.lows:>11,}{s.not_evaluable_pivots:>10,}{pct:>8.3f}%")

    print("\n=== series tri-state, per rung (§8.1) ===")
    print(f"{'rung':8}{'fired':>10}{'not_fired':>12}{'not_evaluable':>16}")
    for name in SWING_LADDER:
        s = totals.rungs[name]
        print(f"{name:8}{s.series_fired:>10,}{s.series_not_fired:>12,}{s.series_not_evaluable:>16,}")

    if not args.no_compare:
        print("\n=== scipy.signal.argrelextrema agreement, unmasked series only (§8.2) ===")
        print(f"{'rung':8}{'series':>9}{'matched':>12}{'ours_only':>12}{'scipy_only':>12}{'..at edge':>11}")
        for name in SWING_LADDER:
            s = totals.rungs[name]
            print(
                f"{name:8}{s.cmp_series:>9,}{s.cmp_matched:>12,}{s.cmp_ours_only:>12,}"
                f"{s.cmp_scipy_only:>12,}{s.cmp_scipy_only_at_boundary:>11,}"
            )

    print("\n=== cost, all six primitives (§8.6) ===")
    for label, seconds in sorted(totals.timings.items(), key=lambda kv: -kv[1]):
        print(f"  {label:<20}{seconds:>9.1f}s")
    print(f"  {'TOTAL wall':<20}{time.perf_counter() - started:>9.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
