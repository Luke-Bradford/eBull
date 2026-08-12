"""Full-population verification of phase-5e-5b's random-entry synthetic control (#2240).

    PYTHONPATH=. uv run python scripts/verify_2240_random_entry_cohort.py --prepare
    PYTHONPATH=. uv run python scripts/verify_2240_random_entry_cohort.py --cohort --strategy S-1 --members 0:125
    PYTHONPATH=. uv run python scripts/verify_2240_random_entry_cohort.py --report

⚠ NOTHING IS WRITTEN TO THE DATABASE. Every arm reads Postgres and writes only
to its cache directory. Gate on the EXIT CODE — 0 means every assertion passed.

⚠ NEVER PIPE THIS INTO ``head``/``tail``. A pipe returns the pipe's status, so a
failure reads as success, and it buffers the progress lines a long run is judged
by (`.claude/CLAUDE.md`). Redirect to a file and read the file.

WHY THREE ARMS AND NOT ONE
--------------------------
§9's cohort is 1,000 strategies over the whole validated universe. Measured on
this corpus at stage 5b, S-1 alone produces 3,135,355 positions, and
``build_equity_curve`` over that book takes ~17 s — so the cohort is ~5.5 CPU
hours of ARITHMETIC sitting behind ~1.5 hours of DATABASE READ that is identical
for every member. Doing both in one process would re-read the corpus 1,000
times.

  ``--prepare``  ONE sweep of the corpus. Runs the real S-1 and S-3 sleeves
                 through the shipped path, records their criterion-7 metrics,
                 and writes the cohort's inputs to a cache: per series, the
                 ELIGIBLE FILL BARS with their opens and half-spreads, the
                 panel-aligned closes a leg's marks are sliced from, and per
                 strategy the multiset of realised HOLDING PERIODS to permute.
  ``--cohort``   A contiguous slice of members, off the cache, no database.
                 Shardable across processes; member ``m``'s stream is a pure
                 function of ``(COHORT_ROOT_SEED, m)``, so the shard boundaries
                 cannot move a single draw.
  ``--report``   Every shard's members, §9's two thresholds, and the assertions.

THE ASSERTIONS
--------------
  R1  **The match is EXACT where the construction makes it exact.** Every member
      trades the same number of positions as the strategy it was permuted from,
      per series and in total. ⚠ A tolerance here would hide the one failure
      mode the permutation can have — a series whose holds were silently
      dropped — so this is equality.
  R2  **The holding-period multiset is preserved**, per series. Asserted by
      re-deriving it from the placed legs rather than from the input array.
  R3  **No member's positions overlap within an instrument**, which is §3.1's
      pyramiding rule. Asserted on the placed ordinals, not assumed from the
      generator.
  R4  **Every leg is inside the eligible space**: past the strategy's declared
      warm-up, inside the evaluation window, on a bar with a usable open.
  R5  **The cohort is the declared size and the seeds are the declared ones.**
      Every member index appears exactly once across the shards, and the union
      is ``0 … SPEC_COHORT_SIZE-1``.
  R6  **Both §9 thresholds are computed and reported.** ⚠ A strategy FAILING
      them is a RESULT, not a script failure — §10 of the spec says the most
      likely outcome of stage 5e is that some or all of S-1..S-4 fail the random
      cohort. The exit code gates the HARNESS properties (R1-R5, R7), never the
      verdict.
  R7  **The cohort's engine is the strategy's engine.** Members are priced by
      the same cost model and run through the same ``build_equity_curve`` and
      ``compute_metrics``; the bridge is asserted by re-pricing a sample of legs
      through ``cost_model``'s Decimal path and requiring agreement.

⚠ WHAT IS NOT COVERED, STATED SO THE GAP IS A DECISION. Only S-1 and S-3 run,
for phase 5a's reason: S-2 (``calendar``) needs its whole panel resident at once
and S-4 (``level``) needs the resolver over the corpus. So the control exists for
two of the four catalogued strategies, and the two it does not cover are the two
whose exit regimes are not signal-pair.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import stat as stat_module
import sys
import time
from collections import Counter
from datetime import date
from decimal import Decimal
from pathlib import Path

import numpy as np
import numpy.typing as npt
import psycopg

from app.config import settings
from app.services.cost_model import (
    CARRY_UNMODELLED,
    COST_MODEL_ID,
    FX_UNMODELLED,
    UNKNOWN_NOMINAL_PRICE_BAND,
)
from app.services.equity_curve import SIZING_RULE_ID, EquityCurve, LegBook, build_equity_curve
from app.services.indicator_series import BarSeries
from app.services.position_builder import Window, build_positions
from app.services.position_costing import cost_positions
from app.services.random_entry_cohort import (
    COHORT_MODEL_ID,
    COHORT_ROOT_SEED,
    SPEC_COHORT_SIZE,
    SPEC_SHARPE_PERCENTILE,
    MemberOutcome,
    decimal_net_prices,
    evaluate_control,
    match_residual,
    member_seed,
    net_entry_prices,
    net_exit_prices,
    place_entries,
    slack,
)
from app.services.research_price_structure_store import QUARANTINE_RULE_SET_VERSION, load_masked_series
from app.services.signal_ledger import resolve_fills
from app.services.strategies.s1_time_series_momentum import (
    S1_STRATEGY_ID,
    s1_identity,
    s1_signals,
)
from app.services.strategies.s1_time_series_momentum import (
    WARMUP_BARS as S1_WARMUP_BARS,
)
from app.services.strategies.s3_mean_reversion_in_trend import (
    S3_STRATEGY_ID,
    s3_identity,
    s3_signals,
)
from app.services.strategies.s3_mean_reversion_in_trend import (
    WARMUP_BARS as S3_WARMUP_BARS,
)
from app.services.strategies.validated_universe import load_validated_universe
from app.services.strategy_result import EVALUATION_WINDOW_END, EVALUATION_WINDOW_START
from app.services.strategy_statistics import METRIC_SET_ID, StrategyMetrics, TradeReturns, compute_metrics

# ⚠ REUSED, not re-derived. 5a built the corpus→positions path, 5b the costing
# and 5d the curve layer. This script owns the PERMUTATION and the aggregation.
from scripts.verify_2240_position_builder import (
    _S1_REGIME,
    _S3_REGIME,
    UNIVERSE,
    _fills,
    _stamped_versions,
    _to_series,
)
from scripts.verify_2240_statistics import _AXIS_SQL, _benchmark_leg, _Sleeve

#: Where ``--prepare`` writes and the other three arms read.
#:
#: ⚠ NOT a ``tempfile.mkdtemp``, and the reason is the sharding: the cache is
#: ~640 MB, takes over 20 minutes to rebuild, and is read by SEPARATE PROCESSES
#: that must agree on the path. A per-process temp directory would make
#: ``--cohort`` unable to find what ``--prepare`` wrote.
#:
#: ⚠⚠ AND NOT ``/tmp`` EITHER. A fixed, predictable path under a world-writable
#: directory, reused with ``exist_ok=True``, is a symlink/TOCTOU target on a
#: shared box — somebody else's symlink at that name redirects every ``np.save``
#: below (review bot WARNING, PR #2395). The default now lives under the
#: invoking user's own cache directory, and ``_ensure_cache_root`` refuses any
#: path that is a symlink or is not owned by the current uid.
DEFAULT_CACHE = Path.home() / ".cache" / "ebull" / "2240_cohort"


def _ensure_cache_root(root: Path, *, create: bool) -> Path:
    """The cache directory, refused unless it is ours and is a real directory.

    ⚠ THE CHECK IS ON THE PATH ITSELF, not on its parent: ``O_NOFOLLOW`` on the
    final component is what a symlink attack needs to defeat, and ``lstat``
    is what sees it. A directory owned by another uid is refused for the same
    reason — this arm writes ~640 MB into it and later reads that back as the
    population every figure is computed on.
    """
    if create:
        root.mkdir(parents=True, exist_ok=True)
    if not root.exists():
        raise SystemExit(f"cache {root} does not exist — run --prepare first")
    stat = root.lstat()
    if stat_module.S_ISLNK(stat.st_mode):
        raise SystemExit(f"cache {root} is a symlink; refusing to read or write through it")
    if not root.is_dir():
        raise SystemExit(f"cache {root} is not a directory")
    if stat.st_uid != os.getuid():
        raise SystemExit(f"cache {root} is owned by uid {stat.st_uid}, not {os.getuid()}; refusing to use it")
    return root


_LABELS: tuple[str, ...] = ("S-1", "S-3")
_WARMUP: dict[str, int] = {"S-1": S1_WARMUP_BARS, "S-3": S3_WARMUP_BARS}

#: How many legs ``--report`` re-prices through the Decimal cost model for R7.
#: ⚠ A SAMPLE, and it says so — the exhaustive check is the bridge test in
#: ``tests/test_random_entry_cohort.py``, which covers every band boundary.
#: This one catches a wiring slip between the cache and the arithmetic.
_REPRICE_SAMPLE = 5000


# ---------------------------------------------------------------------------
# The cache
# ---------------------------------------------------------------------------


class _Cache:
    """The cohort's inputs, flat and per-series-offset, memory-mapped on read.

    ⚠ FLAT ARRAYS WITH OFFSETS, not a list of per-series objects. 5,266 series
    of Python objects is the shape ``LegBook``'s header already rejects for the
    same reason: the data is read once, in index order, by a loop that runs
    1,000 times.
    """

    def __init__(self, root: Path) -> None:
        self.root = root

    def save(self, name: str, array: npt.NDArray[np.generic]) -> None:
        np.save(self.root / f"{name}.npy", array)

    def load(self, name: str) -> npt.NDArray[np.generic]:
        return np.load(self.root / f"{name}.npy", mmap_mode="r")

    def write_meta(self, meta: dict[str, object]) -> None:
        (self.root / "meta.json").write_text(json.dumps(meta, indent=2, sort_keys=True, default=str))

    def read_meta(self) -> dict[str, object]:
        return json.loads((self.root / "meta.json").read_text())


def _metrics_payload(metrics: StrategyMetrics) -> dict[str, float | int]:
    """The strategy-side figures §9's comparison and its match residual read."""
    return {
        "sharpe": metrics.sharpe,
        "total_return_pct": metrics.total_return_pct,
        "exposure_time_pct": metrics.exposure_time_pct,
        "turnover_annualised": metrics.turnover_annualised,
        "trade_count": metrics.trade_count,
    }


# ---------------------------------------------------------------------------
# --prepare
# ---------------------------------------------------------------------------


def prepare(*, cache_root: Path, limit: int | None) -> int:
    """One corpus sweep: the real sleeves, and the cohort's inputs."""
    started = time.monotonic()
    cache = _Cache(_ensure_cache_root(cache_root, create=True))
    s1_version, s3_version, builder_version = _stamped_versions()
    print(f"\n[prepare] {S1_STRATEGY_ID} {s1_version}", flush=True)
    print(f"          {S3_STRATEGY_ID} {s3_version}", flush=True)
    print(f"          builder {builder_version}   cost model {COST_MODEL_ID}   sizing {SIZING_RULE_ID}", flush=True)
    print(f"          metrics {METRIC_SET_ID}   cohort {COHORT_MODEL_ID}   quarantine {QUARANTINE_RULE_SET_VERSION}")
    print(f"          warm-up  S-1 {S1_WARMUP_BARS} bars   S-3 {S3_WARMUP_BARS} bars", flush=True)
    window = Window(start=EVALUATION_WINDOW_START, end=EVALUATION_WINDOW_END)

    problems: list[str] = []
    sleeves = {label: _Sleeve(label) for label in _LABELS}
    benchmark = LegBook()

    # Flat accumulators. ⚠ Python lists of numpy arrays, concatenated once at the
    # end — appending to a growing ndarray is quadratic.
    t_panel: list[npt.NDArray[np.int32]] = []
    t_open: list[npt.NDArray[np.float64]] = []
    t_half: list[npt.NDArray[np.float64]] = []
    t_offset: list[int] = [0]
    close_chunks: list[npt.NDArray[np.float64]] = []
    close_offset: list[int] = [0]
    first_panel: list[int] = []
    t_start: dict[str, list[int]] = {label: [] for label in _LABELS}
    holds: dict[str, list[npt.NDArray[np.int32]]] = {label: [] for label in _LABELS}
    holds_offset: dict[str, list[int]] = {label: [0] for label in _LABELS}
    unmatchable: Counter[str] = Counter()
    no_slack: Counter[str] = Counter()

    with psycopg.connect(settings.database_url) as conn:
        universe = load_validated_universe(conn)
        bounds = {"ids": list(universe), "start": EVALUATION_WINDOW_START, "end": EVALUATION_WINDOW_END}
        axis = tuple(row[0] for row in conn.execute(_AXIS_SQL, bounds).fetchall())
        axis_pos = {when: index for index, when in enumerate(axis)}
        print(f"  validated universe   {len(universe):>12,} instruments", flush=True)
        print(f"  evaluation axis      {len(axis):>12,} trading dates", flush=True)

        pairs = conn.execute(
            "SELECT instrument_id, series_id FROM research_price_series "
            "WHERE instrument_id = ANY(%(ids)s) ORDER BY instrument_id, series_id",
            {"ids": list(universe)},
        ).fetchall()
        if limit is not None:
            pairs = pairs[:limit]
            print(f"  ⚠ LIMITED to the first {len(pairs)} series — NOT a full-population figure", flush=True)

        for n, (instrument_id, series_id) in enumerate(pairs, start=1):
            loaded = load_masked_series(conn, int(series_id))
            series, _masked_opens = _to_series(loaded.bars)
            placed = _absorb_series(
                series,
                instrument_id=int(instrument_id),
                axis_pos=axis_pos,
                window=window,
                sleeves=sleeves,
                benchmark=benchmark,
                versions=(s1_version, s3_version),
            )
            # ⚠ EVERY SERIES GETS A SLOT, including one that produced no trade —
            # the offsets are positional and a skipped series would shift every
            # later one, which is the silent kind of wrong.
            if placed is None:
                t_panel.append(np.empty(0, dtype=np.int32))
                t_open.append(np.empty(0, dtype=np.float64))
                t_half.append(np.empty(0, dtype=np.float64))
                close_chunks.append(np.empty(0, dtype=np.float64))
                first_panel.append(0)
                for label in _LABELS:
                    t_start[label].append(0)
                    holds[label].append(np.empty(0, dtype=np.int32))
            else:
                t_panel.append(placed.panel)
                t_open.append(placed.opens)
                t_half.append(placed.half_spreads)
                close_chunks.append(placed.closes)
                first_panel.append(placed.first_panel)
                for label in _LABELS:
                    t_start[label].append(placed.warm_start[label])
                    series_holds = placed.holds[label]
                    unmatchable[label] += placed.unmatchable[label]
                    eligible = int(placed.panel.size) - placed.warm_start[label]
                    if series_holds.size and slack(eligible=eligible, holds=series_holds.astype(np.int64)) < 0:
                        # ⚠ A CONTRADICTION, not a rare shape: the real positions
                        # are non-overlapping in this same ordinal space, so their
                        # holds must fit in it. Counted and REFUSED rather than
                        # trimmed, because trimming would change the match.
                        no_slack[label] += 1
                        series_holds = np.empty(0, dtype=np.int32)
                    holds[label].append(series_holds)
            t_offset.append(t_offset[-1] + int(t_panel[-1].size))
            close_offset.append(close_offset[-1] + int(close_chunks[-1].size))
            for label in _LABELS:
                holds_offset[label].append(holds_offset[label][-1] + int(holds[label][-1].size))
            if n % 250 == 0:
                print(f"  {n}/{len(pairs)} series ({time.monotonic() - started:.0f}s)", flush=True)

    print(f"\n  corpus read in {time.monotonic() - started:.1f}s", flush=True)
    benchmark_curve = build_equity_curve(benchmark, date_count=len(axis))
    print(f"  benchmark legs       {len(benchmark):>12,}", flush=True)

    real: dict[str, dict[str, float | int]] = {}
    for label in _LABELS:
        sleeve = sleeves[label]
        metrics = sleeve.report(axis=axis, benchmark_curve=benchmark_curve)
        problems.extend(sleeve.problems)
        if metrics is None:
            problems.append(f"R6 {label}: the real sleeve produced no metric set, so §9 has nothing to compare")
            continue
        real[label] = _metrics_payload(metrics)

    cache.save("axis_days", np.asarray([when.toordinal() for when in axis], dtype=np.int64))
    cache.save("t_panel", np.concatenate(t_panel) if t_panel else np.empty(0, dtype=np.int32))
    cache.save("t_open", np.concatenate(t_open) if t_open else np.empty(0, dtype=np.float64))
    cache.save("t_half", np.concatenate(t_half) if t_half else np.empty(0, dtype=np.float64))
    cache.save("t_offset", np.asarray(t_offset, dtype=np.int64))
    cache.save("closes", np.concatenate(close_chunks) if close_chunks else np.empty(0, dtype=np.float64))
    cache.save("close_offset", np.asarray(close_offset, dtype=np.int64))
    cache.save("first_panel", np.asarray(first_panel, dtype=np.int64))
    for label in _LABELS:
        key = label.replace("-", "")
        cache.save(f"{key}_t_start", np.asarray(t_start[label], dtype=np.int64))
        cache.save(f"{key}_holds", np.concatenate(holds[label]) if holds[label] else np.empty(0, dtype=np.int32))
        cache.save(f"{key}_holds_offset", np.asarray(holds_offset[label], dtype=np.int64))
    cache.save("benchmark_equity", benchmark_curve.equity)
    cache.save("benchmark_invested", benchmark_curve.invested)
    cache.save("benchmark_open_count", benchmark_curve.open_count)
    cache.save("benchmark_traded", benchmark_curve.traded_notional)
    cache.write_meta(
        {
            "cohort_model_id": COHORT_MODEL_ID,
            "cohort_root_seed": COHORT_ROOT_SEED,
            "cohort_size": SPEC_COHORT_SIZE,
            "cost_model_id": COST_MODEL_ID,
            "carry_unmodelled": CARRY_UNMODELLED,
            "fx_unmodelled": FX_UNMODELLED,
            "sizing_rule": SIZING_RULE_ID,
            "metric_set_id": METRIC_SET_ID,
            "quarantine_rule_set_version": QUARANTINE_RULE_SET_VERSION,
            "builder_version": builder_version,
            "strategy_versions": {"S-1": s1_version, "S-3": s3_version},
            "warmup_bars": {label: _WARMUP[label] for label in _LABELS},
            "series_count": len(t_offset) - 1,
            "axis_dates": len(axis),
            "window_start": EVALUATION_WINDOW_START,
            "window_end": EVALUATION_WINDOW_END,
            "limited_to_series": limit,
            "real": real,
            "unmatchable_positions": dict(unmatchable),
            "series_without_slack": dict(no_slack),
        }
    )

    for label in _LABELS:
        total = int(np.asarray(holds_offset[label][-1]))
        print(f"\n  [{label}] holds cached      {total:>12,}")
        print(f"        unmatchable       {unmatchable[label]:>12,}   (an endpoint outside the eligible bars)")
        print(f"        series w/o slack  {no_slack[label]:>12,}   ⚠ must be 0")
        if no_slack[label]:
            problems.append(
                f"R1 {label}: {no_slack[label]} series cannot carry their own realised holds in the eligible space"
            )
    print(f"\n  eligible bars cached {int(t_offset[-1]):>12,}")
    print(f"  mark closes cached   {int(close_offset[-1]):>12,}")
    print(f"\n  property violations: {len(problems)}")
    for problem in problems[:20]:
        print(f"    {problem}")
    print(f"\n  elapsed  {time.monotonic() - started:.1f}s", flush=True)
    _stamped_versions()
    return 1 if problems else 0


class _Placed:
    """One series' cohort inputs, plus the real sleeves' holds off it."""

    __slots__ = ("closes", "first_panel", "half_spreads", "holds", "opens", "panel", "unmatchable", "warm_start")

    def __init__(
        self,
        *,
        panel: npt.NDArray[np.int32],
        opens: npt.NDArray[np.float64],
        half_spreads: npt.NDArray[np.float64],
        closes: npt.NDArray[np.float64],
        first_panel: int,
        warm_start: dict[str, int],
        holds: dict[str, npt.NDArray[np.int32]],
        unmatchable: dict[str, int],
    ) -> None:
        self.panel = panel
        self.opens = opens
        self.half_spreads = half_spreads
        self.closes = closes
        self.first_panel = first_panel
        self.warm_start = warm_start
        self.holds = holds
        self.unmatchable = unmatchable


def _absorb_series(
    series: BarSeries,
    *,
    instrument_id: int,
    axis_pos: dict[date, int],
    window: Window,
    sleeves: dict[str, _Sleeve],
    benchmark: LegBook,
    versions: tuple[str, str],
) -> _Placed | None:
    """5d's loop body, plus the eligible-bar table the permutation places into."""
    indices = [axis_pos[when] for when in series.dates if when in axis_pos]
    if len(indices) < 2:
        return None
    first_axis_index, last_axis_index = indices[0], indices[-1]
    closes = [float("nan")] * (last_axis_index - first_axis_index + 1)
    for when, row in zip(series.dates, series.rows, strict=True):
        slot = axis_pos.get(when)
        close = row.get("close")
        if slot is not None and close is not None:
            closes[slot - first_axis_index] = float(close)

    _benchmark_leg(
        benchmark,
        series=series,
        axis_pos=axis_pos,
        closes=closes,
        first_axis_index=first_axis_index,
    )

    # ⚠⚠ THE ELIGIBLE FILL BARS, and every clause is a condition the real
    # strategy is also under. A bar with no usable open cannot be filled on
    # (`signal_ledger` calls it `no_fill_bar`); a bar outside the window is not
    # in the evaluation; a bar off the panel axis has no equity-curve slot. The
    # warm-up is applied per strategy below, because the two differ.
    eligible_bar: list[int] = []
    eligible_panel: list[int] = []
    eligible_open: list[float] = []
    eligible_half: list[float] = []
    for index, when in enumerate(series.dates):
        if not window.contains(when):
            continue
        slot = axis_pos.get(when)
        if slot is None:
            continue
        bar_open = series.rows[index].get("open")
        if bar_open is None or bar_open <= 0:
            continue
        eligible_bar.append(index)
        eligible_panel.append(slot)
        eligible_open.append(float(bar_open))
        eligible_half.append(float(UNKNOWN_NOMINAL_PRICE_BAND.half_spread))

    bar_to_ordinal = {bar: ordinal for ordinal, bar in enumerate(eligible_bar)}
    warm_start: dict[str, int] = {}
    for label in _LABELS:
        warm_start[label] = int(np.searchsorted(np.asarray(eligible_bar, dtype=np.int64), _WARMUP[label]))

    s1_version, s3_version = versions
    holds: dict[str, npt.NDArray[np.int32]] = {}
    unmatchable: dict[str, int] = {}
    for label, identity, produced, regime, strategy_id, version in (
        (
            "S-1",
            s1_identity,
            s1_signals(series, universe=UNIVERSE, close_reason="quarantined_bar"),
            _S1_REGIME,
            S1_STRATEGY_ID,
            s1_version,
        ),
        (
            "S-3",
            s3_identity,
            s3_signals(series, universe=UNIVERSE, close_reason="quarantined_bar"),
            _S3_REGIME,
            S3_STRATEGY_ID,
            s3_version,
        ),
    ):
        rows = resolve_fills(
            produced,
            series=series,
            identity=identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID),
            instrument_id=instrument_id,
        )
        entries, exits = _fills(rows, instrument_id)
        built = build_positions(
            strategy_id=strategy_id,
            strategy_version=version,
            entries=entries,
            exits=exits,
            outcomes=[],
            outcome_pin=None,
            series={instrument_id: series},
            regime=regime,
            window=window,
        )
        costed = list(cost_positions(built.positions, price_basis="split_adjusted"))
        sleeves[label].absorb(
            costed,
            series=series,
            window=window,
            axis_pos=axis_pos,
            closes=closes,
            first_axis_index=first_axis_index,
        )
        # ⚠ REALISED CLOSES ONLY. A position open at the window end exits at a
        # MARK (a close, one side costed), not at an open, so a permuted twin of
        # it would be a differently-priced trade. Counted as unmatchable and
        # reported — the cohort matches the realised population, and §9's
        # "matched on exposure and turnover" is measured against that.
        bar_index = {when: index for index, when in enumerate(series.dates)}
        matched: list[int] = []
        dropped = 0
        for row in costed:
            position = row.position
            if row.uncosted_reason is not None or position.close_bar_date is None:
                dropped += 1
                continue
            entry_bar = bar_index.get(position.entry_fill_bar_date)
            exit_bar = bar_index.get(position.close_bar_date)
            if entry_bar is None or exit_bar is None:
                dropped += 1
                continue
            entry_ordinal = bar_to_ordinal.get(entry_bar)
            exit_ordinal = bar_to_ordinal.get(exit_bar)
            if entry_ordinal is None or exit_ordinal is None or exit_ordinal < entry_ordinal:
                dropped += 1
                continue
            matched.append(exit_ordinal - entry_ordinal)
        holds[label] = np.asarray(matched, dtype=np.int32)
        unmatchable[label] = dropped

    return _Placed(
        panel=np.asarray(eligible_panel, dtype=np.int32),
        opens=np.asarray(eligible_open, dtype=np.float64),
        half_spreads=np.asarray(eligible_half, dtype=np.float64),
        closes=np.asarray(closes, dtype=np.float64),
        first_panel=first_axis_index,
        warm_start=warm_start,
        holds=holds,
        unmatchable=unmatchable,
    )


# ---------------------------------------------------------------------------
# --cohort
# ---------------------------------------------------------------------------


def cohort(*, cache_root: Path, label: str, first: int, last: int, zero_cost: bool = False) -> int:
    """Members ``[first, last)`` of ``label``'s cohort, off the cache.

    ⚠⚠ ``zero_cost`` IS A DIAGNOSTIC ABLATION AND IS NOT AN ARM OF §9. It reruns
    the identical placement — same seeds, same entries, same holds — with the
    half-spread set to zero on both fill sides and on the rebalance, and writes
    to an ``ablation_`` shard that ``--report``'s ``members_`` glob cannot pick
    up. It exists to answer ONE question that §9.2 must not assert without
    measuring: is the cohort's catastrophic mean net return the COST MODEL, or
    something else (universe drift, a placement bug, the sizing rule, a mispriced
    exit)? A cost-free rerun that lands near zero says the first; one that is
    still catastrophic says the diagnosis was wrong. (Codex checkpoint 1 on §9.2:
    *"the measured −99.59% is not proven to be the cost model doing its job"*.)

    ⚠ Its output MUST NOT be quoted as a §9 figure. A zero-cost backtest violates
    criterion 2 outright.
    """
    started = time.monotonic()
    cache_root = _ensure_cache_root(cache_root, create=False)
    cache = _Cache(cache_root)
    meta = cache.read_meta()
    key = label.replace("-", "")
    axis = tuple(date.fromordinal(int(day)) for day in cache.load("axis_days"))
    benchmark_curve = EquityCurve(
        equity=np.asarray(cache.load("benchmark_equity"), dtype=np.float64),
        invested=np.asarray(cache.load("benchmark_invested"), dtype=np.float64),
        open_count=np.asarray(cache.load("benchmark_open_count"), dtype=np.int32),
        traded_notional=np.asarray(cache.load("benchmark_traded"), dtype=np.float64),
        # ⚠ The four counters are the BENCHMARK's own narrowings and are already
        # reported by 5d. `compute_metrics` reads only `equity[-1]` off this
        # object, so re-persisting them would be four columns nobody reads.
        rebalance_costs=0.0,
        event_dates=0,
        short_funded_entries=0,
        stale_marks=0,
        unrealised_held=0,
    )
    t_panel = cache.load("t_panel")
    t_open = cache.load("t_open")
    t_half = cache.load("t_half")
    t_offset = np.asarray(cache.load("t_offset"), dtype=np.int64)
    closes = cache.load("closes")
    close_offset = np.asarray(cache.load("close_offset"), dtype=np.int64)
    first_panel = np.asarray(cache.load("first_panel"), dtype=np.int64)
    t_start = np.asarray(cache.load(f"{key}_t_start"), dtype=np.int64)
    holds_all = cache.load(f"{key}_holds")
    holds_offset = np.asarray(cache.load(f"{key}_holds_offset"), dtype=np.int64)
    series_count = int(t_offset.size) - 1
    expected_trades = int(holds_offset[-1])

    arm = "  ⚠ ZERO-COST ABLATION — a diagnostic, NOT a §9 figure" if zero_cost else ""
    print(f"\n[cohort] {label}   members {first}…{last - 1}   {COHORT_MODEL_ID}{arm}", flush=True)
    print(f"         series {series_count:,}   axis {len(axis):,}   holds {expected_trades:,}", flush=True)
    print(f"         root seed {meta['cohort_root_seed']}   cost model {meta['cost_model_id']}", flush=True)

    problems: list[str] = []
    results: list[MemberOutcome] = []
    for index in range(first, last):
        rng = np.random.Generator(np.random.PCG64(member_seed(index)))
        book = LegBook()
        returns: list[float] = []
        entry_dates: list[date] = []
        for s in range(series_count):
            lo, hi = int(holds_offset[s]), int(holds_offset[s + 1])
            if lo == hi:
                continue
            series_holds = np.asarray(holds_all[lo:hi], dtype=np.int64)
            base = int(t_offset[s]) + int(t_start[s])
            eligible = int(t_offset[s + 1]) - base
            entries, permuted = place_entries(rng, eligible=eligible, holds=series_holds)
            entry_slot = base + entries
            exit_slot = entry_slot + permuted
            spreads = np.asarray(t_half[entry_slot], dtype=np.float64)
            if zero_cost:
                # ⚠ Zeroed AFTER the lookup, not by skipping it, so the ablation
                # walks the identical code path and differs in the value alone.
                spreads = np.zeros_like(spreads)
            entry_net = net_entry_prices(np.asarray(t_open[entry_slot], dtype=np.float64), spreads)
            exit_net = net_exit_prices(np.asarray(t_open[exit_slot], dtype=np.float64), spreads)
            entry_panel = np.asarray(t_panel[entry_slot], dtype=np.int64)
            exit_panel = np.asarray(t_panel[exit_slot], dtype=np.int64)
            mark_base = int(close_offset[s]) - int(first_panel[s])
            for leg in range(series_holds.size):
                lo_mark = mark_base + int(entry_panel[leg])
                hi_mark = mark_base + int(exit_panel[leg]) + 1
                book.add(
                    entry_index=int(entry_panel[leg]),
                    exit_index=int(exit_panel[leg]),
                    entry_price=float(entry_net[leg]),
                    exit_price=float(exit_net[leg]),
                    half_spread=float(spreads[leg]),
                    realised=True,
                    marks=closes[lo_mark:hi_mark].tolist(),
                )
                returns.append(float((exit_net[leg] - entry_net[leg]) / entry_net[leg] * 100.0))
                entry_dates.append(axis[int(entry_panel[leg])])
        # R1 — equality, per member, on the whole book.
        if len(book) != expected_trades:
            problems.append(f"R1 member {index}: {len(book):,} legs against the strategy's {expected_trades:,}")
        curve = build_equity_curve(book, date_count=len(axis))
        metrics = compute_metrics(
            curve,
            dates=axis,
            trades=TradeReturns(
                net_return_pct=tuple(returns),
                entry_fill_date=tuple(entry_dates),
                open_count=0,
                unpriced_count=0,
            ),
            buy_and_hold=benchmark_curve,
            # ⚠ NO BOOTSTRAP. §9 reads the cohort's Sharpe and net return; the
            # criterion-3 correction is a property of the REAL sleeve's trade
            # population and running it 1,000 times would add hours to compute a
            # number no threshold consumes.
            bootstrap_seed=None,
        )
        results.append(
            MemberOutcome(
                index=index,
                sharpe=metrics.sharpe,
                total_return_pct=metrics.total_return_pct,
                exposure_time_pct=metrics.exposure_time_pct,
                turnover_annualised=metrics.turnover_annualised,
                trade_count=metrics.trade_count,
            )
        )
        print(
            f"  member {index:>4}  legs {len(book):>10,}  sharpe {metrics.sharpe:>9.4f}  "
            f"return {metrics.total_return_pct:>18,.2f}%  exposure {metrics.exposure_time_pct:>6.2f}%  "
            f"({time.monotonic() - started:.0f}s)",
            flush=True,
        )

    prefix = "ablation" if zero_cost else "members"
    shard = cache_root / f"{prefix}_{key}_{first:04d}_{last:04d}.npz"
    np.savez(
        shard,
        index=np.asarray([member.index for member in results], dtype=np.int64),
        sharpe=np.asarray([member.sharpe for member in results], dtype=np.float64),
        total_return_pct=np.asarray([member.total_return_pct for member in results], dtype=np.float64),
        exposure_time_pct=np.asarray([member.exposure_time_pct for member in results], dtype=np.float64),
        turnover_annualised=np.asarray([member.turnover_annualised for member in results], dtype=np.float64),
        trade_count=np.asarray([member.trade_count for member in results], dtype=np.int64),
    )
    print(f"\n  wrote {shard}", flush=True)
    print(f"  property violations: {len(problems)}")
    for problem in problems[:20]:
        print(f"    {problem}")
    print(f"\n  elapsed  {time.monotonic() - started:.1f}s", flush=True)
    return 1 if problems else 0


# ---------------------------------------------------------------------------
# --properties (R2/R3/R4/R7, on one member, exhaustively)
# ---------------------------------------------------------------------------


def properties(*, cache_root: Path, label: str, index: int) -> int:
    """R2/R3/R4/R7 on ONE member, over every series it trades.

    ⚠ SEPARATED FROM ``--cohort`` deliberately. These are properties of the
    PLACEMENT, which is a pure function of ``(seed, holds, eligible)`` — running
    them inside every one of 1,000 members would multiply the cohort's cost by
    the check and measure the same construction 1,000 times. Run over the whole
    corpus for one member, they are exhaustive in the dimension that varies
    (the series), and ``tests/test_random_entry_cohort.py`` covers the dimension
    that does not (the seed).
    """
    started = time.monotonic()
    cache = _Cache(_ensure_cache_root(cache_root, create=False))
    key = label.replace("-", "")
    t_panel = cache.load("t_panel")
    t_open = cache.load("t_open")
    t_half = cache.load("t_half")
    t_offset = np.asarray(cache.load("t_offset"), dtype=np.int64)
    t_start = np.asarray(cache.load(f"{key}_t_start"), dtype=np.int64)
    holds_all = cache.load(f"{key}_holds")
    holds_offset = np.asarray(cache.load(f"{key}_holds_offset"), dtype=np.int64)
    series_count = int(t_offset.size) - 1

    print(f"\n[properties] {label} member {index}   {series_count:,} series", flush=True)
    problems: list[str] = []
    checked = 0
    priced = 0
    rng = np.random.Generator(np.random.PCG64(member_seed(index)))
    for s in range(series_count):
        lo, hi = int(holds_offset[s]), int(holds_offset[s + 1])
        if lo == hi:
            continue
        series_holds = np.asarray(holds_all[lo:hi], dtype=np.int64)
        base = int(t_offset[s]) + int(t_start[s])
        eligible = int(t_offset[s + 1]) - base
        entries, permuted = place_entries(rng, eligible=eligible, holds=series_holds)
        checked += 1

        # R2 — the multiset, re-derived from the placement.
        if Counter(permuted.tolist()) != Counter(series_holds.tolist()):
            problems.append(f"R2 series {s}: the placed holds are not a permutation of the strategy's")
        # R3 — no overlap. §3.2 rule 4 permits TOUCHING (an entry on the ordinal
        # a previous position closed on), so the test is on the strict interior.
        order = np.argsort(entries, kind="stable")
        starts, spans = entries[order], permuted[order]
        if np.any(starts[1:] < (starts[:-1] + spans[:-1])):
            problems.append(f"R3 series {s}: two positions overlap inside one instrument")
        # R4 — inside the eligible space.
        if entries.size and (int(entries.min()) < 0 or int((entries + permuted).max()) >= eligible):
            problems.append(f"R4 series {s}: a leg falls outside the {eligible} eligible bars")

        # R7 — the vectorised cost arithmetic against the Decimal path.
        if priced < _REPRICE_SAMPLE:
            take = min(_REPRICE_SAMPLE - priced, entries.size)
            slots = base + entries[:take]
            opens = np.asarray(t_open[slots], dtype=np.float64)
            spreads = np.asarray(t_half[slots], dtype=np.float64)
            fast_entry = net_entry_prices(opens, spreads)
            fast_exit = net_exit_prices(opens, spreads)
            for leg in range(take):
                # ⚠ `float(...)` FIRST. `repr` of a numpy scalar is `np.float64(1.5)` on
                # numpy 2.x, which Decimal refuses — and the refusal is the good case;
                # a silently different string would re-price the whole sample.
                slow_entry, slow_exit = decimal_net_prices(
                    Decimal(repr(float(opens[leg]))), Decimal(repr(float(spreads[leg])))
                )
                if not math.isclose(float(fast_entry[leg]), float(slow_entry), rel_tol=1e-12):
                    problems.append(f"R7 series {s}: entry {fast_entry[leg]} against Decimal {slow_entry}")
                if not math.isclose(float(fast_exit[leg]), float(slow_exit), rel_tol=1e-12):
                    problems.append(f"R7 series {s}: exit {fast_exit[leg]} against Decimal {slow_exit}")
            priced += take
        if checked % 1000 == 0:
            print(f"  {checked} series checked, {len(problems)} problems ({time.monotonic() - started:.0f}s)")

    # ⚠ t_panel is loaded and asserted MONOTONIC per series: the cohort slices
    # marks by panel index, and a non-monotonic table would slice a leg's marks
    # from the wrong span while every other check still passed.
    for s in range(series_count):
        span = np.asarray(t_panel[int(t_offset[s]) : int(t_offset[s + 1])], dtype=np.int64)
        if span.size > 1 and np.any(np.diff(span) <= 0):
            problems.append(f"R4 series {s}: the eligible-bar panel indices are not strictly increasing")

    print(f"\n  series with trades   {checked:>12,}")
    print(f"  legs re-priced       {priced:>12,}   (sample; the band table is covered by the bridge test)")
    print(f"  property violations: {len(problems)}")
    for problem in problems[:20]:
        print(f"    {problem}")
    print(f"\n  elapsed  {time.monotonic() - started:.1f}s", flush=True)
    return 1 if problems else 0


# ---------------------------------------------------------------------------
# --report
# ---------------------------------------------------------------------------


def report(*, cache_root: Path) -> int:
    """Every shard, §9's two thresholds, and R5/R6."""
    cache_root = _ensure_cache_root(cache_root, create=False)
    cache = _Cache(cache_root)
    meta = cache.read_meta()
    real = meta["real"]
    assert isinstance(real, dict)
    problems: list[str] = []
    print(f"\n[report] cohort {meta['cohort_model_id']}   root seed {meta['cohort_root_seed']}", flush=True)
    print(f"         cost model {meta['cost_model_id']}   sizing {meta['sizing_rule']}", flush=True)

    for label in _LABELS:
        key = label.replace("-", "")
        members: list[MemberOutcome] = []
        for shard in sorted(cache_root.glob(f"members_{key}_*.npz")):
            with np.load(shard) as data:
                for row in range(data["index"].size):
                    members.append(
                        MemberOutcome(
                            index=int(data["index"][row]),
                            sharpe=float(data["sharpe"][row]),
                            total_return_pct=float(data["total_return_pct"][row]),
                            exposure_time_pct=float(data["exposure_time_pct"][row]),
                            turnover_annualised=float(data["turnover_annualised"][row]),
                            trade_count=int(data["trade_count"][row]),
                        )
                    )
        print(f"\n  ### {label} — §9's synthetic control", flush=True)
        if label not in real:
            problems.append(f"R6 {label}: no real metric set in the cache, so §9 cannot be evaluated")
            continue
        if not members:
            problems.append(f"R5 {label}: no cohort members were produced")
            continue
        # R5 — the cohort is the declared size and every index appears once.
        indices = sorted(member.index for member in members)
        if indices != list(range(SPEC_COHORT_SIZE)):
            problems.append(
                f"R5 {label}: the member indices are not exactly 0…{SPEC_COHORT_SIZE - 1} "
                f"({len(indices)} members, min {indices[0]}, max {indices[-1]})"
            )
        strategy = real[label]
        control = evaluate_control(
            tuple(members),
            strategy_sharpe=float(strategy["sharpe"]),
            strategy_return_pct=float(strategy["total_return_pct"]),
            root_seed=int(meta["cohort_root_seed"]),  # type: ignore[arg-type]
            percentile=SPEC_SHARPE_PERCENTILE,
        )
        residual = match_residual(
            tuple(members),
            strategy_trade_count=int(strategy["trade_count"]),
            strategy_exposure_time_pct=float(strategy["exposure_time_pct"]),
            strategy_turnover_annualised=float(strategy["turnover_annualised"]),
        )
        # R1 — the match is exact where the construction makes it exact.
        if not residual.trade_count_matches:
            problems.append(
                f"R1 {label}: the cohort's mean trade count {residual.cohort_mean_trade_count:,.4f} is not the "
                f"strategy's {residual.strategy_trade_count:,}"
            )
        print(f"      cohort size            {control.cohort_size:>16,}")
        print(
            f"      trades / member        {residual.cohort_mean_trade_count:>16,.1f}   strategy "
            f"{residual.strategy_trade_count:,}   ⚠ exact by construction"
        )
        print(
            f"      exposure %             {residual.cohort_mean_exposure_time_pct:>16.4f}   strategy "
            f"{residual.strategy_exposure_time_pct:.4f}   delta {residual.exposure_delta_pct_points:+.4f} pts"
        )
        print(
            f"      turnover /yr           {residual.cohort_mean_turnover_annualised:>16.4f}   strategy "
            f"{residual.strategy_turnover_annualised:.4f}   delta {residual.turnover_delta:+.4f}"
        )
        print()
        print(f"      cohort mean return     {control.mean_return_pct:>16,.4f}%")
        print(
            f"        95% bootstrap CI     [{control.mean_return_ci_low_pct:,.4f}%, "
            f"{control.mean_return_ci_high_pct:,.4f}%]"
        )
        print(f"        contains zero        {str(control.mean_return_ci_contains_zero):>16}   ← §9 threshold 1")
        print(f"      cohort sharpe p{control.sharpe_percentile:g}      {control.cohort_sharpe_threshold:>16.4f}")
        print(f"      strategy sharpe        {control.strategy_sharpe:>16.4f}")
        print(f"        exceeds cohort       {str(control.sharpe_exceeds_cohort):>16}   ← §9 threshold 2")
        print(f"      cohort return p{control.sharpe_percentile:g}      {control.cohort_return_threshold_pct:>16,.4f}%")
        print(f"      strategy return        {control.strategy_return_pct:>16,.4f}%")
        print(f"        exceeds cohort       {str(control.return_exceeds_cohort):>16}   ⚠ reported, does NOT gate")
        print(f"      §9 VERDICT             {str(control.passed):>16}")
        # ⚠⚠ THE EMPIRICAL p-VALUE, and it is reported because a p95 PASS/FAIL
        # throws away the resolution the 1,000 members bought. The conventional
        # Monte-Carlo form counts the observed value in its own null —
        # `(1 + #{null >= observed}) / (N + 1)` — so it can never be zero, and
        # its floor at N = 1,000 is 1/1001 = 0.000999. A run printing "0 of
        # 1,000 members reach it" without that floor invites "p = 0", which no
        # finite cohort can support. (Codex checkpoint 1 on §9.2.)
        sharpes = np.asarray([member.sharpe for member in members], dtype=np.float64)
        returns = np.asarray([member.total_return_pct for member in members], dtype=np.float64)
        at_or_above_sharpe = int((sharpes >= control.strategy_sharpe).sum())
        at_or_above_return = int((returns >= control.strategy_return_pct).sum())
        n = len(members)
        print(
            f"      members >= sharpe      {at_or_above_sharpe:>16,}   of {n:,}   "
            f"empirical p {(1 + at_or_above_sharpe) / (n + 1):.6f}   (floor {1 / (n + 1):.6f})"
        )
        print(
            f"      members >= return      {at_or_above_return:>16,}   of {n:,}   "
            f"empirical p {(1 + at_or_above_return) / (n + 1):.6f}"
        )
        # ⚠ The zero-cost ABLATION, if one was run. Diagnostic only: it answers
        # "is the cohort's mean the cost model or something else", and it is
        # never a §9 figure — a zero-cost backtest violates criterion 2 outright.
        ablation_returns: list[float] = []
        ablation_exposure: list[float] = []
        ablation_sharpe: list[float] = []
        for shard in sorted(cache_root.glob(f"ablation_{key}_*.npz")):
            with np.load(shard) as data:
                ablation_returns.extend(float(value) for value in data["total_return_pct"])
                ablation_exposure.extend(float(value) for value in data["exposure_time_pct"])
                ablation_sharpe.extend(float(value) for value in data["sharpe"])
        if ablation_returns:
            print(
                f"      [ablation h=0]  mean return {float(np.mean(ablation_returns)):>16,.4f}%   "
                f"mean sharpe {float(np.mean(ablation_sharpe)):.4f}   "
                f"mean exposure {float(np.mean(ablation_exposure)):.2f}%   over {len(ablation_returns):,} members"
            )
            # ⚠⚠ THE EXPOSURE LINE IS WHY THE ABLATION CARRIES MORE THAN THE
            # RETURN. The costed cohort's exposure sits far below the strategy's,
            # and the obvious reading — "the permutation failed to reproduce the
            # strategy's concurrency" — is testable: the SAME placements with the
            # half-spread zeroed hold their capital at work. If the ablation's
            # exposure is at or above the strategy's, the gap is the cohort's
            # RUIN (an equity path collapsing toward zero carries no capital),
            # not a mis-specified null.
            print(
                "      ⚠ DIAGNOSTIC ONLY — a zero-cost backtest violates criterion 2 outright and is never a §9 figure"
            )
        # R6 — the verdict is a RESULT. §10: "the most likely outcome of stage 5e
        # … is that some or all of them fail the random-cohort threshold. That is
        # a result, not a failure of the phase." So it prints and does not gate.

    print(f"\n  property violations: {len(problems)}")
    for problem in problems[:20]:
        print(f"    {problem}")
    return 1 if problems else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepare", action="store_true", help="one corpus sweep; writes the cohort cache")
    parser.add_argument("--cohort", action="store_true", help="run a slice of members off the cache")
    parser.add_argument(
        "--zero-cost",
        action="store_true",
        help="DIAGNOSTIC: rerun the same placement with the half-spread zeroed. Never a §9 figure.",
    )
    parser.add_argument("--properties", action="store_true", help="R2/R3/R4/R7 on one member, over every series")
    parser.add_argument("--report", action="store_true", help="aggregate the shards and evaluate §9")
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--strategy", choices=_LABELS, default="S-1")
    parser.add_argument("--members", type=str, default=f"0:{SPEC_COHORT_SIZE}", help="half-open FIRST:LAST")
    parser.add_argument("--member", type=int, default=0, help="which member --properties checks")
    parser.add_argument("--limit", type=int, default=None, help="first N series only — a smoke run, never a figure")
    args = parser.parse_args()
    if not (args.prepare or args.cohort or args.properties or args.report):
        parser.error("choose --prepare, --cohort, --properties or --report")

    status = 0
    if args.prepare:
        status |= prepare(cache_root=args.cache, limit=args.limit)
    if args.cohort:
        first, _, last = args.members.partition(":")
        status |= cohort(
            cache_root=args.cache,
            label=args.strategy,
            first=int(first),
            last=int(last),
            zero_cost=args.zero_cost,
        )
    if args.properties:
        status |= properties(cache_root=args.cache, label=args.strategy, index=args.member)
    if args.report:
        status |= report(cache_root=args.cache)
    print("\nPASS" if status == 0 else "\nFAIL")
    return status


if __name__ == "__main__":
    sys.exit(main())
