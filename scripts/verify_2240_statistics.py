"""Full-population verification of the phase-5d equity curve and metric set (#2240).

    PYTHONPATH=. uv run python scripts/verify_2240_statistics.py --all

⚠ NOTHING IS WRITTEN. Every arm reads and prints. Gate on the EXIT CODE — 0
means every arm passed, 1 means at least one did not.

⚠ NEVER PIPE THIS INTO ``head``/``tail``. A pipe returns the pipe's status, so a
failure reads as success, and it buffers the progress lines a long run is judged
by (`.claude/CLAUDE.md`). Redirect to a file and read the file.

THE ARMS
--------
``--panel`` — measure the evaluation axis and the panel's density. ⚠ This is the
arithmetic behind §4's REJECTED ``vectorbt`` adoption: a matrix simulator wants
a dense date x instrument panel, and this arm says how much of ours would be NaN
padding. It is reported rather than asserted, because the density is a property
of the corpus and moves with every harvest.

``--curve`` — build every S-1 and S-3 position over the §4.0 validated universe
through the real path (``s1_signals`` / ``s3_signals`` →
``signal_ledger.resolve_fills`` → ``build_positions`` → ``cost_positions``),
run the sleeve equity curve over the whole corpus, compute criterion 7's metric
set, and assert six properties.

  P1  **§2.1's EQUALITY, on the whole trade list.** Every leg's entry date
      EQUALS the position's ``entry_fill_bar_date``, every leg's entry price
      EQUALS the costed net entry, and ``fill_bar_date > signal_bar_date``. ⚠ An
      inequality ("no order lands on a signal date") is too weak — it passes for
      a simulator filling on the WRONG future bar, which is a different error
      with the same sign.
  P2  **No leverage.** ``equity - invested`` is cash, and it never goes negative
      on any date. This is what "sells before buys, buys capped by cash" buys,
      and a single-pass rebalance would fail it by exactly the cost charged.
  P3  **Equity never goes negative**, on any date.
  P4  **The metric set constructs.** ``StrategyMetrics.__post_init__`` is the
      assertion — drawdown non-positive, exposure inside 0-100, the two null
      denominators consistent with their counts.
  P5  **The open-position mark agrees with the series.** For every position left
      open at the window end, ``position_builder``'s ``mark_price`` equals the
      close of the bar this script independently located as the last usable one.
  P6  **Conservation.** Every costed position becomes exactly one leg or is
      counted in the excluded census; no position is silently dropped.

  ⚠ WHAT IS NOT COVERED, STATED SO THE GAP IS A DECISION. Only S-1 and S-3 run
  here, for phase 5a's reason: C2 (``level``) needs the resolver over the whole
  corpus and C4 (``calendar``) needs S-2's panel resident at once. So an
  ``ambiguous`` close — reachable only through an outcome row, and only for S-4
  (§3) — never appears in this arm, and §3.4's two ambiguity arms therefore
  cannot diverge here. ``tests/test_equity_curve.py`` covers the mechanics.
"""

from __future__ import annotations

import argparse
import sys
import time
from array import array
from collections import Counter
from datetime import date
from decimal import Decimal

import psycopg

from app.config import settings
from app.services.cost_model import CARRY_UNMODELLED, COST_MODEL_ID, half_spread_for
from app.services.equity_curve import SIZING_RULE_ID, EquityCurve, LegBook, build_equity_curve
from app.services.indicator_series import BarSeries
from app.services.position_builder import Window, build_positions
from app.services.position_costing import CostedPosition, cost_positions
from app.services.research_price_structure_store import load_masked_series
from app.services.signal_ledger import resolve_fills
from app.services.strategies.s1_time_series_momentum import S1_STRATEGY_ID, s1_identity, s1_signals
from app.services.strategies.s3_mean_reversion_in_trend import S3_STRATEGY_ID, s3_identity, s3_signals
from app.services.strategies.validated_universe import load_validated_universe
from app.services.strategy_result import EVALUATION_WINDOW_END, EVALUATION_WINDOW_START
from app.services.strategy_statistics import METRIC_SET_ID, StrategyMetrics, TradeReturns, compute_metrics

# ⚠ REUSED, not re-derived. Phase 5a built the corpus→positions path and 5b the
# costing on top of it; a second copy here would be a second place for the fill
# rule to drift. Only the curve layer is new.
from scripts.verify_2240_position_builder import (
    _S1_REGIME,
    _S3_REGIME,
    UNIVERSE,
    _fills,
    _stamped_versions,
    _to_series,
)

_AXIS_SQL = """
    SELECT DISTINCT d.bar_date
    FROM research_price_series s
    JOIN research_price_daily d ON d.series_id = s.series_id
    WHERE s.instrument_id = ANY(%(ids)s)
      AND d.bar_date BETWEEN %(start)s AND %(end)s
    ORDER BY 1
"""

_BAR_COUNT_SQL = """
    SELECT count(*)
    FROM research_price_series s
    JOIN research_price_daily d ON d.series_id = s.series_id
    WHERE s.instrument_id = ANY(%(ids)s)
      AND d.bar_date BETWEEN %(start)s AND %(end)s
"""


def _mark_index(series: BarSeries, *, window: Window, not_before: date) -> int | None:
    """Where ``position_builder._mark_price`` took its mark.

    ⚠ A SECOND IMPLEMENTATION OF THE SAME SCAN, and that is the point: P5
    asserts the price the builder returned equals the close at the bar this
    locator found. A shared helper would agree with itself.
    """
    for index in range(len(series) - 1, -1, -1):
        when = series.dates[index]
        if when > window.end:
            continue
        if when < not_before:
            return None
        if series.rows[index].get("close") is not None:
            return index
    return None


class _Sleeve:
    """One strategy's legs, trade returns and census, accumulated across the corpus."""

    def __init__(self, label: str) -> None:
        self.label = label
        self.book = LegBook()
        self.returns: array[float] = array("d")
        self.positions = 0
        self.open_at_end = 0
        self.excluded: Counter[str] = Counter()
        self.problems: list[str] = []

    def absorb(
        self,
        rows: list[CostedPosition],
        *,
        series: BarSeries,
        window: Window,
        axis_pos: dict[date, int],
        closes: list[float],
        first_axis_index: int,
    ) -> None:
        for row in rows:
            self.positions += 1
            position = row.position

            # P1 — §2.1's equality, on the whole trade list.
            if position.entry_fill_bar_date <= position.entry_signal_bar_date:
                self.problems.append(
                    f"{self.label}/{position.instrument_id}: P1 fill {position.entry_fill_bar_date} does not follow "
                    f"signal {position.entry_signal_bar_date}"
                )
                continue
            entry_index = axis_pos.get(position.entry_fill_bar_date)
            if entry_index is None:
                self.problems.append(
                    f"{self.label}/{position.instrument_id}: P1 entry fill {position.entry_fill_bar_date} is not on "
                    "the evaluation axis"
                )
                continue

            if row.uncosted_reason is not None:
                # §3.4 — excluded and COUNTED, never dropped silently. Neither
                # reachable state carries an exit price, so neither can be
                # placed on the curve.
                self.excluded[row.uncosted_reason] += 1
                continue

            assert row.exit_price_net is not None and row.gross_return_pct is not None
            assert row.net_return_pct is not None

            if position.close_bar_date is not None:
                exit_index = axis_pos.get(position.close_bar_date)
                realised = True
            else:
                self.open_at_end += 1
                realised = False
                located = _mark_index(series, window=window, not_before=position.entry_fill_bar_date)
                if located is None:
                    self.excluded["mark_bar_unlocatable"] += 1
                    continue
                # P5 — the builder's mark equals the close at the bar this
                # script located independently.
                mark_close = series.rows[located].get("close")
                if mark_close != position.mark_price:
                    self.problems.append(
                        f"{self.label}/{position.instrument_id}: P5 mark {position.mark_price} against the close "
                        f"{mark_close} at {series.dates[located]}"
                    )
                exit_index = axis_pos.get(series.dates[located])
            if exit_index is None:
                self.problems.append(
                    f"{self.label}/{position.instrument_id}: P1 close bar is not on the evaluation axis"
                )
                continue
            if exit_index < entry_index:
                self.problems.append(f"{self.label}/{position.instrument_id}: P1 close index precedes the entry index")
                continue

            span_from = entry_index - first_axis_index
            marks = closes[span_from : exit_index - first_axis_index + 1]
            self.book.add(
                entry_index=entry_index,
                exit_index=exit_index,
                entry_price=float(row.entry_price_net),
                exit_price=float(row.exit_price_net),
                half_spread=float(row.half_spread),
                realised=realised,
                marks=marks,
            )
            if realised:
                self.returns.append(float(row.net_return_pct))

    def report(self, *, axis: tuple[date, ...], benchmark_curve: EquityCurve) -> StrategyMetrics | None:
        started = time.monotonic()
        curve = build_equity_curve(self.book, date_count=len(axis))
        print(f"\n  [{self.label}]  curve built in {time.monotonic() - started:.1f}s", flush=True)
        print(f"      positions              {self.positions:>12,}")
        print(f"      legs on the curve      {len(self.book):>12,}")
        print(f"      realised closes        {len(self.returns):>12,}")
        print(f"      open at window end     {self.open_at_end:>12,}")
        for reason, count in self.excluded.most_common():
            print(f"        excluded {reason:<22} {count:>10,}")
        print(f"      event dates            {curve.event_dates:>12,}   of {len(axis):,} on the axis")
        print(f"      short-funded entries   {curve.short_funded_entries:>12,}")
        print(f"      stale marks (halts)    {curve.stale_marks:>12,}")
        # ⚠ Must equal `open at window end` minus the marks that could not be
        # located: an unrealised leg is FROZEN at its mark, never liquidated,
        # and a divergence here means the engine treated one as an exit.
        print(f"      frozen at their mark   {curve.unrealised_held:>12,}   (open, marked, unsellable)")
        print(f"      rebalance costs        {curve.rebalance_costs:>12.6f}   (pot = 1.0)")

        cash = curve.equity - curve.invested
        # P2 — no leverage. The tolerance is relative to the pot, not absolute:
        # float64 accumulation over 16k dates cannot be exact.
        floor = -1e-9 * float(max(curve.equity.max(), 1.0))
        if float(cash.min()) < floor:
            self.problems.append(f"{self.label}: P2 cash reached {float(cash.min())} — the rebalance borrowed")
        # P3 — equity never negative.
        if float(curve.equity.min()) < 0.0:
            self.problems.append(f"{self.label}: P3 equity reached {float(curve.equity.min())}")

        try:
            metrics = compute_metrics(
                curve,
                dates=axis,
                trades=TradeReturns(
                    net_return_pct=tuple(self.returns),
                    open_count=self.open_at_end,
                    unpriced_count=sum(self.excluded.values()),
                ),
                buy_and_hold=benchmark_curve,
            )
        except ValueError as exc:
            # P4 — the metric set constructs.
            self.problems.append(f"{self.label}: P4 metric set refused: {exc}")
            return None

        print(f"      periods per year       {metrics.periods_per_year:>12.2f}   (MEASURED, not 252)")
        print(f"      total return           {metrics.total_return_pct:>12,.2f}%")
        print(f"      buy & hold             {metrics.buy_and_hold_return_pct:>12,.2f}%")
        print(f"      vs buy & hold          {metrics.return_vs_buy_and_hold_pct:>12,.2f}%")
        print(f"      CAGR                   {metrics.cagr_pct:>12.3f}%")
        print(f"      annualised vol         {metrics.annualised_volatility_pct:>12.3f}%")
        print(f"      Sharpe                 {metrics.sharpe:>12.4f}")
        sortino = "None (no losing period)" if metrics.sortino is None else f"{metrics.sortino:.4f}"
        print(f"      Sortino                {sortino:>12}")
        print(f"      max drawdown           {metrics.max_drawdown_pct:>12.3f}%")
        print(f"      exposure time          {metrics.exposure_time_pct:>12.3f}%")
        print(f"      turnover / yr          {metrics.turnover_annualised:>12.3f}")
        print(f"      expectancy / trade     {metrics.expectancy_per_trade_pct:>12.4f}%")
        pf = "None (no losing trade)" if metrics.profit_factor is None else f"{metrics.profit_factor:.4f}"
        print(f"      profit factor          {pf:>12}")
        print(f"      trades / losers        {metrics.trade_count:>12,} / {metrics.losing_trade_count:,}")
        print(f"      effective sample size  {str(metrics.effective_sample_size):>12}   (criterion 3 — stage 5e)")
        return metrics


def _benchmark_leg(
    book: LegBook,
    *,
    series: BarSeries,
    axis_pos: dict[date, int],
    closes: list[float],
    first_axis_index: int,
) -> None:
    """One buy-and-hold leg: the instrument's first usable close to its last.

    ⚠⚠ THE BENCHMARK RUNS THROUGH THE SAME ENGINE, and criterion 7's twelfth
    metric is why. *"Return relative to buy-and-hold"* has no published
    definition on an unbalanced panel where instruments list and delist inside
    the window, so it is fixed by construction — and computing it with different
    machinery would attribute the machinery's difference to the strategy.

    ⚠ It is charged the SAME cost model: one round trip at the entry band's
    half-spread. A cost-free benchmark would make every strategy look worse by
    exactly the amount the cost model charges, which is a comparison of cost
    models rather than of strategies.
    """
    usable = [i for i in range(len(series)) if series.rows[i].get("close") is not None]
    if len(usable) < 2:
        return
    first, last = usable[0], usable[-1]
    entry_close = series.rows[first].get("close")
    exit_close = series.rows[last].get("close")
    if entry_close is None or exit_close is None or entry_close <= 0 or exit_close <= 0:
        return
    entry_index = axis_pos.get(series.dates[first])
    exit_index = axis_pos.get(series.dates[last])
    if entry_index is None or exit_index is None or exit_index <= entry_index:
        return
    half = half_spread_for(entry_close)
    one = Decimal(1)
    book.add(
        entry_index=entry_index,
        exit_index=exit_index,
        entry_price=float(entry_close * (one + half)),
        exit_price=float(exit_close * (one - half)),
        half_spread=float(half),
        realised=True,
        marks=closes[entry_index - first_axis_index : exit_index - first_axis_index + 1],
    )


def panel() -> int:
    """The axis and the panel density — §4's ``vectorbt`` arithmetic, measured."""
    started = time.monotonic()
    print("\n[panel] the evaluation axis, and how dense a matrix simulator would find it", flush=True)
    with psycopg.connect(settings.database_url) as conn:
        universe = load_validated_universe(conn)
        bounds = {"ids": list(universe), "start": EVALUATION_WINDOW_START, "end": EVALUATION_WINDOW_END}
        axis = [row[0] for row in conn.execute(_AXIS_SQL, bounds).fetchall()]
        bars = conn.execute(_BAR_COUNT_SQL, bounds).fetchone()
        series_count = conn.execute(
            "SELECT count(*) FROM research_price_series WHERE instrument_id = ANY(%(ids)s)",
            {"ids": list(universe)},
        ).fetchone()
    assert bars is not None and series_count is not None
    print(f"  validated universe (§4.0)  {len(universe):>12,} instruments")
    print(f"  research series in it      {series_count[0]:>12,}")
    print(f"  window                     {EVALUATION_WINDOW_START} … {EVALUATION_WINDOW_END}")
    print(f"  distinct trading dates     {len(axis):>12,}")
    print(f"  bars                       {bars[0]:>12,}")
    cells = series_count[0] * len(axis)
    density = 100.0 * bars[0] / cells if cells else 0.0
    print(f"  dense panel cells          {cells:>12,}")
    print(f"  density                    {density:>12.1f}%   NaN padding {100.0 - density:.1f}%")
    print(f"  one float64 matrix         {cells * 8 / 2**30:>12.2f} GiB")
    print(f"  elapsed  {time.monotonic() - started:.1f}s", flush=True)
    return 0


def curve(*, limit: int | None) -> int:
    started = time.monotonic()
    s1_version, s3_version, builder_version = _stamped_versions()
    print(f"\n[curve] {S1_STRATEGY_ID} {s1_version}", flush=True)
    print(f"        {S3_STRATEGY_ID} {s3_version}", flush=True)
    print(f"        builder {builder_version}", flush=True)
    print(f"        cost model {COST_MODEL_ID}   carry_unmodelled {CARRY_UNMODELLED}", flush=True)
    print(f"        sizing rule {SIZING_RULE_ID}   metric set {METRIC_SET_ID}", flush=True)
    window = Window(start=EVALUATION_WINDOW_START, end=EVALUATION_WINDOW_END)
    print(f"        window {window.start} … {window.end}", flush=True)

    with psycopg.connect(settings.database_url) as conn:
        universe = load_validated_universe(conn)
        bounds = {"ids": list(universe), "start": EVALUATION_WINDOW_START, "end": EVALUATION_WINDOW_END}
        axis = tuple(row[0] for row in conn.execute(_AXIS_SQL, bounds).fetchall())
        axis_pos = {when: index for index, when in enumerate(axis)}
        print(f"  evaluation axis  {len(axis):,} trading dates", flush=True)

        pairs = conn.execute(
            "SELECT instrument_id, series_id FROM research_price_series "
            "WHERE instrument_id = ANY(%(ids)s) ORDER BY instrument_id, series_id",
            {"ids": list(universe)},
        ).fetchall()
        instruments = {int(row[0]) for row in pairs}
        if len(instruments) != len(pairs):
            print(f"  *** {len(pairs) - len(instruments)} instruments carry more than one research series — refusing")
            return 1
        if limit is not None:
            pairs = pairs[:limit]
            print(f"  ⚠ LIMITED to the first {len(pairs)} series — NOT a full-population figure", flush=True)

        sleeves = {"S-1": _Sleeve("S-1"), "S-3": _Sleeve("S-3")}
        benchmark = LegBook()
        empty = 0

        for n, (instrument_id, series_id) in enumerate(pairs, start=1):
            masked = load_masked_series(conn, series_id)
            if not masked.bars:
                empty += 1
                continue
            series, _masked_opens = _to_series(masked.bars)
            # ⚠ One dense close array per INSTRUMENT, spanning its own first to
            # last axis index and `nan` in between. That is what makes a leg's
            # mark slice O(1) to cut, and it is ~25 M floats over the corpus
            # rather than the 85 M a full dense panel would need.
            indices = [axis_pos[when] for when in series.dates if when in axis_pos]
            if len(indices) < 2:
                empty += 1
                continue
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

            for label, identity, signals, regime, strategy_id, version in (
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
                    signals,
                    series=series,
                    identity=identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID),
                    instrument_id=int(instrument_id),
                )
                entries, exits = _fills(rows, int(instrument_id))
                built = build_positions(
                    strategy_id=strategy_id,
                    strategy_version=version,
                    entries=entries,
                    exits=exits,
                    outcomes=[],
                    outcome_pin=None,
                    series={int(instrument_id): series},
                    regime=regime,
                    window=window,
                )
                costed = list(cost_positions(built.positions))
                # P6 — conservation across the layer boundary.
                if len(costed) != len(built.positions):
                    sleeves[label].problems.append(
                        f"{label}/{instrument_id}: P6 {len(built.positions)} positions produced {len(costed)} "
                        "costed rows"
                    )
                sleeves[label].absorb(
                    costed,
                    series=series,
                    window=window,
                    axis_pos=axis_pos,
                    closes=closes,
                    first_axis_index=first_axis_index,
                )
            if n % 250 == 0:
                seen = sum(len(sleeve.problems) for sleeve in sleeves.values())
                print(f"  {n}/{len(pairs)} series, {seen} problems ({time.monotonic() - started:.0f}s)", flush=True)

    print(f"\n  series with usable bars  {len(pairs) - empty}   (fail-closed empties: {empty})", flush=True)
    print(f"  benchmark legs           {len(benchmark):,}   (one per instrument, own-history buy & hold)", flush=True)
    bench_started = time.monotonic()
    benchmark_curve = build_equity_curve(benchmark, date_count=len(axis))
    print(
        f"  benchmark curve built in {time.monotonic() - bench_started:.1f}s   "
        f"total return {(float(benchmark_curve.equity[-1]) - 1.0) * 100.0:,.2f}%",
        flush=True,
    )
    # ⚠⚠ THE BENCHMARK IS REBALANCED TOO, and this line is what stops that being
    # invisible. Its legs open and close at listing bounds, so on a 5,266-name
    # panel a listing event lands on most dates — which means the "buy and hold"
    # arm is an equal-weight portfolio re-equalised on nearly every date, not a
    # portfolio nobody touched. On a SURVIVOR-ONLY corpus that harvests the
    # cross-sectional dispersion of names that all survived, and it is the
    # single largest reason the relative-return metric is not yet a fair
    # comparison (#2284).
    print(
        f"  benchmark event dates    {benchmark_curve.event_dates:,}   of {len(axis):,} on the axis   "
        f"(rebalance costs {benchmark_curve.rebalance_costs:,.3f} on a 1.0 pot)",
        flush=True,
    )

    problems: list[str] = []
    for sleeve in sleeves.values():
        sleeve.report(axis=axis, benchmark_curve=benchmark_curve)
        problems.extend(sleeve.problems)

    print(f"\n  property violations: {len(problems)}")
    for problem in problems[:20]:
        print(f"    {problem}")
    if len(problems) > 20:
        print(f"    … and {len(problems) - 20} more")
    print(f"\n  elapsed  {time.monotonic() - started:.1f}s", flush=True)
    # ⚠ Re-checked AFTER the sweep as well as before — phase 5a's reason: a probe
    # harness that mutated and restored a source file mid-run would pass an
    # entry check alone.
    _stamped_versions()
    return 1 if problems else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--panel", action="store_true", help="axis size and dense-panel density")
    parser.add_argument("--curve", action="store_true", help="full-population equity curve + metrics; assert P1-P6")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--limit", type=int, default=None, help="first N series only — a smoke run, never a figure")
    args = parser.parse_args()
    if not (args.panel or args.curve or args.all):
        parser.error("pick at least one arm: --panel, --curve or --all")
    status = 0
    if args.panel or args.all:
        status |= panel()
    if args.curve or args.all:
        status |= curve(limit=args.limit)
    return status


if __name__ == "__main__":
    sys.exit(main())
