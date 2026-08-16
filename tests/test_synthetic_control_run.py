"""§9's synthetic control, driven end to end over a committed fixed panel (#2601).

⚠⚠ THE PANEL IS CONSTRUCTED, NOT SAMPLED, AND EVERY CONSTANT IS DERIVED. The
acceptance this file has to express is *"the same wiring produces no synthetic
refusal for a strategy with an edge and both halves of the failure for one
without"*, and that is only a test if the two arms differ in ONE thing. So the
panel is built from three declared quantities — a break-even drift, a spike
size, and a spike calendar — and the two arms differ only in WHICH bars the
strategy exits on. No random-number generator seeds the fixture; the only
randomness in the file is the cohort's own, which is the thing under test.

THE DRIFT IS SOLVED FOR, THEN CORRECTED BY A MEASUREMENT
--------------------------------------------------------
A cohort priced through the real cost model always loses money on a flat panel —
both fill sides are charged — so §9's first threshold (*"the mean net return of
the random cohort must lie within its own 95% bootstrap CI of zero"*) could
never pass and the "edge" arm would fail for a reason that has nothing to do
with the edge. The panel therefore drifts, and the drift has two parts:
``_FILL_BREAK_EVEN`` is arithmetic from the cost model, and
``_REBALANCE_OFFSET`` is the sizing rule's turnover drag, which has no closed
form and is frozen against a measurement recorded beside it. The property both
exist to produce is ASSERTED in ``TestBothDirections``, so a change to either
fails loudly rather than moving the null under the comparison.

⚠ THE SPIKES ARE PAIRED AND SIGNED so the null stays centred. A one-bar spike is
captured only by a leg that EXITS on it, and the panel carries one up spike and
one down spike per series at mirrored positions — so a random exit is as likely
to land on either and the cohort's mean is not pushed off zero by the very
feature the edge arm exploits. A single-signed spike would have moved the null
along with the strategy and the first threshold would fail in BOTH arms.
"""

from __future__ import annotations

import inspect
import re
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import numpy as np
import pytest

from app.services import synthetic_control_run
from app.services.cost_model import UNKNOWN_NOMINAL_PRICE_BAND
from app.services.equity_curve import LegBook, build_equity_curve
from app.services.indicator_series import BarSeries
from app.services.position_builder import Position, Window
from app.services.position_costing import CostedPosition, cost_positions
from app.services.random_entry_cohort import COHORT_ROOT_SEED, member_seed, place_entries
from app.services.signal_ledger import LedgerRow
from app.services.strategy_result import EVALUATION_WINDOW_START, HOLDOUT_BOUNDARY
from app.services.strategy_statistics import DatedEquityCurve, StrategyMetrics, TradeReturns, compute_metrics
from app.services.synthetic_control_run import (
    CONTROL_NAMESPACE,
    PLACEMENT_SPACE_ID,
    CohortCollector,
    ScaleBudgetExceeded,
    SyntheticControlScaleBudget,
    run_cohort,
)
from app.services.technical_analysis import OHLCVRow

_HALF_SPREAD = float(UNKNOWN_NOMINAL_PRICE_BAND.half_spread)

#: Bars per series, and how many series the panel carries. Small enough that a
#: cohort runs in seconds, large enough that ``compute_metrics``' annualisation
#: and the block structure of the equity curve are exercised on real spans.
_BARS = 260
_SERIES = 6

#: The hold every position takes. ⚠ ONE VALUE, deliberately: the break-even
#: drift below is exact only for the hold it was solved for, and a mixed hold
#: multiset would leave the null's mean off zero by an amount nobody declared.
_HOLD = 10

#: The drift that returns a ``_HOLD``-bar leg to zero after BOTH FILL SIDES.
#: Arithmetic from ``UNKNOWN_NOMINAL_PRICE_BAND``, so it moves by itself if the
#: cost model is recalibrated.
_FILL_BREAK_EVEN = ((1.0 + _HALF_SPREAD) / (1.0 - _HALF_SPREAD)) ** (1.0 / _HOLD)

#: ⚠⚠ THE FILL SIDES ARE NOT THE WHOLE COST, and this constant is what that
#: cost us. ``build_equity_curve``'s sizing rule (``equal_weight_concurrent_v1``)
#: re-imposes equal weight on every EVENT DATE and charges the half-spread on
#: the turnover it creates, so a panel drifting at ``_FILL_BREAK_EVEN`` alone
#: still returns a measured **-10.30%** mean over the cohort — the same
#: mechanism that put the real corpus's null at -99.59% at stage 5e-5b.
#:
#: There is no closed form for it: the drag depends on how many legs happen to
#: be open beside each other, which is a property of the placement. So it is
#: FIXED BY CONSTRUCTION and frozen here, and the property it was chosen for is
#: asserted rather than assumed — ``test_the_null_is_centred_so_the_first_
#: threshold_can_pass`` fails if the null stops straddling zero. Measured on
#: this panel at 200 members: 0.00055 → CI [-0.944, +0.299]; 0.0006 → [-0.047,
#: +1.207]; 0.00065 → [+0.859, +2.129], which no longer contains zero.
_REBALANCE_OFFSET = 0.00055

_DRIFT = _FILL_BREAK_EVEN * (1.0 + _REBALANCE_OFFSET)

#: A one-bar spike, applied to the bar's price and removed on the next bar. Only
#: a leg exiting ON the spike bar captures it.
_SPIKE = 1.08

_START = date(2015, 1, 5)


def _axis() -> tuple[date, ...]:
    """``_BARS`` consecutive weekdays, entirely inside the in-sample side.

    ⚠ ASSERTED AGAINST THE FROZEN BOUNDARY rather than assumed. The control is
    an in-sample construction (``CONTROL_NAMESPACE``), so a panel that strayed
    past ``HOLDOUT_BOUNDARY`` would silently produce an empty placement space
    and the failure would read as "no series carries a placeable position".
    """
    days: list[date] = []
    cursor = _START
    while len(days) < _BARS:
        if cursor.weekday() < 5:
            days.append(cursor)
        cursor += timedelta(days=1)
    assert days[0] >= EVALUATION_WINDOW_START
    assert days[-1] < HOLDOUT_BOUNDARY
    return tuple(days)


AXIS = _axis()
AXIS_POS = {when: index for index, when in enumerate(AXIS)}
WINDOW = Window(start=AXIS[0], end=AXIS[-1])


def _spikes(series_index: int) -> dict[int, float]:
    """This series' signed one-bar spikes, mirrored about the panel's midpoint."""
    up = 40 + series_index * 7
    down = _BARS - 1 - up
    return {up: _SPIKE, down: 1.0 / _SPIKE}


def _prices(series_index: int) -> list[float]:
    base = 50.0 + series_index
    spikes = _spikes(series_index)
    return [base * _DRIFT**bar * spikes.get(bar, 1.0) for bar in range(_BARS)]


def _series(series_index: int) -> BarSeries:
    """One instrument. ⚠ ``open == close`` on every bar, and that is what makes
    the arithmetic checkable by hand: a leg fills at an open and is marked at a
    close, so equal ones mean a leg's net return is exactly the price ratio
    times the two-sided spread and nothing else."""
    prices = _prices(series_index)
    rows: tuple[OHLCVRow, ...] = tuple(
        OHLCVRow(
            open=Decimal(repr(price)),
            high=Decimal(repr(price * 1.001)),
            low=Decimal(repr(price * 0.999)),
            close=Decimal(repr(price)),
            volume=1000,
        )
        for price in prices
    )
    return BarSeries(dates=AXIS, rows=rows)


def _rows(series: BarSeries, *, warmup: int) -> list[LedgerRow]:
    """One ENTRY and one EXIT verdict per bar, cold entries ``insufficient_warmup``.

    ⚠ THIS IS THE PLACEMENT SPACE'S ONLY SOURCE. ``synthetic_control_run`` reads
    evaluability off these rows rather than off a declared ``WARMUP_BARS``, so a
    test that skipped the cold rows would be handing it a space no strategy
    stream produces.

    ⚠⚠ THE EXIT LEG IS WARM THROUGHOUT, DELIBERATELY. S-1 and S-3 emit both legs
    on every bar, and a fixture whose two legs went cold together could not tell
    an entry-keyed placement space from a both-legs one — the widening would be
    invisible. Here the exit leg is evaluable from bar 0, so a space that
    counted exits would start ``warmup`` bars too early and trade where the
    strategy could not open.
    """
    rows: list[LedgerRow] = []
    for index, when in enumerate(series.dates):
        cold = index < warmup
        rows.append(
            LedgerRow(
                strategy_id="fixture",
                strategy_version="v1",
                instrument_id=1,
                signal_bar_date=when,
                signal_kind="entry",
                verdict="not_evaluable" if cold else "not_fired",
                universe="survivor_only",
                input_rule_set_versions={"fixture": "v1"},
                not_evaluable_reason="insufficient_warmup" if cold else None,
            )
        )
        rows.append(
            LedgerRow(
                strategy_id="fixture",
                strategy_version="v1",
                instrument_id=1,
                signal_bar_date=when,
                signal_kind="exit",
                verdict="not_fired",
                universe="survivor_only",
                input_rule_set_versions={"fixture": "v1"},
            )
        )
    return rows


def _positions(series: BarSeries, *, series_index: int, entry_bars: list[int]) -> list[CostedPosition]:
    """Realised positions of fixed hold, costed through the shipped model."""
    built = [
        Position(
            strategy_id="fixture",
            strategy_version="v1",
            instrument_id=series_index + 1,
            entry_signal_id=series_index * 1000 + n,
            entry_signal_bar_date=series.dates[entry_bar - 1],
            entry_fill_bar_date=series.dates[entry_bar],
            entry_fill_price=Decimal(series.rows[entry_bar]["open"]),  # type: ignore[arg-type]
            close_source="signal_pair",
            close_bar_date=series.dates[entry_bar + _HOLD],
            close_price=Decimal(series.rows[entry_bar + _HOLD]["open"]),  # type: ignore[arg-type]
            bars_held=_HOLD,
            open_reason=None,
            mark_price=None,
        )
        for n, entry_bar in enumerate(entry_bars)
    ]
    return list(cost_positions(built, price_basis="split_adjusted"))


def _entry_bars(series_index: int, *, exits_on_spike: bool) -> list[int]:
    """Where the fixture strategy opens, in bar indices.

    The two arms differ HERE and nowhere else: the edge arm times one position
    so that it EXITS on the series' up spike; the no-edge arm shifts that one
    position two bars later, so it exits on an ordinary bar. Everything else —
    the count, the hold, the panel, the cost model — is identical.
    """
    up = 40 + series_index * 7
    timed = up - _HOLD if exits_on_spike else up - _HOLD + 2
    others = [_BARS - 60 + n * (_HOLD + 3) for n in range(4)]
    return sorted([timed, *[bar for bar in others if bar + _HOLD < _BARS]])


def _collector(*, exits_on_spike: bool) -> CohortCollector:
    collector = CohortCollector(window=WINDOW)
    for series_index in range(_SERIES):
        series = _series(series_index)
        prices = _prices(series_index)
        collector.collect(
            rows=_rows(series, warmup=20),
            series=series,
            costed=_positions(
                series,
                series_index=series_index,
                entry_bars=_entry_bars(series_index, exits_on_spike=exits_on_spike),
            ),
            axis_pos=AXIS_POS,
            raw_closes=prices,
            wealth_closes=prices,
            first_axis_index=0,
        )
    return collector


def _sleeve_metrics(*, exits_on_spike: bool) -> StrategyMetrics:
    """The fixture strategy's own criterion-7 metrics, through the real engine.

    ⚠ THE SAME ``build_equity_curve`` / ``compute_metrics`` THE COHORT USES. §9's
    comparison is only meaningful while both sides share the engine; a
    hand-computed Sharpe on one side would measure the hand-computation.
    """
    book = LegBook()
    returns: list[float] = []
    entry_dates: list[date] = []
    exit_dates: list[date] = []
    for series_index in range(_SERIES):
        series = _series(series_index)
        prices = _prices(series_index)
        for row in _positions(
            series,
            series_index=series_index,
            entry_bars=_entry_bars(series_index, exits_on_spike=exits_on_spike),
        ):
            assert row.exit_price_net is not None
            entry_bar = AXIS_POS[row.position.entry_fill_bar_date]
            assert row.position.close_bar_date is not None
            exit_bar = AXIS_POS[row.position.close_bar_date]
            book.add(
                entry_index=entry_bar,
                exit_index=exit_bar,
                entry_price=float(row.entry_price_net),
                exit_price=float(row.exit_price_net),
                half_spread=float(row.half_spread),
                realised=True,
                marks=prices[entry_bar : exit_bar + 1],
            )
            returns.append(float(row.net_return_pct or 0.0))
            entry_dates.append(row.position.entry_fill_bar_date)
            assert row.position.close_bar_date is not None
            exit_dates.append(row.position.close_bar_date)
    low, high = min(book.entry_index), max(book.exit_index)
    dates = AXIS[low : high + 1]
    return compute_metrics(
        DatedEquityCurve(dates=dates, curve=build_equity_curve(book.rebased(low), date_count=len(dates))),
        trades=TradeReturns(
            net_return_pct=tuple(returns),
            entry_fill_date=tuple(entry_dates),
            exit_bar_date=tuple(exit_dates),
            open_count=0,
            unpriced_count=0,
        ),
        buy_and_hold=None,
        bootstrap_seed=None,
    )


#: A cohort large enough for a stable 95th percentile and small enough for the
#: fast tier. ⚠ NOT ``SPEC_COHORT_SIZE``: ``run_backtest`` pins that literal and
#: refuses to take a size from its caller, which is exactly why ``run_cohort``
#: takes one — so a test can run the machinery without minting a second
#: production cohort size.
_TEST_COHORT = 200


class TestPlacementSpace:
    """What counts as a bar this strategy could have opened on."""

    def test_the_cold_prefix_is_excluded_because_the_rows_say_it_is(self) -> None:
        """⚠ MEASURED FROM THE VERDICTS, not from a declared warm-up. A member
        placing an entry inside the warm-up would be trading where the real
        strategy structurally could not."""
        collector = CohortCollector(window=WINDOW)
        series = _series(0)
        collector.collect(
            rows=_rows(series, warmup=20),
            series=series,
            costed=_positions(series, series_index=0, entry_bars=_entry_bars(0, exits_on_spike=True)),
            axis_pos=AXIS_POS,
            raw_closes=_prices(0),
            wealth_closes=_prices(0),
            first_axis_index=0,
        )
        placement = collector.placements[0]
        # The first eligible FILL bar is the bar after the first evaluable
        # SIGNAL bar — `resolve_fills`' own `signal_index + 1`.
        assert int(placement.panel[0]) == 21
        # The last signal bar has no `t+1`, so `no_fill_bar` removes it.
        assert int(placement.panel[-1]) == _BARS - 1
        assert placement.panel.size == _BARS - 21

    def test_a_wider_warm_up_narrows_the_space_it_is_read_from(self) -> None:
        spaces = []
        for warmup in (20, 60):
            collector = CohortCollector(window=WINDOW)
            series = _series(0)
            collector.collect(
                rows=_rows(series, warmup=warmup),
                series=series,
                costed=_positions(series, series_index=0, entry_bars=_entry_bars(0, exits_on_spike=True)),
                axis_pos=AXIS_POS,
                raw_closes=_prices(0),
                wealth_closes=_prices(0),
                first_axis_index=0,
            )
            spaces.append(int(collector.placements[0].panel.size))
        assert spaces[0] - spaces[1] == 40

    def test_the_placement_price_is_carried_onto_the_total_return_basis(self) -> None:
        """⚠⚠ THE SAME CARRY ``_absorb`` APPLIES TO A REAL LEG. The sleeve's
        legs are priced ``net * wealth_close / raw_close``; a cohort priced off
        the raw open would compare a total-return strategy with a
        price-return null and attribute the dividend stream to the edge."""
        collector = CohortCollector(window=WINDOW)
        series = _series(0)
        raw = _prices(0)
        wealth = [price * 1.25 for price in raw]
        collector.collect(
            rows=_rows(series, warmup=20),
            series=series,
            costed=_positions(series, series_index=0, entry_bars=_entry_bars(0, exits_on_spike=True)),
            axis_pos=AXIS_POS,
            raw_closes=raw,
            wealth_closes=wealth,
            first_axis_index=0,
        )
        placement = collector.placements[0]
        assert float(placement.adjusted_open[0]) == pytest.approx(raw[21] * 1.25)

    def test_a_bar_whose_close_is_missing_is_not_placeable(self) -> None:
        """A leg cannot be marked from a bar with no close, so it cannot open
        there either — ``_absorb``'s ``total_return_price_missing``, applied to
        the endpoint a permuted leg would be priced at."""
        collector = CohortCollector(window=WINDOW)
        series = _series(0)
        raw = _prices(0)
        holed = list(raw)
        holed[21] = float("nan")
        collector.collect(
            rows=_rows(series, warmup=20),
            series=series,
            costed=_positions(series, series_index=0, entry_bars=_entry_bars(0, exits_on_spike=True)),
            axis_pos=AXIS_POS,
            raw_closes=holed,
            wealth_closes=holed,
            first_axis_index=0,
        )
        assert 21 not in collector.placements[0].panel.tolist()
        assert int(collector.placements[0].panel[0]) == 22

    def test_a_hold_out_bar_is_never_placed_into_the_in_sample_null(self) -> None:
        """⚠⚠ THE ONE THAT MATTERS FOR CRITERION 5. A panel straddling the
        frozen boundary must contribute only its in-sample bars; a member
        entering after ``HOLDOUT_BOUNDARY`` would be an in-sample control that
        traded withheld prices."""
        straddling = tuple(HOLDOUT_BOUNDARY + timedelta(days=offset - 40) for offset in range(80) if True)
        dates = tuple(when for when in straddling if when.weekday() < 5)
        prices = [50.0 * _DRIFT**bar for bar in range(len(dates))]
        rows: tuple[OHLCVRow, ...] = tuple(
            OHLCVRow(
                open=Decimal(repr(price)),
                high=Decimal(repr(price)),
                low=Decimal(repr(price)),
                close=Decimal(repr(price)),
                volume=1000,
            )
            for price in prices
        )
        series = BarSeries(dates=dates, rows=rows)
        collector = CohortCollector(window=Window(start=dates[0], end=dates[-1]))
        collector.collect(
            rows=_rows(series, warmup=0),
            series=series,
            costed=[],
            axis_pos={when: index for index, when in enumerate(dates)},
            raw_closes=prices,
            wealth_closes=prices,
            first_axis_index=0,
        )
        # No position, so no placement is retained — but the space that WOULD
        # have been built is asserted through the collector's own filter by
        # rebuilding it with one in-sample position below.
        assert collector.placements == []
        with_position = CohortCollector(window=Window(start=dates[0], end=dates[-1]))
        entry_bar = 5
        held = [
            Position(
                strategy_id="fixture",
                strategy_version="v1",
                instrument_id=1,
                entry_signal_id=1,
                entry_signal_bar_date=dates[entry_bar - 1],
                entry_fill_bar_date=dates[entry_bar],
                entry_fill_price=Decimal(rows[entry_bar]["open"]),
                close_source="signal_pair",
                close_bar_date=dates[entry_bar + 3],
                close_price=Decimal(rows[entry_bar + 3]["open"]),
                bars_held=3,
                open_reason=None,
                mark_price=None,
            )
        ]
        with_position.collect(
            rows=_rows(series, warmup=0),
            series=series,
            costed=list(cost_positions(held, price_basis="split_adjusted")),
            axis_pos={when: index for index, when in enumerate(dates)},
            raw_closes=prices,
            wealth_closes=prices,
            first_axis_index=0,
        )
        placed = with_position.placements[0]
        assert all(dates[int(slot)] < HOLDOUT_BOUNDARY for slot in placed.panel)
        assert any(when >= HOLDOUT_BOUNDARY for when in dates), "the panel must straddle the boundary to test this"

    def test_an_out_of_window_bar_is_not_placeable(self) -> None:
        collector = CohortCollector(window=Window(start=AXIS[0], end=AXIS[100]))
        series = _series(0)
        collector.collect(
            rows=_rows(series, warmup=20),
            series=series,
            costed=_positions(series, series_index=0, entry_bars=_entry_bars(0, exits_on_spike=True)),
            axis_pos=AXIS_POS,
            raw_closes=_prices(0),
            wealth_closes=_prices(0),
            first_axis_index=0,
        )
        assert int(collector.placements[0].panel[-1]) == 100


class TestTheMatchIsExact:
    """R1/R2 of stage 5e-5b, asserted through the orchestration this time."""

    def test_every_member_trades_the_strategys_own_position_count(self) -> None:
        collector = _collector(exits_on_spike=True)
        result = run_cohort(
            collector,
            axis=AXIS,
            strategy_metrics=_sleeve_metrics(exits_on_spike=True),
            benchmark=None,
            cohort_size=_TEST_COHORT,
        )
        assert result.residual.trade_count_matches
        assert result.residual.strategy_trade_count == collector.matchable_trade_count
        assert result.control.cohort_size == _TEST_COHORT
        assert result.placement_space_id == PLACEMENT_SPACE_ID

    def test_every_member_is_measured_on_the_complete_fixed_axis(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Random placement may move a member's legs, never its metric dates."""
        measured_axes: list[tuple[date, ...]] = []
        real_compute_metrics = synthetic_control_run.compute_metrics

        def capture_axis(curve: DatedEquityCurve, **kwargs: object) -> StrategyMetrics:
            measured_axes.append(curve.dates)
            return real_compute_metrics(curve, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(synthetic_control_run, "compute_metrics", capture_axis)
        run_cohort(
            _collector(exits_on_spike=True),
            axis=AXIS,
            strategy_metrics=_sleeve_metrics(exits_on_spike=True),
            benchmark=None,
            cohort_size=2,
        )

        assert measured_axes == [AXIS, AXIS]

    def test_progress_reports_each_completed_member_without_metrics(self) -> None:
        events: list[tuple[int, int]] = []
        run_cohort(
            _collector(exits_on_spike=True),
            axis=AXIS,
            strategy_metrics=_sleeve_metrics(exits_on_spike=True),
            benchmark=None,
            cohort_size=_TEST_COHORT,
            progress=lambda completed, total: events.append((completed, total)),
        )
        assert events == [(completed, _TEST_COHORT) for completed in range(1, _TEST_COHORT + 1)]

    def test_the_holding_period_multiset_survives_the_permutation(self) -> None:
        """⚠ RE-DERIVED FROM THE PLACED LEGS, never read back off the input."""
        collector = _collector(exits_on_spike=True)
        rng_holds: list[int] = []
        for placement in collector.placements:
            rng = __import__("numpy").random.Generator(__import__("numpy").random.PCG64(member_seed(0)))
            entries, permuted = place_entries(rng, eligible=int(placement.panel.size), holds=placement.holds)
            assert sorted(permuted.tolist()) == sorted(placement.holds.tolist())
            # Non-overlap inside one instrument — §3.1's pyramiding rule.
            closes = entries + permuted
            order = entries.argsort()
            assert all(int(closes[order[n]]) <= int(entries[order[n + 1]]) for n in range(int(entries.size) - 1))
            rng_holds.extend(permuted.tolist())
        assert sorted(rng_holds) == sorted(
            hold for placement in collector.placements for hold in placement.holds.tolist()
        )

    def test_the_seed_is_the_declared_root_and_the_run_reproduces(self) -> None:
        first = run_cohort(
            _collector(exits_on_spike=True),
            axis=AXIS,
            strategy_metrics=_sleeve_metrics(exits_on_spike=True),
            benchmark=None,
            cohort_size=_TEST_COHORT,
        )
        second = run_cohort(
            _collector(exits_on_spike=True),
            axis=AXIS,
            strategy_metrics=_sleeve_metrics(exits_on_spike=True),
            benchmark=None,
            cohort_size=_TEST_COHORT,
        )
        assert first.control.root_seed == COHORT_ROOT_SEED
        assert first.control == second.control

    def test_spawned_workers_are_byte_for_byte_equivalent_to_serial_members(self) -> None:
        """Execution order and process boundaries cannot move a draw or float."""
        collector = _collector(exits_on_spike=True)
        metrics = _sleeve_metrics(exits_on_spike=True)
        serial = run_cohort(
            collector,
            axis=AXIS,
            strategy_metrics=metrics,
            benchmark=None,
            cohort_size=12,
            max_workers=1,
        )
        progress: list[tuple[int, int]] = []
        spawned = run_cohort(
            collector,
            axis=AXIS,
            strategy_metrics=metrics,
            benchmark=None,
            cohort_size=12,
            max_workers=2,
            progress=lambda completed, total: progress.append((completed, total)),
        )
        assert spawned.control == serial.control
        assert spawned.residual == serial.residual
        assert progress == [(completed, 12) for completed in range(1, 13)]

    def test_compact_shared_marks_are_exactly_equivalent_to_the_reference_book(self) -> None:
        """The optimized layout may remove copies, never change a draw or curve."""
        collector = _collector(exits_on_spike=True)
        reference_rng = np.random.Generator(np.random.PCG64(member_seed(7)))
        compact_rng = np.random.Generator(np.random.PCG64(member_seed(7)))
        reference, reference_returns, reference_entries, reference_exits = synthetic_control_run._place_member(  # noqa: SLF001
            reference_rng,
            collector.placements,
            axis=AXIS,
        )
        compact, compact_returns, compact_entries, compact_exits = (  # noqa: SLF001
            synthetic_control_run._place_member_compact(
                compact_rng,
                collector.placements,
                axis=AXIS,
            )
        )

        np.testing.assert_array_equal(compact.entry_index, reference.entry_index)
        np.testing.assert_array_equal(compact.exit_index, reference.exit_index)
        np.testing.assert_array_equal(compact.entry_price, reference.entry_price)
        np.testing.assert_array_equal(compact.exit_price, reference.exit_price)
        assert compact_returns == reference_returns
        assert compact_entries == reference_entries
        assert compact_exits == reference_exits

        reference_curve = build_equity_curve(reference, date_count=len(AXIS))
        compact_curve = build_equity_curve(compact, date_count=len(AXIS))
        assert compact_curve.rebalance_costs == reference_curve.rebalance_costs
        assert compact_curve.event_dates == reference_curve.event_dates
        assert compact_curve.short_funded_entries == reference_curve.short_funded_entries
        assert compact_curve.stale_marks == reference_curve.stale_marks
        assert compact_curve.unrealised_held == reference_curve.unrealised_held
        np.testing.assert_array_equal(compact_curve.equity, reference_curve.equity)
        np.testing.assert_array_equal(compact_curve.invested, reference_curve.invested)
        np.testing.assert_array_equal(compact_curve.open_count, reference_curve.open_count)
        np.testing.assert_array_equal(compact_curve.traded_notional, reference_curve.traded_notional)

    def test_the_run_offers_no_seed_override(self) -> None:
        """⚠⚠ THE OVERRIDE STAYED DELETED. An earlier draft took a ``root_seed``,
        recorded it on the row and still drew every member from
        ``member_seed(index)`` — which keys on ``COHORT_ROOT_SEED`` alone — so
        the stored seed described the bootstrap and not the cohort beside it
        (Codex checkpoint 2; prevention log). ⚠ The assertion above passes on
        that defect, because the default IS the constant; only the absent
        parameter rules it out."""
        assert "root_seed" not in inspect.signature(run_cohort).parameters


class TestBothDirections:
    """The acceptance: one wiring, two arms, exactly the refusals each earns."""

    @pytest.fixture(scope="class")
    def edge(self) -> object:
        return run_cohort(
            _collector(exits_on_spike=True),
            axis=AXIS,
            strategy_metrics=_sleeve_metrics(exits_on_spike=True),
            benchmark=None,
            cohort_size=_TEST_COHORT,
        )

    @pytest.fixture(scope="class")
    def flat(self) -> object:
        return run_cohort(
            _collector(exits_on_spike=False),
            axis=AXIS,
            strategy_metrics=_sleeve_metrics(exits_on_spike=False),
            benchmark=None,
            cohort_size=_TEST_COHORT,
        )

    def test_the_null_is_centred_so_the_first_threshold_can_pass(self, edge) -> None:  # type: ignore[no-untyped-def]
        """⚠ THE PANEL'S PROPERTY, ASSERTED. If the paired spikes or the solved
        drift stopped centring the cohort, BOTH arms would fail on
        ``cohort_shows_edge`` and the test below would pass for the wrong
        reason."""
        assert edge.control.mean_return_ci_contains_zero

    def test_a_strategy_with_an_edge_earns_no_synthetic_refusal(self, edge) -> None:  # type: ignore[no-untyped-def]
        assert edge.control.sharpe_exceeds_cohort
        assert edge.control.passed

    def test_the_same_wiring_refuses_a_strategy_without_one(self, flat) -> None:  # type: ignore[no-untyped-def]
        assert flat.control.mean_return_ci_contains_zero, "the null must stay centred in BOTH arms"
        assert not flat.control.sharpe_exceeds_cohort
        assert not flat.control.passed

    def test_the_two_arms_share_a_null(self, edge, flat) -> None:  # type: ignore[no-untyped-def]
        """⚠ THE POINT OF THE CONSTRUCTION. The arms differ in the strategy's
        exit bars and in nothing else, so the cohort's own threshold must be
        near-identical — if it moved, the comparison would be measuring the
        panel rather than the edge."""
        assert edge.control.cohort_sharpe_threshold == flat.control.cohort_sharpe_threshold
        assert edge.control.mean_return_pct == flat.control.mean_return_pct
        assert edge.control.strategy_sharpe > flat.control.strategy_sharpe


class TestRefusals:
    """What the cohort will not do quietly."""

    def test_an_empty_placement_space_raises_rather_than_passing(self) -> None:
        with pytest.raises(ValueError, match="empty placement space"):
            run_cohort(
                CohortCollector(window=WINDOW),
                axis=AXIS,
                strategy_metrics=_sleeve_metrics(exits_on_spike=True),
                benchmark=None,
                cohort_size=_TEST_COHORT,
            )

    def test_a_hold_out_position_is_not_placed_into_the_in_sample_null(self) -> None:
        """``CONTROL_NAMESPACE`` is the only namespace a control is built for."""
        assert CONTROL_NAMESPACE == "in_sample"
        collector = CohortCollector(window=WINDOW)
        series = _series(0)
        prices = _prices(0)
        held_open = [
            Position(
                strategy_id="fixture",
                strategy_version="v1",
                instrument_id=1,
                entry_signal_id=1,
                entry_signal_bar_date=series.dates[29],
                entry_fill_bar_date=series.dates[30],
                entry_fill_price=Decimal(series.rows[30]["open"]),  # type: ignore[arg-type]
                close_source=None,
                close_bar_date=None,
                close_price=None,
                bars_held=None,
                open_reason="window_end",
                mark_price=Decimal(series.rows[-1]["close"]),  # type: ignore[arg-type]
            )
        ]
        collector.collect(
            rows=_rows(series, warmup=20),
            series=series,
            costed=list(cost_positions(held_open, price_basis="split_adjusted")),
            axis_pos=AXIS_POS,
            raw_closes=prices,
            wealth_closes=prices,
            first_axis_index=0,
        )
        # An open position is `hold_out` by `namespace_for_position`, so it is
        # neither matched nor counted against the in-sample cohort.
        assert collector.placements == []
        assert collector.matchable_trade_count == 0


class TestScaleGate:
    def test_a_production_cohort_refuses_after_the_fixed_pilot_before_fanout(self) -> None:
        progress: list[tuple[int, int]] = []
        budget = SyntheticControlScaleBudget(max_cohort_s=0.0, max_run_s=0.0)
        with pytest.raises(ScaleBudgetExceeded, match="projected cohort wall time"):
            run_cohort(
                _collector(exits_on_spike=True),
                axis=AXIS,
                strategy_metrics=_sleeve_metrics(exits_on_spike=True),
                benchmark=None,
                cohort_size=1000,
                progress=lambda completed, total: progress.append((completed, total)),
                scale_budget=budget,
                label="fixture/admitted",
            )
        assert progress == [(1, 1000), (2, 1000), (3, 1000)]
        assert budget.projected_run_s == 0.0

    def test_the_cumulative_budget_refuses_the_arm_that_would_cross_it(self) -> None:
        budget = SyntheticControlScaleBudget(max_cohort_s=100.0, max_run_s=15.0)
        budget.reserve(label="first", projected_s=10.0)
        with pytest.raises(ScaleBudgetExceeded, match="cumulative projected run wall time"):
            budget.reserve(label="second", projected_s=6.0)
        assert budget.projected_run_s == 10.0


class TestSealedEvaluatorsDeclareTheirControl:
    """#2601 scope 2 — never silently absent, and enforced rather than agreed.

    ⚠⚠ A TEST AND NOT A CONVENTION, which is #2614's lesson applied one ticket
    on: *"remember to call the helper" is exactly what failed five consecutive
    times before #2599 existed*. A bespoke sealed evaluator writes no result row,
    so no ledger chokepoint can notice that it never considered §9's control.
    This walk is the only thing that can.
    """

    @staticmethod
    def _evaluators() -> list[Path]:
        found = sorted(Path("scripts").glob("evaluate_*.py"))
        assert found, "the walk found no scripts/evaluate_*.py — a guard over an empty set proves nothing"
        return found

    def test_every_bespoke_evaluator_declares_a_control_or_a_reason(self) -> None:
        for path in self._evaluators():
            source = path.read_text()
            if "synthetic_control_run" in source:
                # It constructs its cohort through the shared module — the other
                # arm of scope 2, and nothing further is owed.
                continue
            assert 'SYNTHETIC_CONTROL: Final = "not_applicable"' in source, (
                f"{path} neither builds its cohort through app.services.synthetic_control_run nor declares "
                'SYNTHETIC_CONTROL = "not_applicable" — a sealed study that never considered §9\'s control is '
                "exactly the second path #2614 found, one criterion further on"
            )
            reason = re.search(r"SYNTHETIC_CONTROL_REASON: Final = \(\s*\n?\s*(.+?)\)", source, re.S)
            assert reason is not None and len(reason.group(1).strip()) > 40, (
                f"{path} declares not_applicable with no reason — 'not applicable' without one is indistinguishable "
                "from 'not thought about'"
            )
