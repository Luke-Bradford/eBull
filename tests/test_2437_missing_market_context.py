"""#2437 — a benchmark hole is ``not_evaluable``, not ``not_fired``.

S-5, S-6 and S-9 gate on a market regime classified from SPY. The regime was
checked INSIDE each strategy body, so ``strategy_registry.evaluate`` never got
the chance to refuse a bar whose regime was unknown: ``permits`` returned
``False`` and the bar was stored as ``not_fired``. Measured on the full
validated universe (dev DB, 2026-08-14) that is **9,688 bars over 360 dates**,
worst 2026-02-06 with **1,735 instruments trading against no SPY bar**.

⚠ THE WHOLE DIFFICULTY IS THAT TWO DIFFERENT FACTS BOTH ARRIVE AS ``None``, so
every test here is really about the same distinction:

* the benchmark traded and is not yet classifiable  → ``insufficient_warmup``
* the benchmark did not trade at all                → ``missing_market_context``
* the benchmark traded, classified, and the regime is one the strategy declines
                                                    → ``not_fired``, unchanged

The third is not a data gap and must NOT move — the bar WAS judged, and the
parent spec's §0 rule 2 makes firing outside a declared domain the defect.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.services.indicator_series import BarSeries, IndicatorSeries, OHLCVRow
from app.services.market_regime import Regime, RegimeSeries
from app.services.market_regime_provider import MarketRegimeProvider
from app.services.price_segments import series_segment_bounds
from app.services.strategies.s5_support_bounce import PERMITTED_REGIMES as S5_REGIMES
from app.services.strategies.s5_support_bounce import S5_STRATEGY_ID, s5_signals
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_registry import (
    OUR_ADDITIONAL_REASON_CODES,
    PARENT_REASON_CODES,
    StrategyInput,
    evaluate,
)
from app.services.strategy_segmented_evaluation import segmented_signals

U = "survivor_only"


def _days(count: int, start: date = date(2020, 1, 1)) -> tuple[date, ...]:
    return tuple(start + timedelta(days=i) for i in range(count))


class TestRegimeSeriesRefusesAnIncoherentRefusalSet:
    """``not_evaluable_indices`` describes THIS series or it describes nothing."""

    def test_an_index_past_the_end_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="outside a series of 2 bars"):
            RegimeSeries(values=(None, None), not_evaluable_indices=(2,))

    def test_a_negative_index_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="outside a series"):
            RegimeSeries(values=(None,), not_evaluable_indices=(-1,))

    def test_a_classified_bar_cannot_be_marked_a_benchmark_hole(self) -> None:
        """⚠ The inverse defect, and it is the silent one: this would
        manufacture a refusal for a bar the benchmark actually classified."""
        with pytest.raises(ValueError, match="is not a benchmark hole"):
            RegimeSeries(values=(Regime.BULL_QUIET,), not_evaluable_indices=(0,))

    def test_the_default_is_an_empty_set(self) -> None:
        """Every existing construction site keeps its meaning — an unpopulated
        series claims no holes rather than claiming all of its ``None``s are."""
        assert RegimeSeries(values=(None, Regime.BEAR_QUIET)).not_evaluable_indices == ()


class TestSegmentRemapsTheRefusalSet:
    """⚠⚠ THE DEFECT THIS CHANGE WOULD OTHERWISE HAVE SHIPPED.

    ``segmented_signals`` re-slices the regime once per series segment (a scale
    break splits an instrument's history). It did so as
    ``RegimeSeries(values=regime.values[start:end])`` — which type-checks, and
    drops ``not_evaluable_indices`` on the floor. Every benchmark hole inside a
    segmented instrument would have reverted to a bare ``None`` and been counted
    as the benchmark's own warm-up: the new reason code would read zero on
    exactly the instruments most likely to have gaps, and nothing would raise.
    """

    def test_indices_are_rebased_to_the_segment(self) -> None:
        series = RegimeSeries(
            values=(Regime.BULL_QUIET, None, Regime.BEAR_QUIET, None, Regime.BULL_QUIET),
            not_evaluable_indices=(1, 3),
        )
        assert series.segment(2, 5).not_evaluable_indices == (1,)

    def test_holes_outside_the_segment_are_dropped(self) -> None:
        series = RegimeSeries(values=(None, Regime.BULL_QUIET, Regime.BULL_QUIET), not_evaluable_indices=(0,))
        assert series.segment(1, 3).not_evaluable_indices == ()

    def test_the_values_still_line_up(self) -> None:
        series = RegimeSeries(values=(Regime.BULL_QUIET, None, Regime.BEAR_QUIET), not_evaluable_indices=(1,))
        segment = series.segment(1, 3)
        assert segment.values == (None, Regime.BEAR_QUIET)
        assert len(segment) == 2

    def test_the_rule_set_version_is_carried(self) -> None:
        """A segment is the same classification, not a new one."""
        series = RegimeSeries(values=(Regime.BULL_QUIET, Regime.BULL_QUIET))
        assert series.segment(0, 1).rule_set_version == series.rule_set_version

    @pytest.mark.parametrize("bounds", [(-1, 2), (0, 4), (2, 1)])
    def test_bounds_outside_the_series_are_rejected(self, bounds: tuple[int, int]) -> None:
        series = RegimeSeries(values=(Regime.BULL_QUIET,) * 3)
        with pytest.raises(ValueError, match="is not inside a series of 3 bars"):
            series.segment(*bounds)

    def test_a_hole_after_a_scale_break_still_reports_the_new_code(self) -> None:
        """⚠⚠ PINNED AT THE CALL SITE, NOT JUST ON THE METHOD. A correct
        ``segment`` that nothing calls fixes nothing, and the wrong form is the
        one a reader writes by reflex. The hole here sits in the SECOND segment,
        where a dropped index set is indistinguishable from warm-up — which is
        exactly how this would have shipped looking green.
        """
        closes = [100.0 + (i % 11) for i in range(120)]
        series = _bars(closes)
        break_date = series.dates[60]
        permitted = next(iter(S5_REGIMES))
        values: list[Regime | None] = [permitted] * len(closes)
        values[95] = None
        regime = RegimeSeries(values=tuple(values), not_evaluable_indices=(95,))

        bounds = series_segment_bounds(series, unresolved_breaks=(break_date,))
        assert len(bounds) == 2, "the fixture must actually segment, or this proves nothing"

        signals = segmented_signals(
            STRATEGY_MANIFEST[S5_STRATEGY_ID],
            series,
            universe=U,
            masked_reason="quarantined_bar",
            unresolved_breaks=(break_date,),
            regime=regime,
        )
        at_95 = next(s for s in signals if s.signal_index == 95)
        assert (at_95.verdict, at_95.reason) == ("not_evaluable", "missing_market_context")


class TestProviderSplitsTheTwoKindsOfNone:
    """``MarketRegimeProvider.for_dates`` is the ONLY place that can split them.

    Once a ``RegimeSeries`` is built, "absent" and "present but unclassifiable"
    are both ``None`` and the distinction is unrecoverable.
    """

    @staticmethod
    def _provider() -> MarketRegimeProvider:
        days = _days(3)
        # Day 0: classified.  Day 1: benchmark TRADED and is unclassifiable
        # (warm-up).  Day 2: absent from the map entirely.
        return MarketRegimeProvider(regime_by_date={days[0]: Regime.BULL_QUIET, days[1]: None})

    def test_a_missing_benchmark_date_is_flagged(self) -> None:
        series = self._provider().for_dates(_days(3))
        assert series.not_evaluable_indices == (2,)

    def test_a_present_but_unclassifiable_benchmark_bar_is_not_flagged(self) -> None:
        """⚠ THE `in`-VERSUS-`get() is None` DISTINCTION, pinned. Day 1 is in the
        map with value ``None``; treating that as a hole would report the
        benchmark's own warm-up as a data gap and inflate the new code by the
        whole 200-SMA + 126-bar BandWidth run-up (~326 bars per instrument)."""
        series = self._provider().for_dates(_days(3))
        assert series.values[1] is None
        assert 1 not in series.not_evaluable_indices

    def test_a_classified_date_carries_its_regime(self) -> None:
        series = self._provider().for_dates(_days(3))
        assert series.values[0] is Regime.BULL_QUIET


def _regime_input(series: RegimeSeries) -> StrategyInput:
    return StrategyInput(series=series, reason="missing_market_context")


class TestEvaluateRefusesAnUnknownRegimeBeforeTheBodyRuns:
    """The point of the change: ``evaluate`` decides evaluability, not the body."""

    @staticmethod
    def _verdicts(series: RegimeSeries) -> list[tuple[str, str | None]]:
        # ⚠ The body would fire on EVERY bar. Any `not_fired` below therefore
        # comes from the gate and not from the rule declining — which is what
        # makes the reason codes attributable.
        signals = evaluate(lambda _index: True, inputs=(_regime_input(series),), n_bars=len(series))
        return [(s.verdict, s.reason) for s in signals]

    def test_a_benchmark_hole_is_missing_market_context(self) -> None:
        series = RegimeSeries(values=(Regime.BULL_QUIET, None, Regime.BULL_QUIET), not_evaluable_indices=(1,))
        assert self._verdicts(series)[1] == ("not_evaluable", "missing_market_context")

    def test_benchmark_warmup_stays_insufficient_warmup(self) -> None:
        """⚠ Derived structurally from the bare ``None``, not declared. A caller
        cannot get this one wrong, which is why it is not a parameter."""
        series = RegimeSeries(values=(None, Regime.BULL_QUIET, Regime.BULL_QUIET))
        assert self._verdicts(series)[0] == ("not_evaluable", "insufficient_warmup")

    def test_a_warmup_bar_does_not_raise_on_the_multi_series_branch(self) -> None:
        """⚠ REGRESSION GUARD for the isinstance flip in
        ``_unevaluable_reason_at``. It used to read
        ``isinstance(series, IndicatorSeries)`` with ``.components`` in the
        ``else``, so a ``RegimeSeries`` warm-up bar would have taken the multi
        branch and raised ``AttributeError``. The test above only reaches that
        line because this one asserts it does not explode."""
        series = RegimeSeries(values=(None, Regime.BULL_QUIET))
        verdicts = self._verdicts(series)
        assert verdicts[0][0] == "not_evaluable"

    def test_a_classified_bar_reaches_the_body(self) -> None:
        series = RegimeSeries(values=(Regime.BEAR_VOLATILE, Regime.BULL_QUIET))
        # Index 1 is the last bar (`no_fill_bar`); index 0 is the live one.
        assert self._verdicts(series)[0] == ("fired", None)

    def test_an_instrument_level_reason_wins_over_the_market_one(self) -> None:
        """⚠ Declaration ORDER is load-bearing, and this pins the intent rather
        than the accident. A bar that is both quarantined and missing its market
        context is counted under the instrument's own defect — the more specific
        fact, and the one an operator can act on."""
        quarantined = IndicatorSeries(values=(None, 1.0), universe=U, not_evaluable_indices=(0,))
        regime = RegimeSeries(values=(None, Regime.BULL_QUIET), not_evaluable_indices=(0,))
        signals = evaluate(
            lambda _index: True,
            inputs=(
                StrategyInput(series=quarantined, reason="quarantined_bar"),
                _regime_input(regime),
            ),
            n_bars=2,
        )
        assert (signals[0].verdict, signals[0].reason) == ("not_evaluable", "quarantined_bar")


def _bars(closes: list[float], *, start: date = date(2020, 1, 1)) -> BarSeries:
    rows: list[OHLCVRow] = [
        {
            "open": Decimal(str(c)),
            "high": Decimal(str(c + 1)),
            "low": Decimal(str(c - 1)),
            "close": Decimal(str(c)),
            "volume": 1_000,
        }  # type: ignore[typeddict-item]
        for c in closes
    ]
    return BarSeries(dates=tuple(start + timedelta(days=i) for i in range(len(closes))), rows=tuple(rows))


class TestAStrategyEndToEnd:
    """S-5 is the smallest of the three; the input wiring is identical in all."""

    _CLOSES = [100.0 + (i % 11) for i in range(120)]

    def _run(self, regime: RegimeSeries) -> list[tuple[str, str | None]]:
        series = _bars(self._CLOSES)
        signals = s5_signals(series, universe=U, masked_reason="quarantined_bar", regime=regime)
        return [(s.verdict, s.reason) for s in signals]

    def test_a_benchmark_hole_is_reported_by_the_strategy(self) -> None:
        n = len(self._CLOSES)
        permitted = next(iter(S5_REGIMES))
        values: list[Regime | None] = [permitted] * n
        values[80] = None
        verdicts = self._run(RegimeSeries(values=tuple(values), not_evaluable_indices=(80,)))
        assert verdicts[80] == ("not_evaluable", "missing_market_context")

    def test_a_refused_regime_is_still_not_fired(self) -> None:
        """⚠⚠ THE HALF THAT MUST NOT MOVE. ``bear_quiet`` is outside S-5's
        declared domain, and that bar WAS judged — turning it into
        ``not_evaluable`` would delete the denominator the strategy's own
        selectivity is measured against."""
        n = len(self._CLOSES)
        assert Regime.BEAR_QUIET not in S5_REGIMES
        verdicts = self._run(RegimeSeries(values=(Regime.BEAR_QUIET,) * n))
        # Every bar but the last (`no_fill_bar`) and the ATR warm-up run.
        assert ("not_evaluable", "missing_market_context") not in verdicts
        assert verdicts[80] == ("not_fired", None)


class TestTheNewCodeIsDeclaredAsOurs:
    def test_it_is_not_claimed_as_the_parents(self) -> None:
        """Criterion 8 lists seven codes; this is our fourth addition."""
        assert "missing_market_context" in OUR_ADDITIONAL_REASON_CODES
        assert "missing_market_context" not in PARENT_REASON_CODES
        assert len(PARENT_REASON_CODES) == 7
