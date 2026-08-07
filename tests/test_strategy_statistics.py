"""Phase 5d — criterion 7's metric set.

Pure tier: no database.

⚠ WHERE A METRIC HAS A CLOSED-FORM ANSWER, THE TEST RESTATES THE FORMULA WITH
THE STDLIB (``statistics.mean`` / ``statistics.stdev``) RATHER THAN IMPORTING
THE MODULE'S OWN HELPERS. Importing them would make the assertion a tautology —
the #2240 S-3 lesson — and these are exactly the metrics where an off-by-one in
the annualisation or a ``ddof`` slip is invisible to any smoke test.
"""

from __future__ import annotations

import math
import statistics
from datetime import date, timedelta

import numpy as np
import pytest

from app.services.equity_curve import EquityCurve
from app.services.strategy_statistics import (
    DAYS_PER_YEAR,
    METRIC_SET_ID,
    StrategyMetrics,
    TradeReturns,
    compute_metrics,
    max_drawdown_pct,
    periods_per_year,
)

#: Transcribed from the spec/parent, never imported.
SPEC_METRIC_COUNT = 12


def _dates(count: int, *, start: date = date(2020, 1, 1), step: int = 1) -> tuple[date, ...]:
    return tuple(start + timedelta(days=step * i) for i in range(count))


def _curve(equity: list[float], *, invested: list[float] | None = None, traded: float = 0.0) -> EquityCurve:
    n = len(equity)
    return EquityCurve(
        equity=np.asarray(equity, dtype=np.float64),
        invested=np.asarray(invested if invested is not None else equity, dtype=np.float64),
        open_count=np.ones(n, dtype=np.int32),
        traded_notional=np.asarray([traded] + [0.0] * (n - 1), dtype=np.float64),
        rebalance_costs=0.0,
        event_dates=2,
        short_funded_entries=0,
        stale_marks=0,
        unrealised_held=0,
    )


def _trades(
    returns: list[float],
    *,
    open_count: int = 0,
    unpriced_count: int = 0,
    dates: list[date] | None = None,
) -> TradeReturns:
    # ⚠ Default entry dates are DISTINCT ascending days, not one repeated day:
    # a single shared date would collapse the whole trade list into one cluster
    # and quietly make every bootstrap in this file degenerate.
    entry_dates = dates if dates is not None else [date(2020, 1, 1) + timedelta(days=i) for i in range(len(returns))]
    return TradeReturns(
        net_return_pct=tuple(returns),
        entry_fill_date=tuple(entry_dates),
        open_count=open_count,
        unpriced_count=unpriced_count,
    )


class TestAnnualisation:
    """⚠⚠ THE MEASURED FACTOR, which is the whole reason ``vectorbt`` was not adopted."""

    def test_it_is_derived_from_the_axis_and_is_not_252(self) -> None:
        """A 253-date axis spanning one calendar year. ⚠ ``len - 1`` intervals,
        not ``len`` endpoints: counting endpoints annualises a 2-day series as
        if it held two years of observations."""
        axis = tuple(date(2020, 1, 1) + timedelta(days=i) for i in range(253))
        span_days = (axis[-1] - axis[0]).days
        expected = 252 / (span_days / DAYS_PER_YEAR)
        assert periods_per_year(axis) == pytest.approx(expected)
        assert periods_per_year(axis) != 252.0

    def test_a_one_date_axis_has_no_span_and_is_refused(self) -> None:
        """⚠ Inventing a span would put a divide-by-zero behind a plausible
        number, which is the failure mode an annualised metric hides best."""
        with pytest.raises(ValueError, match="no span"):
            periods_per_year((date(2020, 1, 1),))

    def test_a_zero_length_span_is_refused(self) -> None:
        with pytest.raises(ValueError, match="spans 0 days"):
            periods_per_year((date(2020, 1, 1), date(2020, 1, 1)))

    def test_the_year_is_gregorian_not_365(self) -> None:
        """⚠ 365.25, not 365. Over the corpus's 64-year span the leap-day drift
        is 16 days — a sixteenth of a year sitting on a CAGR exponent."""
        assert DAYS_PER_YEAR == 365.25


class TestMaxDrawdown:
    def test_it_is_the_deepest_fall_from_a_RUNNING_peak(self) -> None:
        """⚠ From the running peak, not from the start and not from the end. The
        path below finishes ABOVE where it started, so a start-to-trough or a
        peak-to-end reading both miss the -50%."""
        assert max_drawdown_pct(np.asarray([1.0, 2.0, 1.0, 1.5], dtype=np.float64)) == pytest.approx(-50.0)

    def test_a_monotone_path_has_no_drawdown(self) -> None:
        assert max_drawdown_pct(np.asarray([1.0, 1.1, 1.2], dtype=np.float64)) == pytest.approx(0.0)

    def test_it_is_reported_as_a_NON_POSITIVE_number(self) -> None:
        """A sign flip on this column reads as a good result, so the sign is
        pinned here and by ``sql/263``'s CHECK."""
        assert max_drawdown_pct(np.asarray([2.0, 1.0], dtype=np.float64)) <= 0.0

    def test_an_empty_path_is_zero_not_nan(self) -> None:
        assert max_drawdown_pct(np.empty(0, dtype=np.float64)) == 0.0


class TestSharpeAndSortino:
    def test_sharpe_matches_the_stdlib_formula_with_a_zero_risk_free_rate(self) -> None:
        """§5.4 defines cash return as zero, so the excess return IS the return.
        ⚠ ``ddof=1`` — the sample standard deviation, which is what
        ``statistics.stdev`` computes and what a population ``std`` does not."""
        equity = [1.0, 1.02, 1.01, 1.05, 1.03, 1.08]
        axis = _dates(len(equity))
        metrics = compute_metrics(_curve(equity), dates=axis, trades=_trades([1.0, -1.0]), buy_and_hold=None)
        returns = [equity[i + 1] / equity[i] - 1.0 for i in range(len(equity) - 1)]
        expected = statistics.mean(returns) / statistics.stdev(returns) * math.sqrt(periods_per_year(axis))
        assert metrics.sharpe == pytest.approx(expected)

    def test_a_flat_path_has_a_zero_sharpe_rather_than_a_nan(self) -> None:
        """⚠ A NaN here propagates into every downstream comparison, including
        the random-cohort threshold stage 5e gates on."""
        metrics = compute_metrics(_curve([1.0, 1.0, 1.0, 1.0]), dates=_dates(4), trades=_trades([]), buy_and_hold=None)
        assert metrics.sharpe == 0.0
        assert metrics.annualised_volatility_pct == 0.0

    def test_sortino_divides_by_ALL_periods_not_just_the_losing_ones(self) -> None:
        """⚠ THE ONE THAT IS ROUTINELY CONFUSED. Dividing the downside deviation
        by the count of losing periods is a different statistic that rewards a
        strategy for rarely losing twice over."""
        equity = [1.0, 1.10, 1.05, 1.20, 1.15]
        axis = _dates(len(equity))
        metrics = compute_metrics(_curve(equity), dates=axis, trades=_trades([1.0, -1.0]), buy_and_hold=None)
        returns = [equity[i + 1] / equity[i] - 1.0 for i in range(len(equity) - 1)]
        downside = math.sqrt(sum(r * r for r in returns if r < 0) / len(returns))
        expected = statistics.mean(returns) / downside * math.sqrt(periods_per_year(axis))
        assert metrics.sortino == pytest.approx(expected)
        assert metrics.losing_period_count == 2

    def test_sortino_is_NULL_exactly_when_there_is_no_losing_period(self) -> None:
        """Null because the DENOMINATOR is empty — a real state, not a missing
        measurement. ``sql/263`` ties it to ``losing_period_count`` by CHECK so
        the two cannot be confused."""
        metrics = compute_metrics(
            _curve([1.0, 1.1, 1.2, 1.3]), dates=_dates(4), trades=_trades([1.0]), buy_and_hold=None
        )
        assert metrics.sortino is None
        assert metrics.losing_period_count == 0


class TestCagr:
    def test_a_quadrupling_over_two_years_is_100_percent_a_year(self) -> None:
        axis = (date(2020, 1, 1), date(2021, 1, 1), date(2022, 1, 1))
        metrics = compute_metrics(_curve([1.0, 2.0, 4.0]), dates=axis, trades=_trades([]), buy_and_hold=None)
        years = (axis[-1] - axis[0]).days / DAYS_PER_YEAR
        assert metrics.cagr_pct == pytest.approx((4.0 ** (1.0 / years) - 1.0) * 100.0)
        assert metrics.total_return_pct == pytest.approx(300.0)

    def test_a_wiped_out_sleeve_is_minus_100_with_no_special_case(self) -> None:
        """⚠ No branch is needed and none is written: ``0.0 ** x == 0.0`` in
        Python, so the general formula already returns exactly -100%. A ``<= 0``
        special case would be dead code that a test named after it could not
        exercise — which is what a revert probe found."""
        metrics = compute_metrics(_curve([1.0, 0.5, 0.0]), dates=_dates(3), trades=_trades([]), buy_and_hold=None)
        assert metrics.cagr_pct == -100.0

    def test_a_NEGATIVE_final_equity_raises_rather_than_returning_a_COMPLEX_number(self) -> None:
        """⚠⚠ THE STATE THAT MUST NOT FALL THROUGH. Python returns a complex
        number for a negative base under a fractional exponent —
        ``(-0.5) ** 0.0155`` is ``0.988+0.048j`` — which would travel into
        ``StrategyMetrics`` and compare against thresholds in ways nothing
        downstream expects. It is unreachable while ``build_equity_curve``'s
        cash-capped rebalance holds, so it raises."""
        with pytest.raises(ValueError, match="COMPLEX number"):
            compute_metrics(_curve([1.0, 0.5, -0.2]), dates=_dates(3), trades=_trades([]), buy_and_hold=None)


class TestTradeLevelMetrics:
    def test_expectancy_is_the_mean_NET_return_per_closed_trade(self) -> None:
        metrics = compute_metrics(
            _curve([1.0, 1.1]), dates=_dates(2), trades=_trades([4.0, -2.0, 1.0]), buy_and_hold=None
        )
        assert metrics.expectancy_per_trade_pct == pytest.approx(1.0)
        assert metrics.trade_count == 3

    def test_profit_factor_is_gains_over_losses(self) -> None:
        metrics = compute_metrics(
            _curve([1.0, 1.1]), dates=_dates(2), trades=_trades([6.0, -2.0, -1.0]), buy_and_hold=None
        )
        assert metrics.profit_factor == pytest.approx(2.0)
        assert metrics.losing_trade_count == 2

    def test_profit_factor_is_NULL_exactly_when_there_is_no_losing_trade(self) -> None:
        metrics = compute_metrics(_curve([1.0, 1.1]), dates=_dates(2), trades=_trades([3.0, 1.0]), buy_and_hold=None)
        assert metrics.profit_factor is None
        assert metrics.losing_trade_count == 0

    def test_open_and_unpriced_positions_are_counted_and_kept_OUT_of_expectancy(self) -> None:
        """§3.4 — an ``ambiguous`` close and a position open at the window end
        are *"excluded, counted"* from the win rate and expectancy while staying
        IN exposure and ON the equity curve. ⚠ A ``trade_count`` read without
        these two understates the capital that was committed."""
        metrics = compute_metrics(
            _curve([1.0, 1.1]),
            dates=_dates(2),
            trades=_trades([2.0, 4.0], open_count=7, unpriced_count=3),
            buy_and_hold=None,
        )
        assert metrics.trade_count == 2
        assert metrics.expectancy_per_trade_pct == pytest.approx(3.0)
        assert metrics.open_trade_count == 7
        assert metrics.unpriced_trade_count == 3


class TestExposureAndTurnover:
    def test_a_fully_invested_sleeve_is_100_percent_exposed(self) -> None:
        metrics = compute_metrics(
            _curve([1.0, 1.0, 1.0], invested=[1.0, 1.0, 1.0]), dates=_dates(3), trades=_trades([]), buy_and_hold=None
        )
        assert metrics.exposure_time_pct == pytest.approx(100.0)

    def test_exposure_is_capital_days_and_NOT_a_bar_count(self) -> None:
        """§5.4: *"It is NOT sum(bars_held); sql/256 says bars_held is a bar
        count and NOT exposure time, and the difference is concurrency."* Half
        the pot at work throughout is 50%, whatever the bar count says."""
        metrics = compute_metrics(
            _curve([1.0, 1.0, 1.0, 1.0], invested=[0.5, 0.5, 0.5, 0.5]),
            dates=_dates(4),
            trades=_trades([]),
            buy_and_hold=None,
        )
        assert metrics.exposure_time_pct == pytest.approx(50.0)

    def test_an_idle_sleeve_is_zero_exposed_and_does_not_divide_by_zero(self) -> None:
        metrics = compute_metrics(
            _curve([1.0, 1.0], invested=[0.0, 0.0]), dates=_dates(2), trades=_trades([]), buy_and_hold=None
        )
        assert metrics.exposure_time_pct == 0.0

    def test_turnover_halves_the_traded_notional_into_round_trips(self) -> None:
        """⚠ The halving is what makes 1.0 mean "the pot turned over once",
        which is the reading every desk uses; the un-halved form doubles it and
        looks like twice the trading."""
        axis = (date(2020, 1, 1), date(2021, 1, 1))
        metrics = compute_metrics(_curve([1.0, 1.0], traded=2.0), dates=axis, trades=_trades([]), buy_and_hold=None)
        years = (axis[-1] - axis[0]).days / DAYS_PER_YEAR
        assert metrics.turnover_annualised == pytest.approx(2.0 / 2.0 / 1.0 / years)


class TestBuyAndHold:
    def test_the_relative_return_is_the_difference_of_the_two_curves(self) -> None:
        metrics = compute_metrics(
            _curve([1.0, 1.2]),
            dates=_dates(2),
            trades=_trades([]),
            buy_and_hold=_curve([1.0, 1.5]),
        )
        assert metrics.total_return_pct == pytest.approx(20.0)
        assert metrics.buy_and_hold_return_pct == pytest.approx(50.0)
        assert metrics.return_vs_buy_and_hold_pct == pytest.approx(-30.0)

    def test_a_benchmark_on_a_different_axis_is_refused(self) -> None:
        """⚠ Two curves of different lengths are two different WINDOWS, and
        subtracting them attributes the window difference to the strategy."""
        with pytest.raises(ValueError, match="the two must run on the same axis"):
            compute_metrics(
                _curve([1.0, 1.2]), dates=_dates(2), trades=_trades([]), buy_and_hold=_curve([1.0, 1.1, 1.3])
            )

    def test_no_benchmark_reports_zero_on_BOTH_fields_so_the_absence_is_visible(self) -> None:
        """⚠ Not a silent 0% benchmark: ``buy_and_hold_return_pct`` carries the
        same 0.0, so a reader can tell "no benchmark" from "the benchmark was
        flat" only by looking — which is why both are stored."""
        metrics = compute_metrics(_curve([1.0, 1.2]), dates=_dates(2), trades=_trades([]), buy_and_hold=None)
        assert metrics.buy_and_hold_return_pct == 0.0
        assert metrics.return_vs_buy_and_hold_pct == pytest.approx(20.0)


class TestTheShippedState:
    def test_the_effective_sample_size_is_ALWAYS_NULL_from_this_stage(self) -> None:
        """⚠⚠ Criterion 3's block bootstrap is stage 5e (spec §8), and filling
        this with a nominal *n* would be worse than leaving it null: criterion 3
        says *"no bare percentage and no nominal n is reported anywhere"*, so an
        overlap-ignoring count would be the exact number the criterion forbids,
        wearing the name of the one it requires. The promotion gate refuses on
        the null."""
        metrics = compute_metrics(
            _curve([1.0, 1.1, 1.05]), dates=_dates(3), trades=_trades([1.0, -1.0]), buy_and_hold=None
        )
        assert metrics.effective_sample_size is None

    def test_the_metric_set_carries_its_own_id(self) -> None:
        metrics = compute_metrics(_curve([1.0, 1.1]), dates=_dates(2), trades=_trades([]), buy_and_hold=None)
        assert metrics.metric_set_id == METRIC_SET_ID

    def test_all_twelve_criterion_7_metrics_are_present(self) -> None:
        """Criterion 7: *"a result missing any of the twelve is incomplete"*.
        ⚠ The names are LISTED here rather than derived from the dataclass —
        deriving them would pass however many fields happen to exist."""
        twelve = (
            "expectancy_per_trade_pct",
            "profit_factor",
            "cagr_pct",
            "annualised_volatility_pct",
            "sharpe",
            "sortino",
            "max_drawdown_pct",
            "exposure_time_pct",
            "turnover_annualised",
            "trade_count",
            "effective_sample_size",
            "return_vs_buy_and_hold_pct",
        )
        assert len(twelve) == SPEC_METRIC_COUNT
        metrics = compute_metrics(
            _curve([1.0, 1.1, 1.05]), dates=_dates(3), trades=_trades([1.0, -1.0]), buy_and_hold=None
        )
        for name in twelve:
            assert hasattr(metrics, name), name


class TestMetricsRefuse:
    """``StrategyMetrics`` raises — it is a writer-side shape, like ``StrategyResult``."""

    def _base(self, **overrides: object) -> StrategyMetrics:
        base: dict[str, object] = {
            "expectancy_per_trade_pct": 0.5,
            "profit_factor": 1.2,
            "cagr_pct": 4.0,
            "annualised_volatility_pct": 12.0,
            "sharpe": 0.33,
            "sortino": 0.44,
            "max_drawdown_pct": -18.0,
            "exposure_time_pct": 61.0,
            "turnover_annualised": 2.5,
            "trade_count": 100,
            "effective_sample_size": None,
            "return_vs_buy_and_hold_pct": -1.5,
            "losing_trade_count": 40,
            "losing_period_count": 300,
            "open_trade_count": 2,
            "unpriced_trade_count": 1,
            "periods_per_year": 251.7,
            "total_return_pct": 21.0,
            "buy_and_hold_return_pct": 22.5,
        }
        base.update(overrides)
        return StrategyMetrics(**base)  # type: ignore[arg-type]

    def test_the_base_is_valid(self) -> None:
        assert self._base().trade_count == 100

    def test_a_positive_drawdown_is_refused(self) -> None:
        with pytest.raises(ValueError, match="is positive"):
            self._base(max_drawdown_pct=1.0)

    def test_more_losers_than_trades_is_refused(self) -> None:
        with pytest.raises(ValueError, match="a subset cannot exceed its set"):
            self._base(trade_count=10, losing_trade_count=11)

    @pytest.mark.parametrize("exposure", [-0.01, 100.01])
    def test_an_exposure_outside_0_100_is_refused(self, exposure: float) -> None:
        with pytest.raises(ValueError, match="outside 0-100"):
            self._base(exposure_time_pct=exposure)

    def test_a_null_profit_factor_with_losing_trades_is_refused(self) -> None:
        """⚠ The null must mean "the denominator was empty" and nothing else. A
        null standing in for "not computed" is the state #2288 clause 2
        refuses."""
        with pytest.raises(ValueError, match="never as a stand-in"):
            self._base(profit_factor=None, losing_trade_count=40)

    def test_a_profit_factor_with_no_losing_trades_is_refused(self) -> None:
        with pytest.raises(ValueError, match="denominator is empty"):
            self._base(profit_factor=1.2, losing_trade_count=0)

    def test_a_null_sortino_with_losing_periods_is_refused(self) -> None:
        with pytest.raises(ValueError, match="downside deviation has no observations"):
            self._base(sortino=None, losing_period_count=300)

    def test_a_sortino_with_no_losing_periods_is_refused(self) -> None:
        with pytest.raises(ValueError, match="downside deviation has no observations"):
            self._base(sortino=0.44, losing_period_count=0)

    def test_a_non_positive_annualisation_is_refused(self) -> None:
        with pytest.raises(ValueError, match="periods_per_year must be positive"):
            self._base(periods_per_year=0.0)

    def test_a_zero_effective_sample_size_is_refused_while_null_is_not(self) -> None:
        """Null is the fail-closed state the gate refuses on; zero is not a
        sample size anybody measured."""
        assert self._base(effective_sample_size=None).effective_sample_size is None
        with pytest.raises(ValueError, match="must be positive when declared"):
            self._base(effective_sample_size=0.0)


class TestCurveAndAxisMustAgree:
    def test_a_curve_of_a_different_length_from_its_axis_is_refused(self) -> None:
        with pytest.raises(ValueError, match="points against"):
            compute_metrics(_curve([1.0, 1.1]), dates=_dates(5), trades=_trades([]), buy_and_hold=None)


class TestCriterion3IsWiredIn:
    """Stage 5e-2 — the block bootstrap reaching the metric set.

    ⚠ The bootstrap's own arithmetic is covered in
    ``tests/test_block_bootstrap.py``. What is asserted here is only the WIRING:
    that a declared seed fills criterion 3's fields and an undeclared one leaves
    the whole set null.
    """

    def _inputs(self, count: int = 200) -> tuple[EquityCurve, tuple[date, ...], TradeReturns]:
        rng = np.random.default_rng(101)
        equity = list(np.cumprod(1.0 + rng.standard_normal(count) * 0.01))
        returns = [float(v) for v in rng.standard_normal(count)]
        return _curve(equity), _dates(count), _trades(returns)

    def test_no_seed_leaves_the_whole_criterion_3_block_null(self) -> None:
        """⚠ The fail-closed default. The promotion gate refuses on the null ESS,
        so a caller that forgets the seed gets a REFUSED result rather than a
        silently uncorrected one."""
        curve, dates, trades = self._inputs()
        metrics = compute_metrics(curve, dates=dates, trades=trades, buy_and_hold=None)

        assert metrics.effective_sample_size is None
        assert metrics.expectancy_ci_low_pct is None
        assert metrics.expectancy_ci_high_pct is None
        assert metrics.bootstrap_model_id is None
        assert metrics.bootstrap_seed is None

    def test_a_declared_seed_fills_the_sample_size_and_the_interval(self) -> None:
        curve, dates, trades = self._inputs()
        metrics = compute_metrics(curve, dates=dates, trades=trades, buy_and_hold=None, bootstrap_seed=42)

        assert metrics.effective_sample_size is not None
        assert metrics.effective_sample_size > 0.0
        assert metrics.expectancy_ci_low_pct is not None
        assert metrics.expectancy_ci_high_pct is not None
        assert metrics.expectancy_ci_low_pct <= metrics.expectancy_ci_high_pct
        assert metrics.bootstrap_seed == 42
        assert metrics.bootstrap_model_id == "c3-block-bootstrap-v1"
        assert metrics.bootstrap_cluster_count == 200
        # ⚠ The trade count is untouched by the correction — criterion 7 reports
        # BOTH, and a reader compares them. Overwriting the nominal count with
        # the effective one would destroy the comparison the criterion is for.
        assert metrics.trade_count == 200

    def test_a_degenerate_trade_population_leaves_the_block_null_rather_than_guessing(self) -> None:
        """A seed was declared but the measurement could not be made (every trade
        on one date). ⚠ Criterion 3 forbids a nominal-n fallback, so the correct
        output is the same null the gate refuses on."""
        curve, dates, _ = self._inputs(60)
        single_day = _trades([1.0, 2.0, 3.0], dates=[date(2020, 3, 2)] * 3)
        metrics = compute_metrics(curve, dates=dates, trades=single_day, buy_and_hold=None, bootstrap_seed=42)

        assert metrics.trade_count == 3
        assert metrics.effective_sample_size is None
        assert metrics.bootstrap_model_id is None


class TestTradeReturnsParallelism:
    def test_returns_and_entry_dates_must_be_parallel(self) -> None:
        """⚠ A mismatch would cluster returns under the wrong dates, which does
        not raise anywhere downstream — it just produces a wrong effective sample
        size that looks entirely plausible."""
        with pytest.raises(ValueError, match="positionally parallel"):
            TradeReturns(
                net_return_pct=(1.0, 2.0, 3.0),
                entry_fill_date=(date(2020, 1, 1), date(2020, 1, 2)),
                open_count=0,
                unpriced_count=0,
            )


class TestBootstrapFieldsAreAllOrNothing:
    """Criterion 3 asks for the sample size AND the interval, so a partial set is
    a corrected number whose correction cannot be judged."""

    def _full(self) -> dict[str, object]:
        return {
            "expectancy_per_trade_pct": 0.5,
            "profit_factor": 1.2,
            "cagr_pct": 4.0,
            "annualised_volatility_pct": 12.0,
            "sharpe": 0.33,
            "sortino": 0.44,
            "max_drawdown_pct": -18.0,
            "exposure_time_pct": 61.0,
            "turnover_annualised": 2.5,
            "trade_count": 100,
            "return_vs_buy_and_hold_pct": -1.5,
            "losing_trade_count": 40,
            "losing_period_count": 300,
            "open_trade_count": 2,
            "unpriced_trade_count": 1,
            "periods_per_year": 251.7,
            "total_return_pct": 21.0,
            "buy_and_hold_return_pct": 22.5,
            "effective_sample_size": 41.0,
            "expectancy_ci_low_pct": -0.2,
            "expectancy_ci_high_pct": 1.1,
            "bootstrap_block_length": 9,
            "bootstrap_cluster_count": 80,
            "bootstrap_resamples": 2_000,
            "bootstrap_seed": 1,
            "bootstrap_design_effect": 2.44,
            "bootstrap_model_id": "c3-block-bootstrap-v1",
        }

    def test_the_complete_set_constructs(self) -> None:
        assert StrategyMetrics(**self._full()).effective_sample_size == 41.0  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "dropped",
        [
            "effective_sample_size",
            "expectancy_ci_low_pct",
            "expectancy_ci_high_pct",
            "bootstrap_block_length",
            "bootstrap_cluster_count",
            "bootstrap_resamples",
            "bootstrap_seed",
            "bootstrap_design_effect",
            "bootstrap_model_id",
        ],
    )
    def test_dropping_any_single_field_is_refused(self, dropped: str) -> None:
        base = self._full()
        base[dropped] = None
        with pytest.raises(ValueError, match="block-bootstrap fields"):
            StrategyMetrics(**base)  # type: ignore[arg-type]

    def test_an_inverted_interval_is_refused(self) -> None:
        base = self._full()
        base["expectancy_ci_low_pct"] = 2.0
        base["expectancy_ci_high_pct"] = 1.0
        with pytest.raises(ValueError, match="inverted"):
            StrategyMetrics(**base)  # type: ignore[arg-type]
