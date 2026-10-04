"""#3609 step 0 baselines: the pure rules of ``scripts/report_3609_baselines.py``."""

from __future__ import annotations

from datetime import date

import numpy as np
import pytest

from app.services.series_termination import SHUMWAY_HAIRCUT, TerminationClass
from app.services.total_return_reader import INTRADER_VENDOR, PWB_VENDOR, MonthlyTotalReturn
from scripts.report_3609_baselines import (
    COVERAGE_EXIT,
    FUNDS,
    REQUIRED_VERDICTS,
    Period,
    ScreenClass,
    Termination,
    build_trajectory,
    common_end_month,
    draw_names,
    formation_population,
    holding_periods,
    max_drawdown,
    month_range,
    nearest_rank,
    newey_west_lag,
    ols_newey_west,
    regression_months,
    screen_class,
    simulate,
)

FULL = set(month_range((2010, 1), (2026, 8)))


def _fund_months(**ends: tuple[int, int]) -> dict[str, set[tuple[int, int]]]:
    return {s: set(month_range((2010, 1), ends.get(s, (2026, 4)))) for s in FUNDS}


# ---- Amendment 1: the common end month -------------------------------------


def test_end_month_is_the_earliest_fund_endpoint_and_names_the_limiting_funds() -> None:
    end = common_end_month(REQUIRED_VERDICTS, _fund_months(AGG=(2026, 2), IEF=(2026, 2), SPY=(2026, 3)))
    assert end.month == (2026, 2)
    assert end.limiting == ("AGG", "IEF")


def test_a_fallback_verdict_refuses_rather_than_shortening_the_end() -> None:
    verdicts = {**REQUIRED_VERDICTS, "AGG": "refused_identity"}
    with pytest.raises(RuntimeError, match="accepted extension verdict"):
        common_end_month(verdicts, _fund_months())


def test_an_interior_gap_refuses_and_never_moves_the_end_earlier() -> None:
    months = _fund_months()
    months["TIP"].discard((2018, 6))
    with pytest.raises(RuntimeError, match="missing months"):
        common_end_month(REQUIRED_VERDICTS, months)


def test_an_end_before_the_last_formations_holding_month_refuses() -> None:
    with pytest.raises(RuntimeError, match="before 2026-01"):
        common_end_month(REQUIRED_VERDICTS, _fund_months(EEM=(2025, 11)))


# ---- Positions and paths ----------------------------------------------------

MONTHS = tuple(month_range((2011, 1), (2011, 4)))


def test_a_terminating_name_realises_its_fraction_the_month_after_its_last_row() -> None:
    termination = Termination(TerminationClass.EXCHANGE_FAILURE, last_row_month=(2011, 2))
    traj = build_trajectory(MONTHS, {(2011, 1): 0.10, (2011, 2): 0.0}, termination=termination, arm="best_case")
    assert traj.exit_index == 2
    assert traj.exit_kind == TerminationClass.EXCHANGE_FAILURE.value
    assert traj.growth.tolist() == pytest.approx([1.1, 1.1, 1.1 * (1 - SHUMWAY_HAIRCUT), 1.1 * (1 - SHUMWAY_HAIRCUT)])


def test_an_unknown_termination_takes_each_arm() -> None:
    termination = Termination(TerminationClass.UNKNOWN, last_row_month=(2011, 1))
    best = build_trajectory(MONTHS, {(2011, 1): 0.0}, termination=termination, arm="best_case")
    worst = build_trajectory(MONTHS, {(2011, 1): 0.0}, termination=termination, arm="worst_case")
    assert best.growth[-1] == pytest.approx(1.0)
    assert worst.growth[-1] == pytest.approx(1 - SHUMWAY_HAIRCUT)


def test_a_gap_before_the_last_row_is_a_coverage_exit_without_a_haircut() -> None:
    termination = Termination(TerminationClass.EXCHANGE_FAILURE, last_row_month=(2011, 4))
    traj = build_trajectory(
        MONTHS, {(2011, 1): 0.2, (2011, 3): 0.5, (2011, 4): 0.5}, termination=termination, arm="worst_case"
    )
    assert traj.exit_kind == COVERAGE_EXIT
    assert traj.exit_index == 1
    assert traj.growth.tolist() == pytest.approx([1.2, 1.2, 1.2, 1.2])


def test_holding_periods_run_to_the_next_decision_close_and_stop_at_the_end() -> None:
    periods = holding_periods([(2009, 12), (2010, 12)], (2011, 2))
    assert periods[0] == ((2009, 12), tuple(month_range((2010, 1), (2010, 12))))
    assert periods[1] == ((2010, 12), ((2011, 1), (2011, 2)))


def _period(decision, months, assets, returns, entry):  # type: ignore[no-untyped-def]
    trajectories = tuple(
        build_trajectory(months, {m: r for m, r in zip(months, rs, strict=True)}, termination=None, arm="worst_case")
        for rs in returns
    )
    return Period(
        decision=decision,
        months=tuple(months),
        assets=tuple(assets),
        weights=(1.0 / len(assets),) * len(assets),
        trajectories=trajectories,
        entry_half_spreads=tuple(entry),
    )


def test_costs_initial_cost_lands_in_the_first_month_and_liquidation_is_kept_apart() -> None:
    period = _period((2009, 12), [(2010, 1), (2010, 2)], ["A"], [[0.10, 0.0]], [0.01])
    result = simulate([period], cost_multiplier=1.0)
    assert result.continuing[(2010, 1)] == pytest.approx(0.99 * 1.1 - 1)
    assert result.continuing[(2010, 2)] == pytest.approx(0.0)
    assert result.liquidation == pytest.approx(0.01)
    assert result.returns()[(2010, 2)] == pytest.approx(-0.01)
    assert result.turnover == {}


def test_a_held_position_keeps_its_entry_band_and_trades_only_the_weight_difference() -> None:
    first = _period((2009, 12), [(2010, 1)], ["A", "B"], [[1.0], [0.0]], [0.01, 0.01])
    # "A" is redrawn with a cheaper entry band that must NOT apply; "C" is new.
    second = _period((2010, 1), [(2010, 2)], ["A", "C"], [[0.0], [0.0]], [0.001, 0.02])
    gross = simulate([first, second], cost_multiplier=0.0)
    net = simulate([first, second], cost_multiplier=1.0)
    nav = 1.0 - 0.01  # after the initial purchase
    a, b = nav * 0.5 * 2.0, nav * 0.5  # month-end values before the rebalance
    pre = a + b
    traded = abs(0.5 * pre - a) + abs(0.0 - b) + 0.5 * pre
    cost = abs(0.5 * pre - a) * 0.01 + b * 0.01 + 0.5 * pre * 0.02
    assert net.continuing[(2010, 1)] == pytest.approx((pre - cost) / 1.0 - 1)
    assert net.turnover[(2010, 1)] == pytest.approx(traded / 2 / pre)
    assert gross.liquidation == 0.0
    assert net.liquidation == pytest.approx(0.5 * 0.01 + 0.5 * 0.02)
    # A slice ending at the decision month does not rebalance: it liquidates the pre-trade book instead.
    pre_trade_liquidation = (a * 0.01 + b * 0.01) / pre
    assert net.rebalance_cost[(2010, 1)] == pytest.approx(cost / pre)
    assert net.liquidation_by_month[(2010, 1)] == pytest.approx(pre_trade_liquidation)
    assert net.slice_end_return((2010, 1)) == pytest.approx(pre * (1 - pre_trade_liquidation) - 1)


def test_an_exited_position_is_cash_and_a_redraw_is_a_new_entry() -> None:
    months = [(2010, 1), (2010, 2)]
    exited = build_trajectory(
        months,
        {(2010, 1): 0.0},
        termination=Termination(TerminationClass.OPERATION_OF_LAW, last_row_month=(2010, 1)),
        arm="worst_case",
    )
    first = Period((2009, 12), tuple(months), ("A",), (1.0,), (exited,), (0.01,))
    second = _period((2010, 2), [(2010, 3)], ["A"], [[0.0]], [0.02])
    result = simulate([first, second], cost_multiplier=1.0)
    assert result.exits == [(TerminationClass.OPERATION_OF_LAW.value, pytest.approx(1.0))]
    # Re-entering from cash pays the NEW entry band on the whole NAV, no sale of the old position.
    assert result.continuing[(2010, 2)] == pytest.approx(-0.02)
    assert result.turnover[(2010, 2)] == pytest.approx(0.5)


# ---- Statistics ---------------------------------------------------------------


def test_nearest_rank_and_drawdown() -> None:
    values = [float(v) for v in range(1, 1001)]
    assert nearest_rank(values, 5) == 50.0
    assert nearest_rank(values, 50) == 500.0
    assert nearest_rank(values, 95) == 950.0
    assert max_drawdown([0.10, -0.50, 0.20]) == pytest.approx(-0.5)


def test_newey_west_lag_rule_and_coefficient_recovery() -> None:
    assert newey_west_lag(137) == 4
    assert newey_west_lag(200) == 4
    rng = np.random.default_rng(3609)
    x = rng.normal(size=(300, 2))
    y = 0.01 + x @ np.array([1.5, -0.5]) + rng.normal(scale=1e-4, size=300)
    fit = ols_newey_west(y, x)
    assert fit.coefficients == pytest.approx([0.01, 1.5, -0.5], abs=1e-4)
    assert fit.lag == newey_west_lag(300)


def test_regression_months_cut_only_the_tail_and_need_sixty() -> None:
    names = ("Mkt-RF", "SMB", "HML", "RMW", "CMA", "Mom", "RF")
    window = month_range((2010, 1), (2016, 12))
    factors = {n: {m: 0.0 for m in month_range((2010, 1), (2015, 6))} for n in names}
    assert regression_months(window, factors) == month_range((2010, 1), (2015, 6))
    factors["Mom"].pop((2012, 3))
    with pytest.raises(RuntimeError, match="interior"):
        regression_months(window, factors)
    short = {n: {m: 0.0 for m in month_range((2010, 1), (2014, 11))} for n in names}
    assert regression_months(window, short) is None


# ---- B3 population, draws, screen ----------------------------------------------


def _row(key: int, month: tuple[int, int], ret: float, end_bar: date, vendor: str = INTRADER_VENDOR):
    return MonthlyTotalReturn(
        name_key=key,
        month=date(*month, 1),
        total_return=ret,
        start_bar=date(*month, 1),
        end_bar=end_bar,
        vendor=vendor,
        series_id=key,
        dividend_capture_degraded=False,
        survivor_only=False,
    )


def _history(key: int, *, stale: bool = False, drop: tuple[int, int] | None = None):
    out = {}
    for m in month_range((2015, 1), (2015, 12)):
        if m == drop:
            continue
        end = date(2015, 12, 20) if stale and m == (2015, 12) else date(*m, 28)
        out[m] = _row(key, m, 0.01, end)
    return out


def test_formation_population_applies_history_staleness_and_the_raw_close_rules() -> None:
    rows = {k: _history(k) for k in range(1, 61)}
    rows[100] = _history(100, stale=True)
    rows[101] = _history(101, drop=(2015, 4))
    rows[102] = _history(102)
    rows[103] = _history(103)
    closes = {102: 4.99, 103: None}
    population, census = formation_population(
        2015, rows, raw_close=lambda k, _m: closes.get(k, 20.0), linked=lambda k: k % 2 == 0
    )
    assert population == list(range(1, 61))
    assert (census.failed_stale, census.failed_history, census.failed_low_price, census.failed_no_raw_close) == (
        1,
        1,
        1,
        1,
    )
    assert census.unlinked == 30


def test_formation_population_refuses_below_fifty_names() -> None:
    rows = {k: _history(k) for k in range(1, 50)}
    with pytest.raises(RuntimeError, match="fewer than 50"):
        formation_population(2015, rows, raw_close=None, linked=lambda _k: True)


def test_draws_are_deterministic_and_order_independent() -> None:
    population = list(range(1000, 1300))
    first = draw_names(population, variant="all", draw=7, year=2015)
    assert first == draw_names(list(reversed(population)), variant="all", draw=7, year=2015)
    assert len(set(first)) == 50
    assert first != draw_names(population, variant="5", draw=7, year=2015)


def test_screen_classes() -> None:
    end = date(2023, 5, 31)
    assert screen_class(_row(1, (2023, 5), 2.0, end, PWB_VENDOR), 2.1) is None
    assert screen_class(_row(1, (2023, 5), 38.9, end, PWB_VENDOR), 0.02) == ScreenClass.CONTRADICTED
    assert screen_class(_row(1, (2023, 5), 3.5, end, PWB_VENDOR), 3.2) == ScreenClass.CORROBORATED
    assert screen_class(_row(1, (2023, 5), -0.95, end, INTRADER_VENDOR), None) == ScreenClass.UNARBITRATED
    assert screen_class(_row(1, (2025, 5), 4.0, date(2025, 5, 30), PWB_VENDOR), None) == ScreenClass.UNARBITRATED
