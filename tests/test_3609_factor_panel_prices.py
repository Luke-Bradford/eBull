"""#3609 step 1 slice 3c: price characteristics, daily screen, holding statuses and liquidity terciles."""

from __future__ import annotations

import math
from collections.abc import Callable
from datetime import date, timedelta
from decimal import Decimal

import numpy as np
import pytest

from app.services.factor_panel import PanelError, SplitStamp
from app.services.factor_panel_prices import (
    DailyBar,
    DailyMonthly,
    HoldingStatus,
    PriceMissing,
    ReturnBounds,
    SessionGrid,
    liquidity_terciles,
    me_discontinuity,
    series_prices,
)
from app.services.series_termination import SHUMWAY_HAIRCUT, TerminationClass

SESSIONS = [
    date(2018, 9, 3) + timedelta(days=i)
    for i in range((date(2019, 12, 31) - date(2018, 9, 3)).days + 1)
    if (date(2018, 9, 3) + timedelta(days=i)).weekday() < 5
]
M = date(2019, 10, 31)  # a Thursday: s(M) = M
HELD_LAST = date(2019, 11, 29)
RF = 0.0001
#: The holding month's JKP cutoffs, wide enough that no test below them is clipped.
WIDE = {(2019, 11): ReturnBounds((2019, 11), -1.0, 10.0)}


def _grid(rf: float | None = RF) -> SessionGrid:
    rates = {} if rf is None else dict.fromkeys(SESSIONS, rf)
    return SessionGrid.build(SESSIONS, rates, {M: M})


def _bars(adj: Callable[[int, date], float | None], *, close: float = 10.0, volume: int = 1000) -> list[DailyBar]:
    out = []
    for i, day in enumerate(SESSIONS):
        value = adj(i, day)
        if value is not None:
            out.append(DailyBar(day, close, value, volume, stamped=False, usable=True))
    return out


def _run(
    bars: list[DailyBar],
    *,
    termination: tuple[TerminationClass, date] | None = None,
    rf: float | None = RF,
    bounds: dict[tuple[int, int], ReturnBounds] = WIDE,
):
    return series_prices(
        bars, _grid(rf), holding_last_session={M: HELD_LAST}, termination=termination, return_bounds=bounds
    )


def _growing(i: int, _day: date) -> float:
    return 10.0 * 1.001**i


def test_momentum_compounds_months_t_minus_11_to_t_minus_1_and_skips_t() -> None:
    got = _run(_bars(_growing)).by_formation[M]
    month_end = {(d.year, d.month): i for i, d in enumerate(SESSIONS)}
    start, end = month_end[(2018, 10)], month_end[(2019, 9)]
    assert got.ret_12_1.value == pytest.approx(1.001 ** (end - start) - 1.0)
    assert (got.ret_12_1.observations, got.ret_12_1.missing) == (11, None)


def test_momentum_needs_all_eleven_months() -> None:
    gap = _run(_bars(lambda i, d: None if (d.year, d.month) == (2019, 3) else _growing(i, d)))
    # March missing breaks March's and April's returns: 9 of 11.
    assert gap.by_formation[M].ret_12_1 == gap.by_formation[M].ret_12_1.__class__(
        None, 9, PriceMissing.INSUFFICIENT_MONTHS
    )


def test_rvol_is_sample_sd_of_excess_returns_over_21_sessions() -> None:
    rng = np.random.default_rng(3609)
    level = np.cumprod(1.0 + rng.normal(0, 0.01, len(SESSIONS))) * 10
    got = _run(_bars(lambda i, _d: float(level[i]))).by_formation[M]
    k = SESSIONS.index(M)
    returns = level[k - 20 : k + 1] / level[k - 21 : k] - 1.0 - RF
    assert got.rvol_21d.value == pytest.approx(float(np.std(returns, ddof=1)))
    assert got.rvol_21d.observations == 21


def test_rvol_needs_15_adjacent_returns() -> None:
    k = SESSIONS.index(M)
    # Every other session missing in the window: no two admitted bars are adjacent.
    sparse = _run(_bars(lambda i, d: None if k - 21 <= i <= k - 1 and i % 2 == (k - 1) % 2 else _growing(i, d)))
    assert sparse.by_formation[M].rvol_21d.missing is PriceMissing.INSUFFICIENT_RETURNS


def test_extreme_daily_return_flags_both_bars_and_leaves_rvol_missing() -> None:
    k = SESSIONS.index(M)
    got = _run(_bars(lambda i, d: _growing(i, d) * (5.0 if i >= k - 5 else 1.0)))
    assert got.by_formation[M].rvol_21d.missing is PriceMissing.SCREEN_FLAGGED
    assert got.flags_by_year == {2019: 2}


def test_a_jump_after_s_m_does_not_screen_the_formation() -> None:
    k = SESSIONS.index(M)
    got = _run(_bars(lambda i, d: _growing(i, d) * (5.0 if i > k else 1.0)))
    assert got.by_formation[M].rvol_21d.missing is None
    assert got.flags_by_year == {2019: 2}  # both bars still flagged in the census


def test_a_pair_ending_at_the_window_start_screens_it() -> None:
    k = SESSIONS.index(M)
    got = _run(_bars(lambda i, d: _growing(i, d) * (5.0 if i >= k - 20 else 1.0)))
    assert got.by_formation[M].rvol_21d.missing is PriceMissing.SCREEN_FLAGGED


def test_unstamped_ratio_jump_flags_and_a_stamp_excuses_it() -> None:
    k = SESSIONS.index(M)
    jump_day = SESSIONS[k - 5]

    def bars(stamped: bool) -> list[DailyBar]:
        out = []
        for i, day in enumerate(SESSIONS):
            close = 10.0 if i < k - 5 else 20.0  # raw close doubles, adj_close does not move
            out.append(DailyBar(day, close, 10.0, 1000, stamped=stamped and day == jump_day, usable=True))
        return out

    assert _run(bars(False)).by_formation[M].rvol_21d.missing is PriceMissing.SCREEN_FLAGGED
    assert _run(bars(True)).by_formation[M].rvol_21d.missing is None


def test_missing_rf_inside_the_window_refuses() -> None:
    with pytest.raises(PanelError, match="RF missing"):
        _run(_bars(_growing), rf=None)


def test_dollar_volume_needs_63_bars_in_126_sessions() -> None:
    k = SESSIONS.index(M)
    full = _run(_bars(_growing, close=2.0, volume=50)).by_formation[M]
    assert (full.dollar_volume, full.dollar_volume_bars) == (100.0, 126)
    thin = _run(_bars(lambda i, d: _growing(i, d) if i > k - 62 else None)).by_formation[M]
    assert (thin.dollar_volume, thin.dollar_volume_bars) == (None, 62)


def test_observed_holding_runs_from_s_m_to_month_end() -> None:
    got = _run(_bars(_growing)).by_formation[M]
    k, end = SESSIONS.index(M), SESSIONS.index(HELD_LAST)
    assert got.holding.status is HoldingStatus.OBSERVED
    assert got.holding.by_arm["best_case"] == pytest.approx(1.001 ** (end - k) - 1.0)
    assert got.daily_monthly is DailyMonthly.AGREE
    assert got.month_end_after_decision is False


def _ends_on(last: date) -> Callable[[int, date], float | None]:
    return lambda i, d: _growing(i, d) if d <= last else None


def test_terminal_holding_realises_the_partial_month_then_the_class_fraction_per_arm() -> None:
    last = date(2019, 11, 15)
    got = _run(_bars(_ends_on(last)), termination=(TerminationClass.UNKNOWN, last)).by_formation[M]
    partial = 1.001 ** (SESSIONS.index(last) - SESSIONS.index(M)) - 1.0
    assert got.holding.status is HoldingStatus.TERMINAL
    assert got.holding.end_bar == last
    assert got.holding.by_arm["best_case"] == pytest.approx(partial)
    assert got.holding.by_arm["worst_case"] == pytest.approx((1 + partial) * (1 - SHUMWAY_HAIRCUT) - 1)


def test_terminal_with_no_bar_in_the_holding_month_starts_from_zero() -> None:
    got = _run(_bars(_ends_on(M)), termination=(TerminationClass.EXCHANGE_FAILURE, M)).by_formation[M]
    assert (got.holding.status, got.holding.period_return, got.holding.end_bar) == (HoldingStatus.TERMINAL, 0.0, None)
    assert got.holding.by_arm == {"best_case": -SHUMWAY_HAIRCUT, "worst_case": -SHUMWAY_HAIRCUT}
    assert got.daily_monthly is DailyMonthly.NO_MONTHLY


def test_live_series_ending_early_is_a_coverage_exit_at_the_partial_return() -> None:
    last = date(2019, 11, 15)
    got = _run(_bars(_ends_on(last))).by_formation[M]
    assert got.holding.status is HoldingStatus.COVERAGE_EXIT
    assert got.holding.by_arm["worst_case"] == got.holding.period_return > 0


def test_amendment_3_caps_an_observed_holding_and_keeps_the_raw_value() -> None:
    jump = _run(_bars(lambda i, d: _growing(i, d) * (250.0 if d > M else 1.0)))
    holding = jump.by_formation[M].holding
    cap = ReturnBounds((2019, 11), -0.75, 1.5)
    capped = _run(_bars(lambda i, d: _growing(i, d) * (250.0 if d > M else 1.0)), bounds={(2019, 11): cap})
    got = capped.by_formation[M].holding
    assert holding.raw_by_arm == got.raw_by_arm  # the raw value does not depend on the bounds
    assert got.raw_by_arm["best_case"] > 200
    assert got.by_arm == {"best_case": 1.5, "worst_case": 1.5}
    assert (got.bounds, got.period_return, got.status) == (cap, holding.period_return, HoldingStatus.OBSERVED)


def test_amendment_3_clips_after_the_terminal_imputation_per_arm() -> None:
    last = date(2019, 11, 15)
    floor = ReturnBounds((2019, 11), -0.1, 1.5)
    got = _run(_bars(_ends_on(last)), termination=(TerminationClass.UNKNOWN, last), bounds={(2019, 11): floor})
    holding = got.by_formation[M].holding
    partial = 1.001 ** (SESSIONS.index(last) - SESSIONS.index(M)) - 1.0
    assert holding.raw_by_arm["worst_case"] == pytest.approx((1 + partial) * (1 - SHUMWAY_HAIRCUT) - 1)
    assert holding.by_arm == {"best_case": pytest.approx(partial), "worst_case": -0.1}


def test_amendment_3_refuses_a_holding_month_without_cutoffs() -> None:
    with pytest.raises(PanelError, match="no JKP return cutoff row for holding month"):
        _run(_bars(_growing), bounds={(2019, 10): ReturnBounds((2019, 10), -1.0, 10.0)})


@pytest.mark.parametrize(("low", "high"), [(0.5, -0.5), (math.nan, 1.0), (-1.0, math.inf)])
def test_return_bounds_refuse_an_inverted_or_non_finite_pair(low: float, high: float) -> None:
    with pytest.raises(PanelError, match="JKP return cutoffs"):
        ReturnBounds((2019, 11), low, high)


def test_a_terminating_series_that_trades_through_the_month_is_observed() -> None:
    got = _run(_bars(_growing), termination=(TerminationClass.UNKNOWN, date(2019, 12, 31))).by_formation[M]
    assert got.holding.status is HoldingStatus.OBSERVED


def test_a_missing_session_in_the_holding_month_is_a_reconciliation_gap() -> None:
    gap_day = date(2019, 11, 12)
    got = _run(_bars(lambda i, d: None if d == gap_day else _growing(i, d))).by_formation[M]
    assert got.daily_monthly is DailyMonthly.GAP
    assert got.holding.status is HoldingStatus.OBSERVED


def test_no_bar_on_s_m_means_no_formation_row() -> None:
    assert M not in _run(_bars(lambda i, d: None if d == M else _growing(i, d))).by_formation


def test_unusable_bars_are_ignored_but_their_stamps_count() -> None:
    bars = _bars(_growing)
    k = SESSIONS.index(M)
    bars[k - 3] = DailyBar(SESSIONS[k - 3], 10.0, math.nan, 1000, stamped=False, usable=False)
    got = _run(bars).by_formation[M]
    assert got.rvol_21d.observations == 19


def test_grid_refuses_a_decision_without_lookback() -> None:
    with pytest.raises(PanelError, match="lookbacks"):
        SessionGrid.build(SESSIONS, {}, {SESSIONS[10]: SESSIONS[10]})


def test_liquidity_terciles_nearest_rank_with_name_key_ties() -> None:
    volumes = {1: 5.0, 2: 5.0, 3: 1.0, 4: 9.0, 5: None}
    keys = {1: 20, 2: 10, 3: 30, 4: 40, 5: 50}
    # n = 4: ranks 1-2 low, 3 middle, 4 high; the tie at 5.0 orders series 2 (key 10) before series 1.
    assert liquidity_terciles(volumes, keys) == {3: 1, 2: 1, 1: 2, 4: 3}


def test_me_discontinuity_band() -> None:
    assert me_discontinuity(1.1, 1.0) is None
    assert me_discontinuity(2.0, 1.0) == 2.0
    assert me_discontinuity(1.0, 1.3) == pytest.approx(1 / 1.3)


def test_an_excused_ratio_move_is_counted_unless_a_split_factor_explains_it() -> None:
    j = SESSIONS.index(M) - 40
    bars = [
        DailyBar(b.bar_date, 20.0 if i >= j else 10.0, b.adj_close, b.volume, stamped=i == j, usable=True)
        for i, b in enumerate(_bars(_growing))
    ]
    grid = _grid()
    unexplained = series_prices(bars, grid, holding_last_session={M: HELD_LAST}, termination=None, return_bounds=WIDE)
    assert unexplained.excused_unexplained_by_year == {2019: 1}
    assert unexplained.flags_by_year == {}  # excused: not screened
    split = [SplitStamp(SESSIONS[j], Decimal("0.5"))]
    explained = series_prices(
        bars, grid, holding_last_session={M: HELD_LAST}, termination=None, return_bounds=WIDE, split_stamps=split
    )
    assert explained.excused_unexplained_by_year == {}
    wrong_way = [SplitStamp(SESSIONS[j], Decimal(2))]  # predicts the ratio doubling, not halving
    reversed_ = series_prices(
        bars, grid, holding_last_session={M: HELD_LAST}, termination=None, return_bounds=WIDE, split_stamps=wrong_way
    )
    assert reversed_.excused_unexplained_by_year == {2019: 1}


def test_the_liquidity_window_reports_a_screened_bar() -> None:
    k = SESSIONS.index(M)
    assert _run(_bars(_growing)).by_formation[M].liquidity_screened is False
    got = _run(_bars(lambda i, d: _growing(i, d) * (5.0 if i >= k - 100 else 1.0)))
    assert got.by_formation[M].liquidity_screened is True
