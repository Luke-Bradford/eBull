"""#3609 step 2 slice 3c-v(a): the operations diagnostics, on synthetic series only (no corpus, no stage B)."""

from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import date

import pytest

from app.services.factor_book_path import (
    BoundaryState,
    Decision,
    HoldingReturn,
    Month,
    PathResult,
    Trade,
    TradeCategory,
    next_month,
)
from app.services.factor_book_references import B1Path
from app.services.factor_book_series import ARMS, Scenario, SeriesRun, Summary
from app.services.factor_panel_prices import HoldingStatus
from scripts.report_3609_step2_operations import (
    book_stats,
    calendar_years,
    control_distributions,
    mid_rank,
    minimum_ticket,
    operations,
    stage_windows,
    status_weights,
    turnover_per_year,
    window_returns,
)

#: The smallest representable return above −1: 1 + r is 1.1e-16.
TINY = math.nextafter(-1.0, 0.0)
COSTS = {"gross": 0.0, "net": 0.01, "stress_2x": 0.02}
BOUNDARY: Month = (2021, 5)
STATE = BoundaryState(date(2021, 5, 31), (), 1.0, 1.0)


def _months(first: Month, last: Month) -> tuple[Month, ...]:
    out = [first]
    while out[-1] < last:
        out.append(next_month(out[-1]))
    return tuple(out)


MONTHS = _months((2014, 10), (2024, 8))
FORMATIONS = _months((2014, 9), (2024, 7))
#: Distinct per month so a misaligned window or a swapped series shows.
B1 = {m: 0.004 + 0.001 * (i % 5) for i, m in enumerate(MONTHS)}


def _trade(notional: float, pre_nav: float, month: Month = (2015, 1)) -> Trade:
    return Trade(month, 1, TradeCategory.ENTRY, notional, 0.0, pre_nav)


def _path(monthly: float, *, boundary_cost: float = 0.0, trades: tuple[Trade, ...] = ()) -> PathResult:
    return PathResult(
        returns=dict.fromkeys(MONTHS, monthly),
        nav={BOUNDARY: 1.0 - boundary_cost},
        turnover={m: 0.1 for m in FORMATIONS[1:]},
        holdings={f: 20 + i % 3 for i, f in enumerate(FORMATIONS)},
        trades=list(trades or (_trade(0.05, 1.0),)),
        boundary=STATE,
    )


def _run(control: tuple[float, ...] = (0.0, 0.005, 0.01), *, boundary_cost: float = 0.0) -> SeriesRun:
    """The book earns 1% a month less its cost scenario's monthly drag (gross 1%, net 0%, stress −1%); each draw a
    constant return less the same."""
    scenarios: list[Scenario] = [(arm, cost) for arm in ARMS for cost in COSTS]
    flat = PathResult(returns=dict.fromkeys(MONTHS, 0.0), boundary=STATE)

    def draw(r: float) -> Summary:
        return Summary(dict.fromkeys(MONTHS, r), {m: r * 10 for m in FORMATIONS[1:]}, {BOUNDARY: 1.0}, {}, None, STATE)

    return SeriesRun(
        months=MONTHS,
        book={s: _path(0.01 - COSTS[s[1]], boundary_cost=boundary_cost) for s in scenarios},
        equal_weight=dict.fromkeys(scenarios, flat),
        cap_weighted=dict.fromkeys(scenarios, flat),
        control={s: tuple(draw(r - COSTS[s[1]]) for r in control) for s in scenarios},
        b1={c: B1Path(dict(B1), "band", 0.0, 0.0) for c in COSTS},
        pools=(),
        insufficient=(),
    )


def _decision(formation: Month, statuses: Mapping[int, HoldingStatus]) -> Decision:
    returns = {n: HoldingReturn(s, dict.fromkeys(ARMS, 0.0)) for n, s in statuses.items()}
    first = date(*formation, 28)
    return Decision(first, tuple(statuses), {}, dict.fromkeys(statuses, 10.0), returns)


def _decisions() -> list[Decision]:
    """Four names each month; in 2016 one of them ends terminal."""
    out = []
    for f in FORMATIONS:
        last = HoldingStatus.TERMINAL if next_month(f)[0] == 2016 else HoldingStatus.OBSERVED
        out.append(
            _decision(f, {1: HoldingStatus.OBSERVED, 2: HoldingStatus.OBSERVED, 3: HoldingStatus.OBSERVED, 4: last})
        )
    return out


# --------------------------------------------------------------------------- windows


def test_the_stage_windows_charge_the_boundary_formation_to_both_stages_and_pooled_once() -> None:
    a, b, pooled = stage_windows(_run())
    assert (a.months[0], a.months[-1], len(a.months)) == ((2014, 10), (2021, 5), 80)
    assert a.formations == a.months and not a.from_boundary
    assert (b.months[0], b.months[-1], len(b.months)) == ((2021, 6), (2024, 8), 39)
    assert (b.formations[0], b.formations[-1]) == ((2021, 5), (2024, 7)) and b.from_boundary
    assert pooled.months == MONTHS and pooled.formations == MONTHS
    years = calendar_years(_run())
    assert [(w.label, len(w.months)) for w in (years[0], years[-1])] == [("2014", 3), ("2024", 8)]
    assert sum(len(w.months) for w in years) == 119


def test_turnover_per_year_reads_the_windows_formations() -> None:
    turnover = {m: 0.0 for m in FORMATIONS}
    turnover[(2021, 5)] = 1.2  # the boundary formation: stage B's, and stage A's last month
    a, b, pooled = stage_windows(_run())
    assert turnover_per_year(turnover, b) == pytest.approx(1.2 / (39 / 12))
    assert turnover_per_year(turnover, a) == pytest.approx(1.2 / (80 / 12))
    assert turnover_per_year(turnover, pooled) == pytest.approx(1.2 / (119 / 12))


def test_stage_b_folds_the_boundary_cost_and_the_other_windows_do_not() -> None:
    path = _path(0.01, boundary_cost=0.02)
    a, b, _ = stage_windows(_run())
    assert window_returns(path.returns, path.nav, path.boundary, b)[(2021, 6)] == pytest.approx(1.01 * 0.98 - 1)
    assert window_returns(path.returns, path.nav, path.boundary, a)[(2021, 5)] == 0.01


# --------------------------------------------------------------------------- the minimum ticket


def test_the_minimum_ticket_takes_each_maximum_over_every_trade_separately() -> None:
    # NAV thresholds 100, 400, 250; initial-capital thresholds 100, 200, 500.
    trades = (_trade(0.1, 1.0), _trade(0.05, 2.0, (2016, 3)), _trade(0.02, 0.5, (2024, 8)))
    ticket = minimum_ticket(_path(0.0, trades=trades))
    assert (ticket.nav, ticket.nav_trade) == (pytest.approx(400.0), trades[1])
    assert (ticket.capital, ticket.capital_trade) == (pytest.approx(500.0), trades[2])


def test_a_zero_notional_trade_falls_below_the_ticket_at_any_nav() -> None:
    trades = (_trade(0.1, 1.0), _trade(0.0, 1.0, (2016, 3)))
    ticket = minimum_ticket(_path(0.0, trades=trades))
    assert ticket.nav == math.inf and ticket.nav_trade == trades[1] and ticket.capital == math.inf


# --------------------------------------------------------------------------- the book


def test_status_weights_average_the_weight_held_into_each_month_by_its_ending_status() -> None:
    years = {w.label: w for w in calendar_years(_run())}
    weights = status_weights(_decisions(), years["2016"])
    assert weights[HoldingStatus.TERMINAL] == pytest.approx(0.25)
    assert weights[HoldingStatus.OBSERVED] == pytest.approx(0.75)
    assert weights[HoldingStatus.COVERAGE_EXIT] == 0.0
    # 2015-12's holdings (formed then) end in 2016-01: they belong to 2016, not 2015.
    assert status_weights(_decisions(), years["2015"])[HoldingStatus.TERMINAL] == 0.0


def test_book_stats_use_the_net_series_against_b1_and_the_windows_turnover() -> None:
    run = _run()
    _, b, _ = stage_windows(run)
    stats = book_stats(run, _decisions(), ARMS[0], b)
    assert stats["ann_net"] == pytest.approx(0.0)
    assert stats["ann_gross"] == pytest.approx(1.01**12 - 1)
    assert stats["ann_stress_2x"] == pytest.approx(0.99**12 - 1)
    active = [0.0 - B1[m] for m in b.months]
    assert stats["active_vs_b1"] == pytest.approx(12 * sum(active) / len(active))
    assert stats["turnover_per_yr"] == pytest.approx(0.1 * 39 / (39 / 12))
    assert stats["book_size"] == pytest.approx(sum(20 + FORMATIONS.index(f) % 3 for f in b.formations) / 39)


def test_book_stats_stay_defined_where_the_wealth_product_would_overflow() -> None:
    # 2014-15 take NAV to ~5e-239 (15 factors of 1.1e-16, the smallest 1 + r above 0); 2016 then gains 1e400 in two
    # months, to a finite ~5e161 that a raw product over the year overflows.
    run = _run()
    path = run.book[(ARMS[0], "net")]
    for m in MONTHS:
        path.returns[m] = {2014: TINY, 2015: TINY, 2016: 1e200 - 1.0 if m[1] <= 2 else 0.0}.get(m[0], 0.0)
    year = {w.label: w for w in calendar_years(run)}["2016"]
    stats = book_stats(run, _decisions(), ARMS[0], year)
    assert stats["ann_net_g"] == pytest.approx(2 * math.log(1e200))
    assert stats["ann_net"] == math.inf  # printed "outside representable range" beside the finite G
    assert stats["max_dd_monthly"] == 0.0
    assert stats["ann_vol"] is None and stats["cost_drag"] is None
    assert book_stats(run, _decisions(), ARMS[0], {w.label: w for w in calendar_years(run)}["2015"])[
        "max_dd_monthly"
    ] == pytest.approx(-1.0)


# --------------------------------------------------------------------------- the control


def test_mid_rank_counts_half_the_ties() -> None:
    assert mid_rank([1.0, 2.0, 2.0, 3.0], 2.0) == 0.5
    assert mid_rank([1.0, 2.0, 2.0, 3.0], 4.0) == 1.0
    assert mid_rank([1.0, 2.0, 2.0, 3.0], 0.0) == 0.0


def test_control_distributions_compare_g_and_take_nearest_rank_quantiles() -> None:
    run = _run(control=(0.0, 0.005, 0.01, 0.02))
    _, _, pooled = stage_windows(run)
    out = control_distributions(run, ARMS[0], pooled)
    net = out["net_g"]
    # Net draws −1%, −0.5%, 0%, +1% a month; the book nets 0%, tying the third: (2 + 0.5) / 4.
    assert net.quantiles == pytest.approx(tuple(12 * math.log(1 + r) for r in (-0.01, -0.005, 0.01)))
    assert net.book == 0.0 and net.book_percentile == 0.625
    assert out["gross_g"].book_percentile == 0.625
    assert out["turnover_per_yr"].book == pytest.approx(0.1 * 118 / (119 / 12))
    assert out["cost_drag"].book == pytest.approx(1.01**12 - 1)


def test_the_control_stage_b_figures_fold_the_boundary_cost() -> None:
    run = _run(control=(0.01,), boundary_cost=0.05)
    _, b, _ = stage_windows(run)
    net = control_distributions(run, ARMS[0], b)["net_g"]
    assert net.book < net.quantiles[0] and net.book_percentile == 0.0


def test_operations_assembles_every_arm_and_window() -> None:
    out = operations(_run(), _decisions())
    assert set(out.minimum_ticket) == set(ARMS)
    for arm in ARMS:
        assert list(out.book[arm])[:3] == ["stage A", "stage B", "pooled"] and len(out.book[arm]) == 3 + 11
        assert list(out.control[arm]) == ["stage A", "stage B", "pooled"]
