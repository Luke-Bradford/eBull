"""#3609 step 2 slice 3c-v(g): the per-band sub-books' monthly series and trades by band, on synthetic decisions valued
by the real ``value_path`` (no corpus, no stage B)."""

from __future__ import annotations

import math
import random
from collections.abc import Mapping, Sequence
from datetime import date

import pytest

from app.services.factor_book_path import (
    Decision,
    HoldingReturn,
    Month,
    PathResult,
    TradeCategory,
    month_of,
    next_month,
    value_path,
)
from app.services.factor_book_references import B1Path
from app.services.factor_book_series import ARMS, Scenario, SeriesRun
from app.services.factor_panel_prices import HoldingStatus
from app.services.strategy_result import AmbiguityArm
from scripts.report_3609_baselines import FACTOR_REGRESSORS, newey_west_lag
from scripts.report_3609_step2_subbooks import (
    THIN_NAMES,
    MonthStatus,
    all_sub_books,
    all_window_metrics,
    band_at,
    net_return,
    sub_book_month,
    sub_books,
    window_trades,
    windows_of,
)
from scripts.report_3609_step2_verdict import BASE, GROSS

ARM: AmbiguityArm = "worst_case"
LOW, MID = "$5-20", "$20-100"
F1, F2, F3 = date(2021, 3, 31), date(2021, 4, 30), date(2021, 5, 31)


def _held(r: float, status: HoldingStatus = HoldingStatus.OBSERVED) -> HoldingReturn:
    return HoldingReturn(status, {ARM: r})


def _decision(
    formation: date,
    targets: Sequence[int],
    close: Mapping[int, float],
    returns: Mapping[int, HoldingReturn],
    sales: Mapping[int, TradeCategory] | None = None,
) -> Decision:
    return Decision(formation, tuple(targets), dict(sales or {}), dict(close), dict(returns))


def _decisions() -> list[Decision]:
    """Six $50 names (1..6) and two $10 names (7, 8). At F2 name 8 is sold, now at $50; name 9 enters at $10 and its
    holding month ends terminal. At F3 name 9's band holds only name 7."""
    first = {n: 50.0 for n in range(1, 7)} | {7: 10.0, 8: 10.0}
    returns1 = {n: _held(0.01 * n) for n in range(1, 9)}
    second_close = {n: 50.0 for n in range(1, 7)} | {7: 10.0, 8: 50.0, 9: 10.0}
    returns2 = {n: _held(0.02) for n in range(1, 8)} | {9: _held(-0.4, HoldingStatus.TERMINAL)}
    third_close = {n: 50.0 for n in range(1, 7)} | {7: 10.0}
    returns3 = {n: _held(-0.01) for n in range(1, 8)}
    return [
        _decision(F1, range(1, 9), first, returns1),
        _decision(F2, [*range(1, 8), 9], second_close, returns2, {8: TradeCategory.DISCRETIONARY_EXIT}),
        _decision(F3, range(1, 8), third_close, returns3),
    ]


def _path(decisions: Sequence[Decision], multiplier: float = 1.0) -> PathResult:
    return value_path(decisions, arm=ARM, cost_multiplier=multiplier)


def test_bands_partition_the_books_value_cost_and_turnover() -> None:
    decisions = _decisions()
    path = _path(decisions)
    books = sub_books(decisions, path, ARM)
    assert set(books) == {MID, LOW}
    for decision in decisions:
        month = month_of(decision.formation)
        held = (month[0], month[1] + 1)
        assert math.fsum(b.months[held].value for b in books.values()) == pytest.approx(path.nav[month], rel=1e-12)
    total_cost = math.fsum(t.cost for t in path.trades)
    assert math.fsum(c for b in books.values() for c in b.trades.cost.values()) == pytest.approx(total_cost)
    for month, book_turnover in path.turnover.items():
        assert math.fsum(b.trades.turnover[month] for b in books.values()) == pytest.approx(book_turnover)


def test_a_sale_is_reported_in_its_band_at_s_m_and_charged_at_its_entry_band() -> None:
    decisions = _decisions()
    path = _path(decisions)
    sale = next(t for t in path.trades if t.category is TradeCategory.DISCRETIONARY_EXIT)
    assert sale.name == 8 and sale.cost == pytest.approx(sale.notional * 0.002855)  # the $5-20 half-spread
    books = sub_books(decisions, path, ARM)
    f2 = month_of(F2)
    mid_costs = [t.cost for t in path.trades if t.month == f2 and (t.name == 8 or decisions[1].close[t.name] == 50.0)]
    assert books[MID].trades.cost[f2] == pytest.approx(math.fsum(mid_costs))


def test_the_monthly_return_is_the_ratio_form_on_band_capital_before_costs() -> None:
    decisions = _decisions()
    path = _path(decisions)
    row = sub_books(decisions, path, ARM)[MID].months[(2021, 4)]
    names = range(1, 7)
    value = math.fsum(decisions[0].share(path.nav[month_of(F1)], n) for n in names)
    gross = math.fsum(decisions[0].share(path.nav[month_of(F1)], n) * 0.01 * n for n in names) / value
    cost = math.fsum(t.cost for t in path.trades if t.month == month_of(F1) and t.name in names)
    assert (row.names, row.status, row.thin) == (6, MonthStatus.OK, False)
    assert row.value == pytest.approx(value) and row.gross == pytest.approx(gross) and row.cost == pytest.approx(cost)
    assert row.liquidation == 0.0
    assert row.net == pytest.approx((1 + gross) / (1 + cost / value) - 1)


def test_the_final_liquidation_is_charged_in_the_last_holding_month_only_by_its_s_last_band() -> None:
    decisions = _decisions()
    path = _path(decisions)
    books = sub_books(decisions, path, ARM)
    liquidation = [t for t in path.trades if t.category is TradeCategory.FINAL_LIQUIDATION]
    assert {t.month for t in liquidation} == {(2021, 6)}
    assert books[MID].months[(2021, 6)].liquidation == pytest.approx(
        math.fsum(t.cost for t in liquidation if t.name <= 6)
    )
    assert books[LOW].months[(2021, 6)].liquidation == pytest.approx(next(t.cost for t in liquidation if t.name == 7))
    assert all(r.liquidation == 0.0 for b in books.values() for m, r in b.months.items() if m != (2021, 6))
    # Excluded from turnover, printed separately with the initial purchase.
    assert books[LOW].trades.excluded[(2021, 6)] == pytest.approx(next(t.notional for t in liquidation if t.name == 7))
    assert (2021, 6) not in books[LOW].trades.turnover and month_of(F1) not in books[LOW].trades.turnover


def test_thin_months_are_flagged_and_a_band_with_no_holdings_is_undefined() -> None:
    decisions = _decisions()
    low = sub_books(decisions, _path(decisions), ARM)[LOW]
    assert all(r.thin and r.names < THIN_NAMES for r in low.months.values())
    assert [r.names for r in low.months.values()] == [2, 2, 1]
    single = [_decision(F1, range(1, 7), {n: 50.0 for n in range(1, 7)}, {n: _held(0.0) for n in range(1, 7)})]
    books = sub_books(single, _path(single), ARM)
    assert list(books) == [MID]
    empty = sub_book_month(single[0], LOW, ARM, 1.0, 0.0, 0.0)
    assert (empty.status, empty.names, empty.value, empty.net, empty.gross) == (
        MonthStatus.UNDEFINED,
        0,
        0.0,
        None,
        None,
    )


def test_terminal_realisations_are_printed_by_their_formation_band() -> None:
    decisions = _decisions()
    path = _path(decisions)
    books = sub_books(decisions, path, ARM)
    realised = next(r for r in path.realisations if r.name == 9)
    assert books[LOW].trades.realised == {(2021, 5): pytest.approx(realised.value)}
    assert books[MID].trades.realised == {}


def test_with_one_band_the_sub_book_factors_compound_to_the_books_final_nav() -> None:
    close = {n: 50.0 for n in range(1, 7)}
    decisions = [
        _decision(F1, range(1, 7), close, {n: _held(0.01 * n) for n in range(1, 7)}),
        _decision(F2, range(1, 6), close, {n: _held(-0.02) for n in range(1, 6)}, {6: TradeCategory.FORCED_EXIT}),
        _decision(
            F3, range(1, 6), close, {1: _held(-0.5, HoldingStatus.TERMINAL)} | {n: _held(0.03) for n in range(2, 6)}
        ),
    ]
    path = _path(decisions)
    (book,) = sub_books(decisions, path, ARM).values()
    assert math.prod(1.0 + r.net for r in book.months.values() if r.net is not None) == pytest.approx(
        path.nav[(2021, 6)], rel=1e-12
    )


def test_at_zero_cost_the_net_return_is_the_gross() -> None:
    decisions = _decisions()
    books = sub_books(decisions, _path(decisions, 0.0), ARM)
    rows = [r for b in books.values() for r in b.months.values() if r.status is MonthStatus.OK]
    assert rows and all(r.net == pytest.approx(r.gross, abs=1e-15) for r in rows)


@pytest.mark.parametrize(
    ("args", "reason"),
    [
        ((1.0, -1.5, 0.0, 0.0), "g_b"),
        ((1.0, math.nan, 0.0, 0.0), "g_b"),
        ((1.0, 0.0, -0.1, 0.0), "C_b"),
        ((1.0, 0.0, 0.0, math.inf), "L_b"),
        ((math.inf, 0.0, 0.0, 0.0), "V_b"),
        ((1.0, 0.0, 0.0, 1.0), "factor"),
        ((1e-300, 0.0, 1e300, 0.0), "quotient"),
        ((1.0, -0.9999999999999999, 1e10, 0.0), "exactly -1"),
    ],
)
def test_invalid_inputs_and_results_mark_the_month_invalid(
    args: tuple[float, float, float, float], reason: str
) -> None:
    net, why = net_return(*args)
    assert net is None and why is not None and reason in why


def test_an_invalid_holding_return_marks_the_month_invalid_not_a_refusal() -> None:
    decision = _decision(F1, range(1, 7), {n: 50.0 for n in range(1, 7)}, {n: _held(-2.0) for n in range(1, 7)})
    row = sub_book_month(decision, MID, ARM, 1.0, 0.0, 0.0)
    assert (row.status, row.net) == (MonthStatus.INVALID, None) and row.reason and "g_b" in row.reason


def test_a_name_without_a_close_at_s_m_is_a_caller_error() -> None:
    decision = _decision(F1, [1], {}, {1: _held(0.0)})
    with pytest.raises(ValueError, match="no raw close"):
        band_at(decision, 1)


def test_a_stopped_path_is_refused() -> None:
    decisions = _decisions()
    path = _path(decisions)
    path.nonpositive = (2021, 5)
    with pytest.raises(ValueError, match="stopped"):
        sub_books(decisions, path, ARM)


def test_every_scenario_of_the_books_path_gets_its_arms_sub_books() -> None:
    decisions = _decisions()
    gross, net = _path(decisions, 0.0), _path(decisions)
    run = SeriesRun((), {(ARM, "gross"): gross, (ARM, "net"): net}, {}, {}, {}, {}, (), ())
    books = all_sub_books(decisions, run)
    assert list(books) == [(ARM, "gross"), (ARM, "net")]
    assert books[(ARM, "net")] == sub_books(decisions, net, ARM)


# --------------------------------------------------------------------------- per window


def _months(first: Month, last: Month) -> list[Month]:
    out = [first]
    while out[-1] < last:
        out.append(next_month(out[-1]))
    return out


HELD = _months((2014, 10), (2024, 8))
FORMATIONS = [date(y, m, 28) for y, m in [(2014, 9), *HELD[:-1]]]
#: Six $50 names (1..6), five $10 names (7..11), two $150 names (12, 13): the last band is thin every month.
CLOSE = {n: 50.0 for n in range(1, 7)} | {n: 10.0 for n in range(7, 12)} | {12: 150.0, 13: 150.0}


def _long_decisions() -> list[Decision]:
    rng = random.Random(3609)
    return [
        Decision(
            formation,
            tuple(CLOSE),
            {},
            CLOSE,
            {n: HoldingReturn(HoldingStatus.OBSERVED, dict.fromkeys(ARMS, rng.gauss(0.01, 0.05))) for n in CLOSE},
        )
        for formation in FORMATIONS
    ]


def _factors(months: Sequence[Month]) -> dict[str, dict[Month, float]]:
    rng = random.Random(7)
    return {name: {m: rng.gauss(0.0, 0.03) for m in months} for name in (*FACTOR_REGRESSORS, "RF")}


def _long_run(decisions: Sequence[Decision]) -> SeriesRun:
    book: dict[Scenario, PathResult] = {
        (arm, cost): value_path(decisions, arm=arm, cost_multiplier=multiplier)
        for arm in ARMS
        for cost, multiplier in ((GROSS, 0.0), (BASE, 1.0))
    }
    b1 = B1Path({m: 0.008 for m in HELD}, MID, 0.0, 0.0)
    return SeriesRun(tuple(HELD), book, {}, {}, {}, {BASE: b1}, (), ())


def test_windows_are_stage_a_stage_b_and_pooled_over_holding_months() -> None:
    windows = windows_of(_long_run(_long_decisions()))
    assert [(w.label, w.months[0], w.months[-1]) for w in windows] == [
        ("stage A", (2014, 10), (2021, 5)),
        ("stage B", (2021, 6), (2024, 8)),
        ("pooled", (2014, 10), (2024, 8)),
    ]


def test_window_trades_split_the_formations_so_the_stages_sum_to_pooled() -> None:
    decisions = _long_decisions()
    run = _long_run(decisions)
    books = all_sub_books(decisions, run)
    a, b, pooled = (window_trades(books[(ARMS[0], BASE)][MID].trades, w, HELD[-1]) for w in windows_of(run))
    for field in ("notional", "cost", "excluded"):
        assert getattr(a, field) + getattr(b, field) == pytest.approx(getattr(pooled, field))
    # Stage B holds the liquidation and not the initial purchase; stage A the reverse.
    path = run.book[(ARMS[0], BASE)]
    first = math.fsum(t.notional for t in path.trades if t.month == (2014, 9) and t.name <= 6)
    last = math.fsum(t.notional for t in path.trades if t.category is TradeCategory.FINAL_LIQUIDATION and t.name <= 6)
    assert a.excluded == pytest.approx(first) and b.excluded == pytest.approx(last)
    # Annual turnover: the band's turnover summed over the window's formations, per year of months.
    band = books[(ARMS[0], BASE)][MID].trades.turnover
    assert b.turnover_per_yr == pytest.approx(
        math.fsum(v for m, v in band.items() if (2021, 5) <= m <= (2024, 7)) / (39 / 12)
    )


def test_a_thin_band_is_undefined_in_every_window_and_its_trades_still_print() -> None:
    decisions = _long_decisions()
    run = _long_run(decisions)
    metrics = all_window_metrics(decisions, run, _factors(HELD))
    thin = metrics[ARMS[0]][">=$100"]["stage B"]
    assert thin.undefined == "(2021, 6): thin (2 names)"
    assert (thin.stats, thin.regression, thin.summary, thin.insufficient) == ({}, None, None, False)
    assert thin.trades.cost > 0


def test_a_computable_window_prints_step_0s_metrics_g1_and_n_eff() -> None:
    decisions = _long_decisions()
    run = _long_run(decisions)
    books = all_sub_books(decisions, run)
    metrics = all_window_metrics(decisions, run, _factors(HELD))
    arm = ARMS[0]
    window = metrics[arm][MID]["pooled"]
    net = [books[(arm, BASE)][MID].months[m].net for m in HELD]
    assert window.undefined is None
    g = 12 / len(HELD) * math.fsum(math.log1p(r) for r in net if r is not None)
    assert window.stats["ann_net_g"] == pytest.approx(g) and window.stats["ann_net"] == pytest.approx(math.expm1(g))
    assert window.stats["active_vs_b1"] == pytest.approx(
        12 * (math.fsum(r for r in net if r is not None) / 119 - 0.008)
    )
    assert window.stats["cost_drag"] is not None and window.stats["cost_drag"] > 0
    assert window.regression is not None and window.regression.refusal is None and window.regression.lag == 4
    assert window.summary is not None and window.summary.defined == 119 and window.summary.n_eff is not None
    assert window.insufficient == (window.summary.n_eff < 24)
    stage_b = metrics[arm][MID]["stage B"]
    assert stage_b.regression is not None and stage_b.regression.lag == newey_west_lag(39)


def test_a_g1_refusal_leaves_the_window_computed_with_its_regression_undefined() -> None:
    decisions = _long_decisions()
    run = _long_run(decisions)
    factors = _factors(HELD[:-1])  # 2024-08 missing
    window = all_window_metrics(decisions, run, factors)[ARMS[0]][LOW]["stage B"]
    assert window.undefined is None and window.stats["ann_net"] is not None
    assert window.regression is not None and window.regression.refusal is not None
    assert window.regression.refusal.startswith("missing_factor_month")
