"""#3609 step 2 slice 3b: the book's holdings rule and its self-financing path, on synthetic formations only."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import date

import pytest

from app.services.factor_book import BookRefusal, Exact, bands
from app.services.factor_book_path import (
    MIN_HOLDINGS,
    BookDecisions,
    Decision,
    ExitReason,
    Formation,
    HoldingReturn,
    TradeCategory,
    archive_seasoned,
    book_decisions,
    value_path,
)
from app.services.factor_panel_prices import HoldingStatus

OBSERVED, TERMINAL, COVERAGE_EXIT = HoldingStatus.OBSERVED, HoldingStatus.TERMINAL, HoldingStatus.COVERAGE_EXIT
SEASONED = date(2000, 1, 3)
#: Half-spreads of the $20-100 and >=$100 bands (``cost_model.BANDS``).
H_MID, H_HIGH = 0.002545, 0.00161


def _formation(
    month: int,
    order: Sequence[int],
    *,
    universe: Sequence[int] | None = None,
    close: Mapping[int, float] | None = None,
    first_bar: Mapping[int, date] | None = None,
    returns: Mapping[int, HoldingReturn | float] | None = None,
    year: int = 2015,
) -> Formation:
    """Formation ``year-month``; ``order`` is the composite order (best first); every name closes at $50."""
    names = list(universe if universe is not None else order)
    composite = {name: Exact.raw(float(-rank)) for rank, name in enumerate(order)}
    got = {n: r if isinstance(r, HoldingReturn) else _observed(r) for n, r in (returns or {}).items()}
    day = date(year, month, 28)
    return Formation(
        formation=day,
        session=day,
        universe=frozenset(names),
        bands=bands(composite),
        close={**{n: 50.0 for n in names}, **(close or {})},
        first_bar={**{n: SEASONED for n in names}, **(first_bar or {})},
        returns={**{n: _observed(0.0) for n in names}, **got},
    )


def _observed(r: float) -> HoldingReturn:
    return HoldingReturn(OBSERVED, {"best_case": r, "worst_case": r})


def _decision(
    month: int,
    targets: Sequence[int],
    *,
    sales: Mapping[int, TradeCategory] | None = None,
    close: Mapping[int, float] | None = None,
    returns: Mapping[int, HoldingReturn | float] | None = None,
    year: int = 2015,
) -> Decision:
    got = {n: r if isinstance(r, HoldingReturn) else _observed(r) for n, r in (returns or {}).items()}
    return Decision(
        formation=date(year, month, 28),
        targets=tuple(targets),
        sales=dict(sales or {}),
        close={**{n: 50.0 for n in targets}, **(close or {})},
        returns={**{n: _observed(0.0) for n in targets}, **got},
    )


# --------------------------------------------------------------------------- seasoning


def test_archive_seasoning_is_36_calendar_months_inclusive_with_the_day_clamped() -> None:
    assert archive_seasoned(date(2014, 9, 30), date(2017, 9, 29)) is False
    assert archive_seasoned(date(2014, 9, 29), date(2017, 9, 29)) is True
    # 2017-02-28 − 36 months clamps to 2014-02-28.
    assert archive_seasoned(date(2014, 2, 28), date(2017, 2, 28)) is True
    assert archive_seasoned(date(2014, 3, 1), date(2017, 2, 28)) is False


# --------------------------------------------------------------------------- holdings rule


def test_entry_needs_the_top_decile_five_dollars_and_seasoning() -> None:
    order = list(range(30))  # decile = ranks 1..3, tercile = 1..10
    formation = _formation(1, order, close={1: 4.99}, first_bar={2: date(2014, 1, 29)})
    got = book_decisions([formation])
    # 0 enters; 1 is below $5 at s(M); 2 is seasoned only 35 months; 3..9 are in the tercile but not the decile.
    assert got.decisions[0].targets == (0,)


def test_a_holding_stays_in_the_tercile_and_leaves_below_it_as_a_discretionary_exit() -> None:
    first = _formation(1, list(range(30)))
    # Month 2: 0 drops to rank 10 (in the tercile, kept), 1 to rank 11 (out), 2 stays at the top.
    second = _formation(2, [2, 10, 11, 12, 13, 14, 15, 16, 17, 0, 1, *range(3, 10), *range(18, 30)])
    got = book_decisions([first, second])
    assert got.decisions[0].targets == (0, 1, 2)
    step = got.decisions[1]
    assert step.targets == (0, 2, 10, 11)
    assert step.sales == {1: TradeCategory.DISCRETIONARY_EXIT}
    assert step.reasons == {1: (ExitReason.LEFT_TERCILE,)}
    assert step.discretionary == 1


def test_forced_exits_take_the_first_reason_in_order_and_flag_the_rest() -> None:
    first = _formation(1, list(range(30)))
    # 0 leaves the universe; 1 stays but has no composite; 2 closes at $4.
    second = _formation(2, [2, *range(3, 32)], universe=[1, 2, *range(3, 32)], close={0: 50.0, 2: 4.0})
    got = book_decisions([first, second]).decisions[1]
    assert got.reasons == {
        0: (ExitReason.LEFT_UNIVERSE, ExitReason.LOST_COMPOSITE, ExitReason.LEFT_TERCILE),
        1: (ExitReason.LOST_COMPOSITE, ExitReason.LEFT_TERCILE),
        2: (ExitReason.BELOW_PRICE_FLOOR,),
    }
    assert set(got.sales.values()) == {TradeCategory.FORCED_EXIT}
    assert got.discretionary == 0


def test_a_held_name_below_five_dollars_in_the_decile_is_sold_and_cannot_re_enter_at_once() -> None:
    first = _formation(1, list(range(30)))
    second = _formation(2, list(range(30)), close={0: 4.0})
    got = book_decisions([first, second]).decisions[1]
    assert 0 not in got.targets
    assert got.reasons[0] == (ExitReason.BELOW_PRICE_FLOOR,)


def test_a_young_holding_is_not_sold_for_its_age() -> None:
    """Seasoning applies at entry only."""
    first = _formation(1, list(range(30)))
    second = _formation(2, list(range(30)), first_bar={0: date(2015, 1, 1)})
    assert 0 in book_decisions([first, second]).decisions[1].targets


@pytest.mark.parametrize("bad", [None, 0.0, -1.0, math.nan, math.inf])
def test_a_universe_name_without_a_valid_raw_close_refuses_even_if_it_would_never_trade(bad: float | None) -> None:
    formation = _formation(1, list(range(30)))
    close = {k: v for k, v in formation.close.items() if k != 25}
    if bad is not None:
        close[25] = bad
    with pytest.raises(BookRefusal) as caught:
        book_decisions([replace(formation, close=close)])
    assert caught.value.code == "PRICE_INVALID"


def test_a_holding_that_left_the_universe_still_needs_its_raw_close() -> None:
    second = _formation(2, list(range(3, 33)))
    close = {k: v for k, v in second.close.items() if k != 0}
    with pytest.raises(BookRefusal, match="PRICE_INVALID"):
        book_decisions([_formation(1, list(range(30))), replace(second, close=close)])


@pytest.mark.parametrize("status", [TERMINAL, COVERAGE_EXIT])
def test_a_terminal_or_coverage_exit_holding_is_cash_at_the_next_formation_and_re_enters_as_new(
    status: HoldingStatus,
) -> None:
    ended = HoldingReturn(status, {"best_case": -0.1, "worst_case": -0.6})
    first = _formation(1, list(range(30)), returns={0: ended})
    second = _formation(2, list(range(30)))
    got = book_decisions([first, second]).decisions
    assert got[1].sales == {}  # no sale: it was already cash
    assert got[1].targets == (0, 1, 2)
    path = value_path(got, arm="worst_case", cost_multiplier=1.0)
    assert [(r.name, r.status) for r in path.realisations] == [(0, status)]
    entries = [t for t in path.trades if t.month == (2015, 2) and t.category is TradeCategory.ENTRY]
    assert [t.name for t in entries] == [0]


def test_fewer_than_ten_holdings_after_trades_marks_the_formation_insufficient() -> None:
    enough = _formation(1, list(range(MIN_HOLDINGS * 10)))  # decile of 100 names = 10
    short = _formation(2, list(range(90)))  # decile 9; the ten held stay in the tercile
    thin = _formation(3, list(range(50, 140)), close=dict.fromkeys(range(10), 50.0))  # every holding leaves
    got: BookDecisions = book_decisions([enough, short, thin])
    assert [len(d.targets) for d in got.decisions] == [10, 10, 9]
    assert got.insufficient == (date(2015, 3, 28),)


def test_a_holding_without_a_holding_month_return_is_a_contract_error() -> None:
    formation = _formation(1, list(range(30)))
    returns = {k: v for k, v in formation.returns.items() if k != 0}
    with pytest.raises(ValueError, match="no holding-month return"):
        book_decisions([replace(formation, returns=returns)])


# --------------------------------------------------------------------------- valuation


def test_a_two_name_path_matches_the_one_pass_rule_by_hand() -> None:
    # Formation 1 buys A=1 at $50 (mid band) and B=2 at $150 (high band); month 2: A +10%, B −10%.
    # Formation 2 keeps both (no sale): rebalance at A's and B's own entry bands, B now closing at $90.
    # Month 3: both +0%; the path ends with the liquidation.
    one = _decision(1, [1, 2], close={2: 150.0}, returns={1: 0.10, 2: -0.10})
    two = _decision(2, [1, 2], close={2: 90.0})
    path = value_path([one, two], arm="best_case", cost_multiplier=1.0)

    buy = 0.5 * H_MID + 0.5 * H_HIGH
    post1 = 1.0 - buy
    a, b = 0.5 * post1 * 1.1, 0.5 * post1 * 0.9
    pre2 = a + b
    target = pre2 / 2
    cost2 = (a - target) * H_MID + (target - b) * H_HIGH  # B keeps its >=$100 band at $90
    post2 = pre2 - cost2
    liquidation = post2 / 2 * H_MID + post2 / 2 * H_HIGH

    assert path.returns[(2015, 2)] == pytest.approx(post2 / 1.0 - 1.0, abs=1e-15)  # initial purchase charged here
    assert path.returns[(2015, 3)] == pytest.approx((post2 - liquidation) / post2 - 1.0, abs=1e-15)
    assert list(path.returns) == [(2015, 2), (2015, 3)]
    assert path.turnover == {(2015, 2): pytest.approx(((a - target) + (target - b)) / 2 / pre2, abs=1e-15)}
    assert path.nav[(2015, 3)] == pytest.approx(post2 - liquidation, abs=1e-15)
    categories = {(t.month, t.name): t.category for t in path.trades}
    assert categories == {
        ((2015, 1), 1): TradeCategory.INITIAL_PURCHASE,
        ((2015, 1), 2): TradeCategory.INITIAL_PURCHASE,
        ((2015, 2), 1): TradeCategory.REBALANCE_TRIM,
        ((2015, 2), 2): TradeCategory.REBALANCE_ADD,
        ((2015, 3), 1): TradeCategory.FINAL_LIQUIDATION,
        ((2015, 3), 2): TradeCategory.FINAL_LIQUIDATION,
    }
    # Order cost reconciles to the NAV lost to trading.
    assert sum(t.cost for t in path.trades) == pytest.approx(buy + cost2 + liquidation, abs=1e-15)


def test_gross_is_the_equal_weight_return_and_stress_doubles_every_charge() -> None:
    one = _decision(1, [1, 2], returns={1: 0.2, 2: 0.0})
    two = _decision(2, [1, 3], sales={2: TradeCategory.DISCRETIONARY_EXIT})
    gross = value_path([one, two], arm="best_case", cost_multiplier=0.0)
    assert gross.returns[(2015, 2)] == pytest.approx(0.1, abs=1e-15)
    assert gross.returns[(2015, 3)] == 0.0
    base = value_path([one, two], arm="best_case", cost_multiplier=1.0)
    stress = value_path([one, two], arm="best_case", cost_multiplier=2.0)
    assert stress.trades[0].cost == pytest.approx(2 * base.trades[0].cost, abs=1e-15)
    assert {t.category for t in base.trades if t.month == (2015, 2)} == {
        TradeCategory.DISCRETIONARY_EXIT,
        TradeCategory.ENTRY,
        TradeCategory.REBALANCE_TRIM,
    }


def test_the_arms_value_a_terminal_holding_differently() -> None:
    ended = HoldingReturn(TERMINAL, {"best_case": -0.2, "worst_case": -1.0})
    one = _decision(1, [1, 2], returns={1: ended})
    two = _decision(2, [2])
    best = value_path([one, two], arm="best_case", cost_multiplier=0.0)
    worst = value_path([one, two], arm="worst_case", cost_multiplier=0.0)
    assert best.returns[(2015, 2)] == pytest.approx(-0.1, abs=1e-15)
    assert worst.returns[(2015, 2)] == pytest.approx(-0.5, abs=1e-15)
    # The realised cash is redeployed at the next formation without a sale; worst case it realised nothing.
    assert [t.category for t in best.trades if t.month == (2015, 2)] == [TradeCategory.REBALANCE_ADD]
    assert [t for t in worst.trades if t.month == (2015, 2)] == []


def test_the_boundary_state_is_the_pre_trade_state_of_its_formation() -> None:
    ended = HoldingReturn(COVERAGE_EXIT, {"best_case": 0.0, "worst_case": 0.0})
    one = _decision(1, [1, 2], close={2: 150.0}, returns={1: 0.1, 2: ended})
    two = _decision(2, [1, 3], close={3: 10.0})
    path = value_path([one, two], arm="best_case", cost_multiplier=0.0, boundary=date(2015, 2, 28))
    state = path.boundary
    assert state is not None
    assert [(p.name, p.entry, p.band) for p in state.positions] == [(1, date(2015, 1, 28), "$20-100")]
    assert state.positions[0].value == pytest.approx(0.55, abs=1e-15)
    assert state.cash == pytest.approx(0.5, abs=1e-15)
    assert state.nav == pytest.approx(1.05, abs=1e-15)


def test_non_positive_wealth_stops_the_path_at_its_month() -> None:
    one = _decision(1, [1, 2], returns={1: -1.0, 2: -1.0})
    two = _decision(2, [1, 2])
    three = _decision(3, [1, 2])
    path = value_path([one, two, three], arm="best_case", cost_multiplier=0.0)
    assert path.nonpositive == (2015, 2)
    assert list(path.returns) == [(2015, 2)]
    assert path.returns[(2015, 2)] == -1.0


def test_an_empty_book_holds_cash_at_zero_and_re_enters_later() -> None:
    one = _decision(1, [1], returns={1: 0.1})
    two = _decision(2, [], sales={1: TradeCategory.FORCED_EXIT})
    three = _decision(3, [4])
    path = value_path([one, two, three], arm="best_case", cost_multiplier=0.0)
    assert path.returns[(2015, 3)] == 0.0
    assert path.holdings == {(2015, 1): 1, (2015, 2): 0, (2015, 3): 1}


def test_decisions_must_be_consecutive_and_sales_exactly_the_dropped_holdings() -> None:
    with pytest.raises(ValueError, match="consecutive"):
        value_path([_decision(1, [1]), _decision(3, [1])], arm="best_case", cost_multiplier=1.0)
    with pytest.raises(ValueError, match="sales must be exactly"):
        value_path([_decision(1, [1]), _decision(2, [2])], arm="best_case", cost_multiplier=1.0)


def test_the_book_rule_feeds_the_valuation_end_to_end() -> None:
    first = _formation(1, list(range(30)), returns={0: 0.05})
    second = _formation(2, [5, 6, 7, *range(8, 30), 0, 1, 2, 3, 4], returns={5: -0.02})
    got = book_decisions([first, second])
    path = value_path(got.decisions, arm="worst_case", cost_multiplier=1.0)
    by_category: dict[TradeCategory, float] = {}
    for trade in path.trades:
        by_category[trade.category] = by_category.get(trade.category, 0.0) + trade.notional
    assert set(by_category) == {
        TradeCategory.INITIAL_PURCHASE,
        TradeCategory.DISCRETIONARY_EXIT,
        TradeCategory.ENTRY,
        TradeCategory.FINAL_LIQUIDATION,
    }
    assert got.decisions[1].targets == (5, 6, 7)
    assert math.isfinite(path.returns[(2015, 3)])
