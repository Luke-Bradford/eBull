"""#3740 parent slice 2: the step-3 path with cash, missed formations and the first actual purchase (synthetic)."""

from __future__ import annotations

import math
import random
from collections.abc import Mapping, Sequence
from datetime import date

import pytest

from app.services import factor_book_path as step2
from app.services.factor_book_path import HoldingReturn, TradeCategory
from app.services.factor_panel_prices import HoldingStatus
from app.services.vw_factor_book_path import Captured, Missed, Step, VwPathResult, value_path

OBSERVED, TERMINAL = HoldingStatus.OBSERVED, HoldingStatus.TERMINAL
#: Half-spread of the $20-100 band (``cost_model.BANDS``); every name closes at $50 here.
H_MID = 0.002545
ARM = "best_case"
TOL = 1e-12


def _ret(r: float, status: HoldingStatus = OBSERVED) -> HoldingReturn:
    return HoldingReturn(status, {"best_case": r, "worst_case": r})


def _captured(
    month: int,
    weights: Mapping[int, float],
    *,
    cash: float = 0.0,
    sales: Mapping[int, TradeCategory] | None = None,
    returns: Mapping[int, HoldingReturn] | None = None,
) -> Captured:
    return Captured(
        formation=date(2027, month, 28),
        targets=tuple(sorted(weights)),
        weights=dict(weights),
        cash=cash,
        sales=dict(sales or {}),
        close=dict.fromkeys(weights, 50.0),
        returns={**{n: _ret(0.0) for n in weights}, **(returns or {})},
    )


def _missed(month: int, returns: Mapping[int, HoldingReturn]) -> Missed:
    return Missed(date(2027, month, 28), "SESSION_MISSING", dict(returns))


def _conserved(result: VwPathResult) -> None:
    for row in result.ledger:
        before = row.pre_positions + row.pre_cash
        assert before == pytest.approx(row.post_positions + row.post_cash + row.cost, abs=TOL), row


# --------------------------------------------------------------------------- cash


def test_a_cash_weight_is_held_at_zero_return_and_costs_nothing() -> None:
    weights = dict.fromkeys(range(5), 0.1)
    result = value_path(
        [_captured(1, weights, cash=0.5, returns={n: _ret(0.10) for n in weights})], arm=ARM, cost_multiplier=1.0
    )
    cost = 0.5 * H_MID  # only the invested half trades
    assert result.ledger[0].cost == pytest.approx(cost, abs=TOL)
    assert result.ledger[0].post_cash == pytest.approx(0.5 * (1 - cost), abs=TOL)
    # Month 2: the invested half earns 10% and is liquidated at its band; the cash half earns nothing.
    invested = 0.5 * (1 - cost) * 1.10
    end = invested * (1 - H_MID) + 0.5 * (1 - cost)
    assert result.returns[(2027, 2)] == pytest.approx(end - 1.0, abs=TOL)
    _conserved(result)


def test_carried_cash_and_sale_proceeds_fund_the_next_rebalance() -> None:
    first = _captured(1, {1: 0.5}, cash=0.5)
    second = _captured(2, {2: 0.25, 3: 0.25}, cash=0.5, sales={1: TradeCategory.DISCRETIONARY_EXIT})
    result = value_path([first, second], arm=ARM, cost_multiplier=1.0)
    pre = result.ledger[1].pre_positions + result.ledger[1].pre_cash
    trades = {t.name: t for t in result.trades if t.month == (2027, 2)}
    assert trades[1].category is TradeCategory.DISCRETIONARY_EXIT
    assert trades[2].notional == pytest.approx(0.25 * pre, abs=TOL)
    # Turnover counts position trades only: sell 0.5 pre, buy 0.5 pre.
    assert result.turnover[(2027, 2)] == pytest.approx(0.5, abs=1e-9)
    _conserved(result)


def test_zero_targets_leave_the_book_all_cash_and_are_not_the_initial_purchase() -> None:
    result = value_path([_captured(1, {}, cash=1.0), _captured(2, {1: 1.0})], arm=ARM, cost_multiplier=1.0)
    assert result.initial_purchase == (2027, 2)
    assert {t.category for t in result.trades if t.month == (2027, 2)} == {TradeCategory.INITIAL_PURCHASE}
    assert (2027, 2) not in result.turnover


# --------------------------------------------------------------------------- missed formations


def test_a_missed_formation_trades_nothing_and_carries_positions_bands_and_cash() -> None:
    weights = {1: 0.4, 2: 0.4}
    steps: list[Step] = [
        _captured(1, weights, cash=0.2),
        _missed(2, {1: _ret(0.05), 2: _ret(-0.5, TERMINAL)}),
        _captured(3, {1: 1.0}),
    ]
    result = value_path(steps, arm=ARM, cost_multiplier=1.0)
    assert result.missed == {(2027, 2): "SESSION_MISSING"}
    assert not [t for t in result.trades if t.month == (2027, 2)]
    assert (2027, 2) not in result.turnover
    # Month 2 is the first holding month: it carries the initial purchase's cost and no trade of its own.
    assert result.returns[(2027, 2)] == pytest.approx(-0.8 * H_MID, abs=TOL)
    # The terminal name is realised into cash during month 3, which funds month 3's rebalance.
    post1 = 1 - 0.8 * H_MID
    assert [(r.month, r.name) for r in result.realisations] == [((2027, 3), 2)]
    row = result.ledger[2]
    assert row.pre_cash == pytest.approx(0.2 * post1 + 0.4 * post1 * 0.5, abs=TOL)
    assert row.pre_positions == pytest.approx(0.4 * post1 * 1.05, abs=TOL)
    # Name 1 keeps its entry formation and band through the miss.
    assert {t.category for t in result.trades if t.month == (2027, 3) and t.name == 1} == {TradeCategory.REBALANCE_ADD}
    _conserved(result)


def test_a_missed_first_formation_stays_cash_and_the_next_purchase_is_the_initial_one() -> None:
    steps: list[Step] = [_missed(1, {}), _captured(2, {1: 0.5, 2: 0.5})]
    result = value_path(steps, arm=ARM, cost_multiplier=1.0)
    assert result.initial_purchase == (2027, 2)
    assert (2027, 2) not in result.turnover
    # A delayed first purchase is charged in its own formation month's return.
    assert result.returns[(2027, 2)] == pytest.approx(-H_MID, abs=TOL)
    assert result.nav[(2027, 1)] == 1.0


def test_a_missed_formation_needs_a_return_for_every_carried_position() -> None:
    with pytest.raises(ValueError, match="no best_case return"):
        value_path([_captured(1, {1: 1.0}), _missed(2, {})], arm=ARM, cost_multiplier=1.0)


# --------------------------------------------------------------------------- refusals


@pytest.mark.parametrize(
    ("weights", "cash", "match"),
    [
        ({1: 0.5}, 0.4, "sum to 1"),
        ({1: 0.5}, 0.5000000005, "sum to 1"),  # Codex checkpoint 2: inside the old 1e-9 bound, it leaked NAV
        ({1: 1.1}, -0.1, "non-negative"),
        ({1: 0.0}, 1.0, "positive"),
        ({1: math.nan}, 0.0, "positive"),
    ],
)
def test_invalid_weights_or_cash_are_refused(weights: dict[int, float], cash: float, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        value_path([_captured(1, weights, cash=cash)], arm=ARM, cost_multiplier=1.0)


def test_sales_must_be_exactly_the_holdings_not_kept() -> None:
    with pytest.raises(ValueError, match="sales"):
        value_path([_captured(1, {1: 1.0}), _captured(2, {2: 1.0})], arm=ARM, cost_multiplier=1.0)


# --------------------------------------------------------------------------- equivalence and conservation


def _random_targets(rng: random.Random, held: Sequence[int]) -> tuple[int, ...]:
    kept = [n for n in held if rng.random() < 0.7]
    return tuple(sorted({*kept, *rng.sample(range(40), rng.randint(0, 6))}))


@pytest.mark.parametrize("seed", range(40))
def test_equal_weights_without_cash_agree_with_step_2s_path(seed: int) -> None:
    """A semantic cross-check, not a bitwise claim: step 3 is a new construction (explicit weights, ``fsum``), and
    step 2's artefacts keep coming from step 2's unchanged ``factor_book_path``."""
    rng = random.Random(f"3740-path:{seed}")
    held: tuple[int, ...] = ()
    ours: list[Step] = []
    theirs: list[step2.Decision] = []
    for month in range(1, 13):
        targets = _random_targets(rng, held) or (rng.randrange(40),)
        sales = {n: TradeCategory.DISCRETIONARY_EXIT for n in held if n not in targets}
        returns = {n: _ret(rng.gauss(0.01, 0.08)) for n in targets}
        close = {n: rng.choice([3.0, 12.0, 50.0, 150.0]) for n in targets}
        weights = {n: 1 / len(targets) for n in targets}
        ours.append(Captured(date(2027, month, 28), targets, weights, 0.0, sales, close, returns))
        theirs.append(step2.Decision(date(2027, month, 28), targets, sales, close, returns))
        held = targets
    a = value_path(ours, arm=ARM, cost_multiplier=1.0)
    b = step2.value_path(theirs, arm=ARM, cost_multiplier=1.0)
    assert a.returns.keys() == b.returns.keys() and a.turnover.keys() == b.turnover.keys()
    assert all(a.returns[m] == pytest.approx(b.returns[m], abs=TOL) for m in b.returns)
    assert all(a.turnover[m] == pytest.approx(b.turnover[m], abs=TOL) for m in b.turnover)
    assert [(t.month, t.name, t.category) for t in a.trades] == [(t.month, t.name, t.category) for t in b.trades]
    _conserved(a)


@pytest.mark.parametrize("seed", range(40))
def test_nav_is_conserved_at_every_formation_with_cash_misses_and_terminations(seed: int) -> None:
    rng = random.Random(f"3740-nav:{seed}")
    held: set[int] = set()
    steps: list[Step] = []
    for month in range(1, 13):
        if rng.random() < 0.2:
            returns = {n: _ret(rng.gauss(0.0, 0.1), rng.choice([OBSERVED, OBSERVED, TERMINAL])) for n in held}
            steps.append(_missed(month, returns))
        else:
            targets = _random_targets(rng, sorted(held))
            raw = {n: rng.random() + 0.01 for n in targets}
            invested = rng.choice([1.0, 0.7]) if targets else 0.0
            weights = {n: invested * v / math.fsum(raw.values()) for n, v in raw.items()}
            sales = {n: TradeCategory.FORCED_EXIT for n in held if n not in targets}
            returns = {n: _ret(rng.gauss(0.0, 0.1), rng.choice([OBSERVED, OBSERVED, TERMINAL])) for n in targets}
            steps.append(_captured(month, weights, cash=1.0 - invested, sales=sales, returns=returns))
        last = steps[-1]
        carried = set(last.targets) if isinstance(last, Captured) else held
        held = {n for n in carried if last.returns[n].status is OBSERVED}
    result = value_path(steps, arm=ARM, cost_multiplier=2.0)
    assert len(result.ledger) == 12
    _conserved(result)
    # Wealth chains: the product of monthly factors is the final NAV (the first factor's denominator is 1.0).
    assert math.prod(1 + r for r in result.returns.values()) == pytest.approx(result.nav[(2028, 1)], rel=1e-12)
