"""#3609 step 2 construction counts: uninformative groups, membership patterns and identifier-decided selections, with
the book's post-trade weight and its sold names' pre-trade weight, on synthetic inputs only (no corpus, no stage B)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date

import pytest

from app.services.factor_book import COMPOSITE, Bands, Exact, Scores, bands
from app.services.factor_book_path import Decision, HoldingReturn, PathResult, TradeCategory, value_path
from app.services.factor_book_series import ARMS, Scenario
from app.services.factor_panel_prices import HoldingStatus
from scripts.report_3609_step2_construction import (
    NO_SCORE,
    OPERATIONS,
    Affected,
    IdentifierDecided,
    SoldWeights,
    Uninformative,
    construction,
    construction_month,
    pattern,
    sold_weights,
)

A = date(2021, 4, 30)
M = date(2021, 5, 31)
UNIVERSE = (1, 2, 3, 4, 5, 6)
BEST, WORST = ARMS
GROSS: Scenario = (BEST, "gross")
#: No sales at M: every sold weight is an empty sum.
NO_SALES: SoldWeights = {GROSS: {}}


def _a(names: int, held: int, weight: float, sold: int = 0, sold_weight: float = 0.0) -> Affected:
    return Affected(names, held, weight, sold, {GROSS: sold_weight})


def _scores(scored: Mapping[str, Sequence[int]], uninformative: Mapping[tuple[str, str], tuple[int, ...]]) -> Scores:
    return Scores(
        by_operation={op: {n: Exact.raw(float(n)) for n in scored.get(op, ())} for op in OPERATIONS},
        uninformative=dict(uninformative),
    )


def _decision(
    targets: Sequence[int], weights: Mapping[int, float] | None = None, sales: Sequence[int] = ()
) -> Decision:
    return Decision(M, tuple(targets), dict.fromkeys(sales, TradeCategory.DISCRETIONARY_EXIT), {}, {}, weights=weights)


SCORES = _scores(
    {
        "be_me": (1, 2, 3),
        "ni_me": (1, 2),
        "ocf_me": (1, 2, 3, 4),
        "value": (1, 2, 3, 4),
        COMPOSITE: (1, 2, 3, 4),
    },
    {("ni_me", "Manuf"): (5, 6), ("ni_me", "HiTec"): (3,), ("value", "Shops"): (5,)},
)
BANDS = Bands(
    order=(1, 2, 3),
    decile=frozenset({1}),
    tercile=frozenset({1}),
    decile_by_identifier=frozenset({1, 2}),
    tercile_by_identifier=frozenset(),
)


def test_uninformative_groups_are_counted_per_operation_with_the_books_post_trade_weight() -> None:
    row = construction_month(UNIVERSE, SCORES, BANDS, _decision((1, 3, 5)), NO_SALES)
    assert list(row.uninformative) == list(OPERATIONS)
    # Two ni_me groups, three names; the book holds 3 and 5 of them at 1/3 each.
    assert row.uninformative["ni_me"] == Uninformative(2, _a(3, 2, 2 / 3))
    assert row.uninformative["value"] == Uninformative(1, _a(1, 1, 1 / 3))
    assert row.uninformative[COMPOSITE] == Uninformative(0, _a(0, 0, 0.0))


def test_book_weight_follows_the_decisions_target_weights() -> None:
    row = construction_month(UNIVERSE, SCORES, BANDS, _decision((3, 5), weights={3: 0.25, 5: 0.75}), NO_SALES)
    assert row.uninformative["ni_me"].affected == _a(3, 2, 1.0)
    assert row.uninformative["value"].affected == _a(1, 1, 0.75)


def test_an_all_cash_formation_weighs_nothing() -> None:
    row = construction_month(UNIVERSE, SCORES, BANDS, _decision(()), NO_SALES)
    assert row.uninformative["ni_me"].affected == _a(3, 0, 0.0)
    assert sum(p.names for p in row.patterns.values()) == len(UNIVERSE)


def test_membership_patterns_partition_the_universe_in_operation_order() -> None:
    assert OPERATIONS == ("be_me", "ni_me", "ocf_me", "value", COMPOSITE)
    assert pattern(SCORES, 1) == "be_me+ni_me+ocf_me+value+composite"
    assert pattern(SCORES, 4) == "ocf_me+value+composite"
    assert pattern(SCORES, 5) == NO_SCORE
    row = construction_month(UNIVERSE, SCORES, BANDS, _decision((1, 2, 4)), NO_SALES)
    assert row.patterns == {
        "ocf_me+value+composite": _a(1, 1, 1 / 3),
        "be_me+ocf_me+value+composite": _a(1, 0, 0.0),
        "be_me+ni_me+ocf_me+value+composite": _a(2, 2, 2 / 3),
        NO_SCORE: _a(2, 0, 0.0),
    }
    assert sum(p.names for p in row.patterns.values()) == len(UNIVERSE)
    assert sum(p.book_weight for p in row.patterns.values()) == pytest.approx(1.0)


def test_identifier_decided_selections_come_from_the_bands_tie_sets() -> None:
    # Ten composites with 1 and 2 tied at the top: the decile holds one name, so name_key puts 1 in and 2 out.
    composite = {n: Exact.raw(5.0 if n in (1, 2) else -float(n)) for n in range(1, 11)}
    tied = bands(composite)
    assert tied.decile_by_identifier == frozenset({1, 2})
    scores = _scores({COMPOSITE: tuple(range(1, 11))}, {})
    row = construction_month(tuple(range(1, 11)), scores, tied, _decision((1, 3, 4, 5)), NO_SALES)
    assert row.decile == IdentifierDecided(1, _a(2, 1, 0.25))
    # ceil(10/3) = 4 and names 4 and 5 differ, so no tie straddles the tercile boundary.
    assert row.tercile == IdentifierDecided(0, _a(0, 0, 0.0))


def test_a_book_or_group_name_outside_the_universe_or_an_unknown_operation_refuses() -> None:
    with pytest.raises(ValueError, match="book holds a name outside"):
        construction_month(UNIVERSE, SCORES, BANDS, _decision((1, 99)), NO_SALES)
    stray = _scores({}, {("be_me", "Manuf"): (99,)})
    with pytest.raises(ValueError, match="uninformative group holds a name outside"):
        construction_month(UNIVERSE, stray, BANDS, _decision(()), NO_SALES)
    unknown = _scores({}, {("rvol_21d", "Manuf"): (1,)})
    with pytest.raises(ValueError, match="unknown operation"):
        construction_month(UNIVERSE, unknown, BANDS, _decision(()), NO_SALES)


def _path_decisions() -> list[Decision]:
    """A buys 1..4; over the holding month name 1 doubles on the best-case arm and is flat on the worst; M sells 1."""

    def held(best: float) -> HoldingReturn:
        return HoldingReturn(HoldingStatus.OBSERVED, {BEST: best, WORST: 0.0})

    returns = {1: held(1.0), 2: held(0.0), 3: held(0.0), 4: held(0.0)}
    close = dict.fromkeys(range(1, 5), 10.0)
    sale = {1: TradeCategory.DISCRETIONARY_EXIT}
    return [Decision(A, (1, 2, 3, 4), {}, close, returns), Decision(M, (2, 3, 4), sale, close, returns)]


def test_a_sold_name_is_affected_at_its_pre_trade_weight_on_each_path() -> None:
    decisions = _path_decisions()
    paths: dict[Scenario, PathResult] = {
        (arm, "gross"): value_path(decisions, arm=arm, cost_multiplier=0.0) for arm in ARMS
    }
    # Best case: 1 is worth 0.5 of a pre-trade NAV of 1.25. Worst case: 0.25 of 1.0.
    assert sold_weights(decisions[1], paths) == {(BEST, "gross"): {1: 0.4}, (WORST, "gross"): {1: 0.25}}
    assert sold_weights(decisions[0], paths) == {(BEST, "gross"): {}, (WORST, "gross"): {}}
    rows = construction([UNIVERSE, UNIVERSE], [SCORES, SCORES], [BANDS, BANDS], decisions, paths)
    sold = rows[1].decile.affected  # the tie set {1, 2}: 2 held, 1 sold
    assert (sold.names, sold.book_names, sold.book_weight, sold.sold_names) == (2, 1, 1 / 3, 1)
    assert sold.sold_weight == {(BEST, "gross"): 0.4, (WORST, "gross"): 0.25}
    assert rows[0].decile.affected.sold_weight == {(BEST, "gross"): 0.0, (WORST, "gross"): 0.0}


def test_a_path_stopped_before_the_formation_has_no_sold_weight_and_a_mismatch_refuses() -> None:
    decision = _path_decisions()[1]
    stopped = PathResult(nonpositive=(2021, 4))
    assert sold_weights(decision, {GROSS: stopped}) == {GROSS: None}
    row = construction_month(UNIVERSE, SCORES, BANDS, decision, {GROSS: None})
    assert row.decile.affected.sold_weight == {GROSS: None}
    with pytest.raises(ValueError, match="sold"):
        sold_weights(decision, {GROSS: PathResult()})
    with pytest.raises(ValueError, match="do not cover exactly"):
        construction_month(UNIVERSE, SCORES, BANDS, decision, {GROSS: {}})


def test_construction_refuses_unequal_inputs() -> None:
    with pytest.raises(ValueError):
        construction([UNIVERSE], [SCORES, SCORES], [BANDS], [_decision(())], {GROSS: PathResult()})
