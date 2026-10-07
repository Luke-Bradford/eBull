"""#3609 step 2 slice 3c: the matched random control, on synthetic formations only."""

from __future__ import annotations

import random
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import date

import pytest

from app.services.factor_book import BookRefusal, Exact, bands
from app.services.factor_book_control import control_decisions, control_seed
from app.services.factor_book_path import (
    BookDecisions,
    Formation,
    HoldingReturn,
    TradeCategory,
    book_decisions,
    value_path,
)
from app.services.factor_panel_prices import HoldingStatus

SEASONED = date(2000, 1, 3)


def _observed(r: float) -> HoldingReturn:
    return HoldingReturn(HoldingStatus.OBSERVED, {"best_case": r, "worst_case": r})


def _formation(
    month: int,
    order: Sequence[int],
    *,
    close: Mapping[int, float] | None = None,
    first_bar: Mapping[int, date] | None = None,
    returns: Mapping[int, HoldingReturn] | None = None,
) -> Formation:
    """Universe = ``order`` (best first); every name closes at $50, is seasoned and returns 0% observed."""
    day = date(2015, month, 28)
    return Formation(
        formation=day,
        session=day,
        universe=frozenset(order),
        bands=bands({name: Exact.raw(float(-rank)) for rank, name in enumerate(order)}),
        close={**dict.fromkeys(order, 50.0), **(close or {})},
        first_bar={**dict.fromkeys(order, SEASONED), **(first_bar or {})},
        returns={**{n: _observed(0.0) for n in order}, **(returns or {})},
    )


def _run(formations: Sequence[Formation], draw: int = 0) -> tuple[BookDecisions, list[tuple[int, ...]]]:
    book = book_decisions(formations)
    control = control_decisions(formations, book, draw)
    return book, [d.targets for d in control.decisions]


def test_the_first_formation_buys_n_names_with_the_declared_seed_and_sorted_population() -> None:
    order = [i * 7919 for i in range(100, 0, -1)]  # the composite order is not name order
    assert list(frozenset(order)) != sorted(order)  # nor is a set's iteration order, so sorting is tested
    formation = _formation(1, order)
    book, targets = _run([formation], draw=7)
    expected = random.Random(control_seed(7, formation.formation))
    # Steps 2 and 3 sample from an empty holding list before the purchases, as the declared call order requires.
    expected.sample([], 0)
    expected.sample([], 0)
    want = tuple(sorted(expected.sample(sorted(order), len(book.decisions[0].targets))))
    assert control_seed(7, formation.formation) == "3609-step2:7:2015-01-28"
    assert targets == [want]


def test_draws_are_reproducible_and_distinct() -> None:
    formations = [_formation(m, list(range(200))) for m in (1, 2, 3)]
    assert _run(formations, 3)[1] == _run(formations, 3)[1]
    assert _run(formations, 3)[1] != _run(formations, 4)[1]


def test_the_control_holds_the_book_count_and_caps_random_replacements_at_k() -> None:
    order = list(range(200))
    reshuffled = [*range(20, 200), *range(20)]  # the book's 20 fall out of the tercile: k = 20, entrants 20..39
    formations = [_formation(1, order), _formation(2, reshuffled), _formation(3, order)]
    book = book_decisions(formations)
    for draw in range(20):
        control = control_decisions(formations, book, draw)
        for b, c in zip(book.decisions, control.decisions, strict=True):
            assert len(c.targets) == len(b.targets)
            assert c.discretionary <= b.discretionary
        # No forced exits and no shrink here, so with h >= k the cap binds exactly.
        assert [c.discretionary for c in control.decisions] == [b.discretionary for b in book.decisions]
        # Names sold at a formation are not re-bought there, though they stay eligible.
        for c in control.decisions:
            assert not c.sales.keys() & set(c.targets)


def test_the_control_force_sells_its_own_holdings_below_five_dollars() -> None:
    order = list(range(200))
    first = _formation(1, order)
    book = book_decisions([first])
    held = control_decisions([first], book, 0).decisions[0].targets
    second = _formation(2, order, close=dict.fromkeys(held, 4.0))
    both = [first, second]
    control = control_decisions(both, book_decisions(both), 0).decisions[1]
    assert control.sales == dict.fromkeys(held, TradeCategory.FORCED_EXIT)
    assert not set(held) & set(control.targets)  # sold names are not re-bought at the same formation


def test_a_shrinking_book_makes_count_adjustment_sales() -> None:
    order = list(range(200))
    first = _formation(1, order)  # book: 0..19
    second = _formation(2, order, close=dict.fromkeys(range(10), 4.0))  # book force-sells 0..9: n = 10, k = 0
    formations = [first, second]
    book = book_decisions(formations)
    assert (len(book.decisions[1].targets), book.decisions[1].discretionary) == (10, 0)
    for draw in range(10):
        control = control_decisions(formations, book, draw)
        before, after = control.decisions
        sales = after.sales
        forced = {n for n, c in sales.items() if c is TradeCategory.FORCED_EXIT}
        shrink = {n for n, c in sales.items() if c is TradeCategory.COUNT_ADJUSTMENT}
        assert forced == set(before.targets) & set(range(10))
        assert len(shrink) == 20 - len(forced) - 10
        assert TradeCategory.DISCRETIONARY_EXIT not in sales.values()
        assert len(after.targets) == 10


def test_control_short_refuses() -> None:
    order = list(range(30))
    formation = _formation(1, order, first_bar=dict.fromkeys(order[2:], date(2014, 12, 1)))
    book = book_decisions([formation])  # holds 0 and 1 (the eligible decile names)
    bigger = replace(book.decisions[0], targets=(0, 1, 2))
    with pytest.raises(BookRefusal) as caught:
        control_decisions([formation], BookDecisions((bigger,), ()), 5)
    assert caught.value.code == "CONTROL_SHORT"
    assert "draw 5 at 2015-01-28" in str(caught.value)


def test_pools_record_the_purchase_pool_and_the_need() -> None:
    formations = [_formation(1, list(range(200))), _formation(2, list(range(200)))]
    control = control_decisions(formations, book_decisions(formations), 2)
    assert [(p.pool, p.needed) for p in control.pools] == [(200, 20), (180, 0)]


def test_a_control_draw_is_valued_by_the_books_path() -> None:
    order = list(range(200))
    formations = [_formation(1, order, returns={n: _observed(0.01) for n in order}), _formation(2, order)]
    control = control_decisions(formations, book_decisions(formations), 0)
    path = value_path(control.decisions, arm="worst_case", cost_multiplier=0.0)
    assert path.returns[(2015, 2)] == pytest.approx(0.01, abs=1e-15)


def test_the_books_decisions_must_match_the_formations() -> None:
    formations = [_formation(1, list(range(200))), _formation(2, list(range(200)))]
    book = book_decisions(formations[:1])
    with pytest.raises(ValueError, match="exactly the control's formations"):
        control_decisions(formations, book, 0)
