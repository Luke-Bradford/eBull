"""#3740: the step-3 matched random control, on synthetic formations only."""

from __future__ import annotations

from datetime import date

import pytest

from app.services.factor_book import BookRefusal
from app.services.factor_book_path import TradeCategory
from app.services.vw_factor_book_control import control_decisions, control_seed
from app.services.vw_factor_book_decisions import book_decisions, capped_weights
from app.services.vw_factor_book_path import Captured, Missed, value_path
from tests.test_3740_vw_factor_book_decisions import TERMINAL, N, _formation, _ret


def _captured(step: object) -> Captured:
    assert isinstance(step, Captured)
    return step


def test_the_seed_names_the_step_3_trial() -> None:
    assert control_seed(7, date(2027, 1, 29)) == "3740-step3:7:2027-01-29"


def test_the_control_matches_the_books_count_buys_from_its_eligible_set_and_is_reproducible() -> None:
    order = list(range(N))
    me = {n: float(n + 1) for n in range(N)}
    formations = [_formation(m, order[m:] + order[:m], me=me, flagged=[N - 1]) for m in range(1, 7)]
    book = book_decisions(formations)
    draw = control_decisions(formations, book, 3)
    assert draw == control_decisions(formations, book, 3)
    assert draw.steps != control_decisions(formations, book, 4).steps
    held: set[int] = set()
    for formation, ours, theirs in zip(formations, draw.steps, book.steps, strict=True):
        ours, theirs = _captured(ours), _captured(theirs)
        assert len(ours.targets) == len(theirs.targets)
        assert set(ours.targets) - held <= formation.eligible  # purchases come from the book's eligible set
        assert N - 1 not in ours.targets  # MAX-flagged: never eligible
        # Value weights under both caps, from the same cap solution and reference as the book.
        solution = capped_weights(formation, ours.targets)
        assert ours.weights == solution.weights and ours.cash == solution.cash
        held = set(ours.targets)


def test_max_flagged_and_untradable_holdings_are_forced_out() -> None:
    order = list(range(N))
    first = _formation(1, order)
    held = set(_captured(control_decisions([first], book_decisions([first]), 0).steps[0]).targets)
    flagged, untradable = sorted(held)[:2]
    second = _formation(2, order, flagged=[flagged], tradable=[n for n in order if n != untradable])
    book = book_decisions([first, second])
    step = _captured(control_decisions([first, second], book, 0).steps[1])
    assert step.sales[flagged] is TradeCategory.FORCED_EXIT
    assert step.sales[untradable] is TradeCategory.FORCED_EXIT


def test_a_shortage_of_eligible_names_refuses() -> None:
    order = list(range(N))
    first = _formation(1, order)
    held = set(_captured(control_decisions([first], book_decisions([first]), 0).steps[0]).targets)
    assert held != {0, 1, 2, 3}  # the seeded draw did not buy exactly the book's names
    # Month 2: nobody passes the age rule, so nothing is eligible; the book keeps its 4 holdings (exits ignore age),
    # and every control holding outside the book's names is no longer tradable and must be replaced.
    second = _formation(2, order, aged=[], tradable=[0, 1, 2, 3])
    book = book_decisions([first, second])
    assert len(_captured(book.steps[1]).targets) == 4
    with pytest.raises(BookRefusal, match="CONTROL_SHORT"):
        control_decisions([first, second], book, 0)


def test_a_missed_formation_is_passed_through_and_the_draw_values_with_nav_conserved() -> None:
    order = list(range(N))
    first = _formation(1, order)
    missed = Missed(date(2027, 2, 28), "SESSION_MISSING", {n: _ret(0.01) for n in range(N)} | {5: _ret(-1, TERMINAL)})
    third = _formation(3, order)
    book = book_decisions([first, missed, third])
    draw = control_decisions([first, missed, third], book, 11)
    assert draw.steps[1] is missed
    result = value_path(draw.steps, arm="best_case", cost_multiplier=1.0)
    for row in result.ledger:
        assert row.pre_positions + row.pre_cash == pytest.approx(
            row.post_positions + row.post_cash + row.cost, abs=1e-12
        )


def test_the_book_must_cover_the_same_formations_captured_or_missed() -> None:
    order = list(range(N))
    first, second = _formation(1, order), _formation(2, order)
    book = book_decisions([first, Missed(second.formation, "SESSION_MISSING", {n: _ret() for n in range(N)})])
    with pytest.raises(ValueError, match="captured or missed"):
        control_decisions([first, second], book, 0)
