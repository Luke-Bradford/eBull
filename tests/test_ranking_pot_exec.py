"""#2842 slice 5a — the executed book's classification, slots and cooldown (spec §4, pure)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from fractions import Fraction
from typing import cast

import pytest

from app.services import ranking_pot as pot
from app.services import ranking_pot_exec as ex

T = date(2026, 11, 2)


@pytest.mark.parametrize(
    ("verdict", "trade", "today", "status"),
    [
        (None, None, T, "entry_pending"),  # unsubmitted, its session is today
        (None, None, date(2026, 11, 3), "expired"),  # r3-100: never submitted, holds nothing
        ("rejected", None, T, "refused"),
        ("allocated", None, T, "entry_pending"),  # fail closed: an allocation holds its slot
        ("allocated", "planned", T, "entry_pending"),
        ("allocated", "submitted", T, "entry_pending"),
        ("allocated", "reconcile_required", T, "entry_pending"),  # r3-64: uncertain holds its slot
        ("allocated", "open", T, "open"),
        ("allocated", "closing", T, "open"),
        ("allocated", "failed", T, "failed"),
        ("allocated", "closed", T, "closed"),
    ],
)
def test_classify(verdict: str | None, trade: str | None, today: date, status: str) -> None:
    assert ex.classify(funding_verdict=verdict, trade_status=trade, target_session=T, ny_today=today) == status


@pytest.mark.parametrize(("verdict", "trade"), [(None, "open"), ("maybe", None), ("allocated", "vanished")])
def test_classify_refuses_unknown_shapes(verdict: str | None, trade: str | None) -> None:
    with pytest.raises(ex.ExecutedBookError):
        ex.classify(funding_verdict=verdict, trade_status=trade, target_session=T, ny_today=T)


def _universes(scores: dict[int, str], *, f: set[int] | None = None) -> pot.Universes:
    r = frozenset(scores)
    f_ids = frozenset(r if f is None else f)
    return pot.Universes(
        breakpoint=Decimal(1),
        max_cut=Fraction(1, 10),
        r_ids=r,
        f_ids=f_ids,
        hold_failure={99: cast(pot.HoldRule, "not_tradable")},
        entry_failure=dict.fromkeys(r - f_ids, cast(pot.EntryRule, "quote_ineligible")),
        own_score={i: Decimal(s) for i, s in scores.items()},
        max_return={i: Fraction(1, 100) for i in r},
        atr={i: Fraction(1) for i in r},
        max_population=len(r),
        nyse_cap_population=len(r),
    )


def _lc(lc_id: int, iid: int, slot: int, status: str = "open", stamped: bool = False) -> ex.Lifecycle:
    return ex.Lifecycle(lc_id, iid, slot, status, "open" if status == "open" else None, stamped)  # type: ignore[arg-type]


def test_entries_take_free_slots_then_the_slots_this_rebalance_releases() -> None:
    # N = 3. Held: 99 (leaves R → exit, slot 1), 10 (holds, slot 2). Slot 3 free. 11 and 12 enter.
    u = _universes({10: "0.9", 11: "0.8", 12: "0.7"})
    plan = ex.plan_executed(u, [_lc(1, 99, 1), _lc(2, 10, 2)], previous_closed=frozenset(), entries_allowed=True, n=3)
    assert [(lc.lifecycle_id, reason) for lc, reason in plan.stamps] == [(1, "ineligible:not_tradable")]
    assert plan.entries == (ex.Entry(11, 3, None), ex.Entry(12, 1, 1))


def test_a_previously_stamped_exit_keeps_its_slot() -> None:
    # r3-99 / §5.1 rule 1: a stamped lifecycle is exit_pending and still occupies slot 1.
    u = _universes({10: "0.9", 11: "0.8", 12: "0.7"})
    plan = ex.plan_executed(u, [_lc(1, 10, 1, stamped=True)], previous_closed=frozenset(), entries_allowed=True, n=2)
    assert plan.stamps == ()
    assert plan.entries == (ex.Entry(11, 2, None),)
    assert {r.instrument_id: r.action for r in plan.decision.rows}[10] == "exit_pending"


def test_terminal_lifecycles_release_their_slot() -> None:
    u = _universes({10: "0.9"})
    lifecycles = [_lc(1, 10, 1, "expired"), _lc(2, 10, 2, "refused"), _lc(3, 10, 3, "failed")]
    plan = ex.plan_executed(u, lifecycles, previous_closed=None, entries_allowed=True, n=1)
    assert plan.held == () and plan.entries == (ex.Entry(10, 1, None),)


def test_recently_exited_is_closed_now_and_not_closed_at_the_previous_decision() -> None:
    u = _universes({10: "0.9", 11: "0.8"})
    lifecycles = [_lc(1, 10, 1, "closed"), _lc(2, 11, 2, "closed")]
    plan = ex.plan_executed(u, lifecycles, previous_closed=frozenset({2}), entries_allowed=True, n=2)
    assert plan.recently_exited == frozenset({10}) and plan.closed_ids == frozenset({1, 2})
    rows = {r.instrument_id: (r.action, r.reason) for r in plan.decision.rows}
    assert rows[10] == ("not_selected", "recently_exited") and rows[11] == ("enter", None)


def test_entries_withheld_still_exit() -> None:
    u = _universes({10: "0.9", 11: "0.8"})
    plan = ex.plan_executed(u, [_lc(1, 99, 1)], previous_closed=frozenset(), entries_allowed=False, n=2)
    assert len(plan.stamps) == 1 and plan.entries == ()
    assert {r.reason for r in plan.decision.rows if r.action == "not_selected"} == {"entries_halted"}


@pytest.mark.parametrize(
    "lifecycles",
    [
        [_lc(1, 10, 1), _lc(2, 10, 2)],  # one name, two live lifecycles
        [_lc(1, 10, 1), _lc(2, 11, 1)],  # one slot, two live lifecycles
        [_lc(1, 10, 3)],  # slot beyond N
    ],
)
def test_the_reader_refuses_an_inconsistent_book(lifecycles: list[ex.Lifecycle]) -> None:
    with pytest.raises(ex.ExecutedBookError):
        ex.plan_executed(
            _universes({10: "0.9", 11: "0.8"}), lifecycles, previous_closed=None, entries_allowed=True, n=2
        )
