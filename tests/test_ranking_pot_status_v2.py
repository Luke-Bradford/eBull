"""#3592 slice 4c — the v2 page's holdings: each name's §5 reasons come from the decision that entered it."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from app.services import ranking_pot_sim as sim
from app.services import ranking_pot_status_v2 as ps2
from app.services import ranking_pot_v2_step as st

D = Decimal
JAN, FEB, MAR = date(2026, 1, 5), date(2026, 2, 2), date(2026, 3, 2)


def _position(iid: int, slot: int, entered: date) -> sim.Position:
    return sim.Position(
        instrument_id=iid,
        lifecycle=iid,
        slot=slot,
        units=D(1),
        entry_session=entered,
        entry_fill=D(10),
        invested=D("0.5"),
        stop_loss=D(8),
        take_profit=D(14),
        last_close=D(11),
        last_close_session=MAR,
        value=D("0.55"),
        missing_run=0,
        h_exit=D("0.001"),
    )


def _book(positions: tuple[sim.Position, ...], pending: tuple[sim.PendingEntry, ...] = ()) -> st.Book:
    return st.Book(sim.BookState(2, (D(0), D(0)), positions, pending, 9, MAR))


def _why(iid: int, composite: str) -> dict[str, Any]:
    return {"instrument_id": iid, "composite": composite}


def test_reasons_come_from_the_latest_decision_at_or_before_entry() -> None:
    # Name 7 entered in January, exited, and re-entered in March: the March reasons, not January's. Name 3 entered in
    # February; March's decision (after its entry) is never read for it.
    book = _book((_position(7, 1, MAR), _position(3, 0, FEB)))
    entries = [
        (JAN, [_why(7, "1/2")]),
        (FEB, [_why(3, "2/3")]),
        (MAR, [_why(7, "5/6"), _why(3, "1/9")]),
    ]
    out = ps2.holdings(book, entries, {7: "AAA"})
    assert [(h.slot, h.instrument_id, h.symbol, h.state) for h in out] == [(0, 3, None, "held"), (1, 7, "AAA", "held")]
    assert [h.reasons for h in out] == [_why(3, "2/3"), _why(7, "5/6")]
    assert (out[1].entry_fill, out[1].value, out[1].stop_loss) == (D(10), D("0.55"), D(8))


def test_a_pending_entry_and_a_decision_without_reasons() -> None:
    pending = (sim.PendingEntry(5, MAR, D("0.001"), D(1), D(10), FEB),)
    out = ps2.holdings(_book((_position(4, 0, FEB),), pending), [(FEB, None), (MAR, [_why(5, "1/1")])], {})
    assert [(h.instrument_id, h.state, h.slot, h.entry_fill, h.reasons) for h in out] == [
        (4, "held", 0, D(10), None),  # its decision stored no reasons: shown as such, never borrowed from another
        (5, "pending", None, None, _why(5, "1/1")),
    ]


def test_no_decision_before_entry_leaves_no_reasons() -> None:
    out = ps2.holdings(_book((_position(2, 0, JAN),)), [(FEB, [_why(2, "1/2")])], {})
    assert out[0].reasons is None
