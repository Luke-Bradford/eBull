"""#2842 slice 5c-i — the pot's exit due rule and executed-flatness check (pure; spec §7.4)."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone

import pytest

from app.services.ranking_pot_exits import ExitsError, executed_not_flat, pot_exit_due

_SESSION = date(2026, 11, 2)
_DUE = datetime(2026, 11, 2, 15, 0, tzinfo=UTC)


def test_an_exit_is_due_from_15_utc_on_its_session_and_stays_due() -> None:
    assert not pot_exit_due(_SESSION, _DUE - timedelta(microseconds=1))
    assert pot_exit_due(_SESSION, _DUE)
    # The same instant in New York time; and every later cycle retries.
    assert pot_exit_due(_SESSION, _DUE.astimezone(timezone(timedelta(hours=-5))))
    assert pot_exit_due(_SESSION, _DUE + timedelta(days=3))
    with pytest.raises(ValueError, match="timezone-aware"):
        pot_exit_due(_SESSION, datetime(2026, 11, 2, 16, 0))


_TODAY = date(2026, 11, 2)


@pytest.mark.parametrize(
    ("row", "flat"),
    [
        # Undecided: held until its target session has passed, then expired.
        ((1, _TODAY, None, None, None, False), False),
        ((1, _TODAY - timedelta(days=1), None, None, None, False), True),
        # Refused.
        ((1, _TODAY, "rejected", None, None, False), True),
        # Allocated: flat only once the trade is closed or failed and owns nothing.
        ((1, _TODAY, "allocated", None, None, False), False),
        ((1, _TODAY, "allocated", 7, "planned", False), False),
        ((1, _TODAY, "allocated", 7, "submitted", False), False),
        ((1, _TODAY, "allocated", 7, "reconcile_required", False), False),
        ((1, _TODAY, "allocated", 7, "open", True), False),
        ((1, _TODAY, "allocated", 7, "closing", True), False),
        ((1, _TODAY, "allocated", 7, "closed", True), False),
        ((1, _TODAY, "allocated", 7, "closed", False), True),
        ((1, _TODAY, "allocated", 7, "failed", False), True),
    ],
)
def test_executed_flatness_reads_the_raw_facts(row: tuple[object, ...], flat: bool) -> None:
    assert (executed_not_flat([row], ny_today=_TODAY) is None) is flat


def test_an_empty_book_is_flat_and_contradictions_raise() -> None:
    assert executed_not_flat([], ny_today=_TODAY) is None
    with pytest.raises(ExitsError, match="without a funding decision"):
        executed_not_flat([(1, _TODAY, None, 7, "open", False)], ny_today=_TODAY)
    with pytest.raises(ExitsError, match="rejected funding decision with a trade"):
        executed_not_flat([(1, _TODAY, "rejected", 7, "failed", False)], ny_today=_TODAY)
    with pytest.raises(ExitsError, match="unknown funding verdict"):
        executed_not_flat([(1, _TODAY, "maybe", None, None, False)], ny_today=_TODAY)
