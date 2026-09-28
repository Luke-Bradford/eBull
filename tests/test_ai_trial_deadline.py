"""#3471 slice 2c-ii — the §8 per-trade deadline's session arithmetic (pure)."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from app.services.ai_trial_deadline import deadline_exit_due, exit_deadline_session, fill_session


@pytest.mark.parametrize(
    ("executed_at", "session"),
    [
        # 11:00 New York on a Monday session.
        (datetime(2026, 10, 5, 15, 0, tzinfo=UTC), date(2026, 10, 5)),
        # 23:30 New York is still Monday's civil date: after-hours fills count to that session.
        (datetime(2026, 10, 6, 3, 30, tzinfo=UTC), date(2026, 10, 5)),
        # A Saturday civil date rolls forward to Monday.
        (datetime(2026, 10, 3, 15, 0, tzinfo=UTC), date(2026, 10, 5)),
        # Thanksgiving (closed) rolls to the Friday half day.
        (datetime(2026, 11, 26, 15, 0, tzinfo=UTC), date(2026, 11, 27)),
    ],
)
def test_the_fill_session_is_the_new_york_date_or_the_next_session(executed_at: datetime, session: date) -> None:
    assert fill_session(executed_at) == session


def test_a_naive_fill_time_is_refused() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        fill_session(datetime(2026, 10, 5, 15, 0))


@pytest.mark.parametrize(
    ("fill", "horizon", "deadline"),
    [
        # Session 0 is the fill; five sessions on is the next Monday.
        (date(2026, 10, 5), 5, date(2026, 10, 12)),
        # Columbus Day is a NYSE session, so ten sessions is Monday 19th.
        (date(2026, 10, 5), 10, date(2026, 10, 19)),
        # Thanksgiving is skipped; the Friday half day counts (23, 24, 25, 27, 30).
        (date(2026, 11, 20), 5, date(2026, 11, 30)),
        # Christmas, New Year and MLK Day (2027-01-18) are skipped.
        (date(2026, 12, 18), 20, date(2027, 1, 20)),
    ],
)
def test_the_deadline_is_session_horizon_counting_the_fill_as_zero(fill: date, horizon: int, deadline: date) -> None:
    assert exit_deadline_session(fill, horizon) == deadline


def test_a_deadline_needs_a_session_fill_and_a_positive_horizon() -> None:
    with pytest.raises(ValueError, match="not a NYSE session"):
        exit_deadline_session(date(2026, 10, 3), 5)
    with pytest.raises(ValueError, match="positive"):
        exit_deadline_session(date(2026, 10, 5), 0)


def test_the_exit_is_due_from_15_utc_on_the_deadline_session_and_stays_due() -> None:
    deadline = date(2026, 10, 19)
    assert not deadline_exit_due(deadline, datetime(2026, 10, 19, 14, 59, 59, tzinfo=UTC))
    assert deadline_exit_due(deadline, datetime(2026, 10, 19, 15, 0, tzinfo=UTC))
    # Overdue: every later cycle retries.
    assert deadline_exit_due(deadline, datetime(2026, 10, 22, 2, 0, tzinfo=UTC))
    with pytest.raises(ValueError, match="timezone-aware"):
        deadline_exit_due(deadline, datetime(2026, 10, 19, 15, 0))
