"""#2844 clause 3 — the consecutive-green countdown.

Pure tests over ``consecutive_reconciled_days``. No database: the counter takes the ledger
rows and the countdown calendar as arguments precisely so every bypass it closes can be
exercised as a table case.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest

from app.services.account_reconciliation_ledger import (
    COUNTDOWN_RULE_VERSION,
    MAX_DECISION_LAG_DAYS,
    MAX_EVIDENCE_AGE_DAYS,
    MAX_STREAK_SPAN_DAYS,
    REQUIRED_GREEN_DAYS,
    ReconciliationDay,
    consecutive_reconciled_days,
)

AS_OF = date(2026, 9, 30)


def _day(
    day: date,
    *,
    state: str = "reconciled",
    comparable: bool = True,
    decided_offset: int = 1,
    version: str = COUNTDOWN_RULE_VERSION,
) -> ReconciliationDay:
    decided = (
        None if not comparable else datetime.combine(day, datetime.min.time(), UTC) + timedelta(days=decided_offset)
    )
    return ReconciliationDay(
        snapshot_date=day,
        reconciliation_state=state,  # type: ignore[arg-type]
        comparable=comparable,
        countdown_rule_version=version,
        decided_at=decided,
    )


def _sessions(count: int, *, oldest_first_gap: int = 1, start_back: int = MAX_DECISION_LAG_DAYS + 1) -> list[date]:
    """``count`` countdown days, newest first, all already due."""
    newest = AS_OF - timedelta(days=start_back)
    return [newest - timedelta(days=i * oldest_first_gap) for i in range(count)]


def _ledger(days: list[date], **kwargs: object) -> dict[date, ReconciliationDay]:
    return {day: _day(day, **kwargs) for day in days}  # type: ignore[arg-type]


def test_five_green_due_days_reach_the_bar() -> None:
    days = _sessions(REQUIRED_GREEN_DAYS)
    streak = consecutive_reconciled_days(_ledger(days), set(days), AS_OF)
    assert streak.green_days == REQUIRED_GREEN_DAYS
    assert streak.green is True
    assert streak.newest_counted_date == days[0]


def test_four_green_days_do_not() -> None:
    days = _sessions(REQUIRED_GREEN_DAYS - 1)
    streak = consecutive_reconciled_days(_ledger(days), set(days), AS_OF)
    assert streak.green_days == REQUIRED_GREEN_DAYS - 1
    assert streak.green is False


def test_empty_calendar_is_zero_not_green() -> None:
    streak = consecutive_reconciled_days({}, set(), AS_OF)
    assert streak.green_days == 0
    assert streak.stop_reason == "no_due_countdown_day"


def test_a_countdown_day_with_no_row_stops_the_run() -> None:
    """The calendar is derived independently of the ledger so this case can exist at all.

    This is the host-outage shape: ``price_daily`` records the sessions, nothing judged
    them, and without it two days either side of a 17-day gap would splice into one run.
    """
    days = _sessions(REQUIRED_GREEN_DAYS + 1)
    rows = _ledger(days)
    del rows[days[0]]
    streak = consecutive_reconciled_days(rows, set(days), AS_OF)
    assert streak.green_days == 0
    assert streak.stop_reason == "day_never_recorded"


def test_a_diverged_day_inside_the_run_stops_it() -> None:
    days = _sessions(REQUIRED_GREEN_DAYS + 1)
    rows = _ledger(days)
    rows[days[2]] = _day(days[2], state="diverged")
    streak = consecutive_reconciled_days(rows, set(days), AS_OF)
    assert streak.green_days == 2
    assert streak.stop_reason == "diverged"


def test_a_permanent_refusal_at_the_head_is_a_failure_not_a_skip() -> None:
    """The bypass the first draft shipped.

    An earlier design skipped leading ``comparable=False`` rows as "not yet decided". A
    PERMANENT refusal then sat at the head forever while five old greens held the gate
    open. Past its due date a refusal counts as a failure like any other.
    """
    days = _sessions(REQUIRED_GREEN_DAYS + 1)
    rows = _ledger(days)
    rows[days[0]] = _day(days[0], state="refused", comparable=False)
    streak = consecutive_reconciled_days(rows, set(days), AS_OF)
    assert streak.green_days == 0
    assert streak.stop_reason == "still_refused_past_due"


def test_an_undecided_day_inside_the_grace_window_is_not_a_failure() -> None:
    """The measured 0-3 day local-snapshot lag must not redden the head every day."""
    pending = AS_OF - timedelta(days=1)
    days = _sessions(REQUIRED_GREEN_DAYS)
    rows = _ledger(days)
    rows[pending] = _day(pending, state="refused", comparable=False)
    streak = consecutive_reconciled_days(rows, {*days, pending}, AS_OF)
    assert streak.green_days == REQUIRED_GREEN_DAYS


def test_a_decided_divergence_inside_the_grace_window_voids_the_streak() -> None:
    """Grace excludes a day from COUNTING; it does not excuse a known current failure."""
    recent = AS_OF - timedelta(days=1)
    days = _sessions(REQUIRED_GREEN_DAYS)
    rows = _ledger(days)
    rows[recent] = _day(recent, state="diverged")
    streak = consecutive_reconciled_days(rows, {*days, recent}, AS_OF)
    assert streak.green_days == 0
    assert streak.stop_reason == "divergence_inside_grace_window"


def test_a_backfilled_verdict_does_not_count() -> None:
    """Otherwise a first install judges thirty historical days at once and is instantly green."""
    days = _sessions(REQUIRED_GREEN_DAYS)
    rows = _ledger(days)
    rows[days[3]] = _day(days[3], decided_offset=MAX_DECISION_LAG_DAYS + 1)
    streak = consecutive_reconciled_days(rows, set(days), AS_OF)
    assert streak.green_days == 3
    assert streak.stop_reason == "backfilled_not_observed"


def test_a_verdict_dated_before_its_own_day_does_not_count() -> None:
    days = _sessions(REQUIRED_GREEN_DAYS)
    rows = _ledger(days)
    rows[days[1]] = _day(days[1], decided_offset=-1)
    streak = consecutive_reconciled_days(rows, set(days), AS_OF)
    assert streak.green_days == 1
    assert streak.stop_reason == "verdict_predates_its_day"


def test_a_stale_calendar_voids_the_streak() -> None:
    """Five old greens must not hold the gate open after the whole pipeline stops."""
    days = _sessions(REQUIRED_GREEN_DAYS, start_back=MAX_EVIDENCE_AGE_DAYS + 1)
    streak = consecutive_reconciled_days(_ledger(days), set(days), AS_OF)
    assert streak.green_days == 0
    assert streak.stop_reason == "countdown_calendar_stale"


def test_a_sparse_calendar_cannot_splice_five_scattered_greens() -> None:
    """Without the span bound, five timely greens months apart read as consecutive."""
    days = _sessions(REQUIRED_GREEN_DAYS, oldest_first_gap=MAX_STREAK_SPAN_DAYS)
    streak = consecutive_reconciled_days(_ledger(days), set(days), AS_OF)
    assert streak.green_days == 0
    assert streak.stop_reason == "streak_span_too_wide"


def test_a_superseded_countdown_rule_version_stops_the_run() -> None:
    days = _sessions(REQUIRED_GREEN_DAYS)
    rows = _ledger(days)
    rows[days[2]] = _day(days[2], version="f0-countdown-v0")
    streak = consecutive_reconciled_days(rows, set(days), AS_OF)
    assert streak.green_days == 2
    assert streak.stop_reason == "countdown_rule_version_superseded"


def test_future_dated_calendar_entries_are_ignored() -> None:
    days = _sessions(REQUIRED_GREEN_DAYS)
    future = AS_OF + timedelta(days=3)
    rows = _ledger(days)
    rows[future] = _day(future)
    streak = consecutive_reconciled_days(rows, {*days, future}, AS_OF)
    assert streak.green_days == REQUIRED_GREEN_DAYS


def test_duplicate_calendar_dates_cannot_count_one_day_five_times() -> None:
    """The calendar is a set by type, and the counter must not depend on the caller's."""
    only = _sessions(1)
    streak = consecutive_reconciled_days(_ledger(only), {only[0], only[0]}, AS_OF)
    assert streak.green_days == 1


@pytest.mark.parametrize("lag", [0, MAX_DECISION_LAG_DAYS])
def test_every_lag_inside_the_measured_window_still_counts(lag: int) -> None:
    days = _sessions(REQUIRED_GREEN_DAYS)
    streak = consecutive_reconciled_days(_ledger(days, decided_offset=lag), set(days), AS_OF)
    assert streak.green_days == REQUIRED_GREEN_DAYS
