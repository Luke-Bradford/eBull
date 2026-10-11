"""The scheduled kill-switch drill: its slot calendar, registration and job body (#3614 slice 3).

Pure: no database. The job body runs with the drill and ``_tracked_job`` stubbed.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

import app.services.kill_switch_drill as drill
import app.workers.scheduler as scheduler
from app.services.kill_switch_drill_schedule import (
    DRILL_OCCURRENCE_GRACE,
    DRILL_WATCH_FROM,
    occurrence_at_or_before,
    occurrences_between,
    overdue_occurrences,
)
from app.workers.scheduler import SCHEDULED_JOBS, Cadence, compute_next_run

_NOV = datetime(2026, 11, 3, 6, 12, tzinfo=UTC)
_DEC = datetime(2026, 12, 3, 6, 12, tzinfo=UTC)


def _job() -> scheduler.ScheduledJob:
    return next(j for j in SCHEDULED_JOBS if j.name == scheduler.JOB_KILL_SWITCH_DRILL)


def test_the_job_is_registered_on_the_spec_slot() -> None:
    job = _job()
    assert job.cadence == Cadence.monthly(day=3, hour=6, minute=12)
    assert job.source == "risk_metrics"
    assert job.catch_up_on_boot is True
    assert job.rearm_on_lost_fire is False

    from app.jobs.runtime import _INVOKERS

    assert scheduler.JOB_KILL_SWITCH_DRILL in _INVOKERS


def test_the_slot_calendar_agrees_with_the_scheduler() -> None:
    """The watch counts the same slots the scheduler fires: one calendar, two readers."""
    cadence = _job().cadence
    for o in occurrences_between(DRILL_WATCH_FROM, DRILL_WATCH_FROM + timedelta(days=400)):
        assert compute_next_run(cadence, o - timedelta(seconds=1)) == o


def test_occurrence_at_or_before() -> None:
    assert occurrence_at_or_before(_NOV) == _NOV
    assert occurrence_at_or_before(_NOV - timedelta(microseconds=1)) == datetime(2026, 10, 3, 6, 12, tzinfo=UTC)
    assert occurrence_at_or_before(datetime(2027, 1, 2, tzinfo=UTC)) == _DEC


def test_occurrences_between_is_inclusive_and_ordered() -> None:
    assert occurrences_between(_NOV, _DEC) == [_NOV, _DEC]
    assert occurrences_between(_NOV + timedelta(seconds=1), _DEC - timedelta(seconds=1)) == []


def test_the_first_watched_slot_is_the_first_after_deploy() -> None:
    assert occurrences_between(DRILL_WATCH_FROM, _NOV)[0] == _NOV


def test_a_slot_is_overdue_only_after_its_grace_and_only_without_a_drill_inside_it() -> None:
    due = _NOV + DRILL_OCCURRENCE_GRACE
    assert overdue_occurrences([], now=due - timedelta(seconds=1)) == []
    assert overdue_occurrences([], now=due) == [_NOV]
    # Any drill started in [o, o + grace) satisfies it, a manual one included.
    assert overdue_occurrences([_NOV], now=due) == []
    assert overdue_occurrences([due - timedelta(seconds=1)], now=due) == []
    # Before the slot or at the end of its grace does not.
    assert overdue_occurrences([_NOV - timedelta(seconds=1)], now=due) == [_NOV]
    assert overdue_occurrences([due], now=due) == [_NOV]
    # A late catch-up fire does not satisfy an earlier slot.
    assert overdue_occurrences([_DEC], now=_DEC + DRILL_OCCURRENCE_GRACE) == [_NOV]


class _Tracker:
    run_id = 41
    row_count: int | None = None
    note: str | None = None


def _stub_tracked_job(monkeypatch: pytest.MonkeyPatch, tracker: _Tracker) -> None:
    @contextlib.contextmanager
    def _tracked(name: str) -> Iterator[_Tracker]:
        assert name == scheduler.JOB_KILL_SWITCH_DRILL
        yield tracker

    monkeypatch.setattr(scheduler, "_tracked_job", _tracked)


def test_the_job_runs_a_scheduled_drill_for_the_latest_slot(monkeypatch: pytest.MonkeyPatch) -> None:
    tracker = _Tracker()
    _stub_tracked_job(monkeypatch, tracker)
    calls: list[dict[str, Any]] = []

    def _run(connect: object, **kw: Any) -> drill.DrillRun:
        calls.append(kw)
        run = drill.DrillRun(drill.uuid.uuid4(), kw["trigger"], kw["actor"], datetime.now(UTC))
        run.event_id, run.mode = 9, "sandbox"
        return run

    monkeypatch.setattr(drill, "run_kill_switch_drill", _run)
    scheduler.kill_switch_drill()
    assert calls == [
        {
            "trigger": "scheduled",
            "actor": drill.DRILL_ACTOR,
            "scheduled_for": occurrence_at_or_before(datetime.now(UTC)),
            "job_run_id": 41,
        }
    ]
    assert tracker.row_count == 1
    assert tracker.note == "event=9 mode=sandbox entry_verdict=not_run run_failure=None"


def test_the_job_fails_when_nothing_was_recorded(monkeypatch: pytest.MonkeyPatch) -> None:
    tracker = _Tracker()
    tracker.run_id = 0  # ``record_job_start`` failed: there is no job_runs row to reference
    _stub_tracked_job(monkeypatch, tracker)
    seen: list[object] = []
    monkeypatch.setattr(drill, "run_kill_switch_drill", lambda _c, **kw: seen.append(kw["job_run_id"]))
    with pytest.raises(RuntimeError, match="holds the drill lock"):
        scheduler.kill_switch_drill()
    assert seen == [None]
