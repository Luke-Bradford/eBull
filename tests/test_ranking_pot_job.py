"""#2842 slice 4b-ii — the ranking pot's decision window and scorer provenance (pure)."""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta

import pytest

from app.services import ranking_pot_job as job
from app.services.scoring import _WEIGHT_MODES, ThesisProvenance, _score_from_data
from app.workers import scheduler
from tests.test_scoring import _NOW, _full_data


def _at(d: date, hh: int, mm: int) -> datetime:
    return datetime.combine(d, time(hh, mm), UTC)


THU, FRI, SAT, MON = date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 3), date(2026, 10, 5)


@pytest.mark.parametrize(
    ("as_of", "phase"),
    [
        (_at(THU, 23, 29), "closed"),  # before the window opens
        (_at(THU, 23, 30), "waiting"),
        (_at(FRI, 6, 50), "waiting"),  # the latest measured refresh finish
        (_at(FRI, 8, 59), "waiting"),
        (_at(FRI, 9, 0), "final"),
        (_at(FRI, 11, 59), "final"),
        (_at(FRI, 12, 0), "closed"),  # the window closes before the session opens
        (_at(FRI, 23, 40), "waiting"),  # Friday's session: its window runs into Saturday morning
        (_at(SAT, 11, 40), "final"),
        (_at(SAT, 23, 40), "closed"),  # Saturday is no session: no window follows it
        (_at(MON, 1, 0), "closed"),  # Monday early: the last completed session is Friday, whose window closed
    ],
)
def test_window_phase(as_of: datetime, phase: str) -> None:
    assert job.window_phase(as_of) == phase


def test_window_phase_needs_an_aware_time() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        job.window_phase(datetime(2026, 10, 1, 23, 40))


def test_hourly_fires_reach_the_final_phase_inside_the_window() -> None:
    """The scheduler's :MM fires: the window holds waiting fires and at least one final fire, the last fire
    before the close (a short coverage there is recorded, so a month never stays open for want of a verdict)."""
    minute = scheduler.RANKING_POT_FIRE_MINUTE
    assert minute == job.FIRE_MINUTE  # the scheduler's copy of the hashed constant
    fires = [_at(THU, 23, minute) + timedelta(hours=h) for h in range(14)]
    phases = [job.window_phase(f) for f in fires]
    assert phases[0] == "waiting"
    assert phases.count("final") == 3  # 09:40, 10:40, 11:40
    inside = [p for p in phases if p != "closed"]
    assert inside[-1] == "final" and phases[len(inside) :] == ["closed"] * (len(phases) - len(inside))
    registered = next(j for j in scheduler.SCHEDULED_JOBS if j.name == scheduler.JOB_RANKING_POT_REBALANCE)
    assert (registered.source, registered.cadence.kind, registered.cadence.minute) == ("db", "hourly", minute)
    # Same lane as the scheduled scorer, so the pot's own scoring run never overlaps it (§4 step 1).
    from app.jobs.sources import source_for

    assert source_for(scheduler.JOB_RANKING_POT_REBALANCE) == source_for(scheduler.JOB_MORNING_CANDIDATE_REVIEW)


def test_the_score_carries_the_consumed_thesis_and_nothing_else_changes() -> None:
    data = _full_data()
    weights = _WEIGHT_MODES["v1-balanced"]
    without = _score_from_data(7, data, weights, "v1-balanced", _NOW, None)
    assert without.thesis_used is None  # a row read without its id carries no provenance

    thesis = dict(data["thesis_row"])  # type: ignore[call-overload]
    thesis |= {"thesis_id": 99, "model": "m", "prompt_version": "p1"}
    with_id = _score_from_data(7, data | {"thesis_row": thesis}, weights, "v1-balanced", _NOW, None)
    assert with_id.thesis_used == ThesisProvenance(99, thesis["created_at"], "m", "p1")
    assert (with_id.total_score, with_id.raw_total) == (without.total_score, without.raw_total)

    quarantined = _score_from_data(7, data | {"thesis_row": None}, weights, "v1-balanced", _NOW, None)
    assert quarantined.thesis_used is None
