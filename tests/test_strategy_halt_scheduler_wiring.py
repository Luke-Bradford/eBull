"""Scheduler/runtime wiring for the primary-source strategy halt feed."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from app.jobs.runtime import _INVOKERS
from app.jobs.sources import source_for
from app.services.strategy_halts import HaltSnapshot
from app.workers import scheduler
from app.workers.scheduler import SCHEDULED_JOBS, Cadence


def _job() -> scheduler.ScheduledJob:
    return next(job for job in SCHEDULED_JOBS if job.name == scheduler.JOB_STRATEGY_HALT_FEED_REFRESH)


def test_halt_feed_job_is_bounded_and_independently_locked() -> None:
    job = _job()
    assert job.cadence == Cadence.every_n_minutes(interval=5)
    assert job.source == "nasdaq"
    assert job.catch_up_on_boot is False
    assert job.prerequisite is scheduler._strategy_halt_collection_due
    assert source_for(job.name) == "nasdaq"
    assert _INVOKERS[job.name].__wrapped__ is scheduler.strategy_halt_feed_refresh  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    ("instant", "expected"),
    [
        ("2026-08-10T12:59:00+00:00", False),  # 08:59 ET
        ("2026-08-10T13:00:00+00:00", True),
        ("2026-08-10T20:15:00+00:00", True),
        ("2026-08-10T20:16:00+00:00", False),
        ("2026-11-27T18:15:00+00:00", True),  # early close + 15m
        ("2026-11-27T18:16:00+00:00", False),
        ("2026-08-09T15:00:00+00:00", False),  # Sunday
    ],
)
def test_halt_poll_window(instant: str, expected: bool) -> None:
    assert scheduler._strategy_halt_collection_window_open(datetime.fromisoformat(instant)) is expected


def test_halt_poll_window_requires_an_aware_time() -> None:
    with pytest.raises(ValueError, match="aware datetime"):
        scheduler._strategy_halt_collection_window_open(datetime(2026, 8, 10, 13, 0))


def test_halt_job_tracks_provider_publication_and_item_count() -> None:
    snapshot = HaltSnapshot(
        source_pub_at=datetime(2026, 8, 10, 13, 50, tzinfo=UTC),
        payload_sha256="0" * 64,
        halts=(),
    )
    tracker = MagicMock()
    tracker_cm = MagicMock()
    tracker_cm.__enter__.return_value = tracker
    tracker_cm.__exit__.return_value = False
    with (
        patch("app.workers.scheduler._tracked_job", return_value=tracker_cm),
        patch("app.workers.scheduler._refresh_strategy_halt_feed", return_value=snapshot),
    ):
        scheduler.strategy_halt_feed_refresh()

    assert tracker.row_count == 0
    assert "source_pub_at=2026-08-10T13:50:00+00:00" in tracker.note
    assert "items=0" in tracker.note
