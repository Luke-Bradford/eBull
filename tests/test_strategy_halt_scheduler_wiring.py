"""Scheduler/runtime wiring for the primary-source strategy halt feed."""

from __future__ import annotations

from contextlib import ExitStack
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from app.jobs.runtime import _INVOKERS
from app.jobs.sources import source_for
from app.services.strategy_halts import HaltFeedError, HaltSnapshot
from app.services.strategy_paper_runtime import StrategyPaperCycleResult
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
        content_sha256="1" * 64,
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


@pytest.mark.parametrize("refresh_fails", [True, False])
def test_a_failed_halt_refresh_withholds_entries_but_never_the_risk_reducing_half(refresh_fails: bool) -> None:
    """#3546: the halt feed gates new entries only; reconciliation, de-risking and the
    AI-trial halts still run when its refresh fails, and the run is degraded, not failed."""
    snapshot = HaltSnapshot(
        source_pub_at=datetime(2026, 8, 10, 13, 50, tzinfo=UTC),
        payload_sha256="0" * 64,
        content_sha256="1" * 64,
        halts=(),
    )
    tracker = MagicMock()
    tracker_cm = MagicMock()
    tracker_cm.__enter__.return_value = tracker
    tracker_cm.__exit__.return_value = False
    cycle = MagicMock(return_value=StrategyPaperCycleResult(1, 2, 0, 0, {"no_change": 2}))
    halts = MagicMock(return_value="checked=1 halted=none unmeasured=0")
    refresh = (
        MagicMock(side_effect=HaltFeedError("halt feed request failed"))
        if refresh_fails
        else MagicMock(return_value=snapshot)
    )
    with (
        patch.object(scheduler.settings, "etoro_env", "demo"),
        patch("app.workers.scheduler._load_etoro_credentials", return_value=("k", "u")),
        patch("app.workers.scheduler._tracked_job", return_value=tracker_cm),
        patch("app.workers.scheduler._refresh_strategy_halt_feed", refresh),
        patch("app.workers.scheduler.connect_job"),
        patch("app.providers.implementations.etoro_broker.EtoroBrokerProvider"),
        patch("app.services.strategy_paper_runtime.run_strategy_paper_cycle", cycle),
        patch("app.workers.scheduler._record_trial_pair_lifecycle", return_value=0),
        patch("app.workers.scheduler._enforce_trial_halts", halts),
    ):
        scheduler.strategy_paper_cycle()

    assert cycle.call_args.kwargs["entries"] is not refresh_fails
    halts.assert_called_once()
    if refresh_fails:
        assert tracker.progress.errors == {"halt_feed_refresh": 1}
        assert "halt_source_pub_at=error (halt feed request failed; entries withheld)" in tracker.note
    else:
        assert "halt_source_pub_at=2026-08-10T13:50:00+00:00" in tracker.note
    assert "managed=2 (no_change:2)" in tracker.note


def _paper_cycle_patches(cycle: MagicMock, tracker: MagicMock, halts: MagicMock) -> ExitStack:
    tracker_cm = MagicMock()
    tracker_cm.__enter__.return_value = tracker
    tracker_cm.__exit__.return_value = False
    snapshot = HaltSnapshot(
        source_pub_at=datetime(2026, 8, 10, 13, 50, tzinfo=UTC),
        payload_sha256="0" * 64,
        content_sha256="1" * 64,
        halts=(),
    )
    stack = ExitStack()
    for target in (
        patch.object(scheduler.settings, "etoro_env", "demo"),
        patch("app.workers.scheduler._load_etoro_credentials", return_value=("k", "u")),
        patch("app.workers.scheduler._tracked_job", return_value=tracker_cm),
        patch("app.workers.scheduler._refresh_strategy_halt_feed", return_value=snapshot),
        patch("app.workers.scheduler.connect_job"),
        patch("app.providers.implementations.etoro_broker.EtoroBrokerProvider"),
        patch("app.services.strategy_paper_runtime.run_strategy_paper_cycle", cycle),
        patch("app.workers.scheduler._record_trial_pair_lifecycle", return_value=0),
        patch("app.workers.scheduler._enforce_trial_halts", halts),
    ):
        stack.enter_context(target)
    return stack


def test_a_failing_cycle_still_runs_the_trial_halts_and_fails_the_run() -> None:
    """#3546 gap B: the AI-trial loss halts do not depend on the paper cycle completing."""
    tracker = MagicMock()
    halts = MagicMock(return_value="checked=1 halted=none unmeasured=0")
    cycle = MagicMock(side_effect=RuntimeError("connection lost"))
    with _paper_cycle_patches(cycle, tracker, halts), pytest.raises(RuntimeError, match="connection lost"):
        scheduler.strategy_paper_cycle()
    halts.assert_called_once()


def test_a_broker_teardown_failure_still_runs_the_trial_halts() -> None:
    tracker = MagicMock()
    halts = MagicMock(return_value="checked=1 halted=none unmeasured=0")
    cycle = MagicMock(return_value=StrategyPaperCycleResult(0, 0, 0, 0))
    with _paper_cycle_patches(cycle, tracker, halts) as stack:
        provider = stack.enter_context(patch("app.providers.implementations.etoro_broker.EtoroBrokerProvider"))
        provider.return_value.__exit__.side_effect = RuntimeError("http client close failed")
        with pytest.raises(RuntimeError, match="http client close failed"):
            scheduler.strategy_paper_cycle()
    halts.assert_called_once()


def test_contained_cycle_failures_degrade_the_run() -> None:
    tracker = MagicMock()
    halts = MagicMock(return_value="checked=0 halted=none unmeasured=0")
    cycle = MagicMock(return_value=StrategyPaperCycleResult(0, 1, 0, None, {"applied": 1}, {"manage": 1, "health": 1}))
    with _paper_cycle_patches(cycle, tracker, halts):
        scheduler.strategy_paper_cycle()
    assert tracker.progress.errors == {"manage": 1, "health": 1}
    assert "active_blocks=error" in tracker.note
