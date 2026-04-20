"""Unit tests for ``app.jobs.runtime.JobRuntime``.

These tests use stub invokers and a stub database URL via dependency
injection on the constructor -- they do NOT touch Postgres or
APScheduler timing. The lock primitive itself is exercised by
``tests/test_jobs_locks.py`` against a real DB.

Coverage:
  * Unknown job name -> UnknownJob
  * Manual trigger queues the invoker and runs it
  * Double trigger while in flight -> JobAlreadyRunning on the second
    call (the in-process per-job ``threading.Lock`` is acquired
    synchronously by ``trigger()`` -- the advisory ``JobLock`` lives
    on the worker thread)
  * Distinct-job manual triggers run concurrently (no head-of-line
    blocking on the manual executor -- BLOCKING 2 regression target)
  * Wrapped scheduled-fire path swallows JobAlreadyRunning
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from app.jobs.locks import JobAlreadyRunning
from app.jobs.runtime import JobRuntime, UnknownJob
from app.workers.scheduler import Cadence, ScheduledJob


class _FakeLock:
    """Drop-in replacement for ``JobLock`` used in unit tests.

    Tracks per-name "held" state in a class-level dict so two
    instances constructed with the same name can collide. Avoids
    needing a real database connection in unit tests.
    """

    _held: dict[str, threading.Lock] = {}
    _registry_lock = threading.Lock()

    def __init__(self, _database_url: str, job_name: str) -> None:
        self._job_name = job_name
        with _FakeLock._registry_lock:
            if job_name not in _FakeLock._held:
                _FakeLock._held[job_name] = threading.Lock()
        self._lock = _FakeLock._held[job_name]
        self._acquired = False

    def __enter__(self) -> _FakeLock:
        if not self._lock.acquire(blocking=False):
            raise JobAlreadyRunning(self._job_name)
        self._acquired = True
        return self

    def __exit__(self, *_args: object) -> None:
        if self._acquired:
            self._lock.release()
            self._acquired = False


@pytest.fixture(autouse=True)
def _reset_fake_locks() -> Iterator[None]:
    # Pre-clear guards against a previous test that crashed before
    # its teardown ran (post-clear is the normal teardown path).
    _FakeLock._held.clear()
    yield
    _FakeLock._held.clear()


@pytest.fixture
def patched_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("app.jobs.runtime.JobLock", _FakeLock)


def _make_runtime(invokers: dict[str, object]) -> JobRuntime:
    # The mypy/pyright complaint about object vs Callable is silenced
    # by the cast at construction; the test invokers are all callables.
    return JobRuntime(
        database_url="postgresql://stub/stub",
        invokers=invokers,  # type: ignore[arg-type]
    )


class TestUnknownJob:
    def test_trigger_unknown_raises(self, patched_runtime: None) -> None:
        rt = _make_runtime({"known_job": lambda: None})
        with pytest.raises(UnknownJob) as exc_info:
            rt.trigger("unknown_job")
        assert exc_info.value.job_name == "unknown_job"


class TestManualTrigger:
    def test_trigger_runs_invoker(self, patched_runtime: None) -> None:
        called = threading.Event()

        def invoker() -> None:
            called.set()

        rt = _make_runtime({"my_job": invoker})
        try:
            rt.trigger("my_job")
            # Manual executor is single-thread, daemon. Wait briefly
            # for the worker to pick up and run.
            assert called.wait(timeout=2.0), "invoker was not called"
        finally:
            rt._manual_executor.shutdown(wait=True)

    def test_double_trigger_raises_already_running(self, patched_runtime: None) -> None:
        # Block the first invoker so the lock is held when the second
        # trigger arrives. The second trigger acquires synchronously
        # on the calling thread and must raise immediately.
        first_started = threading.Event()
        release_first = threading.Event()

        def slow_invoker() -> None:
            first_started.set()
            release_first.wait(timeout=2.0)

        rt = _make_runtime({"slow_job": slow_invoker})
        try:
            rt.trigger("slow_job")
            assert first_started.wait(timeout=2.0), "first run did not start"

            with pytest.raises(JobAlreadyRunning) as exc_info:
                rt.trigger("slow_job")
            assert exc_info.value.job_name == "slow_job"
        finally:
            release_first.set()
            rt._manual_executor.shutdown(wait=True)


class TestDistinctJobConcurrency:
    def test_distinct_jobs_do_not_queue(self, patched_runtime: None) -> None:
        # BLOCKING 2 regression target: with the previous
        # ``max_workers=1`` executor, triggering job B while job A was
        # still running would queue B behind A and the API caller's
        # 202 response would be a lie. Both jobs must be in flight
        # simultaneously.
        a_started = threading.Event()
        b_started = threading.Event()
        release = threading.Event()

        def a() -> None:
            a_started.set()
            release.wait(timeout=2.0)

        def b() -> None:
            b_started.set()
            release.wait(timeout=2.0)

        rt = _make_runtime({"a": a, "b": b})
        try:
            rt.trigger("a")
            assert a_started.wait(timeout=2.0), "a did not start"
            rt.trigger("b")
            assert b_started.wait(timeout=2.0), "b queued behind a -- head-of-line blocking regressed"
        finally:
            release.set()
            rt._manual_executor.shutdown(wait=True)


class TestScheduledFireWrapper:
    def test_wrapped_invoker_swallows_already_running(self, patched_runtime: None) -> None:
        # The wrapped (scheduled-fire) path must NOT raise on lock
        # contention -- it logs and skips so APScheduler does not
        # produce a noisy traceback for an expected race.
        invocations = []

        def invoker() -> None:
            invocations.append(1)

        rt = _make_runtime({"j": invoker})

        # Hold the fake lock externally to simulate a manual run in
        # flight.
        held = _FakeLock("ignored", "j")
        held.__enter__()
        try:
            wrapped = rt._wrap_invoker("j", invoker)
            wrapped()  # must not raise
            assert invocations == []  # invoker did not run
        finally:
            held.__exit__()

        # After release, the wrapper should run the invoker normally.
        wrapped = rt._wrap_invoker("j", invoker)
        wrapped()
        assert invocations == [1]

    def test_wrapped_invoker_swallows_general_exception(self, patched_runtime: None) -> None:
        # A scheduled-fire failure must be logged but not propagated
        # -- otherwise APScheduler treats the job as broken and stops
        # firing it. We want it to retry on the next cadence.
        def boom() -> None:
            raise RuntimeError("boom")

        rt = _make_runtime({"boom_job": boom})
        wrapped = rt._wrap_invoker("boom_job", boom)
        wrapped()  # must not raise


class TestStartWiring:
    def test_start_only_registers_intersection(self, patched_runtime: None, monkeypatch: pytest.MonkeyPatch) -> None:
        # Wire an invoker for a name that IS in SCHEDULED_JOBS, plus
        # one that is not. start() should register the first and
        # silently ignore the second.
        from app.workers.scheduler import JOB_ORCHESTRATOR_FULL_SYNC

        added: list[str] = []

        rt = _make_runtime(
            {
                JOB_ORCHESTRATOR_FULL_SYNC: lambda: None,
                "not_in_registry": lambda: None,
            }
        )

        def fake_add_job(*args: object, **kwargs: object) -> None:
            added.append(str(kwargs.get("id", "")))

        monkeypatch.setattr(rt._scheduler, "add_job", fake_add_job)
        monkeypatch.setattr(rt._scheduler, "start", lambda: None)
        monkeypatch.setattr(rt, "_catch_up", lambda: None)

        rt.start()

        assert added == [f"recurring:{JOB_ORCHESTRATOR_FULL_SYNC}"]

    def test_double_start_raises(self, patched_runtime: None) -> None:
        rt = _make_runtime({})
        rt._started = True  # simulate already started
        with pytest.raises(RuntimeError):
            rt.start()


class TestGetNextRunTimes:
    def test_returns_live_fire_times_from_scheduler(
        self, patched_runtime: None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """get_next_run_times queries APScheduler's in-memory jobs."""
        from app.workers.scheduler import JOB_ORCHESTRATOR_FULL_SYNC

        fire_time = datetime(2026, 4, 11, 3, 0, 0, tzinfo=UTC)

        fake_aps_job = MagicMock()
        fake_aps_job.next_run_time = fire_time

        rt = _make_runtime({JOB_ORCHESTRATOR_FULL_SYNC: lambda: None})
        monkeypatch.setattr(
            rt._scheduler,
            "get_job",
            lambda job_id: fake_aps_job if job_id == f"recurring:{JOB_ORCHESTRATOR_FULL_SYNC}" else None,
        )

        result = rt.get_next_run_times()
        assert result[JOB_ORCHESTRATOR_FULL_SYNC] == fire_time

    def test_returns_none_for_missing_scheduler_job(
        self, patched_runtime: None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If APScheduler doesn't know about a job, return None."""
        from app.workers.scheduler import JOB_ORCHESTRATOR_FULL_SYNC

        rt = _make_runtime({JOB_ORCHESTRATOR_FULL_SYNC: lambda: None})
        monkeypatch.setattr(rt._scheduler, "get_job", lambda _job_id: None)

        result = rt.get_next_run_times()
        assert result[JOB_ORCHESTRATOR_FULL_SYNC] is None

    def test_excludes_unwired_scheduled_jobs(self, patched_runtime: None, monkeypatch: pytest.MonkeyPatch) -> None:
        """SCHEDULED_JOBS entries not in the invoker map are excluded.

        Uses JOB_EXECUTE_APPROVED_ORDERS as the 'unwired' case — it IS
        still in SCHEDULED_JOBS after Phase 4 but is deliberately NOT
        passed to _make_runtime. A scheduled job with no invoker must
        be excluded from get_next_run_times(), otherwise the Admin UI
        would show a next-run time for a job that would silently 404
        on manual trigger.
        """
        from app.workers.scheduler import (
            JOB_EXECUTE_APPROVED_ORDERS,
            JOB_ORCHESTRATOR_FULL_SYNC,
        )

        # Wire only one of the two scheduled jobs — the unwired one
        # (execute_approved_orders) must be absent from the result.
        rt = _make_runtime({JOB_ORCHESTRATOR_FULL_SYNC: lambda: None})
        monkeypatch.setattr(rt._scheduler, "get_job", lambda _job_id: None)

        result = rt.get_next_run_times()
        assert JOB_ORCHESTRATOR_FULL_SYNC in result
        assert JOB_EXECUTE_APPROVED_ORDERS not in result


class TestProductionInvokerRegistry:
    """Every scheduled job must have an invoker; on-demand jobs may exist
    in ``_INVOKERS`` without a ``SCHEDULED_JOBS`` entry.

    Drift guard: a job declared in the registry without an invoker
    would silently 404 on the manual trigger endpoint.
    """

    def test_every_scheduled_job_has_an_invoker(self) -> None:
        from app.jobs.runtime import _INVOKERS
        from app.workers.scheduler import SCHEDULED_JOBS

        registry_names = {job.name for job in SCHEDULED_JOBS}
        invoker_names = set(_INVOKERS.keys())
        missing = registry_names - invoker_names
        assert not missing, f"Scheduled jobs without invokers (would never fire): {sorted(missing)}"

    def test_every_invoker_is_scheduled_or_on_demand(self) -> None:
        """On-demand jobs live in _INVOKERS but not SCHEDULED_JOBS.

        This test documents the expected on-demand set so adding a new
        invoker without scheduling it is a deliberate, visible choice.
        """
        from app.jobs.runtime import _INVOKERS
        from app.workers.scheduler import SCHEDULED_JOBS

        registry_names = {job.name for job in SCHEDULED_JOBS}
        invoker_names = set(_INVOKERS.keys())
        on_demand = invoker_names - registry_names
        # Phase 4: 12 former-scheduled jobs are now driven by the
        # orchestrator_full_sync DAG walk. They stay in _INVOKERS so
        # POST /jobs/{name}/run continues to work, but they are
        # no longer independently scheduled.
        expected_on_demand = {
            # Pre-Phase-4 on-demand (unchanged):
            "daily_tax_reconciliation",
            "nightly_universe_sync",
            # Phase-4 moved from SCHEDULED_JOBS to orchestrator-driven:
            "daily_candle_refresh",
            "daily_portfolio_sync",
            "daily_research_refresh",
            "fx_rates_refresh",
            "monthly_report",
            "morning_candidate_review",
            "seed_cost_models",
            "weekly_report",
            # Phase 1.4: attribution_summary retired from SCHEDULED_JOBS
            # (no UI consumer). Function stays in _INVOKERS for manual
            # trigger from Admin "Run now".
            "attribution_summary",
            # daily_cik_refresh + daily_financial_facts retired from _INVOKERS
            # in Chunk 3 of the 2026-04-19 research-tool refocus; they are
            # now called from inside fundamentals_sync.
            # daily_news_refresh + daily_thesis_refresh retired from _INVOKERS
            # in Phase 1.2 — thesis is now on-demand via
            # POST /instruments/{symbol}/thesis; news is deferred pending
            # a concrete NewsProvider wiring.
        }
        assert on_demand == expected_on_demand, (
            f"Unexpected on-demand invokers (update this test if intentional): "
            f"unexpected={sorted(on_demand - expected_on_demand)} "
            f"missing={sorted(expected_on_demand - on_demand)}"
        )


# ---------------------------------------------------------------------------
# Catch-up on boot
# ---------------------------------------------------------------------------

# Frozen "now" used across all catch-up tests so cadence arithmetic is
# deterministic.  Chosen to be after the daily 02:00 fire so a daily
# job with hour=2 whose last success is >24 h ago is overdue.
_NOW = datetime(2026, 4, 10, 3, 0, 0, tzinfo=UTC)

_DAILY_JOB = ScheduledJob(
    name="daily_job",
    description="test daily",
    cadence=Cadence.daily(hour=2, minute=0),
    catch_up_on_boot=True,
)

_NO_CATCHUP_JOB = ScheduledJob(
    name="no_catchup_job",
    description="test no catchup",
    cadence=Cadence.daily(hour=2, minute=0),
    catch_up_on_boot=False,
)

_PREREQ_MET_JOB = ScheduledJob(
    name="prereq_met_job",
    description="test with met prerequisite",
    cadence=Cadence.daily(hour=2, minute=0),
    catch_up_on_boot=True,
    prerequisite=lambda _conn: (True, ""),
)

_PREREQ_UNMET_JOB = ScheduledJob(
    name="prereq_unmet_job",
    description="test with unmet prerequisite",
    cadence=Cadence.daily(hour=2, minute=0),
    catch_up_on_boot=True,
    prerequisite=lambda _conn: (False, "no coverage rows"),
)


def _make_catchup_runtime(
    jobs: list[ScheduledJob],
    invokers: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    latest_runs: dict[str, datetime] | None = None,
) -> tuple[JobRuntime, list[str]]:
    """Build a runtime wired for catch-up testing.

    Returns ``(runtime, fired)`` where ``fired`` accumulates the names
    of jobs submitted to the executor by ``_catch_up``.
    """
    # Replace SCHEDULED_JOBS with only the test jobs so the registry
    # lookup inside _catch_up finds them.
    monkeypatch.setattr("app.jobs.runtime.SCHEDULED_JOBS", jobs)

    # Stub the DB query.
    monkeypatch.setattr(
        "app.jobs.runtime.fetch_latest_successful_runs",
        lambda _conn, _names: latest_runs or {},
    )

    # Stub record_job_skip so catch-up can record skips without a real DB.
    monkeypatch.setattr(
        "app.jobs.runtime.record_job_skip",
        lambda _conn, _name, _reason: 0,
    )

    # Stub psycopg.connect so _catch_up gets a no-op connection.
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    monkeypatch.setattr("app.jobs.runtime.psycopg.connect", lambda _url, **_kw: mock_conn)

    # Pin the clock.
    monkeypatch.setattr(
        "app.jobs.runtime.datetime",
        type("_FakeDT", (), {"now": staticmethod(lambda tz: _NOW)}),
    )

    rt = JobRuntime(
        database_url="postgresql://stub/stub",
        invokers=invokers,  # type: ignore[arg-type]
    )

    # The caller tracks invocations via side-effects in the invoker
    # closures; the second return value is kept for signature compat.
    return rt, []


class TestCatchUpOnBoot:
    """Tests for ``JobRuntime._catch_up()``.

    Each test calls ``_catch_up()`` directly (not via ``start()``) so
    the scheduler registration path is not exercised — that is already
    covered by ``TestStartWiring``.
    """

    def test_never_run_job_is_fired(
        self,
        patched_runtime: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fired: list[str] = []

        def invoker() -> None:
            fired.append("daily_job")

        rt, _ = _make_catchup_runtime(
            [_DAILY_JOB],
            {"daily_job": invoker},
            monkeypatch,
            latest_runs={},  # no successful runs
        )
        rt._catch_up()
        rt._manual_executor.shutdown(wait=True)
        assert fired == ["daily_job"]

    def test_overdue_job_is_fired(
        self,
        patched_runtime: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fired: list[str] = []

        def invoker() -> None:
            fired.append("daily_job")

        # Last success was 2 days ago — next fire would have been
        # yesterday at 02:00, which is before _NOW.
        two_days_ago = _NOW - timedelta(days=2)
        rt, _ = _make_catchup_runtime(
            [_DAILY_JOB],
            {"daily_job": invoker},
            monkeypatch,
            latest_runs={"daily_job": two_days_ago},
        )
        rt._catch_up()
        rt._manual_executor.shutdown(wait=True)
        assert fired == ["daily_job"]

    def test_current_job_is_not_fired(
        self,
        patched_runtime: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fired: list[str] = []

        def invoker() -> None:
            fired.append("daily_job")

        # Last success was 30 minutes ago — next fire is tomorrow at
        # 02:00, which is after _NOW.
        recent = _NOW - timedelta(minutes=30)
        rt, _ = _make_catchup_runtime(
            [_DAILY_JOB],
            {"daily_job": invoker},
            monkeypatch,
            latest_runs={"daily_job": recent},
        )
        rt._catch_up()
        rt._manual_executor.shutdown(wait=True)
        assert fired == []

    def test_catch_up_on_boot_false_skips_never_run(
        self,
        patched_runtime: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fired: list[str] = []

        def invoker() -> None:
            fired.append("no_catchup_job")

        rt, _ = _make_catchup_runtime(
            [_NO_CATCHUP_JOB],
            {"no_catchup_job": invoker},
            monkeypatch,
            latest_runs={},
        )
        rt._catch_up()
        rt._manual_executor.shutdown(wait=True)
        assert fired == []

    def test_db_failure_logs_and_does_not_crash(
        self,
        patched_runtime: None,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        monkeypatch.setattr("app.jobs.runtime.SCHEDULED_JOBS", [_DAILY_JOB])
        monkeypatch.setattr(
            "app.jobs.runtime.psycopg.connect",
            MagicMock(side_effect=RuntimeError("connection refused")),
        )
        monkeypatch.setattr(
            "app.jobs.runtime.datetime",
            type("_FakeDT", (), {"now": staticmethod(lambda tz: _NOW)}),
        )

        rt = JobRuntime(
            database_url="postgresql://stub/stub",
            invokers={"daily_job": lambda: None},  # type: ignore[dict-item]
        )
        # Must not raise; must log the failure.
        with caplog.at_level(logging.ERROR, logger="app.jobs.runtime"):
            rt._catch_up()
        assert "failed to query job_runs" in caplog.text

    def test_mixed_overdue_and_current(
        self,
        patched_runtime: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Only the overdue job fires; the current one is skipped."""
        fired: list[str] = []

        hourly_job = ScheduledJob(
            name="hourly_job",
            description="test hourly",
            cadence=Cadence.hourly(minute=5),
            catch_up_on_boot=True,
        )

        def daily_invoker() -> None:
            fired.append("daily_job")

        def hourly_invoker() -> None:
            fired.append("hourly_job")

        # daily_job: last success 2 days ago → overdue
        # hourly_job: last success 30 min ago → current
        rt, _ = _make_catchup_runtime(
            [_DAILY_JOB, hourly_job],
            {"daily_job": daily_invoker, "hourly_job": hourly_invoker},
            monkeypatch,
            latest_runs={
                "daily_job": _NOW - timedelta(days=2),
                "hourly_job": _NOW - timedelta(minutes=30),
            },
        )
        rt._catch_up()
        rt._manual_executor.shutdown(wait=True)
        assert fired == ["daily_job"]


# ---------------------------------------------------------------------------
# Catch-up with prerequisites
# ---------------------------------------------------------------------------


class TestCatchUpPrerequisites:
    """Tests for prerequisite-gated catch-up on boot."""

    def test_unmet_prerequisite_skips_overdue_job(
        self,
        patched_runtime: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fired: list[str] = []

        def invoker() -> None:
            fired.append("prereq_unmet_job")

        rt, _ = _make_catchup_runtime(
            [_PREREQ_UNMET_JOB],
            {"prereq_unmet_job": invoker},
            monkeypatch,
            latest_runs={},  # never run → overdue
        )
        rt._catch_up()
        rt._manual_executor.shutdown(wait=True)
        assert fired == []  # skipped, not fired

    def test_met_prerequisite_fires_overdue_job(
        self,
        patched_runtime: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fired: list[str] = []

        def invoker() -> None:
            fired.append("prereq_met_job")

        rt, _ = _make_catchup_runtime(
            [_PREREQ_MET_JOB],
            {"prereq_met_job": invoker},
            monkeypatch,
            latest_runs={},
        )
        rt._catch_up()
        rt._manual_executor.shutdown(wait=True)
        assert fired == ["prereq_met_job"]

    def test_mixed_prerequisites_only_met_jobs_fire(
        self,
        patched_runtime: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fired: list[str] = []

        def met_invoker() -> None:
            fired.append("prereq_met_job")

        def unmet_invoker() -> None:
            fired.append("prereq_unmet_job")

        rt, _ = _make_catchup_runtime(
            [_PREREQ_MET_JOB, _PREREQ_UNMET_JOB],
            {"prereq_met_job": met_invoker, "prereq_unmet_job": unmet_invoker},
            monkeypatch,
            latest_runs={},
        )
        rt._catch_up()
        rt._manual_executor.shutdown(wait=True)
        assert fired == ["prereq_met_job"]

    def test_no_prerequisite_job_fires_normally(
        self,
        patched_runtime: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Jobs without a prerequisite (None) are always fired when overdue."""
        fired: list[str] = []

        def invoker() -> None:
            fired.append("daily_job")

        rt, _ = _make_catchup_runtime(
            [_DAILY_JOB],
            {"daily_job": invoker},
            monkeypatch,
            latest_runs={},
        )
        rt._catch_up()
        rt._manual_executor.shutdown(wait=True)
        assert fired == ["daily_job"]

    def test_prerequisite_check_records_skip(
        self,
        patched_runtime: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """When a prerequisite is unmet, record_job_skip is called."""
        skip_calls: list[tuple[str, str]] = []

        monkeypatch.setattr("app.jobs.runtime.SCHEDULED_JOBS", [_PREREQ_UNMET_JOB])
        monkeypatch.setattr(
            "app.jobs.runtime.fetch_latest_successful_runs",
            lambda _conn, _names: {},
        )
        monkeypatch.setattr(
            "app.jobs.runtime.record_job_skip",
            lambda _conn, name, reason: skip_calls.append((name, reason)) or 0,
        )

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        monkeypatch.setattr("app.jobs.runtime.psycopg.connect", lambda _url, **_kw: mock_conn)
        monkeypatch.setattr(
            "app.jobs.runtime.datetime",
            type("_FakeDT", (), {"now": staticmethod(lambda tz: _NOW)}),
        )

        rt = JobRuntime(
            database_url="postgresql://stub/stub",
            invokers={"prereq_unmet_job": lambda: None},  # type: ignore[dict-item]
        )
        rt._catch_up()
        rt._manual_executor.shutdown(wait=True)
        assert len(skip_calls) == 1
        assert skip_calls[0] == ("prereq_unmet_job", "no coverage rows")


# ---------------------------------------------------------------------------
# Scheduled-fire prerequisite gate
# ---------------------------------------------------------------------------


class TestScheduledFirePrerequisite:
    """Tests for prerequisite checking in the scheduled-fire wrapper."""

    def test_unmet_prerequisite_skips_scheduled_fire(
        self,
        patched_runtime: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        invocations: list[int] = []

        def invoker() -> None:
            invocations.append(1)

        # Stub record_job_skip.
        monkeypatch.setattr(
            "app.jobs.runtime.record_job_skip",
            lambda _conn, _name, _reason: 0,
        )

        # Stub psycopg.connect for the prerequisite check.
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        monkeypatch.setattr("app.jobs.runtime.psycopg.connect", lambda _url, **_kw: mock_conn)
        monkeypatch.setattr("app.jobs.runtime.SCHEDULED_JOBS", [_PREREQ_UNMET_JOB])

        rt = _make_runtime({"prereq_unmet_job": invoker})
        wrapped = rt._wrap_invoker("prereq_unmet_job", invoker)
        wrapped()
        assert invocations == []  # invoker was NOT called

    def test_met_prerequisite_runs_scheduled_fire(
        self,
        patched_runtime: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        invocations: list[int] = []

        def invoker() -> None:
            invocations.append(1)

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        monkeypatch.setattr("app.jobs.runtime.psycopg.connect", lambda _url, **_kw: mock_conn)
        monkeypatch.setattr("app.jobs.runtime.SCHEDULED_JOBS", [_PREREQ_MET_JOB])

        rt = _make_runtime({"prereq_met_job": invoker})
        wrapped = rt._wrap_invoker("prereq_met_job", invoker)
        wrapped()
        assert invocations == [1]
