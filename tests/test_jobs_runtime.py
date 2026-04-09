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
        from app.workers.scheduler import JOB_NIGHTLY_UNIVERSE_SYNC

        added: list[str] = []

        rt = _make_runtime(
            {
                JOB_NIGHTLY_UNIVERSE_SYNC: lambda: None,
                "not_in_registry": lambda: None,
            }
        )

        def fake_add_job(*args: object, **kwargs: object) -> None:
            added.append(str(kwargs.get("id", "")))

        monkeypatch.setattr(rt._scheduler, "add_job", fake_add_job)
        monkeypatch.setattr(rt._scheduler, "start", lambda: None)
        monkeypatch.setattr(rt, "_catch_up", lambda: None)

        rt.start()

        assert added == [f"recurring:{JOB_NIGHTLY_UNIVERSE_SYNC}"]

    def test_double_start_raises(self, patched_runtime: None) -> None:
        rt = _make_runtime({})
        rt._started = True  # simulate already started
        with pytest.raises(RuntimeError):
            rt.start()


class TestProductionInvokerRegistry:
    """The production ``_INVOKERS`` map must equal ``SCHEDULED_JOBS``.

    Drift guard for PR B: a job declared in the registry without an
    invoker would silently 404 on the manual trigger endpoint, and an
    invoker without a registry entry would never run on its cadence.
    Both states are bugs we want to catch at the test layer rather
    than discovering in production.
    """

    def test_invokers_cover_every_scheduled_job(self) -> None:
        from app.jobs.runtime import _INVOKERS
        from app.workers.scheduler import SCHEDULED_JOBS

        registry_names = {job.name for job in SCHEDULED_JOBS}
        invoker_names = set(_INVOKERS.keys())
        assert registry_names == invoker_names, (
            f"Drift between SCHEDULED_JOBS and _INVOKERS:\n"
            f"  in registry but not wired: {sorted(registry_names - invoker_names)}\n"
            f"  wired but not in registry: {sorted(invoker_names - registry_names)}"
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

    # Stub psycopg.connect so _catch_up gets a no-op connection.
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    monkeypatch.setattr("app.jobs.runtime.psycopg.connect", lambda _url: mock_conn)

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
