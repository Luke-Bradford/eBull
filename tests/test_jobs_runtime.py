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

import threading

import pytest

from app.jobs.locks import JobAlreadyRunning
from app.jobs.runtime import JobRuntime, UnknownJob


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
def _reset_fake_locks() -> None:
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

        rt.start()

        assert added == [f"recurring:{JOB_NIGHTLY_UNIVERSE_SYNC}"]

    def test_double_start_raises(self, patched_runtime: None) -> None:
        rt = _make_runtime({})
        rt._started = True  # simulate already started
        with pytest.raises(RuntimeError):
            rt.start()
