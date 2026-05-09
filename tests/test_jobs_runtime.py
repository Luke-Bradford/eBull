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
    # #1071 — admin control hub PR3 added a lock+fence prelude inside
    # _wrap_invoker / _run_manual that opens its own DB connection.
    # Tests in this module pass a stub URL ("postgresql://stub/stub")
    # and exercise the JobRuntime shape without staging a real DB —
    # bypass the prelude entirely so the invoker still runs.
    monkeypatch.setattr(
        "app.jobs.runtime.run_with_prelude",
        lambda _url, _name, invoker, **_kw: invoker(),
    )


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
            # #994 (first-install bootstrap orchestrator) — these jobs
            # are dispatched by the bootstrap orchestrator (not SCHEDULED)
            # but registered in _INVOKERS so the orchestrator can call
            # them via JobLock + so admin Run-now still works:
            "bootstrap_orchestrator",
            "bootstrap_filings_history_seed",
            "bootstrap_sec_13f_recent_sweep",
            "sec_first_install_drain",
            # #994 also un-retired these for bootstrap dispatch. They
            # are NOT in SCHEDULED_JOBS — daily_cik_refresh and
            # daily_financial_facts run only when the bootstrap
            # orchestrator dispatches them or when the operator
            # triggers them manually:
            "daily_cik_refresh",
            "daily_financial_facts",
            # daily_news_refresh + daily_thesis_refresh retired from _INVOKERS
            # in Phase 1.2 — thesis is now on-demand via
            # POST /instruments/{symbol}/thesis; news is deferred pending
            # a concrete NewsProvider wiring.
            # #1020 / #1027 / #1029 — bulk-archive Phase A3/C ingesters.
            # Registered in _INVOKERS so the bootstrap orchestrator can
            # dispatch them; NOT in SCHEDULED_JOBS (only fire via
            # bootstrap-driven dispatch or operator Run-now).
            "sec_bulk_download",
            "sec_submissions_ingest",
            "sec_submissions_files_walk",
            "sec_companyfacts_ingest",
            "sec_13f_ingest_from_dataset",
            "sec_insider_ingest_from_dataset",
            "sec_nport_ingest_from_dataset",
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
    source="db",  # PR1a — required field; arbitrary safe choice for test fixture
    catch_up_on_boot=True,
)

_NO_CATCHUP_JOB = ScheduledJob(
    name="no_catchup_job",
    description="test no catchup",
    cadence=Cadence.daily(hour=2, minute=0),
    source="db",
    catch_up_on_boot=False,
)

_PREREQ_MET_JOB = ScheduledJob(
    name="prereq_met_job",
    description="test with met prerequisite",
    cadence=Cadence.daily(hour=2, minute=0),
    source="db",
    catch_up_on_boot=True,
    prerequisite=lambda _conn: (True, ""),
)

_PREREQ_UNMET_JOB = ScheduledJob(
    name="prereq_unmet_job",
    description="test with unmet prerequisite",
    cadence=Cadence.daily(hour=2, minute=0),
    source="db",
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
    # #1071 — bypass the lock+fence prelude in catch-up tests; this
    # fixture's mocked psycopg.connect would otherwise return a
    # MagicMock whose ``cur.fetchone()`` reads as truthy and triggers
    # a false "fence held" branch, skipping every catch-up invoker.
    monkeypatch.setattr(
        "app.jobs.runtime.run_with_prelude",
        lambda _url, _name, invoker, **_kw: invoker(),
    )

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
            source="db",  # PR1a — required field; arbitrary safe choice for test fixture
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


class TestStartCatchUpEnvGate:
    """Tests for the ``EBULL_SKIP_CATCH_UP`` env-var gate on ``start()``.

    The gate wraps the ``self._catch_up()`` call at the end of ``start()``
    so pytest sessions can enter the FastAPI lifespan without firing real
    overdue APScheduler jobs. Direct calls to ``rt._catch_up()`` are
    NOT gated (covered in ``TestCatchUpOnBoot``).
    """

    def test_env_var_set_skips_catch_up_in_start(
        self,
        patched_runtime: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from app.jobs import runtime as rt_mod

        calls: list[str] = []
        monkeypatch.setattr(
            rt_mod.JobRuntime,
            "_catch_up",
            lambda self: calls.append("called"),
        )
        monkeypatch.setenv("EBULL_SKIP_CATCH_UP", "1")

        rt = rt_mod.JobRuntime()
        try:
            rt.start()
            assert calls == [], "start() must skip _catch_up() when EBULL_SKIP_CATCH_UP=1"
        finally:
            rt.shutdown()

    def test_env_var_unset_runs_catch_up_in_start(
        self,
        patched_runtime: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from app.jobs import runtime as rt_mod

        calls: list[str] = []
        monkeypatch.setattr(
            rt_mod.JobRuntime,
            "_catch_up",
            lambda self: calls.append("called"),
        )
        monkeypatch.delenv("EBULL_SKIP_CATCH_UP", raising=False)

        rt = rt_mod.JobRuntime()
        try:
            rt.start()
            assert calls == ["called"], "start() must invoke _catch_up() when env var unset"
        finally:
            rt.shutdown()

    def test_env_var_zero_runs_catch_up_in_start(
        self,
        patched_runtime: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Exact-match gate: only '1' skips. Any other value fires catch-up.

        Lets a developer override the conftest.py default with
        EBULL_SKIP_CATCH_UP=0 pytest to reproduce catch-up bugs.
        """
        from app.jobs import runtime as rt_mod

        calls: list[str] = []
        monkeypatch.setattr(
            rt_mod.JobRuntime,
            "_catch_up",
            lambda self: calls.append("called"),
        )
        monkeypatch.setenv("EBULL_SKIP_CATCH_UP", "0")

        rt = rt_mod.JobRuntime()
        try:
            rt.start()
            assert calls == ["called"], "EBULL_SKIP_CATCH_UP=0 must still fire catch-up"
        finally:
            rt.shutdown()


class TestShutdownBoundedWait:
    """Bounded shutdown — a hung scheduler/executor must NOT block
    lifespan teardown indefinitely. The shutdown call escalates to
    ``wait=False`` after ``timeout_s`` seconds and returns.
    """

    def test_hung_scheduler_does_not_block_shutdown(
        self, patched_runtime: None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When ``shutdown(wait=True)`` hangs, ``JobRuntime.shutdown``
        abandons the daemon thread after ``timeout_s`` and returns.
        No concurrent ``shutdown(wait=False)`` re-entry — that's
        unsafe for APScheduler (Codex review)."""
        import time

        rt = _make_runtime({"j1": lambda: None})
        rt.start()

        def _hang(*args: object, **kwargs: object) -> None:
            threading.Event().wait()  # blocks forever

        monkeypatch.setattr(rt._scheduler, "shutdown", _hang)
        monkeypatch.setattr(rt._manual_executor, "shutdown", lambda *a, **kw: None)

        start = time.monotonic()
        rt.shutdown(timeout_s=0.5)
        elapsed = time.monotonic() - start
        # Bounded by 0.5s timeout + thread join overhead. Without the
        # fix this would hang forever; allow generous slack.
        assert elapsed < 5.0, f"shutdown took {elapsed:.2f}s, expected <5s"

    def test_hung_executor_does_not_block_shutdown(
        self, patched_runtime: None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Same bounded wait + abandon for the manual executor."""
        import time

        rt = _make_runtime({"j1": lambda: None})
        rt.start()

        def _hang(*args: object, **kwargs: object) -> None:
            threading.Event().wait()

        monkeypatch.setattr(rt._scheduler, "shutdown", lambda *a, **kw: None)
        monkeypatch.setattr(rt._manual_executor, "shutdown", _hang)

        start = time.monotonic()
        rt.shutdown(timeout_s=0.5)
        elapsed = time.monotonic() - start
        assert elapsed < 5.0, f"shutdown took {elapsed:.2f}s"


class TestShutdownDoesNotBlockOnInflightJobs:
    """#657 — shutdown must NOT wait for in-flight jobs. APScheduler is
    stopped with wait=False; the manual executor with wait=False +
    cancel_futures=True. Recovery is the boot reaper's job."""

    def test_scheduler_shutdown_uses_wait_false(self, patched_runtime: None, monkeypatch: pytest.MonkeyPatch) -> None:
        """The previous impl used wait=True and routinely hit the 30s
        cap on edits-during-fundamentals-sync, causing the uvicorn
        --reload supervisor to give up and exit dirty. wait=False
        means in-flight jobs get hard-killed by process exit and the
        boot reaper transitions their orphaned sync_runs / layer rows
        on the next startup."""
        rt = _make_runtime({"j1": lambda: None})
        rt.start()

        captured: dict[str, object] = {}

        def fake_scheduler_shutdown(*args: object, **kwargs: object) -> None:
            captured["scheduler_kwargs"] = kwargs
            captured["scheduler_args"] = args

        monkeypatch.setattr(rt._scheduler, "shutdown", fake_scheduler_shutdown)
        monkeypatch.setattr(rt._manual_executor, "shutdown", lambda *a, **kw: None)

        rt.shutdown()

        assert captured["scheduler_kwargs"] == {"wait": False}

    def test_executor_shutdown_uses_wait_false_and_cancel_futures(
        self, patched_runtime: None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """cancel_futures=True drops manual triggers that are still
        queued so they don't keep the executor alive past shutdown.
        In-flight ones get hard-killed by process exit."""
        rt = _make_runtime({"j1": lambda: None})
        rt.start()

        captured: dict[str, object] = {}

        def fake_executor_shutdown(*args: object, **kwargs: object) -> None:
            captured["executor_kwargs"] = kwargs

        monkeypatch.setattr(rt._scheduler, "shutdown", lambda *a, **kw: None)
        monkeypatch.setattr(rt._manual_executor, "shutdown", fake_executor_shutdown)

        rt.shutdown()

        assert captured["executor_kwargs"] == {"wait": False, "cancel_futures": True}

    def test_inflight_job_does_not_delay_shutdown(self, patched_runtime: None, monkeypatch: pytest.MonkeyPatch) -> None:
        """Real-process simulation: a manual trigger submits a job that
        sleeps long; shutdown must return promptly because we no longer
        wait for it. The previous wait=True path would have blocked
        until the sleep completed."""
        import time

        slept_for = 60.0  # job would sleep 60s if waited on
        rt = _make_runtime({"slow": lambda: time.sleep(slept_for)})
        rt.start()
        rt._manual_executor.submit(lambda: time.sleep(slept_for))

        start = time.monotonic()
        rt.shutdown()
        elapsed = time.monotonic() - start
        assert elapsed < 2.0, (
            f"shutdown took {elapsed:.2f}s — should be sub-second with wait=False; "
            f"a value near {slept_for}s would mean wait=True regressed"
        )

    def test_default_timeout_is_short(self, patched_runtime: None) -> None:
        """Default timeout is the belt-and-suspenders cap for the case
        where the underlying shutdown(wait=False) call ITSELF wedges
        (very rare). 5s is short enough that the uvicorn supervisor
        does not give up on the relaunch but long enough to absorb
        any cleanup the libraries do."""
        import inspect

        sig = inspect.signature(JobRuntime.shutdown)
        default = sig.parameters["timeout_s"].default
        assert default <= 10.0, (
            f"shutdown default timeout is {default}s — should be <=10s so "
            f"the uvicorn --reload supervisor does not abandon the relaunch"
        )
