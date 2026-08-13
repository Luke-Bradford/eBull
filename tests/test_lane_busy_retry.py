"""#1538 — ``_fire_scheduled_with_lane_retry`` bounded-retry behaviour.

Pure unit tests (no DB, no real sleeping): a fake ``JobLock`` controls whether
the *acquire* succeeds, the body is an injected callable, and ``sleep`` is a
recorder. Verifies the two invariants Codex flagged at spec checkpoint-1:
retry ONLY the acquire (body-raised ``JobAlreadyRunning`` is not replayed), and
the ``BoundedSemaphore`` caps + releases waiter slots without leaking.
"""

from __future__ import annotations

import threading

import pytest

from app.jobs import runtime
from app.jobs.locks import JobAlreadyRunning

_BACKOFF = (0.25, 0.5, 1.0)  # 3 retries; matches the shape, values arbitrary for tests


def _fake_joblock_factory(state: dict[str, int]):
    """Return a JobLock stand-in. ``__enter__`` raises ``JobAlreadyRunning`` for
    the first ``state['fail_n']`` calls (acquire failures), then succeeds."""

    class _FakeLock:
        def __init__(self, _database_url: str, _job_name: str) -> None:
            pass

        def __enter__(self) -> _FakeLock:
            state["enter"] += 1
            if state["enter"] <= state["fail_n"]:
                raise JobAlreadyRunning("fake")
            return self

        def __exit__(self, *_exc: object) -> bool:
            return False

    return _FakeLock


def _slots_available(sem: threading.BoundedSemaphore) -> int:
    """Count free slots non-destructively (acquire all, then release them back)."""
    taken = 0
    while sem.acquire(blocking=False):
        taken += 1
    for _ in range(taken):
        sem.release()
    return taken


@pytest.fixture
def fresh_slots(monkeypatch: pytest.MonkeyPatch) -> threading.BoundedSemaphore:
    sem = threading.BoundedSemaphore(runtime._MAX_CONCURRENT_LANE_WAITERS)
    monkeypatch.setattr(runtime, "_LANE_WAIT_SLOTS", sem)
    return sem


def test_retry_then_run(monkeypatch: pytest.MonkeyPatch, fresh_slots: threading.BoundedSemaphore) -> None:
    state = {"enter": 0, "fail_n": 2}
    monkeypatch.setattr(runtime, "JobLock", _fake_joblock_factory(state))
    sleeps: list[float] = []
    ran: list[int] = []

    runtime._fire_scheduled_with_lane_retry(
        "db://x", "job", lambda: ran.append(1), backoff=_BACKOFF, sleep=sleeps.append
    )

    assert ran == [1]  # body ran exactly once
    assert state["enter"] == 3  # 2 failed acquires + 1 success
    assert sleeps == [0.25, 0.5]  # slept before each retry, not after success
    assert _slots_available(fresh_slots) == runtime._MAX_CONCURRENT_LANE_WAITERS  # slot released


def test_exhaust_then_skip(monkeypatch: pytest.MonkeyPatch, fresh_slots: threading.BoundedSemaphore) -> None:
    state = {"enter": 0, "fail_n": 99}  # always busy
    monkeypatch.setattr(runtime, "JobLock", _fake_joblock_factory(state))
    sleeps: list[float] = []
    ran: list[int] = []

    runtime._fire_scheduled_with_lane_retry(
        "db://x", "job", lambda: ran.append(1), backoff=_BACKOFF, sleep=sleeps.append
    )

    assert ran == []  # never ran
    assert state["enter"] == len(_BACKOFF) + 1  # initial + 3 retries
    assert sleeps == list(_BACKOFF)  # slept between each, then gave up
    assert _slots_available(fresh_slots) == runtime._MAX_CONCURRENT_LANE_WAITERS  # slot released


def test_body_raised_job_already_running_is_not_retried(
    monkeypatch: pytest.MonkeyPatch, fresh_slots: threading.BoundedSemaphore
) -> None:
    state = {"enter": 0, "fail_n": 0}  # acquire always succeeds
    monkeypatch.setattr(runtime, "JobLock", _fake_joblock_factory(state))
    sleeps: list[float] = []

    def _body() -> None:
        raise JobAlreadyRunning("body-origin")

    # Body-origin JobAlreadyRunning propagates to the caller, NOT retried.
    with pytest.raises(JobAlreadyRunning):
        runtime._fire_scheduled_with_lane_retry("db://x", "job", _body, backoff=_BACKOFF, sleep=sleeps.append)

    assert state["enter"] == 1  # exactly one acquire, no retry
    assert sleeps == []  # never slept
    assert _slots_available(fresh_slots) == runtime._MAX_CONCURRENT_LANE_WAITERS  # no slot taken/leaked


def test_no_free_slot_skips_immediately(
    monkeypatch: pytest.MonkeyPatch, fresh_slots: threading.BoundedSemaphore
) -> None:
    # Drain every waiter slot so the colliding fire cannot retry.
    for _ in range(runtime._MAX_CONCURRENT_LANE_WAITERS):
        assert fresh_slots.acquire(blocking=False)
    state = {"enter": 0, "fail_n": 99}
    monkeypatch.setattr(runtime, "JobLock", _fake_joblock_factory(state))
    sleeps: list[float] = []
    ran: list[int] = []

    runtime._fire_scheduled_with_lane_retry(
        "db://x", "job", lambda: ran.append(1), backoff=_BACKOFF, sleep=sleeps.append
    )

    assert ran == []  # skipped
    assert state["enter"] == 1  # one acquire attempt, then no slot → immediate skip
    assert sleeps == []  # never slept (no slot to wait in)
    # The helper took no slot it must release; the 3 we drained are still ours.
    assert _slots_available(fresh_slots) == 0


def test_body_other_exception_after_retry_releases_slot(
    monkeypatch: pytest.MonkeyPatch, fresh_slots: threading.BoundedSemaphore
) -> None:
    # attempt 0 fails acquire (slot taken) → attempt 1 acquires → body raises a
    # non-JobAlreadyRunning error. It must propagate AND the slot must be freed.
    state = {"enter": 0, "fail_n": 1}
    monkeypatch.setattr(runtime, "JobLock", _fake_joblock_factory(state))
    sleeps: list[float] = []

    def _body() -> None:
        raise ValueError("boom")

    with pytest.raises(ValueError):
        runtime._fire_scheduled_with_lane_retry("db://x", "job", _body, backoff=_BACKOFF, sleep=sleeps.append)

    assert state["enter"] == 2  # one failed acquire + one success (then body raised)
    assert sleeps == [0.25]  # slept once before the retry that acquired
    assert _slots_available(fresh_slots) == runtime._MAX_CONCURRENT_LANE_WAITERS  # slot released in finally


def test_body_job_already_running_after_retry_not_replayed(
    monkeypatch: pytest.MonkeyPatch, fresh_slots: threading.BoundedSemaphore
) -> None:
    # attempt 0 fails acquire (slot taken) → attempt 1 acquires → body raises
    # JobAlreadyRunning. acquired=True so it is NOT replayed; propagates; slot freed.
    state = {"enter": 0, "fail_n": 1}
    monkeypatch.setattr(runtime, "JobLock", _fake_joblock_factory(state))
    sleeps: list[float] = []
    body_calls: list[int] = []

    def _body() -> None:
        body_calls.append(1)
        raise JobAlreadyRunning("body-origin")

    with pytest.raises(JobAlreadyRunning):
        runtime._fire_scheduled_with_lane_retry("db://x", "job", _body, backoff=_BACKOFF, sleep=sleeps.append)

    assert body_calls == [1]  # body ran once, NOT replayed after raising
    assert state["enter"] == 2  # failed acquire + the acquire whose body raised
    assert sleeps == [0.25]  # only the pre-retry sleep
    assert _slots_available(fresh_slots) == runtime._MAX_CONCURRENT_LANE_WAITERS  # slot released in finally


def test_full_sync_gets_a_longer_patient_backoff_than_the_default() -> None:
    """A DAILY sync (orchestrator_full_sync) must wait out a multi-second
    high-freq gate hold, not the ~1.75s default (which made it skip a whole DAY,
    7/7 — last real run 15 Jun). A FREQUENT peer keeps the cheap default."""
    from app.workers.scheduler import (
        JOB_ORCHESTRATOR_FULL_SYNC,
        JOB_ORCHESTRATOR_HIGH_FREQUENCY_SYNC,
    )

    patient = runtime._lane_backoff_for(JOB_ORCHESTRATOR_FULL_SYNC)
    assert sum(patient) > sum(runtime._LANE_BUSY_RETRY_BACKOFF)
    assert len(patient) > len(runtime._LANE_BUSY_RETRY_BACKOFF)
    # the every-5-min peer that holds the gate keeps the cheap default — skipping
    # one of its cadences is harmless, and it must never wait long enough to
    # starve the pool.
    assert runtime._lane_backoff_for(JOB_ORCHESTRATOR_HIGH_FREQUENCY_SYNC) == runtime._LANE_BUSY_RETRY_BACKOFF


def test_every_daily_or_coarser_job_gets_patient_backoff() -> None:
    """#1710 regression guard. #1707 hardcoded patient backoff to
    orchestrator_full_sync ONLY, so raw_data_retention_sweep (02:00) +
    fundamentals_sync (02:30) — same 5-min-grid co-fire with high-freq — kept
    the ~1.75s default and were starved 2+ days. Patience is now DERIVED from
    cadence, so EVERY daily-or-coarser job is protected and the set can't drift
    from the schedule again."""
    from app.workers.scheduler import SCHEDULED_JOBS

    patient_kinds = runtime._PATIENT_BACKOFF_CADENCE_KINDS
    for job in SCHEDULED_JOBS:
        backoff = runtime._lane_backoff_for(job.name)
        if job.cadence.kind in patient_kinds:
            assert backoff is runtime._LANE_BACKOFF_DAILY_PATIENT, (
                f"{job.name} ({job.cadence.kind}) must get the patient backoff"
            )
        else:
            assert backoff is runtime._LANE_BUSY_RETRY_BACKOFF, (
                f"{job.name} ({job.cadence.kind}) must keep the cheap default"
            )

    # the two jobs that #1707 missed, asserted by name as explicit regression pins
    assert runtime._lane_backoff_for("raw_data_retention_sweep") is (runtime._LANE_BACKOFF_DAILY_PATIENT)
    assert runtime._lane_backoff_for("fundamentals_sync") is (runtime._LANE_BACKOFF_DAILY_PATIENT)
    # an unknown name (no SCHEDULED_JOBS row) falls back to the cheap default
    assert runtime._lane_backoff_for("not_a_real_job") is runtime._LANE_BUSY_RETRY_BACKOFF


def test_patient_backoff_rides_out_a_hold_the_default_would_skip(
    monkeypatch: pytest.MonkeyPatch, fresh_slots: threading.BoundedSemaphore
) -> None:
    """The fix, proven on the exact starvation shape: high-freq holds the sync
    gate for ~5s ≈ 4 failed acquires before releasing. The DEFAULT window (3
    retries → 4 acquires) exhausts and SKIPS (the day-long starvation). The
    PATIENT window (5 retries → 6 acquires) rides it out and RUNS."""
    from app.workers.scheduler import JOB_ORCHESTRATOR_FULL_SYNC

    patient = runtime._lane_backoff_for(JOB_ORCHESTRATOR_FULL_SYNC)

    # DEFAULT: busy through all 4 acquires → never runs (the bug).
    default_state = {"enter": 0, "fail_n": 4}
    monkeypatch.setattr(runtime, "JobLock", _fake_joblock_factory(default_state))
    default_ran: list[int] = []
    runtime._fire_scheduled_with_lane_retry(
        "db://x", "orchestrator_full_sync", lambda: default_ran.append(1), backoff=_BACKOFF, sleep=lambda _s: None
    )
    assert default_ran == []  # default window starves

    # PATIENT: same 4-acquire hold, then the 5th acquire (gate released) succeeds → runs.
    patient_state = {"enter": 0, "fail_n": 4}
    monkeypatch.setattr(runtime, "JobLock", _fake_joblock_factory(patient_state))
    patient_ran: list[int] = []
    runtime._fire_scheduled_with_lane_retry(
        "db://x", "orchestrator_full_sync", lambda: patient_ran.append(1), backoff=patient, sleep=lambda _s: None
    )
    assert patient_ran == [1]  # patient window rides out the hold and runs
    assert patient_state["enter"] == 5  # 4 failed acquires + 1 success
    assert _slots_available(fresh_slots) == runtime._MAX_CONCURRENT_LANE_WAITERS  # slot released


# ---------------------------------------------------------------------------
# #2052 — both lane-busy skip exits write a job_runs skip row (telemetry).
# ---------------------------------------------------------------------------


def test_exhaust_skip_records_lane_busy_row(
    monkeypatch: pytest.MonkeyPatch, fresh_slots: threading.BoundedSemaphore
) -> None:
    state = {"enter": 0, "fail_n": 99}  # always busy
    monkeypatch.setattr(runtime, "JobLock", _fake_joblock_factory(state))
    recorded: list[tuple[str, str, str, object]] = []
    monkeypatch.setattr(
        runtime,
        "_record_lane_busy_skip",
        lambda url, name, detail, params: recorded.append((url, name, detail, params)),
    )

    runtime._fire_scheduled_with_lane_retry(
        "db://x", "job", lambda: None, backoff=_BACKOFF, sleep=lambda _s: None, params={"k": 1}
    )

    assert len(recorded) == 1
    url, name, detail, params = recorded[0]
    assert (url, name) == ("db://x", "job")
    assert "retry window" in detail
    assert params == {"k": 1}


def test_no_slot_skip_records_lane_busy_row(
    monkeypatch: pytest.MonkeyPatch, fresh_slots: threading.BoundedSemaphore
) -> None:
    for _ in range(runtime._MAX_CONCURRENT_LANE_WAITERS):
        assert fresh_slots.acquire(blocking=False)
    state = {"enter": 0, "fail_n": 99}
    monkeypatch.setattr(runtime, "JobLock", _fake_joblock_factory(state))
    recorded: list[tuple[str, str]] = []
    monkeypatch.setattr(
        runtime,
        "_record_lane_busy_skip",
        lambda _url, name, detail, _params: recorded.append((name, detail)),
    )

    runtime._fire_scheduled_with_lane_retry("db://x", "job", lambda: None, backoff=_BACKOFF, sleep=lambda _s: None)

    assert len(recorded) == 1
    assert "lane-retry slots" in recorded[0][1]


def test_successful_run_records_no_skip_row(
    monkeypatch: pytest.MonkeyPatch, fresh_slots: threading.BoundedSemaphore
) -> None:
    state = {"enter": 0, "fail_n": 1}  # one lost acquire, then runs
    monkeypatch.setattr(runtime, "JobLock", _fake_joblock_factory(state))
    recorded: list[str] = []
    monkeypatch.setattr(
        runtime,
        "_record_lane_busy_skip",
        lambda _url, name, _detail, _params: recorded.append(name),
    )

    runtime._fire_scheduled_with_lane_retry("db://x", "job", lambda: None, backoff=_BACKOFF, sleep=lambda _s: None)

    assert recorded == []


def test_body_origin_job_already_running_records_no_skip_row(
    monkeypatch: pytest.MonkeyPatch, fresh_slots: threading.BoundedSemaphore
) -> None:
    state = {"enter": 0, "fail_n": 0}
    monkeypatch.setattr(runtime, "JobLock", _fake_joblock_factory(state))
    recorded: list[str] = []
    monkeypatch.setattr(
        runtime,
        "_record_lane_busy_skip",
        lambda _url, name, _detail, _params: recorded.append(name),
    )

    def _body() -> None:
        raise JobAlreadyRunning("body-origin")

    with pytest.raises(JobAlreadyRunning):
        runtime._fire_scheduled_with_lane_retry("db://x", "job", _body, backoff=_BACKOFF, sleep=lambda _s: None)

    assert recorded == []


def test_record_lane_busy_skip_composes_prefix_and_swallows_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The writer prepends the machine-checkable prefix and never raises."""
    from app.services.ops_monitor import LANE_BUSY_SKIP_PREFIX

    calls: list[tuple[str, str, dict[str, object] | None]] = []

    class _FakeConn:
        def __enter__(self) -> _FakeConn:
            return self

        def __exit__(self, *_exc: object) -> bool:
            return False

    def _fake_record(
        conn: object, job_name: str, reason: str, *, params_snapshot: dict[str, object] | None = None
    ) -> int:
        calls.append((job_name, reason, params_snapshot))
        return 1

    monkeypatch.setattr(runtime.psycopg, "connect", lambda *_a, **_k: _FakeConn())
    monkeypatch.setattr(runtime, "record_job_skip", _fake_record)

    runtime._record_lane_busy_skip("db://x", "job", "lane stayed busy", {"p": 2})
    assert calls == [("job", LANE_BUSY_SKIP_PREFIX + "lane stayed busy", {"p": 2})]

    # Writer failure is logged, never raised.
    def _boom(*_a: object, **_k: object) -> int:
        raise RuntimeError("db down")

    monkeypatch.setattr(runtime, "record_job_skip", _boom)
    runtime._record_lane_busy_skip("db://x", "job", "lane stayed busy", None)  # must not raise
