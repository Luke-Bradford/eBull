"""#1226 — ``_run_stage_under_lock_with_retry`` bounded acquire-retry.

Pure unit tests (no DB, no real sleeping): a fake ``JobLock`` controls whether
the *acquire* succeeds, the body is an injected callable, and ``sleep`` is a
recorder. A bootstrap stage that races a standalone cron / manual trigger for
the source ``JobLock`` waits the holder out instead of erroring on the first
miss (#1226). Mirrors the #1538 invariant: retry ONLY the acquire; a
body-raised ``JobAlreadyRunning`` is propagated, never replayed.
"""

from __future__ import annotations

import pytest

from app.jobs.locks import JobAlreadyRunning
from app.services import bootstrap_orchestrator as orch
from app.services.bootstrap_state import BootstrapStageCancelled

_BACKOFF = (0.25, 0.5, 1.0)  # 3 retries; shape matches prod, values arbitrary for tests


def _fake_joblock_factory(state: dict[str, int]):
    """JobLock stand-in. ``__enter__`` raises ``JobAlreadyRunning`` for the first
    ``state['fail_n']`` calls (acquire failures), then succeeds."""

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


def _call(**kw):
    # Default cancel_check to a no-op so the pure tests never touch a DB; the
    # cancel-during-retry tests override it.
    kw.setdefault("cancel_check", lambda: False)
    return orch._run_stage_under_lock_with_retry(
        database_url="db://x",
        job_name="sec_business_summary_bootstrap",
        run_id=3,
        stage_key="sec_business_summary_bootstrap",
        **kw,
    )


def test_lock_free_runs_immediately(monkeypatch: pytest.MonkeyPatch) -> None:
    state = {"enter": 0, "fail_n": 0}
    monkeypatch.setattr(orch, "JobLock", _fake_joblock_factory(state))
    sleeps: list[float] = []
    ran: list[int] = []

    _call(run=lambda: ran.append(1), backoff=_BACKOFF, sleep=sleeps.append)

    assert ran == [1]
    assert state["enter"] == 1  # one acquire, no retry
    assert sleeps == []  # never slept on a free lock


def test_busy_then_free_waits_out_the_holder(monkeypatch: pytest.MonkeyPatch) -> None:
    # The reported symptom: a cron holds the lock for the first 2 acquires,
    # releases, and the stage then runs — no error, no manual retry click.
    state = {"enter": 0, "fail_n": 2}
    monkeypatch.setattr(orch, "JobLock", _fake_joblock_factory(state))
    sleeps: list[float] = []
    ran: list[int] = []

    _call(run=lambda: ran.append(1), backoff=_BACKOFF, sleep=sleeps.append)

    assert ran == [1]  # body ran exactly once after the holder released
    assert state["enter"] == 3  # 2 failed acquires + 1 success
    assert sleeps == [0.25, 0.5]  # slept before each retry, not after success


def test_persistent_contention_raises_after_window(monkeypatch: pytest.MonkeyPatch) -> None:
    # Lock held through the whole window → genuine contention/deadlock: re-raise
    # so the caller records a stage error (the prompt operator signal).
    state = {"enter": 0, "fail_n": 99}
    monkeypatch.setattr(orch, "JobLock", _fake_joblock_factory(state))
    sleeps: list[float] = []
    ran: list[int] = []

    with pytest.raises(JobAlreadyRunning):
        _call(run=lambda: ran.append(1), backoff=_BACKOFF, sleep=sleeps.append)

    assert ran == []  # never ran
    assert state["enter"] == len(_BACKOFF) + 1  # initial + 3 retries
    assert sleeps == list(_BACKOFF)  # slept between each, then gave up


def test_body_raised_job_already_running_is_not_replayed(monkeypatch: pytest.MonkeyPatch) -> None:
    # acquire succeeds, the body itself raises JobAlreadyRunning (an invoker
    # sub-call hit its own JobLock). acquired=True → propagate, never replay.
    state = {"enter": 0, "fail_n": 0}
    monkeypatch.setattr(orch, "JobLock", _fake_joblock_factory(state))
    sleeps: list[float] = []
    body_calls: list[int] = []

    def _body() -> None:
        body_calls.append(1)
        raise JobAlreadyRunning("body-origin")

    with pytest.raises(JobAlreadyRunning):
        _call(run=_body, backoff=_BACKOFF, sleep=sleeps.append)

    assert body_calls == [1]  # ran once, NOT replayed
    assert state["enter"] == 1  # exactly one acquire
    assert sleeps == []  # never slept


def test_body_other_exception_propagates(monkeypatch: pytest.MonkeyPatch) -> None:
    # A non-JobAlreadyRunning body error propagates unchanged (the caller's
    # broad `except Exception` records it as a stage error).
    state = {"enter": 0, "fail_n": 1}
    monkeypatch.setattr(orch, "JobLock", _fake_joblock_factory(state))
    sleeps: list[float] = []

    def _body() -> None:
        raise ValueError("boom")

    with pytest.raises(ValueError):
        _call(run=_body, backoff=_BACKOFF, sleep=sleeps.append)

    assert state["enter"] == 2  # one failed acquire + the acquire whose body raised
    assert sleeps == [0.25]  # slept once before the retry that acquired


def test_cancel_after_acquire_does_not_run_the_body(monkeypatch: pytest.MonkeyPatch) -> None:
    # The dispatcher swept the run cancelled while we waited for the lock. The
    # lock then frees; we acquire — but must NOT run the body post-cancel.
    state = {"enter": 0, "fail_n": 0}  # acquire succeeds immediately
    monkeypatch.setattr(orch, "JobLock", _fake_joblock_factory(state))
    ran: list[int] = []

    with pytest.raises(BootstrapStageCancelled):
        _call(run=lambda: ran.append(1), backoff=_BACKOFF, sleep=lambda _s: None, cancel_check=lambda: True)

    assert ran == []  # body never ran after cancel
    assert state["enter"] == 1  # acquired once, then bailed before run()


def test_cancel_during_backoff_aborts_without_finishing_window(monkeypatch: pytest.MonkeyPatch) -> None:
    # Lock stays busy; the run is cancelled mid-wait → bail cooperatively instead
    # of burning the whole ~30s window then erroring.
    state = {"enter": 0, "fail_n": 99}  # always busy
    monkeypatch.setattr(orch, "JobLock", _fake_joblock_factory(state))
    sleeps: list[float] = []
    checks = {"n": 0}

    def _cancel() -> bool:
        # Not cancelled on the first check (so one sleep happens), cancelled after.
        checks["n"] += 1
        return checks["n"] > 1

    with pytest.raises(BootstrapStageCancelled):
        _call(run=lambda: None, backoff=_BACKOFF, sleep=sleeps.append, cancel_check=_cancel)

    assert sleeps == [0.25]  # only the first retry slept; then cancel aborted the wait
    assert state["enter"] == 2  # initial acquire-fail + one retry acquire-fail


def test_prod_window_outlasts_a_carveout_job_hold() -> None:
    """The default window must comfortably exceed a short carve-out job's hold
    so the common contention case auto-resolves rather than erroring."""
    assert sum(orch._LOCK_BUSY_RETRY_BACKOFF) >= 25.0
    assert len(orch._LOCK_BUSY_RETRY_BACKOFF) >= 4
