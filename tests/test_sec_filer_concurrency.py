"""Pure-logic tests for the parallel filer-ingest driver (#1274).

No DB: the per-filer ``run_one`` callable is faked, and ``make_filer_runner``'s
connection handling is exercised against a fake ``connect_job``. The real
connection path is covered by the dev-verify step in the #1274 spec.
"""

from __future__ import annotations

import threading
import time

import app.services.sec_filer_concurrency as sfc
from app.jobs.job_connection import job_statement_timeout_ms
from app.services.sec_filer_concurrency import (
    FilerWorkResult,
    drain_filers_concurrently,
    make_filer_runner,
)


def _ok(cik: str) -> FilerWorkResult[str]:
    return FilerWorkResult(cik=cik, summary=f"summary:{cik}", error=None)


# ---------------------------------------------------------------------------
# drain_filers_concurrently — fan-out + bounded concurrency
# ---------------------------------------------------------------------------


def test_fan_out_completes_all_once_within_concurrency_cap() -> None:
    ciks = [f"{i:010d}" for i in range(40)]
    live = 0
    max_live = 0
    lock = threading.Lock()

    def run_one(cik: str) -> FilerWorkResult[str]:
        nonlocal live, max_live
        with lock:
            live += 1
            max_live = max(max_live, live)
        time.sleep(0.005)
        with lock:
            live -= 1
        return _ok(cik)

    results: list[FilerWorkResult[str]] = []
    outcome = drain_filers_concurrently(
        ciks,
        run_one=run_one,
        concurrency=4,
        deadline_ts=None,
        on_result=results.append,
    )

    assert outcome.completed == 40
    assert outcome.submitted == 40
    assert not outcome.cancelled and not outcome.deadline_hit
    assert sorted(r.cik for r in results) == sorted(ciks)  # each exactly once
    assert max_live <= 4  # never exceeded the cap


def test_one_filer_crash_does_not_poison_the_batch() -> None:
    ciks = ["A", "B", "C", "D", "E"]

    def run_one(cik: str) -> FilerWorkResult[str]:
        if cik == "C":
            # make_filer_runner would catch the real exception and hand us this
            return FilerWorkResult(cik=cik, summary=None, error="C: boom")
        return _ok(cik)

    results: list[FilerWorkResult[str]] = []
    outcome = drain_filers_concurrently(
        ciks, run_one=run_one, concurrency=2, deadline_ts=None, on_result=results.append
    )

    # Assert DOWNSTREAM of the injected failure: every other filer completed.
    assert outcome.completed == 5
    crashed = [r for r in results if r.crashed]
    ok = [r for r in results if not r.crashed]
    assert [r.cik for r in crashed] == ["C"]
    assert sorted(r.cik for r in ok) == ["A", "B", "D", "E"]


def test_cancel_halts_new_submissions_and_drains_in_flight() -> None:
    ciks = [str(i) for i in range(20)]
    cancel_flag = threading.Event()
    completed: list[str] = []

    def run_one(cik: str) -> FilerWorkResult[str]:
        time.sleep(0.02)
        return _ok(cik)

    def on_result(result: FilerWorkResult[str]) -> None:
        completed.append(result.cik)
        cancel_flag.set()  # request cancel as soon as the first filer completes

    outcome = drain_filers_concurrently(
        ciks,
        run_one=run_one,
        concurrency=2,
        deadline_ts=None,
        on_result=on_result,
        should_cancel=cancel_flag.is_set,
        heartbeat_seconds=0.01,
    )

    assert outcome.cancelled is True
    assert outcome.deadline_hit is False
    # Stopped submitting after cancel: far fewer than the full cohort ran, but
    # the in-flight set drained (submitted == completed, always).
    assert outcome.completed == outcome.submitted
    assert outcome.completed < 20


def test_expired_deadline_at_entry_submits_nothing() -> None:
    ciks = ["A", "B", "C"]
    calls: list[str] = []

    def run_one(cik: str) -> FilerWorkResult[str]:
        calls.append(cik)
        return _ok(cik)

    outcome = drain_filers_concurrently(
        ciks,
        run_one=run_one,
        concurrency=4,
        deadline_ts=time.monotonic() - 1.0,  # already past
        on_result=lambda _r: None,
    )

    assert outcome.deadline_hit is True
    assert outcome.cancelled is False
    assert outcome.submitted == 0
    assert outcome.completed == 0
    assert calls == []


def test_duplicate_ciks_are_deduped_before_dispatch() -> None:
    ciks = ["A", "A", "B", "A", "B", "C"]
    calls: list[str] = []
    lock = threading.Lock()

    def run_one(cik: str) -> FilerWorkResult[str]:
        with lock:
            calls.append(cik)
        return _ok(cik)

    outcome = drain_filers_concurrently(
        ciks, run_one=run_one, concurrency=3, deadline_ts=None, on_result=lambda _r: None
    )

    assert outcome.completed == 3
    assert sorted(calls) == ["A", "B", "C"]  # each distinct cik run exactly once


def test_concurrency_clamped_to_at_least_one() -> None:
    ciks = ["A", "B", "C"]
    results: list[FilerWorkResult[str]] = []

    outcome = drain_filers_concurrently(ciks, run_one=_ok, concurrency=0, deadline_ts=None, on_result=results.append)

    assert outcome.completed == 3
    assert sorted(r.cik for r in results) == ["A", "B", "C"]


def test_on_progress_ticks_at_least_once_with_completed_count() -> None:
    ciks = ["A", "B"]
    ticks: list[int] = []

    drain_filers_concurrently(
        ciks,
        run_one=_ok,
        concurrency=2,
        deadline_ts=None,
        on_result=lambda _r: None,
        on_progress=ticks.append,
    )

    assert ticks  # at least one progress tick
    assert ticks[-1] == 2  # final tick reports all completed


# ---------------------------------------------------------------------------
# make_filer_runner — connection / transaction / ContextVar handling
# ---------------------------------------------------------------------------


class _FakeConn:
    """Mimics psycopg3 Connection-as-context-manager: commit on clean exit,
    rollback if the body raised."""

    def __init__(self) -> None:
        self.committed = False
        self.rolled_back = False

    def __enter__(self) -> _FakeConn:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:  # type: ignore[no-untyped-def]
        if exc_type is None:
            self.committed = True
        else:
            self.rolled_back = True
        return False  # propagate the exception

    def commit(self) -> None:
        self.committed = True


def test_make_filer_runner_success_commits_and_returns_summary(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    fake = _FakeConn()
    monkeypatch.setattr(sfc, "connect_job", lambda: fake)

    runner = make_filer_runner(lambda conn, cik: f"done:{cik}", statement_timeout_ms=None)
    result = runner("0000000001")

    assert result.summary == "done:0000000001"
    assert result.error is None
    assert fake.committed is True
    assert fake.rolled_back is False


def test_make_filer_runner_crash_rolls_back_and_isolates(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    fake = _FakeConn()
    monkeypatch.setattr(sfc, "connect_job", lambda: fake)

    def work(conn, cik):  # type: ignore[no-untyped-def]
        raise RuntimeError("kaboom")

    runner = make_filer_runner(work, statement_timeout_ms=None)
    result = runner("0000000002")

    # The except wraps the `with` from OUTSIDE → the failed tx is ROLLED BACK,
    # never committed. If the catch were inside the `with`, fake.committed
    # would be True here (the Codex ckpt-1 HIGH-3 regression).
    assert result.summary is None
    assert result.crashed is True
    assert "kaboom" in (result.error or "")
    assert fake.rolled_back is True
    assert fake.committed is False


def test_make_filer_runner_resets_statement_timeout_contextvar(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr(sfc, "connect_job", _FakeConn)
    token = job_statement_timeout_ms.set(1234)
    try:
        runner = make_filer_runner(lambda conn, cik: "ok", statement_timeout_ms=9999)
        runner("x")
        # After the runner returns, the worker's set() was reset in `finally`,
        # so the caller's value is intact (no stale leak across pool tasks).
        assert job_statement_timeout_ms.get() == 1234
    finally:
        job_statement_timeout_ms.reset(token)


def test_make_filer_runner_retries_deadlock_then_succeeds(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from psycopg import errors as pg_errors

    monkeypatch.setattr(sfc, "connect_job", _FakeConn)
    calls = {"n": 0}

    def work(conn, cik):  # type: ignore[no-untyped-def]
        calls["n"] += 1
        if calls["n"] == 1:
            raise pg_errors.DeadlockDetected("deadlock detected")
        return f"ok:{cik}"

    runner = make_filer_runner(work, statement_timeout_ms=None, max_deadlock_retries=3)
    result = runner("0000000009")

    # First attempt deadlocked + rolled back; the retry on a fresh conn won.
    assert calls["n"] == 2
    assert result.summary == "ok:0000000009"
    assert result.crashed is False


def test_make_filer_runner_exhausts_deadlock_retries_returns_crashed(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from psycopg import errors as pg_errors

    monkeypatch.setattr(sfc, "connect_job", _FakeConn)
    calls = {"n": 0}

    def work(conn, cik):  # type: ignore[no-untyped-def]
        calls["n"] += 1
        raise pg_errors.DeadlockDetected("deadlock detected")

    runner = make_filer_runner(work, statement_timeout_ms=None, max_deadlock_retries=2)
    result = runner("0000000010")

    # 1 initial + 2 retries = 3 attempts, then isolated as a crashed result.
    assert calls["n"] == 3
    assert result.crashed is True
    assert "DeadlockDetected" in (result.error or "")
