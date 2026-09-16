"""Pure-logic tests for the manifest tick's telemetry flush (#3111 slice 2).

No DB: a fake connection is enough, because what these pin is the ISOLATION
contract — the flush reports on the work and must never become a gate on it.
"""

from __future__ import annotations

from typing import Any

import pytest

from app.services.job_telemetry import JobTelemetryAggregator
from app.workers.scheduler import _flush_manifest_worker_telemetry


class _FakeConn:
    def __init__(self, *, fail_on: str | None = None) -> None:
        self.calls: list[str] = []
        self.fail_on = fail_on

    def _maybe_fail(self, what: str) -> None:
        self.calls.append(what)
        if self.fail_on == what:
            raise RuntimeError(f"{what} exploded")

    def rollback(self) -> None:
        self._maybe_fail("rollback")

    def commit(self) -> None:
        self._maybe_fail("commit")

    def execute(self, *_a: Any, **_k: Any) -> Any:
        self._maybe_fail("execute")
        return None

    def cursor(self) -> Any:
        self._maybe_fail("cursor")
        return _FakeCursor()


class _FakeCursor:
    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *_a: Any) -> None:
        return None

    def execute(self, *_a: Any, **_k: Any) -> None:
        return None


def _agg() -> JobTelemetryAggregator:
    agg = JobTelemetryAggregator()
    agg.record_error(error_class="DispatchFailed:RuntimeError", message="boom", subject="acc")
    return agg


def test_a_run_with_no_job_runs_row_is_skipped_not_written() -> None:
    """``_tracked_job`` deliberately runs the body with ``run_id=0`` when
    start-recording failed. An UPDATE matching nothing is not a report."""
    conn = _FakeConn()

    _flush_manifest_worker_telemetry(conn, run_id=0, agg=_agg())  # type: ignore[arg-type]

    assert conn.calls == []


def test_the_flush_bounds_its_own_statement_timeout() -> None:
    """The worker connection carries the JOB's 30-minute bound. A reporting
    write must never outlast the work it reports on — the tick is already
    committed by the time this runs."""
    conn = _FakeConn()

    _flush_manifest_worker_telemetry(conn, run_id=7, agg=_agg())  # type: ignore[arg-type]

    assert conn.calls == ["rollback", "execute", "cursor", "commit"]


@pytest.mark.parametrize("fail_on", ["rollback", "execute", "cursor", "commit"])
def test_a_telemetry_failure_never_raises_into_the_job(fail_on: str) -> None:
    """⚠⚠ The whole point of the wrapper.

    This runs in a ``finally``: a raise here would REPLACE the real exception on
    the failure path, and on the success path would turn work that is already
    committed into a recorded job failure — which the retry classifier then acts
    on. Every stage of the flush is probed, not just the write.
    """
    conn = _FakeConn(fail_on=fail_on)

    _flush_manifest_worker_telemetry(conn, run_id=7, agg=_agg())  # type: ignore[arg-type]

    assert fail_on in conn.calls
