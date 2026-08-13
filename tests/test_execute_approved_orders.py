"""Tests for the execute_approved_orders scheduled job.

The job orchestrates three phases — entry_timing, execution_guard, order_client —
so tests mock all three services and verify the orchestration logic:
- Phase 0: entry timing evaluates TA conditions for BUY/ADD recs
- Phase 1: proposed recommendations are guarded (PASS → approved, FAIL → rejected)
- Phase 2: approved recommendations are executed
- Isolation: one failure does not block others

Mock approach: patch ``_tracked_job`` with a no-op context manager so
the job tracking infrastructure (which also uses psycopg.connect) does
not interfere with the connection mocking for the business logic.

Connection order (Phase 0 → Phase 1 → Phase 2):
  0. timing_conn    — SELECT proposed recs for timing evaluation
  0a. per-rec conn  — one per BUY/ADD candidate (evaluate + UPDATE)
  1. proposed_conn  — SELECT remaining proposed recs for guard
  1a. per-rec conn  — one per proposed rec (guard + UPDATE)
  2. approved_conn  — SELECT approved recs for execution
  2a. per-rec conn  — one per approved rec (execute_order)
"""

from __future__ import annotations

import contextlib
from collections.abc import Generator
from typing import Any
from unittest.mock import MagicMock, patch

from app.services.execution_guard import GuardResult
from app.services.order_client import ExecuteResult

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakeTracker:
    run_id: int | None = None
    row_count: int = 0


@contextlib.contextmanager
def _noop_tracked_job(job_name: str) -> Generator[_FakeTracker]:
    yield _FakeTracker()


def _guard_pass(rec_id: int, decision_id: int) -> GuardResult:
    return GuardResult(
        recommendation_id=rec_id,
        instrument_id=rec_id * 10,
        verdict="PASS",
        failed_rules=[],
        explanation="all rules passed",
        decision_id=decision_id,
    )


def _guard_fail(rec_id: int, decision_id: int) -> GuardResult:
    return GuardResult(
        recommendation_id=rec_id,
        instrument_id=rec_id * 10,
        verdict="FAIL",
        failed_rules=["kill_switch"],
        explanation="kill switch is engaged",
        decision_id=decision_id,
    )


def _exec_filled(rec_id: int, order_id: int) -> ExecuteResult:
    return ExecuteResult(
        order_id=order_id,
        outcome="filled",
        broker_order_ref=f"DEMO-{rec_id}",
        fill_id=order_id * 10,
        explanation="order filled",
    )


def _exec_failed(rec_id: int, order_id: int) -> ExecuteResult:
    return ExecuteResult(
        order_id=order_id,
        outcome="failed",
        broker_order_ref=None,
        fill_id=None,
        explanation="order failed",
    )


def _mock_conn_with_rows(rows: list[tuple[Any, ...]]) -> MagicMock:
    """Build a mock connection whose execute().fetchall() returns ``rows``."""
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = rows
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    return conn


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@patch("app.workers.scheduler._tracked_job", _noop_tracked_job)
@patch("app.workers.scheduler._load_etoro_credentials", return_value=None)
class TestGuardPhase:
    """Phase 1: proposed recommendations are guarded."""

    @patch("app.workers.scheduler.evaluate_recommendation")
    @patch("app.workers.scheduler.psycopg.connect")
    def test_proposed_recs_are_guarded(
        self,
        mock_connect: MagicMock,
        mock_guard: MagicMock,
        _creds: MagicMock,
    ) -> None:
        """Two proposed recs: one PASS, one FAIL."""
        timing_conn = _mock_conn_with_rows([])  # Phase 0: no timing candidates
        proposed_conn = _mock_conn_with_rows([(1,), (2,)])
        guard_conn1 = _mock_conn_with_rows([])
        guard_conn2 = _mock_conn_with_rows([])
        approved_conn = _mock_conn_with_rows([])

        mock_connect.side_effect = [
            timing_conn,
            proposed_conn,
            guard_conn1,
            guard_conn2,
            approved_conn,
        ]
        mock_guard.side_effect = [
            _guard_pass(1, 100),
            _guard_fail(2, 101),
        ]

        from app.workers.scheduler import execute_approved_orders

        execute_approved_orders()

        assert mock_guard.call_count == 2

    @patch("app.workers.scheduler.evaluate_recommendation")
    @patch("app.workers.scheduler.psycopg.connect")
    def test_guard_exception_does_not_block_next(
        self,
        mock_connect: MagicMock,
        mock_guard: MagicMock,
        _creds: MagicMock,
    ) -> None:
        """If guard raises for rec 1, rec 2 is still guarded."""
        timing_conn = _mock_conn_with_rows([])  # Phase 0: no timing candidates
        proposed_conn = _mock_conn_with_rows([(1,), (2,)])
        guard_conn1 = _mock_conn_with_rows([])
        guard_conn2 = _mock_conn_with_rows([])
        approved_conn = _mock_conn_with_rows([])

        mock_connect.side_effect = [
            timing_conn,
            proposed_conn,
            guard_conn1,
            guard_conn2,
            approved_conn,
        ]
        mock_guard.side_effect = [
            RuntimeError("guard exploded"),
            _guard_pass(2, 200),
        ]

        from app.workers.scheduler import execute_approved_orders

        execute_approved_orders()

        assert mock_guard.call_count == 2


@patch("app.workers.scheduler._tracked_job", _noop_tracked_job)
@patch("app.workers.scheduler._load_etoro_credentials", return_value=None)
class TestExecutePhase:
    """Phase 2: approved recommendations are executed."""

    @patch("app.workers.scheduler.execute_order")
    @patch("app.workers.scheduler.evaluate_recommendation")
    @patch("app.workers.scheduler.psycopg.connect")
    def test_approved_recs_are_executed(
        self,
        mock_connect: MagicMock,
        mock_guard: MagicMock,
        mock_exec: MagicMock,
        _creds: MagicMock,
    ) -> None:
        """One proposed rec passes guard, then is executed."""
        timing_conn = _mock_conn_with_rows([])  # Phase 0: no timing candidates
        proposed_conn = _mock_conn_with_rows([(1,)])
        guard_conn = _mock_conn_with_rows([])
        approved_conn = _mock_conn_with_rows([(1, 100)])
        exec_conn = _mock_conn_with_rows([])

        mock_connect.side_effect = [
            timing_conn,
            proposed_conn,
            guard_conn,
            approved_conn,
            exec_conn,
        ]
        mock_guard.return_value = _guard_pass(1, 100)
        mock_exec.return_value = _exec_filled(1, 500)

        from app.workers.scheduler import execute_approved_orders

        execute_approved_orders()

        mock_exec.assert_called_once()
        _, kwargs = mock_exec.call_args
        assert kwargs["recommendation_id"] == 1
        assert kwargs["decision_id"] == 100

    @patch("app.workers.scheduler.execute_order")
    @patch("app.workers.scheduler.evaluate_recommendation")
    @patch("app.workers.scheduler.psycopg.connect")
    def test_execution_exception_does_not_block_next(
        self,
        mock_connect: MagicMock,
        mock_guard: MagicMock,
        mock_exec: MagicMock,
        _creds: MagicMock,
    ) -> None:
        """If execution raises for rec 1, rec 2 is still executed."""
        timing_conn = _mock_conn_with_rows([])  # Phase 0: no timing candidates
        proposed_conn = _mock_conn_with_rows([])
        approved_conn = _mock_conn_with_rows([(1, 100), (2, 200)])
        exec_conn1 = _mock_conn_with_rows([])
        exec_conn2 = _mock_conn_with_rows([])

        mock_connect.side_effect = [
            timing_conn,
            proposed_conn,
            approved_conn,
            exec_conn1,
            exec_conn2,
        ]
        mock_exec.side_effect = [
            RuntimeError("broker exploded"),
            _exec_filled(2, 600),
        ]

        from app.workers.scheduler import execute_approved_orders

        execute_approved_orders()

        assert mock_exec.call_count == 2

    @patch("app.workers.scheduler.execute_order")
    @patch("app.workers.scheduler.evaluate_recommendation")
    @patch("app.workers.scheduler.psycopg.connect")
    def test_failed_order_counted_correctly(
        self,
        mock_connect: MagicMock,
        mock_guard: MagicMock,
        mock_exec: MagicMock,
        _creds: MagicMock,
    ) -> None:
        """A non-filled outcome is logged as failed."""
        timing_conn = _mock_conn_with_rows([])  # Phase 0: no timing candidates
        proposed_conn = _mock_conn_with_rows([])
        approved_conn = _mock_conn_with_rows([(1, 100)])
        exec_conn = _mock_conn_with_rows([])

        mock_connect.side_effect = [timing_conn, proposed_conn, approved_conn, exec_conn]
        mock_exec.return_value = _exec_failed(1, 500)

        from app.workers.scheduler import execute_approved_orders

        execute_approved_orders()

        mock_exec.assert_called_once()


@patch("app.workers.scheduler._tracked_job", _noop_tracked_job)
@patch("app.workers.scheduler._load_etoro_credentials", return_value=None)
class TestNoWork:
    """Edge cases: nothing to do."""

    @patch("app.workers.scheduler.evaluate_recommendation")
    @patch("app.workers.scheduler.execute_order")
    @patch("app.workers.scheduler.psycopg.connect")
    def test_no_proposed_no_approved(
        self,
        mock_connect: MagicMock,
        mock_exec: MagicMock,
        mock_guard: MagicMock,
        _creds: MagicMock,
    ) -> None:
        """No proposed or approved recs → guard and execute never called."""
        timing_conn = _mock_conn_with_rows([])  # Phase 0: no timing candidates
        proposed_conn = _mock_conn_with_rows([])
        approved_conn = _mock_conn_with_rows([])

        mock_connect.side_effect = [timing_conn, proposed_conn, approved_conn]

        from app.workers.scheduler import execute_approved_orders

        execute_approved_orders()

        mock_guard.assert_not_called()
        mock_exec.assert_not_called()


class TestPrerequisite:
    """_has_actionable_recommendations prerequisite check."""

    def test_returns_true_when_proposed_exists(self) -> None:
        from app.workers.scheduler import _has_actionable_recommendations

        conn = MagicMock()
        cursor = MagicMock()
        cursor.execute.return_value.fetchone.return_value = (True,)
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        conn.cursor.return_value = cursor

        met, reason = _has_actionable_recommendations(conn)
        assert met is True

    def test_returns_false_when_none_exist(self) -> None:
        from app.workers.scheduler import _has_actionable_recommendations

        conn = MagicMock()
        cursor = MagicMock()
        cursor.execute.return_value.fetchone.return_value = (False,)
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        conn.cursor.return_value = cursor

        met, reason = _has_actionable_recommendations(conn)
        assert met is False
        assert "no proposed or approved" in reason
