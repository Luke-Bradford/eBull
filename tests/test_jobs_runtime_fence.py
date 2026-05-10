"""Tests for the lock+fence prelude wired into ``_tracked_job`` (#1071, PR3).

Spec: ``docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md``
      §Full-wash execution fence + §Scheduled run interaction.

The prelude lives at ``app.jobs.runtime``; ``_tracked_job`` (in
``app.workers.scheduler``) consumes the pre-allocated run_id. Tests
exercise the prelude directly so we cover the fence behaviour without
needing an APScheduler boot.
"""

from __future__ import annotations

import psycopg
import pytest

from app.jobs import runtime as jobs_runtime
from app.services.job_telemetry import JobTelemetryAggregator
from tests.fixtures.ebull_test_db import test_database_url


def _ensure_kill_switch_off(conn: psycopg.Connection[tuple]) -> None:
    """Some prelude smoke shapes hit kill_switch indirectly; keep it off."""
    conn.execute(
        """
        INSERT INTO kill_switch (id, is_active, activated_at, activated_by, reason)
        VALUES (TRUE, FALSE, NULL, NULL, NULL)
        ON CONFLICT (id) DO UPDATE
        SET is_active = FALSE, activated_at = NULL, activated_by = NULL, reason = NULL
        """
    )


def _read_latest_job_run(conn: psycopg.Connection[tuple], *, job_name: str) -> tuple[int, str, str | None]:
    row = conn.execute(
        """
        SELECT run_id, status, error_msg
          FROM job_runs
         WHERE job_name = %s
         ORDER BY started_at DESC
         LIMIT 1
        """,
        (job_name,),
    ).fetchone()
    assert row is not None
    return int(row[0]), str(row[1]), row[2]


def _job_runs_count(conn: psycopg.Connection[tuple], *, job_name: str) -> int:
    row = conn.execute("SELECT COUNT(*) FROM job_runs WHERE job_name = %s", (job_name,)).fetchone()
    assert row is not None
    return int(row[0])


def test_prelude_no_fence_writes_running_row_and_runs_invoker(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """No fence row → prelude writes ``status='running'`` + runs invoker."""
    _ensure_kill_switch_off(ebull_test_conn)
    ebull_test_conn.commit()

    captured_run_id: list[int | None] = []

    def _invoker(_p=None) -> None:
        captured_run_id.append(jobs_runtime.consume_prelude_run_id())

    invoked_signal = jobs_runtime.run_with_prelude(test_database_url(), "fence_test_no_fence", _invoker)

    # Reload from a fresh connection-side snapshot.
    ebull_test_conn.rollback()  # release any open implicit tx
    run_id, status, error_msg = _read_latest_job_run(ebull_test_conn, job_name="fence_test_no_fence")
    assert status == "running"
    assert error_msg is None
    # The invoker captured the same run_id the prelude wrote.
    assert captured_run_id == [run_id]
    assert invoked_signal is True


def test_prelude_fence_held_writes_skipped_and_does_not_run_invoker(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Active full_wash fence row → prelude writes 'skipped' (R5-W1) +
    invoker NOT called."""
    _ensure_kill_switch_off(ebull_test_conn)
    ebull_test_conn.execute(
        """
        INSERT INTO pending_job_requests
            (request_kind, job_name, process_id, mode, status)
        VALUES ('manual_job', 'fence_test_held', 'fence_test_held',
                'full_wash', 'pending')
        """
    )
    ebull_test_conn.commit()

    invoked = []

    def _invoker(_p=None) -> None:
        invoked.append(True)

    invoked_signal = jobs_runtime.run_with_prelude(test_database_url(), "fence_test_held", _invoker)

    ebull_test_conn.rollback()
    _, status, error_msg = _read_latest_job_run(ebull_test_conn, job_name="fence_test_held")
    assert status == "skipped"
    assert error_msg == "full-wash in progress for this process"
    # Invoker was skipped — caller observes a clean no-op return.
    assert invoked == []
    # Return value gates the queue-row transition in _run_manual
    # (PR #1072 review BLOCKING fix).
    assert invoked_signal is False


def test_prelude_fence_held_by_sibling_sharing_freshness_source(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """PR4 (#1075 / #1073 Codex round 5/6): the prelude fence check
    must walk sibling jobs sharing the same scheduler source.

    A full-wash fence on ``daily_financial_facts`` (process_id) must
    cause an APScheduler fire of ``fundamentals_sync`` (sibling on
    ``freshness_source='sec_xbrl_facts'``) to self-skip — otherwise
    the sibling worker reads the just-reset ``data_freshness_index``
    rows under the holder's feet.
    """
    _ensure_kill_switch_off(ebull_test_conn)
    ebull_test_conn.execute(
        """
        INSERT INTO pending_job_requests
            (request_kind, job_name, process_id, mode, status)
        VALUES ('manual_job', 'daily_financial_facts',
                'daily_financial_facts', 'full_wash', 'dispatched')
        """
    )
    ebull_test_conn.commit()

    invoked: list[bool] = []

    def _invoker(_p=None) -> None:
        invoked.append(True)

    invoked_signal = jobs_runtime.run_with_prelude(
        test_database_url(),
        "fundamentals_sync",
        _invoker,
    )

    ebull_test_conn.rollback()
    _, status, error_msg = _read_latest_job_run(ebull_test_conn, job_name="fundamentals_sync")
    assert status == "skipped"
    assert error_msg is not None
    assert "shared scheduler source" in error_msg
    assert "daily_financial_facts" in error_msg
    assert invoked == []
    assert invoked_signal is False


def test_prelude_bypass_fence_runs_invoker_even_with_fence_row(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """``bypass_fence_check=True`` (the full-wash holder path) ignores
    the fence row and runs the invoker — otherwise the worker would
    self-skip on its own queue row."""
    _ensure_kill_switch_off(ebull_test_conn)
    ebull_test_conn.execute(
        """
        INSERT INTO pending_job_requests
            (request_kind, job_name, process_id, mode, status)
        VALUES ('manual_job', 'fence_test_bypass', 'fence_test_bypass',
                'full_wash', 'dispatched')
        """
    )
    ebull_test_conn.commit()

    invoked = []

    def _invoker(_p=None) -> None:
        invoked.append(True)

    jobs_runtime.run_with_prelude(
        test_database_url(),
        "fence_test_bypass",
        _invoker,
        bypass_fence_check=True,
    )

    ebull_test_conn.rollback()
    _, status, error_msg = _read_latest_job_run(ebull_test_conn, job_name="fence_test_bypass")
    assert status == "running"
    assert error_msg is None
    assert invoked == [True]


def test_consume_prelude_run_id_clears_after_first_call() -> None:
    """The ContextVar is single-use per invocation — a nested
    ``_tracked_job`` would otherwise reuse the parent's run_id."""
    token = jobs_runtime._prelude_run_id.set(99)
    try:
        first = jobs_runtime.consume_prelude_run_id()
        second = jobs_runtime.consume_prelude_run_id()
    finally:
        jobs_runtime._prelude_run_id.reset(token)
    assert first == 99
    assert second is None


def test_consume_prelude_run_id_default_is_none() -> None:
    """Outside a prelude wrapper, the ContextVar is None — the legacy
    direct-call path of ``_tracked_job`` falls back to record_job_start."""
    assert jobs_runtime.consume_prelude_run_id() is None


def test_prelude_propagates_linked_request_id_to_job_runs(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Codex round 7 fix: queue-dispatched manual_job runs must populate
    ``job_runs.linked_request_id`` so boot-recovery's
    ``reset_stale_in_flight`` NOT EXISTS clause suppresses double-replay
    of completed runs."""
    _ensure_kill_switch_off(ebull_test_conn)
    # Synthetic queue request — the prelude doesn't read it, just stores
    # the request_id on the job_runs row.
    row = ebull_test_conn.execute(
        """
        INSERT INTO pending_job_requests
            (request_kind, job_name, requested_by, status)
        VALUES ('manual_job', 'fence_test_linked', 'test', 'claimed')
        RETURNING request_id
        """
    ).fetchone()
    assert row is not None
    request_id = int(row[0])
    ebull_test_conn.commit()

    invoked = []

    def _invoker(_p=None) -> None:
        invoked.append(True)

    jobs_runtime.run_with_prelude(
        test_database_url(),
        "fence_test_linked",
        _invoker,
        linked_request_id=request_id,
    )
    assert invoked == [True]

    ebull_test_conn.rollback()
    persisted = ebull_test_conn.execute(
        """
        SELECT linked_request_id, status FROM job_runs
        WHERE job_name = 'fence_test_linked'
        ORDER BY started_at DESC
        LIMIT 1
        """
    ).fetchone()
    assert persisted is not None
    assert persisted[0] == request_id
    assert persisted[1] == "running"


def test_prelude_failure_propagates_does_not_run_invoker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex round 7 fix: if the prelude itself raises (DB unreachable,
    fence query errors), we must NOT silently fall through to running
    the invoker — that would let scheduled / iterate work race past an
    active full-wash."""
    invoked = []

    def _invoker(_p=None) -> None:
        invoked.append(True)

    def _explode(*_args: object, **_kw: object) -> int | None:
        raise RuntimeError("simulated prelude failure")

    monkeypatch.setattr("app.jobs.runtime._run_prelude", _explode)

    with pytest.raises(RuntimeError, match="simulated prelude failure"):
        jobs_runtime.run_with_prelude("postgresql://stub/stub", "fence_test_explode", _invoker)
    assert invoked == []


def test_invoker_can_use_telemetry_aggregator_against_pre_allocated_run(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """End-to-end: prelude writes the run row; the invoker pulls the id
    via consume_prelude_run_id and flushes telemetry to that row."""
    _ensure_kill_switch_off(ebull_test_conn)
    ebull_test_conn.commit()

    def _invoker(_p=None) -> None:
        run_id = jobs_runtime.consume_prelude_run_id()
        assert run_id is not None
        agg = JobTelemetryAggregator()
        agg.set_target(2)
        agg.record_processed(count=2)
        # Open our own conn so the test fixture's conn is not perturbed.
        with psycopg.connect(test_database_url()) as conn:
            from app.services.job_telemetry import flush_to_job_run

            flush_to_job_run(conn, run_id=run_id, agg=agg)
            conn.commit()

    jobs_runtime.run_with_prelude(test_database_url(), "fence_test_telemetry", _invoker)

    ebull_test_conn.rollback()
    run_id, status, _ = _read_latest_job_run(ebull_test_conn, job_name="fence_test_telemetry")
    assert status == "running"
    row = ebull_test_conn.execute(
        "SELECT processed_count, target_count FROM job_runs WHERE run_id = %s",
        (run_id,),
    ).fetchone()
    assert row is not None
    assert row[0] == 2
    assert row[1] == 2
