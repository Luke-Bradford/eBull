"""#1233 / #1296 — bootstrap auto-resume on jobs-process restart.

Verifies :func:`app.services.bootstrap_state.attempt_boot_resume`:

- ``no_in_flight_run`` when bootstrap_state.status != 'running'.
- ``no_in_flight_run`` when bootstrap_state.last_run_id IS NULL.
- ``terminated_max_attempts`` when the run has been auto-resumed
  ``_MAX_BOOT_RESUMES`` times already (i.e. the prior boot's resume
  also crashed). Falls through to the existing reaper at the call
  site.
- ``terminated_max_attempts`` when the operator clicked Cancel
  before the crash — honour the cancel intent rather than
  auto-resuming.
- ``resumed`` on first attempt: counter bumped + a
  ``manual_job`` queue row enqueued for ``bootstrap_orchestrator``.

Migration sql/170 is verified implicitly: the per-worker template DB
applies it, so a broken migration would prevent ``ebull_test_conn``
from connecting.
"""

from __future__ import annotations

import psycopg
import pytest

from app.services.bootstrap_state import (
    BootResumeDecision,
    StageSpec,
    attempt_boot_resume,
    start_run,
)
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable"),
]


def _reset_state(conn: psycopg.Connection[tuple]) -> None:
    conn.execute(
        """
        UPDATE bootstrap_state
           SET status='pending', last_run_id=NULL, last_completed_at=NULL
         WHERE id=1
        """
    )
    conn.commit()


def _seed_running_run(conn: psycopg.Connection[tuple]) -> int:
    """Create a bootstrap_runs row in ``running`` and flip
    bootstrap_state. Returns the run_id."""
    specs = (
        StageSpec(
            stage_key="universe_sync",
            stage_order=1,
            lane="init",
            job_name="universe_sync",
        ),
    )
    run_id = start_run(conn, operator_id=None, stage_specs=specs)
    conn.commit()
    return run_id


def _read_counter(conn: psycopg.Connection[tuple], run_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT boot_resume_attempts FROM bootstrap_runs WHERE id = %s",
            (run_id,),
        )
        row = cur.fetchone()
    assert row is not None
    return int(row[0])


def _count_queue_rows(conn: psycopg.Connection[tuple], job_name: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM pending_job_requests
             WHERE job_name = %s
               AND request_kind = 'manual_job'
            """,
            (job_name,),
        )
        row = cur.fetchone()
    assert row is not None
    return int(row[0])


def test_returns_no_in_flight_when_state_not_running(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _reset_state(ebull_test_conn)
    decision = attempt_boot_resume(ebull_test_conn, requested_by="test")
    assert decision == BootResumeDecision(decision="no_in_flight_run", run_id=None, attempts=0)


def test_returns_resumed_on_first_attempt(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _reset_state(ebull_test_conn)
    run_id = _seed_running_run(ebull_test_conn)
    queue_before = _count_queue_rows(ebull_test_conn, "bootstrap_orchestrator")

    decision = attempt_boot_resume(ebull_test_conn, requested_by="test-boot")
    ebull_test_conn.commit()

    assert decision.decision == "resumed"
    assert decision.run_id == run_id
    assert decision.attempts == 1
    assert _read_counter(ebull_test_conn, run_id) == 1
    assert _count_queue_rows(ebull_test_conn, "bootstrap_orchestrator") == queue_before + 1


def test_second_attempt_returns_terminated_max_attempts(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Cap is 1 by default — the prior boot already auto-resumed.
    A second crash means resume is broken; fall through to the
    terminate reaper.
    """
    _reset_state(ebull_test_conn)
    run_id = _seed_running_run(ebull_test_conn)

    first = attempt_boot_resume(ebull_test_conn, requested_by="boot-1")
    ebull_test_conn.commit()
    assert first.decision == "resumed"
    assert _read_counter(ebull_test_conn, run_id) == 1

    queue_before = _count_queue_rows(ebull_test_conn, "bootstrap_orchestrator")
    second = attempt_boot_resume(ebull_test_conn, requested_by="boot-2")
    ebull_test_conn.commit()

    assert second.decision == "terminated_max_attempts"
    assert second.run_id == run_id
    assert second.attempts == 1
    # Counter NOT incremented past the cap; no extra queue row.
    assert _read_counter(ebull_test_conn, run_id) == 1
    assert _count_queue_rows(ebull_test_conn, "bootstrap_orchestrator") == queue_before


def test_operator_cancel_returns_terminated(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """When the operator clicked Cancel before the crash, the
    resume must honour the cancel intent — not auto-restart a
    cancelled run.
    """
    _reset_state(ebull_test_conn)
    run_id = _seed_running_run(ebull_test_conn)
    ebull_test_conn.execute(
        "UPDATE bootstrap_runs SET cancel_requested_at = NOW() WHERE id = %s",
        (run_id,),
    )
    ebull_test_conn.commit()

    decision = attempt_boot_resume(ebull_test_conn, requested_by="test")
    ebull_test_conn.commit()

    assert decision.decision == "terminated_max_attempts"
    assert decision.run_id == run_id
    # Counter MUST NOT advance on cancel-driven termination — if the
    # operator later un-cancels (which they can't today, but the
    # invariant is "we did not auto-resume") the cap budget is
    # preserved.
    assert _read_counter(ebull_test_conn, run_id) == 0


def test_terminated_when_run_status_is_not_running(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Codex 2 MEDIUM on #1296: a stale state singleton can point
    at a finalised ``bootstrap_runs`` row (e.g. mid-finalize crash
    where bootstrap_state was updated before bootstrap_runs in some
    code path, or operator SQL hand-edit). The resume MUST NOT
    flip a terminal run back to running — it must terminate.
    """
    _reset_state(ebull_test_conn)
    run_id = _seed_running_run(ebull_test_conn)
    # Stale-singleton scenario: run finalised but state still says running.
    ebull_test_conn.execute(
        "UPDATE bootstrap_runs SET status = 'partial_error' WHERE id = %s",
        (run_id,),
    )
    ebull_test_conn.commit()

    decision = attempt_boot_resume(ebull_test_conn, requested_by="test")
    ebull_test_conn.commit()

    assert decision.decision == "terminated_max_attempts"
    assert decision.run_id == run_id
    # No counter advance — terminal-run guard fires before the bump.
    assert _read_counter(ebull_test_conn, run_id) == 0
    # No queue row enqueued for a terminal run.
    assert _count_queue_rows(ebull_test_conn, "bootstrap_orchestrator") == 0


def test_stage_success_resets_resume_counter(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Codex 2 BLOCKING on #1296: the cap is "consecutive crashes
    without a successful stage", not "total resumes for the run's
    lifetime". ``mark_stage_success`` must zero the counter so a
    healthy resume regains its full resume budget for any subsequent
    crash. Without this reset, an operator restart later in the same
    run would hit the cap and terminate a healthy bootstrap.
    """
    from app.services.bootstrap_state import mark_stage_running, mark_stage_success

    _reset_state(ebull_test_conn)
    run_id = _seed_running_run(ebull_test_conn)

    # First crash → resume → counter=1.
    first = attempt_boot_resume(ebull_test_conn, requested_by="boot-1")
    ebull_test_conn.commit()
    assert first.decision == "resumed"
    assert _read_counter(ebull_test_conn, run_id) == 1

    # Resume succeeds: orchestrator transitions a stage to success.
    mark_stage_running(ebull_test_conn, run_id=run_id, stage_key="universe_sync")
    mark_stage_success(ebull_test_conn, run_id=run_id, stage_key="universe_sync", rows_processed=42)
    ebull_test_conn.commit()

    # Counter must be 0 — the resume worked.
    assert _read_counter(ebull_test_conn, run_id) == 0, (
        "boot_resume_attempts not reset after stage success — the resume "
        "budget will be permanently exhausted by the first restart even "
        "though the run is healthy"
    )


def test_higher_max_attempts_allows_more_resumes(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The cap is a parameter — production passes the default
    (1) but tests + future operator overrides can widen it. Verify
    the counter advances cleanly up to ``max_attempts``.
    """
    _reset_state(ebull_test_conn)
    _seed_running_run(ebull_test_conn)

    for n in range(1, 4):
        decision = attempt_boot_resume(ebull_test_conn, requested_by=f"boot-{n}", max_attempts=3)
        ebull_test_conn.commit()
        assert decision.decision == "resumed"
        assert decision.attempts == n

    # 4th attempt past cap.
    decision = attempt_boot_resume(ebull_test_conn, requested_by="boot-4", max_attempts=3)
    ebull_test_conn.commit()
    assert decision.decision == "terminated_max_attempts"
    assert decision.attempts == 3
