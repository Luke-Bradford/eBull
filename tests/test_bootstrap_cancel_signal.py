"""Tests for the bootstrap cancel-signal helper.

Issue #1064 PR3d. The helper exposes ``bootstrap_cancel_requested()``
to long-running stage invokers (the SEC drain, the 13F sweep) so they
can poll periodically and raise ``BootstrapStageCancelled`` to bail
out cooperatively. Without this plumbing, cancel observation falls
back to the orchestrator's between-stage checkpoint at the next
boundary — which can be 20+ minutes for a SEC drain.
"""

from __future__ import annotations

import psycopg

from app.services.bootstrap_state import (
    StageSpec,
    cancel_run,
    start_run,
)
from app.services.processes.bootstrap_cancel_signal import (
    active_bootstrap_run,
    active_bootstrap_stage_key,
    bootstrap_cancel_requested,
)

# Use real registered job names so JobLock's source_for() resolves.
# ``daily_cik_refresh`` is in SCHEDULED_JOBS (sec_rate source);
# ``daily_financial_facts`` is also sec_rate.
_SPECS = (
    StageSpec(stage_key="alpha", stage_order=1, lane="sec_rate", job_name="daily_cik_refresh"),
    StageSpec(stage_key="bravo", stage_order=2, lane="sec_rate", job_name="daily_financial_facts"),
)


def _reset_state(conn: psycopg.Connection[tuple]) -> None:
    conn.execute(
        """
        UPDATE bootstrap_state
           SET status            = 'pending',
               last_run_id       = NULL,
               last_completed_at = NULL
         WHERE id = 1
        """
    )
    conn.commit()


def test_returns_false_when_contextvar_unset() -> None:
    """Outside ``active_bootstrap_run`` the helper short-circuits to
    False without touching the DB. Scheduled / manual triggers of the
    same job are unaffected by the bootstrap-cancel surface.
    """
    assert bootstrap_cancel_requested() is False


def test_active_bootstrap_stage_key_returns_none_when_unset() -> None:
    """#1114: outside ``active_bootstrap_run`` the reader returns None."""
    assert active_bootstrap_stage_key() is None


def test_active_bootstrap_stage_key_returns_stage_key_when_set() -> None:
    """#1114: under ``active_bootstrap_run(run_id, stage_key)`` the
    reader returns the stage_key so adopters can label their
    ``BootstrapStageCancelled`` exceptions without hardcoding."""
    with active_bootstrap_run(99, "filings_history_seed"):
        assert active_bootstrap_stage_key() == "filings_history_seed"
    # Reset on exit.
    assert active_bootstrap_stage_key() is None


def test_returns_false_when_no_cancel_pending(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """ContextVar set, no cancel pending → False."""
    _reset_state(ebull_test_conn)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    ebull_test_conn.commit()

    with active_bootstrap_run(run_id, "alpha"):
        assert bootstrap_cancel_requested(conn=ebull_test_conn) is False


def test_returns_true_after_cancel_run(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """ContextVar set, cancel_run wrote the stop row → True."""
    _reset_state(ebull_test_conn)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    ebull_test_conn.commit()

    cancel_run(ebull_test_conn, requested_by_operator_id=None)
    ebull_test_conn.commit()

    with active_bootstrap_run(run_id, "alpha"):
        assert bootstrap_cancel_requested(conn=ebull_test_conn) is True


def test_contextvar_resets_on_exit(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """``active_bootstrap_run`` must clear the contextvar on exit so
    a subsequent call (different stage, different run, or the
    scheduled fire of an unrelated job) doesn't see stale state.
    """
    _reset_state(ebull_test_conn)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    cancel_run(ebull_test_conn, requested_by_operator_id=None)
    ebull_test_conn.commit()

    with active_bootstrap_run(run_id, "alpha"):
        assert bootstrap_cancel_requested(conn=ebull_test_conn) is True
    # Outside the with-block: contextvar reset, helper returns False.
    assert bootstrap_cancel_requested(conn=ebull_test_conn) is False


def test_run_one_stage_maps_cancelled_invoker_to_cancelled_outcome(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """PR3d #1064 — when a stage invoker raises BootstrapStageCancelled,
    ``_run_one_stage`` catches it, marks the stage as ``cancelled``
    (not ``error``), and returns ``_StageOutcome(cancelled=True)``.

    Pins both the exception priority (BootstrapStageCancelled before
    generic Exception) and the schema-migration plumbing — without
    PR3c's sql/142 the UPDATE would CheckViolation.
    """
    from app.services.bootstrap_orchestrator import _run_one_stage
    from app.services.bootstrap_state import (
        BootstrapStageCancelled,
        mark_stage_running,
    )
    from tests.fixtures.ebull_test_db import test_database_url

    _reset_state(ebull_test_conn)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    mark_stage_running(ebull_test_conn, run_id=run_id, stage_key="alpha")
    ebull_test_conn.commit()

    def _cancelling_invoker(_params: object) -> None:
        raise BootstrapStageCancelled("operator cancelled mid-stage", stage_key="alpha")

    outcome = _run_one_stage(
        run_id=run_id,
        stage_key="alpha",
        job_name="daily_cik_refresh",
        invoker=_cancelling_invoker,
        database_url=test_database_url(),
    )
    assert outcome.cancelled is True
    assert outcome.success is False
    assert outcome.skipped is False
    # Stage row carries the new status + reason.
    row = ebull_test_conn.execute(
        "SELECT status, last_error FROM bootstrap_stages WHERE bootstrap_run_id = %s AND stage_key = %s",
        (run_id, "alpha"),
    ).fetchone()
    assert row is not None
    assert row[0] == "cancelled"
    assert row[1] is not None
    assert "operator" in row[1]


def test_run_one_stage_generic_exception_still_maps_to_error(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Regression guard: a non-cancel exception still terminates the
    stage as ``error`` so the ``BootstrapStageCancelled`` catch block
    doesn't swallow real failures.
    """
    from app.services.bootstrap_orchestrator import _run_one_stage
    from app.services.bootstrap_state import mark_stage_running
    from tests.fixtures.ebull_test_db import test_database_url

    _reset_state(ebull_test_conn)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    mark_stage_running(ebull_test_conn, run_id=run_id, stage_key="alpha")
    ebull_test_conn.commit()

    def _broken_invoker(_params: object) -> None:
        raise RuntimeError("network kaput")

    outcome = _run_one_stage(
        run_id=run_id,
        stage_key="alpha",
        job_name="daily_cik_refresh",
        invoker=_broken_invoker,
        database_url=test_database_url(),
    )
    assert outcome.cancelled is False
    assert outcome.success is False
    assert outcome.error is not None and "network kaput" in outcome.error
    row = ebull_test_conn.execute(
        "SELECT status FROM bootstrap_stages WHERE bootstrap_run_id = %s AND stage_key = %s",
        (run_id, "alpha"),
    ).fetchone()
    assert row is not None and row[0] == "error"
