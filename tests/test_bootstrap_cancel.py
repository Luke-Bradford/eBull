"""Tests for cooperative cancel of the bootstrap orchestrator (#1069).

Spec: docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md
      §Cancel semantics — cooperative + §PR2.

Real-DB tests against the worker ``ebull_test`` database. Mocking the
process_stop_requests partial-unique index would defeat half the
correctness contract under test, so these all go through psycopg
against the truncated test DB.
"""

from __future__ import annotations

from typing import Any

import psycopg
import pytest

from app.services.bootstrap_orchestrator import (
    _phase_batched_dispatch,
    _RunnableStage,
    run_bootstrap_orchestrator,
)
from app.services.bootstrap_state import (
    BootstrapNotRunning,
    StageSpec,
    cancel_run,
    finalize_run,
    force_mark_complete,
    mark_run_cancelled,
    mark_stage_error,
    mark_stage_running,
    mark_stage_success,
    read_latest_run_with_stages,
    read_state,
    reap_orphaned_running,
    reset_failed_stages_for_retry,
    start_run,
)
from app.services.process_stop import (
    StopAlreadyPendingError,
    is_stop_requested,
)


# Singleton reset — same pattern as test_bootstrap_state.py.
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


def _bind_settings_to_test_db(monkeypatch: pytest.MonkeyPatch) -> str:
    from app.config import settings as app_settings
    from tests.fixtures.ebull_test_db import test_database_url

    url = test_database_url()
    monkeypatch.setattr(app_settings, "database_url", url)
    return url


_SPECS = (
    StageSpec(stage_key="alpha", stage_order=1, lane="init", job_name="alpha_job"),
    StageSpec(stage_key="bravo", stage_order=2, lane="sec", job_name="bravo_job"),
    StageSpec(stage_key="charlie", stage_order=3, lane="sec", job_name="charlie_job"),
)


# ---------------------------------------------------------------------------
# cancel_run
# ---------------------------------------------------------------------------


def test_cancel_run_inserts_stop_row_and_marks_run(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _reset_state(ebull_test_conn)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    ebull_test_conn.commit()

    cancelled_run_id = cancel_run(ebull_test_conn, requested_by_operator_id=None)
    ebull_test_conn.commit()

    assert cancelled_run_id == run_id

    # Stop request exists, unobserved, uncompleted.
    stop = is_stop_requested(
        ebull_test_conn,
        target_run_kind="bootstrap_run",
        target_run_id=run_id,
    )
    assert stop is not None
    assert stop.process_id == "bootstrap"
    assert stop.mechanism == "bootstrap"
    assert stop.mode == "cooperative"
    assert stop.observed_at is None
    assert stop.completed_at is None

    # Fast-path observation column populated.
    row = ebull_test_conn.execute(
        "SELECT cancel_requested_at, status FROM bootstrap_runs WHERE id = %s",
        (run_id,),
    ).fetchone()
    assert row is not None
    assert row[0] is not None
    # Run row is still 'running' — the cancel signal is just a flag;
    # the orchestrator transitions to 'cancelled' on observation.
    assert row[1] == "running"


def test_cancel_run_raises_when_not_running(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _reset_state(ebull_test_conn)
    # state='pending'
    with pytest.raises(BootstrapNotRunning):
        cancel_run(ebull_test_conn, requested_by_operator_id=None)


def test_cancel_run_records_terminate_mode_when_requested(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """PR3b #1092 — operator's modal selection (cooperative vs terminate)
    flows through to ``process_stop_requests.mode``.

    Pre-fix the helper hardcoded ``mode='cooperative'``, masking what
    the operator actually asked for. Worker behaviour stays cooperative
    in both cases (genuine terminate requires a jobs-process restart
    per the cancel runbook); only the durable mode signal differs.
    """
    _reset_state(ebull_test_conn)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    ebull_test_conn.commit()

    cancelled_run_id = cancel_run(
        ebull_test_conn,
        requested_by_operator_id=None,
        mode="terminate",
    )
    ebull_test_conn.commit()

    assert cancelled_run_id == run_id
    stop = is_stop_requested(
        ebull_test_conn,
        target_run_kind="bootstrap_run",
        target_run_id=run_id,
    )
    assert stop is not None
    assert stop.mode == "terminate"


def test_processes_bootstrap_cancel_api_persists_terminate_mode(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """PR3b #1092 — POST /system/processes/bootstrap/cancel with
    ``mode='terminate'`` reaches the helper which persists
    ``terminate`` on the durable stop row.

    Codex pre-push round 1 — the helper-level test alone would still
    pass if the API path reverted to a hardcoded cooperative call;
    this test pins the wiring at the request boundary.
    """
    from fastapi.testclient import TestClient

    from app.db import get_conn
    from app.main import app

    _reset_state(ebull_test_conn)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    ebull_test_conn.commit()

    def _yield_conn() -> Any:
        yield ebull_test_conn

    app.dependency_overrides[get_conn] = _yield_conn
    try:
        client = TestClient(app)
        resp = client.post(
            "/system/processes/bootstrap/cancel",
            json={"mode": "terminate"},
        )
    finally:
        app.dependency_overrides.pop(get_conn, None)

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["target_run_kind"] == "bootstrap_run"
    assert body["target_run_id"] == run_id

    stop = is_stop_requested(
        ebull_test_conn,
        target_run_kind="bootstrap_run",
        target_run_id=run_id,
    )
    assert stop is not None
    assert stop.mode == "terminate"


def test_cancel_run_raises_when_complete(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _reset_state(ebull_test_conn)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    for spec in _SPECS:
        mark_stage_running(ebull_test_conn, run_id=run_id, stage_key=spec.stage_key)
        mark_stage_success(ebull_test_conn, run_id=run_id, stage_key=spec.stage_key)
    finalize_run(ebull_test_conn, run_id=run_id)
    ebull_test_conn.commit()
    assert read_state(ebull_test_conn).status == "complete"

    with pytest.raises(BootstrapNotRunning):
        cancel_run(ebull_test_conn, requested_by_operator_id=None)


def test_cancel_run_double_click_raises_already_pending(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _reset_state(ebull_test_conn)
    start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    ebull_test_conn.commit()

    cancel_run(ebull_test_conn, requested_by_operator_id=None)
    ebull_test_conn.commit()

    with pytest.raises(StopAlreadyPendingError):
        cancel_run(ebull_test_conn, requested_by_operator_id=None)
    # Outer connection still usable after the SAVEPOINT-wrapped insert
    # rejected — verify with a follow-up read.
    state = read_state(ebull_test_conn)
    assert state.status == "running"


# ---------------------------------------------------------------------------
# mark_run_cancelled
# ---------------------------------------------------------------------------


def test_mark_run_cancelled_terminalises_run_and_state(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _reset_state(ebull_test_conn)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    mark_stage_running(ebull_test_conn, run_id=run_id, stage_key="alpha")
    mark_stage_success(ebull_test_conn, run_id=run_id, stage_key="alpha")
    mark_stage_running(ebull_test_conn, run_id=run_id, stage_key="bravo")
    ebull_test_conn.commit()

    mark_run_cancelled(
        ebull_test_conn,
        run_id=run_id,
        notes_line="cancelled by operator at dispatcher checkpoint",
    )
    ebull_test_conn.commit()

    snap = read_latest_run_with_stages(ebull_test_conn)
    assert snap is not None
    assert snap.run_status == "cancelled"
    by_key = {s.stage_key: s for s in snap.stages}
    assert by_key["alpha"].status == "success"
    # PR3c #1093: running + pending stages swept to ``cancelled``
    # instead of ``error`` so the Timeline can tone gray (operator
    # cancel) rather than red (genuine failure). Re-Iterate's
    # ``reset_failed_stages_for_retry`` resets cancelled stages too,
    # so retry semantics stay intact.
    assert by_key["bravo"].status == "cancelled"
    assert by_key["charlie"].status == "cancelled"

    state = read_state(ebull_test_conn)
    assert state.status == "cancelled"

    # Notes audit line written.
    notes = ebull_test_conn.execute("SELECT notes FROM bootstrap_runs WHERE id = %s", (run_id,)).fetchone()
    assert notes is not None and notes[0] is not None
    assert "cancelled by operator" in notes[0]


def test_mark_run_cancelled_idempotent(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _reset_state(ebull_test_conn)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    ebull_test_conn.commit()

    mark_run_cancelled(ebull_test_conn, run_id=run_id)
    ebull_test_conn.commit()
    first_state = read_state(ebull_test_conn)

    mark_run_cancelled(ebull_test_conn, run_id=run_id)
    ebull_test_conn.commit()
    second_state = read_state(ebull_test_conn)

    # Both reads see cancelled; second call's status='running' guard
    # made the UPDATEs no-op.
    assert first_state.status == "cancelled"
    assert second_state.status == "cancelled"


# ---------------------------------------------------------------------------
# finalize_run interaction with cancelled
# ---------------------------------------------------------------------------


def test_finalize_run_terminalises_late_cancel(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Codex pre-push round 1 BLOCKING B2 regression: dispatcher
    finished its loop without observing the stop signal, then operator
    clicks Cancel in the gap before finalize_run runs. Without the
    cancel_requested_at branch in finalize_run, the run terminalises
    as 'complete' and the stop row is orphaned. With the fix, finalize
    routes the run to 'cancelled'.
    """
    _reset_state(ebull_test_conn)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    for spec in _SPECS:
        mark_stage_running(ebull_test_conn, run_id=run_id, stage_key=spec.stage_key)
        mark_stage_success(ebull_test_conn, run_id=run_id, stage_key=spec.stage_key)
    ebull_test_conn.commit()

    # Operator cancels AFTER all stages finished — dispatcher loop
    # already exited; checkpoint never observed the stop row.
    cancel_run(ebull_test_conn, requested_by_operator_id=None)
    ebull_test_conn.commit()

    terminal = finalize_run(ebull_test_conn, run_id=run_id)
    ebull_test_conn.commit()

    assert terminal == "cancelled"
    state = read_state(ebull_test_conn)
    assert state.status == "cancelled"


def test_finalize_run_preserves_cancelled(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Race: orchestrator completed Phase A successfully, cancel
    observed before Phase B kicks off. mark_run_cancelled fires; if the
    dispatcher's caller still calls finalize_run, the status='running'
    guard on the UPDATE preserves 'cancelled'.
    """
    _reset_state(ebull_test_conn)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    mark_stage_running(ebull_test_conn, run_id=run_id, stage_key="alpha")
    mark_stage_success(ebull_test_conn, run_id=run_id, stage_key="alpha")
    ebull_test_conn.commit()

    mark_run_cancelled(ebull_test_conn, run_id=run_id)
    ebull_test_conn.commit()

    terminal = finalize_run(ebull_test_conn, run_id=run_id)
    ebull_test_conn.commit()

    assert terminal == "cancelled"
    assert read_state(ebull_test_conn).status == "cancelled"


# ---------------------------------------------------------------------------
# reap_orphaned_running with cancel_requested_at
# ---------------------------------------------------------------------------


def test_reap_orphaned_running_routes_cancel_requested_to_cancelled(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Operator clicked cancel; jobs process restarted before the
    worker observed. Boot-recovery must terminalise as 'cancelled', not
    mask it as 'partial_error'.
    """
    _reset_state(ebull_test_conn)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    mark_stage_running(ebull_test_conn, run_id=run_id, stage_key="alpha")
    cancel_run(ebull_test_conn, requested_by_operator_id=None)
    ebull_test_conn.commit()

    swept = reap_orphaned_running(ebull_test_conn)
    ebull_test_conn.commit()

    assert swept is True
    state = read_state(ebull_test_conn)
    assert state.status == "cancelled"

    notes = ebull_test_conn.execute("SELECT notes, status FROM bootstrap_runs WHERE id = %s", (run_id,)).fetchone()
    assert notes is not None
    assert notes[1] == "cancelled"
    assert "terminated by operator before jobs restart" in (notes[0] or "")


def test_reap_orphaned_running_cancel_path_writes_cancelled_stage_status(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """PR3c #1093 — when boot-recovery routes the cancel path, running
    + pending stages get ``status='cancelled'`` not ``status='error'``.
    The Timeline tones cancelled stages gray (operator-driven) instead
    of red (genuine failure).
    """
    _reset_state(ebull_test_conn)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    mark_stage_running(ebull_test_conn, run_id=run_id, stage_key="alpha")
    cancel_run(ebull_test_conn, requested_by_operator_id=None)
    ebull_test_conn.commit()

    reap_orphaned_running(ebull_test_conn)
    ebull_test_conn.commit()

    rows = ebull_test_conn.execute(
        "SELECT stage_key, status FROM bootstrap_stages WHERE bootstrap_run_id = %s",
        (run_id,),
    ).fetchall()
    by_key = dict(rows)
    # alpha was running → cancelled (operator-cancel branch)
    assert by_key["alpha"] == "cancelled"
    # bravo + charlie were pending → cancelled
    assert by_key["bravo"] == "cancelled"
    assert by_key["charlie"] == "cancelled"


def test_reap_orphaned_running_no_cancel_path_keeps_error_stage_status(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Generic crash recovery (no operator cancel) still writes
    ``status='error'`` on running + pending stages — these were
    server-side failures, not operator-driven termination.
    """
    _reset_state(ebull_test_conn)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    mark_stage_running(ebull_test_conn, run_id=run_id, stage_key="alpha")
    ebull_test_conn.commit()

    reap_orphaned_running(ebull_test_conn)
    ebull_test_conn.commit()

    rows = ebull_test_conn.execute(
        "SELECT stage_key, status FROM bootstrap_stages WHERE bootstrap_run_id = %s",
        (run_id,),
    ).fetchall()
    by_key = dict(rows)
    assert by_key["alpha"] == "error"
    assert by_key["bravo"] == "error"
    assert by_key["charlie"] == "error"


def test_reset_failed_for_retry_picks_up_cancelled_stages(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """PR3c #1093 — re-Iterate after operator cancel resets cancelled
    stages back to pending so the retry can pick them up. Without this,
    a cancelled stage would stay terminal and the retry would never
    reach it.
    """
    _reset_state(ebull_test_conn)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    mark_stage_running(ebull_test_conn, run_id=run_id, stage_key="alpha")
    mark_stage_success(ebull_test_conn, run_id=run_id, stage_key="alpha")
    mark_stage_running(ebull_test_conn, run_id=run_id, stage_key="bravo")
    ebull_test_conn.commit()

    mark_run_cancelled(ebull_test_conn, run_id=run_id)
    ebull_test_conn.commit()

    # Bravo + charlie are now ``cancelled``; retry-failed should reset
    # them to ``pending`` (cancelled is treated like error/blocked for
    # the lane-min-order logic).
    reset = reset_failed_stages_for_retry(ebull_test_conn, run_id=run_id)
    ebull_test_conn.commit()
    assert reset > 0

    rows = ebull_test_conn.execute(
        "SELECT stage_key, status FROM bootstrap_stages WHERE bootstrap_run_id = %s",
        (run_id,),
    ).fetchall()
    by_key = dict(rows)
    assert by_key["alpha"] == "success"  # untouched, prior success
    assert by_key["bravo"] == "pending"
    assert by_key["charlie"] == "pending"


def test_reap_orphaned_running_partial_error_when_no_cancel(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Generic crash without operator cancel still terminalises as
    partial_error per the existing contract.
    """
    _reset_state(ebull_test_conn)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    mark_stage_running(ebull_test_conn, run_id=run_id, stage_key="alpha")
    ebull_test_conn.commit()

    reap_orphaned_running(ebull_test_conn)
    ebull_test_conn.commit()

    assert read_state(ebull_test_conn).status == "partial_error"
    row = ebull_test_conn.execute("SELECT status FROM bootstrap_runs WHERE id = %s", (run_id,)).fetchone()
    assert row is not None and row[0] == "partial_error"


# ---------------------------------------------------------------------------
# orchestrator end-to-end with cancel checkpoint
# ---------------------------------------------------------------------------


def test_dispatcher_observes_cancel_at_top_of_loop_and_returns_cancelled(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pre-stage cancel: insert a stop row before invoking the
    dispatcher; the very first iteration's checkpoint observes it,
    transitions the run to cancelled, and returns ``cancelled=True``
    without dispatching any stage. None of the test invokers are
    called.
    """
    _reset_state(ebull_test_conn)
    test_db_url = _bind_settings_to_test_db(monkeypatch)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    cancel_run(ebull_test_conn, requested_by_operator_id=None)
    ebull_test_conn.commit()

    calls: list[str] = []
    runnable = [
        _RunnableStage(
            stage_key="alpha",
            job_name="alpha_job",
            lane="init",
            invoker=lambda _params: calls.append("alpha"),
            requires=(),
        ),
        _RunnableStage(
            stage_key="bravo",
            job_name="bravo_job",
            lane="sec",
            invoker=lambda _params: calls.append("bravo"),
            requires=("alpha",),
        ),
    ]

    statuses, cancelled = _phase_batched_dispatch(
        run_id=run_id,
        runnable=runnable,
        database_url=test_db_url,
    )

    assert cancelled is True
    # No stage was dispatched — checkpoint fired before the first batch.
    assert calls == []
    # Statuses dict is the initial pending map (no stages advanced).
    assert statuses == {"alpha": "pending", "bravo": "pending"}

    state = read_state(ebull_test_conn)
    assert state.status == "cancelled"

    snap = read_latest_run_with_stages(ebull_test_conn)
    assert snap is not None
    assert snap.run_status == "cancelled"


def test_dispatcher_observes_cancel_between_batches(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mid-flight cancel: alpha's invoker requests the cancel as a
    side effect; the dispatcher's next-iteration checkpoint observes
    it before bravo dispatches.
    """
    _reset_state(ebull_test_conn)
    test_db_url = _bind_settings_to_test_db(monkeypatch)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    ebull_test_conn.commit()

    calls: list[str] = []

    def alpha_invoker(_params: object = None) -> None:
        calls.append("alpha")
        # Simulate operator clicking Cancel during alpha's run.
        with psycopg.connect(test_db_url) as conn:
            cancel_run(conn, requested_by_operator_id=None)
            conn.commit()

    def bravo_invoker(_params: object = None) -> None:  # pragma: no cover — must NOT run
        calls.append("bravo")

    runnable = [
        _RunnableStage(
            stage_key="alpha",
            job_name="alpha_job",
            lane="init",
            invoker=alpha_invoker,
            requires=(),
        ),
        _RunnableStage(
            stage_key="bravo",
            job_name="bravo_job",
            lane="sec",
            invoker=bravo_invoker,
            requires=("alpha",),
        ),
    ]

    _statuses, cancelled = _phase_batched_dispatch(
        run_id=run_id,
        runnable=runnable,
        database_url=test_db_url,
    )

    assert cancelled is True
    # Alpha completed; bravo never dispatched.
    assert calls == ["alpha"]

    snap = read_latest_run_with_stages(ebull_test_conn)
    assert snap is not None
    by_key = {s.stage_key: s for s in snap.stages}
    assert by_key["alpha"].status == "success"
    # bravo: was 'pending' when cancel observed; mark_run_cancelled
    # sweeps pending → error so retry-failed picks it up.
    assert by_key["bravo"].status == "error"
    assert read_state(ebull_test_conn).status == "cancelled"


def test_cancel_then_iterate_resumes_via_reset_failed(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After cancellation, the existing reset_failed_stages_for_retry
    path resets pending/error stages on the same run and flips state
    back to running — Iterate is just a re-publish of the orchestrator
    job. This test exercises the resume contract without re-running
    the orchestrator.
    """
    _reset_state(ebull_test_conn)
    _bind_settings_to_test_db(monkeypatch)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    mark_stage_running(ebull_test_conn, run_id=run_id, stage_key="alpha")
    mark_stage_success(ebull_test_conn, run_id=run_id, stage_key="alpha")
    ebull_test_conn.commit()

    # Operator cancels.
    cancel_run(ebull_test_conn, requested_by_operator_id=None)
    ebull_test_conn.commit()
    mark_run_cancelled(ebull_test_conn, run_id=run_id)
    ebull_test_conn.commit()
    assert read_state(ebull_test_conn).status == "cancelled"

    # Operator clicks Iterate (= reset failed/pending + republish).
    reset_count = reset_failed_stages_for_retry(ebull_test_conn, run_id=run_id)
    ebull_test_conn.commit()

    # alpha success preserved; bravo + charlie were swept to error
    # and reset back to pending.
    assert reset_count == 2
    snap = read_latest_run_with_stages(ebull_test_conn)
    assert snap is not None
    by_key = {s.stage_key: s for s in snap.stages}
    assert by_key["alpha"].status == "success"
    assert by_key["bravo"].status == "pending"
    assert by_key["charlie"].status == "pending"
    assert read_state(ebull_test_conn).status == "running"


# ---------------------------------------------------------------------------
# scheduler-gate verification — _bootstrap_complete rejects 'cancelled'
# ---------------------------------------------------------------------------


def test_bootstrap_complete_returns_false_on_cancelled(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The cancelled state must keep dependent SEC / fundamentals
    jobs gated. Operator must Iterate or force-mark-complete.
    """
    from app.workers.scheduler import _bootstrap_complete

    _reset_state(ebull_test_conn)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    mark_run_cancelled(ebull_test_conn, run_id=run_id)
    ebull_test_conn.commit()

    ok, msg = _bootstrap_complete(ebull_test_conn)
    assert ok is False
    assert "bootstrap" in msg.lower()


def test_force_mark_complete_releases_gate_after_cancel(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """After cancel, the operator escape hatch (force_mark_complete)
    still works — it requires status != 'running', and 'cancelled' is
    a non-running terminal state.
    """
    _reset_state(ebull_test_conn)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    cancel_run(ebull_test_conn, requested_by_operator_id=None)
    mark_run_cancelled(ebull_test_conn, run_id=run_id)
    ebull_test_conn.commit()
    assert read_state(ebull_test_conn).status == "cancelled"

    force_mark_complete(ebull_test_conn)
    ebull_test_conn.commit()
    assert read_state(ebull_test_conn).status == "complete"


# Avoid unused-import lint warnings on helpers used only by the
# integration test above.
_ = mark_stage_error
_ = run_bootstrap_orchestrator
