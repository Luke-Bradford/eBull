"""Real-DB integration tests for #1139.

End-to-end coverage of the atomic enqueue contract:

  * ``POST /system/bootstrap/run`` and ``POST /system/bootstrap/retry-failed``
    create the run / reset stages AND insert the
    ``pending_job_requests`` row inside a single transaction. A publish
    failure rolls **both** sides back so the singleton never strands at
    ``status='running'`` with no queue row.
  * ``bootstrap_runs.triggered_by_operator_id`` is populated from the
    request's authenticated operator UUID on ``/run`` and is **not**
    overwritten by a subsequent ``/retry-failed`` from a different
    operator.
  * The new ``BootstrapNoPriorRun`` / ``BootstrapNotResettable`` /
    ``BootstrapAlreadyRunning`` precedence inside the no-arg
    ``reset_failed_stages_for_retry`` matches the spec table in
    docs/superpowers/specs/2026-05-13-atomic-bootstrap-enqueue.md.

The processes-endpoint coverage of ``_apply_bootstrap_iterate_reset``'s
new exception handling + no-op-no-enqueue path lives in
``tests/test_processes_endpoints.py``.

DB-backed: the contract under test is the atomicity of two SQL writes
on the same connection. Mocks can't show that the publish failure
rolls back the singleton flip — that's a Postgres guarantee, and
verifying it requires the real engine.
"""

from __future__ import annotations

from collections.abc import Iterator
from uuid import UUID, uuid4

import psycopg
import pytest
from fastapi.testclient import TestClient

from app.db import get_conn
from app.main import app
from app.services.bootstrap_state import (
    BootstrapAlreadyRunning,
    BootstrapNoPriorRun,
    BootstrapNotResettable,
    StageSpec,
    finalize_run,
    mark_stage_error,
    mark_stage_running,
    mark_stage_success,
    read_latest_run_with_stages,
    read_state,
    reset_failed_stages_for_retry,
    start_run,
)

client = TestClient(app)


# Use a stage-spec triple that mirrors the production lanes used by
# the orchestrator so the lane-min-order walk in
# ``reset_failed_stages_for_retry`` exercises the same code path as
# production (one init, one etoro, one sec).
_SPECS = (
    StageSpec(stage_key="universe_sync", stage_order=1, lane="init", job_name="nightly_universe_sync"),
    StageSpec(stage_key="candle_refresh", stage_order=2, lane="etoro", job_name="daily_candle_refresh"),
    StageSpec(stage_key="cusip_universe_backfill", stage_order=3, lane="sec", job_name="cusip_universe_backfill"),
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def conn_override(
    ebull_test_conn: psycopg.Connection[tuple],
) -> Iterator[None]:
    """Route the FastAPI ``get_conn`` dep to the test DB connection.

    Mirrors the pattern in tests/test_processes_endpoints.py. Required
    because the #1139 contract under test is that the handler's
    transaction shares a single connection with the publish helper —
    that only holds when the test connection is the one the handler
    uses.
    """

    def _yield_conn() -> Iterator[psycopg.Connection[tuple]]:
        yield ebull_test_conn

    app.dependency_overrides[get_conn] = _yield_conn
    try:
        yield
    finally:
        app.dependency_overrides.pop(get_conn, None)


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


def _wipe_pending_requests(conn: psycopg.Connection[tuple]) -> None:
    """Delete bootstrap-orchestrator queue rows so the tests' assertions
    count their own. Scoped DELETE rather than TRUNCATE because
    ``job_runs`` FK-references ``pending_job_requests``; TRUNCATE
    would 42P12 against the referenced table.
    """
    conn.execute("DELETE FROM pending_job_requests WHERE job_name = 'bootstrap_orchestrator'")
    conn.commit()


def _count_orchestrator_requests(conn: psycopg.Connection[tuple]) -> int:
    row = conn.execute("SELECT COUNT(*) FROM pending_job_requests WHERE job_name = 'bootstrap_orchestrator'").fetchone()
    assert row is not None
    return int(row[0])


def _ensure_operator(conn: psycopg.Connection[tuple], operator_id: UUID) -> None:
    """Insert a minimal operator row so FK constraints on
    ``bootstrap_runs.triggered_by_operator_id`` (UUID REFERENCES
    operators(operator_id)) are satisfied. ON CONFLICT keeps it
    idempotent across re-runs that re-use the same UUID. The
    username embeds the UUID so concurrent tests can each insert
    their own operator without colliding on the UNIQUE(username).
    """
    conn.execute(
        """
        INSERT INTO operators (operator_id, username, password_hash)
        VALUES (%s, %s, %s)
        ON CONFLICT (operator_id) DO NOTHING
        """,
        (operator_id, f"op-{operator_id}", "test-not-a-real-hash"),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# /run — happy path + rollback-on-publish-failure
# ---------------------------------------------------------------------------


def test_run_commits_state_and_queue_row_together(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    """Happy path: /run inserts the bootstrap_runs row, seeds stages,
    flips singleton to running, AND inserts the pending_job_requests
    row — all visible after the single shared transaction commits.
    """
    _reset_state(ebull_test_conn)
    _wipe_pending_requests(ebull_test_conn)

    # Skip the real spec list (24 stages); patch the spec factory to
    # the minimal triple above so this test doesn't depend on the
    # production lane shape.
    from app.api import bootstrap as bootstrap_api

    orig_specs = bootstrap_api.get_bootstrap_stage_specs
    bootstrap_api.get_bootstrap_stage_specs = lambda: _SPECS  # type: ignore[assignment]
    try:
        resp = client.post("/system/bootstrap/run")
    finally:
        bootstrap_api.get_bootstrap_stage_specs = orig_specs  # type: ignore[assignment]

    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["run_id"] is not None
    assert body["request_id"] is not None

    state = read_state(ebull_test_conn)
    assert state.status == "running"
    assert state.last_run_id == body["run_id"]

    snap = read_latest_run_with_stages(ebull_test_conn)
    assert snap is not None
    assert snap.run_id == body["run_id"]
    assert {s.stage_key for s in snap.stages} == {spec.stage_key for spec in _SPECS}

    assert _count_orchestrator_requests(ebull_test_conn) == 1


def test_run_rolls_back_when_publish_raises(
    conn_override: None,
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Publish failure mid-transaction rolls back the singleton flip,
    the bootstrap_runs row, and the seeded stages. No stranded
    'running' state, no orphan queue row. The exact failure window
    #1139 closes."""
    _reset_state(ebull_test_conn)
    _wipe_pending_requests(ebull_test_conn)

    from app.api import bootstrap as bootstrap_api

    def _boom(*_args: object, **_kwargs: object) -> int:
        raise psycopg.OperationalError("queue write wedged")

    monkeypatch.setattr(bootstrap_api, "publish_manual_job_request_with_conn", _boom)

    orig_specs = bootstrap_api.get_bootstrap_stage_specs
    bootstrap_api.get_bootstrap_stage_specs = lambda: _SPECS  # type: ignore[assignment]
    try:
        resp = client.post("/system/bootstrap/run")
    finally:
        bootstrap_api.get_bootstrap_stage_specs = orig_specs  # type: ignore[assignment]

    assert resp.status_code == 503
    assert resp.json()["detail"]["error"] == "queue_publish_failed"

    state = read_state(ebull_test_conn)
    assert state.status == "pending", "singleton must NOT have flipped to running"
    assert state.last_run_id is None, "singleton must NOT carry a run_id from the rolled-back run"

    runs_row = ebull_test_conn.execute("SELECT COUNT(*) FROM bootstrap_runs").fetchone()
    assert runs_row is not None
    assert runs_row[0] == 0, "bootstrap_runs INSERT must be rolled back"

    stages_row = ebull_test_conn.execute("SELECT COUNT(*) FROM bootstrap_stages").fetchone()
    assert stages_row is not None
    assert stages_row[0] == 0, "bootstrap_stages seed must be rolled back"

    assert _count_orchestrator_requests(ebull_test_conn) == 0


def test_run_persists_operator_uuid_on_triggered_by(
    conn_override: None,
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The request's authenticated operator UUID is stamped on
    ``bootstrap_runs.triggered_by_operator_id`` for audit (#1139)."""
    op_uuid = uuid4()
    _ensure_operator(ebull_test_conn, op_uuid)
    _reset_state(ebull_test_conn)
    _wipe_pending_requests(ebull_test_conn)

    from app.api import bootstrap as bootstrap_api

    def _identity(_req: object) -> UUID:
        return op_uuid

    monkeypatch.setattr(bootstrap_api, "_operator_uuid", _identity)

    orig_specs = bootstrap_api.get_bootstrap_stage_specs
    bootstrap_api.get_bootstrap_stage_specs = lambda: _SPECS  # type: ignore[assignment]
    try:
        resp = client.post("/system/bootstrap/run")
    finally:
        bootstrap_api.get_bootstrap_stage_specs = orig_specs  # type: ignore[assignment]

    assert resp.status_code == 202, resp.text
    run_id = resp.json()["run_id"]

    row = ebull_test_conn.execute(
        "SELECT triggered_by_operator_id FROM bootstrap_runs WHERE id = %s", (run_id,)
    ).fetchone()
    assert row is not None
    assert row[0] == op_uuid


# ---------------------------------------------------------------------------
# /retry-failed — sole-gate helper + rollback + operator non-overwrite
# ---------------------------------------------------------------------------


def _seed_partial_error_run(conn: psycopg.Connection[tuple]) -> int:
    """Seed a singleton+run pair where one stage is in 'error' and
    the singleton + run are both 'partial_error'. Returns the run id.
    """
    _reset_state(conn)
    run_id = start_run(conn, operator_id=None, stage_specs=_SPECS)
    conn.commit()

    # Two stages succeed, one fails.
    mark_stage_running(conn, run_id=run_id, stage_key="universe_sync")
    mark_stage_success(conn, run_id=run_id, stage_key="universe_sync")
    mark_stage_running(conn, run_id=run_id, stage_key="candle_refresh")
    mark_stage_success(conn, run_id=run_id, stage_key="candle_refresh")
    mark_stage_running(conn, run_id=run_id, stage_key="cusip_universe_backfill")
    mark_stage_error(
        conn,
        run_id=run_id,
        stage_key="cusip_universe_backfill",
        error_message="seeded for #1139 test",
    )
    finalize_run(conn, run_id=run_id)
    conn.commit()
    assert read_state(conn).status == "partial_error"
    return run_id


def test_retry_failed_commits_reset_and_queue_row_together(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    _seed_partial_error_run(ebull_test_conn)
    _wipe_pending_requests(ebull_test_conn)

    resp = client.post("/system/bootstrap/retry-failed")
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["request_id"] is not None

    assert read_state(ebull_test_conn).status == "running"
    assert _count_orchestrator_requests(ebull_test_conn) == 1


def test_retry_failed_rolls_back_when_publish_raises(
    conn_override: None,
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_id = _seed_partial_error_run(ebull_test_conn)
    _wipe_pending_requests(ebull_test_conn)

    from app.api import bootstrap as bootstrap_api

    def _boom(*_args: object, **_kwargs: object) -> int:
        raise psycopg.OperationalError("queue write wedged")

    monkeypatch.setattr(bootstrap_api, "publish_manual_job_request_with_conn", _boom)

    resp = client.post("/system/bootstrap/retry-failed")
    assert resp.status_code == 503

    # Stage stays 'error', singleton stays 'partial_error' — reset rolled back.
    snap = read_latest_run_with_stages(ebull_test_conn)
    assert snap is not None
    by_key = {s.stage_key: s for s in snap.stages}
    assert by_key["cusip_universe_backfill"].status == "error"
    assert read_state(ebull_test_conn).status == "partial_error"
    assert read_state(ebull_test_conn).last_run_id == run_id

    # No queue row landed.
    assert _count_orchestrator_requests(ebull_test_conn) == 0


def test_retry_failed_does_not_overwrite_original_triggered_by(
    conn_override: None,
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#1139 — retry path must not touch ``triggered_by_operator_id``.

    Operator A starts the run; operator B retries it. The audit column
    must still name operator A so post-mortem can answer "who started
    this run?" correctly.
    """
    op_a = uuid4()
    op_b = uuid4()
    _ensure_operator(ebull_test_conn, op_a)
    _ensure_operator(ebull_test_conn, op_b)
    _reset_state(ebull_test_conn)

    # Start as operator A (call helper directly — bypasses identity middleware).
    run_id = start_run(ebull_test_conn, operator_id=op_a, stage_specs=_SPECS)
    ebull_test_conn.commit()

    # Drive the run to partial_error with one failed stage.
    mark_stage_running(ebull_test_conn, run_id=run_id, stage_key="universe_sync")
    mark_stage_success(ebull_test_conn, run_id=run_id, stage_key="universe_sync")
    mark_stage_running(ebull_test_conn, run_id=run_id, stage_key="candle_refresh")
    mark_stage_error(
        ebull_test_conn,
        run_id=run_id,
        stage_key="candle_refresh",
        error_message="seeded for triggered_by non-overwrite test",
    )
    finalize_run(ebull_test_conn, run_id=run_id)
    ebull_test_conn.commit()
    assert read_state(ebull_test_conn).status == "partial_error"

    _wipe_pending_requests(ebull_test_conn)

    # Now operator B triggers /retry-failed.
    from app.api import bootstrap as bootstrap_api

    monkeypatch.setattr(bootstrap_api, "_operator_uuid", lambda _req: op_b)

    resp = client.post("/system/bootstrap/retry-failed")
    assert resp.status_code == 202, resp.text

    # Audit column still names operator A.
    row = ebull_test_conn.execute(
        "SELECT triggered_by_operator_id FROM bootstrap_runs WHERE id = %s", (run_id,)
    ).fetchone()
    assert row is not None
    assert row[0] == op_a, "retry must NOT overwrite the original triggered_by_operator_id"


# ---------------------------------------------------------------------------
# Helper precedence — direct calls (not through the API)
# ---------------------------------------------------------------------------


def test_helper_no_prior_run_takes_precedence(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """Fresh-install state: status='pending', last_run_id IS NULL.
    Helper raises BootstrapNoPriorRun BEFORE the status check fires —
    preserves the pre-#1139 404 contract for "no prior run".
    """
    _reset_state(ebull_test_conn)
    with pytest.raises(BootstrapNoPriorRun):
        reset_failed_stages_for_retry(ebull_test_conn)
    assert read_state(ebull_test_conn).status == "pending"


def test_helper_no_prior_run_even_with_partial_error_status(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Synthetic: singleton flipped to partial_error but last_run_id NULL
    (could only happen via raw SQL). Helper still raises
    BootstrapNoPriorRun — no-prior-run precedes status check.
    """
    _reset_state(ebull_test_conn)
    ebull_test_conn.execute("UPDATE bootstrap_state SET status='partial_error' WHERE id=1")
    ebull_test_conn.commit()

    with pytest.raises(BootstrapNoPriorRun):
        reset_failed_stages_for_retry(ebull_test_conn)
    assert read_state(ebull_test_conn).status == "partial_error"


def test_helper_not_resettable_for_complete(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """Singleton='complete' with valid last_run_id: helper raises
    BootstrapNotResettable. Verifies the helper is sole authoritative
    gate (#1139) — no caller can sneak past with a stale read.
    """
    _reset_state(ebull_test_conn)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    ebull_test_conn.commit()
    for spec in _SPECS:
        mark_stage_running(ebull_test_conn, run_id=run_id, stage_key=spec.stage_key)
        mark_stage_success(ebull_test_conn, run_id=run_id, stage_key=spec.stage_key)
    finalize_run(ebull_test_conn, run_id=run_id)
    ebull_test_conn.commit()
    assert read_state(ebull_test_conn).status == "complete"

    with pytest.raises(BootstrapNotResettable) as exc_info:
        reset_failed_stages_for_retry(ebull_test_conn)
    assert exc_info.value.status == "complete"
    assert read_state(ebull_test_conn).status == "complete"


def test_helper_already_running_when_singleton_running(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Singleton='running' is the operator-friendly precedence:
    BootstrapAlreadyRunning, NOT BootstrapNotResettable.
    """
    _reset_state(ebull_test_conn)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    ebull_test_conn.commit()

    with pytest.raises(BootstrapAlreadyRunning) as exc_info:
        reset_failed_stages_for_retry(ebull_test_conn)
    assert exc_info.value.run_id == run_id
    assert read_state(ebull_test_conn).status == "running"


def test_retry_failed_targets_singleton_last_run_id(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    """#1139 — the new helper derives the target ``run_id`` from
    ``bootstrap_state.last_run_id`` under the FOR UPDATE lock, so a
    caller cannot accidentally retry an older run. Seed two runs
    (one older, one current), set the singleton to the current one,
    and confirm the API only resets stages on the current run.
    """
    _reset_state(ebull_test_conn)

    # Run 1: completed, then a fresh run 2 takes its place. Drive 2
    # into partial_error with one failed stage. Singleton.last_run_id
    # already points at run 2 (start_run wrote it).
    run_1 = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    ebull_test_conn.commit()
    for spec in _SPECS:
        mark_stage_running(ebull_test_conn, run_id=run_1, stage_key=spec.stage_key)
        mark_stage_success(ebull_test_conn, run_id=run_1, stage_key=spec.stage_key)
    finalize_run(ebull_test_conn, run_id=run_1)
    ebull_test_conn.commit()
    assert read_state(ebull_test_conn).status == "complete"
    # Reset the singleton to 'pending' to allow the second start_run
    # (start_run rejects 'running' and 'complete'-then-restart needs
    # the singleton clear; the helpers used in this test fixture
    # mirror the real "operator restarts after a complete run" flow).
    _reset_state(ebull_test_conn)
    run_2 = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    ebull_test_conn.commit()
    assert run_2 != run_1
    mark_stage_running(ebull_test_conn, run_id=run_2, stage_key="cusip_universe_backfill")
    mark_stage_error(
        ebull_test_conn,
        run_id=run_2,
        stage_key="cusip_universe_backfill",
        error_message="seeded",
    )
    finalize_run(ebull_test_conn, run_id=run_2)
    ebull_test_conn.commit()
    state = read_state(ebull_test_conn)
    assert state.status == "partial_error"
    assert state.last_run_id == run_2
    _wipe_pending_requests(ebull_test_conn)

    # Trigger retry. The helper derives run_id from the singleton, so
    # it targets run 2 (NOT run 1, whose stage rows must be untouched).
    resp = client.post("/system/bootstrap/retry-failed")
    assert resp.status_code == 202, resp.text
    assert resp.json()["run_id"] == run_2

    # Run 1's stage history is unchanged (forensic).
    run_1_rows = ebull_test_conn.execute(
        "SELECT status FROM bootstrap_stages WHERE bootstrap_run_id = %s",
        (run_1,),
    ).fetchall()
    assert all(r[0] == "success" for r in run_1_rows), "run 1 stages must be untouched"


def test_retry_failed_pending_no_prior_run_returns_404(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    """#1139 — fresh install (pending + last_run_id NULL): API returns
    404, preserving the pre-#1139 ``no prior bootstrap run to retry``
    contract. The helper's ``BootstrapNoPriorRun`` precedes the
    status check so this path fires before the pending-state-409
    branch.
    """
    _reset_state(ebull_test_conn)
    resp = client.post("/system/bootstrap/retry-failed")
    assert resp.status_code == 404, resp.text


def test_helper_zero_reset_when_partial_error_has_no_failed_stages(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Synthetic edge case: singleton in partial_error but every stage
    on the latest run is 'success'. Helper returns (run_id, 0) and
    leaves state untouched — API maps to 404, processes endpoint
    maps to 409 'bootstrap_no_failed_stages' (neither enqueues).
    """
    _reset_state(ebull_test_conn)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    ebull_test_conn.commit()
    for spec in _SPECS:
        mark_stage_running(ebull_test_conn, run_id=run_id, stage_key=spec.stage_key)
        mark_stage_success(ebull_test_conn, run_id=run_id, stage_key=spec.stage_key)
    ebull_test_conn.commit()

    # Force singleton + run into a "partial_error with all-success
    # stages" state (couldn't be reached by finalize_run, but covers
    # the edge case via raw SQL — what a manual operator fix could
    # leave).
    ebull_test_conn.execute(
        "UPDATE bootstrap_runs SET status='partial_error', completed_at=now() WHERE id=%s",
        (run_id,),
    )
    ebull_test_conn.execute("UPDATE bootstrap_state SET status='partial_error' WHERE id=1")
    ebull_test_conn.commit()

    helper_run_id, reset_count = reset_failed_stages_for_retry(ebull_test_conn)
    assert helper_run_id == run_id
    assert reset_count == 0
    # State must NOT flip back to running on a no-op retry.
    assert read_state(ebull_test_conn).status == "partial_error"
