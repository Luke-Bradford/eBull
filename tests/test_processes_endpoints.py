"""Endpoint contract tests for ``/system/processes`` (#1071, PR3).

DB-backed: the trigger / cancel endpoints insert into
``pending_job_requests`` + read ``bootstrap_state`` + ``job_runs``, so
mocking the cursor would lose the partial-unique fence guarantee.
"""

from __future__ import annotations

from collections.abc import Iterator

import psycopg
import pytest
from fastapi.testclient import TestClient

from app.db import get_conn
from app.main import app
from app.workers.scheduler import JOB_RETRY_DEFERRED

client = TestClient(app)


@pytest.fixture
def conn_override(
    ebull_test_conn: psycopg.Connection[tuple],
) -> Iterator[None]:
    """Wire the FastAPI ``get_conn`` dependency to the test DB connection.

    Codex round 7 fix: the trigger handler now writes its
    ``pending_job_requests`` row inside the request's tx (atomic with
    fence-check under the per-process advisory lock). That tx uses
    ``conn`` from the FastAPI dep — overriding ``get_conn`` to yield
    the test conn means INSERTs land in the test DB and precondition
    re-reads see them; no separate ``publish_manual_job_request``
    monkeypatch needed.

    Always reset the override on teardown so the next test's fixture
    (or the smoke test) starts clean. The auth no-op override is
    preserved by the conftest autouse fixture.
    """

    def _yield_conn() -> Iterator[psycopg.Connection[tuple]]:
        yield ebull_test_conn

    app.dependency_overrides[get_conn] = _yield_conn
    try:
        yield
    finally:
        app.dependency_overrides.pop(get_conn, None)


def _ensure_kill_switch_off(conn: psycopg.Connection[tuple]) -> None:
    conn.execute(
        """
        INSERT INTO kill_switch (id, is_active, activated_at, activated_by, reason)
        VALUES (TRUE, FALSE, NULL, NULL, NULL)
        ON CONFLICT (id) DO UPDATE
        SET is_active = FALSE, activated_at = NULL, activated_by = NULL, reason = NULL
        """
    )


def _seed_bootstrap_state(conn: psycopg.Connection[tuple], status: str) -> None:
    conn.execute("UPDATE bootstrap_state SET status = %s WHERE id = 1", (status,))


def test_list_processes_returns_envelope_shape(conn_override: None, ebull_test_conn: psycopg.Connection[tuple]) -> None:
    _ensure_kill_switch_off(ebull_test_conn)
    _seed_bootstrap_state(ebull_test_conn, "pending")
    ebull_test_conn.commit()

    resp = client.get("/system/processes")
    assert resp.status_code == 200, resp.text
    payload = resp.json()
    assert "rows" in payload and "partial" in payload
    assert payload["partial"] is False
    assert isinstance(payload["rows"], list)
    process_ids = {r["process_id"] for r in payload["rows"]}
    assert "bootstrap" in process_ids
    assert JOB_RETRY_DEFERRED in process_ids


def test_get_process_unknown_returns_404(conn_override: None) -> None:
    resp = client.get("/system/processes/not_a_real_thing")
    assert resp.status_code == 404


def test_trigger_bootstrap_iterate_from_pending_returns_409(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    """Iterate = retry-failed; from 'pending' there is nothing to resume."""
    _ensure_kill_switch_off(ebull_test_conn)
    _seed_bootstrap_state(ebull_test_conn, "pending")
    ebull_test_conn.commit()

    resp = client.post("/system/processes/bootstrap/trigger", json={"mode": "iterate"})
    assert resp.status_code == 409, resp.text
    detail = resp.json()["detail"]
    assert detail["reason"] == "bootstrap_not_resumable"


def test_trigger_bootstrap_full_wash_inserts_fence_row(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    _ensure_kill_switch_off(ebull_test_conn)
    _seed_bootstrap_state(ebull_test_conn, "pending")
    ebull_test_conn.commit()

    resp = client.post("/system/processes/bootstrap/trigger", json={"mode": "full_wash"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["mode"] == "full_wash"
    assert isinstance(body["request_id"], int)

    # Fence row exists with mode='full_wash' + process_id='bootstrap'.
    row = ebull_test_conn.execute(
        """
        SELECT process_id, mode, status FROM pending_job_requests
        WHERE request_id = %s
        """,
        (body["request_id"],),
    ).fetchone()
    assert row is not None
    assert row[0] == "bootstrap"
    assert row[1] == "full_wash"
    assert row[2] == "pending"


def test_trigger_kill_switch_active_returns_409(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    ebull_test_conn.execute(
        """
        INSERT INTO kill_switch (id, is_active, activated_at, activated_by, reason)
        VALUES (TRUE, TRUE, now(), 'test', 'paused')
        ON CONFLICT (id) DO UPDATE
        SET is_active = TRUE, activated_at = now(), activated_by = 'test', reason = 'paused'
        """
    )
    _seed_bootstrap_state(ebull_test_conn, "pending")
    ebull_test_conn.commit()

    resp = client.post("/system/processes/bootstrap/trigger", json={"mode": "full_wash"})
    assert resp.status_code == 409
    assert resp.json()["detail"]["reason"] == "kill_switch_active"


def test_trigger_scheduled_iterate_dedup_409(conn_override: None, ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """Two iterate triggers in a row → second one 409s on the
    iterate_already_pending precondition."""
    _ensure_kill_switch_off(ebull_test_conn)
    ebull_test_conn.commit()

    first = client.post(
        f"/system/processes/{JOB_RETRY_DEFERRED}/trigger",
        json={"mode": "iterate"},
    )
    assert first.status_code == 200, first.text

    second = client.post(
        f"/system/processes/{JOB_RETRY_DEFERRED}/trigger",
        json={"mode": "iterate"},
    )
    assert second.status_code == 409
    assert second.json()["detail"]["reason"] == "iterate_already_pending"


def test_trigger_scheduled_full_wash_blocks_subsequent_iterate(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    _ensure_kill_switch_off(ebull_test_conn)
    ebull_test_conn.commit()

    first = client.post(
        f"/system/processes/{JOB_RETRY_DEFERRED}/trigger",
        json={"mode": "full_wash"},
    )
    assert first.status_code == 200, first.text

    second = client.post(
        f"/system/processes/{JOB_RETRY_DEFERRED}/trigger",
        json={"mode": "iterate"},
    )
    assert second.status_code == 409
    # Fence check runs FIRST in `_check_scheduled_job_preconditions`
    # (PR #1072 review WARNING fix), so the iterate POST during an
    # active full-wash always reports the spec-aligned fence reason —
    # never `iterate_already_pending`. Pin the exact reason so a
    # future precondition reorder shows up as a test diff.
    assert second.json()["detail"]["reason"] == "full_wash_already_pending"


def test_trigger_invalid_mode_returns_422(conn_override: None) -> None:
    resp = client.post(
        f"/system/processes/{JOB_RETRY_DEFERRED}/trigger",
        json={"mode": "NUKE"},
    )
    assert resp.status_code == 422


def test_cancel_no_active_run_returns_409(conn_override: None, ebull_test_conn: psycopg.Connection[tuple]) -> None:
    _ensure_kill_switch_off(ebull_test_conn)
    ebull_test_conn.commit()

    resp = client.post(
        f"/system/processes/{JOB_RETRY_DEFERRED}/cancel",
        json={"mode": "cooperative"},
    )
    assert resp.status_code == 409
    assert resp.json()["detail"]["reason"] == "no_active_run"


def test_cancel_invalid_mode_returns_422(conn_override: None) -> None:
    resp = client.post(
        f"/system/processes/{JOB_RETRY_DEFERRED}/cancel",
        json={"mode": "halt"},
    )
    assert resp.status_code == 422


def test_partial_flag_when_adapter_throws(
    conn_override: None,
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Spec §Failure-mode invariants: an adapter raising must omit its
    rows, NOT 500 the page. The envelope flips ``partial=true``."""
    _ensure_kill_switch_off(ebull_test_conn)
    _seed_bootstrap_state(ebull_test_conn, "pending")
    ebull_test_conn.commit()

    def _explode(_conn: object) -> list[object]:
        raise RuntimeError("adapter exploded")

    from app.services.processes import scheduled_adapter

    monkeypatch.setattr(scheduled_adapter, "list_rows", _explode)

    resp = client.get("/system/processes")
    assert resp.status_code == 200, resp.text
    payload = resp.json()
    assert payload["partial"] is True
    process_ids = {r["process_id"] for r in payload["rows"]}
    # bootstrap survived; scheduled_jobs are absent.
    assert "bootstrap" in process_ids
    assert JOB_RETRY_DEFERRED not in process_ids
