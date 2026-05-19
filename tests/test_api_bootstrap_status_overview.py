"""Tests for ``GET /system/bootstrap-status`` (#1136 Phase A.3 audit endpoint).

Lean operator readout sibling to ``/system/bootstrap/status``; mirrors
the ``/system/postgres-health`` failure posture. Pins on
``bootstrap_state.last_run_id`` rather than ``ORDER BY id DESC LIMIT 1``
so the readout reflects what ``/retry-failed`` would actually target.

Mock pattern mirrors ``tests/test_api_bootstrap.py`` — patch the
``read_state`` / ``read_run_with_stages`` re-exports the endpoint module
imports, plus a ``MagicMock`` connection so the FastAPI dependency
chain is happy without a live DB.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import psycopg
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services.bootstrap_state import BootstrapState, RunSnapshot, StageRow


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture(autouse=True)
def cleanup() -> Iterator[None]:
    yield
    from app.db import get_conn

    app.dependency_overrides.pop(get_conn, None)


def _install_conn() -> MagicMock:
    conn = MagicMock()

    def _dep() -> Iterator[MagicMock]:
        yield conn

    from app.db import get_conn

    app.dependency_overrides[get_conn] = _dep
    return conn


def _stage(stage_key: str, stage_order: int, lane: str, status: str, **kwargs: object) -> StageRow:
    return StageRow(
        id=stage_order,
        bootstrap_run_id=int(kwargs.get("bootstrap_run_id", 42) or 42),  # type: ignore[arg-type]
        stage_key=stage_key,
        stage_order=stage_order,
        lane=lane,  # type: ignore[arg-type]
        job_name=str(kwargs.get("job_name", "x")),
        status=status,  # type: ignore[arg-type]
        started_at=None,
        completed_at=kwargs.get("completed_at"),  # type: ignore[arg-type]
        rows_processed=kwargs.get("rows_processed"),  # type: ignore[arg-type]
        expected_units=None,
        units_done=None,
        last_error=kwargs.get("last_error"),  # type: ignore[arg-type]
        attempt_count=int(kwargs.get("attempt_count", 1) or 1),  # type: ignore[arg-type]
    )


# ---------------------------------------------------------------------------
# No-prior-run shape
# ---------------------------------------------------------------------------


def test_get_status_overview_no_prior_run(client: TestClient) -> None:
    _install_conn()
    with (
        patch(
            "app.api.system.read_state",
            return_value=BootstrapState(status="pending", last_run_id=None, last_completed_at=None),
        ),
        patch("app.api.system.read_run_with_stages") as read_run_mock,
    ):
        resp = client.get("/system/bootstrap-status")

    assert resp.status_code == 200
    body = resp.json()
    assert body["state_status"] == "pending"
    assert body["current_run_id"] is None
    assert body["stages"] == []
    assert body["retry_available"] is False
    assert body["retry_blocked_reason"] == "no_prior_run"
    assert body["summary"]["total"] == 0
    # No need to consult the run table when there's no run id.
    read_run_mock.assert_not_called()


# ---------------------------------------------------------------------------
# partial_error shape — happy path
# ---------------------------------------------------------------------------


def test_get_status_overview_partial_error(client: TestClient) -> None:
    _install_conn()
    snap = RunSnapshot(
        run_id=42,
        run_status="partial_error",
        triggered_at=datetime(2026, 5, 17, 1, 0, tzinfo=UTC),
        completed_at=datetime(2026, 5, 17, 5, 30, tzinfo=UTC),
        stages=(
            _stage("init", 1, "init", "success"),
            _stage("s17", 17, "sec_rate", "error", last_error="lock contention"),
            _stage("s18", 18, "sec_rate", "error"),
            _stage("s19", 19, "sec_rate", "success"),  # downstream-of-failure
            _stage("s23", 23, "db", "blocked"),
        ),
    )
    state = BootstrapState(
        status="partial_error",
        last_run_id=42,
        last_completed_at=datetime(2026, 5, 17, 5, 30, tzinfo=UTC),
    )
    with (
        patch("app.api.system.read_state", return_value=state),
        patch("app.api.system.read_run_with_stages", return_value=snap),
    ):
        resp = client.get("/system/bootstrap-status")

    assert resp.status_code == 200
    body = resp.json()
    assert body["state_status"] == "partial_error"
    assert body["current_run_id"] == 42
    assert body["retry_available"] is True
    assert body["retry_blocked_reason"] is None

    # Per-stage retryable mirrors the lane-min-order predicate.
    by_key = {s["stage_key"]: s for s in body["stages"]}
    assert by_key["init"]["retryable"] is False
    assert by_key["s17"]["retryable"] is True
    assert by_key["s18"]["retryable"] is True
    assert by_key["s19"]["retryable"] is True  # success but >= min(sec_rate failed)
    assert by_key["s23"]["retryable"] is True

    # Summary counts sum to total.
    s = body["summary"]
    assert s["total"] == 5
    assert s["success"] == 2
    assert s["error"] == 2
    assert s["blocked"] == 1
    assert s["pending"] == 0
    assert (
        s["success"] + s["error"] + s["blocked"] + s["pending"] + s["running"] + s["skipped"] + s["cancelled"]
        == s["total"]
    )


# ---------------------------------------------------------------------------
# Run-id pinning (Codex 1b §2)
# ---------------------------------------------------------------------------


def test_get_status_overview_pins_on_last_run_id(client: TestClient) -> None:
    """Endpoint reads stages keyed off ``bootstrap_state.last_run_id``.

    Two runs could exist with the singleton pointed at the *older*
    one (e.g. post-restart sweep re-seeded a row without touching the
    singleton). The readout must follow the singleton, not
    ``ORDER BY bootstrap_runs.id DESC``.
    """
    _install_conn()
    older_snap = RunSnapshot(
        run_id=42,
        run_status="partial_error",
        triggered_at=datetime(2026, 5, 17, 1, 0, tzinfo=UTC),
        completed_at=datetime(2026, 5, 17, 5, 30, tzinfo=UTC),
        stages=(_stage("older_stage", 1, "init", "success", bootstrap_run_id=42),),
    )
    state = BootstrapState(
        status="partial_error",
        last_run_id=42,  # singleton pinned at older
        last_completed_at=datetime(2026, 5, 17, 5, 30, tzinfo=UTC),
    )

    captured_run_ids: list[int] = []

    def _capture(_conn: object, *, run_id: int) -> RunSnapshot:
        captured_run_ids.append(run_id)
        return older_snap

    with (
        patch("app.api.system.read_state", return_value=state),
        patch("app.api.system.read_run_with_stages", side_effect=_capture),
    ):
        resp = client.get("/system/bootstrap-status")

    assert resp.status_code == 200
    assert captured_run_ids == [42]  # NOT some newer-by-DESC id
    assert resp.json()["current_run_id"] == 42
    assert resp.json()["stages"][0]["stage_key"] == "older_stage"


def test_get_status_overview_stale_last_run_id_returns_empty_stages(
    client: TestClient,
) -> None:
    """If ``last_run_id`` points at a deleted run, surface that honestly.

    Don't mask as "no prior run" — operator needs to see the stale
    pointer. ``current_run_id`` stays populated from state; stages is
    empty.
    """
    _install_conn()
    state = BootstrapState(
        status="partial_error",
        last_run_id=42,
        last_completed_at=datetime(2026, 5, 17, tzinfo=UTC),
    )
    with (
        patch("app.api.system.read_state", return_value=state),
        patch("app.api.system.read_run_with_stages", return_value=None),
    ):
        resp = client.get("/system/bootstrap-status")

    assert resp.status_code == 200
    body = resp.json()
    assert body["state_status"] == "partial_error"
    assert body["current_run_id"] == 42  # operator sees the stale pointer
    assert body["stages"] == []
    # Singleton said resettable but no stages → no_failed_stages.
    assert body["retry_blocked_reason"] == "no_failed_stages"


# ---------------------------------------------------------------------------
# Failure posture
# ---------------------------------------------------------------------------


def test_get_status_overview_returns_503_on_db_error(client: TestClient) -> None:
    _install_conn()
    with patch("app.api.system.read_state", side_effect=psycopg.OperationalError("boom")):
        resp = client.get("/system/bootstrap-status")

    assert resp.status_code == 503
    assert resp.json()["detail"] == "bootstrap status unavailable"


def test_router_carries_auth_dependency() -> None:
    """Endpoint inherits the router-level ``require_session_or_service_token``.

    Validating the auth-reject flow live would require either a real
    DB pool (because the dep itself depends on ``get_conn``) or a
    second layered override; both are heavier than the structural
    invariant that every ``/system/*`` route shares one auth dep.
    Assert that invariant directly so future renames don't accidentally
    strip the protection.
    """
    from app.api.auth import require_session_or_service_token
    from app.api.system import router

    dep_callables = {dep.dependency for dep in router.dependencies}
    assert require_session_or_service_token in dep_callables
