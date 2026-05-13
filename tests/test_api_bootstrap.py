"""Tests for /system/bootstrap/* API.

Mock-DB tests against the FastAPI app: covers status snapshot,
single-flight enforcement on /run, retry-failed dependency walk,
and mark-complete running-state guard. Real-DB end-to-end coverage
of the orchestrator service lives in ``test_bootstrap_orchestrator.py``.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture(autouse=True)
def cleanup() -> Iterator[None]:
    yield
    from app.db import get_conn

    app.dependency_overrides.pop(get_conn, None)


def _install_conn() -> MagicMock:
    """Install a mock connection that the API endpoints will use."""
    conn = MagicMock()

    def _dep() -> Iterator[MagicMock]:
        yield conn

    from app.db import get_conn

    app.dependency_overrides[get_conn] = _dep
    return conn


# ---------------------------------------------------------------------------
# GET /system/bootstrap/status
# ---------------------------------------------------------------------------


def test_get_status_with_no_runs(client: TestClient) -> None:
    """Fresh install: status='pending', no current_run_id, no stages."""
    from app.services.bootstrap_state import BootstrapState

    _install_conn()
    with (
        patch(
            "app.api.bootstrap.read_state",
            return_value=BootstrapState(status="pending", last_run_id=None, last_completed_at=None),
        ),
        patch("app.api.bootstrap.read_latest_run_with_stages", return_value=None),
    ):
        resp = client.get("/system/bootstrap/status")

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "pending"
    assert body["current_run_id"] is None
    assert body["last_completed_at"] is None
    assert body["stages"] == []


def test_get_status_with_running_run(client: TestClient) -> None:
    """Running state with one stage in flight."""
    from app.services.bootstrap_state import BootstrapState, RunSnapshot, StageRow

    _install_conn()

    snap = RunSnapshot(
        run_id=42,
        run_status="running",
        triggered_at=datetime(2026, 5, 7, 1, 0, tzinfo=UTC),
        completed_at=None,
        stages=(
            StageRow(
                id=1,
                bootstrap_run_id=42,
                stage_key="universe_sync",
                stage_order=1,
                lane="init",
                job_name="nightly_universe_sync",
                status="success",
                started_at=datetime(2026, 5, 7, 1, 0, tzinfo=UTC),
                completed_at=datetime(2026, 5, 7, 1, 0, tzinfo=UTC),
                rows_processed=1500,
                expected_units=None,
                units_done=None,
                last_error=None,
                attempt_count=1,
            ),
            StageRow(
                id=2,
                bootstrap_run_id=42,
                stage_key="cusip_universe_backfill",
                stage_order=3,
                lane="sec",
                job_name="cusip_universe_backfill",
                status="running",
                started_at=datetime(2026, 5, 7, 1, 1, tzinfo=UTC),
                completed_at=None,
                rows_processed=None,
                expected_units=9000,
                units_done=4500,
                last_error=None,
                attempt_count=1,
            ),
        ),
    )
    with (
        patch(
            "app.api.bootstrap.read_state",
            return_value=BootstrapState(status="running", last_run_id=42, last_completed_at=None),
        ),
        patch("app.api.bootstrap.read_latest_run_with_stages", return_value=snap),
    ):
        resp = client.get("/system/bootstrap/status")

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "running"
    assert body["current_run_id"] == 42
    assert len(body["stages"]) == 2
    assert body["stages"][0]["status"] == "success"
    assert body["stages"][0]["rows_processed"] == 1500
    assert body["stages"][1]["status"] == "running"
    assert body["stages"][1]["expected_units"] == 9000
    assert body["stages"][1]["units_done"] == 4500


# ---------------------------------------------------------------------------
# POST /system/bootstrap/run
# ---------------------------------------------------------------------------


def test_run_creates_new_run_and_publishes_queue_row(client: TestClient) -> None:
    _install_conn()
    with (
        patch("app.api.bootstrap.start_run", return_value=42) as start_mock,
        patch("app.api.bootstrap.publish_manual_job_request_with_conn", return_value=99) as publish_mock,
    ):
        resp = client.post("/system/bootstrap/run")

    assert resp.status_code == 202
    body = resp.json()
    assert body["run_id"] == 42
    assert body["request_id"] == 99
    start_mock.assert_called_once()
    publish_mock.assert_called_once()
    # The publish call must use the orchestrator job name (positional arg 1
    # after the conn passed at arg 0 — #1139 shared-conn signature).
    publish_args = publish_mock.call_args
    assert publish_args.args[1] == "bootstrap_orchestrator"


def test_run_returns_409_when_already_running(client: TestClient) -> None:
    from app.services.bootstrap_state import BootstrapAlreadyRunning

    _install_conn()
    with (
        patch("app.api.bootstrap.start_run", side_effect=BootstrapAlreadyRunning(run_id=7)),
        patch("app.api.bootstrap.publish_manual_job_request_with_conn") as publish_mock,
    ):
        resp = client.post("/system/bootstrap/run")

    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert detail["error"] == "bootstrap_already_running"
    assert detail["current_run_id"] == 7
    publish_mock.assert_not_called()


def test_run_returns_503_on_publish_operational_error(client: TestClient) -> None:
    """#1139 — transient OperationalError from publish maps to 503.

    The outer ``with conn.transaction():`` sees the HTTPException, rolls
    back (verified end-to-end in the integration test), and surfaces
    503 ``queue_publish_failed`` so the FE can render a retry toast.
    """
    import psycopg

    _install_conn()
    with (
        patch("app.api.bootstrap.start_run", return_value=42),
        patch(
            "app.api.bootstrap.publish_manual_job_request_with_conn",
            side_effect=psycopg.OperationalError("connection wedged"),
        ),
    ):
        resp = client.post("/system/bootstrap/run")

    assert resp.status_code == 503
    assert resp.json()["detail"]["error"] == "queue_publish_failed"


# ---------------------------------------------------------------------------
# POST /system/bootstrap/retry-failed
# ---------------------------------------------------------------------------


def test_retry_failed_resets_and_publishes(client: TestClient) -> None:
    _install_conn()
    with (
        patch(
            "app.api.bootstrap.reset_failed_stages_for_retry",
            return_value=(42, 3),
        ) as reset_mock,
        patch("app.api.bootstrap.publish_manual_job_request_with_conn", return_value=100) as publish_mock,
    ):
        resp = client.post("/system/bootstrap/retry-failed")

    assert resp.status_code == 202
    assert resp.json() == {"run_id": 42, "request_id": 100}
    # No-arg call shape (#1139 — helper is sole gate, derives run_id under lock).
    reset_mock.assert_called_once_with(_anyconn())
    publish_mock.assert_called_once()


def test_retry_failed_returns_409_while_running(client: TestClient) -> None:
    from app.services.bootstrap_state import BootstrapAlreadyRunning

    _install_conn()
    with patch(
        "app.api.bootstrap.reset_failed_stages_for_retry",
        side_effect=BootstrapAlreadyRunning(run_id=7),
    ):
        resp = client.post("/system/bootstrap/retry-failed")
    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert detail["error"] == "bootstrap_running"
    assert detail["current_run_id"] == 7


def test_retry_failed_returns_404_with_no_prior_run(client: TestClient) -> None:
    from app.services.bootstrap_state import BootstrapNoPriorRun

    _install_conn()
    with patch(
        "app.api.bootstrap.reset_failed_stages_for_retry",
        side_effect=BootstrapNoPriorRun(),
    ):
        resp = client.post("/system/bootstrap/retry-failed")
    assert resp.status_code == 404


def test_retry_failed_returns_409_when_not_resettable(client: TestClient) -> None:
    """#1139 — pending/complete/etc surface as 409 bootstrap_not_resettable.

    Preserves the operator-visible distinction between "no prior run"
    (404) and "wrong status for retry" (409).
    """
    from app.services.bootstrap_state import BootstrapNotResettable

    _install_conn()
    with patch(
        "app.api.bootstrap.reset_failed_stages_for_retry",
        side_effect=BootstrapNotResettable(status="complete"),
    ):
        resp = client.post("/system/bootstrap/retry-failed")
    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert detail["error"] == "bootstrap_not_resettable"
    assert detail["current_status"] == "complete"


def test_retry_failed_returns_404_when_no_failed_stages(client: TestClient) -> None:
    _install_conn()
    with (
        patch(
            "app.api.bootstrap.reset_failed_stages_for_retry",
            return_value=(42, 0),
        ),
        patch("app.api.bootstrap.publish_manual_job_request_with_conn") as publish_mock,
    ):
        resp = client.post("/system/bootstrap/retry-failed")
    assert resp.status_code == 404
    publish_mock.assert_not_called()


def test_retry_failed_returns_503_on_publish_operational_error(client: TestClient) -> None:
    """#1139 — publish failure on retry path rolls back the reset."""
    import psycopg

    _install_conn()
    with (
        patch(
            "app.api.bootstrap.reset_failed_stages_for_retry",
            return_value=(42, 3),
        ),
        patch(
            "app.api.bootstrap.publish_manual_job_request_with_conn",
            side_effect=psycopg.OperationalError("connection wedged"),
        ),
    ):
        resp = client.post("/system/bootstrap/retry-failed")

    assert resp.status_code == 503
    assert resp.json()["detail"]["error"] == "queue_publish_failed"


# ---------------------------------------------------------------------------
# POST /system/bootstrap/mark-complete
# ---------------------------------------------------------------------------


def test_mark_complete_flips_state_when_idle(client: TestClient) -> None:
    from app.services.bootstrap_state import BootstrapState

    _install_conn()
    with (
        patch(
            "app.api.bootstrap.read_state",
            return_value=BootstrapState(status="partial_error", last_run_id=42, last_completed_at=None),
        ),
        patch("app.api.bootstrap.force_mark_complete") as force_mock,
    ):
        resp = client.post("/system/bootstrap/mark-complete")

    assert resp.status_code == 200
    assert resp.json() == {"status": "complete"}
    force_mock.assert_called_once()


def test_mark_complete_returns_409_while_running(client: TestClient) -> None:
    """Spec §"running-state guard": releasing the gate while
    orchestrator threads are still mutating data would let nightly
    jobs run against half-populated tables.
    """
    from app.services.bootstrap_state import BootstrapState

    _install_conn()
    with (
        patch(
            "app.api.bootstrap.read_state",
            return_value=BootstrapState(status="running", last_run_id=7, last_completed_at=None),
        ),
        patch("app.api.bootstrap.force_mark_complete") as force_mock,
    ):
        resp = client.post("/system/bootstrap/mark-complete")

    assert resp.status_code == 409
    assert resp.json()["detail"]["error"] == "bootstrap_running"
    force_mock.assert_not_called()


# ---------------------------------------------------------------------------
# POST /system/bootstrap/cancel
# ---------------------------------------------------------------------------


def test_cancel_returns_202_when_running(client: TestClient) -> None:
    _install_conn()
    with patch("app.api.bootstrap.cancel_run", return_value=42) as cancel_mock:
        resp = client.post("/system/bootstrap/cancel")

    assert resp.status_code == 202
    assert resp.json() == {"run_id": 42, "status": "cancel_requested"}
    cancel_mock.assert_called_once()


def test_cancel_returns_409_when_not_running(client: TestClient) -> None:
    from app.services.bootstrap_state import BootstrapNotRunning

    _install_conn()
    with patch(
        "app.api.bootstrap.cancel_run",
        side_effect=BootstrapNotRunning("nothing running"),
    ):
        resp = client.post("/system/bootstrap/cancel")

    assert resp.status_code == 409
    assert resp.json()["detail"]["error"] == "no_active_run"


def test_cancel_returns_409_when_double_clicked(client: TestClient) -> None:
    from app.services.process_stop import StopAlreadyPendingError

    _install_conn()
    with patch(
        "app.api.bootstrap.cancel_run",
        side_effect=StopAlreadyPendingError("already pending"),
    ):
        resp = client.post("/system/bootstrap/cancel")

    assert resp.status_code == 409
    assert resp.json()["detail"]["error"] == "stop_already_pending"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _AnyConn:
    """Sentinel matcher for the conn argument that FastAPI passes via Depends."""

    def __eq__(self, other: object) -> bool:
        return True

    def __ne__(self, other: object) -> bool:
        return False

    def __hash__(self) -> int:
        return 0


def _anyconn() -> _AnyConn:
    return _AnyConn()
