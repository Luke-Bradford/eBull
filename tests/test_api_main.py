"""
Tests for existing DB-backed endpoints in app.main after connection pooling.

Proves that the pooled ``get_conn`` dependency is used correctly by at least
one migrated endpoint.  Service logic is not re-tested here — that is covered
by the per-service test files.

Test strategy:
  Mock DB via FastAPI dependency override (same pattern as test_api_instruments).
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from app.db import get_conn
from app.main import app
from app.services.ops_monitor import SystemHealth

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 4, 6, 12, 0, 0, tzinfo=UTC)


def _mock_conn() -> MagicMock:
    """Build a minimal mock psycopg.Connection."""
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cur
    return conn


def _fallback_conn() -> Iterator[MagicMock]:
    yield _mock_conn()


def _setup(conn: MagicMock) -> None:
    def _override() -> Iterator[MagicMock]:
        yield conn

    app.dependency_overrides[get_conn] = _override


def _cleanup() -> None:
    app.dependency_overrides[get_conn] = _fallback_conn


# Default override so tests that don't set up a specific conn don't crash
# on missing app.state.db_pool (the pool isn't created without lifespan).
app.dependency_overrides.setdefault(get_conn, _fallback_conn)

client = TestClient(app)


# ---------------------------------------------------------------------------
# TestHealthData — proves pooled access for an existing main.py endpoint
# ---------------------------------------------------------------------------


class TestHealthData:
    """GET /health/data — system health via pooled connection."""

    def teardown_method(self) -> None:
        _cleanup()

    def test_returns_health_report_via_pooled_conn(self) -> None:
        """The endpoint receives its connection from get_conn, not raw psycopg.connect."""
        conn = _mock_conn()
        _setup(conn)

        report = SystemHealth(checked_at=_NOW, kill_switch_active=False, kill_switch_detail="")

        with patch("app.main.get_system_health", return_value=report) as mock_health:
            resp = client.get("/health/data")

        assert resp.status_code == 200
        body = resp.json()
        assert body["kill_switch"]["active"] is False
        assert body["layers"] == []
        assert body["jobs"] == []

        # Verify get_system_health was called with the injected mock connection
        mock_health.assert_called_once()
        call_conn = mock_health.call_args[0][0]
        assert call_conn is conn

    def test_service_error_returns_503(self) -> None:
        conn = _mock_conn()
        _setup(conn)

        with patch("app.main.get_system_health", side_effect=RuntimeError("db down")):
            resp = client.get("/health/data")

        assert resp.status_code == 503
        assert "db down" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# TestHealthDb — proves migration_status receives pooled conn
# ---------------------------------------------------------------------------


class TestHealthDb:
    """GET /health/db — migration status via pooled connection."""

    def teardown_method(self) -> None:
        _cleanup()

    def test_returns_db_health_via_pooled_conn(self) -> None:
        conn = _mock_conn()
        _setup(conn)

        migrations: list[dict[str, str]] = [
            {"file": "001_init.sql", "status": "applied", "applied_at": "2026-01-01T00:00:00"}
        ]

        # conn.execute returns rows for the pg_tables query
        conn.execute.return_value = [("instruments",), ("coverage",)]

        with patch("app.main.migration_status", return_value=migrations) as mock_status:
            resp = client.get("/health/db")

        assert resp.status_code == 200
        body = resp.json()
        assert body["db_reachable"] is True
        assert body["tables"] == ["instruments", "coverage"]
        assert len(body["migrations"]) == 1

        # migration_status was called with the pooled connection
        mock_status.assert_called_once_with(conn)


class TestKillSwitch:
    """POST /kill-switch (deprecated alias) — delegates to config router.

    The canonical path is POST /config/kill-switch (tested in
    test_api_config.py).  This test only proves that the deprecated alias
    is still wired and that the pooled connection reaches the service layer.
    """

    def teardown_method(self) -> None:
        _cleanup()

    def test_activate_via_pooled_conn(self) -> None:
        conn = _mock_conn()
        _setup(conn)

        with (
            patch("app.api.config.activate_kill_switch") as mock_activate,
            patch(
                "app.api.config.get_kill_switch_status",
                return_value={
                    "is_active": True,
                    "activated_at": _NOW,
                    "activated_by": "ci",
                    "reason": "test",
                },
            ),
        ):
            resp = client.post(
                "/kill-switch",
                json={"active": True, "reason": "test", "activated_by": "ci"},
            )

        assert resp.status_code == 200
        assert resp.json()["active"] is True

        # Verify the pooled connection was passed to the service (first positional arg)
        mock_activate.assert_called_once()
        call_conn = mock_activate.call_args[0][0]
        assert call_conn is conn
