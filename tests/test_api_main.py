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
# TestHealthDb — proves migration_status receives pooled conn
# ---------------------------------------------------------------------------


class TestHealthDb:
    """GET /health/db — public liveness probe.

    Per #240, the response body MUST contain only ``db_reachable``.
    Table names, migration history, and raw exception text were
    removed because the endpoint is unauthenticated and an attacker
    can use any of those to fingerprint schema / migration / infra
    failure modes.
    """

    def teardown_method(self) -> None:
        _cleanup()

    def test_db_reachable_true_only(self) -> None:
        conn = _mock_conn()
        _setup(conn)

        migrations: list[dict[str, str]] = [
            {"file": "001_init.sql", "status": "applied", "applied_at": "2026-01-01T00:00:00"}
        ]

        with patch("app.main.migration_status", return_value=migrations) as mock_status:
            resp = client.get("/health/db")

        assert resp.status_code == 200
        body = resp.json()
        assert body == {"db_reachable": True}

        # migration_status was called with the pooled connection
        # (probe still verifies the bootstrap table exists).
        mock_status.assert_called_once_with(conn)

    def test_db_unreachable_does_not_leak_exception_text(self) -> None:
        """When ``migration_status`` raises, the response is the
        binary ``db_reachable: false`` only — the original exception
        message must NEVER appear in the response body (#240).
        """
        conn = _mock_conn()
        _setup(conn)

        marker = "internal-leak-marker-XYZ-pg_connect-failed"
        with patch("app.main.migration_status", side_effect=RuntimeError(marker)):
            resp = client.get("/health/db")

        assert resp.status_code == 200
        body = resp.json()
        assert body == {"db_reachable": False}
        assert marker not in resp.text

    def test_response_does_not_include_legacy_fields(self) -> None:
        """Belt-and-braces: even on the success path, ``tables`` and
        ``migrations`` keys must be absent so a future regression
        that accidentally re-adds them trips this test.
        """
        conn = _mock_conn()
        _setup(conn)
        with patch("app.main.migration_status", return_value=[]):
            resp = client.get("/health/db")
        body = resp.json()
        assert "tables" not in body
        assert "migrations" not in body
        assert "db_error" not in body


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

        with patch(
            "app.api.config.activate_kill_switch",
            return_value={
                "is_active": True,
                "activated_at": _NOW,
                "activated_by": "ci",
                "reason": "test",
            },
        ) as mock_activate:
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
