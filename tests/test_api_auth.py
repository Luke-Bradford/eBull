"""Tests for app.api.auth — bearer token enforcement on protected endpoints.

These tests bypass the conftest-level ``require_auth`` no-op override to
exercise the real dependency. Each test clears the override at the top and
relies on ``teardown_method`` to restore it for the rest of the suite.
"""

from __future__ import annotations

from collections.abc import Iterator
from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from app.api.auth import require_auth
from app.config import settings
from app.db import get_conn
from app.main import app

client = TestClient(app)

_VALID_KEY = "test-operator-key-with-32-chars!"  # 32 chars, meets min length


def _mock_conn() -> MagicMock:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cur
    return conn


def _override_conn() -> Iterator[MagicMock]:
    yield _mock_conn()


class _AuthTestBase:
    """Shared setup: enable real auth and provide a stub DB connection.

    The conftest installs a no-op ``require_auth`` override for the rest of
    the suite. We pop it here so the real dependency runs, then restore it
    in teardown so other tests are unaffected.
    """

    def setup_method(self) -> None:
        self._real_key = settings.api_key
        settings.api_key = _VALID_KEY
        # Capture the conftest no-op override at setup time so teardown can
        # restore it deterministically without re-importing. Re-importing in
        # teardown could silently install None if the import path changed.
        # If conftest hasn't installed an override (e.g. running this file in
        # isolation in some odd way), the captured value is None and teardown
        # will simply pop the key — the test still exercises the real
        # require_auth dependency correctly.
        self._prior_auth_override = app.dependency_overrides.get(require_auth)
        app.dependency_overrides.pop(require_auth, None)
        app.dependency_overrides[get_conn] = _override_conn

    def teardown_method(self) -> None:
        settings.api_key = self._real_key
        prior = self._prior_auth_override
        if prior is not None:
            app.dependency_overrides[require_auth] = prior
        else:
            app.dependency_overrides.pop(require_auth, None)
        app.dependency_overrides.pop(get_conn, None)


class TestRequireAuth(_AuthTestBase):
    """The require_auth dependency rejects bad / missing tokens uniformly."""

    def test_missing_authorization_header_returns_401(self) -> None:
        resp = client.post("/kill-switch", json={"active": False})
        assert resp.status_code == 401
        assert resp.json()["detail"] == "Unauthorized"
        assert resp.headers.get("WWW-Authenticate") == "Bearer"

    def test_wrong_token_returns_401(self) -> None:
        resp = client.post(
            "/kill-switch",
            json={"active": False},
            headers={"Authorization": "Bearer not-the-real-key"},
        )
        assert resp.status_code == 401
        assert resp.json()["detail"] == "Unauthorized"

    def test_wrong_scheme_returns_401(self) -> None:
        resp = client.post(
            "/kill-switch",
            json={"active": False},
            headers={"Authorization": f"Basic {_VALID_KEY}"},
        )
        assert resp.status_code == 401

    def test_correct_token_is_accepted(self) -> None:
        # The kill switch route calls deactivate_kill_switch on the conn.
        # We don't care about its result here — we just want to prove auth
        # let the request through. Patch the service to a no-op.
        from unittest.mock import patch

        with patch("app.api.config.deactivate_kill_switch"):
            resp = client.post(
                "/kill-switch",
                json={"active": False, "reason": "test", "activated_by": "ci"},
                headers={"Authorization": f"Bearer {_VALID_KEY}"},
            )
        assert resp.status_code == 200

    def test_unset_api_key_fails_closed(self) -> None:
        """When settings.api_key is None, every protected request is rejected.

        We do not treat unset config as 'auth disabled' — a misconfigured
        deploy must not silently leave the kill switch open.
        """
        settings.api_key = None
        resp = client.post(
            "/kill-switch",
            json={"active": False},
            headers={"Authorization": f"Bearer {_VALID_KEY}"},
        )
        assert resp.status_code == 401

    def test_protected_routers_reject_unauthenticated_reads(self) -> None:
        """portfolio / recommendations / audit are protected at router level."""
        for path in ("/portfolio", "/recommendations", "/audit"):
            resp = client.get(path)
            assert resp.status_code == 401, f"{path} should require auth"

    def test_health_data_requires_auth(self) -> None:
        resp = client.get("/health/data")
        assert resp.status_code == 401


class TestPublicEndpointsRemainOpen:
    """Regression: /health and /health/db must still work without a token.

    This test does NOT extend ``_AuthTestBase``: it leaves the conftest no-op
    in place AND clears any per-test get_conn override, so we exercise the
    public endpoints exactly as an unauthenticated caller would.
    """

    def setup_method(self) -> None:
        # Provide a stub DB connection for /health/db so it doesn't try to
        # reach a real pool. /health does not touch the DB.
        def _override() -> Iterator[MagicMock]:
            conn = _mock_conn()
            conn.execute.return_value = []  # /health/db iterates pg_tables result
            yield conn

        app.dependency_overrides[get_conn] = _override

    def teardown_method(self) -> None:
        app.dependency_overrides.pop(get_conn, None)

    def test_health_is_public(self) -> None:
        resp = client.get("/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"
        # Sanity: never leak the operator API key from /health.
        assert "api_key" not in body

    def test_health_db_is_public(self) -> None:
        from unittest.mock import patch

        with patch("app.main.migration_status", return_value=[]):
            resp = client.get("/health/db")
        assert resp.status_code == 200
        assert resp.json()["db_reachable"] is True
