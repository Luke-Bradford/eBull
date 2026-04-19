"""Tests for app.api.auth — service-token + combined-dependency enforcement.

These tests bypass the conftest-level ``require_session_or_service_token``
no-op override to exercise the real combined dependency. Each test
captures the prior override in ``setup_method`` and restores it in
``teardown_method`` (prevention-log #81 -- never re-fetch from the source).

Coverage:
  * service-token path: missing header / wrong token / wrong scheme /
    unset server-side token / correct token
  * router-level enforcement on portfolio / recommendations / audit
  * /health and /health/db remain public

Browser-session login / logout / /auth/me coverage lives in
``test_api_auth_session.py``.
"""

from __future__ import annotations

from collections.abc import Iterator
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from app.api.auth import require_session_or_service_token
from app.config import settings
from app.db import get_conn
from app.main import app
from app.services.sync_orchestrator.layer_types import LayerState

client = TestClient(app)

_VALID_TOKEN = "test-operator-token-with-32-chars"  # 32 chars, meets min length


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

    The conftest installs a no-op ``require_session_or_service_token``
    override for the rest of the suite. We pop it here so the real
    dependency runs, then restore it in teardown so other tests are
    unaffected.
    """

    def setup_method(self) -> None:
        self._real_token = settings.service_token
        settings.service_token = _VALID_TOKEN
        # Capture-restore (prevention-log #81): never re-fetch in teardown.
        self._prior_override = app.dependency_overrides.get(require_session_or_service_token)
        app.dependency_overrides.pop(require_session_or_service_token, None)
        app.dependency_overrides[get_conn] = _override_conn

    def teardown_method(self) -> None:
        settings.service_token = self._real_token
        if self._prior_override is not None:
            app.dependency_overrides[require_session_or_service_token] = self._prior_override
        else:
            app.dependency_overrides.pop(require_session_or_service_token, None)
        app.dependency_overrides.pop(get_conn, None)


class TestServiceTokenPath(_AuthTestBase):
    """The combined dep accepts a valid bearer token and rejects bad ones."""

    def test_missing_authorization_header_returns_401(self) -> None:
        resp = client.post("/kill-switch", json={"active": False})
        assert resp.status_code == 401
        assert resp.json()["detail"] == "Unauthorized"
        assert resp.headers.get("WWW-Authenticate") == "Bearer"

    def test_wrong_token_returns_401(self) -> None:
        resp = client.post(
            "/kill-switch",
            json={"active": False},
            headers={"Authorization": "Bearer not-the-real-token-xx-xxxxxxxxx"},
        )
        assert resp.status_code == 401
        assert resp.json()["detail"] == "Unauthorized"

    def test_wrong_scheme_returns_401(self) -> None:
        resp = client.post(
            "/kill-switch",
            json={"active": False},
            headers={"Authorization": f"Basic {_VALID_TOKEN}"},
        )
        # HTTPBearer surfaces a non-Bearer Authorization header as None
        # credentials, which the combined dep then routes to the cookie
        # path -- and there is no cookie, so 401.
        assert resp.status_code == 401

    def test_correct_token_is_accepted(self) -> None:
        from unittest.mock import patch

        with patch(
            "app.api.config.deactivate_kill_switch",
            return_value={
                "is_active": False,
                "activated_at": None,
                "activated_by": None,
                "reason": None,
            },
        ):
            resp = client.post(
                "/kill-switch",
                json={"active": False, "reason": "test", "activated_by": "ci"},
                headers={"Authorization": f"Bearer {_VALID_TOKEN}"},
            )
        assert resp.status_code == 200

    def test_unset_service_token_fails_closed(self) -> None:
        """When settings.service_token is None, every protected request is rejected."""
        settings.service_token = None
        resp = client.post(
            "/kill-switch",
            json={"active": False},
            headers={"Authorization": f"Bearer {_VALID_TOKEN}"},
        )
        assert resp.status_code == 401

    def test_protected_routers_reject_unauthenticated_reads(self) -> None:
        """portfolio / recommendations / audit are protected at router level."""
        for path in ("/portfolio", "/recommendations", "/audit"):
            resp = client.get(path)
            assert resp.status_code == 401, f"{path} should require auth"

    def test_protected_endpoints_require_auth(self) -> None:
        # /health/data retired in A.5 chunk 3 (#342). /system/status is
        # the protected canary — same auth dependency.
        resp = client.get("/system/status")
        assert resp.status_code == 401


class TestPublicEndpointsRemainOpen:
    """Regression: /health and /health/db must still work without a token.

    This test does NOT extend ``_AuthTestBase``: it leaves the conftest no-op
    in place AND clears any per-test get_conn override, so we exercise the
    public endpoints exactly as an unauthenticated caller would.
    """

    def setup_method(self) -> None:
        def _override() -> Iterator[MagicMock]:
            conn = _mock_conn()
            conn.execute.return_value = []  # /health/db iterates pg_tables result
            yield conn

        app.dependency_overrides[get_conn] = _override

    def teardown_method(self) -> None:
        app.dependency_overrides.pop(get_conn, None)

    def test_health_is_public(self) -> None:
        # /health acquires its own connection via app.state.db_pool
        # (chunk 7). Stub the pool to yield a mock conn so the handler's
        # inline checkout succeeds; compute_layer_states_from_db is
        # patched separately.
        from contextlib import contextmanager

        @contextmanager
        def _stub_conn_cm():
            yield _mock_conn()

        class _StubPool:
            def connection(self):
                return _stub_conn_cm()

        from app.main import app as _app

        original = getattr(_app.state, "db_pool", None)
        _app.state.db_pool = _StubPool()
        try:
            all_healthy = {"candles": LayerState.HEALTHY}
            with patch(
                "app.main.compute_layer_states_from_db",
                return_value=all_healthy,
            ):
                resp = client.get("/health")
        finally:
            if original is None:
                try:
                    delattr(_app.state, "db_pool")
                except AttributeError:
                    pass
            else:
                _app.state.db_pool = original
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"
        # Sanity: never leak the operator service token from /health.
        assert "service_token" not in body
        assert "api_key" not in body

    def test_health_db_is_public(self) -> None:
        from unittest.mock import patch

        with patch("app.main.migration_status", return_value=[]):
            resp = client.get("/health/db")
        assert resp.status_code == 200
        assert resp.json()["db_reachable"] is True
