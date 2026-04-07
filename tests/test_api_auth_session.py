"""Tests for app.api.auth_session — login / logout / /auth/me + rate limiter.

Mocks are applied at the helper function boundary
(``verify_password``, ``create_session``, ``delete_session``,
``touch_last_login``, ``get_active_session``) so we exercise the route
shape, the cookie wiring, the rate limiter, and the generic-401
discipline without spinning up Postgres or paying real Argon2id costs.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch
from uuid import uuid4

from fastapi.testclient import TestClient

from app.api.auth import require_session_or_service_token
from app.api.auth_session import _RATE_MAX_USERNAME, _rate_limiter
from app.config import settings
from app.db import get_conn
from app.main import app
from app.security.sessions import SessionRow

client = TestClient(app)

_OPERATOR_ID = uuid4()
_USERNAME = "alice"
_PASSWORD = "correct horse battery staple"
_DUMMY_HASH = "$argon2id$v=19$m=65536,t=3,p=4$ZHVtbXk$ZHVtbXloYXNo"


def _mock_conn() -> MagicMock:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cur
    txn = MagicMock()
    txn.__enter__ = MagicMock(return_value=txn)
    txn.__exit__ = MagicMock(return_value=False)
    conn.transaction.return_value = txn
    return conn


def _override_conn() -> Iterator[MagicMock]:
    yield _mock_conn()


def _override_with(conn: MagicMock) -> None:
    """Install ``conn`` as the get_conn dependency override.

    Wraps the conn in a fresh generator on each request so that the
    dependency contract (yield once) is satisfied -- a bare
    ``lambda: iter([conn])`` returns an iterator object directly to the
    route handler instead of yielding the conn.
    """

    def _gen() -> Iterator[MagicMock]:
        yield conn

    app.dependency_overrides[get_conn] = _gen


def _operator_row() -> dict[str, object]:
    return {
        "operator_id": _OPERATOR_ID,
        "username": _USERNAME,
        "password_hash": _DUMMY_HASH,
    }


class _BaseLogin:
    """Mock get_conn + reset rate limiter between tests."""

    def setup_method(self) -> None:
        app.dependency_overrides[get_conn] = _override_conn
        # Reset rate limiter so prior tests do not poison this one.
        _rate_limiter._buckets.clear()  # type: ignore[attr-defined]

    def teardown_method(self) -> None:
        app.dependency_overrides.pop(get_conn, None)
        _rate_limiter._buckets.clear()  # type: ignore[attr-defined]


class TestLogin(_BaseLogin):
    def test_happy_path_sets_cookie_and_returns_operator(self) -> None:
        with (
            patch("app.api.auth_session.verify_password", return_value=True),
            patch(
                "app.api.auth_session.create_session",
                return_value=("session-id-xyz", datetime.now(UTC) + timedelta(hours=12)),
            ),
            patch("app.api.auth_session.touch_last_login"),
        ):
            conn = _mock_conn()
            conn.cursor.return_value.fetchone.return_value = _operator_row()
            _override_with(conn)

            resp = client.post(
                "/auth/login",
                json={"username": _USERNAME, "password": _PASSWORD},
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["operator"]["username"] == _USERNAME
        assert body["operator"]["id"] == str(_OPERATOR_ID)
        # Cookie present
        set_cookie = resp.headers.get("set-cookie", "")
        assert settings.session_cookie_name in set_cookie
        assert "HttpOnly" in set_cookie
        assert "samesite=lax" in set_cookie.lower()

    def test_unknown_user_returns_generic_401(self) -> None:
        conn = _mock_conn()
        conn.cursor.return_value.fetchone.return_value = None
        _override_with(conn)

        resp = client.post(
            "/auth/login",
            json={"username": "ghost", "password": "anything12345"},
        )
        assert resp.status_code == 401
        assert resp.json()["detail"] == "Unauthorized"

    def test_wrong_password_returns_generic_401(self) -> None:
        conn = _mock_conn()
        conn.cursor.return_value.fetchone.return_value = _operator_row()
        _override_with(conn)

        with patch("app.api.auth_session.verify_password", return_value=False):
            resp = client.post(
                "/auth/login",
                json={"username": _USERNAME, "password": "wrong"},
            )
        assert resp.status_code == 401
        assert resp.json()["detail"] == "Unauthorized"

    def test_rate_limit_kicks_in_after_threshold(self) -> None:
        conn = _mock_conn()
        conn.cursor.return_value.fetchone.return_value = _operator_row()
        _override_with(conn)

        with patch("app.api.auth_session.verify_password", return_value=False):
            for _ in range(_RATE_MAX_USERNAME):
                client.post(
                    "/auth/login",
                    json={"username": _USERNAME, "password": "wrong"},
                )
            # Next attempt is over the per-username threshold.
            resp = client.post(
                "/auth/login",
                json={"username": _USERNAME, "password": "wrong"},
            )
        assert resp.status_code == 429


class TestLogout(_BaseLogin):
    def test_logout_clears_cookie_even_without_session(self) -> None:
        resp = client.post("/auth/logout")
        assert resp.status_code == 204
        # Delete-cookie sets a Max-Age=0 / expires-in-the-past header.
        set_cookie = resp.headers.get("set-cookie", "")
        assert settings.session_cookie_name in set_cookie


class TestMe:
    """``/auth/me`` requires a real session -- bare ``require_session``."""

    def test_me_without_cookie_returns_401(self) -> None:
        # No override on require_session here -- conftest only overrides
        # the combined dep, not the bare session dep.
        app.dependency_overrides[get_conn] = _override_conn
        try:
            resp = client.get("/auth/me")
            assert resp.status_code == 401
        finally:
            app.dependency_overrides.pop(get_conn, None)

    def test_me_with_valid_session_returns_operator(self) -> None:
        app.dependency_overrides[get_conn] = _override_conn
        now = datetime.now(UTC)
        try:
            with patch(
                "app.api.auth.get_active_session",
                return_value=SessionRow(
                    session_id="sid",
                    operator_id=_OPERATOR_ID,
                    username=_USERNAME,
                    expires_at=now + timedelta(hours=12),
                    last_seen_at=now,
                ),
            ):
                resp = client.get(
                    "/auth/me",
                    cookies={settings.session_cookie_name: "sid"},
                )
            assert resp.status_code == 200
            body = resp.json()
            assert body["username"] == _USERNAME
            assert body["id"] == str(_OPERATOR_ID)
        finally:
            app.dependency_overrides.pop(get_conn, None)


class TestCombinedDepCookiePath:
    """The combined dep accepts a session cookie too (not only a bearer token)."""

    def setup_method(self) -> None:
        # Pop the conftest no-op to exercise the real combined dep.
        self._prior = app.dependency_overrides.get(require_session_or_service_token)
        app.dependency_overrides.pop(require_session_or_service_token, None)
        app.dependency_overrides[get_conn] = _override_conn

    def teardown_method(self) -> None:
        if self._prior is not None:
            app.dependency_overrides[require_session_or_service_token] = self._prior
        else:
            app.dependency_overrides.pop(require_session_or_service_token, None)
        app.dependency_overrides.pop(get_conn, None)

    def test_valid_cookie_grants_access(self) -> None:
        from unittest.mock import patch

        now = datetime.now(UTC)
        with (
            patch(
                "app.api.auth.get_active_session",
                return_value=SessionRow(
                    session_id="sid",
                    operator_id=_OPERATOR_ID,
                    username=_USERNAME,
                    expires_at=now + timedelta(hours=12),
                    last_seen_at=now,
                ),
            ),
            patch(
                "app.api.config.deactivate_kill_switch",
                return_value={
                    "is_active": False,
                    "activated_at": None,
                    "activated_by": None,
                    "reason": None,
                },
            ),
        ):
            resp = client.post(
                "/kill-switch",
                json={"active": False, "reason": "test", "activated_by": "ci"},
                cookies={settings.session_cookie_name: "sid"},
            )
        assert resp.status_code == 200
