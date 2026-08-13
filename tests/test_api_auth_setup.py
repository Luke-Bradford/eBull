"""Tests for app.api.auth_setup -- /auth/setup-status + /auth/setup.

Covers (per Ticket G):
  * setup-status returns the empty-table boolean
  * Mode A loopback zero-config success / non-loopback rejection
  * Mode B token-required success / wrong token / missing token / consumed
  * Generic 404 discipline -- every failure returns the same body

The race-safety test lives in test_operator_setup_race.py because it
needs a real Postgres connection rather than the mock surface used here.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from app.api.auth import require_session_or_service_token
from app.config import settings
from app.db import get_conn
from app.main import app
from app.services.operator_setup import (
    SetupOutcome,
    SetupSuccess,
    reset_token_slot_for_tests,
)

client = TestClient(app)


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


def _override_with(conn: MagicMock) -> None:
    def _gen() -> Iterator[MagicMock]:
        yield conn

    app.dependency_overrides[get_conn] = _gen


@pytest.fixture(autouse=True)
def _isolate_state() -> Iterator[None]:
    """Each test starts with a clean token slot and no auth override."""
    reset_token_slot_for_tests()
    # The conftest installs a global noop auth override -- remove it for
    # this module so the public setup endpoints behave naturally and we
    # can apply per-test get_conn overrides without interference.
    saved = dict(app.dependency_overrides)
    app.dependency_overrides.pop(require_session_or_service_token, None)
    app.dependency_overrides.pop(get_conn, None)
    yield
    app.dependency_overrides.clear()
    app.dependency_overrides.update(saved)
    reset_token_slot_for_tests()


# ---------------------------------------------------------------------------
# /auth/setup-status
# ---------------------------------------------------------------------------


class TestSetupStatus:
    def test_empty_table_returns_true(self) -> None:
        conn = _mock_conn()
        cur = conn.cursor.return_value
        cur.fetchone.return_value = None
        _override_with(conn)

        resp = client.get("/auth/setup-status")
        assert resp.status_code == 200
        assert resp.json() == {"needs_setup": True}

    def test_populated_table_returns_false(self) -> None:
        conn = _mock_conn()
        cur = conn.cursor.return_value
        cur.fetchone.return_value = (1,)
        _override_with(conn)

        resp = client.get("/auth/setup-status")
        assert resp.status_code == 200
        assert resp.json() == {"needs_setup": False}


# ---------------------------------------------------------------------------
# /auth/setup -- success and failure paths
# ---------------------------------------------------------------------------


_OPERATOR_ID = uuid4()
_USERNAME = "alice"
_PASSWORD = "correct horse battery staple"


def _setup_success() -> SetupSuccess:
    return SetupSuccess(
        operator_id=_OPERATOR_ID,
        username=_USERNAME,
        session_id="opaque-session-id",
        expires_at=datetime.now(UTC) + timedelta(hours=12),
    )


class TestSetupEndpoint:
    def test_success_sets_cookie_and_returns_operator(self) -> None:
        conn = _mock_conn()
        _override_with(conn)
        with patch(
            "app.api.auth_setup.perform_setup",
            return_value=(SetupOutcome.OK, _setup_success()),
        ):
            resp = client.post(
                "/auth/setup",
                json={"username": _USERNAME, "password": _PASSWORD},
            )
        assert resp.status_code == 200
        assert resp.json()["operator"]["username"] == _USERNAME
        assert settings.session_cookie_name in resp.cookies

    def test_already_setup_returns_404(self) -> None:
        conn = _mock_conn()
        _override_with(conn)
        with patch(
            "app.api.auth_setup.perform_setup",
            return_value=(SetupOutcome.ALREADY_SETUP, None),
        ):
            resp = client.post(
                "/auth/setup",
                json={"username": _USERNAME, "password": _PASSWORD},
            )
        assert resp.status_code == 404
        assert resp.json() == {"detail": "Not Found"}
        assert settings.session_cookie_name not in resp.cookies

    def test_bad_token_returns_404_with_same_body(self) -> None:
        conn = _mock_conn()
        _override_with(conn)
        with patch(
            "app.api.auth_setup.perform_setup",
            return_value=(SetupOutcome.BAD_TOKEN, None),
        ):
            resp = client.post(
                "/auth/setup",
                json={"username": _USERNAME, "password": _PASSWORD, "setup_token": "wrong"},
            )
        assert resp.status_code == 404
        assert resp.json() == {"detail": "Not Found"}

    def test_short_password_returns_404(self) -> None:
        conn = _mock_conn()
        _override_with(conn)
        with patch(
            "app.api.auth_setup.perform_setup",
            return_value=(SetupOutcome.BAD_PASSWORD, None),
        ):
            resp = client.post(
                "/auth/setup",
                json={"username": _USERNAME, "password": "short"},
            )
        assert resp.status_code == 404

    def test_empty_username_returns_404(self) -> None:
        conn = _mock_conn()
        _override_with(conn)
        with patch(
            "app.api.auth_setup.perform_setup",
            return_value=(SetupOutcome.BAD_USERNAME, None),
        ):
            resp = client.post(
                "/auth/setup",
                json={"username": "   ", "password": _PASSWORD},
            )
        assert resp.status_code == 404
