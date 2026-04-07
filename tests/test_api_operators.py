"""Tests for app.api.operators -- list / create / delete.

Mocks are applied at the service-function boundary
(``list_operators``, ``create_operator``, ``delete_operator``) so we
exercise the route shape, the auth wiring, and the cookie-clear on
self-delete without spinning up Postgres.

The audit-row + FK-cascade behaviour is owned by the service layer; it
is covered by service-level tests, not here.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from app.api.auth import require_session, require_session_or_service_token
from app.config import settings
from app.db import get_conn
from app.main import app
from app.security.sessions import SessionRow
from app.services.operators import (
    CreateOutcome,
    DeleteOutcome,
    OperatorRow,
)

client = TestClient(app)

_SELF_ID = uuid4()
_OTHER_ID = uuid4()
_USERNAME = "alice"
_SESSION_ID = "opaque-session-id"


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


def _session_row() -> SessionRow:
    now = datetime.now(UTC)
    return SessionRow(
        session_id=_SESSION_ID,
        operator_id=_SELF_ID,
        username=_USERNAME,
        expires_at=now + timedelta(hours=12),
        last_seen_at=now,
    )


def _operator_row(operator_id: UUID, username: str) -> OperatorRow:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return OperatorRow(
        operator_id=operator_id,
        username=username,
        created_at=now,
        last_login_at=None,
    )


@pytest.fixture(autouse=True)
def _isolate_state() -> Iterator[None]:
    saved = dict(app.dependency_overrides)
    app.dependency_overrides.pop(require_session_or_service_token, None)
    app.dependency_overrides.pop(get_conn, None)

    conn = _mock_conn()

    def _gen() -> Iterator[MagicMock]:
        yield conn

    app.dependency_overrides[get_conn] = _gen
    app.dependency_overrides[require_session] = _session_row

    # Plant a session cookie so the response.cookies clearing assertion
    # in self-delete has something to compare against.
    client.cookies.set(settings.session_cookie_name, _SESSION_ID)

    yield

    app.dependency_overrides.clear()
    app.dependency_overrides.update(saved)
    client.cookies.clear()


# ---------------------------------------------------------------------------
# GET /operators
# ---------------------------------------------------------------------------


class TestListOperators:
    def test_returns_rows_with_is_self_marker(self) -> None:
        rows = [
            _operator_row(_SELF_ID, _USERNAME),
            _operator_row(_OTHER_ID, "bob"),
        ]
        with patch("app.api.operators.list_operators", return_value=rows):
            resp = client.get("/operators")
        assert resp.status_code == 200
        body = resp.json()
        assert len(body) == 2
        by_name = {row["username"]: row for row in body}
        assert by_name[_USERNAME]["is_self"] is True
        assert by_name["bob"]["is_self"] is False


# ---------------------------------------------------------------------------
# POST /operators
# ---------------------------------------------------------------------------


class TestCreateOperator:
    def test_success_returns_201_and_row(self) -> None:
        new_row = _operator_row(_OTHER_ID, "bob")
        with patch(
            "app.api.operators.create_operator",
            return_value=(CreateOutcome.OK, new_row),
        ):
            resp = client.post(
                "/operators",
                json={"username": "bob", "password": "correct horse battery staple"},
            )
        assert resp.status_code == 201
        body = resp.json()
        assert body["operator"]["username"] == "bob"
        assert body["operator"]["is_self"] is False

    def test_bad_password_returns_400(self) -> None:
        with patch(
            "app.api.operators.create_operator",
            return_value=(CreateOutcome.BAD_PASSWORD, None),
        ):
            resp = client.post(
                "/operators",
                json={"username": "bob", "password": "short"},
            )
        assert resp.status_code == 400

    def test_bad_username_returns_400(self) -> None:
        with patch(
            "app.api.operators.create_operator",
            return_value=(CreateOutcome.BAD_USERNAME, None),
        ):
            resp = client.post(
                "/operators",
                json={"username": "   ", "password": "correct horse battery staple"},
            )
        assert resp.status_code == 400

    def test_duplicate_returns_409(self) -> None:
        with patch(
            "app.api.operators.create_operator",
            return_value=(CreateOutcome.DUPLICATE, None),
        ):
            resp = client.post(
                "/operators",
                json={"username": "bob", "password": "correct horse battery staple"},
            )
        assert resp.status_code == 409

    def test_requires_session(self) -> None:
        # Drop the session override -- the route must reject.
        app.dependency_overrides.pop(require_session, None)
        client.cookies.clear()
        resp = client.post(
            "/operators",
            json={"username": "bob", "password": "correct horse battery staple"},
        )
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# DELETE /operators/{id}
# ---------------------------------------------------------------------------


class TestDeleteOperator:
    def test_delete_other_returns_204_and_keeps_cookie(self) -> None:
        with patch(
            "app.api.operators.delete_operator",
            return_value=DeleteOutcome.OK_OTHER,
        ):
            resp = client.delete(f"/operators/{_OTHER_ID}")
        assert resp.status_code == 204
        # Cookie not cleared on the response (no Set-Cookie wiping it).
        set_cookie = resp.headers.get("set-cookie", "")
        assert settings.session_cookie_name not in set_cookie or "Max-Age=0" not in set_cookie

    def test_self_delete_returns_204_and_clears_cookie(self) -> None:
        with patch(
            "app.api.operators.delete_operator",
            return_value=DeleteOutcome.OK_SELF,
        ):
            resp = client.delete(f"/operators/{_SELF_ID}")
        assert resp.status_code == 204
        set_cookie = resp.headers.get("set-cookie", "")
        # _clear_session_cookie writes a Max-Age=0 entry for our cookie.
        assert settings.session_cookie_name in set_cookie

    def test_not_found_returns_404(self) -> None:
        with patch(
            "app.api.operators.delete_operator",
            return_value=DeleteOutcome.NOT_FOUND,
        ):
            resp = client.delete(f"/operators/{uuid4()}")
        assert resp.status_code == 404

    def test_last_operator_returns_409(self) -> None:
        with patch(
            "app.api.operators.delete_operator",
            return_value=DeleteOutcome.LAST_OPERATOR,
        ):
            resp = client.delete(f"/operators/{_SELF_ID}")
        assert resp.status_code == 409

    def test_requires_session(self) -> None:
        app.dependency_overrides.pop(require_session, None)
        client.cookies.clear()
        resp = client.delete(f"/operators/{_OTHER_ID}")
        assert resp.status_code == 401
