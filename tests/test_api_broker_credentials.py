"""Tests for app.api.broker_credentials (issue #99).

Mocks at the service-function boundary -- exercises the route shape,
auth wiring, response schema, and 401/404/409/400 mapping without
spinning up Postgres.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from app.api.auth import require_session, require_session_or_service_token
from app.config import settings
from app.db import get_conn
from app.main import app
from app.security.sessions import SessionRow
from app.services.broker_credentials import (
    CredentialAlreadyExists,
    CredentialMetadata,
    CredentialNotFound,
    CredentialValidationError,
)

client = TestClient(app)

_OPERATOR_ID = uuid4()
_USERNAME = "alice"
_SESSION_ID = "opaque-session-id"
_CRED_ID = uuid4()


def _mock_conn() -> MagicMock:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cur
    return conn


def _session_row() -> SessionRow:
    now = datetime.now(UTC)
    return SessionRow(
        session_id=_SESSION_ID,
        operator_id=_OPERATOR_ID,
        username=_USERNAME,
        expires_at=now + timedelta(hours=12),
        last_seen_at=now,
    )


def _meta(*, revoked: bool = False, label: str = "primary") -> CredentialMetadata:
    return CredentialMetadata(
        id=_CRED_ID,
        operator_id=_OPERATOR_ID,
        provider="etoro",
        label=label,
        last_four="1234",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        last_used_at=None,
        revoked_at=datetime(2026, 1, 2, tzinfo=UTC) if revoked else None,
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

    client.cookies.set(settings.session_cookie_name, _SESSION_ID)

    yield

    app.dependency_overrides.clear()
    app.dependency_overrides.update(saved)
    client.cookies.clear()


# ---------------------------------------------------------------------------
# GET /broker-credentials
# ---------------------------------------------------------------------------


class TestList:
    def test_returns_active_and_revoked(self) -> None:
        rows = [_meta(label="primary"), _meta(revoked=True, label="old")]
        with patch("app.api.broker_credentials.list_credentials", return_value=rows):
            resp = client.get("/broker-credentials")
        assert resp.status_code == 200
        body = resp.json()
        assert len(body) == 2
        assert body[0]["label"] == "primary"
        assert body[0]["revoked_at"] is None
        assert body[1]["label"] == "old"
        assert body[1]["revoked_at"] is not None

    def test_response_schema_excludes_secret_fields(self) -> None:
        with patch(
            "app.api.broker_credentials.list_credentials",
            return_value=[_meta()],
        ):
            resp = client.get("/broker-credentials")
        body = resp.json()
        assert len(body) == 1
        row = body[0]
        # Field-by-field assertion: any new ciphertext / secret /
        # plaintext field will fail this test loudly.
        assert set(row.keys()) == {
            "id",
            "provider",
            "label",
            "last_four",
            "created_at",
            "last_used_at",
            "revoked_at",
        }
        assert "ciphertext" not in row
        assert "secret" not in row

    def test_requires_session(self) -> None:
        app.dependency_overrides.pop(require_session, None)
        client.cookies.clear()
        resp = client.get("/broker-credentials")
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# POST /broker-credentials
# ---------------------------------------------------------------------------


class TestCreate:
    def test_success_returns_201_with_metadata_only(self) -> None:
        with patch(
            "app.api.broker_credentials.store_credential",
            return_value=_meta(),
        ) as mock:
            resp = client.post(
                "/broker-credentials",
                json={
                    "provider": "etoro",
                    "label": "primary",
                    "secret": "secret-value-1234",
                },
            )
        assert resp.status_code == 201
        body = resp.json()
        assert body["last_four"] == "1234"
        assert "secret" not in body
        assert "ciphertext" not in body
        # Service receives operator_id from session, not from body.
        kwargs = mock.call_args.kwargs
        assert kwargs["operator_id"] == _OPERATOR_ID
        assert kwargs["plaintext"] == "secret-value-1234"

    def test_validation_error_returns_400(self) -> None:
        with patch(
            "app.api.broker_credentials.store_credential",
            side_effect=CredentialValidationError("unsupported provider"),
        ):
            resp = client.post(
                "/broker-credentials",
                json={
                    "provider": "kraken",
                    "label": "primary",
                    "secret": "secret-value-1234",
                },
            )
        assert resp.status_code == 400

    def test_duplicate_returns_409(self) -> None:
        with patch(
            "app.api.broker_credentials.store_credential",
            side_effect=CredentialAlreadyExists("dup"),
        ):
            resp = client.post(
                "/broker-credentials",
                json={
                    "provider": "etoro",
                    "label": "primary",
                    "secret": "secret-value-1234",
                },
            )
        assert resp.status_code == 409

    def test_empty_secret_rejected_at_pydantic(self) -> None:
        resp = client.post(
            "/broker-credentials",
            json={"provider": "etoro", "label": "primary", "secret": ""},
        )
        assert resp.status_code == 422

    def test_requires_session(self) -> None:
        app.dependency_overrides.pop(require_session, None)
        client.cookies.clear()
        resp = client.post(
            "/broker-credentials",
            json={
                "provider": "etoro",
                "label": "primary",
                "secret": "secret-value-1234",
            },
        )
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# DELETE /broker-credentials/{id}
# ---------------------------------------------------------------------------


class TestDelete:
    def test_revoke_returns_204(self) -> None:
        with patch(
            "app.api.broker_credentials.revoke_credential",
            return_value=None,
        ) as mock:
            resp = client.delete(f"/broker-credentials/{_CRED_ID}")
        assert resp.status_code == 204
        kwargs = mock.call_args.kwargs
        assert kwargs["credential_id"] == _CRED_ID
        assert kwargs["operator_id"] == _OPERATOR_ID

    def test_not_found_returns_404(self) -> None:
        with patch(
            "app.api.broker_credentials.revoke_credential",
            side_effect=CredentialNotFound("missing"),
        ):
            resp = client.delete(f"/broker-credentials/{uuid4()}")
        assert resp.status_code == 404

    def test_requires_session(self) -> None:
        app.dependency_overrides.pop(require_session, None)
        client.cookies.clear()
        resp = client.delete(f"/broker-credentials/{_CRED_ID}")
        assert resp.status_code == 401
