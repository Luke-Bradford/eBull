"""Tests for app.api.broker_credentials (issue #99, #139).

Mocks at the service-function boundary -- exercises the route shape,
auth wiring, response schema, and 401/404/409/400 mapping without
spinning up Postgres.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch
from uuid import uuid4

import httpx
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
    # Pre-flight duplicate check in POST /broker-credentials runs a
    # raw SELECT and treats a non-None fetchone as "duplicate". The
    # service-layer mock paths don't care about that query, so we
    # default to "no existing row" -- tests that want to assert the
    # 409 path mock the service-layer exception explicitly.
    cur.fetchone.return_value = None
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


def _meta(
    *,
    revoked: bool = False,
    label: str = "primary",
    environment: str = "demo",
) -> CredentialMetadata:
    return CredentialMetadata(
        id=_CRED_ID,
        operator_id=_OPERATOR_ID,
        provider="etoro",
        label=label,
        environment=environment,
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

    # POST /broker-credentials no longer mounts require_master_key
    # (the create handler self-gates so it can lazy-generate on
    # first save). We still set these flags so the lazy-gen branch
    # is skipped on the service-mock tests below -- "normal + key
    # loaded" routes through the simple store_credential mock path.
    saved_state = (
        getattr(app.state, "boot_state", None),
        getattr(app.state, "broker_key_loaded", None),
        getattr(app.state, "recovery_required", None),
    )
    app.state.boot_state = "normal"
    app.state.broker_key_loaded = True
    app.state.recovery_required = False

    client.cookies.set(settings.session_cookie_name, _SESSION_ID)

    yield

    app.dependency_overrides.clear()
    app.dependency_overrides.update(saved)
    # Restore saved app.state to avoid leaking between tests.
    app.state.boot_state = saved_state[0]
    app.state.broker_key_loaded = saved_state[1]
    app.state.recovery_required = saved_state[2]
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
            "environment",
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
        # Response is now {credential, recovery_phrase} per #114.
        assert body["credential"]["last_four"] == "1234"
        assert body["credential"]["environment"] == "demo"
        assert "secret" not in body["credential"]
        assert "ciphertext" not in body["credential"]
        # Service receives operator_id from session, not from body.
        kwargs = mock.call_args.kwargs
        assert kwargs["operator_id"] == _OPERATOR_ID
        assert kwargs["environment"] == "demo"
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

    def test_commit_called_before_transaction_to_flush_implicit_pending_tx(self) -> None:
        """#112 regression — the duplicate pre-check (`_active_credential_exists`)
        opens an implicit transaction on the autocommit-off pool conn.
        Without an explicit ``conn.commit()`` before ``conn.transaction()``,
        the wrapping block nests as a SAVEPOINT and the INSERT commits
        only on get_conn teardown — i.e. AFTER the 201 response.
        Assert ``conn.commit`` was called during the successful create."""
        gen = app.dependency_overrides[get_conn]
        # The dependency yields a MagicMock; grab it.
        mock_conn = next(iter(gen()))
        with patch(
            "app.api.broker_credentials.store_credential",
            return_value=_meta(),
        ):
            resp = client.post(
                "/broker-credentials",
                json={
                    "provider": "etoro",
                    "label": "primary",
                    "secret": "secret-value-1234",
                },
            )
        assert resp.status_code == 201
        assert mock_conn.commit.called, (
            "POST /broker-credentials must call conn.commit() to flush the "
            "implicit transaction opened by _active_credential_exists before "
            "entering conn.transaction(); otherwise the INSERT defers commit "
            "to get_conn teardown, after the response is sent."
        )

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


# ---------------------------------------------------------------------------
# POST /broker-credentials/validate (#139)
# ---------------------------------------------------------------------------


class TestValidate:
    """Tests for the transient credential validation endpoint."""

    _BODY = {"api_key": "test-api-key", "user_key": "test-user-key", "environment": "demo"}

    @patch("app.api.broker_credentials.httpx.Client")
    def test_valid_credentials_both_levels(self, mock_client_cls: MagicMock) -> None:
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        # L1: /me returns 200
        me_resp = MagicMock()
        me_resp.status_code = 200
        me_resp.json.return_value = {"gcid": 123, "demoCid": 456, "realCid": 789}
        # L2: /pnl returns 200
        env_resp = MagicMock()
        env_resp.status_code = 200
        mock_client.get.side_effect = [me_resp, env_resp]

        resp = client.post("/broker-credentials/validate", json=self._BODY)
        assert resp.status_code == 200
        body = resp.json()
        assert body["auth_valid"] is True
        assert body["env_valid"] is True
        assert body["identity"]["gcid"] == 123
        assert body["identity"]["demo_cid"] == 456
        assert body["identity"]["real_cid"] == 789
        assert body["environment"] == "demo"

    @patch("app.api.broker_credentials.httpx.Client")
    def test_invalid_auth_returns_auth_valid_false(self, mock_client_cls: MagicMock) -> None:
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        me_resp = MagicMock()
        me_resp.status_code = 401
        mock_client.get.return_value = me_resp

        resp = client.post("/broker-credentials/validate", json=self._BODY)
        assert resp.status_code == 200
        body = resp.json()
        assert body["auth_valid"] is False
        assert body["env_valid"] is False
        assert body["env_check"] == "skipped"

    @patch("app.api.broker_credentials.httpx.Client")
    def test_valid_auth_invalid_env(self, mock_client_cls: MagicMock) -> None:
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        me_resp = MagicMock()
        me_resp.status_code = 200
        me_resp.json.return_value = {"gcid": 1}
        env_resp = MagicMock()
        env_resp.status_code = 403
        mock_client.get.side_effect = [me_resp, env_resp]

        resp = client.post("/broker-credentials/validate", json=self._BODY)
        assert resp.status_code == 200
        body = resp.json()
        assert body["auth_valid"] is True
        assert body["env_valid"] is False

    @patch("app.api.broker_credentials.httpx.Client")
    def test_network_error_handled_gracefully(self, mock_client_cls: MagicMock) -> None:
        mock_http = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_http)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_http.get.side_effect = httpx.ConnectError("connection refused")

        resp = client.post("/broker-credentials/validate", json=self._BODY)
        assert resp.status_code == 200
        body = resp.json()
        assert body["auth_valid"] is False
        assert body["env_valid"] is False

    @patch("app.api.broker_credentials.httpx.Client")
    def test_timeout_error_handled_gracefully(self, mock_client_cls: MagicMock) -> None:
        """Regression for #162: httpx.TimeoutException must be caught
        and returned as a 200 with auth_valid=False, not propagated as
        an unhandled 500. The catch at _probe_etoro's except clause is
        httpx.HTTPError (the base class), which covers TimeoutException,
        ReadError, ConnectError, and the rest of the transport-error
        hierarchy. This test pins the timeout branch explicitly so a
        future narrowing of the catch cannot silently regress it.
        """
        mock_http = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_http)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_http.get.side_effect = httpx.TimeoutException("read timeout")

        resp = client.post("/broker-credentials/validate", json=self._BODY)
        assert resp.status_code == 200
        body = resp.json()
        assert body["auth_valid"] is False
        assert body["env_valid"] is False

    def test_invalid_environment_rejected(self) -> None:
        body = {**self._BODY, "environment": "production"}
        resp = client.post("/broker-credentials/validate", json=body)
        assert resp.status_code == 400

    def test_requires_session(self) -> None:
        app.dependency_overrides.pop(require_session, None)
        client.cookies.clear()
        resp = client.post("/broker-credentials/validate", json=self._BODY)
        assert resp.status_code == 401

    @patch("app.api.broker_credentials.httpx.Client")
    def test_no_etoro_error_details_leaked(self, mock_client_cls: MagicMock) -> None:
        """Verify raw eToro error text is not echoed in the response."""
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        me_resp = MagicMock()
        me_resp.status_code = 403
        me_resp.text = "INTERNAL_ETORO_SECRET_ERROR_DETAIL_xyz"
        mock_client.get.return_value = me_resp

        resp = client.post("/broker-credentials/validate", json=self._BODY)
        assert resp.status_code == 200
        assert "INTERNAL_ETORO_SECRET_ERROR_DETAIL_xyz" not in resp.text


# ---------------------------------------------------------------------------
# POST /broker-credentials/validate-stored (#144)
# ---------------------------------------------------------------------------


class TestValidateStored:
    """Tests for the stored-credential validation endpoint."""

    @patch("app.api.broker_credentials.httpx.Client")
    @patch("app.api.broker_credentials.load_credential_for_provider_use")
    def test_valid_stored_credentials(self, mock_load: MagicMock, mock_client_cls: MagicMock) -> None:
        mock_load.side_effect = ["stored-api-key", "stored-user-key"]

        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        me_resp = MagicMock()
        me_resp.status_code = 200
        me_resp.json.return_value = {"gcid": 42}
        env_resp = MagicMock()
        env_resp.status_code = 200
        mock_client.get.side_effect = [me_resp, env_resp]

        resp = client.post("/broker-credentials/validate-stored")
        assert resp.status_code == 200
        body = resp.json()
        assert body["auth_valid"] is True
        assert body["env_valid"] is True
        assert body["identity"]["gcid"] == 42

    @patch("app.api.broker_credentials.load_credential_for_provider_use")
    def test_missing_credential_returns_404(self, mock_load: MagicMock) -> None:
        mock_load.side_effect = CredentialNotFound("no active credential")

        resp = client.post("/broker-credentials/validate-stored")
        assert resp.status_code == 404
        assert "must be stored" in resp.json()["detail"]

    @patch("app.api.broker_credentials.load_credential_for_provider_use")
    def test_decrypt_failure_returns_503(self, mock_load: MagicMock) -> None:
        from app.services.broker_credentials import CredentialDecryptError

        mock_load.side_effect = CredentialDecryptError("bad ciphertext")

        resp = client.post("/broker-credentials/validate-stored")
        assert resp.status_code == 503
        assert "decryption failed" in resp.json()["detail"].lower()

    @patch("app.api.broker_credentials.load_credential_for_provider_use")
    def test_no_etoro_error_details_leaked(self, mock_load: MagicMock) -> None:
        """Verify raw CredentialDecryptError text is not echoed."""
        from app.services.broker_credentials import CredentialDecryptError

        mock_load.side_effect = CredentialDecryptError("SECRET_INTERNAL_xyz")

        resp = client.post("/broker-credentials/validate-stored")
        assert "SECRET_INTERNAL_xyz" not in resp.text

    def test_requires_session(self) -> None:
        app.dependency_overrides.pop(require_session, None)
        client.cookies.clear()
        resp = client.post("/broker-credentials/validate-stored")
        assert resp.status_code == 401

    @patch("app.api.broker_credentials.httpx.Client")
    @patch("app.api.broker_credentials.load_credential_for_provider_use")
    def test_loads_both_keys_with_correct_labels(self, mock_load: MagicMock, mock_client_cls: MagicMock) -> None:
        mock_load.side_effect = ["key-a", "key-b"]

        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        me_resp = MagicMock()
        me_resp.status_code = 200
        me_resp.json.return_value = {}
        env_resp = MagicMock()
        env_resp.status_code = 200
        mock_client.get.side_effect = [me_resp, env_resp]

        client.post("/broker-credentials/validate-stored")

        assert mock_load.call_count == 2
        call_labels = [c.kwargs["label"] for c in mock_load.call_args_list]
        assert call_labels == ["api_key", "user_key"]
        call_callers = [c.kwargs["caller"] for c in mock_load.call_args_list]
        assert all(c == "validate-stored" for c in call_callers)


# ---------------------------------------------------------------------------
# normalise_environment (service layer, #139)
# ---------------------------------------------------------------------------


class TestNormaliseEnvironment:
    def test_valid_demo(self) -> None:
        from app.services.broker_credentials import normalise_environment

        assert normalise_environment("demo") == "demo"

    def test_valid_real(self) -> None:
        from app.services.broker_credentials import normalise_environment

        assert normalise_environment("real") == "real"

    def test_case_normalised(self) -> None:
        from app.services.broker_credentials import normalise_environment

        assert normalise_environment("DEMO") == "demo"
        assert normalise_environment("Real") == "real"

    def test_whitespace_stripped(self) -> None:
        from app.services.broker_credentials import normalise_environment

        assert normalise_environment("  demo  ") == "demo"

    def test_invalid_rejected(self) -> None:
        from app.services.broker_credentials import normalise_environment

        with pytest.raises(CredentialValidationError, match="unsupported environment"):
            normalise_environment("production")
