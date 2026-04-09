"""Tests for the scheduler eToro credential loading path and the
migration script (issue #100).

Scheduler tests verify that ``_load_etoro_api_key`` correctly loads
credentials through the encrypted store and skips gracefully on
missing operator / missing credential.

Migration script tests verify the one-shot env-to-store migration
handles all expected scenarios.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from app.security import secrets_crypto
from app.services.broker_credentials import CredentialAlreadyExists, CredentialNotFound
from app.services.operators import AmbiguousOperatorError, NoOperatorError


@pytest.fixture(autouse=True)
def _key() -> Iterator[None]:
    secrets_crypto.set_active_key(os.urandom(32))
    yield
    secrets_crypto._reset_for_tests()


# ---------------------------------------------------------------------------
# _load_etoro_api_key (scheduler helper)
# ---------------------------------------------------------------------------


class TestLoadEtoroApiKey:
    """Tests for app.workers.scheduler._load_etoro_api_key."""

    @patch("app.workers.scheduler.psycopg")
    @patch("app.workers.scheduler.load_credential_for_provider_use")
    @patch("app.workers.scheduler.sole_operator_id")
    def test_success_returns_key(
        self,
        mock_sole_op: MagicMock,
        mock_load_cred: MagicMock,
        mock_psycopg: MagicMock,
    ) -> None:
        from app.workers.scheduler import _load_etoro_api_key

        op_id = uuid4()
        mock_sole_op.return_value = op_id
        mock_load_cred.return_value = "the-api-key"
        mock_conn = MagicMock()
        mock_psycopg.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_psycopg.connect.return_value.__exit__ = MagicMock(return_value=False)

        result = _load_etoro_api_key("test_job")
        assert result == "the-api-key"
        mock_sole_op.assert_called_once_with(mock_conn)
        mock_load_cred.assert_called_once_with(
            mock_conn,
            operator_id=op_id,
            provider="etoro",
            label="api_key",
            environment="demo",
            caller="test_job",
        )
        mock_conn.commit.assert_called_once()

    @patch("app.workers.scheduler.psycopg")
    @patch("app.workers.scheduler.sole_operator_id")
    def test_no_operator_returns_none(
        self,
        mock_sole_op: MagicMock,
        mock_psycopg: MagicMock,
    ) -> None:
        from app.workers.scheduler import _load_etoro_api_key

        mock_sole_op.side_effect = NoOperatorError("no operator")
        mock_conn = MagicMock()
        mock_psycopg.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_psycopg.connect.return_value.__exit__ = MagicMock(return_value=False)

        assert _load_etoro_api_key("test_job") is None

    @patch("app.workers.scheduler.psycopg")
    @patch("app.workers.scheduler.sole_operator_id")
    def test_ambiguous_operator_returns_none(
        self,
        mock_sole_op: MagicMock,
        mock_psycopg: MagicMock,
    ) -> None:
        from app.workers.scheduler import _load_etoro_api_key

        mock_sole_op.side_effect = AmbiguousOperatorError("two operators")
        mock_conn = MagicMock()
        mock_psycopg.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_psycopg.connect.return_value.__exit__ = MagicMock(return_value=False)

        assert _load_etoro_api_key("test_job") is None

    @patch("app.workers.scheduler.psycopg")
    @patch("app.workers.scheduler.load_credential_for_provider_use")
    @patch("app.workers.scheduler.sole_operator_id")
    def test_credential_not_found_returns_none(
        self,
        mock_sole_op: MagicMock,
        mock_load_cred: MagicMock,
        mock_psycopg: MagicMock,
    ) -> None:
        from app.workers.scheduler import _load_etoro_api_key

        mock_sole_op.return_value = uuid4()
        mock_load_cred.side_effect = CredentialNotFound("no cred")
        mock_conn = MagicMock()
        mock_psycopg.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_psycopg.connect.return_value.__exit__ = MagicMock(return_value=False)

        assert _load_etoro_api_key("test_job") is None


# ---------------------------------------------------------------------------
# Migration script
# ---------------------------------------------------------------------------


class TestMigrateEtoroCredential:
    """Tests for scripts/migrate_etoro_credential.main()."""

    @patch("scripts.migrate_etoro_credential.psycopg")
    @patch("scripts.migrate_etoro_credential.store_credential")
    @patch("scripts.migrate_etoro_credential.sole_operator_id")
    @patch("scripts.migrate_etoro_credential.settings")
    @patch("scripts.migrate_etoro_credential._READ_KEY", "test-read-key-1234")
    @patch("scripts.migrate_etoro_credential._WRITE_KEY", "")
    def test_read_key_migrated(
        self,
        mock_settings: MagicMock,
        mock_sole_op: MagicMock,
        mock_store: MagicMock,
        mock_psycopg: MagicMock,
    ) -> None:
        from scripts.migrate_etoro_credential import main

        op_id = uuid4()
        mock_sole_op.return_value = op_id
        mock_settings.secrets_key = "some-key"
        mock_settings.database_url = "postgresql://test"
        mock_conn = MagicMock()
        mock_psycopg.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_psycopg.connect.return_value.__exit__ = MagicMock(return_value=False)

        assert main() == 0
        mock_store.assert_called_once_with(
            mock_conn,
            operator_id=op_id,
            provider="etoro",
            label="api_key",
            environment="demo",
            plaintext="test-read-key-1234",
        )

    @patch("scripts.migrate_etoro_credential._READ_KEY", "")
    @patch("scripts.migrate_etoro_credential._WRITE_KEY", "")
    def test_nothing_to_migrate(self) -> None:
        from scripts.migrate_etoro_credential import main

        assert main() == 0

    @patch("scripts.migrate_etoro_credential.psycopg")
    @patch("scripts.migrate_etoro_credential.sole_operator_id")
    @patch("scripts.migrate_etoro_credential.settings")
    @patch("scripts.migrate_etoro_credential._READ_KEY", "test-key-12345")
    @patch("scripts.migrate_etoro_credential._WRITE_KEY", "")
    def test_no_operator_exits_nonzero(
        self,
        mock_settings: MagicMock,
        mock_sole_op: MagicMock,
        mock_psycopg: MagicMock,
    ) -> None:
        from scripts.migrate_etoro_credential import main

        mock_settings.secrets_key = "some-key"
        mock_settings.database_url = "postgresql://test"
        mock_sole_op.side_effect = NoOperatorError("no operator")
        mock_conn = MagicMock()
        mock_psycopg.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_psycopg.connect.return_value.__exit__ = MagicMock(return_value=False)

        assert main() == 1

    @patch("scripts.migrate_etoro_credential.psycopg")
    @patch("scripts.migrate_etoro_credential.store_credential")
    @patch("scripts.migrate_etoro_credential.sole_operator_id")
    @patch("scripts.migrate_etoro_credential.settings")
    @patch("scripts.migrate_etoro_credential._READ_KEY", "test-key-12345")
    @patch("scripts.migrate_etoro_credential._WRITE_KEY", "")
    def test_already_exists_skips(
        self,
        mock_settings: MagicMock,
        mock_sole_op: MagicMock,
        mock_store: MagicMock,
        mock_psycopg: MagicMock,
    ) -> None:
        from scripts.migrate_etoro_credential import main

        mock_sole_op.return_value = uuid4()
        mock_store.side_effect = CredentialAlreadyExists("exists")
        mock_settings.secrets_key = "some-key"
        mock_settings.database_url = "postgresql://test"
        mock_conn = MagicMock()
        mock_psycopg.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_psycopg.connect.return_value.__exit__ = MagicMock(return_value=False)

        assert main() == 0

    @patch("scripts.migrate_etoro_credential._READ_KEY", "test-key-12345")
    @patch("scripts.migrate_etoro_credential._WRITE_KEY", "")
    @patch("scripts.migrate_etoro_credential.settings")
    def test_no_secrets_key_exits_nonzero(
        self,
        mock_settings: MagicMock,
    ) -> None:
        from scripts.migrate_etoro_credential import main

        mock_settings.secrets_key = None

        assert main() == 1


# ---------------------------------------------------------------------------
# _load_etoro_credentials (new scheduler helper, #139)
# ---------------------------------------------------------------------------


class TestLoadEtoroCredentials:
    """Tests for app.workers.scheduler._load_etoro_credentials."""

    @patch("app.workers.scheduler.psycopg")
    @patch("app.workers.scheduler.load_credential_for_provider_use")
    @patch("app.workers.scheduler.sole_operator_id")
    def test_success_returns_tuple(
        self,
        mock_sole_op: MagicMock,
        mock_load_cred: MagicMock,
        mock_psycopg: MagicMock,
    ) -> None:
        from app.workers.scheduler import _load_etoro_credentials

        op_id = uuid4()
        mock_sole_op.return_value = op_id
        mock_load_cred.side_effect = ["the-api-key", "the-user-key"]
        mock_conn = MagicMock()
        mock_psycopg.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_psycopg.connect.return_value.__exit__ = MagicMock(return_value=False)

        result = _load_etoro_credentials("test_job")
        assert result == ("the-api-key", "the-user-key")
        assert mock_load_cred.call_count == 2
        # First call: api_key
        call_1 = mock_load_cred.call_args_list[0]
        assert call_1.kwargs["label"] == "api_key"
        assert call_1.kwargs["environment"] == "demo"
        # Second call: user_key
        call_2 = mock_load_cred.call_args_list[1]
        assert call_2.kwargs["label"] == "user_key"
        assert call_2.kwargs["environment"] == "demo"
        mock_conn.commit.assert_called_once()

    @patch("app.workers.scheduler.psycopg")
    @patch("app.workers.scheduler.sole_operator_id")
    def test_no_operator_returns_none(
        self,
        mock_sole_op: MagicMock,
        mock_psycopg: MagicMock,
    ) -> None:
        from app.workers.scheduler import _load_etoro_credentials

        mock_sole_op.side_effect = NoOperatorError("no operator")
        mock_conn = MagicMock()
        mock_psycopg.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_psycopg.connect.return_value.__exit__ = MagicMock(return_value=False)

        assert _load_etoro_credentials("test_job") is None

    @patch("app.workers.scheduler.psycopg")
    @patch("app.workers.scheduler.load_credential_for_provider_use")
    @patch("app.workers.scheduler.sole_operator_id")
    def test_missing_user_key_returns_none(
        self,
        mock_sole_op: MagicMock,
        mock_load_cred: MagicMock,
        mock_psycopg: MagicMock,
    ) -> None:
        from app.workers.scheduler import _load_etoro_credentials

        mock_sole_op.return_value = uuid4()
        # api_key succeeds, user_key not found
        mock_load_cred.side_effect = ["the-api-key", CredentialNotFound("no user_key")]
        mock_conn = MagicMock()
        mock_psycopg.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_psycopg.connect.return_value.__exit__ = MagicMock(return_value=False)

        assert _load_etoro_credentials("test_job") is None

    @patch("app.workers.scheduler.psycopg")
    @patch("app.workers.scheduler.load_credential_for_provider_use")
    @patch("app.workers.scheduler.sole_operator_id")
    def test_missing_api_key_returns_none(
        self,
        mock_sole_op: MagicMock,
        mock_load_cred: MagicMock,
        mock_psycopg: MagicMock,
    ) -> None:
        from app.workers.scheduler import _load_etoro_credentials

        mock_sole_op.return_value = uuid4()
        mock_load_cred.side_effect = CredentialNotFound("no api_key")
        mock_conn = MagicMock()
        mock_psycopg.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_psycopg.connect.return_value.__exit__ = MagicMock(return_value=False)

        assert _load_etoro_credentials("test_job") is None
