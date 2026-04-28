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
from app.security.master_key import BootResult, MasterKeyError
from app.security.secrets_crypto import CredentialCryptoConfigError
from app.services.broker_credentials import CredentialAlreadyExists, CredentialNotFound
from app.services.operators import NoOperatorError


def _normal_boot(key: bytes | None = None) -> BootResult:
    return BootResult(
        state="normal",
        broker_encryption_key=key if key is not None else b"\x00" * 32,
        needs_setup=False,
        recovery_required=False,
    )


@pytest.fixture(autouse=True)
def _key() -> Iterator[None]:
    secrets_crypto.set_active_key(os.urandom(32))
    yield
    secrets_crypto._reset_for_tests()


# ---------------------------------------------------------------------------
# Migration script
# ---------------------------------------------------------------------------


class TestMigrateEtoroCredential:
    """Tests for scripts/migrate_etoro_credential.main()."""

    @patch("scripts.migrate_etoro_credential.set_active_key")
    @patch("scripts.migrate_etoro_credential.master_key")
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
        mock_master_key: MagicMock,
        mock_set_active: MagicMock,
    ) -> None:
        from scripts.migrate_etoro_credential import main

        op_id = uuid4()
        mock_sole_op.return_value = op_id
        mock_settings.database_url = "postgresql://test"
        derived_key = b"\xab" * 32
        mock_master_key.bootstrap.return_value = _normal_boot(derived_key)
        mock_conn = MagicMock()
        mock_psycopg.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_psycopg.connect.return_value.__exit__ = MagicMock(return_value=False)

        assert main() == 0
        # bootstrap must run before any encrypt call
        mock_master_key.bootstrap.assert_called_once_with(mock_conn)
        mock_set_active.assert_called_once_with(derived_key)
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

    @patch("scripts.migrate_etoro_credential.set_active_key")
    @patch("scripts.migrate_etoro_credential.master_key")
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
        mock_master_key: MagicMock,
        mock_set_active: MagicMock,
    ) -> None:
        from scripts.migrate_etoro_credential import main

        mock_settings.database_url = "postgresql://test"
        mock_master_key.bootstrap.return_value = _normal_boot()
        mock_sole_op.side_effect = NoOperatorError("no operator")
        mock_conn = MagicMock()
        mock_psycopg.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_psycopg.connect.return_value.__exit__ = MagicMock(return_value=False)

        assert main() == 1

    @patch("scripts.migrate_etoro_credential.set_active_key")
    @patch("scripts.migrate_etoro_credential.master_key")
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
        mock_master_key: MagicMock,
        mock_set_active: MagicMock,
    ) -> None:
        from scripts.migrate_etoro_credential import main

        mock_sole_op.return_value = uuid4()
        mock_store.side_effect = CredentialAlreadyExists("exists")
        mock_master_key.bootstrap.return_value = _normal_boot()
        mock_settings.database_url = "postgresql://test"
        mock_conn = MagicMock()
        mock_psycopg.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_psycopg.connect.return_value.__exit__ = MagicMock(return_value=False)

        assert main() == 0

    @patch("scripts.migrate_etoro_credential.set_active_key")
    @patch("scripts.migrate_etoro_credential.master_key")
    @patch("scripts.migrate_etoro_credential.psycopg")
    @patch("scripts.migrate_etoro_credential.settings")
    @patch("scripts.migrate_etoro_credential._READ_KEY", "test-key-12345")
    @patch("scripts.migrate_etoro_credential._WRITE_KEY", "")
    def test_clean_install_state_exits_nonzero(
        self,
        mock_settings: MagicMock,
        mock_psycopg: MagicMock,
        mock_master_key: MagicMock,
        mock_set_active: MagicMock,
    ) -> None:
        from scripts.migrate_etoro_credential import main

        mock_settings.database_url = "postgresql://test"
        mock_master_key.bootstrap.return_value = BootResult(
            state="clean_install",
            broker_encryption_key=None,
            needs_setup=True,
            recovery_required=False,
        )
        mock_conn = MagicMock()
        mock_psycopg.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_psycopg.connect.return_value.__exit__ = MagicMock(return_value=False)

        assert main() == 1
        # active key MUST NOT be installed when bootstrap returned None
        mock_set_active.assert_not_called()

    @patch("scripts.migrate_etoro_credential.set_active_key")
    @patch("scripts.migrate_etoro_credential.master_key")
    @patch("scripts.migrate_etoro_credential.psycopg")
    @patch("scripts.migrate_etoro_credential.settings")
    @patch("scripts.migrate_etoro_credential._READ_KEY", "test-key-12345")
    @patch("scripts.migrate_etoro_credential._WRITE_KEY", "")
    def test_recovery_required_state_exits_nonzero(
        self,
        mock_settings: MagicMock,
        mock_psycopg: MagicMock,
        mock_master_key: MagicMock,
        mock_set_active: MagicMock,
    ) -> None:
        from scripts.migrate_etoro_credential import main

        mock_settings.database_url = "postgresql://test"
        mock_master_key.bootstrap.return_value = BootResult(
            state="recovery_required",
            broker_encryption_key=None,
            needs_setup=False,
            recovery_required=True,
        )
        mock_conn = MagicMock()
        mock_psycopg.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_psycopg.connect.return_value.__exit__ = MagicMock(return_value=False)

        assert main() == 1
        mock_set_active.assert_not_called()

    @patch("scripts.migrate_etoro_credential.master_key")
    @patch("scripts.migrate_etoro_credential.psycopg")
    @patch("scripts.migrate_etoro_credential.settings")
    @patch("scripts.migrate_etoro_credential._READ_KEY", "test-key-12345")
    @patch("scripts.migrate_etoro_credential._WRITE_KEY", "")
    def test_master_key_error_exits_nonzero(
        self,
        mock_settings: MagicMock,
        mock_psycopg: MagicMock,
        mock_master_key: MagicMock,
    ) -> None:
        from scripts.migrate_etoro_credential import main

        mock_settings.database_url = "postgresql://test"
        mock_master_key.bootstrap.side_effect = MasterKeyError("EBULL_SECRETS_KEY does not match existing ciphertext")
        mock_conn = MagicMock()
        mock_psycopg.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_psycopg.connect.return_value.__exit__ = MagicMock(return_value=False)

        assert main() == 1

    @patch("scripts.migrate_etoro_credential.master_key")
    @patch("scripts.migrate_etoro_credential.psycopg")
    @patch("scripts.migrate_etoro_credential.settings")
    @patch("scripts.migrate_etoro_credential._READ_KEY", "test-key-12345")
    @patch("scripts.migrate_etoro_credential._WRITE_KEY", "")
    def test_malformed_env_key_exits_nonzero(
        self,
        mock_settings: MagicMock,
        mock_psycopg: MagicMock,
        mock_master_key: MagicMock,
    ) -> None:
        """``decode_env_key`` raises CredentialCryptoConfigError on a malformed
        EBULL_SECRETS_KEY. master_key.bootstrap propagates it; the script must
        catch and exit cleanly rather than dump a traceback.
        """
        from scripts.migrate_etoro_credential import main

        mock_settings.database_url = "postgresql://test"
        mock_master_key.bootstrap.side_effect = CredentialCryptoConfigError(
            "EBULL_SECRETS_KEY must decode to exactly 32 bytes"
        )
        mock_conn = MagicMock()
        mock_psycopg.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_psycopg.connect.return_value.__exit__ = MagicMock(return_value=False)

        assert main() == 1

    @patch("scripts.migrate_etoro_credential.set_active_key")
    @patch("scripts.migrate_etoro_credential.master_key")
    @patch("scripts.migrate_etoro_credential.psycopg")
    @patch("scripts.migrate_etoro_credential.store_credential")
    @patch("scripts.migrate_etoro_credential.sole_operator_id")
    @patch("scripts.migrate_etoro_credential.settings")
    @patch("scripts.migrate_etoro_credential._READ_KEY", "test-read-key-1234")
    @patch("scripts.migrate_etoro_credential._WRITE_KEY", "")
    def test_clean_install_with_env_override_proceeds(
        self,
        mock_settings: MagicMock,
        mock_sole_op: MagicMock,
        mock_store: MagicMock,
        mock_psycopg: MagicMock,
        mock_master_key: MagicMock,
        mock_set_active: MagicMock,
    ) -> None:
        """When EBULL_SECRETS_KEY is set on a clean_install database,
        bootstrap returns state=clean_install with the env_key installed.
        The script must proceed (key is loaded), not bail on the state
        label. This is the env-override migration path.
        """
        from scripts.migrate_etoro_credential import main

        op_id = uuid4()
        mock_sole_op.return_value = op_id
        mock_settings.database_url = "postgresql://test"
        env_key = b"\xcd" * 32
        mock_master_key.bootstrap.return_value = BootResult(
            state="clean_install",
            broker_encryption_key=env_key,
            needs_setup=True,
            recovery_required=False,
        )
        mock_conn = MagicMock()
        mock_psycopg.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_psycopg.connect.return_value.__exit__ = MagicMock(return_value=False)

        assert main() == 0
        mock_set_active.assert_called_once_with(env_key)
        mock_store.assert_called_once()


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
        # Commit after each load so audit rows are durable individually
        assert mock_conn.commit.call_count == 2

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
    def test_missing_user_key_returns_none_but_commits_api_key_audit(
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
        # api_key audit row must have been committed before the user_key
        # lookup raised — partial failure must not roll back the first
        # audit entry.
        mock_conn.commit.assert_called_once()

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
