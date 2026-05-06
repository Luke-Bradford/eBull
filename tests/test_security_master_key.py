"""Tests for app.security.master_key (#114 / ADR-0003, amended 2026-05-07).

Pure-function + filesystem + DB-integration coverage. The post-amendment
module exposes a two-state boot machine (clean_install | normal),
soft-revokes stale ciphertext at boot, and removes the recovery phrase
ceremony entirely.
"""

from __future__ import annotations

import base64
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import psycopg
import pytest

from app.security import master_key, secrets_crypto
from app.security.master_key import (
    BootResult,
    MasterKeyError,
    bootstrap,
    compute_boot_state,
    derive_broker_encryption_key,
    read_root_secret,
    resolve_data_dir,
    root_secret_path,
    write_root_secret,
)

# ``ebull_test_conn`` is a fixture made globally available via
# tests/conftest.py — no local import needed.


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def isolated_data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point EBULL_DATA_DIR at a tmp dir for the test."""
    monkeypatch.setenv("EBULL_DATA_DIR", str(tmp_path))
    return tmp_path


@pytest.fixture
def crypto_key() -> Iterator[bytes]:
    """Set + tear down secrets_crypto active key around each test."""
    key = os.urandom(32)
    secrets_crypto.set_active_key(key)
    yield key
    secrets_crypto._reset_for_tests()


# ---------------------------------------------------------------------------
# Pure / filesystem
# ---------------------------------------------------------------------------


class TestResolveDataDir:
    def test_env_var_wins(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("EBULL_DATA_DIR", str(tmp_path))
        assert resolve_data_dir() == tmp_path

    def test_settings_falls_back_to_platformdirs(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("EBULL_DATA_DIR", raising=False)
        monkeypatch.setattr(master_key.settings, "data_dir", None)
        path = resolve_data_dir()
        assert "eBull" in str(path)


class TestRootSecretFile:
    def test_round_trip(self, isolated_data_dir: Path) -> None:
        secret = os.urandom(32)
        write_root_secret(secret)
        assert read_root_secret() == secret
        assert root_secret_path() == isolated_data_dir / "root_secret.bin"

    def test_missing_file_returns_none(self, isolated_data_dir: Path) -> None:
        assert read_root_secret() is None

    def test_corrupt_file_raises(self, isolated_data_dir: Path) -> None:
        path = isolated_data_dir / "root_secret.bin"
        path.write_bytes(b"\x00" * 16)
        with pytest.raises(MasterKeyError, match="corrupt"):
            read_root_secret()


class TestDerivedKey:
    def test_deterministic(self) -> None:
        secret = os.urandom(32)
        a = derive_broker_encryption_key(secret)
        b = derive_broker_encryption_key(secret)
        assert a == b
        assert len(a) == 32

    def test_different_input_different_output(self) -> None:
        a = derive_broker_encryption_key(os.urandom(32))
        b = derive_broker_encryption_key(os.urandom(32))
        assert a != b

    def test_wrong_length_rejected(self) -> None:
        with pytest.raises(MasterKeyError):
            derive_broker_encryption_key(b"\x00" * 16)


class TestComputeBootState:
    def test_no_credentials_is_clean_install(self) -> None:
        assert compute_boot_state(credentials_exist=False, root_secret_present=False) == "clean_install"
        assert compute_boot_state(credentials_exist=False, root_secret_present=True) == "clean_install"

    def test_credentials_with_key_is_normal(self) -> None:
        assert compute_boot_state(credentials_exist=True, root_secret_present=True) == "normal"

    def test_credentials_no_key_falls_back_to_clean_install(self) -> None:
        # Stale-revoke is expected to run upstream and clear any
        # surviving rows; this branch is conservative belt-and-braces.
        assert compute_boot_state(credentials_exist=True, root_secret_present=False) == "clean_install"


class TestBootResult:
    def test_dataclass_is_frozen(self) -> None:
        r = BootResult(state="normal", broker_encryption_key=b"\x00" * 32)
        with pytest.raises(Exception):
            r.state = "clean_install"  # type: ignore[misc]


class TestGenerateSplit:
    """The two-phase generate -> persist API: a DB error after key
    install must never leave the operator with a persisted root secret
    that they cannot reach (review-prevention from PR #118)."""

    def test_in_memory_does_not_touch_disk(self, isolated_data_dir: Path) -> None:
        master_key.generate_root_secret_in_memory()
        assert not (isolated_data_dir / "root_secret.bin").exists()

    def test_persist_writes_file(self, isolated_data_dir: Path) -> None:
        secret, _derived = master_key.generate_root_secret_in_memory()
        master_key.persist_generated_root_secret(secret)
        assert (isolated_data_dir / "root_secret.bin").read_bytes() == secret


@pytest.mark.skipif(os.name == "nt", reason="POSIX-only perms check")
def test_data_dir_locked_to_0700(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EBULL_DATA_DIR", str(tmp_path / "secrets"))
    write_root_secret(os.urandom(32))
    mode = (tmp_path / "secrets").stat().st_mode & 0o777
    assert mode == 0o700


# ---------------------------------------------------------------------------
# Stale-cipher soft-revoke (DB-integration)
# ---------------------------------------------------------------------------


def _insert_operator(conn: psycopg.Connection[Any]) -> UUID:
    op_id = uuid4()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO operators (operator_id, username, password_hash) VALUES (%s, %s, %s)",
            (op_id, f"op-{op_id.hex[:8]}", "argon2:dummy"),
        )
    conn.commit()
    return op_id


def _encrypted_blob(
    operator_id: UUID,
    *,
    label: str,
    key: bytes,
    key_version: int = 1,
) -> bytes:
    """Encrypt a placeholder secret under *key* with the same AAD shape
    the production encryption uses, so a stale-revoke pass that calls
    ``_key_decrypts_row`` correctly classifies it as decryptable.
    Inlined rather than calling ``secrets_crypto.encrypt`` because the
    mismatch test needs to encrypt under a key that is NOT the
    cache-active key."""
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    aad = secrets_crypto.build_aad(
        operator_id=operator_id,
        provider="etoro",
        label=label,
        key_version=key_version,
    )
    nonce = os.urandom(secrets_crypto.NONCE_LEN)
    ct = AESGCM(key).encrypt(nonce, b"placeholder", aad)
    return nonce + ct


def _insert_credential(
    conn: psycopg.Connection[Any],
    *,
    operator_id: UUID,
    label: str,
    ciphertext: bytes,
    revoked: bool = False,
) -> UUID:
    cred_id = uuid4()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO broker_credentials
                (id, operator_id, provider, label, environment,
                 ciphertext, last_four, key_version, health_state, revoked_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                cred_id,
                operator_id,
                "etoro",
                label,
                "demo",
                ciphertext,
                "abcd",
                1,
                "untested",
                None,
            ),
        )
        if revoked:
            cur.execute(
                "UPDATE broker_credentials SET revoked_at = NOW() WHERE id = %s",
                (cred_id,),
            )
    conn.commit()
    return cred_id


def _active_count(conn: psycopg.Connection[Any]) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM broker_credentials WHERE revoked_at IS NULL")
        row = cur.fetchone()
    assert row is not None
    return int(row[0])


class TestRevokeStaleCiphertext:
    def test_no_op_on_empty_db(self, ebull_test_conn: psycopg.Connection[Any]) -> None:
        result = master_key._revoke_stale_ciphertext(ebull_test_conn, derived_key=os.urandom(32))
        assert result.total == 0

    def test_orphan_branch_is_defense_in_depth_only(
        self,
        ebull_test_conn: psycopg.Connection[Any],
    ) -> None:
        """The orphan branch in ``_revoke_stale_ciphertext`` is
        defense-in-depth: ``broker_credentials.operator_id`` has
        ``REFERENCES operators(operator_id) ON DELETE CASCADE``, so
        deleting an operator drops their credentials in the same
        transaction and orphans cannot exist via supported paths. The
        branch stays in the code so a future schema relaxation or an
        out-of-band DBA delete cannot leave undecryptable rows behind.
        Pin the FK shape here so a future change that removes CASCADE
        forces this comment to be re-read."""
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT confdeltype
                  FROM pg_constraint
                 WHERE conname = 'broker_credentials_operator_id_fkey'
                """
            )
            row = cur.fetchone()
        assert row is not None
        # 'c' = CASCADE per pg_constraint.confdeltype.
        assert row[0] == "c"

    def test_no_key_branch_revokes_every_active_row(
        self,
        ebull_test_conn: psycopg.Connection[Any],
        crypto_key: bytes,
    ) -> None:
        op_id = _insert_operator(ebull_test_conn)
        blob = _encrypted_blob(op_id, label="api_key", key=crypto_key)
        _insert_credential(ebull_test_conn, operator_id=op_id, label="api_key", ciphertext=blob)
        _insert_credential(ebull_test_conn, operator_id=op_id, label="user_key", ciphertext=blob)

        result = master_key._revoke_stale_ciphertext(ebull_test_conn, derived_key=None)
        assert result.no_key == 2
        assert _active_count(ebull_test_conn) == 0

    def test_mismatch_branch_revokes_only_mismatched_rows(
        self,
        ebull_test_conn: psycopg.Connection[Any],
        crypto_key: bytes,
    ) -> None:
        op_id = _insert_operator(ebull_test_conn)
        blob_under_key = _encrypted_blob(op_id, label="api_key", key=crypto_key)
        wrong_key = os.urandom(32)
        blob_under_wrong_key = _encrypted_blob(op_id, label="user_key", key=wrong_key)
        _insert_credential(ebull_test_conn, operator_id=op_id, label="api_key", ciphertext=blob_under_key)
        _insert_credential(
            ebull_test_conn,
            operator_id=op_id,
            label="user_key",
            ciphertext=blob_under_wrong_key,
        )

        result = master_key._revoke_stale_ciphertext(ebull_test_conn, derived_key=crypto_key)
        assert result.mismatch == 1
        assert result.no_key == 0
        assert result.orphan == 0
        assert _active_count(ebull_test_conn) == 1

    def test_already_revoked_rows_are_ignored(
        self,
        ebull_test_conn: psycopg.Connection[Any],
        crypto_key: bytes,
    ) -> None:
        op_id = _insert_operator(ebull_test_conn)
        blob = _encrypted_blob(op_id, label="api_key", key=crypto_key)
        _insert_credential(
            ebull_test_conn,
            operator_id=op_id,
            label="api_key",
            ciphertext=blob,
            revoked=True,
        )
        result = master_key._revoke_stale_ciphertext(ebull_test_conn, derived_key=None)
        assert result.total == 0

    def test_idempotent_under_repeated_calls(
        self,
        ebull_test_conn: psycopg.Connection[Any],
        crypto_key: bytes,
    ) -> None:
        op_id = _insert_operator(ebull_test_conn)
        blob = _encrypted_blob(op_id, label="api_key", key=crypto_key)
        _insert_credential(ebull_test_conn, operator_id=op_id, label="api_key", ciphertext=blob)

        first = master_key._revoke_stale_ciphertext(ebull_test_conn, derived_key=None)
        assert first.no_key == 1
        second = master_key._revoke_stale_ciphertext(ebull_test_conn, derived_key=None)
        assert second.total == 0


# ---------------------------------------------------------------------------
# Bootstrap end-to-end (env override + revoke pass)
# ---------------------------------------------------------------------------


class TestBootstrap:
    def test_function_is_callable(self) -> None:
        assert callable(bootstrap)

    def test_env_override_no_creds_is_clean_install(
        self,
        ebull_test_conn: psycopg.Connection[Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            master_key.settings,
            "secrets_key",
            base64.b64encode(os.urandom(32)).decode(),
        )
        # Empty broker_credentials in this fresh worker DB.
        result = bootstrap(ebull_test_conn)
        assert result.state == "clean_install"
        assert result.broker_encryption_key is not None

    def test_bootstrap_revokes_stale_rows_and_lands_clean_install(
        self,
        ebull_test_conn: psycopg.Connection[Any],
        crypto_key: bytes,
        isolated_data_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # No env override. No file. But cipher rows exist → revoke
        # fires (no-key branch) and bootstrap lands clean_install.
        monkeypatch.setattr(master_key.settings, "secrets_key", None)
        op_id = _insert_operator(ebull_test_conn)
        blob = _encrypted_blob(op_id, label="api_key", key=crypto_key)
        _insert_credential(ebull_test_conn, operator_id=op_id, label="api_key", ciphertext=blob)

        result = bootstrap(ebull_test_conn)
        assert result.state == "clean_install"
        assert result.broker_encryption_key is None
        assert _active_count(ebull_test_conn) == 0
