"""Tests for app.security.master_key (#114 / ADR-0003).

Pure-function and filesystem tests only -- DB integration is exercised
through the broker_credentials API tests.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from app.security import master_key
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


@pytest.fixture
def isolated_data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point EBULL_DATA_DIR at a tmp dir for the test."""
    monkeypatch.setenv("EBULL_DATA_DIR", str(tmp_path))
    return tmp_path


class TestResolveDataDir:
    def test_env_var_wins(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("EBULL_DATA_DIR", str(tmp_path))
        assert resolve_data_dir() == tmp_path

    def test_settings_falls_back_to_platformdirs(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("EBULL_DATA_DIR", raising=False)
        monkeypatch.setattr(master_key.settings, "data_dir", None)
        # Just assert it returns *something* under the user profile,
        # not the exact path -- platformdirs is OS-specific.
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
        path.write_bytes(b"too-short")
        with pytest.raises(MasterKeyError):
            read_root_secret()

    def test_write_rejects_wrong_length(self, isolated_data_dir: Path) -> None:
        with pytest.raises(MasterKeyError):
            write_root_secret(b"\x00" * 16)

    def test_write_creates_dir(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        nested = tmp_path / "a" / "b" / "c"
        monkeypatch.setenv("EBULL_DATA_DIR", str(nested))
        write_root_secret(os.urandom(32))
        assert (nested / "root_secret.bin").exists()

    def test_atomic_write_temp_file_in_dest_dir(self, isolated_data_dir: Path) -> None:
        """Temp file must live in the destination dir for an atomic rename.

        We can't observe the temp file directly (it's renamed before
        write_root_secret returns), but we can confirm no temp file
        leaks on the happy path and that the resulting file lives at
        the canonical path.
        """
        write_root_secret(os.urandom(32))
        leaked = [p for p in isolated_data_dir.iterdir() if p.name.startswith(".root_secret.")]
        assert leaked == []
        assert (isolated_data_dir / "root_secret.bin").exists()


class TestDeriveKey:
    def test_deterministic(self) -> None:
        secret = b"\x01" * 32
        a = derive_broker_encryption_key(secret)
        b = derive_broker_encryption_key(secret)
        assert a == b
        assert len(a) == 32

    def test_different_secrets_yield_different_keys(self) -> None:
        a = derive_broker_encryption_key(b"\x01" * 32)
        b = derive_broker_encryption_key(b"\x02" * 32)
        assert a != b

    def test_wrong_length_rejected(self) -> None:
        with pytest.raises(MasterKeyError):
            derive_broker_encryption_key(b"\x00" * 16)


class TestComputeBootState:
    def test_no_credentials_is_clean_install(self) -> None:
        assert (
            compute_boot_state(
                operators_empty=True,
                credentials_exist=False,
                root_secret_present=False,
                key_matches=False,
            )
            == "clean_install"
        )

    def test_no_credentials_with_stale_file_still_clean_install(self) -> None:
        assert (
            compute_boot_state(
                operators_empty=False,
                credentials_exist=False,
                root_secret_present=True,
                key_matches=False,
            )
            == "clean_install"
        )

    def test_credentials_and_matching_key_is_normal(self) -> None:
        assert (
            compute_boot_state(
                operators_empty=False,
                credentials_exist=True,
                root_secret_present=True,
                key_matches=True,
            )
            == "normal"
        )

    def test_credentials_no_file_is_recovery_required(self) -> None:
        assert (
            compute_boot_state(
                operators_empty=False,
                credentials_exist=True,
                root_secret_present=False,
                key_matches=False,
            )
            == "recovery_required"
        )

    def test_credentials_mismatched_file_is_recovery_required(self) -> None:
        assert (
            compute_boot_state(
                operators_empty=False,
                credentials_exist=True,
                root_secret_present=True,
                key_matches=False,
            )
            == "recovery_required"
        )


class TestBootResult:
    def test_dataclass_is_frozen(self) -> None:
        r = BootResult(
            state="normal",
            broker_encryption_key=b"\x00" * 32,
            needs_setup=False,
            recovery_required=False,
        )
        with pytest.raises(Exception):
            r.state = "clean_install"  # type: ignore[misc]


class TestGenerateSplit:
    """The two-phase generate -> persist API exists so a DB error after
    key install never leaves the operator with a persisted root secret
    whose phrase they never saw (review-prevention from PR #118)."""

    def test_in_memory_does_not_touch_disk(self, isolated_data_dir: Path) -> None:
        master_key.generate_root_secret_in_memory()
        assert not (isolated_data_dir / "root_secret.bin").exists()

    def test_persist_writes_file(self, isolated_data_dir: Path) -> None:
        secret, _, _ = master_key.generate_root_secret_in_memory()
        master_key.persist_generated_root_secret(secret)
        assert (isolated_data_dir / "root_secret.bin").read_bytes() == secret


@pytest.mark.skipif(os.name == "nt", reason="POSIX-only perms check")
def test_data_dir_locked_to_0700(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EBULL_DATA_DIR", str(tmp_path / "secrets"))
    write_root_secret(os.urandom(32))
    mode = (tmp_path / "secrets").stat().st_mode & 0o777
    assert mode == 0o700


class TestEnvOverrideBootState:
    """Bootstrap with EBULL_SECRETS_KEY set is still clean_install when
    no credentials exist (review-prevention from PR #118 round 2)."""

    def test_env_override_no_creds_is_clean_install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import base64

        from app.security import master_key as mk

        # Stub out the DB by patching the credential-existence helper.
        monkeypatch.setattr(mk, "_credentials_exist", lambda conn: False)
        monkeypatch.setattr(
            mk.settings,
            "secrets_key",
            base64.b64encode(os.urandom(32)).decode(),
        )
        result = mk.bootstrap(conn=None)  # type: ignore[arg-type]
        assert result.state == "clean_install"
        assert result.needs_setup is True
        assert result.recovery_required is False
        assert result.broker_encryption_key is not None


def test_bootstrap_function_exists() -> None:
    """Smoke test: bootstrap is importable and callable.

    Full DB-integration coverage lives in the broker_credentials API
    tests, which exercise the lifespan path end-to-end.
    """
    assert callable(bootstrap)


class TestRecoverFromPhraseGuards:
    """State-machine guards on recover_from_phrase (#118 round 8)."""

    def test_no_active_credential_rejected(self, isolated_data_dir: Path) -> None:
        """Recovery must refuse when there is nothing to verify against.

        ``_key_decrypts_newest_credential`` returns True on a missing
        row (vacuous match) which would otherwise let an operator
        install an arbitrary key in a no-credentials state. The
        recovery flow has its own caller-side guard that catches
        this; this test pins it.
        """
        from unittest.mock import MagicMock

        from app.security.master_key import (
            RecoveryVerificationError,
            generate_root_secret_in_memory,
            recover_from_phrase,
        )

        _, _, phrase_words = generate_root_secret_in_memory()
        phrase = " ".join(phrase_words)
        # File from the in-memory generation has not been written;
        # nothing else on disk either.

        # Cursor returns an empty result -> _newest_active_credential
        # returns None -> recover_from_phrase must raise.
        cur = MagicMock()
        cur.__enter__ = MagicMock(return_value=cur)
        cur.__exit__ = MagicMock(return_value=False)
        cur.fetchone.return_value = None
        conn = MagicMock()
        conn.cursor.return_value = cur

        app_state = MagicMock()
        app_state.broker_key_loaded = False

        with pytest.raises(RecoveryVerificationError, match="no active credential"):
            recover_from_phrase(conn, phrase, app_state)

        # State must NOT have flipped on the failure path.
        assert app_state.broker_key_loaded is False
        # And the file must NOT have been written.
        assert not root_secret_path().exists()

    def test_bad_checksum_phrase_leaves_no_state(self, isolated_data_dir: Path) -> None:
        """A phrase that passes word-count but fails checksum must
        leave no file on disk and not flip any app_state flags
        (review feedback PR #118 round 13).
        """
        from unittest.mock import MagicMock

        from app.security.master_key import recover_from_phrase
        from app.security.recovery_phrase import RecoveryPhraseError

        # 24 valid BIP39 words but the wrong checksum word at the
        # end. "abandon" * 24 has a valid checksum (it's the
        # canonical zero-entropy phrase). Replacing the final
        # word with a different valid wordlist entry breaks the
        # checksum.
        phrase = " ".join(["abandon"] * 23 + ["zoo"])

        conn = MagicMock()
        app_state = MagicMock()
        app_state.broker_key_loaded = False

        with pytest.raises(RecoveryPhraseError):
            recover_from_phrase(conn, phrase, app_state)

        assert app_state.broker_key_loaded is False
        assert not root_secret_path().exists()
        # Lock was never even acquired -- conn must not have
        # been touched at all (any attribute access on a
        # MagicMock auto-creates a child mock, so we assert on
        # mock_calls being empty rather than on cursor.called).
        assert conn.mock_calls == []
