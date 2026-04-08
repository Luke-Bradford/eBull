"""Tests for app.security.secrets_crypto (issue #99)."""

from __future__ import annotations

import base64
import os
from collections.abc import Iterator
from uuid import uuid4

import pytest

from app.security import secrets_crypto
from app.security.secrets_crypto import (
    KEY_VERSION_CURRENT,
    CredentialCryptoConfigError,
    CredentialDecryptError,
    decrypt,
    encrypt,
    load_key,
)


def _b64key(raw: bytes) -> str:
    return base64.b64encode(raw).decode()


@pytest.fixture
def good_key(monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """Install a fresh 32-byte base64 key on settings for the test."""
    key_b64 = _b64key(os.urandom(32))
    monkeypatch.setattr(secrets_crypto.settings, "secrets_key", key_b64)
    secrets_crypto._reset_for_tests()
    yield key_b64
    secrets_crypto._reset_for_tests()


# ---------------------------------------------------------------------------
# load_key startup gate
# ---------------------------------------------------------------------------


class TestLoadKey:
    def test_unset_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(secrets_crypto.settings, "secrets_key", None)
        secrets_crypto._reset_for_tests()
        with pytest.raises(CredentialCryptoConfigError):
            load_key()

    def test_empty_string_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(secrets_crypto.settings, "secrets_key", "")
        secrets_crypto._reset_for_tests()
        with pytest.raises(CredentialCryptoConfigError):
            load_key()

    def test_non_base64_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(secrets_crypto.settings, "secrets_key", "not!base64!!!")
        secrets_crypto._reset_for_tests()
        with pytest.raises(CredentialCryptoConfigError):
            load_key()

    def test_short_key_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(secrets_crypto.settings, "secrets_key", _b64key(os.urandom(16)))
        secrets_crypto._reset_for_tests()
        with pytest.raises(CredentialCryptoConfigError):
            load_key()

    def test_valid_key_returns_bytes(self, good_key: str) -> None:
        key = load_key()
        assert len(key) == 32


# ---------------------------------------------------------------------------
# Round trip
# ---------------------------------------------------------------------------


class TestRoundTrip:
    def test_round_trip(self, good_key: str) -> None:
        op = uuid4()
        blob = encrypt(
            "super-secret",
            operator_id=op,
            provider="etoro",
            label="primary",
        )
        result = decrypt(
            blob,
            operator_id=op,
            provider="etoro",
            label="primary",
            key_version=KEY_VERSION_CURRENT,
        )
        assert result == "super-secret"

    def test_blob_starts_with_random_nonce(self, good_key: str) -> None:
        op = uuid4()
        a = encrypt("x" * 32, operator_id=op, provider="etoro", label="l")
        b = encrypt("x" * 32, operator_id=op, provider="etoro", label="l")
        # Different nonces => different ciphertexts even for identical input.
        assert a != b


# ---------------------------------------------------------------------------
# AAD binding
# ---------------------------------------------------------------------------


class TestAADBinding:
    def test_wrong_operator_fails(self, good_key: str) -> None:
        op = uuid4()
        blob = encrypt("s", operator_id=op, provider="etoro", label="l")
        with pytest.raises(CredentialDecryptError):
            decrypt(
                blob,
                operator_id=uuid4(),
                provider="etoro",
                label="l",
                key_version=KEY_VERSION_CURRENT,
            )

    def test_wrong_provider_fails(self, good_key: str) -> None:
        op = uuid4()
        blob = encrypt("s", operator_id=op, provider="etoro", label="l")
        with pytest.raises(CredentialDecryptError):
            decrypt(
                blob,
                operator_id=op,
                provider="not_etoro",
                label="l",
                key_version=KEY_VERSION_CURRENT,
            )

    def test_wrong_label_fails(self, good_key: str) -> None:
        op = uuid4()
        blob = encrypt("s", operator_id=op, provider="etoro", label="l")
        with pytest.raises(CredentialDecryptError):
            decrypt(
                blob,
                operator_id=op,
                provider="etoro",
                label="other",
                key_version=KEY_VERSION_CURRENT,
            )

    def test_wrong_key_version_fails(self, good_key: str) -> None:
        op = uuid4()
        blob = encrypt("s", operator_id=op, provider="etoro", label="l")
        with pytest.raises(CredentialDecryptError):
            decrypt(
                blob,
                operator_id=op,
                provider="etoro",
                label="l",
                key_version=KEY_VERSION_CURRENT + 1,
            )

    def test_wrong_key_fails(self, good_key: str, monkeypatch: pytest.MonkeyPatch) -> None:
        op = uuid4()
        blob = encrypt("s", operator_id=op, provider="etoro", label="l")
        # Swap to a different key and try to decrypt the previous blob.
        monkeypatch.setattr(secrets_crypto.settings, "secrets_key", _b64key(os.urandom(32)))
        secrets_crypto._reset_for_tests()
        with pytest.raises(CredentialDecryptError):
            decrypt(
                blob,
                operator_id=op,
                provider="etoro",
                label="l",
                key_version=KEY_VERSION_CURRENT,
            )

    def test_truncated_blob_fails(self, good_key: str) -> None:
        with pytest.raises(CredentialDecryptError):
            decrypt(
                b"too-short",
                operator_id=uuid4(),
                provider="etoro",
                label="l",
                key_version=KEY_VERSION_CURRENT,
            )
