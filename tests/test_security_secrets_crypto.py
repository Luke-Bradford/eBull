"""Tests for app.security.secrets_crypto (issue #99 / refactored in #114)."""

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
    decode_env_key,
    decrypt,
    encrypt,
    set_active_key,
)


def _b64key(raw: bytes) -> str:
    return base64.b64encode(raw).decode()


@pytest.fixture
def good_key() -> Iterator[bytes]:
    """Install a fresh 32-byte broker-encryption key for the test."""
    key = os.urandom(32)
    set_active_key(key)
    yield key
    secrets_crypto._reset_for_tests()


# ---------------------------------------------------------------------------
# decode_env_key (env override decoding)
# ---------------------------------------------------------------------------


class TestDecodeEnvKey:
    def test_unset_raises(self) -> None:
        with pytest.raises(CredentialCryptoConfigError):
            decode_env_key(None)

    def test_empty_string_raises(self) -> None:
        with pytest.raises(CredentialCryptoConfigError):
            decode_env_key("")

    def test_non_base64_raises(self) -> None:
        with pytest.raises(CredentialCryptoConfigError):
            decode_env_key("not!base64!!!")

    def test_short_key_raises(self) -> None:
        with pytest.raises(CredentialCryptoConfigError):
            decode_env_key(_b64key(os.urandom(16)))

    def test_valid_key_returns_bytes(self) -> None:
        decoded = decode_env_key(_b64key(os.urandom(32)))
        assert len(decoded) == 32


# ---------------------------------------------------------------------------
# set_active_key / cache discipline
# ---------------------------------------------------------------------------


class TestActiveKeyCache:
    def test_encrypt_without_loaded_key_raises(self) -> None:
        secrets_crypto._reset_for_tests()
        with pytest.raises(CredentialCryptoConfigError):
            encrypt("x", operator_id=uuid4(), provider="etoro", label="l")

    def test_set_active_key_rejects_wrong_length(self) -> None:
        with pytest.raises(CredentialCryptoConfigError):
            set_active_key(b"\x00" * 16)
        secrets_crypto._reset_for_tests()


# ---------------------------------------------------------------------------
# Round trip
# ---------------------------------------------------------------------------


class TestRoundTrip:
    def test_round_trip(self, good_key: bytes) -> None:
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

    def test_blob_starts_with_random_nonce(self, good_key: bytes) -> None:
        op = uuid4()
        a = encrypt("x" * 32, operator_id=op, provider="etoro", label="l")
        b = encrypt("x" * 32, operator_id=op, provider="etoro", label="l")
        # Different nonces => different ciphertexts even for identical input.
        assert a != b


# ---------------------------------------------------------------------------
# AAD binding
# ---------------------------------------------------------------------------


class TestAADBinding:
    def test_wrong_operator_fails(self, good_key: bytes) -> None:
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

    def test_wrong_provider_fails(self, good_key: bytes) -> None:
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

    def test_wrong_label_fails(self, good_key: bytes) -> None:
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

    def test_wrong_key_version_fails(self, good_key: bytes) -> None:
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

    def test_wrong_key_fails(self, good_key: bytes) -> None:
        op = uuid4()
        blob = encrypt("s", operator_id=op, provider="etoro", label="l")
        # Swap to a different key and try to decrypt the previous blob.
        set_active_key(os.urandom(32))
        with pytest.raises(CredentialDecryptError):
            decrypt(
                blob,
                operator_id=op,
                provider="etoro",
                label="l",
                key_version=KEY_VERSION_CURRENT,
            )

    def test_truncated_blob_fails(self, good_key: bytes) -> None:
        with pytest.raises(CredentialDecryptError):
            decrypt(
                b"too-short",
                operator_id=uuid4(),
                provider="etoro",
                label="l",
                key_version=KEY_VERSION_CURRENT,
            )
