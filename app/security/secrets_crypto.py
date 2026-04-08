"""AES-256-GCM encryption for broker credentials (issue #99 / ADR 0001).

Design:
  * Single key loaded from ``settings.secrets_key`` (base64, 32 bytes
    after decode). Validated at startup via :func:`load_key`; if the key
    is missing, malformed, or the wrong length the server refuses to
    start. We never fall back to a generated key -- doing so would lock
    existing ciphertext rows out on the next restart.
  * Every ciphertext row carries a ``key_version`` column. The version is
    part of the AEAD additional-authenticated-data (AAD) string so a row
    written under version N cannot be decrypted under version M. Rotation
    is manual: add a new version, re-encrypt outstanding rows, remove the
    old key, all documented in ADR 0001.
  * AAD binds four values: ``f"{operator_id}|{provider}|{label}|{key_version}"``.
    Cross-row, cross-operator, cross-provider, cross-label and cross-key-
    version decryptions all fail with :class:`CredentialDecryptError`.
  * Output layout: ``nonce (12 bytes) || ciphertext || GCM tag``. Callers
    store the concatenation; :func:`decrypt` splits the nonce off the
    front.

This module is pure: no DB, no logging of plaintext, no globals outside
the key-load function.
"""

from __future__ import annotations

import base64
import binascii
import os
from uuid import UUID

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from app.config import settings

KEY_LEN = 32
NONCE_LEN = 12
KEY_VERSION_CURRENT = 1


class CredentialCryptoConfigError(RuntimeError):
    """Raised at startup when EBULL_SECRETS_KEY is missing or malformed."""


class CredentialDecryptError(Exception):
    """Raised when a ciphertext cannot be decrypted or authenticated."""


def _decode_key(raw: str | None) -> bytes:
    if raw is None or raw == "":
        raise CredentialCryptoConfigError(
            "EBULL_SECRETS_KEY is not set. Generate with: "
            'python -c "import os, base64; print(base64.b64encode(os.urandom(32)).decode())"'
        )
    try:
        decoded = base64.b64decode(raw, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise CredentialCryptoConfigError("EBULL_SECRETS_KEY is not valid base64") from exc
    if len(decoded) != KEY_LEN:
        raise CredentialCryptoConfigError(
            f"EBULL_SECRETS_KEY must decode to exactly {KEY_LEN} bytes (got {len(decoded)})"
        )
    return decoded


def load_key() -> bytes:
    """Decode and validate the secrets key. Call once at startup.

    Raises :class:`CredentialCryptoConfigError` on any problem. This is
    intentionally not cached: callers that need it hot should call
    :func:`_get_aesgcm`, which wraps the decoded key in an ``AESGCM``
    primitive the first time it is asked.
    """
    return _decode_key(settings.secrets_key)


_aesgcm: AESGCM | None = None


def _get_aesgcm() -> AESGCM:
    global _aesgcm
    if _aesgcm is None:
        _aesgcm = AESGCM(load_key())
    return _aesgcm


def _reset_for_tests() -> None:
    """Clear the cached AESGCM so tests can swap the key mid-process."""
    global _aesgcm
    _aesgcm = None


def _build_aad(
    *,
    operator_id: UUID,
    provider: str,
    label: str,
    key_version: int,
) -> bytes:
    return f"{operator_id}|{provider}|{label}|{key_version}".encode()


def encrypt(
    plaintext: str,
    *,
    operator_id: UUID,
    provider: str,
    label: str,
    key_version: int = KEY_VERSION_CURRENT,
) -> bytes:
    """Encrypt *plaintext* and return ``nonce || ciphertext || tag`` bytes."""
    aesgcm = _get_aesgcm()
    nonce = os.urandom(NONCE_LEN)
    aad = _build_aad(
        operator_id=operator_id,
        provider=provider,
        label=label,
        key_version=key_version,
    )
    ct = aesgcm.encrypt(nonce, plaintext.encode("utf-8"), aad)
    return nonce + ct


def decrypt(
    blob: bytes,
    *,
    operator_id: UUID,
    provider: str,
    label: str,
    key_version: int,
) -> str:
    """Decrypt a stored ciphertext blob back to plaintext.

    Raises :class:`CredentialDecryptError` on any authentication failure
    (wrong key, wrong AAD, truncated blob, tampered ciphertext).
    """
    if len(blob) < NONCE_LEN + 16:
        raise CredentialDecryptError("ciphertext too short")
    nonce, ct = blob[:NONCE_LEN], blob[NONCE_LEN:]
    aad = _build_aad(
        operator_id=operator_id,
        provider=provider,
        label=label,
        key_version=key_version,
    )
    try:
        aesgcm = _get_aesgcm()
        pt = aesgcm.decrypt(nonce, ct, aad)
    except InvalidTag as exc:
        raise CredentialDecryptError("authentication failed") from exc
    return pt.decode("utf-8")
