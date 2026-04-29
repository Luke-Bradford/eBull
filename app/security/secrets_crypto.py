"""AES-256-GCM encryption for broker credentials (issue #99 / ADR 0001).

Design:
  * The 32-byte broker-encryption key is supplied at runtime by
    :mod:`app.security.master_key` (#114 / ADR-0003) via
    :func:`set_active_key`. The lifespan calls master_key.bootstrap()
    which either loads the key from the persisted root secret, accepts
    an ``EBULL_SECRETS_KEY`` env override, or leaves the cache empty
    if the server is in clean_install / recovery_required mode. This
    module no longer reads ``settings.secrets_key`` directly --
    bootstrap policy lives in master_key, not here.
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
import threading
from uuid import UUID

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

KEY_LEN = 32
NONCE_LEN = 12
KEY_VERSION_CURRENT = 1


class CredentialCryptoConfigError(RuntimeError):
    """Raised at startup when EBULL_SECRETS_KEY is missing or malformed."""


class MasterKeyNotLoadedError(CredentialCryptoConfigError):
    """Raised when ``encrypt`` / ``decrypt`` is called before
    :func:`set_active_key` (i.e. before ``master_key.bootstrap()``).

    Distinct from the env-key configuration errors above so the
    sync orchestrator's exception classifier can map it to the
    operator-actionable ``MASTER_KEY_MISSING`` category instead of
    the generic ``INTERNAL_ERROR`` "Unclassified error" banner.

    Inherits from :class:`CredentialCryptoConfigError` so existing
    callers that catch the parent class continue to handle it
    correctly (no behavior change for code paths that don't
    distinguish the subclass)."""


class CredentialDecryptError(Exception):
    """Raised when a ciphertext cannot be decrypted or authenticated."""


def decode_env_key(raw: str | None) -> bytes:
    """Decode and validate a base64-encoded EBULL_SECRETS_KEY value.

    Used by :mod:`app.security.master_key` when honouring the env
    override path. Lives here so the key-format rules stay co-located
    with the cipher that consumes them.
    """
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


_aesgcm: AESGCM | None = None
_aesgcm_lock = threading.Lock()


def set_active_key(key: bytes) -> None:
    """Install *key* (32 raw bytes) as the active broker-encryption key.

    Called by :mod:`app.security.master_key` during lifespan bootstrap,
    after a successful recovery, and after lazy generation on the first
    credential save. There is no auto-load fallback -- if the cache is
    empty when ``encrypt`` / ``decrypt`` runs, the call fails loudly.
    The HTTP layer is expected to gate broker routes behind
    ``require_master_key`` so the empty-cache case never reaches a
    handler in production.
    """
    if len(key) != KEY_LEN:
        raise CredentialCryptoConfigError(f"broker-encryption key must be exactly {KEY_LEN} bytes (got {len(key)})")
    global _aesgcm
    with _aesgcm_lock:
        _aesgcm = AESGCM(key)


def clear_active_key() -> None:
    """Drop the cached AESGCM primitive (used by recovery + tests)."""
    global _aesgcm
    with _aesgcm_lock:
        _aesgcm = None


def _get_aesgcm() -> AESGCM:
    cached = _aesgcm
    if cached is None:
        raise MasterKeyNotLoadedError(
            "broker-encryption key is not loaded -- master_key.bootstrap() must run before encrypt/decrypt is called"
        )
    return cached


def _reset_for_tests() -> None:
    """Clear the cached AESGCM so tests can swap the key mid-process."""
    clear_active_key()


def _build_aad(
    *,
    operator_id: UUID,
    provider: str,
    label: str,
    key_version: int,
) -> bytes:
    return f"{operator_id}|{provider}|{label}|{key_version}".encode()


# Public alias for in-package callers (master_key verification path)
# that need to build AAD without going through encrypt/decrypt. Keeps
# the AAD format definition in exactly one place.
build_aad = _build_aad


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
