"""Master key bootstrap (#114 / ADR-0003, amended 2026-05-07).

Owns the lifecycle of the root secret that backs broker credential
encryption. The root secret is a 32-byte random value persisted to a
local file under the app data dir. The broker-encryption key used by
:mod:`app.security.secrets_crypto` is derived from the root secret via
HKDF-SHA256 with a fixed ``info`` label.

Boot states (post-amendment, two-state machine):
  * ``clean_install`` — no broker credentials yet (or all stale rows
                        soft-revoked at boot). The lazy-gen path runs
                        on the first credential save.
  * ``normal``        — root secret present and matches existing
                        ciphertext rows; the derived key is loaded into
                        ``app.state``.

The pre-amendment ``recovery_required`` state and 24-word phrase
ceremony are removed. When the bootstrap finds existing
``broker_credentials`` rows that cannot be decrypted under the current
key (no key, mismatch, or orphan operator), it **soft-revokes** them at
boot via ``UPDATE broker_credentials SET revoked_at = NOW()`` — audit
history is preserved (revoked rows stay; access log FK intact). One
``NOTIFY ebull_credential_health`` per affected operator wakes the
credential-health cache so live subscribers refresh without polling.

This module is deliberately I/O-light: file ops + HKDF + a couple of
DB SELECTs/UPDATEs. Boot-state computation is a pure function so it
can be unit-tested without touching the filesystem.
"""

from __future__ import annotations

import json
import logging
import os
import secrets
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from uuid import UUID

import psycopg
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from platformdirs import user_data_dir

from app.config import settings
from app.security.secrets_crypto import decode_env_key

logger = logging.getLogger(__name__)

ROOT_SECRET_FILENAME = "root_secret.bin"
ROOT_SECRET_LEN = 32
_HKDF_INFO = b"ebull-broker-encryption-key-v1"
_DERIVED_KEY_LEN = 32

# Channel name must match app.services.credential_health.NOTIFY_CHANNEL.
# Inlined here to avoid a circular import at lifespan time
# (services.credential_health imports app.db, which imports app.config).
_CREDENTIAL_HEALTH_NOTIFY_CHANNEL = "ebull_credential_health"

BootState = Literal["clean_install", "normal"]

# Serialises lazy generation of the root secret on the first credential
# save (review-prevention: concurrent first-save race surfaced on PR
# #118). Two simultaneous create-credential requests in clean_install
# mode would otherwise each generate a distinct root secret and the
# subsequent ciphertext rows would be unrecoverable under the
# ultimately-persisted key.
lazy_gen_lock = threading.Lock()


class MasterKeyError(RuntimeError):
    """Raised when the master key cannot be loaded."""


@dataclass(frozen=True)
class BootResult:
    state: BootState
    broker_encryption_key: bytes | None


# ---------------------------------------------------------------------------
# Data dir + file IO
# ---------------------------------------------------------------------------


def resolve_data_dir() -> Path:
    """Return the directory holding the root secret file.

    Resolution order:
      1. ``EBULL_DATA_DIR`` env var
      2. ``settings.data_dir``
      3. ``platformdirs.user_data_dir("eBull")``
    """
    raw = os.environ.get("EBULL_DATA_DIR") or settings.data_dir
    if raw:
        return Path(raw)
    return Path(user_data_dir("eBull"))


def root_secret_path() -> Path:
    return resolve_data_dir() / ROOT_SECRET_FILENAME


def read_root_secret() -> bytes | None:
    """Return the persisted root secret, or None if the file is absent."""
    path = root_secret_path()
    if not path.exists():
        return None
    data = path.read_bytes()
    if len(data) != ROOT_SECRET_LEN:
        raise MasterKeyError(f"root secret file {path} is corrupt: expected {ROOT_SECRET_LEN} bytes, got {len(data)}")
    return data


def write_root_secret(root_secret: bytes) -> Path:
    """Atomically write the root secret to disk with mode 0600.

    The temp file is created in the *same destination directory* (not
    ``$TMPDIR``) so the final ``os.replace`` is a same-filesystem
    rename and therefore atomic. A cross-filesystem rename would
    silently fall back to copy+unlink and could leave a half-written
    file visible.
    """
    if len(root_secret) != ROOT_SECRET_LEN:
        raise MasterKeyError(f"root secret must be exactly {ROOT_SECRET_LEN} bytes (got {len(root_secret)})")
    dest_dir = resolve_data_dir()
    dest_dir.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(dest_dir, 0o700)
    except OSError:
        pass
    dest = dest_dir / ROOT_SECRET_FILENAME

    fd, tmp_path_str = tempfile.mkstemp(prefix=".root_secret.", suffix=".tmp", dir=str(dest_dir))
    tmp_path = Path(tmp_path_str)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(root_secret)
            f.flush()
            os.fsync(f.fileno())
        try:
            os.chmod(tmp_path, 0o600)
        except OSError:
            pass
        os.replace(tmp_path, dest)
    except Exception:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass
        raise
    return dest


# ---------------------------------------------------------------------------
# Key derivation
# ---------------------------------------------------------------------------


def derive_broker_encryption_key(root_secret: bytes) -> bytes:
    """HKDF-SHA256 derive the 32-byte broker-encryption key from *root_secret*."""
    if len(root_secret) != ROOT_SECRET_LEN:
        raise MasterKeyError(f"root secret must be exactly {ROOT_SECRET_LEN} bytes (got {len(root_secret)})")
    hkdf = HKDF(
        algorithm=hashes.SHA256(),
        length=_DERIVED_KEY_LEN,
        salt=None,
        info=_HKDF_INFO,
    )
    return hkdf.derive(root_secret)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def _credentials_exist(conn: psycopg.Connection[object]) -> bool:
    """True if any non-orphan, non-revoked broker credential row exists.

    Orphan rows (operator_id no longer in ``operators``) are excluded so
    a wiped operators table does not falsely report credentials present.
    Used by ``compute_boot_state`` after stale-revoke has run, so the
    only rows reaching this query are decryptable + operator-owned.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
              FROM broker_credentials bc
              JOIN operators o ON o.operator_id = bc.operator_id
             WHERE bc.revoked_at IS NULL
             LIMIT 1
            """
        )
        return cur.fetchone() is not None


def _key_decrypts_row(row: dict[str, object], candidate_key: bytes) -> bool:
    """True iff *candidate_key* successfully decrypts *row*'s ciphertext."""
    from cryptography.exceptions import InvalidTag
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    from app.security.secrets_crypto import NONCE_LEN, build_aad

    blob = bytes(row["ciphertext"])  # type: ignore[arg-type]
    if len(blob) < NONCE_LEN + 16:
        return False
    aad = build_aad(
        operator_id=UUID(str(row["operator_id"])),
        provider=str(row["provider"]),
        label=str(row["label"]),
        key_version=int(row["key_version"]),  # type: ignore[arg-type]
    )
    try:
        AESGCM(candidate_key).decrypt(blob[:NONCE_LEN], blob[NONCE_LEN:], aad)
    except InvalidTag:
        return False
    return True


@dataclass(frozen=True)
class _RevokeBreakdown:
    """Per-class counts for a stale-cipher soft-revoke pass."""

    orphan: int
    no_key: int
    mismatch: int

    @property
    def total(self) -> int:
        return self.orphan + self.no_key + self.mismatch


def _revoke_stale_ciphertext(
    conn: psycopg.Connection[object],
    derived_key: bytes | None,
) -> _RevokeBreakdown:
    """Soft-revoke broker_credentials rows that cannot be decrypted now.

    Three classes (per ADR-0003 amendment 2026-05-07):

      1. Orphan: ``operator_id`` no longer exists in ``operators``.
      2. No key: ``derived_key is None`` (no on-disk file, no env
         override). Every operator-owned active row is unrecoverable.
      3. Mismatch: ``derived_key`` is set but cannot decrypt the row.

    Action: ``UPDATE broker_credentials SET revoked_at = NOW() WHERE
    id = ANY(:stale_ids)``. Audit trail preserved.

    Idempotent: a second pass finds zero stale rows because the
    ``revoked_at IS NULL`` filter excludes already-revoked rows.

    For each affected operator, issues one ``NOTIFY
    ebull_credential_health`` so live credential-health caches refresh
    without waiting for a per-row event.

    Returns a per-class breakdown for logging/test assertions.
    """
    import psycopg.rows

    orphan_ids: list[UUID] = []
    no_key_ids: list[UUID] = []
    mismatch_ids: list[UUID] = []

    # Fetch every active row joined to operator presence; we decide
    # per-row which class it falls into.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT bc.id, bc.operator_id, bc.provider, bc.label,
                   bc.ciphertext, bc.key_version,
                   (o.operator_id IS NULL) AS is_orphan
              FROM broker_credentials bc
              LEFT JOIN operators o ON o.operator_id = bc.operator_id
             WHERE bc.revoked_at IS NULL
            """
        )
        rows = cur.fetchall()

    for row in rows:
        if bool(row["is_orphan"]):
            orphan_ids.append(UUID(str(row["id"])))
            continue
        if derived_key is None:
            no_key_ids.append(UUID(str(row["id"])))
            continue
        if not _key_decrypts_row(row, derived_key):
            mismatch_ids.append(UUID(str(row["id"])))

    breakdown = _RevokeBreakdown(
        orphan=len(orphan_ids),
        no_key=len(no_key_ids),
        mismatch=len(mismatch_ids),
    )
    if breakdown.total == 0:
        return breakdown

    stale_ids = orphan_ids + no_key_ids + mismatch_ids
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE broker_credentials
               SET revoked_at = NOW()
             WHERE id = ANY(%s)
            """,
            (stale_ids,),
        )
        # One NOTIFY per affected operator so the existing
        # ``ebull_credential_health`` listener (payload contract:
        # JSON with ``operator_id`` key) refreshes per-operator
        # aggregate health. Bulk single NOTIFY would be ignored by
        # the listener.
        cur.execute(
            """
            SELECT DISTINCT operator_id
              FROM broker_credentials
             WHERE id = ANY(%s)
            """,
            (stale_ids,),
        )
        affected_ops: list[str] = []
        for r in cur.fetchall():
            row_tuple: tuple[object, ...] = r  # type: ignore[assignment]
            affected_ops.append(str(row_tuple[0]))
        for op_id in affected_ops:
            payload = json.dumps({"operator_id": op_id, "reason": "stale_cipher_revoke"})
            cur.execute(
                "SELECT pg_notify(%s, %s)",
                (_CREDENTIAL_HEALTH_NOTIFY_CHANNEL, payload),
            )

    logger.warning(
        "stale-cipher soft-revoke: orphan=%d no_key=%d mismatch=%d affected_operators=%d",
        breakdown.orphan,
        breakdown.no_key,
        breakdown.mismatch,
        len(affected_ops),
    )
    return breakdown


# ---------------------------------------------------------------------------
# Boot state computation
# ---------------------------------------------------------------------------


def compute_boot_state(
    *,
    credentials_exist: bool,
    root_secret_present: bool,
) -> BootState:
    """Pure boot-state computation (post-amendment, two-state).

    * No credentials → clean_install.
    * Credentials exist + key present → normal.
    * Credentials exist + key missing → clean_install (stale-revoke
      ran upstream of this call so any non-decryptable rows are
      already soft-revoked; if anything still survives without a key
      file present, it is a logic error elsewhere — this branch
      conservatively falls through to clean_install).
    """
    if not credentials_exist:
        return "clean_install"
    if root_secret_present:
        return "normal"
    return "clean_install"


# ---------------------------------------------------------------------------
# Bootstrap entry point (called from lifespan)
# ---------------------------------------------------------------------------


def bootstrap(conn: psycopg.Connection[object]) -> BootResult:
    """Compute the boot state and return the in-memory broker key (if any).

    Called once from the FastAPI lifespan after the connection pool is
    open and before ``yield``. Runs the stale-cipher soft-revoke pass
    unconditionally before computing boot state — cheap UPDATE, no-op
    when nothing matches.

    Never raises on a missing file. Does raise :class:`MasterKeyError`
    if the env override is set but does not match existing ciphertext
    (fail-loud, ADR-0003 §9 contract preserved).
    """
    # Resolve the active key candidate (env override wins over file).
    if settings.secrets_key:
        derived: bytes | None = decode_env_key(settings.secrets_key)
        root_secret_present_for_state = True
    else:
        root_secret = read_root_secret()
        derived = derive_broker_encryption_key(root_secret) if root_secret is not None else None
        root_secret_present_for_state = derived is not None

    # Stale-revoke pass before any existence/state logic. Safe to run
    # even on a fresh DB — returns a zero breakdown.
    _revoke_stale_ciphertext(conn, derived)

    creds_exist = _credentials_exist(conn)

    # Env override + existing creds + verified key → preserved fail-loud
    # behaviour from ADR-0003 §9: if the operator explicitly set
    # EBULL_SECRETS_KEY and any survivor row cannot be decrypted under
    # it, that is a misconfiguration, not a recovery scenario.
    if settings.secrets_key and creds_exist and derived is not None:
        if not _newest_active_decryptable(conn, derived):
            raise MasterKeyError(
                "EBULL_SECRETS_KEY does not match existing broker credential ciphertext. "
                "Refusing to start. See ADR-0003 §9."
            )

    state = compute_boot_state(
        credentials_exist=creds_exist,
        root_secret_present=root_secret_present_for_state,
    )
    # When `EBULL_SECRETS_KEY` is set, the derived key is always
    # available for lazy-gen-on-first-save and must be installed
    # regardless of boot state. Without the env override, we only
    # install the key when state == "normal" (i.e. matching ciphertext
    # exists) — clean_install with no env override returns None so the
    # first credential save runs the lazy-gen path.
    install_key = settings.secrets_key is not None or state == "normal"
    return BootResult(
        state=state,
        broker_encryption_key=derived if install_key else None,
    )


def _newest_active_decryptable(
    conn: psycopg.Connection[object],
    candidate_key: bytes,
) -> bool:
    """True iff the newest active credential decrypts under *candidate_key*.

    Used by the env-override fail-loud check. Stale-revoke has already
    run, so any active row reaching this query should decrypt — but we
    re-check to make the env-override contract explicit.
    """
    import psycopg.rows

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT bc.id, bc.operator_id, bc.provider, bc.label,
                   bc.ciphertext, bc.key_version
              FROM broker_credentials bc
              JOIN operators o ON o.operator_id = bc.operator_id
             WHERE bc.revoked_at IS NULL
             ORDER BY bc.created_at DESC, bc.id DESC
             LIMIT 1
            """
        )
        row = cur.fetchone()
    if row is None:
        return True
    return _key_decrypts_row(row, candidate_key)


# ---------------------------------------------------------------------------
# Lazy generation (first credential save)
# ---------------------------------------------------------------------------


def generate_root_secret_in_memory() -> tuple[bytes, bytes]:
    """Generate a fresh root secret WITHOUT persisting it yet.

    Returns ``(root_secret, derived_key)``. The caller is expected to:

      1. Install ``derived_key`` into the secrets_crypto cache.
      2. Attempt the DB write (e.g. ``store_credential``).
      3. On success, call :func:`persist_generated_root_secret` to
         flush the file to disk.
      4. On failure, call the rollback path to clear the cache so the
         next attempt re-runs the gen-then-store dance fresh.

    Post-amendment 2026-05-07: no recovery phrase is generated. The
    operator never sees the underlying material.
    """
    root_secret = secrets.token_bytes(ROOT_SECRET_LEN)
    derived = derive_broker_encryption_key(root_secret)
    return root_secret, derived


def persist_generated_root_secret(root_secret: bytes) -> Path:
    """Flush a previously-generated root secret to disk."""
    return write_root_secret(root_secret)
