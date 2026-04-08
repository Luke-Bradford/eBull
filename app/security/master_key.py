"""Master key bootstrap and recovery (#114 / ADR-0003).

Owns the lifecycle of the root secret that backs broker credential
encryption. The root secret is a 32-byte random value persisted to a
local file under the app data dir. The broker-encryption key used by
:mod:`app.security.secrets_crypto` is derived from the root secret via
HKDF-SHA256 with a fixed ``info`` label, so the on-disk file and the
24-word recovery phrase are interchangeable representations of the same
secret -- a wiped file can be reconstructed from the phrase, and a
fresh phrase can be reissued from the file.

Boot states (per ADR-0003 §5):
  * ``clean_install``     -- no operators yet; first-run setup will
                             generate the root secret lazily on the
                             first credential save.
  * ``normal``            -- root secret present and matches the
                             existing ciphertext rows; the derived key
                             is loaded into ``app.state``.
  * ``recovery_required`` -- credentials exist but the root secret file
                             is missing or does not match. The app
                             refuses to load the encryption key and the
                             frontend is routed to /recover.

This module is deliberately I/O-light: file ops + HKDF + a couple of
DB SELECTs for verification. Boot-state computation is a pure function
so it can be unit-tested without touching the filesystem.
"""

from __future__ import annotations

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
from app.security.recovery_phrase import (
    ROOT_SECRET_LEN,
    decode_phrase,
    encode_phrase,
)
from app.security.secrets_crypto import decode_env_key, set_active_key

logger = logging.getLogger(__name__)

ROOT_SECRET_FILENAME = "root_secret.bin"
_HKDF_INFO = b"ebull-broker-encryption-key-v1"
_DERIVED_KEY_LEN = 32

BootState = Literal["clean_install", "normal", "recovery_required"]

# Serialises lazy generation of the root secret on the first credential
# save (review-prevention: concurrent first-save race surfaced on PR
# #118). Two simultaneous create-credential requests in clean_install
# mode would otherwise each generate a distinct root secret and the
# operator would only ever see one valid recovery phrase.
lazy_gen_lock = threading.Lock()


class MasterKeyError(RuntimeError):
    """Raised when the master key cannot be loaded or recovered."""


class RecoveryVerificationError(MasterKeyError):
    """Raised when a recovery phrase decodes but does not match stored ciphertext."""


class RecoveryNotApplicableError(MasterKeyError):
    """Raised when recovery is requested but there is no active credential to verify against.

    Distinct from :class:`RecoveryVerificationError` purely so the
    server-side log clearly distinguishes "no row to verify against"
    from "row exists but the derived key did not match". On the wire
    this class is folded into the same generic 400 ``recovery phrase
    invalid`` response as every other phrase-path failure -- ADR-0003
    §6 forbids returning a distinct status code here because it would
    let a caller fingerprint the failure mode by status alone (review
    feedback PR #118 round 18, correcting the round 17 docstring that
    referenced a 409 mapping which was never shipped).
    """


@dataclass(frozen=True)
class BootResult:
    state: BootState
    broker_encryption_key: bytes | None
    needs_setup: bool
    recovery_required: bool


# ---------------------------------------------------------------------------
# Data dir + file IO
# ---------------------------------------------------------------------------


def resolve_data_dir() -> Path:
    """Return the directory holding the root secret file.

    Resolution order (per ADR-0003 §6):
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
    rename and therefore atomic. ADR-0003 §6 makes this an explicit
    requirement -- a cross-filesystem rename would silently fall back
    to copy+unlink and could leave a half-written file visible.
    """
    if len(root_secret) != ROOT_SECRET_LEN:
        raise MasterKeyError(f"root secret must be exactly {ROOT_SECRET_LEN} bytes (got {len(root_secret)})")
    dest_dir = resolve_data_dir()
    dest_dir.mkdir(parents=True, exist_ok=True)
    try:
        # Lock down directory perms on POSIX. Best-effort on Windows
        # (chmod with non-write bits is mostly a no-op there); the dir
        # lives under the user profile data dir which is already
        # user-private.
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
            # Windows perms are best-effort; the file lives under the
            # user's profile data dir which is already user-private.
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
    a wiped operators table does not hard-fail bootstrap on stale
    ciphertext that nobody can ever own again.
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


def _newest_active_credential(
    conn: psycopg.Connection[object],
) -> dict[str, object] | None:
    """Return the newest non-orphan, non-revoked credential row, or None.

    Determinism: ``ORDER BY created_at DESC, id DESC LIMIT 1``. The id
    tiebreaker is lexicographic on UUID, not a real recency signal --
    it just guarantees the same row is picked across processes and
    across env-override and recovery verification paths (ADR-0003 §6).
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
        return cur.fetchone()


def _key_decrypts_row(row: dict[str, object], candidate_key: bytes) -> bool:
    """True iff *candidate_key* successfully decrypts *row*'s ciphertext.

    Pure verification helper: takes an explicit row so the caller
    controls exactly which credential is being checked. Used by
    :func:`_key_decrypts_newest_credential` (env-override boot path)
    and :func:`recover_from_phrase` (recovery path), both of which
    pass a row they have already fetched -- this avoids a
    double-fetch race where a concurrent revocation between two
    `_newest_active_credential` calls could cause one call to see
    the row and the next to see ``None`` (review feedback PR #118
    round 9).
    """
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


def _key_decrypts_newest_credential(conn: psycopg.Connection[object], candidate_key: bytes) -> bool:
    """True iff *candidate_key* successfully decrypts the newest credential.

    Used by the env-override verification path during boot. Builds a
    *local* AESGCM primitive (via ``_key_decrypts_row``) instead of
    swapping the global cache, so a candidate key can never serve a
    concurrent live request during boot-time verification (review-
    prevention: AESGCM-swap race surfaced on PR #118).

    Field coercions: psycopg returns ``operator_id`` as :class:`UUID`
    and ``key_version`` as ``int``; both are stringified into the AAD
    via f-string in :func:`secrets_crypto._build_aad`. We coerce
    explicitly in ``_key_decrypts_row`` so a future row-factory
    change cannot silently flip the AAD encoding and produce a
    false-negative on a valid key.
    """
    row = _newest_active_credential(conn)
    if row is None:
        # No verifiable row -- vacuously "matches". This branch is
        # ONLY safe for callers that have already established the
        # context (env-override boot validation against an
        # all-orphans / clean-install state). The recovery path
        # uses ``_key_decrypts_row`` directly with a row it has
        # already fetched and guarded, so it never reaches this
        # short-circuit (review feedback PR #118 round 8).
        return True

    return _key_decrypts_row(row, candidate_key)


# ---------------------------------------------------------------------------
# Boot state computation
# ---------------------------------------------------------------------------


def compute_boot_state(
    *,
    operators_empty: bool,
    credentials_exist: bool,
    root_secret_present: bool,
    key_matches: bool,
) -> BootState:
    """Pure boot-state computation matching ADR-0003 §5.

    * No credentials at all -> clean_install (regardless of file state;
      a stale file is harmless because nothing depends on it yet).
    * Credentials exist + key matches -> normal.
    * Credentials exist + key missing or mismatched -> recovery_required.

    The ``operators_empty`` flag is accepted for symmetry with the
    first-run setup path but does not change the decision -- bootstrap
    state is driven by ciphertext, not by operator rows. ADR-0003 §5
    rows.
    """
    del operators_empty  # reserved for future use; see ADR-0003 §5
    if not credentials_exist:
        return "clean_install"
    if root_secret_present and key_matches:
        return "normal"
    return "recovery_required"


# ---------------------------------------------------------------------------
# Bootstrap entry point (called from lifespan)
# ---------------------------------------------------------------------------


def bootstrap(conn: psycopg.Connection[object]) -> BootResult:
    """Compute the boot state and return the in-memory broker key (if any).

    Called once from the FastAPI lifespan after the connection pool is
    open and before ``yield``. Never raises on a missing file -- a
    missing file when credentials exist is a recoverable state, not a
    crash. Does raise :class:`MasterKeyError` if the env override is
    set but does not match existing ciphertext (fail-loud, per
    ADR-0003 §6).
    """
    creds_exist = _credentials_exist(conn)

    # Env override path: EBULL_SECRETS_KEY is decoded as-is by
    # secrets_crypto.decode_env_key(); here we only need to *verify*
    # it against existing ciphertext before installing it.
    if settings.secrets_key:
        env_key = decode_env_key(settings.secrets_key)
        if creds_exist and not _key_decrypts_newest_credential(conn, env_key):
            raise MasterKeyError(
                "EBULL_SECRETS_KEY does not match existing broker credential ciphertext. "
                "Refusing to start. See ADR-0003 §6."
            )
        # Env-override + no credentials is still a clean_install
        # boot state -- there's nothing to encrypt yet and the
        # frontend should still route to the setup flow. The
        # env_key IS installed so the first credential save can
        # encrypt against it without ever lazy-generating a file.
        return BootResult(
            state="normal" if creds_exist else "clean_install",
            broker_encryption_key=env_key,
            needs_setup=not creds_exist,
            recovery_required=False,
        )

    # File path: load if present, derive key, verify against ciphertext.
    root_secret = read_root_secret()
    if root_secret is None:
        state = compute_boot_state(
            operators_empty=False,
            credentials_exist=creds_exist,
            root_secret_present=False,
            key_matches=False,
        )
        return BootResult(
            state=state,
            broker_encryption_key=None,
            needs_setup=(state == "clean_install"),
            recovery_required=(state == "recovery_required"),
        )

    derived = derive_broker_encryption_key(root_secret)
    matches = _key_decrypts_newest_credential(conn, derived)
    state = compute_boot_state(
        operators_empty=False,
        credentials_exist=creds_exist,
        root_secret_present=True,
        key_matches=matches,
    )
    return BootResult(
        state=state,
        broker_encryption_key=derived if state == "normal" else None,
        needs_setup=False,
        recovery_required=(state == "recovery_required"),
    )


# ---------------------------------------------------------------------------
# Lazy generation (first credential save) and recovery
# ---------------------------------------------------------------------------


def generate_root_secret_in_memory() -> tuple[bytes, bytes, list[str]]:
    """Generate a fresh root secret WITHOUT persisting it yet.

    Returns ``(root_secret, derived_key, phrase)``. The caller is
    expected to:

      1. Install ``derived_key`` into the secrets_crypto cache.
      2. Attempt the DB write (e.g. ``store_credential``).
      3. On success, call :func:`persist_generated_root_secret` to
         flush the file to disk and return the phrase to the operator.
      4. On failure, call :func:`abandon_generated_root_secret` to
         clear the cache so the next attempt re-runs the gen-then-store
         dance fresh.

    This split exists so a DB error after key generation does not
    leave the operator with a persisted file whose recovery phrase
    they never saw (review-prevention: phrase-lost-on-DB-error
    surfaced on PR #118).
    """
    root_secret = secrets.token_bytes(ROOT_SECRET_LEN)
    derived = derive_broker_encryption_key(root_secret)
    phrase = encode_phrase(root_secret)
    return root_secret, derived, phrase


def persist_generated_root_secret(root_secret: bytes) -> Path:
    """Flush a previously-generated root secret to disk."""
    return write_root_secret(root_secret)


def recover_from_phrase(conn: psycopg.Connection[object], phrase: str, app_state: object) -> bytes:
    """Verify a recovery phrase and install the recovered key.

    Sequence:
      0. Decode the phrase + derive the key (OUTSIDE the lock --
         pure CPU work, no shared state, fails fast on a
         malformed phrase before we contend on the lock).
      1. Acquire ``lazy_gen_lock``.
      2. Verify the derived key decrypts the newest active
         credential.
      3. Persist the root secret to disk.
      4. Install the derived key into the cipher cache.
      5. Atomically flip all gating ``app_state`` flags.
      6. Release ``lazy_gen_lock``.

    Steps 2-5 are atomic under the lock; step 0 is intentionally
    outside (review feedback PR #118 round 12, correcting an
    earlier docstring that implied the decode step was covered).

    Steps 3 and 4 happen INSIDE the lock so a queued lazy-gen waiter
    that acquires the lock the moment recovery returns observes
    ``broker_key_loaded=True`` on its inner re-check and falls
    through to the normal-store path instead of generating a fresh
    root secret and overwriting the file recovery just wrote
    (review feedback PR #118 round 7).

    Returns the derived broker-encryption key on success. Raises
    :class:`RecoveryPhraseError` if the phrase is malformed and
    :class:`RecoveryVerificationError` if it decodes cleanly but does
    not match the newest active credential ciphertext. The caller is
    responsible for the remaining ``app_state`` flags
    (``boot_state``, ``recovery_required``, ``needs_setup``) which
    do not gate the lazy-gen path.

    NOTE: this function only accepts ``phrase: str`` (narrowed in
    round 14). The underlying :func:`decode_phrase` helper still
    accepts ``list[str] | str`` because it is the bidirectional
    counterpart of :func:`encode_phrase` (which returns ``list[str]``)
    and is used by direct callers in tests for round-trip
    encode/decode. Direct callers of ``decode_phrase`` own their
    own word-count validation; the API boundary here is the
    only place where untrusted input enters, and that input is
    always a single string from the JSON body
    (review feedback PR #118 round 16).
    """
    root_secret = decode_phrase(phrase)
    derived = derive_broker_encryption_key(root_secret)
    with lazy_gen_lock:
        # Snapshot freshness: the SELECT below runs on the caller's
        # connection, which FastAPI's get_conn dependency manages.
        # We do NOT issue conn.rollback() here -- that would
        # silently abort any pending work the dependency expects
        # to commit on exit, and is the kind of side-effect a
        # service function with a borrowed Connection must avoid
        # (review-prevention-log: mid-transaction commit/rollback
        # in service functions). Snapshot freshness is provided
        # by PostgreSQL's READ COMMITTED isolation: each
        # statement observes the latest committed data, so the
        # SELECT below sees any revocation that committed before
        # we acquired ``lazy_gen_lock``, regardless of when the
        # caller's transaction started (review feedback PR #118
        # round 11, correcting round 10 defensive rollback).
        # Single fetch + verify against the SAME row object: a
        # double fetch (once for the none-guard, once inside a
        # helper that re-queries) would race against a concurrent
        # revocation under READ COMMITTED -- the second fetch
        # could see ``None`` and short-circuit to "vacuous match",
        # installing an unverified key (review feedback PR #118
        # round 9).
        row = _newest_active_credential(conn)
        if row is None:
            # Refuse recovery when there is nothing to verify
            # against. recovery_required can only be set when
            # ``compute_boot_state`` saw credentials, so reaching
            # here with no row is a state-machine error.
            raise RecoveryNotApplicableError("no active credential to verify recovery phrase against")
        if not _key_decrypts_row(row, derived):
            raise RecoveryVerificationError("recovery phrase did not match stored broker credentials")
        write_root_secret(root_secret)
        set_active_key(derived)
        # ALL gating flags must flip atomically inside the lock so
        # a concurrent client cannot observe an incoherent
        # intermediate state -- e.g. ``broker_key_loaded=True``
        # while ``recovery_required`` is still ``True`` (which
        # would cause ``require_master_key`` to 503 a request
        # that should succeed) or a stale ``bootstrap-state``
        # response that mixes the two (review feedback PR #118
        # round 8).
        app_state.broker_key_loaded = True  # type: ignore[attr-defined]
        app_state.boot_state = "normal"  # type: ignore[attr-defined]
        app_state.recovery_required = False  # type: ignore[attr-defined]
        app_state.needs_setup = False  # type: ignore[attr-defined]
    return derived
