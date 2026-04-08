"""Broker credential storage service (issue #99 / ADR 0001).

Thin service layer over ``broker_credentials`` and
``broker_credential_access_log``. Every decryption attempt writes an
audit row (success OR failure) in the same transaction as the read, so
the forensic log cannot silently drop. Every decryption caller passes a
``caller`` tag (e.g. ``"etoro_order_client"``) that is recorded on the
audit row.

API / HTTP wiring lives in ``app.api.broker_credentials`` -- this module
returns plain dataclasses and raises narrow service exceptions.

Security invariants enforced here:
  * ``operator_id`` always comes from the session; it is never derived
    from request input. The service API takes it as a keyword argument
    so an HTTP handler cannot accidentally leave it out.
  * Metadata views NEVER include the ciphertext column -- the ``list``
    and ``store`` return values are defined without it, so a new HTTP
    handler cannot accidentally pass the blob to a response model.
  * Plaintext is only returned by
    :func:`load_credential_for_provider_use`, which is intended to be
    called from internal provider-adapter code, not from HTTP handlers.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Literal
from uuid import UUID

import psycopg
import psycopg.errors
import psycopg.rows
from psycopg import sql

from app.security.secrets_crypto import (
    KEY_VERSION_CURRENT,
    CredentialDecryptError,
    decrypt,
    encrypt,
)

logger = logging.getLogger(__name__)

# Allow-list of providers accepted by this ticket. Kept narrow: the
# eToro consumer migration (ticket C / #100) is the first real caller
# and we do not want to pre-invent slots for unrelated providers.
Provider = Literal["etoro"]
ALLOWED_PROVIDERS: frozenset[str] = frozenset({"etoro"})

# Broker secrets shorter than this cannot meaningfully produce a
# ``last_four`` preview and are almost certainly a typo. Rejected at
# the service boundary so the HTTP layer does not need its own check.
MIN_SECRET_LEN = 4


@dataclass(frozen=True)
class CredentialMetadata:
    """Non-secret view of a credential row.

    The ciphertext is intentionally absent from this type: a handler
    that returns ``CredentialMetadata`` cannot accidentally leak the
    blob to the HTTP response.
    """

    id: UUID
    operator_id: UUID
    provider: str
    label: str
    last_four: str
    created_at: datetime
    last_used_at: datetime | None
    revoked_at: datetime | None


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class CredentialError(Exception):
    """Base class for broker-credential service errors."""


class CredentialValidationError(CredentialError):
    """Raised when user input fails validation (bad provider, empty field)."""


class CredentialAlreadyExists(CredentialError):
    """Raised when an active credential already exists for (operator, provider, label)."""


class CredentialNotFound(CredentialError):
    """Raised when no active credential matches the lookup."""


# ---------------------------------------------------------------------------
# Input normalisation
# ---------------------------------------------------------------------------


def _normalise_provider(raw: str) -> str:
    cleaned = raw.strip().lower()
    if cleaned not in ALLOWED_PROVIDERS:
        raise CredentialValidationError(f"unsupported provider: {raw!r}")
    return cleaned


def _normalise_label(raw: str) -> str:
    cleaned = raw.strip()
    if not cleaned:
        raise CredentialValidationError("label must not be empty")
    return cleaned


def _normalise_secret(raw: str) -> str:
    cleaned = raw.strip()
    if not cleaned:
        raise CredentialValidationError("secret must not be empty")
    if len(cleaned) < MIN_SECRET_LEN:
        raise CredentialValidationError(f"secret must be at least {MIN_SECRET_LEN} characters")
    return cleaned


# ---------------------------------------------------------------------------
# Row parsing
# ---------------------------------------------------------------------------


_METADATA_COL_NAMES = (
    "id",
    "operator_id",
    "provider",
    "label",
    "last_four",
    "created_at",
    "last_used_at",
    "revoked_at",
)
# Composed psycopg.sql.Identifier list -- safer than f-string interpolation
# of a column-name string into a query template (review-prevention-log entry
# on f-string SQL composition).
_METADATA_COLS_SQL = sql.SQL(", ").join(sql.Identifier(name) for name in _METADATA_COL_NAMES)


def _row_to_metadata(row: dict[str, object]) -> CredentialMetadata:
    return CredentialMetadata(
        id=row["id"],  # type: ignore[arg-type]
        operator_id=row["operator_id"],  # type: ignore[arg-type]
        provider=row["provider"],  # type: ignore[arg-type]
        label=row["label"],  # type: ignore[arg-type]
        last_four=row["last_four"],  # type: ignore[arg-type]
        created_at=row["created_at"],  # type: ignore[arg-type]
        last_used_at=row["last_used_at"],  # type: ignore[arg-type]
        revoked_at=row["revoked_at"],  # type: ignore[arg-type]
    )


# ---------------------------------------------------------------------------
# Service functions
# ---------------------------------------------------------------------------


def store_credential(
    conn: psycopg.Connection[object],
    *,
    operator_id: UUID,
    provider: str,
    label: str,
    plaintext: str,
) -> CredentialMetadata:
    """Encrypt and insert a new credential row.

    Raises:
      CredentialValidationError -- provider / label / secret invalid.
      CredentialAlreadyExists   -- an active row with the same (operator,
                                   provider, label) already exists.
    """
    provider_norm = _normalise_provider(provider)
    label_norm = _normalise_label(label)
    secret_norm = _normalise_secret(plaintext)

    last_four = secret_norm[-4:]
    key_version = KEY_VERSION_CURRENT
    ciphertext = encrypt(
        secret_norm,
        operator_id=operator_id,
        provider=provider_norm,
        label=label_norm,
        key_version=key_version,
    )

    try:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                sql.SQL(
                    """
                INSERT INTO broker_credentials
                    (operator_id, provider, label, ciphertext,
                     last_four, key_version)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING {cols}
                """
                ).format(cols=_METADATA_COLS_SQL),
                (
                    operator_id,
                    provider_norm,
                    label_norm,
                    ciphertext,
                    last_four,
                    key_version,
                ),
            )
            row = cur.fetchone()
            if row is None:
                # RETURNING on a successful INSERT always yields a row.
                # This branch is defensive so pyright does not see an
                # Optional leak downstream.
                raise RuntimeError("INSERT ... RETURNING produced no row")
        conn.commit()
    except psycopg.errors.UniqueViolation as exc:
        conn.rollback()
        raise CredentialAlreadyExists(f"credential already exists for ({provider_norm!r}, {label_norm!r})") from exc
    return _row_to_metadata(row)


def list_credentials(
    conn: psycopg.Connection[object],
    *,
    operator_id: UUID,
) -> list[CredentialMetadata]:
    """Return all credentials for *operator_id*, active and revoked.

    Ordering: active rows first (``revoked_at IS NULL`` sorts before
    non-null), then newest first by ``created_at``. This keeps the UI
    list stable: fresh rows appear at the top, revoked rows sink to the
    bottom, and a simple ``.filter(r => !r.revoked_at)`` on the frontend
    still yields the right active subset.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            sql.SQL(
                """
            SELECT {cols}
            FROM broker_credentials
            WHERE operator_id = %s
            ORDER BY (revoked_at IS NOT NULL), created_at DESC
            """
            ).format(cols=_METADATA_COLS_SQL),
            (operator_id,),
        )
        return [_row_to_metadata(row) for row in cur.fetchall()]


def revoke_credential(
    conn: psycopg.Connection[object],
    *,
    credential_id: UUID,
    operator_id: UUID,
) -> None:
    """Soft-delete a credential. Idempotent-ish: revoking an already-revoked
    row returns ``CredentialNotFound`` so the caller gets a clear 404 and
    cannot accidentally treat "already revoked" as "just revoked".
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE broker_credentials
               SET revoked_at = now()
             WHERE id = %s
               AND operator_id = %s
               AND revoked_at IS NULL
            """,
            (credential_id, operator_id),
        )
        if cur.rowcount == 0:
            # No row was touched, so there is nothing to roll back at
            # this layer. The UPDATE itself either matched or did not.
            raise CredentialNotFound(f"credential {credential_id} not found")
    conn.commit()


def _write_access_log(
    cur: psycopg.Cursor[object],
    *,
    credential_id: UUID | None,
    operator_id: UUID,
    caller: str,
    success: bool,
    failure_reason: str | None,
) -> None:
    cur.execute(
        """
        INSERT INTO broker_credential_access_log
            (credential_id, operator_id, caller, success, failure_reason)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (credential_id, operator_id, caller, success, failure_reason),
    )


def load_credential_for_provider_use(
    conn: psycopg.Connection[object],
    *,
    operator_id: UUID,
    provider: str,
    caller: str,
) -> str:
    """Decrypt and return the plaintext secret for internal provider use.

    Transaction model:
      The function does NOT call ``conn.commit()`` or ``conn.rollback()``
      and does NOT open its own ``conn.transaction()`` block. It runs
      the SELECT / decrypt / audit-write / ``last_used_at`` update on
      whatever transaction the caller has set up; the caller owns the
      lifecycle. This avoids the silent-commit footgun where calling
      this function inside a caller's transaction would otherwise
      flush whatever the caller had accumulated (see review-prevention
      -log entry on mid-transaction commits in service functions).

      *Audit durability*: the audit row is written on the caller's
      transaction. If the caller commits, the audit row is durable. If
      the caller rolls back, the audit row is lost along with the
      caller's other changes. Callers on a trade path MUST commit the
      audit row before performing the external broker call -- the
      documented pattern is::

          secret = load_credential_for_provider_use(conn, ...)
          conn.commit()                  # audit row durable
          place_order(secret, ...)       # external side effect

      A future "always-durable audit on a side connection" mode is
      tracked separately and is out of scope for this ticket.

    Raises:
      CredentialValidationError -- unsupported provider / empty caller.
      CredentialNotFound        -- no active credential.
      CredentialDecryptError    -- stored ciphertext failed AEAD check.
    """
    provider_norm = _normalise_provider(provider)
    caller_clean = caller.strip()
    if not caller_clean:
        raise CredentialValidationError("caller tag must not be empty")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT id, operator_id, provider, label, ciphertext,
                   last_four, key_version, created_at, last_used_at,
                   revoked_at
              FROM broker_credentials
             WHERE operator_id = %s
               AND provider = %s
               AND revoked_at IS NULL
             ORDER BY created_at DESC
             LIMIT 1
            """,
            (operator_id, provider_norm),
        )
        row = cur.fetchone()
        if row is None:
            _write_access_log(
                cur,
                credential_id=None,
                operator_id=operator_id,
                caller=caller_clean,
                success=False,
                failure_reason="not_found",
            )
            raise CredentialNotFound(f"no active credential for provider {provider_norm!r}")

        try:
            plaintext = decrypt(
                bytes(row["ciphertext"]),  # type: ignore[arg-type]
                operator_id=row["operator_id"],  # type: ignore[arg-type]
                provider=row["provider"],  # type: ignore[arg-type]
                label=row["label"],  # type: ignore[arg-type]
                key_version=row["key_version"],  # type: ignore[arg-type]
            )
        except CredentialDecryptError:
            _write_access_log(
                cur,
                credential_id=row["id"],  # type: ignore[arg-type]
                operator_id=operator_id,
                caller=caller_clean,
                success=False,
                failure_reason="decrypt_failed",
            )
            raise

        cur.execute(
            "UPDATE broker_credentials SET last_used_at = now() WHERE id = %s",
            (row["id"],),
        )
        _write_access_log(
            cur,
            credential_id=row["id"],  # type: ignore[arg-type]
            operator_id=operator_id,
            caller=caller_clean,
            success=True,
            failure_reason=None,
        )
    return plaintext
