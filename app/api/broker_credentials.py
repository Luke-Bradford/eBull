"""Broker credential HTTP endpoints (issue #99 / ADR 0001).

Routes (all session-only -- never service_token):

  GET    /broker-credentials       -- list metadata (active + revoked)
  POST   /broker-credentials       -- create; body contains plaintext,
                                      response contains metadata only
  DELETE /broker-credentials/{id}  -- soft-delete (sets revoked_at)

Service-token auth is intentionally not accepted. Per ADR 0001 the
credential-management surface is operator-only by design: a service
token must be able to USE a stored credential (via internal code that
calls ``load_credential_for_provider_use``) but must never be able to
enumerate, create, or revoke them from an HTTP request.

Plaintext discipline:
  * The response model ``CredentialMetadataOut`` has no secret-bearing
    field.
  * The POST handler accepts plaintext, hands it straight to the
    service, and only returns the metadata of the stored row. The
    plaintext is never echoed.
  * There is no "decrypt on demand" route. Ticket C migrates the eToro
    consumer to pull secrets through ``load_credential_for_provider_use``
    from internal code.
"""

from __future__ import annotations

import logging
from datetime import datetime
from uuid import UUID

import psycopg
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field

from app.api.auth import require_session
from app.api.auth_bootstrap import require_master_key
from app.db import get_conn
from app.security import master_key
from app.security.secrets_crypto import clear_active_key, set_active_key
from app.security.sessions import SessionRow
from app.services.broker_credentials import (
    CredentialAlreadyExists,
    CredentialMetadata,
    CredentialNotFound,
    CredentialValidationError,
    list_credentials,
    revoke_credential,
    store_credential,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/broker-credentials", tags=["broker-credentials"])


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class CredentialMetadataOut(BaseModel):
    """HTTP view of a broker credential row.

    NOTE: no ``ciphertext`` and no ``secret`` field. Adding one would be
    a security regression and is covered by the api test that asserts
    the response schema field-by-field.
    """

    id: UUID
    provider: str
    label: str
    last_four: str
    created_at: datetime
    last_used_at: datetime | None
    revoked_at: datetime | None


def _to_out(meta: CredentialMetadata) -> CredentialMetadataOut:
    return CredentialMetadataOut(
        id=meta.id,
        provider=meta.provider,
        label=meta.label,
        last_four=meta.last_four,
        created_at=meta.created_at,
        last_used_at=meta.last_used_at,
        revoked_at=meta.revoked_at,
    )


class CreateCredentialRequest(BaseModel):
    provider: str = Field(min_length=1, max_length=64)
    label: str = Field(min_length=1, max_length=255)
    # Upper bound is defensive: nothing sensible is 4096 chars long.
    secret: str = Field(min_length=1, max_length=4096)


class CreateCredentialResponse(BaseModel):
    """POST /broker-credentials response.

    Carries the standard metadata block AND -- only on the very first
    save in clean_install mode -- the 24-word recovery phrase that the
    operator must record. After that first save the phrase is None and
    is never returned again from any endpoint (#114 / ADR-0003 §4).
    """

    credential: CredentialMetadataOut
    recovery_phrase: list[str] | None = None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get("", response_model=list[CredentialMetadataOut])
def list_(
    session: SessionRow = Depends(require_session),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> list[CredentialMetadataOut]:
    """Return active + revoked credentials for the calling operator.

    The frontend renders revoked rows with a "revoked" badge, so both
    states are included. The service layer guarantees ordering: active
    first, newest first within each group.
    """
    rows = list_credentials(conn, operator_id=session.operator_id)
    return [_to_out(row) for row in rows]


@router.post(
    "",
    response_model=CreateCredentialResponse,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_master_key)],
)
def create(
    body: CreateCredentialRequest,
    request: Request,
    session: SessionRow = Depends(require_session),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> CreateCredentialResponse:
    """Store a new credential.

    On the very first save in clean_install mode this triggers lazy
    generation of the root secret (#114 / ADR-0003 §4): a fresh 32-byte
    secret is created, persisted to disk, the derived broker-encryption
    key is installed into the secrets_crypto cache, and the response
    carries the 24-word recovery phrase exactly once. On every
    subsequent save the phrase field is null.
    """
    phrase: list[str] | None = None
    pending_root_secret: bytes | None = None

    # Lazy-gen path. Serialised across requests by lazy_gen_lock so
    # two simultaneous first-saves cannot each generate a distinct
    # root secret. We re-check boot_state under the lock for the
    # standard double-checked-locking pattern -- the first request
    # through flips it to "normal" while still holding the lock.
    if getattr(request.app.state, "boot_state", "clean_install") == "clean_install":
        with master_key.lazy_gen_lock:
            if getattr(request.app.state, "boot_state", "clean_install") == "clean_install":
                pending_root_secret, derived, phrase = master_key.generate_root_secret_in_memory()
                # Install the key into the cipher cache so the
                # store_credential call below can encrypt against it.
                # The on-disk file is NOT written yet -- if the DB
                # write fails we will clear the cache and the next
                # attempt will start fresh, so the operator never
                # ends up with a persisted secret whose phrase they
                # never saw.
                set_active_key(derived)
                request.app.state.boot_state = "normal"
                request.app.state.needs_setup = False
                request.app.state.recovery_required = False
                logger.info("master key lazy-generated on first credential save (pending DB confirm)")

    try:
        meta = store_credential(
            conn,
            operator_id=session.operator_id,
            provider=body.provider,
            label=body.label,
            plaintext=body.secret,
        )
    except CredentialValidationError as exc:
        if pending_root_secret is not None:
            # Roll back the in-memory key install so the next attempt
            # generates a fresh secret. The on-disk file was never
            # written, so there is nothing to delete.
            clear_active_key()
            request.app.state.boot_state = "clean_install"
            request.app.state.needs_setup = True
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except CredentialAlreadyExists as exc:
        if pending_root_secret is not None:
            clear_active_key()
            request.app.state.boot_state = "clean_install"
            request.app.state.needs_setup = True
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="credential already exists",
        ) from exc
    except Exception:
        if pending_root_secret is not None:
            clear_active_key()
            request.app.state.boot_state = "clean_install"
            request.app.state.needs_setup = True
        raise

    # Store succeeded. NOW it is safe to flush the root secret to
    # disk -- the operator will see the phrase in this response, and
    # the persisted file matches the encrypted ciphertext we just
    # committed.
    if pending_root_secret is not None:
        master_key.persist_generated_root_secret(pending_root_secret)
        logger.info("master key root secret persisted to disk after first credential commit")

    return CreateCredentialResponse(credential=_to_out(meta), recovery_phrase=phrase)


@router.delete("/{credential_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete(
    credential_id: UUID,
    session: SessionRow = Depends(require_session),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> None:
    """Soft-delete a credential. Returns 404 if it does not exist or is
    already revoked; 204 on success."""
    try:
        revoke_credential(
            conn,
            credential_id=credential_id,
            operator_id=session.operator_id,
        )
    except CredentialNotFound as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="credential not found",
        ) from exc
