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

    # Lazy-gen path runs only on the very first credential save in
    # clean_install mode AND only when the cipher cache is empty
    # (env-override clean_install already has the key installed and
    # must skip generation entirely -- review feedback PR #118
    # round 2). The whole sequence -- generate, persist file,
    # install cache, store credential -- runs under
    # ``lazy_gen_lock`` so a queued waiter that arrives during a
    # rollback observes the post-rollback state, not a stale
    # mid-flight one.
    needs_lazy_gen = getattr(request.app.state, "boot_state", "clean_install") == "clean_install" and not getattr(
        request.app.state, "broker_key_loaded", False
    )
    if needs_lazy_gen:
        with master_key.lazy_gen_lock:
            # Re-check inside the lock: another waiter may have
            # already populated the key while we queued.
            if not getattr(request.app.state, "broker_key_loaded", False):
                pending_root_secret, derived, phrase = master_key.generate_root_secret_in_memory()
                # Persist the file BEFORE the DB write. If persist
                # raises (disk full, perms), nothing has been
                # committed yet, the key is not in the cache, and
                # the operator just retries. If persist succeeds
                # but the DB write later fails, we unlink the
                # file in the rollback path below so the next
                # attempt starts clean. Either way the invariant
                # holds: ``credential row exists`` implies
                # ``root secret file is on disk and the operator
                # saw the phrase exactly once``.
                master_key.persist_generated_root_secret(pending_root_secret)
                set_active_key(derived)
                request.app.state.broker_key_loaded = True
                request.app.state.boot_state = "normal"
                request.app.state.needs_setup = False
                request.app.state.recovery_required = False
                logger.info("master key lazy-generated on first credential save (file persisted)")
                try:
                    return _do_store(conn, session, body, phrase)
                except Exception:
                    # Roll back the persisted file + cache. We are
                    # still inside the lock so any queued waiter
                    # observes the cleaned-up state.
                    _rollback_lazy_gen(request)
                    raise
            # Fall through: another waiter populated the key while
            # we queued; proceed as a normal store.

    return _do_store(conn, session, body, phrase=None)


def _do_store(
    conn: psycopg.Connection[object],
    session: SessionRow,
    body: CreateCredentialRequest,
    phrase: list[str] | None,
) -> CreateCredentialResponse:
    try:
        meta = store_credential(
            conn,
            operator_id=session.operator_id,
            provider=body.provider,
            label=body.label,
            plaintext=body.secret,
        )
    except CredentialValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except CredentialAlreadyExists as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="credential already exists",
        ) from exc
    return CreateCredentialResponse(credential=_to_out(meta), recovery_phrase=phrase)


def _rollback_lazy_gen(request: Request) -> None:
    """Undo a lazy-gen install after the DB write failed.

    Best-effort: unlink the persisted file, clear the cipher cache,
    reset ``app.state`` flags. Called from inside ``lazy_gen_lock``
    so a queued first-save observes the cleaned-up state.
    """
    try:
        path = master_key.root_secret_path()
        if path.exists():
            path.unlink()
    except OSError:
        # An orphan file with no credential rows is still a valid
        # clean_install state per compute_boot_state, and the next
        # successful first-save will atomically overwrite it via
        # os.replace. Logged but not fatal.
        logger.exception("lazy-gen rollback: failed to unlink persisted root secret file")
    clear_active_key()
    request.app.state.broker_key_loaded = False
    request.app.state.boot_state = "clean_install"
    request.app.state.needs_setup = True
    request.app.state.recovery_required = False


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
