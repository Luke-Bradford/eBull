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
    normalise_label,
    normalise_provider,
    normalise_secret,
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
    # Recovery-required state-machine guard: refuse credential
    # writes while the operator must run /auth/recover. Without
    # this guard the request would fall through to encrypt() with
    # an empty cipher cache and return 500 instead of the
    # documented 503 (review feedback PR #118 round 13).
    # POST /broker-credentials does NOT mount require_master_key
    # because the create handler self-gates so it can lazy-gen
    # on first save -- this guard is the equivalent gate for the
    # one boot state where lazy-gen is NOT the right answer.
    if getattr(request.app.state, "recovery_required", False):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="recovery required",
        )

    # Pre-validate user input and pre-check for duplicate BEFORE any
    # lazy-gen sequence. We must never reach a state where the root
    # secret file is on disk but a 400 / 409 user error is then
    # returned to the operator -- they would never see the phrase but
    # the file would still be persisted (review feedback PR #118
    # round 3). The pre-checks below are pure / read-only and run
    # outside ``lazy_gen_lock`` -- they cannot leak any state into
    # the lazy-gen path.
    try:
        provider_norm = normalise_provider(body.provider)
        label_norm = normalise_label(body.label)
        # Capture the normalised secret here and pass it through
        # to store_credential below, so a single normalisation
        # pass is shared end-to-end. Previously the pre-check
        # validated and discarded the cleaned secret, then
        # store_credential re-normalised body.secret -- which
        # opened a hypothetical drift window where a secret
        # accepted by the outer pass could still raise
        # CredentialValidationError inside the lazy-gen block
        # and trigger _rollback_lazy_gen on a user-input error
        # (review feedback PR #118 round 12).
        secret_norm = normalise_secret(body.secret)
    except CredentialValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    if _active_credential_exists(conn, session.operator_id, provider_norm, label_norm):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="credential already exists")

    phrase: list[str] | None = None

    # Lazy-gen path runs only on the very first credential save in
    # clean_install mode AND only when the cipher cache is empty
    # (env-override clean_install already has the key installed and
    # must skip generation entirely). The whole sequence -- generate,
    # persist file, install cache, store credential -- runs under
    # ``lazy_gen_lock`` so a concurrent recovery flow or a queued
    # waiter cannot interleave with the file write.
    #
    # NOTE: this read of ``boot_state`` / ``broker_key_loaded`` is
    # OUTSIDE the lock. It is a fast-fail optimisation only -- the
    # authoritative re-check happens inside the lock at line ~190.
    # On CPython attribute reads are atomic under the GIL so we will
    # never see a torn value, but we MUST NOT treat this read as
    # authoritative; a concurrent recovery could flip the flags
    # between this check and the lock acquisition, and the in-lock
    # re-check is what catches that case (review feedback PR #118
    # round 9).
    needs_lazy_gen = getattr(request.app.state, "boot_state", "clean_install") == "clean_install" and not getattr(
        request.app.state, "broker_key_loaded", False
    )
    if needs_lazy_gen:
        with master_key.lazy_gen_lock:
            # Re-check inside the lock: a concurrent recovery may
            # have populated the key while we queued. Recovery now
            # sets ``broker_key_loaded=True`` from inside the same
            # lock (master_key.recover_from_phrase, round 7), so
            # this re-check is authoritative.
            if not getattr(request.app.state, "broker_key_loaded", False):
                # By construction, no concurrent writer can commit a
                # broker_credentials row in this state:
                #   * a normal-store path requires
                #     ``broker_key_loaded=True`` (just re-checked
                #     above and held under the lock)
                #   * a parallel lazy-gen path requires holding
                #     ``lazy_gen_lock`` (we hold it)
                #   * a recovery path also requires
                #     ``lazy_gen_lock`` (held inside
                #     recover_from_phrase)
                # Therefore the outer pre-check at line 173 cannot
                # be a false negative: any writer that committed
                # before our outer pre-check is reflected, and no
                # writer can commit between then and now. We do
                # NOT re-issue _active_credential_exists -- it
                # would share the same READ COMMITTED conn and
                # add no isolation guarantee. The CredentialAlready
                # Exists handler below is defense-in-depth only.
                pending_root_secret, derived, phrase = master_key.generate_root_secret_in_memory()
                # Persist the file BEFORE the DB write. If persist
                # raises (disk full, perms), nothing has been
                # committed yet, the key is not in the cache, and
                # the operator just retries.
                master_key.persist_generated_root_secret(pending_root_secret)
                set_active_key(derived)
                request.app.state.broker_key_loaded = True
                request.app.state.boot_state = "normal"
                request.app.state.needs_setup = False
                request.app.state.recovery_required = False
                logger.info("master key lazy-generated on first credential save (file persisted)")
                try:
                    # Pass the *already-normalised* provider /
                    # label / secret so the pre-check and the
                    # INSERT share a single normalisation pass
                    # end-to-end. Eliminates the hypothetical
                    # drift window where a re-normalisation
                    # inside store_credential could raise on a
                    # value the outer pass accepted (review
                    # feedback PR #118 round 12).
                    meta = store_credential(
                        conn,
                        operator_id=session.operator_id,
                        provider=provider_norm,
                        label=label_norm,
                        plaintext=secret_norm,
                    )
                except (KeyboardInterrupt, SystemExit):
                    # Signal-driven shutdown: do NOT touch the
                    # file. At signal time it is unknowable
                    # whether the INSERT committed -- the signal
                    # could fire before, during, or after the
                    # commit. Possible outcomes:
                    #   * Row landed: rollback would unlink the
                    #     key protecting it -- unrecoverable
                    #     lockout (the bug class fixed in
                    #     rounds 2-5).
                    #   * Row did not land: the file becomes an
                    #     orphan. On next boot, no credentials +
                    #     file present is treated as
                    #     ``clean_install`` (file is reused) by
                    #     ``compute_boot_state``, and the
                    #     operator's next credential save
                    #     proceeds normally with the existing
                    #     key. The phrase is lost forever, but
                    #     no data is at risk.
                    # We choose preservation because the
                    # alternative on the row-landed branch is
                    # unrecoverable while orphan-file-no-phrase
                    # is recoverable (round 6/12).
                    raise
                except (CredentialValidationError, CredentialAlreadyExists) as exc:
                    # Defense in depth: by construction these
                    # cannot fire here -- validation was pre-
                    # checked against the same normalisers, and
                    # the in-lock invariant precludes any
                    # concurrent insert (broker_key_loaded=False
                    # re-check + lazy_gen_lock held). If they DO
                    # somehow fire, no credential row was
                    # committed, the freshly-persisted root
                    # secret protects nothing, and the phrase
                    # was never returned to the operator -- so
                    # we MUST roll back the file + cipher cache +
                    # app.state before surfacing 4xx, otherwise
                    # the app is left in normal boot state with
                    # a key installed and a file on disk but no
                    # credential row and no phrase the operator
                    # has ever seen (review feedback PR #118
                    # round 11, correcting round 5/7 reasoning).
                    _rollback_lazy_gen(request)
                    if isinstance(exc, CredentialAlreadyExists):
                        raise HTTPException(
                            status_code=status.HTTP_409_CONFLICT,
                            detail="credential already exists",
                        ) from exc
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=str(exc),
                    ) from exc
                except Exception:
                    # Genuine unexpected error (psycopg failure,
                    # encryption error, etc.). No row was
                    # committed, the freshly-persisted root
                    # secret protects nothing, and the phrase
                    # was never returned to the operator.
                    # Rollback file + cache + state to keep the
                    # next attempt clean. We are still inside
                    # ``lazy_gen_lock`` so a queued waiter
                    # observes the cleaned-up state.
                    _rollback_lazy_gen(request)
                    raise
                return CreateCredentialResponse(credential=_to_out(meta), recovery_phrase=phrase)
            # Fall through: a concurrent recovery populated the
            # key while we queued; proceed as a normal store.

    return _do_store(
        conn,
        operator_id=session.operator_id,
        provider=provider_norm,
        label=label_norm,
        plaintext=secret_norm,
        phrase=None,
    )


def _active_credential_exists(
    conn: psycopg.Connection[object],
    operator_id: UUID,
    provider: str,
    label: str,
) -> bool:
    """True iff (operator, provider, label) already has an active row.

    Pre-flight duplicate check used by the create handler so a
    conflict is reported as 409 BEFORE the lazy-gen sequence runs --
    a duplicate must never trigger root-secret persistence followed
    by a discarded phrase.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
              FROM broker_credentials
             WHERE operator_id = %s
               AND provider = %s
               AND label = %s
               AND revoked_at IS NULL
             LIMIT 1
            """,
            (operator_id, provider, label),
        )
        return cur.fetchone() is not None


def _do_store(
    conn: psycopg.Connection[object],
    *,
    operator_id: UUID,
    provider: str,
    label: str,
    plaintext: str,
    phrase: list[str] | None,
) -> CreateCredentialResponse:
    try:
        meta = store_credential(
            conn,
            operator_id=operator_id,
            provider=provider,
            label=label,
            plaintext=plaintext,
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
    path = master_key.root_secret_path()
    try:
        if path.exists():
            path.unlink()
    except OSError:
        # Logged but not fatal: an orphan file with no credential
        # rows is still a valid clean_install state per
        # compute_boot_state, and the next successful first-save
        # will atomically overwrite it via os.replace.
        logger.exception("lazy-gen rollback: failed to unlink persisted root secret file")
    # Always log the final filesystem state so operators reading
    # logs after a rollback can tell whether the next attempt
    # enters lazy-gen cleanly (file absent) or reuses an orphan
    # (file present, key never bound to any credential, phrase
    # lost). The orphan branch is recoverable -- compute_boot_state
    # will return clean_install on next boot and the operator's
    # next save reuses the existing key -- but reading the log
    # spares them a confusing reboot (review feedback PR #118
    # round 14).
    if path.exists():
        logger.warning(
            "lazy-gen rollback: root secret file remains on disk at %s -- "
            "next boot will reuse it as clean_install (phrase from the "
            "aborted save is lost)",
            path,
        )
    else:
        logger.info("lazy-gen rollback: root secret file removed cleanly")
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
