"""Broker credential HTTP endpoints (issue #99 / ADR 0001, #139).

Routes (all session-only -- never service_token):

  GET    /broker-credentials                  -- list metadata (active + revoked)
  POST   /broker-credentials                  -- create; body contains plaintext,
                                                 response contains metadata only
  POST   /broker-credentials/validate         -- transient validation of candidate
                                                 credentials against the real eToro API
  POST   /broker-credentials/validate-stored  -- validate already-stored credentials
                                                 by loading from DB and probing eToro
  DELETE /broker-credentials/{id}             -- soft-delete (sets revoked_at)

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
from uuid import UUID, uuid4

import httpx
import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field

from app.api.auth import require_session
from app.config import settings
from app.db import get_conn
from app.security import master_key
from app.security.secrets_crypto import clear_active_key, set_active_key
from app.security.sessions import SessionRow
from app.services.broker_credentials import (
    CredentialAlreadyExists,
    CredentialDecryptError,
    CredentialMetadata,
    CredentialNotFound,
    CredentialValidationError,
    list_credentials,
    load_credential_for_provider_use,
    normalise_environment,
    normalise_label,
    normalise_provider,
    normalise_secret,
    revoke_credential,
    store_credential,
)
from app.services.credential_health import (
    get_operator_credential_health,
    notify_aggregate_if_changed,
    record_health_outcome,
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
    environment: str
    last_four: str
    created_at: datetime
    last_used_at: datetime | None
    revoked_at: datetime | None


def _to_out(meta: CredentialMetadata) -> CredentialMetadataOut:
    return CredentialMetadataOut(
        id=meta.id,
        provider=meta.provider,
        label=meta.label,
        environment=meta.environment,
        last_four=meta.last_four,
        created_at=meta.created_at,
        last_used_at=meta.last_used_at,
        revoked_at=meta.revoked_at,
    )


class CreateCredentialRequest(BaseModel):
    provider: str = Field(min_length=1, max_length=64)
    label: str = Field(min_length=1, max_length=255)
    # Transitional default: the current frontend does not send
    # environment yet (updated in PR D). Default to "demo" so
    # existing UI continues to work until then.
    environment: str = Field(default="demo", min_length=1, max_length=16)
    # Upper bound is defensive: nothing sensible is 4096 chars long.
    secret: str = Field(min_length=1, max_length=4096)


class CreateCredentialResponse(BaseModel):
    """POST /broker-credentials response.

    Carries the credential metadata block. Post-amendment 2026-05-07
    (ADR-0003) the response no longer includes a recovery phrase —
    the lazy-gen path persists the root secret silently and the
    operator never sees the underlying material.
    """

    credential: CredentialMetadataOut


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
    generation of the root secret (#114 / ADR-0003 §4, amended
    2026-05-07): a fresh 32-byte secret is created, persisted to disk,
    and the derived broker-encryption key is installed into the
    secrets_crypto cache. The response no longer carries a recovery
    phrase — the operator never sees the underlying material.
    """
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
        env_norm = normalise_environment(body.environment)
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
    if _active_credential_exists(conn, session.operator_id, provider_norm, label_norm, env_norm):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="credential already exists")

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
            # Re-check inside the lock: a concurrent first-save
            # path may have populated the key while we queued. The
            # in-lock re-check is authoritative.
            if not getattr(request.app.state, "broker_key_loaded", False):
                # By construction, no concurrent writer can commit a
                # broker_credentials row in this state:
                #   * a normal-store path requires
                #     ``broker_key_loaded=True`` (just re-checked
                #     above and held under the lock)
                #   * a parallel lazy-gen path requires holding
                #     ``lazy_gen_lock`` (we hold it)
                # Therefore the outer pre-check above cannot be a
                # false negative: any writer that committed before
                # our outer pre-check is reflected, and no writer
                # can commit between then and now. We do NOT
                # re-issue _active_credential_exists -- it would
                # share the same READ COMMITTED conn and add no
                # isolation guarantee. The CredentialAlreadyExists
                # handler below is defense-in-depth only.
                pending_root_secret, derived = master_key.generate_root_secret_in_memory()
                # Persist the file BEFORE the DB write. If persist
                # raises (disk full, perms), nothing has been
                # committed yet, the key is not in the cache, and
                # the operator just retries.
                master_key.persist_generated_root_secret(pending_root_secret)
                set_active_key(derived)
                request.app.state.broker_key_loaded = True
                request.app.state.boot_state = "normal"
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
                    #
                    # #112: caller owns transaction lifecycle.
                    # ``conn.commit()`` flushes the implicit
                    # transaction opened by ``_active_credential_
                    # exists`` above so ``conn.transaction()``
                    # opens a real top-level txn (not a savepoint
                    # that defers commit until get_conn teardown,
                    # which would publish the recovery phrase
                    # before the credential row is durable —
                    # Codex medium-severity finding).
                    conn.commit()
                    with conn.transaction():
                        meta = store_credential(
                            conn,
                            operator_id=session.operator_id,
                            provider=provider_norm,
                            label=label_norm,
                            environment=env_norm,
                            plaintext=secret_norm,
                        )
                except KeyboardInterrupt, SystemExit:
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
                return CreateCredentialResponse(credential=_to_out(meta))
            # Fall through: another lazy-gen path populated the key
            # while we queued; proceed as a normal store.

    return _do_store(
        conn,
        operator_id=session.operator_id,
        provider=provider_norm,
        label=label_norm,
        environment=env_norm,
        plaintext=secret_norm,
    )


def _active_credential_exists(
    conn: psycopg.Connection[object],
    operator_id: UUID,
    provider: str,
    label: str,
    environment: str,
) -> bool:
    """True iff (operator, provider, label, environment) already has an active row.

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
               AND environment = %s
               AND revoked_at IS NULL
             LIMIT 1
            """,
            (operator_id, provider, label, environment),
        )
        return cur.fetchone() is not None


def _do_store(
    conn: psycopg.Connection[object],
    *,
    operator_id: UUID,
    provider: str,
    label: str,
    environment: str,
    plaintext: str,
) -> CreateCredentialResponse:
    # #112: caller owns transaction lifecycle. The duplicate
    # pre-check (``_active_credential_exists``) runs an earlier
    # SELECT on this autocommit-off pool connection, which opens an
    # implicit transaction. Without the explicit ``conn.commit()``
    # below, the ``with conn.transaction()`` block would nest as a
    # SAVEPOINT and the INSERT would commit only on get_conn
    # teardown — i.e. AFTER the 201 response had been sent
    # (Codex high-severity finding on PR #112). Flush the implicit
    # transaction first; the wrapping block then opens a real
    # top-level txn so the INSERT is durable before the handler
    # returns.
    conn.commit()
    try:
        with conn.transaction():
            meta = store_credential(
                conn,
                operator_id=operator_id,
                provider=provider,
                label=label,
                environment=environment,
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
    return CreateCredentialResponse(credential=_to_out(meta))


def _rollback_lazy_gen(request: Request) -> None:
    """Undo a lazy-gen install after the DB write failed.

    Best-effort: unlink the persisted file, clear the cipher cache,
    reset ``app.state`` flags. Called from inside ``lazy_gen_lock``
    so a queued first-save observes the cleaned-up state.
    """
    path = master_key.root_secret_path()
    # Determine "removed cleanly" via the unlink syscall itself
    # rather than a separate stat. ``FileNotFoundError`` means the
    # file was never written by this aborted save (or already gone)
    # -> nothing to clean up. ``unlink()`` returning normally means
    # we removed exactly the file that existed at that instant.
    # Any other ``OSError`` means the file may still be on disk.
    # No second ``path.exists()`` -- a concurrent writer between
    # the unlink and a stat could otherwise produce a spurious
    # "file remains" warning attributing a successor save's file
    # to this rollback (review feedback PR #118 round 17).
    file_remains: bool
    try:
        path.unlink()
        file_remains = False
    except FileNotFoundError:
        file_remains = False
    except OSError:
        # Logged but not fatal: an orphan file with no credential
        # rows is still a valid clean_install state per
        # compute_boot_state, and the next successful first-save
        # will atomically overwrite it via os.replace.
        logger.exception("lazy-gen rollback: failed to unlink persisted root secret file")
        file_remains = True
    if file_remains:
        # Operators reading logs after a rollback can tell that
        # the next boot will reuse this file as clean_install
        # and that the phrase from the aborted save is lost.
        # The orphan branch is recoverable -- compute_boot_state
        # returns clean_install on next boot and the operator's
        # next save reuses the existing key -- but the warning
        # spares them a confusing reboot.
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


@router.delete("/{credential_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete(
    credential_id: UUID,
    session: SessionRow = Depends(require_session),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> None:
    """Soft-delete a credential. Returns 404 if it does not exist or is
    already revoked; 204 on success."""
    # #112: caller owns transaction lifecycle. ``conn.commit()``
    # flushes any implicit transaction opened earlier on this
    # connection (FastAPI dependencies that issued reads, etc.) so
    # ``conn.transaction()`` opens a real top-level txn — not a
    # savepoint that would defer the soft-delete commit to
    # get_conn teardown (Codex finding pattern, see _do_store).
    conn.commit()
    try:
        with conn.transaction():
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


# ---------------------------------------------------------------------------
# Validate endpoint (#139)
# ---------------------------------------------------------------------------

_VALIDATE_TIMEOUT_S = 10.0


class ValidateCredentialRequest(BaseModel):
    """Candidate credentials to validate against the real eToro API.

    This is a transient probe — nothing is persisted. The caller passes
    candidate api_key, user_key, and environment; the endpoint uses them
    for two read-only validation calls and returns the result.
    """

    api_key: str = Field(min_length=1, max_length=4096)
    user_key: str = Field(min_length=1, max_length=4096)
    environment: str = Field(min_length=1, max_length=16)


class ValidateIdentity(BaseModel):
    """Normalised identity returned by eToro ``/api/v1/me``."""

    gcid: int | None = None
    demo_cid: int | None = None
    real_cid: int | None = None


class ValidateCredentialResponse(BaseModel):
    """Result of transient credential validation.

    ``auth_valid`` and ``env_valid`` reflect whether each probe
    succeeded. Failed credentials return HTTP 200 with the flags set to
    ``False`` — 4xx/5xx is reserved for our own API problems (bad
    request payload, missing session).
    """

    auth_valid: bool
    identity: ValidateIdentity | None = None
    environment: str
    env_valid: bool
    env_check: str
    note: str


def _probe_etoro(
    api_key: str,
    user_key: str,
    environment: str,
) -> ValidateCredentialResponse:
    """Run two read-only probes against the eToro API.

    Extracted so both ``/validate`` (transient credentials) and
    ``/validate-stored`` (DB-loaded credentials) share the same logic.

    Level 1: ``GET /api/v1/me`` — proves the key pair is accepted.
    Level 2: ``GET /api/v1/trading/info/{env}/pnl`` — proves the
    environment surface is reachable.

    Does NOT prove write permission (acknowledged in the ``note``).

    ``environment`` is normalised internally — callers do not need to
    pre-normalise, though doing so is harmless (idempotent).
    """
    environment = normalise_environment(environment)
    headers = {
        "x-api-key": api_key,
        "x-user-key": user_key,
        "x-request-id": str(uuid4()),
        "Content-Type": "application/json",
    }

    # Level 1: basic auth validation
    try:
        with httpx.Client(
            base_url=settings.etoro_base_url,
            headers=headers,
            timeout=_VALIDATE_TIMEOUT_S,
        ) as client:
            me_resp = client.get("/api/v1/me")
    except httpx.HTTPError:
        logger.warning("credential validation: /me request failed", exc_info=True)
        return ValidateCredentialResponse(
            auth_valid=False,
            identity=None,
            environment=environment,
            env_valid=False,
            env_check="skipped",
            note="Connection to eToro failed",
        )

    if me_resp.status_code != 200:
        return ValidateCredentialResponse(
            auth_valid=False,
            identity=None,
            environment=environment,
            env_valid=False,
            env_check="skipped",
            note="Credentials rejected by eToro",
        )

    # Parse identity from /me response
    try:
        me_data = me_resp.json()
        identity = ValidateIdentity(
            gcid=me_data.get("gcid"),
            demo_cid=me_data.get("demoCid"),
            real_cid=me_data.get("realCid"),
        )
    except Exception:
        logger.warning("credential validation: failed to parse /me response", exc_info=True)
        identity = None

    # Level 2: environment validation
    headers["x-request-id"] = str(uuid4())
    env_check_path = f"/api/v1/trading/info/{environment}/pnl"
    try:
        with httpx.Client(
            base_url=settings.etoro_base_url,
            headers=headers,
            timeout=_VALIDATE_TIMEOUT_S,
        ) as client:
            env_resp = client.get(env_check_path)
    except httpx.HTTPError:
        logger.warning("credential validation: env check request failed", exc_info=True)
        return ValidateCredentialResponse(
            auth_valid=True,
            identity=identity,
            environment=environment,
            env_valid=False,
            env_check=f"trading/info/{environment}/pnl unreachable",
            note="Auth valid but environment check failed (network error). Does not verify write permission.",
        )

    if env_resp.status_code == 200:
        return ValidateCredentialResponse(
            auth_valid=True,
            identity=identity,
            environment=environment,
            env_valid=True,
            env_check=f"trading/info/{environment}/pnl reachable",
            note="Does not verify write permission",
        )

    return ValidateCredentialResponse(
        auth_valid=True,
        identity=identity,
        environment=environment,
        env_valid=False,
        env_check=f"trading/info/{environment}/pnl returned {env_resp.status_code}",
        note="Auth valid but environment check failed. Does not verify write permission.",
    )


@router.post("/validate", response_model=ValidateCredentialResponse)
def validate(
    body: ValidateCredentialRequest,
    session: SessionRow = Depends(require_session),
) -> ValidateCredentialResponse:
    """Validate candidate eToro credentials against the real API.

    Two validation levels:
      1. Basic auth — ``GET /api/v1/me`` proves the key pair is accepted
         and returns the account identity (gcid, demoCid, realCid).
      2. Environment — ``GET /api/v1/trading/info/{env}/pnl`` proves the
         env-scoped trading-info surface is reachable.

    Does NOT prove write permission. This is acknowledged in the
    response ``note`` field.

    The endpoint is session-gated but does not touch the DB. It uses
    the supplied credentials transiently for the two probe calls and
    never persists them.
    """
    try:
        env_norm = normalise_environment(body.environment)
    except CredentialValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc

    return _probe_etoro(body.api_key, body.user_key, env_norm)


@router.post("/validate-stored", response_model=ValidateCredentialResponse)
def validate_stored(
    request: Request,
    session: SessionRow = Depends(require_session),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> ValidateCredentialResponse:
    """Validate already-stored eToro credentials against the real API.

    Loads both ``api_key`` and ``user_key`` from the DB for the session's
    operator, decrypts them, and runs the same two-level eToro probe as
    ``/validate``. Nothing is persisted beyond the access-log entries
    written by ``load_credential_for_provider_use`` on a side
    connection (#111).

    Returns 404 if either credential is missing. Returns 503 if
    decryption fails (key material issue).
    """
    environment = "demo"  # hardcoded for v1, matches frontend ENVIRONMENT constant
    # #111: prefer the dedicated audit pool when the lifespan has
    # populated it. Falling back to the request pool would risk
    # losing audit rows under saturation; falling back to None
    # keeps existing unit tests (which don't set up app.state)
    # working via the legacy caller-conn audit path.
    audit_pool = getattr(request.app.state, "audit_pool", None)

    try:
        api_key = load_credential_for_provider_use(
            conn,
            operator_id=session.operator_id,
            provider="etoro",
            label="api_key",
            environment=environment,
            caller="validate-stored",
            audit_pool=audit_pool,
        )
        user_key = load_credential_for_provider_use(
            conn,
            operator_id=session.operator_id,
            provider="etoro",
            label="user_key",
            environment=environment,
            caller="validate-stored",
            audit_pool=audit_pool,
        )
    except CredentialNotFound as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Both api_key and user_key must be stored before validating.",
        ) from exc
    except CredentialDecryptError as exc:
        logger.error("validate-stored: decryption failed", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Credential decryption failed. Check server key material.",
        ) from exc

    # Commit audit rows before the external probe call (audit durability).
    conn.commit()

    # Look up credential_ids before the probe so we can write through
    # health outcomes (#975 / #974/A). Side-tx writes from
    # record_health_outcome use the request app's pool.
    cred_ids = _lookup_active_credential_ids(
        conn,
        operator_id=session.operator_id,
        provider="etoro",
        environment=environment,
    )

    try:
        result = _probe_etoro(api_key, user_key, environment)
    except CredentialValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid environment for stored credential validation.",
        ) from exc

    # Write the probe outcome through to credential health for both
    # rows. validate-stored is THE canonical probe path — source='probe'
    # is required to clear a sticky REJECTED row, per the credential
    # health contract.
    # Use db_pool (lifespan attribute name) — earlier draft used
    # the wrong attribute and silently no-op'd in production
    # (Codex pre-push r1.1).
    request_pool = getattr(request.app.state, "db_pool", None)
    if request_pool is not None:
        for cred_id in cred_ids.values():
            try:
                record_health_outcome(
                    credential_id=cred_id,
                    success=result.auth_valid,
                    source="probe",
                    error_detail=result.note if not result.auth_valid else None,
                    pool=request_pool,
                )
            except Exception:
                # Best-effort beyond the side-tx contract per spec.
                # The probe response IS the user-facing outcome — a
                # health-write failure must not change the API result.
                logger.warning(
                    "validate-stored: credential health write-through failed",
                    exc_info=True,
                )

    return result


# ---------------------------------------------------------------------------
# PUT /broker-credentials/replace — atomic revoke + create (#975 / #974/A)
# ---------------------------------------------------------------------------


class ReplaceCredentialRequest(BaseModel):
    """Atomic revoke + create for an existing label.

    Replaces the active credential row for ``(operator, provider, label,
    environment)`` in a single transaction so subscribers (orchestrator
    pre-flight gate, WS subscriber) never observe a transient MISSING
    state between the revoke and create. Per spec section "Atomic
    credential replacement".
    """

    provider: str = Field(min_length=1, max_length=64)
    label: str = Field(min_length=1, max_length=255)
    environment: str = Field(default="demo", min_length=1, max_length=16)
    secret: str = Field(min_length=1, max_length=4096)


class ReplaceCredentialResponse(BaseModel):
    """Response for PUT /broker-credentials/replace.

    ``changed=False`` when the new secret is identical to the active
    row's plaintext (identical-secret short-circuit). The credential
    metadata still reflects the existing row.
    """

    changed: bool
    credential: CredentialMetadataOut


@router.put("/replace", response_model=ReplaceCredentialResponse)
def replace(
    body: ReplaceCredentialRequest,
    request: Request,
    session: SessionRow = Depends(require_session),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> ReplaceCredentialResponse:
    """Atomically revoke and re-insert an active broker credential row.

    All work runs in one transaction:
      1. SELECT the active row for the supplied label FOR UPDATE.
      2. Decrypt the existing ciphertext and compare to the new
         plaintext. If identical, short-circuit with changed=False;
         no row update, no NOTIFY (avoids spurious VALID -> UNTESTED
         -> VALID flap from an idempotent re-save).
      3. Otherwise: ``revoked_at = NOW()`` on the existing row,
         INSERT a new row with ``health_state='untested'``.
      4. Commit.

    Returns 404 if no active row exists for the label (callers should
    POST instead). Returns 503 if the existing ciphertext cannot be
    decrypted (key material issue).

    The new row is UNTESTED until ``validate-stored`` probe success
    flips it to VALID. Subscribers see one NOTIFY at most: the
    underlying row update fires no NOTIFY itself (handled by the
    insert default state); when validate-stored runs after this
    endpoint and probes successfully, that probe's write-through
    fires the VALID NOTIFY.
    """
    try:
        provider_norm = normalise_provider(body.provider)
        label_norm = normalise_label(body.label)
        env_norm = normalise_environment(body.environment)
        secret_norm = normalise_secret(body.secret)
    except CredentialValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    # Look up the active row for this label. We need to keep the
    # decrypt + compare + revoke + insert together in one tx; doing
    # the lookup inside the tx with FOR UPDATE serialises against
    # concurrent replace calls for the same (operator, label).
    audit_pool = getattr(request.app.state, "audit_pool", None)

    # Flush any implicit transaction the dependency machinery may have
    # opened on this connection so the next ``conn.transaction()``
    # opens a real top-level txn rather than a savepoint that defers
    # commit until the dependency teardown (Codex pre-push r1.4).
    # Mirrors the same pattern in POST /broker-credentials.
    conn.commit()

    with conn.transaction():
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT id, last_four
                  FROM broker_credentials
                 WHERE operator_id = %(op)s
                   AND provider    = %(prov)s
                   AND label       = %(label)s
                   AND environment = %(env)s
                   AND revoked_at IS NULL
                 FOR UPDATE
                """,
                {
                    "op": session.operator_id,
                    "prov": provider_norm,
                    "label": label_norm,
                    "env": env_norm,
                },
            )
            existing = cur.fetchone()

        if existing is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No active credential to replace. Use POST /broker-credentials to create.",
            )

        # Snapshot the operator aggregate BEFORE the revoke+insert so
        # we can NOTIFY subscribers of the resulting transition. Without
        # this, a VALID -> UNTESTED move (replacing a VALID row with a
        # fresh UNTESTED one) would silently bypass the cache (Codex
        # pre-push r1.3).
        old_aggregate = get_operator_credential_health(
            conn,
            operator_id=session.operator_id,
            provider=provider_norm,
            environment=env_norm,
        )

        # Identical-secret short-circuit (Codex r2.3): decrypt existing
        # ciphertext and compare to new plaintext. Avoids the spurious
        # VALID -> UNTESTED -> VALID flap from an idempotent re-save.
        try:
            existing_plaintext = load_credential_for_provider_use(
                conn,
                operator_id=session.operator_id,
                provider=provider_norm,
                label=label_norm,
                environment=env_norm,
                caller="replace-compare",
                audit_pool=audit_pool,
            )
        except CredentialDecryptError as exc:
            logger.error("replace: decryption of existing row failed", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Existing credential decryption failed. Check server key material.",
            ) from exc

        if existing_plaintext == secret_norm:
            # Same secret. Return the existing row's metadata; no row
            # update, no NOTIFY.
            existing_meta = next(
                (m for m in list_credentials(conn, operator_id=session.operator_id) if m.id == existing["id"]),
                None,
            )
            if existing_meta is None:
                # Logical impossibility: we just selected this row inside
                # the transaction and now list_credentials returns no
                # match. Fail loudly rather than silently default.
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Internal: existing credential not found in same transaction.",
                )
            return ReplaceCredentialResponse(
                changed=False,
                credential=_to_out(existing_meta),
            )

        # Different secret. Revoke the existing row, then insert.
        revoke_credential(
            conn,
            credential_id=existing["id"],
            operator_id=session.operator_id,
        )
        new_meta = store_credential(
            conn,
            operator_id=session.operator_id,
            provider=provider_norm,
            label=label_norm,
            environment=env_norm,
            plaintext=secret_norm,
        )

        # NOTIFY subscribers if the aggregate actually moved. Inside
        # the same tx as the row mutations so the notify commits
        # alongside (and after) the durable state change.
        notify_aggregate_if_changed(
            conn,
            operator_id=session.operator_id,
            provider=provider_norm,
            environment=env_norm,
            old_aggregate=old_aggregate,
        )

    return ReplaceCredentialResponse(
        changed=True,
        credential=_to_out(new_meta),
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _lookup_active_credential_ids(
    conn: psycopg.Connection[object],
    *,
    operator_id: UUID,
    provider: str,
    environment: str,
) -> dict[str, UUID]:
    """Return ``{label: credential_id}`` for the operator's active rows.

    Used by validate-stored to find the rows whose health to write
    through after a probe outcome. The probe call doesn't need the IDs
    itself — only the outcome plumbing does.

    Returns an empty dict if the operator has no active rows for the
    provider/environment. Caller is responsible for treating that as
    "no write-throughs to perform".
    """
    out: dict[str, UUID] = {}
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT id, label
              FROM broker_credentials
             WHERE operator_id = %(op)s
               AND provider    = %(prov)s
               AND environment = %(env)s
               AND revoked_at IS NULL
            """,
            {"op": operator_id, "prov": provider, "env": environment},
        )
        for row in cur.fetchall():
            out[row["label"]] = row["id"]
    return out
