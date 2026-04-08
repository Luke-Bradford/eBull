"""Bootstrap-state + recovery endpoints (#114 / ADR-0003).

Public, unauthenticated routes used by the frontend to decide whether
to route to /setup, /recover, or the normal app shell.

  GET  /auth/bootstrap-state   -- {"needs_setup", "recovery_required",
                                   "boot_state"}
  POST /auth/recover           -- accepts a 24-word recovery phrase,
                                   verifies it against existing
                                   ciphertext, persists the recovered
                                   root secret, installs the derived
                                   key, and clears recovery_required.

Both endpoints set ``Cache-Control: no-store`` so a CDN/proxy cannot
serve a stale boot state to a fresh client. The recovery phrase is
read from the request body once and never logged.

The :func:`require_master_key` dependency lives here too: it is the
structural gate that broker routes hang off. If the active key is not
loaded (clean_install with no creds yet, or recovery_required) it
fails with 503 and a fixed-string detail; the frontend interprets the
fixed string and routes to /recover or surfaces a setup prompt.
"""

from __future__ import annotations

import logging

import psycopg
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel, Field

from app.db import get_conn
from app.security import master_key
from app.security.recovery_phrase import PHRASE_WORD_COUNT, RecoveryPhraseError

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["auth"])


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class BootstrapStateResponse(BaseModel):
    boot_state: str
    needs_setup: bool
    recovery_required: bool


class RecoverRequest(BaseModel):
    # The phrase is accepted as a single whitespace-separated string so
    # the frontend does not need to commit to a list shape. Length is
    # validated downstream by decode_phrase, but we cap the raw length
    # so a junk megabyte body cannot be uploaded.
    phrase: str = Field(min_length=1, max_length=4096)


class RecoverResponse(BaseModel):
    boot_state: str
    recovery_required: bool


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get("/bootstrap-state", response_model=BootstrapStateResponse)
def bootstrap_state(request: Request, response: Response) -> BootstrapStateResponse:
    """Return the current boot state from ``app.state``.

    Reads in-memory flags written by the lifespan / recovery / lazy-gen
    paths. Does not touch the database -- bootstrap state is settled
    once at startup and only mutates via explicit recovery or first
    credential save, both of which update ``app.state`` in-process.
    """
    response.headers["Cache-Control"] = "no-store"
    state = getattr(request.app.state, "boot_state", "clean_install")
    return BootstrapStateResponse(
        boot_state=state,
        needs_setup=getattr(request.app.state, "needs_setup", False),
        recovery_required=getattr(request.app.state, "recovery_required", False),
    )


@router.post("/recover", response_model=RecoverResponse)
def recover(
    body: RecoverRequest,
    request: Request,
    response: Response,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> RecoverResponse:
    """Verify a recovery phrase and clear recovery_required on success.

    Failure modes (all return 400 with a fixed-string detail so the
    frontend cannot tell typo from wrong phrase):
      * malformed phrase / wrong word / bad checksum
      * verifies a valid 32-byte secret but does not match ciphertext
      * wrong word count

    On success: persist the root secret to disk, install the derived
    key into the secrets_crypto cache, flip ``app.state.boot_state``
    to ``normal``, and clear ``recovery_required``.
    """
    response.headers["Cache-Control"] = "no-store"

    # Refuse the call entirely if the app is not in recovery_required
    # mode (acceptance criterion / ADR-0003 §6). Recovery is meant to
    # be used exactly once after a wiped data dir; calling it from a
    # healthy boot state is a state-machine error and we surface it
    # as 409 rather than silently re-deriving the key.
    if not getattr(request.app.state, "recovery_required", False):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="recovery not required",
        )

    # Pre-validate word count cheaply so a clearly-wrong submission
    # never reaches the DB verify path.
    words = body.phrase.strip().split()
    if len(words) != PHRASE_WORD_COUNT:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="recovery phrase invalid",
        )

    # ``recover_from_phrase`` performs the entire verify -> write
    # -> install -> flip-all-state sequence atomically inside
    # ``lazy_gen_lock``. We must NOT touch ``app.state`` from
    # here for any gating flag: a concurrent reader of
    # bootstrap-state or any handler gated on ``require_master_key``
    # could otherwise observe an incoherent intermediate state
    # between lock release and our writes (review feedback
    # PR #118 round 8).
    try:
        master_key.recover_from_phrase(conn, body.phrase, request.app.state)
    except (
        RecoveryPhraseError,
        master_key.RecoveryVerificationError,
        master_key.RecoveryNotApplicableError,
    ) as exc:
        # ADR-0003 §6: every phrase-path failure mode returns the
        # SAME generic 400 so a caller cannot fingerprint
        # "wrong phrase" vs "no row to verify against" vs
        # "checksum bad" by comparing status codes. The distinct
        # exception classes exist purely for server-side logs
        # (round 18 reverted the round-17 409 mapping that broke
        # this contract).
        # Same generic detail for every failure mode -- typo,
        # checksum, wrong-but-valid phrase. Full reason in
        # server log.
        logger.warning("recovery phrase rejected: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="recovery phrase invalid",
        ) from exc

    logger.info("master key recovered from phrase; boot_state=normal")

    return RecoverResponse(boot_state="normal", recovery_required=False)


# ---------------------------------------------------------------------------
# Dependency: structural gate for routes that need the broker key
# ---------------------------------------------------------------------------


def require_master_key(request: Request) -> None:
    """Block a request when the broker-encryption key is not loaded.

    Mounted on broker routes that need to encrypt or decrypt right
    now. This dependency is intentionally NOT mounted on
    ``POST /broker-credentials`` -- that route owns its own gating
    because the very first save legitimately enters with no key
    loaded and lazy-generates one inside the handler. Every other
    route mounted on this dependency must have a loaded key.

    Failure modes:
      * ``recovery_required`` -> 503 ``"recovery required"``
        (operator must call /auth/recover)
      * any not-loaded state (clean_install with no key yet,
        env-override misconfiguration, internal bug) -> 503
        ``"master key not loaded"``. Without this branch a route
        mounted on this dependency would slip past and hit
        ``CredentialCryptoConfigError`` -> 500 instead of the
        documented 503 (review feedback PR #118 round 9/10).
    """
    if getattr(request.app.state, "recovery_required", False):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="recovery required",
        )
    if not getattr(request.app.state, "broker_key_loaded", False):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="master key not loaded",
        )
