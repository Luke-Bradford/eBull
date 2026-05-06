"""Bootstrap-state endpoint (#114 / ADR-0003, amended 2026-05-07).

Public, unauthenticated route used by the frontend to decide whether
to route to /setup or the normal app shell.

  GET  /auth/bootstrap-state   -- {"needs_setup", "boot_state"}

Sets ``Cache-Control: no-store`` so a CDN/proxy cannot serve a stale
boot state to a fresh client.

The :func:`require_master_key` dependency lives here too: it is the
structural gate that broker routes hang off. If the active key is not
loaded (clean_install with no creds yet) it fails with 503 and a
fixed-string detail; the frontend interprets the fixed string and
surfaces a setup prompt.

Post-amendment: the recovery_phrase ceremony, ``POST /auth/recover``
endpoint, and ``recovery_required`` boot state are removed. Stale
ciphertext is soft-revoked at lifespan boot; recovery posture is
operator-driven re-entry of eToro keys.
"""

from __future__ import annotations

import logging

import psycopg
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel

from app.db import get_conn
from app.services.operator_setup import operators_empty

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["auth"])


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class BootstrapStateResponse(BaseModel):
    boot_state: str
    needs_setup: bool


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get("/bootstrap-state", response_model=BootstrapStateResponse)
def bootstrap_state(
    request: Request,
    response: Response,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> BootstrapStateResponse:
    """Return the current boot state.

    ``boot_state`` reports the master-key state machine
    (``clean_install`` or ``normal``). ``needs_setup`` is operator-state
    only — derived from ``operators_empty(conn)`` per request, decoupled
    from credential/key state. The two dimensions are independent: an
    existing operator with no broker credentials sees
    ``needs_setup=False`` and is served the normal app shell with an
    "add eToro creds" banner; a fresh DB with no operators sees
    ``needs_setup=True`` and routes to the single-step setup wizard.
    """
    response.headers["Cache-Control"] = "no-store"
    state = getattr(request.app.state, "boot_state", "clean_install")
    return BootstrapStateResponse(
        boot_state=state,
        needs_setup=operators_empty(conn),
    )


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
    """
    if not getattr(request.app.state, "broker_key_loaded", False):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="master key not loaded",
        )
