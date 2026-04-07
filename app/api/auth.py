"""Operator authentication for protected API endpoints.

Two auth modes coexist (per ADR 0001):

  * **Browser session** (the normal operator path)
    The browser logs in via POST /auth/login and is issued an HttpOnly
    cookie holding an opaque session id. ``require_session`` resolves the
    cookie -> sessions table -> operators row.

  * **Service token** (tests, scripts, cron, future webhooks)
    A static bearer token compared timing-safe against
    ``settings.service_token``. Supplied as ``Authorization: Bearer <token>``.
    This is the renamed successor to the old ``settings.api_key`` /
    ``require_auth`` path -- behaviour is unchanged, only the name moved.

  * **Combined dependency** ``require_session_or_service_token``
    Accepts either path. Used by every protected route so the same handler
    serves the browser and the test/script clients without duplication.

Generic 401 discipline:
  Every failure mode -- missing cookie, invalid cookie, expired session,
  missing bearer header, wrong bearer scheme, wrong bearer value, unset
  server-side service_token -- raises the same generic
  ``HTTPException(401, "Unauthorized")``. Callers cannot distinguish which
  failure happened. This matches the prior behaviour of ``require_auth``.

Fail-closed:
  If ``settings.service_token`` is unset (None or empty), the bearer path
  rejects every request. We do **not** treat unset config as "auth
  disabled". The session path is independent: an unset service token does
  not break the operator browser flow.
"""

from __future__ import annotations

import secrets
from datetime import timedelta

import psycopg
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.config import settings
from app.db import get_conn
from app.security.sessions import SessionRow, get_active_session

_bearer_scheme = HTTPBearer(auto_error=False)


def _unauthorized() -> HTTPException:
    """Build a fresh 401 response.

    A fresh ``HTTPException`` is constructed per raise rather than reused
    from a module-level singleton -- removes any risk of shared-state bugs
    and matches the conventional FastAPI pattern.
    """
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Unauthorized",
        headers={"WWW-Authenticate": "Bearer"},
    )


# ---------------------------------------------------------------------------
# Service-token dependency (renamed from require_auth)
# ---------------------------------------------------------------------------


def require_service_token(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
) -> None:
    """FastAPI dependency that enforces a valid bearer service token.

    Returns ``None`` on success; raises ``HTTPException(401)`` otherwise.
    The same generic 401 is raised for missing credentials, wrong scheme,
    wrong token, and unset server-side ``service_token``.
    """
    expected = settings.service_token
    if not expected:
        # Fail closed: misconfigured server must not leak protected data.
        raise _unauthorized()

    if credentials is None or credentials.scheme.lower() != "bearer":
        raise _unauthorized()

    if not secrets.compare_digest(credentials.credentials, expected):
        raise _unauthorized()


# ---------------------------------------------------------------------------
# Browser session dependency
# ---------------------------------------------------------------------------


def require_session(
    request: Request,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> SessionRow:
    """FastAPI dependency that resolves the session cookie to an operator row.

    Returns the ``SessionRow`` on success; raises ``HTTPException(401)``
    otherwise. Same generic-401 discipline as the bearer path: missing
    cookie, malformed cookie, no matching row, expired absolute window,
    and idle timeout exceeded all raise the same exception.
    """
    cookie = request.cookies.get(settings.session_cookie_name)
    if not cookie:
        raise _unauthorized()

    row = get_active_session(
        conn,
        session_id=cookie,
        idle_timeout=timedelta(minutes=settings.session_idle_timeout_minutes),
    )
    if row is None:
        raise _unauthorized()
    return row


# ---------------------------------------------------------------------------
# Combined dependency
# ---------------------------------------------------------------------------


def require_session_or_service_token(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> None:
    """Accepts either a valid session cookie OR a valid bearer service token.

    Order of evaluation:
      1. If a bearer header is present, evaluate it. A present-but-wrong
         bearer is a hard 401 -- we do NOT silently fall back to the
         session path, because the caller has explicitly chosen which auth
         mode they intended.
      2. Otherwise, fall back to the session cookie path.

    Both branches end in the same generic 401 on failure.
    """
    if credentials is not None:
        expected = settings.service_token
        if not expected:
            raise _unauthorized()
        if credentials.scheme.lower() != "bearer":
            raise _unauthorized()
        if not secrets.compare_digest(credentials.credentials, expected):
            raise _unauthorized()
        return

    cookie = request.cookies.get(settings.session_cookie_name)
    if not cookie:
        raise _unauthorized()

    row = get_active_session(
        conn,
        session_id=cookie,
        idle_timeout=timedelta(minutes=settings.session_idle_timeout_minutes),
    )
    if row is None:
        raise _unauthorized()


# ---------------------------------------------------------------------------
# Backwards-compat alias (will be removed once all call sites are migrated
# in this PR -- kept only to make the rename mechanical)
# ---------------------------------------------------------------------------

# Intentionally NOT exporting an alias for the old name. Every call site is
# updated in this PR; leaving an alias would invite the rename to be
# half-done. Grep for ``require_auth`` after this PR -- it must return zero.
