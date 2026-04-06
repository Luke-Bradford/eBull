"""Operator authentication for protected API endpoints.

eBull is a single-operator tool, but it controls real money decisions. The
kill switch, runtime config, portfolio state, recommendations, and execution
audit must not be exposed without authentication.

Mechanism:
  - Static bearer token compared timing-safe against ``settings.api_key``.
  - The token is supplied as ``Authorization: Bearer <token>``.
  - ``HTTPBearer(auto_error=False)`` is used so that we can return a single
    generic 401 for both "no header" and "wrong token" — we never tell a
    caller whether the token was missing or invalid.

Fail-closed:
  - If ``settings.api_key`` is unset (None or empty), every protected request
    is rejected with 401. We do **not** treat unset config as "auth disabled".
    This is a deliberate choice — a misconfigured deploy must not silently
    leave the kill switch open.

Out of scope (per issue #58):
  - User management, RBAC, OAuth/SSO, session management, token rotation.
"""

from __future__ import annotations

import secrets

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.config import settings

_bearer_scheme = HTTPBearer(auto_error=False)

_UNAUTHORIZED = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Unauthorized",
    headers={"WWW-Authenticate": "Bearer"},
)


def require_auth(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
) -> None:
    """FastAPI dependency that enforces a valid bearer token.

    Returns ``None`` on success; raises ``HTTPException(401)`` otherwise.
    The same generic 401 is raised for missing credentials, wrong scheme,
    wrong token, and unset server-side ``api_key`` — callers cannot
    distinguish these cases.
    """
    expected = settings.api_key
    if not expected:
        # Fail closed: misconfigured server must not leak protected data.
        raise _UNAUTHORIZED

    if credentials is None or credentials.scheme.lower() != "bearer":
        raise _UNAUTHORIZED

    if not secrets.compare_digest(credentials.credentials, expected):
        raise _UNAUTHORIZED
