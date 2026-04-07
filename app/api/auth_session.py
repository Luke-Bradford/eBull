"""Browser-session auth endpoints (issue #98).

Routes:
  POST /auth/login   -- username + password -> session cookie
  POST /auth/logout  -- delete session row + clear cookie
  GET  /auth/me      -- current operator (or 401)

Cookie:
  HttpOnly, SameSite=Lax. ``Secure`` is controlled by
  ``settings.session_cookie_secure`` so the dev stack works without TLS.
  Production deploys MUST set the env var to enable Secure (documented in
  .env.example).

Rate limiting:
  In-process per-IP and per-username sliding-window counter. Best-effort
  only -- it does not survive process restart and does not coordinate
  across workers. Documented as such; the durable lockout policy is
  Ticket E (#102).

Generic 401 / 429 discipline:
  All login failure modes (no such operator, wrong password, missing
  fields) return the same generic 401 ``"Unauthorized"``. Rate-limit
  rejection is the only place we return 429, and the body is a fixed
  phrase that does not reveal whether the IP or the username triggered
  the limiter.
"""

from __future__ import annotations

import logging
from collections import deque
from datetime import timedelta
from threading import Lock
from time import monotonic
from uuid import UUID

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel, Field

from app.api.auth import _unauthorized, require_session
from app.config import settings
from app.db import get_conn
from app.security.passwords import verify_password
from app.security.sessions import (
    SessionRow,
    create_session,
    delete_session,
    touch_last_login,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["auth"])


# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------
#
# Sliding-window counter, separate buckets for "ip" and "username". Each
# bucket holds a deque of failure timestamps; an attempt is rejected if the
# bucket has reached MAX_FAILURES within WINDOW_SECONDS.
#
# Successful logins reset the per-username bucket so the limiter does not
# punish a legitimate user who fat-fingered a password. The per-IP bucket
# is intentionally NOT reset on success: a credential-stuffing attacker
# would otherwise wash their bucket every time they happened to guess.

_RATE_WINDOW = timedelta(minutes=15)
_RATE_MAX_IP = 50
_RATE_MAX_USERNAME = 10


class _RateLimiter:
    """Best-effort in-process sliding window. Not durable, not multi-worker."""

    def __init__(self) -> None:
        self._buckets: dict[str, deque[float]] = {}
        self._lock = Lock()

    def _trim(self, key: str, now: float) -> deque[float]:
        bucket = self._buckets.setdefault(key, deque())
        cutoff = now - _RATE_WINDOW.total_seconds()
        while bucket and bucket[0] < cutoff:
            bucket.popleft()
        return bucket

    def is_blocked(self, ip_key: str, username_key: str) -> bool:
        now = monotonic()
        with self._lock:
            ip_bucket = self._trim(ip_key, now)
            username_bucket = self._trim(username_key, now)
            return len(ip_bucket) >= _RATE_MAX_IP or len(username_bucket) >= _RATE_MAX_USERNAME

    def record_failure(self, ip_key: str, username_key: str) -> None:
        now = monotonic()
        with self._lock:
            self._trim(ip_key, now).append(now)
            self._trim(username_key, now).append(now)

    def reset_username(self, username_key: str) -> None:
        with self._lock:
            self._buckets.pop(username_key, None)


_rate_limiter = _RateLimiter()


def _ip_key(request: Request) -> str:
    # request.client may be None for some test transports.
    client = request.client
    return f"ip:{client.host}" if client else "ip:unknown"


def _username_key(username: str) -> str:
    # Lowercase so case variants share a bucket -- avoids trivial bypass.
    return f"user:{username.strip().lower()}"


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class LoginRequest(BaseModel):
    username: str = Field(min_length=1, max_length=255)
    password: str = Field(min_length=1, max_length=1024)


class OperatorView(BaseModel):
    id: UUID
    username: str


class LoginResponse(BaseModel):
    operator: OperatorView


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _set_session_cookie(response: Response, *, session_id: str, max_age_seconds: int) -> None:
    response.set_cookie(
        key=settings.session_cookie_name,
        value=session_id,
        max_age=max_age_seconds,
        httponly=True,
        secure=settings.session_cookie_secure,
        samesite="lax",
        path="/",
    )


def _clear_session_cookie(response: Response) -> None:
    response.delete_cookie(
        key=settings.session_cookie_name,
        path="/",
        httponly=True,
        secure=settings.session_cookie_secure,
        samesite="lax",
    )


def _too_many() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        detail="Too many login attempts",
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.post("/login", response_model=LoginResponse)
def login(
    body: LoginRequest,
    request: Request,
    response: Response,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> LoginResponse:
    """Authenticate the operator and issue a session cookie.

    Failure modes (all return generic 401):
      * unknown username
      * wrong password
      * any DB / verification error

    Rate limit (returns 429): exceeding either the per-IP or per-username
    sliding window. The counter is reset for the username on success.
    """
    ip_key = _ip_key(request)
    username_key = _username_key(body.username)

    if _rate_limiter.is_blocked(ip_key, username_key):
        raise _too_many()

    # Look up the operator. We always run verify_password against *some*
    # hash so the timing of "no such user" matches the timing of "wrong
    # password" -- otherwise an attacker can enumerate usernames.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT operator_id, username, password_hash FROM operators WHERE username = %s",
            (body.username.strip(),),
        )
        row = cur.fetchone()

    # Use a stable dummy hash so the verify-against-nobody branch still
    # spends roughly the same time as the real branch. The dummy must be
    # a valid Argon2id PHC string or argon2-cffi raises before hashing.
    DUMMY_HASH = (
        "$argon2id$v=19$m=65536,t=3,p=4$"
        "ZHVtbXlzYWx0ZHVtbXlzYWx0$"
        "ZHVtbXloYXNoZHVtbXloYXNoZHVtbXloYXNoZHVtbXloYXNoZHVtbXloYXNoMA"
    )
    stored_hash = row["password_hash"] if row is not None else DUMMY_HASH

    if not verify_password(body.password, stored_hash) or row is None:
        _rate_limiter.record_failure(ip_key, username_key)
        raise _unauthorized()

    operator_id: UUID = row["operator_id"]
    username: str = row["username"]

    # Create the session row + bump last_login_at in a single transaction
    # so a partial write cannot leave a session without an updated stamp.
    user_agent = request.headers.get("user-agent")
    client = request.client
    ip = client.host if client else None
    absolute = timedelta(hours=settings.session_absolute_timeout_hours)
    with conn.transaction():
        session_id, expires_at = create_session(
            conn,
            operator_id=operator_id,
            user_agent=user_agent,
            ip=ip,
            absolute_timeout=absolute,
        )
        touch_last_login(conn, operator_id=operator_id)

    _set_session_cookie(
        response,
        session_id=session_id,
        max_age_seconds=int(absolute.total_seconds()),
    )
    _rate_limiter.reset_username(username_key)
    logger.info("operator login: %s", username)

    return LoginResponse(operator=OperatorView(id=operator_id, username=username))


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
def logout(
    request: Request,
    response: Response,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> Response:
    """Delete the current session row and clear the cookie.

    Always returns 204 -- logging out a stale or missing session is not an
    error from the caller's POV.
    """
    cookie = request.cookies.get(settings.session_cookie_name)
    if cookie:
        delete_session(conn, session_id=cookie)
    _clear_session_cookie(response)
    response.status_code = status.HTTP_204_NO_CONTENT
    return response


@router.get("/me", response_model=OperatorView)
def me(session: SessionRow = Depends(require_session)) -> OperatorView:
    """Return the operator behind the current session cookie.

    Used by the frontend on app boot to decide whether to render the app
    or redirect to /login. Bare ``require_session`` (not the combined dep)
    is intentional: this endpoint is the canonical "is my browser session
    alive?" probe and should never accept a service token.
    """
    return OperatorView(id=session.operator_id, username=session.username)
