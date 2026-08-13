"""DB-down degradation of the auth layer + get_conn (#1325 / #1217).

Before this fix, ``require_session_or_service_token`` declared
``conn: psycopg.Connection = Depends(get_conn)`` in its signature.
FastAPI resolves signature sub-dependencies BEFORE the function body,
so a pooled connection was opened on every request to a protected
route — including bearer-token requests that need no DB and the
``/system/*`` diagnostic endpoints whose whole purpose is reporting DB
health. With the DB down that surfaced as a 500/AttributeError (or
masked the failure as a 401).

These tests pin the degraded posture:
  * bearer-valid request succeeds even when the pool is dead (no DB
    touched on the bearer branch);
  * unauthenticated request 401s before touching the DB;
  * a cookie-bearing request whose session lookup needs the (dead) DB
    surfaces ``get_conn``'s 503;
  * ``get_conn`` itself maps every "no usable connection" failure
    (missing pool / OperationalError / PoolTimeout) to 503, with a
    fixed detail phrase (prevention-log #86: no exception-text leak).

Pure unit tests: the auth dep + get_conn are driven directly with a
mock Request, so no live Postgres is required.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import psycopg
import pytest
from fastapi import HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials
from psycopg_pool import PoolTimeout

from app.api.auth import require_session_or_service_token
from app.db import get_conn

_VALID_TOKEN = "test-operator-token-with-32-chars"  # meets min length


def _request(*, cookies: dict[str, str] | None = None, db_pool: object = None) -> Request:
    """Minimal stand-in for a FastAPI Request the auth dep / get_conn read."""
    req = MagicMock(spec=Request)
    req.cookies = cookies or {}
    # app.state.db_pool is read via getattr(..., "db_pool", None); set it
    # explicitly (a bare MagicMock would auto-vivify a truthy attribute).
    req.app = SimpleNamespace(state=SimpleNamespace(db_pool=db_pool))
    return req


def _dead_pool() -> MagicMock:
    """A pool whose checkout raises as if Postgres were unreachable."""
    pool = MagicMock()
    pool.connection.side_effect = psycopg.OperationalError("connection refused")
    return pool


# ---------------------------------------------------------------------------
# require_session_or_service_token — degraded auth posture
# ---------------------------------------------------------------------------


def test_bearer_path_succeeds_when_pool_is_dead() -> None:
    """A valid bearer token authenticates without touching the DB, even
    when the pool is dead (#1217 acceptance #2)."""
    creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials=_VALID_TOKEN)
    req = _request(db_pool=_dead_pool())
    with patch("app.api.auth.settings") as s:
        s.service_token = _VALID_TOKEN
        s.session_cookie_name = "ebull_session"
        s.session_idle_timeout_minutes = 60
        # Must NOT raise — returns None on success.
        assert require_session_or_service_token(req, creds) is None


def test_unauthenticated_returns_401_without_touching_db() -> None:
    """No bearer + no cookie → 401, before any DB access (#1217 #2)."""
    req = _request(cookies={}, db_pool=_dead_pool())
    with patch("app.api.auth.settings") as s:
        s.service_token = _VALID_TOKEN
        s.session_cookie_name = "ebull_session"
        s.session_idle_timeout_minutes = 60
        with pytest.raises(HTTPException) as exc:
            require_session_or_service_token(req, None)
    assert exc.value.status_code == 401
    # Pool was never consulted on the unauthenticated path.
    req.app.state.db_pool.connection.assert_not_called()


def test_cookie_path_surfaces_503_when_pool_is_dead() -> None:
    """A request with a session cookie needs the DB to resolve it; when
    the pool is dead the get_conn 503 propagates (not a 401/500)."""
    req = _request(cookies={"ebull_session": "opaque-session-id"}, db_pool=_dead_pool())
    with patch("app.api.auth.settings") as s:
        s.service_token = _VALID_TOKEN
        s.session_cookie_name = "ebull_session"
        s.session_idle_timeout_minutes = 60
        with pytest.raises(HTTPException) as exc:
            require_session_or_service_token(req, None)
    assert exc.value.status_code == 503


# ---------------------------------------------------------------------------
# get_conn — every "no usable connection" failure maps to 503
# ---------------------------------------------------------------------------


def test_get_conn_503_when_pool_missing() -> None:
    """Lifespan never created the pool (or it was torn down) → 503, not
    an AttributeError/500."""
    gen = get_conn(_request(db_pool=None))
    with pytest.raises(HTTPException) as exc:
        next(gen)
    assert exc.value.status_code == 503
    assert exc.value.detail == "database temporarily unavailable"


@pytest.mark.parametrize(
    "exc_type",
    [psycopg.OperationalError("connection refused"), PoolTimeout("pool saturated")],
)
def test_get_conn_503_on_checkout_failure(exc_type: Exception) -> None:
    """OperationalError (DB down/recovery) and PoolTimeout both map to a
    503 with a fixed detail phrase — never the exception text."""
    pool = MagicMock()
    pool.connection.side_effect = exc_type
    gen = get_conn(_request(db_pool=pool))
    with pytest.raises(HTTPException) as exc:
        next(gen)
    assert exc.value.status_code == 503
    assert exc.value.detail == "database temporarily unavailable"
    # Prevention-log #86: no driver/SQL error text in the response body.
    assert "refused" not in exc.value.detail
    assert "saturated" not in exc.value.detail
