"""DB-backed opaque session storage.

Sessions are opaque random tokens stored server-side. The browser only ever
holds the session id in an HttpOnly cookie -- nothing about the operator
identity is encoded in the token itself. This means revocation is trivial
(delete the row) and a stolen cookie can be killed without rotating any
signing key.

Token shape:
  * 32 bytes from ``secrets.token_urlsafe`` -> ~43 url-safe chars.
  * Stored as the table primary key. We do not hash it: the threat model
    here is "DB read" (in which case all rows are compromised regardless of
    hashing) and "cookie steal" (in which case hashing the column does not
    help). If that calculus changes, hashing can be added later as a
    one-off migration.

Timeouts (enforced in get_active_session):
  * absolute_timeout -- session is killed N hours after creation regardless
    of activity. Forces re-auth on long-lived browser tabs.
  * idle_timeout    -- session is killed if no activity for N minutes.
"""

from __future__ import annotations

import secrets
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID

import psycopg


def _utcnow() -> datetime:
    """Return an aware UTC ``datetime``.

    All session timestamps are TIMESTAMPTZ; we always pass aware datetimes
    to avoid the naive-vs-aware mixed-offset trap (review-prevention-log
    entry on naive datetime in TIMESTAMPTZ params).
    """
    return datetime.now(UTC)


@dataclass(frozen=True)
class SessionRow:
    session_id: str
    operator_id: UUID
    username: str
    expires_at: datetime
    last_seen_at: datetime


def create_session(
    conn: psycopg.Connection[object],
    *,
    operator_id: UUID,
    user_agent: str | None,
    ip: str | None,
    absolute_timeout: timedelta,
) -> tuple[str, datetime]:
    """Insert a new session row and return ``(session_id, expires_at)``.

    The caller is responsible for setting the session id as an HttpOnly
    cookie on the response with the same expiry.
    """
    session_id = secrets.token_urlsafe(32)
    now = _utcnow()
    expires_at = now + absolute_timeout
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sessions (session_id, operator_id, created_at, expires_at,
                                  last_seen_at, user_agent, ip)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (session_id, operator_id, now, expires_at, now, user_agent, ip),
        )
    return session_id, expires_at


def get_active_session(
    conn: psycopg.Connection[object],
    *,
    session_id: str,
    idle_timeout: timedelta,
) -> SessionRow | None:
    """Look up *session_id* and return the row if it is still valid.

    Returns ``None`` for any of: missing row, absolute-timeout exceeded,
    idle-timeout exceeded. The caller MUST treat all None paths identically
    -- the HTTP layer returns the same generic 401 in every case.

    On a successful lookup, ``last_seen_at`` is bumped to now so the idle
    window is rolling.
    """
    now = _utcnow()
    # Explicit commit / rollback per path. We deliberately do NOT wrap
    # the body in ``conn.transaction()`` because in psycopg v3 a normal
    # ``return`` from inside that context manager **commits** the
    # transaction; for the early-return paths (missing row, expired,
    # idle-timeout exceeded) we want a rollback, not a commit. If a
    # future caller wraps this in their own ``conn.transaction()``, an
    # implicit commit on the savepoint would prematurely promote the
    # caller's intermediate state -- a latent correctness bug.
    #
    # Pattern: do the SELECT, decide, then either rollback-and-return or
    # write-then-commit. The outer ``try / except`` converts any
    # unexpected error into a rollback so the connection is never
    # returned to the pool with dirty state.
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT s.session_id, s.operator_id, o.username,
                       s.expires_at, s.last_seen_at
                FROM sessions s
                JOIN operators o USING (operator_id)
                WHERE s.session_id = %s
                """,
                (session_id,),
            )
            row = cur.fetchone()
            if row is None:
                conn.rollback()
                return None
            # Default cursor returns a TupleRow; pyright sees it as
            # ``object`` without an explicit row factory, so we
            # destructure via index.
            sid: str = row[0]  # type: ignore[index]
            operator_id: UUID = row[1]  # type: ignore[index]
            username: str = row[2]  # type: ignore[index]
            expires_at: datetime = row[3]  # type: ignore[index]
            last_seen_at: datetime = row[4]  # type: ignore[index]

            if expires_at <= now or (now - last_seen_at) > idle_timeout:
                # Reap the dead row inline so expired sessions do not
                # accumulate forever -- there is no separate reaper job.
                # We still return None to the caller because the session
                # is no longer valid for auth.
                cur.execute(
                    "DELETE FROM sessions WHERE session_id = %s",
                    (sid,),
                )
                conn.commit()
                return None

            # Roll the idle window.
            cur.execute(
                "UPDATE sessions SET last_seen_at = %s WHERE session_id = %s",
                (now, sid),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return SessionRow(
        session_id=sid,
        operator_id=operator_id,
        username=username,
        expires_at=expires_at,
        last_seen_at=now,
    )


def delete_session(conn: psycopg.Connection[object], *, session_id: str) -> None:
    """Delete *session_id*. No-op if it does not exist.

    Used by /auth/logout. We do not raise on missing rows -- a logout for
    an already-expired session should still succeed from the caller's POV.
    """
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute("DELETE FROM sessions WHERE session_id = %s", (session_id,))


def touch_last_login(conn: psycopg.Connection[object], *, operator_id: UUID) -> None:
    """Stamp ``operators.last_login_at`` on a successful login.

    Self-contained transaction so the write is durable when called outside
    a caller-managed ``conn.transaction()``. When the caller already has an
    open transaction (as ``login`` does), psycopg promotes this to a
    savepoint, which is the desired nesting behaviour.
    """
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE operators SET last_login_at = %s WHERE operator_id = %s",
                (_utcnow(), operator_id),
            )
