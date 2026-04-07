"""First-run setup service (issue #106 / ADR 0002).

Owns:
  * the in-memory bootstrap-token slot (single-use within a process)
  * the bootstrap-mode decision (Mode A loopback / Mode B token-required)
  * the locked transaction that creates the first operator + first
    session + the corresponding ``operator_audit`` row

The HTTP layer in ``app.api.auth_setup`` is a thin wrapper over this
module: it converts request fields into kwargs, calls in here, and
turns the (intentionally narrow) outcome enum into either a 200/cookie
response or a generic 404. All of the policy lives here so the policy
is testable without spinning up FastAPI.
"""

from __future__ import annotations

import logging
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from threading import Lock
from uuid import UUID

import psycopg

from app.config import settings
from app.security.passwords import hash_password
from app.security.sessions import create_session

logger = logging.getLogger(__name__)


# Pinned advisory-lock key for the setup transaction. Documented in
# docs/tickets/ticket-G-first-run-setup-and-operator-management.md.
# Must not be derived at runtime via hashtext() -- a literal makes the
# lock key reviewable and avoids a round-trip just to compute it.
_BOOTSTRAP_LOCK_KEY = 7263011

# Loopback host strings used by Mode A. Both IPv4 and IPv6 loopback are
# accepted because Starlette/uvicorn report the IPv6 form on dual-stack
# binds even when the user typed 127.0.0.1.
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})


def _is_loopback(host: str | None) -> bool:
    return host is not None and host in _LOOPBACK_HOSTS


# Minimum interactive-password length. Mirrors app/cli.py and the
# operator-management endpoint -- humans typing passwords get the same
# floor everywhere.
MIN_PASSWORD_LEN = 12


class SetupOutcome(Enum):
    """Result of a setup attempt.

    The HTTP layer maps everything except OK to the same generic 404 so
    callers cannot distinguish failure modes (ADR 0001 / 0002 generic
    discipline). The enum exists so tests can assert *which* failure
    mode was hit without parsing log lines.
    """

    OK = "ok"
    ALREADY_SETUP = "already_setup"
    BAD_TOKEN = "bad_token"
    BAD_PASSWORD = "bad_password"
    BAD_USERNAME = "bad_username"


@dataclass(frozen=True)
class SetupSuccess:
    operator_id: UUID
    username: str
    session_id: str
    expires_at: datetime


class _BootstrapTokenSlot:
    """Mutable holder for the active one-time bootstrap token.

    Python ``str`` is immutable and short strings are interned, so a
    plain module-level ``_token: str | None`` reassignment cannot give
    a meaningful "consumed" guarantee. We use a holder object whose
    attribute can be set to ``None`` after first use. The guarantee is
    that the holder no longer returns the token, not that the bytes
    are scrubbed from the process (CPython does not let us scrub an
    immutable str).
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._token: str | None = None

    def set(self, token: str | None) -> None:
        with self._lock:
            self._token = token

    def get(self) -> str | None:
        with self._lock:
            return self._token

    def consume(self) -> None:
        """Clear the slot. Idempotent."""
        with self._lock:
            self._token = None


_token_slot = _BootstrapTokenSlot()


def reset_token_slot_for_tests() -> None:
    """Test hook -- clear the slot between tests in the same process."""
    _token_slot.consume()


def resolve_bootstrap_token() -> str | None:
    """Return the currently-active bootstrap token, or None.

    Resolution order:
      1. ``settings.bootstrap_token`` (env-configured) wins if set.
      2. Otherwise return whatever the in-memory slot holds (which may
         have been populated by ``ensure_startup_token`` at app startup
         or cleared by a previous successful setup).
    """
    if settings.bootstrap_token:
        return settings.bootstrap_token
    return _token_slot.get()


def ensure_startup_token(*, operators_empty: bool) -> None:
    """Called from the app startup hook.

    If the server is bound to a non-loopback address AND no env-token
    is configured AND the operators table is empty, generate a fresh
    token and store it in the in-memory slot. Print the token to the
    application log exactly once with a clear banner so the user can
    copy it from the terminal that started the app.

    The token is NOT written to disk. If the user restarts the backend
    before completing setup, they get a new token and a new banner --
    that is the documented behaviour (and the subject of follow-up
    issue #108).
    """
    if not operators_empty:
        return
    if settings.bootstrap_token:
        # Env-configured token wins; don't generate or print anything.
        return
    if _is_loopback(settings.host):
        # Mode A applies (or will apply on a loopback request); no
        # token is required, so don't generate one. The user can still
        # set EBULL_SETUP_TOKEN explicitly to force Mode B.
        return

    token = secrets.token_urlsafe(32)
    _token_slot.set(token)
    banner = (
        "\n"
        "============================================================\n"
        "EBULL BOOTSTRAP TOKEN (use once during /setup):\n"
        f"  {token}\n"
        "============================================================\n"
    )
    # Use logger.warning so it survives default INFO suppression in
    # production logging configs without being a real warning.
    logger.warning(banner)


def is_setup_authorised(
    *,
    request_client_ip: str | None,
    submitted_token: str | None,
) -> bool:
    """Return True iff the request is allowed to proceed past auth.

    Mode A (loopback zero-config): accept with no token iff
      * the request originates from loopback (``request_client_ip`` is
        the source IP as reported by Starlette ``request.client.host``;
        this is the **TCP peer**, not the HTTP ``Host`` header).
      * settings.host is loopback (the server is not LAN-bound)
      * no token is currently configured (env or in-memory slot)

    Mode B (token required): otherwise the request must present a token
    that matches the active token in constant time.

    Reverse-proxy caveat: behind nginx/traefik/etc, ``request.client.host``
    is the proxy's address (typically ``127.0.0.1``), not the real
    client. eBull is documented as a local-only deployment so a reverse
    proxy is out of scope; if you nonetheless put one in front of the
    server, set ``EBULL_BOOTSTRAP_TOKEN`` to force Mode B and ignore the
    loopback path entirely.
    """
    active = resolve_bootstrap_token()
    if active is None and _is_loopback(settings.host) and _is_loopback(request_client_ip):
        return True
    if active is None:
        # Mode B applies but no token is configured -- nothing the
        # caller can present will be accepted. Fail closed.
        return False
    if submitted_token is None:
        return False
    return secrets.compare_digest(active, submitted_token)


def operators_empty(conn: psycopg.Connection[object]) -> bool:
    """True iff the operators table currently has zero rows."""
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM operators LIMIT 1")
        return cur.fetchone() is None


def _normalise_username(raw: str) -> str:
    return raw.strip().lower()


def perform_setup(
    conn: psycopg.Connection[object],
    *,
    username: str,
    password: str,
    submitted_token: str | None,
    request_client_ip: str | None,
    user_agent: str | None,
    request_ip: str | None,
) -> tuple[SetupOutcome, SetupSuccess | None]:
    """Run the locked first-run setup transaction.

    Returns ``(SetupOutcome.OK, SetupSuccess(...))`` on success and
    ``(<failure>, None)`` on every failure path. The HTTP layer maps
    every non-OK outcome to the same 404 body so callers cannot
    distinguish them.
    """
    normalised = _normalise_username(username)
    if not normalised:
        return SetupOutcome.BAD_USERNAME, None
    if len(password) < MIN_PASSWORD_LEN:
        return SetupOutcome.BAD_PASSWORD, None
    if not is_setup_authorised(
        request_client_ip=request_client_ip,
        submitted_token=submitted_token,
    ):
        return SetupOutcome.BAD_TOKEN, None

    password_hash = hash_password(password)

    # Single locked transaction wraps:
    #   * advisory lock
    #   * empty check
    #   * operator INSERT
    #   * session INSERT (via create_session)
    #   * operator_audit INSERT
    # A concurrent setup blocks on pg_advisory_xact_lock, then sees a
    # non-empty operators table and returns ALREADY_SETUP.
    #
    # We capture the result in locals and return AFTER exiting the
    # transaction context manager rather than via early return inside
    # it. ``return`` from inside ``with conn.transaction()`` triggers
    # an implicit commit on the (possibly empty) tx, which is harmless
    # but obscures intent and was flagged in PR review.
    already_setup = False
    operator_id: UUID | None = None
    session_id: str | None = None
    expires_at: datetime | None = None

    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (_BOOTSTRAP_LOCK_KEY,))
            cur.execute("SELECT 1 FROM operators LIMIT 1")
            if cur.fetchone() is not None:
                already_setup = True
            else:
                cur.execute(
                    """
                    INSERT INTO operators (username, password_hash)
                    VALUES (%s, %s)
                    RETURNING operator_id
                    """,
                    (normalised, password_hash),
                )
                row = cur.fetchone()
                if row is None:
                    raise RuntimeError("operators INSERT did not RETURNING a row")
                operator_id = row[0]  # type: ignore[index,assignment]

        if not already_setup:
            if operator_id is None:
                raise RuntimeError("operator_id unset on success path")
            session_id, expires_at = create_session(
                conn,
                operator_id=operator_id,
                user_agent=user_agent,
                ip=request_ip,
                absolute_timeout=timedelta(hours=settings.session_absolute_timeout_hours),
            )

            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO operator_audit (
                        event_type, actor_operator_id, actor_username,
                        target_operator_id, target_username,
                        request_ip, user_agent
                    )
                    VALUES ('setup', NULL, NULL, %s, %s, %s, %s)
                    """,
                    (operator_id, normalised, request_ip, user_agent),
                )

    if already_setup:
        # The race-loser path. The table is now permanently populated,
        # so the bootstrap token has no further legitimate use --
        # consume the slot so it cannot be reused by a third request.
        _token_slot.consume()
        return SetupOutcome.ALREADY_SETUP, None

    # Consume the token slot AFTER successful commit. Doing it inside
    # the tx would mean a tx rollback (e.g. session_create raising)
    # leaves the slot consumed and the user permanently locked out
    # until process restart -- worse than the alternative.
    _token_slot.consume()

    # Defensive: the OK path always populates these. Use explicit
    # raises rather than ``assert`` so the guards survive ``python -O``.
    if operator_id is None or session_id is None or expires_at is None:
        raise RuntimeError("perform_setup OK path missing operator/session state")
    logger.info("first-run setup complete: operator=%s", normalised)

    return SetupOutcome.OK, SetupSuccess(
        operator_id=operator_id,
        username=normalised,
        session_id=session_id,
        expires_at=expires_at,
    )
