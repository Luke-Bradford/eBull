"""Operator management service (issue #106 / ADR 0002).

Owns the post-bootstrap operator lifecycle: list, create, delete. Every
mutation writes a row to ``operator_audit`` inside the same transaction
as the operator-row mutation, so a failed mutation never leaves a
dangling audit row and a successful mutation always has one.

The HTTP layer in ``app.api.operators`` is a thin wrapper that converts
request fields into kwargs and turns the (intentionally narrow) outcome
enums into HTTP responses.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from uuid import UUID

import psycopg
import psycopg.rows

from app.security.passwords import hash_password
from app.services.operator_setup import MIN_PASSWORD_LEN

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exceptions for single-operator resolution
# ---------------------------------------------------------------------------


class OperatorLookupError(Exception):
    """Base for operator-resolution failures."""


class NoOperatorError(OperatorLookupError):
    """Raised when zero operator rows exist."""


class AmbiguousOperatorError(OperatorLookupError):
    """Raised when more than one operator row exists."""


# ---------------------------------------------------------------------------
# Single-operator resolver (issue #100)
# ---------------------------------------------------------------------------


def sole_operator_id(conn: psycopg.Connection[object]) -> UUID:
    """Return the single operator's id.

    The eBull v1 model assumes exactly one operator.  This helper
    enforces that assumption loudly: zero rows is a configuration error,
    multiple rows means the multi-operator path is live but not yet
    wired into the calling code.

    Raises:
        NoOperatorError      -- zero rows in ``operators``.
        AmbiguousOperatorError -- more than one row.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT operator_id FROM operators")
        rows = cur.fetchall()

    if len(rows) == 0:
        raise NoOperatorError("no operator exists — run /auth/setup first")
    if len(rows) > 1:
        raise AmbiguousOperatorError(f"expected exactly one operator, found {len(rows)}")
    return rows[0][0]  # type: ignore[no-any-return]


# Pinned advisory-lock key for the self-delete invariant. Distinct from
# the setup key in operator_setup.py so the two paths never block each
# other. Documented in docs/tickets/ticket-G-first-run-setup-and-operator-management.md.
_SELF_DELETE_LOCK_KEY = 7263012


@dataclass(frozen=True)
class OperatorRow:
    operator_id: UUID
    username: str
    created_at: datetime
    last_login_at: datetime | None


class CreateOutcome(Enum):
    OK = "ok"
    BAD_USERNAME = "bad_username"
    BAD_PASSWORD = "bad_password"
    DUPLICATE = "duplicate"


class DeleteOutcome(Enum):
    OK_OTHER = "ok_other"  # deleted a different operator
    OK_SELF = "ok_self"  # self-delete (caller is now logged out)
    NOT_FOUND = "not_found"
    LAST_OPERATOR = "last_operator"  # 409 -- self-delete blocked


def _normalise_username(raw: str) -> str:
    return raw.strip().lower()


def list_operators(conn: psycopg.Connection[object]) -> list[OperatorRow]:
    """Return every operator row, ordered by created_at ascending.

    Read-only -- no transaction, no audit row.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT operator_id, username, created_at, last_login_at
            FROM operators
            ORDER BY created_at ASC
            """
        )
        rows = cur.fetchall()
    return [
        OperatorRow(
            operator_id=row["operator_id"],
            username=row["username"],
            created_at=row["created_at"],
            last_login_at=row["last_login_at"],
        )
        for row in rows
    ]


def create_operator(
    conn: psycopg.Connection[object],
    *,
    actor_operator_id: UUID,
    actor_username: str,
    new_username: str,
    new_password: str,
    request_ip: str | None,
    user_agent: str | None,
) -> tuple[CreateOutcome, OperatorRow | None]:
    """Create a new operator and write the matching audit row.

    Both writes happen in a single transaction so a failed insert
    never leaves a dangling audit row. Username uniqueness is enforced
    at the DB level (UNIQUE constraint on operators.username) -- we
    catch ``psycopg.errors.UniqueViolation`` and return DUPLICATE.
    """
    normalised = _normalise_username(new_username)
    if not normalised:
        return CreateOutcome.BAD_USERNAME, None
    if len(new_password) < MIN_PASSWORD_LEN:
        return CreateOutcome.BAD_PASSWORD, None

    password_hash = hash_password(new_password)

    try:
        with conn.transaction():
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                # Take the same advisory lock that delete_operator uses
                # for self-delete. This serialises create-vs-self-delete:
                # without it, a self-delete that observes count=2 could
                # commit at the same time a create commits, leaving the
                # database with a single operator that the self-delete
                # never knew about. Both branches now block on the same
                # key, so the count check inside delete is consistent
                # with the create that happens around it.
                cur.execute(
                    "SELECT pg_advisory_xact_lock(%s)",
                    (_SELF_DELETE_LOCK_KEY,),
                )
                cur.execute(
                    """
                    INSERT INTO operators (username, password_hash)
                    VALUES (%s, %s)
                    RETURNING operator_id, username, created_at, last_login_at
                    """,
                    (normalised, password_hash),
                )
                row = cur.fetchone()
                if row is None:
                    raise RuntimeError("operators INSERT did not RETURNING a row")
                cur.execute(
                    """
                    INSERT INTO operator_audit (
                        event_type, actor_operator_id, actor_username,
                        target_operator_id, target_username,
                        request_ip, user_agent
                    )
                    VALUES ('create', %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        actor_operator_id,
                        actor_username,
                        row["operator_id"],
                        row["username"],
                        request_ip,
                        user_agent,
                    ),
                )
    except psycopg.errors.UniqueViolation:
        return CreateOutcome.DUPLICATE, None

    logger.info("operator created: %s by %s", normalised, actor_username)
    return CreateOutcome.OK, OperatorRow(
        operator_id=row["operator_id"],
        username=row["username"],
        created_at=row["created_at"],
        last_login_at=row["last_login_at"],
    )


def delete_operator(
    conn: psycopg.Connection[object],
    *,
    actor_operator_id: UUID,
    actor_username: str,
    target_operator_id: UUID,
    request_ip: str | None,
    user_agent: str | None,
) -> DeleteOutcome:
    """Delete an operator (and, if self-delete, the caller's session).

    Rules (per ADR 0002 §4 / Ticket G):
      * target row must exist; otherwise NOT_FOUND
      * deleting another operator: just deletes the operator row.
        Sessions belonging to that operator are removed automatically
        by ``sessions.operator_id ON DELETE CASCADE`` from sql/016.
      * self-delete:
          - if at least one other operator exists: delete the
            operator row AND the caller's session row in the same
            transaction; the caller is now logged out. Caller's other
            session rows (if any) are also wiped by the FK cascade.
          - if the caller is the only operator: LAST_OPERATOR (409).
            The audit row is NOT written -- nothing happened.
    """
    is_self = target_operator_id == actor_operator_id

    # Capture outcome in a local and return AFTER exiting the
    # transaction context manager. ``return`` from inside
    # ``with conn.transaction()`` triggers an implicit commit on the
    # (possibly empty) tx; not wrong, but flagged in PR review for
    # being easy to misread, so we hoist the return out.
    outcome: DeleteOutcome | None = None
    target_username: str | None = None

    with conn.transaction():
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            # Take the population-invariant advisory lock for EVERY
            # delete (self and non-self alike) so the self-delete count
            # check cannot race a concurrent non-self delete that would
            # otherwise commit between this transaction's SELECT
            # COUNT(*) and DELETE. ``create_operator`` also takes the
            # same key, so create / non-self-delete / self-delete all
            # serialise on the population invariant. set-password and
            # last_login_at do not take the lock -- they don't change
            # the row count.
            cur.execute(
                "SELECT pg_advisory_xact_lock(%s)",
                (_SELF_DELETE_LOCK_KEY,),
            )

            cur.execute(
                "SELECT username FROM operators WHERE operator_id = %s",
                (target_operator_id,),
            )
            target_row = cur.fetchone()
            if target_row is None:
                outcome = DeleteOutcome.NOT_FOUND
            else:
                target_username = target_row["username"]

                last_operator_block = False
                if is_self:
                    cur.execute("SELECT COUNT(*) AS n FROM operators")
                    count_row = cur.fetchone()
                    if count_row is None:
                        raise RuntimeError("COUNT(*) returned no row")
                    last_operator_block = int(count_row["n"]) <= 1

                if last_operator_block:
                    outcome = DeleteOutcome.LAST_OPERATOR
                else:
                    # Delete the operator row. The FK cascade on
                    # sessions.operator_id removes any session rows
                    # belonging to the deleted operator -- including
                    # the caller's current session on self-delete.
                    cur.execute(
                        "DELETE FROM operators WHERE operator_id = %s",
                        (target_operator_id,),
                    )
                    cur.execute(
                        """
                        INSERT INTO operator_audit (
                            event_type, actor_operator_id, actor_username,
                            target_operator_id, target_username,
                            request_ip, user_agent
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            "self_delete" if is_self else "delete",
                            actor_operator_id,
                            actor_username,
                            target_operator_id,
                            target_username,
                            request_ip,
                            user_agent,
                        ),
                    )
                    # No explicit session delete here. The FK cascade
                    # on ``sessions.operator_id`` (sql/016) has already
                    # removed every session row belonging to the
                    # deleted operator -- including the caller's
                    # current session on a self-delete. A second
                    # explicit DELETE would target a row that no
                    # longer exists and silently match zero rows;
                    # reviewer correctly flagged that as misleading.
                    outcome = DeleteOutcome.OK_SELF if is_self else DeleteOutcome.OK_OTHER

    if outcome is None:
        raise RuntimeError("delete_operator exited transaction with no outcome set")
    if outcome is DeleteOutcome.OK_SELF:
        logger.info("operator self-delete: %s", actor_username)
    elif outcome is DeleteOutcome.OK_OTHER:
        logger.info("operator deleted: %s by %s", target_username, actor_username)
    return outcome
