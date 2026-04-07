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
                cur.execute(
                    """
                    INSERT INTO operators (username, password_hash)
                    VALUES (%s, %s)
                    RETURNING operator_id, username, created_at, last_login_at
                    """,
                    (normalised, password_hash),
                )
                row = cur.fetchone()
                assert row is not None
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
    actor_session_id: str,
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

    try:
        with conn.transaction():
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                # Look up target by id so we can capture the username
                # for the audit row before the operator row vanishes.
                cur.execute(
                    "SELECT username FROM operators WHERE operator_id = %s",
                    (target_operator_id,),
                )
                target_row = cur.fetchone()
                if target_row is None:
                    return DeleteOutcome.NOT_FOUND
                target_username: str = target_row["username"]

                if is_self:
                    cur.execute("SELECT COUNT(*) AS n FROM operators")
                    count_row = cur.fetchone()
                    assert count_row is not None
                    if int(count_row["n"]) <= 1:
                        return DeleteOutcome.LAST_OPERATOR

                # Delete the operator row. The FK cascade on
                # sessions.operator_id removes any session rows
                # belonging to the deleted operator -- including the
                # caller's current session if this is a self-delete.
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

                # Belt-and-braces: explicitly delete the caller's
                # session row inside the same transaction even though
                # the FK cascade would have caught it. This makes the
                # invariant ("after self-delete commit, this session
                # id is gone") explicit at the call site instead of
                # implicit in schema config -- and protects against
                # someone changing the FK to RESTRICT in a future
                # migration.
                if is_self:
                    cur.execute(
                        "DELETE FROM sessions WHERE session_id = %s",
                        (actor_session_id,),
                    )
    except Exception:
        raise

    if is_self:
        logger.info("operator self-delete: %s", actor_username)
        return DeleteOutcome.OK_SELF

    # Returning OK_OTHER tells the caller the action succeeded and the
    # caller's session is unaffected.
    logger.info("operator deleted: %s by %s", target_username, actor_username)
    return DeleteOutcome.OK_OTHER
