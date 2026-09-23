"""Seeders for the #2942 recommendation window-B DB tests.

Rows are written directly in the shapes ``execute_order`` leaves (W: the marker
committed, sender died; U: the provider call raised and the park committed), so
each test states exactly which recorded field it is about.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import psycopg
from psycopg.types.json import Jsonb

INSTRUMENT_ID = 992_942
OPERATOR_ID = UUID("2942a0b0-0000-4000-8000-000000002942")
CREATED_AT = datetime(2026, 9, 23, 9, 0, tzinfo=UTC)
ENTERED_AT = CREATED_AT + timedelta(seconds=2)
PARKED_AT = CREATED_AT + timedelta(seconds=5)
EXIT_LOT_ID = 3_308_442_058
EXIT_UNITS = Decimal("10.50000000")


@dataclass(frozen=True)
class Credentials:
    api: UUID
    user: UUID


def seed_world(conn: psycopg.Connection[Any], *, operator_id: UUID = OPERATOR_ID) -> Credentials:
    """Instrument, operator and one live demo credential pair. Undecryptable on
    purpose: nothing in these tests decrypts (cf. ``tests/fixtures/core_restart``)."""
    conn.execute(
        "INSERT INTO instruments (instrument_id,symbol,company_name,is_tradable) "
        "VALUES (%s,'RWB.2942','Recommendation Window B',TRUE) ON CONFLICT DO NOTHING",
        (INSTRUMENT_ID,),
    )
    conn.execute(
        "INSERT INTO operators (operator_id, username, password_hash) VALUES (%s, %s, 'x') "
        "ON CONFLICT (operator_id) DO NOTHING",
        (operator_id, f"rwb-{operator_id}"),
    )
    ids = []
    for label in ("api_key", "user_key"):
        credential_id = uuid4()
        conn.execute(
            """
            INSERT INTO broker_credentials (id, operator_id, provider, label, environment, ciphertext, last_four)
            VALUES (%s, %s, 'etoro', %s, 'demo', '\\x00'::bytea, '0000')
            """,
            (credential_id, operator_id, label),
        )
        ids.append(credential_id)
    conn.commit()
    return Credentials(api=ids[0], user=ids[1])


def seed_recommendation(conn: psycopg.Connection[Any], *, action: str = "BUY", status: str = "approved") -> int:
    row = conn.execute(
        "INSERT INTO trade_recommendations (instrument_id, action, rationale, status) "
        "VALUES (%s,%s,'window b test',%s) RETURNING recommendation_id",
        (INSTRUMENT_ID, action, status),
    ).fetchone()
    assert row is not None
    conn.commit()
    return int(row[0])


def seed_decision(conn: psycopg.Connection[Any]) -> int:
    row = conn.execute(
        "INSERT INTO decision_audit (decision_time, instrument_id, stage, pass_fail, explanation) "
        "VALUES (%s,%s,'execution_guard','PASS','window b test') RETURNING decision_id",
        (CREATED_AT, INSTRUMENT_ID),
    ).fetchone()
    assert row is not None
    conn.commit()
    return int(row[0])


def seed_stranded(
    conn: psycopg.Connection[Any],
    *,
    state: str,
    action: str = "BUY",
    credentials: Credentials | None,
    broker_environment: str | None = "demo",
    sender: tuple[int, str] | None = (424242, "sender-host"),
    parked: bool = True,
    park_message: str | None = "HTTP 504 from /orders",
    raw_payload: Any = None,
    recommendation_status: str | None = None,
) -> tuple[int, int]:
    """A W (``state='W'``) or U (``state='U'``) row plus its recommendation.

    Returns ``(order_id, recommendation_id)``.
    """
    rec = seed_recommendation(
        conn,
        action=action,
        status=recommendation_status or ("approved" if state == "W" else "execution_pending"),
    )
    is_u = state == "U"
    payload = raw_payload if raw_payload is not None else ({"status": 504} if is_u else {"intent": "submitted"})
    row = conn.execute(
        """
        INSERT INTO orders
            (instrument_id, recommendation_id, action, order_type, status, raw_payload_json, created_at,
             recommendation_request_id, recommendation_submission_phase, broker_environment,
             recommendation_exit_position_id, recommendation_exit_units, recommendation_submission_context,
             recommendation_api_key_credential_id, recommendation_user_key_credential_id,
             recommendation_submission_entered_at, recommendation_submission_entered_pid,
             recommendation_submission_entered_host, recommendation_parked_at, recommendation_park_message)
        VALUES
            (%(iid)s, %(rid)s, %(action)s, 'market', %(status)s, %(payload)s, %(created)s,
             %(req)s, 'broker_verb_entered', %(env)s,
             %(lot)s, %(units)s, %(context)s,
             %(api)s, %(user)s,
             %(entered_at)s, %(pid)s, %(host)s, %(parked_at)s, %(message)s)
        RETURNING order_id
        """,
        {
            "iid": INSTRUMENT_ID,
            "rid": rec,
            "action": action,
            "status": "uncertain" if is_u else "submitted",
            "payload": Jsonb(payload),
            "created": CREATED_AT,
            "req": uuid4(),
            "env": broker_environment,
            "lot": EXIT_LOT_ID if action == "EXIT" else None,
            "units": EXIT_UNITS if action == "EXIT" else None,
            "context": Jsonb({"order_params": None}) if action == "EXIT" else None,
            "api": None if credentials is None else credentials.api,
            "user": None if credentials is None else credentials.user,
            "entered_at": None if sender is None else ENTERED_AT,
            "pid": None if sender is None else sender[0],
            "host": None if sender is None else sender[1],
            "parked_at": PARKED_AT if is_u and parked else None,
            "message": park_message if is_u else None,
        },
    ).fetchone()
    assert row is not None
    conn.commit()
    return int(row[0]), rec


def order_status(conn: psycopg.Connection[Any], order_id: int) -> str:
    row = conn.execute("SELECT status FROM orders WHERE order_id=%s", (order_id,)).fetchone()
    conn.commit()
    assert row is not None
    return str(row[0])


def recommendation_status(conn: psycopg.Connection[Any], recommendation_id: int) -> str:
    row = conn.execute(
        "SELECT status FROM trade_recommendations WHERE recommendation_id=%s", (recommendation_id,)
    ).fetchone()
    conn.commit()
    assert row is not None
    return str(row[0])


def key_held_elsewhere(url: str, recommendation_id: int, ns: int = 2942) -> bool:
    """Whether ANY backend holds the per-recommendation key, asked from a fresh session."""
    with psycopg.connect(url) as other:
        row = other.execute(
            """
            SELECT count(*) FROM pg_locks
            WHERE locktype = 'advisory' AND granted
              -- pg_locks spans the whole cluster; xdist workers share it.
              AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
              AND classid::bigint = %s AND objid::bigint = %s AND objsubid = 2
            """,
            (ns, recommendation_id),
        ).fetchone()
    return row is not None and int(row[0]) > 0
