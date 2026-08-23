"""#2602 item 3 — the SQL half, against a real database.

The vocabulary and the parser are pure and live in
``tests/test_2602_position_investment_type.py``. What needs a real Postgres is
the ON CONFLICT branch: ``settlement_type_id`` is the one field on the
``broker_positions`` upsert that does NOT take ``EXCLUDED`` unconditionally, and
whether a later payload can erase a known product identity is not answerable
from a mocked cursor.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import psycopg

from app.providers.broker import BrokerPosition
from app.services.portfolio_sync import _upsert_broker_positions

INSTRUMENT_ID = 990_602
POSITION_ID = 990_602_001
_NOW = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)


def _seed_instrument(conn: psycopg.Connection[Any]) -> int:
    """``is_tradable`` listed explicitly per #1233 §6.2 (chokepoint lint)."""
    conn.execute(
        "INSERT INTO instruments (instrument_id,symbol,company_name,is_tradable) "
        "VALUES (%s,'POS.IDENT','Position Identity Test',TRUE) ON CONFLICT DO NOTHING",
        (INSTRUMENT_ID,),
    )
    return INSTRUMENT_ID


def _position(settlement_type_id: int | None, raw_payload: dict[str, Any] | None = None) -> BrokerPosition:
    return BrokerPosition(
        instrument_id=INSTRUMENT_ID,
        units=Decimal("10"),
        open_price=Decimal("100"),
        current_price=Decimal("100"),
        raw_payload=raw_payload if raw_payload is not None else {"positionID": POSITION_ID},
        position_id=POSITION_ID,
        amount=Decimal("1000"),
        initial_amount_in_dollars=Decimal("1000"),
        open_date_time=_NOW,
        settlement_type_id=settlement_type_id,
    )


def _stored(conn: psycopg.Connection[Any]) -> int | None:
    row = conn.execute(
        "SELECT settlement_type_id FROM broker_positions WHERE position_id = %s", (POSITION_ID,)
    ).fetchone()
    assert row is not None
    return row[0]


def test_first_sync_stores_the_investment_type(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    _seed_instrument(ebull_test_conn)
    _upsert_broker_positions(ebull_test_conn, [_position(0)], _NOW)
    assert _stored(ebull_test_conn) == 0


def test_a_later_sync_restates_a_changed_investment_type(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The broker sends the field on every payload, so it refreshes like any
    other observed value. A stale label would be its own defect."""
    _seed_instrument(ebull_test_conn)
    _upsert_broker_positions(ebull_test_conn, [_position(0)], _NOW)
    _upsert_broker_positions(ebull_test_conn, [_position(1)], _NOW)
    assert _stored(ebull_test_conn) == 1


def test_a_payload_that_omits_the_field_does_not_erase_what_we_knew(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The COALESCE branch — the reason this test needs a real database.

    Taking ``EXCLUDED`` unconditionally would blank a position's product
    identity the first time eToro omitted the key, and the provider documents no
    "type cleared" state, so NULL means "not reported", never "no longer a CFD".
    A blanked row reads as Unknown on the panel and silently drops evidence we
    already had.
    """
    _seed_instrument(ebull_test_conn)
    _upsert_broker_positions(ebull_test_conn, [_position(1)], _NOW)
    _upsert_broker_positions(ebull_test_conn, [_position(None)], _NOW)
    assert _stored(ebull_test_conn) == 1


def test_migration_backfill_reads_the_retained_payload(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """sql/367's backfill is exact — the field was retained all along.

    Pinned as a query rather than trusted from the one-off run: ``raw_payload``
    is the evidence the backfill claimed, so the claim is that the column agrees
    with it for every row that carries the key.
    """
    _seed_instrument(ebull_test_conn)
    _upsert_broker_positions(
        ebull_test_conn,
        [_position(2, raw_payload={"positionID": POSITION_ID, "settlementTypeID": 2})],
        _NOW,
    )
    row = ebull_test_conn.execute(
        """
        SELECT count(*) FILTER (
            WHERE settlement_type_id IS DISTINCT FROM (raw_payload->>'settlementTypeID')::smallint
        )
          FROM broker_positions
         WHERE raw_payload ? 'settlementTypeID'
           AND raw_payload->>'settlementTypeID' ~ '^-?[0-9]+$'
        """
    ).fetchone()
    assert row is not None
    assert row[0] == 0
