"""#3471 slice 2c-iii-c: the WS ``private`` recorder's append-only write path against a real database."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any

import psycopg
import pytest

from app.services.etoro_websocket import private_messages, record_private_events

T0 = datetime(2026, 9, 28, 21, 0, tzinfo=UTC)

_ORDER_CONTENT = '{"OrderID": 384471008, "StatusID": 3, "ErrorCode": 0}'


def _frame() -> str:
    return json.dumps(
        {
            "messages": [
                {"topic": "instrument:1001", "type": "Trading.Instrument.Rate", "content": '{"Bid": 1}'},
                {"topic": "private", "type": "Trading.OrderForOpen.Update", "content": _ORDER_CONTENT, "id": "m-1"},
                {"topic": "private", "type": "Trading.Something.New", "content": "not json"},
                {"topic": "private", "type": "Trading.Something.New", "content": "null"},
            ]
        }
    )


def test_frame_rows_are_recorded_as_served(ebull_test_conn: psycopg.Connection[Any]) -> None:
    conn = ebull_test_conn
    frame_id = uuid.uuid4()
    record_private_events(conn, private_messages(_frame()), received_at=T0, environment="demo", frame_id=frame_id)

    rows = conn.execute(
        """
        SELECT message_index, topic, message_type, message->>'content', content, received_at, environment, frame_id
          FROM broker_private_events ORDER BY message_index
        """
    ).fetchall()
    assert [(r[0], r[1], r[2]) for r in rows] == [
        (1, "private", "Trading.OrderForOpen.Update"),
        (2, "private", "Trading.Something.New"),
        (3, "private", "Trading.Something.New"),
    ]
    # The pushed content string survives byte-for-byte; the parsed copy is NULL when it is not JSON.
    assert rows[0][3] == _ORDER_CONTENT
    assert rows[0][4] == {"OrderID": 384471008, "StatusID": 3, "ErrorCode": 0}
    assert rows[1][3] == "not json"
    assert rows[1][4] is None
    # A valid JSON null is JSONB null, not the SQL NULL of "nothing parseable".
    null_row = conn.execute(
        "SELECT content IS NOT NULL, jsonb_typeof(content) FROM broker_private_events WHERE message_index = 3"
    ).fetchone()
    assert null_row == (True, "null")
    assert {(r[5], r[6], r[7]) for r in rows} == {(T0, "demo", frame_id)}


def test_update_is_refused(ebull_test_conn: psycopg.Connection[Any]) -> None:
    conn = ebull_test_conn
    record_private_events(conn, private_messages(_frame()), received_at=T0, environment="demo", frame_id=uuid.uuid4())
    with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
        conn.execute("UPDATE broker_private_events SET content = NULL")
