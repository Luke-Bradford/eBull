"""The slice 3c lookup capture against the test database (#3616; spec Design 3, Design 6 steps 1-3).

Auto-marked ``db`` (pulls ``ebull_test_conn``). The run commits each attempt on its own autocommit connection, as
the job does; the fixture connection seeds the ledger and wipes between tests. No test reaches the broker.
"""

from __future__ import annotations

from typing import Any

import psycopg
import pytest
from psycopg.types.json import Jsonb

from app.providers.broker import RawOrderLookup
from app.services.tax_broker_evidence import TAX_EVIDENCE_LOCK_KEY, run_order_lookup_capture
from tests.fixtures.ebull_test_db import test_database_url


class _FakeBroker:
    def __init__(self, responses: dict[int, RawOrderLookup]) -> None:
        self.responses = responses
        self.asked: list[int] = []

    def lookup_order_raw(self, order_id: int) -> RawOrderLookup:
        self.asked.append(order_id)
        return self.responses.get(order_id, RawOrderLookup(404, '{"errorMessage": "not found"}', None))


def _connect() -> psycopg.Connection[tuple]:
    return psycopg.connect(test_database_url(), autocommit=True)


def _rows(sql: str) -> list[tuple]:
    with _connect() as conn:
        return conn.execute(sql).fetchall()  # type: ignore[arg-type]


def _seed(conn: psycopg.Connection[Any], *, scope: str | None = "demo") -> None:
    if scope is not None:
        conn.execute(
            "INSERT INTO trade_ledger_scope (singleton, environment, account_id, established_at) "
            "VALUES (TRUE, %s, 4242, now())",
            (scope,),
        )
    # Two orders: 7 named by the column on a history close, 8 only by a sync payload's orderID.
    for position_id, kind, source, order_col, payload in (
        (501, "open", "etoro_history", 7, {"orderId": 7}),
        (501, "close", "etoro_history", 7, {"orderId": 7}),
        (502, "open", "etoro_sync", None, {"orderID": 8, "CID": 4242}),
    ):
        conn.execute(
            """
            INSERT INTO trade_events (position_id, etoro_instrument_id, event_kind, side, units, executed_at,
                                      order_id, source, raw_payload, environment)
            VALUES (%s, 4077, %s, 'buy', 1, now() - interval '1 day', %s, %s, %s, 'demo')
            """,
            (position_id, kind, order_col, source, Jsonb(payload)),
        )
    conn.commit()


def test_capture_stores_each_attempt_as_received(ebull_test_conn: psycopg.Connection[Any]) -> None:
    _seed(ebull_test_conn)
    body = '{"orderId": 7, "accountId": 4242, "totalCosts": 0.10}'
    broker = _FakeBroker({7: RawOrderLookup(200, body, None)})
    with _connect() as conn:
        report = run_order_lookup_capture(conn, broker, "demo")
    assert report.state == "captured"
    assert sorted(broker.asked) == [7, 8]
    assert report.outcomes == {"found": 1, "not_found": 1}
    assert _rows(
        "SELECT order_id, outcome, http_status, body_text, account_id, body_sha256 IS NOT NULL "
        "FROM tax_order_lookups ORDER BY order_id"
    ) == [(7, "found", 200, body, 4242, True), (8, "not_found", 404, '{"errorMessage": "not found"}', None, True)]
    # The decimal text is kept as received: 0.10, not 0.1.
    assert "0.10" in _rows("SELECT body_text FROM tax_order_lookups WHERE order_id = 7")[0][0]
    # A second run asks nothing: no new events, and nothing is due.
    with _connect() as conn:
        assert run_order_lookup_capture(conn, broker, "demo").attempted == 0
    with _connect() as conn, pytest.raises(psycopg.errors.RaiseException, match="append-only"):
        conn.execute("UPDATE tax_order_lookups SET error_detail = 'x'")


def test_rate_limited_stops_the_run(ebull_test_conn: psycopg.Connection[Any]) -> None:
    _seed(ebull_test_conn)
    broker = _FakeBroker({7: RawOrderLookup(429, "", None), 8: RawOrderLookup(429, "", None)})
    with _connect() as conn:
        report = run_order_lookup_capture(conn, broker, "demo")
    assert (report.attempted, report.stopped_by, report.deferred) == (1, "rate_limited", 1)
    assert len(broker.asked) == 1


@pytest.mark.parametrize(("scope", "reason"), [(None, "scope_unestablished"), ("real", "scope_mismatch")])
def test_no_lookup_before_the_scope_check_passes(
    ebull_test_conn: psycopg.Connection[Any], scope: str | None, reason: str
) -> None:
    _seed(ebull_test_conn, scope=scope)
    broker = _FakeBroker({})
    with _connect() as conn:
        report = run_order_lookup_capture(conn, broker, "demo")
    assert (report.state, report.reason, broker.asked) == ("refused", reason, [])
    assert _rows("SELECT count(*) FROM tax_order_lookups") == [(0,)]


def test_a_second_run_skips_while_the_lock_is_held(ebull_test_conn: psycopg.Connection[Any]) -> None:
    _seed(ebull_test_conn)
    broker = _FakeBroker({})
    with _connect() as holder:
        holder.execute("SELECT pg_advisory_lock(%s)", [TAX_EVIDENCE_LOCK_KEY])
        with _connect() as conn:
            assert run_order_lookup_capture(conn, broker, "demo").state == "skipped"
        holder.execute("SELECT pg_advisory_unlock(%s)", [TAX_EVIDENCE_LOCK_KEY])
    assert broker.asked == []
    # The lock is released after a run: the connection can take it again.
    with _connect() as conn:
        assert run_order_lookup_capture(conn, broker, "demo").state == "captured"
        row = conn.execute("SELECT pg_try_advisory_lock(%s)", [TAX_EVIDENCE_LOCK_KEY]).fetchone()
        assert row is not None and row[0] is True
