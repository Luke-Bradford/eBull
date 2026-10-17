from datetime import timedelta
from decimal import Decimal
from typing import Any

import psycopg
import pytest
from psycopg.types.json import Jsonb

from app.providers.market_data import Quote
from app.services import momentum_book_executor as mx
from app.services.execution_tca import load_entry_tca
from app.services.momentum_book_job import form_book
from tests.test_momentum_book_executor_db import NOW, _market, _stub_broker
from tests.test_momentum_book_job_db import SESSION, _seed

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("marked_daily_loss_neutral")]

Conn = psycopg.Connection[Any]


def _entry(conn: Conn) -> tuple[int, int]:
    _seed(conn, held=False)
    formed = form_book(conn, session=SESSION)
    assert formed is not None
    _market(conn, formed.book)
    return formed.entry_signal_ids[0], formed.book[0]


def _arrival_row(conn: Conn, order_id: int) -> tuple[Any, ...] | None:
    return conn.execute(
        "SELECT outcome, quoted_at, bid, ask, requested_at, received_at FROM strategy_order_arrival_quotes "
        "WHERE order_id = %s",
        (order_id,),
    ).fetchone()


def test_the_arrival_quote_is_recorded_before_the_order_is_sent_and_measures_the_fill(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    signal_id, instrument_id = _entry(conn)
    broker = _stub_broker(instrument_id)

    def arrival(iid: int) -> Quote:
        assert iid == instrument_id
        broker.place_demo_strategy_order.assert_not_called()
        return Quote(iid, NOW - timedelta(seconds=2), Decimal("100.1"), Decimal("100.3"), None)

    result = mx.execute_momentum_signal(
        conn, broker=broker, signal_id=signal_id, clock=lambda: NOW, arrival_quote=arrival
    )
    assert result.verdict == "submitted", result
    assert result.order_id is not None
    broker.place_demo_strategy_order.assert_called_once()
    assert _arrival_row(conn, result.order_id) == (
        "quoted",
        NOW - timedelta(seconds=2),
        Decimal("100.1"),
        Decimal("100.3"),
        NOW,
        NOW,
    )

    # The broker's open event for the position the entry owns is the fill.
    trade_id = conn.execute(
        "SELECT strategy_trade_id FROM strategy_trade_orders WHERE order_id = %s AND purpose = 'entry'",
        (result.order_id,),
    ).fetchone()
    assert trade_id is not None
    position_id = 8_800_000 + int(trade_id[0])
    conn.execute(
        "INSERT INTO strategy_position_ownership (strategy_trade_id, broker_position_id, status, claimed_at) "
        "VALUES (%s, %s, 'active', now())",
        (trade_id[0], position_id),
    )
    conn.execute(
        """
        INSERT INTO trade_events (position_id, etoro_instrument_id, instrument_id, event_kind, side, units, price,
                                  executed_at, source, raw_payload, environment)
        VALUES (%s, %s, %s, 'open', 'buy', 0.4, 100.25, %s, 'etoro_sync', %s, 'demo')
        """,
        (position_id, instrument_id, instrument_id, NOW + timedelta(seconds=4), Jsonb({"positionId": position_id})),
    )
    (entry,) = load_entry_tca(conn, since=NOW.date())
    assert entry.order_id == result.order_id
    assert entry.decision == Decimal("100")  # the preflight's quote_ask, the fixture quote's ask
    assert entry.fill == Decimal("100.25")
    assert entry.what_if_cost is not None  # the multiplier comes from the evaluated revision's policy event
    assert entry.unmeasured is None and entry.costs is not None
    assert entry.costs.arrival_unmeasured is None
    assert entry.costs.shortfall == Decimal("0.0025")
    assert entry.costs.delay == Decimal("0.002")
    assert entry.costs.effective_spread == Decimal("0.1") / Decimal("100.2")  # 2 * |100.25 - 100.2| / 100.2


def test_a_failed_fetch_is_recorded_and_the_order_still_submits(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    signal_id, instrument_id = _entry(conn)
    broker = _stub_broker(instrument_id)

    def arrival(_iid: int) -> Quote:
        raise RuntimeError("rates endpoint down")

    result = mx.execute_momentum_signal(
        conn, broker=broker, signal_id=signal_id, clock=lambda: NOW, arrival_quote=arrival
    )
    assert result.verdict == "submitted", result
    assert result.order_id is not None
    row = _arrival_row(conn, result.order_id)
    assert row is not None and row[:4] == ("failed", None, None, None)


def test_a_rejected_arrival_write_rolls_back_alone_and_the_order_still_submits(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    signal_id, instrument_id = _entry(conn)
    broker = _stub_broker(instrument_id)

    # bid = 0 violates the table's CHECK: the insert fails inside its own transaction.
    def arrival(iid: int) -> Quote:
        return Quote(iid, NOW, Decimal("0"), Decimal("100"), None)

    result = mx.execute_momentum_signal(
        conn, broker=broker, signal_id=signal_id, clock=lambda: NOW, arrival_quote=arrival
    )
    assert result.verdict == "submitted", result
    assert result.order_id is not None
    broker.place_demo_strategy_order.assert_called_once()
    assert _arrival_row(conn, result.order_id) is None
    # The connection is usable after the contained failure, and the authority it wrote before is intact.
    status = conn.execute("SELECT status FROM orders WHERE order_id = %s", (result.order_id,)).fetchone()
    assert status is not None
