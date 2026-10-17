from collections.abc import Callable
from datetime import timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import psycopg
import pytest
from psycopg.types.json import Jsonb

from app.providers.market_data import Quote
from app.services import momentum_book_executor as mx
from app.services.execution_tca import ARRIVAL_MAX_AGE_SECONDS, load_entry_tca, record_arrival_quote
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


def _submit(conn: Conn, arrival: Callable[[int], Quote] | None) -> tuple[int, int, MagicMock]:
    signal_id, instrument_id = _entry(conn)
    broker = _stub_broker(instrument_id)
    result = mx.execute_momentum_signal(
        conn, broker=broker, signal_id=signal_id, clock=lambda: NOW, arrival_quote=arrival
    )
    assert result.verdict == "submitted", result
    assert result.order_id is not None
    broker.place_demo_strategy_order.assert_called_once()
    return result.order_id, instrument_id, broker


def _own_fill(conn: Conn, order_id: int, instrument_id: int, price: str) -> None:
    """The broker's open event for the position the entry's trade owns: the entry's fill."""
    trade_id = conn.execute(
        "SELECT strategy_trade_id FROM strategy_trade_orders WHERE order_id = %s AND purpose = 'entry'",
        (order_id,),
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
        VALUES (%s, %s, %s, 'open', 'buy', 0.4, %s, %s, 'etoro_sync', %s, 'demo')
        """,
        (position_id, instrument_id, instrument_id, Decimal(price), NOW + timedelta(seconds=4), Jsonb({})),
    )


def test_the_arrival_quote_is_recorded_before_the_order_is_sent_and_measures_the_fill(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    sent: list[bool] = []
    broker_ref: list[MagicMock] = []

    def arrival(iid: int) -> Quote:
        sent.append(bool(broker_ref))  # has _submit_recorded_order been entered yet?
        return Quote(iid, NOW - timedelta(seconds=2), Decimal("100.1"), Decimal("100.3"), None)

    original = mx._submit_recorded_order

    def submit(*args: Any, **kwargs: Any) -> Any:
        broker_ref.append(kwargs["broker"])
        return original(*args, **kwargs)

    monkeypatch.setattr(mx, "_submit_recorded_order", submit)
    order_id, instrument_id, _ = _submit(conn, arrival)
    assert sent == [False] and len(broker_ref) == 1  # fetched once, before the submission began
    assert _arrival_row(conn, order_id) == (
        "quoted",
        NOW - timedelta(seconds=2),
        Decimal("100.1"),
        Decimal("100.3"),
        NOW,
        NOW,
    )

    _own_fill(conn, order_id, instrument_id, "100.25")
    (entry,) = load_entry_tca(conn, since=NOW.date())
    assert entry.order_id == order_id
    assert entry.decision == Decimal("100")  # the preflight's quote_ask, the fixture quote's ask
    assert entry.fill == Decimal("100.25")
    assert entry.what_if_cost is not None  # the multiplier comes from the evaluated revision's policy event
    assert entry.unmeasured is None and entry.costs is not None
    assert entry.costs.arrival_unmeasured is None
    assert entry.costs.shortfall == Decimal("0.0025")
    assert entry.costs.delay == Decimal("0.002")
    assert entry.costs.effective_spread == Decimal("0.1") / Decimal("100.2")  # 2 * |100.25 - 100.2| / 100.2


def test_an_arrival_quote_past_the_age_bound_is_stored_and_read_as_stale(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    age = timedelta(seconds=ARRIVAL_MAX_AGE_SECONDS + 1)
    order_id, instrument_id, _ = _submit(
        conn, lambda iid: Quote(iid, NOW - age, Decimal("100.1"), Decimal("100.3"), None)
    )
    _own_fill(conn, order_id, instrument_id, "100.25")
    (entry,) = load_entry_tca(conn, since=NOW.date())
    assert entry.costs is not None
    assert entry.costs.arrival_unmeasured == "arrival_stale"
    assert entry.costs.arrival_age_seconds == Decimal(ARRIVAL_MAX_AGE_SECONDS + 1)
    assert entry.costs.shortfall == Decimal("0.0025")


def test_a_failed_fetch_is_recorded_and_the_order_still_submits(ebull_test_conn: Conn) -> None:
    def arrival(_iid: int) -> Quote:
        raise RuntimeError("rates endpoint down")

    order_id, _, _ = _submit(ebull_test_conn, arrival)
    row = _arrival_row(ebull_test_conn, order_id)
    assert row is not None and row[:4] == ("failed", None, None, None)


def test_a_quote_with_a_non_positive_bid_is_recorded_unusable_and_the_order_still_submits(
    ebull_test_conn: Conn,
) -> None:
    order_id, _, _ = _submit(ebull_test_conn, lambda iid: Quote(iid, NOW, Decimal("0"), Decimal("100"), None))
    row = _arrival_row(ebull_test_conn, order_id)
    assert row is not None and row[:4] == ("unusable", NOW, Decimal("0"), Decimal("100"))


def test_a_rejected_arrival_write_rolls_back_only_its_savepoint(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    order_id, instrument_id, _ = _submit(conn, None)
    assert _arrival_row(conn, order_id) is None
    ticks = iter([NOW, NOW + timedelta(seconds=1)])
    quote = Quote(instrument_id, NOW, Decimal("100.1"), Decimal("100.3"), None)
    record_arrival_quote(
        conn, order_id=order_id, instrument_id=instrument_id, arrival_quote=lambda _i: quote, clock=lambda: next(ticks)
    )
    row = _arrival_row(conn, order_id)
    assert row is not None and row[4:] == (NOW, NOW + timedelta(seconds=1))  # requested, then received

    # A second write for the same order violates the primary key. Inside a caller's open transaction, only the
    # savepoint rolls back: the caller's earlier write survives and the connection stays usable.
    with conn.transaction():
        conn.execute("UPDATE orders SET status = 'tca-probe' WHERE order_id = %s", (order_id,))
        record_arrival_quote(
            conn, order_id=order_id, instrument_id=instrument_id, arrival_quote=lambda _i: quote, clock=lambda: NOW
        )
        status = conn.execute("SELECT status FROM orders WHERE order_id = %s", (order_id,)).fetchone()
    assert status == ("tca-probe",)
    assert _arrival_row(conn, order_id) == row
