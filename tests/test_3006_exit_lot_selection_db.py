"""#3006 — which broker lot a recommendation EXIT is allowed to close.

The completion arithmetic is pure and lives in ``tests/test_order_client.py``.
What needs a real Postgres is the SELECT itself: the fix IS a predicate, and a
mocked cursor can only prove the SQL text contains the words. Whether a short
lot and a synthetic ``-order_id`` row are actually excluded — and which of
several eligible lots wins the FIFO ordering — is only answerable by running the
query against rows.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import psycopg

from app.services.order_client import _load_exit_lot

INSTRUMENT_ID = 990_006
_NOW = datetime(2026, 9, 14, 12, 0, tzinfo=UTC)


def _seed_instrument(conn: psycopg.Connection[Any]) -> None:
    """``is_tradable`` listed explicitly per #1233 §6.2 (chokepoint lint)."""
    conn.execute(
        "INSERT INTO instruments (instrument_id,symbol,company_name,is_tradable) "
        "VALUES (%s,'EXIT.LOT','Exit Lot Selection Test',TRUE) ON CONFLICT DO NOTHING",
        (INSTRUMENT_ID,),
    )


def _seed_lot(
    conn: psycopg.Connection[Any],
    *,
    position_id: int,
    units: str,
    is_buy: bool = True,
    opened_days_ago: int = 0,
) -> None:
    conn.execute(
        """
        INSERT INTO broker_positions
            (position_id, instrument_id, is_buy, units, amount,
             initial_amount_in_dollars, open_rate, open_conversion_rate,
             open_date_time, is_no_stop_loss, is_no_take_profit,
             leverage, is_tsl_enabled, total_fees, source, raw_payload, updated_at)
        VALUES
            (%(pid)s, %(iid)s, %(is_buy)s, %(units)s, 1000,
             1000, 100, 1,
             %(opened)s, TRUE, TRUE,
             1, FALSE, 0, 'broker_sync', '{}'::jsonb, %(now)s)
        """,
        {
            "pid": position_id,
            "iid": INSTRUMENT_ID,
            "is_buy": is_buy,
            "units": Decimal(units),
            "opened": _NOW.replace(day=_NOW.day - opened_days_ago),
            "now": _NOW,
        },
    )


def test_the_oldest_long_lot_wins(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """FIFO, and the lot's own units come back with it."""
    _seed_instrument(ebull_test_conn)
    _seed_lot(ebull_test_conn, position_id=3308442058, units="1000", opened_days_ago=3)
    _seed_lot(ebull_test_conn, position_id=3310085041, units="500", opened_days_ago=1)

    lot = _load_exit_lot(ebull_test_conn, INSTRUMENT_ID)
    assert lot is not None
    assert lot.position_id == 3308442058
    # The aggregate is 1500; the selector must return the LOT, which is what
    # the broker will actually be asked to close.
    assert lot.units == Decimal("1000.00000000")


def test_a_short_lot_is_never_selected(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """This path books a long sale — ``(price - avg_cost) * units`` and a cash
    CREDIT. Closing a short through it would post that accounting to a
    buy-to-close, so an unaccompanied short must yield nothing at all rather
    than the nearest available row."""
    _seed_instrument(ebull_test_conn)
    _seed_lot(ebull_test_conn, position_id=4400000001, units="10", is_buy=False, opened_days_ago=5)

    assert _load_exit_lot(ebull_test_conn, INSTRUMENT_ID) is None


def test_a_short_lot_does_not_outrank_a_younger_long(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The dangerous ordering: the short is OLDER, so age alone would pick it."""
    _seed_instrument(ebull_test_conn)
    _seed_lot(ebull_test_conn, position_id=4400000001, units="10", is_buy=False, opened_days_ago=5)
    _seed_lot(ebull_test_conn, position_id=3310085041, units="500", opened_days_ago=1)

    lot = _load_exit_lot(ebull_test_conn, INSTRUMENT_ID)
    assert lot is not None
    assert lot.position_id == 3310085041


def test_a_synthetic_position_id_is_never_selected(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """``_persist_broker_position`` writes ``-order_id`` for every eBull BUY
    fill, live branch included, because the broker's real position id is not in
    the order response. That row is our record of a fill, not a handle the
    broker can close — posting to ``…/positions/-77`` addresses nothing."""
    _seed_instrument(ebull_test_conn)
    _seed_lot(ebull_test_conn, position_id=-77, units="25", opened_days_ago=9)

    assert _load_exit_lot(ebull_test_conn, INSTRUMENT_ID) is None


def test_a_synthetic_id_does_not_outrank_a_real_lot(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """A synthetic row is written at fill time and a real lot arrives at the
    next sync, so the synthetic one is reliably the older of the two — exactly
    the case age-only ordering gets wrong."""
    _seed_instrument(ebull_test_conn)
    _seed_lot(ebull_test_conn, position_id=-77, units="25", opened_days_ago=9)
    _seed_lot(ebull_test_conn, position_id=3308442058, units="1000", opened_days_ago=3)

    lot = _load_exit_lot(ebull_test_conn, INSTRUMENT_ID)
    assert lot is not None
    assert lot.position_id == 3308442058


def test_a_closed_lot_is_not_selected(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """Pre-existing ``units > 0`` filter — pinned so the added predicates cannot
    be read as having replaced it."""
    _seed_instrument(ebull_test_conn)
    _seed_lot(ebull_test_conn, position_id=3308442058, units="0", opened_days_ago=3)

    assert _load_exit_lot(ebull_test_conn, INSTRUMENT_ID) is None
