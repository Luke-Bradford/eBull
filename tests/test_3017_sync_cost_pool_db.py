"""#3017 — ``portfolio_sync`` must move the cost pool with the units.

Against a real database on purpose, for the same reason as
``tests/test_3008_partial_exit_cost_basis_db.py``: the fix is arithmetic inside
``UPDATE`` statements whose ``CASE`` reads the PRE-update row, and the property
that matters is numeric — that ``cost_basis = avg_cost * current_units``
survives a broker-observed units change. A mocked cursor evaluates none of that.

Source rule: HMRC s104 part disposal from an average-cost pool, as implemented
in ``app/services/tax_ledger.py`` and already encoded by the two sibling writers
(``order_client._update_position_exit``, ``app/api/orders.py``'s manual close).
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import psycopg

from app.providers.broker import BrokerPortfolio, BrokerPosition
from app.services.order_client import _update_position_buy
from app.services.portfolio_sync import sync_portfolio

INSTRUMENT_ID = 990_017
OTHER_INSTRUMENT_ID = 990_018
_NOW = datetime(2026, 9, 14, 12, 0, tzinfo=UTC)


def _seed_instruments(conn: psycopg.Connection[Any]) -> None:
    """``is_tradable`` listed explicitly per #1233 §6.2 (chokepoint lint)."""
    conn.execute(
        "INSERT INTO instruments (instrument_id,symbol,company_name,is_tradable) "
        "VALUES (%s,'SYNC.POOL','Sync Cost Pool Test',TRUE) ON CONFLICT DO NOTHING",
        (INSTRUMENT_ID,),
    )
    conn.execute(
        "INSERT INTO instruments (instrument_id,symbol,company_name,is_tradable) "
        "VALUES (%s,'SYNC.KEEP','Sync Cost Pool Other',TRUE) ON CONFLICT DO NOTHING",
        (OTHER_INSTRUMENT_ID,),
    )


def _read(conn: psycopg.Connection[Any]) -> tuple[Decimal, Decimal, Decimal]:
    row = conn.execute(
        "SELECT current_units, avg_cost, cost_basis FROM positions WHERE instrument_id = %s",
        (INSTRUMENT_ID,),
    ).fetchone()
    assert row is not None
    return (Decimal(str(row[0])), Decimal(str(row[1])), Decimal(str(row[2])))


def _broker_position(instrument_id: int, units: str, open_price: str) -> BrokerPosition:
    return BrokerPosition(
        instrument_id=instrument_id,
        units=Decimal(units),
        open_price=Decimal(open_price),
        current_price=Decimal(open_price),
        raw_payload={},
        position_id=instrument_id,
        amount=Decimal(units) * Decimal(open_price),
        open_date_time=_NOW,
    )


def _sync(conn: psycopg.Connection[Any], positions: list[BrokerPosition]) -> None:
    sync_portfolio(
        conn,
        BrokerPortfolio(positions=positions, available_cash=Decimal("0"), raw_payload={}),
        now=_NOW,
    )


def _open(conn: psycopg.Connection[Any], *, price: str, units: str) -> None:
    _seed_instruments(conn)
    _update_position_buy(conn, INSTRUMENT_ID, Decimal(price), Decimal(units), _NOW)


def test_a_broker_side_decrease_withdraws_its_share_of_the_pool(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """500 of 1500 closed in the broker's own app leaves two thirds of the cost,
    and the per-unit cost is untouched — that is what makes it a pool."""
    _open(ebull_test_conn, price="20", units="1500")

    _sync(ebull_test_conn, [_broker_position(INSTRUMENT_ID, "1000", "20")])

    units, avg_cost, cost_basis = _read(ebull_test_conn)
    assert units == Decimal("1000.000000")
    assert avg_cost == Decimal("20.000000")
    assert cost_basis == Decimal("20000.000000")
    assert cost_basis == avg_cost * units


def test_a_position_absent_from_the_payload_empties_the_pool(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The ``closed_externally`` branch. A second position is present so the
    empty-payload guard (``refusing to zero out local state``) does not fire —
    without it this test would exercise the refusal, not the fix."""
    _open(ebull_test_conn, price="750.50", units="1")

    _sync(ebull_test_conn, [_broker_position(OTHER_INSTRUMENT_ID, "5", "10")])

    units, _avg_cost, cost_basis = _read(ebull_test_conn)
    assert units == Decimal("0.000000")
    assert cost_basis == Decimal("0.000000")


def test_a_reopen_after_an_external_close_prices_only_the_new_units(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The consequence the stale pool actually caused, and the reason this is a
    bug rather than a cosmetic mismatch.

    ``_update_position_buy`` recomputes ``avg_cost`` from ``positions.cost_basis``,
    so the re-open inherits whatever the close left behind. With the pool still
    at 750.50 this booked ``(750.50 + 600) / 10 = 135.05`` — more than double the
    price paid — and every later realised-P&L accrual ran off it.
    """
    _open(ebull_test_conn, price="750.50", units="1")
    _sync(ebull_test_conn, [_broker_position(OTHER_INSTRUMENT_ID, "5", "10")])

    _update_position_buy(ebull_test_conn, INSTRUMENT_ID, Decimal("60"), Decimal("10"), _NOW)

    units, avg_cost, cost_basis = _read(ebull_test_conn)
    assert units == Decimal("10.000000")
    assert avg_cost == Decimal("60.000000")
    assert cost_basis == Decimal("600.000000")


def test_repeated_syncs_of_an_unchanged_fractional_holding_do_not_erode_the_pool(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The ratchet Codex found at checkpoint 2.

    ``positions.current_units`` is ``numeric(18,6)``; ``broker_positions.units``
    is ``numeric(20,8)`` and the parser keeps the incoming precision. So an 8-dp
    holding is STORED rounded, and comparing the raw parameter against the stored
    value reads the rounding artefact as a disposal — on an unchanged portfolio,
    every sync forever. Three syncs is enough to see it ratchet.
    """
    _open(ebull_test_conn, price="100", units="1.23456789")
    _, _, opening_pool = _read(ebull_test_conn)

    for _ in range(3):
        _sync(ebull_test_conn, [_broker_position(INSTRUMENT_ID, "1.23456789", "100")])

    units, avg_cost, cost_basis = _read(ebull_test_conn)
    assert units == Decimal("1.234568")
    assert cost_basis == opening_pool
    assert avg_cost == Decimal("100.000000")


def test_a_broker_side_increase_leaves_the_pool_alone(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Pins the deliberate gap so a later reader does not mistake it for an
    oversight. The added units were bought at a price we do not hold; scaling
    the pool at our ``avg_cost`` would invent a cost and importing the broker's
    ``avg_open_price`` would overwrite local tax-lot history. Stated on #3017.
    """
    _open(ebull_test_conn, price="20", units="1000")

    _sync(ebull_test_conn, [_broker_position(INSTRUMENT_ID, "1500", "20")])

    units, avg_cost, cost_basis = _read(ebull_test_conn)
    assert units == Decimal("1500.000000")
    assert avg_cost == Decimal("20.000000")
    # Unchanged — still the pool for the original 1000 units.
    assert cost_basis == Decimal("20000.000000")
