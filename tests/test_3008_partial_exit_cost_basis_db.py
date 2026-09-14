"""#3008 — a partial EXIT must withdraw its share of the cost pool.

Against a real database on purpose. The fix is arithmetic inside a single
`UPDATE`, and the properties that matter are numeric: that the invariant
`cost_basis = avg_cost * current_units` survives each step, and that cost is
CONSERVED across a sequence of part disposals rather than drifting a rounding
error at a time. A mocked cursor cannot evaluate `round()`, `CASE`, or
Postgres's pre-update read semantics, so it cannot see any of that.

Source rule for the arithmetic: HMRC s104 part disposal from an average-cost
pool, as already implemented in ``app/services/tax_ledger.py``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import psycopg

from app.services.order_client import _update_position_buy, _update_position_exit

INSTRUMENT_ID = 990_008
_NOW = datetime(2026, 9, 14, 12, 0, tzinfo=UTC)
_CENT = Decimal("0.000001")


def _seed_instrument(conn: psycopg.Connection[Any]) -> None:
    """``is_tradable`` listed explicitly per #1233 §6.2 (chokepoint lint)."""
    conn.execute(
        "INSERT INTO instruments (instrument_id,symbol,company_name,is_tradable) "
        "VALUES (%s,'COST.BASIS','Partial Exit Cost Basis Test',TRUE) ON CONFLICT DO NOTHING",
        (INSTRUMENT_ID,),
    )


def _read(conn: psycopg.Connection[Any]) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    row = conn.execute(
        "SELECT current_units, avg_cost, cost_basis, realized_pnl FROM positions WHERE instrument_id = %s",
        (INSTRUMENT_ID,),
    ).fetchone()
    assert row is not None
    return (Decimal(str(row[0])), Decimal(str(row[1])), Decimal(str(row[2])), Decimal(str(row[3])))


def _open(conn: psycopg.Connection[Any], *, price: str, units: str) -> None:
    _seed_instrument(conn)
    _update_position_buy(conn, INSTRUMENT_ID, Decimal(price), Decimal(units), _NOW)


def test_a_partial_exit_withdraws_its_share_of_the_pool(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """1000 of 1500 disposed leaves two thirds of the cost, and the per-unit
    cost is untouched — that is what makes it an average-cost pool."""
    _open(ebull_test_conn, price="20", units="1500")
    _update_position_exit(ebull_test_conn, INSTRUMENT_ID, Decimal("25"), Decimal("1000"), _NOW)

    units, avg_cost, cost_basis, realized = _read(ebull_test_conn)
    assert units == Decimal("500.000000")
    assert avg_cost == Decimal("20.000000")
    assert cost_basis == Decimal("10000.000000")
    # Gain denominated at the pool average, unchanged by this ticket.
    assert realized == Decimal("5000.000000")


def test_the_invariant_survives_a_repeating_average(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The case the proportional fraction exists for.

    10000 / 3 is non-terminating, so ``avg_cost`` is a rounded 3333.333333 and
    ``avg * units`` would NOT reproduce the pool. Taking the fraction of the
    total keeps ``cost_basis = avg_cost * current_units`` inside one unit of the
    column's last place.
    """
    _open(ebull_test_conn, price="3333.333333", units="3")
    _update_position_exit(ebull_test_conn, INSTRUMENT_ID, Decimal("4000"), Decimal("1"), _NOW)

    units, avg_cost, cost_basis, _ = _read(ebull_test_conn)
    assert units == Decimal("2.000000")
    assert abs(cost_basis - avg_cost * units) <= _CENT


def test_cost_is_conserved_across_a_sequence_of_part_disposals(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The property a per-step ``avg * units`` subtraction loses.

    Six awkward slices out of a pool, then a final disposal that empties it. The
    pool must land on EXACTLY zero, not on a residue of accumulated rounding.

    ⚠ The intermediate invariant is approximate, and this test pins HOW
    approximate. Each ``round(…, 6)`` can move the basis by up to one unit in
    the column's last place, so after ``n`` part disposals the gap against
    ``avg_cost * units`` is bounded by ``n`` ULP — measured at 1.2e-6 after six.
    That is sub-micro-dollar and it does NOT compound into the final state,
    because full depletion takes the exact remainder instead of a computed one.
    (For scale: the five positions held on dev already sit up to 5e-4 from the
    same identity, since ``avg_cost`` is itself stored rounded.)
    """
    _open(ebull_test_conn, price="123.456789", units="7")
    steps = 6
    for step in range(1, steps + 1):
        _update_position_exit(ebull_test_conn, INSTRUMENT_ID, Decimal("130"), Decimal("0.7"), _NOW)
        units, avg_cost, cost_basis, _ = _read(ebull_test_conn)
        assert abs(cost_basis - avg_cost * units) <= step * _CENT

    units, _, cost_basis, _ = _read(ebull_test_conn)
    assert units == Decimal("2.800000")
    assert cost_basis > 0

    _update_position_exit(ebull_test_conn, INSTRUMENT_ID, Decimal("130"), units, _NOW)
    units, _, cost_basis, _ = _read(ebull_test_conn)
    assert units == Decimal("0.000000")
    assert cost_basis == Decimal("0.000000")


def test_an_over_disposal_empties_the_pool_rather_than_going_negative(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """A broker fill can exceed the local ledger's units (a sync lag, or dust).

    The units column is allowed to go negative — pre-existing behaviour that
    ``_maybe_trigger_attribution`` reads as 'closed' — but a NEGATIVE cost basis
    would be a fabricated credit, and it would flow into the no-quote
    market-value fallback in ``portfolio.py``.
    """
    _open(ebull_test_conn, price="20", units="100")
    _update_position_exit(ebull_test_conn, INSTRUMENT_ID, Decimal("25"), Decimal("150"), _NOW)

    units, _, cost_basis, _ = _read(ebull_test_conn)
    assert units == Decimal("-50.000000")
    assert cost_basis == Decimal("0.000000")


def test_a_later_add_does_not_inherit_an_inflated_average(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The operator-visible consequence this ticket was filed for.

    ``_update_position_buy`` recomputes ``avg_cost`` from
    ``positions.cost_basis``. With the basis left at its full original value,
    100 units at 20 followed by a 50-unit exit and a 50-unit buy at 30 produced
    an average of (2000 + 1500) / 100 = 35.00 — above BOTH purchase prices, and
    every subsequent ``realized_pnl`` accrual ran off it.
    """
    _open(ebull_test_conn, price="20", units="100")
    _update_position_exit(ebull_test_conn, INSTRUMENT_ID, Decimal("25"), Decimal("50"), _NOW)
    _update_position_buy(ebull_test_conn, INSTRUMENT_ID, Decimal("30"), Decimal("50"), _NOW)

    units, avg_cost, cost_basis, _ = _read(ebull_test_conn)
    assert units == Decimal("100.000000")
    # (1000 remaining + 1500 new) / 100 = 25.00, between the two prices paid.
    assert avg_cost == Decimal("25.000000")
    assert cost_basis == Decimal("2500.000000")
    assert avg_cost < Decimal("30")
