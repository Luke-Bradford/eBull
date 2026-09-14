"""#3020 — a filled EXIT must deduct the broker lot it closed.

Against a real database on purpose: the fix is a ``SELECT … FOR UPDATE``, a
prorating ``UPDATE`` with a ``CASE`` and a rowcount branch, and the property that
matters is that ``_load_exit_lot`` stops returning the lot afterwards. A mocked
cursor can prove none of that — it can only show the SQL string contains the
words (``tests/test_order_client.py`` holds the call-site wiring tests, which is
the half a mock CAN prove).

Own module because the ``db`` marker is module-scoped: one DB test inside
``tests/test_order_client.py`` would evict that whole file from the
``-m "not db"`` push gate.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import psycopg
import pytest

from app.services.order_client import _deduct_closed_exit_lot, _load_exit_lot
from tests.fixtures.ebull_test_db import test_database_url

INSTRUMENT_ID = 990_020
POSITION_ID = 3_308_442_058
_NOW = datetime(2026, 9, 14, 12, 0, tzinfo=UTC)


def _seed_lot(
    conn: psycopg.Connection[Any],
    *,
    position_id: int = POSITION_ID,
    units: str = "1000",
    amount: str = "20000",
    open_minute: int = 0,
) -> None:
    conn.execute(
        "INSERT INTO instruments (instrument_id,symbol,company_name,is_tradable) "
        "VALUES (%s,'EXIT.LOT','Exit Lot Deduction Test',TRUE) ON CONFLICT DO NOTHING",
        (INSTRUMENT_ID,),
    )
    conn.execute(
        """
        INSERT INTO broker_positions
            (position_id, instrument_id, is_buy, units, amount,
             initial_amount_in_dollars, open_rate, open_conversion_rate,
             open_date_time, raw_payload)
        VALUES (%s, %s, TRUE, %s, %s, %s, 20, 1, %s, '{}'::jsonb)
        """,
        (
            position_id,
            INSTRUMENT_ID,
            Decimal(units),
            Decimal(amount),
            Decimal(amount),
            _NOW.replace(minute=open_minute),
        ),
    )


def _read_lot(conn: psycopg.Connection[Any], position_id: int = POSITION_ID) -> tuple[Decimal, Decimal]:
    row = conn.execute(
        "SELECT units, amount FROM broker_positions WHERE position_id = %s",
        (position_id,),
    ).fetchone()
    assert row is not None
    return (Decimal(str(row[0])), Decimal(str(row[1])))


def test_a_whole_close_empties_the_lot_and_its_amount(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_lot(ebull_test_conn)

    _deduct_closed_exit_lot(ebull_test_conn, position_id=POSITION_ID, filled_units=Decimal("1000"), now=_NOW)

    units, amount = _read_lot(ebull_test_conn)
    assert units == Decimal("0.00000000")
    assert amount == Decimal("0.00000000")


def test_the_closed_lot_is_no_longer_selectable_by_the_next_exit(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The consequence that makes this a bug rather than a display mismatch.

    ``_load_exit_lot`` is FIFO-oldest over ``units > 0``, so before the deduction
    the lot the broker had just closed was still the one the NEXT EXIT would be
    handed — a close addressed to a position id the broker no longer has.
    """
    _seed_lot(ebull_test_conn, units="1000", amount="20000", open_minute=0)
    _seed_lot(ebull_test_conn, position_id=POSITION_ID + 1, units="500", amount="11000", open_minute=30)

    before = _load_exit_lot(ebull_test_conn, INSTRUMENT_ID)
    assert before is not None and before.position_id == POSITION_ID

    _deduct_closed_exit_lot(ebull_test_conn, position_id=POSITION_ID, filled_units=Decimal("1000"), now=_NOW)

    after = _load_exit_lot(ebull_test_conn, INSTRUMENT_ID)
    assert after is not None
    assert after.position_id == POSITION_ID + 1
    assert after.units == Decimal("500.00000000")


def test_a_partial_close_leaves_the_remainder_selectable(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """``amount`` prorates with the units, so the residual lot still carries the
    cost of exactly what is left."""
    _seed_lot(ebull_test_conn, units="1000", amount="20000")

    _deduct_closed_exit_lot(ebull_test_conn, position_id=POSITION_ID, filled_units=Decimal("400"), now=_NOW)

    units, amount = _read_lot(ebull_test_conn)
    assert units == Decimal("600.00000000")
    assert amount == Decimal("12000.00000000")

    remaining = _load_exit_lot(ebull_test_conn, INSTRUMENT_ID)
    assert remaining is not None and remaining.units == Decimal("600.00000000")


def test_a_missing_lot_warns_and_does_not_raise(
    ebull_test_conn: psycopg.Connection[tuple],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The mirror is refreshed wholesale by the next sync, so a failed deduction
    is self-healing. Raising would roll back a durable record of a fill the
    broker has ALREADY executed — strictly worse than a stale row. Contrast
    ``_update_position_exit`` (#3013), which raises because ``positions`` is the
    ledger, not the mirror."""
    with caplog.at_level("WARNING", logger="app.services.order_client"):
        _deduct_closed_exit_lot(ebull_test_conn, position_id=POSITION_ID, filled_units=Decimal("1000"), now=_NOW)

    assert "absent or locked by a concurrent sync" in caplog.text


def test_a_lot_holding_fewer_units_than_the_fill_warns_and_deducts_nothing(
    ebull_test_conn: psycopg.Connection[tuple],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A concurrent close consumed the units first. Deducting anyway would drive
    the mirror negative."""
    _seed_lot(ebull_test_conn, units="300", amount="6000")

    with caplog.at_level("WARNING", logger="app.services.order_client"):
        _deduct_closed_exit_lot(ebull_test_conn, position_id=POSITION_ID, filled_units=Decimal("1000"), now=_NOW)

    units, amount = _read_lot(ebull_test_conn)
    assert units == Decimal("300.00000000")
    assert amount == Decimal("6000.00000000")
    assert "the row holds only" in caplog.text


def test_a_ninth_decimal_fill_does_not_leave_the_lot_selectable(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """#3017's lesson applied here. ``broker_positions.units`` is
    ``numeric(20,8)``; a fill carrying more precision than the column, compared
    raw, would fail ``units >= %(units)s`` on an exact whole close and silently
    leave the closed lot selectable. The cast makes both sides the same grain."""
    _seed_lot(ebull_test_conn, units="1000.12345678", amount="20000")

    _deduct_closed_exit_lot(
        ebull_test_conn,
        position_id=POSITION_ID,
        filled_units=Decimal("1000.123456784"),
        now=_NOW,
    )

    units, _amount = _read_lot(ebull_test_conn)
    assert units == Decimal("0.00000000")
    assert _load_exit_lot(ebull_test_conn, INSTRUMENT_ID) is None


def test_a_lot_locked_by_a_concurrent_sync_is_skipped_not_waited_on(
    ebull_test_conn: psycopg.Connection[tuple],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The deadlock Codex found at checkpoint 2, driven rather than argued.

    ``portfolio_sync`` reaches ``broker_positions`` (L797, where it DELETEs lots
    absent from the payload) BEFORE the ``positions`` row of an instrument that
    disappeared (L810) — and an instrument whose last lot an EXIT just closed is
    exactly one that disappears. With a plain ``FOR UPDATE`` this transaction,
    already holding ``positions``, would wait on the mirror row and close the
    cycle; Postgres would abort one side, and aborting this one rolls back the
    fill, cash credit and audit for a close the broker already executed.

    ``SKIP LOCKED`` means it never waits. The assertion that matters is that this
    call RETURNS at all — under ``FOR UPDATE`` it would block until the other
    transaction ended.
    """
    _seed_lot(ebull_test_conn, units="1000", amount="20000")
    ebull_test_conn.commit()

    rival = psycopg.connect(test_database_url())
    try:
        rival.execute(
            "SELECT units FROM broker_positions WHERE position_id = %s FOR UPDATE",
            (POSITION_ID,),
        )
        with caplog.at_level("WARNING", logger="app.services.order_client"):
            _deduct_closed_exit_lot(
                ebull_test_conn, position_id=POSITION_ID, filled_units=Decimal("1000"), now=_NOW
            )
        assert "absent or locked by a concurrent sync" in caplog.text
    finally:
        rival.rollback()
        rival.close()

    # Untouched — the sync holding the row is the one that resolves it.
    units, amount = _read_lot(ebull_test_conn)
    assert units == Decimal("1000.00000000")
    assert amount == Decimal("20000.00000000")


def test_the_same_row_would_have_blocked_without_skip_locked(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Pins WHY the clause is there, so removing it fails rather than hanging.

    The test above shows the deduction returns under contention; on its own that
    is also what a lock-free implementation would do. This one shows the
    counterfactual on the same row: a plain ``FOR UPDATE`` WAITS (it is cancelled
    by the statement timeout), which is the wait that closes the deadlock cycle
    against ``portfolio_sync``. A revert of ``SKIP LOCKED`` therefore turns the
    test above into a hang, and a hang is the one failure mode a suite reports
    badly — so the mechanism is asserted directly.
    """
    _seed_lot(ebull_test_conn, units="1000", amount="20000")
    ebull_test_conn.commit()

    rival = psycopg.connect(test_database_url())
    probe = psycopg.connect(test_database_url())
    try:
        rival.execute(
            "SELECT units FROM broker_positions WHERE position_id = %s FOR UPDATE",
            (POSITION_ID,),
        )
        probe.execute("SET statement_timeout = '250ms'")
        with pytest.raises(psycopg.errors.QueryCanceled):
            probe.execute(
                "SELECT units FROM broker_positions WHERE position_id = %s FOR UPDATE",
                (POSITION_ID,),
            ).fetchone()
        probe.rollback()
        probe.execute("SET statement_timeout = '250ms'")
        skipped = probe.execute(
            "SELECT units FROM broker_positions WHERE position_id = %s FOR UPDATE SKIP LOCKED",
            (POSITION_ID,),
        ).fetchone()
        assert skipped is None
    finally:
        probe.rollback()
        probe.close()
        rival.rollback()
        rival.close()
