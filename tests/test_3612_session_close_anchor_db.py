"""#3612 slice 2 — the session-close anchor S (sql/482) is insert-once and finite at rest."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, timedelta
from decimal import Decimal
from typing import LiteralString

import psycopg
import pytest

from app.services.account_equity_evidence import record_session_close_snapshot
from tests.fixtures.ebull_test_db import _reset_planner_tables, _truncate_planner_tables
from tests.test_3612_session_close_anchor import CLOSE, position, snapshot

SESSION = date(2026, 10, 12)


def _stored(conn: psycopg.Connection[tuple]) -> list[tuple]:
    return conn.execute(
        """
        SELECT s.session_date, s.observed_at, s.currency, s.official_direct_long_positions,
               s.official_direct_short_positions, m.position_id, m.unrealized_pnl, m.pnl_timestamp, m.total_fees
        FROM broker_account_session_close_snapshots s
        LEFT JOIN broker_account_session_close_marks m USING (environment, session_date)
        WHERE s.environment = 'demo'
        ORDER BY m.position_id
        """
    ).fetchall()


def test_the_first_qualifying_snapshot_writes_parent_and_children(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    observed = snapshot(positions=(position(1), position(2, pnl="-3")))
    assert record_session_close_snapshot(ebull_test_conn, environment="demo", snapshot=observed)
    rows = _stored(ebull_test_conn)
    assert [(r[0], r[1], r[2], r[3], r[4], r[5], r[6]) for r in rows] == [
        (SESSION, observed.observed_at, "USD", 2, 0, 1, Decimal("5.000000")),
        (SESSION, observed.observed_at, "USD", 2, 0, 2, Decimal("-3.000000")),
    ]
    assert all(r[7] == CLOSE + timedelta(minutes=10) and r[8] == Decimal("0") for r in rows)


def test_a_later_qualifying_snapshot_the_same_evening_changes_nothing(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    first = snapshot(positions=(position(1),))
    later = snapshot(
        observed_at=first.observed_at + timedelta(hours=1),
        positions=(position(1, pnl="9", pnl_timestamp=first.observed_at + timedelta(minutes=59)), position(2)),
    )
    assert record_session_close_snapshot(ebull_test_conn, environment="demo", snapshot=first)
    before = _stored(ebull_test_conn)
    assert not record_session_close_snapshot(ebull_test_conn, environment="demo", snapshot=later)
    assert _stored(ebull_test_conn) == before


def test_a_non_qualifying_snapshot_writes_nothing(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    before_close = snapshot(observed_at=CLOSE - timedelta(seconds=1))
    no_fees = snapshot(positions=(position(total_fees=None),))
    assert not record_session_close_snapshot(ebull_test_conn, environment="demo", snapshot=before_close)
    assert not record_session_close_snapshot(ebull_test_conn, environment="demo", snapshot=no_fees)
    assert _stored(ebull_test_conn) == []


def test_a_genuinely_empty_book_stores_a_parent_with_zero_counts(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    assert record_session_close_snapshot(ebull_test_conn, environment="demo", snapshot=snapshot(positions=()))
    rows = _stored(ebull_test_conn)
    assert [(r[3], r[4], r[5]) for r in rows] == [(0, 0, None)]


_PARENT = "broker_account_session_close_snapshots"
_CHILD = "broker_account_session_close_marks"


@pytest.mark.parametrize(
    ("statements", "guarded"),
    [
        (("UPDATE broker_account_session_close_snapshots SET currency = 'GBP'",), _PARENT),
        (("UPDATE broker_account_session_close_marks SET total_fees = 1",), _CHILD),
        (("DELETE FROM broker_account_session_close_marks",), _CHILD),
        (("DELETE FROM broker_account_session_close_snapshots",), _PARENT),
        (("TRUNCATE broker_account_session_close_marks",), _CHILD),
        # The child's own guard is dropped inside the savepoint, so only the parent's can fire.
        (
            (
                "DROP TRIGGER broker_account_session_close_marks_no_truncate ON broker_account_session_close_marks",
                "TRUNCATE broker_account_session_close_snapshots CASCADE",
            ),
            _PARENT,
        ),
    ],
)
def test_update_delete_and_truncate_are_refused_on_both_tables(
    ebull_test_conn: psycopg.Connection[tuple], statements: tuple[LiteralString, ...], guarded: str
) -> None:
    assert record_session_close_snapshot(ebull_test_conn, environment="demo", snapshot=snapshot())
    with (
        pytest.raises(psycopg.errors.RaiseException, match=f"^{guarded} is append-only"),
        ebull_test_conn.transaction(),
    ):
        for statement in statements:
            ebull_test_conn.execute(statement)


@pytest.mark.parametrize(
    "column", ["units", "amount", "unrealized_pnl", "market_value", "close_rate", "close_conversion_rate", "total_fees"]
)
@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity"])
def test_a_non_finite_numeric_is_refused_at_rest(
    ebull_test_conn: psycopg.Connection[tuple], column: str, value: str
) -> None:
    # A parent declaring the one child this test tries to insert; the insert itself must fail.
    ebull_test_conn.execute(
        """
        INSERT INTO broker_account_session_close_snapshots (
            environment, session_date, observed_at, account_currency_id, currency,
            official_direct_long_positions, official_direct_short_positions
        ) VALUES ('demo', %s, %s, 1, 'USD', 1, 0)
        """,
        (SESSION, CLOSE + timedelta(minutes=10)),
    )
    row = {
        "units": "2",
        "amount": "100",
        "unrealized_pnl": "5",
        "market_value": "105",
        "close_rate": "52.5",
        "close_conversion_rate": "1",
        "total_fees": "0",
    }
    row[column] = value
    with pytest.raises(psycopg.errors.CheckViolation), ebull_test_conn.transaction():
        ebull_test_conn.execute(
            """
            INSERT INTO broker_account_session_close_marks (
                environment, session_date, position_id, instrument_id, is_buy, units, amount, unrealized_pnl,
                market_value, is_partially_altered, close_rate, close_conversion_rate, asset_currency_id,
                pnl_timestamp, total_fees
            ) VALUES ('demo', %(session)s, 1, 101, true, %(units)s::numeric, %(amount)s::numeric,
                      %(unrealized_pnl)s::numeric, %(market_value)s::numeric, false, %(close_rate)s::numeric,
                      %(close_conversion_rate)s::numeric, 1, %(ts)s, %(total_fees)s::numeric)
            """,
            {"session": SESSION, "ts": CLOSE, **row},
        )


def test_a_snapshot_written_for_one_session_does_not_block_the_next(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    monday = snapshot()
    tuesday = snapshot(
        observed_at=monday.observed_at + timedelta(days=1),
        positions=(position(pnl_timestamp=CLOSE + timedelta(days=1, minutes=5)),),
    )
    assert record_session_close_snapshot(ebull_test_conn, environment="demo", snapshot=monday)
    assert record_session_close_snapshot(ebull_test_conn, environment="demo", snapshot=tuesday)
    sessions = {r[0] for r in _stored(ebull_test_conn)}
    assert sessions == {SESSION, SESSION + timedelta(days=1)}


@pytest.mark.parametrize("cleanup", [_reset_planner_tables, _truncate_planner_tables])
def test_the_harness_cleanup_empties_the_append_only_tables(
    ebull_test_conn: psycopg.Connection[tuple], cleanup: Callable[[psycopg.Connection[tuple]], None]
) -> None:
    """Both fixture paths run under ``session_replication_role = replica``, which disables the
    sql/482 UPDATE/DELETE/TRUNCATE triggers, and empty parent and children together."""
    assert record_session_close_snapshot(ebull_test_conn, environment="demo", snapshot=snapshot())
    ebull_test_conn.commit()
    cleanup(ebull_test_conn)
    assert ebull_test_conn.execute(
        "SELECT (SELECT count(*) FROM broker_account_session_close_snapshots),"
        " (SELECT count(*) FROM broker_account_session_close_marks)"
    ).fetchone() == (0, 0)
