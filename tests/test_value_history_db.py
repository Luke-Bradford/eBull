"""DB-backed test for the value-history hybrid units basis (#1594 v1).

One integration test per genuinely-new SQL mechanism (test-tiering
decision 2026-06-07): the hybrid `units_per_day` CTE — fills replay
for instruments with fills, positions.open_date basis for
broker-synced holdings — cannot be exercised by the mocked-cursor
tests in test_api_portfolio_value_history.py.

Calls the endpoint function directly with a real connection (no HTTP /
auth layer — the SQL is the subject under test).

NOTE: ``cash_ledger`` / ``fills`` are not in the worker-DB truncate
list (#1602), so this test cleans up its own rows in ``finally`` to
avoid leaking state into colocated tests.
"""

from __future__ import annotations

from datetime import date, timedelta

import psycopg

from app.api.portfolio import get_value_history
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401


def _db_today(conn: psycopg.Connection[tuple]) -> date:
    """The endpoint's date series ends at Postgres CURRENT_DATE — anchor
    the fixtures on the DB clock, not date.today(). Around midnight the
    two disagree (local BST vs cluster UTC) and `by_date[today]` keys
    miss (prevention log: datetime.now vs DB now() in freshness
    windows)."""
    cur = conn.execute("SELECT CURRENT_DATE")
    row = cur.fetchone()
    assert row is not None
    return row[0]


_IID_BROKER = 789901  # broker-synced: position row, NO fills
_IID_FILLS = 789902  # ebull-path: fills + position row (must not double-count)


def _seed(conn: psycopg.Connection[tuple], today: date) -> None:
    conn.execute("UPDATE runtime_config SET display_currency = 'USD' WHERE id = TRUE")
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%(a)s, 'VHDB1', 'Broker Co', '4', 'USD', TRUE),
               (%(b)s, 'VHDB2', 'Fills Co', '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        {"a": _IID_BROKER, "b": _IID_FILLS},
    )
    # Broker position: 10 units, opened 4 days ago, no fills anywhere.
    conn.execute(
        """
        INSERT INTO positions (instrument_id, open_date, avg_cost, current_units,
                               cost_basis, realized_pnl, unrealized_pnl, source)
        VALUES (%(a)s, %(open)s, 100, 10, 1000, 0, 0, 'broker_sync'),
               (%(b)s, %(open)s, 50, 5, 250, 0, 0, 'ebull')
        ON CONFLICT (instrument_id) DO UPDATE SET
            open_date = EXCLUDED.open_date,
            current_units = EXCLUDED.current_units
        """,
        {"a": _IID_BROKER, "b": _IID_FILLS, "open": today - timedelta(days=4)},
    )
    # Fills-path instrument: BUY 5 units 4 days ago. Its positions row
    # above must be IGNORED by the hybrid basis (fills replay wins).
    conn.execute(
        """
        INSERT INTO orders (order_id, instrument_id, action, order_type, requested_units, status, created_at)
        VALUES (889901, %(b)s, 'BUY', 'market', 5, 'filled', %(t)s)
        ON CONFLICT (order_id) DO NOTHING
        """,
        {"b": _IID_FILLS, "t": today - timedelta(days=4)},
    )
    conn.execute(
        """
        INSERT INTO fills (order_id, units, price, gross_amount, fees, filled_at)
        VALUES (889901, 5, 50, 250, 0, %(t)s)
        """,
        {"t": today - timedelta(days=4)},
    )
    # Daily closes: broker instrument 100 → 120; fills instrument flat 50.
    for offset, close_a in ((4, 100), (2, 110), (0, 120)):
        conn.execute(
            """
            INSERT INTO price_daily (instrument_id, price_date, close)
            VALUES (%(a)s, %(d)s, %(ca)s), (%(b)s, %(d)s, 50)
            ON CONFLICT (instrument_id, price_date) DO UPDATE SET close = EXCLUDED.close
            """,
            {"a": _IID_BROKER, "b": _IID_FILLS, "d": today - timedelta(days=offset), "ca": close_a},
        )
    conn.commit()


def _cleanup(conn: psycopg.Connection[tuple]) -> None:
    """Remove every seeded row + restore mutated singletons. positions
    is in the worker truncate list but fills/orders/instruments rows
    and the runtime_config mutation would otherwise leak into
    colocated tests (#1602; Codex ckpt-2 finding)."""
    conn.rollback()
    conn.execute("DELETE FROM fills WHERE order_id IN (889901, 889902, 889903)")
    conn.execute("DELETE FROM orders WHERE order_id IN (889901, 889902, 889903)")
    conn.execute(
        "DELETE FROM price_daily WHERE instrument_id IN (%(a)s, %(b)s)",
        {"a": _IID_BROKER, "b": _IID_FILLS},
    )
    conn.execute(
        "DELETE FROM positions WHERE instrument_id IN (%(a)s, %(b)s)",
        {"a": _IID_BROKER, "b": _IID_FILLS},
    )
    conn.execute(
        "DELETE FROM instruments WHERE instrument_id IN (%(a)s, %(b)s)",
        {"a": _IID_BROKER, "b": _IID_FILLS},
    )
    conn.execute("UPDATE runtime_config SET display_currency = 'GBP' WHERE id = TRUE")
    conn.commit()


def test_hybrid_units_basis_prices_broker_positions_and_avoids_double_count(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    today = _db_today(conn)
    _seed(conn, today)
    try:
        resp = get_value_history(range="1m", conn=conn)

        by_date = {p.date: p.value for p in resp.points}

        # Day of open (4 days ago): broker 10×100 + fills 5×50 = 1250.
        # Pre-#1594 the broker leg was invisible (fills-only replay)
        # and this read 250.
        assert by_date[today - timedelta(days=4)] == 1250.0

        # Carry-forward close between price rows (3 days ago): same.
        assert by_date[today - timedelta(days=3)] == 1250.0

        # Today: broker 10×120 + fills 5×50 = 1450. A double-counted
        # fills instrument (fills replay + position row) would add
        # another 250.
        assert by_date[today] == 1450.0

        # No points before the broker open_date (no cash seeded).
        assert (today - timedelta(days=5)) not in by_date

        # Marker events mirror the units basis (#1594): the broker
        # open surfaces as a position_open BUY, the fill as a fill
        # BUY — and the fills instrument must NOT also emit a
        # position_open event.
        events = [(e.symbol, e.side, e.units, e.source) for e in resp.events]
        assert ("VHDB1", "BUY", 10.0, "position_open") in events
        assert ("VHDB2", "BUY", 5.0, "fill") in events
        assert len([e for e in events if e[0] == "VHDB2"]) == 1
    finally:
        _cleanup(conn)


def test_broker_reopen_after_closed_fills_lifecycle_stays_priced(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """An instrument with OLD fills that net to zero (bought + fully
    closed via our order path) and a CURRENT broker-synced position is
    a broker re-open: the position basis must price it. An
    any-fills-ever suppression rule made the holding invisible (Codex
    ckpt-2 finding)."""
    conn = ebull_test_conn
    today = _db_today(conn)
    _seed(conn, today)
    try:
        # Closed fills lifecycle on the BROKER instrument: BUY 3 then
        # EXIT 3, both before the current position's open_date.
        conn.execute(
            """
            INSERT INTO orders (order_id, instrument_id, action, order_type, requested_units, status, created_at)
            VALUES (889902, %(a)s, 'BUY', 'market', 3, 'filled', %(t1)s),
                   (889903, %(a)s, 'EXIT', 'market', 3, 'filled', %(t2)s)
            ON CONFLICT (order_id) DO NOTHING
            """,
            {"a": _IID_BROKER, "t1": today - timedelta(days=30), "t2": today - timedelta(days=20)},
        )
        conn.execute(
            """
            INSERT INTO fills (order_id, units, price, gross_amount, fees, filled_at)
            VALUES (889902, 3, 90, 270, 0, %(t1)s),
                   (889903, 3, 95, 285, 0, %(t2)s)
            """,
            {"t1": today - timedelta(days=30), "t2": today - timedelta(days=20)},
        )
        conn.commit()

        resp = get_value_history(range="3m", conn=conn)
        by_date = {p.date: p.value for p in resp.points}

        # Today still prices the re-opened broker position: 10×120 +
        # fills instrument 5×50 = 1450. Suppressed-position bug would
        # read 250.
        assert by_date[today] == 1450.0

        # The re-open keeps its BUY marker too.
        events = [(e.symbol, e.side, e.source) for e in resp.events]
        assert ("VHDB1", "BUY", "position_open") in events
    finally:
        _cleanup(conn)
