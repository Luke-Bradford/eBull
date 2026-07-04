"""DB-tier test for ``load_day_changes`` (#1924).

Exercises the window-query mechanism against a real Postgres: last-two
strictly-positive closes per instrument, zero-close skipping, and omission of
instruments with fewer than two positive closes. Auto-marked ``db`` (pulls
``ebull_test_conn``).

Synthetic instruments only (ids well outside any real-data range).
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import psycopg

from app.services.market_data import load_day_changes

_TWO_CLOSE_ID = 991001
_ZERO_SKIP_ID = 991002
_ONE_CLOSE_ID = 991003

_END = date.today() - timedelta(days=3)


def _seed_instrument(conn: psycopg.Connection[tuple], iid: int) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, currency, country, is_tradable)
        VALUES (%s, %s, %s, 'USD', 'US', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, f"DC{iid}", f"DayChange {iid}"),
    )


def _seed_series(conn: psycopg.Connection[tuple], iid: int, closes: list[float]) -> None:
    """Insert ``closes`` ending at ``_END`` (one calendar day apart, ASC)."""
    n = len(closes)
    for i, close in enumerate(closes):
        conn.execute(
            """
            INSERT INTO price_daily (instrument_id, price_date, close)
            VALUES (%s, %s, %s)
            ON CONFLICT (instrument_id, price_date) DO UPDATE SET close = EXCLUDED.close
            """,
            (iid, _END - timedelta(days=(n - 1 - i)), close),
        )


def test_load_day_changes_two_positive_closes(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    _seed_instrument(conn, _TWO_CLOSE_ID)
    # prior 100 → latest 102 → +0.02, as_of = _END (the latest bar).
    _seed_series(conn, _TWO_CLOSE_ID, [100.0, 102.0])

    result = load_day_changes(conn, [_TWO_CLOSE_ID])

    dc = result[_TWO_CLOSE_ID]
    assert dc.as_of == _END
    assert dc.last_close == Decimal("102.000000")
    assert dc.prior_close == Decimal("100.000000")
    assert dc.change_abs == Decimal("2.000000")
    assert dc.change_pct == Decimal("0.02")


def test_load_day_changes_skips_zero_close(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    _seed_instrument(conn, _ZERO_SKIP_ID)
    # A 0.00 sentinel sits between the two real closes; the window filters it
    # out, so prior = 100 (not 0 → no -100% artefact), latest = 110 → +0.10.
    _seed_series(conn, _ZERO_SKIP_ID, [100.0, 0.0, 110.0])

    dc = load_day_changes(conn, [_ZERO_SKIP_ID])[_ZERO_SKIP_ID]

    assert dc.prior_close == Decimal("100.000000")
    assert dc.last_close == Decimal("110.000000")
    assert dc.change_pct == Decimal("0.1")


def test_load_day_changes_omits_fewer_than_two_positive(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    conn = ebull_test_conn
    _seed_instrument(conn, _ONE_CLOSE_ID)
    # Only one positive close (the other is a zero sentinel) → no day-change.
    _seed_series(conn, _ONE_CLOSE_ID, [0.0, 55.0])

    result = load_day_changes(conn, [_ONE_CLOSE_ID])

    assert _ONE_CLOSE_ID not in result


def test_load_day_changes_empty_ids_returns_empty(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    assert load_day_changes(ebull_test_conn, []) == {}
