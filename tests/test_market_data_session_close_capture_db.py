"""#3740 slice F — `etoro_session_close_captures` (sql/479), the close eToro served on its own session.

Real DB because the property under test is that the capture commits or rolls back with the bar write.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch

import psycopg
import pytest

from app.providers.market_data import OHLCVBar
from app.services.market_data import most_recent_trading_day, refresh_market_data
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]

pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test DB unavailable",
)

_IID = 937_401


def _weekdays_back(end: date, n: int) -> list[date]:
    out: list[date] = []
    d = end
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d -= timedelta(days=1)
    return list(reversed(out))


def _bar(price_date: date, close: str) -> OHLCVBar:
    c = Decimal(close)
    return OHLCVBar(price_date=price_date, open=c, high=c, low=c, close=c, volume=1000)


def _setup(conn: psycopg.Connection[tuple], n: int, close: str) -> list[date]:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
        "VALUES (%s, 'SESSCAP', 'Session Capture Co', TRUE) ON CONFLICT (instrument_id) DO NOTHING",
        (_IID,),
    )
    days = _weekdays_back(most_recent_trading_day(date.today()), n)
    for d in days[:-1]:
        conn.execute(
            "INSERT INTO price_daily (instrument_id, price_date, open, high, low, close, volume) "
            "VALUES (%s, %s, %s, %s, %s, %s, 1000)",
            (_IID, d, Decimal(close), Decimal(close), Decimal(close), Decimal(close)),
        )
    return days


def _run(
    conn: psycopg.Connection[tuple],
    responses: list[list[OHLCVBar]],
    *,
    fresh_through: date | None,
) -> None:
    provider = MagicMock()
    provider.get_daily_candles.side_effect = responses
    refresh_market_data(
        provider,
        conn,
        instruments=[(_IID, "SESSCAP")],
        lookback_days=1000,
        skip_quotes=True,
        fresh_through=fresh_through,
    )


def _captures(conn: psycopg.Connection[tuple]) -> list[tuple[date, Decimal]]:
    return [
        (r[0], r[1])
        for r in conn.execute(
            "SELECT session_date, close FROM etoro_session_close_captures WHERE instrument_id = %s ORDER BY capture_id",
            (_IID,),
        ).fetchall()
    ]


def test_only_the_completed_session_bar_is_captured(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    days = _setup(conn, 4, "100")
    _run(conn, [[_bar(days[-3], "100"), _bar(days[-2], "101"), _bar(days[-1], "102")]], fresh_through=days[-1])
    assert _captures(conn) == [(days[-1], Decimal("102"))]


def test_no_capture_without_a_session_boundary(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """Without `fresh_through` the caller has not said which bar is its own session's."""
    conn = ebull_test_conn
    days = _setup(conn, 4, "100")
    _run(conn, [[_bar(days[-2], "101"), _bar(days[-1], "102")]], fresh_through=None)
    assert _captures(conn) == []


def test_a_heal_captures_the_session_close_from_the_refetch(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """A 3:1 re-basing heals the series; the session bar is captured once, as the heal served it."""
    conn = ebull_test_conn
    days = _setup(conn, 12, "300")
    incremental = [_bar(d, "100") for d in days[-3:]]
    full_history = [_bar(d, "100") for d in days]
    _run(conn, [incremental, full_history], fresh_through=days[-1])
    assert _captures(conn) == [(days[-1], Decimal("100"))]


def test_a_failure_after_the_upsert_leaves_no_capture(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    days = _setup(conn, 4, "100")
    with patch(
        "app.services.market_data._compute_and_store_features",
        side_effect=RuntimeError("feature compute blew up after the upsert"),
    ):
        _run(conn, [[_bar(days[-2], "101"), _bar(days[-1], "102")]], fresh_through=days[-1])
    assert _captures(conn) == []


def test_update_is_refused(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    days = _setup(conn, 4, "100")
    _run(conn, [[_bar(days[-1], "102")]], fresh_through=days[-1])
    with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
        conn.execute("UPDATE etoro_session_close_captures SET close = 1 WHERE instrument_id = %s", (_IID,))
