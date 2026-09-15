"""#2414 item 2 — `price_daily_backdated_insert` against a real cluster.

The pure half (``tests/test_2414_backdated_insert.py``) pins the frontier POLICY.
This file pins the three things only a real DB can establish:

  * a genuine interior gap-fill produces a row carrying the frontier the writer
    observed, while the same payload's forward extension produces none;
  * the row rolls back with the bar write, which is the property the whole
    in-transaction placement rests on;
  * ``price_daily_corpus_mutation`` returns both classes from one fetch.

⚠ ONE file, three tests — per the repo's test-tiering rule (one integration test
per genuinely-new SQL mechanism, not a file per code path).
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

_IID = 920_416


def _weekdays_back(end: date, n: int) -> list[date]:
    out: list[date] = []
    d = end
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d -= timedelta(days=1)
    return list(reversed(out))


def _bar(price_date: date, close: str = "100") -> OHLCVBar:
    c = Decimal(close)
    return OHLCVBar(price_date=price_date, open=c, high=c, low=c, close=c, volume=1000)


def _instrument(conn: psycopg.Connection[tuple]) -> None:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
        "VALUES (%s, 'BKDATED', 'Backdated Insert Co', TRUE) ON CONFLICT (instrument_id) DO NOTHING",
        (_IID,),
    )


def _seed(conn: psycopg.Connection[tuple], days: list[date], close: str = "100") -> None:
    for d in days:
        c = Decimal(close)
        conn.execute(
            "INSERT INTO price_daily (instrument_id, price_date, open, high, low, close, volume) "
            "VALUES (%s, %s, %s, %s, %s, %s, 1000) ON CONFLICT DO NOTHING",
            (_IID, d, c, c, c, c),
        )


def _backdated(conn: psycopg.Connection[tuple]) -> list[tuple[date, date, str]]:
    return [
        (r[0], r[1], r[2])
        for r in conn.execute(
            "SELECT price_date, frontier_before, cause FROM price_daily_backdated_insert "
            "WHERE instrument_id = %s ORDER BY insert_id",
            (_IID,),
        ).fetchall()
    ]


def _run(conn: psycopg.Connection[tuple], bars: list[OHLCVBar]) -> None:
    provider = MagicMock()
    provider.get_daily_candles.return_value = bars
    refresh_market_data(
        provider,
        conn,
        instruments=[(_IID, "BKDATED")],
        lookback_days=1000,
        skip_quotes=True,
    )


def test_a_gap_fill_is_recorded_and_a_forward_extension_is_not(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The class this table exists for, and its boundary in the SAME payload.

    Stored: d0, d2, d3 — d1 missing, which is the interior gap
    ``_candles_fetch_count``'s ``stale_reobservation`` branch exists to close.
    The payload re-observes all five, so d1 is inserted BEHIND the frontier (d3)
    and d4 is inserted in front of it. Only d1 is a corpus movement.
    """
    conn = ebull_test_conn
    _instrument(conn)
    days = _weekdays_back(most_recent_trading_day(date.today()), 5)
    _seed(conn, [days[0], days[2], days[3]])

    _run(conn, [_bar(d) for d in days])

    assert _backdated(conn) == [(days[1], days[3], "incremental")]
    # And the forward extension really did land — otherwise this test would pass
    # by the payload never having been written at all.
    stored = {
        r[0] for r in conn.execute("SELECT price_date FROM price_daily WHERE instrument_id = %s", (_IID,)).fetchall()
    }
    assert days[4] in stored and days[1] in stored


def test_a_failure_after_the_upsert_leaves_no_backdated_row_and_no_bar(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Why the write is inside the bar write's transaction.

    An audit row that outlived a rolled-back write would assert a mutation
    ``price_daily`` has no record of — and unlike a revision, there is not even a
    surviving row to contradict it, because the bar itself is gone.
    """
    conn = ebull_test_conn
    _instrument(conn)
    days = _weekdays_back(most_recent_trading_day(date.today()), 5)
    _seed(conn, [days[0], days[2], days[3]])

    with patch(
        "app.services.market_data._compute_and_store_features",
        side_effect=RuntimeError("feature compute blew up after the upsert"),
    ):
        _run(conn, [_bar(d) for d in days])

    assert _backdated(conn) == []
    stored = {
        r[0] for r in conn.execute("SELECT price_date FROM price_daily WHERE instrument_id = %s", (_IID,)).fetchall()
    }
    assert days[1] not in stored, "the bar rolled back but the audit row would have claimed it"


def test_the_corpus_mutation_view_returns_both_classes_from_one_fetch(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The completeness surface, exercised on a payload that produces both kinds.

    A consumer asking "did the corpus under this decision move?" that reads only
    ``price_daily_revision`` misses d1 entirely. The view is what makes that
    omission impossible to write by accident.

    ⚠ d2's close moves 100 -> 111 (ratio 1.11), deliberately inside
    ``_ADJUSTMENT_RATIO_THRESHOLD`` (1.2) so this exercises the ordinary
    incremental branch rather than the heal.
    """
    conn = ebull_test_conn
    _instrument(conn)
    days = _weekdays_back(most_recent_trading_day(date.today()), 5)
    _seed(conn, [days[0], days[2], days[3]])

    _run(
        conn,
        [
            _bar(days[0]),  # identical — no-op
            _bar(days[1]),  # INSERT below the frontier
            _bar(days[2], "111"),  # REVISION
            _bar(days[3]),  # identical — no-op
            _bar(days[4]),  # insert above the frontier
        ],
    )

    rows = sorted(
        (r[0], r[1], r[2])
        for r in conn.execute(
            "SELECT mutation_kind, price_date, frontier_before FROM price_daily_corpus_mutation "
            "WHERE instrument_id = %s",
            (_IID,),
        ).fetchall()
    )
    assert rows == [
        ("backdated_insert", days[1], days[3]),
        ("revision", days[2], None),
    ]
