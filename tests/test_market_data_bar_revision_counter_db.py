"""Integration test for the #2414 insert-vs-revision split (one DB-tier file
for the genuinely-new SQL mechanism, house rule).

Pins the three-way outcome of ``_upsert_candles``' ``RETURNING (xmax = 0)``:
a NEW bar counts as an insert, a bar whose OHLCV comes back DIFFERENT counts
as a revision, and a bar that comes back IDENTICAL counts as neither because
the ``IS DISTINCT FROM`` guard returns no row at all.

⚠ Why this needs a real DB rather than a mocked cursor: ``xmax`` is a system
column whose value is produced by the ON CONFLICT machinery itself. A mock
returns whatever it was told to, so a mocked version of this test would pass
against an upsert that never distinguished the two paths — which is the exact
defect the counter exists to detect.

⚠ Why the counter matters at all (#2414): ``price_daily`` has no audit column,
so the overwrite and the evidence of it happen in one statement. Before this
split, ``candle_rows_upserted`` reported a new bar and a destroyed one as the
same number, and a revision silently invalidates any ``strategy_signals`` row
written against that bar — 57,139 of them existed when this was written.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

import psycopg
import pytest

from app.providers.market_data import OHLCVBar
from app.services.market_data import (
    MarketRefreshSummary,
    most_recent_trading_day,
    refresh_market_data,
)
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]

pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test DB unavailable",
)

_IID = 920_414


def _weekdays_back(end: date, n: int) -> list[date]:
    """The n weekdays ending at ``end`` (inclusive), oldest-first."""
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


def _seed(conn: psycopg.Connection[tuple], days: list[date], close: str) -> None:
    for d in days:
        conn.execute(
            "INSERT INTO price_daily (instrument_id, price_date, open, high, low, close, volume) "
            "VALUES (%s, %s, %s, %s, %s, %s, 1000) ON CONFLICT DO NOTHING",
            (_IID, d, Decimal(close), Decimal(close), Decimal(close), Decimal(close)),
        )


def _run(conn: psycopg.Connection[tuple], bars: list[OHLCVBar]) -> MarketRefreshSummary:
    provider = MagicMock()
    provider.get_daily_candles.side_effect = [bars]
    return refresh_market_data(
        provider,
        conn,
        instruments=[(_IID, "REVN")],
        lookback_days=1000,
        skip_quotes=True,
    )


def test_new_revised_and_identical_bars_are_counted_separately(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    conn = ebull_test_conn
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
        "VALUES (%s, 'REVN', 'Revision Test Co', TRUE) ON CONFLICT (instrument_id) DO NOTHING",
        (_IID,),
    )

    # Four weekdays ending at the most recent trading day. The last is left
    # unseeded so the freshness skip does not fire and the run enters
    # incremental mode (gap of 1 trading day <= the 3-bar window).
    days = _weekdays_back(most_recent_trading_day(date.today()), 4)
    _seed(conn, days[:-1], "100")

    # The 3-bar incremental window returns:
    #   days[-3] — stored at 100, returned at 100  -> IDENTICAL, counts as neither
    #   days[-2] — stored at 100, returned at 111  -> REVISION
    #   days[-1] — not stored at all               -> INSERT
    summary = _run(
        conn,
        [_bar(days[-3], "100"), _bar(days[-2], "111"), _bar(days[-1], "123")],
    )

    assert summary.candles_failed == 0
    # One overwrite, and it is reported as such rather than folded into the total.
    assert summary.candle_rows_revised == 1
    # The identical bar is in neither count — the IS DISTINCT FROM guard
    # returns no row, so it is not a write at all.
    assert summary.candle_rows_upserted == 2

    # The revision really did land, so the counter is describing a real
    # overwrite and not merely a code path.
    stored = dict(
        conn.execute(
            "SELECT price_date, close FROM price_daily WHERE instrument_id = %s",
            (_IID,),
        ).fetchall()
    )
    assert stored[days[-2]] == Decimal("111")
    assert stored[days[-1]] == Decimal("123")


def test_a_pure_backfill_reports_no_revisions(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The counter must not fire on the ordinary case, or it is noise.

    A revision count that is non-zero on a first-time backfill would make the
    #2414 measurement useless in exactly the situation it is meant to bound —
    a >3-day-stale instrument re-observing ~1000 bars.
    """
    conn = ebull_test_conn
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
        "VALUES (%s, 'REVN', 'Revision Test Co', TRUE) ON CONFLICT (instrument_id) DO NOTHING",
        (_IID,),
    )
    days = _weekdays_back(most_recent_trading_day(date.today()), 4)

    summary = _run(conn, [_bar(d, "100") for d in days])

    assert summary.candles_failed == 0
    assert summary.candle_rows_revised == 0
    assert summary.candle_rows_upserted == len(days)


def test_re_running_identical_bars_reports_neither(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Idempotence is preserved: a repeat of the same data is not a revision.

    Without this, the counter would report the whole re-observed window as
    revised on every stale-instrument refetch and overstate the rate by orders
    of magnitude — the measurement would confirm itself.
    """
    conn = ebull_test_conn
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
        "VALUES (%s, 'REVN', 'Revision Test Co', TRUE) ON CONFLICT (instrument_id) DO NOTHING",
        (_IID,),
    )
    days = _weekdays_back(most_recent_trading_day(date.today()), 4)
    _seed(conn, days, "100")

    # Every bar is already stored at exactly this value, and the last stored
    # date is the most recent trading day — so the freshness skip fires and
    # the provider is never called. Nothing is written either way.
    summary = _run(conn, [_bar(d, "100") for d in days])

    assert summary.candle_rows_revised == 0
    assert summary.candle_rows_upserted == 0
