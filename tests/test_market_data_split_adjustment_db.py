"""Integration test for the #2066 split-cliff heal (one DB-tier file for
the genuinely-new SQL mechanism, house rule).

Pins the end-to-end flow: incremental fetch whose overlap rows come back
on a re-based (post-split) close → adjustment event detected against the
stored closes → in-run full-history re-fetch → idempotent upsert rewrites
the whole stored series onto the new basis, healing the cliff same-day.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

import psycopg
import pytest

from app.providers.market_data import OHLCVBar
from app.services.market_data import _most_recent_trading_day, refresh_market_data
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]

pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test DB unavailable",
)

_IID = 920_066


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


def test_split_overlap_mismatch_triggers_full_refetch_and_heals_series(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    conn = ebull_test_conn
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
        "VALUES (%s, 'SPLT', 'Split Test Co', TRUE) ON CONFLICT (instrument_id) DO NOTHING",
        (_IID,),
    )

    # 11 weekdays ending at the most recent trading day; the LAST one is
    # deliberately NOT seeded so the freshness skip does not fire and the
    # run enters incremental mode (gap of 1 trading day <= 3-bar window).
    days = _weekdays_back(_most_recent_trading_day(date.today()), 11)
    history, fetch_day = days[:-1], days[-1]
    for d in history:
        conn.execute(
            "INSERT INTO price_daily (instrument_id, price_date, open, high, low, close, volume) "
            "VALUES (%s, %s, 100, 100, 100, 100, 1000) ON CONFLICT DO NOTHING",
            (_IID, d),
        )

    # 2:1 split effective on fetch_day: the provider back-adjusts, so the
    # incremental window (last 3 trading days) comes back at close=50 while
    # the two overlap dates are stored at close=100.
    incremental_bars = [_bar(d, "50") for d in days[-3:]]
    full_history_bars = [_bar(d, "50") for d in days]
    provider = MagicMock()
    provider.get_daily_candles.side_effect = [incremental_bars, full_history_bars]

    summary = refresh_market_data(
        provider,
        conn,
        instruments=[(_IID, "SPLT")],
        lookback_days=1000,
        skip_quotes=True,
    )

    assert summary.adjustment_refetches == 1
    assert summary.candles_failed == 0
    # Second provider call is the full-lookback heal.
    assert provider.get_daily_candles.call_count == 2
    assert provider.get_daily_candles.call_args_list[1].args == (_IID, 1000)
    # Every stored row — including the pre-buffer history that an
    # incremental fetch alone would never touch — is on the new basis.
    rows = conn.execute(
        "SELECT close FROM price_daily WHERE instrument_id = %s ORDER BY price_date",
        (_IID,),
    ).fetchall()
    assert len(rows) == len(days)
    assert all(row[0] == Decimal("50") for row in rows)
