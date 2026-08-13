"""DB integration test for the #1857 price_daily fallback in the
``instrument_valuation`` view (sql/236).

The ``priced`` CTE FULL OUTER JOINs the live-quote snapshot with the latest
strictly-positive ``price_daily`` close, quote-first. One test per contract
leg: daily-close fallback (with the non-positive-close sentinel skipped and
the as-of stamp data-anchored to the close's price_date), quote preference
when both exist, degenerate-quote fallthrough, and absence when neither
source has a price. Seeds via the legacy CTE (``fundamentals_snapshot`` is a
base table; ``financial_periods_ttm`` is a view).
"""

from __future__ import annotations

import datetime

import psycopg
import psycopg.rows
import pytest


@pytest.fixture
def _seed(ebull_test_conn: psycopg.Connection[tuple]) -> psycopg.Connection[tuple]:
    conn = ebull_test_conn
    # 11 = daily-close only; 12 = fresh quote + older daily close; 13 =
    # fundamentals only, no price anywhere; 14 = degenerate quote (NULL
    # last, zero bid/ask) + daily close; 15 = STALE quote + fresher close.
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES "
        "(11,'DLY','Daily Only',TRUE),(12,'QTE','Quote Wins',TRUE),"
        "(13,'NOP','No Price',TRUE),(14,'DGN','Degenerate Quote',TRUE),"
        "(15,'STL','Stale Quote',TRUE)"
    )
    # 11: older close 100, latest positive close 120, then a zero close the
    # day after — the sentinel row must be skipped, not chosen.
    conn.execute(
        "INSERT INTO price_daily (instrument_id, price_date, close) VALUES "
        "(11,'2026-07-01',100),(11,'2026-07-10',120),(11,'2026-07-11',0),"
        "(12,'2026-07-10',140),"
        "(14,'2026-07-10',90),"
        "(15,'2026-07-20',110)"
    )
    conn.execute(
        "INSERT INTO quotes (instrument_id, quoted_at, bid, ask, last, spread_flag) VALUES "
        "(12, now(), 149, 151, 150, FALSE),"
        "(14, now(), 0, 0, NULL, FALSE),"
        "(15, '2026-07-05T14:00:00Z', 99, 101, 100, FALSE)"
    )
    for iid in (11, 12, 13, 14, 15):
        conn.execute(
            "INSERT INTO fundamentals_snapshot "
            "(instrument_id, as_of_date, revenue_ttm, gross_margin, operating_margin, "
            " fcf, cash, debt, net_debt, shares_outstanding, book_value, eps) "
            "VALUES (%s, '2025-01-01', 1e11, 0.5, 0.3, 5e10, 1e10, 2e10, 1e10, 1e10, 30, 6)",
            (iid,),
        )
    conn.commit()
    return conn


def _val(conn: psycopg.Connection[tuple], iid: int) -> dict[str, object] | None:
    cur = conn.cursor(row_factory=psycopg.rows.dict_row)
    cur.execute(
        "SELECT current_price, price_as_of, pe_ratio FROM instrument_valuation WHERE instrument_id = %s",
        (iid,),
    )
    return cur.fetchone()


@pytest.mark.db
def test_daily_close_fallback_latest_positive(_seed: psycopg.Connection[tuple]) -> None:
    row = _val(_seed, 11)
    assert row is not None
    # Latest strictly-positive close (120 @ 07-10); the 07-11 zero close is a
    # sentinel, not a price. As-of is data-anchored to the close's price_date.
    assert row["current_price"] == 120
    as_of = row["price_as_of"]
    assert isinstance(as_of, datetime.datetime)
    assert as_of.date() == datetime.date(2026, 7, 10)
    assert row["pe_ratio"] == 20  # 120 / eps 6


@pytest.mark.db
def test_fresh_quote_preferred_over_older_daily_close(_seed: psycopg.Connection[tuple]) -> None:
    row = _val(_seed, 12)
    assert row is not None
    assert row["current_price"] == 150  # quote (today) beats close (07-10)


@pytest.mark.db
def test_stale_quote_loses_to_fresher_daily_close(_seed: psycopg.Connection[tuple]) -> None:
    row = _val(_seed, 15)
    assert row is not None
    # Quote from 07-05 must not shadow the 07-20 close (the dev quotes
    # snapshot only refreshes for subscribed instruments — recency wins).
    assert row["current_price"] == 110
    as_of = row["price_as_of"]
    assert isinstance(as_of, datetime.datetime)
    assert as_of.date() == datetime.date(2026, 7, 20)


@pytest.mark.db
def test_no_price_anywhere_row_absent(_seed: psycopg.Connection[tuple]) -> None:
    assert _val(_seed, 13) is None  # absence, never a fabricated price


@pytest.mark.db
def test_degenerate_quote_falls_through_to_daily_close(_seed: psycopg.Connection[tuple]) -> None:
    row = _val(_seed, 14)
    assert row is not None
    # NULL last + zero bid/ask derives no quote price → daily close, with the
    # stamp paired to the price actually used (price_date, not quoted_at).
    assert row["current_price"] == 90
    as_of = row["price_as_of"]
    assert isinstance(as_of, datetime.datetime)
    assert as_of.date() == datetime.date(2026, 7, 10)
