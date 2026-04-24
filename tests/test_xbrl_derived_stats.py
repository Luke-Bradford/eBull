"""Tests for app.services.xbrl_derived_stats (#432 — yfinance retire)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import psycopg
import pytest

from app.services.xbrl_derived_stats import compute_market_cap
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not _test_db_available(),
        reason="ebull_test DB unavailable",
    ),
]

_NEXT_IID = [50_000]


def _seed_instrument(conn: psycopg.Connection[tuple], *, symbol: str) -> int:
    _NEXT_IID[0] += 1
    iid = _NEXT_IID[0]
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, currency, is_tradable) "
            "VALUES (%s, %s, %s, 'USD', TRUE)",
            (iid, symbol, f"{symbol} Inc."),
        )
    conn.commit()
    return iid


def _seed_shares(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    val: float,
    concept: str = "CommonStockSharesOutstanding",
    taxonomy: str = "us-gaap",
    period_end: date = date(2025, 12, 31),
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO financial_facts_raw (
                instrument_id, taxonomy, concept, unit,
                period_end, val, accession_number, form_type, filed_date,
                fiscal_year, fiscal_period
            ) VALUES (%s, %s, %s, 'shares', %s, %s, %s, '10-K', %s, 2025, 'FY')
            """,
            (
                instrument_id,
                taxonomy,
                concept,
                period_end,
                val,
                f"acc-{instrument_id}",
                period_end,
            ),
        )
    conn.commit()


def _seed_quote(conn: psycopg.Connection[tuple], *, instrument_id: int, price: float) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO quotes (instrument_id, quoted_at, bid, ask, last)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (instrument_id, datetime.now(UTC), price - 0.1, price + 0.1, price),
        )
    conn.commit()


class TestComputeMarketCap:
    def test_dei_shares_and_last_price_yield_product(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, symbol="MC1")
        _seed_shares(
            ebull_test_conn,
            instrument_id=iid,
            val=15_000_000_000,
            taxonomy="dei",
            concept="EntityCommonStockSharesOutstanding",
        )
        _seed_quote(ebull_test_conn, instrument_id=iid, price=200)

        got = compute_market_cap(ebull_test_conn, instrument_id=iid)
        assert got is not None
        assert got.value == Decimal("3000000000000.000000")
        assert got.shares_source == "dei"

    def test_us_gaap_fallback(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, symbol="MC2")
        _seed_shares(ebull_test_conn, instrument_id=iid, val=5_000_000)
        _seed_quote(ebull_test_conn, instrument_id=iid, price=50)

        got = compute_market_cap(ebull_test_conn, instrument_id=iid)
        assert got is not None
        assert got.value == Decimal("250000000.000000")
        assert got.shares_source == "us-gaap"

    def test_no_shares_returns_none(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, symbol="MC3")
        _seed_quote(ebull_test_conn, instrument_id=iid, price=50)
        assert compute_market_cap(ebull_test_conn, instrument_id=iid) is None

    def test_no_quote_returns_none(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, symbol="MC4")
        _seed_shares(ebull_test_conn, instrument_id=iid, val=1_000_000)
        assert compute_market_cap(ebull_test_conn, instrument_id=iid) is None

    def test_zero_shares_returns_none(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, symbol="MC5")
        _seed_shares(ebull_test_conn, instrument_id=iid, val=0)
        _seed_quote(ebull_test_conn, instrument_id=iid, price=50)
        assert compute_market_cap(ebull_test_conn, instrument_id=iid) is None

    def test_falls_back_to_bid_ask_midpoint_when_last_missing(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, symbol="MC6")
        _seed_shares(ebull_test_conn, instrument_id=iid, val=10_000_000)
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO quotes (instrument_id, quoted_at, bid, ask, last)
                VALUES (%s, NOW(), 99, 101, NULL)
                """,
                (iid,),
            )
        ebull_test_conn.commit()

        got = compute_market_cap(ebull_test_conn, instrument_id=iid)
        assert got is not None
        # (99 + 101) / 2 = 100
        assert got.price == Decimal("100.0000000000000000000000000000")
        assert got.value == Decimal("1000000000.00000000000000000000000000000000")
