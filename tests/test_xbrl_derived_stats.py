"""Tests for app.services.xbrl_derived_stats (#432 — yfinance retire)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import psycopg
import pytest

from app.services.xbrl_derived_stats import compute_market_cap, resolve_market_cap_basis
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


def _seed_cik(conn: psycopg.Connection[tuple], *, instrument_id: int, cik: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO external_identifiers (instrument_id, provider, identifier_type, "
            "identifier_value, is_primary) VALUES (%s, 'sec', 'cik', %s, TRUE)",
            (instrument_id, cik),
        )
    conn.commit()


def _seed_class_row(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    cik: str,
    shares: int,
    member: str,
    period_end: date,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO instrument_class_shares_outstanding (
                instrument_id, period_end, shares, class_member, source_cik,
                source_adsh, source_form_type, source_fsds_qtr, source_filed_at,
                resolution_method, parser_version
            ) VALUES (%s, %s, %s, %s, %s, 'acc-x', '10-K', '2025q1', %s, 'curated', 'fsds_class_shares_v1')
            """,
            (instrument_id, period_end, shares, member, cik, period_end),
        )
    conn.commit()


class TestResolveMarketCapBasis:
    """Integration wiring for the per-class total-company cap (#1662). The policy
    branches are table-tested in tests/test_per_class_market_cap.py; here we prove
    the resolver's CIK lookup + curated-FSDS oracle + combined read are wired."""

    def test_single_class_is_not_multiclass(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, symbol="RMB1")
        _seed_shares(ebull_test_conn, instrument_id=iid, val=1_000_000_000)
        _seed_quote(ebull_test_conn, instrument_id=iid, price=10)
        res = resolve_market_cap_basis(ebull_test_conn, instrument_id=iid)
        assert res.basis == "not_multiclass"
        assert res.total is None
        # No per-class float stat for a single-class issuer — market cap already
        # IS the sole class value (#1665).
        assert res.class_market_value is None

    def test_dual_class_total_company_cap(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        # Two siblings sharing a CIK, each with a curated FSDS class row at a fresh
        # instant, a combined us-gaap count near that instant, and quotes.
        pe = date.today().replace(day=1)  # recent → within the 548-day freshness window
        cik = "0009990001"
        a = _seed_instrument(ebull_test_conn, symbol="RMA")
        c = _seed_instrument(ebull_test_conn, symbol="RMC")
        for iid in (a, c):
            _seed_cik(ebull_test_conn, instrument_id=iid, cik=cik)
            # combined all-class us-gaap count (fanned out to both siblings, #1102).
            _seed_shares(ebull_test_conn, instrument_id=iid, val=1_000, period_end=pe)
        _seed_class_row(ebull_test_conn, instrument_id=a, cik=cik, shares=600, member="CommonClassA", period_end=pe)
        _seed_class_row(ebull_test_conn, instrument_id=c, cik=cik, shares=300, member="CapitalClassC", period_end=pe)
        _seed_quote(ebull_test_conn, instrument_id=a, price=10)
        _seed_quote(ebull_test_conn, instrument_id=c, price=20)

        res_a = resolve_market_cap_basis(ebull_test_conn, instrument_id=a)
        res_c = resolve_market_cap_basis(ebull_test_conn, instrument_id=c)
        assert res_a.basis == "total_company" and res_a.total is not None
        # 600*10 + 300*20 + residual(1000-900=100)*10 (largest leg A) = 6000+6000+1000.
        assert res_a.total.value == Decimal("13000")
        assert res_a.total.residual_shares == Decimal("100")
        # Identical regardless of which sibling the page renders.
        assert res_c.basis == "total_company" and res_c.total is not None
        assert res_c.total.value == res_a.total.value
        # #1665: per-class float = the VIEWED sibling's OWN leg (shares × price),
        # a separate stat from the (identical) total — and strictly less than it.
        assert res_a.class_market_value == Decimal("6000")  # A: 600 × 10
        assert res_c.class_market_value == Decimal("6000")  # C: 300 × 20 (per-sibling, not the total)
        cmv_a = res_a.class_market_value
        assert cmv_a is not None and cmv_a < res_a.total.value

    def test_dual_class_unpriced_sibling_suppressed(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        # Curated dual-class but one class has no quote → fail closed (suppress),
        # never fall back to the structurally-wrong combined × this-class price.
        pe = date.today().replace(day=1)
        cik = "0009990002"
        a = _seed_instrument(ebull_test_conn, symbol="RMD")
        c = _seed_instrument(ebull_test_conn, symbol="RME")
        for iid in (a, c):
            _seed_cik(ebull_test_conn, instrument_id=iid, cik=cik)
            _seed_shares(ebull_test_conn, instrument_id=iid, val=1_000, period_end=pe)
        _seed_class_row(ebull_test_conn, instrument_id=a, cik=cik, shares=600, member="CommonClassA", period_end=pe)
        _seed_class_row(ebull_test_conn, instrument_id=c, cik=cik, shares=300, member="CapitalClassC", period_end=pe)
        _seed_quote(ebull_test_conn, instrument_id=a, price=10)  # c has no quote
        res = resolve_market_cap_basis(ebull_test_conn, instrument_id=a)
        assert res.basis == "multiclass_unavailable"
        assert res.total is None
        # No clean total → no per-class float either (#1665).
        assert res.class_market_value is None
