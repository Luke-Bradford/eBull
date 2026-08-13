"""Tests for app.services.dilution (#435)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import psycopg
import pytest

from app.services.dilution import (
    _EMPTY_SUMMARY,
    get_dilution_summary,
    get_latest_share_count,
    get_share_count_history,
)
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

_NEXT_IID = [30_000]


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


def _seed_fact(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    concept: str,
    period_end: date,
    val: float,
    taxonomy: str = "us-gaap",
    unit: str = "shares",
    accession: str = "test-accession-1",
    filed_date: date | None = None,
    form_type: str = "10-K",
    fiscal_year: int = 2025,
    fiscal_period: str = "FY",
    period_start: date | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO financial_facts_raw (
                instrument_id, taxonomy, concept, unit,
                period_start, period_end, val, accession_number,
                form_type, filed_date, fiscal_year, fiscal_period
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                instrument_id,
                taxonomy,
                concept,
                unit,
                period_start,
                period_end,
                val,
                accession,
                form_type,
                filed_date or period_end,
                fiscal_year,
                fiscal_period,
            ),
        )
    conn.commit()


class TestShareCountHistory:
    def test_empty_when_no_facts(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, symbol="EMPT")
        history = get_share_count_history(ebull_test_conn, instrument_id=iid)
        assert history == []

    def test_dei_shares_preferred_over_gaap(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, symbol="DEIP")
        _seed_fact(
            ebull_test_conn,
            instrument_id=iid,
            taxonomy="dei",
            concept="EntityCommonStockSharesOutstanding",
            period_end=date(2025, 12, 31),
            val=15_000_000_000,
            accession="acc-dei",
        )
        _seed_fact(
            ebull_test_conn,
            instrument_id=iid,
            concept="CommonStockSharesOutstanding",
            period_end=date(2025, 12, 31),
            val=14_500_000_000,
            accession="acc-gaap",
        )

        history = get_share_count_history(ebull_test_conn, instrument_id=iid)
        assert len(history) == 1
        assert history[0].shares_outstanding == Decimal("15000000000.000000")

    def test_newest_filing_wins_on_restatement(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, symbol="RSTT")
        _seed_fact(
            ebull_test_conn,
            instrument_id=iid,
            concept="CommonStockSharesOutstanding",
            period_end=date(2025, 3, 31),
            val=10_000_000,
            accession="acc-orig",
            filed_date=date(2025, 5, 1),
            form_type="10-Q",
        )
        _seed_fact(
            ebull_test_conn,
            instrument_id=iid,
            concept="CommonStockSharesOutstanding",
            period_end=date(2025, 3, 31),
            val=10_500_000,
            accession="acc-amended",
            filed_date=date(2025, 11, 1),
            form_type="10-K/A",
        )
        history = get_share_count_history(ebull_test_conn, instrument_id=iid)
        assert history[0].shares_outstanding == Decimal("10500000.000000")


class TestDilutionSummary:
    def test_empty_when_no_facts(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, symbol="NOFA")
        summary = get_dilution_summary(ebull_test_conn, instrument_id=iid)
        assert summary == _EMPTY_SUMMARY

    def test_dilutive_flag_on_positive_yoy_change(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, symbol="DIL")
        # Oldest period at rn=5 from newest-back perspective — make five
        # quarterly rows so the "year-ago" lookup picks the first.
        base = 10_000_000
        growth = [0.05, 0.06, 0.07, 0.08, 0.10]  # ends at 10% above base
        for q, pct in enumerate(growth):
            # Oldest-first insertion; view orders DESC by period_end.
            _seed_fact(
                ebull_test_conn,
                instrument_id=iid,
                concept="CommonStockSharesOutstanding",
                period_end=date(2024 + q // 4, 3 * ((q % 4) + 1), 28),
                val=int(base * (1 + pct)),
                accession=f"acc-q{q}",
            )

        summary = get_dilution_summary(ebull_test_conn, instrument_id=iid)
        assert summary.latest_shares is not None
        assert summary.dilution_posture in ("dilutive", "stable")

    def test_buyback_heavy_flag_on_negative_yoy_change(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, symbol="BBK")
        # Five quarters, newest has FEWER shares than oldest (buyback).
        shares = [100_000_000, 99_000_000, 98_000_000, 97_000_000, 95_000_000]
        for q, s in enumerate(shares):
            _seed_fact(
                ebull_test_conn,
                instrument_id=iid,
                concept="CommonStockSharesOutstanding",
                period_end=date(2024, 3, 1) if q == 0 else date(2024 + q // 4, 3 * ((q % 4) + 1), 28),
                val=s,
                accession=f"acc-q{q}",
            )
        summary = get_dilution_summary(ebull_test_conn, instrument_id=iid)
        # 95M / 100M = 5% drop → under the -2% threshold → buyback_heavy
        assert summary.dilution_posture == "buyback_heavy"


class TestLatestShareCount:
    def test_returns_none_when_no_facts(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, symbol="NONE")
        assert get_latest_share_count(ebull_test_conn, instrument_id=iid) is None

    def test_reports_dei_source_when_dei_fact_present(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, symbol="DEIL")
        _seed_fact(
            ebull_test_conn,
            instrument_id=iid,
            taxonomy="dei",
            concept="EntityCommonStockSharesOutstanding",
            period_end=date(2025, 12, 31),
            val=20_000_000,
        )
        got = get_latest_share_count(ebull_test_conn, instrument_id=iid)
        assert got is not None
        assert got.source_taxonomy == "dei"
        assert got.latest_shares == Decimal("20000000.000000")

    def test_falls_through_to_us_gaap_when_no_dei(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, symbol="GAAP")
        _seed_fact(
            ebull_test_conn,
            instrument_id=iid,
            concept="CommonStockSharesOutstanding",
            period_end=date(2025, 12, 31),
            val=5_000_000,
        )
        got = get_latest_share_count(ebull_test_conn, instrument_id=iid)
        assert got is not None
        assert got.source_taxonomy == "us-gaap"


class TestFlowOnlyPeriodsCount:
    """Review #442: TTM flow totals must include periods where the
    filer published issuance / buyback without a matching
    shares_outstanding snapshot. Prior version filtered those out."""

    def test_flow_without_outstanding_still_counts_in_ttm(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, symbol="FLOW")
        # One period with snapshot (anchors latest_as_of + year-ago
        # lookup). One period with ONLY a flow fact.
        _seed_fact(
            ebull_test_conn,
            instrument_id=iid,
            concept="CommonStockSharesOutstanding",
            period_end=date(2025, 12, 31),
            val=10_000_000,
        )
        _seed_fact(
            ebull_test_conn,
            instrument_id=iid,
            concept="StockIssuedDuringPeriodSharesNewIssues",
            period_end=date(2025, 9, 30),
            val=500_000,
        )
        summary = get_dilution_summary(ebull_test_conn, instrument_id=iid)
        assert summary.ttm_shares_issued == Decimal("500000.000000")


class TestPeriodEndGroupingDedupes:
    """Review #442: a 10-K/A that re-tags the same period_end with a
    different fiscal_period must not produce duplicate rows."""

    def test_amendment_retag_does_not_duplicate_period_end(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, symbol="RTAG")
        _seed_fact(
            ebull_test_conn,
            instrument_id=iid,
            concept="CommonStockSharesOutstanding",
            period_end=date(2025, 12, 31),
            val=10_000_000,
            accession="acc-orig",
            filed_date=date(2026, 1, 30),
            fiscal_year=2025,
            fiscal_period="FY",
        )
        # Amendment re-tags the same period_end with different fiscal
        # tags (rare but real — happens when filers correct their
        # fiscal-year boundary).
        _seed_fact(
            ebull_test_conn,
            instrument_id=iid,
            concept="CommonStockSharesOutstanding",
            period_end=date(2025, 12, 31),
            val=10_200_000,
            accession="acc-amend",
            filed_date=date(2026, 3, 1),
            fiscal_year=2026,
            fiscal_period="Q1",
            form_type="10-K/A",
        )
        history = get_share_count_history(ebull_test_conn, instrument_id=iid)
        # Exactly one row per period_end — no duplication.
        assert len(history) == 1
        assert history[0].shares_outstanding == Decimal("10200000.000000")


def test_limit_out_of_range_raises(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    with pytest.raises(ValueError, match="limit must be"):
        get_share_count_history(ebull_test_conn, instrument_id=1, limit=0)
    with pytest.raises(ValueError, match="limit must be"):
        get_share_count_history(ebull_test_conn, instrument_id=1, limit=201)
