"""Tests for app.services.dividends against the ebull_test DB.

Views (sql/050_dividend_history_views.sql) are SQL-only, so the
service tests run against the real schema to pin the view contract.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import psycopg
import pytest

from app.services.dividends import (
    _EMPTY_SUMMARY,
    get_dividend_history,
    get_dividend_summary,
    get_upcoming_dividends,
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


_NEXT_IID = [10_000]


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


def _seed_quote(conn: psycopg.Connection[tuple], *, instrument_id: int, price: float) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO quotes (instrument_id, quoted_at, bid, ask, last)
            VALUES (%s, NOW(), %s, %s, %s)
            """,
            (instrument_id, price - 0.01, price + 0.01, price),
        )
    conn.commit()


def _seed_period(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    period_end: date,
    period_type: str,
    fiscal_year: int,
    fiscal_quarter: int | None,
    dps_declared: float | None = None,
    dividends_paid: float | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO financial_periods (
                instrument_id, period_end_date, period_type, fiscal_year,
                fiscal_quarter, dps_declared, dividends_paid,
                reported_currency, source, source_ref
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'USD', 'sec', 'test')
            """,
            (instrument_id, period_end, period_type, fiscal_year, fiscal_quarter, dps_declared, dividends_paid),
        )
    conn.commit()


class TestGetDividendSummary:
    def test_never_paid_returns_empty_shape(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, symbol="NEVR")
        summary = get_dividend_summary(ebull_test_conn, instrument_id=iid)
        assert summary == _EMPTY_SUMMARY

    def test_ttm_sums_last_four_quarters(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, symbol="TTMQ")
        _seed_quote(ebull_test_conn, instrument_id=iid, price=100.0)
        # Four $0.50 quarterly dividends → TTM $2.00, yield 2.0%
        for q, m in enumerate([3, 6, 9, 12], start=1):
            _seed_period(
                ebull_test_conn,
                instrument_id=iid,
                period_end=date(2025, m, 28),
                period_type=f"Q{q}",
                fiscal_year=2025,
                fiscal_quarter=q,
                dps_declared=0.50,
                dividends_paid=50_000_000,
            )
        # Older quarter OUTSIDE the last-4 window — must not contribute.
        _seed_period(
            ebull_test_conn,
            instrument_id=iid,
            period_end=date(2024, 12, 28),
            period_type="Q4",
            fiscal_year=2024,
            fiscal_quarter=4,
            dps_declared=99.0,
            dividends_paid=999_000_000,
        )

        summary = get_dividend_summary(ebull_test_conn, instrument_id=iid)
        assert summary.has_dividend is True
        assert summary.ttm_dps == Decimal("2.0000")
        assert summary.ttm_yield_pct == Decimal("2.00000000")
        # Streak walks the full quarterly history, not just the TTM
        # window — 4 quarters of 2025 + the 2024-Q4 row that is outside
        # TTM but still a non-zero paying quarter = 5 consecutive.
        assert summary.dividend_streak_q == 5
        assert summary.latest_dividend_at == date(2025, 12, 28)

    def test_streak_breaks_on_zero_quarter(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, symbol="BRKS")
        # Newest → oldest: $0.50, $0.50, 0 (streak break), $0.50
        for q, m, amt in [
            (4, 12, 0.50),
            (3, 9, 0.50),
            (2, 6, 0.0),
            (1, 3, 0.50),
        ]:
            _seed_period(
                ebull_test_conn,
                instrument_id=iid,
                period_end=date(2025, m, 28),
                period_type=f"Q{q}",
                fiscal_year=2025,
                fiscal_quarter=q,
                dps_declared=amt,
            )

        summary = get_dividend_summary(ebull_test_conn, instrument_id=iid)
        # Streak walks newest-back: $0.50, $0.50, then 0 breaks. Streak = 2.
        assert summary.dividend_streak_q == 2
        # latest_dps must NOT be the zero row — pin that the resolver
        # skips non-paying quarters (Codex review #PR_A).
        assert summary.latest_dps == Decimal("0.5000")
        assert summary.latest_dividend_at == date(2025, 12, 28)

    def test_zero_quarter_excluded_from_history(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        # dividend_history is "paying periods only" — a skipped-dividend
        # quarter (dps=0) must not appear on the chart, even though the
        # row is live in financial_periods.
        iid = _seed_instrument(ebull_test_conn, symbol="ZEROH")
        for q, m, amt in [(1, 3, 0.50), (2, 6, 0.0), (3, 9, 0.50)]:
            _seed_period(
                ebull_test_conn,
                instrument_id=iid,
                period_end=date(2025, m, 28),
                period_type=f"Q{q}",
                fiscal_year=2025,
                fiscal_quarter=q,
                dps_declared=amt,
            )
        history = get_dividend_history(ebull_test_conn, instrument_id=iid)
        assert [p.period_end_date for p in history] == [date(2025, 9, 28), date(2025, 3, 28)]
        assert all(p.dps_declared is not None and p.dps_declared > 0 for p in history)

    def test_superseded_row_excluded(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        # Mirrors the live-row guard used throughout sql/032 — a restated
        # or withdrawn period cannot leak into either the has_dividend
        # filter or the chart.
        iid = _seed_instrument(ebull_test_conn, symbol="SUPR")
        _seed_period(
            ebull_test_conn,
            instrument_id=iid,
            period_end=date(2025, 12, 28),
            period_type="Q4",
            fiscal_year=2025,
            fiscal_quarter=4,
            dps_declared=0.50,
        )
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "UPDATE financial_periods SET superseded_at = NOW() WHERE instrument_id = %s",
                (iid,),
            )
        ebull_test_conn.commit()

        summary = get_dividend_summary(ebull_test_conn, instrument_id=iid)
        assert summary.has_dividend is False
        history = get_dividend_history(ebull_test_conn, instrument_id=iid)
        assert history == []

    def test_fy_row_does_not_duplicate_quarter_in_history(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        # Some issuers report both FY and Q4 with the same period_end_date.
        # history surfaces QUARTERLY only so the chart doesn't double-render
        # the fiscal year's dividend (FY dps = Q1+Q2+Q3+Q4 aggregate).
        iid = _seed_instrument(ebull_test_conn, symbol="FYMIX")
        _seed_period(
            ebull_test_conn,
            instrument_id=iid,
            period_end=date(2025, 12, 31),
            period_type="Q4",
            fiscal_year=2025,
            fiscal_quarter=4,
            dps_declared=0.25,
        )
        _seed_period(
            ebull_test_conn,
            instrument_id=iid,
            period_end=date(2025, 12, 31),
            period_type="FY",
            fiscal_year=2025,
            fiscal_quarter=None,
            dps_declared=1.00,
        )
        history = get_dividend_history(ebull_test_conn, instrument_id=iid)
        assert len(history) == 1
        assert history[0].period_type == "Q4"

        # Summary's latest resolver must tie-break Q4 over FY deterministically.
        summary = get_dividend_summary(ebull_test_conn, instrument_id=iid)
        assert summary.latest_dps == Decimal("0.2500")

    def test_aggregate_only_payer_still_flags_has_dividend(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        # Some filers publish only dividends_paid (aggregate) without a
        # per-share figure — still a dividend payer.
        iid = _seed_instrument(ebull_test_conn, symbol="AGGR")
        _seed_period(
            ebull_test_conn,
            instrument_id=iid,
            period_end=date(2025, 12, 28),
            period_type="Q4",
            fiscal_year=2025,
            fiscal_quarter=4,
            dps_declared=None,
            dividends_paid=10_000_000,
        )
        summary = get_dividend_summary(ebull_test_conn, instrument_id=iid)
        assert summary.has_dividend is True
        assert summary.ttm_dps is None


class TestGetDividendHistory:
    def test_newest_first_ordering(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, symbol="ORDR")
        for q, m in [(1, 3), (2, 6), (3, 9), (4, 12)]:
            _seed_period(
                ebull_test_conn,
                instrument_id=iid,
                period_end=date(2025, m, 28),
                period_type=f"Q{q}",
                fiscal_year=2025,
                fiscal_quarter=q,
                dps_declared=0.25,
            )
        history = get_dividend_history(ebull_test_conn, instrument_id=iid)
        assert len(history) == 4
        assert [p.period_end_date for p in history] == [
            date(2025, 12, 28),
            date(2025, 9, 28),
            date(2025, 6, 28),
            date(2025, 3, 28),
        ]

    def test_excludes_periods_without_dividend(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, symbol="EXCL")
        _seed_period(
            ebull_test_conn,
            instrument_id=iid,
            period_end=date(2025, 3, 28),
            period_type="Q1",
            fiscal_year=2025,
            fiscal_quarter=1,
            dps_declared=0.25,
        )
        # NULL dividend period — should be filtered out.
        _seed_period(
            ebull_test_conn,
            instrument_id=iid,
            period_end=date(2025, 6, 28),
            period_type="Q2",
            fiscal_year=2025,
            fiscal_quarter=2,
            dps_declared=None,
            dividends_paid=None,
        )
        history = get_dividend_history(ebull_test_conn, instrument_id=iid)
        assert len(history) == 1
        assert history[0].period_end_date == date(2025, 3, 28)

    def test_limit_out_of_range_raises(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        with pytest.raises(ValueError, match="limit must be"):
            get_dividend_history(ebull_test_conn, instrument_id=1, limit=0)
        with pytest.raises(ValueError, match="limit must be"):
            get_dividend_history(ebull_test_conn, instrument_id=1, limit=401)


def _seed_dividend_event(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    source_accession: str,
    declaration_date: date | None = None,
    ex_date: date | None = None,
    record_date: date | None = None,
    pay_date: date | None = None,
    dps_declared: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dividend_events
                (instrument_id, source_accession, declaration_date,
                 ex_date, record_date, pay_date, dps_declared, currency)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'USD')
            """,
            (
                instrument_id,
                source_accession,
                declaration_date,
                ex_date,
                record_date,
                pay_date,
                dps_declared,
            ),
        )
    conn.commit()


class TestGetUpcomingDividends:
    """Covers the three forward-looking row shapes (Codex M3 fix)."""

    def test_ex_date_future_surfaces(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, symbol="KO")
        _seed_dividend_event(
            ebull_test_conn,
            instrument_id=iid,
            source_accession="A1",
            ex_date=date(2030, 3, 15),
            pay_date=date(2030, 4, 1),
            dps_declared="0.485",
        )
        rows = get_upcoming_dividends(
            ebull_test_conn,
            instrument_id=iid,
            reference_date=date(2030, 1, 1),
        )
        assert len(rows) == 1
        assert rows[0].ex_date == date(2030, 3, 15)

    def test_ex_date_past_filtered(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, symbol="KO")
        _seed_dividend_event(
            ebull_test_conn,
            instrument_id=iid,
            source_accession="A1",
            ex_date=date(2020, 3, 15),
            pay_date=date(2020, 4, 1),
        )
        rows = get_upcoming_dividends(
            ebull_test_conn,
            instrument_id=iid,
            reference_date=date(2030, 1, 1),
        )
        assert rows == []

    def test_pay_date_only_surfaces(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Announcement parsed pay-date but not ex-date (Codex M3)."""
        iid = _seed_instrument(ebull_test_conn, symbol="KO")
        _seed_dividend_event(
            ebull_test_conn,
            instrument_id=iid,
            source_accession="A1",
            pay_date=date(2030, 4, 1),
            dps_declared="0.485",
        )
        rows = get_upcoming_dividends(
            ebull_test_conn,
            instrument_id=iid,
            reference_date=date(2030, 1, 1),
        )
        assert len(rows) == 1
        assert rows[0].ex_date is None
        assert rows[0].pay_date == date(2030, 4, 1)

    def test_declaration_only_surfaces_within_lookback(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Codex M3 regression — row with ONLY a recent declaration
        date + amount (no ex/pay) must surface so the 'Declared …
        (calendar TBD)' UI branch is reachable."""
        iid = _seed_instrument(ebull_test_conn, symbol="KO")
        _seed_dividend_event(
            ebull_test_conn,
            instrument_id=iid,
            source_accession="A1",
            declaration_date=date(2030, 1, 15),
            dps_declared="0.485",
        )
        rows = get_upcoming_dividends(
            ebull_test_conn,
            instrument_id=iid,
            reference_date=date(2030, 2, 1),
        )
        assert len(rows) == 1
        assert rows[0].ex_date is None
        assert rows[0].pay_date is None
        assert rows[0].declaration_date == date(2030, 1, 15)

    def test_declaration_only_past_lookback_filtered(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Stale declaration-only rows drop off after the 90-day
        lookback so they don't linger forever on the banner."""
        iid = _seed_instrument(ebull_test_conn, symbol="KO")
        _seed_dividend_event(
            ebull_test_conn,
            instrument_id=iid,
            source_accession="A1",
            declaration_date=date(2029, 1, 1),
            dps_declared="0.485",
        )
        # Reference date far past the 90-day window.
        rows = get_upcoming_dividends(
            ebull_test_conn,
            instrument_id=iid,
            reference_date=date(2030, 6, 1),
        )
        assert rows == []

    def test_ordering_by_earliest_date(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """First row is always the earliest future event regardless of
        which date dimension (ex/pay/declaration) won the COALESCE."""
        iid = _seed_instrument(ebull_test_conn, symbol="KO")
        _seed_dividend_event(
            ebull_test_conn,
            instrument_id=iid,
            source_accession="A_LATE",
            ex_date=date(2030, 6, 10),
        )
        _seed_dividend_event(
            ebull_test_conn,
            instrument_id=iid,
            source_accession="A_EARLY",
            ex_date=date(2030, 3, 1),
        )
        rows = get_upcoming_dividends(
            ebull_test_conn,
            instrument_id=iid,
            reference_date=date(2030, 1, 1),
        )
        assert [r.source_accession for r in rows] == ["A_EARLY", "A_LATE"]
