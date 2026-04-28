"""Regression tests for #624 — canonical merge must converge regardless
of arrival order.

The previous canonical merge keyed on ``(period_end_date, period_type)``,
so a late-arriving amendment that reported the same fiscal label
under a different ``period_end_date`` than the row already on file
inserted a NEW canonical row instead of replacing the original. The
fix re-keys the merge on ``(fiscal_year, fiscal_quarter, period_type)``
plus a per-merge cleanup of stale period_end siblings.

Tests seed the raw table with two rows (original + amendment) per
fiscal label in both orders and assert ``_canonical_merge_instrument``
ends with exactly one canonical row carrying the amendment's data.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import psycopg
import psycopg.rows
import pytest

from app.services.fundamentals import _canonical_merge_instrument
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


def _seed_instrument(conn: psycopg.Connection[tuple], iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} test"),
    )


def _seed_raw(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    period_end: date,
    period_type: str,
    fiscal_year: int,
    fiscal_quarter: int | None,
    source_ref: str,
    filed_date: date,
    revenue: Decimal,
    source: str = "sec_edgar",
) -> None:
    conn.execute(
        """
        INSERT INTO financial_periods_raw (
            instrument_id, period_end_date, period_type,
            fiscal_year, fiscal_quarter, revenue,
            source, source_ref, reported_currency, filed_date
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, 'USD', %s
        )
        """,
        (instrument_id, period_end, period_type, fiscal_year, fiscal_quarter, revenue, source, source_ref, filed_date),
    )


def _canonical_rows(
    conn: psycopg.Connection[tuple],
    instrument_id: int,
) -> list[dict[str, object]]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT period_end_date, period_type, fiscal_year, fiscal_quarter,
                   revenue, source_ref, filed_date
            FROM financial_periods
            WHERE instrument_id = %s
            ORDER BY filed_date DESC NULLS LAST, period_end_date DESC
            """,
            (instrument_id,),
        )
        return cur.fetchall()


class TestCanonicalMergeArrivalOrder:
    def test_amendment_after_original(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Original 10-K (period_end=2023-12-31) ingested first, then a
        10-K/A amendment with a different period_end (2024-01-15) and
        a later filed_date. Canonical must converge on the amendment.
        """
        conn = ebull_test_conn
        _seed_instrument(conn, iid=200, symbol="ORIG")

        # Phase 1: original arrives.
        _seed_raw(
            conn,
            instrument_id=200,
            period_end=date(2023, 12, 31),
            period_type="FY",
            fiscal_year=2023,
            fiscal_quarter=None,
            source_ref="acc-original",
            filed_date=date(2024, 2, 23),
            revenue=Decimal("1000"),
        )
        conn.commit()
        _canonical_merge_instrument(conn, 200)
        conn.commit()
        rows = _canonical_rows(conn, 200)
        assert len(rows) == 1
        assert rows[0]["source_ref"] == "acc-original"
        assert rows[0]["revenue"] == Decimal("1000")

        # Phase 2: amendment arrives with different period_end.
        _seed_raw(
            conn,
            instrument_id=200,
            period_end=date(2024, 1, 15),
            period_type="FY",
            fiscal_year=2023,
            fiscal_quarter=None,
            source_ref="acc-amendment",
            filed_date=date(2024, 6, 1),
            revenue=Decimal("1100"),
        )
        conn.commit()
        _canonical_merge_instrument(conn, 200)
        conn.commit()
        rows = _canonical_rows(conn, 200)
        assert len(rows) == 1, [dict(r) for r in rows]
        assert rows[0]["source_ref"] == "acc-amendment"
        assert rows[0]["revenue"] == Decimal("1100")
        # Stale original row deleted, not left behind.
        assert rows[0]["period_end_date"] == date(2024, 1, 15)

    def test_amendment_arrives_first_then_original_does_not_overwrite(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Reverse order: amendment lands first (later filed_date,
        different period_end). Then the *original* 10-K's raw row is
        ingested late (out-of-order replay). The canonical row must
        still reflect the amendment — the older original cannot
        overwrite a newer amendment.
        """
        conn = ebull_test_conn
        _seed_instrument(conn, iid=201, symbol="REV")

        # Amendment first (later filed_date).
        _seed_raw(
            conn,
            instrument_id=201,
            period_end=date(2024, 1, 15),
            period_type="FY",
            fiscal_year=2023,
            fiscal_quarter=None,
            source_ref="acc-amendment",
            filed_date=date(2024, 6, 1),
            revenue=Decimal("1100"),
        )
        conn.commit()
        _canonical_merge_instrument(conn, 201)
        conn.commit()
        rows = _canonical_rows(conn, 201)
        assert len(rows) == 1
        assert rows[0]["source_ref"] == "acc-amendment"

        # Original 10-K's raw row arrives later (out-of-order replay).
        _seed_raw(
            conn,
            instrument_id=201,
            period_end=date(2023, 12, 31),
            period_type="FY",
            fiscal_year=2023,
            fiscal_quarter=None,
            source_ref="acc-original",
            filed_date=date(2024, 2, 23),
            revenue=Decimal("1000"),
        )
        conn.commit()
        _canonical_merge_instrument(conn, 201)
        conn.commit()
        rows = _canonical_rows(conn, 201)
        assert len(rows) == 1, [dict(r) for r in rows]
        # Amendment still wins — its filed_date is later.
        assert rows[0]["source_ref"] == "acc-amendment"
        assert rows[0]["revenue"] == Decimal("1100")
        assert rows[0]["period_end_date"] == date(2024, 1, 15)

    def test_same_period_end_restatement_in_place_update(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Genuine restatement: amendment files the SAME period_end as
        the original (real fiscal calendar doesn't move). Canonical
        row must update in place — no DELETE + INSERT churn — and
        the latest filed_date wins.
        """
        conn = ebull_test_conn
        _seed_instrument(conn, iid=202, symbol="SAME")

        _seed_raw(
            conn,
            instrument_id=202,
            period_end=date(2024, 6, 30),
            period_type="Q2",
            fiscal_year=2024,
            fiscal_quarter=2,
            source_ref="acc-original",
            filed_date=date(2024, 7, 30),
            revenue=Decimal("500"),
        )
        conn.commit()
        _canonical_merge_instrument(conn, 202)
        conn.commit()

        _seed_raw(
            conn,
            instrument_id=202,
            period_end=date(2024, 6, 30),  # SAME period_end
            period_type="Q2",
            fiscal_year=2024,
            fiscal_quarter=2,
            source_ref="acc-restatement",
            filed_date=date(2024, 9, 12),  # later
            revenue=Decimal("550"),
        )
        conn.commit()
        _canonical_merge_instrument(conn, 202)
        conn.commit()

        rows = _canonical_rows(conn, 202)
        assert len(rows) == 1
        assert rows[0]["source_ref"] == "acc-restatement"
        assert rows[0]["revenue"] == Decimal("550")

    def test_partial_unique_index_blocks_direct_dupe(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Migration 077 partial unique index makes a direct duplicate
        INSERT impossible at the DB layer — even if a future code
        path bypasses ``_canonical_merge_instrument`` and tries to
        insert a second row for the same fiscal label, Postgres
        rejects it.
        """
        conn = ebull_test_conn
        _seed_instrument(conn, iid=203, symbol="UQ")
        conn.execute(
            """
            INSERT INTO financial_periods (
                instrument_id, period_end_date, period_type,
                fiscal_year, fiscal_quarter,
                source, source_ref, reported_currency
            ) VALUES (%s, %s, 'FY', %s, NULL, 'sec_edgar', 'acc-a', 'USD')
            """,
            (203, date(2024, 12, 31), 2024),
        )
        conn.commit()

        # Second insert for the same fiscal label MUST raise UniqueViolation.
        with pytest.raises(psycopg.errors.UniqueViolation):
            conn.execute(
                """
                INSERT INTO financial_periods (
                    instrument_id, period_end_date, period_type,
                    fiscal_year, fiscal_quarter,
                    source, source_ref, reported_currency
                ) VALUES (%s, %s, 'FY', %s, NULL, 'sec_edgar', 'acc-b', 'USD')
                """,
                (203, date(2025, 2, 1), 2024),
            )
        conn.rollback()
