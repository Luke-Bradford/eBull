"""Integration test for #731 — ownership columns project through the
canonical merge ON CONFLICT update branch.

Codex pre-push review flagged that the unit tests in
``test_financial_normalization.py`` exercise ``_derive_periods_from_facts``
in isolation but do not pin the round-trip through
``_canonical_merge_instrument``. The four new columns
(``treasury_shares``, ``shares_authorized``, ``shares_issued``,
``retained_earnings``) appear in both the INSERT column list AND the
ON CONFLICT DO UPDATE SET clause of the merge query — the latter is
what populates a canonical row pre-existing in the table from a
prior (pre-088) ingest. This test seeds a raw row with the four new
columns set, runs the merge, and asserts the canonical row's
ownership values match.

A second pass then updates the raw row's ownership values with new
data (simulating a 10-K/A amendment re-deriving the column) and
re-runs the merge. The canonical row must update in place via the
ON CONFLICT branch — the bug Codex called out would surface here as
stale values surviving the second merge.
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


def _seed_raw_with_ownership(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    period_end: date,
    fiscal_year: int,
    source_ref: str,
    filed_date: date,
    treasury_shares: Decimal,
    shares_authorized: Decimal,
    shares_issued: Decimal,
    retained_earnings: Decimal,
) -> None:
    conn.execute(
        """
        INSERT INTO financial_periods_raw (
            instrument_id, period_end_date, period_type,
            fiscal_year, fiscal_quarter, revenue,
            treasury_shares, shares_authorized, shares_issued, retained_earnings,
            source, source_ref, reported_currency, filed_date
        ) VALUES (
            %s, %s, 'FY', %s, NULL, 1000,
            %s, %s, %s, %s,
            'sec_edgar', %s, 'USD', %s
        )
        """,
        (
            instrument_id,
            period_end,
            fiscal_year,
            treasury_shares,
            shares_authorized,
            shares_issued,
            retained_earnings,
            source_ref,
            filed_date,
        ),
    )


def _canonical_ownership_row(
    conn: psycopg.Connection[tuple],
    instrument_id: int,
) -> dict[str, object]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT period_end_date, treasury_shares, shares_authorized,
                   shares_issued, retained_earnings, source_ref
            FROM financial_periods
            WHERE instrument_id = %s
            """,
            (instrument_id,),
        )
        rows = cur.fetchall()
    assert len(rows) == 1, [dict(r) for r in rows]
    return rows[0]  # type: ignore[return-value]


class TestCanonicalMergeOwnershipColumns:
    def test_insert_path_populates_ownership_columns(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Fresh canonical row receives all four ownership columns from
        the raw row's INSERT path — the simple direction."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=731_001, symbol="OWN1")
        _seed_raw_with_ownership(
            conn,
            instrument_id=731_001,
            period_end=date(2024, 12, 31),
            fiscal_year=2024,
            source_ref="acc-original",
            filed_date=date(2025, 2, 14),
            treasury_shares=Decimal("12500000"),
            shares_authorized=Decimal("5000000000"),
            shares_issued=Decimal("1750000000"),
            retained_earnings=Decimal("180000000000"),
        )
        conn.commit()

        _canonical_merge_instrument(conn, 731_001)
        conn.commit()

        row = _canonical_ownership_row(conn, 731_001)
        assert row["treasury_shares"] == Decimal("12500000")
        assert row["shares_authorized"] == Decimal("5000000000")
        assert row["shares_issued"] == Decimal("1750000000")
        assert row["retained_earnings"] == Decimal("180000000000")

    def test_on_conflict_update_path_replaces_ownership_columns(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """The exact backfill scenario Codex flagged: a canonical row
        already exists, then a re-derive lands new values for the four
        ownership columns. The merge's ON CONFLICT DO UPDATE branch
        must overwrite the canonical values — not leave them stale.
        """
        conn = ebull_test_conn
        _seed_instrument(conn, iid=731_002, symbol="OWN2")

        # First arrival: original 10-K with one set of ownership values.
        _seed_raw_with_ownership(
            conn,
            instrument_id=731_002,
            period_end=date(2024, 12, 31),
            fiscal_year=2024,
            source_ref="acc-original",
            filed_date=date(2025, 2, 14),
            treasury_shares=Decimal("12500000"),
            shares_authorized=Decimal("5000000000"),
            shares_issued=Decimal("1750000000"),
            retained_earnings=Decimal("180000000000"),
        )
        conn.commit()
        _canonical_merge_instrument(conn, 731_002)
        conn.commit()

        # Phase 2: a 10-K/A amendment with the SAME period_end (in-place
        # restatement; no DELETE+INSERT churn) re-files the four
        # ownership values. The merge must update via ON CONFLICT.
        _seed_raw_with_ownership(
            conn,
            instrument_id=731_002,
            period_end=date(2024, 12, 31),
            fiscal_year=2024,
            source_ref="acc-amendment",
            filed_date=date(2025, 5, 1),
            treasury_shares=Decimal("13000000"),
            shares_authorized=Decimal("5000000000"),
            shares_issued=Decimal("1755000000"),
            retained_earnings=Decimal("182000000000"),
        )
        conn.commit()
        _canonical_merge_instrument(conn, 731_002)
        conn.commit()

        row = _canonical_ownership_row(conn, 731_002)
        # Amendment values won.
        assert row["source_ref"] == "acc-amendment"
        assert row["treasury_shares"] == Decimal("13000000")
        assert row["shares_issued"] == Decimal("1755000000")
        assert row["retained_earnings"] == Decimal("182000000000")
        # Unchanged column survives the update intact.
        assert row["shares_authorized"] == Decimal("5000000000")
