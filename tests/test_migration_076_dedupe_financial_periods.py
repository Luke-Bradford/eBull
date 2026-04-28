"""Regression test for migration 076 (dedupe duplicate fiscal-period rows, #558).

Seeds duplicate rows in ``financial_periods`` and ``financial_periods_raw``
matching the two patterns in #558:

  * **DEI-context pollution** — same accession, two different
    period_end_date values. The polluted row uses the filing date as
    period_end. Pass 1 keeps the smallest.

  * **Cross-accession restatement leftover** — different accessions,
    same fiscal label, two different period_end_date values. The
    older filing's row should be dropped. Pass 2 keeps the most
    recently filed.

Tests run the actual migration file (``sql/076_dedupe_financial_periods.sql``)
to keep the test/migration pair from drifting.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import psycopg
import psycopg.rows
import pytest

from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


_MIGRATION_PATH = Path(__file__).resolve().parents[1] / "sql" / "076_dedupe_financial_periods.sql"


def _run_migration(conn: psycopg.Connection[tuple]) -> None:
    """Re-execute migration 076 against the test DB.

    The migration file is idempotent, so re-running on top of the
    already-applied state is a no-op for already-deduped rows but
    will collapse rows seeded by the test after the auto-apply.
    Uses ClientCursor because the file uses BEGIN/COMMIT (multiple
    statements) and the simple-query protocol is required.
    """
    sql_text = _MIGRATION_PATH.read_text(encoding="utf-8")
    with psycopg.ClientCursor(conn) as cur:
        cur.execute(sql_text)  # type: ignore[call-overload]


def _seed_instrument(conn: psycopg.Connection[tuple], instrument_id: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (instrument_id, symbol, f"{symbol} test"),
    )


def _seed_period(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    period_end: date,
    period_type: str,
    fiscal_year: int,
    fiscal_quarter: int | None,
    source_ref: str,
    filed_date: date | None = None,
    revenue: Decimal | None = Decimal("100"),
    source: str = "sec_edgar",
) -> None:
    conn.execute(
        """
        INSERT INTO financial_periods (
            instrument_id, period_end_date, period_type,
            fiscal_year, fiscal_quarter,
            revenue,
            source, source_ref, reported_currency, filed_date
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, 'USD', %s
        )
        """,
        (
            instrument_id,
            period_end,
            period_type,
            fiscal_year,
            fiscal_quarter,
            revenue,
            source,
            source_ref,
            filed_date,
        ),
    )


def _seed_period_raw(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    period_end: date,
    period_type: str,
    fiscal_year: int,
    fiscal_quarter: int | None,
    source_ref: str,
    filed_date: date | None = None,
    revenue: Decimal | None = Decimal("100"),
    source: str = "sec_edgar",
) -> None:
    conn.execute(
        """
        INSERT INTO financial_periods_raw (
            instrument_id, period_end_date, period_type,
            fiscal_year, fiscal_quarter,
            revenue,
            source, source_ref, reported_currency, filed_date
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, 'USD', %s
        )
        """,
        (
            instrument_id,
            period_end,
            period_type,
            fiscal_year,
            fiscal_quarter,
            revenue,
            source,
            source_ref,
            filed_date,
        ),
    )


class TestDedupePass1SameAccession:
    """Pass 1: collapse same source_ref, smaller period_end wins (DEI pollution)."""

    def test_canonical_table_keeps_smallest_period_end(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, instrument_id=1, symbol="GME")
        _seed_period(
            conn,
            instrument_id=1,
            period_end=date(2026, 1, 31),  # real Q4 end
            period_type="Q4",
            fiscal_year=2025,
            fiscal_quarter=4,
            source_ref="0001326380-26-000013",
            filed_date=date(2026, 3, 19),
            revenue=Decimal("2732400000"),
        )
        _seed_period(
            conn,
            instrument_id=1,
            period_end=date(2026, 3, 18),  # filing-date pollution
            period_type="Q4",
            fiscal_year=2025,
            fiscal_quarter=4,
            source_ref="0001326380-26-000013",
            filed_date=date(2026, 3, 19),
            revenue=Decimal("2732400000"),
        )
        conn.commit()

        _run_migration(conn)
        conn.commit()

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT period_end_date FROM financial_periods "
                "WHERE instrument_id = 1 AND fiscal_year = 2025 "
                "AND fiscal_quarter = 4 AND period_type = 'Q4'"
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0]["period_end_date"] == date(2026, 1, 31)

    def test_fy_row_with_null_quarter_is_collapsed(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """FY rows have fiscal_quarter = NULL. The IS NOT DISTINCT FROM
        join must still match two FY rows with the same source_ref.
        """
        conn = ebull_test_conn
        _seed_instrument(conn, instrument_id=2, symbol="AAPL")
        _seed_period(
            conn,
            instrument_id=2,
            period_end=date(2025, 9, 28),
            period_type="FY",
            fiscal_year=2025,
            fiscal_quarter=None,
            source_ref="acc-fy",
            filed_date=date(2025, 11, 5),
        )
        _seed_period(
            conn,
            instrument_id=2,
            period_end=date(2025, 11, 4),
            period_type="FY",
            fiscal_year=2025,
            fiscal_quarter=None,
            source_ref="acc-fy",
            filed_date=date(2025, 11, 5),
        )
        conn.commit()

        _run_migration(conn)
        conn.commit()

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute("SELECT period_end_date FROM financial_periods WHERE instrument_id = 2 AND period_type = 'FY'")
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0]["period_end_date"] == date(2025, 9, 28)


class TestDedupePass2CrossAccession:
    """Pass 2: collapse same (fy, fq, period_type), keep latest filed_date."""

    def test_keeps_latest_filed_amendment(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Cross-accession restatement leftover. The instrument page must
        not show two columns for the same fiscal label; the latest
        filing's values supersede the original.
        """
        conn = ebull_test_conn
        _seed_instrument(conn, instrument_id=3, symbol="MSFT")
        _seed_period(
            conn,
            instrument_id=3,
            period_end=date(2024, 6, 30),
            period_type="Q4",
            fiscal_year=2024,
            fiscal_quarter=4,
            source_ref="acc-original",
            filed_date=date(2024, 7, 30),
            revenue=Decimal("1000"),
        )
        _seed_period(
            conn,
            instrument_id=3,
            period_end=date(2024, 7, 15),
            period_type="Q4",
            fiscal_year=2024,
            fiscal_quarter=4,
            source_ref="acc-amendment",
            filed_date=date(2024, 9, 12),  # later
            revenue=Decimal("1100"),
        )
        conn.commit()

        _run_migration(conn)
        conn.commit()

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT period_end_date, source_ref, revenue FROM financial_periods "
                "WHERE instrument_id = 3 AND fiscal_quarter = 4"
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0]["source_ref"] == "acc-amendment"
        assert rows[0]["revenue"] == Decimal("1100")

    def test_tied_filed_date_falls_back_to_period_end(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """When two cross-accession rows were filed on the same day,
        the larger period_end_date wins (most-recently-reported period
        end).
        """
        conn = ebull_test_conn
        _seed_instrument(conn, instrument_id=8, symbol="ORCL")
        _seed_period(
            conn,
            instrument_id=8,
            period_end=date(2024, 11, 30),
            period_type="Q2",
            fiscal_year=2025,
            fiscal_quarter=2,
            source_ref="acc-a",
            filed_date=date(2024, 12, 11),
        )
        _seed_period(
            conn,
            instrument_id=8,
            period_end=date(2024, 12, 1),
            period_type="Q2",
            fiscal_year=2025,
            fiscal_quarter=2,
            source_ref="acc-b",
            filed_date=date(2024, 12, 11),  # same filed_date
        )
        conn.commit()

        _run_migration(conn)
        conn.commit()

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute("SELECT period_end_date FROM financial_periods WHERE instrument_id = 8 AND fiscal_quarter = 2")
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0]["period_end_date"] == date(2024, 12, 1)

    def test_null_filed_date_loses_to_known(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """A row with a known filed_date supersedes a row whose
        filed_date is NULL — known-provenance always beats stub.
        """
        conn = ebull_test_conn
        _seed_instrument(conn, instrument_id=9, symbol="ADBE")
        _seed_period(
            conn,
            instrument_id=9,
            period_end=date(2024, 8, 30),
            period_type="Q3",
            fiscal_year=2024,
            fiscal_quarter=3,
            source_ref="acc-stub",
            filed_date=None,
        )
        _seed_period(
            conn,
            instrument_id=9,
            period_end=date(2024, 8, 31),
            period_type="Q3",
            fiscal_year=2024,
            fiscal_quarter=3,
            source_ref="acc-real",
            filed_date=date(2024, 9, 13),
        )
        conn.commit()

        _run_migration(conn)
        conn.commit()

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute("SELECT source_ref FROM financial_periods WHERE instrument_id = 9")
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0]["source_ref"] == "acc-real"


class TestDedupeIsolation:
    def test_different_instruments_isolated(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Dedupe is per-instrument. A row on instrument 4 must not
        delete a row on instrument 5 even when source_ref + fiscal
        labels collide.
        """
        conn = ebull_test_conn
        _seed_instrument(conn, instrument_id=4, symbol="X")
        _seed_instrument(conn, instrument_id=5, symbol="Y")
        _seed_period(
            conn,
            instrument_id=4,
            period_end=date(2025, 12, 31),
            period_type="Q4",
            fiscal_year=2025,
            fiscal_quarter=4,
            source_ref="shared-acc",
            filed_date=date(2026, 2, 1),
        )
        _seed_period(
            conn,
            instrument_id=5,
            period_end=date(2025, 12, 31),
            period_type="Q4",
            fiscal_year=2025,
            fiscal_quarter=4,
            source_ref="shared-acc",
            filed_date=date(2026, 2, 1),
        )
        conn.commit()

        _run_migration(conn)
        conn.commit()

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute("SELECT instrument_id FROM financial_periods ORDER BY instrument_id")
            rows = cur.fetchall()
        assert [r["instrument_id"] for r in rows] == [4, 5]

    def test_different_sources_kept(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """sec_edgar and (hypothetical) companies_house rows for the
        same fiscal label are independently authoritative — the
        per-source scope on both passes preserves them.
        """
        conn = ebull_test_conn
        _seed_instrument(conn, instrument_id=10, symbol="ZZZ")
        # Different period_end_date so the canonical PK
        # (instrument_id, period_end_date, period_type) tolerates both
        # rows; the dedupe is what we're testing, not insertability.
        _seed_period(
            conn,
            instrument_id=10,
            period_end=date(2024, 12, 31),
            period_type="FY",
            fiscal_year=2024,
            fiscal_quarter=None,
            source_ref="sec-a",
            filed_date=date(2025, 3, 15),
            source="sec_edgar",
        )
        _seed_period(
            conn,
            instrument_id=10,
            period_end=date(2025, 1, 1),
            period_type="FY",
            fiscal_year=2024,
            fiscal_quarter=None,
            source_ref="ch-a",
            filed_date=date(2025, 4, 1),
            source="companies_house",
        )
        conn.commit()

        _run_migration(conn)
        conn.commit()

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute("SELECT source FROM financial_periods WHERE instrument_id = 10 ORDER BY source")
            rows = cur.fetchall()
        assert [r["source"] for r in rows] == ["companies_house", "sec_edgar"]


class TestDedupeIdempotent:
    def test_second_run_is_noop(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Running the migration a second time on already-deduped data
        does not delete any further rows.
        """
        conn = ebull_test_conn
        _seed_instrument(conn, instrument_id=6, symbol="NVDA")
        _seed_period(
            conn,
            instrument_id=6,
            period_end=date(2025, 1, 26),
            period_type="Q4",
            fiscal_year=2024,
            fiscal_quarter=4,
            source_ref="acc-x",
            filed_date=date(2025, 2, 28),
        )
        _seed_period(
            conn,
            instrument_id=6,
            period_end=date(2025, 2, 28),
            period_type="Q4",
            fiscal_year=2024,
            fiscal_quarter=4,
            source_ref="acc-x",
            filed_date=date(2025, 2, 28),
        )
        conn.commit()

        _run_migration(conn)
        conn.commit()
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute("SELECT COUNT(*) AS n FROM financial_periods WHERE instrument_id = 6")
            count_after_first = cur.fetchone()["n"]  # type: ignore[index]

        _run_migration(conn)
        conn.commit()
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute("SELECT COUNT(*) AS n FROM financial_periods WHERE instrument_id = 6")
            count_after_second = cur.fetchone()["n"]  # type: ignore[index]

        assert count_after_first == 1
        assert count_after_second == 1


class TestDedupeRawTable:
    def test_raw_table_pass1(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """The migration also collapses duplicates in
        financial_periods_raw so a future canonical merge cannot
        re-pollute the canonical table from leftover raw rows.
        """
        conn = ebull_test_conn
        _seed_instrument(conn, instrument_id=7, symbol="TSLA")
        _seed_period_raw(
            conn,
            instrument_id=7,
            period_end=date(2025, 12, 31),
            period_type="Q4",
            fiscal_year=2025,
            fiscal_quarter=4,
            source_ref="acc-raw",
            filed_date=date(2026, 2, 1),
        )
        _seed_period_raw(
            conn,
            instrument_id=7,
            period_end=date(2026, 2, 1),
            period_type="Q4",
            fiscal_year=2025,
            fiscal_quarter=4,
            source_ref="acc-raw",
            filed_date=date(2026, 2, 1),
        )
        conn.commit()

        _run_migration(conn)
        conn.commit()

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute("SELECT period_end_date FROM financial_periods_raw WHERE instrument_id = 7")
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0]["period_end_date"] == date(2025, 12, 31)
