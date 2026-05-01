"""Integration test for #732 — Tier 1 + Tier 2 column expansion projects
through the canonical merge ON CONFLICT update branch.

Mirrors the residual-risk pin added in PR #737 / #731 for the four
ownership columns. Confirms that all ten new columns added by
migration 089 round-trip from financial_periods_raw through
_canonical_merge_instrument's ON CONFLICT DO UPDATE branch — the path
that backfills canonical rows pre-existing from a prior ingest.
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


_TIER12_COLUMNS = (
    "assets_current",
    "liabilities_current",
    "cash_restricted",
    "comprehensive_income",
    "intangible_amortization",
    "deferred_income_tax",
    "other_nonoperating_income",
    "additional_paid_in_capital",
    "accumulated_oci",
    "antidilutive_securities",
)


def _seed_instrument(conn: psycopg.Connection[tuple], iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} test"),
    )


def _seed_raw_with_tier12(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    period_end: date,
    fiscal_year: int,
    source_ref: str,
    filed_date: date,
    values: dict[str, Decimal],
) -> None:
    """Seed a raw row with whatever subset of Tier 1 / Tier 2 columns
    appears in ``values``. Columns not in the dict default to NULL."""
    columns = list(values.keys())
    placeholders = ", ".join(f"%({c})s" for c in columns)
    column_list = ", ".join(columns)
    sql = f"""
        INSERT INTO financial_periods_raw (
            instrument_id, period_end_date, period_type,
            fiscal_year, fiscal_quarter, revenue,
            {column_list},
            source, source_ref, reported_currency, filed_date
        ) VALUES (
            %(instrument_id)s, %(period_end)s, 'FY',
            %(fiscal_year)s, NULL, 1000,
            {placeholders},
            'sec_edgar', %(source_ref)s, 'USD', %(filed_date)s
        )
    """  # noqa: S608 — column list is a hardcoded test whitelist
    params: dict[str, object] = {
        "instrument_id": instrument_id,
        "period_end": period_end,
        "fiscal_year": fiscal_year,
        "source_ref": source_ref,
        "filed_date": filed_date,
    }
    params.update(values)  # type: ignore[arg-type]
    conn.execute(sql, params)  # type: ignore[arg-type]  # SQL built from hardcoded test whitelist


def _canonical_tier12_row(
    conn: psycopg.Connection[tuple],
    instrument_id: int,
) -> dict[str, object]:
    select_cols = ", ".join(["period_end_date", "source_ref", *_TIER12_COLUMNS])
    sql = f"""
        SELECT {select_cols}
        FROM financial_periods
        WHERE instrument_id = %s
    """  # noqa: S608 — column list is a hardcoded test whitelist
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(sql, (instrument_id,))
        rows = cur.fetchall()
    assert len(rows) == 1, [dict(r) for r in rows]
    return rows[0]  # type: ignore[return-value]


class TestCanonicalMergeTier1Tier2Columns:
    def test_insert_path_populates_all_tier12_columns(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Fresh canonical row receives every Tier 1 + Tier 2 column from
        the raw row's INSERT path."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=732_001, symbol="T12A")

        values = {
            "assets_current": Decimal("450000000"),
            "liabilities_current": Decimal("320000000"),
            "cash_restricted": Decimal("85000000"),
            "comprehensive_income": Decimal("220000"),
            "intangible_amortization": Decimal("18000"),
            "deferred_income_tax": Decimal("12000"),
            "other_nonoperating_income": Decimal("-3500"),
            "additional_paid_in_capital": Decimal("250000000"),
            "accumulated_oci": Decimal("-15000000"),
            "antidilutive_securities": Decimal("4500000"),
        }
        _seed_raw_with_tier12(
            conn,
            instrument_id=732_001,
            period_end=date(2024, 12, 31),
            fiscal_year=2024,
            source_ref="acc-original",
            filed_date=date(2025, 2, 14),
            values=values,
        )
        conn.commit()

        _canonical_merge_instrument(conn, 732_001)
        conn.commit()

        row = _canonical_tier12_row(conn, 732_001)
        for col, expected in values.items():
            assert row[col] == expected, f"{col}: expected {expected}, got {row[col]!r}"

    def test_on_conflict_update_path_replaces_all_tier12_columns(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """All ten Tier 1 + Tier 2 columns must round-trip through the
        merge's ON CONFLICT DO UPDATE clause. A missed assignment for
        any single column would let stale data survive the amendment.

        The amendment seeds DIFFERENT values for every column so each
        column's update path is independently observable — a missed
        SET clause for one column would leave the original value
        intact and fail the per-column assertion.
        """
        conn = ebull_test_conn
        _seed_instrument(conn, iid=732_002, symbol="T12B")

        original: dict[str, Decimal] = {
            "assets_current": Decimal("400000000"),
            "liabilities_current": Decimal("300000000"),
            "cash_restricted": Decimal("80000000"),
            "comprehensive_income": Decimal("200000"),
            "intangible_amortization": Decimal("15000"),
            "deferred_income_tax": Decimal("10000"),
            "other_nonoperating_income": Decimal("-3000"),
            "additional_paid_in_capital": Decimal("240000000"),
            "accumulated_oci": Decimal("-14000000"),
            "antidilutive_securities": Decimal("4000000"),
        }
        _seed_raw_with_tier12(
            conn,
            instrument_id=732_002,
            period_end=date(2024, 12, 31),
            fiscal_year=2024,
            source_ref="acc-original",
            filed_date=date(2025, 2, 14),
            values=original,
        )
        conn.commit()
        _canonical_merge_instrument(conn, 732_002)
        conn.commit()

        # Amendment re-files every column with a distinct value so any
        # missed DO UPDATE SET assignment trips the per-column assert.
        amended: dict[str, Decimal] = {
            "assets_current": Decimal("450000000"),
            "liabilities_current": Decimal("320000000"),
            "cash_restricted": Decimal("85000000"),
            "comprehensive_income": Decimal("220000"),
            "intangible_amortization": Decimal("18000"),
            "deferred_income_tax": Decimal("12000"),
            "other_nonoperating_income": Decimal("-3500"),
            "additional_paid_in_capital": Decimal("250000000"),
            "accumulated_oci": Decimal("-15000000"),
            "antidilutive_securities": Decimal("4500000"),
        }
        # Sanity: every original/amended pair MUST differ — otherwise
        # a missed SET clause could pass silently because the column
        # value happens to match.
        for col in original:
            assert original[col] != amended[col], f"test design bug: {col} unchanged between original and amendment"
        _seed_raw_with_tier12(
            conn,
            instrument_id=732_002,
            period_end=date(2024, 12, 31),
            fiscal_year=2024,
            source_ref="acc-amendment",
            filed_date=date(2025, 5, 1),
            values=amended,
        )
        conn.commit()
        _canonical_merge_instrument(conn, 732_002)
        conn.commit()

        row = _canonical_tier12_row(conn, 732_002)
        assert row["source_ref"] == "acc-amendment"
        for col, expected in amended.items():
            assert row[col] == expected, f"{col}: expected {expected}, got {row[col]!r}"

    def test_on_conflict_update_clears_columns_dropped_by_amendment(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """A column populated on the original but absent (NULL) on the
        amendment must be cleared on the canonical row. The
        ``EXCLUDED.col`` semantics propagate NULL correctly only if
        the column appears in the DO UPDATE SET clause — verifying
        per Codex residual-risk note on PR review."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=732_003, symbol="T12C")

        # Original: every Tier 1 + Tier 2 column populated.
        original: dict[str, Decimal] = {col: Decimal(1_000_000 + i) for i, col in enumerate(_TIER12_COLUMNS)}
        _seed_raw_with_tier12(
            conn,
            instrument_id=732_003,
            period_end=date(2024, 12, 31),
            fiscal_year=2024,
            source_ref="acc-original",
            filed_date=date(2025, 2, 14),
            values=original,
        )
        conn.commit()
        _canonical_merge_instrument(conn, 732_003)
        conn.commit()

        # Amendment: ONLY assets_current populated; the other nine
        # columns absent → NULL in the raw row → must propagate to
        # canonical via EXCLUDED.col.
        amended: dict[str, Decimal] = {"assets_current": Decimal("999000000")}
        _seed_raw_with_tier12(
            conn,
            instrument_id=732_003,
            period_end=date(2024, 12, 31),
            fiscal_year=2024,
            source_ref="acc-amendment",
            filed_date=date(2025, 5, 1),
            values=amended,
        )
        conn.commit()
        _canonical_merge_instrument(conn, 732_003)
        conn.commit()

        row = _canonical_tier12_row(conn, 732_003)
        assert row["assets_current"] == Decimal("999000000")
        for col in _TIER12_COLUMNS:
            if col == "assets_current":
                continue
            assert row[col] is None, f"{col}: expected NULL after amendment dropped it, got {row[col]!r}"
