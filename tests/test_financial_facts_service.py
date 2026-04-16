"""Tests for financial facts service — XBRL fact storage and ingestion tracking."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

from app.providers.fundamentals import XbrlFact
from app.services.financial_facts import (
    _finish_ingestion_run,
    _start_ingestion_run,
    _upsert_facts,
)


def _make_fact(
    *,
    concept: str = "Revenues",
    val: Decimal = Decimal("50000000"),
    period_end: date = date(2024, 3, 31),
    period_start: date | None = date(2024, 1, 1),
    frame: str | None = "CY2024Q1",
    accession_number: str = "0000320193-24-000042",
    form_type: str = "10-Q",
    filed_date: date = date(2024, 5, 1),
    fiscal_year: int | None = 2024,
    fiscal_period: str | None = "Q1",
    unit: str = "USD",
) -> XbrlFact:
    return XbrlFact(
        concept=concept,
        taxonomy="us-gaap",
        unit=unit,
        period_start=period_start,
        period_end=period_end,
        val=val,
        frame=frame,
        accession_number=accession_number,
        form_type=form_type,
        filed_date=filed_date,
        fiscal_year=fiscal_year,
        fiscal_period=fiscal_period,
        decimals="-3",
    )


class TestStartIngestionRun:
    def test_returns_run_id(self) -> None:
        conn = MagicMock()
        cursor = MagicMock()
        cursor.fetchone.return_value = (42,)
        conn.execute.return_value = cursor
        run_id = _start_ingestion_run(conn, source="sec_edgar", endpoint="/api/xbrl/companyfacts", instrument_count=5)
        assert run_id == 42
        conn.execute.assert_called_once()


class TestFinishIngestionRun:
    def test_updates_run_status(self) -> None:
        conn = MagicMock()
        _finish_ingestion_run(conn, run_id=42, status="success", rows_upserted=100, rows_skipped=3)
        conn.execute.assert_called_once()
        call_args = conn.execute.call_args
        sql = call_args[0][0]
        assert "finished_at" in sql
        assert "status" in sql


class TestUpsertFacts:
    def test_upserts_single_fact(self) -> None:
        conn = MagicMock()
        cursor = MagicMock()
        cursor.rowcount = 1
        conn.execute.return_value = cursor
        facts = [_make_fact()]
        upserted, skipped = _upsert_facts(conn, instrument_id=1, facts=facts, ingestion_run_id=42)
        assert upserted == 1
        assert skipped == 0

    def test_handles_empty_facts(self) -> None:
        conn = MagicMock()
        upserted, skipped = _upsert_facts(conn, instrument_id=1, facts=[], ingestion_run_id=42)
        assert upserted == 0
        assert skipped == 0

    def test_counts_skipped_when_unchanged(self) -> None:
        conn = MagicMock()
        cursor = MagicMock()
        cursor.rowcount = 0  # ON CONFLICT skipped because data unchanged
        conn.execute.return_value = cursor
        facts = [_make_fact()]
        upserted, skipped = _upsert_facts(conn, instrument_id=1, facts=facts, ingestion_run_id=42)
        assert upserted == 0
        assert skipped == 1
