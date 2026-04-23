"""Tests for financial facts service — XBRL fact storage and ingestion tracking."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

from app.providers.fundamentals import XbrlFact
from app.services.fundamentals import (
    finish_ingestion_run,
    start_ingestion_run,
    upsert_facts_for_instrument,
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
        run_id = start_ingestion_run(conn, source="sec_edgar", endpoint="/api/xbrl/companyfacts", instrument_count=5)
        assert run_id == 42
        conn.execute.assert_called_once()


class TestFinishIngestionRun:
    def test_updates_run_status(self) -> None:
        conn = MagicMock()
        finish_ingestion_run(conn, run_id=42, status="success", rows_upserted=100, rows_skipped=3)
        conn.execute.assert_called_once()
        call_args = conn.execute.call_args
        sql = call_args[0][0]
        assert "finished_at" in sql
        assert "status" in sql


def _mock_conn_with_rowcount(rowcount: int) -> tuple[MagicMock, MagicMock]:
    """Return ``(conn, cur)`` where ``conn.cursor()`` yields a cursor
    whose ``executemany`` sets ``rowcount`` to the provided value.

    The ADR 0004 shape calls ``conn.cursor()`` as a context manager,
    then ``cur.executemany(sql, chunk)``, then reads ``cur.rowcount``.
    The test double models that path so unit tests do not need a real
    Postgres.
    """
    conn = MagicMock()
    cur = MagicMock()
    cur.rowcount = rowcount
    conn.cursor.return_value.__enter__.return_value = cur
    return conn, cur


class TestUpsertFacts:
    def test_upserts_single_fact(self) -> None:
        conn, cur = _mock_conn_with_rowcount(1)
        facts = [_make_fact()]
        upserted, skipped = upsert_facts_for_instrument(conn, instrument_id=1, facts=facts, ingestion_run_id=42)
        assert upserted == 1
        assert skipped == 0
        cur.executemany.assert_called_once()

    def test_handles_empty_facts(self) -> None:
        conn = MagicMock()
        upserted, skipped = upsert_facts_for_instrument(conn, instrument_id=1, facts=[], ingestion_run_id=42)
        assert upserted == 0
        assert skipped == 0
        # Empty facts must short-circuit before opening a cursor —
        # avoids a wasted round-trip on instruments with no XBRL facts.
        conn.cursor.assert_not_called()

    def test_counts_skipped_when_unchanged(self) -> None:
        # ``IS DISTINCT FROM`` filter matches zero rows — all facts are
        # idempotent no-ops. rowcount=0 across the whole chunk.
        conn, _ = _mock_conn_with_rowcount(0)
        facts = [_make_fact()]
        upserted, skipped = upsert_facts_for_instrument(conn, instrument_id=1, facts=facts, ingestion_run_id=42)
        assert upserted == 0
        assert skipped == 1

    def test_batches_large_payload_via_executemany(self) -> None:
        # 2500 facts must split into three chunks at page_size=1000.
        # Each chunk reports rowcount = chunk_size (all INSERTed).
        conn = MagicMock()
        cur = MagicMock()
        rowcounts = iter([1000, 1000, 500])

        def set_rowcount(_sql: object, params: list[object]) -> None:
            cur.rowcount = len(params)

        cur.executemany.side_effect = set_rowcount
        cur.rowcount = next(rowcounts)  # pre-set for first .rowcount read
        conn.cursor.return_value.__enter__.return_value = cur
        facts = [_make_fact(accession_number=f"acc-{i:05d}") for i in range(2500)]
        upserted, skipped = upsert_facts_for_instrument(conn, instrument_id=1, facts=facts, ingestion_run_id=42)
        # Three executemany calls; each one sets rowcount to its chunk
        # length. Aggregate == total facts.
        assert cur.executemany.call_count == 3
        chunk_sizes = [len(call.args[1]) for call in cur.executemany.call_args_list]
        assert chunk_sizes == [1000, 1000, 500]
        assert upserted == 2500
        assert skipped == 0

    def test_negative_rowcount_raises(self) -> None:
        # rowcount == -1 means the driver did not report a command
        # tag. Silently treating that as "all rows were skipped"
        # would contaminate upserted/skipped accounting. The contract
        # is to raise so the caller rolls back and the watermark
        # stays at its previous value (the next run retries).
        import pytest

        conn, _ = _mock_conn_with_rowcount(-1)
        facts = [_make_fact()]
        with pytest.raises(RuntimeError, match="rowcount=-1"):
            upsert_facts_for_instrument(conn, instrument_id=1, facts=facts, ingestion_run_id=42)
