"""Tests for the self-healing ownership repair sweep (#873)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import uuid4

import psycopg
import pytest

from app.jobs.ownership_observations_repair import (
    run_observations_repair_sweep,
)
from app.services.ownership_observations import (
    record_insider_observation,
    refresh_insiders_current,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401

pytestmark = pytest.mark.integration


def _seed(conn: psycopg.Connection[tuple]) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (1, 'X', 'X Inc', '4', 'USD', TRUE)
        """
    )
    conn.commit()


class TestRepairSweep:
    def test_healthy_install_finds_no_drift(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # No observations + no current rows → repair sweep is a no-op.
        stats = run_observations_repair_sweep(ebull_test_conn)
        assert stats.total_drifted == 0

    def test_observation_advances_drifts_until_repaired(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed(ebull_test_conn)
        run_id = uuid4()

        # 1. Initial observation in one tx; refresh in a separate tx —
        # mirrors the production write-through pattern (record + commit,
        # then refresh + commit) so refreshed_at lands AFTER the
        # observation's ingested_at clock.
        record_insider_observation(
            ebull_test_conn,
            instrument_id=1,
            holder_cik="0000000001",
            holder_name="Alice",
            ownership_nature="direct",
            source="form4",
            source_document_id="DOC-1",
            source_accession="ACC-1",
            source_field=None,
            source_url=None,
            filed_at=datetime(2026, 1, 1, tzinfo=UTC),
            period_start=None,
            period_end=date(2026, 1, 1),
            ingest_run_id=run_id,
            shares=Decimal("100"),
        )
        ebull_test_conn.commit()
        refresh_insiders_current(ebull_test_conn, instrument_id=1)
        ebull_test_conn.commit()

        # No drift yet
        stats = run_observations_repair_sweep(ebull_test_conn)
        ebull_test_conn.commit()
        assert stats.total_drifted == 0

        # 2. New observation lands without an inline refresh — drift
        # appears (ingested_at on the new obs > _current.refreshed_at)
        record_insider_observation(
            ebull_test_conn,
            instrument_id=1,
            holder_cik="0000000002",
            holder_name="Bob",
            ownership_nature="direct",
            source="form4",
            source_document_id="DOC-2",
            source_accession="ACC-2",
            source_field=None,
            source_url=None,
            filed_at=datetime(2026, 1, 2, tzinfo=UTC),
            period_start=None,
            period_end=date(2026, 1, 1),
            ingest_run_id=run_id,
            shares=Decimal("200"),
        )
        ebull_test_conn.commit()

        # 3. Repair sweep detects + refreshes
        stats = run_observations_repair_sweep(ebull_test_conn)
        ebull_test_conn.commit()
        per_insider = next(c for c in stats.per_category if c.category == "ownership_insiders_current")
        assert per_insider.drifted_instruments == 1
        assert per_insider.refreshed_rows == 2  # Alice + Bob

        # 4. Second sweep is now a no-op (_current is current)
        stats2 = run_observations_repair_sweep(ebull_test_conn)
        ebull_test_conn.commit()
        assert stats2.total_drifted == 0
