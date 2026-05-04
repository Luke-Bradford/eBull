"""Tests for targeted manifest rebuild (#872)."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import psycopg
import pytest

from app.jobs.sec_rebuild import RebuildScope, run_sec_rebuild
from app.services.data_freshness import get_freshness_row, record_poll_outcome
from app.services.sec_manifest import (
    get_manifest_row,
    record_manifest_entry,
    transition_status,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401

pytestmark = pytest.mark.integration


def _seed_aapl_with_state(conn: psycopg.Connection[tuple]) -> None:
    """AAPL with one parsed manifest row + scheduler row in 'current' state."""
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (1701, 'AAPL', 'Apple', '4', 'USD', TRUE)
        """
    )
    conn.execute("INSERT INTO instrument_sec_profile (instrument_id, cik) VALUES (1701, '0000320193')")
    record_manifest_entry(
        conn,
        "0000320193-26-000001",
        cik="0000320193",
        form="DEF 14A",
        source="sec_def14a",
        subject_type="issuer",
        subject_id="1701",
        instrument_id=1701,
        filed_at=datetime(2026, 2, 14, tzinfo=UTC),
    )
    transition_status(conn, "0000320193-26-000001", ingest_status="parsed", parser_version="v1")
    record_poll_outcome(
        conn,
        subject_type="issuer",
        subject_id="1701",
        source="sec_def14a",
        outcome="current",
        last_known_filing_id="0000320193-26-000001",
        last_known_filed_at=datetime(2026, 2, 14, tzinfo=UTC),
        cik="0000320193",
        instrument_id=1701,
    )
    conn.commit()


def _fake_get(payload: dict):
    body = json.dumps(payload).encode("utf-8")

    def _impl(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
        return 200, body

    return _impl


class TestRebuildScope:
    def test_per_instrument_scope_resets_all_sources(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_aapl_with_state(ebull_test_conn)

        stats = run_sec_rebuild(
            ebull_test_conn,
            RebuildScope(instrument_id=1701),
            discover=False,
        )
        ebull_test_conn.commit()

        assert stats.scope_triples == 1
        assert stats.manifest_rows_reset == 1

        # Manifest row flipped back to pending; parser_version preserved
        manifest_row = get_manifest_row(ebull_test_conn, "0000320193-26-000001")
        assert manifest_row is not None
        assert manifest_row.ingest_status == "pending"
        assert manifest_row.parser_version == "v1"

        # Scheduler reset
        sched = get_freshness_row(ebull_test_conn, subject_type="issuer", subject_id="1701", source="sec_def14a")
        assert sched is not None
        assert sched.state == "unknown"
        assert sched.last_known_filing_id is None
        # expected_next_at is NOW() — close to current time
        assert sched.expected_next_at is not None

    def test_per_source_scope_universe_wide(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_aapl_with_state(ebull_test_conn)
        stats = run_sec_rebuild(
            ebull_test_conn,
            RebuildScope(source="sec_def14a"),
            discover=False,
        )
        ebull_test_conn.commit()
        assert stats.scope_triples == 1

    def test_empty_scope_rejected(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        with pytest.raises(ValueError, match="at least one of"):
            run_sec_rebuild(ebull_test_conn, RebuildScope(), discover=False)
        ebull_test_conn.rollback()

    def test_unmatched_scope_no_op(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        stats = run_sec_rebuild(
            ebull_test_conn,
            RebuildScope(instrument_id=99999),
            discover=False,
        )
        ebull_test_conn.commit()
        assert stats.scope_triples == 0
        assert stats.manifest_rows_reset == 0


class TestDiscoveryPass:
    def test_discovery_repairs_missing_accession(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_aapl_with_state(ebull_test_conn)

        # Submissions response carries TWO accessions; only one is in
        # the manifest (the seeded one). Discovery should add the
        # missing one.
        payload = {
            "cik": "320193",
            "filings": {
                "recent": {
                    "accessionNumber": ["0000320193-26-000099", "0000320193-26-000001"],
                    "filingDate": ["2026-04-30", "2026-02-14"],
                    "form": ["DEF 14A", "DEF 14A"],
                    "acceptanceDateTime": [
                        "2026-04-30T16:00:00.000Z",
                        "2026-02-14T08:00:00.000Z",
                    ],
                    "primaryDocument": ["proxy-amend.htm", "proxy.htm"],
                },
                "files": [],
            },
        }

        stats = run_sec_rebuild(
            ebull_test_conn,
            RebuildScope(instrument_id=1701),
            http_get=_fake_get(payload),
            discover=True,
        )
        ebull_test_conn.commit()

        assert stats.discovery_new_manifest_rows >= 1
        new_row = get_manifest_row(ebull_test_conn, "0000320193-26-000099")
        assert new_row is not None
        assert new_row.source == "sec_def14a"
