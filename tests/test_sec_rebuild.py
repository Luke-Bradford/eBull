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

    def test_discovery_walks_filings_files_secondary_pages(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # #936: rebuild claims full-history discovery; ``check_freshness``
        # reads only ``recent[]``. An accession aged out into a
        # secondary ``filings.files[]`` page must still be recovered by
        # the rebuild path. ``recent`` here carries no in-scope
        # accessions; the secondary page carries the one we expect to
        # discover.
        _seed_aapl_with_state(ebull_test_conn)

        recent_payload = {
            "cik": "320193",
            "filings": {
                "recent": {
                    "accessionNumber": ["0000320193-26-000001"],
                    "filingDate": ["2026-02-14"],
                    "form": ["DEF 14A"],
                    "acceptanceDateTime": ["2026-02-14T08:00:00.000Z"],
                    "primaryDocument": ["proxy.htm"],
                },
                "files": [{"name": "CIK0000320193-submissions-001.json"}],
            },
        }
        secondary_payload = {
            "accessionNumber": ["0000320193-22-000050"],
            "filingDate": ["2022-01-15"],
            "form": ["DEF 14A"],
            "acceptanceDateTime": ["2022-01-15T08:00:00.000Z"],
            "primaryDocument": ["proxy-2022.htm"],
        }
        recent_body = json.dumps(recent_payload).encode("utf-8")
        secondary_body = json.dumps(secondary_payload).encode("utf-8")

        seen_urls: list[str] = []

        def _fake_get_routed(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
            seen_urls.append(url)
            if url.endswith("CIK0000320193.json"):
                return 200, recent_body
            if "submissions-001.json" in url:
                return 200, secondary_body
            return 404, b""

        stats = run_sec_rebuild(
            ebull_test_conn,
            RebuildScope(instrument_id=1701),
            http_get=_fake_get_routed,
            discover=True,
        )
        ebull_test_conn.commit()

        # Aged-out accession from secondary page is now manifested.
        secondary_row = get_manifest_row(ebull_test_conn, "0000320193-22-000050")
        assert secondary_row is not None
        assert secondary_row.source == "sec_def14a"
        # The recent payload's only DEF 14A is already seeded so it
        # contributes 0 to discovery_new_manifest_rows; the secondary
        # page contributes 1. Bot review NITPICK: ``>= 1`` would pass
        # even if the secondary walk did nothing as long as recent
        # introduced a new accession. ``>= 1`` here proves the new
        # code path because the recent payload is intentionally
        # already-seeded (no recent contribution).
        assert stats.discovery_new_manifest_rows >= 1

        # Confirm the secondary URL was actually fetched (would fail
        # silently without the pagination walk).
        assert any("submissions-001.json" in u for u in seen_urls), f"rebuild did not fetch secondary page: {seen_urls}"

        # Codex pre-push: rebuild must NOT re-fetch the primary CIK
        # JSON to read ``files[]`` — that doubles the request count
        # and creates a new failure mode. Now that ``FreshnessDelta``
        # carries ``files_pages`` from the original parse, the primary
        # is fetched exactly once.
        primary_fetches = [u for u in seen_urls if u.endswith("CIK0000320193.json")]
        assert len(primary_fetches) == 1, (
            f"primary CIK JSON should be fetched exactly once; saw {len(primary_fetches)}: {primary_fetches}"
        )

    def test_discovery_isolates_secondary_page_failures(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # Codex pre-push: transport error or malformed secondary page
        # must NOT abort the rebuild — the rest of the scope still
        # drains. Two pages: page 1 raises on fetch, page 2 returns a
        # valid body.
        _seed_aapl_with_state(ebull_test_conn)

        recent_payload = {
            "cik": "320193",
            "filings": {
                "recent": {
                    "accessionNumber": ["0000320193-26-000001"],
                    "filingDate": ["2026-02-14"],
                    "form": ["DEF 14A"],
                    "acceptanceDateTime": ["2026-02-14T08:00:00.000Z"],
                    "primaryDocument": ["proxy.htm"],
                },
                "files": [
                    {"name": "CIK0000320193-submissions-001.json"},
                    {"name": "CIK0000320193-submissions-002.json"},
                ],
            },
        }
        page2_payload = {
            "accessionNumber": ["0000320193-21-000077"],
            "filingDate": ["2021-06-01"],
            "form": ["DEF 14A"],
            "acceptanceDateTime": ["2021-06-01T08:00:00.000Z"],
            "primaryDocument": ["proxy-2021.htm"],
        }
        recent_body = json.dumps(recent_payload).encode("utf-8")
        page2_body = json.dumps(page2_payload).encode("utf-8")

        def _fake_get_with_failure(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
            if url.endswith("CIK0000320193.json"):
                return 200, recent_body
            if "submissions-001.json" in url:
                raise RuntimeError("simulated transport failure on page 1")
            if "submissions-002.json" in url:
                return 200, page2_body
            return 404, b""

        # Should not raise — the failed page is isolated; the other
        # page still discovers its accession.
        stats = run_sec_rebuild(
            ebull_test_conn,
            RebuildScope(instrument_id=1701),
            http_get=_fake_get_with_failure,
            discover=True,
        )
        ebull_test_conn.commit()

        assert stats.discovery_new_manifest_rows >= 1
        page2_row = get_manifest_row(ebull_test_conn, "0000320193-21-000077")
        assert page2_row is not None
