"""Integration tests for ``ingest_filing_documents`` (#452)."""

from __future__ import annotations

from typing import cast

import psycopg
import pytest

from app.services.filing_documents import (
    ingest_filing_documents,
    list_filing_documents,
)

pytestmark = pytest.mark.integration


class _StubIndexFetcher:
    def __init__(self, by_accession: dict[str, dict[str, object] | None]) -> None:
        self._by = by_accession
        self.calls: list[str] = []

    def fetch_filing_index(self, accession: str) -> dict[str, object] | None:
        self.calls.append(accession)
        return self._by.get(accession)


def _seed_instrument(conn: psycopg.Connection[tuple], iid: int = 501) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name) VALUES (%s, %s, %s) RETURNING instrument_id",
            (iid, "APEX", "Apex Inc."),
        )
        row = cur.fetchone()
        assert row is not None
    conn.commit()
    return int(row[0])


def _seed_filing(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    accession: str,
    url: str = "https://www.sec.gov/doc.htm",
    filing_date: str = "2026-04-01",
    filing_type: str = "10-K",
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO filing_events
                (instrument_id, filing_date, filing_type, provider,
                 provider_filing_id, primary_document_url)
            VALUES (%s, %s, %s, 'sec', %s, %s)
            RETURNING filing_event_id
            """,
            (instrument_id, filing_date, filing_type, accession, url),
        )
        row = cur.fetchone()
        assert row is not None
    conn.commit()
    return int(row[0])


_INDEX_JSON = {
    "cik": "320193",
    "form": "10-K",
    "primaryDocument": "apex-10k.htm",
    "items": [
        {"name": "apex-10k.htm", "type": "10-K", "description": "10-K", "size": 999000},
        {"name": "ex-21.htm", "type": "EX-21", "description": "Subsidiaries", "size": 2000},
        {"name": "ex-99-1.htm", "type": "EX-99.1", "description": "Press release", "size": 5000},
    ],
}


class TestIngestFilingDocuments:
    def test_documents_land_per_filing(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn)
        fid = _seed_filing(
            ebull_test_conn,
            instrument_id=iid,
            accession="APEX-10K-1",
        )
        fetcher = _StubIndexFetcher({"APEX-10K-1": _INDEX_JSON})

        result = ingest_filing_documents(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        assert result.filings_parsed == 1
        assert result.documents_inserted == 3

        docs = list_filing_documents(ebull_test_conn, filing_event_id=fid)
        assert len(docs) == 3
        # Primary document always sorts first.
        assert docs[0].is_primary is True
        assert docs[0].document_name == "apex-10k.htm"
        ex_types = {d.document_type for d in docs}
        assert "EX-21" in ex_types
        assert "EX-99.1" in ex_types

    def test_rerun_skips_filings_with_existing_children(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, iid=502)
        _seed_filing(
            ebull_test_conn,
            instrument_id=iid,
            accession="APEX-10K-2",
        )
        fetcher = _StubIndexFetcher({"APEX-10K-2": _INDEX_JSON})
        ingest_filing_documents(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        # Second pass: existing child rows mean the filing is no
        # longer a candidate.
        second = _StubIndexFetcher({"APEX-10K-2": _INDEX_JSON})
        ingest_filing_documents(ebull_test_conn, cast("object", second))  # type: ignore[arg-type]
        assert second.calls == []

    def test_fetch_404_increments_fetch_errors_without_raising(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        iid = _seed_instrument(ebull_test_conn, iid=503)
        _seed_filing(
            ebull_test_conn,
            instrument_id=iid,
            accession="DEAD-ACC",
        )
        fetcher = _StubIndexFetcher({"DEAD-ACC": None})
        result = ingest_filing_documents(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]
        assert result.fetch_errors == 1
        assert result.filings_parsed == 0

    def test_non_sec_filings_ignored(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, iid=504)
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO filing_events
                    (instrument_id, filing_date, filing_type, provider,
                     provider_filing_id, primary_document_url)
                VALUES (%s, CURRENT_DATE, '10-K', 'companies_house',
                        'CH-1', 'https://example.com/x.pdf')
                """,
                (iid,),
            )
        ebull_test_conn.commit()
        fetcher = _StubIndexFetcher({})
        result = ingest_filing_documents(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]
        assert result.filings_scanned == 0
        assert fetcher.calls == []
