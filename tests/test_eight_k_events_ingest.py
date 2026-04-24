"""Integration tests for ``ingest_8k_events`` (#450)."""

from __future__ import annotations

from typing import cast

import psycopg
import pytest

from app.services.eight_k_events import ingest_8k_events, list_8k_filings

pytestmark = pytest.mark.integration


class _StubFetcher:
    def __init__(self, by_url: dict[str, str | None]) -> None:
        self._by_url = by_url
        self.calls: list[str] = []

    def fetch_document_text(self, absolute_url: str) -> str | None:
        self.calls.append(absolute_url)
        return self._by_url.get(absolute_url)


def _seed_instrument(conn: psycopg.Connection[tuple], symbol: str = "APEX", iid: int = 401) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name) VALUES (%s, %s, %s) RETURNING instrument_id",
            (iid, symbol, "Apex Inc."),
        )
        row = cur.fetchone()
        assert row is not None
    conn.commit()
    return int(row[0])


def _seed_8k(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    accession: str,
    url: str,
    items: list[str],
    filing_date: str = "2026-03-15",
    filing_type: str = "8-K",
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO filing_events
                (instrument_id, filing_date, filing_type, provider,
                 provider_filing_id, primary_document_url, items)
            VALUES (%s, %s, %s, 'sec', %s, %s, %s)
            """,
            (instrument_id, filing_date, filing_type, accession, url, items),
        )
    conn.commit()


_RICH_8K = """
<html><body>
<p>FORM 8-K</p>
<p>Date of Report (Date of earliest event reported): March 15, 2026</p>
<p>(Exact name of registrant) APEX INDUSTRIES INC.</p>
<p>Commission File Number 001-12345</p>
<p>Item 1.01. Entry into a Material Definitive Agreement.</p>
<p>On March 14, 2026, the Company entered into a credit agreement
   with Acme Bank for a $500 million revolving credit facility.</p>
<p>Item 9.01. Financial Statements and Exhibits.</p>
<p>99.1 Press Release dated March 15, 2026 announcing the credit facility</p>
<p>10.1 Credit Agreement dated March 14, 2026</p>
<p>SIGNATURE</p>
<p>By: /s/ Jane Smith</p>
<p>Title: Chief Financial Officer</p>
<p>Date: March 16, 2026</p>
</body></html>
"""


class TestIngest8KEvents:
    def test_rich_filing_lands_header_items_exhibits(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn)
        _seed_8k(
            ebull_test_conn,
            instrument_id=iid,
            accession="APEX-8K-1",
            url="https://www.sec.gov/Archives/apex-8k.htm",
            items=["1.01", "9.01"],
        )
        fetcher = _StubFetcher({"https://www.sec.gov/Archives/apex-8k.htm": _RICH_8K})

        result = ingest_8k_events(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        assert result.filings_parsed == 1
        assert result.items_inserted >= 2

        filings = list_8k_filings(ebull_test_conn, instrument_id=iid)
        assert len(filings) == 1
        f = filings[0]
        assert f.document_type == "8-K"
        assert f.is_amendment is False
        assert {it.item_code for it in f.items} == {"1.01", "9.01"}
        # Labels + severity from sec_8k_item_codes lookup propagate.
        item_101 = next(it for it in f.items if it.item_code == "1.01")
        assert item_101.severity == "material"
        assert "Material Definitive Agreement" in item_101.item_label
        # Exhibits captured.
        ex = {e.exhibit_number for e in f.exhibits}
        assert "99.1" in ex
        assert "10.1" in ex

    def test_fetch_404_writes_tombstone_and_skips_next_run(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn)
        _seed_8k(
            ebull_test_conn,
            instrument_id=iid,
            accession="DEAD-8K",
            url="https://www.sec.gov/dead.htm",
            items=["8.01"],
        )
        fetcher = _StubFetcher({"https://www.sec.gov/dead.htm": None})
        ingest_8k_events(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT is_tombstone FROM eight_k_filings WHERE accession_number = %s",
                ("DEAD-8K",),
            )
            row = cur.fetchone()
            assert row is not None and row[0] is True

        # Second pass: no re-fetch.
        second_fetcher = _StubFetcher({"https://www.sec.gov/dead.htm": None})
        ingest_8k_events(ebull_test_conn, cast("object", second_fetcher))  # type: ignore[arg-type]
        assert second_fetcher.calls == []

    def test_tombstone_filings_excluded_from_reader(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn)
        _seed_8k(
            ebull_test_conn,
            instrument_id=iid,
            accession="REAL-8K",
            url="https://www.sec.gov/real.htm",
            items=["1.01"],
        )
        _seed_8k(
            ebull_test_conn,
            instrument_id=iid,
            accession="DEAD-8K-2",
            url="https://www.sec.gov/dead2.htm",
            items=["8.01"],
        )
        fetcher = _StubFetcher(
            {
                "https://www.sec.gov/real.htm": _RICH_8K,
                "https://www.sec.gov/dead2.htm": None,
            }
        )
        ingest_8k_events(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        filings = list_8k_filings(ebull_test_conn, instrument_id=iid)
        assert len(filings) == 1
        assert filings[0].accession_number == "REAL-8K"

    def test_non_8k_skipped(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn)
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO filing_events
                    (instrument_id, filing_date, filing_type, provider,
                     provider_filing_id, primary_document_url)
                VALUES (%s, CURRENT_DATE, '10-K', 'sec', 'NOT-8K',
                        'https://www.sec.gov/10k.htm')
                """,
                (iid,),
            )
        ebull_test_conn.commit()
        fetcher = _StubFetcher({})
        result = ingest_8k_events(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]
        assert result.filings_scanned == 0
        assert fetcher.calls == []
