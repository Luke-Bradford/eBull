"""Integration tests for ``ingest_business_summaries`` (#428)."""

from __future__ import annotations

from typing import cast

import psycopg
import pytest

from app.services.business_summary import (
    get_business_sections,
    get_business_summary,
    ingest_business_summaries,
)

pytestmark = pytest.mark.integration


class _StubFetcher:
    def __init__(self, by_url: dict[str, str | None]) -> None:
        self._by_url = by_url
        self.calls: list[str] = []

    def fetch_document_text(self, absolute_url: str) -> str | None:
        self.calls.append(absolute_url)
        return self._by_url.get(absolute_url)


def _seed_instrument(conn: psycopg.Connection[tuple], symbol: str = "MMM", iid: int = 77) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name) VALUES (%s, %s, %s) RETURNING instrument_id",
            (iid, symbol, "Test Co"),
        )
        row = cur.fetchone()
        assert row is not None
    conn.commit()
    return int(row[0])


def _seed_10k(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    accession: str,
    url: str,
    filing_date: str = "2026-02-15",
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO filing_events
                (instrument_id, filing_date, filing_type, provider,
                 provider_filing_id, primary_document_url)
            VALUES (%s, %s, '10-K', 'sec', %s, %s)
            """,
            (instrument_id, filing_date, accession, url),
        )
    conn.commit()


_ITEM_1_HTML = """
<html><body>
<h2>Table of Contents</h2>
<p>Item 1. Business .... 3</p>
<p>Item 1A. Risk Factors ... 10</p>
<h2>Item 1. Business</h2>
<p>The Company is a global diversified manufacturer of specialty
   materials serving aerospace, automotive, healthcare, and
   consumer end markets. The Company operates through four
   reportable segments, each with its own leadership, brands, and
   customer base. Our products are sold in over 70 countries.</p>
<h2>Item 1A. Risk Factors</h2>
<p>Risk factors follow.</p>
</body></html>
"""


class TestIngestBusinessSummaries:
    def test_happy_path_inserts(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn)
        _seed_10k(
            ebull_test_conn,
            instrument_id=iid,
            accession="0000066740-26-000001",
            url="https://www.sec.gov/Archives/mmm-10k.htm",
        )
        fetcher = _StubFetcher({"https://www.sec.gov/Archives/mmm-10k.htm": _ITEM_1_HTML})

        result = ingest_business_summaries(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        assert result.filings_scanned == 1
        assert result.rows_inserted == 1
        body = get_business_summary(ebull_test_conn, instrument_id=iid)
        assert body is not None
        assert "global diversified manufacturer" in body

    def test_superseding_10k_replaces_body(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """A newer 10-K with a different accession updates the row."""
        iid = _seed_instrument(ebull_test_conn)
        # Old filing first.
        _seed_10k(
            ebull_test_conn,
            instrument_id=iid,
            accession="OLD-ACC",
            url="https://www.sec.gov/Archives/old.htm",
            filing_date="2024-02-15",
        )
        # New filing second — should win on DISTINCT ON ordering
        # (filing_date DESC).
        _seed_10k(
            ebull_test_conn,
            instrument_id=iid,
            accession="NEW-ACC",
            url="https://www.sec.gov/Archives/new.htm",
            filing_date="2026-02-15",
        )
        fetcher = _StubFetcher(
            {
                "https://www.sec.gov/Archives/new.htm": _ITEM_1_HTML,
                "https://www.sec.gov/Archives/old.htm": "<html>old boring</html>",
            }
        )

        result = ingest_business_summaries(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]
        assert result.rows_inserted == 1
        # Only the newer URL was fetched.
        assert fetcher.calls == ["https://www.sec.gov/Archives/new.htm"]

    def test_rerun_within_ttl_skipped(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn)
        _seed_10k(
            ebull_test_conn,
            instrument_id=iid,
            accession="A1",
            url="https://www.sec.gov/Archives/mmm-10k.htm",
        )
        fetcher = _StubFetcher({"https://www.sec.gov/Archives/mmm-10k.htm": _ITEM_1_HTML})
        ingest_business_summaries(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        second_fetcher = _StubFetcher({"https://www.sec.gov/Archives/mmm-10k.htm": _ITEM_1_HTML})
        second = ingest_business_summaries(ebull_test_conn, cast("object", second_fetcher))  # type: ignore[arg-type]

        assert second.filings_scanned == 0
        assert second_fetcher.calls == []

    def test_parse_miss_writes_tombstone_and_ttl_gates(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """A 10-K whose primary doc has no extractable Item 1 writes a
        tombstone (empty-body sentinel) and the next run skips via
        TTL — no second-day SEC re-fetch."""
        iid = _seed_instrument(ebull_test_conn)
        _seed_10k(
            ebull_test_conn,
            instrument_id=iid,
            accession="A1",
            url="https://www.sec.gov/Archives/empty.htm",
        )
        fetcher = _StubFetcher(
            {"https://www.sec.gov/Archives/empty.htm": "<html><body><p>No disclosures.</p></body></html>"}
        )
        result = ingest_business_summaries(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]
        assert result.parse_misses == 1
        # Reader returns None for the tombstone row so the UI still
        # falls through to yfinance.
        assert get_business_summary(ebull_test_conn, instrument_id=iid) is None

        # Second pass must NOT re-fetch.
        second_fetcher = _StubFetcher(
            {"https://www.sec.gov/Archives/empty.htm": "<html><body><p>No disclosures.</p></body></html>"}
        )
        second = ingest_business_summaries(ebull_test_conn, cast("object", second_fetcher))  # type: ignore[arg-type]
        assert second.filings_scanned == 0
        assert second_fetcher.calls == []

    def test_10k_amendment_included(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Codex #428 H1 regression — 10-K/A amendments must be
        considered, not just plain 10-K. Amended annual reports
        carry the authoritative restated narrative."""
        iid = _seed_instrument(ebull_test_conn)
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO filing_events
                    (instrument_id, filing_date, filing_type, provider,
                     provider_filing_id, primary_document_url)
                VALUES (%s, '2026-03-01', '10-K/A', 'sec', 'A-AMEND',
                        'https://www.sec.gov/Archives/amend.htm')
                """,
                (iid,),
            )
        ebull_test_conn.commit()
        fetcher = _StubFetcher({"https://www.sec.gov/Archives/amend.htm": _ITEM_1_HTML})
        result = ingest_business_summaries(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]
        assert result.filings_scanned == 1
        assert result.rows_inserted == 1

    def test_tombstone_preserves_existing_body_on_fetch_error(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Codex #428 H2 / #446 BLOCKING pattern — a later 10-K whose
        fetch fails must NOT overwrite the body extracted from an
        earlier successful 10-K parse. Tombstone UPDATE path touches
        only ``source_accession`` + ``last_parsed_at``."""
        iid = _seed_instrument(ebull_test_conn)
        # First pass: good 10-K, body stored.
        _seed_10k(
            ebull_test_conn,
            instrument_id=iid,
            accession="GOOD-ACC",
            url="https://www.sec.gov/Archives/good.htm",
            filing_date="2024-02-15",
        )
        ingest_business_summaries(
            ebull_test_conn,
            cast("object", _StubFetcher({"https://www.sec.gov/Archives/good.htm": _ITEM_1_HTML})),  # type: ignore[arg-type]
        )
        first_body = get_business_summary(ebull_test_conn, instrument_id=iid)
        assert first_body is not None and "global diversified" in first_body

        # Second pass: newer 10-K/A arrives but fetch returns 404.
        # Tombstone must not clobber the stored body.
        _seed_10k(
            ebull_test_conn,
            instrument_id=iid,
            accession="NEW-ACC",
            url="https://www.sec.gov/Archives/dead.htm",
            filing_date="2026-03-15",
        )
        dead_fetcher = _StubFetcher({"https://www.sec.gov/Archives/dead.htm": None})
        ingest_business_summaries(ebull_test_conn, cast("object", dead_fetcher))  # type: ignore[arg-type]

        preserved = get_business_summary(ebull_test_conn, instrument_id=iid)
        assert preserved is not None
        assert "global diversified" in preserved

    def test_first_time_fetch_error_tombstones(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """A first-time attempt that fails (no prior row) must write
        a tombstone so TTL gates re-fetch at weekly. Mirrors the
        Codex #446 pattern for dividend_calendar."""
        iid = _seed_instrument(ebull_test_conn)
        _seed_10k(
            ebull_test_conn,
            instrument_id=iid,
            accession="A1",
            url="https://www.sec.gov/Archives/gone.htm",
        )
        fetcher = _StubFetcher({"https://www.sec.gov/Archives/gone.htm": None})
        result = ingest_business_summaries(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]
        assert result.fetch_errors == 1

        # Reader returns None (empty body = tombstone).
        assert get_business_summary(ebull_test_conn, instrument_id=iid) is None

        # Second immediate pass must NOT re-fetch.
        second_fetcher = _StubFetcher({"https://www.sec.gov/Archives/gone.htm": None})
        second = ingest_business_summaries(ebull_test_conn, cast("object", second_fetcher))  # type: ignore[arg-type]
        assert second.filings_scanned == 0
        assert second_fetcher.calls == []

    def test_non_10k_skipped(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn)
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO filing_events
                    (instrument_id, filing_date, filing_type, provider,
                     provider_filing_id, primary_document_url)
                VALUES (%s, CURRENT_DATE, '10-Q', 'sec', 'Q-ACC',
                        'https://www.sec.gov/Archives/q.htm')
                """,
                (iid,),
            )
        ebull_test_conn.commit()
        fetcher = _StubFetcher({})
        result = ingest_business_summaries(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]
        assert result.filings_scanned == 0
        assert fetcher.calls == []


class TestBusinessSectionsIngest:
    """#449 — sections table populated alongside the blob."""

    _RICH_10K = """
    <html><body>
    <h2>Item 1. Business</h2>
    <p>Apex Industries is a global manufacturer of specialty chemicals
       serving aerospace, automotive, and healthcare end markets.</p>
    <h3>Products</h3>
    <p>We sell coatings, films, and adhesives. See Item 7 for segment
       breakdowns.</p>
    <h3>Competition</h3>
    <p>Competitors include Acme Corp and GlobalChem Inc.</p>
    <h3>Human Capital Resources</h3>
    <p>As of year-end we employed 12,400 people across 24 countries.</p>
    <h3>Government Regulation</h3>
    <p>Operations are subject to environmental rules per Note 15.</p>
    <h2>Item 1A. Risk Factors</h2>
    <p>The following risks apply.</p>
    </body></html>
    """

    def test_sections_land_alongside_blob(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, symbol="APEX", iid=201)
        _seed_10k(
            ebull_test_conn,
            instrument_id=iid,
            accession="APEX-10K-1",
            url="https://www.sec.gov/Archives/apex.htm",
        )
        fetcher = _StubFetcher({"https://www.sec.gov/Archives/apex.htm": self._RICH_10K})
        result = ingest_business_summaries(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]
        assert result.rows_inserted == 1

        # Blob view still populated (back-compat).
        assert get_business_summary(ebull_test_conn, instrument_id=iid) is not None

        # Sections view: general + 4 subsections, all keys canonical.
        sections = get_business_sections(ebull_test_conn, instrument_id=iid)
        assert len(sections) >= 5
        keys = {s.section_key for s in sections}
        assert "general" in keys
        assert "products" in keys
        assert "competition" in keys
        assert "human_capital" in keys
        assert "regulatory" in keys
        # Verbatim label preserved.
        hc = next(s for s in sections if s.section_key == "human_capital")
        assert hc.section_label == "Human Capital Resources"
        # Cross-references captured per section.
        products = next(s for s in sections if s.section_key == "products")
        prod_refs = {(r.reference_type, r.target) for r in products.cross_references}
        assert ("item", "Item 7") in prod_refs

    def test_reparse_supersedes_prior_sections(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """A later 10-K for the same instrument must replace the
        stored sections snapshot, not leak stale rows."""
        iid = _seed_instrument(ebull_test_conn, symbol="APEX", iid=202)
        _seed_10k(
            ebull_test_conn,
            instrument_id=iid,
            accession="APEX-10K-OLD",
            url="https://www.sec.gov/Archives/old.htm",
            filing_date="2024-02-15",
        )
        _seed_10k(
            ebull_test_conn,
            instrument_id=iid,
            accession="APEX-10K-NEW",
            url="https://www.sec.gov/Archives/new.htm",
            filing_date="2026-02-15",
        )
        fetcher = _StubFetcher(
            {
                "https://www.sec.gov/Archives/old.htm": self._RICH_10K,
                "https://www.sec.gov/Archives/new.htm": self._RICH_10K.replace("Products", "Offerings"),
            }
        )
        # First pass: newest-first candidate picks NEW.
        ingest_business_summaries(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        sections = get_business_sections(ebull_test_conn, instrument_id=iid)
        # Reader scopes to the most recent source_accession.
        assert all(s.source_accession == "APEX-10K-NEW" for s in sections)
        # Canonical key for a custom "Offerings" heading is "other"
        # with the verbatim label preserved.
        offering_sections = [s for s in sections if s.section_label == "Offerings"]
        assert len(offering_sections) == 1
        assert offering_sections[0].section_key == "other"
