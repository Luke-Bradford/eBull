"""Integration tests for ``ingest_business_summaries`` (#428)."""

from __future__ import annotations

from typing import cast

import psycopg
import pytest

from app.services.business_summary import (
    bootstrap_business_summaries,
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

    def test_insert_failure_rolls_back_delete_atomically(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """#449 BLOCKING regression — a failure inside the sections
        upsert must not leave an empty sections table committed. If
        the INSERT loop raises, the DELETE rolls back too. Older
        sections survive."""
        from app.services.business_summary import (
            ParsedBusinessSection,
            upsert_business_sections,
        )

        iid = _seed_instrument(ebull_test_conn, symbol="APEX", iid=205)
        # Seed a prior sections snapshot to prove it survives a failed
        # re-upsert.
        upsert_business_sections(
            ebull_test_conn,
            instrument_id=iid,
            source_accession="APEX-10K-A",
            sections=(
                ParsedBusinessSection(
                    section_order=0,
                    section_key="general",
                    section_label="General",
                    body="Existing body.",
                    cross_references=(),
                ),
            ),
        )
        ebull_test_conn.commit()

        # Now try to replace with a payload that will fail at INSERT
        # time. We force the failure by using an oversize section_key
        # value... actually the schema doesn't cap section_key length.
        # Simulate with a synthetic sentinel that psycopg rejects:
        # a section_order that violates the UNIQUE constraint (two
        # rows at the same order) — the second INSERT will raise.
        failing_payload = (
            ParsedBusinessSection(
                section_order=0,
                section_key="general",
                section_label="Replacement general",
                body="New body.",
                cross_references=(),
            ),
            ParsedBusinessSection(
                section_order=0,  # duplicate → UNIQUE violation on 2nd INSERT
                section_key="products",
                section_label="Products",
                body="Product body.",
                cross_references=(),
            ),
        )

        with pytest.raises(Exception):  # UniqueViolation expected
            upsert_business_sections(
                ebull_test_conn,
                instrument_id=iid,
                source_accession="APEX-10K-A",
                sections=failing_payload,
            )
        ebull_test_conn.rollback()  # caller would rollback on their own error path

        # The savepoint inside upsert_business_sections rolls back
        # the DELETE; the original "Existing body." row survives.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT body FROM instrument_business_summary_sections
                WHERE instrument_id = %s
                  AND source_accession = 'APEX-10K-A'
                ORDER BY section_order
                """,
                (iid,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "Existing body."

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


class TestFailureBackoffAndQuarantine:
    """#533 — failure-reason taxonomy + exponential backoff + quarantine.

    Pins the contract that the ingester records a per-failure
    category + bumps attempt_count + writes a backoff-scheduled
    next_retry_at. Hopeless cases (Part-III amendments without
    Item 1 etc.) escalate from 1d → 7d → 30d → 365d quarantine
    instead of cycling weekly forever.
    """

    _NO_ITEM_1_HTML = "<html><body><p>Item 15(a)(3) of Part IV of the Original 10-K.</p></body></html>"

    def test_first_parse_miss_records_no_item_1_marker_reason_and_1d_backoff(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        iid = _seed_instrument(ebull_test_conn, symbol="MISSITEM1", iid=601)
        _seed_10k(
            ebull_test_conn,
            instrument_id=iid,
            accession="A1",
            url="https://www.sec.gov/Archives/no_item1.htm",
        )
        fetcher = _StubFetcher({"https://www.sec.gov/Archives/no_item1.htm": self._NO_ITEM_1_HTML})
        ingest_business_summaries(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT attempt_count, last_failure_reason, next_retry_at, last_parsed_at "
                "FROM instrument_business_summary WHERE instrument_id = %s",
                (iid,),
            )
            row = cur.fetchone()
        assert row is not None
        attempt_count, reason, next_retry_at, last_parsed_at = row
        assert attempt_count == 1
        assert reason == "no_item_1_marker"
        # 1-day backoff after first miss.
        delta = (next_retry_at - last_parsed_at).total_seconds()
        assert 23 * 3600 <= delta <= 25 * 3600

    def test_repeated_misses_escalate_to_quarantine(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """4 consecutive failures push next_retry_at to NOW + 365d
        (effective quarantine). The candidate query then excludes
        the row. Pin via direct ``record_parse_attempt`` calls so the
        backoff arithmetic is unambiguous."""
        from app.services.business_summary import record_parse_attempt

        iid = _seed_instrument(ebull_test_conn, symbol="QUAR", iid=602)
        for _ in range(4):
            record_parse_attempt(
                ebull_test_conn,
                instrument_id=iid,
                source_accession="A1",
                reason="no_item_1_marker",
            )
            ebull_test_conn.commit()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT attempt_count, next_retry_at, last_parsed_at "
                "FROM instrument_business_summary WHERE instrument_id = %s",
                (iid,),
            )
            row = cur.fetchone()
        assert row is not None
        attempt_count, next_retry_at, last_parsed_at = row
        assert attempt_count == 4
        # 365-day quarantine — at least 360 days out.
        delta_days = (next_retry_at - last_parsed_at).total_seconds() / 86400
        assert 360 <= delta_days <= 366

    def test_quarantined_row_excluded_from_candidate_query(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """A row whose next_retry_at is in the future falls out of
        the candidate set even when filing_events has a fresh 10-K."""
        iid = _seed_instrument(ebull_test_conn, symbol="QUAR2", iid=603)
        _seed_10k(
            ebull_test_conn,
            instrument_id=iid,
            accession="A1",
            url="https://www.sec.gov/Archives/x.htm",
        )
        # Pre-quarantine the row.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO instrument_business_summary "
                "(instrument_id, body, source_accession, attempt_count, "
                " last_failure_reason, next_retry_at) "
                "VALUES (%s, '', 'A1', 4, 'no_item_1_marker', NOW() + INTERVAL '365 days')",
                (iid,),
            )
        ebull_test_conn.commit()

        fetcher = _StubFetcher({"https://www.sec.gov/Archives/x.htm": _ITEM_1_HTML})
        result = ingest_business_summaries(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]
        assert result.filings_scanned == 0
        assert fetcher.calls == []

    def test_successful_upsert_resets_failure_columns(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """A previously-quarantined instrument that now parses
        successfully exits quarantine cleanly (attempt_count=0,
        next_retry_at NULL, last_failure_reason NULL)."""
        iid = _seed_instrument(ebull_test_conn, symbol="EXITQ", iid=604)
        # Pre-existing tombstone with attempt_count=2.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO instrument_business_summary "
                "(instrument_id, body, source_accession, attempt_count, "
                " last_failure_reason, next_retry_at) "
                "VALUES (%s, '', 'A1', 2, 'no_item_1_marker', NOW() - INTERVAL '1 hour')",
                (iid,),
            )
        ebull_test_conn.commit()

        # Schedule a fresh 10-K with valid Item 1 (different accession
        # so the candidate query picks it up via the
        # source_accession <> stored branch — even though
        # next_retry_at has elapsed).
        _seed_10k(
            ebull_test_conn,
            instrument_id=iid,
            accession="A2",
            url="https://www.sec.gov/Archives/exitq.htm",
            filing_date="2026-04-01",
        )
        fetcher = _StubFetcher({"https://www.sec.gov/Archives/exitq.htm": _ITEM_1_HTML})
        ingest_business_summaries(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT body, attempt_count, last_failure_reason, next_retry_at "
                "FROM instrument_business_summary WHERE instrument_id = %s",
                (iid,),
            )
            row = cur.fetchone()
        assert row is not None
        body, attempt_count, reason, next_retry_at = row
        assert body and "global diversified" in body
        assert attempt_count == 0
        assert reason is None
        assert next_retry_at is None

    def test_upsert_exception_path_rolls_back_then_records_failure(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        """Pin the upsert-exception ordering: rollback → record →
        commit. If ``upsert_business_summary`` raises a DB error,
        the connection is in aborted-transaction state and the
        immediately-following ``record_parse_attempt`` would raise
        ``InFailedSqlTransaction`` without a preceding rollback.

        Forces the failure by monkey-patching
        ``upsert_business_summary`` to raise psycopg.errors.DataError.
        Then asserts the failure row exists with attempt_count=1 and
        reason='upsert_exception' — proving the post-rollback record
        + commit path actually wrote.
        """
        from unittest.mock import patch

        import psycopg.errors

        from app.services import business_summary as bs_module

        iid = _seed_instrument(ebull_test_conn, symbol="UPSERTRAISE", iid=607)
        _seed_10k(
            ebull_test_conn,
            instrument_id=iid,
            accession="A1",
            url="https://www.sec.gov/Archives/raise.htm",
        )
        fetcher = _StubFetcher({"https://www.sec.gov/Archives/raise.htm": _ITEM_1_HTML})

        def boom(*args: object, **kwargs: object) -> bool:
            # Synthesise a real psycopg DataError so the connection
            # actually transitions to aborted-transaction state — the
            # exact failure mode the rollback exists to guard against.
            with ebull_test_conn.cursor() as cur:
                try:
                    cur.execute("SELECT CAST('not_a_number' AS INTEGER)")
                except psycopg.errors.InvalidTextRepresentation:
                    pass
            raise psycopg.errors.DataError("synthetic upsert failure")

        with patch.object(bs_module, "upsert_business_summary", side_effect=boom):
            ingest_business_summaries(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT attempt_count, last_failure_reason FROM instrument_business_summary WHERE instrument_id = %s",
                (iid,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 1
        assert row[1] == "upsert_exception"

    def test_admin_reset_requeues_failed_row_for_next_ingest(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Codex residual-risk pin: after the admin reset endpoint
        clears the failure columns and stamps ``next_retry_at = NOW()``,
        the next ``ingest_business_summaries`` run picks the row up
        and re-attempts the same accession. Without ``NOW()`` (e.g.
        plain ``NULL``) the row would disappear from the dashboard
        without ever being retried."""
        iid = _seed_instrument(ebull_test_conn, symbol="REQUEUE", iid=606)
        _seed_10k(
            ebull_test_conn,
            instrument_id=iid,
            accession="A1",
            url="https://www.sec.gov/Archives/requeue.htm",
        )
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO instrument_business_summary "
                "(instrument_id, body, source_accession, attempt_count, "
                " last_failure_reason, next_retry_at) "
                "VALUES (%s, '', 'A1', 4, 'no_item_1_marker', NOW() + INTERVAL '365 days')",
                (iid,),
            )
        ebull_test_conn.commit()

        skip_fetcher = _StubFetcher({"https://www.sec.gov/Archives/requeue.htm": _ITEM_1_HTML})
        skipped = ingest_business_summaries(ebull_test_conn, cast("object", skip_fetcher))  # type: ignore[arg-type]
        assert skipped.filings_scanned == 0

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                UPDATE instrument_business_summary
                   SET attempt_count       = 0,
                       last_failure_reason = NULL,
                       next_retry_at       = NOW()
                 WHERE instrument_id  = %s
                   AND next_retry_at IS NOT NULL
                """,
                (iid,),
            )
        ebull_test_conn.commit()

        good_fetcher = _StubFetcher({"https://www.sec.gov/Archives/requeue.htm": _ITEM_1_HTML})
        result = ingest_business_summaries(ebull_test_conn, cast("object", good_fetcher))  # type: ignore[arg-type]
        assert result.filings_scanned == 1
        assert result.rows_inserted + result.rows_updated == 1

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT body, attempt_count, last_failure_reason, next_retry_at "
                "FROM instrument_business_summary WHERE instrument_id = %s",
                (iid,),
            )
            row = cur.fetchone()
        assert row is not None
        body, attempt_count, reason, next_retry_at = row
        assert body and len(body) > 100
        assert attempt_count == 0
        assert reason is None
        assert next_retry_at is None

    def test_fetch_5xx_classified_as_fetch_http_5xx(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """An HTTPError with response.status_code=503 maps to
        ``fetch_http_5xx`` reason, not ``fetch_other``."""
        from types import SimpleNamespace

        class _RaisingFetcher:
            def fetch_document_text(self, absolute_url: str) -> str | None:
                err = Exception("boom")
                err.response = SimpleNamespace(status_code=503)  # type: ignore[attr-defined]
                raise err

        iid = _seed_instrument(ebull_test_conn, symbol="HTTP5", iid=605)
        _seed_10k(
            ebull_test_conn,
            instrument_id=iid,
            accession="A1",
            url="https://www.sec.gov/Archives/x.htm",
        )
        ingest_business_summaries(ebull_test_conn, cast("object", _RaisingFetcher()))  # type: ignore[arg-type]

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT last_failure_reason FROM instrument_business_summary WHERE instrument_id = %s",
                (iid,),
            )
            row = cur.fetchone()
        assert row is not None and row[0] == "fetch_http_5xx"


class TestTenKAFallback:
    """#534 — 10-K/A amendments missing Item 1 fall back to the most
    recent prior plain 10-K. Without the fallback, every Part-III
    amendment instrument permanently lost its Item 1 narrative."""

    _PART_III_HTML = (
        "<html><body><p>Item 15(a)(3) of Part IV of the Original 10-K. "
        "Item 10. Directors. Item 11. Executive Compensation.</p></body></html>"
    )

    def test_amendment_no_item1_falls_back_to_prior_plain_10k(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, symbol="FBACK1", iid=701)
        # Original 10-K with full Item 1.
        _seed_10k(
            ebull_test_conn,
            instrument_id=iid,
            accession="ORIG-10K",
            url="https://www.sec.gov/Archives/orig.htm",
            filing_date="2025-02-15",
        )
        # Later 10-K/A Part-III amendment missing Item 1.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO filing_events (instrument_id, filing_date, filing_type, provider, "
                "provider_filing_id, primary_document_url) VALUES (%s, '2026-03-01', '10-K/A', 'sec', "
                "'AMEND-A', 'https://www.sec.gov/Archives/amend.htm')",
                (iid,),
            )
        ebull_test_conn.commit()

        fetcher = _StubFetcher(
            {
                "https://www.sec.gov/Archives/amend.htm": self._PART_III_HTML,
                "https://www.sec.gov/Archives/orig.htm": _ITEM_1_HTML,
            }
        )
        result = ingest_business_summaries(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]
        assert result.rows_inserted + result.rows_updated == 1

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT body, source_accession, attempt_count, last_failure_reason "
                "FROM instrument_business_summary WHERE instrument_id = %s",
                (iid,),
            )
            row = cur.fetchone()
        assert row is not None
        body, source_accession, attempt_count, reason = row
        assert body and "global diversified" in body
        # Stored under the fallback's accession, NOT the amendment's.
        assert source_accession == "ORIG-10K"
        assert attempt_count == 0
        assert reason is None

    def test_amendment_no_item1_no_fallback_falls_through_to_tombstone(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        """A 10-K/A with no prior plain 10-K available still records
        a no_item_1_marker tombstone (no silent skip)."""
        iid = _seed_instrument(ebull_test_conn, symbol="FBACK2", iid=702)
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO filing_events (instrument_id, filing_date, filing_type, provider, "
                "provider_filing_id, primary_document_url) VALUES (%s, '2026-03-01', '10-K/A', 'sec', "
                "'AMEND-B', 'https://www.sec.gov/Archives/amend2.htm')",
                (iid,),
            )
        ebull_test_conn.commit()

        fetcher = _StubFetcher({"https://www.sec.gov/Archives/amend2.htm": self._PART_III_HTML})
        ingest_business_summaries(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT body, source_accession, last_failure_reason "
                "FROM instrument_business_summary WHERE instrument_id = %s",
                (iid,),
            )
            row = cur.fetchone()
        assert row is not None
        body, source_accession, reason = row
        assert body == ""
        assert source_accession == "AMEND-B"
        assert reason == "no_item_1_marker"


class TestBootstrapDrain:
    """#535 — bootstrap drain mode loops the standard ingester until
    the queue empties or the deadline elapses. Used for first-time
    backfill of the SEC-CIK universe."""

    def test_drains_multiple_chunks_until_queue_empty(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Seed 3 instruments + 3 10-Ks, run bootstrap with chunk_limit=2.
        Loop must execute twice (chunk1=2, chunk2=1, chunk3=0 = stop)
        and persist all 3 bodies."""
        urls = {}
        for i in range(3):
            iid = _seed_instrument(ebull_test_conn, symbol=f"BS{i}", iid=801 + i)
            url = f"https://www.sec.gov/Archives/bs{i}.htm"
            _seed_10k(
                ebull_test_conn,
                instrument_id=iid,
                accession=f"BS-{i}",
                url=url,
                filing_date=f"2026-0{i + 1}-15",
            )
            urls[url] = _ITEM_1_HTML

        fetcher = _StubFetcher(urls)
        result = bootstrap_business_summaries(
            ebull_test_conn,
            cast("object", fetcher),  # type: ignore[arg-type]
            chunk_limit=2,
            max_runtime_seconds=30,
        )
        assert result.rows_inserted == 3
        # 2 + 1 from the candidate cycles (the third call sees zero).
        assert result.filings_scanned == 3

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM instrument_business_summary WHERE instrument_id IN (801,802,803) AND body != ''"
            )
            row = cur.fetchone()
        assert row is not None and row[0] == 3

    def test_idempotent_repeat_is_a_noop(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """A second bootstrap run on a drained queue performs zero
        fetches and reports zero scanned."""
        iid = _seed_instrument(ebull_test_conn, symbol="BSIDEM", iid=810)
        _seed_10k(
            ebull_test_conn,
            instrument_id=iid,
            accession="A1",
            url="https://www.sec.gov/Archives/idem.htm",
        )
        fetcher = _StubFetcher({"https://www.sec.gov/Archives/idem.htm": _ITEM_1_HTML})
        bootstrap_business_summaries(
            ebull_test_conn,
            cast("object", fetcher),  # type: ignore[arg-type]
            chunk_limit=10,
            max_runtime_seconds=30,
        )
        rerun_fetcher = _StubFetcher({"https://www.sec.gov/Archives/idem.htm": _ITEM_1_HTML})
        result = bootstrap_business_summaries(
            ebull_test_conn,
            cast("object", rerun_fetcher),  # type: ignore[arg-type]
            chunk_limit=10,
            max_runtime_seconds=30,
        )
        assert result.filings_scanned == 0
        assert rerun_fetcher.calls == []
