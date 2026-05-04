"""Integration tests for the DEF 14A ingester (#769 PR 2).

The service interacts with three boundaries:
  1. SEC HTTP — abstracted as :class:`SecDocFetcher` so tests
     substitute a deterministic in-memory fake.
  2. Postgres — real ``ebull_test`` DB.
  3. The pure parser from #769 PR 1 — exercised end-to-end.

Each test seeds the inputs (an instrument with an SEC profile, a
filing_events row for DEF 14A, a fake fetcher mapped to the
primary doc URL) and asserts the canonical row state.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import psycopg
import psycopg.rows
import pytest

from app.services.def14a_ingest import (
    bootstrap_def14a,
    discover_pending_def14a,
    ingest_def14a,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Fixture builders
# ---------------------------------------------------------------------------


def _proxy_html_with_table() -> str:
    """Minimal proxy with a recognisable beneficial-ownership table."""
    return """<!DOCTYPE html>
<html><body>
<h1>Annual Meeting</h1>
<h2>Security Ownership of Certain Beneficial Owners and Management</h2>
<p>The following table sets forth the beneficial ownership as of March 1, 2026.</p>
<table>
  <tr>
    <th>Name and Address of Beneficial Owner</th>
    <th>Number of Shares Beneficially Owned</th>
    <th>Percent of Class</th>
  </tr>
  <tr><td>John Doe, CEO</td><td>1,500,000</td><td>5.5%</td></tr>
  <tr><td>Jane Smith, Director</td><td>250,000</td><td>1.0%</td></tr>
  <tr><td>Vanguard Group, Inc.</td><td>3,000,000</td><td>11.0%</td></tr>
</table>
</body></html>
"""


def _proxy_html_unrecognisable_table() -> str:
    """Proxy with no recognisable beneficial-ownership table."""
    return """<!DOCTYPE html>
<html><body>
<h1>Annual Meeting</h1>
<table>
  <tr><th>Auditor</th><th>Term</th></tr>
  <tr><td>Acme LLP</td><td>1 year</td></tr>
</table>
</body></html>
"""


class _InMemoryFetcher:
    def __init__(self, payloads: dict[str, str | None]) -> None:
        self._payloads = payloads
        self.calls: list[str] = []

    def fetch_document_text(self, absolute_url: str) -> str | None:
        self.calls.append(absolute_url)
        return self._payloads.get(absolute_url)


def _seed_instrument(conn: psycopg.Connection[tuple], *, iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} Inc"),
    )


def _seed_sec_profile(conn: psycopg.Connection[tuple], *, instrument_id: int, cik: str) -> None:
    conn.execute(
        """
        INSERT INTO instrument_sec_profile (instrument_id, cik)
        VALUES (%s, %s)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (instrument_id, cik),
    )


def _seed_filing_event(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    accession: str,
    filing_date: date,
    primary_document_url: str,
    filing_type: str = "DEF 14A",
) -> None:
    conn.execute(
        """
        INSERT INTO filing_events (
            instrument_id, filing_date, filing_type,
            provider, provider_filing_id, primary_document_url
        ) VALUES (%s, %s, %s, 'sec', %s, %s)
        ON CONFLICT (provider, provider_filing_id) DO NOTHING
        """,
        (instrument_id, filing_date, filing_type, accession, primary_document_url),
    )


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


class TestDiscoverPendingDef14a:
    def test_returns_empty_when_no_filings(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        result = discover_pending_def14a(ebull_test_conn)
        assert result == []

    def test_returns_def14a_filings_only(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=769_001, symbol="TEST")
        _seed_filing_event(
            conn,
            instrument_id=769_001,
            accession="0001234567-25-000001",
            filing_date=date(2026, 3, 15),
            primary_document_url="https://www.sec.gov/test/proxy.htm",
            filing_type="DEF 14A",
        )
        _seed_filing_event(
            conn,
            instrument_id=769_001,
            accession="0001234567-25-000002",
            filing_date=date(2026, 2, 1),
            primary_document_url="https://www.sec.gov/test/10k.htm",
            filing_type="10-K",
        )
        _seed_filing_event(
            conn,
            instrument_id=769_001,
            accession="0001234567-25-000003",
            filing_date=date(2026, 4, 1),
            primary_document_url="https://www.sec.gov/test/proxy-additional.htm",
            filing_type="DEFA14A",
        )
        conn.commit()

        result = discover_pending_def14a(conn)
        accessions = [r.accession_number for r in result]
        assert "0001234567-25-000001" in accessions
        assert "0001234567-25-000003" in accessions
        assert "0001234567-25-000002" not in accessions  # 10-K excluded
        # Ordered by filing_date DESC — DEFA14A (2026-04-01) before
        # DEF 14A (2026-03-15).
        assert accessions[0] == "0001234567-25-000003"

    def test_skips_filings_without_primary_document_url(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=769_002, symbol="TEST")
        conn.execute(
            """
            INSERT INTO filing_events (
                instrument_id, filing_date, filing_type,
                provider, provider_filing_id, primary_document_url
            ) VALUES (%s, %s, %s, 'sec', %s, NULL)
            """,
            (769_002, date(2026, 3, 15), "DEF 14A", "0001234567-25-000010"),
        )
        conn.commit()

        result = discover_pending_def14a(conn)
        assert result == []

    def test_skips_already_attempted_accessions(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=769_003, symbol="TEST")
        _seed_filing_event(
            conn,
            instrument_id=769_003,
            accession="0001234567-25-000020",
            filing_date=date(2026, 3, 15),
            primary_document_url="https://www.sec.gov/test/proxy.htm",
        )
        # Pre-populate the ingest log to mark this accession attempted.
        conn.execute(
            """
            INSERT INTO def14a_ingest_log (accession_number, issuer_cik, status, rows_inserted)
            VALUES ('0001234567-25-000020', '0001234567', 'success', 3)
            """,
        )
        conn.commit()

        result = discover_pending_def14a(conn)
        assert result == []

    def test_instrument_id_filter_scopes_discovery(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=769_004, symbol="A")
        _seed_instrument(conn, iid=769_005, symbol="B")
        _seed_filing_event(
            conn,
            instrument_id=769_004,
            accession="A-25-000001",
            filing_date=date(2026, 3, 15),
            primary_document_url="https://www.sec.gov/A/proxy.htm",
        )
        _seed_filing_event(
            conn,
            instrument_id=769_005,
            accession="B-25-000001",
            filing_date=date(2026, 3, 15),
            primary_document_url="https://www.sec.gov/B/proxy.htm",
        )
        conn.commit()

        scoped = discover_pending_def14a(conn, instrument_id=769_004)
        assert len(scoped) == 1
        assert scoped[0].accession_number == "A-25-000001"


# ---------------------------------------------------------------------------
# End-to-end ingest
# ---------------------------------------------------------------------------


class TestIngestDef14a:
    @pytest.fixture
    def _setup(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> psycopg.Connection[tuple]:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=769_100, symbol="AAPL")
        _seed_sec_profile(conn, instrument_id=769_100, cik="0000320193")
        conn.commit()
        return conn

    def test_happy_path_ingests_three_holders(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        conn = _setup
        url = "https://www.sec.gov/test/proxy.htm"
        _seed_filing_event(
            conn,
            instrument_id=769_100,
            accession="0001234567-25-000001",
            filing_date=date(2026, 3, 15),
            primary_document_url=url,
        )
        conn.commit()
        fetcher = _InMemoryFetcher({url: _proxy_html_with_table()})

        summary = ingest_def14a(conn, fetcher)

        assert summary.accessions_seen == 1
        assert summary.accessions_succeeded == 1
        assert summary.accessions_partial == 0
        assert summary.accessions_failed == 0
        assert summary.accessions_ingested == 1  # back-compat alias
        assert summary.rows_inserted == 3
        assert summary.rows_updated == 0

        # Holdings persisted with parsed values.
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT holder_name, holder_role, shares, percent_of_class, as_of_date, issuer_cik
                FROM def14a_beneficial_holdings
                WHERE instrument_id = %s
                ORDER BY shares DESC
                """,
                (769_100,),
            )
            rows = cur.fetchall()
        assert [r["holder_name"] for r in rows] == [
            "Vanguard Group, Inc.",
            "John Doe, CEO",
            "Jane Smith, Director",
        ]
        assert rows[1]["holder_role"] == "officer"
        assert rows[2]["holder_role"] == "director"
        assert rows[0]["shares"] == Decimal("3000000")
        assert rows[0]["percent_of_class"] == Decimal("11.0")
        assert rows[0]["as_of_date"] == date(2026, 3, 1)
        assert rows[0]["issuer_cik"] == "0000320193"

        # Ingest log records success.
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT status, rows_inserted FROM def14a_ingest_log WHERE accession_number = %s",
                ("0001234567-25-000001",),
            )
            log = cur.fetchone()
        assert log is not None
        assert log["status"] == "success"
        assert log["rows_inserted"] == 3

    def test_raw_payload_persisted_for_def14a_body(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """DEF 14A ingester must persist the proxy body to
        ``filing_raw_documents`` before parsing — operator audit
        2026-05-03 + PR #808 contract."""
        from app.services import raw_filings

        conn = _setup
        url = "https://www.sec.gov/test/proxy_raw.htm"
        accession = "0001234567-25-RAW001"
        _seed_filing_event(
            conn,
            instrument_id=769_100,
            accession=accession,
            filing_date=date(2026, 3, 15),
            primary_document_url=url,
        )
        conn.commit()
        fetcher = _InMemoryFetcher({url: _proxy_html_with_table()})
        ingest_def14a(conn, fetcher)
        conn.commit()

        doc = raw_filings.read_raw(
            conn,
            accession_number=accession,
            document_kind="def14a_body",
        )
        assert doc is not None
        assert doc.parser_version == "def14a-v1"
        assert doc.source_url == url
        assert len(doc.payload) > 0

    def test_re_ingest_promotes_via_upsert_not_insert(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Idempotent re-ingest: if the operator clears the log row
        and re-runs, holders UPSERT (rows_updated counter increments,
        rows_inserted stays zero on second pass)."""
        conn = _setup
        url = "https://www.sec.gov/test/proxy.htm"
        _seed_filing_event(
            conn,
            instrument_id=769_100,
            accession="0001234567-25-000002",
            filing_date=date(2026, 3, 15),
            primary_document_url=url,
        )
        conn.commit()
        fetcher = _InMemoryFetcher({url: _proxy_html_with_table()})

        first = ingest_def14a(conn, fetcher)
        assert first.rows_inserted == 3

        # Operator clears the log row to force re-ingest of the same
        # accession.
        conn.execute(
            "DELETE FROM def14a_ingest_log WHERE accession_number = %s",
            ("0001234567-25-000002",),
        )
        conn.commit()

        second = ingest_def14a(conn, fetcher)
        assert second.accessions_seen == 1
        assert second.accessions_ingested == 1
        assert second.rows_inserted == 0
        assert second.rows_updated == 3

        # No duplicate rows in the canonical table.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM def14a_beneficial_holdings WHERE accession_number = %s",
                ("0001234567-25-000002",),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 3

    def test_unrecognisable_table_tombstones_partial(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        conn = _setup
        url = "https://www.sec.gov/test/notice-only.htm"
        _seed_filing_event(
            conn,
            instrument_id=769_100,
            accession="0001234567-25-000003",
            filing_date=date(2026, 3, 15),
            primary_document_url=url,
        )
        conn.commit()
        fetcher = _InMemoryFetcher({url: _proxy_html_unrecognisable_table()})

        summary = ingest_def14a(conn, fetcher)

        # Tombstoned as 'partial' (parser returned empty rows) — the
        # accession is logged so re-runs skip it; operator can clear
        # the log row to force retry once parser improves.
        assert summary.accessions_partial == 1
        assert summary.accessions_succeeded == 0
        assert summary.accessions_failed == 0
        assert summary.rows_inserted == 0

        # Run-level audit reflects the degraded state — Codex review
        # of an earlier draft caught a partial run silently logging
        # ``data_ingestion_runs.status='success'``.
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT status, error
                FROM data_ingestion_runs
                WHERE source = 'sec_edgar_def14a'
                ORDER BY ingestion_run_id DESC
                LIMIT 1
                """
            )
            run = cur.fetchone()
        assert run is not None
        assert run["status"] == "partial"
        assert run["error"] is not None and "tombstoned partial" in run["error"]

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT status, error FROM def14a_ingest_log WHERE accession_number = %s",
                ("0001234567-25-000003",),
            )
            log = cur.fetchone()
        assert log is not None
        assert log["status"] == "partial"
        assert log["error"] is not None and "no beneficial-ownership table" in log["error"]

        # No canonical rows persisted.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM def14a_beneficial_holdings WHERE accession_number = %s",
                ("0001234567-25-000003",),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 0

    def test_404_fetch_tombstones_failed(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        conn = _setup
        url = "https://www.sec.gov/test/missing.htm"
        _seed_filing_event(
            conn,
            instrument_id=769_100,
            accession="0001234567-25-000004",
            filing_date=date(2026, 3, 15),
            primary_document_url=url,
        )
        conn.commit()
        fetcher = _InMemoryFetcher({})  # url -> None

        summary = ingest_def14a(conn, fetcher)
        assert summary.accessions_failed == 1
        assert summary.rows_inserted == 0

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT status FROM def14a_ingest_log WHERE accession_number = %s",
                ("0001234567-25-000004",),
            )
            log = cur.fetchone()
        assert log is not None
        assert log["status"] == "failed"

    def test_missing_sec_profile_uses_cik_sentinel(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Instrument without an instrument_sec_profile row still
        ingests — the issuer_cik column gets the sentinel so the
        NOT NULL constraint is satisfied. PR 3's drift detector
        ignores sentinel rows."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=769_200, symbol="NOPROF")
        # Note: no _seed_sec_profile call.
        url = "https://www.sec.gov/test/proxy-noprof.htm"
        _seed_filing_event(
            conn,
            instrument_id=769_200,
            accession="0001234567-25-000050",
            filing_date=date(2026, 3, 15),
            primary_document_url=url,
        )
        conn.commit()
        fetcher = _InMemoryFetcher({url: _proxy_html_with_table()})

        summary = ingest_def14a(conn, fetcher)
        assert summary.rows_inserted == 3

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT issuer_cik FROM def14a_beneficial_holdings WHERE instrument_id = %s LIMIT 1",
                (769_200,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row["issuer_cik"] == "CIK-MISSING"

    def test_no_pending_returns_empty_summary(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        conn = _setup
        fetcher = _InMemoryFetcher({})
        summary = ingest_def14a(conn, fetcher)
        assert summary.accessions_seen == 0
        assert summary.rows_inserted == 0


# ---------------------------------------------------------------------------
# Bootstrap drain (#839 — operator audit found table empty)
# ---------------------------------------------------------------------------


class TestBootstrapDef14a:
    """Mirror of the :func:`bootstrap_business_summaries` test surface
    — the bootstrap helper loops the standard ingester until the
    candidate query empties or the deadline elapses, summing per-chunk
    counts. Idempotent: a second invocation is a fast no-op because
    every accession lands in ``def14a_ingest_log`` after the first
    pass."""

    @pytest.fixture
    def _setup(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> psycopg.Connection[tuple]:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=839_001, symbol="BOOT")
        _seed_sec_profile(conn, instrument_id=839_001, cik="0000839001")
        return conn

    def test_drains_multiple_accessions_in_one_call(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Three pending accessions, chunk_limit=2 → two chunks
        consumed by one bootstrap call (the deadline doesn't fire,
        the empty-chunk break does)."""
        conn = _setup
        urls: dict[str, str | None] = {}
        for i in range(3):
            url = f"https://www.sec.gov/Archives/edgar/data/839001/000083900125-00000{i}/d.htm"
            urls[url] = _proxy_html_with_table()
            _seed_filing_event(
                conn,
                instrument_id=839_001,
                accession=f"0000839001-25-00000{i}",
                filing_date=date(2026, 1, 15 + i),
                primary_document_url=url,
            )
        conn.commit()
        fetcher = _InMemoryFetcher(urls)

        summary = bootstrap_def14a(conn, fetcher, chunk_limit=2, max_runtime_seconds=60)
        assert summary.accessions_seen == 3
        assert summary.accessions_succeeded == 3
        assert summary.rows_inserted == 9  # 3 holders × 3 accessions

    def test_idempotent_second_run_is_noop(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Acceptance #5: re-running the bootstrap after every
        accession is logged is zero-work (no duplicate rows, no SEC
        re-fetch). Mirrors the
        ``app.services.business_summary.bootstrap_business_summaries``
        idempotency contract."""
        conn = _setup
        url = "https://www.sec.gov/Archives/edgar/data/839001/000083900125-000010/d.htm"
        _seed_filing_event(
            conn,
            instrument_id=839_001,
            accession="0000839001-25-000010",
            filing_date=date(2026, 2, 1),
            primary_document_url=url,
        )
        conn.commit()
        fetcher = _InMemoryFetcher({url: _proxy_html_with_table()})

        first = bootstrap_def14a(conn, fetcher, chunk_limit=10, max_runtime_seconds=30)
        assert first.accessions_succeeded == 1
        first_calls = len(fetcher.calls)

        second = bootstrap_def14a(conn, fetcher, chunk_limit=10, max_runtime_seconds=30)
        assert second.accessions_seen == 0
        assert second.rows_inserted == 0
        # No additional SEC fetches — the discovery selector excluded
        # the already-logged accession.
        assert len(fetcher.calls) == first_calls

        # Holdings table has exactly 3 rows (one accession × 3 holders).
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM def14a_beneficial_holdings WHERE instrument_id = %s",
                (839_001,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 3

    def test_empty_pending_returns_empty_summary(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        conn = _setup
        fetcher = _InMemoryFetcher({})
        summary = bootstrap_def14a(conn, fetcher, chunk_limit=100, max_runtime_seconds=10)
        assert summary.accessions_seen == 0
        assert summary.rows_inserted == 0
        assert fetcher.calls == []

    def test_crash_path_tombstones_failed_so_bootstrap_doesnt_redrive(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Codex pre-push review for #839 (#2): a per-accession crash
        used to roll back without writing a log row, so the next chunk's
        discovery query rediscovered the same accession and re-crashed
        on it. In bootstrap mode that wasted SEC calls + clock for the
        entire 1-hour deadline. Now the crash path writes a 'failed'
        tombstone in a fresh transaction so the loop progresses.

        Repro: a fetcher whose ``fetch_document_text`` raises. After
        bootstrap completes, the accession must be in
        ``def14a_ingest_log`` with status='failed', and a second
        bootstrap call must see zero pending."""
        from typing import NoReturn

        conn = _setup
        url = "https://www.sec.gov/Archives/edgar/data/839001/CRASH/d.htm"
        _seed_filing_event(
            conn,
            instrument_id=839_001,
            accession="0000839001-25-000099",
            filing_date=date(2026, 3, 1),
            primary_document_url=url,
        )
        conn.commit()

        class _CrashingFetcher:
            def __init__(self) -> None:
                self.calls = 0

            def fetch_document_text(self, _absolute_url: str) -> NoReturn:
                self.calls += 1
                raise RuntimeError("synthetic SEC fetch crash")

        fetcher = _CrashingFetcher()
        summary = bootstrap_def14a(conn, fetcher, chunk_limit=10, max_runtime_seconds=10)  # type: ignore[arg-type]
        assert summary.accessions_seen == 1
        assert summary.accessions_failed == 1
        assert summary.rows_inserted == 0

        # Tombstone landed.
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT status, error FROM def14a_ingest_log WHERE accession_number = %s",
                ("0000839001-25-000099",),
            )
            row = cur.fetchone()
        assert row is not None
        assert row["status"] == "failed"
        assert "synthetic SEC fetch crash" in (row["error"] or "")

        # Second bootstrap call sees zero pending — the discovery
        # query excludes already-tombstoned accessions.
        prior_calls = fetcher.calls
        second = bootstrap_def14a(conn, fetcher, chunk_limit=10, max_runtime_seconds=5)  # type: ignore[arg-type]
        assert second.accessions_seen == 0
        assert fetcher.calls == prior_calls  # no SEC re-fetches
