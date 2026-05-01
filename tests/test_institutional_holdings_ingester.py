"""Integration tests for the 13F-HR ingester (#730 PR 2).

The service interacts with three boundaries:
  1. SEC HTTP — abstracted as :class:`SecArchiveFetcher` so tests
     can substitute a deterministic in-memory fake.
  2. Postgres — the real ``ebull_test`` DB, since the service
     issues several intertwined statements (existing-accessions
     scan, CUSIP resolution, filer + holdings upserts) and
     mocking that path would erase the value of these tests.
  3. The pure parser from #730 PR 1 — exercised end-to-end here.

Each test seeds the inputs (an instrument with a known CUSIP
mapping, a filer seed, a fake fetcher for HTTP) and asserts the
final canonical row state.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import UTC, date, datetime
from decimal import Decimal

import psycopg
import psycopg.rows
import pytest

from app.services.institutional_holdings import (
    ingest_all_active_filers,
    ingest_filer_13f,
    parse_archive_index,
    parse_submissions_index,
    seed_filer,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Fixture builders — minimal SEC payloads
# ---------------------------------------------------------------------------


def _submissions_json(*, accessions: list[tuple[str, str, str, str]]) -> str:
    """Build a fake submissions JSON. Each tuple is
    ``(accession, form, filing_date, report_date)``."""
    return json.dumps(
        {
            "filings": {
                "recent": {
                    "accessionNumber": [a[0] for a in accessions],
                    "form": [a[1] for a in accessions],
                    "filingDate": [a[2] for a in accessions],
                    "reportDate": [a[3] for a in accessions],
                },
                "files": [],
            }
        }
    )


def _archive_index_json(*, primary_doc: str = "primary_doc.xml", infotable: str = "infotable.xml") -> str:
    """Build a fake archive index.json listing primary_doc + infotable."""
    items: list[dict[str, str]] = []
    if primary_doc:
        items.append({"name": primary_doc, "type": "text.gif", "size": "1234"})
    if infotable:
        items.append({"name": infotable, "type": "text.gif", "size": "5678"})
    items.append({"name": "0001067983-25-000001-index-headers.html", "type": "text.gif", "size": "100"})
    return json.dumps({"directory": {"name": "/Archives/...", "item": items}})


_PRIMARY_DOC_XML = """<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="http://www.sec.gov/edgar/thirteenffiler">
  <headerData>
    <filerInfo>
      <filer>
        <credentials>
          <cik>0001067983</cik>
        </credentials>
      </filer>
    </filerInfo>
  </headerData>
  <formData>
    <coverPage>
      <reportCalendarOrQuarter>09-30-2024</reportCalendarOrQuarter>
      <filingManager>
        <name>BERKSHIRE HATHAWAY INC</name>
      </filingManager>
    </coverPage>
    <signatureBlock>
      <signatureDate>11-14-2024</signatureDate>
    </signatureBlock>
  </formData>
</edgarSubmission>
"""


def _infotable_xml(*, holdings: Iterable[dict[str, str]]) -> str:
    rows: list[str] = []
    for h in holdings:
        rows.append(
            f"""<infoTable>
  <nameOfIssuer>{h.get("name", "APPLE INC")}</nameOfIssuer>
  <titleOfClass>COM</titleOfClass>
  <cusip>{h["cusip"]}</cusip>
  <value>{h.get("value", "69900000")}</value>
  <shrsOrPrnAmt>
    <sshPrnamt>{h.get("shares", "300000000")}</sshPrnamt>
    <sshPrnamtType>SH</sshPrnamtType>
  </shrsOrPrnAmt>
  <investmentDiscretion>SOLE</investmentDiscretion>
  <votingAuthority>
    <Sole>{h.get("sole", "300000000")}</Sole>
    <Shared>0</Shared>
    <None>0</None>
  </votingAuthority>
</infoTable>"""
        )
    body = "\n  ".join(rows)
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
  {body}
</informationTable>
"""


class _InMemoryFetcher:
    """Deterministic SecArchiveFetcher fake."""

    def __init__(self, payloads: dict[str, str | None]) -> None:
        self._payloads = payloads
        self.calls: list[str] = []

    def fetch_document_text(self, absolute_url: str) -> str | None:
        self.calls.append(absolute_url)
        return self._payloads.get(absolute_url)


# ---------------------------------------------------------------------------
# DB seeding helpers
# ---------------------------------------------------------------------------


def _seed_instrument(conn: psycopg.Connection[tuple], *, iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} test"),
    )


def _seed_cusip_mapping(conn: psycopg.Connection[tuple], *, instrument_id: int, cusip: str) -> None:
    conn.execute(
        """
        INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, is_primary)
        VALUES (%s, 'sec', 'cusip', %s, TRUE)
        ON CONFLICT (provider, identifier_type, identifier_value) DO NOTHING
        """,
        (instrument_id, cusip.upper()),
    )


# ---------------------------------------------------------------------------
# Pure-parser tests (no DB)
# ---------------------------------------------------------------------------


class TestParseSubmissionsIndex:
    def test_filters_to_13f_forms_only(self) -> None:
        payload = _submissions_json(
            accessions=[
                ("0001067983-25-000001", "13F-HR", "2025-02-14", "2024-12-31"),
                ("0001067983-25-000002", "10-Q", "2025-02-14", "2024-12-31"),
                ("0001067983-25-000003", "13F-HR/A", "2025-03-01", "2024-12-31"),
                ("0001067983-25-000004", "8-K", "2025-04-01", ""),
            ]
        )
        refs = parse_submissions_index(payload)
        assert len(refs) == 2
        assert {r.filing_type for r in refs} == {"13F-HR", "13F-HR/A"}

    def test_period_and_filed_at_parsed(self) -> None:
        payload = _submissions_json(accessions=[("0001067983-25-000001", "13F-HR", "2025-02-14", "2024-12-31")])
        ref = parse_submissions_index(payload)[0]
        assert ref.period_of_report == date(2024, 12, 31)
        assert ref.filed_at == datetime(2025, 2, 14)

    def test_malformed_json_returns_empty_list(self) -> None:
        assert parse_submissions_index("not json") == []

    def test_missing_recent_section_returns_empty_list(self) -> None:
        assert parse_submissions_index('{"filings": {}}') == []


class TestParseArchiveIndex:
    def test_typical_listing(self) -> None:
        primary, infotable = parse_archive_index(_archive_index_json())
        assert primary == "primary_doc.xml"
        assert infotable == "infotable.xml"

    def test_pre_2018_form13f_naming(self) -> None:
        primary, infotable = parse_archive_index(_archive_index_json(infotable="form13fInfoTable.xml"))
        assert primary == "primary_doc.xml"
        assert infotable == "form13fInfoTable.xml"

    def test_agent_built_naming(self) -> None:
        primary, infotable = parse_archive_index(_archive_index_json(infotable="0001067983-25-000001_infotable.xml"))
        assert infotable == "0001067983-25-000001_infotable.xml"

    def test_information_table_long_form(self) -> None:
        primary, infotable = parse_archive_index(_archive_index_json(infotable="information_table.xml"))
        assert infotable == "information_table.xml"

    def test_missing_primary_returns_none(self) -> None:
        primary, infotable = parse_archive_index(_archive_index_json(primary_doc=""))
        assert primary is None
        assert infotable == "infotable.xml"

    def test_missing_infotable_returns_none(self) -> None:
        primary, infotable = parse_archive_index(_archive_index_json(infotable=""))
        assert primary == "primary_doc.xml"
        assert infotable is None


# ---------------------------------------------------------------------------
# Integration: end-to-end ingest
# ---------------------------------------------------------------------------


class TestIngestFiler13F:
    @pytest.fixture
    def _setup(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> psycopg.Connection[tuple]:
        conn = ebull_test_conn
        # Seed two instruments + their CUSIPs so resolution succeeds.
        _seed_instrument(conn, iid=730_001, symbol="AAPL")
        _seed_cusip_mapping(conn, instrument_id=730_001, cusip="037833100")
        _seed_instrument(conn, iid=730_002, symbol="MSFT")
        _seed_cusip_mapping(conn, instrument_id=730_002, cusip="594918104")
        # Seed the filer.
        seed_filer(conn, cik="0001067983", label="BERKSHIRE HATHAWAY")
        conn.commit()
        return conn

    def _build_fetcher(self, *, holdings: list[dict[str, str]]) -> _InMemoryFetcher:
        cik_int = 1067983
        accession = "0001067983-25-000001"
        accn_no_dashes = accession.replace("-", "")
        archive_base = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accn_no_dashes}/"
        payloads: dict[str, str | None] = {
            "https://data.sec.gov/submissions/CIK0001067983.json": _submissions_json(
                accessions=[(accession, "13F-HR", "2025-02-14", "2024-12-31")]
            ),
            archive_base + "index.json": _archive_index_json(),
            archive_base + "primary_doc.xml": _PRIMARY_DOC_XML,
            archive_base + "infotable.xml": _infotable_xml(holdings=holdings),
        }
        return _InMemoryFetcher(payloads)

    def test_end_to_end_ingest_populates_filer_and_holdings(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        conn = _setup
        fetcher = self._build_fetcher(
            holdings=[
                {"cusip": "037833100", "name": "APPLE INC", "value": "69900000", "shares": "300000000"},
                {"cusip": "594918104", "name": "MICROSOFT CORP", "value": "42000000", "shares": "100000000"},
            ]
        )

        summary = ingest_filer_13f(conn, fetcher, filer_cik="0001067983")
        conn.commit()

        assert summary.accessions_seen == 1
        assert summary.accessions_ingested == 1
        assert summary.holdings_inserted == 2
        assert summary.holdings_skipped_no_cusip == 0

        # Filer row exists.
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute("SELECT cik, name FROM institutional_filers WHERE cik = '0001067983'")
            filer = cur.fetchone()
        assert filer is not None
        assert filer["name"] == "BERKSHIRE HATHAWAY INC"

        # Both holdings rows.
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT i.symbol, h.shares, h.market_value_usd, h.voting_authority
                FROM institutional_holdings h
                JOIN instruments i ON i.instrument_id = h.instrument_id
                ORDER BY i.symbol
                """,
            )
            rows = cur.fetchall()
        assert [r["symbol"] for r in rows] == ["AAPL", "MSFT"]
        assert rows[0]["shares"] == Decimal("300000000")
        assert rows[0]["market_value_usd"] == Decimal("69900000")
        assert rows[0]["voting_authority"] == "SOLE"

    def test_unknown_cusip_is_skipped_with_counter(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Holdings whose CUSIP is not yet mapped via #740 land in
        ``holdings_skipped_no_cusip`` rather than raising."""
        conn = _setup
        fetcher = self._build_fetcher(
            holdings=[
                {"cusip": "037833100", "name": "APPLE INC"},  # known
                {"cusip": "999999999", "name": "UNKNOWN CO"},  # not mapped
                {"cusip": "888888888", "name": "OTHER UNKNOWN"},  # not mapped
            ]
        )
        summary = ingest_filer_13f(conn, fetcher, filer_cik="0001067983")
        conn.commit()
        assert summary.holdings_inserted == 1
        assert summary.holdings_skipped_no_cusip == 2

    def test_re_ingest_is_idempotent(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Running the ingester twice on the same filer leaves the
        canonical row count unchanged. Skipping is keyed on
        ``existing_accessions`` so the second run never even fetches
        the archive index for already-ingested accessions."""
        conn = _setup
        fetcher = self._build_fetcher(holdings=[{"cusip": "037833100", "name": "APPLE INC"}])
        first = ingest_filer_13f(conn, fetcher, filer_cik="0001067983")
        conn.commit()
        assert first.holdings_inserted == 1

        # Second run must not re-fetch the archive index.json or the
        # XML attachments because the accession is already present.
        fetcher.calls.clear()
        second = ingest_filer_13f(conn, fetcher, filer_cik="0001067983")
        conn.commit()
        assert second.accessions_seen == 1
        assert second.accessions_ingested == 0
        assert second.holdings_inserted == 0

        # Only the submissions JSON was re-fetched.
        assert any("submissions" in url for url in fetcher.calls)
        assert not any("primary_doc.xml" in url for url in fetcher.calls)

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM institutional_holdings")
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 1

    def test_missing_submissions_returns_zero_summary(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """If data.sec.gov returns 404 for the submissions JSON, the
        ingester logs and returns an empty summary rather than
        raising. CIK-not-found is a real condition (filer fell off
        the registered list, or the seeded CIK is wrong)."""
        conn = _setup
        fetcher = _InMemoryFetcher({})  # everything 404s
        summary = ingest_filer_13f(conn, fetcher, filer_cik="0001067983")
        conn.commit()
        assert summary.accessions_seen == 0
        assert summary.accessions_ingested == 0

    def test_missing_archive_index_records_failed_accession(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """A submissions JSON listing an accession but archive index
        404 — that's a transient real condition (SEC indexes the
        filing in the manager listing before the archive directory
        is published). Surface as accessions_failed; next run
        retries."""
        conn = _setup
        cik_int = 1067983
        accession = "0001067983-25-000001"
        archive_base = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession.replace('-', '')}/"
        payloads: dict[str, str | None] = {
            "https://data.sec.gov/submissions/CIK0001067983.json": _submissions_json(
                accessions=[(accession, "13F-HR", "2025-02-14", "2024-12-31")]
            ),
            archive_base + "index.json": None,  # 404
        }
        fetcher = _InMemoryFetcher(payloads)
        summary = ingest_filer_13f(conn, fetcher, filer_cik="0001067983")
        conn.commit()
        assert summary.accessions_seen == 1
        assert summary.accessions_ingested == 0
        assert summary.accessions_failed == 1
        # No filer row written.
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM institutional_filers WHERE cik = '0001067983'")
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 0

    def test_partial_unique_index_blocks_duplicate_equity_row(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """The partial UNIQUE INDEX from migration 090 must reject a
        duplicate equity row for the same (accession, instrument)
        pair while still admitting a sibling PUT or CALL row."""
        conn = _setup

        # Equity holding lands.
        fetcher = self._build_fetcher(holdings=[{"cusip": "037833100", "name": "APPLE INC"}])
        ingest_filer_13f(conn, fetcher, filer_cik="0001067983")
        conn.commit()

        # Manually try to insert a duplicate equity row — must fail
        # the partial UNIQUE INDEX. Use a savepoint so the failed
        # insert doesn't poison the test transaction.
        with conn.cursor() as cur:
            cur.execute("SAVEPOINT s1")
            with pytest.raises(psycopg.errors.UniqueViolation):
                cur.execute(
                    """
                    INSERT INTO institutional_holdings (
                        filer_id, instrument_id, accession_number, period_of_report,
                        shares, market_value_usd, voting_authority, is_put_call, filed_at
                    )
                    SELECT filer_id, %(iid)s, %(accn)s, %(pd)s, 1, 1, 'SOLE', NULL, %(fd)s
                    FROM institutional_filers WHERE cik = '0001067983'
                    """,
                    {
                        "iid": 730_001,
                        "accn": "0001067983-25-000001",
                        "pd": date(2024, 12, 31),
                        "fd": datetime(2025, 2, 14, tzinfo=UTC),
                    },
                )
            cur.execute("ROLLBACK TO SAVEPOINT s1")

        # A sibling PUT row IS admitted (different is_put_call value).
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO institutional_holdings (
                    filer_id, instrument_id, accession_number, period_of_report,
                    shares, market_value_usd, voting_authority, is_put_call, filed_at
                )
                SELECT filer_id, %(iid)s, %(accn)s, %(pd)s, 1, 1, 'SOLE', 'PUT', %(fd)s
                FROM institutional_filers WHERE cik = '0001067983'
                """,
                {
                    "iid": 730_001,
                    "accn": "0001067983-25-000001",
                    "pd": date(2024, 12, 31),
                    "fd": datetime(2025, 2, 14, tzinfo=UTC),
                },
            )
            conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT is_put_call FROM institutional_holdings ORDER BY is_put_call NULLS FIRST")
            rows = cur.fetchall()
            assert [r[0] for r in rows] == [None, "PUT"]


class TestIngestLogIdempotency:
    """#730 PR 2 review pin: idempotency must hold for accessions
    that produce zero canonical rows (empty 13F-HR or every CUSIP
    unresolved). Without the institutional_holdings_ingest_log
    tombstone the ingester re-fetched these accessions on every
    run and the unresolved-CUSIP counter ratcheted forever."""

    @pytest.fixture
    def _setup(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> psycopg.Connection[tuple]:
        conn = ebull_test_conn
        seed_filer(conn, cik="0001067983", label="BERKSHIRE")
        conn.commit()
        return conn

    def _build_fetcher(self, *, holdings: list[dict[str, str]]) -> _InMemoryFetcher:
        cik_int = 1067983
        accession = "0001067983-25-000001"
        accn_no_dashes = accession.replace("-", "")
        archive_base = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accn_no_dashes}/"
        return _InMemoryFetcher(
            {
                "https://data.sec.gov/submissions/CIK0001067983.json": _submissions_json(
                    accessions=[(accession, "13F-HR", "2025-02-14", "2024-12-31")]
                ),
                archive_base + "index.json": _archive_index_json(),
                archive_base + "primary_doc.xml": _PRIMARY_DOC_XML,
                archive_base + "infotable.xml": _infotable_xml(holdings=holdings),
            }
        )

    def test_zero_canonical_rows_still_logs_skip_marker(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Every CUSIP unresolved -> zero canonical rows. Without the
        log row the second run re-fetches and re-counts the skip."""
        conn = _setup
        fetcher = self._build_fetcher(holdings=[{"cusip": "999999999", "name": "UNKNOWN"}])

        first = ingest_filer_13f(conn, fetcher, filer_cik="0001067983")
        conn.commit()
        assert first.holdings_inserted == 0
        assert first.holdings_skipped_no_cusip == 1

        # Log row exists with status='partial' (unresolved CUSIP gap).
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, holdings_inserted, holdings_skipped FROM institutional_holdings_ingest_log "
                "WHERE accession_number = '0001067983-25-000001'"
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == "partial"
        assert row[1] == 0
        assert row[2] == 1

        # Second run: must skip — accession_seen counts the
        # pending list, but accessions_ingested + holdings_skipped
        # remain zero because the existing-log gate fires.
        fetcher.calls.clear()
        second = ingest_filer_13f(conn, fetcher, filer_cik="0001067983")
        conn.commit()
        assert second.accessions_seen == 1
        assert second.accessions_ingested == 0
        assert second.holdings_skipped_no_cusip == 0
        assert not any("primary_doc.xml" in url for url in fetcher.calls)

    def test_failed_accession_logged_with_error_message(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """A 404 on the archive index records a failed log row with
        the failure reason so re-runs skip until the operator
        clears the row."""
        conn = _setup
        cik_int = 1067983
        accession = "0001067983-25-000001"
        archive_base = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession.replace('-', '')}/"
        fetcher = _InMemoryFetcher(
            {
                "https://data.sec.gov/submissions/CIK0001067983.json": _submissions_json(
                    accessions=[(accession, "13F-HR", "2025-02-14", "2024-12-31")]
                ),
                archive_base + "index.json": None,  # 404
            }
        )

        ingest_filer_13f(conn, fetcher, filer_cik="0001067983")
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, error FROM institutional_holdings_ingest_log "
                "WHERE accession_number = '0001067983-25-000001'"
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == "failed"
        assert row[1] is not None
        assert "index.json" in row[1]


class TestIngestAllActiveFilersDataIngestionRuns:
    """Codex pre-push pin: per-accession failures must surface in
    data_ingestion_runs.error so the ops monitor (#13) sees more
    than 'success' on a run that silently dropped accessions."""

    def test_partial_status_when_accessions_failed_but_no_crash(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        seed_filer(conn, cik="0001067983", label="BERKSHIRE")
        conn.commit()

        cik_int = 1067983
        accession = "0001067983-25-000001"
        archive_base = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession.replace('-', '')}/"
        fetcher = _InMemoryFetcher(
            {
                "https://data.sec.gov/submissions/CIK0001067983.json": _submissions_json(
                    accessions=[(accession, "13F-HR", "2025-02-14", "2024-12-31")]
                ),
                archive_base + "index.json": None,  # 404 → failed accession
            }
        )

        ingest_all_active_filers(conn, fetcher)

        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, error FROM data_ingestion_runs "
                "WHERE source = 'sec_edgar_13f' "
                "ORDER BY ingestion_run_id DESC LIMIT 1"
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == "partial"
        assert row[1] is not None
        assert "accession" in row[1].lower()


class TestSeedFiler:
    def test_idempotent_seed(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        seed_filer(conn, cik="1067983", label="BERKSHIRE")
        seed_filer(conn, cik="0001067983", label="BERKSHIRE HATHAWAY", notes="updated")
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT label, notes, active FROM institutional_filer_seeds WHERE cik = '0001067983'")
            row = cur.fetchone()
            assert row is not None
            assert row[0] == "BERKSHIRE HATHAWAY"
            assert row[1] == "updated"
            assert row[2] is True
