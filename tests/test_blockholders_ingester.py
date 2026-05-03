"""Integration tests for the 13D/G blockholders ingester (#766 PR 2).

The service interacts with three boundaries:
  1. SEC HTTP — abstracted as :class:`SecArchiveFetcher` so tests
     can substitute a deterministic in-memory fake.
  2. Postgres — the real ``ebull_test`` DB, since the service
     issues several intertwined statements (existing-accessions
     scan, CUSIP resolution, filer + filings upserts) and mocking
     that path would erase the value of these tests.
  3. The pure parser from #766 PR 1 — exercised end-to-end here.

Each test seeds the inputs (an instrument with a known CUSIP
mapping, a filer seed, a fake fetcher for HTTP) and asserts the
final canonical row state.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

import psycopg
import psycopg.rows
import pytest

from app.services.blockholders import (
    ingest_all_active_filers,
    ingest_filer_blockholders,
    latest_blockholder_positions,
    parse_submissions_index,
    seed_filer,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Fixture builders — minimal SEC payloads
# ---------------------------------------------------------------------------


def _submissions_json(*, accessions: list[tuple[str, str, str]]) -> str:
    """Build a fake submissions JSON. Each tuple is
    ``(accession, form, filing_date)``."""
    return json.dumps(
        {
            "filings": {
                "recent": {
                    "accessionNumber": [a[0] for a in accessions],
                    "form": [a[1] for a in accessions],
                    "filingDate": [a[2] for a in accessions],
                },
                "files": [],
            }
        }
    )


_NS_13D = "http://www.sec.gov/edgar/schedule13D"
_NS_13G = "http://www.sec.gov/edgar/schedule13g"


def _13d_xml(
    *,
    submission_type: str = "SCHEDULE 13D",
    primary_filer_cik: str = "0001234567",
    primary_filer_name: str = "Test Activist Fund LP",
    issuer_cik: str = "0000012345",
    issuer_cusip: str = "037833100",
    issuer_name: str = "APPLE INC",
    signature_date: str = "11/06/2025",
    aggregate_shares: str = "1500000",
    percent: str = "5.5",
) -> str:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="{_NS_13D}">
  <headerData>
    <submissionType>{submission_type}</submissionType>
    <filerInfo>
      <filer>
        <filerCredentials>
          <cik>{primary_filer_cik}</cik>
        </filerCredentials>
      </filer>
    </filerInfo>
  </headerData>
  <formData>
    <coverPageHeader>
      <securitiesClassTitle>Common Stock</securitiesClassTitle>
      <dateOfEvent>11/03/2025</dateOfEvent>
      <issuerInfo>
        <issuerCIK>{issuer_cik}</issuerCIK>
        <issuerCUSIP>{issuer_cusip}</issuerCUSIP>
        <issuerName>{issuer_name}</issuerName>
      </issuerInfo>
    </coverPageHeader>
    <reportingPersons>
      <reportingPersonInfo>
        <reportingPersonCIK>{primary_filer_cik}</reportingPersonCIK>
        <reportingPersonNoCIK>N</reportingPersonNoCIK>
        <reportingPersonName>{primary_filer_name}</reportingPersonName>
        <memberOfGroup>b</memberOfGroup>
        <citizenshipOrOrganization>DE</citizenshipOrOrganization>
        <soleVotingPower>{aggregate_shares}</soleVotingPower>
        <sharedVotingPower>0</sharedVotingPower>
        <soleDispositivePower>{aggregate_shares}</soleDispositivePower>
        <sharedDispositivePower>0</sharedDispositivePower>
        <aggregateAmountOwned>{aggregate_shares}</aggregateAmountOwned>
        <percentOfClass>{percent}</percentOfClass>
        <typeOfReportingPerson>PN</typeOfReportingPerson>
      </reportingPersonInfo>
    </reportingPersons>
    <signatureInfo>
      <signaturePerson>
        <signatureDetails>
          <date>{signature_date}</date>
        </signatureDetails>
      </signaturePerson>
    </signatureInfo>
  </formData>
</edgarSubmission>
"""


def _13g_xml(
    *,
    submission_type: str = "SCHEDULE 13G",
    primary_filer_cik: str = "0001234567",
    primary_filer_name: str = "Test Passive Holder",
    issuer_cik: str = "0000012345",
    issuer_cusip: str = "037833100",
    issuer_name: str = "APPLE INC",
    signature_date: str = "09/15/2025",
    aggregate_shares: str = "500000",
    percent: str = "2.5",
) -> str:
    """Build a real-shape 13G primary_doc.xml for the supersession test.

    13G uses a different namespace (``schedule13g``), different
    casing on issuer fields (``issuerCik`` not ``issuerCIK``), and
    repeats ``<coverPageHeaderReportingPersonDetails>`` directly under
    ``<formData>`` rather than wrapping reporters in
    ``<reportingPersons>``. Codex pre-push review caught the prior
    test using ``_13d_xml(submission_type='SCHEDULE 13G/A')`` which
    was syntactically 13D but labelled 13G — the parser raised
    instead of exercising the supersession path.
    """
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="{_NS_13G}">
  <headerData>
    <submissionType>{submission_type}</submissionType>
    <filerInfo>
      <filer>
        <filerCredentials>
          <cik>{primary_filer_cik}</cik>
        </filerCredentials>
      </filer>
    </filerInfo>
  </headerData>
  <formData>
    <coverPageHeader>
      <securitiesClassTitle>Common Stock</securitiesClassTitle>
      <eventDateRequiresFilingThisStatement>09/10/2025</eventDateRequiresFilingThisStatement>
      <issuerInfo>
        <issuerCik>{issuer_cik}</issuerCik>
        <issuerName>{issuer_name}</issuerName>
        <issuerCusip>{issuer_cusip}</issuerCusip>
      </issuerInfo>
    </coverPageHeader>
    <coverPageHeaderReportingPersonDetails>
      <reportingPersonCik>{primary_filer_cik}</reportingPersonCik>
      <reportingPersonName>{primary_filer_name}</reportingPersonName>
      <citizenshipOrOrganization>DE</citizenshipOrOrganization>
      <reportingPersonBeneficiallyOwnedNumberOfShares>
        <soleVotingPower>{aggregate_shares}</soleVotingPower>
        <sharedVotingPower>0</sharedVotingPower>
        <soleDispositivePower>{aggregate_shares}</soleDispositivePower>
        <sharedDispositivePower>0</sharedDispositivePower>
      </reportingPersonBeneficiallyOwnedNumberOfShares>
      <reportingPersonBeneficiallyOwnedAggregateNumberOfShares>{aggregate_shares}</reportingPersonBeneficiallyOwnedAggregateNumberOfShares>
      <classPercent>{percent}</classPercent>
      <typeOfReportingPerson>IA</typeOfReportingPerson>
    </coverPageHeaderReportingPersonDetails>
    <signatureInfo>
      <signaturePerson>
        <signatureDetails>
          <date>{signature_date}</date>
        </signatureDetails>
      </signaturePerson>
    </signatureInfo>
  </formData>
</edgarSubmission>
"""


def _13d_multi_reporter_xml(
    *,
    primary_filer_cik: str = "0001234567",
    issuer_cusip: str = "037833100",
    signature_date: str = "11/06/2025",
) -> str:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="{_NS_13D}">
  <headerData>
    <submissionType>SCHEDULE 13D</submissionType>
    <filerInfo>
      <filer>
        <filerCredentials>
          <cik>{primary_filer_cik}</cik>
        </filerCredentials>
      </filer>
    </filerInfo>
  </headerData>
  <formData>
    <coverPageHeader>
      <securitiesClassTitle>Common Stock</securitiesClassTitle>
      <dateOfEvent>11/03/2025</dateOfEvent>
      <issuerInfo>
        <issuerCIK>0000012345</issuerCIK>
        <issuerCUSIP>{issuer_cusip}</issuerCUSIP>
        <issuerName>APPLE INC</issuerName>
      </issuerInfo>
    </coverPageHeader>
    <reportingPersons>
      <reportingPersonInfo>
        <reportingPersonCIK>{primary_filer_cik}</reportingPersonCIK>
        <reportingPersonNoCIK>N</reportingPersonNoCIK>
        <reportingPersonName>Test Activist Fund LP</reportingPersonName>
        <memberOfGroup>b</memberOfGroup>
        <citizenshipOrOrganization>DE</citizenshipOrOrganization>
        <aggregateAmountOwned>1500000</aggregateAmountOwned>
        <percentOfClass>5.5</percentOfClass>
        <typeOfReportingPerson>PN</typeOfReportingPerson>
      </reportingPersonInfo>
      <reportingPersonInfo>
        <reportingPersonNoCIK>Y</reportingPersonNoCIK>
        <reportingPersonName>Jane Doe (managing member)</reportingPersonName>
        <memberOfGroup>b</memberOfGroup>
        <citizenshipOrOrganization>NY</citizenshipOrOrganization>
        <aggregateAmountOwned>1500000</aggregateAmountOwned>
        <percentOfClass>5.5</percentOfClass>
        <typeOfReportingPerson>IN</typeOfReportingPerson>
      </reportingPersonInfo>
    </reportingPersons>
    <signatureInfo>
      <signaturePerson>
        <signatureDetails>
          <date>{signature_date}</date>
        </signatureDetails>
      </signaturePerson>
    </signatureInfo>
  </formData>
</edgarSubmission>
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


def _archive_url(filer_cik: str, accession: str, filename: str) -> str:
    return f"https://www.sec.gov/Archives/edgar/data/{int(filer_cik)}/{accession.replace('-', '')}/{filename}"


# ---------------------------------------------------------------------------
# Pure-parser tests (no DB)
# ---------------------------------------------------------------------------


class TestParseSubmissionsIndex:
    def test_filters_to_13dg_forms_only(self) -> None:
        payload = _submissions_json(
            accessions=[
                ("0001234567-25-000001", "SC 13D", "2025-11-06"),
                ("0001234567-25-000002", "10-K", "2025-09-15"),
                ("0001234567-25-000003", "SC 13D/A", "2025-11-15"),
                ("0001234567-25-000004", "SC 13G", "2025-10-01"),
                ("0001234567-25-000005", "SC 13G/A", "2025-10-15"),
                ("0001234567-25-000006", "13F-HR", "2025-11-14"),
            ]
        )
        refs = parse_submissions_index(payload)
        assert refs is not None
        assert len(refs) == 4
        assert {r.filing_type for r in refs} == {"SC 13D", "SC 13D/A", "SC 13G", "SC 13G/A"}

    def test_accepts_long_form_schedule_labels(self) -> None:
        """Real-world SEC submissions JSON uses the long ``SCHEDULE
        13D`` form for post-BOM-rule filings (post-2024-12-19) —
        verified against Carl Icahn submissions. The filter must
        accept both short and long forms or every modern filing
        is silently skipped."""
        payload = _submissions_json(
            accessions=[
                ("0000921669-25-000001", "SCHEDULE 13D", "2025-03-01"),
                ("0000921669-25-000002", "SCHEDULE 13D/A", "2025-04-01"),
                ("0000921669-25-000003", "SCHEDULE 13G", "2025-05-01"),
                ("0000921669-25-000004", "SCHEDULE 13G/A", "2025-06-01"),
            ]
        )
        refs = parse_submissions_index(payload)
        assert refs is not None
        assert len(refs) == 4
        assert {r.filing_type for r in refs} == {
            "SCHEDULE 13D",
            "SCHEDULE 13D/A",
            "SCHEDULE 13G",
            "SCHEDULE 13G/A",
        }

    def test_filed_at_is_utc_tz_aware(self) -> None:
        payload = _submissions_json(accessions=[("0001234567-25-000001", "SC 13D", "2025-11-06")])
        refs = parse_submissions_index(payload)
        assert refs is not None
        ref = refs[0]
        assert ref.filed_at == datetime(2025, 11, 6, tzinfo=UTC)
        assert ref.filed_at is not None and ref.filed_at.tzinfo is UTC

    def test_malformed_json_returns_none(self) -> None:
        """Malformed payload returns None (not []) so the ingester can
        distinguish ``no 13D/G filings on file`` (legitimate empty
        list) from ``cannot parse the index`` (treat as a failure).
        """
        assert parse_submissions_index("not json") is None

    def test_missing_recent_returns_empty_list(self) -> None:
        """Valid JSON with no 'recent' array is a legitimate empty
        result — distinct from malformed JSON above."""
        assert parse_submissions_index('{"filings": {}}') == []


# ---------------------------------------------------------------------------
# Integration: end-to-end ingest
# ---------------------------------------------------------------------------


class TestIngestFilerBlockholders:
    @pytest.fixture
    def _setup(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> psycopg.Connection[tuple]:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=766_001, symbol="AAPL")
        _seed_cusip_mapping(conn, instrument_id=766_001, cusip="037833100")
        seed_filer(conn, cik="0001234567", label="TEST ACTIVIST FUND")
        conn.commit()
        return conn

    def _build_fetcher(
        self,
        *,
        accessions: list[tuple[str, str, str]],
        xml_by_accession: dict[str, str],
    ) -> _InMemoryFetcher:
        cik = "0001234567"
        payloads: dict[str, str | None] = {
            f"https://data.sec.gov/submissions/CIK{cik}.json": _submissions_json(accessions=accessions),
        }
        for accession, xml in xml_by_accession.items():
            payloads[_archive_url(cik, accession, "primary_doc.xml")] = xml
        return _InMemoryFetcher(payloads)

    def test_single_reporter_13d_writes_one_row(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        conn = _setup
        accession = "0001234567-25-000001"
        fetcher = self._build_fetcher(
            accessions=[(accession, "SC 13D", "2025-11-06")],
            xml_by_accession={accession: _13d_xml()},
        )

        summary = ingest_filer_blockholders(conn, fetcher, filer_cik="0001234567")
        conn.commit()

        assert summary.accessions_seen == 1
        assert summary.accessions_ingested == 1
        assert summary.rows_inserted == 1

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT bf.accession_number, bf.submission_type, bf.status,
                       bf.reporter_name, bf.reporter_cik, bf.aggregate_amount_owned,
                       bf.percent_of_class, bf.instrument_id, i.symbol, f.name AS filer_name
                FROM blockholder_filings bf
                JOIN blockholder_filers f USING (filer_id)
                JOIN instruments i ON i.instrument_id = bf.instrument_id
                """
            )
            row = cur.fetchone()
        assert row is not None
        assert row["submission_type"] == "SCHEDULE 13D"
        assert row["status"] == "active"
        assert row["reporter_cik"] == "0001234567"
        assert row["aggregate_amount_owned"] == Decimal("1500000")
        assert row["percent_of_class"] == Decimal("5.5")
        assert row["symbol"] == "AAPL"
        assert row["filer_name"] == "Test Activist Fund LP"

    def test_raw_payload_persisted_for_primary_doc_13dg(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """13D/G ingester must persist primary_doc.xml body to
        ``filing_raw_documents`` before parsing — operator audit
        2026-05-03 + PR #808 contract."""
        from app.services import raw_filings

        conn = _setup
        accession = "0001234567-25-000099"
        fetcher = self._build_fetcher(
            accessions=[(accession, "SC 13D", "2025-11-06")],
            xml_by_accession={accession: _13d_xml()},
        )
        ingest_filer_blockholders(conn, fetcher, filer_cik="0001234567")
        conn.commit()

        doc = raw_filings.read_raw(
            conn,
            accession_number=accession,
            document_kind="primary_doc_13dg",
        )
        assert doc is not None
        assert "<edgarSubmission" in doc.payload or "<schedule13D" in doc.payload.lower()
        assert doc.parser_version == "13dg-primary-v1"

    def test_multi_reporter_writes_n_rows(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Joint 13D with a fund + a no-CIK individual reporter writes
        2 rows under the same accession; both link to the same
        instrument and the same filer record."""
        conn = _setup
        accession = "0001234567-25-000002"
        fetcher = self._build_fetcher(
            accessions=[(accession, "SC 13D", "2025-11-06")],
            xml_by_accession={accession: _13d_multi_reporter_xml()},
        )

        summary = ingest_filer_blockholders(conn, fetcher, filer_cik="0001234567")
        conn.commit()

        assert summary.rows_inserted == 2

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT reporter_name, reporter_cik, reporter_no_cik FROM blockholder_filings ORDER BY reporter_name"
            )
            rows = cur.fetchall()
        assert [r["reporter_name"] for r in rows] == [
            "Jane Doe (managing member)",
            "Test Activist Fund LP",
        ]
        assert rows[0]["reporter_cik"] is None
        assert rows[0]["reporter_no_cik"] is True
        assert rows[1]["reporter_cik"] == "0001234567"
        assert rows[1]["reporter_no_cik"] is False

    def test_re_ingest_is_idempotent(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        conn = _setup
        accession = "0001234567-25-000003"
        fetcher = self._build_fetcher(
            accessions=[(accession, "SC 13D", "2025-11-06")],
            xml_by_accession={accession: _13d_xml()},
        )

        first = ingest_filer_blockholders(conn, fetcher, filer_cik="0001234567")
        conn.commit()
        assert first.rows_inserted == 1

        fetcher.calls.clear()
        second = ingest_filer_blockholders(conn, fetcher, filer_cik="0001234567")
        conn.commit()
        assert second.accessions_seen == 1
        assert second.accessions_ingested == 0
        assert second.rows_inserted == 0
        # Only the submissions JSON was re-fetched; primary_doc skipped.
        assert any("submissions" in url for url in fetcher.calls)
        assert not any("primary_doc.xml" in url for url in fetcher.calls)

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM blockholder_filings")
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 1

    def test_unresolved_cusip_writes_partial_row(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """A filing whose issuer CUSIP isn't yet mapped via #740 writes
        the reporter row(s) with ``instrument_id IS NULL`` and tags
        the ingest log entry as ``partial``."""
        conn = _setup
        accession = "0001234567-25-000004"
        fetcher = self._build_fetcher(
            accessions=[(accession, "SC 13D", "2025-11-06")],
            xml_by_accession={accession: _13d_xml(issuer_cusip="999999999")},
        )

        summary = ingest_filer_blockholders(conn, fetcher, filer_cik="0001234567")
        conn.commit()

        assert summary.accessions_ingested == 1
        assert summary.rows_inserted == 1
        assert summary.rows_skipped_no_cusip == 1

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute("SELECT instrument_id, issuer_cusip FROM blockholder_filings")
            row = cur.fetchone()
        assert row is not None
        assert row["instrument_id"] is None
        assert row["issuer_cusip"] == "999999999"

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT status, error FROM blockholder_filings_ingest_log WHERE accession_number = %s",
                (accession,),
            )
            log = cur.fetchone()
        assert log is not None
        assert log["status"] == "partial"
        assert log["error"] is not None and "999999999" in log["error"]

    def test_primary_doc_404_is_recorded_failed(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        conn = _setup
        accession = "0001234567-25-000005"
        fetcher = self._build_fetcher(
            accessions=[(accession, "SC 13D", "2025-11-06")],
            xml_by_accession={},  # primary_doc absent → 404
        )

        summary = ingest_filer_blockholders(conn, fetcher, filer_cik="0001234567")
        conn.commit()

        assert summary.accessions_failed == 1
        assert summary.rows_inserted == 0
        assert summary.first_error is not None and "primary_doc" in summary.first_error

        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, error FROM blockholder_filings_ingest_log WHERE accession_number = %s",
                (accession,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == "failed"

    def test_parse_failure_recorded_failed(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Malformed primary_doc.xml lands in the log as failed; the
        next run skips the accession instead of re-fetching."""
        conn = _setup
        accession = "0001234567-25-000006"
        bad_xml = """<?xml version="1.0"?><edgarSubmission xmlns="http://www.sec.gov/edgar/schedule13D"><headerData><submissionType>WRONG</submissionType></headerData></edgarSubmission>"""
        fetcher = self._build_fetcher(
            accessions=[(accession, "SC 13D", "2025-11-06")],
            xml_by_accession={accession: bad_xml},
        )

        summary = ingest_filer_blockholders(conn, fetcher, filer_cik="0001234567")
        conn.commit()

        assert summary.accessions_failed == 1
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM blockholder_filings_ingest_log WHERE accession_number = %s",
                (accession,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == "failed"

    def test_submissions_404_is_no_op(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """If the per-CIK submissions JSON is unreachable the ingester
        logs a warning and returns an empty summary — the next run
        retries without writing any tombstone."""
        conn = _setup
        fetcher = _InMemoryFetcher({})

        summary = ingest_filer_blockholders(conn, fetcher, filer_cik="0001234567")
        conn.commit()

        assert summary.accessions_seen == 0
        assert summary.rows_inserted == 0


# ---------------------------------------------------------------------------
# Amendment-chain aggregator
# ---------------------------------------------------------------------------


class TestLatestBlockholderPositions:
    @pytest.fixture
    def _setup(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> psycopg.Connection[tuple]:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=766_010, symbol="AAPL")
        _seed_cusip_mapping(conn, instrument_id=766_010, cusip="037833100")
        seed_filer(conn, cik="0001234567", label="TEST FUND")
        conn.commit()
        return conn

    def _ingest(self, conn: psycopg.Connection[tuple], accession: str, xml: str) -> None:
        fetcher = _InMemoryFetcher(
            {
                "https://data.sec.gov/submissions/CIK0001234567.json": _submissions_json(
                    accessions=[(accession, "SC 13D", "2025-11-06")]
                ),
                _archive_url("0001234567", accession, "primary_doc.xml"): xml,
            }
        )
        ingest_filer_blockholders(conn, fetcher, filer_cik="0001234567")
        conn.commit()
        # Reset the seed-list ingest skip so the next call picks up
        # a new accession. We do that by NOT calling the same
        # ingest cycle again; instead each test ingests once per
        # accession via separate fetchers below.

    def test_13d_supersedes_prior_13g_amendment(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Passive→active conversion: a 13D filed after a prior 13G/A
        by the same reporter on the same issuer must win the chain.
        Verifies the aggregator picks the latest filed_at regardless
        of form-family (the schema's intended supersession semantic).
        """
        conn = _setup
        # First filing: real-shape 13G/A (passive). Build with
        # _13g_xml so the parser actually round-trips it; previous
        # version of this test built a 13D body with a 13G label
        # which silently failed to parse. Codex pre-push review.
        first_accession = "0001234567-25-000010"
        first_xml = _13g_xml(
            submission_type="SCHEDULE 13G/A",
            signature_date="09/15/2025",
            aggregate_shares="500000",
            percent="2.5",
        )
        # Second filing: 13D active, supersedes the prior 13G/A
        second_accession = "0001234567-25-000011"
        second_xml = _13d_xml(
            submission_type="SCHEDULE 13D",
            signature_date="11/06/2025",
            aggregate_shares="2000000",
            percent="7.0",
        )

        f1 = _InMemoryFetcher(
            {
                "https://data.sec.gov/submissions/CIK0001234567.json": _submissions_json(
                    accessions=[(first_accession, "SC 13G/A", "2025-09-15")]
                ),
                _archive_url("0001234567", first_accession, "primary_doc.xml"): first_xml,
            }
        )
        first_summary = ingest_filer_blockholders(conn, f1, filer_cik="0001234567")
        conn.commit()
        # Sanity: the prior 13G/A must actually have ingested. Without
        # this the supersession assertion below would pass vacuously.
        assert first_summary.rows_inserted == 1
        assert first_summary.accessions_failed == 0

        f2 = _InMemoryFetcher(
            {
                "https://data.sec.gov/submissions/CIK0001234567.json": _submissions_json(
                    accessions=[(second_accession, "SC 13D", "2025-11-06")]
                ),
                _archive_url("0001234567", second_accession, "primary_doc.xml"): second_xml,
            }
        )
        second_summary = ingest_filer_blockholders(conn, f2, filer_cik="0001234567")
        conn.commit()
        assert second_summary.rows_inserted == 1

        positions = latest_blockholder_positions(conn, instrument_id=766_010)

        assert len(positions) == 1
        position = positions[0]
        assert position.reporter_cik == "0001234567"
        assert position.submission_type == "SCHEDULE 13D"
        assert position.status == "active"
        assert position.aggregate_amount_owned == Decimal("2000000")
        assert position.percent_of_class == Decimal("7.0")
        assert position.accession_number == second_accession

    def test_no_cik_reporter_chains_by_name(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Two filings by the same no-CIK natural-person reporter on
        the same issuer should collapse to one position keyed on the
        reporter's name."""
        conn = _setup
        first_accession = "0001234567-25-000020"
        second_accession = "0001234567-25-000021"

        f1 = _InMemoryFetcher(
            {
                "https://data.sec.gov/submissions/CIK0001234567.json": _submissions_json(
                    accessions=[(first_accession, "SC 13D", "2025-09-01")]
                ),
                _archive_url("0001234567", first_accession, "primary_doc.xml"): _13d_multi_reporter_xml(
                    signature_date="09/01/2025",
                ),
            }
        )
        ingest_filer_blockholders(conn, f1, filer_cik="0001234567")
        conn.commit()

        f2 = _InMemoryFetcher(
            {
                "https://data.sec.gov/submissions/CIK0001234567.json": _submissions_json(
                    accessions=[(second_accession, "SC 13D/A", "2025-11-06")]
                ),
                _archive_url("0001234567", second_accession, "primary_doc.xml"): _13d_multi_reporter_xml(
                    signature_date="11/06/2025",
                ),
            }
        )
        ingest_filer_blockholders(conn, f2, filer_cik="0001234567")
        conn.commit()

        positions = latest_blockholder_positions(conn, instrument_id=766_010)
        # 2 reporters in the joint filing, each chains independently;
        # the latest accession wins for each.
        assert len(positions) == 2
        accessions = {p.accession_number for p in positions}
        assert accessions == {second_accession}


# ---------------------------------------------------------------------------
# Batch entry point
# ---------------------------------------------------------------------------


class TestIngestAllActiveFilers:
    def test_no_seeds_returns_empty(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        fetcher = _InMemoryFetcher({})
        result = ingest_all_active_filers(ebull_test_conn, fetcher)
        assert result == []

    def test_per_filer_crash_does_not_abort_batch(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """A filer whose ingest raises (DB error, parser bug, network
        exception bubbling past the per-accession handler) must not
        abort the batch — the loop catches and records the crash,
        then continues. Codex pre-push review caught the prior
        version of this test exercising the clean no-op path
        instead of the actual ``except Exception`` branch."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=766_030, symbol="AAPL")
        _seed_cusip_mapping(conn, instrument_id=766_030, cusip="037833100")
        seed_filer(conn, cik="0001234567", label="WORKING FUND")
        seed_filer(conn, cik="0009999999", label="CRASHING FUND")
        conn.commit()

        accession = "0001234567-25-000050"
        good_payloads: dict[str, str | None] = {
            "https://data.sec.gov/submissions/CIK0001234567.json": _submissions_json(
                accessions=[(accession, "SC 13D", "2025-11-06")]
            ),
            _archive_url("0001234567", accession, "primary_doc.xml"): _13d_xml(),
        }

        class _RaisingForOneCikFetcher:
            """Returns happy payloads for the working filer; raises a
            transport-style exception for the crashing filer's
            submissions URL. Exercises the batch's
            ``except Exception`` branch."""

            def __init__(self) -> None:
                self.calls: list[str] = []

            def fetch_document_text(self, absolute_url: str) -> str | None:
                self.calls.append(absolute_url)
                if "CIK0009999999" in absolute_url:
                    raise RuntimeError("simulated transport failure")
                return good_payloads.get(absolute_url)

        fetcher = _RaisingForOneCikFetcher()
        summaries = ingest_all_active_filers(conn, fetcher)

        # Working filer's summary is present; the crashing filer is
        # absent (its summary never returned because the function
        # raised — the loop's except-branch logged and continued).
        assert len(summaries) == 1
        assert summaries[0].filer_cik == "0001234567"
        assert summaries[0].rows_inserted == 1

        # data_ingestion_runs row records the partial run with the
        # crash citation in error.
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute("SELECT status, error FROM data_ingestion_runs ORDER BY ingestion_run_id DESC LIMIT 1")
            row = cur.fetchone()
        assert row is not None
        assert row["status"] == "partial"
        assert row["error"] is not None and "0009999999" in row["error"]

    def test_malformed_submissions_payload_downgrades_run_to_partial(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """A 200-body submissions payload that is not valid JSON (e.g.
        an HTML error page returned with a 200 status) must downgrade
        the batch run to ``partial``, not silently report ``success``
        with zero rows. Codex pre-push review caught this on PR
        review."""
        conn = ebull_test_conn
        seed_filer(conn, cik="0001234567", label="MALFORMED FUND")
        conn.commit()

        fetcher = _InMemoryFetcher(
            {
                "https://data.sec.gov/submissions/CIK0001234567.json": "<html>500 error</html>",
            }
        )
        summaries = ingest_all_active_filers(conn, fetcher)

        assert len(summaries) == 1
        assert summaries[0].submissions_fetch_failed is True
        assert summaries[0].first_error == "submissions JSON malformed"

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute("SELECT status, error FROM data_ingestion_runs ORDER BY ingestion_run_id DESC LIMIT 1")
            row = cur.fetchone()
        assert row is not None
        assert row["status"] == "partial"
        assert row["error"] is not None and "submissions fetch failed" in row["error"]

    def test_submissions_404_downgrades_run_to_partial(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """A curated filer whose per-CIK submissions JSON 404s gets a
        partial-status run with an explicit error citation, instead
        of a silent 'success' that hides the stale seed entry. Codex
        pre-push review caught the silent-success bug."""
        conn = ebull_test_conn
        seed_filer(conn, cik="0009999999", label="STALE FUND")
        conn.commit()

        fetcher = _InMemoryFetcher({})  # all fetches return None
        summaries = ingest_all_active_filers(conn, fetcher)

        assert len(summaries) == 1
        assert summaries[0].submissions_fetch_failed is True
        assert summaries[0].first_error == "submissions JSON 404/error"

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute("SELECT status, error FROM data_ingestion_runs ORDER BY ingestion_run_id DESC LIMIT 1")
            row = cur.fetchone()
        assert row is not None
        assert row["status"] == "partial"
        assert row["error"] is not None and "submissions fetch failed" in row["error"]

    def test_malformed_xml_is_tombstoned_not_raised(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Malformed primary_doc.xml (truncated / non-XML body) must
        produce a per-accession ``failed`` tombstone row, not a raise
        that escapes the per-accession handler. Codex pre-push
        review caught the missing ``ET.ParseError`` catch."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=766_040, symbol="AAPL")
        _seed_cusip_mapping(conn, instrument_id=766_040, cusip="037833100")
        seed_filer(conn, cik="0001234567", label="TEST")
        conn.commit()

        accession = "0001234567-25-000099"
        fetcher = _InMemoryFetcher(
            {
                "https://data.sec.gov/submissions/CIK0001234567.json": _submissions_json(
                    accessions=[(accession, "SC 13D", "2025-11-06")]
                ),
                _archive_url("0001234567", accession, "primary_doc.xml"): "<not-xml",
            }
        )

        summary = ingest_filer_blockholders(conn, fetcher, filer_cik="0001234567")
        conn.commit()

        assert summary.accessions_failed == 1
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT status, error FROM blockholder_filings_ingest_log WHERE accession_number = %s",
                (accession,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row["status"] == "failed"
        assert row["error"] is not None and "primary_doc.xml parse failed" in row["error"]
