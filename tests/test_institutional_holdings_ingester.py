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
      <periodOfReport>09-30-2024</periodOfReport>
    </filerInfo>
  </headerData>
  <formData>
    <coverPage>
      <reportCalendarOrQuarter>09-30-2024</reportCalendarOrQuarter>
      <filingManager>
        <name>BERKSHIRE HATHAWAY INC</name>
        <address>
          <street1>3555 Farnam Street</street1>
          <city>Omaha</city>
          <stateOrCountry>NE</stateOrCountry>
          <zipCode>68131</zipCode>
        </address>
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
        ON CONFLICT (provider, identifier_type, identifier_value)
            WHERE NOT (provider = 'sec' AND identifier_type = 'cik')
        DO NOTHING
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
        # filed_at must be tz-aware UTC so it lands in TIMESTAMPTZ
        # without psycopg falling back to the server's local zone.
        assert ref.filed_at == datetime(2025, 2, 14, tzinfo=UTC)
        assert ref.filed_at is not None and ref.filed_at.tzinfo is UTC

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

    def test_vanguard_13f_prefixed_naming(self) -> None:
        """Live data showed Vanguard's infotable named
        ``13F_<cik>_<period_end>.xml`` — caught when the curated
        seed ingest landed zero holdings on every Vanguard
        accession. Parser must accept the ``13F`` prefix."""
        primary, infotable = parse_archive_index(_archive_index_json(infotable="13F_0000102909_20251231.xml"))
        assert primary == "primary_doc.xml"
        assert infotable == "13F_0000102909_20251231.xml"

    def test_form13f_prefix_naming(self) -> None:
        """``form13f`` prefix variant (BlackRock and similar)."""
        primary, infotable = parse_archive_index(_archive_index_json(infotable="form13fInformationTable_2025Q4.xml"))
        assert infotable == "form13fInformationTable_2025Q4.xml"

    def test_unrecognised_single_xml_falls_back(self) -> None:
        """When neither name pattern matches but exactly one
        non-primary_doc XML exists, it must be picked as the
        infotable. SEC convention caps a 13F-HR submission at two
        XML attachments so single-extra-XML is unambiguous."""
        primary, infotable = parse_archive_index(_archive_index_json(infotable="something_unusual.xml"))
        assert primary == "primary_doc.xml"
        assert infotable == "something_unusual.xml"

    def test_multiple_unrecognised_xmls_returns_none(self) -> None:
        """Two non-primary_doc XMLs neither matching the name
        patterns is genuinely ambiguous — fallback must NOT pick
        one arbitrarily."""
        # Build a payload with primary_doc + two unmarked XMLs.
        import json as _json

        items: list[dict[str, str]] = [
            {"name": "primary_doc.xml", "type": "text.gif", "size": "100"},
            {"name": "weird_a.xml", "type": "text.gif", "size": "100"},
            {"name": "weird_b.xml", "type": "text.gif", "size": "100"},
        ]
        payload = _json.dumps({"directory": {"name": "/Archives/...", "item": items}})
        primary, infotable = parse_archive_index(payload)
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

    def test_raw_payload_persisted_for_primary_doc_and_infotable(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """13F ingester must persist BOTH the primary_doc.xml and
        the infotable.xml bodies to ``filing_raw_documents`` before
        parsing — operator audit 2026-05-03 + PR #808 contract.
        Re-wash workflows depend on these rows."""
        from app.services import raw_filings

        conn = _setup
        fetcher = self._build_fetcher(
            holdings=[
                {"cusip": "037833100", "name": "APPLE INC", "value": "1", "shares": "1"},
            ]
        )
        ingest_filer_13f(conn, fetcher, filer_cik="0001067983")
        conn.commit()

        primary = raw_filings.read_raw(
            conn,
            accession_number="0001067983-25-000001",
            document_kind="primary_doc",
        )
        assert primary is not None
        assert "BERKSHIRE" in primary.payload.upper() or "<edgarSubmission" in primary.payload
        assert primary.parser_version == "13f-primary-v1"
        assert primary.source_url is not None
        assert primary.source_url.endswith("primary_doc.xml")

        infotable = raw_filings.read_raw(
            conn,
            accession_number="0001067983-25-000001",
            document_kind="infotable_13f",
        )
        assert infotable is not None
        assert "037833100" in infotable.payload  # the seeded CUSIP
        assert infotable.parser_version == "13f-infotable-v1"
        assert infotable.source_url is not None
        assert infotable.source_url.endswith("infotable.xml")

    def test_raw_payload_survives_parse_failure(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Codex pre-push review (PR follow-up to #808): if
        primary_doc.xml or infotable.xml is malformed and the parser
        raises ET.ParseError, the previously-stored raw body must
        survive — the whole point of raw retention is debugging
        parser failures without re-fetching SEC."""
        from app.services import raw_filings

        conn = _setup
        # Corrupt the infotable so parse_infotable raises ParseError
        # while primary_doc.xml is still valid.
        cik_int = 1067983
        accession = "0001067983-25-000099"
        accn_no_dashes = accession.replace("-", "")
        archive_base = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accn_no_dashes}/"
        payloads: dict[str, str | None] = {
            "https://data.sec.gov/submissions/CIK0001067983.json": _submissions_json(
                accessions=[(accession, "13F-HR", "2025-02-14", "2024-12-31")]
            ),
            archive_base + "index.json": _archive_index_json(),
            archive_base + "primary_doc.xml": _PRIMARY_DOC_XML,
            archive_base + "infotable.xml": "<not-valid-xml<<<",  # malformed
        }
        fetcher = _InMemoryFetcher(payloads)

        ingest_filer_13f(conn, fetcher, filer_cik="0001067983")
        conn.commit()

        # Both raw rows persisted despite the parse failure.
        primary = raw_filings.read_raw(
            conn,
            accession_number=accession,
            document_kind="primary_doc",
        )
        assert primary is not None
        infotable = raw_filings.read_raw(
            conn,
            accession_number=accession,
            document_kind="infotable_13f",
        )
        assert infotable is not None
        assert infotable.payload == "<not-valid-xml<<<"

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


class TestUniverseSweep:
    """#913 / #841 PR2: ``ingest_all_active_filers`` ingests every
    CIK in ``institutional_filers`` (the directory populated by
    sec_13f_filer_directory_sync #912), with optional deadline budget
    so a long sweep stops cleanly + resumes next fire."""

    def _build_filer_payloads(
        self,
        *,
        cik: str,
        accession: str,
        period: str,
    ) -> dict[str, str | None]:
        cik_int = int(cik)
        primary_doc = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<edgarSubmission xmlns="http://www.sec.gov/edgar/thirteenffiler">\n'
            "  <headerData><filerInfo><filer><credentials>\n"
            f"    <cik>{cik_int}</cik></credentials></filer></filerInfo></headerData>\n"
            "  <formData>\n"
            f"    <coverPage><reportCalendarOrQuarter>{period}</reportCalendarOrQuarter></coverPage>\n"
            f"    <signatureBlock><signatureDate>{period}</signatureDate></signatureBlock>\n"
            f"    <filingManager><name>FAKE FILER {cik}</name></filingManager>\n"
            "    <summaryPage><tableValueTotal>0</tableValueTotal><tableEntryTotal>0</tableEntryTotal></summaryPage>\n"
            "  </formData>\n"
            "</edgarSubmission>\n"
        )
        infotable = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable" />\n'
        )
        archive_base = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession.replace('-', '')}/"
        return {
            f"https://data.sec.gov/submissions/CIK{cik}.json": _submissions_json(
                accessions=[(accession, "13F-HR", period, period)],
            ),
            archive_base + "index.json": _archive_index_json(),
            archive_base + "primary_doc.xml": primary_doc,
            archive_base + "infotable.xml": infotable,
        }

    def test_list_directory_filer_ciks_orders_by_last_filing_desc_then_cik(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        from app.services.institutional_holdings import list_directory_filer_ciks

        # Mix of dated + NULL ``last_filing_at`` rows; expect dated
        # rows newest-first, then NULL rows ordered by cik.
        conn = ebull_test_conn
        conn.execute(
            "INSERT INTO institutional_filers (cik, name, filer_type, last_filing_at) VALUES "
            "('0000000300', 'C', 'INV', '2026-03-01'::timestamptz), "
            "('0000000100', 'A', 'INV', '2026-04-01'::timestamptz), "
            "('0000000200', 'B', 'INV', NULL), "
            "('0000000400', 'D', 'INV', NULL)"
        )
        conn.commit()

        result = list_directory_filer_ciks(conn)
        assert result == ["0000000100", "0000000300", "0000000200", "0000000400"]

    def test_explicit_ciks_list_overrides_seed_lookup(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Passing ``ciks=[...]`` walks exactly those CIKs and
        ignores ``institutional_filer_seeds`` entirely. The
        sec_13f_quarterly_sweep job uses this to walk the directory."""
        conn = ebull_test_conn
        # Seed-list CIK that should NOT be reached.
        seed_filer(conn, cik="0009999991", label="SEED-ONLY")
        # Directory CIK that should be reached.
        _seed_instrument(conn, iid=913_001, symbol="AAPL")
        _seed_cusip_mapping(conn, instrument_id=913_001, cusip="037833100")
        conn.commit()

        accession = "0001067983-25-000001"
        fetcher = _InMemoryFetcher(
            self._build_filer_payloads(cik="0001067983", accession=accession, period="2024-12-31")
        )

        summaries = ingest_all_active_filers(
            conn,
            fetcher,
            ciks=["0001067983"],
            source_label="sec_edgar_13f_directory",
        )

        # SEED-ONLY filer was NOT contacted.
        assert not any("0009999991" in url for url in fetcher.calls)
        # Directory filer WAS contacted.
        assert any("0001067983" in url for url in fetcher.calls)
        assert [s.filer_cik for s in summaries] == ["0001067983"]

        # data_ingestion_runs row tagged with the directory source.
        with conn.cursor() as cur:
            cur.execute("SELECT source FROM data_ingestion_runs ORDER BY ingestion_run_id DESC LIMIT 1")
            row = cur.fetchone()
        assert row is not None
        assert row[0] == "sec_edgar_13f_directory"

    def test_bootstrap_cancel_signal_stops_loop_and_raises(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """PR3d follow-up #1064 — ``ingest_all_active_filers`` polls the
        bootstrap cancel-signal between filers when invoked under
        ``active_bootstrap_run``. The cooperative cancel observation
        latency drops from the 6h soft deadline to one filer-iteration
        (~30s in production).

        The bookkeeping path runs before the raise so
        ``data_ingestion_runs`` records ``status='partial'`` with the
        operator-cancel reason, and the orchestrator's
        ``BootstrapStageCancelled`` handler then writes the
        ``cancelled`` row to ``bootstrap_stages``.
        """
        from app.config import settings as app_settings
        from app.services.bootstrap_state import (
            BootstrapStageCancelled,
            StageSpec,
            cancel_run,
            start_run,
        )
        from app.services.processes.bootstrap_cancel_signal import (
            active_bootstrap_run,
        )
        from tests.fixtures.ebull_test_db import test_database_url

        # bootstrap_cancel_requested() opens a fresh autocommit
        # connection from settings.database_url to probe the stop
        # request row. Point that at the test DB so the probe sees the
        # cancel_run we issue below (without the rebind it queries the
        # dev DB and finds nothing).
        monkeypatch.setattr(app_settings, "database_url", test_database_url())
        conn = ebull_test_conn
        # Reset bootstrap_state so start_run is allowed.
        conn.execute(
            """
            UPDATE bootstrap_state
               SET status='pending', last_run_id=NULL, last_completed_at=NULL
             WHERE id=1
            """
        )
        # One real registered job per stage so JobLock's source resolver
        # would work; the test never enters JobLock since
        # ingest_all_active_filers itself is the unit under test.
        run_id = start_run(
            conn,
            operator_id=None,
            stage_specs=(
                StageSpec(
                    stage_key="sec_13f_quarterly_sweep",
                    stage_order=1,
                    lane="sec_rate",
                    job_name="sec_13f_quarterly_sweep",
                ),
            ),
        )
        cancel_run(conn, requested_by_operator_id=None)
        conn.commit()

        _seed_instrument(conn, iid=1064001, symbol="AAPL")
        _seed_cusip_mapping(conn, instrument_id=1064001, cusip="037833100")
        conn.commit()

        ciks = ["0000000010", "0000000020", "0000000030"]
        payloads: dict[str, str | None] = {}
        for c in ciks:
            payloads.update(self._build_filer_payloads(cik=c, accession=f"{c}-25-000001", period="2024-12-31"))
        fetcher = _InMemoryFetcher(payloads)

        with active_bootstrap_run(run_id):
            with pytest.raises(BootstrapStageCancelled) as exc_info:
                ingest_all_active_filers(
                    conn,
                    fetcher,
                    ciks=ciks,
                    source_label="sec_edgar_13f_directory",
                )

        assert "cancelled by operator" in str(exc_info.value)

        # Bookkeeping path ran before the raise: data_ingestion_runs
        # carries the partial state + cancel reason.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, error FROM data_ingestion_runs "
                "WHERE source = 'sec_edgar_13f_directory' "
                "ORDER BY ingestion_run_id DESC LIMIT 1"
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == "partial"
        assert row[1] is not None
        assert "cancelled by operator" in row[1].lower()

        # Cancel was observed on the very first iteration, so no filer
        # should have been contacted.
        contacted_ciks = {c for c in ciks if any(c in url for url in fetcher.calls)}
        assert contacted_ciks == set()

    def test_bootstrap_cancel_outranks_deadline_when_both_fire(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Codex pre-push round 1 — when the soft deadline AND the
        operator-cancel signal both fire on the same iteration, cancel
        must win. Otherwise the deadline branch returns normally,
        BootstrapStageCancelled is never raised, and the orchestrator
        marks the stage ``success`` instead of ``cancelled``.
        """
        from app.config import settings as app_settings
        from app.services import institutional_holdings as svc
        from app.services.bootstrap_state import (
            BootstrapStageCancelled,
            StageSpec,
            cancel_run,
            start_run,
        )
        from app.services.processes.bootstrap_cancel_signal import (
            active_bootstrap_run,
        )
        from tests.fixtures.ebull_test_db import test_database_url

        monkeypatch.setattr(app_settings, "database_url", test_database_url())
        conn = ebull_test_conn
        conn.execute(
            """
            UPDATE bootstrap_state
               SET status='pending', last_run_id=NULL, last_completed_at=NULL
             WHERE id=1
            """
        )
        run_id = start_run(
            conn,
            operator_id=None,
            stage_specs=(
                StageSpec(
                    stage_key="sec_13f_quarterly_sweep",
                    stage_order=1,
                    lane="sec_rate",
                    job_name="sec_13f_quarterly_sweep",
                ),
            ),
        )
        cancel_run(conn, requested_by_operator_id=None)
        conn.commit()

        # Stub time.monotonic so the deadline IS expired by the
        # first loop iteration's check, while the deadline_ts
        # computation at function entry sees t=0.
        #   call 1 (deadline_ts setup):     0.0    → deadline_ts = 1.0
        #   call 2+ (loop iter checks):     5.0    → 5.0 >= 1.0, expired
        # Last value sticks if extra calls land. Codex round 2.
        clock_ticks = [0.0, 5.0]
        clock_idx = [0]

        def _fake_monotonic() -> float:
            i = min(clock_idx[0], len(clock_ticks) - 1)
            clock_idx[0] += 1
            return clock_ticks[i]

        monkeypatch.setattr(svc.time, "monotonic", _fake_monotonic)

        ciks = ["0000000010", "0000000020"]
        payloads: dict[str, str | None] = {}
        for c in ciks:
            payloads.update(self._build_filer_payloads(cik=c, accession=f"{c}-25-000001", period="2024-12-31"))
        fetcher = _InMemoryFetcher(payloads)

        with active_bootstrap_run(run_id):
            with pytest.raises(BootstrapStageCancelled):
                ingest_all_active_filers(
                    conn,
                    fetcher,
                    ciks=ciks,
                    deadline_seconds=1.0,
                    source_label="sec_edgar_13f_directory",
                )

        # data_ingestion_runs.error mentions cancellation but NOT
        # deadline — proves the cancel branch fired before the
        # deadline check on the same iteration. With the previous
        # ordering (deadline first) this assertion would invert.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, error FROM data_ingestion_runs "
                "WHERE source = 'sec_edgar_13f_directory' "
                "ORDER BY ingestion_run_id DESC LIMIT 1"
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == "partial"
        assert row[1] is not None
        error_lower = row[1].lower()
        assert "cancelled by operator" in error_lower
        assert "deadline" not in error_lower

    def test_deadline_budget_stops_loop_cleanly_and_records_partial(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A soft deadline interrupts the loop between filers; the
        partial work commits, ``data_ingestion_runs.error`` records
        the cut-off so the operator knows the next sweep resumes."""
        from app.services import institutional_holdings as svc

        conn = ebull_test_conn
        _seed_instrument(conn, iid=913_010, symbol="AAPL")
        _seed_cusip_mapping(conn, instrument_id=913_010, cusip="037833100")
        conn.commit()

        # Build payloads for three filers; deadline trips after the
        # first iteration.
        payloads: dict[str, str | None] = {}
        ciks = ["0000000010", "0000000020", "0000000030"]
        for c in ciks:
            payloads.update(self._build_filer_payloads(cik=c, accession=f"{c}-25-000001", period="2024-12-31"))
        fetcher = _InMemoryFetcher(payloads)

        # Stub time.monotonic to fire the deadline immediately AFTER
        # the first filer iteration. Sequence:
        #   call 1: deadline_ts = 0 + 1 = 1
        #   call 2: pre-loop check (filer 0) → 0 < 1, proceed
        #   call 3+: pre-loop check (filer 1+) → 5 >= 1, deadline_hit
        # The clock returns the LAST value once exhausted (rather
        # than raising StopIteration) so any extra deadline checks
        # added in future refactors don't make the test brittle.
        # Codex pre-push review #913.
        clock_ticks = [0.0, 0.0, 5.0]
        clock_idx = [0]

        def _fake_monotonic() -> float:
            i = min(clock_idx[0], len(clock_ticks) - 1)
            clock_idx[0] += 1
            return clock_ticks[i]

        monkeypatch.setattr(svc.time, "monotonic", _fake_monotonic)

        ingest_all_active_filers(
            conn,
            fetcher,
            ciks=ciks,
            deadline_seconds=1.0,
            source_label="sec_edgar_13f_directory",
        )

        # data_ingestion_runs.error mentions deadline.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, error FROM data_ingestion_runs "
                "WHERE source = 'sec_edgar_13f_directory' "
                "ORDER BY ingestion_run_id DESC LIMIT 1"
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == "partial"
        assert row[1] is not None
        assert "deadline" in row[1].lower()

        # Only the first filer's submissions URL was fetched — the
        # remaining two never got contacted.
        contacted_ciks = {c for c in ciks if any(c in url for url in fetcher.calls)}
        assert contacted_ciks == {"0000000010"}


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
