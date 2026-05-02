"""Integration tests for the N-CEN filer-type classifier (#782)."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import psycopg
import psycopg.rows
import pytest

from app.services.institutional_holdings import classify_filer_type, seed_etf_filer, seed_filer
from app.services.ncen_classifier import (
    NCenClassification,
    _derive_filer_type,
    classify_filers_via_ncen,
    compose_filer_type,
    iter_classifications,
    parse_ncen_primary_doc,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Fixture builders
# ---------------------------------------------------------------------------


_NCEN_NS = "http://www.sec.gov/edgar/ncen"


def _ncen_xml(
    *,
    cik: str = "0001234567",
    investment_company_type: str = "N-1A",
    registrant_name: str = "TEST FUND",
) -> str:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="{_NCEN_NS}">
  <headerData>
    <submissionType>N-CEN</submissionType>
    <filerInfo>
      <filer>
        <issuerCredentials>
          <cik>{cik}</cik>
        </issuerCredentials>
      </filer>
      <investmentCompanyType>{investment_company_type}</investmentCompanyType>
    </filerInfo>
  </headerData>
  <formData>
    <registrantInfo>
      <registrantFullName>{registrant_name}</registrantFullName>
      <registrantCik>{cik}</registrantCik>
    </registrantInfo>
  </formData>
</edgarSubmission>
"""


def _submissions_json(*, accessions: list[tuple[str, str, str]]) -> str:
    """Same shape as data.sec.gov/submissions/CIK{cik}.json. Each
    tuple is ``(accession_number, form, filing_date)``."""
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


class _InMemoryFetcher:
    def __init__(self, payloads: dict[str, str | None]) -> None:
        self._payloads = payloads
        self.calls: list[str] = []

    def fetch_document_text(self, absolute_url: str) -> str | None:
        self.calls.append(absolute_url)
        return self._payloads.get(absolute_url)


def _archive_url(cik: str, accession: str, filename: str) -> str:
    return f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/{filename}"


def _seed_classification(
    conn: psycopg.Connection[tuple],
    *,
    cik: str,
    investment_company_type: str = "N-1A",
    derived_filer_type: str = "INV",
    accession_number: str = "ACC-1",
    filed_at: datetime | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO ncen_filer_classifications
            (cik, investment_company_type, derived_filer_type,
             accession_number, filed_at)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            cik,
            investment_company_type,
            derived_filer_type,
            accession_number,
            filed_at or datetime(2025, 11, 6, tzinfo=UTC),
        ),
    )


# ---------------------------------------------------------------------------
# Pure parser + mapping tests
# ---------------------------------------------------------------------------


class TestParser:
    def test_extracts_investment_company_type(self) -> None:
        assert parse_ncen_primary_doc(_ncen_xml(investment_company_type="N-1A")) == "N-1A"
        assert parse_ncen_primary_doc(_ncen_xml(investment_company_type="N-2")) == "N-2"
        assert parse_ncen_primary_doc(_ncen_xml(investment_company_type="N-3")) == "N-3"

    def test_missing_field_raises(self) -> None:
        broken = _ncen_xml().replace("<investmentCompanyType>N-1A</investmentCompanyType>", "")
        with pytest.raises(ValueError, match="investmentCompanyType"):
            parse_ncen_primary_doc(broken)


class TestDeriveFilerType:
    """Pin the SEC investment-company-type → filer-type mapping."""

    def test_n1a_maps_to_inv(self) -> None:
        assert _derive_filer_type("N-1A") == "INV"

    def test_n2_maps_to_inv(self) -> None:
        assert _derive_filer_type("N-2") == "INV"

    def test_variable_insurance_codes_map_to_ins(self) -> None:
        assert _derive_filer_type("N-3") == "INS"
        assert _derive_filer_type("N-4") == "INS"
        assert _derive_filer_type("N-6") == "INS"

    def test_n5_sbic_maps_to_inv(self) -> None:
        assert _derive_filer_type("N-5") == "INV"

    def test_unknown_code_maps_to_other(self) -> None:
        # Unknown codes (e.g. a future SEC enum addition) default
        # to OTHER so the data surfaces visibly without breaking
        # the classifier.
        assert _derive_filer_type("N-99") == "OTHER"
        assert _derive_filer_type("") == "OTHER"


# ---------------------------------------------------------------------------
# End-to-end batch classify
# ---------------------------------------------------------------------------


class TestClassifyBatch:
    @pytest.fixture
    def _setup(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> psycopg.Connection[tuple]:
        conn = ebull_test_conn
        seed_filer(conn, cik="0001234567", label="MUTUAL FUND")
        seed_filer(conn, cik="0007654321", label="VARIABLE INSURANCE")
        conn.commit()
        return conn

    def test_classifies_n1a_as_inv(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        conn = _setup
        accession = "0001234567-25-000001"
        cik = "0001234567"
        fetcher = _InMemoryFetcher(
            {
                f"https://data.sec.gov/submissions/CIK{cik}.json": _submissions_json(
                    accessions=[(accession, "N-CEN", "2025-11-14")]
                ),
                _archive_url(cik, accession, "primary_doc.xml"): _ncen_xml(cik=cik, investment_company_type="N-1A"),
            }
        )

        report = classify_filers_via_ncen(conn, fetcher, ciks=[cik])

        assert report.classifications_written == 1
        assert report.no_ncen_found == 0

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT investment_company_type, derived_filer_type FROM ncen_filer_classifications WHERE cik = %s",
                (cik,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row["investment_company_type"] == "N-1A"
        assert row["derived_filer_type"] == "INV"

    def test_classifies_n3_as_ins(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        conn = _setup
        cik = "0007654321"
        accession = "0007654321-25-000001"
        fetcher = _InMemoryFetcher(
            {
                f"https://data.sec.gov/submissions/CIK{cik}.json": _submissions_json(
                    accessions=[(accession, "N-CEN", "2025-11-14")]
                ),
                _archive_url(cik, accession, "primary_doc.xml"): _ncen_xml(cik=cik, investment_company_type="N-3"),
            }
        )

        report = classify_filers_via_ncen(conn, fetcher, ciks=[cik])
        assert report.classifications_written == 1

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT derived_filer_type FROM ncen_filer_classifications WHERE cik = %s",
                (cik,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row["derived_filer_type"] == "INS"

    def test_no_ncen_in_submissions_is_counted_separately(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Filer with only 13F filings (no N-CEN) is not a failure
        — the no_ncen_found counter increments and no row is
        written."""
        conn = _setup
        cik = "0001234567"
        fetcher = _InMemoryFetcher(
            {
                f"https://data.sec.gov/submissions/CIK{cik}.json": _submissions_json(
                    accessions=[("0001234567-25-000010", "13F-HR", "2025-11-06")]
                ),
            }
        )

        report = classify_filers_via_ncen(conn, fetcher, ciks=[cik])
        assert report.classifications_written == 0
        assert report.no_ncen_found == 1

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ncen_filer_classifications")
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 0

    def test_picks_latest_ncen(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """When the submissions index has multiple N-CEN filings,
        the classifier picks the first (newest) match. The
        recent-filings array is ordered newest-first by SEC
        convention."""
        conn = _setup
        cik = "0001234567"
        latest_accession = "0001234567-25-000050"
        fetcher = _InMemoryFetcher(
            {
                f"https://data.sec.gov/submissions/CIK{cik}.json": _submissions_json(
                    accessions=[
                        (latest_accession, "N-CEN", "2025-11-14"),
                        ("0001234567-24-000050", "N-CEN", "2024-11-14"),
                    ]
                ),
                _archive_url(cik, latest_accession, "primary_doc.xml"): _ncen_xml(
                    cik=cik, investment_company_type="N-2"
                ),
            }
        )

        classify_filers_via_ncen(conn, fetcher, ciks=[cik])

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT accession_number, derived_filer_type FROM ncen_filer_classifications WHERE cik = %s",
                (cik,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row["accession_number"] == latest_accession

    def test_re_run_upserts_in_place(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Two passes on the same N-CEN UPSERT — one row in the
        table, fetched_at refreshed."""
        conn = _setup
        cik = "0001234567"
        accession = "0001234567-25-000001"
        fetcher = _InMemoryFetcher(
            {
                f"https://data.sec.gov/submissions/CIK{cik}.json": _submissions_json(
                    accessions=[(accession, "N-CEN", "2025-11-14")]
                ),
                _archive_url(cik, accession, "primary_doc.xml"): _ncen_xml(cik=cik),
            }
        )

        first = classify_filers_via_ncen(conn, fetcher, ciks=[cik])
        assert first.classifications_written == 1

        second = classify_filers_via_ncen(conn, fetcher, ciks=[cik])
        assert second.classifications_written == 1

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ncen_filer_classifications WHERE cik = %s", (cik,))
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 1

    def test_malformed_submissions_json_counts_as_fetch_failure(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """A 200-OK garbage submissions body must surface as a
        fetch_failure rather than silently no-op into no_ncen.
        Codex pre-push review caught _find_latest_ncen collapsing
        malformed JSON into a None ref (same bucket as 'no N-CEN
        on file')."""
        conn = _setup
        cik = "0001234567"
        fetcher = _InMemoryFetcher(
            {
                f"https://data.sec.gov/submissions/CIK{cik}.json": "<html>500 error</html>",
            }
        )

        report = classify_filers_via_ncen(conn, fetcher, ciks=[cik])
        assert report.classifications_written == 0
        assert report.no_ncen_found == 0
        assert report.fetch_failures == 1
        assert report.parse_failures == 0

    def test_primary_doc_404_counts_as_fetch_failure(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """A submissions index that names an N-CEN whose primary
        doc 404s is a fetch_failure, not a parse_failure. Codex
        pre-push review caught the prior 'fetch' substring check
        bucketing this as a parse failure."""
        conn = _setup
        cik = "0001234567"
        fetcher = _InMemoryFetcher(
            {
                f"https://data.sec.gov/submissions/CIK{cik}.json": _submissions_json(
                    accessions=[("0001234567-25-000001", "N-CEN", "2025-11-14")]
                ),
                # No payload for the primary doc URL — fetch returns None.
            }
        )

        report = classify_filers_via_ncen(conn, fetcher, ciks=[cik])
        assert report.fetch_failures == 1
        assert report.parse_failures == 0

    def test_upsert_failure_does_not_abort_batch(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """A DB error during the upsert / commit for one filer must
        not abort the batch — the next filer continues. Codex
        pre-push review caught the prior code that only wrapped
        _classify_single_filer in try/except."""
        from typing import Any as _Any

        from app.services import ncen_classifier as ncen_mod

        conn = ebull_test_conn
        seed_filer(conn, cik="0000000001", label="A")
        seed_filer(conn, cik="0000000002", label="B")
        conn.commit()

        accession_a = "0000000001-25-000001"
        accession_b = "0000000002-25-000001"
        fetcher = _InMemoryFetcher(
            {
                "https://data.sec.gov/submissions/CIK0000000001.json": _submissions_json(
                    accessions=[(accession_a, "N-CEN", "2025-11-14")]
                ),
                "https://data.sec.gov/submissions/CIK0000000002.json": _submissions_json(
                    accessions=[(accession_b, "N-CEN", "2025-11-14")]
                ),
                _archive_url("0000000001", accession_a, "primary_doc.xml"): _ncen_xml(cik="0000000001"),
                _archive_url("0000000002", accession_b, "primary_doc.xml"): _ncen_xml(cik="0000000002"),
            }
        )

        original_upsert = ncen_mod._upsert_classification
        calls: list[str] = []

        def _failing_upsert(c: _Any, classification: NCenClassification) -> None:
            calls.append(classification.cik)
            if classification.cik == "0000000001":
                raise RuntimeError("simulated DB error")
            original_upsert(c, classification)

        ncen_mod._upsert_classification = _failing_upsert  # type: ignore[assignment]
        try:
            report = classify_filers_via_ncen(conn, fetcher, ciks=["0000000001", "0000000002"])
        finally:
            ncen_mod._upsert_classification = original_upsert  # type: ignore[assignment]

        # Both filers were attempted — the failure on filer A did
        # not abort the batch.
        assert calls == ["0000000001", "0000000002"]
        # Filer B succeeded despite filer A's crash.
        assert report.classifications_written == 1
        assert report.crash_failures == 1
        assert report.fetch_failures == 0
        assert report.parse_failures == 0

        # Filer B's row landed.
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ncen_filer_classifications WHERE cik = %s", ("0000000002",))
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 1

    def test_parse_failure_counted_separately(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        conn = _setup
        cik = "0001234567"
        accession = "0001234567-25-000001"
        broken_xml = _ncen_xml(cik=cik).replace("<investmentCompanyType>N-1A</investmentCompanyType>", "")
        fetcher = _InMemoryFetcher(
            {
                f"https://data.sec.gov/submissions/CIK{cik}.json": _submissions_json(
                    accessions=[(accession, "N-CEN", "2025-11-14")]
                ),
                _archive_url(cik, accession, "primary_doc.xml"): broken_xml,
            }
        )

        report = classify_filers_via_ncen(conn, fetcher, ciks=[cik])
        assert report.classifications_written == 0
        assert report.parse_failures == 1


# ---------------------------------------------------------------------------
# Compose function — priority chain
# ---------------------------------------------------------------------------


class TestComposeFilerType:
    def test_etf_seed_overrides_ncen(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Curated ETF list always wins, even when N-CEN says
        N-1A → INV. (Vanguard ETFs are open-end mutual funds at
        the registrant level but the CIK-specific share class is
        an ETF — the curated list disambiguates.)"""
        conn = ebull_test_conn
        cik = "0001234567"
        seed_filer(conn, cik=cik, label="VANGUARD ETF")
        seed_etf_filer(conn, cik=cik, label="Vanguard S&P 500 ETF")
        _seed_classification(conn, cik=cik, investment_company_type="N-1A", derived_filer_type="INV")
        conn.commit()

        assert compose_filer_type(conn, cik) == "ETF"

    def test_ncen_classification_overrides_default(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """N-CEN-derived INS beats the INV default."""
        conn = ebull_test_conn
        cik = "0007654321"
        seed_filer(conn, cik=cik, label="VARIABLE INSURANCE")
        _seed_classification(conn, cik=cik, investment_company_type="N-3", derived_filer_type="INS")
        conn.commit()

        assert compose_filer_type(conn, cik) == "INS"

    def test_no_classification_returns_inv_default(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        cik = "0009999999"
        seed_filer(conn, cik=cik, label="UNKNOWN FILER")
        conn.commit()

        assert compose_filer_type(conn, cik) == "INV"

    def test_classify_filer_type_delegates_to_compose(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Sanity check: ``classify_filer_type`` (the public name
        called by the 13F-HR ingester) returns the same answer as
        ``compose_filer_type`` directly."""
        conn = ebull_test_conn
        cik = "0001234567"
        seed_filer(conn, cik=cik, label="TEST")
        _seed_classification(conn, cik=cik, investment_company_type="N-3", derived_filer_type="INS")
        conn.commit()

        assert classify_filer_type(conn, cik) == "INS"
        assert classify_filer_type(conn, cik) == compose_filer_type(conn, cik)


# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------


class TestIterClassifications:
    def test_filter_by_derived_filer_type(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        seed_filer(conn, cik="0000000001", label="A")
        seed_filer(conn, cik="0000000002", label="B")
        _seed_classification(
            conn,
            cik="0000000001",
            investment_company_type="N-1A",
            derived_filer_type="INV",
        )
        _seed_classification(
            conn,
            cik="0000000002",
            investment_company_type="N-3",
            derived_filer_type="INS",
        )
        conn.commit()

        ins_only = list(iter_classifications(conn, derived_filer_type="INS"))
        assert len(ins_only) == 1
        assert ins_only[0]["cik"] == "0000000002"

        all_rows = list(iter_classifications(conn))
        assert len(all_rows) == 2
