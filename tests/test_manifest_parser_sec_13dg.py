"""Tests for the SC 13D / 13G manifest-worker parser adapter (#873).

One callable registered against BOTH ``sec_13d`` and ``sec_13g``
sources. Tests cover:

- Happy path 13D + 13G: XML fetch → store_raw → parse → upsert
  filer + filings → observation write-through (when CUSIP resolves)
  → ParseOutcome(parsed).
- Tombstone on empty fetch: 404 returns tombstoned + records
  ``failed`` ingest-log row.
- Fetch raises: returns failed with 1h backoff.
- Parse-phase exception preserves raw_status='stored'.
- CUSIP unresolved: filing rows still upserted, observation skipped,
  ingest-log status='partial'.
- Registration: both sec_13d AND sec_13g wired by register_all_parsers.

The fetch boundary is monkeypatched at
``SecFilingsProvider.fetch_document_text`` level so tests run
without touching SEC.
"""

from __future__ import annotations

from datetime import UTC, datetime

import psycopg
import pytest

from app.jobs.sec_manifest_worker import (
    clear_registered_parsers,
    run_manifest_worker,
)
from app.services.sec_manifest import get_manifest_row, record_manifest_entry
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

# 13D and 13G primary_doc.xml fixtures — pattern copied from
# tests/test_sec_13dg_parser.py so they are guaranteed-parseable
# upstream. Single reporter each to keep assertions tight.

_NS_13D = "http://www.sec.gov/edgar/schedule13D"
_NS_13G = "http://www.sec.gov/edgar/schedule13g"


_FAKE_13D_XML = f"""<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="{_NS_13D}">
  <headerData>
    <submissionType>SCHEDULE 13D</submissionType>
    <filerInfo>
      <filer>
        <filerCredentials>
          <cik>0002093607</cik>
        </filerCredentials>
      </filer>
    </filerInfo>
  </headerData>
  <formData>
    <coverPageHeader>
      <securitiesClassTitle>Class A Common Stock, par value $.01 per share</securitiesClassTitle>
      <dateOfEvent>11/03/2025</dateOfEvent>
      <issuerInfo>
        <issuerCIK>0001001250</issuerCIK>
        <issuerCUSIP>518439104</issuerCUSIP>
        <issuerName>The Estee Lauder Companies Inc.</issuerName>
      </issuerInfo>
    </coverPageHeader>
    <reportingPersons>
      <reportingPersonInfo>
        <reportingPersonCIK>0002093607</reportingPersonCIK>
        <reportingPersonNoCIK>N</reportingPersonNoCIK>
        <reportingPersonName>Roaring Fork Trust Company, Inc.</reportingPersonName>
        <memberOfGroup>b</memberOfGroup>
        <citizenshipOrOrganization>SD</citizenshipOrOrganization>
        <soleVotingPower>1500000</soleVotingPower>
        <sharedVotingPower>0</sharedVotingPower>
        <soleDispositivePower>1500000</soleDispositivePower>
        <sharedDispositivePower>0</sharedDispositivePower>
        <aggregateAmountOwned>1500000</aggregateAmountOwned>
        <percentOfClass>5.5</percentOfClass>
        <typeOfReportingPerson>CO</typeOfReportingPerson>
      </reportingPersonInfo>
    </reportingPersons>
    <signatureInfo>
      <signaturePerson>
        <signatureDetails>
          <date>11/06/2025</date>
        </signatureDetails>
      </signaturePerson>
    </signatureInfo>
  </formData>
</edgarSubmission>
"""


_FAKE_13G_XML = f"""<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="{_NS_13G}">
  <headerData>
    <submissionType>SCHEDULE 13G</submissionType>
    <filerInfo>
      <filer>
        <filerCredentials>
          <cik>0002083532</cik>
        </filerCredentials>
      </filer>
    </filerInfo>
  </headerData>
  <formData>
    <coverPageHeader>
      <securitiesClassTitle>Common Shares, no par value</securitiesClassTitle>
      <eventDateRequiresFilingThisStatement>09/30/2025</eventDateRequiresFilingThisStatement>
      <issuerInfo>
        <issuerCik>0001468642</issuerCik>
        <issuerName>Aura Minerals Inc.</issuerName>
        <issuerCusip>G06973112</issuerCusip>
      </issuerInfo>
    </coverPageHeader>
    <coverPageHeaderReportingPersonDetails>
      <reportingPersonName>De Brito Paulo Carlos</reportingPersonName>
      <citizenshipOrOrganization>D5</citizenshipOrOrganization>
      <reportingPersonBeneficiallyOwnedNumberOfShares>
        <soleVotingPower>39838685.00</soleVotingPower>
        <sharedVotingPower>0.00</sharedVotingPower>
        <soleDispositivePower>39838685.00</soleDispositivePower>
        <sharedDispositivePower>0.00</sharedDispositivePower>
      </reportingPersonBeneficiallyOwnedNumberOfShares>
      <reportingPersonBeneficiallyOwnedAggregateNumberOfShares>39838685.00</reportingPersonBeneficiallyOwnedAggregateNumberOfShares>
      <classPercent>47.6843</classPercent>
      <typeOfReportingPerson>IN</typeOfReportingPerson>
    </coverPageHeaderReportingPersonDetails>
    <signatureInfo>
      <signaturePerson>
        <signatureDetails>
          <date>10/15/2025</date>
        </signatureDetails>
      </signaturePerson>
    </signatureInfo>
  </formData>
</edgarSubmission>
"""


def _seed_instrument_with_cusip(
    conn: psycopg.Connection[tuple],
    *,
    iid: int,
    symbol: str,
    cusip: str,
) -> None:
    """Seed an instrument + CUSIP mapping in external_identifiers so
    _resolve_cusip_to_instrument_id can join."""
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} co"),
    )
    conn.execute(
        """
        INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, is_primary)
        VALUES (%s, 'sec', 'cusip', %s, TRUE)
        ON CONFLICT DO NOTHING
        """,
        (iid, cusip),
    )


def _seed_pending_13dg(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    filer_cik: str,
    source: str,
    form: str,
) -> None:
    """Seed a pending manifest row for a 13D/G accession. Subject is
    the filer (subject_type='blockholder_filer'); instrument_id is
    NULL — issuer linkage resolves at parse-time via CUSIP."""
    record_manifest_entry(
        conn,
        accession,
        cik=filer_cik,
        form=form,
        source=source,  # type: ignore[arg-type]
        subject_type="blockholder_filer",
        subject_id=filer_cik,
        instrument_id=None,
        filed_at=datetime(2026, 5, 11, tzinfo=UTC),
        primary_document_url="https://www.sec.gov/Archives/edgar/data/2093607/000114036125040863/primary_doc.xml",
    )


@pytest.fixture(autouse=True)
def _reset_registry_then_reload():
    from app.services.manifest_parsers import register_all_parsers

    clear_registered_parsers()
    register_all_parsers()
    yield
    clear_registered_parsers()
    register_all_parsers()


def test_13d_happy_path_resolves_cusip_and_writes_observation(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SC 13D: fetch → store_raw → parse → upsert filer + filing →
    observation refresh → ingest-log success."""
    import app.services.manifest_parsers  # noqa: F401 — register

    _seed_instrument_with_cusip(ebull_test_conn, iid=8750001, symbol="EL", cusip="518439104")
    _seed_pending_13dg(
        ebull_test_conn,
        accession="0001140361-25-040863",
        filer_cik="0002093607",
        source="sec_13d",
        form="SC 13D",
    )
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        lambda self, url: _FAKE_13D_XML,
    )

    stats = run_manifest_worker(ebull_test_conn, source="sec_13d", max_rows=10)
    ebull_test_conn.commit()

    assert stats.parsed == 1
    row = get_manifest_row(ebull_test_conn, "0001140361-25-040863")
    assert row is not None
    assert row.ingest_status == "parsed"
    assert row.raw_status == "stored"

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT submission_type, instrument_id FROM blockholder_filings "
            "WHERE accession_number = '0001140361-25-040863'"
        )
        bf = cur.fetchall()
    assert len(bf) == 1
    assert bf[0][0] == "SCHEDULE 13D"
    assert bf[0][1] == 8750001  # CUSIP resolved

    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT status FROM blockholder_filings_ingest_log WHERE accession_number = '0001140361-25-040863'")
        log = cur.fetchone()
    assert log is not None and log[0] == "success"


def test_13g_happy_path(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SC 13G: same parser, different schema dispatched by submissionType."""
    import app.services.manifest_parsers  # noqa: F401 — register

    _seed_instrument_with_cusip(ebull_test_conn, iid=8750002, symbol="AURA", cusip="G06973112")
    _seed_pending_13dg(
        ebull_test_conn,
        accession="0000950103-25-014355",
        filer_cik="0002083532",
        source="sec_13g",
        form="SC 13G",
    )
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        lambda self, url: _FAKE_13G_XML,
    )

    stats = run_manifest_worker(ebull_test_conn, source="sec_13g", max_rows=10)
    ebull_test_conn.commit()

    assert stats.parsed == 1
    row = get_manifest_row(ebull_test_conn, "0000950103-25-014355")
    assert row is not None and row.ingest_status == "parsed"

    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT submission_type FROM blockholder_filings WHERE accession_number = '0000950103-25-014355'")
        bf = cur.fetchone()
    assert bf is not None and bf[0] == "SCHEDULE 13G"


def test_cusip_unresolved_returns_parsed_with_partial_log(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No CUSIP→instrument mapping → filing rows STILL upserted (NULL
    instrument_id) but observation write-through skipped; ingest-log
    records ``partial``. Matches legacy semantics — audit trail
    preserved even when rollup join is gated by CUSIP backfill."""
    import app.services.manifest_parsers  # noqa: F401 — register

    # Note: NO _seed_instrument_with_cusip — CUSIP unresolved.
    _seed_pending_13dg(
        ebull_test_conn,
        accession="0009999999-25-000001",
        filer_cik="0002093607",
        source="sec_13d",
        form="SC 13D",
    )
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        lambda self, url: _FAKE_13D_XML,
    )

    stats = run_manifest_worker(ebull_test_conn, source="sec_13d", max_rows=10)
    ebull_test_conn.commit()

    assert stats.parsed == 1
    row = get_manifest_row(ebull_test_conn, "0009999999-25-000001")
    assert row is not None and row.ingest_status == "parsed"

    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT instrument_id FROM blockholder_filings WHERE accession_number = '0009999999-25-000001'")
        bf = cur.fetchone()
    assert bf is not None and bf[0] is None  # CUSIP unresolved

    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT status FROM blockholder_filings_ingest_log WHERE accession_number = '0009999999-25-000001'")
        log = cur.fetchone()
    assert log is not None and log[0] == "partial"


def test_empty_fetch_tombstones(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Empty/404 primary_doc.xml → manifest tombstoned + log status='failed'."""
    import app.services.manifest_parsers  # noqa: F401 — register

    _seed_pending_13dg(
        ebull_test_conn,
        accession="0008888888-25-000002",
        filer_cik="0002093607",
        source="sec_13d",
        form="SC 13D",
    )
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        lambda self, url: None,
    )

    stats = run_manifest_worker(ebull_test_conn, source="sec_13d", max_rows=10)
    ebull_test_conn.commit()

    assert stats.tombstoned == 1
    row = get_manifest_row(ebull_test_conn, "0008888888-25-000002")
    assert row is not None and row.ingest_status == "tombstoned"

    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT status FROM blockholder_filings_ingest_log WHERE accession_number = '0008888888-25-000002'")
        log = cur.fetchone()
    assert log is not None and log[0] == "failed"


def test_fetch_exception_marks_failed_with_backoff(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fetch raise → manifest row failed + next_retry_at = now+1h."""
    import app.services.manifest_parsers  # noqa: F401 — register

    _seed_pending_13dg(
        ebull_test_conn,
        accession="0007777777-25-000003",
        filer_cik="0002093607",
        source="sec_13d",
        form="SC 13D",
    )
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    def _boom(self, url):  # noqa: ARG001
        raise RuntimeError("network kaput")

    monkeypatch.setattr(sec_edgar.SecFilingsProvider, "fetch_document_text", _boom)

    before = datetime.now(tz=UTC)
    stats = run_manifest_worker(ebull_test_conn, source="sec_13d", max_rows=10)
    ebull_test_conn.commit()

    assert stats.failed == 1
    row = get_manifest_row(ebull_test_conn, "0007777777-25-000003")
    assert row is not None and row.ingest_status == "failed"
    assert row.error is not None and "fetch error" in row.error
    assert row.next_retry_at is not None
    delta = (row.next_retry_at - before).total_seconds()
    assert 3300 < delta < 3900


def test_parse_phase_exception_preserves_stored_raw_status(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Parse raise AFTER store_raw → failed + raw_status='stored'.
    Without the fix the manifest would diverge from filing_raw_documents."""
    from app.providers.implementations import sec_edgar
    from app.services.manifest_parsers import sec_13dg as parser_module

    _seed_instrument_with_cusip(ebull_test_conn, iid=8750003, symbol="EL2", cusip="518439104")
    _seed_pending_13dg(
        ebull_test_conn,
        accession="0006666666-25-000004",
        filer_cik="0002093607",
        source="sec_13d",
        form="SC 13D",
    )
    ebull_test_conn.commit()

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        lambda self, url: _FAKE_13D_XML,
    )

    def _raising_parse(xml):  # noqa: ARG001
        raise RuntimeError("synthetic parser crash")

    monkeypatch.setattr(parser_module, "parse_primary_doc", _raising_parse)

    stats = run_manifest_worker(ebull_test_conn, source="sec_13d", max_rows=10)
    ebull_test_conn.commit()

    assert stats.failed == 1
    row = get_manifest_row(ebull_test_conn, "0006666666-25-000004")
    assert row is not None
    assert row.ingest_status == "failed"
    assert row.raw_status == "stored"

    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT 1 FROM filing_raw_documents WHERE accession_number = '0006666666-25-000004'")
        assert cur.fetchone() is not None


def test_parser_registered_for_both_sources() -> None:
    """``register_all_parsers`` wires the SAME callable against
    sec_13d AND sec_13g."""
    from app.jobs.sec_manifest_worker import registered_parser_sources
    from app.services.manifest_parsers import register_all_parsers

    assert "sec_13d" in registered_parser_sources()
    assert "sec_13g" in registered_parser_sources()

    clear_registered_parsers()
    assert "sec_13d" not in registered_parser_sources()
    assert "sec_13g" not in registered_parser_sources()

    register_all_parsers()
    assert "sec_13d" in registered_parser_sources()
    assert "sec_13g" in registered_parser_sources()
