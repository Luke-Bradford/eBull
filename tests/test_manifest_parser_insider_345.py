"""Tests for the Form 3 / Form 4 manifest-worker parser adapter (#873).

One callable registered per source — Form 3 and Form 4 share the
EDGAR ownership XML namespace but persist into different tables
(``insider_initial_holdings`` vs ``insider_transactions``) and
carry separate parser_version watermarks. Form 5 is intentionally
unregistered (no legacy support yet) and continues to skip.

Tests cover:

- Happy path Form 4: XML fetch → store_raw → parse → upsert
  ``insider_filings`` + ``insider_transactions`` rows → observation
  write-through.
- Happy path Form 3: XML fetch → store_raw → parse → upsert
  ``insider_initial_holdings`` rows.
- Tombstone on empty fetch: writes a tombstone row in
  ``insider_filings`` so legacy discovery skips the accession.
- Tombstone on parse-None (malformed XML): same tombstone path,
  raw_status='stored' so manifest matches filing_raw_documents.
- Parse-phase exception preserves raw_status='stored'.
- Fetch raises: returns failed + 1h backoff.
- Form 5 unregistered: sec_form5 source is debug-skipped by the
  worker (no parser).
- URL canonicalisation: SEC XSL-rendered URL → raw XML URL via
  ``_canonical_form_4_url``.
- Registration: register_all_parsers wires sec_form3 + sec_form4
  but NOT sec_form5.
"""

from __future__ import annotations

from datetime import UTC, datetime
from textwrap import dedent

import psycopg
import pytest

from app.jobs.sec_manifest_worker import (
    clear_registered_parsers,
    run_manifest_worker,
)
from app.services.sec_manifest import get_manifest_row, record_manifest_entry
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

_FAKE_FORM_4_XML = dedent("""<?xml version="1.0"?>
<ownershipDocument>
  <documentType>4</documentType>
  <periodOfReport>2026-04-15</periodOfReport>
  <issuer>
    <issuerCik>0000320193</issuerCik>
    <issuerName>Apple Inc.</issuerName>
    <issuerTradingSymbol>AAPL</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>0001000001</rptOwnerCik>
      <rptOwnerName>Jane Smith</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isDirector>1</isDirector>
      <isOfficer>1</isOfficer>
      <officerTitle>Chief Financial Officer</officerTitle>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2026-04-15</value></transactionDate>
      <transactionCoding>
        <transactionCode>P</transactionCode>
      </transactionCoding>
      <transactionAmounts>
        <transactionShares><value>250</value></transactionShares>
        <transactionPricePerShare><value>185.42</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership><value>D</value></directOrIndirectOwnership>
      </ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
  <ownerSignature>
    <signatureName>Jane Smith</signatureName>
    <signatureDate>2026-04-16</signatureDate>
  </ownerSignature>
</ownershipDocument>
""")


_FAKE_FORM_3_XML = dedent("""<?xml version="1.0"?>
<ownershipDocument>
  <schemaVersion>X0202</schemaVersion>
  <documentType>3</documentType>
  <periodOfReport>2026-01-15</periodOfReport>
  <issuer>
    <issuerCik>0000320193</issuerCik>
    <issuerName>Apple Inc.</issuerName>
    <issuerTradingSymbol>AAPL</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>0001000001</rptOwnerCik>
      <rptOwnerName>Smith, Jane</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isOfficer>1</isOfficer>
      <officerTitle>Chief Financial Officer</officerTitle>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeHolding>
      <securityTitle><value>Common Stock</value></securityTitle>
      <postTransactionAmounts>
        <sharesOwnedFollowingTransaction><value>50000</value></sharesOwnedFollowingTransaction>
      </postTransactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership><value>D</value></directOrIndirectOwnership>
      </ownershipNature>
    </nonDerivativeHolding>
  </nonDerivativeTable>
  <ownerSignature>
    <signatureName>Jane Smith</signatureName>
    <signatureDate>2026-01-16</signatureDate>
  </ownerSignature>
</ownershipDocument>
""")


def _seed_instrument(conn: psycopg.Connection[tuple], iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} co"),
    )


def _seed_pending(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    instrument_id: int,
    source: str,
    form: str,
    url: str = "https://www.sec.gov/Archives/edgar/data/320193/000032019326000010/primary_doc.xml",
) -> None:
    record_manifest_entry(
        conn,
        accession,
        cik="0000320193",
        form=form,
        source=source,  # type: ignore[arg-type]
        subject_type="issuer",
        subject_id=str(instrument_id),
        instrument_id=instrument_id,
        filed_at=datetime(2026, 5, 11, tzinfo=UTC),
        primary_document_url=url,
    )


@pytest.fixture(autouse=True)
def _reset_registry_then_reload():
    from app.services.manifest_parsers import register_all_parsers

    clear_registered_parsers()
    register_all_parsers()
    yield
    clear_registered_parsers()
    register_all_parsers()


def test_form4_happy_path(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Manifest worker drains a Form 4 pending row: fetch → store_raw
    → parse → upsert insider_filings + insider_transactions."""
    import app.services.manifest_parsers  # noqa: F401 — register

    iid = 8760001
    _seed_instrument(ebull_test_conn, iid=iid, symbol="AAPL")
    _seed_pending(
        ebull_test_conn,
        accession="0000320193-26-000010",
        instrument_id=iid,
        source="sec_form4",
        form="4",
    )
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        lambda self, url: _FAKE_FORM_4_XML,
    )

    stats = run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
    ebull_test_conn.commit()

    assert stats.parsed == 1
    row = get_manifest_row(ebull_test_conn, "0000320193-26-000010")
    assert row is not None and row.ingest_status == "parsed"
    assert row.raw_status == "stored"
    assert row.parser_version == "form4-v1"

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT document_type, is_tombstone FROM insider_filings WHERE accession_number = '0000320193-26-000010'"
        )
        f = cur.fetchone()
    assert f is not None
    assert f[0] == "4"
    assert f[1] is False

    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM insider_transactions WHERE accession_number = '0000320193-26-000010'")
        c = cur.fetchone()
    assert c is not None and c[0] >= 1

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT byte_count FROM filing_raw_documents "
            "WHERE accession_number = '0000320193-26-000010' AND document_kind = 'form4_xml'"
        )
        r = cur.fetchone()
    assert r is not None and r[0] > 0


def test_form3_happy_path(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Form 3 routes through parse_form_3_xml + upsert_form_3_filing →
    insider_initial_holdings row."""
    import app.services.manifest_parsers  # noqa: F401 — register

    iid = 8760002
    _seed_instrument(ebull_test_conn, iid=iid, symbol="AAPL3")
    _seed_pending(
        ebull_test_conn,
        accession="0000320193-26-000020",
        instrument_id=iid,
        source="sec_form3",
        form="3",
    )
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        lambda self, url: _FAKE_FORM_3_XML,
    )

    stats = run_manifest_worker(ebull_test_conn, source="sec_form3", max_rows=10)
    ebull_test_conn.commit()

    assert stats.parsed == 1
    row = get_manifest_row(ebull_test_conn, "0000320193-26-000020")
    assert row is not None and row.ingest_status == "parsed"
    assert row.raw_status == "stored"
    assert row.parser_version == "form3-v1"

    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT document_type FROM insider_filings WHERE accession_number = '0000320193-26-000020'")
        f = cur.fetchone()
    assert f is not None and f[0] == "3"

    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM insider_initial_holdings WHERE accession_number = '0000320193-26-000020'")
        c = cur.fetchone()
    assert c is not None and c[0] >= 1

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM filing_raw_documents "
            "WHERE accession_number = '0000320193-26-000020' AND document_kind = 'form3_xml'"
        )
        assert cur.fetchone() is not None


def test_form4_empty_fetch_tombstones(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Empty/404 → manifest tombstoned + insider_filings tombstone row."""
    import app.services.manifest_parsers  # noqa: F401 — register

    iid = 8760003
    _seed_instrument(ebull_test_conn, iid=iid, symbol="DEAD4")
    _seed_pending(
        ebull_test_conn,
        accession="0000999999-26-000010",
        instrument_id=iid,
        source="sec_form4",
        form="4",
    )
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        lambda self, url: None,
    )

    stats = run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
    ebull_test_conn.commit()

    assert stats.tombstoned == 1
    row = get_manifest_row(ebull_test_conn, "0000999999-26-000010")
    assert row is not None and row.ingest_status == "tombstoned"
    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT is_tombstone FROM insider_filings WHERE accession_number = '0000999999-26-000010'")
        f = cur.fetchone()
    assert f is not None and f[0] is True


def test_form4_parse_none_tombstones_with_stored_raw(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Malformed XML → parser returns None → manifest tombstoned with
    raw_status='stored' since store_raw committed BEFORE the parse."""
    import app.services.manifest_parsers  # noqa: F401 — register

    iid = 8760004
    _seed_instrument(ebull_test_conn, iid=iid, symbol="MAL4")
    _seed_pending(
        ebull_test_conn,
        accession="0000888888-26-000010",
        instrument_id=iid,
        source="sec_form4",
        form="4",
    )
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    # Body is non-empty but not ownership XML — parser returns None.
    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        lambda self, url: "<not-ownership-xml/>",
    )

    stats = run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
    ebull_test_conn.commit()

    assert stats.tombstoned == 1
    row = get_manifest_row(ebull_test_conn, "0000888888-26-000010")
    assert row is not None
    assert row.ingest_status == "tombstoned"
    assert row.raw_status == "stored"


def test_form4_fetch_exception_marks_failed_with_backoff(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fetch raise → failed + 1h backoff so worker doesn't hammer SEC."""
    import app.services.manifest_parsers  # noqa: F401 — register

    iid = 8760005
    _seed_instrument(ebull_test_conn, iid=iid, symbol="BOOM4")
    _seed_pending(
        ebull_test_conn,
        accession="0000777777-26-000010",
        instrument_id=iid,
        source="sec_form4",
        form="4",
    )
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    def _boom(self, url):  # noqa: ARG001
        raise RuntimeError("network kaput")

    monkeypatch.setattr(sec_edgar.SecFilingsProvider, "fetch_document_text", _boom)

    before = datetime.now(tz=UTC)
    stats = run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
    ebull_test_conn.commit()

    assert stats.failed == 1
    row = get_manifest_row(ebull_test_conn, "0000777777-26-000010")
    assert row is not None and row.ingest_status == "failed"
    assert row.error is not None and "fetch error" in row.error
    assert row.next_retry_at is not None
    delta = (row.next_retry_at - before).total_seconds()
    assert 3300 < delta < 3900


def test_form4_parse_phase_exception_preserves_stored_raw_status(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Parse raise AFTER store_raw → failed + raw_status='stored'."""
    from app.providers.implementations import sec_edgar
    from app.services.manifest_parsers import insider_345 as parser_module

    iid = 8760006
    _seed_instrument(ebull_test_conn, iid=iid, symbol="CRASH4")
    _seed_pending(
        ebull_test_conn,
        accession="0000666666-26-000010",
        instrument_id=iid,
        source="sec_form4",
        form="4",
    )
    ebull_test_conn.commit()

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        lambda self, url: _FAKE_FORM_4_XML,
    )

    def _raising_parse(xml):  # noqa: ARG001
        raise RuntimeError("synthetic Form 4 parser crash")

    monkeypatch.setattr(parser_module, "parse_form_4_xml", _raising_parse)

    stats = run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
    ebull_test_conn.commit()

    assert stats.failed == 1
    row = get_manifest_row(ebull_test_conn, "0000666666-26-000010")
    assert row is not None
    assert row.ingest_status == "failed"
    assert row.raw_status == "stored"

    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT 1 FROM filing_raw_documents WHERE accession_number = '0000666666-26-000010'")
        assert cur.fetchone() is not None


def test_form4_upsert_failure_tombstones(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR #1130 review WARNING: Form 4 upsert failure must tombstone
    (not failed+1h retry) so deterministic constraint violations on
    insider_filings/insider_transactions don't loop the worker
    refetching the same dead XML hourly. Symmetry with Form 3 policy."""
    from app.providers.implementations import sec_edgar
    from app.services.manifest_parsers import insider_345 as parser_module

    iid = 8760010
    _seed_instrument(ebull_test_conn, iid=iid, symbol="UFAIL4")
    _seed_pending(
        ebull_test_conn,
        accession="0000222222-26-000010",
        instrument_id=iid,
        source="sec_form4",
        form="4",
    )
    ebull_test_conn.commit()

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        lambda self, url: _FAKE_FORM_4_XML,
    )

    def _raising_upsert(*args, **kwargs):  # noqa: ARG001
        raise RuntimeError("synthetic Form 4 upsert constraint violation")

    monkeypatch.setattr(parser_module, "upsert_filing", _raising_upsert)

    stats = run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
    ebull_test_conn.commit()

    assert stats.tombstoned == 1
    assert stats.failed == 0
    row = get_manifest_row(ebull_test_conn, "0000222222-26-000010")
    assert row is not None
    assert row.ingest_status == "tombstoned"
    assert row.raw_status == "stored"
    # PR #1131: error string carries exception class name so the
    # backfill at ``tombstone_stale_failed_upserts`` can discriminate
    # transient psycopg errors from deterministic ones.
    assert row.error is not None
    assert "upsert error" in row.error
    assert "RuntimeError" in row.error

    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT is_tombstone FROM insider_filings WHERE accession_number = '0000222222-26-000010'")
        f = cur.fetchone()
    assert f is not None and f[0] is True


def test_form3_upsert_failure_tombstones_per_legacy_parity(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex pre-push: Form 3 upsert failure must tombstone (matching
    legacy ``_process_form_3_candidates`` at insider_form3_ingest.py:685)
    so a deterministic constraint violation doesn't loop the worker
    refetching the same dead XML hourly. Manifest row transitions to
    ``tombstoned`` (not ``failed``) and writes the tombstone row in
    ``insider_filings``."""
    from app.providers.implementations import sec_edgar
    from app.services.manifest_parsers import insider_345 as parser_module

    iid = 8760009
    _seed_instrument(ebull_test_conn, iid=iid, symbol="UFAIL3")
    _seed_pending(
        ebull_test_conn,
        accession="0000333333-26-000010",
        instrument_id=iid,
        source="sec_form3",
        form="3",
    )
    ebull_test_conn.commit()

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        lambda self, url: _FAKE_FORM_3_XML,
    )

    def _raising_upsert(*args, **kwargs):  # noqa: ARG001
        raise RuntimeError("synthetic upsert constraint violation")

    monkeypatch.setattr(parser_module, "upsert_form_3_filing", _raising_upsert)

    stats = run_manifest_worker(ebull_test_conn, source="sec_form3", max_rows=10)
    ebull_test_conn.commit()

    assert stats.tombstoned == 1
    assert stats.failed == 0
    row = get_manifest_row(ebull_test_conn, "0000333333-26-000010")
    assert row is not None
    assert row.ingest_status == "tombstoned"
    assert row.raw_status == "stored"
    # PR #1131 error-format pinning (class name for backfill discrim).
    assert row.error is not None
    assert "upsert error" in row.error
    assert "RuntimeError" in row.error

    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT is_tombstone FROM insider_filings WHERE accession_number = '0000333333-26-000010'")
        f = cur.fetchone()
    assert f is not None and f[0] is True


def test_form4_transient_upsert_exception_retries(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR #1131: an ``OperationalError`` on the Form 4 upsert keeps
    the manifest in ``failed`` with a 1h backoff — the parsed XML is
    not the problem, the DB-side state is. Without this discrimination
    a deadlock would tombstone a perfectly good accession."""
    import psycopg.errors

    from app.providers.implementations import sec_edgar
    from app.services.manifest_parsers import insider_345 as parser_module

    iid = 8760070
    _seed_instrument(ebull_test_conn, iid=iid, symbol="TFAIL4")
    _seed_pending(
        ebull_test_conn,
        accession="0000222222-26-000070",
        instrument_id=iid,
        source="sec_form4",
        form="4",
    )
    ebull_test_conn.commit()

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        lambda self, url: _FAKE_FORM_4_XML,
    )

    def _raising_upsert(*args, **kwargs):  # noqa: ARG001
        raise psycopg.errors.SerializationFailure("synthetic serialisation failure")

    monkeypatch.setattr(parser_module, "upsert_filing", _raising_upsert)

    stats = run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
    ebull_test_conn.commit()

    assert stats.failed == 1
    assert stats.tombstoned == 0
    row = get_manifest_row(ebull_test_conn, "0000222222-26-000070")
    assert row is not None
    assert row.ingest_status == "failed"
    assert row.raw_status == "stored"
    assert row.error is not None
    assert "SerializationFailure" in row.error

    # No insider_filings tombstone — transient keeps the accession
    # alive for retry. Tombstoning here would burn the row's retry
    # eligibility on a recoverable DB-side hiccup.
    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT 1 FROM insider_filings WHERE accession_number = '0000222222-26-000070'")
        assert cur.fetchone() is None


def test_form3_transient_upsert_exception_retries(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Symmetry check: Form 3 must also discriminate transient on
    upsert."""
    import psycopg.errors

    from app.providers.implementations import sec_edgar
    from app.services.manifest_parsers import insider_345 as parser_module

    iid = 8760071
    _seed_instrument(ebull_test_conn, iid=iid, symbol="TFAIL3")
    _seed_pending(
        ebull_test_conn,
        accession="0000333333-26-000071",
        instrument_id=iid,
        source="sec_form3",
        form="3",
    )
    ebull_test_conn.commit()

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        lambda self, url: _FAKE_FORM_3_XML,
    )

    def _raising_upsert(*args, **kwargs):  # noqa: ARG001
        raise psycopg.errors.DeadlockDetected("synthetic deadlock")

    monkeypatch.setattr(parser_module, "upsert_form_3_filing", _raising_upsert)

    stats = run_manifest_worker(ebull_test_conn, source="sec_form3", max_rows=10)
    ebull_test_conn.commit()

    assert stats.failed == 1
    assert stats.tombstoned == 0
    row = get_manifest_row(ebull_test_conn, "0000333333-26-000071")
    assert row is not None
    assert row.ingest_status == "failed"
    assert row.error is not None
    assert "DeadlockDetected" in row.error


def test_xsl_url_canonicalised_before_fetch(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Atom discovery may carry the XSL-rendered URL (with
    ``/xslF345X05/`` segment). The parser canonicalises it before
    fetch so the worker reads raw XML, not XSL-transformed HTML."""
    import app.services.manifest_parsers  # noqa: F401 — register

    iid = 8760007
    _seed_instrument(ebull_test_conn, iid=iid, symbol="XSL4")
    _seed_pending(
        ebull_test_conn,
        accession="0000555555-26-000010",
        instrument_id=iid,
        source="sec_form4",
        form="4",
        url="https://www.sec.gov/Archives/edgar/data/320193/000032019326000010/xslF345X05/primary_doc.xml",
    )
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    fetched_urls: list[str] = []

    def _capture(self, url):  # noqa: ARG001
        fetched_urls.append(url)
        return _FAKE_FORM_4_XML

    monkeypatch.setattr(sec_edgar.SecFilingsProvider, "fetch_document_text", _capture)

    stats = run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
    ebull_test_conn.commit()

    assert stats.parsed == 1
    # Confirm canonicalisation actually stripped the XSL segment.
    assert len(fetched_urls) == 1
    assert "/xslF345X05/" not in fetched_urls[0]


def test_form5_unregistered_skips(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Form 5 (annual statement) has no registered parser. The worker
    debug-skips the row and the manifest stays pending so a future
    Form 5 onboarding sees the same backlog."""
    import app.services.manifest_parsers  # noqa: F401 — register

    iid = 8760008
    _seed_instrument(ebull_test_conn, iid=iid, symbol="ANN5")
    _seed_pending(
        ebull_test_conn,
        accession="0000444444-26-000010",
        instrument_id=iid,
        source="sec_form5",
        form="5",
    )
    ebull_test_conn.commit()

    stats = run_manifest_worker(ebull_test_conn, source="sec_form5", max_rows=10)
    ebull_test_conn.commit()

    assert stats.skipped_no_parser == 1
    assert stats.parsed == 0
    assert stats.tombstoned == 0
    row = get_manifest_row(ebull_test_conn, "0000444444-26-000010")
    assert row is not None and row.ingest_status == "pending"


def test_parser_registered_form3_and_form4_but_not_form5() -> None:
    """sec_form3 + sec_form4 wired; sec_form5 deliberately NOT."""
    from app.jobs.sec_manifest_worker import registered_parser_sources
    from app.services.manifest_parsers import register_all_parsers

    sources = registered_parser_sources()
    assert "sec_form3" in sources
    assert "sec_form4" in sources
    assert "sec_form5" not in sources

    clear_registered_parsers()
    assert "sec_form3" not in registered_parser_sources()
    register_all_parsers()
    assert "sec_form3" in registered_parser_sources()
    assert "sec_form4" in registered_parser_sources()
    assert "sec_form5" not in registered_parser_sources()
