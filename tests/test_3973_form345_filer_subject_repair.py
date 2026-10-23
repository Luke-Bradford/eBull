"""#3973 repair — Form 3/4/5 manifest rows re-subjected to the issuer named in the document."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import uuid4

import psycopg
import pytest

from app.services.ownership_observations import record_insider_observation
from app.services.raw_filings import store_raw
from app.services.sec_manifest import get_manifest_row
from scripts.repair_3973_form345_filer_subjects import repair
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration

_ISSUER_IID = 397_301
_ISSUER_CIK = "0000397301"
_OWNER_IID = 397_302  # a universe issuer that is also a reporting owner
_OWNER_ISSUER_CIK = "0000397302"
_FILER_CIK = "0001397301"
_OUTSIDE_CIK = "0000397399"  # an issuer outside the universe

_STORED = "0001397301-24-000001"
_OUTSIDE = "0001397301-24-000002"
_FETCHED = "0001397301-24-000003"
_PARSED = "0001397301-24-000004"
_STUCK_ISSUER = "0001397301-24-000005"
_UNLISTED = "0001397301-24-000006"


def _form3(issuer_cik: str) -> str:
    return f"""<?xml version="1.0"?>
<ownershipDocument>
  <schemaVersion>X0202</schemaVersion>
  <documentType>3</documentType>
  <periodOfReport>2024-05-01</periodOfReport>
  <issuer><issuerCik>{issuer_cik}</issuerCik><issuerName>Issuer</issuerName><issuerTradingSymbol>TOMB</issuerTradingSymbol></issuer>
  <reportingOwner>
    <reportingOwnerId><rptOwnerCik>{_FILER_CIK}</rptOwnerCik><rptOwnerName>Fund LP</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship><isTenPercentOwner>1</isTenPercentOwner></reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeHolding>
      <securityTitle><value>Common Stock</value></securityTitle>
      <postTransactionAmounts><sharesOwnedFollowingTransaction><value>1000</value></sharesOwnedFollowingTransaction></postTransactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeHolding>
  </nonDerivativeTable>
</ownershipDocument>"""


def _instrument(conn: psycopg.Connection[tuple], iid: int, symbol: str, cik: str) -> None:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)"
        " VALUES (%s, %s, %s, '4', 'USD', TRUE)",
        (iid, symbol, f"{symbol} Inc"),
    )
    conn.execute(
        "INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, is_primary)"
        " VALUES (%s, 'sec', 'cik', %s, TRUE)",
        (iid, cik),
    )


def _manifest(
    conn: psycopg.Connection[tuple],
    acc: str,
    *,
    subject_type: str = "institutional_filer",
    subject_id: str = _FILER_CIK,
    instrument_id: int | None = None,
    status: str = "tombstoned",
    error: str | None = "missing instrument_id",
) -> None:
    conn.execute(
        """
        INSERT INTO sec_filing_manifest (accession_number, cik, form, source, subject_type, subject_id,
                                         instrument_id, filed_at, primary_document_url, ingest_status, error)
        VALUES (%s, %s, '3', 'sec_form3', %s, %s, %s, '2024-05-01T00:00:00Z',
                %s, %s, %s)
        """,
        (
            acc,
            _FILER_CIK,
            subject_type,
            subject_id,
            instrument_id,
            f"https://www.sec.gov/Archives/edgar/data/{int(_FILER_CIK)}/{acc.replace('-', '')}/doc3.xml",
            status,
            error,
        ),
    )


def _listed(conn: psycopg.Connection[tuple], acc: str, iid: int) -> None:
    conn.execute(
        "INSERT INTO filing_events (instrument_id, filing_date, provider, provider_filing_id)"
        " VALUES (%s, '2024-05-01', 'sec', %s)",
        (iid, acc),
    )


def _store(conn: psycopg.Connection[tuple], acc: str, issuer_cik: str) -> None:
    store_raw(conn, accession_number=acc, document_kind="form3_xml", payload=_form3(issuer_cik))


def test_repair_follows_the_documents_issuer(ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
    conn = ebull_test_conn
    _instrument(conn, _ISSUER_IID, "TOMB", _ISSUER_CIK)
    _instrument(conn, _OWNER_IID, "OWNR", _OWNER_ISSUER_CIK)
    _manifest(conn, _STORED)
    _store(conn, _STORED, _ISSUER_CIK)
    _manifest(conn, _OUTSIDE)
    _store(conn, _OUTSIDE, _OUTSIDE_CIK)
    _manifest(conn, _FETCHED)
    _manifest(conn, _PARSED, subject_type="blockholder_filer", status="parsed", error=None)
    _store(conn, _PARSED, _ISSUER_CIK)
    _manifest(conn, _STUCK_ISSUER, subject_type="issuer", subject_id=str(_OWNER_IID), instrument_id=_OWNER_IID)
    _store(conn, _STUCK_ISSUER, _ISSUER_CIK)
    # The outside-universe filing is listed by the owner-issuer's own submissions; the unlisted one by none.
    for acc, iid in ((_STORED, _ISSUER_IID), (_OUTSIDE, _OWNER_IID), (_FETCHED, _ISSUER_IID), (_PARSED, _ISSUER_IID)):
        _listed(conn, acc, iid)
    _manifest(conn, _UNLISTED)
    conn.execute(
        "INSERT INTO data_freshness_index (subject_type, subject_id, cik, source) VALUES"
        " ('institutional_filer', %s, %s, 'sec_form4'), ('institutional_filer', %s, %s, 'sec_13f_hr')",
        (_FILER_CIK, _FILER_CIK, _FILER_CIK, _FILER_CIK),
    )

    fetched_urls: list[str] = []

    def fetch(url: str) -> str | None:
        fetched_urls.append(url)
        return _form3(_ISSUER_CIK)

    stats = repair(conn, fetch=fetch)

    assert dict(stats) == {
        "candidates": 5,
        "fetched": 1,
        "skipped_issuer_outside_universe": 1,
        "resubjected": 4,
        "reset_to_pending": 3,
        "freshness_rows_deleted": 1,
    }
    assert fetched_urls == [f"https://www.sec.gov/Archives/edgar/data/{int(_FILER_CIK)}/000139730124000003/doc3.xml"]

    for acc in (_STORED, _FETCHED, _STUCK_ISSUER):
        row = get_manifest_row(conn, acc)
        assert row is not None
        assert (row.subject_type, row.instrument_id, row.cik) == ("issuer", _ISSUER_IID, _ISSUER_CIK), acc
        assert (row.ingest_status, row.error) == ("pending", None), acc

    parsed = get_manifest_row(conn, _PARSED)
    assert parsed is not None
    assert (parsed.subject_type, parsed.instrument_id, parsed.ingest_status) == ("issuer", _ISSUER_IID, "parsed")

    # The document's issuer is outside the universe: the writer would fail open onto the
    # discovery instrument (#828), so the row stays tombstoned under its filer subject.
    for acc in (_OUTSIDE, _UNLISTED):
        untouched = get_manifest_row(conn, acc)
        assert untouched is not None
        assert (untouched.subject_type, untouched.ingest_status) == ("institutional_filer", "tombstoned"), acc

    stored = conn.execute(
        "SELECT count(*) FROM filing_raw_documents WHERE accession_number = %s AND document_kind = 'form3_xml'",
        (_FETCHED,),
    ).fetchone()
    assert stored == (1,)

    remaining = conn.execute(
        "SELECT source FROM data_freshness_index WHERE subject_id = %s ORDER BY source", (_FILER_CIK,)
    ).fetchall()
    assert remaining == [("sec_13f_hr",)]


def test_dry_run_reads_stored_bodies_only(ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
    conn = ebull_test_conn
    _instrument(conn, _ISSUER_IID, "TOMB", _ISSUER_CIK)
    _manifest(conn, _FETCHED)
    _listed(conn, _FETCHED, _ISSUER_IID)

    stats = repair(conn, fetch=None)

    assert stats["skipped_no_body"] == 1
    row = get_manifest_row(conn, _FETCHED)
    assert row is not None
    assert (row.subject_type, row.ingest_status) == ("institutional_filer", "tombstoned")


def test_form3_superseded_by_later_owner_evidence_is_left(ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
    # The holder's later observation is the current evidence; parsing the older
    # Form 3 would only restore its initial balance on a separate D/I key (#3227).
    conn = ebull_test_conn
    _instrument(conn, _ISSUER_IID, "TOMB", _ISSUER_CIK)
    _manifest(conn, _STORED)
    _store(conn, _STORED, _ISSUER_CIK)
    _listed(conn, _STORED, _ISSUER_IID)
    record_insider_observation(
        conn,
        instrument_id=_ISSUER_IID,
        holder_cik=_FILER_CIK,
        holder_name="Fund LP",
        ownership_nature="beneficial",
        source="form4",
        source_document_id="0001397301-25-000009:NDT:1",
        source_accession="0001397301-25-000009",
        source_field=None,
        source_url=None,
        filed_at=datetime(2025, 3, 1, tzinfo=UTC),
        period_start=None,
        period_end=date(2025, 2, 27),
        ingest_run_id=uuid4(),
        shares=Decimal("0"),
    )

    stats = repair(conn, fetch=None)

    assert stats["skipped_form3_superseded"] == 1
    row = get_manifest_row(conn, _STORED)
    assert row is not None
    assert (row.subject_type, row.ingest_status) == ("institutional_filer", "tombstoned")


def test_joint_form3_kept_when_only_a_co_owner_has_later_evidence(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    _instrument(conn, _ISSUER_IID, "TOMB", _ISSUER_CIK)
    _manifest(conn, _STORED)
    joint = _form3(_ISSUER_CIK).replace(
        "</reportingOwner>",
        "</reportingOwner>\n  <reportingOwner><reportingOwnerId><rptOwnerCik>0001397309</rptOwnerCik>"
        "<rptOwnerName>Fund GP</rptOwnerName></reportingOwnerId>"
        "<reportingOwnerRelationship><isTenPercentOwner>1</isTenPercentOwner></reportingOwnerRelationship>"
        "</reportingOwner>",
        1,
    )
    store_raw(conn, accession_number=_STORED, document_kind="form3_xml", payload=joint)
    _listed(conn, _STORED, _ISSUER_IID)
    record_insider_observation(
        conn,
        instrument_id=_ISSUER_IID,
        holder_cik="0001397309",
        holder_name="Fund GP",
        ownership_nature="beneficial",
        source="form4",
        source_document_id="0001397301-25-000009:NDT:1",
        source_accession="0001397301-25-000009",
        source_field=None,
        source_url=None,
        filed_at=datetime(2025, 3, 1, tzinfo=UTC),
        period_start=None,
        period_end=date(2025, 2, 27),
        ingest_run_id=uuid4(),
        shares=Decimal("0"),
    )

    stats = repair(conn, fetch=None)

    # The holding is attributed to the first owner, who has no later evidence.
    assert stats["skipped_form3_superseded"] == 0
    assert stats["reset_to_pending"] == 1
