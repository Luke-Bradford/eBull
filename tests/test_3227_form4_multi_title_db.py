"""#3227 multi-title — condition 5c's earlier balances filter by the unit's title test, and both Form 4
observation writers sum a holding group whose titles name one class. Spec:
docs/specs/ownership/2026-10-18-3227-form4-multi-title.md."""

from __future__ import annotations

from datetime import UTC, datetime, time, timedelta
from decimal import Decimal
from typing import Any

import psycopg

from app.services.insider_transactions import (
    form4_earlier_balances,
    form4_title_match,
    parse_form_4_xml,
    upsert_filing,
)
from app.services.ownership_observations_sync import sync_insiders
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

_IID = 3_227_601
_CIK = "0003227601"
_EARLIER = "0003227601-26-000001"
_LATER = "0003227601-26-000002"
_SIG = (datetime.now(UTC) - timedelta(days=30)).date()
_FILED = datetime.combine(_SIG, time(21, 30), tzinfo=UTC)


def _holding(shares: str, di: str, title: str, nature: str | None = None) -> str:
    nature_el = f"<natureOfOwnership><value>{nature}</value></natureOfOwnership>" if nature else ""
    return f"""
    <nonDerivativeHolding>
      <securityTitle><value>{title}</value></securityTitle>
      <postTransactionAmounts>
        <sharesOwnedFollowingTransaction><value>{shares}</value></sharesOwnedFollowingTransaction>
      </postTransactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership><value>{di}</value></directOrIndirectOwnership>{nature_el}
      </ownershipNature>
    </nonDerivativeHolding>"""


def _txn(title: str, post: str, di: str = "I", nature: str = "By Trust") -> str:
    return f"""
    <nonDerivativeTransaction>
      <securityTitle><value>{title}</value></securityTitle>
      <transactionDate><value>{_SIG - timedelta(days=2)}</value></transactionDate>
      <transactionCoding><transactionFormType>4</transactionFormType><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>10</value></transactionShares>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <postTransactionAmounts>
        <sharesOwnedFollowingTransaction><value>{post}</value></sharesOwnedFollowingTransaction>
      </postTransactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership><value>{di}</value></directOrIndirectOwnership>
        <natureOfOwnership><value>{nature}</value></natureOfOwnership>
      </ownershipNature>
    </nonDerivativeTransaction>"""


def _xml(table: str) -> str:
    return f"""<?xml version="1.0"?>
<ownershipDocument>
  <documentType>4</documentType>
  <periodOfReport>{_SIG - timedelta(days=2)}</periodOfReport>
  <issuer><issuerCik>0009999999</issuerCik><issuerName>T</issuerName></issuer>
  <reportingOwner>
    <reportingOwnerId><rptOwnerCik>{_CIK}</rptOwnerCik><rptOwnerName>Holder, B</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship><isDirector>1</isDirector></reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>{table}
  </nonDerivativeTable>
  <ownerSignature><signatureName>B</signatureName><signatureDate>{_SIG}</signatureDate></ownerSignature>
</ownershipDocument>
"""


def _seed_and_file(conn: psycopg.Connection[Any], filings: list[tuple[str, datetime, str]]) -> None:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, 'T3227M', 'T', TRUE)",
        (_IID,),
    )
    for accession, filed, _ in filings:
        conn.execute(
            "INSERT INTO sec_filing_manifest (accession_number, cik, form, source, subject_type, subject_id,"
            " instrument_id, filed_at, ingest_status)"
            " VALUES (%s, %s, '4', 'sec_form4', 'issuer', %s, %s, %s, 'parsed')",
            (accession, _CIK, str(_IID), _IID, filed),
        )
    conn.commit()
    for accession, filed, xml in filings:
        parsed = parse_form_4_xml(xml)
        assert parsed is not None
        upsert_filing(
            conn,
            instrument_id=_IID,
            accession_number=accession,
            primary_document_url="https://example.test/m.xml",
            parsed=parsed,
            filed_at=filed,
        )
    conn.commit()


def test_earlier_balances_filter_by_the_title_test(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    earlier = _xml(
        _txn("Common Stock", "100")
        + _txn("Common Stock, par value $0.01", "200", nature="By LLC")
        + _txn("Class B Common Stock", "300", nature="By Fund")
    )
    later = _xml(_txn("Common Stock", "110"))
    _seed_and_file(conn, [(_EARLIER, _FILED - timedelta(days=10), earlier), (_LATER, _FILED, later)])

    def read(title: object) -> set[Decimal]:
        rows = form4_earlier_balances(
            conn,
            title,  # type: ignore[arg-type]  # a str or a matcher
            instrument_id=_IID,
            accession_number=_LATER,
            filer_cik=_CIK,
        )
        assert rows is not None
        return {r[2] for r in rows}

    # One exact title: the query before this change.
    assert read("Common Stock") == {Decimal(100)}
    # The class test of a multi-title unit: both common spellings, never Class B.
    match = form4_title_match(["Common Stock", "Common Stock 401(k)"])
    assert match is not None
    assert read(match) == {Decimal(100), Decimal(200)}


def test_both_writers_sum_a_one_class_holding_group(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    xml = _xml(_holding("100", "D", "Common Stock") + _holding("50", "D", "Common Stock 401(k)"))
    _seed_and_file(conn, [(_LATER, _FILED, xml)])

    def observed() -> set[tuple[Any, ...]]:
        rows = conn.execute(
            "SELECT holder_cik, ownership_nature, shares FROM ownership_insiders_observations"
            " WHERE instrument_id = %s AND source = 'form4' AND known_to IS NULL",
            (_IID,),
        ).fetchall()
        return {(c, n, Decimal(s)) for c, n, s in rows}

    live = observed()
    assert live == {(_CIK, "direct", Decimal(150))}
    conn.execute("DELETE FROM ownership_insiders_observations WHERE instrument_id = %s", (_IID,))
    conn.commit()
    sync_insiders(conn)
    conn.commit()
    assert observed() == live
