"""#3227 — both Form 4 observation writers turn a stored holdings-only filing and a mixed filing into
the SAME holding observations: the live writer through ``upsert_filing``, and the weekly
``sync_insiders`` from ``insider_form4_holdings``. A tombstoned holdings-only filing turns live."""

from __future__ import annotations

from datetime import UTC, datetime, time, timedelta
from decimal import Decimal
from typing import Any

import psycopg

from app.services.insider_transactions import _PARSER_VERSION, parse_form_4_xml, upsert_filing
from app.services.ownership_observations_sync import sync_insiders
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

_IID = 3_227_501
_CIK = "0003227501"
_HOLDINGS_ONLY = "0003227501-26-000001"
_MIXED = "0003227501-26-000002"
_SIG = (datetime.now(UTC) - timedelta(days=30)).date()
_FILED = datetime.combine(_SIG, time(21, 30), tzinfo=UTC)


def _holding(shares: str, di: str, title: str = "Common Stock", nature: str | None = None) -> str:
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


_TXN_D = f"""
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>{_SIG - timedelta(days=2)}</value></transactionDate>
      <transactionCoding><transactionFormType>4</transactionFormType><transactionCode>S</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>100</value></transactionShares>
        <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <postTransactionAmounts>
        <sharesOwnedFollowingTransaction><value>900</value></sharesOwnedFollowingTransaction>
      </postTransactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeTransaction>"""


def _xml(table: str) -> str:
    return f"""<?xml version="1.0"?>
<ownershipDocument>
  <documentType>4</documentType>
  <periodOfReport>{_SIG - timedelta(days=2)}</periodOfReport>
  <issuer><issuerCik>0009999999</issuerCik><issuerName>T</issuerName></issuer>
  <reportingOwner>
    <reportingOwnerId><rptOwnerCik>{_CIK}</rptOwnerCik><rptOwnerName>Holder, A</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship><isDirector>1</isDirector></reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>{table}
  </nonDerivativeTable>
  <ownerSignature><signatureName>A</signatureName><signatureDate>{_SIG}</signatureDate></ownerSignature>
</ownershipDocument>
"""


# Holdings only: D one line; I two lines of one title, which sum.
_HOLDINGS_ONLY_XML = _xml(_holding("108141", "D") + _holding("500", "I", nature="By Trust") + _holding("250", "I"))
# Mixed: the D holding sits beside a D transaction (the transaction rule governs D); I is holding-only.
_MIXED_XML = _xml(_TXN_D + _holding("5", "D") + _holding("7", "I"))


def _seed(conn: psycopg.Connection[Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable)"
            " VALUES (%s, 'T3227H', 'T', TRUE)",
            (_IID,),
        )
        for accession in (_HOLDINGS_ONLY, _MIXED):
            cur.execute(
                "INSERT INTO sec_filing_manifest (accession_number, cik, form, source, subject_type, subject_id,"
                " instrument_id, filed_at, ingest_status)"
                " VALUES (%s, %s, '4', 'sec_form4', 'issuer', %s, %s, %s, 'parsed')",
                (accession, _CIK, str(_IID), _IID, _FILED),
            )
        # The old parser tombstoned the holdings-only filing.
        cur.execute(
            "INSERT INTO insider_filings (accession_number, instrument_id, document_type, primary_document_url,"
            " parser_version, is_tombstone) VALUES (%s, %s, '4', 'https://example.test/h.xml', 2, TRUE)",
            (_HOLDINGS_ONLY, _IID),
        )
    conn.commit()


def _observations(conn: psycopg.Connection[Any]) -> set[tuple[Any, ...]]:
    rows = conn.execute(
        "SELECT source_accession, holder_cik, ownership_nature, period_end, filed_at, shares"
        "  FROM ownership_insiders_observations"
        " WHERE instrument_id = %s AND source = 'form4' AND known_to IS NULL",
        (_IID,),
    ).fetchall()
    return {tuple(r) for r in rows}


def test_both_writers_write_the_same_holding_observations(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    _seed(conn)
    for accession, xml in ((_HOLDINGS_ONLY, _HOLDINGS_ONLY_XML), (_MIXED, _MIXED_XML)):
        parsed = parse_form_4_xml(xml)
        assert parsed is not None
        upsert_filing(
            conn,
            instrument_id=_IID,
            accession_number=accession,
            primary_document_url="https://example.test/h.xml",
            parsed=parsed,
            filed_at=_FILED,
        )
    conn.commit()

    live = _observations(conn)
    assert live == {
        (_HOLDINGS_ONLY, _CIK, "direct", _SIG, _FILED, Decimal("108141")),
        (_HOLDINGS_ONLY, _CIK, "indirect", _SIG, _FILED, Decimal("750")),
        # Mixed: D keeps the transaction rule's balance (900 at the transaction date), not the 5.
        (_MIXED, _CIK, "direct", _SIG - timedelta(days=2), _FILED, Decimal("900")),
        (_MIXED, _CIK, "indirect", _SIG, _FILED, Decimal("7")),
    }
    tomb, version = conn.execute(
        "SELECT is_tombstone, parser_version FROM insider_filings WHERE accession_number = %s", (_HOLDINGS_ONLY,)
    ).fetchone() or (None, None)
    assert (tomb, version) == (False, _PARSER_VERSION)
    stored = conn.execute(
        "SELECT accession_number, row_num, direct_indirect, shares FROM insider_form4_holdings"
        " WHERE instrument_id = %s ORDER BY 1, 2",
        (_IID,),
    ).fetchall()
    assert [(a, n, d, Decimal(s)) for a, n, d, s in stored] == [
        (_HOLDINGS_ONLY, 0, "D", Decimal("108141")),
        (_HOLDINGS_ONLY, 1, "I", Decimal("500")),
        (_HOLDINGS_ONLY, 2, "I", Decimal("250")),
        (_MIXED, 0, "D", Decimal("5")),
        (_MIXED, 1, "I", Decimal("7")),
    ]

    conn.execute("DELETE FROM ownership_insiders_observations WHERE instrument_id = %s", (_IID,))
    conn.commit()
    sync_insiders(conn)
    conn.commit()
    assert _observations(conn) == live

    # ``since`` applies to the holding's period_end.
    conn.execute("DELETE FROM ownership_insiders_observations WHERE instrument_id = %s", (_IID,))
    conn.commit()
    sync_insiders(conn, since=_SIG + timedelta(days=1))
    conn.commit()
    assert _observations(conn) == set()
    sync_insiders(conn, since=_SIG)
    conn.commit()
    assert {(o[0], o[2]) for o in _observations(conn)} == {
        (_HOLDINGS_ONLY, "direct"),
        (_HOLDINGS_ONLY, "indirect"),
        (_MIXED, "indirect"),
    }


def test_form4_consumers_read_the_holding_lines(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """The name resolver, the record-holder evidence and the drill-through count read a
    holdings-only Form 4 (Codex checkpoint 2 on the build)."""
    from app.services.holder_name_resolver import resolve_holder_to_filer
    from app.services.ownership_drillthrough import _form4_state
    from app.services.ownership_rollup import _read_record_holder_evidence

    conn = ebull_test_conn
    _seed(conn)
    conn.execute(
        "INSERT INTO filing_events (instrument_id, filing_date, provider, provider_filing_id)"
        " VALUES (%s, %s, 'sec', %s)",
        (_IID, _SIG, _HOLDINGS_ONLY),
    )
    conn.execute(
        "INSERT INTO filing_raw_documents (accession_number, document_kind, payload, parser_version)"
        " VALUES (%s, 'form4_xml', %s, 'form4-v1')",
        (_HOLDINGS_ONLY, _HOLDINGS_ONLY_XML),
    )
    parsed = parse_form_4_xml(_HOLDINGS_ONLY_XML)
    assert parsed is not None
    upsert_filing(
        conn,
        instrument_id=_IID,
        accession_number=_HOLDINGS_ONLY,
        primary_document_url="https://example.test/h.xml",
        parsed=parsed,
        filed_at=_FILED,
    )
    conn.commit()

    assert resolve_holder_to_filer(conn, instrument_id=_IID, holder_name="Holder, A") == (
        True,
        _CIK,
        Decimal("250"),  # the latest holding line by row
    )
    assert _read_record_holder_evidence(conn, [_HOLDINGS_ONLY])[(_HOLDINGS_ONLY, Decimal("500"))] == ("By Trust",)
    state = _form4_state(conn, _IID)
    assert state.typed_row_count == 3
    assert not any("rewash candidate" in n for n in state.notes)


_ORIGINAL = "0003227501-26-000003"
_AMENDMENT = "0003227501-26-000004"


def test_an_amendment_restating_the_transactions_keeps_the_originals_holding(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """FUSE ``0001493152-26-001021``: the original's ``D`` transaction is restated by its 4/A, its
    ``I`` holding is not (Instruction 9 — lines not amended are not repeated). The holding stays."""
    from app.services.ownership_observations import refresh_insiders_current

    conn = ebull_test_conn
    _seed(conn)
    later = _FILED + timedelta(days=20)
    for accession, filed in ((_ORIGINAL, _FILED), (_AMENDMENT, later)):
        conn.execute(
            "INSERT INTO sec_filing_manifest (accession_number, cik, form, source, subject_type, subject_id,"
            " instrument_id, filed_at, ingest_status)"
            " VALUES (%s, %s, '4', 'sec_form4', 'issuer', %s, %s, %s, 'parsed')",
            (accession, _CIK, str(_IID), _IID, filed),
        )
    conn.commit()
    original = _xml(_TXN_D + _holding("2677292", "I"))
    amendment = original.replace("<documentType>4</documentType>", "<documentType>4/A</documentType>").replace(
        "<periodOfReport>",
        f"<dateOfOriginalSubmission>{_FILED.date()}</dateOfOriginalSubmission><periodOfReport>",
    )
    amendment = amendment.replace(_holding("2677292", "I"), "")
    for accession, xml, filed in ((_ORIGINAL, original, _FILED), (_AMENDMENT, amendment, later)):
        parsed = parse_form_4_xml(xml)
        assert parsed is not None
        upsert_filing(
            conn,
            instrument_id=_IID,
            accession_number=accession,
            primary_document_url="https://example.test/h.xml",
            parsed=parsed,
            filed_at=filed,
        )
    refresh_insiders_current(conn, instrument_id=_IID)
    conn.commit()
    rows = conn.execute(
        "SELECT ownership_nature, source_accession, shares FROM ownership_insiders_current"
        " WHERE instrument_id = %s AND holder_cik = %s",
        (_IID, _CIK),
    ).fetchall()
    current = {nature: (accession, Decimal(shares)) for nature, accession, shares in rows}
    assert current["indirect"] == (_ORIGINAL, Decimal("2677292"))
    assert current["direct"][0] == _AMENDMENT  # the restated transaction is superseded
