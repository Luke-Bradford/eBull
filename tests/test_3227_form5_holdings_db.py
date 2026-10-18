"""#3227 — a Form 5's holding lines, against a real DB.

Spec: docs/specs/ownership/2026-10-18-3227-form5-holdings.md, Acceptance 1. Both observation writers
(the live one through ``upsert_filing``, the weekly ``sync_insiders`` from ``insider_form4_holdings``)
turn stored Form 5s into the SAME observations and ``_current``; a derivative-only tombstone revives
with no observation; the rollup's component scope and the drill-through count read the right lines.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any
from uuid import uuid4

import psycopg

from app.services.insider_transactions import parse_form_5_xml, upsert_filing
from app.services.ownership_observations import record_insider_observation, refresh_insiders_current
from app.services.ownership_observations_sync import sync_insiders
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

_IID = 3_227_551
_FILED = datetime.combine((datetime.now(UTC) - timedelta(days=40)).date(), time(21, 30), tzinfo=UTC)
_FYE = _FILED.date() - timedelta(days=44)
_TXN_DATE = _FYE - timedelta(days=100)
_URL = "https://example.test/f5.xml"


def _acc(n: int) -> str:
    return f"0003227551-26-{n:06d}"


def _cik(n: int) -> str:
    return f"00032275{n:02d}"


def _holding(shares: str, di: str, *, form_type: str | None = None, derivative: bool = False) -> str:
    tag = "derivativeHolding" if derivative else "nonDerivativeHolding"
    coding = (
        f"<transactionCoding><transactionFormType>{form_type}</transactionFormType></transactionCoding>"
        if form_type
        else ""
    )
    return f"""
    <{tag}>
      <securityTitle><value>{"Stock Option" if derivative else "Common Stock"}</value></securityTitle>{coding}
      <postTransactionAmounts>
        <sharesOwnedFollowingTransaction><value>{shares}</value></sharesOwnedFollowingTransaction>
      </postTransactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>{di}</value></directOrIndirectOwnership></ownershipNature>
    </{tag}>"""


def _txn(post: str | None, txn_date: date = _TXN_DATE) -> str:
    post_el = (
        f"<postTransactionAmounts><sharesOwnedFollowingTransaction><value>{post}</value>"
        "</sharesOwnedFollowingTransaction></postTransactionAmounts>"
        if post is not None
        else ""
    )
    return f"""
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>{txn_date}</value></transactionDate>
      <transactionCoding><transactionFormType>5</transactionFormType><transactionCode>G</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>100</value></transactionShares>
        <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
      </transactionAmounts>{post_el}
      <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeTransaction>"""


def _xml(n: int, table: str, *, document_type: str = "5", exit_box: bool = False, derivative: str = "") -> str:
    return f"""<?xml version="1.0"?>
<ownershipDocument>
  <documentType>{document_type}</documentType>
  <periodOfReport>{_FYE}</periodOfReport>
  <notSubjectToSection16>{1 if exit_box else 0}</notSubjectToSection16>
  <issuer><issuerCik>0009999999</issuerCik><issuerName>T</issuerName></issuer>
  <reportingOwner>
    <reportingOwnerId><rptOwnerCik>{_cik(n)}</rptOwnerCik><rptOwnerName>Holder {n}</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship><isDirector>1</isDirector></reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>{table}
  </nonDerivativeTable>
  <derivativeTable>{derivative}
  </derivativeTable>
  <ownerSignature><signatureName>H</signatureName><signatureDate>{_FILED.date()}</signatureDate></ownerSignature>
</ownershipDocument>
"""


# One accession (and one filer) per case, so each holder's keys are the case's own.
_CASES: dict[int, str] = {
    # holdings-only: D one line; I two lines of one title, summed
    1: _xml(1, _holding("4000", "D") + _holding("2000", "I") + _holding("70.436", "I")),
    # mixed: the D holding sits beside a kept D transaction; I is holding-only
    2: _xml(2, _txn("900") + _holding("5", "D") + _holding("7", "I")),
    # mixed with D transactions the writers do not keep (no balance; a date after the filing)
    3: _xml(3, _txn(None) + _txn("12", _FILED.date() + timedelta(days=3)) + _holding("11", "D")),
    # gates
    4: _xml(4, _holding("13", "D"), document_type="5/A"),
    5: _xml(5, _holding("17", "D"), exit_box=True),
    # a delinquent-Form-3 line beside an ordinary one refuses D; I is written. A DERA ``direct``
    # winner of the same accession is dropped by de-collision clause 1 (the spec's residual).
    6: _xml(6, _holding("19", "D") + _holding("23", "D", form_type="3") + _holding("29", "I")),
    # a same-accession DERA tie, which de-collision hands to the XML row
    7: _xml(7, _holding("300", "D")),
}
_DERA = {6: ("direct", Decimal("2070.44")), 7: ("direct", Decimal("300"))}


def _seed(conn: psycopg.Connection[Any], cases: dict[int, str]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable)"
            " VALUES (%s, 'T3227F', 'T', TRUE)",
            (_IID,),
        )
        for n in cases:
            cur.execute(
                "INSERT INTO sec_filing_manifest (accession_number, cik, form, source, subject_type, subject_id,"
                " instrument_id, filed_at, ingest_status)"
                " VALUES (%s, %s, '5', 'sec_form5', 'issuer', %s, %s, %s, 'parsed')",
                (_acc(n), _cik(n), str(_IID), _IID, _FILED),
            )
            cur.execute(
                "INSERT INTO filing_events (instrument_id, filing_date, provider, provider_filing_id)"
                " VALUES (%s, %s, 'sec', %s)",
                (_IID, _FILED.date(), _acc(n)),
            )
    for n, (nature, shares) in _DERA.items():
        if n in cases:
            record_insider_observation(
                conn,
                instrument_id=_IID,
                holder_cik=_cik(n),
                holder_name=f"Holder {n}",
                ownership_nature=nature,  # type: ignore[arg-type]
                source="form4",
                source_document_id=f"{_acc(n)}:NDH:1",
                source_accession=_acc(n),
                source_field=None,
                source_url=None,
                filed_at=_FILED,
                period_start=None,
                period_end=_FYE,
                ingest_run_id=uuid4(),
                shares=shares,
            )
    conn.commit()


def _ingest(conn: psycopg.Connection[Any], cases: dict[int, str]) -> None:
    for n, xml in cases.items():
        parsed = parse_form_5_xml(xml)
        assert parsed is not None
        upsert_filing(conn, instrument_id=_IID, accession_number=_acc(n), primary_document_url=_URL, parsed=parsed)
    conn.commit()


def _xml_observations(conn: psycopg.Connection[Any]) -> set[tuple[Any, ...]]:
    rows = conn.execute(
        "SELECT source_accession, holder_cik, ownership_nature, period_end, filed_at, shares"
        "  FROM ownership_insiders_observations"
        " WHERE instrument_id = %s AND source = 'form4' AND known_to IS NULL AND source_document_id !~ ':'",
        (_IID,),
    ).fetchall()
    return {(a, c, n, p, f, Decimal(s)) for a, c, n, p, f, s in rows}


def _current(conn: psycopg.Connection[Any]) -> set[tuple[Any, ...]]:
    rows = conn.execute(
        "SELECT holder_cik, ownership_nature, source_document_id, shares FROM ownership_insiders_current"
        " WHERE instrument_id = %s",
        (_IID,),
    ).fetchall()
    return {(c, n, d, Decimal(s)) for c, n, d, s in rows}


def test_both_writers_write_the_same_form5_observations(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    _seed(conn, _CASES)
    _ingest(conn, _CASES)
    refresh_insiders_current(conn, instrument_id=_IID)
    conn.commit()

    live = _xml_observations(conn)
    assert live == {
        (_acc(1), _cik(1), "direct", _FYE, _FILED, Decimal("4000")),
        (_acc(1), _cik(1), "indirect", _FYE, _FILED, Decimal("2070.436")),
        # mixed: D keeps the transaction rule's balance at its date, I is the holding
        (_acc(2), _cik(2), "direct", _TXN_DATE, _FILED, Decimal("900")),
        (_acc(2), _cik(2), "indirect", _FYE, _FILED, Decimal("7")),
        # case 3: no D observation at all; cases 4 and 5 gated; case 6 D gated
        (_acc(6), _cik(6), "indirect", _FYE, _FILED, Decimal("29")),
        (_acc(7), _cik(7), "direct", _FYE, _FILED, Decimal("300")),
    }
    current = _current(conn)
    # The gated D key of case 6 loses its DERA winner to clause 1; the case-7 tie goes to the XML row.
    assert not any(c == _cik(6) and n == "direct" for c, n, _d, _s in current)
    assert (_cik(7), "direct", _acc(7), Decimal("300")) in current
    stored = conn.execute(
        "SELECT accession_number, row_num, direct_indirect, shares, holding_form_type FROM insider_form4_holdings"
        " WHERE accession_number = %s ORDER BY row_num",
        (_acc(6),),
    ).fetchall()
    assert [(n, d, Decimal(s), t) for _a, n, d, s, t in stored] == [
        (0, "D", Decimal("19"), None),
        (1, "D", Decimal("23"), "3"),
        (2, "I", Decimal("29"), None),
    ]

    conn.execute(
        "DELETE FROM ownership_insiders_observations WHERE instrument_id = %s AND source_document_id !~ ':'", (_IID,)
    )
    conn.commit()
    sync_insiders(conn)
    conn.commit()
    assert _xml_observations(conn) == live
    assert _current(conn) == current

    # ``since`` applies to a Form 5 holding's period of report.
    conn.execute(
        "DELETE FROM ownership_insiders_observations WHERE instrument_id = %s AND source_document_id !~ ':'", (_IID,)
    )
    conn.commit()
    sync_insiders(conn, since=_FYE + timedelta(days=1))
    conn.commit()
    assert _xml_observations(conn) == set()
    sync_insiders(conn, since=_FYE)
    conn.commit()
    assert _xml_observations(conn) == {o for o in live if o[3] == _FYE}


def test_weekly_limit_counts_form5_holding_units(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    from app.services.ownership_observations_sync import derive_form4_observations

    conn = ebull_test_conn
    cases = {n: _CASES[n] for n in (1, 7)}
    _seed(conn, cases)
    _ingest(conn, cases)
    # Units order by (instrument, accession, filer): LIMIT 1 is case 1's unit alone.
    got = {(o.accession_number, o.ownership_nature, o.kind) for o in derive_form4_observations(conn, limit=1)}
    assert got == {(_acc(1), "direct", "holding"), (_acc(1), "indirect", "holding")}


def test_a_derivative_only_tombstone_revives_with_no_observation(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    from app.services.raw_filings import read_raw, store_raw
    from app.services.rewash_filings import _apply_form5

    conn = ebull_test_conn
    _seed(conn, {8: ""})
    xml = _xml(8, "", derivative=_holding("500", "D", derivative=True))
    conn.execute(
        "INSERT INTO insider_filings (accession_number, instrument_id, document_type, primary_document_url,"
        " parser_version, is_tombstone) VALUES (%s, %s, '5', %s, 3, TRUE)",
        (_acc(8), _IID, _URL),
    )
    store_raw(conn, accession_number=_acc(8), document_kind="form5_xml", payload=xml, parser_version="form5-v1")
    conn.commit()
    raw = read_raw(conn, accession_number=_acc(8), document_kind="form5_xml")
    assert raw is not None
    assert _apply_form5(conn, raw) is True
    conn.commit()

    assert conn.execute(
        "SELECT is_tombstone FROM insider_filings WHERE accession_number = %s", (_acc(8),)
    ).fetchone() == (False,)
    rows = conn.execute(
        "SELECT is_derivative, shares FROM insider_form4_holdings WHERE accession_number = %s", (_acc(8),)
    ).fetchall()
    assert [(d, Decimal(s)) for d, s in rows] == [(True, Decimal("500"))]
    assert _xml_observations(conn) == set()


def test_the_component_scope_excludes_gated_form5_lines(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """A code-3 line, an exit-box line and a 5/A line, each on an accession whose holder wins
    another key, never reach the control-group match; the ordinary lines do."""
    from app.services.ownership_rollup import _INSIDER_LINE_COMPONENTS_SQL

    conn = ebull_test_conn
    cases = {
        # code 3: the I line wins its key, the code-3 D line must not appear
        6: _CASES[6],
        # exit box and 5/A: the holder wins through a kept D transaction
        9: _xml(9, _txn("900") + _holding("31", "I"), exit_box=True),
        10: _xml(10, _txn("900") + _holding("37", "I"), document_type="5/A"),
        # Form 5 control: the same shape, ordinary — its I holding line is in scope
        11: _xml(11, _txn("900") + _holding("41", "I")),
    }
    _seed(conn, cases)
    _ingest(conn, cases)
    refresh_insiders_current(conn, instrument_id=_IID)
    conn.commit()
    rows = conn.execute(_INSIDER_LINE_COMPONENTS_SQL, {"iid": _IID}).fetchall()
    holding_terms = {(r[0], r[2], Decimal(r[4])) for r in rows if r[3] == "H"}
    assert holding_terms == {
        (_acc(6), "D", Decimal("19")),
        (_acc(6), "I", Decimal("29")),
        (_acc(11), "I", Decimal("41")),
    }


def test_the_drill_through_counts_form5_holding_rows(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    from app.services.ownership_drillthrough import _form4_state

    conn = ebull_test_conn
    cases = {
        1: _CASES[1],  # holdings-only 5: three lines
        4: _CASES[4],  # holdings-only 5/A: one line
        12: _xml(12, "", derivative=_holding("500", "D", derivative=True)),  # derivative-only 5: one line
    }
    _seed(conn, cases | {13: ""})
    _ingest(conn, cases)
    # A holdings-only 4/A stays excluded from both counts (a pre-existing gap, kept).
    conn.execute(
        "INSERT INTO insider_filings (accession_number, instrument_id, document_type, primary_document_url,"
        " parser_version) VALUES (%s, %s, '4/A', %s, 3)",
        (_acc(13), _IID, _URL),
    )
    conn.execute(
        "INSERT INTO insider_form4_holdings (instrument_id, accession_number, row_num, filer_cik, filer_name, shares,"
        " is_derivative, direct_indirect) VALUES (%s, %s, 0, %s, 'H', 1, FALSE, 'D')",
        (_IID, _acc(13), _cik(13)),
    )
    conn.commit()
    state = _form4_state(conn, _IID)
    assert state.typed_row_count == 5
    assert not any("rewash candidate" in n for n in state.notes)


def test_the_backfill_stamps_a_revived_filing_as_parsed(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Codex checkpoint 2: the replay leaves the manifest row ``parsed`` at ``form5-v2`` (via
    ``pending``, the state machine's re-parse gate) and the raw body at ``form5-v2``."""
    from app.services.insider_transactions import _PARSER_VERSION_FORM5
    from app.services.raw_filings import store_raw
    from scripts.ab_3227_form5_holdings import _stamp

    conn = ebull_test_conn
    _seed(conn, {1: _CASES[1], 2: _CASES[2]})
    conn.execute(
        "UPDATE sec_filing_manifest SET ingest_status = 'tombstoned', parser_version = 'form5-v1'"
        " WHERE accession_number = %s",
        (_acc(1),),
    )
    conn.execute("UPDATE sec_filing_manifest SET parser_version = 'form5-v1' WHERE accession_number = %s", (_acc(2),))
    for n in (1, 2):
        store_raw(
            conn, accession_number=_acc(n), document_kind="form5_xml", payload=_CASES[n], parser_version="form5-v1"
        )
    conn.commit()
    for n in (1, 2):
        _stamp(conn, _acc(n))
    conn.commit()
    rows = conn.execute(
        "SELECT m.ingest_status, m.parser_version, m.error, r.parser_version FROM sec_filing_manifest m"
        " JOIN filing_raw_documents r ON r.accession_number = m.accession_number AND r.document_kind = 'form5_xml'"
        " WHERE m.accession_number = ANY(%s) ORDER BY m.accession_number",
        ([_acc(1), _acc(2)],),
    ).fetchall()
    assert rows == [("parsed", _PARSER_VERSION_FORM5, None, _PARSER_VERSION_FORM5)] * 2
