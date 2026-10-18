"""#3227 — both Form 4 observation writers fold a mixed key's holding lines through
``form4_fold_shares``: the live XML writer (``parsed.holdings``) and the weekly re-derivation
(``insider_form4_holdings``), with equal observations. Spec:
docs/specs/ownership/2026-10-18-3227-form4-mixed-groups.md, Acceptance 1."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import psycopg

from app.services.insider_transactions import (
    ParsedFiler,
    ParsedFiling,
    ParsedHolding,
    ParsedTransaction,
    _record_form4_observations_for_filing,
)
from app.services.ownership_observations_sync import derive_form4_observations
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

_IID = 3_227_501
_CIK = "0003227501"
_EARLIER = "0003227501-26-000001"
_CURRENT = "0003227501-26-000002"
_FILED = datetime(2026, 6, 11, 21, tzinfo=UTC)
_JUNE_1 = date(2026, 6, 1)

# (A/D, shares, post, D/I, nature) per transaction line; (shares, D/I, nature) per holding line.
_Txn = tuple[str, int, int, str, str | None]
_Hold = tuple[int, str, str | None]


def _seed_filing(
    cur: psycopg.Cursor[Any], accession: str, filed: datetime, txns: tuple[_Txn, ...], holds: tuple[_Hold, ...]
) -> None:
    cur.execute(
        "INSERT INTO insider_filings (accession_number, instrument_id, document_type, primary_document_url)"
        " VALUES (%s, %s, '4', 'https://example.test/f4.xml')",
        (accession, _IID),
    )
    cur.execute(
        "INSERT INTO insider_filers (accession_number, filer_cik, filer_name) VALUES (%s, %s, 'Mixed Owner')",
        (accession, _CIK),
    )
    cur.execute(
        "INSERT INTO sec_filing_manifest (accession_number, cik, form, source, subject_type, subject_id,"
        " instrument_id, filed_at, ingest_status) VALUES (%s, %s, '4', 'sec_form4', 'issuer', %s, %s, %s, 'parsed')",
        (accession, _CIK, str(_IID), _IID, filed),
    )
    for row_num, (ad, shares, post, di, nature) in enumerate(txns):
        cur.execute(
            "INSERT INTO insider_transactions (accession_number, txn_row_num, instrument_id, filer_cik, filer_name,"
            " security_title, txn_date, txn_code, acquired_disposed_code, shares, post_transaction_shares,"
            " direct_indirect, nature_of_ownership, is_derivative)"
            " VALUES (%s, %s, %s, %s, 'Mixed Owner', 'Common Stock', %s, 'P', %s, %s, %s, %s, %s, FALSE)",
            (accession, row_num, _IID, _CIK, _JUNE_1, ad, shares, post, di, nature),
        )
    for row_num, (shares, di, nature) in enumerate(holds):
        cur.execute(
            "INSERT INTO insider_form4_holdings (instrument_id, accession_number, row_num, filer_cik, filer_name,"
            " security_title, shares, direct_indirect, nature_of_ownership)"
            " VALUES (%s, %s, %s, %s, 'Mixed Owner', 'Common Stock', %s, %s, %s)",
            (_IID, accession, row_num, _CIK, shares, di, nature),
        )


def _parsed(txns: tuple[_Txn, ...], holds: tuple[_Hold, ...]) -> ParsedFiling:
    filer = ParsedFiler(
        filer_cik=_CIK,
        filer_name="Mixed Owner",
        street1=None,
        street2=None,
        city=None,
        state=None,
        zip_code=None,
        state_description=None,
        is_director=True,
        is_officer=None,
        officer_title=None,
        is_ten_percent_owner=None,
        is_other=None,
        other_text=None,
    )
    return ParsedFiling(
        document_type="4",
        period_of_report=None,
        date_of_original_submission=None,
        not_subject_to_section_16=None,
        form3_holdings_reported=None,
        form4_transactions_reported=None,
        issuer_cik=None,
        issuer_name=None,
        issuer_trading_symbol=None,
        remarks=None,
        signature_name=None,
        signature_date=None,
        filers=(filer,),
        footnotes=(),
        transactions=tuple(
            ParsedTransaction(
                txn_row_num=i,
                is_derivative=False,
                filer_cik=_CIK,
                security_title="Common Stock",
                txn_date=_JUNE_1,
                deemed_execution_date=None,
                txn_code="P",
                equity_swap_involved=None,
                txn_form_type="4",
                transaction_timeliness=None,
                shares=Decimal(shares),
                price=None,
                acquired_disposed_code=ad,
                post_transaction_shares=Decimal(post),
                direct_indirect=di,
                nature_of_ownership=nature,
                conversion_exercise_price=None,
                exercise_date=None,
                expiration_date=None,
                underlying_security_title=None,
                underlying_shares=None,
                underlying_value=None,
            )
            for i, (ad, shares, post, di, nature) in enumerate(txns)
        ),
        holdings=tuple(
            ParsedHolding(
                row_num=i,
                is_derivative=False,
                filer_cik=_CIK,
                security_title="Common Stock",
                shares=Decimal(shares),
                value_owned=None,
                direct_indirect=di,
                nature_of_ownership=nature,
                conversion_exercise_price=None,
                exercise_date=None,
                expiration_date=None,
                underlying_security_title=None,
                underlying_shares=None,
                underlying_value=None,
            )
            for i, (shares, di, nature) in enumerate(holds)
        ),
    )


def _both_writers(
    conn: psycopg.Connection[Any],
    txns: tuple[_Txn, ...],
    holds: tuple[_Hold, ...],
    earlier: tuple[_Txn, ...] = (),
) -> tuple[dict[str, Decimal], dict[str, Decimal]]:
    """Seed the filing, run the live writer and read what it stored, then derive the weekly
    writer's observations of the same accession. Returns ``({nature: shares}, {nature: shares})``."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable)"
            " VALUES (%s, 'T3227M', 'T', TRUE)",
            (_IID,),
        )
        if earlier:
            _seed_filing(cur, _EARLIER, datetime(2026, 5, 1, 21, tzinfo=UTC), earlier, ())
        _seed_filing(cur, _CURRENT, _FILED, txns, holds)
    conn.commit()
    _record_form4_observations_for_filing(
        conn,
        instrument_id=_IID,
        accession_number=_CURRENT,
        primary_document_url="https://example.test/f4.xml",
        parsed=_parsed(txns, holds),
        filed_at=_FILED,
    )
    conn.commit()
    live = {
        r[0]: r[1]
        for r in conn.execute(
            "SELECT ownership_nature, shares FROM ownership_insiders_observations"
            " WHERE source = 'form4' AND source_document_id = %s AND known_to IS NULL",
            (_CURRENT,),
        )
    }
    weekly = {o.ownership_nature: o.shares for o in derive_form4_observations(conn) if o.accession_number == _CURRENT}
    return live, weekly


def test_both_writers_fold_a_mixed_d_and_i_filing_equally(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    live, weekly = _both_writers(
        ebull_test_conn,
        (("A", 100, 100, "D", None), ("A", 30, 30, "I", "By Trust")),
        ((40, "D", None), (9, "I", "By LLC")),
    )
    expected = {"direct": Decimal("140.0000"), "indirect": Decimal("39.0000")}
    assert live == expected
    assert weekly == expected


def test_a_refused_d_key_beside_a_summable_i_key_keeps_its_latest_d_line(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    # D opens at 10, which an earlier filing reported as another form's balance and never as D's
    # (5c refuses D); I opens at zero, so 5c is not reached for I.
    live, weekly = _both_writers(
        ebull_test_conn,
        (("A", 10, 20, "D", None), ("A", 30, 30, "I", "By Trust")),
        ((7, "D", None), (9, "I", "By LLC")),
        earlier=(("A", 10, 10, "I", "By Other"),),
    )
    expected = {"direct": Decimal(20), "indirect": Decimal(39)}
    assert live == expected
    assert weekly == expected


def test_a_refused_i_fold_keeps_todays_vehicle_sum(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    # The LLC holding equals Trust A's balance (5b'): the fold refuses, the vehicle sum stands.
    live, weekly = _both_writers(
        ebull_test_conn,
        (("A", 10, 10, "I", "By Trust A"), ("A", 20, 20, "I", "By Trust B")),
        ((10, "I", "By LLC"),),
    )
    assert live == {"indirect": Decimal(30)}
    assert weekly == {"indirect": Decimal(30)}
