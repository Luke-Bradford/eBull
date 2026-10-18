"""#3227 — both Form 4 observation writers apply ``form4_observation_shares``: the live XML writer
(including its cross-filing 5c query) and the weekly ``sync_insiders`` re-derivation (units first,
so a ``since`` window starting mid-filing still writes the whole-filing sum)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import psycopg

from app.services.insider_transactions import (
    ParsedFiler,
    ParsedFiling,
    ParsedTransaction,
    _record_form4_observations_for_filing,
)
from app.services.ownership_observations_sync import sync_insiders
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

_IID = 3_227_401
_CIK = "0003227401"
_EARLIER = "0003227401-26-000001"
_CURRENT = "0003227401-26-000002"

# (A/D, shares, post, D/I, nature, txn_date) per line, txn_row_num in order.
_Spec = tuple[str, int, int | None, str, str | None, date]


def _seed_filing(cur: psycopg.Cursor[Any], accession: str, filed: datetime, lines: tuple[_Spec, ...]) -> None:
    cur.execute(
        "INSERT INTO insider_filings (accession_number, instrument_id, document_type, primary_document_url)"
        " VALUES (%s, %s, '4', 'https://example.test/f4.xml')",
        (accession, _IID),
    )
    cur.execute(
        "INSERT INTO insider_filers (accession_number, filer_cik, filer_name) VALUES (%s, %s, 'Vehicle Owner')",
        (accession, _CIK),
    )
    cur.execute(
        "INSERT INTO sec_filing_manifest (accession_number, cik, form, source, subject_type, subject_id,"
        " instrument_id, filed_at, ingest_status) VALUES (%s, %s, '4', 'sec_form4', 'issuer', %s, %s, %s, 'parsed')",
        (accession, _CIK, str(_IID), _IID, filed),
    )
    for row_num, (ad, shares, post, di, nature, txn_date) in enumerate(lines):
        cur.execute(
            "INSERT INTO insider_transactions (accession_number, txn_row_num, instrument_id, filer_cik, filer_name,"
            " security_title, txn_date, txn_code, acquired_disposed_code, shares, post_transaction_shares,"
            " direct_indirect, nature_of_ownership, is_derivative)"
            " VALUES (%s, %s, %s, %s, 'Vehicle Owner', 'Common Stock', %s, 'P', %s, %s, %s, %s, %s, FALSE)",
            (accession, row_num, _IID, _CIK, txn_date, ad, shares, post, di, nature),
        )


def _seed(conn: psycopg.Connection[Any], earlier: tuple[_Spec, ...], current: tuple[_Spec, ...]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable)"
            " VALUES (%s, 'T3227V', 'T', TRUE)",
            (_IID,),
        )
        if earlier:
            _seed_filing(cur, _EARLIER, datetime(2026, 5, 1, 21, tzinfo=UTC), earlier)
        _seed_filing(cur, _CURRENT, datetime(2026, 6, 11, 21, tzinfo=UTC), current)
    conn.commit()


def _parsed(lines: tuple[_Spec, ...]) -> ParsedFiling:
    filer = ParsedFiler(
        filer_cik=_CIK,
        filer_name="Vehicle Owner",
        street1=None,
        street2=None,
        city=None,
        state=None,
        zip_code=None,
        state_description=None,
        is_director=None,
        is_officer=None,
        officer_title=None,
        is_ten_percent_owner=True,
        is_other=None,
        other_text=None,
    )
    txns = tuple(
        ParsedTransaction(
            txn_row_num=i,
            is_derivative=False,
            filer_cik=_CIK,
            security_title="Common Stock",
            txn_date=txn_date,
            deemed_execution_date=None,
            txn_code="P",
            equity_swap_involved=None,
            txn_form_type="4",
            transaction_timeliness=None,
            shares=Decimal(shares),
            price=None,
            acquired_disposed_code=ad,
            post_transaction_shares=Decimal(post) if post is not None else None,
            direct_indirect=di,
            nature_of_ownership=nature,
            conversion_exercise_price=None,
            exercise_date=None,
            expiration_date=None,
            underlying_security_title=None,
            underlying_shares=None,
            underlying_value=None,
        )
        for i, (ad, shares, post, di, nature, txn_date) in enumerate(lines)
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
        transactions=txns,
    )


def _shares(conn: psycopg.Connection[Any], nature: str) -> list[Decimal]:
    rows = conn.execute(
        "SELECT shares FROM ownership_insiders_observations"
        " WHERE source = 'form4' AND source_document_id = %s AND ownership_nature = %s AND known_to IS NULL",
        (_CURRENT, nature),
    ).fetchall()
    return [r[0] for r in rows]


_JUNE_1, _JUNE_10 = date(2026, 6, 1), date(2026, 6, 10)
# Form B opens at 900. Earlier, the filer reported 900 as Form A's balance and never as Form B's,
# so it is a running total carried across filings (5c); as Form B's own balance it clears.
_CURRENT_LINES: tuple[_Spec, ...] = (
    ("A", 100, 500, "I", "By A", _JUNE_1),
    ("A", 100, 1_000, "I", "By B", _JUNE_10),
)


def _live(conn: psycopg.Connection[Any]) -> None:
    _record_form4_observations_for_filing(
        conn,
        instrument_id=_IID,
        accession_number=_CURRENT,
        primary_document_url="https://example.test/f4.xml",
        parsed=_parsed(_CURRENT_LINES),
        filed_at=datetime(2026, 6, 11, 21, tzinfo=UTC),
    )
    conn.commit()


def test_live_writer_sums_when_the_earlier_balance_is_the_same_form(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    _seed(
        conn,
        (("A", 400, 400, "I", "By A", date(2026, 4, 1)), ("A", 900, 900, "I", "By B", date(2026, 4, 1))),
        _CURRENT_LINES,
    )
    _live(conn)
    assert _shares(conn, "indirect") == [Decimal(1_500)]


def test_live_writer_refuses_a_running_total_across_filings(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    _seed(
        conn,
        (("A", 400, 400, "I", "By A", date(2026, 3, 1)), ("A", 500, 900, "I", "By A", date(2026, 4, 1))),
        _CURRENT_LINES,
    )
    _live(conn)
    assert _shares(conn, "indirect") == [Decimal(1_000)]  # the latest line, as before #3227


def test_weekly_writer_since_window_mid_filing_writes_the_whole_filing_sum(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    _seed(conn, (), _CURRENT_LINES)
    sync_insiders(conn, since=date(2026, 6, 5))  # only Form B's line is dated in the window
    conn.commit()
    assert _shares(conn, "indirect") == [Decimal(1_500)]


def test_weekly_writer_writes_no_indirect_observation_without_an_eligible_i_line(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    _seed(
        conn,
        (),
        (
            ("A", 100, 100, "D", None, _JUNE_1),
            ("A", 10, None, "I", "By A", _JUNE_1),
            ("A", 5, None, "I", "By B", _JUNE_1),
        ),
    )
    sync_insiders(conn)
    conn.commit()
    assert _shares(conn, "direct") == [Decimal(100)]
    assert _shares(conn, "indirect") == []
