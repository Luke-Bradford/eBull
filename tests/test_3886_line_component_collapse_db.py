"""#3886 — ``_INSIDER_LINE_COMPONENTS_SQL`` and the ``line_components`` it drives, against a real DB.

One Form 4 for one filer: a ``D`` line, two ``I`` series (one with an earlier balance before its
final, one ending at zero) and an ``I`` holding line. Each case changes the stored observation.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

import psycopg
import pytest

from app.services import ownership_observations as oo
from app.services.ownership_rollup import _collect_canonical_holders_from_current

_IID = 938_860
_ACCN = "0001507650-26-000007"
_CIK = "0001507650"

# (row, D/I, nature, txn_date, post_transaction_shares)
_TXNS = [
    (1, "D", None, date(2026, 6, 10), "31000"),
    (2, "I", "By Spouse", date(2026, 6, 10), "573000000"),
    (3, "I", "By Spouse", date(2026, 6, 12), "573883879"),  # the series' final
    (4, "I", "By LLC", date(2026, 6, 11), "0"),  # a series ending at zero contributes nothing
    (5, "I", "By LLC", date(2026, 6, 10), "4000000"),  # earlier date, not the final
]
_HOLDING = "25824474"
_SUM = Decimal("573883879") + Decimal(_HOLDING)


@pytest.fixture
def conn(ebull_test_conn):
    return ebull_test_conn


def _seed(conn: psycopg.Connection[Any], indirect_shares: Decimal, *, null_di_line: bool, holding_title: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)",
            (_IID, "T3886", "Test 3886 Co"),
        )
        cur.execute(
            "INSERT INTO insider_filings (accession_number, instrument_id, document_type) VALUES (%s, %s, '4')",
            (_ACCN, _IID),
        )
        cur.execute(
            "INSERT INTO insider_filers (accession_number, filer_cik, filer_name) VALUES (%s, %s, 'Zanganeh Mahkam')",
            (_ACCN, _CIK),
        )
        for row, di, nature, txn_date, post in _TXNS:
            cur.execute(
                "INSERT INTO insider_transactions (accession_number, txn_row_num, instrument_id, filer_cik,"
                " filer_name, txn_date, txn_code, shares, direct_indirect, nature_of_ownership, is_derivative,"
                " post_transaction_shares, acquired_disposed_code, security_title)"
                " VALUES (%s, %s, %s, %s, 'Zanganeh Mahkam', %s, 'G', 1, %s, %s, FALSE, %s, 'A', 'Common Stock')",
                (_ACCN, row, _IID, _CIK, txn_date, di, nature, Decimal(post)),
            )
        if null_di_line:
            # A line no writer can sum (``form4_fold_shares`` condition 2), so no unit decomposes.
            cur.execute(
                "INSERT INTO insider_transactions (accession_number, txn_row_num, instrument_id, filer_cik,"
                " filer_name, txn_date, txn_code, shares, is_derivative, post_transaction_shares,"
                " acquired_disposed_code, security_title)"
                " VALUES (%s, 6, %s, %s, 'Zanganeh Mahkam', '2026-06-12', 'G', 1, FALSE, 7, 'A', 'Common Stock')",
                (_ACCN, _IID, _CIK),
            )
        cur.execute(
            "INSERT INTO insider_form4_holdings (instrument_id, accession_number, row_num, filer_cik, filer_name,"
            " security_title, shares, is_derivative, direct_indirect, nature_of_ownership)"
            " VALUES (%s, %s, 1, %s, 'Zanganeh Mahkam', %s, %s, FALSE, 'I', 'By the Trust')",
            (_IID, _ACCN, _CIK, holding_title, Decimal(_HOLDING)),
        )
    for nature, shares in (("direct", Decimal(31000)), ("indirect", indirect_shares)):
        oo.record_insider_observation(
            conn,
            instrument_id=_IID,
            holder_cik=_CIK,
            holder_name="Zanganeh Mahkam",
            ownership_nature=nature,  # type: ignore[arg-type]
            source="form4",
            source_document_id=_ACCN,
            source_accession=_ACCN,
            source_field=None,
            source_url=None,
            filed_at=datetime(2026, 6, 15, tzinfo=UTC),
            period_start=None,
            period_end=date(2026, 6, 12),
            ingest_run_id=uuid4(),
            shares=shares,
        )
    oo.refresh_insiders_current(conn, instrument_id=_IID)
    conn.commit()


@pytest.mark.parametrize(
    ("indirect_shares", "null_di_line", "holding_title", "expected"),
    [
        pytest.param(
            _SUM,
            False,
            "Common Stock",
            (Decimal("573883879"), Decimal(_HOLDING)),
            id="summed_observation_has_its_lines",
        ),
        pytest.param(Decimal("573883879"), False, "Common Stock", (), id="latest_line_observation_has_none"),
        pytest.param(_SUM, True, "Common Stock", (), id="a_null_di_unit_has_none"),
        pytest.param(_SUM, False, "Class B Common Stock", (), id="a_two_title_unit_has_none"),
    ],
)
def test_line_components(
    conn: psycopg.Connection[Any], indirect_shares: Decimal, null_di_line: bool, holding_title: str, expected: tuple
) -> None:
    _seed(conn, indirect_shares, null_di_line=null_di_line, holding_title=holding_title)
    by_nature = {c.ownership_nature: c.line_components for c in _collect_canonical_holders_from_current(conn, _IID)}
    assert by_nature["indirect"] == expected
    assert by_nature["direct"] == ()  # one line
