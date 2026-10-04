"""Normalisation of N-PORT data-set rows by the #3619 slice 2b loader."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from scripts.load_3619_nport_returns import month_rows

_SUB = {
    "ACCESSION_NUMBER": "0001752724-24-232931",
    "FILING_DATE": "30-OCT-2024",
    "SUB_TYPE": "NPORT-P",
    "REPORT_DATE": "30-SEP-2024",
}


def _row(r1: str, r2: str, r3: str, class_id: str = "C000012090") -> dict[str, str]:
    return {
        "ACCESSION_NUMBER": _SUB["ACCESSION_NUMBER"],
        "MONTHLY_TOTAL_RETURN_ID": "761014",
        "CLASS_ID": class_id,
        "MONTHLY_TOTAL_RETURN1": r1,
        "MONTHLY_TOTAL_RETURN2": r2,
        "MONTHLY_TOTAL_RETURN3": r3,
    }


def test_positions_map_to_the_three_months_ending_at_the_report_date() -> None:
    rows = list(month_rows([_row("-.77", "4.39", "4.29")], {_SUB["ACCESSION_NUMBER"]: _SUB}))
    assert [(r.month_position, r.month, r.return_pct) for r in rows] == [
        (1, date(2024, 7, 1), Decimal("-.77")),
        (2, date(2024, 8, 1), Decimal("4.39")),
        (3, date(2024, 9, 1), Decimal("4.29")),
    ]
    assert {(r.report_date, r.filing_date, r.sub_type) for r in rows} == {
        (date(2024, 9, 30), date(2024, 10, 30), "NPORT-P")
    }


def test_a_blank_cell_stores_no_row_and_a_blank_class_is_none() -> None:
    rows = list(month_rows([_row("", "1.5", "", class_id="")], {_SUB["ACCESSION_NUMBER"]: _SUB}))
    assert [(r.month_position, r.class_id) for r in rows] == [(2, None)]


def test_a_return_row_without_its_submission_is_a_format_error() -> None:
    with pytest.raises(ValueError, match="no usable submission"):
        list(month_rows([_row("1", "2", "3")], {}))
