"""#3623 — published factor libraries: global-q q5, JKP, AQR QMJ/BAB/TSMOM and the Kenneth French sort files."""

from __future__ import annotations

import io
import zipfile
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import httpx
import pytest
from openpyxl import Workbook

from app.services.reference_data import (
    AQR_DATASET_KEYS,
    FACTOR_LIBRARY_DATASET_KEYS,
    FRED_DATASET_KEYS,
    FRENCH_DATASET_KEYS,
    GLOBAL_Q_INDEX_URL,
    REFERENCE_DATASETS,
    ReferenceDataSourceError,
    parse_aqr_monthly_sheet,
    parse_fed_ebp_csv,
    parse_global_q_monthly_csv,
    parse_jkp_monthly_zip,
    resolve_global_q_monthly_url,
)


def _zip(text: str) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("data.csv", text)
    return output.getvalue()


def _xlsx(sheet_name: str, *rows: tuple[Any, ...]) -> bytes:
    workbook = Workbook()
    sheet = workbook.active
    assert sheet is not None
    sheet.title = sheet_name
    for row in rows:
        sheet.append(row)
    output = io.BytesIO()
    workbook.save(output)
    workbook.close()
    return output.getvalue()


def test_every_dataset_belongs_to_exactly_one_scheduled_group() -> None:
    groups = (*FRENCH_DATASET_KEYS, *AQR_DATASET_KEYS, *FRED_DATASET_KEYS, *FACTOR_LIBRARY_DATASET_KEYS)
    assert len(groups) == len(set(groups))
    assert set(groups) == set(REFERENCE_DATASETS)
    assert {REFERENCE_DATASETS[key].source for key in FACTOR_LIBRARY_DATASET_KEYS} == {"global_q", "jkp"}


def test_global_q_parser_normalises_percent_to_decimal_month_end() -> None:
    parsed = parse_global_q_monthly_csv(b"year,month,R_F,R_MKT,R_ME,R_IA,R_ROE,R_EG\n2024,2,0.4,1.5,,-2,3,0.25\n\n")
    assert parsed.missing_count == 1
    assert {(o.series_key, o.observation_date, o.value) for o in parsed.observations} == {
        ("R_F", date(2024, 2, 29), Decimal("0.004")),
        ("R_MKT", date(2024, 2, 29), Decimal("0.015")),
        ("R_IA", date(2024, 2, 29), Decimal("-0.02")),
        ("R_ROE", date(2024, 2, 29), Decimal("0.03")),
        ("R_EG", date(2024, 2, 29), Decimal("0.0025")),
    }
    with pytest.raises(ReferenceDataSourceError, match="header"):
        parse_global_q_monthly_csv(b"year,month,R_F,R_MKT\n2024,1,1,1\n")


def test_global_q_resolver_takes_the_newest_year_stamped_file() -> None:
    page = (
        '<a href="/uploads/1/2/2/6/122679606/q5_factors_monthly_2024.csv">old</a>'
        '<a href="/uploads/1/2/2/6/122679606/q5_factors_monthly_2025.csv">new</a>'
        '<a href="/uploads/1/2/2/6/122679606/q5_factors_annual_2026.csv">annual</a>'
    )

    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == GLOBAL_Q_INDEX_URL
        return httpx.Response(200, text=page, request=request)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        assert (
            resolve_global_q_monthly_url(client, GLOBAL_Q_INDEX_URL)
            == "https://global-q.org/uploads/1/2/2/6/122679606/q5_factors_monthly_2025.csv"
        )

    def empty(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="<html></html>", request=request)

    with httpx.Client(transport=httpx.MockTransport(empty)) as client:
        with pytest.raises(ReferenceDataSourceError, match="links no"):
            resolve_global_q_monthly_url(client, GLOBAL_Q_INDEX_URL)


_JKP_HEADER = "location,name,freq,weighting,direction,n_stocks,n_stocks_min,date,ret\n"


def test_jkp_parser_stores_signed_returns_per_factor() -> None:
    parsed = parse_jkp_monthly_zip(
        _zip(
            _JKP_HEADER + "usa,age,monthly,vw_cap,-1,505,8,1926-02-28,-0.066\n"
            "usa,be_me,monthly,vw_cap,1,505,8,1926-02-28,0.012\n"
            "usa,be_me,monthly,vw_cap,1,505,8,1926-03-31,NA\n"
        ),
        location="usa",
        weighting="vw_cap",
    )
    assert parsed.missing_count == 1
    assert [(o.series_key, o.observation_date, o.value) for o in parsed.observations] == [
        ("age", date(1926, 2, 28), Decimal("-0.066")),
        ("be_me", date(1926, 2, 28), Decimal("0.012")),
    ]


@pytest.mark.parametrize(
    ("row", "match"),
    [
        ("usa,age,monthly,ew,-1,505,8,1926-02-28,0.1\n", "expected usa/monthly/vw_cap"),
        ("usa,age,monthly,vw_cap,-1,505,8\n", "ragged"),
        ("usa,age,monthly,vw_cap,-1,505,8,1926-02,0.1\n", "invalid date"),
    ],
)
def test_jkp_parser_refuses_the_wrong_slice_or_shape(row: str, match: str) -> None:
    with pytest.raises(ReferenceDataSourceError, match=match):
        parse_jkp_monthly_zip(_zip(_JKP_HEADER + row), location="usa", weighting="vw_cap")


def test_aqr_tsmom_sheet_with_a_blank_date_header_cell() -> None:
    header = (None, "TSMOM", "TSMOM^CM", "TSMOM^EQ", "TSMOM^FI", "TSMOM^FX")
    payload = _xlsx(
        "TSMOM Factors",
        ("intro",),
        header,
        (datetime(1985, 1, 31), 0.04, -0.01, 0.15, "", 0.05),
        ("", "", "", "", "", ""),
    )
    parsed = REFERENCE_DATASETS["aqr_tsmom_monthly"].parser(payload)
    assert parsed.missing_count == 1
    assert {o.series_key for o in parsed.observations} == {"TSMOM", "TSMOM^CM", "TSMOM^EQ", "TSMOM^FX"}
    assert {o.observation_date for o in parsed.observations} == {date(1985, 1, 31)}

    with pytest.raises(ReferenceDataSourceError, match="no 'QMJ Factors' worksheet"):
        REFERENCE_DATASETS["aqr_qmj_monthly"].parser(payload)


def test_aqr_header_may_leave_only_the_date_cell_empty() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        parse_aqr_monthly_sheet(b"", sheet="x", header=("DATE", None))


def test_fed_ebp_parser_types_spreads_and_probability() -> None:
    parsed = parse_fed_ebp_csv(b"date,gz_spread,ebp,est_prob\n7/1/2026,0.84,-0.32,0.108\n8/1/2026,0.9,,0.2\n")
    assert parsed.missing_count == 1
    assert [(o.series_key, o.observation_date, o.value, o.unit) for o in parsed.observations] == [
        ("ebp", date(2026, 7, 1), Decimal("-0.32"), "percent_per_annum"),
        ("est_prob", date(2026, 7, 1), Decimal("0.108"), "probability"),
        ("gz_spread", date(2026, 7, 1), Decimal("0.84"), "percent_per_annum"),
        ("est_prob", date(2026, 8, 1), Decimal("0.2"), "probability"),
        ("gz_spread", date(2026, 8, 1), Decimal("0.9"), "percent_per_annum"),
    ]


@pytest.mark.parametrize(
    ("text", "match"),
    [
        (b"date,gz_spread,ebp\n7/1/2026,1,1\n", "header"),
        (b"date,gz_spread,ebp,est_prob\n2026-07-01,1,1,0.1\n", "invalid date"),
        (b"date,gz_spread,ebp,est_prob\n7/1/2026,1,1,1.5\n", r"outside \[0, 1\]"),
    ],
)
def test_fed_ebp_parser_refuses_drift(text: bytes, match: str) -> None:
    with pytest.raises(ReferenceDataSourceError, match=match):
        parse_fed_ebp_csv(text)
