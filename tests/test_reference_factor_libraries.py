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
    OSAP_DATA_PAGE_URL,
    OSAP_USER_AGENT,
    REFERENCE_DATASETS,
    ReferenceDataSourceError,
    parse_aqr_monthly_sheet,
    parse_fed_ebp_csv,
    parse_french_daily_zip,
    parse_global_q_monthly_csv,
    parse_jkp_monthly_zip,
    parse_jkp_nyse_cutoffs_csv,
    parse_jkp_return_cutoffs_csv,
    parse_osap_ls_wide_csv,
    resolve_global_q_monthly_url,
    resolve_osap_ls_wide_url,
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
    assert {REFERENCE_DATASETS[key].source for key in FACTOR_LIBRARY_DATASET_KEYS} == {"global_q", "jkp", "osap"}


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


_FRENCH_DAILY = (
    "This file was created by using the 202608 CRSP database.\r\n"
    "The Tbill return is the simple daily rate that, over the number of trading days\r\n"
    "\r\n"
    ",Mkt-RF,SMB,HML,RF\r\n"
    "20140829,    0.29,   -0.02,   -0.18,    0.00\r\n"
    "20140902,   -0.03,    0.31,   -0.05,   -99.99\r\n"
    "\r\n"
    "Copyright 2026 Eugene F. Fama and Kenneth R. French\r\n"
)


def test_french_daily_parser_reads_yyyymmdd_and_the_missing_code() -> None:
    parsed = REFERENCE_DATASETS["french_three_factor_daily"].parser(_zip(_FRENCH_DAILY))
    assert parsed.missing_count == 1
    assert {(o.series_key, o.observation_date, o.value) for o in parsed.observations if o.series_key != "Mkt-RF"} == {
        ("SMB", date(2014, 8, 29), Decimal("-0.0002")),
        ("HML", date(2014, 8, 29), Decimal("-0.0018")),
        ("RF", date(2014, 8, 29), Decimal("0")),
        ("SMB", date(2014, 9, 2), Decimal("0.0031")),
        ("HML", date(2014, 9, 2), Decimal("-0.0005")),
    }
    assert {o.unit for o in parsed.observations} == {"decimal_return"}


def test_french_daily_parser_refuses_an_impossible_date_and_a_monthly_file() -> None:
    with pytest.raises(ReferenceDataSourceError, match="invalid YYYYMMDD"):
        parse_french_daily_zip(_zip(",RF\n20140231,0.01\n"))
    # A monthly file's YYYYMM stamps are not daily rows: nothing parses, which the validator refuses.
    with pytest.raises(ReferenceDataSourceError, match="zero observations"):
        parse_french_daily_zip(_zip(",RF\n201408,0.01\n"))


_CUTOFFS_HEADER = b"eom,n,nyse_p1,nyse_p20,nyse_p50,nyse_p80\n"


def test_jkp_cutoffs_parser_types_usd_millions_and_counts() -> None:
    parsed = parse_jkp_nyse_cutoffs_csv(
        _CUTOFFS_HEADER + b"2014-08-31,1400,30.5,600.25,2500,15000\n" + b"2014-09-30,1401,31,610,NA,15100\n"
    )
    assert parsed.missing_count == 1
    rows = {(o.series_key, o.observation_date): (o.value, o.unit) for o in parsed.observations}
    assert rows[("n", date(2014, 9, 30))] == (Decimal("1401"), "count")
    assert rows[("nyse_p20", date(2014, 8, 31))] == (Decimal("600.25"), "usd_millions")
    assert ("nyse_p50", date(2014, 9, 30)) not in rows


@pytest.mark.parametrize(
    ("body", "match"),
    [
        (b"2014-08-31,1400,30,600,2500,15000\n2014-10-31,1400,30,600,2500,15000\n", "does not follow"),
        (b"2014-08-30,1400,30,600,2500,15000\n", "not a month end"),
        (b"2014-08-31,1400,30,2600,2500,15000\n", "percentiles decrease"),
        (b"2014-08-31,1400.5,30,600,2500,15000\n", "not an integer count"),
        (b"2014-08-31,1400,0,600,2500,15000\n", "must be positive"),
        (b"2014-08-31,1400,30,600,2500\n", "ragged"),
    ],
)
def test_jkp_cutoffs_parser_refuses_drift(body: bytes, match: str) -> None:
    with pytest.raises(ReferenceDataSourceError, match=match):
        parse_jkp_nyse_cutoffs_csv(_CUTOFFS_HEADER + body)
    with pytest.raises(ReferenceDataSourceError, match="header"):
        parse_jkp_nyse_cutoffs_csv(b"eom,n,nyse_p20\n2014-08-31,1,2\n")


_RETURN_CUTOFFS_HEADER = (
    b"eom,n,ret_0_1,ret_1,ret_99,ret_99_9,ret_local_0_1,ret_local_1,ret_local_99,ret_local_99_9,"
    b"ret_exc_0_1,ret_exc_1,ret_exc_99,ret_exc_99_9\n"
)
# Two rows in JKP's published shape: the excess family is the total family minus the month's one rate (0.003).
_RETURN_CUTOFFS_ROWS = (
    b"2019-10-31,40000,-0.6,-0.3,0.4,1.5,-0.61,-0.3,0.4,1.6,-0.603,-0.303,0.397,1.497\n"
    b"2019-11-30,40001,-0.7,-0.35,0.5,1.8,-0.7,-0.35,NA,1.8,-0.702,-0.352,0.498,1.798\n"
)


def test_jkp_return_cutoffs_parser_keeps_every_family_as_decimal_returns() -> None:
    parsed = parse_jkp_return_cutoffs_csv(_RETURN_CUTOFFS_HEADER + _RETURN_CUTOFFS_ROWS)
    assert parsed.missing_count == 1
    rows = {(o.series_key, o.observation_date): (o.value, o.unit) for o in parsed.observations}
    assert rows[("ret_99_9", date(2019, 11, 30))] == (Decimal("1.8"), "decimal_return")
    assert rows[("ret_exc_0_1", date(2019, 10, 31))] == (Decimal("-0.603"), "decimal_return")
    assert rows[("n", date(2019, 10, 31))] == (Decimal("40000"), "count")
    assert ("ret_local_99", date(2019, 11, 30)) not in rows


@pytest.mark.parametrize(
    ("body", "match"),
    [
        # The excess pair is shifted by 0.003 at the top and 0.004 at the bottom: not one rate.
        (b"2019-10-31,40000,-0.6,-0.3,0.4,1.5,-0.6,-0.3,0.4,1.5,-0.604,-0.303,0.397,1.497\n", "unequal shifts"),
        (b"2019-10-31,40000,NA,-0.3,0.4,1.5,-0.6,-0.3,0.4,1.5,-0.603,-0.303,0.397,1.497\n", "missing ret_0_1"),
        (b"2019-10-31,40000,0.6,-0.3,0.4,1.5,-0.6,-0.3,0.4,1.5,0.597,-0.303,0.397,1.497\n", "ret percentiles decrease"),
        (b"2019-10-30,40000,-0.6,-0.3,0.4,1.5,-0.6,-0.3,0.4,1.5,-0.603,-0.303,0.397,1.497\n", "not a month end"),
        (_RETURN_CUTOFFS_ROWS.replace(b"2019-11-30", b"2019-12-31"), "does not follow"),
        (b"2019-10-31,0.5,-0.6,-0.3,0.4,1.5,-0.6,-0.3,0.4,1.5,-0.603,-0.303,0.397,1.497\n", "positive integer"),
        (b"2019-10-31,NA,-0.6,-0.3,0.4,1.5,-0.6,-0.3,0.4,1.5,-0.603,-0.303,0.397,1.497\n", "missing n"),
        (b"2019-10-31,40000,-0.6,-0.3,0.4,1.5\n", "ragged"),
    ],
)
def test_jkp_return_cutoffs_parser_refuses_drift(body: bytes, match: str) -> None:
    with pytest.raises(ReferenceDataSourceError, match=match):
        parse_jkp_return_cutoffs_csv(_RETURN_CUTOFFS_HEADER + body)
    with pytest.raises(ReferenceDataSourceError, match="header"):
        parse_jkp_return_cutoffs_csv(_CUTOFFS_HEADER + b"2014-08-31,1400,30,600,2500,15000\n")


def test_osap_parser_normalises_percent_to_decimal_as_published() -> None:
    parsed = parse_osap_ls_wide_csv(
        b"date,MaxRet,ShortInterest\n1926-01-30,NA,NA\n1980-02-29,1.25,NA\n1980-03-31,-0.5,2\n"
    )
    assert parsed.missing_count == 3
    assert [(o.series_key, o.observation_date, o.value, o.unit) for o in parsed.observations] == [
        ("MaxRet", date(1980, 2, 29), Decimal("0.0125"), "decimal_return"),
        ("MaxRet", date(1980, 3, 31), Decimal("-0.005"), "decimal_return"),
        ("ShortInterest", date(1980, 3, 31), Decimal("0.02"), "decimal_return"),
    ]


@pytest.mark.parametrize(
    ("payload", "match"),
    [
        (
            b"<!DOCTYPE html><html><head><title>Google Drive - Quota exceeded</title></head></html>",
            "HTML page \\('Google Drive - Quota exceeded'\\)",
        ),
        (b"yyyymm,MaxRet\n198002,1\n", "expected 'date'"),
        (b"date,MaxRet,MaxRet\n1980-02-29,1,1\n", "duplicated"),
        (b"date,MaxRet\n1980-02-29\n", "ragged"),
        (b"date,MaxRet\n1980-02,1\n", "invalid date"),
        (b"date,MaxRet\n1980-03-31,1\n1980-03-01,1\n", "does not follow"),
        (b"date,MaxRet\n1980-02-29,x\n", "not decimal"),
    ],
)
def test_osap_parser_refuses_a_refusal_page_or_a_changed_shape(payload: bytes, match: str) -> None:
    with pytest.raises(ReferenceDataSourceError, match=match):
        parse_osap_ls_wide_csv(payload)


def test_osap_resolver_follows_the_data_page_link_to_a_direct_download() -> None:
    link = (
        '<a href="https://drive.google.com/file/d/{id}/view?usp=drive_link" target="_blank" '
        'rel="noreferrer noopener">Monthly long-short returns of {n} predictors following OPs (wide csv)</a>'
    )

    def serving(*links: str) -> httpx.MockTransport:
        def handler(request: httpx.Request) -> httpx.Response:
            assert str(request.url) == OSAP_DATA_PAGE_URL
            assert request.headers["User-Agent"] == OSAP_USER_AGENT
            return httpx.Response(200, text="<li>" + "</li><li>".join(links) + "</li>", request=request)

        return httpx.MockTransport(handler)

    with httpx.Client(transport=serving(link.format(id="10sOryk_dd-jk", n=212))) as client:
        assert (
            resolve_osap_ls_wide_url(client, OSAP_DATA_PAGE_URL)
            == "https://drive.usercontent.google.com/download?id=10sOryk_dd-jk&export=download&confirm=t"
        )
    for links in ((), (link.format(id="a", n=212), link.format(id="b", n=213))):
        with httpx.Client(transport=serving(*links)) as client:
            with pytest.raises(ReferenceDataSourceError, match="expected exactly one"):
                resolve_osap_ls_wide_url(client, OSAP_DATA_PAGE_URL)
