from __future__ import annotations

import io
import zipfile
from datetime import date
from decimal import Decimal
from typing import Any

import httpx
import psycopg
import pytest
from openpyxl import Workbook

from app.services.reference_data import (
    AQR_PARSER_VERSION,
    REFERENCE_DATASETS,
    ReferenceDatasetSpec,
    ReferenceDataSourceError,
    parse_aqr_vme_monthly,
    parse_fred_csv,
    parse_french_monthly_zip,
    refresh_reference_dataset,
)


def _zip_csv(text: str) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("factor.csv", text)
    return output.getvalue()


def _aqr_xlsx(*rows: tuple[Any, ...]) -> bytes:
    workbook = Workbook()
    sheet = workbook.active
    assert sheet is not None
    sheet.title = "VME Factors"
    for row in rows:
        sheet.append(row)
    output = io.BytesIO()
    workbook.save(output)
    workbook.close()
    return output.getvalue()


def test_french_parser_normalizes_percent_and_stops_before_annual_section() -> None:
    parsed = parse_french_monthly_zip(
        _zip_csv(
            "created from test data\n"
            ",Mom,RF\n"
            "202401, 2.50, 0.40\n"
            "202402,-99.99, 0.41\n"
            "\n"
            " Annual Factors: January-December \n"
            ",Mom,RF\n"
            "2024,99,99\n"
        )
    )
    assert parsed.missing_count == 1
    assert [(item.series_key, item.observation_date, item.value) for item in parsed.observations] == [
        ("Mom", date(2024, 1, 31), Decimal("0.025")),
        ("RF", date(2024, 1, 31), Decimal("0.004")),
        ("RF", date(2024, 2, 29), Decimal("0.0041")),
    ]


def test_french_parser_rejects_duplicate_month() -> None:
    payload = _zip_csv(",Mom\n202401,1\n202401,2\n")
    with pytest.raises(ReferenceDataSourceError, match="duplicate observation"):
        parse_french_monthly_zip(payload)


def test_french_production_specs_reject_a_swapped_or_drifted_header() -> None:
    payload = _zip_csv("This file was created by test\n,Mom\n202401,1.25\n\n Annual Factors: January-December\n")
    five_factor = REFERENCE_DATASETS["french_five_factor_monthly"]
    momentum = REFERENCE_DATASETS["french_momentum_monthly"]

    with pytest.raises(ReferenceDataSourceError, match="expected"):
        five_factor.parser(payload)
    assert momentum.parser(payload).observations[0].series_key == "Mom"


def test_aqr_parser_requires_named_sheet_and_exact_header() -> None:
    header = (
        "DATE",
        "VAL",
        "MOM",
        "VAL^SS",
        "MOM^SS",
        "VAL^AA",
        "MOM^AA",
        "VALLS_VME_US90",
        "MOMLS_VME_US90",
        "VALLS_VME_UK90",
        "MOMLS_VME_UK90",
        "VALLS_VME_ROE90",
        "MOMLS_VME_ROE90",
        "VALLS_VME_JP90",
        "MOMLS_VME_JP90",
        "VALLS_VME_EQ",
        "MOMLS_VME_EQ",
        "VALLS_VME_FX",
        "MOMLS_VME_FX",
        "VALLS_VME_FI",
        "MOMLS_VME_FI",
        "VALLS_VME_COM",
        "MOMLS_VME_COM",
    )
    row = ("01/31/2024", Decimal("0.01"), Decimal("-0.02"), *([None] * 20))
    parsed = parse_aqr_vme_monthly(_aqr_xlsx(("intro",), header, row, tuple("" for _ in header)))
    assert parsed.missing_count == 20
    assert [(item.series_key, item.value) for item in parsed.observations] == [
        ("MOM", Decimal("-0.02")),
        ("VAL", Decimal("0.01")),
    ]

    with pytest.raises(ReferenceDataSourceError, match="exact factor header"):
        parse_aqr_vme_monthly(_aqr_xlsx(("DATE", "MOM"), ("01/31/2024", 0.1)))


def test_fred_parser_preserves_blank_as_missing_and_checks_binary_unit() -> None:
    parsed = parse_fred_csv(
        b"observation_date,USREC\n2024-01-01,0\n2024-02-01,\n2024-03-01,1\n",
        series_key="USREC",
        unit="binary_indicator",
    )
    assert parsed.missing_count == 1
    assert [item.value for item in parsed.observations] == [Decimal(0), Decimal(1)]

    with pytest.raises(ReferenceDataSourceError, match="expected binary"):
        parse_fred_csv(
            b"observation_date,USREC\n2024-01-01,2\n",
            series_key="USREC",
            unit="binary_indicator",
        )


@pytest.mark.integration
def test_refresh_retains_rejected_raw_and_deduplicates_accepted_response(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    accepted_payload = _aqr_xlsx(
        (
            "DATE",
            "VAL",
            "MOM",
            "VAL^SS",
            "MOM^SS",
            "VAL^AA",
            "MOM^AA",
            "VALLS_VME_US90",
            "MOMLS_VME_US90",
            "VALLS_VME_UK90",
            "MOMLS_VME_UK90",
            "VALLS_VME_ROE90",
            "MOMLS_VME_ROE90",
            "VALLS_VME_JP90",
            "MOMLS_VME_JP90",
            "VALLS_VME_EQ",
            "MOMLS_VME_EQ",
            "VALLS_VME_FX",
            "MOMLS_VME_FX",
            "VALLS_VME_FI",
            "MOMLS_VME_FI",
            "VALLS_VME_COM",
            "MOMLS_VME_COM",
        ),
        ("01/31/2024", 0.01, -0.02, *([None] * 20)),
    )
    responses = [b"not-an-xlsx", accepted_payload, accepted_payload]

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=responses.pop(0), request=request)

    spec_rejected = ReferenceDatasetSpec(
        "aqr", "test_rejected", "https://example.test/rejected.xlsx", AQR_PARSER_VERSION, parse_aqr_vme_monthly
    )
    spec_accepted = ReferenceDatasetSpec(
        "aqr", "test_accepted", "https://example.test/accepted.xlsx", AQR_PARSER_VERSION, parse_aqr_vme_monthly
    )
    ebull_test_conn.autocommit = True
    try:
        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            with pytest.raises(ReferenceDataSourceError, match="readable XLSX"):
                refresh_reference_dataset(ebull_test_conn, client=client, spec=spec_rejected)
            first = refresh_reference_dataset(ebull_test_conn, client=client, spec=spec_accepted)
            second = refresh_reference_dataset(ebull_test_conn, client=client, spec=spec_accepted)

        assert first.status == "accepted"
        assert first.row_count == 2
        assert second.status == "unchanged"
        assert second.snapshot_id == first.snapshot_id
        assert ebull_test_conn.execute(
            "SELECT parse_status, payload FROM reference_data_snapshots WHERE dataset_key = 'test_rejected'"
        ).fetchone() == ("rejected", b"not-an-xlsx")
        assert ebull_test_conn.execute(
            "SELECT count(*) FROM reference_data_snapshots WHERE dataset_key = 'test_accepted'"
        ).fetchone() == (1,)
        assert ebull_test_conn.execute(
            "SELECT series_key, value, unit FROM reference_data_observations "
            "WHERE snapshot_id = %s ORDER BY series_key",
            (first.snapshot_id,),
        ).fetchall() == [
            ("MOM", Decimal("-0.02"), "decimal_return"),
            ("VAL", Decimal("0.01"), "decimal_return"),
        ]
    finally:
        ebull_test_conn.execute(
            "DELETE FROM reference_data_observations WHERE snapshot_id IN "
            "(SELECT snapshot_id FROM reference_data_snapshots WHERE dataset_key LIKE 'test_%%')"
        )
        ebull_test_conn.execute("DELETE FROM reference_data_snapshots WHERE dataset_key LIKE 'test_%%'")
        ebull_test_conn.autocommit = False
        ebull_test_conn.commit()
