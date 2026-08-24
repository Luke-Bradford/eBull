from __future__ import annotations

from datetime import date

from scripts.census_2900_price_metadata import build_report


def test_price_metadata_report_reconciles_without_price_values(tmp_path) -> None:
    report = build_report(
        [
            {
                "bar_count": 10,
                "delisting_date": date(2024, 1, 1),
                "delisting_filed_date": date(2024, 1, 2),
                "delisting_provision": "(b)",
                "delisting_source": "sec_form25",
                "first_bar": date(2020, 1, 1),
                "instrument_id": None,
                "last_bar": date(2024, 1, 1),
                "series_id": 1,
                "vendor_symbol": "DEAD",
            },
            {
                "bar_count": 20,
                "delisting_date": None,
                "delisting_filed_date": None,
                "delisting_provision": None,
                "delisting_source": None,
                "first_bar": date(2020, 1, 1),
                "instrument_id": 2,
                "last_bar": date(2024, 9, 27),
                "series_id": 2,
                "vendor_symbol": "LIVE",
            },
        ],
        mirror_root=tmp_path,
        mirror_commit="abc",
    )

    assert report["counts"] == {
        "delisting_source_none": 1,
        "delisting_source_sec_form25": 1,
        "linked": 1,
        "series": 2,
        "unlinked": 1,
        "with_bars": 2,
    }
    assert all("close" not in record and "return" not in record for record in report["records"])
