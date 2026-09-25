"""#3384 slice 2b — pure logic of the market-level sources probe (no DB, no network)."""

from __future__ import annotations

import io
import zipfile
from datetime import date
from pathlib import Path

import pytest

from scripts.probe_3384_market_sources import (
    CBOE_LAUNCH,
    COT_EMINI_SP,
    _cot_dates,
    bisect_first_vintage,
    cadence,
    count_revisions,
    parse_cboe_index_csv,
    parse_fred_csv,
    parse_putcall_archive,
)


def test_parse_cboe_ohlc_and_single_value_csv() -> None:
    ohlc = parse_cboe_index_csv(
        "DATE,OPEN,HIGH,LOW,CLOSE\n01/02/1990,17.24,17.24,17.24,17.24\n01/03/1990,18,18,18,18\n"
    )
    assert ohlc[date(1990, 1, 2)] == ("17.24", "17.24", "17.24", "17.24") and len(ohlc) == 2
    skew = parse_cboe_index_csv("DATE,SKEW\n01/02/1990,126.09\n")
    assert skew == {date(1990, 1, 2): ("126.09",)}
    with pytest.raises(ValueError, match="unexpected Cboe header"):
        parse_cboe_index_csv("Date of trade,X\n")


def test_putcall_archive_keeps_preamble_and_parses_rows() -> None:
    text = (
        "Volume and Put/Call Ratio data is compiled for the convenience,,,,\n"
        ",PRODUCT: TOTAL,,EXCHANGE: Cboe,\n"
        "DATE,CALLS,PUTS,TOTAL,P/C Ratio\n"
        "11/1/2006,1401036,1271445,2672481,0.91\n"
        "10/04/2019, 2175006, 2289715, 4464721, 1.05\n"
    )
    notes, days = parse_putcall_archive(text)
    assert notes == ["Volume and Put/Call Ratio data is compiled for the convenience", "PRODUCT: TOTAL,,EXCHANGE: Cboe"]
    assert days == [date(2006, 11, 1), date(2019, 10, 4)]


def test_cadence_gaps_weekdays_and_duplicates() -> None:
    c = cadence([date(2024, 1, 2), date(2024, 1, 9), date(2024, 1, 9), date(2024, 1, 23)])
    assert c["n"] == 3 and c["gap_days"] == {7: 1, 14: 1} and c["weekdays"] == {"Tue": 3}
    assert c["per_year"] == {"2024": 3} and (c["first"], c["last"]) == ("2024-01-02", "2024-01-23")
    assert cadence([])["first"] is None


def test_bisect_first_vintage() -> None:
    first = date(1960, 3, 4)
    calls: list[date] = []

    def exists(d: date) -> bool:
        calls.append(d)
        return d >= first

    assert bisect_first_vintage(exists, date(1940, 1, 1), date(2026, 9, 25)) == first
    assert len(calls) < 25
    assert bisect_first_vintage(lambda d: False, date(1940, 1, 1), date(2026, 9, 25)) is None
    assert bisect_first_vintage(lambda d: True, date(1940, 1, 1), date(2026, 9, 25)) == date(1940, 1, 1)


def test_fred_csv_and_revision_count() -> None:
    old = parse_fred_csv("observation_date,UNRATE_20150601\n2014-11-01,5.8\n2014-12-01,5.6\n2015-01-01,5.7\n")
    new = parse_fred_csv("observation_date,UNRATE\n2014-11-01,5.8\n2014-12-01,5.60\n2015-01-01,5.8\n")
    # 5.6 vs 5.60 is the same number; 2015-01 is past the cut.
    assert count_revisions(old, new, date(2014, 12, 31)) == {"common_observations": 2, "changed": 0, "only_in_old": 0}
    assert count_revisions(old, new, date(2015, 1, 31))["changed"] == 1
    missing = parse_fred_csv("observation_date,X\n2014-11-01,.\n")
    assert count_revisions(missing, {date(2014, 11, 1): "1.0"}, date(2014, 12, 31))["changed"] == 1


def test_cboe_launch_rows_are_post_2003_and_cite_a_cboe_document() -> None:
    for launch in CBOE_LAUNCH.values():
        assert launch.published_from.year >= 2003 and launch.source.startswith("https://cdn.cboe.com/")


def test_cot_dates_reads_both_column_conventions(tmp_path: Path) -> None:
    def zipped(name: str, body: str) -> Path:
        path = tmp_path / f"{name}.zip"
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as z:
            z.writestr(f"{name}.txt", body)
        path.write_bytes(buf.getvalue())
        return path

    legacy = zipped(
        "legacy",
        '"Market and Exchange Names","As of Date in Form YYMMDD","As of Date in Form YYYY-MM-DD",'
        '"CFTC Contract Market Code"\n'
        f'"E-MINI S&P 500",240102,2024-01-02,{COT_EMINI_SP}\n'
        '"WHEAT",240109,2024-01-09,001602\n',
    )
    every, emini, column = _cot_dates(legacy)
    assert every == {date(2024, 1, 2), date(2024, 1, 9)} and emini == {date(2024, 1, 2)}
    assert column == "As of Date in Form YYYY-MM-DD"
    tff = zipped(
        "tff",
        "Market_and_Exchange_Names,Report_Date_as_YYYY-MM-DD,CFTC_Contract_Market_Code\n"
        f"E-MINI S&P 500,2024-01-02 00:00:00,{COT_EMINI_SP}\n"
        # fin_fut_txt_2006_2016 writes the ISO-named column as MM/DD/YYYY.
        f"E-MINI S&P 500,12/27/2016,{COT_EMINI_SP}\n"
        f"E-MINI S&P 500,1/3/2012 12:00:00 AM,{COT_EMINI_SP}\n",
    )
    days = {date(2024, 1, 2), date(2016, 12, 27), date(2012, 1, 3)}
    assert _cot_dates(tff)[:2] == (days, days)
