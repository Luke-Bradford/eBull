"""#3740 condition 1: window validity, the 24-month floor, the mean's sign, independence and family roll-up."""

from __future__ import annotations

import io
import zipfile
from datetime import date
from typing import Literal

import pytest

from scripts.report_3740_condition1 import Member, calendar_months, evaluate, evaluate_window, read_series


def _rows(start: tuple[int, int], end: tuple[int, int], value: str = "0.01") -> list[tuple[date, str]]:
    return [(date(y, m, 28), value) for y, m in calendar_months(start, end)]


def _member(name: str, family: str, *, developed: Literal["no", "yes", "undetermined"] = "no") -> Member:
    return Member(name, family, "Author (2020)", 2020, "1963-2010", developed, "read from the paper")


def test_window_runs_from_the_january_after_publication_to_2024_08() -> None:
    inside = (date(2021, 1, 1), date(2024, 8, 31))
    rows = [(day, "0.01" if inside[0] <= day <= inside[1] else "-0.5") for day, _ in _rows((2019, 1), (2025, 12))]
    result = evaluate_window(rows, region="usa", start=(2021, 1), counting=True)
    assert (result.start, result.end, result.months, result.passes) == ("2021-01", "2024-08", 44, True)
    assert result.mean is not None and abs(result.mean - 0.01) < 1e-12


def test_a_gap_a_duplicate_or_a_non_finite_month_fails_the_window() -> None:
    full = _rows((2020, 1), (2024, 8))
    gapped = [row for row in full if row[0] != date(2022, 6, 28)]
    assert evaluate_window(gapped, region="usa", start=(2020, 1), counting=True).failure == "2022-06 is missing"
    duplicated = [*full, (date(2022, 6, 30), "0.02")]
    assert evaluate_window(duplicated, region="usa", start=(2020, 1), counting=True).failure == "2022-06 is duplicated"
    for raw in ("NA", "", "inf"):
        bad = [(day, raw if day == date(2023, 3, 28) else value) for day, value in full]
        failure = evaluate_window(bad, region="usa", start=(2020, 1), counting=True).failure
        assert failure is not None and failure.startswith("2023-03 is not finite")


def test_series_ending_before_2024_08_fails() -> None:
    result = evaluate_window(_rows((2020, 1), (2024, 7)), region="usa", start=(2020, 1), counting=True)
    assert result.failure == "2024-08 is missing"


def test_fewer_than_24_months_or_a_non_positive_mean_fails() -> None:
    short = evaluate_window(_rows((2022, 10), (2024, 8)), region="usa", start=(2022, 10), counting=True)
    assert (short.months, short.failure) == (23, "23 months, fewer than 24")
    flat = evaluate_window(_rows((2020, 1), (2024, 8), "0"), region="usa", start=(2020, 1), counting=True)
    assert flat.failure == "the mean monthly return is not positive"
    assert evaluate_window([], region="usa", start=(2025, 1), counting=True).failure == "the window is empty"


def test_a_non_counting_developed_window_cannot_decide_and_families_need_every_member() -> None:
    good = _rows((1986, 1), (2024, 8))
    bad = _rows((1986, 1), (2024, 8), "-0.01")
    members = [
        _member("a", "value", developed="yes"),  # its paper used developed ex-US data: (i) alone decides
        _member("b", "value"),
        _member("c", "investment"),
        _member("d", "investment", developed="undetermined"),  # not shown independent: (i) alone decides
    ]
    series = {
        "usa": {"a": good, "b": good, "c": good, "d": good},
        "developed": {"a": bad, "b": good, "c": bad, "d": bad},
    }
    result = evaluate(series, members)
    assert [row["supported"] for row in result["members"]] == [True, True, False, True]
    assert result["families"] == {"value": True, "investment": False}
    first = result["members"][0]["windows"][1]
    assert (first["region"], first["start"], first["counting"], first["passes"]) == (
        "developed",
        "1986-01",
        False,
        False,
    )


def test_a_member_absent_from_a_counting_file_is_unsupported() -> None:
    result = evaluate({"usa": {}, "developed": {}}, [_member("a", "value")])
    assert result["members"][0]["supported"] is False
    assert result["families"] == {"value": False}


def _zip(text: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("f.csv", text)
    return buffer.getvalue()


def test_read_series_keeps_na_rows_and_refuses_a_header_without_a_read_column() -> None:
    header = "location,name,freq,weighting,direction,n_countries,date,ret\n"
    rows = read_series(_zip(header + "developed,be_me,monthly,vw_cap,1,4,1986-01-31,NA\n"), "developed")
    assert rows == {"be_me": [(date(1986, 1, 31), "NA")]}
    with pytest.raises(ValueError, match="lacks a column"):
        read_series(_zip("location,name,freq,weighting,direction,n_countries,date,value\n"), "developed")
    for ragged in (
        "developed,be_me,monthly,vw_cap,1,4,1986-01-31\n",
        "developed,be_me,monthly,vw_cap,1,4,1986-01-31,0.1,x\n",
    ):
        with pytest.raises(ValueError, match="ragged row at line 2"):
            read_series(_zip(header + ragged), "developed")
    with pytest.raises(ValueError, match="in the developed file"):
        read_series(_zip(header + "world,be_me,monthly,vw_cap,1,52,1986-01-31,0.1\n"), "developed")
