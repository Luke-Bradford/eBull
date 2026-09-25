"""#3384 — pure logic of the hunt admissibility census (no DB)."""

from __future__ import annotations

from datetime import date

import pytest

from scripts.census_3384_hunt_admissibility import (
    ALIVE,
    ALIVE_UNADMITTED,
    EXCLUDED_TEST_ISSUE,
    HUNT_WINDOWS,
    _overlaps,
    _stratum,
    _terciles,
    _window_of,
)

FLOOR = date(2024, 9, 20)


def _row(series_id: int, symbol: str, last_bar: date, source: str | None = None, provision: str | None = None):
    return (series_id, symbol, date(2000, 1, 3), last_bar, None, None, source, provision)


@pytest.mark.parametrize(
    ("row", "admitted", "expected"),
    [
        (_row(1, "AAPL", date(2024, 9, 27)), {1}, ALIVE),
        (_row(2, "XYZ", date(2024, 9, 27)), set(), ALIVE_UNADMITTED),
        (_row(3, "ZVZZT", date(2014, 1, 6)), set(), EXCLUDED_TEST_ISSUE),
        (_row(8, "ZVZZT", date(2024, 9, 27)), set(), EXCLUDED_TEST_ISSUE),
        (_row(4, "OLD", date(2010, 1, 4), "sec_form25", "(b)"), {4}, "exchange_failure"),
        (_row(5, "MRG", date(2015, 6, 1), "sec_form25", "(a)(3)"), {5}, "operation_of_law"),
        (_row(6, "BKRPQ", date(2012, 3, 1)), {6}, "q_suffix_otc_unverified"),
        (_row(7, "GONE", date(2012, 3, 1)), {7}, "unknown_termination"),
    ],
)
def test_stratum(row, admitted, expected) -> None:
    assert _stratum(row, admitted, FLOOR) == expected


def test_unadmitted_terminating_non_test_issue_is_refused() -> None:
    with pytest.raises(RuntimeError, match="neither admitted nor a test issue"):
        _stratum(_row(9, "GONE", date(2015, 1, 2)), set(), FLOOR)


def test_windows_are_contiguous_and_2021_straddles_validation_and_holdout() -> None:
    for (_, _, end), (_, start, _) in zip(HUNT_WINDOWS, HUNT_WINDOWS[1:], strict=False):
        assert (start - end).days == 1
    assert _window_of(2021) == ["validation", "holdout"]
    assert _window_of(1990) == ["discovery"]
    assert _window_of(1989) == ["pre_discovery"]


def test_terciles_split_sorted_values() -> None:
    assert _terciles([float(v) for v in range(9)]) == (3.0, 6.0)


def test_overlap_is_by_date_not_calendar_year() -> None:
    validation = next((s, e) for n, s, e in HUNT_WINDOWS if n == "validation")
    # A holdout-only row starting 2021-09-28 shares a calendar year with validation but no date.
    assert not _overlaps(date(2021, 9, 28), date(2024, 9, 27), *validation)
    assert _overlaps(date(1962, 1, 2), date(2021, 6, 28), *validation)
