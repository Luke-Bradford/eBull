"""#3630 — the Official List's publication lag after quarter end is not a failure.

Pure tests: ``fetch_latest_published_list`` takes the fetcher as an argument.
"""

from __future__ import annotations

import urllib.error
from collections.abc import Callable
from datetime import date
from email.message import Message

import pytest

from app.services.sec_13f_securities_list import (
    LIST_PUBLICATION_GRACE_DAYS,
    fetch_latest_published_list,
)


def _http_error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError("test://13flist", code, "err", Message(), None)


def _fetcher(published: set[tuple[int, int]], calls: list[tuple[int, int]]) -> Callable[[int, int], tuple[str, str]]:
    def fetch(year: int, quarter: int) -> tuple[str, str]:
        calls.append((year, quarter))
        if (year, quarter) not in published:
            raise _http_error(404)
        return f"list {year}q{quarter}", f"test://13flist{year}q{quarter}"

    return fetch


def test_target_quarter_published_is_used() -> None:
    calls: list[tuple[int, int]] = []
    result = fetch_latest_published_list(date(2026, 10, 10), _fetcher({(2026, 3), (2026, 2)}, calls))
    assert result == (2026, 3, "list 2026q3", "test://13flist2026q3", None)
    assert calls == [(2026, 3)]


def test_unpublished_target_inside_grace_uses_prior_quarter() -> None:
    calls: list[tuple[int, int]] = []
    result = fetch_latest_published_list(date(2026, 10, 4), _fetcher({(2026, 2)}, calls))
    assert result == (2026, 2, "list 2026q2", "test://13flist2026q2", (2026, 3))
    assert calls == [(2026, 3), (2026, 2)]


def test_q1_fallback_crosses_the_year() -> None:
    calls: list[tuple[int, int]] = []
    result = fetch_latest_published_list(date(2027, 1, 5), _fetcher({(2026, 3)}, calls))
    assert (result.year, result.quarter) == (2026, 3)
    assert result.unpublished_quarter == (2026, 4)


def test_grace_bound_is_inclusive_then_raises() -> None:
    # 2026q3 ends 2026-09-30; day 45 still falls back, day 46 raises.
    last_grace_day = date(2026, 11, 14)
    assert (last_grace_day - date(2026, 9, 30)).days == LIST_PUBLICATION_GRACE_DAYS
    assert fetch_latest_published_list(last_grace_day, _fetcher({(2026, 2)}, [])).unpublished_quarter == (2026, 3)
    with pytest.raises(urllib.error.HTTPError):
        fetch_latest_published_list(date(2026, 11, 15), _fetcher({(2026, 2)}, []))


def test_non_404_is_raised_without_fallback() -> None:
    calls: list[tuple[int, int]] = []

    def fetch(year: int, quarter: int) -> tuple[str, str]:
        calls.append((year, quarter))
        raise _http_error(503)

    with pytest.raises(urllib.error.HTTPError) as exc:
        fetch_latest_published_list(date(2026, 10, 4), fetch)
    assert exc.value.code == 503
    assert calls == [(2026, 3)]


def test_prior_quarter_missing_too_raises() -> None:
    with pytest.raises(urllib.error.HTTPError):
        fetch_latest_published_list(date(2026, 10, 4), _fetcher(set(), []))
