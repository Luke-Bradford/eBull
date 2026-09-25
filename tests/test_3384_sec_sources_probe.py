"""#3384 slice 2a — pure logic of the SEC sources probe (no DB, no network)."""

from __future__ import annotations

from datetime import date

from scripts.probe_3384_sec_sources import (
    SEC,
    _parse_day,
    _quantiles,
    parse_ftd_href,
    parse_midas_href,
    unify_symbol,
)


def test_ftd_half_month_periods() -> None:
    a = parse_ftd_href("/files/data/fails-deliver-data/cnsfails202402a.zip")
    b = parse_ftd_href("/files/data/fails-deliver-data/cnsfails202402b.zip")
    assert a is not None and (a.period_start, a.period_end, a.label) == (date(2024, 2, 1), date(2024, 2, 15), "202402a")
    assert b is not None and (b.period_start, b.period_end) == (date(2024, 2, 16), date(2024, 2, 29))
    assert a.url == SEC + "/files/data/fails-deliver-data/cnsfails202402a.zip"


def test_ftd_repost_suffix_and_december_b() -> None:
    f = parse_ftd_href("/files/data/other/fails-deliver-data/cnsfails202312b_0.zip")
    assert f is not None and (f.period_end, f.label) == (date(2023, 12, 31), "202312b")


def test_ftd_quarterly_foia_file() -> None:
    f = parse_ftd_href("/files/data/x/cnsp_sec_fails_2006q4.zip")
    assert f is not None and (f.period_start, f.period_end, f.label) == (
        date(2006, 10, 1),
        date(2006, 12, 31),
        "2006q4",
    )


def test_midas_irregular_quarter_suffix() -> None:
    f = parse_midas_href("/files/opa/x/individual_security_exchange_2012_q20.zip")
    assert f is not None and (f.period_start, f.period_end, f.label) == (date(2012, 4, 1), date(2012, 6, 30), "2012q2")
    assert parse_midas_href("/files/ocoo01-guidance.pdf") is None


def test_unify_symbol_separators() -> None:
    assert {unify_symbol(s) for s in ("BRK.B", "brk-b", "BRK/B", " BRK_B ")} == {"BRK.B"}


def test_parse_day_formats() -> None:
    assert _parse_day("20240103") == _parse_day("2024-01-03") == _parse_day("01/03/2024") == date(2024, 1, 3)


def test_quantiles() -> None:
    assert _quantiles([]) == {"n": 0}
    assert _quantiles(list(range(10))) == {"n": 10, "min": 0, "p50": 5, "p90": 9, "max": 9}
