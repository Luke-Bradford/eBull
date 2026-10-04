"""#3619 slice 2 — the splice contract's pure rules (spec 2026-10-04-3619-total-return-splice.md)."""

from __future__ import annotations

from datetime import date

import pytest

from app.services.total_return_reader import (
    IDENTITY_MIN_MONTHS,
    INTRADER_VENDOR,
    PWB_VENDOR,
    MonthEnd,
    SpliceVerdict,
    add_months,
    identity_check,
    monthly_returns,
    splice_name,
)

Month = tuple[int, int]


def _ends(start: Month, closes: list[float], *, adj: list[float] | None = None) -> dict[Month, MonthEnd]:
    """One month-end per month from ``start``; the bar sits on the 28th so every month has it."""
    out: dict[Month, MonthEnd] = {}
    for i, close in enumerate(closes):
        month = add_months(start, i)
        out[month] = MonthEnd(date(month[0], month[1], 28), (adj or closes)[i], close)
    return out


def _path(n: int, *, step: float = 0.01, start: float = 100.0) -> list[float]:
    """Alternating up/down closes so every month has a distinct non-zero return."""
    level, out = start, []
    for i in range(n):
        level *= 1 + (step if i % 2 == 0 else -step / 2)
        out.append(level)
    return out


def test_returns_need_the_previous_month_in_the_same_series() -> None:
    ends = _ends((2024, 1), [100.0, 110.0, 99.0])
    del ends[(2024, 2)]
    assert monthly_returns(ends, field="close", through=(2024, 12)) == {}


def test_returns_stop_at_the_through_month_and_carry_their_bars() -> None:
    returns = monthly_returns(_ends((2024, 7), [100.0, 110.0, 99.0]), field="close", through=(2024, 8))
    assert list(returns) == [(2024, 8)]
    assert returns[(2024, 8)].value == pytest.approx(0.10)
    assert (returns[(2024, 8)].start_bar, returns[(2024, 8)].end_bar) == (date(2024, 7, 28), date(2024, 8, 28))


def test_identity_passes_on_the_same_price_path_despite_missing_dividends() -> None:
    # 2021-07..2024-06; Intrader's adj_close stops carrying dividends, PWB's carries them.
    closes = _path(36)
    intrader = _ends((2021, 7), closes)
    pwb = _ends((2021, 7), closes, adj=[c * 0.97 ** (i // 3) for i, c in enumerate(closes)])
    check = identity_check(intrader, pwb, [])
    assert check.passed
    assert check.median_abs_diff == pytest.approx(0.0)
    assert check.paired_months == 30  # 2022-01 .. 2024-06: earlier months decide nothing


def test_identity_refuses_a_different_security() -> None:
    intrader = _ends((2021, 1), _path(48, step=0.04))
    pwb = _ends((2021, 1), _path(48, step=0.01))
    check = identity_check(intrader, pwb, [])
    assert not check.passed
    assert check.median_abs_diff is not None and check.median_abs_diff > 0.0005


def test_identity_ignores_pre_switch_disagreement() -> None:
    closes = _path(48)
    intrader = _ends((2019, 1), [c * (1.5 if i < 30 else 1) for i, c in enumerate(closes)])  # differs to 2021-06
    assert identity_check(intrader, _ends((2019, 1), closes), []).passed


def test_identity_needs_the_minimum_overlap() -> None:
    closes = _path(IDENTITY_MIN_MONTHS)  # n levels give n - 1 returns
    check = identity_check(_ends((2023, 1), closes), _ends((2023, 1), closes), [])
    assert not check.passed
    assert check.median_abs_diff is None
    assert check.paired_months == IDENTITY_MIN_MONTHS - 1


def test_identity_skips_the_return_interval_holding_a_split_stamp_not_its_calendar_month() -> None:
    closes = _path(30)  # 2022-01 .. 2024-06, month-end bars on the 28th
    pwb = _ends((2022, 1), closes)
    del pwb[(2023, 4)]  # PWB has no April or May 2023 return: 27 paired months
    intrader = _ends((2022, 1), closes)
    assert identity_check(intrader, pwb, []).paired_months == 27
    # Stamped 2023-03-29: calendar March, but after March's month-end bar, so inside April's
    # interval (03-28, 04-28] — already unpaired. Attributing it to March would drop a month.
    assert identity_check(intrader, pwb, [date(2023, 3, 29)]).paired_months == 27
    # Stamped on March's own month-end bar: inside (02-28, 03-28], March is skipped.
    assert identity_check(intrader, pwb, [date(2023, 3, 28)]).paired_months == 26


def test_identity_compares_only_returns_over_the_same_bars() -> None:
    closes = _path(30)
    pwb = _ends((2022, 1), closes)
    pwb[(2023, 3)] = MonthEnd(date(2023, 3, 31), closes[14], closes[14])  # a later month-end bar
    check = identity_check(_ends((2022, 1), closes), pwb, [])
    assert check.paired_months == 27  # March and April run between different bars


def test_identity_ignores_months_after_intrader_survivorship_ends() -> None:
    closes = _path(40)
    pwb_closes = closes[:24] + [c * 3 for c in closes[24:]]  # diverges only from 2024-09 onward
    intrader = _ends((2022, 9), closes[:25])  # 2022-09 .. 2024-09
    pwb = _ends((2022, 9), pwb_closes)
    check = identity_check(intrader, pwb, [])
    assert check.passed
    assert check.paired_months == 23  # 2022-10 .. 2024-08


def _splice(**overrides: object):  # type: ignore[no-untyped-def]
    closes = _path(60)
    args: dict[str, object] = {
        "name_key": 7,
        "intrader_series_id": 1,
        "intrader": _ends((2020, 1), closes[:57]),  # 2020-01 .. 2024-09
        "intrader_split_dates": [],
        "terminating": False,
        "pwb_series_id": 2,
        "pwb": _ends((2020, 1), closes),  # 2020-01 .. 2024-12
    }
    args.update(overrides)
    return splice_name(**args)  # type: ignore[arg-type]


def test_a_spliced_name_switches_vendor_at_2022_and_flags_survivor_only_after_2024_08() -> None:
    result = _splice()
    assert result.verdict is SpliceVerdict.SPLICED
    by_month = {(r.month.year, r.month.month): r for r in result.rows}
    assert by_month[(2021, 12)].vendor == INTRADER_VENDOR
    assert not by_month[(2021, 12)].dividend_capture_degraded
    assert by_month[(2022, 1)].vendor == PWB_VENDOR
    assert by_month[(2024, 8)].vendor == PWB_VENDOR and not by_month[(2024, 8)].survivor_only
    assert by_month[(2024, 9)].vendor == PWB_VENDOR and by_month[(2024, 9)].survivor_only
    assert max(by_month) == (2024, 12)
    assert len(by_month) == len(result.rows)  # exactly one vendor per month


def test_a_spliced_name_falls_back_to_degraded_intrader_where_pwb_has_a_gap() -> None:
    pwb = _ends((2020, 1), _path(60))
    del pwb[(2023, 5)]  # breaks PWB's 2023-05 and 2023-06 returns
    by_month = {(r.month.year, r.month.month): r for r in _splice(pwb=pwb).rows}
    assert by_month[(2023, 5)].vendor == INTRADER_VENDOR and by_month[(2023, 5)].dividend_capture_degraded
    assert by_month[(2023, 6)].vendor == INTRADER_VENDOR
    assert by_month[(2023, 7)].vendor == PWB_VENDOR


def test_intrader_never_supplies_a_month_after_2024_08() -> None:
    result = _splice(intrader=_ends((2020, 1), _path(57)), terminating=True)  # ends 2024-09
    assert max((r.month.year, r.month.month) for r in result.rows) == (2024, 8)


def test_a_terminating_name_never_continues_on_pwb_and_keeps_its_final_month() -> None:
    intrader = _ends((2020, 1), _path(40))  # ends 2023-04
    result = _splice(intrader=intrader, terminating=True)
    assert result.verdict is SpliceVerdict.TERMINATING
    assert result.identity is None
    assert {r.vendor for r in result.rows} == {INTRADER_VENDOR}
    assert max((r.month.year, r.month.month) for r in result.rows) == (2023, 4)
    assert all(r.dividend_capture_degraded for r in result.rows if r.month >= date(2022, 1, 1))


def test_an_unspliced_alive_name_stops_before_intrader_capture_month() -> None:
    result = _splice(pwb_series_id=None, pwb=None)
    assert result.verdict is SpliceVerdict.NO_PWB_SERIES
    assert max((r.month.year, r.month.month) for r in result.rows) == (2024, 8)
    assert not any(r.survivor_only for r in result.rows)


def test_a_refused_identity_stays_on_intrader() -> None:
    result = _splice(pwb=_ends((2020, 1), _path(60, step=0.05)))
    assert result.verdict is SpliceVerdict.REFUSED_PRICE_DISAGREEMENT
    assert {r.vendor for r in result.rows} == {INTRADER_VENDOR}
