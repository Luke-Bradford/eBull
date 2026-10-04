"""Pure helpers behind the #3619 total-return fidelity report."""

from __future__ import annotations

from datetime import date

import pytest

from scripts.report_3619_total_return_fidelity import (
    DividendCell,
    NportReturn,
    add_months,
    instrument_ratios,
    is_miss,
    latest_per_month,
    month_end_levels,
    monthly_returns,
    nport_months,
    split_adjusted_closes,
    tracking,
    without_month,
)


def test_add_months_crosses_year_boundaries() -> None:
    assert add_months((2024, 1), -1) == (2023, 12)
    assert add_months((2023, 11), 2) == (2024, 1)


def test_nport_months_are_the_three_ending_at_the_report_date() -> None:
    assert nport_months(date(2026, 2, 28)) == ((2025, 12), (2026, 1), (2026, 2))


def test_monthly_returns_use_last_bar_and_skip_a_missing_previous_month() -> None:
    levels = month_end_levels(
        [(date(2024, 1, 2), 90.0), (date(2024, 1, 31), 100.0), (date(2024, 2, 29), 110.0), (date(2024, 4, 30), 99.0)]
    )
    returns = monthly_returns(levels)
    assert returns == {(2024, 2): pytest.approx(0.10)}  # April has no March level


def test_without_month_drops_only_the_chosen_edge() -> None:
    returns = {(2024, 7): 0.01, (2024, 8): 0.02, (2024, 9): 0.03}
    assert without_month(returns, max) == {(2024, 7): 0.01, (2024, 8): 0.02}
    assert without_month(returns, min) == {(2024, 8): 0.02, (2024, 9): 0.03}
    assert without_month({}, max) == {}


def test_split_adjusted_closes_rescale_bars_before_the_split() -> None:
    bars = [(date(2020, 8, 28), 500.0, 1.0), (date(2020, 8, 31), 129.0, 4.0), (date(2020, 9, 1), 134.0, 1.0)]
    assert split_adjusted_closes(bars) == [
        (date(2020, 8, 28), 125.0),
        (date(2020, 8, 31), 129.0),
        (date(2020, 9, 1), 134.0),
    ]


def test_latest_filing_wins_and_disagreements_are_counted() -> None:
    month = (2024, 3)
    rows = [
        NportReturn("C1", month, 1.0, date(2024, 5, 1), "a"),
        NportReturn("C1", month, 1.5, date(2024, 6, 1), "b"),  # amendment
        NportReturn("C2", month, 2.0, date(2024, 5, 1), "c"),
        NportReturn("C2", month, 2.0, date(2024, 8, 1), "d"),  # later filing repeats the month
    ]
    returns, disagreements = latest_per_month(rows)
    assert returns == {("C1", month): pytest.approx(0.015), ("C2", month): pytest.approx(0.02)}
    assert disagreements == 1


def test_tracking_separates_a_total_return_arm_from_a_price_arm() -> None:
    months = [(2024, m) for m in range(1, 13)]
    ref = {m: 0.01 + 0.001 * i for i, m in enumerate(months)}
    adj = dict(ref)
    price = {m: v - 0.002 for m, v in ref.items()}  # 2.4pp/yr of distributions missing
    t = tracking(adj, price, ref)
    assert t is not None
    assert t.months == 12
    assert t.adj_td_pp == pytest.approx(0.0)
    assert t.price_td_pp == pytest.approx(-2.4)
    assert t.correlation == pytest.approx(1.0)


def _cell(dps: float, quarter: float, wide: float, *, instrument: int = 1, split: bool = False) -> DividendCell:
    return DividendCell("v", instrument, date(2023, 3, 31), dps, quarter, wide, split)


def test_a_miss_is_a_declared_dividend_with_no_payment_across_two_quarters() -> None:
    assert is_miss(_cell(0.5, 0.0, 0.0))
    assert not is_miss(_cell(0.5, 0.0, 0.5))  # declared late, paid next quarter
    assert not is_miss(_cell(0.0, 0.0, 0.0))


def test_instrument_ratios_sum_quarters_and_carry_the_split_flag() -> None:
    cells = [
        _cell(0.5, 0.5, 1.0, instrument=1),
        _cell(0.5, 0.25, 0.5, instrument=1, split=True),
        _cell(0.4, 0.0, 0.0, instrument=2),  # no vendor payments at all: excluded from the ratio
    ]
    assert instrument_ratios(cells) == {(1, True): pytest.approx(0.75)}
