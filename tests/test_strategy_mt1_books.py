"""Pure construction tests for the frozen MT-1/S-8 four-arm books."""

from __future__ import annotations

import math
from datetime import date, timedelta

import pytest

from app.services.equity_curve import LegBook
from app.services.market_calendar import us_market_status
from app.services.strategies.s10_relative_strength_leader import s10_rebalance_dates
from app.services.strategy_mt1_books import MT1BookConstructionRefused, build_mt1_four_arm_books
from app.services.strategy_mt1_trial import MIN_COMMON_MONTHS, evaluate_mt1_trial

FIRST_MONTH = date(2010, 1, 1)
LAST_DAY = date(2020, 6, 30)


def _sessions(first: date = FIRST_MONTH, last: date = LAST_DAY) -> tuple[date, ...]:
    result: list[date] = []
    current = first
    while current <= last:
        if us_market_status(current) != "closed":
            result.append(current)
        current += timedelta(days=1)
    return tuple(result)


def _book(dates: tuple[date, ...], *, altered: dict[date, float] | None = None) -> LegBook:
    altered = altered or {}
    marks = [100.0]
    for index, when in enumerate(dates[1:], start=1):
        daily_return = altered.get(when, 0.0005 + 0.00015 * math.sin(index / 19.0))
        marks.append(marks[-1] * (1.0 + daily_return))
    book = LegBook()
    book.add(
        entry_index=0,
        exit_index=len(dates) - 1,
        entry_price=marks[0],
        exit_price=marks[-1],
        half_spread=0.0005,
        realised=True,
        marks=marks,
    )
    return book


@pytest.fixture(scope="module")
def dates() -> tuple[date, ...]:
    return _sessions()


def test_all_four_fresh_books_share_the_post_training_month_axis(dates: tuple[date, ...]) -> None:
    built = build_mt1_four_arm_books(
        mt1_book=_book(dates),
        s8_book=_book(dates),
        dates=dates,
        expected_first_month=FIRST_MONTH,
    )

    assert built.evaluation_months[0] == date(2020, 2, 1)
    assert built.evaluation_months[-1] == date(2020, 6, 1)
    assert len(built.evaluation_months) == 5
    assert built.mt1.structural.decision_dates == built.s8.structural.decision_dates
    assert built.mt1.structural.decision_dates == tuple(
        when for when in sorted(s10_rebalance_dates(dates)) if when >= built.mt1.structural.decision_dates[0]
    )
    assert built.mt1.structural.exposure_reconciled is True
    assert built.s8.structural.exposure_reconciled is True
    assert all(
        decision.history_end_month
        == date(
            decision.decision_date.year - (decision.decision_date.month == 1),
            12 if decision.decision_date.month == 1 else decision.decision_date.month - 1,
            1,
        )
        for decision in built.mt1.exposure_decisions
    )
    assert built.mt1.scaled_curve.rebalance_costs > 0
    evaluation_indices = [index for index, when in enumerate(dates) if when >= built.mt1.structural.decision_dates[0]]
    traded = sum(built.mt1.scaled_curve.traded_notional[index] for index in evaluation_indices)
    assert built.mt1.structural.traded_notional == pytest.approx(traded)
    years = (dates[evaluation_indices[-1]] - dates[evaluation_indices[0]]).days / 365.25
    mean_equity = sum(built.mt1.scaled_curve.equity[index] for index in evaluation_indices) / len(evaluation_indices)
    assert built.mt1.structural.annualised_turnover == pytest.approx(traded / 2 / mean_equity / years)


def test_the_decision_bar_outcome_cannot_change_its_own_target(dates: tuple[date, ...]) -> None:
    baseline = build_mt1_four_arm_books(
        mt1_book=_book(dates),
        s8_book=_book(dates),
        dates=dates,
        expected_first_month=FIRST_MONTH,
    )
    first_decision = baseline.mt1.exposure_decisions[0].decision_date
    changed = build_mt1_four_arm_books(
        mt1_book=_book(dates, altered={first_decision: 0.25}),
        s8_book=_book(dates),
        dates=dates,
        expected_first_month=FIRST_MONTH,
    )

    assert changed.mt1.exposure_decisions[0] == baseline.mt1.exposure_decisions[0]
    assert changed.mt1.unscaled.monthly_returns[0].value != baseline.mt1.unscaled.monthly_returns[0].value


def test_a_later_zero_variance_month_refuses_instead_of_omitting_a_clock_date(
    dates: tuple[date, ...],
) -> None:
    march = {when: 0.0 for when in dates if (when.year, when.month) == (2020, 3)}
    with pytest.raises(MT1BookConstructionRefused, match="exposure is unavailable on 2020-04-01"):
        build_mt1_four_arm_books(
            mt1_book=_book(dates, altered=march),
            s8_book=_book(dates),
            dates=dates,
            expected_first_month=FIRST_MONTH,
        )


def test_portfolio_activity_on_a_closed_union_axis_date_refuses() -> None:
    dates = (date(2020, 1, 5), date(2020, 1, 6))  # Sunday, then Monday.
    book = _book(dates)
    with pytest.raises(MT1BookConstructionRefused, match="activity on closed NYSE date 2020-01-05"):
        build_mt1_four_arm_books(
            mt1_book=book,
            s8_book=book,
            dates=dates,
            expected_first_month=date(2020, 1, 1),
        )


def test_a_missing_declared_session_refuses_a_supposedly_complete_month() -> None:
    dates = (date(2020, 1, 2), date(2020, 1, 6))  # Jan 3 was an open NYSE session.
    with pytest.raises(MT1BookConstructionRefused, match="missing declared NYSE session 2020-01-03"):
        build_mt1_four_arm_books(
            mt1_book=LegBook(),
            s8_book=LegBook(),
            dates=dates,
            expected_first_month=date(2020, 1, 1),
        )


def test_a_partial_terminal_month_is_not_silently_dropped() -> None:
    dates = _sessions(date(2020, 1, 1), date(2020, 1, 15))
    with pytest.raises(MT1BookConstructionRefused, match="terminal month 2020-01-01 is incomplete"):
        build_mt1_four_arm_books(
            mt1_book=LegBook(),
            s8_book=LegBook(),
            dates=dates,
            expected_first_month=date(2020, 1, 1),
        )


def test_the_constructed_books_are_the_direct_four_arm_evaluator_input() -> None:
    long_dates = _sessions(date(2000, 1, 1), date(2020, 12, 31))
    built = build_mt1_four_arm_books(
        mt1_book=_book(long_dates),
        s8_book=_book(long_dates),
        dates=long_dates,
        expected_first_month=date(2000, 1, 1),
    )

    result = evaluate_mt1_trial(
        mt1_scaled=built.mt1.scaled,
        mt1_unscaled=built.mt1.unscaled,
        s8_scaled=built.s8.scaled,
        s8_unscaled=built.s8.unscaled,
        mt1_structural=built.mt1.structural,
        s8_structural=built.s8.structural,
    )

    assert len(result.common_months) >= MIN_COMMON_MONTHS
    assert result.common_months == built.evaluation_months
    assert result.primary_difference_in_differences == pytest.approx(0.0)
