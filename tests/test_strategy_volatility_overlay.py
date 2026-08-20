from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.services.strategy_volatility_overlay import (
    MAX_EXPOSURE,
    MIN_TRAINING_MONTHS,
    RULE_SET_ID,
    RULE_SET_VERSION,
    CompletedPortfolioMonth,
    DailyPortfolioReturn,
    VolatilityOverlayUnavailable,
    capped_inverse_variance_exposure,
    compounded_month_return,
    realised_month_variance,
)


def _month(index: int) -> date:
    year, zero_based_month = divmod(index, 12)
    return date(2000 + year, zero_based_month + 1, 1)


def _record(month: date, *values: Decimal) -> CompletedPortfolioMonth:
    sessions = tuple(date(month.year, month.month, day + 1) for day in range(len(values)))
    return CompletedPortfolioMonth(
        month=month,
        daily_returns=tuple(
            DailyPortfolioReturn(session, value) for session, value in zip(sessions, values, strict=True)
        ),
        expected_sessions=sessions,
    )


def _history(*, count: int = MIN_TRAINING_MONTHS + 1, final_multiple: int = 1) -> tuple[CompletedPortfolioMonth, ...]:
    base = Decimal("0.01")
    return tuple(
        _record(
            _month(index),
            (base if index % 2 == 0 else -base) * (final_multiple if index == count - 1 else 1),
        )
        for index in range(count)
    )


def _exposure(history: tuple[CompletedPortfolioMonth, ...]):
    return capped_inverse_variance_exposure(history, expected_first_month=history[0].month)


def test_month_return_and_variance_use_decimal_compounding_and_equation_four() -> None:
    record = _record(date(2026, 1, 1), Decimal("0.10"), Decimal("-0.05"))

    assert compounded_month_return(record) == Decimal("0.0450")
    assert realised_month_variance(record) == Decimal("0.13750")


def test_constant_variance_normalises_to_full_but_never_levered_exposure() -> None:
    decision = _exposure(_history())

    assert decision.training_months == MIN_TRAINING_MONTHS
    assert decision.raw_exposure == pytest.approx(Decimal("1"), abs=Decimal("1e-45"))
    assert decision.exposure == MAX_EXPOSURE
    assert decision.rule_set_version == RULE_SET_VERSION
    assert RULE_SET_VERSION.startswith(f"{RULE_SET_ID}+")


def test_last_month_variance_shock_reduces_next_month_exposure() -> None:
    decision = _exposure(_history(final_multiple=2))

    # The training pairs all divide by the preceding constant variance, so c_t
    # remains that variance; the final month's four-times variance makes the
    # next exposure exactly one quarter.
    assert decision.raw_exposure == pytest.approx(Decimal("0.25"), abs=Decimal("1e-45"))
    assert decision.exposure == pytest.approx(Decimal("0.25"), abs=Decimal("1e-45"))


def test_expanding_history_is_used_rather_than_truncated_to_120_pairs() -> None:
    decision = _exposure(_history(count=MIN_TRAINING_MONTHS + 13))

    assert decision.training_months == MIN_TRAINING_MONTHS + 12


@pytest.mark.parametrize(
    ("history", "message"),
    [
        (_history(count=MIN_TRAINING_MONTHS), "needs 121 completed months"),
        (
            tuple(
                _record(
                    _month(index + (1 if index == 10 else 0)),
                    Decimal("0.01" if index % 2 == 0 else "-0.01"),
                )
                for index in range(MIN_TRAINING_MONTHS + 1)
            ),
            "unique and consecutive",
        ),
    ],
)
def test_missing_history_refuses_instead_of_defaulting_to_full_exposure(
    history: tuple[CompletedPortfolioMonth, ...], message: str
) -> None:
    with pytest.raises(VolatilityOverlayUnavailable, match=message):
        _exposure(history)


def test_zero_variance_refuses_instead_of_defaulting_to_full_exposure() -> None:
    history = tuple(_record(_month(index), Decimal("0")) for index in range(MIN_TRAINING_MONTHS + 1))

    with pytest.raises(VolatilityOverlayUnavailable, match="prior complete month's realised variance is not positive"):
        _exposure(history)


def test_old_cash_month_stays_in_expanding_calendar_but_only_usable_pairs_count() -> None:
    history = list(_history(count=MIN_TRAINING_MONTHS + 2))
    history[0] = _record(history[0].month, Decimal("0"))

    decision = _exposure(tuple(history))

    assert decision.training_months == MIN_TRAINING_MONTHS


def test_incomplete_session_coverage_refuses() -> None:
    history = list(_history())
    extra_session = date(history[10].month.year, history[10].month.month, 2)
    history[10] = CompletedPortfolioMonth(
        history[10].month,
        history[10].daily_returns,
        (*history[10].expected_sessions, extra_session),
    )

    with pytest.raises(VolatilityOverlayUnavailable, match="return dates do not match"):
        _exposure(tuple(history))


def test_truncated_expanding_history_refuses_against_frozen_origin() -> None:
    history = _history(count=MIN_TRAINING_MONTHS + 13)

    with pytest.raises(VolatilityOverlayUnavailable, match="frozen first eligible month"):
        capped_inverse_variance_exposure(history[12:], expected_first_month=history[0].month)


def test_non_month_start_and_invalid_return_are_refused() -> None:
    with pytest.raises(VolatilityOverlayUnavailable, match="first calendar day"):
        wrong_month = _record(date(2026, 1, 1), Decimal("0.01"))
        compounded_month_return(
            CompletedPortfolioMonth(date(2026, 1, 2), wrong_month.daily_returns, wrong_month.expected_sessions)
        )
    with pytest.raises(VolatilityOverlayUnavailable, match="below-minus-one"):
        realised_month_variance(_record(date(2026, 1, 1), Decimal("-1.01")))
