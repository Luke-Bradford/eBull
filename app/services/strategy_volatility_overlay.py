"""Causal portfolio-level inverse-variance exposure for MT-1 (#2437).

Contract: ``docs/proposals/ta/2026-08-15-mt1-volatility-managed-relative-strength-
preregistration.md``.  This module is deliberately pure and sees only completed
monthly returns.  It does not know a strategy, instrument, signal, price row or
outcome namespace, so it cannot silently turn the portfolio overlay into
per-name inverse-volatility weighting.

Published core: Cederburg et al. (2020), equations (3), (4), and their real-time
normalisation: prior-month realised variance, a 120-month initial training
period, then expanding history.  eBull's leverage ban adds the by-construction
``[0, 1]`` cap.  Missing or degenerate history refuses; there is no 100% fallback.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, localcontext
from pathlib import Path
from typing import Final

RULE_SET_ID: Final = "capped-portfolio-inverse-variance-v1"
MIN_TRAINING_MONTHS: Final = 120
TRADING_DAYS_PER_MONTH: Final = Decimal("22")
MIN_EXPOSURE: Final = Decimal("0")
MAX_EXPOSURE: Final = Decimal("1")
DECIMAL_PRECISION: Final = 50


def _code_hash() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


RULE_SET_VERSION: Final = f"{RULE_SET_ID}+{_code_hash()}"


class VolatilityOverlayUnavailable(ValueError):
    """The frozen overlay cannot produce an exposure from the supplied history."""


@dataclass(frozen=True)
class DailyPortfolioReturn:
    """One dated after-cost decimal return on the frozen panel calendar."""

    session: date
    value: Decimal


@dataclass(frozen=True)
class CompletedPortfolioMonth:
    """One complete calendar month and the exact sessions it must cover."""

    month: date
    daily_returns: tuple[DailyPortfolioReturn, ...]
    expected_sessions: tuple[date, ...]


@dataclass(frozen=True)
class VolatilityExposure:
    """One month-ahead exposure and the causal inputs that produced it."""

    exposure: Decimal
    raw_exposure: Decimal
    normalization: Decimal
    prior_month_variance: Decimal
    training_months: int
    rule_set_version: str = RULE_SET_VERSION


def _next_month(month: date) -> date:
    return date(month.year + (month.month == 12), 1 if month.month == 12 else month.month + 1, 1)


def _validated_returns(record: CompletedPortfolioMonth) -> tuple[Decimal, ...]:
    if record.month.day != 1:
        raise VolatilityOverlayUnavailable("completed month keys must be the first calendar day")
    if not record.expected_sessions:
        raise VolatilityOverlayUnavailable(f"{record.month.isoformat()} expected session calendar is empty")
    if tuple(sorted(set(record.expected_sessions))) != record.expected_sessions:
        raise VolatilityOverlayUnavailable(f"{record.month.isoformat()} expected sessions must be sorted and unique")
    if any(
        (session.year, session.month) != (record.month.year, record.month.month) for session in record.expected_sessions
    ):
        raise VolatilityOverlayUnavailable(f"{record.month.isoformat()} expected sessions cross a month boundary")
    actual_sessions = tuple(point.session for point in record.daily_returns)
    if actual_sessions != record.expected_sessions:
        raise VolatilityOverlayUnavailable(
            f"{record.month.isoformat()} daily return dates do not match the expected session calendar"
        )
    values = tuple(point.value for point in record.daily_returns)
    for value in values:
        if not value.is_finite() or value < Decimal("-1"):
            raise VolatilityOverlayUnavailable(
                f"{record.month.isoformat()} contains a non-finite or below-minus-one daily return"
            )
    return values


def compounded_month_return(record: CompletedPortfolioMonth) -> Decimal:
    """Compound one complete month; inputs and output are decimal returns."""
    with localcontext() as context:
        context.prec = DECIMAL_PRECISION
        wealth = Decimal("1")
        for value in _validated_returns(record):
            wealth *= Decimal("1") + value
        result = wealth - Decimal("1")
    if not result.is_finite():
        raise VolatilityOverlayUnavailable(f"{record.month.isoformat()} compounded return is non-finite")
    return result


def realised_month_variance(record: CompletedPortfolioMonth) -> Decimal:
    """Cederburg et al. equation (4): ``(22 / J) * sum(daily_return**2)``."""
    returns = _validated_returns(record)
    with localcontext() as context:
        context.prec = DECIMAL_PRECISION
        variance = (
            TRADING_DAYS_PER_MONTH / Decimal(len(returns)) * sum((value * value for value in returns), Decimal("0"))
        )
    if not variance.is_finite():
        raise VolatilityOverlayUnavailable(f"{record.month.isoformat()} realised variance is non-finite")
    return variance


def _sample_stddev(values: list[Decimal], *, label: str) -> Decimal:
    if len(values) < 2:
        raise VolatilityOverlayUnavailable(f"{label} needs at least two observations")
    with localcontext() as context:
        context.prec = DECIMAL_PRECISION
        mean = sum(values, Decimal("0")) / Decimal(len(values))
        variance = sum(((value - mean) ** 2 for value in values), Decimal("0")) / Decimal(len(values) - 1)
        result = variance.sqrt()
    if not variance.is_finite() or variance <= 0:
        raise VolatilityOverlayUnavailable(f"{label} sample variance is not positive")
    if not result.is_finite() or result <= 0:  # pragma: no cover - guarded by variance
        raise VolatilityOverlayUnavailable(f"{label} sample standard deviation is not positive")
    return result


def capped_inverse_variance_exposure(
    completed_history: tuple[CompletedPortfolioMonth, ...],
    *,
    expected_first_month: date,
) -> VolatilityExposure:
    """Return the next month's causal portfolio exposure.

    ``completed_history[-1]`` is the month immediately before the decision.
    Each training observation ``m`` pairs that month's return with positive
    variance from ``m-1``. Calendar months remain consecutive, including
    zero-variance cash months, while an unusable pair is omitted from the
    expanding normalisation. Consequently at least 120 usable observations and
    121 completed months are required. ``expected_first_month`` is the
    evaluator's frozen first eligible complete reference month; binding the
    first row to it makes a rolling/truncated history fail instead of merely
    documenting that it should.
    """
    if expected_first_month.day != 1:
        raise VolatilityOverlayUnavailable("expected first month must be the first calendar day")
    required_history = MIN_TRAINING_MONTHS + 1
    if len(completed_history) < required_history:
        raise VolatilityOverlayUnavailable(
            f"inverse-variance exposure needs {required_history} completed months "
            f"for {MIN_TRAINING_MONTHS} training observations"
        )
    if completed_history[0].month != expected_first_month:
        raise VolatilityOverlayUnavailable("completed history does not start at the frozen first eligible month")

    previous: date | None = None
    monthly_returns: list[Decimal] = []
    monthly_variances: list[Decimal] = []
    for record in completed_history:
        if previous is not None and record.month != _next_month(previous):
            raise VolatilityOverlayUnavailable("completed portfolio months must be unique and consecutive")
        previous = record.month
        monthly_returns.append(compounded_month_return(record))
        monthly_variances.append(realised_month_variance(record))

    prior_variance = monthly_variances[-1]
    if prior_variance <= 0:
        raise VolatilityOverlayUnavailable("the prior complete month's realised variance is not positive")

    # f[m] and f[m] / v[m-1] over identical usable months. A zero-variance cash
    # month stays in the calendar but cannot serve as the next pair's divisor.
    # The last completed month's return can participate in c[t], while its own
    # positive variance controls month t exposure.
    with localcontext() as context:
        context.prec = DECIMAL_PRECISION
        training_pairs = [
            (monthly_returns[index], monthly_variances[index - 1])
            for index in range(1, len(monthly_returns))
            if monthly_variances[index - 1] > 0
        ]
        if len(training_pairs) < MIN_TRAINING_MONTHS:
            raise VolatilityOverlayUnavailable(
                f"inverse-variance normalization needs {MIN_TRAINING_MONTHS} usable training months; "
                f"received {len(training_pairs)}"
            )
        training_returns = [monthly_return for monthly_return, _ in training_pairs]
        raw_scaled_returns = [
            monthly_return / previous_variance for monthly_return, previous_variance in training_pairs
        ]
        normalization = _sample_stddev(training_returns, label="unscaled training returns") / _sample_stddev(
            raw_scaled_returns, label="raw inverse-variance training returns"
        )
        raw_exposure = normalization / prior_variance
    if not normalization.is_finite() or normalization <= 0 or not raw_exposure.is_finite():
        raise VolatilityOverlayUnavailable("inverse-variance normalization or exposure is not positive and finite")
    exposure = min(MAX_EXPOSURE, max(MIN_EXPOSURE, raw_exposure))
    return VolatilityExposure(
        exposure=exposure,
        raw_exposure=raw_exposure,
        normalization=normalization,
        prior_month_variance=prior_variance,
        training_months=len(training_returns),
    )


__all__ = [
    "MAX_EXPOSURE",
    "DECIMAL_PRECISION",
    "MIN_EXPOSURE",
    "MIN_TRAINING_MONTHS",
    "RULE_SET_ID",
    "RULE_SET_VERSION",
    "TRADING_DAYS_PER_MONTH",
    "CompletedPortfolioMonth",
    "DailyPortfolioReturn",
    "VolatilityExposure",
    "VolatilityOverlayUnavailable",
    "capped_inverse_variance_exposure",
    "compounded_month_return",
    "realised_month_variance",
]
