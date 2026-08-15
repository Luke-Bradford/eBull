"""Fresh, causal four-arm portfolio construction for MT-1 (#2437).

The public constructor is deliberately four-arm: both source books are walked
afresh and both structural audits pass before monthly outcome arms are exposed.
Daily volatility observations use declared NYSE sessions.  Closed dates on the
union-price axis are allowed only when the entire source portfolio is inert.
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Final

import numpy as np

from app.services.equity_curve import EquityCurve, LegBook, build_capped_target_exposure_curve, build_equity_curve
from app.services.market_calendar import us_market_status
from app.services.strategies.s10_relative_strength_leader import s10_rebalance_dates
from app.services.strategy_mt1_trial import (
    MAX_ANNUALISED_TURNOVER,
    MonthlyReturn,
    PortfolioArm,
    ScaledBookStructuralAudit,
    structural_gate,
)
from app.services.strategy_volatility_overlay import (
    CompletedPortfolioMonth,
    DailyPortfolioReturn,
    VolatilityExposure,
    VolatilityOverlayUnavailable,
    capped_inverse_variance_exposure,
    compounded_month_return,
)

BOOK_RULE_ID: Final = "mt1-fresh-four-arm-nyse-session-books-v1"
_DAYS_PER_YEAR: Final = 365.25


def _code_hash() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


BOOK_RULE_VERSION: Final = f"{BOOK_RULE_ID}+{_code_hash()}"


class MT1BookConstructionRefused(ValueError):
    """The frozen four-arm construction cannot be produced as declared."""


@dataclass(frozen=True)
class ExposureDecision:
    """One target and the completed information set that produced it."""

    decision_date: date
    history_end_month: date
    result: VolatilityExposure


@dataclass(frozen=True)
class SourceMonthlyBooks:
    """One source rule's scaled/unscaled books after structural validation."""

    scaled: PortfolioArm
    unscaled: PortfolioArm
    structural: ScaledBookStructuralAudit
    exposure_decisions: tuple[ExposureDecision, ...]
    scaled_curve: EquityCurve
    unscaled_curve: EquityCurve


@dataclass(frozen=True)
class MT1FourArmBooks:
    """The complete paved input to :func:`evaluate_mt1_trial`."""

    mt1: SourceMonthlyBooks
    s8: SourceMonthlyBooks
    evaluation_months: tuple[date, ...]
    rule_version: str = BOOK_RULE_VERSION


def _next_month(month: date) -> date:
    return date(month.year + (month.month == 12), 1 if month.month == 12 else month.month + 1, 1)


def _previous_month(month: date) -> date:
    return date(month.year - (month.month == 1), 12 if month.month == 1 else month.month - 1, 1)


def _month_sessions(month: date) -> tuple[date, ...]:
    following = _next_month(month)
    current = month
    sessions: list[date] = []
    while current < following:
        if us_market_status(current) != "closed":
            sessions.append(current)
        current += timedelta(days=1)
    return tuple(sessions)


def _validate_axis_and_curve(
    label: str,
    *,
    dates: tuple[date, ...],
    curve: EquityCurve,
    starting_equity: float,
) -> dict[date, int]:
    if len(dates) < 2:
        raise MT1BookConstructionRefused(f"{label} date axis needs at least two dates")
    if any(later <= earlier for earlier, later in zip(dates, dates[1:], strict=False)):
        raise MT1BookConstructionRefused(f"{label} date axis must be strictly increasing")
    if len(curve.equity) != len(dates):
        raise MT1BookConstructionRefused(f"{label} curve has {len(curve.equity)} points against {len(dates)} dates")
    if not math.isfinite(starting_equity) or starting_equity <= 0:
        raise MT1BookConstructionRefused(f"{label} starting equity must be positive and finite")
    if (
        not np.all(np.isfinite(curve.equity))
        or not np.all(np.isfinite(curve.invested))
        or not np.all(np.isfinite(curve.traded_notional))
        or np.any(curve.equity <= 0)
        or np.any(curve.invested < 0)
        or np.any(curve.traded_notional < 0)
        or np.any(curve.invested - curve.equity > 1e-12)
    ):
        raise MT1BookConstructionRefused(f"{label} curve is not finite, positive, unlevered, and non-negative")

    for index, when in enumerate(dates):
        if us_market_status(when) != "closed":
            continue
        previous_equity = starting_equity if index == 0 else float(curve.equity[index - 1])
        previous_invested = 0.0 if index == 0 else float(curve.invested[index - 1])
        previous_open = 0 if index == 0 else int(curve.open_count[index - 1])
        if (
            float(curve.equity[index]) != previous_equity
            or float(curve.invested[index]) != previous_invested
            or int(curve.open_count[index]) != previous_open
            or float(curve.traded_notional[index]) != 0.0
        ):
            raise MT1BookConstructionRefused(f"{label} has portfolio activity on closed NYSE date {when.isoformat()}")
    return {when: index for index, when in enumerate(dates)}


def _completed_history(
    label: str,
    *,
    dates: tuple[date, ...],
    curve: EquityCurve,
    starting_equity: float,
    expected_first_month: date,
) -> tuple[CompletedPortfolioMonth, ...]:
    if expected_first_month.day != 1:
        raise MT1BookConstructionRefused("expected first month must be the first calendar day")
    index_by_date = _validate_axis_and_curve(
        label,
        dates=dates,
        curve=curve,
        starting_equity=starting_equity,
    )
    first_sessions = _month_sessions(expected_first_month)
    if not first_sessions or dates[0] < expected_first_month or dates[0] > first_sessions[0]:
        raise MT1BookConstructionRefused(
            f"{label} axis starts after the first expected session of {expected_first_month.isoformat()}"
        )

    result: list[CompletedPortfolioMonth] = []
    month = expected_first_month
    while month <= date(dates[-1].year, dates[-1].month, 1):
        sessions = _month_sessions(month)
        missing = tuple(session for session in sessions if session not in index_by_date)
        if missing:
            past_holes = tuple(session for session in missing if session <= dates[-1])
            if past_holes:
                raise MT1BookConstructionRefused(
                    f"{label} is missing declared NYSE session {past_holes[0].isoformat()} in {month.isoformat()}"
                )
            raise MT1BookConstructionRefused(
                f"{label} terminal month {month.isoformat()} is incomplete at {dates[-1].isoformat()}"
            )
        daily: list[DailyPortfolioReturn] = []
        for session in sessions:
            index = index_by_date[session]
            previous = starting_equity if index == 0 else float(curve.equity[index - 1])
            value = float(curve.equity[index]) / previous - 1.0
            if not math.isfinite(value) or value < -1.0:
                raise MT1BookConstructionRefused(f"{label} has an invalid daily return on {session.isoformat()}")
            daily.append(DailyPortfolioReturn(session=session, value=Decimal(str(value))))
        result.append(
            CompletedPortfolioMonth(
                month=month,
                daily_returns=tuple(daily),
                expected_sessions=sessions,
            )
        )
        month = _next_month(month)
    if not result:
        raise MT1BookConstructionRefused(f"{label} has no complete portfolio month")
    return tuple(result)


def _exposure_decisions(
    label: str,
    *,
    history: tuple[CompletedPortfolioMonth, ...],
    dates: tuple[date, ...],
    expected_first_month: date,
) -> tuple[ExposureDecision, ...]:
    last_complete_month = history[-1].month
    clock = tuple(
        when
        for when in sorted(s10_rebalance_dates(dates))
        if expected_first_month < date(when.year, when.month, 1) <= last_complete_month
    )
    if any(us_market_status(when) == "closed" for when in clock):
        bad = next(when for when in clock if us_market_status(when) == "closed")
        raise MT1BookConstructionRefused(f"{label} S-10 decision clock lands on closed NYSE date {bad.isoformat()}")

    decisions: list[ExposureDecision] = []
    for decision_date in clock:
        decision_month = date(decision_date.year, decision_date.month, 1)
        history_end = _previous_month(decision_month)
        prefix = tuple(record for record in history if record.month <= history_end)
        if not prefix or prefix[-1].month != history_end:
            if decisions:
                raise MT1BookConstructionRefused(f"{label} completed history does not reach {history_end.isoformat()}")
            continue
        try:
            exposure = capped_inverse_variance_exposure(prefix, expected_first_month=expected_first_month)
        except VolatilityOverlayUnavailable as exc:
            if not decisions:
                continue
            raise MT1BookConstructionRefused(
                f"{label} exposure is unavailable on {decision_date.isoformat()}: {exc}"
            ) from exc
        decisions.append(
            ExposureDecision(
                decision_date=decision_date,
                history_end_month=history_end,
                result=exposure,
            )
        )

    if not decisions:
        raise MT1BookConstructionRefused(f"{label} produced no exposure after the frozen training window")
    expected_after_warmup = tuple(when for when in clock if when >= decisions[0].decision_date)
    actual = tuple(decision.decision_date for decision in decisions)
    if actual != expected_after_warmup:
        raise MT1BookConstructionRefused(f"{label} exposure decisions do not cover the exact post-warm-up S-10 clock")

    for decision in decisions:
        prefix = tuple(record for record in history if record.month <= decision.history_end_month)
        replay = capped_inverse_variance_exposure(prefix, expected_first_month=expected_first_month)
        if replay != decision.result or prefix[-1].month != decision.history_end_month:
            raise MT1BookConstructionRefused(
                f"{label} exposure does not replay on {decision.decision_date.isoformat()}"
            )
    return tuple(decisions)


def _arm(history: tuple[CompletedPortfolioMonth, ...], *, first_month: date) -> PortfolioArm:
    return PortfolioArm(
        monthly_returns=tuple(
            MonthlyReturn(month=record.month, value=float(compounded_month_return(record)))
            for record in history
            if record.month >= first_month
        )
    )


def _turnover_audit(
    label: str,
    *,
    curve: EquityCurve,
    dates: tuple[date, ...],
    decisions: tuple[ExposureDecision, ...],
) -> ScaledBookStructuralAudit:
    first = decisions[0].decision_date
    session_indices = tuple(
        index for index, when in enumerate(dates) if when >= first and us_market_status(when) != "closed"
    )
    if len(session_indices) < 2:
        raise MT1BookConstructionRefused(f"{label} evaluation axis has fewer than two NYSE sessions")
    first_session = dates[session_indices[0]]
    last_session = dates[session_indices[-1]]
    years = (last_session - first_session).days / _DAYS_PER_YEAR
    mean_equity = float(np.mean(curve.equity[list(session_indices)]))
    traded_notional = float(np.sum(curve.traded_notional[list(session_indices)]))
    if not math.isfinite(mean_equity) or mean_equity <= 0 or not math.isfinite(traded_notional) or years <= 0:
        raise MT1BookConstructionRefused(f"{label} turnover denominator is not positive and finite")
    annualised = traded_notional / 2.0 / mean_equity / years
    if not math.isfinite(annualised) or annualised > MAX_ANNUALISED_TURNOVER:
        raise MT1BookConstructionRefused(
            f"{label} annualised turnover {annualised:.12g} exceeds the frozen 600% ceiling"
        )
    decision_dates = tuple(decision.decision_date for decision in decisions)
    return ScaledBookStructuralAudit(
        decision_dates=decision_dates,
        expected_decision_dates=decision_dates,
        annualised_turnover=annualised,
        traded_notional=traded_notional,
        exposure_reconciled=True,
    )


def _build_source(
    label: str,
    *,
    book: LegBook,
    dates: tuple[date, ...],
    expected_first_month: date,
    starting_equity: float,
) -> SourceMonthlyBooks:
    unscaled_curve = build_equity_curve(book, date_count=len(dates), starting_equity=starting_equity)
    unscaled_history = _completed_history(
        f"{label} unscaled",
        dates=dates,
        curve=unscaled_curve,
        starting_equity=starting_equity,
        expected_first_month=expected_first_month,
    )
    decisions = _exposure_decisions(
        label,
        history=unscaled_history,
        dates=dates,
        expected_first_month=expected_first_month,
    )
    scaled_curve = build_capped_target_exposure_curve(
        book,
        dates=dates,
        target_exposure_by_date={decision.decision_date: float(decision.result.exposure) for decision in decisions},
        starting_equity=starting_equity,
    )
    scaled_history = _completed_history(
        f"{label} scaled",
        dates=dates,
        curve=scaled_curve,
        starting_equity=starting_equity,
        expected_first_month=expected_first_month,
    )
    first_month = date(decisions[0].decision_date.year, decisions[0].decision_date.month, 1)
    structural = _turnover_audit(label, curve=scaled_curve, dates=dates, decisions=decisions)
    return SourceMonthlyBooks(
        scaled=_arm(scaled_history, first_month=first_month),
        unscaled=_arm(unscaled_history, first_month=first_month),
        structural=structural,
        exposure_decisions=decisions,
        scaled_curve=scaled_curve,
        unscaled_curve=unscaled_curve,
    )


def build_mt1_four_arm_books(
    *,
    mt1_book: LegBook,
    s8_book: LegBook,
    dates: tuple[date, ...],
    expected_first_month: date,
    starting_equity: float = 1.0,
) -> MT1FourArmBooks:
    """Build all four arms; accept no caller-owned returns or audit flags."""
    mt1 = _build_source(
        "MT-1",
        book=mt1_book,
        dates=dates,
        expected_first_month=expected_first_month,
        starting_equity=starting_equity,
    )
    s8 = _build_source(
        "S-8 negative control",
        book=s8_book,
        dates=dates,
        expected_first_month=expected_first_month,
        starting_equity=starting_equity,
    )
    structural_gate(mt1.structural, s8.structural)
    axes = (
        tuple(point.month for point in mt1.scaled.monthly_returns),
        tuple(point.month for point in mt1.unscaled.monthly_returns),
        tuple(point.month for point in s8.scaled.monthly_returns),
        tuple(point.month for point in s8.unscaled.monthly_returns),
    )
    if not axes[0] or any(axis != axes[0] for axis in axes[1:]):
        raise MT1BookConstructionRefused("the four fresh books do not share one complete evaluation-month axis")
    return MT1FourArmBooks(mt1=mt1, s8=s8, evaluation_months=axes[0])


__all__ = [
    "BOOK_RULE_ID",
    "BOOK_RULE_VERSION",
    "ExposureDecision",
    "MT1BookConstructionRefused",
    "MT1FourArmBooks",
    "SourceMonthlyBooks",
    "build_mt1_four_arm_books",
]
