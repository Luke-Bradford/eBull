from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.services.market_calendar import us_market_status
from app.services.r6_operational_rules import FactorValuationRecord, turn_of_month_preference_window


def _nyse_sessions(start: date, end: date) -> tuple[date, ...]:
    sessions: list[date] = []
    current = start
    while current <= end:
        if us_market_status(current) != "closed":
            sessions.append(current)
        current += timedelta(days=1)
    return tuple(sessions)


def test_turn_of_month_window_uses_session_offsets_across_month_boundary() -> None:
    sessions = _nyse_sessions(date(2026, 1, 20), date(2026, 2, 10))
    assert turn_of_month_preference_window(sessions, target_year=2026, target_month=1) == (
        date(2026, 1, 27),
        date(2026, 1, 28),
        date(2026, 1, 29),
        date(2026, 1, 30),
        date(2026, 2, 2),
        date(2026, 2, 3),
        date(2026, 2, 4),
    )


@pytest.mark.parametrize(
    ("sessions", "message"),
    [
        ((date(2026, 1, 30),), "complete"),
        (
            (
                date(2026, 1, 29),
                date(2026, 1, 30),
                date(2026, 1, 30),
                *(_nyse_sessions(date(2026, 2, 2), date(2026, 2, 6))),
            ),
            "strictly increasing",
        ),
        (_nyse_sessions(date(2026, 2, 2), date(2026, 2, 12)), "target-month"),
    ],
)
def test_turn_of_month_window_refuses_malformed_or_incomplete_calendars(
    sessions: tuple[date, ...], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        turn_of_month_preference_window(sessions, target_year=2026, target_month=1)


def test_unavailable_factor_valuation_carries_no_proxy_value() -> None:
    record = FactorValuationRecord(
        factor_id="momentum",
        status="unavailable",
        reason="#2912 contains factor returns, not valuation spreads",
    )
    assert record.spread_value is None


def test_unavailable_factor_valuation_refuses_hidden_values() -> None:
    with pytest.raises(ValueError, match="cannot carry values"):
        FactorValuationRecord(
            factor_id="momentum",
            status="unavailable",
            reason="no genuine spread series",
            spread_value=Decimal("0.1"),
        )


@pytest.mark.parametrize("unit", ["decimal_return", "percent_per_annum", "binary_indicator"])
def test_factor_return_or_context_unit_cannot_masquerade_as_valuation_spread(unit: str) -> None:
    with pytest.raises(ValueError, match="cannot be recorded as a valuation spread"):
        FactorValuationRecord(
            factor_id="momentum",
            status="recorded",
            reason="deployment context",
            spread_measure="long-minus-short valuation",
            spread_value=Decimal("0.2"),
            spread_unit=unit,
            observation_date=date(2026, 8, 23),
            history_start=date(2000, 1, 1),
            history_end=date(2026, 8, 23),
            historical_percentile=Decimal("0.8"),
            source="aqr",
            dataset_key="vme_monthly",
            series_key="MOM",
            source_snapshot_sha256="a" * 64,
        )


def test_recorded_factor_valuation_requires_as_of_safe_history_and_provenance() -> None:
    record = FactorValuationRecord(
        factor_id="value",
        status="recorded",
        reason="deployment context only",
        spread_measure="book-to-market long-minus-short spread",
        spread_value=Decimal("0.42"),
        spread_unit="book_to_market_ratio",
        observation_date=date(2026, 8, 23),
        history_start=date(2000, 1, 1),
        history_end=date(2026, 8, 22),
        historical_percentile=Decimal("0.77"),
        source="future_free_reference_source",
        dataset_key="factor_valuation",
        series_key="VALUE_SPREAD",
        source_snapshot_sha256="b" * 64,
    )
    assert record.status == "recorded"
