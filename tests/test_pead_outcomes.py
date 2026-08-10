from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from app.services.pead_candidate import QuarterObservation, SueEvent, TriggeredSueEvent
from app.services.pead_outcomes import (
    EventOutcome,
    OutcomeSummary,
    _net_return_pct,
    build_matched_control_events,
    concurrency_counts,
    earliest_entry_date,
    segment_outcomes,
)


def _outcome(entry: date, net: float) -> EventOutcome:
    return EventOutcome(
        instrument_id=1,
        issuer_cik="0000000001",
        accession_number=f"a-{entry}",
        side="long",
        entry_date=entry,
        exit_date=date(entry.year, min(entry.month + 2, 12), entry.day),
        gross_return_pct=net,
        net_return_pct=net,
        net_return_5_pct=None,
        net_return_20_pct=None,
        net_return_40_pct=None,
        market_relative_net_return_pct=None,
        sector_relative_net_return_pct=None,
        sector_symbol=None,
    )


def test_costs_reduce_both_long_and_short_returns() -> None:
    long_gross, long_net = _net_return_pct("long", Decimal("100"), Decimal("110"))
    short_gross, short_net = _net_return_pct("short", Decimal("100"), Decimal("90"))
    assert long_gross == 10.0
    assert short_gross == 10.0
    assert long_net < long_gross
    assert short_net < short_gross


def test_short_return_direction_is_not_a_long_sign_flip() -> None:
    _, profitable = _net_return_pct("short", Decimal("100"), Decimal("80"))
    _, losing = _net_return_pct("short", Decimal("100"), Decimal("120"))
    assert profitable > 0
    assert losing < 0
    assert abs(losing) > profitable


def test_summary_reports_trade_distribution_without_fabricating_bootstrap() -> None:
    summary = OutcomeSummary(
        outcomes=(
            _outcome(date(2024, 1, 2), 10.0),
            _outcome(date(2024, 2, 2), -5.0),
            _outcome(date(2024, 3, 2), -1.0),
        ),
        bootstrap=None,
        refusals={},
    )
    assert summary.expectancy_pct == 4 / 3
    assert summary.win_rate_pct == pytest.approx(100 / 3)
    assert summary.profit_factor == 10 / 6
    assert summary.worst_trade_pct == -5.0
    assert summary.expected_shortfall_5_pct == -5.0


def test_segments_are_closed_and_use_entry_date() -> None:
    outcomes = tuple(_outcome(date(2024, month, 2), float(month)) for month in (1, 2, 3))
    assert segment_outcomes(outcomes, date(2024, 2, 2), date(2024, 3, 2)) == outcomes[1:]


def test_exact_acceptance_time_controls_the_earliest_open() -> None:
    filed = date(2024, 6, 3)
    assert earliest_entry_date(filed, datetime(2024, 6, 3, 13, 29, tzinfo=UTC)) == filed
    assert earliest_entry_date(filed, datetime(2024, 6, 3, 13, 30, tzinfo=UTC)) == date(2024, 6, 4)
    assert earliest_entry_date(filed, None) == date(2024, 6, 4)


def test_naive_acceptance_time_is_refused() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        earliest_entry_date(date(2024, 6, 3), datetime(2024, 6, 3, 8, 0))


def test_concurrency_counts_overlapping_calendar_spans() -> None:
    outcomes = (
        _outcome(date(2024, 1, 2), 1.0),
        _outcome(date(2024, 2, 2), 1.0),
    )
    maximum, median_open = concurrency_counts(outcomes)
    assert maximum == 2
    assert median_open is not None
    assert 1 <= median_open <= 2


def _trigger(instrument_id: int, sue: float, side: str | None) -> TriggeredSueEvent:
    observation = QuarterObservation(
        instrument_id=instrument_id,
        fiscal_year=2024,
        fiscal_quarter=2,
        value=Decimal("1"),
        filed_date=date(2024, 5, instrument_id),
        accepted_at=None,
        accession_number=f"accession-{instrument_id}",
        source_accessions=(f"accession-{instrument_id}",),
        derived_q4=False,
    )
    return TriggeredSueEvent(
        event=SueEvent(observation=observation, sue=sue),
        lower_threshold=-2.0,
        upper_threshold=2.0,
        side=side,
        threshold_population=200,
    )


def test_matched_controls_are_one_for_one_and_inherit_direction() -> None:
    events = (
        _trigger(1, 3.0, "long"),
        _trigger(2, -3.0, "short"),
        _trigger(3, 0.1, None),
        _trigger(4, -0.1, None),
    )
    controls, census = build_matched_control_events(events, seed=7)
    assert len(controls) == 2
    assert {item.side for item in controls} == {"long", "short"}
    assert {item.event.observation.instrument_id for item in controls} == {3, 4}
    assert census == {"matched_control_events": 2, "signal_events_to_match": 2}
