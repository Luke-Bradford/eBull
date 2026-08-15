from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from app.services.pead_feasibility import (
    _PRE_OUTCOME_WINDOWS_SQL,
    PreOutcomeWindow,
    eligible_events,
    market_session_dates,
    purged_date_count,
)


def _window(*, instrument_id: int = 1, accession: str = "a", median: str = "20000000") -> PreOutcomeWindow:
    return PreOutcomeWindow(
        event_index=instrument_id,
        instrument_id=instrument_id,
        accession_number=accession,
        series_id=instrument_id,
        entry_date=date(2025, 1, 2),
        entry_open=Decimal("10"),
        entry_return_usable=True,
        prior_sessions=20,
        valid_liquidity_sessions=20,
        median_dollar_volume=Decimal(median),
    )


def test_census_sql_cannot_read_a_post_entry_close_or_outcome_horizon() -> None:
    lowered = _PRE_OUTCOME_WINDOWS_SQL.lower()
    assert "exit" not in lowered
    assert "return_pct" not in lowered
    assert "d.bar_date < p.entry_date" in lowered
    assert "entry_open" in lowered
    assert "d.close" in lowered  # prior liquidity only, guarded by the strict boundary above


def test_eligibility_uses_pre_entry_liquidity_and_deduplicates_share_classes() -> None:
    events, census = eligible_events(
        (
            _window(instrument_id=1, accession="same", median="15000000"),
            _window(instrument_id=2, accession="same", median="25000000"),
            _window(instrument_id=3, accession="thin", median="9000000"),
        )
    )
    assert [(item.instrument_id, item.accession_number) for item in events] == [(2, "same")]
    assert census["share_class_duplicates_suppressed"] == 1
    assert census["median_dollar_volume_below_floor"] == 1


def test_purged_count_uses_session_distance_not_nominal_events() -> None:
    start = date(2025, 1, 1)
    sessions = tuple(start + timedelta(days=offset) for offset in range(200))
    entries = (sessions[0], sessions[0], sessions[10], sessions[61], sessions[62], sessions[124])
    assert purged_date_count(entries, sessions, hold_sessions=62) == 3


def test_market_session_dates_uses_declared_nyse_calendar() -> None:
    # 2025-07-04 was an NYSE closure; the surrounding Thursday/Monday were open.
    assert market_session_dates(date(2025, 7, 3), date(2025, 7, 7)) == (
        date(2025, 7, 3),
        date(2025, 7, 7),
    )
