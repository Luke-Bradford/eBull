"""Tests for the NYSE trading-calendar gate (#609 Phase A).

Verifies full closures + 13:00 ET half days against NYSE's published
calendars, the Juneteenth start-date guard, observed-day shifts, and the
closure-wins precedence over half days.
"""

from __future__ import annotations

from datetime import date

from app.providers.implementations.sec_calendar import is_us_federal_holiday
from app.services.market_calendar import us_market_reason, us_market_specials

# NYSE published full closures (https://www.nyse.com/markets/hours-calendars).
_CLOSURES_2025 = {
    date(2025, 1, 1),  # New Year's Day
    date(2025, 1, 9),  # National day of mourning — President Carter (extraordinary)
    date(2025, 1, 20),  # MLK
    date(2025, 2, 17),  # Washington's Birthday
    date(2025, 4, 18),  # Good Friday
    date(2025, 5, 26),  # Memorial Day
    date(2025, 6, 19),  # Juneteenth
    date(2025, 7, 4),  # Independence Day
    date(2025, 9, 1),  # Labor Day
    date(2025, 11, 27),  # Thanksgiving
    date(2025, 12, 25),  # Christmas
}
_CLOSURES_2026 = {
    date(2026, 1, 1),
    date(2026, 1, 19),
    date(2026, 2, 16),
    date(2026, 4, 3),  # Good Friday
    date(2026, 5, 25),
    date(2026, 6, 19),
    date(2026, 7, 3),  # Independence Day observed (Jul 4 is a Saturday)
    date(2026, 9, 7),
    date(2026, 11, 26),
    date(2026, 12, 25),
}


def test_full_closures_2025_match_nyse() -> None:
    assert us_market_specials(2025).full_closures == frozenset(_CLOSURES_2025)


def test_full_closures_2026_match_nyse() -> None:
    assert us_market_specials(2026).full_closures == frozenset(_CLOSURES_2026)


def test_half_days_2025() -> None:
    # Jul 4 2025 = Friday (full holiday) → Jul 3 Thu is an early close.
    assert us_market_specials(2025).half_days == frozenset({date(2025, 7, 3), date(2025, 11, 28), date(2025, 12, 24)})


def test_half_days_2026_closure_wins_over_jul3() -> None:
    # Jul 4 2026 = Saturday → Jul 3 is the *observed full closure*, NOT a half
    # day. The half set is just {day-after-Thanksgiving, Christmas Eve}.
    specials = us_market_specials(2026)
    assert date(2026, 7, 3) in specials.full_closures
    assert date(2026, 7, 3) not in specials.half_days
    assert specials.half_days == frozenset({date(2026, 11, 27), date(2026, 12, 24)})


def test_extraordinary_closures_included() -> None:
    # Ad-hoc NYSE closures not produced by the scheduled-holiday rules.
    assert date(2012, 10, 29) in us_market_specials(2012).full_closures  # Hurricane Sandy
    assert date(2012, 10, 30) in us_market_specials(2012).full_closures
    assert date(2018, 12, 5) in us_market_specials(2018).full_closures  # Bush mourning
    assert date(2025, 1, 9) in us_market_specials(2025).full_closures  # Carter mourning
    for d in (date(2001, 9, 11), date(2001, 9, 12), date(2001, 9, 13), date(2001, 9, 14)):
        assert d in us_market_specials(2001).full_closures


def test_saturday_christmas_has_no_dec_half_day() -> None:
    # 2021: Dec 25 = Sat → observed Fri Dec 24 closure; NYSE had NO Dec early
    # close that year (only the day after Thanksgiving). The rule must agree.
    specials = us_market_specials(2021)
    assert date(2021, 12, 24) in specials.full_closures
    assert specials.half_days == frozenset({date(2021, 11, 26)})


def test_juneteenth_guard_pre_2022() -> None:
    # NYSE first observed Juneteenth in 2022; 2021 must carry neither Jun 18
    # nor Jun 19 as a closure.
    closures = us_market_specials(2021).full_closures
    assert date(2021, 6, 18) not in closures
    assert date(2021, 6, 19) not in closures


def test_juneteenth_present_from_2022() -> None:
    assert date(2022, 6, 20) in us_market_specials(2022).full_closures  # Jun 19 Sun → Mon 20


def test_good_friday_is_market_but_not_federal() -> None:
    # The whole reason this calendar can't reuse sec_calendar: Good Friday is
    # an NYSE closure but not a US federal holiday.
    good_friday_2026 = date(2026, 4, 3)
    assert good_friday_2026 in us_market_specials(2026).full_closures
    assert not is_us_federal_holiday(good_friday_2026)


def test_observed_new_year_attributed_by_observed_date() -> None:
    # Jan 1 2028 = Saturday → observed Fri Dec 31 2027. The straddle window
    # attributes it to 2027 (the year its observed date lands in).
    assert date(2027, 12, 31) in us_market_specials(2027).full_closures


def test_result_is_cached_and_immutable() -> None:
    a = us_market_specials(2026)
    b = us_market_specials(2026)
    assert a is b
    assert isinstance(a.full_closures, frozenset)


# ---------------------------------------------------------------------------
# Endpoint (pure — no DB; the session_profile join is dev-verified live).
# ---------------------------------------------------------------------------


def test_endpoint_serialises_sorted_iso_dates_and_sets_cache() -> None:
    from fastapi import Response

    from app.api.market_calendar import get_us_market_calendar

    resp = Response()
    body = get_us_market_calendar(2026, resp)
    assert body["year"] == 2026
    assert body["full_closures"] == [
        "2026-01-01",
        "2026-01-19",
        "2026-02-16",
        "2026-04-03",
        "2026-05-25",
        "2026-06-19",
        "2026-07-03",
        "2026-09-07",
        "2026-11-26",
        "2026-12-25",
    ]
    assert body["half_days"] == ["2026-11-27", "2026-12-24"]
    assert resp.headers["Cache-Control"] == "public, max-age=86400"


def test_endpoint_rejects_out_of_range_year() -> None:
    from fastapi import HTTPException, Response

    from app.api.market_calendar import get_us_market_calendar

    for bad in (1999, 2101):
        try:
            get_us_market_calendar(bad, Response())
        except HTTPException as exc:
            assert exc.status_code == 400
        else:  # pragma: no cover - guard
            raise AssertionError(f"year {bad} should have raised")


class TestUsMarketReason:
    """`us_market_reason` (#1766) — the operator-facing name for a non-open
    NYSE day, or None on a regular session. Names come from the same pandas
    rules that derive the closure set (no drift)."""

    def test_scheduled_holiday_name(self) -> None:
        assert us_market_reason(date(2026, 1, 1)) == "New Year's Day"
        assert us_market_reason(date(2026, 12, 25)) == "Christmas"

    def test_observed_shift_carries_name(self) -> None:
        # Jul 4 2026 is a Saturday → observed full closure on Fri Jul 3.
        assert us_market_reason(date(2026, 7, 3)) == "Independence Day"

    def test_independence_day_eve_vs_observed_closure_across_years(self) -> None:
        # 2025: Jul 4 is a Friday (full closure), Jul 3 Thu is the half-day eve.
        assert us_market_reason(date(2025, 7, 3)) == "Independence Day eve"
        assert us_market_reason(date(2025, 7, 4)) == "Independence Day"
        # 2026: Jul 4 Sat → Jul 3 is the OBSERVED closure, not an eve.
        assert us_market_reason(date(2026, 7, 3)) == "Independence Day"

    def test_half_day_names(self) -> None:
        assert us_market_reason(date(2026, 12, 24)) == "Christmas Eve"
        assert us_market_reason(date(2026, 11, 27)) == "Friday after Thanksgiving"

    def test_extraordinary_closure_name(self) -> None:
        assert us_market_reason(date(2025, 1, 9)) == "Day of mourning — President Carter"
        assert us_market_reason(date(2012, 10, 30)) == "Hurricane Sandy"

    def test_weekend(self) -> None:
        assert us_market_reason(date(2026, 6, 27)) == "Weekend"  # Saturday
        assert us_market_reason(date(2026, 6, 28)) == "Weekend"  # Sunday

    def test_plain_weekday_is_none(self) -> None:
        assert us_market_reason(date(2026, 6, 23)) is None  # Tuesday

    def test_reasons_total_over_specials(self) -> None:
        # Every special date has a reason; closure/half-day sets stay disjoint.
        sp = us_market_specials(2026)
        assert sp.full_closures.isdisjoint(sp.half_days)
        for d in sp.full_closures | sp.half_days:
            assert sp.reasons.get(d), f"missing reason for {d}"
