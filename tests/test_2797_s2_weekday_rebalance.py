"""#2797 — S-2 must rebalance on the first WEEKDAY bar of a new month.

The bug these pin: ``price_daily`` carries weekend rows (3,669 across 389
instruments on 329 distinct dates, measured 2026-08-20 on the validated
universe), and because S-2's rule takes the FIRST bar of a new month over the
panel's *union* calendar, one weekend artefact consumed the whole month. The
real first trading day then never rebalanced, and the cross-section on the
weekend date was 4-117 names against 3,629-5,747 on every real one.

The most recent instance, Sat 2026-08-01, had 9 eligible names — below S-2's
``MIN_CROSS_SECTION`` of 10 — and is the entire reason S-2 had fired zero
signals in production under every version.
"""

from __future__ import annotations

from datetime import date, timedelta

from app.services.strategies.s2_cross_sectional_momentum import rebalance_dates
from app.services.strategies.s10_relative_strength_leader import s10_rebalance_dates


class TestWeekendBarsDoNotTakeTheMonth:
    def test_a_saturday_does_not_consume_the_month_from_the_first_trading_day(self) -> None:
        """The 2026-08 regression, reproduced on its own dates.

        Sat 2026-08-01 is in the union calendar because a handful of instruments
        carry a weekend row. Mon 2026-08-03 is the first real session.
        """
        calendar = [
            date(2026, 7, 30),  # Thu
            date(2026, 7, 31),  # Fri
            date(2026, 8, 1),  # Sat — junk
            date(2026, 8, 3),  # Mon — the real first session of August
            date(2026, 8, 4),  # Tue
        ]
        assert rebalance_dates(calendar) == {date(2026, 8, 3)}

    def test_a_sunday_does_not_consume_the_month_either(self) -> None:
        calendar = [date(2025, 10, 31), date(2025, 11, 1), date(2025, 11, 3), date(2025, 11, 4)]
        assert date(2025, 11, 1).weekday() == 5  # Sat, the measured 2025-11 case
        assert rebalance_dates(calendar) == {date(2025, 11, 3)}

    def test_a_month_whose_only_bars_are_weekend_bars_does_not_rebalance(self) -> None:
        """No weekday bar means no session §4 can mean, so the month is skipped.

        It is NOT silently reassigned to the next weekday in a later month:
        that would rebalance on a date whose month already had one.
        """
        calendar = [
            date(2026, 5, 29),  # Fri
            date(2026, 6, 6),  # Sat — the only June bar
            date(2026, 7, 1),  # Wed
        ]
        assert rebalance_dates(calendar) == {date(2026, 7, 1)}

    def test_a_weekend_only_calendar_has_no_rebalance_at_all(self) -> None:
        calendar = [date(2026, 8, 1), date(2026, 8, 2), date(2026, 9, 5), date(2026, 9, 6)]
        assert rebalance_dates(calendar) == frozenset()

    def test_the_weekday_rule_is_a_cut_and_not_a_shift(self) -> None:
        """A month whose first bar is already a weekday is untouched.

        The fix must not move the 60 of 73 rebalance dates that were correct.
        """
        calendar = [date(2026, 6, 30), date(2026, 7, 1), date(2026, 7, 2)]
        assert rebalance_dates(calendar) == {date(2026, 7, 1)}

    def test_the_first_weekday_is_never_a_rebalance(self) -> None:
        """Leading weekend rows must not promote the first weekday into one.

        There is still no previous bar for its month to differ from.
        """
        assert rebalance_dates([date(2026, 8, 1), date(2026, 8, 2), date(2026, 8, 3)]) == frozenset()


def _calendar_with_weekends(start: date, days: int) -> list[date]:
    """Every date in a span, weekends included — the worst case for the rule.

    A real union calendar is this minus most weekends and minus holidays; using
    the dense form means the two implementations are compared on every month
    boundary shape a real calendar can present, including the 1st falling on
    each of the seven weekdays.
    """
    return [start + timedelta(days=offset) for offset in range(days)]


class TestNoDriftAgainstS10:
    def test_s2_and_s10_agree_on_the_rebalance_calendar(self) -> None:
        """The anti-drift binding for the duplicated rule (#2797).

        S-2 and S-10 hold two independent copies of "first weekday bar of a new
        month". Sharing one implementation would make this check a tautology —
        the failure mode this repo has already shipped once — so they are
        compared behaviourally instead, over four years of dense calendar.
        """
        calendar = _calendar_with_weekends(date(2022, 12, 15), 4 * 365)
        assert rebalance_dates(calendar) == s10_rebalance_dates(calendar)

    def test_they_agree_on_a_calendar_with_holes(self) -> None:
        """Halts, holidays and corpus gaps, not just a dense span."""
        dense = _calendar_with_weekends(date(2023, 1, 1), 3 * 365)
        holed = [when for index, when in enumerate(dense) if index % 7 != 3 and index % 11 != 5]
        assert rebalance_dates(holed) == s10_rebalance_dates(holed)

    def test_they_agree_when_every_month_starts_on_a_weekend(self) -> None:
        """Two of the 13 measured weekend-landed dates, side by side.

        2026-07-31 leads so that August is a real month change rather than the
        calendar's first weekday, which is never a rebalance.
        """
        calendar = [
            date(2026, 7, 31),  # Fri
            date(2026, 8, 1),  # Sat — measured weekend-landed rebalance
            date(2026, 8, 3),  # Mon
            date(2026, 11, 1),  # Sun
            date(2026, 11, 2),  # Mon
        ]
        assert rebalance_dates(calendar) == s10_rebalance_dates(calendar)
        assert rebalance_dates(calendar) == {date(2026, 8, 3), date(2026, 11, 2)}


class TestTheRuleStillHoldsWhatItHeld:
    """§4's month rule, re-pinned through the weekday cut."""

    def test_a_year_boundary_is_still_a_month_change(self) -> None:
        assert rebalance_dates([date(2019, 12, 31), date(2020, 1, 2)]) == {date(2020, 1, 2)}

    def test_the_same_month_a_year_apart_is_still_a_change(self) -> None:
        assert rebalance_dates([date(2019, 3, 1), date(2020, 3, 2)]) == {date(2020, 3, 2)}

    def test_unordered_input_still_gives_the_same_answer(self) -> None:
        calendar = [date(2026, 8, 3), date(2026, 8, 1), date(2026, 7, 31)]
        assert rebalance_dates(calendar) == rebalance_dates(sorted(calendar))
