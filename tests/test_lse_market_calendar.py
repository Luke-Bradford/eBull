"""The LSE calendar against its published source population (#2312).

Pure-logic; no DB. The headline test is a FULL-POPULATION check: every England &
Wales bank holiday GOV.UK publishes over the calendar's verified horizon, by date
AND by name. A sample would not have caught either defect this file pins -- the two
moved holidays (a date-set error) or the 2022 December label swap (a name-only
error that date-set equality cannot see).
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from app.services.lse_market_calendar import (
    VERIFIED_FROM_YEAR,
    VERIFIED_THROUGH_YEAR,
    LseCalendarHorizonError,
    lse_market_reason,
    lse_market_specials,
    lse_market_status,
    lse_session_is_open,
)

_LONDON = ZoneInfo("Europe/London")
_FIXTURE = Path(__file__).parent / "fixtures" / "gov_uk_bank_holidays_england_and_wales.json"


def _gov_uk_population() -> dict[date, str]:
    """The frozen GOV.UK england-and-wales division, date -> title.

    ⚠ The apostrophe is normalised: GOV.UK writes "New Year’s Day" with U+2019 and
    the module's rule names use an ASCII quote. That is the only normalisation, and
    it is narrow on purpose -- anything broader would let a real label difference
    through, which is the whole failure this test exists to catch.
    """
    events = json.loads(_FIXTURE.read_text())["england-and-wales"]["events"]
    return {date.fromisoformat(e["date"]): str(e["title"]).replace("’", "'") for e in events}


def _derived_population() -> dict[date, str]:
    out: dict[date, str] = {}
    for year in range(VERIFIED_FROM_YEAR, VERIFIED_THROUGH_YEAR + 1):
        specials = lse_market_specials(year)
        out.update({d: specials.reasons[d] for d in specials.full_closures})
    return out


def test_the_full_gov_uk_population_is_reproduced_by_date_and_by_name() -> None:
    """83 dates over 2019-2028, not a sample.

    78 come from the scheduled rules; 5 royal/commemorative days no rule can derive
    are transcribed; 2 rule-derived dates are suppressed because their holiday was
    MOVED rather than added. ⚠ Comparing the two dicts (not the two key sets) is
    deliberate -- see ``test_the_2022_december_pair_is_labelled_the_way_gov_uk_labels_it``.
    """
    reference = _gov_uk_population()
    assert len(reference) == 83
    assert _derived_population() == reference


def test_the_moved_holidays_do_not_close_the_mondays_they_moved_from() -> None:
    """The suppression half, stated as behaviour rather than as a set difference.

    An additions-only model reproduces all 83 GOV.UK dates AND wrongly closes these
    two ordinary trading Mondays, because a moved holiday is not an extra one.
    """
    for moved_from in (date(2020, 5, 4), date(2022, 5, 30)):
        assert lse_market_status(moved_from) == "open"
        assert lse_market_reason(moved_from) is None


def test_the_2022_december_pair_is_labelled_the_way_gov_uk_labels_it() -> None:
    """The date set is right under either labelling, so only names catch this.

    Christmas Day 2022 fell on the Sunday and Boxing Day on the Monday, so Boxing
    Day is NOT moved and the Christmas substitute lands on the Tuesday --
    ``next_monday`` / ``next_monday_or_tuesday`` assign them the other way round.
    """
    assert lse_market_reason(date(2022, 12, 26)) == "Boxing Day"
    assert lse_market_reason(date(2022, 12, 27)) == "Christmas Day"


@pytest.mark.parametrize(
    ("day", "expected"),
    [
        (date(2026, 8, 31), "closed"),  # Summer bank holiday
        (date(2026, 4, 3), "closed"),  # Good Friday
        (date(2026, 4, 6), "closed"),  # Easter Monday
        (date(2026, 5, 1), "open"),  # ⚠ the UK has no 1 May holiday; most of Europe does
        (date(2026, 12, 24), "half_day"),
        (date(2026, 12, 31), "half_day"),
        (date(2026, 12, 25), "closed"),
        (date(2026, 12, 28), "closed"),  # Boxing Day substitute (26th was a Saturday)
        (date(2026, 9, 19), "closed"),  # a Saturday
    ],
)
def test_status_on_the_dates_the_published_calendar_names(day: date, expected: str) -> None:
    assert lse_market_status(day) == expected


def test_no_december_eve_is_ever_a_closure_so_the_closure_wins_rule_is_inert_here() -> None:
    """⚠ The UK has NO half-day/closure collision, and that is a consequence of the
    forward-substitute rule rather than a coincidence: Christmas and Boxing Day
    substitutes reach 28 December at the latest, and a New Year substitute lands in
    January, so 24 and 31 December can never be a full closure.

    ⚠⚠ This is where the NYSE analogy actively misleads and it caught the author:
    NYSE really did close Friday 2021-12-24, because ``nearest_workday`` moves a
    Saturday Christmas BACK. The UK moves it FORWARD to Monday the 27th and leaves
    the 24th an ordinary half day. Measured over the whole horizon rather than
    reasoned about; the closure-wins subtraction stays in the module because
    "inert today" and "safe to delete" are different claims.
    """
    for year in range(VERIFIED_FROM_YEAR, VERIFIED_THROUGH_YEAR + 1):
        closures = lse_market_specials(year).full_closures
        assert date(year, 12, 24) not in closures
        assert date(year, 12, 31) not in closures
    assert lse_market_status(date(2021, 12, 24)) == "half_day"
    assert lse_market_status(date(2021, 12, 27)) == "closed"


def test_the_2025_half_days_match_the_lse_published_calendar() -> None:
    """Cross-source: the LSEG *Turquoise Trading Calendar 2025* marks exactly
    ``Wed 24-Dec-25`` and ``Wed 31-Dec-25`` as Early Close in its London Stock
    Exchange column, and nothing else in the year. The bank-holiday population is
    checked against GOV.UK; the half-day population has no such machine-readable
    source, so this is the one year where an LSE document pins it directly."""
    assert lse_market_specials(2025).half_days == frozenset({date(2025, 12, 24), date(2025, 12, 31)})


def test_a_year_outside_the_verified_horizon_raises_rather_than_guessing() -> None:
    """Fail closed, loudly. Outside the horizon the rules may be missing a royal or
    commemorative closure, and an un-transcribed closure renders as a regular
    session -- i.e. it fails OPEN, which is the direction that must not happen."""
    for bad in (VERIFIED_FROM_YEAR - 1, VERIFIED_THROUGH_YEAR + 1):
        with pytest.raises(LseCalendarHorizonError, match="outside the verified GOV.UK horizon"):
            lse_market_status(date(bad, 6, 15))


def test_the_session_window_is_inclusive_at_the_open_and_exclusive_at_the_close() -> None:
    """A submission at the closing bell refuses -- the NYSE gate's shape."""
    day = date(2026, 9, 16)  # an ordinary Wednesday
    assert lse_market_status(day) == "open"
    assert lse_session_is_open(datetime.combine(day, datetime.min.time(), _LONDON).replace(hour=8)) is True
    assert lse_session_is_open(datetime.combine(day, datetime.min.time(), _LONDON).replace(hour=7, minute=59)) is False
    assert lse_session_is_open(datetime.combine(day, datetime.min.time(), _LONDON).replace(hour=16, minute=29)) is True
    assert lse_session_is_open(datetime.combine(day, datetime.min.time(), _LONDON).replace(hour=16, minute=30)) is False


def test_the_window_tracks_bst_and_is_not_fixed_gmt() -> None:
    """⚠⚠ The failure this pins: treating London as permanently GMT.

    The same 08:00-16:30 LOCAL window is 07:00-15:30 UTC in British Summer Time and
    08:00-16:30 UTC in winter. A fixed-GMT implementation is wrong by an hour for
    about seven months of the year, and right for the other five -- which is the
    shape that survives a casually-chosen test date.
    """
    # 2026-07-15, BST (UTC+1): open at 07:00 UTC, shut at 15:30 UTC.
    assert lse_session_is_open(datetime(2026, 7, 15, 7, 0, tzinfo=UTC)) is True
    assert lse_session_is_open(datetime(2026, 7, 15, 6, 59, tzinfo=UTC)) is False
    assert lse_session_is_open(datetime(2026, 7, 15, 15, 29, tzinfo=UTC)) is True
    assert lse_session_is_open(datetime(2026, 7, 15, 15, 30, tzinfo=UTC)) is False
    # 2026-01-14, GMT (UTC+0): open at 08:00 UTC, shut at 16:30 UTC.
    assert lse_session_is_open(datetime(2026, 1, 14, 8, 0, tzinfo=UTC)) is True
    assert lse_session_is_open(datetime(2026, 1, 14, 7, 59, tzinfo=UTC)) is False
    assert lse_session_is_open(datetime(2026, 1, 14, 16, 29, tzinfo=UTC)) is True
    assert lse_session_is_open(datetime(2026, 1, 14, 16, 30, tzinfo=UTC)) is False


def test_the_half_day_cutoff_is_1230_london() -> None:
    """2026-12-24 is a Thursday. December is GMT, so 12:30 London == 12:30 UTC ==
    the "12:30GMT" of the LSE service announcement -- asserted rather than assumed
    to coincide."""
    assert lse_session_is_open(datetime(2026, 12, 24, 12, 29, tzinfo=UTC)) is True
    assert lse_session_is_open(datetime(2026, 12, 24, 12, 30, tzinfo=UTC)) is False
    # …and the same clock on an ordinary day is still open, so the assertion above
    # is about the half day and not about 12:30 being outside the window.
    assert lse_session_is_open(datetime(2026, 12, 23, 12, 30, tzinfo=UTC)) is True


def test_a_naive_instant_raises_rather_than_being_read_as_host_local_time() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        lse_session_is_open(datetime(2026, 9, 16, 12, 0))


def test_a_closed_day_is_closed_at_every_hour() -> None:
    """The weekday-hours test and the closure test are independent; without this a
    calendar that ignored closures entirely would still pass both."""
    boxing_substitute = datetime(2026, 12, 28, 11, 0, tzinfo=UTC)
    assert lse_market_status(boxing_substitute.date()) == "closed"
    assert lse_session_is_open(boxing_substitute) is False


def test_a_suppression_that_matches_nothing_is_rejected() -> None:
    """The import-time integrity check, exercised -- otherwise it is a guard nobody
    has ever seen fire.

    ⚠ A typo'd suppression fails SILENTLY without it: the date it names is not
    produced by the rules, so removing it does nothing, and the holiday it was
    meant to reopen stays closed with no signal at all.

    ⚠ It only recomputes the years the suppressions fall in, which is equivalent
    because ``_scheduled_closure_names(Y)`` keeps only year-``Y`` dates. This
    asserts the narrowing still catches a bad entry, including one in a year no
    other suppression touches.
    """
    from app.services import lse_market_calendar as module

    for bad in (date(2020, 5, 5), date(2024, 7, 4)):  # neither is an E&W bank holiday
        original = module._RULE_SUPPRESSIONS
        module._RULE_SUPPRESSIONS = {**original, bad: "typo"}  # type: ignore[misc]
        try:
            with pytest.raises(AssertionError, match="suppression matches no rule-derived date"):
                module._assert_override_integrity()
        finally:
            module._RULE_SUPPRESSIONS = original  # type: ignore[misc]


def test_the_reasons_map_is_total_over_the_special_dates() -> None:
    """A special date with no reason renders as an unexplained refusal, and a reason
    for a date that is not special is the residue of a suppression applied too late.
    Both are ruled out by construction; this asserts the construction."""
    for year in range(VERIFIED_FROM_YEAR, VERIFIED_THROUGH_YEAR + 1):
        specials = lse_market_specials(year)
        assert not (specials.full_closures & specials.half_days)
        assert set(specials.reasons) == specials.full_closures | specials.half_days
