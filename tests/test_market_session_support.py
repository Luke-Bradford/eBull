"""The venue-support allow-list is shared by the core submission and selection
paths (#2312 / #2603); these pin the direction it fails in."""

from datetime import UTC, datetime

import pytest

from app.services.market_session_support import (
    SESSION_SUPPORTED_ASSET_CLASSES,
    session_support_reason,
    venue_calendar_for,
    venue_local_now,
    venue_session_is_open,
)


def test_the_only_supported_class_is_the_one_with_both_halves() -> None:
    """⚠⚠ A CALENDAR IS NOT ADMISSION, and this is where that is pinned.

    As of #2312 (2026-09-16) there are TWO calendars -- NYSE and LSE -- and the
    supported set is still ``{"us_equity"}``, because admission is derived as
    "has a calendar AND has halt coverage" and ``strategy_core_preflight``'s halt
    reads are hardcoded to ``nasdaq_trader_rss``. Adding the LSE calendar must not
    move this set; if it ever does, an LSE submission is being admitted on a halt
    feed that carries no ``.L`` identity, i.e. on a gate that fails open.
    """
    assert SESSION_SUPPORTED_ASSET_CLASSES == frozenset({"us_equity"})
    assert session_support_reason("us_equity") is None
    assert venue_calendar_for("uk_equity") is not None  # the calendar exists…
    assert session_support_reason("uk_equity") is not None  # …and admits nothing


@pytest.mark.parametrize(
    "asset_class",
    # `uk_equity` is the live one: two of three #2833 core candidates sit there.
    # The rest are real `exchanges.asset_class` values, plus a class added AFTER
    # this code was written — an allow-list must put that on the refuse side.
    ["uk_equity", "eu_equity", "asia_equity", "mena_equity", "crypto", "commodity", "fx", "index", "unknown", None],
)
def test_every_unsupported_or_unknown_venue_class_refuses(asset_class: str | None) -> None:
    reason = session_support_reason(asset_class)
    assert reason is not None
    assert reason.startswith(f"asset_class={asset_class!r}")


def test_the_detail_prefix_is_stable_for_the_refusal_that_records_it() -> None:
    """``core_unsupported_market_session`` records this string as its ``detail``.
    The prefix is the part that identifies WHICH class was seen, so it is pinned
    separately from the explanatory tail."""
    assert session_support_reason("uk_equity").startswith("asset_class='uk_equity'")  # type: ignore[union-attr]


def test_the_two_refusals_are_distinguishable_because_they_are_different_work() -> None:
    """ "No calendar at all" and "calendar, no halt evidence" are different amounts
    of remaining work. Reporting the first when the second is true points the next
    session at work that is already done -- which is the operator-visible payoff of
    #2312 and the reason the message branches rather than staying one string."""
    uk = session_support_reason("uk_equity")
    eu = session_support_reason("eu_equity")
    assert uk is not None and eu is not None
    assert "has a trading-session calendar" in uk
    assert "nasdaq_trader_rss" in uk
    assert "has no trading-session calendar in this repo" in eu


def test_a_venue_with_no_calendar_is_never_open() -> None:
    """An unanswerable session question is a CLOSED one on a submission path."""
    assert venue_session_is_open("eu_equity", datetime(2026, 9, 16, 12, 0, tzinfo=UTC)) is False
    assert venue_session_is_open(None, datetime(2026, 9, 16, 12, 0, tzinfo=UTC)) is False


def test_the_dispatch_gives_each_venue_its_own_clock() -> None:
    """13:00 UTC on an ordinary Wednesday: London is open (14:00 BST, before the
    16:30 close) and New York is NOT (09:00 ET, before the 09:30 open). A single
    shared clock cannot produce both answers, which is what makes this the
    regression test for the NY hardcode #2312 removed."""
    midday = datetime(2026, 9, 16, 13, 0, tzinfo=UTC)
    assert venue_session_is_open("uk_equity", midday) is True
    assert venue_session_is_open("us_equity", midday) is False
    assert venue_local_now("uk_equity", midday).hour == 14  # BST
    assert venue_local_now("us_equity", midday).hour == 9  # EDT


def test_us_equity_keeps_the_boundaries_it_had_before_the_venue_dispatch() -> None:
    """The equivalence evidence for the refactor: inclusive at 09:30 ET, exclusive
    at 16:00 ET, closed on an NYSE holiday. ⚠ 2026-07-03 is the OBSERVED
    Independence Day closure (Jul 4 is a Saturday) -- a case where NYSE moves a
    holiday BACK, which the LSE calendar must never do."""
    assert venue_session_is_open("us_equity", datetime(2026, 9, 16, 13, 30, tzinfo=UTC)) is True  # 09:30 ET
    assert venue_session_is_open("us_equity", datetime(2026, 9, 16, 13, 29, tzinfo=UTC)) is False
    assert venue_session_is_open("us_equity", datetime(2026, 9, 16, 19, 59, tzinfo=UTC)) is True  # 15:59 ET
    assert venue_session_is_open("us_equity", datetime(2026, 9, 16, 20, 0, tzinfo=UTC)) is False
    assert venue_session_is_open("us_equity", datetime(2026, 7, 3, 15, 0, tzinfo=UTC)) is False


def test_an_unverified_year_refuses_rather_than_raising_into_the_caller() -> None:
    """The LSE calendar raises outside its verified horizon; the submission-path
    helper must turn that into a REFUSAL, not an exception a preflight caller has
    to catch. Fail closed, and stay a predicate."""
    assert venue_session_is_open("uk_equity", datetime(2035, 9, 19, 12, 0, tzinfo=UTC)) is False


def test_a_naive_instant_raises_rather_than_being_read_as_host_local_time() -> None:
    for call in (venue_session_is_open, venue_local_now):
        with pytest.raises(ValueError, match="timezone-aware"):
            call("us_equity", datetime(2026, 9, 16, 12, 0))
