"""#3471 slice 1b-iii — the pure helpers of ``app/services/ai_trial_pack_reader`` (no DB)."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from app.providers.market_data import IntradayBar
from app.services import ai_trial_pack_reader as r

AS_OF = datetime(2026, 10, 2, 23, 30, tzinfo=UTC)


@pytest.mark.parametrize(
    ("session", "close"),
    [
        (date(2026, 10, 2), datetime(2026, 10, 2, 20, 0, tzinfo=UTC)),  # EDT: 16:00 ET
        (date(2026, 12, 1), datetime(2026, 12, 1, 21, 0, tzinfo=UTC)),  # EST: 16:00 ET
        (date(2026, 11, 27), datetime(2026, 11, 27, 18, 0, tzinfo=UTC)),  # half day: 13:00 EST
    ],
)
def test_session_close(session: date, close: datetime) -> None:
    assert r.session_close_utc(session) == close


def test_session_close_refuses_a_closed_day() -> None:
    with pytest.raises(ValueError, match="not a NYSE session"):
        r.session_close_utc(date(2026, 10, 3))  # Saturday


def test_next_session_and_session_count() -> None:
    assert r.next_us_session(date(2026, 10, 2)) == date(2026, 10, 5)  # Friday → Monday
    assert r.next_us_session(date(2026, 11, 25)) == date(2026, 11, 27)  # skips Thanksgiving
    assert r.us_sessions_between(date(2026, 11, 23), date(2026, 11, 29)) == 4
    assert r.us_sessions_between(date(2026, 10, 5), date(2026, 10, 4)) == 0


def test_filing_title_is_built_from_structured_fields() -> None:
    assert r.filing_title("8-K", date(2026, 9, 25), ["2.02", "9.01"], date(2026, 9, 24)) == (
        "8-K filed 2026-09-25 (items 2.02, 9.01) (period 2026-09-24)"
    )
    assert r.filing_title("4", date(2026, 9, 24), None, None) == "4 filed 2026-09-24"


def _bar(t: datetime, *, close: str = "10", volume: int | None = 100) -> IntradayBar:
    return IntradayBar(t, Decimal("10"), Decimal("11"), Decimal("9"), Decimal(close), volume)


def _series(start: datetime, n: int) -> list[IntradayBar]:
    return [_bar(start + timedelta(hours=4 * k)) for k in range(n)]


def test_select_intraday_keeps_completed_bars_inside_the_window() -> None:
    start = AS_OF - timedelta(days=35)
    got = r.select_intraday(_series(start, 220), as_of=AS_OF, requested=400, sessions_in_window=20)
    assert not isinstance(got, str)
    assert got[0].timestamp >= AS_OF - r.INTRADAY_WINDOW
    assert got[-1].timestamp + r.INTRADAY_BAR_LENGTH <= AS_OF
    assert [b.timestamp for b in got] == sorted(b.timestamp for b in got)
    # The bar that opened 2h before as_of has not closed, so it is dropped.
    open_bar = _bar(AS_OF - timedelta(hours=2))
    again = r.select_intraday([*_series(start, 200), open_bar], as_of=AS_OF, requested=400, sessions_in_window=20)
    assert not isinstance(again, str) and open_bar not in again


@pytest.mark.parametrize(
    ("bars", "requested", "sessions", "reason"),
    [
        ([], 400, 20, "intraday_empty"),
        # Filled the request and still starts inside the window: the window is cut short.
        (_series(AS_OF - timedelta(days=10), 50), 50, 5, "intraday_truncated"),
        ([_bar(AS_OF - timedelta(days=1), close="NaN")], 400, 1, "intraday_non_finite"),
        ([_bar(AS_OF - timedelta(days=1), volume=-1)], 400, 1, "intraday_non_finite"),
        (_series(AS_OF - timedelta(days=5), 10), 400, 20, "intraday_too_few_bars"),
        ([_bar(AS_OF - timedelta(days=1)), _bar(AS_OF - timedelta(days=1))], 400, 1, "intraday_duplicate_bar"),
    ],
)
def test_select_intraday_refusals(bars: list[IntradayBar], requested: int, sessions: int, reason: str) -> None:
    assert r.select_intraday(bars, as_of=AS_OF, requested=requested, sessions_in_window=sessions) == reason
