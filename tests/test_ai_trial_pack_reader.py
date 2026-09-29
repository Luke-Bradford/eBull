"""#3471 slice 1b-iii — the pure helpers of ``app/services/ai_trial_pack_reader`` (no DB)."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from app.providers.market_data import IntradayBar
from app.services import ai_trial_pack_reader as r
from app.services.ai_trial_pack import ShortlistCandidate
from app.services.indicator_series import BarSeries

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


def test_a_shared_symbol_drops_every_copy() -> None:
    def cand(i: int, symbol: str) -> ShortlistCandidate:
        return ShortlistCandidate(i, symbol, Decimal("10"), Decimal("10.01"), AS_OF, 1.0, None)

    kept = r.drop_symbol_collisions([cand(1, "AAA"), cand(2, "BBB"), cand(3, "AAA")])
    assert [c.instrument_id for c in kept] == [2]


# --- slice 3b: the pack never spans an unresolved price_series_break -------------------------


def _daily(n: int, start: date = date(2026, 1, 5)) -> BarSeries:
    dates = tuple(start + timedelta(days=i) for i in range(n))
    rows = tuple(
        {"open": Decimal(i), "high": Decimal(i), "low": Decimal(i), "close": Decimal(i), "volume": 1} for i in range(n)
    )
    return BarSeries(dates=dates, rows=rows)  # type: ignore[arg-type]


def test_latest_segment_cuts_at_the_last_break_on_or_before_the_session() -> None:
    series = _daily(10)
    last = series.dates[7]
    # Breaks on day 2 and day 5 (first bar at the new scale); one after the session is ignored.
    seg = r.latest_segment(
        series, last_session=last, unresolved_breaks=[series.dates[2], series.dates[5], series.dates[9]]
    )
    assert seg.dates == series.dates[5:8]


def test_latest_segment_without_breaks_is_every_bar_up_to_the_session() -> None:
    series = _daily(10)
    assert r.latest_segment(series, last_session=series.dates[6], unresolved_breaks=[]).dates == series.dates[:7]


def test_latest_segment_break_on_the_last_bar_leaves_one_bar() -> None:
    series = _daily(10)
    seg = r.latest_segment(series, last_session=series.dates[9], unresolved_breaks=[series.dates[9]])
    assert seg.dates == (series.dates[9],)


def test_latest_segment_with_no_bar_by_the_session_is_empty() -> None:
    series = _daily(5, start=date(2026, 2, 1))
    assert r.latest_segment(series, last_session=date(2026, 1, 1), unresolved_breaks=[]).dates == ()


def test_read_bars_refuses_when_the_break_map_moves_during_the_read(monkeypatch: pytest.MonkeyPatch) -> None:
    series = _daily(80)
    maps = iter([{}, {7: (series.dates[40],)}])  # a break detected between the two reads
    monkeypatch.setattr(r, "load_unresolved_breaks", lambda conn, ids: next(maps))
    monkeypatch.setattr(r, "load_masked_bars", lambda conn, iid: type("M", (), {"series": series})())
    with pytest.raises(r.PackReadRace):
        r.read_bars(object(), [7], last_session=series.dates[-1])  # type: ignore[arg-type]


def test_read_bars_cuts_each_name_at_its_own_break(monkeypatch: pytest.MonkeyPatch) -> None:
    series = _daily(80)
    monkeypatch.setattr(r, "load_unresolved_breaks", lambda conn, ids: {7: (series.dates[40],)})
    monkeypatch.setattr(r, "load_masked_bars", lambda conn, iid: type("M", (), {"series": series})())
    out = r.read_bars(object(), [7, 8], last_session=series.dates[-1])  # type: ignore[arg-type]
    assert out[7][0] == list(series.dates[40:]) and out[8][0] == list(series.dates)
