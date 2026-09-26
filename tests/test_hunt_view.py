"""#3385 slice 3c — the signal view: bars ≤ t, rebased to the as-traded close on t."""

from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping
from datetime import date, timedelta
from pathlib import Path

import pytest

from app.services import hunt_harness as hh
from app.services import hunt_view as hv


def _bars(closes: list[float], start: int = 0) -> hv.Bars:
    ordinals = tuple(range(start, start + len(closes)))
    return hv.Bars(
        ordinals=ordinals,
        open=tuple(closes),
        high=tuple(c * 1.01 for c in closes),
        low=tuple(c * 0.99 for c in closes),
        close=tuple(closes),
        volume=tuple(1000.0 for _ in closes),
    )


# A 2:1 split between sessions 2 and 3. As traded: 100, 102, 104, 52, 53.
# Ratio basis, re-denominated to the terminal unit: 50, 51, 52, 52, 53.
RATIO = _bars([50.0, 51.0, 52.0, 52.0, 53.0])
AS_TRADED_CLOSE = {0: 100.0, 1: 102.0, 2: 104.0, 3: 52.0, 4: 53.0}
SESSIONS = tuple(
    (d.year, d.month, d.day, d.weekday()) for d in (date(1990, 1, 2) + timedelta(days=i) for i in range(6))
)
# Ex-dates on the ratio basis: 0.5 on session 1 (as traded 1.0, before the split), 0.3 on
# session 3 (after it), and 0.4 on session 5, a session with no bar and after every t below.
PAID = hv.Dividends((1, 3, 5), (0.5, 0.3, 0.4))


def _view(
    t: int,
    ratio_bars: Mapping[int, hv.Bars],
    as_traded_close_on_t: Mapping[int, float],
    dividends: Mapping[int, hv.Dividends] | None = None,
) -> hv.SignalView:
    return hv.rebased_view(t, ratio_bars, as_traded_close_on_t, sessions=SESSIONS, dividends=dividends or {})


def test_a_split_after_t_cannot_move_the_levels_the_signal_sees() -> None:
    view = _view(2, {7: RATIO}, {7: AS_TRADED_CLOSE[2]})
    bars = view.series[7]
    assert tuple(bars.close) == pytest.approx((100.0, 102.0, 104.0))  # the as-traded levels
    assert tuple(bars.volume) == pytest.approx((500.0, 500.0, 500.0))  # price × volume unchanged
    assert bars.close[-1] * bars.volume[-1] == pytest.approx(RATIO.close[2] * RATIO.volume[2])


def test_after_the_split_the_view_is_split_continuous_ending_at_the_as_traded_close() -> None:
    bars = _view(4, {7: RATIO}, {7: AS_TRADED_CLOSE[4]}).series[7]
    assert tuple(bars.close) == pytest.approx((50.0, 51.0, 52.0, 52.0, 53.0))


def test_look_ahead_probe_a_signal_indexing_past_t_finds_nothing() -> None:
    def peeking_signal(t: int, view: hv.SignalView, _constants: dict) -> dict[int, float]:
        bars = view.series[7]
        return {7: bars.close[bars.ordinals.index(t) + 1]}  # the bar after t

    view = _view(2, {7: RATIO}, {7: AS_TRADED_CLOSE[2]})
    assert max(view.series[7].ordinals) == 2
    with pytest.raises(IndexError):
        peeking_signal(2, view, {})


def test_a_series_without_a_bar_on_t_is_not_eligible() -> None:
    gappy = hv.Bars(
        ordinals=(0, 1, 3),
        open=(1.0, 1.0, 1.0),
        high=(1.0, 1.0, 1.0),
        low=(1.0, 1.0, 1.0),
        close=(1.0, 1.0, 1.0),
        volume=(1.0, 1.0, 1.0),
    )
    with pytest.raises(ValueError):
        _view(2, {7: gappy}, {7: 1.0})


def test_a_missing_or_invalid_as_traded_close_refuses() -> None:
    with pytest.raises(ValueError):
        _view(2, {7: RATIO}, {})
    with pytest.raises(ValueError):
        _view(2, {7: RATIO}, {7: 0.0})


def test_a_view_cannot_be_built_with_a_series_ending_off_t() -> None:
    view = _view(2, {7: RATIO}, {7: AS_TRADED_CLOSE[2]})
    with pytest.raises(ValueError):
        hv.SignalView(t=3, series=view.series, dates=hv.Head(SESSIONS, 4))


def test_a_prefix_is_a_lazy_bounded_scaled_sequence() -> None:
    prefix = hv.Prefix((1.0, 2.0, 3.0, 4.0), 3, 10.0)
    assert len(prefix) == 3
    assert (prefix[0], prefix[-1]) == (10.0, 30.0)
    assert prefix[1:] == (20.0, 30.0)
    assert prefix[-5:] == (10.0, 20.0, 30.0)
    assert list(prefix) == [10.0, 20.0, 30.0]
    with pytest.raises(IndexError):
        prefix[3]
    with pytest.raises(IndexError):
        prefix[-4]


def test_bars_validate_shape_and_order() -> None:
    with pytest.raises(ValueError):
        hv.Bars(ordinals=(0, 1), open=(1.0,), high=(1.0,), low=(1.0,), close=(1.0,), volume=(1.0,))
    with pytest.raises(ValueError):
        hv.Bars(ordinals=(1, 1), open=(1.0,) * 2, high=(1.0,) * 2, low=(1.0,) * 2, close=(1.0,) * 2, volume=(1.0,) * 2)


def test_the_view_code_is_part_of_the_harness_model_id() -> None:
    expected = hashlib.sha256(Path(str(hv.__file__)).read_bytes()).hexdigest()
    assert hh._model_constants()["model_code_sha256"]["app.services.hunt_view"] == expected


# ---------------------------------------------------------------------------
# #3386 slice 2 — dates and ex-dates ≤ t
# ---------------------------------------------------------------------------


def test_the_dates_end_at_t() -> None:
    view = _view(2, {7: RATIO}, {7: AS_TRADED_CLOSE[2]})
    assert tuple(view.dates) == SESSIONS[:3]
    assert (view.dates[-1], view.dates[0]) == (SESSIONS[2], SESSIONS[0])
    assert view.dates[1:] == SESSIONS[1:3]
    assert view.dates[::-1] == tuple(reversed(SESSIONS[:3]))
    with pytest.raises(IndexError):
        view.dates[3]
    with pytest.raises(IndexError):
        view.dates[-4]


def test_ex_dates_after_t_are_invisible_and_one_on_t_is_seen() -> None:
    before = _view(2, {7: RATIO}, {7: AS_TRADED_CLOSE[2]}, {7: PAID}).series[7]
    assert tuple(before.dividend_ordinals) == (1,)
    on_t = _view(3, {7: RATIO}, {7: AS_TRADED_CLOSE[3]}, {7: PAID}).series[7]
    assert tuple(on_t.dividend_ordinals) == (1, 3)
    for view in (before, on_t):
        with pytest.raises(IndexError):
            view.dividend_ordinals[len(view.dividend_ordinals)]
        with pytest.raises(IndexError):
            view.dividend_amounts[-len(view.dividend_amounts) - 1]
    assert on_t.dividend_amounts[-1:] == pytest.approx((0.3,))


def test_a_dividend_on_a_session_without_a_bar_is_kept() -> None:
    last = _view(4, {7: RATIO}, {7: AS_TRADED_CLOSE[4]}, {7: hv.Dividends((1, 3, 4), (0.5, 0.3, 0.4))}).series[7]
    assert tuple(last.dividend_ordinals) == (1, 3, 4)
    gap = hv.Dividends((2,), (0.1,))  # the series below has no bar on session 2
    gappy = hv.Bars((0, 1, 3), (1.0,) * 3, (1.0,) * 3, (1.0,) * 3, (1.0,) * 3, (1.0,) * 3)
    assert tuple(_view(3, {7: gappy}, {7: 1.0}, {7: gap}).series[7].dividend_ordinals) == (2,)


def test_amounts_rebase_with_the_prices_so_a_later_split_cannot_move_them() -> None:
    # On t = 2 (before the 2:1 split) the as-traded dividend on session 1 is 1.0.
    before = _view(2, {7: RATIO}, {7: AS_TRADED_CLOSE[2]}, {7: PAID}).series[7]
    assert tuple(before.dividend_amounts) == pytest.approx((1.0,))
    assert before.dividend_amounts[0] / before.close[1] == pytest.approx(0.5 / RATIO.close[1])
    # On t = 4 (after it) the same payment reads on the post-split basis, the yield unchanged.
    after = _view(4, {7: RATIO}, {7: AS_TRADED_CLOSE[4]}, {7: PAID}).series[7]
    assert tuple(after.dividend_amounts) == pytest.approx((0.5, 0.3))
    assert after.dividend_amounts[0] / after.close[1] == pytest.approx(before.dividend_amounts[0] / before.close[1])


def test_a_series_without_dividends_sees_none_and_a_nan_amount_passes_through() -> None:
    bare = _view(2, {7: RATIO}, {7: AS_TRADED_CLOSE[2]}).series[7]
    assert (len(bare.dividend_ordinals), len(bare.dividend_amounts)) == (0, 0)
    nan = _view(2, {7: RATIO}, {7: AS_TRADED_CLOSE[2]}, {7: hv.Dividends((1,), (math.nan,))}).series[7]
    assert math.isnan(nan.dividend_amounts[0])


def test_look_ahead_probe_covers_dates_and_ex_dates() -> None:
    view = _view(2, {7: RATIO}, {7: AS_TRADED_CLOSE[2]}, {7: PAID})
    series = view.series[7]
    for peek in (
        lambda: view.dates[view.t + 1],
        lambda: series.dividend_ordinals[len(series.dividend_ordinals)],
        lambda: series.dividend_amounts[len(series.dividend_amounts)],
    ):
        with pytest.raises(IndexError):
            peek()
    assert view.dates[view.t + 1 :] == ()
    assert series.dividend_ordinals[5:] == ()


def test_a_t_outside_the_sessions_refuses() -> None:
    with pytest.raises(ValueError, match="not a loaded session"):
        hv.rebased_view(6, {7: RATIO}, {7: 1.0}, sessions=SESSIONS, dividends={})


def test_dividends_validate_shape_and_order() -> None:
    with pytest.raises(ValueError):
        hv.Dividends((1, 2), (0.1,))
    with pytest.raises(ValueError):
        hv.Dividends((2, 2), (0.1, 0.1))


def test_a_date_is_a_plain_int_tuple_not_a_clock() -> None:
    view = _view(2, {7: RATIO}, {7: AS_TRADED_CLOSE[2]})
    assert view.dates[0] == (1990, 1, 2, 1)  # a Tuesday
    assert all(type(part) is int for day in view.dates for part in day)
