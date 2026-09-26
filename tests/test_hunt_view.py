"""#3385 slice 3c — the signal view: bars ≤ t, rebased to the as-traded close on t."""

from __future__ import annotations

import hashlib
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


def test_a_split_after_t_cannot_move_the_levels_the_signal_sees() -> None:
    view = hv.rebased_view(2, {7: RATIO}, {7: AS_TRADED_CLOSE[2]})
    bars = view.series[7]
    assert tuple(bars.close) == pytest.approx((100.0, 102.0, 104.0))  # the as-traded levels
    assert tuple(bars.volume) == pytest.approx((500.0, 500.0, 500.0))  # price × volume unchanged
    assert bars.close[-1] * bars.volume[-1] == pytest.approx(RATIO.close[2] * RATIO.volume[2])


def test_after_the_split_the_view_is_split_continuous_ending_at_the_as_traded_close() -> None:
    bars = hv.rebased_view(4, {7: RATIO}, {7: AS_TRADED_CLOSE[4]}).series[7]
    assert tuple(bars.close) == pytest.approx((50.0, 51.0, 52.0, 52.0, 53.0))


def test_look_ahead_probe_a_signal_indexing_past_t_finds_nothing() -> None:
    def peeking_signal(t: int, view: hv.SignalView, _constants: dict) -> dict[int, float]:
        bars = view.series[7]
        return {7: bars.close[bars.ordinals.index(t) + 1]}  # the bar after t

    view = hv.rebased_view(2, {7: RATIO}, {7: AS_TRADED_CLOSE[2]})
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
        hv.rebased_view(2, {7: gappy}, {7: 1.0})


def test_a_missing_or_invalid_as_traded_close_refuses() -> None:
    with pytest.raises(ValueError):
        hv.rebased_view(2, {7: RATIO}, {})
    with pytest.raises(ValueError):
        hv.rebased_view(2, {7: RATIO}, {7: 0.0})


def test_a_view_cannot_be_built_with_a_series_ending_off_t() -> None:
    view = hv.rebased_view(2, {7: RATIO}, {7: AS_TRADED_CLOSE[2]})
    with pytest.raises(ValueError):
        hv.SignalView(t=3, series=view.series)


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
