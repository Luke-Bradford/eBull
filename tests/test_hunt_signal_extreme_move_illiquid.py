"""#3387 — hunt 1's signal: the illiquid third's 5-session returns (spec "The trial")."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import pytest

from app.services import hunt_view as hv
from app.services.hunt_signals import extreme_move_illiquid as sig

CONSTANTS: dict[str, Any] = {
    "history": 26,
    "amihud_sessions": 20,
    "formation": 5,
    "min_close": 5.0,
    "illiquid_divisor": 3,
}
SESSIONS = tuple((d.year, d.month, d.day, d.weekday()) for d in (date(2000, 1, 3) + timedelta(i) for i in range(60)))
T = 40


def _bars(closes: list[float], volume: float | list[float], *, first: int = T - 25) -> hv.Bars:
    volumes = volume if isinstance(volume, list) else [volume] * len(closes)
    return hv.Bars(
        ordinals=tuple(range(first, first + len(closes))),
        open=tuple(closes),
        high=tuple(c * 1.01 for c in closes),
        low=tuple(c * 0.99 for c in closes),
        close=tuple(closes),
        volume=tuple(volumes),
    )


def _zigzag(level: float, *, swing: float = 0.02, last: float | None = None) -> list[float]:
    """26 closes alternating ±swing around ``level``; the last one set to ``last``."""
    closes = [level * (1.0 + swing * (1 if i % 2 else -1)) for i in range(26)]
    if last is not None:
        closes[-1] = last
    return closes


def _score(series: dict[int, hv.Bars], *, t: int = T, constants: dict[str, Any] | None = None) -> dict[int, float]:
    closes = {sid: bars.close[list(bars.ordinals).index(t)] for sid, bars in series.items()}
    view = hv.rebased_view(t, series, closes, sessions=SESSIONS, dividends={})
    return sig.score(t, view, constants or CONSTANTS)


def test_the_score_is_the_five_session_return() -> None:
    closes = _zigzag(10.0, last=8.0)
    scores = _score({1: _bars(closes, 1_000.0)})
    assert scores == {1: pytest.approx(8.0 / closes[-6] - 1.0, rel=1e-12)}


def test_only_the_most_illiquid_third_is_scored_ties_by_series_id() -> None:
    # Same price path, so Amihud ∝ 1 / volume: lower volume = more illiquid.
    closes = _zigzag(10.0)
    volumes = {1: 100.0, 2: 200.0, 3: 100.0, 4: 400.0, 5: 500.0, 6: 600.0, 7: 700.0}
    scores = _score({sid: _bars(closes, v) for sid, v in volumes.items()})
    # N = 7 → ⌈7/3⌉ = 3: the tie {1, 3} at the top, then 2.
    assert set(scores) == {1, 2, 3}


def test_the_domain_drops_gaps_short_history_non_positive_inputs_and_sub_five_closes() -> None:
    good = _zigzag(10.0)
    gappy = hv.Bars(
        ordinals=(*range(T - 26, T - 13), *range(T - 12, T + 1)),
        open=tuple(good),
        high=tuple(c * 1.01 for c in good),
        low=tuple(c * 0.99 for c in good),
        close=tuple(good),
        volume=(50.0,) * 26,
    )
    zero_volume = [50.0] * 26
    zero_volume[10] = 0.0
    series = {
        1: _bars(good, 50.0),  # the only name in the domain
        2: gappy,  # 26 bars, but not on the 26 most recent sessions
        3: _bars(good[1:], 50.0, first=T - 24),  # 25 bars
        4: _bars(good, zero_volume),  # a zero volume inside the window
        5: _bars(_zigzag(4.9), 50.0),  # close_t < $5
    }
    assert set(_score(series)) == {1}


def test_a_split_after_t_cannot_move_a_score() -> None:
    """Look-ahead probe: bars after t (here a 2:1 split on the ratio basis) change nothing."""
    closes = _zigzag(10.0, last=9.0)
    before = _score({1: _bars(closes, 100.0), 2: _bars(_zigzag(20.0), 100.0)})
    later = [c / 2.0 for c in (*closes, 4.6, 4.7)]  # ratio basis re-denominated after a later split
    loaded = hv.Bars(
        ordinals=tuple(range(T - 25, T + 3)),
        open=tuple(later),
        high=tuple(c * 1.01 for c in later),
        low=tuple(c * 0.99 for c in later),
        close=tuple(later),
        volume=tuple([200.0] * 28),
    )
    view = hv.rebased_view(
        T, {1: loaded, 2: _bars(_zigzag(20.0), 100.0)}, {1: 9.0, 2: _zigzag(20.0)[-1]}, sessions=SESSIONS, dividends={}
    )
    assert sig.score(T, view, CONSTANTS) == pytest.approx(before, rel=1e-12)


@pytest.mark.parametrize(
    "bad",
    [
        {**CONSTANTS, "extra": 1},
        {k: v for k, v in CONSTANTS.items() if k != "history"},
        {**CONSTANTS, "min_close": 5},
        {**CONSTANTS, "history": 20},
        {**CONSTANTS, "illiquid_divisor": True},
    ],
)
def test_malformed_constants_refuse(bad: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        _score({1: _bars(_zigzag(10.0), 100.0)}, constants=bad)
