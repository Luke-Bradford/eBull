"""#3621 slice 1: the avoidance-filter flags (rmax1_21d with screens and missing values, cutoff, price, seasoning)."""

from __future__ import annotations

import random
from collections.abc import Callable
from datetime import date, timedelta

import pytest

from app.services.avoidance_filters import (
    FILTER_SETS,
    AvoidanceError,
    Filter,
    MaxMissing,
    MaxReading,
    MaxSeries,
    NameInputs,
    flag_formation,
    max_cutoff,
)
from app.services.factor_panel_prices import DailyBar
from scripts import measure_3621_filter_premise as premise

SESSIONS = [
    date(2019, 8, 1) + timedelta(days=i)
    for i in range((date(2019, 12, 31) - date(2019, 8, 1)).days + 1)
    if (date(2019, 8, 1) + timedelta(days=i)).weekday() < 5
]
K = SESSIONS.index(date(2019, 10, 31))  # s(M): October's last session
OCTOBER = [i for i, d in enumerate(SESSIONS) if (d.year, d.month) == (2019, 10)]


def _bars(adj: Callable[[int], float | None], *, close: Callable[[int], float] = lambda _i: 10.0) -> list[DailyBar]:
    out = []
    for i, day in enumerate(SESSIONS):
        value = adj(i)
        if value is not None:
            out.append(DailyBar(day, close(i), value, 1000, stamped=False, usable=True))
    return out


def _read(bars: list[DailyBar]) -> MaxReading:
    return MaxSeries(bars, SESSIONS).at(K)


def _steps(returns: dict[int, float], default: float = 0.001) -> Callable[[int], float]:
    """An adj_close path whose return on session i is ``returns.get(i, default)``."""
    path = [10.0]
    for i in range(1, len(SESSIONS)):
        path.append(path[-1] * (1.0 + returns.get(i, default)))
    return lambda i: path[i]


def test_window_is_s_m_calendar_month_including_the_return_into_its_first_session() -> None:
    first = OCTOBER[0]
    reading = _read(_bars(_steps({first - 1: 0.5, first: 0.07, K + 1: 0.9})))
    assert reading.missing is None
    assert reading.returns == len(OCTOBER)
    assert reading.value == pytest.approx(0.07)


def test_a_return_needs_adjacent_sessions() -> None:
    gaps = set(OCTOBER[5:13])  # 8 missing sessions of October's 23 leave 5 + 9 = 14 adjacent-session returns
    reading = _read(_bars(lambda i: None if i in gaps else _steps({})(i)))
    assert reading.missing is MaxMissing.SHORT
    assert reading.value is None
    assert reading.returns == 14


def test_fifteen_returns_suffice_and_ten_zeros_drop_the_stock_month() -> None:
    nine = {i: 0.0 for i in OCTOBER[:9]}
    assert _read(_bars(_steps(nine))).missing is None
    ten = {i: 0.0 for i in OCTOBER[:10]}
    reading = _read(_bars(_steps(ten)))
    assert (reading.missing, reading.zeros, reading.value) == (MaxMissing.ZERO_HEAVY, 10, None)


def test_extreme_return_screens_and_screened_outranks_short() -> None:
    assert _read(_bars(_steps({OCTOBER[3]: 3.5}))).missing is MaxMissing.SCREENED
    assert _read(_bars(_steps({OCTOBER[3]: -0.95}))).missing is MaxMissing.SCREENED
    sparse = set(OCTOBER[5:20])
    reading = _read(_bars(lambda i: None if i in sparse else _steps({OCTOBER[2]: 3.5})(i)))
    assert reading.returns < 15
    assert reading.missing is MaxMissing.SCREENED


def test_extreme_return_after_s_m_does_not_screen() -> None:
    assert _read(_bars(_steps({K + 1: 3.5}))).missing is None


def _ratio_jump(at: int, *, stamped_at: int | None = None, missing: frozenset[int] = frozenset()) -> list[DailyBar]:
    """close drops by 2/3 from session ``at`` on while adj_close keeps growing: ratio x3."""
    adj = _steps({})
    out = []
    for i, day in enumerate(SESSIONS):
        if i in missing:
            if i == stamped_at:
                out.append(DailyBar(day, 10.0, adj(i), 1000, stamped=True, usable=False))
            continue
        close = 10.0 / 3.0 if i >= at else 10.0
        out.append(DailyBar(day, close, adj(i), 1000, stamped=i == stamped_at, usable=True))
    return out


def test_unstamped_ratio_move_screens_and_a_stamp_in_the_pair_excuses_it() -> None:
    at = OCTOBER[10]
    assert _read(_ratio_jump(at)).missing is MaxMissing.SCREENED
    assert _read(_ratio_jump(at, stamped_at=at)).missing is None


def test_ratio_screen_spans_missing_sessions_and_a_stamp_on_an_unusable_bar_still_excuses() -> None:
    at = OCTOBER[10]
    gap = frozenset({at - 1, at})
    assert _read(_ratio_jump(at, missing=gap)).missing is MaxMissing.SCREENED
    assert _read(_ratio_jump(at, stamped_at=at, missing=gap)).missing is None


def test_ratio_pair_ending_on_the_window_first_session_screens_and_one_ending_after_s_m_does_not() -> None:
    assert _read(_ratio_jump(OCTOBER[0])).missing is MaxMissing.SCREENED
    assert _read(_ratio_jump(K + 1)).missing is None


def test_highest_return_tied_on_ten_days_is_kept_where_jkp_rank_filter_drops_it() -> None:
    """JKP keeps ``ret_rank <= 5`` before ``max(ret)``, which the spec reads as dropping a highest return tied across
    ten or more days. The spec's stated substitution is the plain maximum, which keeps the tied value."""
    tied = {i: 0.02 for i in OCTOBER[:10]}
    reading = _read(_bars(_steps(tied)))
    assert reading.missing is None
    assert reading.value == pytest.approx(0.02)


def test_cutoff_is_the_ceil_0_9n_th_smallest_and_ties_at_it_all_flag() -> None:
    values = [float(v) for v in range(1000)]
    q = max_cutoff(values)
    assert q == 899.0
    assert sum(v >= q for v in values) == 101
    tied = [1.0] * 5 + [2.0] * 5
    assert max_cutoff(tied) == 2.0
    assert max_cutoff([0.3]) == 0.3
    with pytest.raises(AvoidanceError, match="MAX_EMPTY"):
        max_cutoff([])


def _reading(value: float | None, missing: MaxMissing | None = None) -> MaxReading:
    return MaxReading(value, 20, 0, missing)


def test_flag_formation_applies_every_filter_and_missing_values_by_precedence() -> None:
    s_m = date(2019, 10, 31)
    seasoned = date(2016, 10, 31)  # exactly 36 months before s(M)
    names = {f"n{i}": NameInputs(_reading(0.01 * i), 10.0, seasoned) for i in range(10)}
    names["screened"] = NameInputs(_reading(None, MaxMissing.SCREENED), 10.0, seasoned)
    names["short"] = NameInputs(_reading(None, MaxMissing.SHORT), 10.0, seasoned)
    names["zero"] = NameInputs(_reading(None, MaxMissing.ZERO_HEAVY), 10.0, seasoned)
    names["cheap"] = NameInputs(_reading(0.0), 4.99, seasoned)
    names["floor"] = NameInputs(_reading(0.0), 5.0, seasoned)
    names["young"] = NameInputs(_reading(0.0), 10.0, seasoned + timedelta(days=1))

    cutoff, flags = flag_formation(names, s_m)

    # 13 valid values (n0..n9, cheap, floor, young); ceil(11.7) = 12th smallest = 0.08.
    assert cutoff == pytest.approx(0.08)
    assert {k for k, f in flags.items() if Filter.MAX in f.flagged} == {"n8", "n9", "screened"}
    assert {k for k, f in flags.items() if Filter.SUB5 in f.flagged} == {"cheap"}
    assert {k for k, f in flags.items() if Filter.YOUNG in f.flagged} == {"young"}
    assert flags["short"].flagged == frozenset() and flags["zero"].flagged == frozenset()


def test_filter_sets_are_the_five_spec_sets_and_removal_is_any_member() -> None:
    assert len(FILTER_SETS) == len(set(FILTER_SETS)) == 5
    s_m = date(2019, 10, 31)
    _, flags = flag_formation(
        {"a": NameInputs(_reading(0.0), 4.0, date(2010, 1, 1)), "b": NameInputs(_reading(0.1), 10.0, s_m)}, s_m
    )
    removed = {k: [fs for fs in FILTER_SETS if f.removed_by(fs)] for k, f in flags.items()}
    assert removed["a"] == [FILTER_SETS[1], FILTER_SETS[3], FILTER_SETS[4]]
    assert removed["b"] == [FILTER_SETS[0], FILTER_SETS[2], FILTER_SETS[3], FILTER_SETS[4]]


def test_agrees_with_the_premise_script_on_random_series() -> None:
    """The premise script is the independent implementation whose stage-A counts this module must reproduce."""
    rng = random.Random(3621)
    seen: set[MaxMissing | None] = set()
    for _ in range(400):
        adj, ratio = 10.0, 1.0
        bars: list[DailyBar] = []
        for day in SESSIONS:
            draw = rng.random()
            r = 0.0 if draw < 0.3 else 3.5 if draw < 0.305 else -0.95 if draw < 0.31 else rng.gauss(0, 0.03)
            adj *= 1.0 + r
            if rng.random() < 0.01:
                ratio *= rng.choice([1.6, 0.6, 1.2])
            bars.append(DailyBar(day, adj / ratio, adj, 1, stamped=rng.random() < 0.01, usable=rng.random() > 0.08))
        ours = MaxSeries(bars, SESSIONS).at(K)
        theirs = premise.rmax(
            premise.SeriesBars([(b.bar_date, b.close, b.adj_close, b.stamped, b.usable) for b in bars], SESSIONS),
            SESSIONS,
            K,
        )
        if isinstance(theirs, float):
            assert ours.missing is None and ours.value == theirs
        else:
            assert ours.missing is MaxMissing(theirs)
        seen.add(ours.missing)
    assert seen == {None, *MaxMissing}
