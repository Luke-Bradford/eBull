"""#3385 slice 3b — the evaluator, pure logic, every expected number written out by hand."""

from __future__ import annotations

import hashlib
import math
from datetime import date, timedelta
from pathlib import Path

import pytest

from app.services import hunt_evaluator as ev
from app.services import hunt_harness as hh
from app.services.hunt_inference import StatRefused

HS = 0.01


def _hs(_series: int, _entry: int, _book: ev.Book) -> float:
    return HS


def _series(
    opens: dict[int, float],
    closes: dict[int, float],
    dividends: dict[int, float] | None = None,
    terminal: int | None = None,
) -> ev.SeriesPrices:
    return ev.SeriesPrices(opens=opens, closes=closes, dividends=dividends or {}, terminal_ordinal=terminal)


# --- the spec's fixture: two overlapping cohorts ------------------------------------------------------------------
#
# Sessions 0 … 7, lag = 1, h = 2, open entry, close exit, half-spread 1% per side.
# Cohort t = 0 holds e = 1 → x = 2; cohort t = 1 holds e = 2 → x = 3.
#   1: every bar, a 0.50 dividend ex-dated on session 2.
#   2: no bar on session 2 — a missing EXIT bar for t = 0 (stale exit at 21) and a NO-ENTRY name for t = 1.
#   4: terminates at session 2, policy fraction 0.3 — t = 0 exits first (x = last_bar); t = 1 is haircut.
#   5: live, bars end after session 1 — CENSORED: t = 0 exits stale at 8.5, t = 1 never enters.
PRICES = {
    1: _series({1: 10.0, 2: 11.0, 3: 12.0}, {1: 11.0, 2: 12.0, 3: 13.0}, {2: 0.5}),
    2: _series({1: 20.0, 3: 22.0}, {1: 21.0, 3: 23.0}),
    4: _series({1: 5.0, 2: 5.0}, {1: 6.0, 2: 4.0}, terminal=2),
    5: _series({1: 8.0}, {1: 8.5}),
}
FRACTIONS = {4: 0.3}
COHORTS = {
    0: ev.Cohort(control=frozenset({1, 2, 4, 5}), arm=frozenset({1, 4})),
    1: ev.Cohort(control=frozenset({1, 2, 4, 5}), arm=frozenset({2, 5})),
}
GRID = ev.Grid(formations=tuple(range(6)), first=1, last=7)

# Position values V(e), V(x), by hand from spec "A position's value factor".
T0 = {
    1: (11 / 10.1, 11 / 10.1 * (12 * 0.99 + 0.5) / 11),
    2: (21 / 20.2, 21 / 20.2 * 0.99),
    4: (6 / 5.05, 6 / 5.05 * (4 * 0.99) / 6),
    5: (8.5 / 8.08, 8.5 / 8.08 * 0.99),
}
T1 = {
    1: (12 / 11.11, 12 / 11.11 * (13 * 0.99) / 12),  # ex-date 2 = e: the buyer gets no dividend
    2: (1.0, 1.0),
    4: (4 / 5.05 * 0.3, 4 / 5.05 * 0.3),
    5: (1.0, 1.0),
}


def _book(names: frozenset[int], values: dict[int, tuple[float, float]]) -> tuple[float, float]:
    return sum(values[n][0] for n in names), sum(values[n][1] for n in names)


def _expected(book: str) -> list[float]:
    t0 = _book(getattr(COHORTS[0], book), T0)
    t1 = _book(getattr(COHORTS[1], book), T1)
    n0, n1 = len(getattr(COHORTS[0], book)), len(getattr(COHORTS[1], book))
    d1 = (t0[0] / n0 + 1.0) / 2 - 1  # slot 0 = t0's first session; slot 1 idle
    d2 = (t1[0] / n1 + t0[1] / t0[0]) / 2 - 1
    d3 = (1.0 + t1[1] / t1[0]) / 2 - 1  # slot 0 (t = 2) idle
    return [d1, d2, d3, 0.0, 0.0, 0.0, 0.0]


def _evaluate(**overrides: object) -> ev.BookSeries:
    kwargs: dict = dict(
        lag=1,
        h=2,
        entry_point="open",
        exit_point="close",
        half_spread=_hs,
        terminal_fractions=FRACTIONS,
        with_dividends=True,
    )
    kwargs.update(overrides)
    result = ev.evaluate_books(GRID, kwargs.pop("cohorts", COHORTS), PRICES, **kwargs)
    assert isinstance(result, ev.BookSeries)
    return result


@pytest.mark.parametrize(
    ("name", "t", "values"), [(n, 0, v) for n, v in T0.items()] + [(n, 1, v) for n, v in T1.items()]
)
def test_each_position_path_by_hand(name: int, t: int, values: tuple[float, float]) -> None:
    path = ev.position_path(
        PRICES[name],
        e=t + 1,
        x=t + 2,
        entry_point="open",
        exit_point="close",
        half_spread=HS,
        terminal_fraction=FRACTIONS.get(name),
        with_dividends=True,
    )
    assert isinstance(path, ev.PositionPath)
    assert path.values == pytest.approx(values, rel=1e-12)


def test_the_two_overlapping_cohorts_by_hand() -> None:
    books = _evaluate()
    assert list(books.sessions) == list(range(1, 8))
    assert list(books.arm) == pytest.approx(_expected("arm"), rel=1e-12, abs=1e-15)
    assert list(books.control) == pytest.approx(_expected("control"), rel=1e-12, abs=1e-15)
    assert books.active == pytest.approx([a - c for a, c in zip(books.arm, books.control, strict=True)])
    # t = 1's arm is {2, 5}, neither of which enters.
    assert books.entered_formations == (0,)


def test_without_dividends_drops_only_the_dividend() -> None:
    books = _evaluate(with_dividends=False)
    t0_name1 = 11 / 10.1 * (12 * 0.99) / 11
    control_t0 = (t0_name1 + T0[2][1] + T0[4][1] + T0[5][1]) / sum(v[0] for v in T0.values())
    assert books.control[1] == pytest.approx((sum(v[0] for v in T1.values()) / 4 + control_t0) / 2 - 1, rel=1e-12)


def test_a_no_signal_spec_has_exactly_zero_active_return() -> None:
    same = {t: ev.Cohort(control=c.control, arm=c.control) for t, c in COHORTS.items()}
    books = _evaluate(cohorts=same)
    assert books.active == (0.0,) * 7


# --- position edge cases ----------------------------------------------------------------------------------------


def _path(series: ev.SeriesPrices, **overrides: object) -> ev.PositionPath | StatRefused:
    kwargs: dict = dict(
        e=1, x=2, entry_point="open", exit_point="close", half_spread=HS, terminal_fraction=None, with_dividends=True
    )
    kwargs.update(overrides)
    return ev.position_path(series, **kwargs)


def test_an_open_exit_never_reads_the_exit_session_s_close() -> None:
    # Close entry on 1, open exit on 2; session 2 has a close but no valid open.
    series = _series({1: 9.0, 2: math.nan}, {1: 10.0, 2: 50.0})
    path = _path(series, entry_point="close", exit_point="open")
    assert isinstance(path, ev.PositionPath)
    assert path.values == pytest.approx((10 / 10.1, 10 / 10.1 * (10 * 0.99) / 10))


def test_an_open_exit_uses_the_open() -> None:
    path = _path(_series({1: 9.0, 2: 11.0}, {1: 10.0, 2: 50.0}), entry_point="close", exit_point="open")
    assert isinstance(path, ev.PositionPath)
    assert path.values[-1] == pytest.approx(10 / 10.1 * (11 * 0.99) / 10)


def test_a_close_entry_on_the_terminal_bar_is_haircut_that_session() -> None:
    path = _path(_series({}, {1: 10.0}, terminal=1), entry_point="close", exit_point="open", terminal_fraction=0.5)
    assert isinstance(path, ev.PositionPath)
    assert path.values == pytest.approx((0.5 / 1.01, 0.5 / 1.01))


def test_an_exit_on_the_terminal_bar_is_an_exit_not_a_haircut() -> None:
    path = _path(_series({1: 10.0}, {1: 10.0, 2: 10.0}, terminal=2), terminal_fraction=0.0)
    assert isinstance(path, ev.PositionPath)
    assert path.values[-1] == pytest.approx(0.99 / 1.01)


def test_a_dividend_in_a_gap_is_credited_at_the_next_mark() -> None:
    series = _series({1: 10.0}, {1: 10.0, 3: 10.0, 4: 10.0}, {2: 1.0})
    path = _path(series, x=4)
    assert isinstance(path, ev.PositionPath)
    assert path.values == pytest.approx((10 / 10.1, 10 / 10.1, 10 / 10.1 * 11 / 10, 10 / 10.1 * 11 / 10 * 0.99))


def test_a_pending_dividend_is_credited_before_the_terminal_haircut() -> None:
    # Terminal bar 2 has only an open, so the dividend ex-dated on 2 has no later mark.
    series = _series({1: 10.0, 2: 10.0}, {1: 10.0}, {2: 1.0}, terminal=2)
    path = _path(series, x=3, terminal_fraction=0.5)
    assert isinstance(path, ev.PositionPath)
    assert path.values[1:] == pytest.approx((10 / 10.1 * 11 / 10 * 0.5,) * 2)


@pytest.mark.parametrize("amount", [-0.1, math.nan, math.inf])
def test_a_bad_dividend_refuses_the_cell(amount: float) -> None:
    result = _path(_series({1: 10.0}, {1: 10.0, 2: 10.0}, {2: amount}))
    assert isinstance(result, StatRefused) and result.reason == "bad_dividend"


def test_without_dividends_a_bad_dividend_is_not_read() -> None:
    assert isinstance(_path(_series({1: 10.0}, {1: 10.0, 2: 10.0}, {2: -1.0}), with_dividends=False), ev.PositionPath)


def test_one_session_hold_is_sell_close_over_buy_open() -> None:
    path = _path(_series({1: 10.0}, {1: 12.0}), x=1)
    assert isinstance(path, ev.PositionPath)
    assert path.values == pytest.approx((12 * 0.99 / (10 * 1.01),))


def test_a_price_after_the_terminal_bar_is_refused() -> None:
    with pytest.raises(ValueError):
        _series({3: 1.0}, {}, terminal=2)


def test_a_terminal_fraction_must_match_termination() -> None:
    with pytest.raises(ValueError):
        _path(_series({1: 10.0}, {1: 10.0}), terminal_fraction=0.5)
    with pytest.raises(ValueError):
        _path(_series({1: 10.0}, {1: 10.0}, terminal=1))


# --- the books ------------------------------------------------------------------------------------------------------


def test_a_near_total_single_session_loss_is_book_ruin() -> None:
    prices = {1: _series({1: 100.0}, {1: 1e-12})}
    result = ev.evaluate_books(
        ev.Grid(formations=(0,), first=1, last=1),
        {0: ev.Cohort(control=frozenset({1}), arm=frozenset({1}))},
        prices,
        lag=1,
        h=1,
        entry_point="open",
        exit_point="close",
        half_spread=_hs,
        terminal_fractions={},
        with_dividends=True,
    )
    assert isinstance(result, StatRefused) and result.reason == "book_ruin"


def test_stress_charges_only_the_arm() -> None:
    def stress(_series: int, _entry: int, book: ev.Book) -> float:
        return 0.05 if book == "arm" else HS

    same = {t: ev.Cohort(control=c.control, arm=c.control) for t, c in COHORTS.items()}
    books = _evaluate(cohorts=same, half_spread=stress)
    assert books.active[0] < 0.0
    assert books.control == _evaluate(cohorts=same).control


def test_cohorts_must_sit_on_the_grid() -> None:
    with pytest.raises(ValueError):
        _evaluate(cohorts={9: COHORTS[0]})


# --- grid and selection -----------------------------------------------------------------------------------------------


def _days(count: int) -> list[date]:
    return [date(2000, 1, 3) + timedelta(days=i) for i in range(count)]


def test_the_grid_purges_and_embargoes_from_the_calendar() -> None:
    days = _days(20)
    grid = ev.formation_grid(days, start=days[2], end=days[15], lag=2, h=3, embargo=4)
    assert isinstance(grid, ev.Grid)
    # t from 2 + 4 = 6 to 15 − 2 − 3 + 1 = 11; entry of 6 is 8, exit of 11 is 15.
    assert grid.formations == tuple(range(6, 12))
    assert (grid.first, grid.last) == (8, 15)


def test_a_split_too_short_for_one_formation_is_an_empty_grid() -> None:
    days = _days(10)
    result = ev.formation_grid(days, start=days[0], end=days[5], lag=3, h=3, embargo=1)
    assert isinstance(result, StatRefused) and result.reason == "empty_grid"
    outside = ev.formation_grid(days, start=date(2030, 1, 1), end=date(2030, 2, 1), lag=1, h=1, embargo=0)
    assert isinstance(outside, StatRefused) and outside.reason == "empty_grid"


def test_selection_count_uses_the_decimal_fraction() -> None:
    assert math.ceil(0.07 * 100) == 8  # the float product rounds up past the decimal answer
    assert ev.selection_count(0.07, 100) == 7
    assert ev.selection_count(0.1, 31) == 4
    assert ev.selection_count(0.5, 1) == 1
    assert ev.selection_count(0.2, 0) == 0


def test_select_arm_includes_ties_and_honours_sign() -> None:
    scores = {1: 5.0, 2: 4.0, 3: 4.0, 4: 1.0, 5: 0.0}
    assert ev.select_arm(scores, sign=1, fraction=0.4) == frozenset({1, 2, 3})
    assert ev.select_arm(scores, sign=-1, fraction=0.2) == frozenset({5})
    assert ev.select_arm({}, sign=1, fraction=0.5) == frozenset()
    with pytest.raises(ValueError):
        ev.select_arm({1: math.nan}, sign=1, fraction=0.5)


@pytest.mark.parametrize("fraction", [0.0, -0.1, 1.5, math.nan, math.inf])
def test_a_fraction_outside_zero_one_is_refused(fraction: float) -> None:
    with pytest.raises(ValueError):
        ev.select_arm({1: 1.0, 2: 2.0}, sign=1, fraction=fraction)


def test_the_evaluator_code_is_part_of_the_harness_model_id() -> None:
    constants = hh._model_constants()["model_code_sha256"]
    assert constants["app.services.hunt_evaluator"] == hashlib.sha256(Path(str(ev.__file__)).read_bytes()).hexdigest()


# --- Codex ckpt-2 findings -------------------------------------------------------------------------------------


def _books(prices: dict[int, ev.SeriesPrices], cohort: ev.Cohort, half_spread: ev.HalfSpread = _hs) -> object:
    return ev.evaluate_books(
        ev.Grid(formations=(0,), first=1, last=2),
        {0: cohort},
        prices,
        lag=1,
        h=2,
        entry_point="open",
        exit_point="close",
        half_spread=half_spread,
        terminal_fractions={},
        with_dividends=True,
    )


def test_a_name_that_never_enters_is_never_charged() -> None:
    def strict(series: int, _entry: int, _book: ev.Book) -> float:
        if series == 2:
            raise AssertionError("no fill exists to key a band on")
        return HS

    prices = {1: _series({1: 10.0}, {1: 10.0, 2: 10.0}), 2: _series({}, {})}
    result = _books(prices, ev.Cohort(control=frozenset({1, 2}), arm=frozenset({1})), strict)
    assert isinstance(result, ev.BookSeries)


def test_an_overflowing_cohort_sum_refuses() -> None:
    prices = {n: _series({1: 1.0}, {1: 1e308, 2: 1e308}) for n in (1, 2)}
    result = _books(prices, ev.Cohort(control=frozenset({1, 2}), arm=frozenset({1})))
    assert isinstance(result, StatRefused) and result.reason == "non_finite"


def test_an_overflowing_entry_fill_refuses() -> None:
    result = _path(_series({1: 1e308}, {1: 1e308, 2: 1e308}), half_spread=0.9)
    assert isinstance(result, StatRefused) and result.reason == "non_finite"
