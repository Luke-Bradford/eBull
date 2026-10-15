import random
from datetime import date
from statistics import median

import pytest

from app.services import momentum_book as mb
from app.services import momentum_book_scorecard as sc

SPY = 9000
D0, D1, D2 = date(2026, 10, 14), date(2026, 11, 2), date(2026, 12, 1)


def _closes(prices: dict[date, dict[int, float]]) -> dict[date, dict[int, tuple[date, float]]]:
    return {d: {iid: (d, c) for iid, c in by_id.items()} for d, by_id in prices.items()}


def _period(month: date, start: date, end: date, book: tuple[int, ...], universe: tuple[int, ...]) -> sc.Period:
    return sc.Period(formation_month=month, start=start, end=end, book=book, universe=universe)


def test_basket_return_is_equal_weight_and_skips_unpriced_names() -> None:
    closes = _closes({D0: {1: 10.0, 2: 20.0}, D1: {1: 11.0, 2: 18.0, 3: 5.0}})
    r, priced = sc.basket_return((1, 2, 3), D0, D1, closes)
    assert priced == 2
    assert r == pytest.approx((0.1 - 0.1) / 2)


def test_basket_return_values_a_name_that_stopped_trading_at_its_last_close() -> None:
    mid = date(2026, 10, 20)
    closes = {D0: {1: (D0, 10.0)}, D1: {1: (mid, 12.0)}}  # acquired on the 20th
    assert sc.basket_return((1,), D0, D1, closes) == (pytest.approx(0.2), 1)


def test_basket_return_drops_a_stale_start_close() -> None:
    closes = {D0: {1: (date(2026, 10, 1), 10.0)}, D1: {1: (D1, 12.0)}}
    assert sc.basket_return((1,), D0, D1, closes) == (None, 0)


def test_percentile_counts_ties_half() -> None:
    assert sc.percentile_of(0.1, [0.0, 0.1, 0.2, 0.3]) == pytest.approx(1.5 / 4)


def test_score_chains_book_spy_and_each_control_path() -> None:
    universe = tuple(range(1, 61))
    # Names 1..30 (the book) rise 10% each period; 31..60 are flat.
    prices = {d: {i: 10.0 * (1.1**k if i <= 30 else 1.0) for i in universe} for k, d in enumerate((D0, D1, D2))}
    for k, d in enumerate((D0, D1, D2)):
        prices[d][SPY] = 100.0 * 1.05**k
    periods = [
        _period(date(2026, 9, 1), D0, D1, tuple(range(1, 31)), universe),
        _period(date(2026, 10, 1), D1, D2, tuple(range(1, 31)), universe),
    ]
    card = sc.score(periods, _closes(prices), spy_id=SPY, draws=20)
    assert [p.book_return for p in card.periods] == [pytest.approx(0.1)] * 2
    assert card.book_cumulative == pytest.approx(0.21)
    assert card.spy_cumulative == pytest.approx(1.05**2 - 1)
    # A random 30 of 60 is a mix of risers and flat names: always below the all-riser book.
    assert card.book_cumulative_percentile == 1.0
    assert card.control_cumulative_median is not None and 0 < card.control_cumulative_median < 0.21

    # The draws are reproducible from the seed alone.
    def draw_return(d: int, month: date) -> float:
        names = random.Random(sc.control_seed(d, month)).sample(sorted(universe), mb.BOOK_SIZE)
        return sum(0.1 if i <= 30 else 0.0 for i in names) / mb.BOOK_SIZE

    assert card.periods[0].control_median == pytest.approx(median(draw_return(d, date(2026, 9, 1)) for d in range(20)))
    paths = [(1 + draw_return(d, date(2026, 9, 1))) * (1 + draw_return(d, date(2026, 10, 1))) - 1 for d in range(20)]
    assert card.control_cumulative_median == pytest.approx(median(paths))


def test_a_period_without_a_close_after_its_start_is_pending() -> None:
    card = sc.score([_period(date(2026, 9, 1), D0, D0, (1,), (1,))], {}, spy_id=SPY)
    assert card.periods == ()
    assert card.pending == (date(2026, 9, 1),)
    assert card.book_cumulative is None


def test_a_period_without_a_recorded_universe_scores_the_book_but_not_the_control() -> None:
    closes = _closes({D0: {1: 10.0, SPY: 100.0}, D1: {1: 11.0, SPY: 101.0}})
    card = sc.score([_period(date(2026, 9, 1), D0, D1, (1,), ())], closes, spy_id=SPY, draws=5)
    (period,) = card.periods
    assert period.book_return == pytest.approx(0.1)
    assert period.control_median is None and period.book_percentile is None
    assert card.book_cumulative is None  # nothing chains without the control
