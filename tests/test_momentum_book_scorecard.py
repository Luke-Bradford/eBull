import random
from datetime import date, timedelta
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
    r = sc.basket_return((1, 2, 3), D0, D1, closes, {})
    assert (r.priced, r.stale) == (2, 0)
    assert r.value == pytest.approx((0.1 - 0.1) / 2)


def test_basket_return_counts_a_stale_end_mark_instead_of_scoring_it() -> None:
    closes = {D0: {1: (D0, 10.0), 2: (D0, 10.0)}, D1: {1: (date(2026, 10, 20), 12.0), 2: (D1, 11.0)}}
    r = sc.basket_return((1, 2), D0, D1, closes, {})
    assert (r.value, r.priced, r.stale) == (pytest.approx(0.1), 1, 1)


def test_basket_return_never_crosses_an_unresolved_break() -> None:
    closes = _closes({D0: {1: 10.0, 2: 10.0}, D1: {1: 5.0, 2: 11.0}})
    r = sc.basket_return((1, 2), D0, D1, closes, {1: (date(2026, 10, 20),), 2: (D0,)})
    assert (r.value, r.priced) == (pytest.approx(0.1), 1)  # a break ON the start bar is not crossed


def test_basket_return_drops_a_stale_start_close() -> None:
    closes = {D0: {1: (date(2026, 10, 1), 10.0)}, D1: {1: (D1, 12.0)}}
    assert sc.basket_return((1,), D0, D1, closes, {}) == sc.BasketReturn(None, 0, 0)


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
    card = sc.score(periods, _closes(prices), {}, spy_id=SPY, draws=20)
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
    card = sc.score([_period(date(2026, 9, 1), D0, D0, (1,), (1,))], {}, {}, spy_id=SPY)
    assert card.periods == ()
    assert card.pending == (date(2026, 9, 1),)
    assert card.book_cumulative is None


def test_a_period_without_a_recorded_universe_scores_the_book_but_not_the_control() -> None:
    closes = _closes({D0: {1: 10.0, SPY: 100.0}, D1: {1: 11.0, SPY: 101.0}})
    card = sc.score([_period(date(2026, 9, 1), D0, D1, (1,), ())], closes, {}, spy_id=SPY, draws=5)
    (period,) = card.periods
    assert period.book_return == pytest.approx(0.1)
    assert period.control_median is None and period.book_percentile is None
    assert card.book_cumulative is None  # nothing chains without the control


def test_score_refuses_zero_draws() -> None:
    with pytest.raises(ValueError, match="draws"):
        sc.score([], {}, {}, spy_id=SPY, draws=0)


def test_the_endpoint_serialises_a_scorecard_and_maps_a_missing_spy_to_503(monkeypatch: pytest.MonkeyPatch) -> None:
    # Runs the real handler body (its `asdict`/`cast`/`Any` names included) with the DB reads stubbed.
    from contextlib import nullcontext

    from fastapi import HTTPException

    from app.api import strategies

    card = sc.score(
        [_period(date(2026, 9, 1), D0, D1, (1,), ())],
        _closes({D0: {1: 10.0, SPY: 100.0}, D1: {1: 11.0, SPY: 101.0}}),
        {},
        spy_id=SPY,
        draws=1,
    )
    monkeypatch.setattr(strategies, "snapshot_read", lambda conn: nullcontext())
    monkeypatch.setattr(strategies, "load_momentum_scorecard", lambda conn: card)
    response = strategies.get_momentum_book_scorecard(conn=object())  # type: ignore[arg-type]
    assert response.periods[0].book_return == pytest.approx(0.1)
    assert response.control_draws == 1

    def missing(conn: object) -> sc.Scorecard:
        raise sc.BenchmarkInstrumentError("expected one canonical 'SPY' instrument, found 0")

    monkeypatch.setattr(strategies, "load_momentum_scorecard", missing)
    with pytest.raises(HTTPException) as exc:
        strategies.get_momentum_book_scorecard(conn=object())  # type: ignore[arg-type]
    assert exc.value.status_code == 503


# --- total return (#3844) ---------------------------------------------------------------------------------------


def _divs(events: dict[int, list[tuple[date, float | None]]], known_from: date, known_through: date) -> sc.Dividends:
    return sc.Dividends(events, known_from, known_through)


def test_dividends_count_ex_dates_after_the_start_bar_through_the_end_bar_net_of_withholding() -> None:
    divs = _divs({1: [(D0, 9.0), (date(2026, 10, 20), 1.0), (D1, 2.0), (D2, 9.0)]}, D0, D2)
    # The start bar's own ex-date is not the holder's; the end bar's is.
    assert divs.net(1, D0, D1) == pytest.approx(3.0 * (1 - sc.DIVIDEND_WITHHOLDING))
    assert divs.net(2, D0, D1) == 0.0


def test_dividends_are_unknown_outside_the_captured_span_or_without_an_amount() -> None:
    divs = _divs({1: [(date(2026, 10, 20), None)]}, date(2026, 10, 10), D1)
    assert divs.net(2, date(2026, 10, 8), D1) is None  # starts before the first capture
    assert divs.net(2, date(2026, 10, 9), D1) == 0.0  # the first capture sees ex-dates from the day after the start
    assert divs.net(2, D0, D2) is None  # runs past the last capture
    assert divs.net(1, D0, D1) is None  # a known date without an amount
    gapped = _divs({}, date(2026, 10, 10), D1)
    gapped = sc.Dividends(gapped.events, gapped.known_from, gapped.known_through, frozenset({date(2026, 10, 20)}))
    assert gapped.net(2, D0, D1) is None  # an uncaptured day inside the span: a dividend there would be missed
    assert gapped.net(2, date(2026, 10, 20), D1) == 0.0  # the gap is the start bar, so no ex-date there counts


def test_basket_total_adds_net_dividends_to_the_end_close_and_is_unknown_if_any_name_is() -> None:
    closes = _closes({D0: {1: 10.0, 2: 20.0}, D1: {1: 10.0, 2: 20.0}})
    divs = _divs({1: [(date(2026, 10, 20), 1.0)]}, D0, D1)
    r = sc.basket_return((1, 2), D0, D1, closes, {}, divs)
    assert r.value == 0.0
    assert r.total == pytest.approx((1.0 * (1 - sc.DIVIDEND_WITHHOLDING) / 10.0 + 0.0) / 2)
    unknown = _divs({2: [(date(2026, 10, 20), None)]}, D0, D1)
    assert sc.basket_return((1, 2), D0, D1, closes, {}, unknown).total is None
    assert sc.basket_return((1, 2), D0, D1, closes, {}).total is None


def test_score_gives_spy_a_total_only_when_no_unpriced_ex_date_falls_inside() -> None:
    universe = tuple(range(1, 31))
    prices = {d: {i: 10.0 for i in (*universe, SPY)} for d in (D0, D1, D2)}
    periods = [
        _period(date(2026, 9, 1), D0, D1, universe, universe),
        _period(date(2026, 10, 1), D1, D2, universe, universe),
    ]
    stock = _divs({1: [(date(2026, 10, 20), 3.0)]}, date(2026, 10, 10), D2)
    spy = _divs({SPY: [(date(2026, 11, 20), None)]}, date(2026, 1, 1), sc.SPY_SCHEDULE_THROUGH)
    card = sc.score(
        periods,
        _closes(prices),
        {},
        spy_id=SPY,
        draws=5,
        stock_dividends=stock,
        spy_dividends=spy,
        captured_days=[D0 + timedelta(days=k) for k in range(20)],  # 10-14 .. 11-02
    )
    first, second = card.periods
    assert first.book_total_return == pytest.approx(3.0 * 0.85 / 10.0 / 30)
    assert first.spy_total_return == 0.0
    assert first.dividend_days_uncaptured == 0
    assert second.spy_total_return is None
    assert second.dividend_days_uncaptured == 29  # 11-03 .. 12-01, none captured
    assert card.book_total_cumulative == pytest.approx(3.0 * 0.85 / 10.0 / 30)
    assert card.spy_total_cumulative is None
    assert card.book_cumulative == 0.0
