from datetime import date

import pytest

from app.services import momentum_book as mb


def _closes(t: int, monthly: list[float], start: float = 100.0) -> dict[int, float]:
    """Closes for months t-12..t from 12 monthly returns (months t-11..t)."""
    out = {t - 12: start}
    price = start
    for k, r in enumerate(monthly, start=1):
        price *= 1 + r
        out[t - 12 + k] = price
    return out


T = mb.month_key(date(2026, 9, 1))


def test_month_key_orders_months() -> None:
    assert mb.month_key(date(2026, 1, 31)) + 1 == mb.month_key(date(2026, 2, 1))
    assert mb.month_key(date(2025, 12, 1)) + 1 == mb.month_key(date(2026, 1, 1))


def test_formation_returns_skip_month_t() -> None:
    closes = _closes(T, [0.01] * 11 + [0.5])  # month t's +50% must not count
    returns = mb.formation_returns(closes, T)
    assert returns is not None and len(returns) == 11
    assert all(r == pytest.approx(0.01) for r in returns)


def test_formation_returns_need_every_close() -> None:
    closes = _closes(T, [0.01] * 12)
    del closes[T - 5]
    assert mb.formation_returns(closes, T) is None


def test_score_is_compounded_over_sd() -> None:
    rs = [0.02, -0.01] * 5 + [0.02]
    mean = sum(rs) / 11
    sd = (sum((r - mean) ** 2 for r in rs) / 10) ** 0.5
    expected = 1.0
    for r in rs:
        expected *= 1 + r
    assert mb.score(rs) == pytest.approx((expected - 1) / sd)
    assert mb.score([0.01] * 11) is None  # zero deviation
    assert mb.score([0.01] * 10) is None


def test_target_book_ranks_risk_adjusted_and_breaks_ties() -> None:
    steady = mb.Candidate(1, 1e9, _closes(T, [0.02, 0.01] * 6))
    volatile = mb.Candidate(2, 1e9, _closes(T, [0.30, -0.20] * 6))
    loser = mb.Candidate(3, 1e9, _closes(T, [-0.02, -0.01] * 6))
    twin = mb.Candidate(0, 1e9, _closes(T, [0.02, 0.01] * 6))
    book = mb.target_book([loser, volatile, steady, twin], T)
    assert book[:2] == (0, 1)  # equal score: lower id first
    assert book.index(1) < book.index(2) < book.index(3)


def test_target_book_universe_and_price_floor() -> None:
    cheap = mb.Candidate(10, 1e12, _closes(T, [0.05, 0.04] * 6, start=1.0))  # ends far below $5
    big = [mb.Candidate(100 + k, 1e9 - k, _closes(T, [0.01, 0.02] * 6)) for k in range(mb.UNIVERSE_SIZE)]
    small_star = mb.Candidate(99, 1.0, _closes(T, [0.10, 0.09] * 6))  # outside the top-500 by volume
    book = mb.target_book([cheap, small_star, *big], T)
    assert 10 not in book and 99 not in book
    assert len(book) == mb.BOOK_SIZE


def test_rebalance_plan() -> None:
    plan = mb.rebalance_plan(target=[5, 1, 2], held=[1, 7, 9])
    assert plan.exits == (7, 9)
    assert plan.entries == (5, 2)
    assert plan.keeps == (1,)


def test_universe_is_the_set_target_book_ranks() -> None:
    cands = [mb.Candidate(i, 1e9 - i, _closes(T, [0.01 * (i % 7 + 1), 0.005] * 6)) for i in range(40)]
    universe = mb.universe(cands, T)
    assert set(mb.target_book(cands, T)) <= set(universe)
    assert len(universe) == 40
