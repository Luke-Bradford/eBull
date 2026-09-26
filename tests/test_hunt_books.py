"""#3385 — the vectorised books against their oracle, ``hunt_evaluator.evaluate_books``."""

from __future__ import annotations

import math
import random
from collections import Counter
from datetime import date, timedelta

import numpy as np
import pytest

from app.services import hunt_books as hb
from app.services import hunt_evaluator as ev
from app.services.hunt_inference import StatRefused

SEEDS = range(400)


def _day(i: int) -> date:
    return date(2000, 1, 3) + timedelta(days=i)


def _bad_price(rng: random.Random) -> float:
    return rng.choice([0.0, -1.0, math.nan, math.inf])


def _case(seed: int) -> tuple[ev.Grid, dict[int, ev.Cohort], dict[int, ev.SeriesPrices], dict[str, object]]:
    """A random panel with gaps, invalid bars, dividends, terminations and idle formations."""
    rng = random.Random(seed)
    sessions = rng.randint(12, 40)
    lag = rng.randint(1, 2)
    h = rng.randint(1, 6)
    entry_point, exit_point = (
        ("open", "close") if h == 1 else (rng.choice(["open", "close"]), rng.choice(["open", "close"]))
    )
    refusing = rng.random() < 0.15  # occasionally a bad dividend or a ruinous spread

    prices: dict[int, ev.SeriesPrices] = {}
    fractions: dict[int, float] = {}
    for name in range(rng.randint(2, 12)):
        start = rng.randint(0, sessions // 3)
        end = rng.randint(start, sessions - 1)
        terminal = end if rng.random() < 0.3 else None
        opens: dict[int, float] = {}
        closes: dict[int, float] = {}
        level = rng.uniform(1.0, 200.0)
        for d in range(start, end + 1):
            level *= math.exp(rng.gauss(0.0, 0.05))
            if rng.random() < 0.1:
                continue  # no bar
            opens[d] = _bad_price(rng) if rng.random() < 0.03 else level * rng.uniform(0.98, 1.02)
            if rng.random() < 0.9:
                closes[d] = _bad_price(rng) if rng.random() < 0.03 else level
        dividends = {d: rng.uniform(0.0, 2.0) for d in range(start, sessions) if rng.random() < 0.05}
        if refusing and rng.random() < 0.3:
            dividends[rng.randint(start, sessions - 1)] = rng.choice([-0.5, math.nan])
        prices[name] = ev.SeriesPrices(opens=opens, closes=closes, dividends=dividends, terminal_ordinal=terminal)
        if terminal is not None:
            fractions[name] = rng.choice([0.0, 1.0, rng.random()])

    grid = ev.formation_grid(
        [_day(i) for i in range(sessions)],
        start=_day(0),
        end=_day(sessions - 1),
        lag=lag,
        h=h,
        embargo=0,
    )
    assert isinstance(grid, ev.Grid)
    cohorts: dict[int, ev.Cohort] = {}
    names = sorted(prices)
    for t in grid.formations:
        if rng.random() < 0.2:
            continue  # idle
        control = frozenset(rng.sample(names, rng.randint(1, len(names))))
        arm = frozenset(rng.sample(sorted(control), rng.randint(1, len(control))))
        cohorts[t] = ev.Cohort(control=control, arm=arm)

    spread_scale = 0.999 if refusing and rng.random() < 0.3 else 0.02

    def half_spread(series: int, e: int, book: ev.Book) -> float:
        charge = random.Random(f"{seed}-{series}-{e}").uniform(0.0005, spread_scale)
        return min(charge * (2.0 if book == "arm" else 1.0), 0.9995)

    timeline: dict[str, object] = {
        "lag": lag,
        "h": h,
        "entry_point": entry_point,
        "exit_point": exit_point,
        "half_spread": half_spread,
        "terminal_fractions": fractions,
        "with_dividends": rng.random() < 0.7,
    }
    return grid, cohorts, prices, timeline


def test_the_fast_books_match_the_oracle() -> None:
    outcomes: Counter[str] = Counter()
    for seed in SEEDS:
        grid, cohorts, prices, timeline = _case(seed)
        oracle = ev.evaluate_books(grid, cohorts, prices, **timeline)  # type: ignore[arg-type]
        fast = hb.evaluate_books_fast(grid, cohorts, hb.pack_prices(prices), **timeline)  # type: ignore[arg-type]
        if fast is None:
            assert isinstance(oracle, StatRefused), f"seed {seed}: fast deferred but the oracle computed"
            outcomes[f"refused:{oracle.reason}"] += 1
            continue
        assert isinstance(oracle, ev.BookSeries), f"seed {seed}: fast computed but the oracle refused {oracle}"
        assert fast.sessions == oracle.sessions
        assert fast.entered_formations == oracle.entered_formations
        for field in ("arm", "control", "active"):
            got, want = getattr(fast, field), getattr(oracle, field)
            assert len(got) == len(want)
            for g, w in zip(got, want, strict=True):
                assert math.isclose(g, w, rel_tol=1e-12, abs_tol=1e-15), f"seed {seed} {field}: {g} != {w}"
        outcomes["computed"] += 1
    # The suite must exercise both branches, or it proves half of what it claims.
    assert outcomes["computed"] > 200
    assert sum(count for key, count in outcomes.items() if key.startswith("refused:")) > 5, outcomes


def test_a_series_without_prices_is_a_contract_error() -> None:
    grid, cohorts, prices, timeline = _case(1)
    name = next(iter(next(iter(cohorts.values())).control))
    packed = hb.pack_prices({k: v for k, v in prices.items() if k != name})
    with pytest.raises(ValueError, match="no prices for series"):
        hb.evaluate_books_fast(grid, cohorts, packed, **timeline)  # type: ignore[arg-type]


def test_a_charge_outside_the_open_interval_is_a_contract_error() -> None:
    prices = {0: ev.SeriesPrices(opens={1: 10.0}, closes={1: 10.0}, dividends={}, terminal_ordinal=None)}
    grid = ev.Grid(formations=(0,), first=1, last=1)
    cohorts = {0: ev.Cohort(control=frozenset({0}), arm=frozenset({0}))}
    with pytest.raises(ValueError, match="half_spread"):
        hb.evaluate_books_fast(
            grid,
            cohorts,
            hb.pack_prices(prices),
            lag=1,
            h=1,
            entry_point="open",
            exit_point="close",
            half_spread=lambda _s, _e, _b: 1.0,
            terminal_fractions={},
            with_dividends=True,
        )


def test_a_session_gross_the_oracle_must_decide_defers() -> None:
    """Codex ckpt-2: overflow and the ruin boundary are the oracle's calls, not the fast path's."""
    assert hb.session_gross(np.array([1.7e308, 1.7e308]), 2) is None
    assert hb.session_gross(np.array([math.nan]), 1) is None
    assert hb.session_gross(np.array([ev.BOOK_RUIN_FACTOR * (1.0 + 1e-12)]), 1) is None
    assert hb.session_gross(np.array([1.0, 1.02]), 2) == 1.01


def test_blocks_split_and_every_block_matches_the_oracle(monkeypatch: pytest.MonkeyPatch) -> None:
    """A tiny block size forces many blocks; the books must not depend on where they split."""
    monkeypatch.setattr(hb, "BLOCK_POSITIONS", 3)
    for seed in range(40):
        grid, cohorts, prices, timeline = _case(seed)
        oracle = ev.evaluate_books(grid, cohorts, prices, **timeline)  # type: ignore[arg-type]
        fast = hb.evaluate_books_fast(grid, cohorts, hb.pack_prices(prices), **timeline)  # type: ignore[arg-type]
        if fast is None:
            assert isinstance(oracle, StatRefused)
            continue
        assert isinstance(oracle, ev.BookSeries)
        assert fast.entered_formations == oracle.entered_formations
        for g, w in zip(fast.active, oracle.active, strict=True):
            assert math.isclose(g, w, rel_tol=1e-12, abs_tol=1e-15)
