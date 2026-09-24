from __future__ import annotations

import math
from datetime import date, datetime

import pytest

from app.services.r6_exclusion_trial import (
    HALF_SPREAD,
    PriceBar,
    PriceSeries,
    haircut_net_return,
    simulate_portfolio,
    validate_factor,
)


def _series(symbol: str, final: float = 100.0) -> PriceSeries:
    return PriceSeries(
        symbol=symbol,
        bars=(
            PriceBar(date(2022, 6, 30), 100.0, 100.0),
            PriceBar(date(2022, 7, 1), 100.0, 100.0),
            PriceBar(date(2024, 9, 27), final, final),
        ),
        invalid_rows=0,
    )


def test_flat_one_period_portfolio_charges_exact_round_trip() -> None:
    schedule = ((datetime(2022, 6, 30, 16), frozenset({"AAA", "BBB"})),)
    result = simulate_portfolio(
        schedule=schedule,
        prices={"AAA": _series("AAA"), "BBB": _series("BBB")},
        case="worst",
        half_spread=HALF_SPREAD,
    )
    expected = (1 - HALF_SPREAD) / (1 + HALF_SPREAD) - 1
    assert result.total_return == pytest.approx(expected)
    assert sum(event.spread_cost for event in result.events) > 0


def test_termination_bounds_are_explicit() -> None:
    terminated = PriceSeries(
        "AAA",
        (
            PriceBar(date(2022, 6, 30), 100.0, 100.0),
            PriceBar(date(2022, 7, 1), 100.0, 100.0),
            PriceBar(date(2023, 1, 3), 120.0, 120.0),
        ),
        0,
    )
    schedule = ((datetime(2022, 6, 30, 16), frozenset({"AAA"})),)
    best = simulate_portfolio(schedule=schedule, prices={"AAA": terminated}, case="best", half_spread=0)
    worst = simulate_portfolio(schedule=schedule, prices={"AAA": terminated}, case="worst", half_spread=0)
    assert best.total_return == pytest.approx(0.20)
    assert worst.total_return == -1.0


def test_in_series_halt_uses_same_declared_bounds() -> None:
    halted = PriceSeries(
        "AAA",
        (
            PriceBar(date(2022, 6, 30), 100.0, 100.0),
            PriceBar(date(2022, 7, 1), 100.0, 100.0),
            PriceBar(date(2023, 1, 3), 120.0, 120.0),
            PriceBar(date(2025, 1, 2), 80.0, 80.0),
        ),
        0,
    )
    schedule = ((datetime(2022, 6, 30, 16), frozenset({"AAA"})),)
    best = simulate_portfolio(schedule=schedule, prices={"AAA": halted}, case="best", half_spread=0)
    worst = simulate_portfolio(schedule=schedule, prices={"AAA": halted}, case="worst", half_spread=0)
    assert best.total_return == pytest.approx(0.20)
    assert worst.total_return == -1.0
    assert best.events[-1].censored_holdings == 1


def test_haircut_never_rescues_negative_gross_edge() -> None:
    assert haircut_net_return(
        strategy_gross=0.05,
        strategy_net=0.04,
        buy_hold_gross=0.10,
        haircut=0.58,
    ) == pytest.approx(0.04)
    assert haircut_net_return(
        strategy_gross=0.15,
        strategy_net=0.13,
        buy_hold_gross=0.10,
        haircut=0.58,
    ) == pytest.approx(0.101)


def test_factor_gate_requires_contemporaneous_positive_identity() -> None:
    keys = [(2022 + index // 12, index % 12 + 1) for index in range(24)]
    reference = {key: math.sin(index * 1.7) for index, key in enumerate(keys)}
    ours = {key: 0.003 + 1.2 * reference[key] for key in keys}
    result = validate_factor(ours, reference)
    assert result.passed
    assert result.correlation == pytest.approx(1.0)
    assert result.beta == pytest.approx(1.2)


def test_factor_gate_rejects_a_one_month_displacement() -> None:
    keys = [(2022 + index // 12, index % 12 + 1) for index in range(24)]
    reference = {key: math.sin(index * 1.7) for index, key in enumerate(keys)}
    ours = {key: reference[keys[index - 1]] if index else 0.0 for index, key in enumerate(keys)}
    assert not validate_factor(ours, reference).passed


def _bars(symbol: str, rows: tuple[tuple[str, float, float], ...]) -> PriceSeries:
    return PriceSeries(symbol, tuple(PriceBar(date.fromisoformat(day), o, c) for day, o, c in rows), 0)


#: #3362 acceptance 1 — a multi-rebalance fixture pinned on the PRE-refactor harness (``case="best"`` /
#: ``case="worst"``): a live name, a gap at a rebalance (GAP), a mid-window termination (DED), a
#: second-formation entry (NEW) and a name whose last bar sits inside the alive-at-capture cut (ALV).
REPLAY_PRICES = {
    "LIV": _bars(
        "LIV",
        (
            ("2022-06-30", 100, 100),
            ("2022-07-01", 100, 101),
            ("2023-07-03", 110, 111),
            ("2024-07-01", 120, 119),
            ("2024-09-27", 125, 126),
        ),
    ),
    "GAP": _bars(
        "GAP",
        (
            ("2022-06-30", 50, 50),
            ("2022-07-01", 50, 52),
            ("2023-06-29", 40, 41),
            ("2024-07-01", 45, 46),
            ("2024-09-27", 47, 48),
        ),
    ),
    "DED": _bars("DED", (("2022-06-30", 20, 20), ("2022-07-01", 20, 21), ("2023-01-03", 15, 14))),
    "NEW": _bars(
        "NEW",
        (("2023-06-30", 30, 30), ("2023-07-03", 31, 32), ("2024-07-01", 33, 34), ("2024-09-27", 35, 36)),
    ),
    "ALV": _bars(
        "ALV",
        (("2023-06-30", 10, 10), ("2023-07-03", 10, 11), ("2024-07-01", 12, 13), ("2024-09-25", 14, 15)),
    ),
}
REPLAY_SCHEDULE = (
    (datetime(2022, 6, 30, 16), frozenset({"LIV", "GAP", "DED"})),
    (datetime(2023, 6, 30, 16), frozenset({"LIV", "NEW", "ALV"})),
    (datetime(2024, 6, 28, 16), frozenset({"LIV", "GAP", "ALV"})),
)
#: (total_return, per-event (pre_cost_wealth, traded_notional, spread_cost, target_count, censored_holdings)).
REPLAY_GOLDEN = {
    "best": (
        0.06470303112516507,
        (
            (1.0, 0.9928021841648053, 0.007197815835194838, 3, 0),
            (0.8670472408372634, 1.1532759043475114, 0.008361250306519458, 3, 2),
            (0.9604188762123922, 0.6544779443800937, 0.004744965096755679, 3, 0),
            (1.072478500251992, 1.072478500251992, 0.007775469126826943, 0, 1),
        ),
    ),
    "worst": (
        -0.7189573474261477,
        (
            (1.0, 0.9928021841648053, 0.007197815835194838, 3, 0),
            (0.36402746752709525, 0.4841998071688024, 0.0035104486019738174, 3, 2),
            (0.4032292991731417, 0.27478081634278734, 0.0019921609184852082, 3, 0),
            (0.2830950919907854, 0.2830950919907854, 0.0020524394169331942, 0, 1),
        ),
    ),
}


@pytest.mark.parametrize("case", ["best", "worst"])
def test_multi_rebalance_replay_is_pinned(case: str) -> None:
    result = simulate_portfolio(
        schedule=REPLAY_SCHEDULE,
        prices=REPLAY_PRICES,
        case=case,  # type: ignore[arg-type]
        half_spread=HALF_SPREAD,
    )
    total, events = REPLAY_GOLDEN[case]
    assert result.total_return == pytest.approx(total, rel=1e-12, abs=1e-15)
    assert len(result.events) == len(events)
    for event, expected in zip(result.events, events, strict=True):
        observed = (event.pre_cost_wealth, event.traded_notional, event.spread_cost)
        assert observed == pytest.approx(expected[:3], rel=1e-12, abs=1e-15)
        assert (event.target_count, event.censored_holdings) == expected[3:]
