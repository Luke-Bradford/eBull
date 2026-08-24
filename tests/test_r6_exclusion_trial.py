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
