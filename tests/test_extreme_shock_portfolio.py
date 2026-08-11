from __future__ import annotations

from datetime import date, timedelta

import pytest

from app.services.extreme_shock_portfolio import (
    PortfolioStressConfig,
    ShockTradePath,
    simulate_extreme_shock_portfolio,
)

DAY = date(2026, 1, 5)


def _trade(
    trade_id: str,
    *,
    result: float = 0.10,
    sector: str | None = "technology",
    entry_offset: int = 0,
    duration: int = 1,
) -> ShockTradePath:
    entry = DAY + timedelta(days=entry_offset)
    marks = tuple((entry + timedelta(days=index), result * (index + 1) / duration) for index in range(duration))
    return ShockTradePath(
        trade_id=trade_id,
        series_id=int(trade_id.removeprefix("t")),
        entry_date=entry,
        exit_date=marks[-1][0],
        sector=sector,
        cumulative_returns=marks,
    )


def test_rejects_leverage_and_malformed_paths() -> None:
    with pytest.raises(ValueError, match="does not permit leverage"):
        PortfolioStressConfig(per_name_cap=0.1, gross_cap=1.01)
    with pytest.raises(ValueError, match="span entry_date"):
        ShockTradePath(
            trade_id="bad",
            series_id=1,
            entry_date=DAY,
            exit_date=DAY + timedelta(days=1),
            sector=None,
            cumulative_returns=((DAY, 0.0),),
        )


def test_equal_signal_batch_is_permutation_invariant() -> None:
    trades = [_trade("t1", result=0.10, sector="a"), _trade("t2", result=-0.05, sector="b")]
    config = PortfolioStressConfig(per_name_cap=0.5, sector_cap=None, round_trip_cost=0, carry_cost=0)

    forwards = simulate_extreme_shock_portfolio(trades, config)
    backwards = simulate_extreme_shock_portfolio(list(reversed(trades)), config)

    assert forwards == backwards
    assert forwards.funded_trades == 2
    assert forwards.max_gross_exposure == pytest.approx(1.0)
    assert forwards.ending_return == pytest.approx(0.025)
    assert forwards.capital_weighted_trade_return == pytest.approx(0.025)
    assert forwards.annual_returns[0][0] == 2026
    assert forwards.annual_returns[0][1] == pytest.approx(0.025)


def test_sector_cap_treats_unknown_as_one_conservative_bucket() -> None:
    trades = [_trade("t1", sector=None), _trade("t2", sector=None)]
    result = simulate_extreme_shock_portfolio(
        trades,
        PortfolioStressConfig(per_name_cap=0.5, sector_cap=0.25, round_trip_cost=0, carry_cost=0),
    )

    assert result.max_sector_exposure == pytest.approx(0.25)
    assert result.max_gross_exposure == pytest.approx(0.25)
    assert result.unknown_sector_funded_pct == 1.0


def test_costs_are_charged_once_across_a_multi_mark_path() -> None:
    trade = _trade("t1", result=0.10, duration=2)
    result = simulate_extreme_shock_portfolio(
        [trade],
        PortfolioStressConfig(
            per_name_cap=1.0,
            sector_cap=None,
            round_trip_cost=0.005,
            carry_cost=0.006,
        ),
    )

    assert result.ending_return == pytest.approx(0.089)
    assert result.max_gross_exposure == pytest.approx(1.0)


def test_declared_one_name_wipeout_worsens_the_observed_portfolio() -> None:
    result = simulate_extreme_shock_portfolio(
        [_trade("t1", result=0.10)],
        PortfolioStressConfig(per_name_cap=0.5, sector_cap=None, round_trip_cost=0, carry_cost=0),
    )

    assert result.max_drawdown == 0.0
    assert result.ending_return == pytest.approx(0.05)
    assert result.one_name_loss_stressed_ending_return == pytest.approx(-0.45)
    assert result.one_name_loss_stressed_max_drawdown == pytest.approx(-0.45)
