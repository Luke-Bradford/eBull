from __future__ import annotations

from datetime import date, timedelta

import pytest

from app.services.market_regime import Regime
from app.services.strategy_regime_evidence import RegimeTradeObservation, build_regime_cohorts


def _rows() -> list[RegimeTradeObservation]:
    start = date(2024, 1, 1)
    regimes = (Regime.BULL_QUIET, Regime.BEAR_QUIET, None)
    return [
        RegimeTradeObservation(
            instrument_key=(index % 4) + 1,
            signal_date=start + timedelta(days=index),
            net_return_pct=(1.0 if index % 3 else -0.5) + index / 100,
            regime=regimes[index % len(regimes)],
        )
        for index in range(36)
    ]


def test_every_trade_lands_in_exactly_one_closed_regime_cohort() -> None:
    observations = _rows()
    cohorts = build_regime_cohorts(observations, root_seed=4242)
    assert [row.regime for row in cohorts] == ["bear_quiet", "bull_quiet", "unclassified"]
    assert sum(row.trade_count for row in cohorts) == len(observations)
    assert all(row.instrument_count == 4 for row in cohorts)
    assert all(row.decision_date_count == row.trade_count for row in cohorts)


def test_bootstrap_is_deterministic_per_regime() -> None:
    first = build_regime_cohorts(_rows(), root_seed=4242)
    second = build_regime_cohorts(_rows(), root_seed=4242)
    assert first == second
    assert all(row.effective_sample_size is not None for row in first)
    assert len({row.bootstrap_seed for row in first}) == len(first)


def test_non_finite_return_is_refused_before_it_can_reach_storage() -> None:
    with pytest.raises(ValueError, match="net_return_pct must be finite"):
        RegimeTradeObservation(
            instrument_key=1,
            signal_date=date(2024, 1, 1),
            net_return_pct=float("nan"),
            regime=Regime.BULL_QUIET,
        )
