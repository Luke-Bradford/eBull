"""Determinism and refusal boundaries for opportunity ranking."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from app.services.strategy_opportunity_ranker import (
    OpportunityRankingError,
    RankableOpportunity,
    rank_positive_opportunities,
)


def _opportunity(*, signal_id: int, instrument_id: int, expectancy: str) -> RankableOpportunity:
    return RankableOpportunity(
        signal_id=signal_id,
        forecast_id=signal_id + 1000,
        strategy_id="candidate-a",
        strategy_version="v1",
        instrument_id=instrument_id,
        signal_bar_date=date(2026, 8, 10),
        side="long",
        horizon_market_days=5,
        setup_version="setup-v1",
        exit_policy_version="exit-v1",
        decided_at=datetime(2026, 8, 11, 14, tzinfo=UTC),
        conservative_net_expectancy_pct=Decimal(expectancy),
    )


def test_stronger_opportunity_wins_regardless_of_input_and_signal_id_order() -> None:
    weak = _opportunity(signal_id=900, instrument_id=1, expectancy="0.2")
    strong = _opportunity(signal_id=1, instrument_id=2, expectancy="1.1")

    forward = rank_positive_opportunities([weak, strong])
    reverse = rank_positive_opportunities([replace(strong, signal_id=999), replace(weak, signal_id=2)])

    assert [item.instrument_id for item in forward] == [2, 1]
    assert [item.economic_key for item in forward] == [item.economic_key for item in reverse]


def test_ties_use_economic_identity_and_non_positive_values_abstain() -> None:
    later_key = _opportunity(signal_id=1, instrument_id=20, expectancy="0.5")
    earlier_key = _opportunity(signal_id=999, instrument_id=10, expectancy="0.5")
    no_trade = _opportunity(signal_id=3, instrument_id=30, expectancy="0")

    ranked = rank_positive_opportunities([later_key, no_trade, earlier_key])

    assert [item.instrument_id for item in ranked] == [10, 20]


def test_duplicate_economic_identity_fails_closed() -> None:
    opportunity = _opportunity(signal_id=1, instrument_id=10, expectancy="0.5")

    with pytest.raises(OpportunityRankingError, match="duplicate economic"):
        rank_positive_opportunities([opportunity, replace(opportunity, signal_id=2)])
