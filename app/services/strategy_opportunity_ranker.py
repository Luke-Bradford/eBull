"""Deterministic precursor to portfolio-level opportunity allocation.

This ranks already-calibrated, positive conservative forecasts.  It does not
claim to optimise a portfolio: correlation, factor and core/cash competition
remain the responsibility of the immutable batch allocator in #2525.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

RANKING_POLICY_VERSION = "conservative-opportunity-rank-v1"


class OpportunityRankingError(ValueError):
    """The proposed opportunity set cannot be ranked safely."""


@dataclass(frozen=True)
class RankableOpportunity:
    signal_id: int
    strategy_id: str
    strategy_version: str
    instrument_id: int
    signal_bar_date: date
    side: str
    horizon_market_days: int
    setup_version: str
    exit_policy_version: str
    decided_at: datetime
    conservative_net_expectancy_pct: Decimal

    @property
    def economic_key(self) -> tuple[str, str, int, date, str, int, str, str, datetime]:
        """Stable identity containing no database arrival or surrogate ids."""
        return (
            self.strategy_id,
            self.strategy_version,
            self.instrument_id,
            self.signal_bar_date,
            self.side,
            self.horizon_market_days,
            self.setup_version,
            self.exit_policy_version,
            self.decided_at,
        )


def rank_positive_opportunities(opportunities: list[RankableOpportunity]) -> list[RankableOpportunity]:
    """Return positive forecasts strongest-first with deterministic ties.

    Duration is deliberately not used as an ``EV / time`` shortcut: the
    programme contract requires portfolio simulation of overlapping capital
    occupation before duration can influence allocation.
    """
    positive: list[RankableOpportunity] = []
    seen: set[tuple[str, str, int, date, str, int, str, str, datetime]] = set()
    for opportunity in opportunities:
        expectancy = opportunity.conservative_net_expectancy_pct
        if not expectancy.is_finite():
            raise OpportunityRankingError("conservative expectancy must be finite")
        if opportunity.economic_key in seen:
            raise OpportunityRankingError("duplicate economic opportunity identity")
        seen.add(opportunity.economic_key)
        if expectancy > 0:
            positive.append(opportunity)
    return sorted(
        positive,
        key=lambda opportunity: (-opportunity.conservative_net_expectancy_pct, opportunity.economic_key),
    )


__all__ = [
    "RANKING_POLICY_VERSION",
    "OpportunityRankingError",
    "RankableOpportunity",
    "rank_positive_opportunities",
]
