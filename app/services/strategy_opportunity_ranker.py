"""Deterministic precursor to portfolio-level opportunity allocation.

This ranks already-calibrated, positive conservative forecasts.  It does not
claim to optimise a portfolio: correlation, factor and core/cash competition
remain the responsibility of the immutable batch allocator in #2525.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import psycopg

RANKING_POLICY_VERSION = "conservative-opportunity-rank-v1"


class OpportunityRankingError(ValueError):
    """The proposed opportunity set cannot be ranked safely."""


@dataclass(frozen=True)
class RankableOpportunity:
    signal_id: int
    forecast_id: int
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


@dataclass(frozen=True)
class RankingMember:
    ranking_member_id: int
    opportunity: RankableOpportunity
    selected: bool


def _candidate_set_digest(opportunities: list[RankableOpportunity]) -> str:
    payload = [
        [
            opportunity.strategy_id,
            opportunity.strategy_version,
            opportunity.instrument_id,
            opportunity.signal_bar_date.isoformat(),
            opportunity.side,
            opportunity.horizon_market_days,
            opportunity.setup_version,
            opportunity.exit_policy_version,
            opportunity.decided_at.isoformat(),
            format(opportunity.conservative_net_expectancy_pct.normalize(), "f"),
        ]
        for opportunity in opportunities
    ]
    encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode()
    return hashlib.sha256(encoded).hexdigest()


def persist_ranking_batch(
    conn: psycopg.Connection[Any],
    *,
    opportunities: list[RankableOpportunity],
    selection_limit: int,
    decided_at: datetime,
) -> list[RankingMember]:
    """Persist or reuse one narrow batch and return every ranked member."""
    if selection_limit <= 0:
        raise OpportunityRankingError("selection limit must be positive")
    ranked = rank_positive_opportunities(opportunities)
    if not ranked:
        return []
    selected_count = min(selection_limit, len(ranked))
    digest = _candidate_set_digest(ranked)
    with conn.transaction():
        pool = conn.execute(
            """
            SELECT strategy_paper_pool_event_id
            FROM strategy_paper_pool_events
            ORDER BY strategy_paper_pool_event_id DESC LIMIT 1
            """
        ).fetchone()
        if pool is None:
            raise OpportunityRankingError("paper pool mandate is missing")
        batch = conn.execute(
            """
            INSERT INTO strategy_opportunity_ranking_batches (
                decided_at,ranking_policy_version,strategy_paper_pool_event_id,
                selection_limit,considered_count,selected_count,candidate_set_sha256
            ) VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (
                ranking_policy_version,strategy_paper_pool_event_id,
                selection_limit,candidate_set_sha256
            ) DO NOTHING
            RETURNING ranking_batch_id
            """,
            (
                decided_at,
                RANKING_POLICY_VERSION,
                int(pool[0]),
                selection_limit,
                len(ranked),
                selected_count,
                digest,
            ),
        ).fetchone()
        if batch is None:
            batch = conn.execute(
                """
                SELECT ranking_batch_id
                FROM strategy_opportunity_ranking_batches
                WHERE ranking_policy_version=%s
                  AND strategy_paper_pool_event_id=%s
                  AND selection_limit=%s AND candidate_set_sha256=%s
                """,
                (RANKING_POLICY_VERSION, int(pool[0]), selection_limit, digest),
            ).fetchone()
        if batch is None:  # pragma: no cover - conflict lookup must find the row
            raise OpportunityRankingError("ranking batch identity was not returned")
        batch_id = int(batch[0])
        for rank, opportunity in enumerate(ranked, start=1):
            selected = rank <= selected_count
            conn.execute(
                """
                INSERT INTO strategy_opportunity_ranking_members (
                    ranking_batch_id,forecast_id,rank,
                    conservative_net_expectancy_pct,selected,reason_code
                ) VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (ranking_batch_id,forecast_id) DO NOTHING
                """,
                (
                    batch_id,
                    opportunity.forecast_id,
                    rank,
                    opportunity.conservative_net_expectancy_pct,
                    selected,
                    "selected_for_execution" if selected else "below_execution_batch_limit",
                ),
            )
        rows = conn.execute(
            """
            SELECT ranking_member_id,forecast_id,selected
            FROM strategy_opportunity_ranking_members
            WHERE ranking_batch_id=%s
            ORDER BY rank
            """,
            (batch_id,),
        ).fetchall()
        by_forecast = {opportunity.forecast_id: opportunity for opportunity in ranked}
        if len(rows) != len(ranked) or sum(bool(row[2]) for row in rows) != selected_count:
            raise OpportunityRankingError("persisted ranking batch is incomplete")
        return [RankingMember(int(row[0]), by_forecast[int(row[1])], bool(row[2])) for row in rows]


__all__ = [
    "RANKING_POLICY_VERSION",
    "OpportunityRankingError",
    "RankableOpportunity",
    "RankingMember",
    "persist_ranking_batch",
    "rank_positive_opportunities",
]
