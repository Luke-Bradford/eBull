"""Compact, exact-owned strategy-pot wealth history.

The strategy sleeve does not duplicate quotes or positions.  It joins the
main portfolio's once-per-session position evidence to durable exact broker
position ownership, then adds reconciled close P&L and the configured USD
principal.  Missing marks or close P&L make a point incomplete; they are never
coerced to zero.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

import psycopg
import psycopg.rows


@dataclass(frozen=True)
class StrategyWealthPoint:
    date: date
    principal: Decimal
    external_flow: Decimal
    realised_pnl: Decimal | None
    unrealised_pnl: Decimal | None
    total_pnl: Decimal | None
    pot_value: Decimal | None
    complete: bool
    incomplete_reasons: tuple[str, ...]


_WEALTH_SQL = """
    WITH snapshots AS (
        SELECT snapshot_date, computed_at
        FROM portfolio_eod_snapshots
        WHERE snapshot_date >= CURRENT_DATE - %(days)s
    ), owned_marks AS (
        SELECT snap.snapshot_date,
               COUNT(own.ownership_id) AS expected_positions,
               COUNT(pos.position_id) AS observed_positions,
               COUNT(pos.position_id) FILTER (
                   WHERE pos.position_id IS NOT NULL AND pos.unrealised_pnl_usd IS NULL
               ) AS missing_marks,
               COALESCE(SUM(pos.unrealised_pnl_usd), 0) AS unrealised_pnl
        FROM snapshots snap
        LEFT JOIN strategy_position_ownership own
          ON (own.claimed_at AT TIME ZONE 'UTC')::date <= snap.snapshot_date
         AND (own.released_at IS NULL OR (own.released_at AT TIME ZONE 'UTC')::date > snap.snapshot_date)
        LEFT JOIN portfolio_eod_position_snapshots pos
          ON pos.snapshot_date=snap.snapshot_date
         AND pos.position_id=own.broker_position_id
        GROUP BY snap.snapshot_date
    ), owned_broker_positions AS (
        -- broker_position_id is UNIQUE in the ownership ledger today.  Keep
        -- the aggregation boundary explicit so a future history-table shape
        -- cannot multiply one close event before SUM(realized_pnl_usd).
        SELECT DISTINCT broker_position_id
        FROM strategy_position_ownership
    ), realised AS (
        SELECT snap.snapshot_date,
               COALESCE(SUM(event.realized_pnl_usd), 0) AS realised_pnl,
               COUNT(*) FILTER (
                   WHERE event.event_id IS NOT NULL AND event.realized_pnl_usd IS NULL
               ) AS missing_realised
        FROM snapshots snap
        LEFT JOIN owned_broker_positions own ON TRUE
        LEFT JOIN trade_events event
          ON event.position_id=own.broker_position_id
         AND event.event_kind='close'
         AND (event.executed_at AT TIME ZONE 'UTC')::date <= snap.snapshot_date
        GROUP BY snap.snapshot_date
    ), released_without_close AS (
        SELECT snap.snapshot_date, COUNT(DISTINCT own.broker_position_id) AS missing_closes
        FROM snapshots snap
        JOIN strategy_position_ownership own
          ON own.status='released'
         AND (own.released_at AT TIME ZONE 'UTC')::date <= snap.snapshot_date
        LEFT JOIN trade_events event
          ON event.position_id=own.broker_position_id AND event.event_kind='close'
        WHERE event.event_id IS NULL
        GROUP BY snap.snapshot_date
    )
    SELECT snap.snapshot_date,
           COALESCE(pool.capital_limit, 0) AS principal,
           marks.expected_positions, marks.observed_positions, marks.missing_marks,
           marks.unrealised_pnl, realised.realised_pnl, realised.missing_realised,
           COALESCE(missing.missing_closes, 0) AS missing_closes
    FROM snapshots snap
    JOIN owned_marks marks USING (snapshot_date)
    JOIN realised USING (snapshot_date)
    LEFT JOIN released_without_close missing USING (snapshot_date)
    LEFT JOIN LATERAL (
        SELECT capital_limit
        FROM strategy_paper_pool_events
        WHERE (changed_at AT TIME ZONE 'UTC')::date <= snap.snapshot_date
        ORDER BY strategy_paper_pool_event_id DESC
        LIMIT 1
    ) pool ON TRUE
    ORDER BY snap.snapshot_date
"""


def load_strategy_wealth_history(conn: psycopg.Connection[Any], *, days: int = 365) -> list[StrategyWealthPoint]:
    """Read a bounded daily strategy NAV series without periodic extra rows."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(_WEALTH_SQL, {"days": days})
        rows = list(cur.fetchall())

    points: list[StrategyWealthPoint] = []
    previous_principal = Decimal("0")
    for row in rows:
        principal = Decimal(str(row["principal"]))
        reasons: list[str] = []
        if int(row["observed_positions"]) != int(row["expected_positions"]):
            reasons.append("owned_position_snapshot_missing")
        if int(row["missing_marks"]):
            reasons.append("owned_position_mark_missing")
        if int(row["missing_realised"]):
            reasons.append("realised_pnl_missing_from_history")
        if int(row["missing_closes"]):
            reasons.append("released_position_missing_close_history")

        complete = not reasons
        realised = Decimal(str(row["realised_pnl"])) if complete else None
        unrealised = Decimal(str(row["unrealised_pnl"])) if complete else None
        total = realised + unrealised if realised is not None and unrealised is not None else None
        points.append(
            StrategyWealthPoint(
                date=row["snapshot_date"],
                principal=principal,
                external_flow=principal - previous_principal,
                realised_pnl=realised,
                unrealised_pnl=unrealised,
                total_pnl=total,
                pot_value=principal + total if total is not None else None,
                complete=complete,
                incomplete_reasons=tuple(reasons),
            )
        )
        previous_principal = principal
    return points


__all__ = ["StrategyWealthPoint", "load_strategy_wealth_history"]
