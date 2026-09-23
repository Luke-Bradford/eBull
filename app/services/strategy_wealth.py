"""Compact, exact-owned strategy-pot wealth history.

The strategy sleeve does not duplicate quotes or positions.  It joins the
main portfolio's once-per-session position evidence to durable exact broker
position ownership, then adds reconciled close P&L and the configured USD
principal.  Missing marks or close P&L make a point incomplete; they are never
coerced to zero.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any, Literal

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


ReturnUnavailableReason = Literal["no_complete_point", "unfunded", "insufficient_history", "chain_broken"]


@dataclass(frozen=True)
class PointReturn:
    period_return: Decimal | None
    period_start: date | None
    cumulative_return: Decimal | None


@dataclass(frozen=True)
class TimeWeightedReturns:
    points: tuple[PointReturn, ...]
    #: Describes the LAST COMPLETE point — the one the UI summary strip reads.
    total_return_available: bool
    return_since: date | None
    unavailable_reason: ReturnUnavailableReason | None


def time_weighted_returns(points: Sequence[StrategyWealthPoint]) -> TimeWeightedReturns:
    """Geometrically linked sub-period returns over the pot NAV (#3334 item 3).

    Spec: ``docs/specs/metrics/2026-09-23-3334-pot-time-weighted-return.md``.
    GIPS 2020 2.A.24(f) links sub-periods; flow timing is the firm's policy
    (2.A.24(e)).  Ours, by construction: a flow reported on a point arrives at
    the START of the sub-period ending there (``D = V_base + F``) — the only
    treatment defined at inception, where ``V_base = 0``.

    ⚠ Any flow inside an unvalued span (an incomplete point) breaks the chain,
    and so does a pot valued at or below zero: neither sub-period can be
    isolated honestly.  After a break ``cumulative_return`` stays NULL for the
    rest of the window, but ``period_return`` keeps being computed.
    """
    out: list[PointReturn] = []
    null = PointReturn(None, None, None)
    base: StrategyWealthPoint | None = None
    growth = Decimal("1")
    started = broken = gap = gap_flow = False
    pending_flow = Decimal("0")
    return_since: date | None = None
    last_complete_cumulative: Decimal | None = None
    last_complete_reason: ReturnUnavailableReason | None = "no_complete_point"

    for point in points:
        if not point.complete or point.pot_value is None:
            if base is not None:
                gap = True
                gap_flow = gap_flow or point.external_flow != 0
                pending_flow += point.external_flow
            out.append(null)
            continue

        value = point.pot_value
        if base is None:
            # The first complete point is the base; flows up to it are what it
            # is valued at, so they are not returns.
            base = point
            out.append(null)
            last_complete_cumulative = None
            last_complete_reason = "unfunded" if point.principal == 0 and value == 0 else "insufficient_history"
            continue

        assert base.pot_value is not None
        base_value = base.pot_value
        flow = pending_flow + point.external_flow
        denominator = base_value + flow
        result = null
        if not started and point.principal == 0 and value == 0:
            last_complete_reason = "unfunded"
        elif (
            (gap and (gap_flow or point.external_flow != 0))
            or base_value < 0
            or (started and base_value == 0)
            or denominator <= 0
            or value <= 0
        ):
            broken = True
        else:
            period_return = value / denominator - 1
            cumulative: Decimal | None = None
            if not broken:
                growth *= 1 + period_return
                cumulative = growth - 1
                if not started:
                    started = True
                    return_since = base.date if base_value > 0 else point.date
            result = PointReturn(period_return, base.date, cumulative)

        if broken:
            last_complete_reason = "chain_broken"
        elif result.cumulative_return is not None:
            last_complete_reason = None
        last_complete_cumulative = result.cumulative_return
        out.append(result)
        base = point
        gap = gap_flow = False
        pending_flow = Decimal("0")

    available = last_complete_cumulative is not None
    return TimeWeightedReturns(
        points=tuple(out),
        total_return_available=available,
        return_since=return_since if available else None,
        unavailable_reason=None if available else last_complete_reason,
    )


__all__ = [
    "PointReturn",
    "ReturnUnavailableReason",
    "StrategyWealthPoint",
    "TimeWeightedReturns",
    "load_strategy_wealth_history",
    "time_weighted_returns",
]
