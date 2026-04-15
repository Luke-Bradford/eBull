"""
Reporting service — weekly (and future monthly) performance report snapshots.

Reads from existing tables and returns plain dicts suitable for JSONB storage
in the report_snapshots table.

All values are current-state snapshots, not true period deltas.
Decimal values are serialised to strings for JSON compatibility.

Issue: #207
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import psycopg
import psycopg.rows

from app.services.budget import compute_budget_state

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

WeeklyReport = dict[str, Any]
MonthlyReport = dict[str, Any]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _dec(v: Decimal | None) -> str | None:
    """Decimal → str for JSON serialisation, preserving None."""
    return str(v) if v is not None else None


# ---------------------------------------------------------------------------
# Section functions
# ---------------------------------------------------------------------------


def _pnl_snapshot(conn: psycopg.Connection[Any]) -> dict[str, Any]:
    """Current realised + unrealised P&L totals from the positions table.

    This is a current-state snapshot, not a period delta.  Both realized_pnl
    and unrealized_pnl are the running totals on open positions.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT COALESCE(SUM(realized_pnl), 0)   AS realized,
                   COALESCE(SUM(unrealized_pnl), 0) AS unrealized
            FROM positions
            WHERE current_units > 0
            """
        )
        row = cur.fetchone()

    realized: Decimal = row["realized"] if row else Decimal("0")
    unrealized: Decimal = row["unrealized"] if row else Decimal("0")
    total = realized + unrealized
    return {
        "realized_pnl": _dec(realized),
        "unrealized_pnl": _dec(unrealized),
        "total_pnl": _dec(total),
        "note": "current-state snapshot, not period delta",
    }


def _top_bottom_performers(
    conn: psycopg.Connection[Any],
    n: int = 3,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Top N and bottom N open positions by unrealized_pnl.

    Returns (top_list, bottom_list).  If total open positions <= n, the bottom
    list is empty to avoid duplicating entries that already appear in the top.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT p.instrument_id,
                   i.symbol,
                   i.company_name,
                   p.unrealized_pnl,
                   p.current_units,
                   p.avg_cost
            FROM positions p
            JOIN instruments i USING (instrument_id)
            WHERE p.current_units > 0
            ORDER BY p.unrealized_pnl DESC
            """
        )
        rows = cur.fetchall()

    def _row_to_dict(r: dict[str, Any]) -> dict[str, Any]:
        return {
            "instrument_id": r["instrument_id"],
            "symbol": r["symbol"],
            "company_name": r["company_name"],
            "unrealized_pnl": _dec(r["unrealized_pnl"]),
        }

    total = len(rows)
    top = [_row_to_dict(r) for r in rows[:n]]
    # Avoid duplicates: if there are n or fewer positions, bottom is empty.
    bottom = [_row_to_dict(r) for r in rows[max(n, total - n) :]] if total > n else []
    return top, bottom


def _positions_opened_closed(
    conn: psycopg.Connection[Any],
    period_start: date,
    period_end: date,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """BUY and EXIT fills in the report period.

    Joins recommendations via orders.recommendation_id (not through
    decision_audit).
    """

    def _fetch(action: str) -> list[dict[str, Any]]:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT o.instrument_id,
                       i.symbol,
                       o.action,
                       tr.rationale,
                       f.price,
                       f.units,
                       f.filled_at
                FROM fills f
                JOIN orders o USING (order_id)
                JOIN instruments i ON i.instrument_id = o.instrument_id
                LEFT JOIN trade_recommendations tr
                       ON tr.recommendation_id = o.recommendation_id
                WHERE o.action = %(action)s
                  AND f.filled_at >= %(start)s
                  AND f.filled_at < %(end)s::date + 1
                ORDER BY f.filled_at
                """,
                {"action": action, "start": period_start, "end": period_end},
            )
            raw = cur.fetchall()
        return [
            {
                "instrument_id": r["instrument_id"],
                "symbol": r["symbol"],
                "action": r["action"],
                "rationale": r["rationale"],
                "price": _dec(r["price"]),
                "units": _dec(r["units"]),
                "filled_at": r["filled_at"].isoformat() if r["filled_at"] is not None else None,
            }
            for r in raw
        ]

    opened = _fetch("BUY")
    closed = _fetch("EXIT")
    return opened, closed


def _upcoming_earnings(
    conn: psycopg.Connection[Any],
    lookahead_days: int = 14,
) -> list[dict[str, Any]]:
    """Upcoming earnings events for currently held positions."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT ee.instrument_id,
                   i.symbol,
                   i.company_name,
                   ee.reporting_date,
                   ee.eps_estimate
            FROM earnings_events ee
            JOIN instruments i USING (instrument_id)
            JOIN positions p USING (instrument_id)
            WHERE p.current_units > 0
              AND ee.reporting_date >= CURRENT_DATE
              AND ee.reporting_date < CURRENT_DATE + make_interval(days => %(days)s)
            ORDER BY ee.reporting_date
            """,
            {"days": lookahead_days},
        )
        rows = cur.fetchall()
    return [
        {
            "instrument_id": r["instrument_id"],
            "symbol": r["symbol"],
            "company_name": r["company_name"],
            "reporting_date": r["reporting_date"].isoformat() if r["reporting_date"] is not None else None,
            "eps_estimate": _dec(r["eps_estimate"]),
        }
        for r in rows
    ]


def _score_changes(
    conn: psycopg.Connection[Any],
    period_start: date,
    period_end: date,
    min_rank_delta: int = 5,
) -> list[dict[str, Any]]:
    """Significant rank movements in the report period.

    Filters to rows where ABS(rank_delta) >= min_rank_delta.
    rank and rank_delta were added to scores in migration 007.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT s.instrument_id,
                   i.symbol,
                   s.total_score,
                   s.rank,
                   s.rank_delta,
                   s.scored_at
            FROM scores s
            JOIN instruments i USING (instrument_id)
            WHERE s.scored_at >= %(start)s
              AND s.scored_at < %(end)s::date + 1
              AND s.rank_delta IS NOT NULL
              AND ABS(s.rank_delta) >= %(min_delta)s
            ORDER BY ABS(s.rank_delta) DESC
            """,
            {"start": period_start, "end": period_end, "min_delta": min_rank_delta},
        )
        rows = cur.fetchall()
    return [
        {
            "instrument_id": r["instrument_id"],
            "symbol": r["symbol"],
            "total_score": _dec(r["total_score"]),
            "rank": r["rank"],
            "rank_delta": r["rank_delta"],
            "scored_at": r["scored_at"].isoformat() if r["scored_at"] is not None else None,
        }
        for r in rows
    ]


def _budget_snapshot(conn: psycopg.Connection[Any]) -> dict[str, Any]:
    """Current budget state via compute_budget_state."""
    budget = compute_budget_state(conn)
    return {
        "cash_balance": _dec(budget.cash_balance),
        "deployed_capital": _dec(budget.deployed_capital),
        "estimated_tax_usd": _dec(budget.estimated_tax_usd),
        "available_for_deployment": _dec(budget.available_for_deployment),
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def generate_weekly_report(
    conn: psycopg.Connection[Any],
    period_start: date,
    period_end: date,
) -> WeeklyReport:
    """Generate a weekly performance report snapshot.

    Reads from positions, fills, orders, instruments, trade_recommendations,
    earnings_events, scores, and the budget service.

    The caller owns the transaction; this function never calls conn.commit().

    Returns a plain dict suitable for storage in report_snapshots.snapshot_json.
    """
    pnl = _pnl_snapshot(conn)
    top_performers, bottom_performers = _top_bottom_performers(conn)
    positions_opened, positions_closed = _positions_opened_closed(conn, period_start, period_end)
    upcoming_earnings = _upcoming_earnings(conn)
    score_changes = _score_changes(conn, period_start, period_end)
    budget = _budget_snapshot(conn)

    return {
        "report_type": "weekly",
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "pnl": pnl,
        "top_performers": top_performers,
        "bottom_performers": bottom_performers,
        "positions_opened": positions_opened,
        "positions_closed": positions_closed,
        "upcoming_earnings": upcoming_earnings,
        "score_changes": score_changes,
        "budget": budget,
    }
