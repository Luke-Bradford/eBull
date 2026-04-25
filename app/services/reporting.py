"""
Reporting service — weekly (and future monthly) performance report snapshots.

Reads from existing tables and returns plain dicts suitable for JSONB storage
in the report_snapshots table.

All values are current-state snapshots, not true period deltas.
Decimal values are serialised to strings for JSON compatibility.

Issue: #207
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import psycopg
import psycopg.rows
from psycopg.types.json import Jsonb

from app.services.budget import FxRateUnavailable, compute_budget_state

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

    This is a current-state snapshot, not a period delta.  Realized P&L spans
    all positions (open and closed); unrealized P&L only applies to positions
    that still hold units.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT COALESCE(SUM(realized_pnl), 0) AS realized,
                   COALESCE(SUM(unrealized_pnl) FILTER (WHERE current_units > 0), 0)
                       AS unrealized
            FROM positions
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
    bottom = [_row_to_dict(r) for r in rows[total - n :]] if total > n else []
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
    """Current budget state via compute_budget_state.

    Reporting paths must NOT hard-fail on a missing GBP→USD rate
    (#502 PR C, Codex round 2 finding 2). Reports are read-only
    snapshots — surfacing "FX unavailable" in-line is the right
    degrade for a weekly/monthly report, vs the execution-guard
    fail-closed posture which actually blocks orders.
    """
    try:
        budget = compute_budget_state(conn)
    except FxRateUnavailable:
        logger.warning("_budget_snapshot: GBP→USD rate unavailable; emitting null tax/budget figures")
        return {
            "cash_balance": None,
            "deployed_capital": None,
            "estimated_tax_usd": None,
            "available_for_deployment": None,
            "fx_unavailable": True,
        }
    return {
        "cash_balance": _dec(budget.cash_balance),
        "deployed_capital": _dec(budget.deployed_capital),
        "estimated_tax_usd": _dec(budget.estimated_tax_usd),
        "available_for_deployment": _dec(budget.available_for_deployment),
    }


# ---------------------------------------------------------------------------
# Monthly-only section functions
# ---------------------------------------------------------------------------


def _position_pnl_breakdown(
    conn: psycopg.Connection[Any],
    period_start: date,
    period_end: date,
) -> list[dict[str, Any]]:
    """Per-position P&L for positions that had fill activity in the period."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT p.instrument_id,
                   i.symbol,
                   i.company_name,
                   p.cost_basis,
                   p.realized_pnl,
                   p.unrealized_pnl,
                   p.current_units,
                   p.avg_cost
            FROM positions p
            JOIN instruments i USING (instrument_id)
            WHERE p.instrument_id IN (
                SELECT DISTINCT o.instrument_id
                FROM fills f
                JOIN orders o USING (order_id)
                WHERE f.filled_at >= %(start)s
                  AND f.filled_at < %(end)s::date + 1
            )
            ORDER BY p.realized_pnl + p.unrealized_pnl DESC
            """,
            {"start": period_start, "end": period_end},
        )
        rows = cur.fetchall()
    return [
        {
            "instrument_id": r["instrument_id"],
            "symbol": r["symbol"],
            "company_name": r["company_name"],
            "cost_basis": _dec(r["cost_basis"]),
            "realized_pnl": _dec(r["realized_pnl"]),
            "unrealized_pnl": _dec(r["unrealized_pnl"]),
            "current_units": _dec(r["current_units"]),
        }
        for r in rows
    ]


def _win_rate_and_holding(
    conn: psycopg.Connection[Any],
    period_start: date,
    period_end: date,
) -> dict[str, Any]:
    """Win rate and average holding period for positions closed in the period."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT ra.gross_return_pct, ra.hold_days
            FROM return_attribution ra
            WHERE ra.hold_end >= %(start)s
              AND ra.hold_end <= %(end)s
            """,
            {"start": period_start, "end": period_end},
        )
        rows = cur.fetchall()

    total = len(rows)
    if total == 0:
        return {
            "total_closed": 0,
            "winners": 0,
            "losers": 0,
            "win_rate_pct": None,
            "avg_holding_days": None,
        }

    winners = sum(1 for r in rows if r["gross_return_pct"] is not None and r["gross_return_pct"] > 0)
    losers = total - winners
    win_rate = f"{100 * winners / total:.2f}"
    hold_days_vals = [float(r["hold_days"]) for r in rows if r["hold_days"] is not None]
    avg_holding = sum(hold_days_vals) / len(hold_days_vals) if hold_days_vals else None
    return {
        "total_closed": total,
        "winners": winners,
        "losers": losers,
        "win_rate_pct": win_rate,
        "avg_holding_days": avg_holding,
    }


def _best_worst_trade(
    conn: psycopg.Connection[Any],
    period_start: date,
    period_end: date,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Best and worst attributed trade closed in the period."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT ra.instrument_id,
                   i.symbol,
                   ra.gross_return_pct,
                   ra.hold_days,
                   ra.model_alpha_pct
            FROM return_attribution ra
            JOIN instruments i USING (instrument_id)
            WHERE ra.hold_end >= %(start)s
              AND ra.hold_end <= %(end)s
            ORDER BY ra.gross_return_pct DESC
            """,
            {"start": period_start, "end": period_end},
        )
        rows = cur.fetchall()

    if not rows:
        return None, None

    def _to_dict(r: dict[str, Any]) -> dict[str, Any]:
        return {
            "instrument_id": r["instrument_id"],
            "symbol": r["symbol"],
            "gross_return_pct": _dec(r["gross_return_pct"]),
            "hold_days": r["hold_days"],
            "model_alpha_pct": _dec(r["model_alpha_pct"]),
        }

    best = _to_dict(rows[0])
    worst = _to_dict(rows[-1]) if len(rows) > 1 else _to_dict(rows[0])
    return best, worst


def _period_attribution(
    conn: psycopg.Connection[Any],
    period_start: date,
    period_end: date,
) -> dict[str, Any]:
    """Period-bounded attribution aggregated directly from return_attribution."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS positions_attributed,
                   AVG(gross_return_pct)   AS avg_gross,
                   AVG(market_return_pct)  AS avg_market,
                   AVG(model_alpha_pct)    AS avg_alpha
            FROM return_attribution
            WHERE hold_end >= %(start)s
              AND hold_end <= %(end)s
            """,
            {"start": period_start, "end": period_end},
        )
        row = cur.fetchone()

    if row is None:
        return {
            "positions_attributed": 0,
            "avg_gross_return_pct": None,
            "avg_market_return_pct": None,
            "avg_model_alpha_pct": None,
        }
    return {
        "positions_attributed": row["positions_attributed"],
        "avg_gross_return_pct": _dec(row["avg_gross"]),
        "avg_market_return_pct": _dec(row["avg_market"]),
        "avg_model_alpha_pct": _dec(row["avg_alpha"]),
    }


def _thesis_accuracy(
    conn: psycopg.Connection[Any],
    period_start: date,
    period_end: date,
) -> list[dict[str, Any]]:
    """Thesis accuracy for closed positions, using thesis active at position open.

    For each closed position, determines whether the exit price hit the bull,
    base, or bear target from the thesis that was active when the position
    was opened (nearest thesis by created_at before hold_start).
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT ra.instrument_id,
                   i.symbol,
                   ra.gross_return_pct,
                   t.base_value,
                   t.bull_value,
                   t.bear_value,
                   t.stance,
                   t.confidence_score,
                   f_exit.price AS exit_price
            FROM return_attribution ra
            JOIN instruments i USING (instrument_id)
            LEFT JOIN LATERAL (
                SELECT base_value, bull_value, bear_value, stance, confidence_score
                FROM theses
                WHERE instrument_id = ra.instrument_id
                  AND created_at < (ra.hold_start::timestamptz + interval '1 day')
                ORDER BY created_at DESC
                LIMIT 1
            ) t ON true
            LEFT JOIN fills f_exit ON f_exit.fill_id = ra.exit_fill_id
            WHERE ra.hold_end >= %(start)s
              AND ra.hold_end <= %(end)s
            """,
            {"start": period_start, "end": period_end},
        )
        rows = cur.fetchall()

    results: list[dict[str, Any]] = []
    for r in rows:
        exit_price = r["exit_price"]
        bull_value = r["bull_value"]
        base_value = r["base_value"]
        bear_value = r["bear_value"]

        target_hit: str | None
        if exit_price is None or bull_value is None or base_value is None or bear_value is None:
            target_hit = None
        elif exit_price >= bull_value:
            target_hit = "bull"
        elif exit_price >= base_value:
            target_hit = "base"
        elif exit_price <= bear_value:
            target_hit = "bear"
        else:
            target_hit = "between_bear_and_base"

        results.append(
            {
                "instrument_id": r["instrument_id"],
                "symbol": r["symbol"],
                "gross_return_pct": _dec(r["gross_return_pct"]),
                "stance": r["stance"],
                "confidence_score": _dec(r["confidence_score"]),
                "exit_price": _dec(exit_price),
                "base_value": _dec(base_value),
                "bull_value": _dec(bull_value),
                "bear_value": _dec(bear_value),
                "target_hit": target_hit,
            }
        )
    return results


def _tax_provision_snapshot(conn: psycopg.Connection[Any]) -> dict[str, Any]:
    """Current tax provision from the budget service. Degrades to a
    null snapshot when FX is unavailable rather than hard-failing
    the monthly report (#502 PR C)."""
    try:
        budget = compute_budget_state(conn)
    except FxRateUnavailable:
        logger.warning("_tax_provision_snapshot: GBP→USD rate unavailable; emitting null")
        return {
            "estimated_tax_gbp": None,
            "estimated_tax_usd": None,
            "tax_year": None,
            "fx_unavailable": True,
        }
    return {
        "estimated_tax_gbp": _dec(budget.estimated_tax_gbp),
        "estimated_tax_usd": _dec(budget.estimated_tax_usd),
        "tax_year": budget.tax_year,
    }


# ---------------------------------------------------------------------------
# Persistence layer
# ---------------------------------------------------------------------------


def _positions_snapshot(conn: psycopg.Connection[Any]) -> list[dict[str, Any]]:
    """Per-instrument open-position P&L at snapshot time.

    Stored on each `report_snapshots.snapshot_json["positions"]` so the
    next snapshot can diff against it to surface period contributors
    (Slice 4 of the per-stock research page spec).
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT p.instrument_id,
                   i.symbol,
                   i.company_name,
                   p.unrealized_pnl,
                   p.cost_basis
            FROM positions p
            JOIN instruments i USING (instrument_id)
            WHERE p.current_units > 0
            ORDER BY p.instrument_id
            """
        )
        rows = cur.fetchall()
    return [
        {
            "instrument_id": r["instrument_id"],
            "symbol": r["symbol"],
            "company_name": r["company_name"],
            "unrealized_pnl": _dec(r["unrealized_pnl"]),
            "cost_basis": _dec(r["cost_basis"]),
        }
        for r in rows
    ]


def _load_prior_snapshot(
    conn: psycopg.Connection[Any],
    *,
    report_type: str,
    period_start: date,
) -> dict[str, Any] | None:
    """Return the `snapshot_json` of the most recent snapshot of the
    given `report_type` whose `period_start` is strictly BEFORE the
    supplied `period_start`, or None if there isn't one.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT snapshot_json
            FROM report_snapshots
            WHERE report_type = %(report_type)s
              AND period_start < %(period_start)s
            ORDER BY period_start DESC
            LIMIT 1
            """,
            {"report_type": report_type, "period_start": period_start},
        )
        row = cur.fetchone()
    if row is None:
        return None
    # `.get` rather than `[]` so a row shape missing the column (only
    # possible under test mocks) degrades to "no prior snapshot"
    # rather than raising KeyError.
    snapshot_json = row.get("snapshot_json")
    # psycopg3 with the default JSONB adapter returns a dict. Some
    # driver configurations or downstream adapters return the raw
    # JSON string — decode it so a valid prior snapshot isn't
    # silently dropped (Codex slice-4 round-2 note).
    if isinstance(snapshot_json, str):
        try:
            snapshot_json = json.loads(snapshot_json)
        except json.JSONDecodeError:
            return None
    return snapshot_json if isinstance(snapshot_json, dict) else None


def _compute_contributors(
    current: list[dict[str, Any]],
    prior: list[dict[str, Any]] | None,
    *,
    top_n: int = 5,
) -> dict[str, list[dict[str, Any]]]:
    """Diff per-instrument unrealised P&L between two position snapshots.

    Returns `{contributors: [...top_n gainers...], drags: [...top_n losers...]}`
    where each row is `{instrument_id, symbol, pnl_delta, pnl_pct}`.

    - `pnl_delta` = current unrealized_pnl - prior unrealized_pnl (Decimal → str).
    - `pnl_pct`   = pnl_delta / prior_cost_basis (None if no prior row or
      prior cost_basis is zero — avoids div/0 and the misleading "∞%"
      for a brand-new position).
    - New positions (in `current`, absent from `prior`) surface with
      their full unrealized_pnl as the delta and `pnl_pct = null`.
    - Closed positions (in `prior`, absent from `current`) are NOT
      reported here — their contribution lives in the realised-P&L
      aggregate, which the existing `pnl` section already covers.
    - When `prior is None` (first snapshot, or backfilled historical
      snapshots with no `positions` key), both lists are empty so the
      UI gracefully degrades.
    """
    if prior is None:
        return {"contributors": [], "drags": []}

    prior_by_id = {p["instrument_id"]: p for p in prior}
    # Keep the raw Decimal delta alongside the serialised string so
    # sort + filter never round-trip through `Decimal(str)` re-parsing
    # (Codex slice-4 round-2 note).
    entries: list[tuple[Decimal, dict[str, Any]]] = []
    for curr in current:
        iid = curr["instrument_id"]
        prior_row = prior_by_id.get(iid)
        curr_pnl = Decimal(curr["unrealized_pnl"] or "0")
        prior_pnl = Decimal(prior_row["unrealized_pnl"] or "0") if prior_row is not None else Decimal("0")
        delta = curr_pnl - prior_pnl
        if delta == 0:
            continue
        prior_cost = Decimal(prior_row["cost_basis"] or "0") if prior_row is not None else Decimal("0")
        pnl_pct: Decimal | None = delta / prior_cost if prior_cost > 0 else None
        entries.append(
            (
                delta,
                {
                    "instrument_id": iid,
                    "symbol": curr["symbol"],
                    "pnl_delta": _dec(delta),
                    "pnl_pct": _dec(pnl_pct),
                },
            )
        )

    # Contributors: positive deltas, descending (biggest gainer first).
    # Drags: negative deltas, ascending (most-negative first). Both
    # sort on the raw Decimal and slice from the head so the ordering
    # intent is unambiguous and doesn't depend on which end of the
    # combined list we slice from.
    positives = sorted((e for e in entries if e[0] > 0), key=lambda e: e[0], reverse=True)
    negatives = sorted((e for e in entries if e[0] < 0), key=lambda e: e[0])
    contributors = [row for _, row in positives[:top_n]]
    drags = [row for _, row in negatives[:top_n]]
    return {"contributors": contributors, "drags": drags}


def persist_report_snapshot(
    conn: psycopg.Connection[Any],
    *,
    report_type: str,
    period_start: date,
    period_end: date,
    snapshot: dict[str, Any],
) -> None:
    """Upsert a report snapshot into report_snapshots.

    Idempotent: ON CONFLICT replaces the snapshot for the same
    (report_type, period_start) pair. The caller owns the commit.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO report_snapshots (report_type, period_start, period_end, snapshot_json)
            VALUES (%(report_type)s, %(period_start)s, %(period_end)s, %(snapshot)s)
            ON CONFLICT (report_type, period_start) DO UPDATE
            SET period_end   = EXCLUDED.period_end,
                snapshot_json = EXCLUDED.snapshot_json,
                computed_at  = NOW()
            """,
            {
                "report_type": report_type,
                "period_start": period_start,
                "period_end": period_end,
                "snapshot": Jsonb(snapshot),
            },
        )


def load_report_snapshots(
    conn: psycopg.Connection[Any],
    *,
    report_type: str,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Load the most recent report snapshots of a given type."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT snapshot_id, report_type, period_start, period_end,
                   snapshot_json, computed_at
            FROM report_snapshots
            WHERE report_type = %(report_type)s
            ORDER BY period_start DESC
            LIMIT %(limit)s
            """,
            {"report_type": report_type, "limit": limit},
        )
        return cur.fetchall()


# ---------------------------------------------------------------------------
# Main entry points
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
    positions_now = _positions_snapshot(conn)
    prior = _load_prior_snapshot(conn, report_type="weekly", period_start=period_start)
    # When `prior` is absent OR lacks the `positions` key (pre-feature
    # snapshots from before Slice 4), pass `None` so
    # `_compute_contributors` degrades to empty lists. Passing `[]`
    # here would treat every current holding as a brand-new
    # contributor. Codex slice-4 finding.
    prior_positions = prior.get("positions") if isinstance(prior, dict) and "positions" in prior else None
    period_contribution = _compute_contributors(positions_now, prior_positions)

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
        # Per-instrument position snapshot so the *next* weekly
        # snapshot can compute period contribution against it.
        "positions": positions_now,
        # Contributors + drags computed against the prior snapshot.
        # Slice 4 of per-stock research page spec. Empty arrays when
        # there is no prior snapshot yet (fresh install or backfilled
        # historicals with no `positions` key).
        "period_contribution": period_contribution,
    }


def generate_monthly_report(
    conn: psycopg.Connection[Any],
    period_start: date,
    period_end: date,
) -> MonthlyReport:
    """Generate a monthly performance report snapshot.

    Reads from positions, fills, orders, instruments, return_attribution,
    theses, and the budget service.

    The caller owns the transaction; this function never calls conn.commit().

    Returns a plain dict suitable for storage in report_snapshots.snapshot_json.
    """
    pnl = _pnl_snapshot(conn)
    position_pnl = _position_pnl_breakdown(conn, period_start, period_end)
    win_rate_data = _win_rate_and_holding(conn, period_start, period_end)
    best_trade, worst_trade = _best_worst_trade(conn, period_start, period_end)
    attribution_summary = _period_attribution(conn, period_start, period_end)
    thesis_accuracy = _thesis_accuracy(conn, period_start, period_end)
    tax_provision = _tax_provision_snapshot(conn)
    positions_now = _positions_snapshot(conn)
    prior = _load_prior_snapshot(conn, report_type="monthly", period_start=period_start)
    # When `prior` is absent OR lacks the `positions` key (pre-feature
    # snapshots from before Slice 4), pass `None` so
    # `_compute_contributors` degrades to empty lists. Passing `[]`
    # here would treat every current holding as a brand-new
    # contributor. Codex slice-4 finding.
    prior_positions = prior.get("positions") if isinstance(prior, dict) and "positions" in prior else None
    period_contribution = _compute_contributors(positions_now, prior_positions)

    return {
        "report_type": "monthly",
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "pnl": pnl,
        "position_pnl": position_pnl,
        "win_rate": win_rate_data["win_rate_pct"],
        "avg_holding_days": win_rate_data["avg_holding_days"],
        "best_trade": best_trade,
        "worst_trade": worst_trade,
        "attribution_summary": attribution_summary,
        "thesis_accuracy": thesis_accuracy,
        "tax_provision": tax_provision,
        "positions": positions_now,
        "period_contribution": period_contribution,
    }
