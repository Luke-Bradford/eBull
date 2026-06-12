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
from app.services.fx import FxRateNotFound, convert
from app.services.valuation import PortfolioValuation, compute_portfolio_valuation

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Type aliases / constants
# ---------------------------------------------------------------------------

WeeklyReport = dict[str, Any]
MonthlyReport = dict[str, Any]

# Snapshot schema version (#1596, spec docs/proposals/ui/2026-06-12-report-ia.md).
# v1 snapshots have no `schema_version` key; the FE branches on it.
SCHEMA_VERSION = 2

# Benchmark resolved BY SYMBOL at generation time (spec §7 — no hardcoded
# instrument_id; dev-data availability is not a repo invariant). Builder
# constant in v1, not operator config.
BENCHMARK_SYMBOL = "SPX500"
BENCHMARK_LABEL = "S&P 500 (price index)"

# Volatility / drawdown need a minimum observation count or they print
# noise (spec §4.9): below this the risk section reports
# insufficient_history instead of figures.
_MIN_RISK_OBSERVATIONS = 6

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _dec(v: Decimal | None) -> str | None:
    """Decimal → str for JSON serialisation, preserving None."""
    return str(v) if v is not None else None


_MONEY_Q = Decimal("0.000001")


def _dec_f(v: float | None) -> str | None:
    """float → Decimal-string (6 dp, matching NUMERIC(18,6)), None-safe.

    Valuation figures arrive as floats (market-data pipeline); quantizing
    keeps float repr noise (0.30000000000000004) out of the snapshot.
    """
    if v is None:
        return None
    return str(Decimal(str(v)).quantize(_MONEY_Q))


def _parse_dec(v: Any) -> Decimal | None:
    """Lenient str/number → Decimal for reading values back out of a
    prior snapshot's JSON. None / unparseable → None."""
    if v is None:
        return None
    try:
        return Decimal(str(v))
    except ArithmeticError:
        return None


def _dec_q(v: Decimal | None) -> str | None:
    """Decimal → 6-dp string for v2 snapshot figures, None-safe.

    Division/sqrt produce 28-significant-digit Decimals and
    float-derived Decimals carry repr noise — one quantum at the
    serialisation boundary keeps the stored JSON canonical."""
    if v is None:
        return None
    return str(v.quantize(_MONEY_Q))


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
                       f.fees,
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
                "fees": _dec(r["fees"]),
                "filled_at": r["filled_at"].isoformat() if r["filled_at"] is not None else None,
            }
            for r in raw
        ]

    opened = _fetch("BUY")
    closed = _fetch("EXIT")
    return opened, closed


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
            "avg_win_pct": None,
            "avg_loss_pct": None,
            "payoff_ratio": None,
        }

    winners = sum(1 for r in rows if r["gross_return_pct"] is not None and r["gross_return_pct"] > 0)
    losers = total - winners
    win_rate = f"{100 * winners / total:.2f}"
    hold_days_vals = [float(r["hold_days"]) for r in rows if r["hold_days"] is not None]
    avg_holding = sum(hold_days_vals) / len(hold_days_vals) if hold_days_vals else None
    # Payoff ratio (avg win % / |avg loss %|) — win rate alone is the
    # classic misleading stat (90% win rate + one −50% loser = losing
    # book). Spec §4.9.
    returns: list[Decimal] = [r["gross_return_pct"] for r in rows if r["gross_return_pct"] is not None]
    win_returns = [r for r in returns if r > 0]
    loss_returns = [r for r in returns if r < 0]
    avg_win: Decimal | None = sum(win_returns, Decimal(0)) / len(win_returns) if win_returns else None
    avg_loss: Decimal | None = sum(loss_returns, Decimal(0)) / len(loss_returns) if loss_returns else None
    payoff = (avg_win / abs(avg_loss)) if avg_win is not None and avg_loss is not None and avg_loss != 0 else None
    return {
        "total_closed": total,
        "winners": winners,
        "losers": losers,
        "win_rate_pct": win_rate,
        "avg_holding_days": avg_holding,
        "avg_win_pct": _dec(avg_win),
        "avg_loss_pct": _dec(avg_loss),
        "payoff_ratio": _dec(payoff),
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
                   AVG(sector_return_pct)  AS avg_sector,
                   AVG(model_alpha_pct)    AS avg_alpha,
                   AVG(timing_alpha_pct)   AS avg_timing,
                   AVG(cost_drag_pct)      AS avg_cost_drag
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
            "avg_sector_return_pct": None,
            "avg_model_alpha_pct": None,
            "avg_timing_alpha_pct": None,
            "avg_cost_drag_pct": None,
            "weighting": "equal",
        }
    return {
        "positions_attributed": row["positions_attributed"],
        "avg_gross_return_pct": _dec(row["avg_gross"]),
        "avg_market_return_pct": _dec(row["avg_market"]),
        # Full decomposition (spec §4.10): gross = market + sector +
        # model alpha + timing alpha − cost drag. timing/cost-drag were
        # stored in return_attribution all along but silently dropped
        # here — cost drag is the figure that teaches why churn loses.
        "avg_sector_return_pct": _dec(row["avg_sector"]),
        "avg_model_alpha_pct": _dec(row["avg_alpha"]),
        "avg_timing_alpha_pct": _dec(row["avg_timing"]),
        "avg_cost_drag_pct": _dec(row["avg_cost_drag"]),
        # Equal-weighted across closed trades — a $50 close weighs the
        # same as a $5,000 one. Labelled so the FE can say it.
        "weighting": "equal",
    }


def _thesis_accuracy(
    conn: psycopg.Connection[Any],
    period_start: date,
    period_end: date,
) -> list[dict[str, Any]]:
    """Thesis accuracy for closed positions, using thesis active at position open.

    For each closed position, determines whether the exit price hit the bull,
    base, or bear target from the thesis that was active when the position
    was opened.

    Selection precedence for the entry timestamp (#244):

      1. If ``return_attribution.entry_fill_id`` is set, use
         ``fills.filled_at`` for that fill (timestamp-precise).
      2. Otherwise fall back to
         ``trade_recommendations.created_at`` via
         ``return_attribution.recommendation_id`` — the recommendation
         is created and approved before the order fills, so its
         timestamp is a strict upper bound on entry.
      3. If neither anchor is available, the row's thesis joins as
         NULL — the report renders ``target_hit = null`` rather than
         picking a hindsight thesis.

    Crucially, the previous query used
    ``created_at < (ra.hold_start::timestamptz + interval '1 day')``
    where ``hold_start`` is a DATE. That admitted any thesis written on
    the same calendar day, including ones generated AFTER the entry
    fill — turning the report into a hindsight evaluation. The new
    bound is ``created_at <= entry_anchor`` against a real timestamp.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            WITH attribution AS (
                SELECT ra.instrument_id,
                       ra.gross_return_pct,
                       ra.exit_fill_id,
                       COALESCE(f_entry.filled_at, tr.created_at) AS entry_anchor
                FROM return_attribution ra
                LEFT JOIN fills f_entry
                       ON f_entry.fill_id = ra.entry_fill_id
                LEFT JOIN trade_recommendations tr
                       ON tr.recommendation_id = ra.recommendation_id
                WHERE ra.hold_end >= %(start)s
                  AND ra.hold_end <= %(end)s
            )
            SELECT a.instrument_id,
                   i.symbol,
                   a.gross_return_pct,
                   a.entry_anchor,
                   t.base_value,
                   t.bull_value,
                   t.bear_value,
                   t.stance,
                   t.confidence_score,
                   f_exit.price AS exit_price
            FROM attribution a
            JOIN instruments i USING (instrument_id)
            LEFT JOIN LATERAL (
                SELECT base_value, bull_value, bear_value, stance, confidence_score
                FROM theses
                WHERE instrument_id = a.instrument_id
                  AND a.entry_anchor IS NOT NULL
                  AND created_at <= a.entry_anchor
                ORDER BY created_at DESC
                LIMIT 1
            ) t ON true
            LEFT JOIN fills f_exit ON f_exit.fill_id = a.exit_fill_id
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
                   p.cost_basis,
                   p.realized_pnl,
                   p.current_units
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
            # realized_pnl + current_units added by #1596 so the NEXT
            # snapshot can fold realised deltas into period
            # contribution (spec §4.4). Additive — v1 readers ignore.
            "realized_pnl": _dec(r["realized_pnl"]),
            "current_units": _dec(r["current_units"]),
        }
        for r in rows
    ]


def _realized_by_instrument(conn: psycopg.Connection[Any]) -> dict[int, dict[str, Any]]:
    """Lifetime realised P&L per instrument across ALL positions rows,
    including fully-closed ones (current_units = 0). Used to fold
    realised deltas into period contribution — a position closed
    mid-period vanishes from the open-positions snapshot but its
    realised P&L still moved this period (#1596, spec §4.4)."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT p.instrument_id, i.symbol, p.realized_pnl
            FROM positions p
            JOIN instruments i USING (instrument_id)
            """
        )
        rows = cur.fetchall()
    return {
        r["instrument_id"]: {"symbol": r["symbol"], "realized_pnl": r["realized_pnl"] or Decimal("0")} for r in rows
    }


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
    realized_now: dict[int, dict[str, Any]] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Diff per-instrument period P&L between two position snapshots.

    Returns `{contributors: [...top_n gainers...], drags: [...top_n losers...]}`
    where each row is `{instrument_id, symbol, pnl_delta, pnl_pct}`.

    - `pnl_delta` = (current unrealized_pnl − prior unrealized_pnl)
      PLUS, when `realized_now` is supplied and the prior row carries a
      `realized_pnl` key (v2 snapshots, #1596 spec §4.4), the realised
      delta (current lifetime realised − prior lifetime realised). The
      pre-#1596 unrealised-only diff made a position closed mid-period
      vanish and a trim read as a phantom loss.
    - Closed positions (in `prior`, absent from `current`): with
      `realized_now`, the close itself contributes
      `(realised delta − prior unrealised)` — the open P&L converts to
      realised and the net move lands in the chart. Without
      `realized_now` (legacy callers/tests), closed positions are
      skipped as before.
    - `pnl_pct`   = pnl_delta / prior_cost_basis (None if no prior row or
      prior cost_basis is zero — avoids div/0 and the misleading "∞%"
      for a brand-new position).
    - New positions (in `current`, absent from `prior`) surface with
      their full unrealized_pnl as the delta and `pnl_pct = null`.
    - When `prior is None` (first snapshot, or backfilled historical
      snapshots with no `positions` key), both lists are empty so the
      UI gracefully degrades.
    - Known limit (Codex ckpt-2): a position opened AND fully closed
      within one period has no snapshot row on either side, so it
      cannot appear per-instrument here (its realised P&L still lands
      in the cover's aggregate `realized_delta`). Likewise a
      same-period open+trim shows its unrealised delta only. Exact
      per-instrument intra-period attribution needs the #1593 ledger.
    """
    if prior is None:
        return {"contributors": [], "drags": []}

    prior_by_id = {p["instrument_id"]: p for p in prior}

    def _realized_delta(iid: int, prior_row: dict[str, Any] | None) -> Decimal:
        """Realised P&L movement this period; 0 unless both sides know it."""
        if realized_now is None or prior_row is None:
            return Decimal("0")
        prior_realized = _parse_dec(prior_row.get("realized_pnl"))
        if prior_realized is None:  # v1 prior snapshot — no realised baseline
            return Decimal("0")
        now_row = realized_now.get(iid)
        if now_row is None:
            # positions rows persist after close (current_units = 0) and
            # are never deleted, so a prior-snapshot instrument always
            # has a realized_now entry; this guard covers test doubles /
            # hypothetical row deletion, where 0 (no realised movement)
            # is the conservative answer (PR #1597 review nitpick).
            return Decimal("0")
        return Decimal(now_row["realized_pnl"]) - prior_realized

    # Keep the raw Decimal delta alongside the serialised string so
    # sort + filter never round-trip through `Decimal(str)` re-parsing
    # (Codex slice-4 round-2 note).
    entries: list[tuple[Decimal, dict[str, Any]]] = []
    seen_current: set[int] = set()
    for curr in current:
        iid = curr["instrument_id"]
        seen_current.add(iid)
        prior_row = prior_by_id.get(iid)
        curr_pnl = Decimal(curr["unrealized_pnl"] or "0")
        prior_pnl = Decimal(prior_row["unrealized_pnl"] or "0") if prior_row is not None else Decimal("0")
        delta = curr_pnl - prior_pnl + _realized_delta(iid, prior_row)
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

    # Positions closed during the period: prior row exists, no current
    # row. Net contribution = realised movement − the unrealised P&L
    # that was on the books at period start (it converted to realised).
    if realized_now is not None:
        for iid, prior_row in prior_by_id.items():
            if iid in seen_current:
                continue
            if _parse_dec(prior_row.get("realized_pnl")) is None:
                continue  # v1 prior snapshot — no realised baseline
            prior_pnl = Decimal(prior_row["unrealized_pnl"] or "0")
            delta = _realized_delta(iid, prior_row) - prior_pnl
            if delta == 0:
                continue
            prior_cost = Decimal(prior_row["cost_basis"] or "0")
            closed_pct: Decimal | None = delta / prior_cost if prior_cost > 0 else None
            entries.append(
                (
                    delta,
                    {
                        "instrument_id": iid,
                        "symbol": prior_row["symbol"],
                        "pnl_delta": _dec(delta),
                        "pnl_pct": _dec(closed_pct),
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
# v2 sections (#1596 — spec docs/proposals/ui/2026-06-12-report-ia.md)
# ---------------------------------------------------------------------------


def _benchmark_closes(
    conn: psycopg.Connection[Any],
    period_start: date,
    period_end: date,
) -> dict[str, Any]:
    """Benchmark closes around the period, resolved BY SYMBOL.

    Baseline = latest close STRICTLY BEFORE period_start (the value the
    period grew from); end = latest close at-or-before period_end.
    Returns nulls when the benchmark instrument or its closes are
    missing (spec §7: dev-data availability is not a repo invariant) —
    the FE renders portfolio-only with a notice.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id FROM instruments
            WHERE UPPER(symbol) = %(sym)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"sym": BENCHMARK_SYMBOL},
        )
        inst = cur.fetchone()
    if inst is None:
        return {
            "symbol": BENCHMARK_SYMBOL,
            "label": BENCHMARK_LABEL,
            "close_start": None,
            "close_end": None,
            "return_pct": None,
        }

    def _close(on_or_before: date, *, strict_before: bool) -> Decimal | None:
        op = "<" if strict_before else "<="
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                f"""
                SELECT close FROM price_daily
                WHERE instrument_id = %(iid)s
                  AND close IS NOT NULL
                  AND price_date {op} %(day)s
                ORDER BY price_date DESC
                LIMIT 1
                """,  # noqa: S608 — `op` is a literal chosen above, not user input
                {"iid": inst["instrument_id"], "day": on_or_before},
            )
            row = cur.fetchone()
        return row["close"] if row else None

    close_start = _close(period_start, strict_before=True)
    close_end = _close(period_end, strict_before=False)
    return_pct: Decimal | None = None
    if close_start is not None and close_end is not None and close_start > 0:
        return_pct = close_end / close_start - 1
    return {
        "symbol": BENCHMARK_SYMBOL,
        "label": BENCHMARK_LABEL,
        "close_start": _dec(close_start),
        "close_end": _dec(close_end),
        "return_pct": _dec_q(return_pct),
    }


def _external_flows(
    conn: psycopg.Connection[Any],
    period_start: date,
    period_end: date,
    display_currency: str,
    rates: dict[tuple[str, str], Decimal],
) -> list[tuple[date, Decimal]]:
    """External capital flows in the period, signed, display currency.

    `capital_events.amount` is always positive; `event_type` carries
    direction (sql/027 CHECK). Only `injection` (+) / `withdrawal` (−)
    are EXTERNAL flows — `tax_provision` / `tax_release` are internal
    earmarks; the cash never leaves the account (spec §3.4).

    Timing caveat (Codex ckpt-2): flows count when RECORDED in
    `capital_events`; the cash itself reaches valuation via the
    broker-sync cash ledger. If a recording and its broker sync land
    on opposite sides of a period boundary, that period's return is
    under/overstated and the next one symmetrically corrects — the
    mismatch is visible as a non-zero bridge residual, never silent.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT event_time::date AS flow_date, event_type, amount, currency
            FROM capital_events
            WHERE event_type IN ('injection', 'withdrawal')
              AND event_time >= %(start)s
              AND event_time < %(end)s::date + 1
            ORDER BY event_time
            """,
            {"start": period_start, "end": period_end},
        )
        rows = cur.fetchall()

    flows: list[tuple[date, Decimal]] = []
    for r in rows:
        amount: Decimal = r["amount"]
        ccy = str(r["currency"] or "USD")
        if ccy != display_currency:
            try:
                amount = convert(amount, ccy, display_currency, rates)
            except FxRateNotFound:
                logger.warning(
                    "capital_events flow %s→%s rate missing; flow used in native currency",
                    ccy,
                    display_currency,
                )
        signed = amount if r["event_type"] == "injection" else -amount
        flows.append((r["flow_date"], signed))
    return flows


def _modified_dietz(
    v_start: Decimal | None,
    v_end: Decimal,
    flows: list[tuple[date, Decimal]],
    period_start: date,
    period_end: date,
) -> Decimal | None:
    """Flow-adjusted period return (spec §3.4).

    `(V_end − V_start − ΣF) / (V_start + Σ(F × w))` with F signed
    (injection +, withdrawal −) and w = fraction of the period
    remaining after the flow lands. Degenerates to the simple ratio in
    flow-free periods. None when there is no opening value or the
    denominator is non-positive (e.g. account funded entirely
    mid-period — a return number would be meaningless).
    """
    if v_start is None:
        return None
    period_len = (period_end - period_start).days + 1
    if period_len <= 0:
        return None
    flow_sum = sum((f for _, f in flows), Decimal(0))
    weighted = Decimal(0)
    for flow_date, f in flows:
        remaining = (period_end - flow_date).days + 1
        w = Decimal(max(0, min(remaining, period_len))) / Decimal(period_len)
        weighted += f * w
    denominator = v_start + weighted
    if denominator <= 0:
        return None
    return (v_end - v_start - flow_sum) / denominator


def _chain_link(returns: list[Decimal]) -> Decimal | None:
    """Geometric chain of period returns: Π(1+r) − 1. None on empty."""
    if not returns:
        return None
    acc = Decimal(1)
    for r in returns:
        acc *= Decimal(1) + r
    return acc - 1


def _prior_v2_chain(
    conn: psycopg.Connection[Any],
    *,
    report_type: str,
    period_start: date,
) -> list[dict[str, Any]]:
    """Ordered (oldest→newest) prior v2 snapshots' chained figures.

    One row per prior snapshot with `schema_version >= 2`:
    `{period_start, period_return (Decimal|None), display_currency}`.
    Feeds YTD / since-inception chaining and the risk section's
    return series. v1 snapshots carry no return — excluded.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT period_start,
                   snapshot_json -> 'cover' ->> 'period_return' AS period_return,
                   snapshot_json -> 'cover' ->> 'display_currency' AS display_currency
            FROM report_snapshots
            WHERE report_type = %(report_type)s
              AND period_start < %(period_start)s
              AND (snapshot_json ->> 'schema_version')::int >= 2
            ORDER BY period_start ASC
            """,
            {"report_type": report_type, "period_start": period_start},
        )
        rows = cur.fetchall()
    return [
        {
            "period_start": r["period_start"],
            "period_return": _parse_dec(r["period_return"]),
            "display_currency": r["display_currency"],
        }
        for r in rows
    ]


def _cover_and_performance(
    conn: psycopg.Connection[Any],
    *,
    valuation: PortfolioValuation,
    prior: dict[str, Any] | None,
    chain: list[dict[str, Any]],
    period_start: date,
    period_end: date,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Account-summary cover + the single-period performance point.

    Closing value comes from the SHARED valuation helper — the same
    code path as the dashboard headline (spec §3.3), so the two cannot
    drift. Opening value = prior v2 snapshot's closing value; None on
    the first v2 snapshot (or after a display-currency change, where
    mixing bases would be dishonest) → period_return null, FE labels
    "(since inception)".
    """
    closing = Decimal(str(valuation.total_aum))
    opening: Decimal | None = None
    prior_cover = prior.get("cover") if isinstance(prior, dict) else None
    if isinstance(prior_cover, dict):
        if prior_cover.get("display_currency") == valuation.display_currency:
            opening = _parse_dec(prior_cover.get("closing_value"))
        else:
            logger.warning(
                "prior snapshot display currency %s != current %s; period return suppressed",
                prior_cover.get("display_currency"),
                valuation.display_currency,
            )

    flows = _external_flows(conn, period_start, period_end, valuation.display_currency, valuation.rates)
    net_flows = sum((f for _, f in flows), Decimal(0))
    period_return = _modified_dietz(opening, closing, flows, period_start, period_end)
    benchmark = _benchmark_closes(conn, period_start, period_end)
    benchmark_return = _parse_dec(benchmark["return_pct"])
    excess = period_return - benchmark_return if period_return is not None and benchmark_return is not None else None

    # Lifetime P&L aggregates → period deltas vs the prior snapshot's
    # stored aggregates (`pnl` key — present on v1 priors too).
    pnl_now = _pnl_snapshot(conn)
    realized_now = _parse_dec(pnl_now["realized_pnl"]) or Decimal(0)
    unrealized_now = _parse_dec(pnl_now["unrealized_pnl"]) or Decimal(0)
    realized_delta: Decimal | None = None
    unrealized_delta: Decimal | None = None
    prior_pnl = prior.get("pnl") if isinstance(prior, dict) else None
    if isinstance(prior_pnl, dict):
        prior_realized = _parse_dec(prior_pnl.get("realized_pnl"))
        prior_unrealized = _parse_dec(prior_pnl.get("unrealized_pnl"))
        if prior_realized is not None:
            realized_delta = realized_now - prior_realized
        if prior_unrealized is not None:
            unrealized_delta = unrealized_now - prior_unrealized

    # Value bridge (spec §4.1): opening + external flows + realised +
    # change in unrealised + residual = closing. The residual absorbs
    # everything the ledger can't itemise yet — broker_sync deltas
    # (dividends, fees, sync corrections) and valuation-basis drift.
    # Honest line, labelled "Broker adjustments (unitemised)".
    residual: Decimal | None = None
    if opening is not None and realized_delta is not None and unrealized_delta is not None:
        residual = closing - opening - net_flows - realized_delta - unrealized_delta

    # YTD / since-inception: chain-link prior v2 period returns with
    # this period's. Only chains in the SAME display currency count.
    chained_rows = [
        c for c in chain if c["period_return"] is not None and c["display_currency"] == valuation.display_currency
    ]
    chain_returns = [c["period_return"] for c in chained_rows]
    current_returns = [period_return] if period_return is not None else []
    si_return = _chain_link(chain_returns + current_returns)
    ytd_chain = [c["period_return"] for c in chained_rows if c["period_start"].year == period_end.year]
    ytd_return = _chain_link(ytd_chain + current_returns)

    # Benchmark over the same spans, from closes (cheap, [NOW]). The SI
    # span starts at the first chain row that SURVIVES the currency
    # filter — starting at chain[0] regardless would compare a
    # later-inception portfolio chain against an older benchmark span
    # (Codex ckpt-2).
    si_start = chained_rows[0]["period_start"] if chained_rows else period_start
    benchmark_si = _benchmark_closes(conn, si_start, period_end)
    ytd_start = date(period_end.year, 1, 1)
    benchmark_ytd = _benchmark_closes(conn, ytd_start, period_end)

    cover = {
        "display_currency": valuation.display_currency,
        "closing_value": _dec_q(closing),
        "opening_value": _dec_q(opening),
        "cash": _dec_f(valuation.cash_balance),
        "mirror_equity": _dec_f(valuation.mirror_equity),
        "period_return": _dec_q(period_return),
        "benchmark_return": benchmark["return_pct"],
        "excess_return": _dec_q(excess),
        "ytd_return": _dec_q(ytd_return),
        "si_return": _dec_q(si_return),
        "benchmark_ytd_return": benchmark_ytd["return_pct"],
        "benchmark_si_return": benchmark_si["return_pct"],
        "realized_delta": _dec_q(realized_delta),
        "unrealized_delta": _dec_q(unrealized_delta),
        "bridge": {
            "opening_value": _dec_q(opening),
            "net_external_flows": _dec_q(net_flows),
            "realized_delta": _dec_q(realized_delta),
            "unrealized_delta": _dec_q(unrealized_delta),
            "broker_adjustments_residual": _dec_q(residual),
            "closing_value": _dec_q(closing),
        },
        "return_method": "modified_dietz_v1",
    }
    performance = {
        "portfolio_value": _dec_q(closing),
        "period_return": _dec_q(period_return),
        "benchmark": benchmark,
        "fx_mode": "generation_date",
        "method": "modified_dietz_v1",
        "observations": len(chain_returns) + len(current_returns),
    }
    return cover, performance


def _holdings_section(
    valuation: PortfolioValuation,
    prior_positions: list[dict[str, Any]] | None,
    realized_now: dict[int, dict[str, Any]],
    opening_value: Decimal | None,
) -> list[dict[str, Any]]:
    """Holdings-at-generation table rows (spec §4.5).

    Weight is vs total_aum (the whole account, cash + mirrors
    included). Period contribution folds realised + unrealised deltas
    vs the prior snapshot's per-instrument rows; bps vs opening value.
    """
    prior_by_id = {p["instrument_id"]: p for p in prior_positions or []}
    total_aum = Decimal(str(valuation.total_aum))
    rows: list[dict[str, Any]] = []
    for h in valuation.holdings:
        mv = Decimal(str(h.market_value))
        cost = Decimal(str(h.cost_basis))
        weight = mv / total_aum if total_aum > 0 else None
        since_entry = (mv - cost) / cost if cost > 0 else None

        contribution: Decimal | None = None
        contribution_bps: Decimal | None = None
        prior_row = prior_by_id.get(h.instrument_id)
        if prior_row is not None:
            prior_unreal = _parse_dec(prior_row.get("unrealized_pnl")) or Decimal(0)
            delta = Decimal(str(h.unrealized_pnl)) - prior_unreal
            prior_real = _parse_dec(prior_row.get("realized_pnl"))
            now_real = realized_now.get(h.instrument_id)
            if prior_real is not None and now_real is not None:
                delta += Decimal(now_real["realized_pnl"]) - prior_real
            contribution = delta
            if opening_value is not None and opening_value > 0:
                contribution_bps = delta / opening_value * 10000

        rows.append(
            {
                "instrument_id": h.instrument_id,
                "symbol": h.symbol,
                "company_name": h.company_name,
                # Resolved name, not the raw numeric industry id (#1598).
                # None when the instrument has no sector or the id is
                # unmapped — the FE renders its nil treatment.
                "sector": h.sector_name,
                "units": _dec_f(h.current_units),
                "price": _dec_f(h.current_price),
                "market_value": _dec_f(h.market_value),
                "cost_basis": _dec_f(h.cost_basis),
                "weight_pct": _dec_q(weight),
                "since_entry_return_pct": _dec_q(since_entry),
                "unrealized_pnl": _dec_f(h.unrealized_pnl),
                "period_contribution": _dec_q(contribution),
                "period_contribution_bps": _dec_q(contribution_bps),
                "valuation_source": h.valuation_source,
            }
        )
    # `market_value` is `_dec_f` over a non-optional float — never None.
    rows.sort(key=lambda r: Decimal(r["market_value"]), reverse=True)
    return rows


def _income_section(
    conn: psycopg.Connection[Any],
    period_start: date,
    period_end: date,
) -> dict[str, Any]:
    """Estimated dividend income: declarations with ex-date in the
    period × CURRENT units (period-end proxy — spec §4.7 names the
    approximation; exact units-at-ex-date needs the #1593 ledger).
    Estimates only — not confirmed received."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT de.instrument_id,
                   i.symbol,
                   de.ex_date,
                   de.pay_date,
                   de.dps_declared,
                   de.currency,
                   p.current_units
            FROM dividend_events de
            JOIN instruments i USING (instrument_id)
            JOIN positions p USING (instrument_id)
            WHERE p.current_units > 0
              AND de.ex_date >= %(start)s
              AND de.ex_date <= %(end)s
              AND de.dps_declared IS NOT NULL
            ORDER BY de.ex_date, i.symbol
            """,
            {"start": period_start, "end": period_end},
        )
        rows = cur.fetchall()

    items: list[dict[str, Any]] = []
    totals_by_ccy: dict[str, Decimal] = {}
    for r in rows:
        amount = r["dps_declared"] * r["current_units"]
        ccy = str(r["currency"] or "USD")
        totals_by_ccy[ccy] = totals_by_ccy.get(ccy, Decimal(0)) + amount
        items.append(
            {
                "instrument_id": r["instrument_id"],
                "symbol": r["symbol"],
                "ex_date": r["ex_date"].isoformat() if r["ex_date"] is not None else None,
                "pay_date": r["pay_date"].isoformat() if r["pay_date"] is not None else None,
                "dps_declared": _dec(r["dps_declared"]),
                "currency": ccy,
                "units": _dec(r["current_units"]),
                "estimated_amount": _dec(amount),
            }
        )
    return {
        "items": items,
        "estimated_totals": {ccy: _dec(total) for ccy, total in sorted(totals_by_ccy.items())},
        "basis": "declared_dps_x_current_units",
    }


def _costs_section(
    conn: psycopg.Connection[Any],
    period_start: date,
    period_end: date,
) -> dict[str, Any]:
    """Own-platform fees in the period. Broker-side fees are invisible
    until #1593 (they arrive folded into broker_sync cash deltas)."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS fill_count,
                   COALESCE(SUM(f.fees), 0) AS fees_total
            FROM fills f
            WHERE f.filled_at >= %(start)s
              AND f.filled_at < %(end)s::date + 1
            """,
            {"start": period_start, "end": period_end},
        )
        row = cur.fetchone()
    fill_count = row["fill_count"] if row else 0
    fees_total: Decimal = row["fees_total"] if row else Decimal(0)
    return {
        "fees_total": _dec(fees_total),
        "fill_count": fill_count,
        "scope": "own_platform_orders_only",
    }


def _risk_section(
    valuation: PortfolioValuation,
    chain: list[dict[str, Any]],
    current_period_return: Decimal | None,
    *,
    observation_label: str,
) -> dict[str, Any]:
    """Concentration + sector exposure (point-in-time, always
    computable) and volatility / max drawdown over the FULL snapshot
    return history — never the single period (spec §4.9: n≤13 windows
    whipsaw). Below `_MIN_RISK_OBSERVATIONS` the series stats render
    as insufficient history. Explicitly NO Sharpe/Sortino/IR — not
    honestly computable before #1593 daily series."""
    total_aum = Decimal(str(valuation.total_aum))
    mvs = sorted((Decimal(str(h.market_value)) for h in valuation.holdings), reverse=True)
    top5 = sum(mvs[:5], Decimal(0))
    concentration_top5 = top5 / total_aum if total_aum > 0 else None

    sector_weights: dict[str, Decimal] = {}
    for h in valuation.holdings:
        # instruments.sector stores eToro's numeric industry id; the name
        # is resolved in the valuation query via etoro_stocks_industries
        # (#1598). An id with no catalogue row folds into "Unknown" — warn
        # so catalogue drift surfaces before it skews the exposure table.
        if h.sector is not None and h.sector_name is None:
            logger.warning(
                "sector id %s on %s has no etoro_stocks_industries row; grouped as Unknown",
                h.sector,
                h.symbol,
            )
        sector = h.sector_name or "Unknown"
        sector_weights[sector] = sector_weights.get(sector, Decimal(0)) + Decimal(str(h.market_value))
    sector_exposure = (
        {s: _dec_q(w / total_aum) for s, w in sorted(sector_weights.items(), key=lambda kv: kv[1], reverse=True)}
        if total_aum > 0
        else {}
    )

    returns = [
        c["period_return"]
        for c in chain
        if c["period_return"] is not None and c["display_currency"] == valuation.display_currency
    ]
    if current_period_return is not None:
        returns = [*returns, current_period_return]

    n = len(returns)
    volatility: Decimal | None = None
    max_drawdown: Decimal | None = None
    if n >= _MIN_RISK_OBSERVATIONS:
        mean = sum(returns, Decimal(0)) / n
        variance = sum(((r - mean) ** 2 for r in returns), Decimal(0)) / (n - 1)
        volatility = variance.sqrt()
        # Max drawdown on the chained return index.
        index = Decimal(1)
        peak = Decimal(1)
        worst = Decimal(0)
        for r in returns:
            index *= Decimal(1) + r
            peak = max(peak, index)
            drawdown = index / peak - 1
            worst = min(worst, drawdown)
        max_drawdown = worst

    return {
        "holding_count": len(valuation.holdings),
        "concentration_top5_pct": _dec_q(concentration_top5),
        "sector_exposure": sector_exposure,
        "volatility": _dec_q(volatility),
        "max_drawdown": _dec_q(max_drawdown),
        "observations": n,
        "observation_label": observation_label,
        "insufficient_history": n < _MIN_RISK_OBSERVATIONS,
    }


_ROLLING_WINDOWS: dict[str, int | None] = {"1m": 1, "3m": 3, "6m": 6, "1y": 12, "si": None}


def _add_month(d: date) -> date:
    """First-of-next-month for a first-of-month date."""
    return date(d.year + 1, 1, 1) if d.month == 12 else date(d.year, d.month + 1, 1)


def _is_contiguous_monthly(window: list[dict[str, Any]], current_period_start: date) -> bool:
    """True when the window's monthly periods run back-to-back and the
    newest one immediately precedes the current period. Count alone is
    not enough: with gaps (missed/backfilled months), N stale returns
    would masquerade as an N-month window (Codex ckpt-2)."""
    expected = current_period_start
    for row in reversed(window):
        prev = row["period_start"].replace(day=1)
        if _add_month(prev) != expected:
            return False
        expected = prev
    return True


def _rolling_returns(
    conn: psycopg.Connection[Any],
    chain: list[dict[str, Any]],
    current_period_return: Decimal | None,
    display_currency: str,
    period_start: date,
    period_end: date,
) -> dict[str, Any]:
    """Rolling-returns table (monthly statement, spec §4.3): portfolio
    chained over the last N monthly periods vs benchmark over the same
    span. Portfolio cells null until snapshot depth exists."""
    chained = [c for c in chain if c["period_return"] is not None and c["display_currency"] == display_currency]
    out: dict[str, Any] = {}
    for label, months in _ROLLING_WINDOWS.items():
        if months is None:
            window = chained
        else:
            window = chained[-(months - 1) :] if months > 1 else []
        window_returns = [c["period_return"] for c in window]
        if current_period_return is not None:
            window_returns = [*window_returns, current_period_return]
        # A window is only honest when it is FULL (or si): chaining 2
        # months and calling it "6m" overstates history. Fullness is
        # count AND calendar contiguity — with gaps, N stale returns
        # would masquerade as an N-month window (Codex ckpt-2).
        portfolio: Decimal | None
        if months is None:
            portfolio = _chain_link(window_returns)
            span_start = window[0]["period_start"] if window else period_start
        elif len(window_returns) == months and _is_contiguous_monthly(window, period_start):
            portfolio = _chain_link(window_returns)
            span_start = window[0]["period_start"] if window else period_start
        else:
            portfolio = None
            span_start = None
        benchmark: Decimal | None = None
        if span_start is not None:
            benchmark = _parse_dec(_benchmark_closes(conn, span_start, period_end)["return_pct"])
        excess = portfolio - benchmark if portfolio is not None and benchmark is not None else None
        out[label] = {
            "portfolio": _dec_q(portfolio),
            "benchmark": _dec_q(benchmark),
            "excess": _dec_q(excess),
        }
    return out


def _thesis_summary(thesis_rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate hit/miss/not-evaluable buckets over the per-trade
    thesis_accuracy rows (spec §4.10 — hit rates without a
    not-yet-evaluable bucket are unfalsifiable). Hit = exit at or
    above the base target."""
    evaluated = [r for r in thesis_rows if r.get("target_hit") is not None]
    not_evaluable = len(thesis_rows) - len(evaluated)
    hits = [r for r in evaluated if r["target_hit"] in ("bull", "base")]
    misses = [r for r in evaluated if r["target_hit"] not in ("bull", "base")]

    def _rate(stance: str) -> dict[str, Any]:
        stance_rows = [r for r in evaluated if r.get("stance") == stance]
        stance_hits = sum(1 for r in stance_rows if r["target_hit"] in ("bull", "base"))
        n = len(stance_rows)
        return {
            "n": n,
            "hits": stance_hits,
            "hit_rate_pct": f"{100 * stance_hits / n:.2f}" if n > 0 else None,
        }

    return {
        "total": len(thesis_rows),
        "evaluated": len(evaluated),
        "hits": len(hits),
        "misses": len(misses),
        "not_evaluable": not_evaluable,
        "buy": _rate("buy"),
        "avoid": _rate("avoid"),
    }


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
    scores, and the budget service.

    The caller owns the transaction; this function never calls conn.commit().

    Returns a plain dict suitable for storage in report_snapshots.snapshot_json.
    """
    pnl = _pnl_snapshot(conn)
    top_performers, bottom_performers = _top_bottom_performers(conn)
    positions_opened, positions_closed = _positions_opened_closed(conn, period_start, period_end)
    # #539: upcoming-earnings calendar sourced from FMP retired. Field
    # remains in the snapshot shape as an empty list so existing
    # readers (frontend, downstream report consumers) don't NPE.
    upcoming_earnings: list[dict[str, Any]] = []
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
    realized_now = _realized_by_instrument(conn)
    period_contribution = _compute_contributors(positions_now, prior_positions, realized_now=realized_now)

    # v2 sections (#1596) — shared valuation = same code path as the
    # dashboard headline (spec §3.3).
    valuation = compute_portfolio_valuation(conn)
    chain = _prior_v2_chain(conn, report_type="weekly", period_start=period_start)
    cover, performance = _cover_and_performance(
        conn,
        valuation=valuation,
        prior=prior,
        chain=chain,
        period_start=period_start,
        period_end=period_end,
    )
    holdings = _holdings_section(
        valuation,
        prior_positions,
        realized_now,
        _parse_dec(cover["opening_value"]),
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "weekly",
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "cover": cover,
        "performance": performance,
        "holdings": holdings,
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
    # Rank movers were weekly-only pre-#1596 (committee finding: the
    # monthly model-review section cited a key the monthly builder
    # never wrote).
    score_changes = _score_changes(conn, period_start, period_end)
    prior = _load_prior_snapshot(conn, report_type="monthly", period_start=period_start)
    # When `prior` is absent OR lacks the `positions` key (pre-feature
    # snapshots from before Slice 4), pass `None` so
    # `_compute_contributors` degrades to empty lists. Passing `[]`
    # here would treat every current holding as a brand-new
    # contributor. Codex slice-4 finding.
    prior_positions = prior.get("positions") if isinstance(prior, dict) and "positions" in prior else None
    realized_now = _realized_by_instrument(conn)
    period_contribution = _compute_contributors(positions_now, prior_positions, realized_now=realized_now)

    # v2 sections (#1596) — shared valuation = same code path as the
    # dashboard headline (spec §3.3).
    valuation = compute_portfolio_valuation(conn)
    chain = _prior_v2_chain(conn, report_type="monthly", period_start=period_start)
    cover, performance = _cover_and_performance(
        conn,
        valuation=valuation,
        prior=prior,
        chain=chain,
        period_start=period_start,
        period_end=period_end,
    )
    holdings = _holdings_section(
        valuation,
        prior_positions,
        realized_now,
        _parse_dec(cover["opening_value"]),
    )
    current_period_return = _parse_dec(cover["period_return"])
    risk = _risk_section(
        valuation,
        chain,
        current_period_return,
        observation_label="since inception, monthly observations",
    )
    rolling_returns = _rolling_returns(
        conn,
        chain,
        current_period_return,
        valuation.display_currency,
        period_start,
        period_end,
    )
    income = _income_section(conn, period_start, period_end)
    costs = _costs_section(conn, period_start, period_end)
    thesis_summary = _thesis_summary(thesis_accuracy)

    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "monthly",
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "cover": cover,
        "performance": performance,
        "holdings": holdings,
        "rolling_returns": rolling_returns,
        "income": income,
        "costs": costs,
        "risk": risk,
        "thesis_summary": thesis_summary,
        "score_changes": score_changes,
        "pnl": pnl,
        "position_pnl": position_pnl,
        "win_rate": win_rate_data["win_rate_pct"],
        "avg_holding_days": win_rate_data["avg_holding_days"],
        # Full closed-trade review incl. payoff ratio (spec §4.9) —
        # `win_rate`/`avg_holding_days` stay as legacy top-level keys.
        "trade_stats": win_rate_data,
        "best_trade": best_trade,
        "worst_trade": worst_trade,
        "attribution_summary": attribution_summary,
        "thesis_accuracy": thesis_accuracy,
        "tax_provision": tax_provision,
        "positions": positions_now,
        "period_contribution": period_contribution,
    }
