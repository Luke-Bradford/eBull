"""Transaction cost model — per-instrument cost estimation and reconciliation.

Computes spread, overnight carry, and FX conversion costs for a proposed trade.
The execution guard calls estimate_cost() to decide whether a trade's costs
are prohibitive relative to its expected return.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import psycopg
import psycopg.rows
import psycopg.types.json
from psycopg import sql

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class TransactionCostConfigCorrupt(RuntimeError):
    """Raised when the transaction_cost_config singleton row is missing.

    Callers on safety-critical paths must catch this and fail closed.
    """


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CostEstimate:
    """Estimated transaction cost for a proposed trade."""

    spread_bps: Decimal
    overnight_bps_per_day: Decimal
    fx_markup_bps: Decimal
    estimated_hold_days: int
    total_entry_cost_bps: Decimal  # spread + fx
    total_carry_cost_bps: Decimal  # overnight × hold_days
    total_cost_bps: Decimal  # entry + carry
    is_cost_prohibitive: bool
    prohibitive_reason: str | None


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


def get_transaction_cost_config(
    conn: psycopg.Connection[Any],
) -> dict[str, Any]:
    """Load the singleton transaction_cost_config row.

    Raises TransactionCostConfigCorrupt if the row is missing.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT max_total_cost_bps,
                   min_return_vs_cost_ratio,
                   default_hold_days,
                   updated_at,
                   updated_by,
                   reason
            FROM transaction_cost_config
            WHERE id = TRUE
            """
        )
        row = cur.fetchone()
    if row is None:
        raise TransactionCostConfigCorrupt("transaction_cost_config singleton row missing")
    return dict(row)


def update_transaction_cost_config(
    conn: psycopg.Connection[Any],
    *,
    max_total_cost_bps: Decimal | None = None,
    min_return_vs_cost_ratio: Decimal | None = None,
    default_hold_days: int | None = None,
    updated_by: str,
    reason: str,
) -> dict[str, Any]:
    """Update the transaction_cost_config singleton.

    Only provided (non-None) fields are changed.  Always updates
    updated_at, updated_by, and reason for audit.

    Raises TransactionCostConfigCorrupt if the row is missing.
    """
    # Build dynamic SET clause using psycopg.sql for type-safe composition.
    # Column names come from a fixed set in code (never user input).
    set_parts: list[sql.Composable] = [
        sql.SQL("updated_at = NOW()"),
        sql.SQL("updated_by = {by}").format(by=sql.Placeholder("updated_by")),
        sql.SQL("reason = {reason}").format(reason=sql.Placeholder("reason")),
    ]
    params: dict[str, Any] = {"updated_by": updated_by, "reason": reason}

    if max_total_cost_bps is not None:
        set_parts.append(sql.SQL("max_total_cost_bps = {v}").format(v=sql.Placeholder("max_total_cost_bps")))
        params["max_total_cost_bps"] = max_total_cost_bps
    if min_return_vs_cost_ratio is not None:
        set_parts.append(
            sql.SQL("min_return_vs_cost_ratio = {v}").format(v=sql.Placeholder("min_return_vs_cost_ratio"))
        )
        params["min_return_vs_cost_ratio"] = min_return_vs_cost_ratio
    if default_hold_days is not None:
        set_parts.append(sql.SQL("default_hold_days = {v}").format(v=sql.Placeholder("default_hold_days")))
        params["default_hold_days"] = default_hold_days

    query = sql.SQL(
        "UPDATE transaction_cost_config SET {sets} WHERE id = TRUE"
        " RETURNING max_total_cost_bps, min_return_vs_cost_ratio,"
        " default_hold_days, updated_at, updated_by, reason"
    ).format(sets=sql.SQL(", ").join(set_parts))

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(query, params)
        row = cur.fetchone()
    if row is None:
        raise TransactionCostConfigCorrupt("transaction_cost_config singleton row missing")
    return dict(row)


# ---------------------------------------------------------------------------
# Cost estimation
# ---------------------------------------------------------------------------


def estimate_cost(
    *,
    spread_bps: Decimal,
    overnight_rate: Decimal,
    fx_markup_bps: Decimal,
    hold_days: int,
    max_total_cost_bps: Decimal,
    min_return_vs_cost_ratio: Decimal,
    expected_return_pct: Decimal | None,
) -> CostEstimate:
    """Compute estimated transaction cost for a proposed trade.

    Parameters
    ----------
    spread_bps : entry spread in basis points (from quotes or cost_model)
    overnight_rate : daily carry cost in bps (0 for real-stock positions)
    fx_markup_bps : one-way FX conversion markup in bps (0 for USD instruments)
    hold_days : estimated holding period in days
    max_total_cost_bps : absolute threshold — above this, trade is prohibitive
    min_return_vs_cost_ratio : expected_return / total_cost must exceed this
    expected_return_pct : thesis-derived expected return (None = skip ratio check)
    """
    # FX applies on both entry and exit (round-trip)
    total_entry_cost_bps = spread_bps + fx_markup_bps * 2
    total_carry_cost_bps = overnight_rate * hold_days
    total_cost_bps = total_entry_cost_bps + total_carry_cost_bps

    is_prohibitive = False
    reason: str | None = None

    if total_cost_bps > max_total_cost_bps:
        is_prohibitive = True
        reason = f"total cost {total_cost_bps} bps exceeds threshold {max_total_cost_bps} bps"
    elif expected_return_pct is not None and total_cost_bps > 0:
        # expected_return_pct is in percent (e.g. 10.0 = 10%)
        # total_cost_bps is in basis points (e.g. 80 = 0.8%)
        # Convert cost to percent for comparison
        cost_pct = total_cost_bps / Decimal("100")
        ratio = expected_return_pct / cost_pct
        if ratio < min_return_vs_cost_ratio:
            is_prohibitive = True
            reason = (
                f"return/cost ratio {ratio:.2f} below minimum "
                f"{min_return_vs_cost_ratio} "
                f"(return={expected_return_pct}%, cost={cost_pct:.2f}%)"
            )

    return CostEstimate(
        spread_bps=spread_bps,
        overnight_bps_per_day=overnight_rate,
        fx_markup_bps=fx_markup_bps,
        estimated_hold_days=hold_days,
        total_entry_cost_bps=total_entry_cost_bps,
        total_carry_cost_bps=total_carry_cost_bps,
        total_cost_bps=total_cost_bps,
        is_cost_prohibitive=is_prohibitive,
        prohibitive_reason=reason,
    )


# ---------------------------------------------------------------------------
# Instrument cost lookup
# ---------------------------------------------------------------------------


def load_instrument_cost(
    conn: psycopg.Connection[Any],
    instrument_id: int,
) -> dict[str, Any] | None:
    """Load the active fee schedule for an instrument.

    Returns the most recent cost_model row where valid_to IS NULL.
    Returns None if no cost_model row exists (caller should fall back
    to computing spread from live quote data).
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT spread_bps, overnight_rate, fx_pair, fx_markup_bps
            FROM cost_model
            WHERE instrument_id = %(iid)s
              AND valid_to IS NULL
            ORDER BY valid_from DESC
            LIMIT 1
            """,
            {"iid": instrument_id},
        )
        row = cur.fetchone()
    return dict(row) if row is not None else None


def spread_pct_to_bps(spread_pct: Decimal | None) -> Decimal | None:
    """Convert spread_pct (percent, e.g. 0.45%) to basis points (45 bps).

    quotes.spread_pct stores (ask - bid) / mid * 100, i.e. already in percent.
    Basis points = percent * 100.
    """
    if spread_pct is None:
        return None
    return spread_pct * 100


# ---------------------------------------------------------------------------
# Cost record persistence
# ---------------------------------------------------------------------------


def record_estimated_cost(
    conn: psycopg.Connection[Any],
    *,
    order_id: int,
    recommendation_id: int,
    instrument_id: int,
    estimate: CostEstimate,
) -> None:
    """Persist the estimated cost breakdown for a trade at order time."""
    breakdown = {
        "spread_bps": str(estimate.spread_bps),
        "overnight_bps_per_day": str(estimate.overnight_bps_per_day),
        "fx_markup_bps": str(estimate.fx_markup_bps),
        "estimated_hold_days": estimate.estimated_hold_days,
        "total_entry_cost_bps": str(estimate.total_entry_cost_bps),
        "total_carry_cost_bps": str(estimate.total_carry_cost_bps),
        "total_cost_bps": str(estimate.total_cost_bps),
        "is_cost_prohibitive": estimate.is_cost_prohibitive,
        "prohibitive_reason": estimate.prohibitive_reason,
    }
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO trade_cost_record (
                order_id, recommendation_id, instrument_id,
                estimated_spread_bps, estimated_carry_bps,
                estimated_fx_bps, estimated_total_bps,
                cost_breakdown
            ) VALUES (
                %(order_id)s, %(recommendation_id)s, %(instrument_id)s,
                %(estimated_spread_bps)s, %(estimated_carry_bps)s,
                %(estimated_fx_bps)s, %(estimated_total_bps)s,
                %(cost_breakdown)s
            )
            """,
            {
                "order_id": order_id,
                "recommendation_id": recommendation_id,
                "instrument_id": instrument_id,
                "estimated_spread_bps": estimate.spread_bps,
                "estimated_carry_bps": estimate.total_carry_cost_bps,
                "estimated_fx_bps": estimate.fx_markup_bps,
                "estimated_total_bps": estimate.total_cost_bps,
                "cost_breakdown": psycopg.types.json.Jsonb(breakdown),
            },
        )


def seed_cost_models_from_quotes(
    conn: psycopg.Connection[Any],
) -> dict[str, int]:
    """Create or refresh cost_model rows from current quote spread data.

    For each Tier 1 instrument with a valid spread_pct:
    - Close any existing active cost_model row (set valid_to = NOW())
    - Insert a new active row with computed spread

    FX markup is set based on instrument currency:
    - USD → 0 bps
    - non-USD → 50 bps (eToro's typical FX markup)

    Overnight rate is 0 for all instruments (real stocks, not CFDs in v1).
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT i.instrument_id, q.spread_pct, i.currency
            FROM instruments i
            JOIN coverage c USING (instrument_id)
            JOIN quotes q USING (instrument_id)
            WHERE c.coverage_tier = 1
              AND q.spread_pct IS NOT NULL
            """
        )
        rows = cur.fetchall()

    processed = 0
    skipped = 0
    for row in rows:
        if row["spread_pct"] is None:
            skipped += 1
            continue

        spread_bps = spread_pct_to_bps(row["spread_pct"])
        currency = row["currency"] or "USD"
        fx_markup_bps = Decimal("0") if currency == "USD" else Decimal("50")

        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE cost_model
                SET valid_to = NOW()
                WHERE instrument_id = %(iid)s
                  AND valid_to IS NULL
                """,
                {"iid": row["instrument_id"]},
            )
            cur.execute(
                """
                INSERT INTO cost_model (
                    instrument_id, spread_bps, overnight_rate,
                    fx_pair, fx_markup_bps, source
                ) VALUES (
                    %(iid)s, %(spread_bps)s, 0,
                    %(fx_pair)s, %(fx_markup_bps)s, 'computed'
                )
                """,
                {
                    "iid": row["instrument_id"],
                    "spread_bps": spread_bps,
                    "fx_pair": None if currency == "USD" else f"{currency}/USD",
                    "fx_markup_bps": fx_markup_bps,
                },
            )
        processed += 1

    return {"processed": processed, "skipped": skipped}


def record_actual_cost(
    conn: psycopg.Connection[Any],
    *,
    order_id: int,
    actual_spread_bps: Decimal,
    actual_total_bps: Decimal,
) -> None:
    """Update the cost record with actual costs computed from fill data."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE trade_cost_record
            SET actual_spread_bps = %(actual_spread_bps)s,
                actual_total_bps = %(actual_total_bps)s
            WHERE order_id = %(order_id)s
            """,
            {
                "order_id": order_id,
                "actual_spread_bps": actual_spread_bps,
                "actual_total_bps": actual_total_bps,
            },
        )
