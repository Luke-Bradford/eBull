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
                   default_hold_days
            FROM transaction_cost_config
            WHERE id = TRUE
            """
        )
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
