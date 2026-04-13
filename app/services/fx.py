"""
FX conversion service.

Handles currency conversion for display purposes using live_fx_rates.
Tax-related conversions continue to use the fx_rates table (sql/013).

FX invariant: rate = units of to_currency per 1 unit of from_currency.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

import psycopg
import psycopg.rows

logger = logging.getLogger(__name__)


class FxRateNotFound(ValueError):
    """Raised when no FX rate is available for a currency pair."""


def convert(
    amount: Decimal,
    from_ccy: str,
    to_ccy: str,
    rates: dict[tuple[str, str], Decimal],
) -> Decimal:
    """Convert amount from one currency to another using the rates dict.

    Tries the direct pair first, then the inverse.  Raises FxRateNotFound
    if neither is available.
    """
    if from_ccy == to_ccy:
        return amount
    key = (from_ccy, to_ccy)
    if key in rates:
        return amount * rates[key]
    inv_key = (to_ccy, from_ccy)
    if inv_key in rates:
        return amount / rates[inv_key]
    raise FxRateNotFound(f"No FX rate for {from_ccy} \u2192 {to_ccy}")


def load_live_fx_rates(
    conn: psycopg.Connection[Any],
) -> dict[tuple[str, str], Decimal]:
    """Load all live FX rates into a lookup dict keyed by (from, to)."""
    rows = conn.execute(
        "SELECT from_currency, to_currency, rate FROM live_fx_rates",
    ).fetchall()
    return {(r[0], r[1]): r[2] for r in rows}


def load_live_fx_rates_with_metadata(
    conn: psycopg.Connection[Any],
) -> dict[tuple[str, str], dict[str, Any]]:
    """Load live FX rates with quoted_at metadata for API responses."""
    rows = conn.execute(
        "SELECT from_currency, to_currency, rate, quoted_at FROM live_fx_rates",
    ).fetchall()
    return {(r[0], r[1]): {"rate": r[2], "quoted_at": r[3]} for r in rows}
