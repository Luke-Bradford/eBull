"""
FX conversion service.

Handles currency conversion for display purposes using live_fx_rates.
Tax-related conversions continue to use the fx_rates table (sql/013).

FX invariant: rate = units of to_currency per 1 unit of from_currency.
"""

from __future__ import annotations

import logging
from datetime import datetime
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


def convert_quote_fields(
    bid: Decimal,
    ask: Decimal,
    last: Decimal | None,
    *,
    native_ccy: str,
    display_ccy: str,
    rates: dict[tuple[str, str], Decimal],
) -> tuple[Decimal, Decimal, Decimal | None] | None:
    """Convert a triple of quote prices (bid/ask/last) into ``display_ccy``.

    Returns ``None`` when no FX rate is available for the pair —
    callers fall back to the native triple. ``last`` may be None
    independently and is passed through that way after conversion.

    Why a single helper for the triple instead of three ``convert``
    calls: SSE delivery hot-path runs this per tick. Looking up the
    pair once and reusing the rate avoids three dict lookups + three
    branching tries when only the input numbers differ.
    """
    if native_ccy == display_ccy:
        return bid, ask, last
    direct = rates.get((native_ccy, display_ccy))
    if direct is not None:
        rate = direct
    else:
        inv = rates.get((display_ccy, native_ccy))
        if inv is None:
            return None
        # Inverse path: use 1/inv. Decimal division preserves
        # precision better than float reciprocal.
        rate = Decimal(1) / inv
    return (
        bid * rate,
        ask * rate,
        None if last is None else last * rate,
    )


def upsert_live_fx_rate(
    conn: psycopg.Connection[Any],
    *,
    from_currency: str,
    to_currency: str,
    rate: Decimal,
    quoted_at: datetime,
) -> None:
    """Insert or update a single live FX rate row."""
    conn.execute(
        """
        INSERT INTO live_fx_rates (from_currency, to_currency, rate, quoted_at)
        VALUES (%(from_currency)s, %(to_currency)s, %(rate)s, %(quoted_at)s)
        ON CONFLICT (from_currency, to_currency) DO UPDATE SET
            rate = EXCLUDED.rate,
            quoted_at = EXCLUDED.quoted_at
        """,
        {
            "from_currency": from_currency,
            "to_currency": to_currency,
            "rate": rate,
            "quoted_at": quoted_at,
        },
    )
