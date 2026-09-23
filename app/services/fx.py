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


#: Minor-unit quote currencies → (major ISO currency, major units per minor unit).
#: GBX (pence) is how most LSE lines are priced (#3322); it is not ISO 4217, so it
#: never appears in an FX table and is resolved here, at the one conversion chokepoint.
MINOR_UNITS: dict[str, tuple[str, Decimal]] = {"GBX": ("GBP", Decimal("0.01"))}


def _pair_rate(
    from_ccy: str,
    to_ccy: str,
    rates: dict[tuple[str, str], Decimal],
) -> Decimal | None:
    """Units of ``to_ccy`` per 1 ``from_ccy`` (direct, then inverse), minor units resolved."""
    if from_ccy == to_ccy:
        return Decimal(1)
    from_major, from_scale = MINOR_UNITS.get(from_ccy, (from_ccy, Decimal(1)))
    to_major, to_scale = MINOR_UNITS.get(to_ccy, (to_ccy, Decimal(1)))
    if from_major == to_major:
        major_rate = Decimal(1)
    elif (from_major, to_major) in rates:
        major_rate = rates[(from_major, to_major)]
    elif (to_major, from_major) in rates:
        major_rate = Decimal(1) / rates[(to_major, from_major)]
    else:
        return None
    # from minor → from major (× from_scale), convert, to major → to minor (÷ to_scale).
    return from_scale * major_rate / to_scale


def convert(
    amount: Decimal,
    from_ccy: str,
    to_ccy: str,
    rates: dict[tuple[str, str], Decimal],
) -> Decimal:
    """Convert amount from one currency to another using the rates dict.

    Tries the direct pair first, then the inverse; minor-unit currencies (GBX)
    convert through their major currency.  Raises FxRateNotFound if no pair is
    available.
    """
    rate = _pair_rate(from_ccy, to_ccy, rates)
    if rate is None:
        raise FxRateNotFound(f"No FX rate for {from_ccy} \u2192 {to_ccy}")
    return amount * rate


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
    rate = _pair_rate(native_ccy, display_ccy, rates)
    if rate is None:
        return None
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
