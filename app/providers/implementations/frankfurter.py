"""
Frankfurter FX rate provider.

Fetches ECB reference exchange rates from the Frankfurter API
(https://frankfurter.dev/). No API key required.

ECB publishes rates daily around 16:00 CET on working days.
Rates are informational reference rates — suitable for display
currency conversion, not for trade execution pricing.
"""

from __future__ import annotations

import logging
from decimal import Decimal

import httpx

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.frankfurter.dev"
_TIMEOUT_S = 15.0


def fetch_latest_rates(
    base: str,
    targets: list[str],
) -> dict[tuple[str, str], Decimal]:
    """Fetch latest ECB rates for base → each target currency.

    Returns a dict keyed by (from_currency, to_currency) → rate.
    Rate semantics: 1 unit of ``base`` = ``rate`` units of ``target``.

    Raises on network/HTTP errors — caller should handle.
    """
    if not targets:
        return {}

    # Frankfurter API: GET /v1/latest?base=USD&symbols=GBP,EUR
    symbols = ",".join(targets)
    with httpx.Client(timeout=_TIMEOUT_S) as client:
        response = client.get(
            f"{_BASE_URL}/v1/latest",
            params={"base": base, "symbols": symbols},
        )
        response.raise_for_status()

    data = response.json()
    raw_rates: dict[str, object] = data.get("rates", {})

    result: dict[tuple[str, str], Decimal] = {}
    for ccy, value in raw_rates.items():
        if value is not None:
            try:
                result[(base, ccy)] = Decimal(str(value))
            except Exception:
                logger.warning("Failed to parse Frankfurter rate %s→%s: %s", base, ccy, value)

    return result
