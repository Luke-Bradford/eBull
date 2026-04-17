"""
Frankfurter FX rate provider.

Fetches ECB reference exchange rates from the Frankfurter API
(https://frankfurter.dev/). No API key required.

ECB publishes rates daily around 16:00 CET on working days.
Rates are informational reference rates — suitable for display
currency conversion, not for trade execution pricing.

Conditional fetch (verified 2026-04-17): the Frankfurter API
honours ``ETag`` / ``If-None-Match`` → 304. ``If-Modified-Since``
is ignored. Use ``fetch_latest_rates_conditional`` for the
incremental-fetch path and keep the existing ``fetch_latest_rates``
for unconditional callers.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal

import httpx

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.frankfurter.dev"
_TIMEOUT_S = 15.0


@dataclass(frozen=True)
class FrankfurterResult:
    """Outcome of a conditional-GET fetch against Frankfurter.

    ``rates`` keyed by ``(from_currency, to_currency) -> rate``.
    ``ecb_date`` is the ISO date string for the ECB publication this
    payload reflects. ``etag`` is the server's ``ETag`` header value
    (including the surrounding quotes) — persist this so the next
    run can send it as ``If-None-Match``.
    """

    rates: dict[tuple[str, str], Decimal]
    ecb_date: str | None
    etag: str | None


def fetch_latest_rates(
    base: str,
    targets: list[str],
) -> tuple[dict[tuple[str, str], Decimal], str | None]:
    """Fetch latest ECB rates for base → each target currency.

    Returns ``(rates, ecb_date)`` where *rates* is keyed by
    ``(from_currency, to_currency) → rate`` and *ecb_date* is the ISO
    date string from the API response (e.g. ``"2026-04-11"``).  On
    weekends/holidays the date reflects the last ECB publication day.

    Rate semantics: 1 unit of ``base`` = ``rate`` units of ``target``.

    Raises on network/HTTP errors — caller should handle.
    """
    if not targets:
        return {}, None

    # Frankfurter API: GET /v1/latest?base=USD&symbols=GBP,EUR
    symbols = ",".join(targets)
    with httpx.Client(timeout=_TIMEOUT_S) as client:
        response = client.get(
            f"{_BASE_URL}/v1/latest",
            params={"base": base, "symbols": symbols},
        )
        response.raise_for_status()

    data = response.json()
    ecb_date: str | None = data.get("date")
    raw_rates: dict[str, object] = data.get("rates", {})

    result: dict[tuple[str, str], Decimal] = {}
    for ccy, value in raw_rates.items():
        if value is not None:
            try:
                result[(base, ccy)] = Decimal(str(value))
            except Exception:
                logger.warning("Failed to parse Frankfurter rate %s→%s: %s", base, ccy, value)

    return result, ecb_date


def fetch_latest_rates_conditional(
    base: str,
    targets: list[str],
    *,
    if_none_match: str | None = None,
) -> FrankfurterResult | None:
    """Conditional variant of ``fetch_latest_rates``.

    Sends ``If-None-Match: <if_none_match>`` when an ETag from a prior
    run is available. Returns:

    - ``None`` when the server responds 304 Not Modified.
    - ``FrankfurterResult`` with parsed rates + ecb_date + new ETag
      on 200 OK.

    ECB only publishes once per working day around 16:00 CET, so most
    intra-day invocations land on the 304 path.
    """
    if not targets:
        return FrankfurterResult(rates={}, ecb_date=None, etag=None)

    symbols = ",".join(targets)
    headers: dict[str, str] = {}
    if if_none_match:
        headers["If-None-Match"] = if_none_match

    with httpx.Client(timeout=_TIMEOUT_S) as client:
        response = client.get(
            f"{_BASE_URL}/v1/latest",
            params={"base": base, "symbols": symbols},
            headers=headers,
        )
        if response.status_code == 304:
            logger.info("Frankfurter: 304 Not Modified")
            return None
        response.raise_for_status()

    data = response.json()
    ecb_date: str | None = data.get("date")
    raw_rates: dict[str, object] = data.get("rates", {})

    rates: dict[tuple[str, str], Decimal] = {}
    for ccy, value in raw_rates.items():
        if value is not None:
            try:
                rates[(base, ccy)] = Decimal(str(value))
            except Exception:
                logger.warning("Failed to parse Frankfurter rate %s→%s: %s", base, ccy, value)

    return FrankfurterResult(
        rates=rates,
        ecb_date=ecb_date,
        etag=response.headers.get("ETag"),
    )
