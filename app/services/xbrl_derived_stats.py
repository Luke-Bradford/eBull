"""Compute-from-XBRL helpers (#432) — replaces yfinance key-stats
where SEC data + quotes can answer the same question.

Each helper takes a ``psycopg.Connection`` + ``instrument_id`` and
returns a typed ``Decimal | None``. None = insufficient data (operator
UI falls back to yfinance cleanly).

Helpers land as they retire a specific yfinance call site — see the
ticket ladder in #432 for the per-call-site status.

Shipped in this PR:
  - compute_market_cap           (retires profile.market_cap)

Queued for follow-ups:
  - compute_pe_ttm, compute_pb, compute_roe, compute_roa,
    compute_debt_to_equity, compute_revenue_growth_yoy,
    compute_earnings_growth_yoy.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any, Literal

import psycopg
import psycopg.rows

MarketCapSource = Literal["dei", "us-gaap", "unavailable"]


@dataclass(frozen=True)
class MarketCap:
    value: Decimal
    shares: Decimal
    price: Decimal
    price_as_of: date | None
    shares_as_of: date
    shares_source: MarketCapSource


def compute_market_cap(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
) -> MarketCap | None:
    """Compute live market cap from the newest SEC share count ×
    latest quote (bid/ask midpoint, or ``last`` when available).

    Returns ``None`` if either input is missing. Wires to
    ``instrument_share_count_latest`` (sql/052, #435) for the share
    count — that view prefers DEI over us-gaap, picks the newest
    restated value, and exposes the source taxonomy used. Price
    mirrors the pattern in ``instrument_dividend_summary.priced``:
    NULLIF(GREATEST(last, 0), 0) first, then (bid+ask)/2.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            WITH shares AS (
                SELECT latest_shares, as_of_date, source_taxonomy
                FROM instrument_share_count_latest
                WHERE instrument_id = %(iid)s
            ),
            priced AS (
                -- ``quotes`` is 1:1 current-snapshot by contract, but
                -- pin ORDER BY + LIMIT 1 as a defensive belt so any
                -- future migration that holds historical rows cannot
                -- fan-out this LEFT JOIN into a non-deterministic
                -- fetchone (review #444 BLOCKING).
                SELECT
                    COALESCE(
                        NULLIF(GREATEST(last, 0), 0),
                        CASE WHEN bid > 0 AND ask > 0 THEN (bid + ask) / 2 END
                    ) AS price,
                    quoted_at::date AS price_as_of
                FROM quotes
                WHERE instrument_id = %(iid)s
                ORDER BY quoted_at DESC
                LIMIT 1
            )
            SELECT s.latest_shares, s.as_of_date, s.source_taxonomy,
                   p.price, p.price_as_of
            FROM shares s
            LEFT JOIN priced p ON TRUE
            """,
            {"iid": instrument_id},
        )
        row = cur.fetchone()

    if row is None:
        return None
    shares = row["latest_shares"]
    price = row["price"]
    if shares is None or price is None or shares <= 0 or price <= 0:
        return None

    source_raw = str(row["source_taxonomy"])
    if source_raw not in ("dei", "us-gaap", "unavailable"):
        source: MarketCapSource = "unavailable"
    else:
        source = source_raw  # type: ignore[assignment]

    return MarketCap(
        value=Decimal(shares) * Decimal(price),
        shares=Decimal(shares),
        price=Decimal(price),
        price_as_of=row["price_as_of"],
        shares_as_of=row["as_of_date"],
        shares_source=source,
    )
