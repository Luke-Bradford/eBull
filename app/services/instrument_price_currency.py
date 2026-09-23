"""Per-instrument price currency from the broker's conversion rate (#3322).

Spec: ``docs/specs/etl/2026-09-23-instrument-price-currency.md``.

``instruments.currency`` defaults to the venue's curated currency (sql/159), which is
wrong wherever one venue quotes lines in several currencies: most LSE lines are
priced in pence, and some LSE and EUR-venue lines in USD. eToro's metadata endpoints
expose no currency; its rates endpoint documents ``conversionRateAsk/Bid`` as the
"conversion rate from instrument's currency to USD". That rate is the only
per-instrument denomination signal, so the currency is the candidate whose USD rate it
matches.

Only venues whose default is GBP or EUR are reclassified: ``live_fx_rates`` prices
exactly USD/EUR/GBP, so there the label currency is itself a candidate and "exactly
one candidate matches" is well-defined. Elsewhere an unpriced currency could match the
wrong candidate uniquely.
"""

from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Final, Literal

import psycopg

from app.providers.market_data import MarketDataProvider

logger = logging.getLogger(__name__)

# Fixed by construction from the 2026-09-23 full-population census (spec §"Full-
# population verification of the production rule"): the largest residual on a quote
# ≤7d old was 0.64%, the closest candidate pair (EUR/USD) is 14.6% apart, and no quote
# matched two candidates at 1%, 2% or 3%. Quotes older than 7d carry a frozen rate
# from when the listing died and must not reclassify anything.
TOLERANCE: Final = Decimal("0.02")
MAX_AGE: Final = timedelta(days=7)
# A live quote can carry a broker timestamp a moment ahead of our clock; beyond this it
# is not a quote we can date, so it is refused rather than trusted.
MAX_FUTURE_SKEW: Final = timedelta(minutes=5)

#: Venue currencies this module may reclassify (see module docstring).
RECLASSIFIABLE_VENUE_CURRENCIES: Final = ("GBP", "EUR")

#: Pence. Not ISO 4217; the market convention for LSE lines quoted in 1/100 GBP.
GBX: Final = "GBX"

Reason = Literal["derived", "stale_quote", "stale_fx", "bad_rate", "no_match", "ambiguous"]


@dataclass(frozen=True)
class FxReference:
    """USD per one unit of each candidate currency, plus when it was quoted."""

    usd_per_unit: Mapping[str, Decimal]
    quoted_at: datetime


def load_fx_reference(conn: psycopg.Connection[Any]) -> FxReference | None:
    """Build candidate USD rates from ``live_fx_rates`` (stored as USD→X).

    Returns ``None`` when EUR or GBP is missing — the candidate set would be
    incomplete, and uniqueness over an incomplete set is not evidence.
    """
    rows = conn.execute(
        """
        SELECT to_currency, rate, quoted_at
          FROM live_fx_rates
         WHERE from_currency = 'USD' AND to_currency IN ('EUR', 'GBP') AND rate > 0
        """
    ).fetchall()
    by_ccy = {r[0]: (Decimal(r[1]), r[2]) for r in rows}
    if set(by_ccy) != {"EUR", "GBP"}:
        return None
    usd_per_unit = {"USD": Decimal(1), "EUR": 1 / by_ccy["EUR"][0], "GBP": 1 / by_ccy["GBP"][0]}
    usd_per_unit[GBX] = usd_per_unit["GBP"] / 100
    return FxReference(usd_per_unit=usd_per_unit, quoted_at=min(q for _, q in by_ccy.values()))


def classify_price_currency(
    conversion_rate: Decimal | None,
    quoted_at: datetime,
    fx: FxReference,
    now: datetime,
) -> tuple[str | None, Reason]:
    """The one candidate currency whose USD rate matches ``conversion_rate``, or None."""
    if not timedelta(0) <= now - fx.quoted_at <= MAX_AGE:
        return None, "stale_fx"
    if not -MAX_FUTURE_SKEW <= now - quoted_at <= MAX_AGE:
        return None, "stale_quote"
    if conversion_rate is None or not conversion_rate.is_finite() or conversion_rate <= 0:
        return None, "bad_rate"
    matches = [ccy for ccy, usd in fx.usd_per_unit.items() if abs(conversion_rate / usd - 1) <= TOLERANCE]
    if not matches:
        return None, "no_match"
    if len(matches) > 1:
        return None, "ambiguous"
    return matches[0], "derived"


_BATCH: Final = 100  # rates endpoint maxItems (get-instrument-market-rates.md)


def derive_instrument_price_currencies(
    conn: psycopg.Connection[Any],
    provider: MarketDataProvider,
    clock: Callable[[], datetime],
) -> Counter[str]:
    """Re-derive ``instruments.currency`` for tradable GBP/EUR-venue lines.

    Unclassifiable rows are left untouched. Returns counts by outcome; ``changed``
    counts rows whose stored currency moved.
    """
    counts: Counter[str] = Counter()
    fx = load_fx_reference(conn)
    if fx is None:
        counts["no_fx_reference"] += 1
        logger.warning("instrument price currency: live_fx_rates lacks USD→EUR/GBP; nothing derived")
        return counts
    rows = conn.execute(
        """
        SELECT i.instrument_id, i.currency, e.currency AS venue_currency
          FROM instruments i
          JOIN exchanges e ON e.exchange_id = i.exchange
         WHERE i.is_tradable AND e.currency = ANY(%(venues)s)
         ORDER BY i.instrument_id
        """,
        {"venues": list(RECLASSIFIABLE_VENUE_CURRENCIES)},
    ).fetchall()
    current = {r[0]: (r[1], r[2]) for r in rows}
    ids: Sequence[int] = list(current)
    # Fetch every quote before opening the write transaction: ~30 HTTP round-trips
    # must not hold row locks on ``instruments``.
    derived: dict[int, tuple[str, datetime]] = {}
    quoted: set[int] = set()
    for start in range(0, len(ids), _BATCH):
        batch = provider.get_quotes(list(ids[start : start + _BATCH]))
        # Read the clock AFTER the fetch: a quote stamped during it is not "future".
        now = clock()
        for q in batch:
            if q.instrument_id not in current or q.instrument_id in quoted:
                continue
            quoted.add(q.instrument_id)
            ccy, reason = classify_price_currency(q.conversion_rate, q.timestamp, fx, now)
            counts[reason] += 1
            if ccy is not None:
                derived[q.instrument_id] = (ccy, q.timestamp)
    with conn.transaction():
        for iid, (ccy, at) in derived.items():
            stored, venue = current[iid]
            if ccy != stored:
                counts["changed"] += 1
            conn.execute(
                """
                UPDATE instruments
                   SET currency = %(ccy)s,
                       currency_source = %(source)s,
                       currency_derived_at = %(at)s
                 WHERE instrument_id = %(iid)s
                """,
                {"ccy": ccy, "source": "exchange" if ccy == venue else "broker_rate", "at": at, "iid": iid},
            )
    counts["no_quote"] = len(ids) - len(quoted)
    return counts
