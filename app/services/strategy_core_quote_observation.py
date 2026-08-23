"""Prospective spread + denomination evidence for core sleeve candidates.

#2833 step 2.  The application-wide ``quotes`` table is a mutable latest
snapshot -- one row per instrument, enforced by its primary key -- so it can
never supply "a spread percentile over ~5 trading days of stored quote
snapshots", however long we wait.  ``sql/366`` carries the full reasoning.

This module writes one immutable row per candidate per hourly bucket from the
``Quote`` objects ``refresh_quotes`` has already fetched, so the lane costs no
additional provider calls.

It also persists ``conversion_rate`` -- eToro's documented "conversion rate
from the instrument's currency to USD" -- which is the ONLY per-instrument
denomination signal the API exposes.  ``instruments.currency`` is a venue
lookup and reads GBP for every ``.L`` line regardless of actual denomination.

A quote the provider re-serves from a shut market is refused as
``quote_stale`` rather than recorded: the sample this lane exists to build is
a spread PERCENTILE, and a duplicate is the one error that moves a percentile
without any new information arriving.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Final, Literal

import psycopg

from app.providers.market_data import Quote

SOURCE: Final = "etoro/core_candidate/best_bid_ask"
#: ``quotes_refresh`` ticks hourly; one observation per candidate per tick.
SAMPLE_INTERVAL_HOURS: Final = 1

#: A quote further than ONE SAMPLING INTERVAL from the observation instant --
#: in EITHER direction -- is not new evidence.  Too old and it is the same
#: book the previous tick already recorded; too far ahead and the clock it
#: came from is not ours, so it never ages out.  Either way counting it would
#: inflate the percentile with duplicates of a single spread.
#:
#: ⚠ Not an invented threshold: it is the tick cadence itself.  At one sample
#: per hour, anything older than an hour was already observable last tick, so
#: this is the widest bound that cannot double-count.  A bucket-relative test
#: would be wrong in the other direction -- a 14:59:58 quote recorded in the
#: 15:00 bucket is five seconds old and is perfectly good evidence.
#:
#: This is a MEASURED hazard, not a hypothetical.  eToro returns the last
#: quote when the market is shut (#2833: "markets shut; the provider returns
#: the last quote"), and the data-sources/etoro-api skill records 18 of 20
#: preflight timestamps arriving ~41 hours old.  Dev-verified 2026-08-23 (a
#: Sunday): all ten candidates returned Friday quotes, which without this
#: bound would have written ten `observed` rows per hour all weekend.
MAX_QUOTE_AGE: Final = timedelta(hours=SAMPLE_INTERVAL_HOURS)

ObservationStatus = Literal["observed", "missing", "invalid"]


@dataclass(frozen=True)
class CoreQuoteObservation:
    instrument_id: int
    sample_bucket: datetime
    observed_at: datetime
    quote_at: datetime | None
    bid: Decimal | None
    ask: Decimal | None
    last: Decimal | None
    spread_bps: Decimal | None
    conversion_rate: Decimal | None
    observation_status: ObservationStatus
    refusal_reason: str | None
    source: str = SOURCE


def sample_bucket(observed_at: datetime) -> datetime:
    """Truncate *observed_at* to its hourly bucket in UTC."""
    if observed_at.tzinfo is None:
        raise ValueError("observed_at must be timezone-aware")
    return observed_at.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


def normalise_quote(
    *,
    instrument_id: int,
    quote: Quote | None,
    observed_at: datetime,
) -> CoreQuoteObservation:
    """Turn one provider response into a durable observation.

    A missing or malformed quote is a row too: absence of evidence must stay
    measurable rather than silently thinning the sample the pass bar reads.
    """
    bucket = sample_bucket(observed_at)
    stamped = observed_at.astimezone(UTC)

    def _refused(status: ObservationStatus, reason: str) -> CoreQuoteObservation:
        return CoreQuoteObservation(
            instrument_id=instrument_id,
            sample_bucket=bucket,
            observed_at=stamped,
            quote_at=None,
            bid=None,
            ask=None,
            last=None,
            spread_bps=None,
            conversion_rate=None,
            observation_status=status,
            refusal_reason=reason,
        )

    if quote is None:
        return _refused("missing", "provider_omitted_quote")

    if quote.timestamp.tzinfo is None:
        return _refused("invalid", "quote_timestamp_naive")
    if quote.bid <= 0 or quote.ask <= 0:
        return _refused("invalid", "nonpositive_bid_or_ask")
    if quote.ask < quote.bid:
        return _refused("invalid", "crossed_market")
    if quote.last is not None and quote.last <= 0:
        return _refused("invalid", "nonpositive_last")
    # Checked AFTER the shape guards so a malformed quote reports the reason
    # an operator can act on, rather than being masked as merely stale.
    #
    # ⚠ The bound is SYMMETRIC, and deliberately so.  A one-sided staleness
    # test leaves the mirror-image hole open: a future-dated quote has a
    # NEGATIVE age, so it never expires and could populate every later bucket
    # with the same response -- the identical duplicate-evidence failure this
    # guard exists to prevent, arriving from the other direction.  Reusing
    # MAX_QUOTE_AGE rather than minting a skew constant keeps the rule "the
    # quote must belong to this tick's neighbourhood", with no second
    # threshold to justify.  The two causes stay separate reasons because a
    # future timestamp is a clock or provider fault, not a shut market.
    age = stamped - quote.timestamp.astimezone(UTC)
    if age > MAX_QUOTE_AGE:
        return _refused("invalid", "quote_stale")
    if age < -MAX_QUOTE_AGE:
        return _refused("invalid", "quote_future")

    bid = Decimal(quote.bid)
    ask = Decimal(quote.ask)
    midpoint = (bid + ask) / Decimal(2)
    # ⚠ A non-positive rate is dropped to NULL rather than stored: it would
    # read as a denomination we cannot honour, and NULL already means "the
    # provider did not tell us" (never "USD" -- see sql/366).
    rate = None if quote.conversion_rate is None else Decimal(quote.conversion_rate)
    conversion_rate = rate if rate is not None and rate > 0 else None
    return CoreQuoteObservation(
        instrument_id=instrument_id,
        sample_bucket=bucket,
        observed_at=stamped,
        quote_at=quote.timestamp.astimezone(UTC),
        bid=bid,
        ask=ask,
        last=None if quote.last is None else Decimal(quote.last),
        spread_bps=(ask - bid) / midpoint * Decimal(10_000),
        conversion_rate=conversion_rate,
        observation_status="observed",
        refusal_reason=None,
    )


_INSERT_OBSERVATION = """
    INSERT INTO strategy_core_quote_observations (
        instrument_id, sample_bucket, observed_at, quote_at,
        bid, ask, last, spread_bps, conversion_rate,
        observation_status, refusal_reason, source
    ) VALUES (
        %(instrument_id)s, %(sample_bucket)s, %(observed_at)s, %(quote_at)s,
        %(bid)s, %(ask)s, %(last)s, %(spread_bps)s, %(conversion_rate)s,
        %(observation_status)s, %(refusal_reason)s, %(source)s
    )
    ON CONFLICT (instrument_id, sample_bucket) DO NOTHING
"""


def _params(observation: CoreQuoteObservation) -> dict[str, Any]:
    return {
        "instrument_id": observation.instrument_id,
        "sample_bucket": observation.sample_bucket,
        "observed_at": observation.observed_at,
        "quote_at": observation.quote_at,
        "bid": observation.bid,
        "ask": observation.ask,
        "last": observation.last,
        "spread_bps": observation.spread_bps,
        "conversion_rate": observation.conversion_rate,
        "observation_status": observation.observation_status,
        "refusal_reason": observation.refusal_reason,
        "source": observation.source,
    }


def record_core_quote_observations(
    conn: psycopg.Connection[Any],
    observations: Sequence[CoreQuoteObservation],
) -> int:
    """Insert *observations*, keeping any row already in its bucket.

    Returns the number of rows written.  ``DO NOTHING`` rather than an upsert
    because the first observation in a bucket is the evidence -- a later
    manual dispatch must not replace it with a quote that knows more of the
    bucket (sql/366's immutability trigger enforces the same rule).

    One statement for the whole tick.  The caller owns the transaction: the
    connection runs autocommit, so a per-observation transaction would be a
    real BEGIN/COMMIT round trip each.  Every row is shape-valid by
    construction (``normalise_quote`` guarantees the sql/366 CHECK is
    satisfiable for observed AND refused rows alike), so the failure mode
    that would lose a batch is systemic -- and a systemic failure would have
    failed each isolated insert too.
    """
    if not observations:
        return 0
    with conn.cursor() as cursor:
        cursor.executemany(_INSERT_OBSERVATION, [_params(o) for o in observations])
        # psycopg3 accumulates affected rows across an executemany batch, so
        # this is the count actually inserted -- bucket collisions (the
        # idempotent re-run case) contribute zero rather than being counted
        # as fresh evidence.
        return max(cursor.rowcount, 0)


__all__ = [
    "MAX_QUOTE_AGE",
    "SAMPLE_INTERVAL_HOURS",
    "SOURCE",
    "CoreQuoteObservation",
    "ObservationStatus",
    "normalise_quote",
    "record_core_quote_observations",
    "sample_bucket",
]
