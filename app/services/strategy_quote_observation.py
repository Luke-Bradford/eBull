"""Bounded prospective best-bid/ask evidence for strategy research.

The application-wide ``quotes`` table is a mutable latest snapshot.  This
module samples only the exact active intraday research panel, once per symbol
per five-minute bucket, so later strategy evaluation can use the spread that
was actually observed rather than today's quote or a static estimate.

Missing and invalid provider responses are durable coverage observations.  Raw
ticks, depth and derived indicator histories are deliberately not retained.
"""

from __future__ import annotations

import calendar
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Final, Literal

import psycopg

from app.providers.market_data import MarketDataProvider, Quote

SAMPLE_MINUTES: Final = 5
RETENTION_MONTHS: Final = 24
MAX_PANEL_INSTRUMENTS: Final = 50

ObservationStatus = Literal["observed", "missing", "invalid"]


@dataclass(frozen=True)
class QuoteMember:
    symbol: str
    instrument_id: int | None
    resolution_error: str | None = None


@dataclass(frozen=True)
class QuoteObservation:
    universe_version: str
    instrument_id: int
    sample_bucket: datetime
    observed_at: datetime
    quote_at: datetime | None
    bid: Decimal | None
    ask: Decimal | None
    last: Decimal | None
    spread_bps: Decimal | None
    observation_status: ObservationStatus
    refusal_reason: str | None
    source: str


@dataclass(frozen=True)
class QuoteCaptureFailure:
    symbol: str
    reason: str


@dataclass(frozen=True)
class QuoteCaptureReport:
    universe_version: str
    expected: int
    observed: int
    missing: int
    invalid: int
    rows_written: int
    failures: tuple[QuoteCaptureFailure, ...]


def sample_bucket(observed_at: datetime) -> datetime:
    if observed_at.tzinfo is None:
        raise ValueError("observed_at must be timezone-aware")
    stamp = observed_at.astimezone(UTC)
    return stamp.replace(minute=stamp.minute - stamp.minute % SAMPLE_MINUTES, second=0, microsecond=0)


def _active_quote_members(conn: psycopg.Connection[Any]) -> tuple[str, tuple[QuoteMember, ...]]:
    versions = conn.execute(
        """
        SELECT universe_version
        FROM strategy_intraday_universe_versions
        WHERE status = 'active'
        ORDER BY universe_version
        """
    ).fetchall()
    if len(versions) != 1:
        raise RuntimeError(f"expected exactly one active intraday universe, found {len(versions)}")
    version = str(versions[0][0])
    rows = conn.execute(
        """
        SELECT member.symbol,
               array_agg(DISTINCT instrument.instrument_id ORDER BY instrument.instrument_id)
                   FILTER (WHERE instrument.instrument_id IS NOT NULL) AS instrument_ids
        FROM strategy_intraday_universe_members AS member
        LEFT JOIN instruments AS instrument
          ON instrument.symbol = member.symbol
         AND instrument.is_tradable = true
        WHERE member.universe_version = %(version)s
        GROUP BY member.symbol
        ORDER BY member.symbol
        """,
        {"version": version},
    ).fetchall()
    if not rows:
        raise RuntimeError(f"active intraday universe {version!r} has no members")
    if len(rows) > MAX_PANEL_INSTRUMENTS:
        raise RuntimeError(
            f"active intraday universe {version!r} has {len(rows)} unique symbols; quote cap is {MAX_PANEL_INSTRUMENTS}"
        )
    members: list[QuoteMember] = []
    for symbol, instrument_ids in rows:
        ids = [int(value) for value in instrument_ids or []]
        members.append(
            QuoteMember(
                symbol=str(symbol),
                instrument_id=ids[0] if len(ids) == 1 else None,
                resolution_error=None if len(ids) == 1 else f"expected one tradable instrument, found {len(ids)}",
            )
        )
    return version, tuple(members)


def _normalise_quote(
    *,
    universe_version: str,
    instrument_id: int,
    quote: Quote | None,
    observed_at: datetime,
) -> QuoteObservation:
    bucket = sample_bucket(observed_at)
    source = f"etoro/{universe_version}/best_bid_ask"
    if quote is None:
        return QuoteObservation(
            universe_version,
            instrument_id,
            bucket,
            observed_at.astimezone(UTC),
            None,
            None,
            None,
            None,
            None,
            "missing",
            "provider_omitted_quote",
            source,
        )

    refusal: str | None = None
    if quote.timestamp.tzinfo is None:
        refusal = "quote_timestamp_naive"
    elif quote.bid <= 0 or quote.ask <= 0:
        refusal = "nonpositive_bid_or_ask"
    elif quote.ask < quote.bid:
        refusal = "crossed_market"
    elif quote.last is not None and quote.last <= 0:
        refusal = "nonpositive_last"
    if refusal is not None:
        return QuoteObservation(
            universe_version,
            instrument_id,
            bucket,
            observed_at.astimezone(UTC),
            None,
            None,
            None,
            None,
            None,
            "invalid",
            refusal,
            source,
        )

    bid = Decimal(quote.bid)
    ask = Decimal(quote.ask)
    midpoint = (bid + ask) / Decimal(2)
    return QuoteObservation(
        universe_version,
        instrument_id,
        bucket,
        observed_at.astimezone(UTC),
        quote.timestamp.astimezone(UTC),
        bid,
        ask,
        None if quote.last is None else Decimal(quote.last),
        (ask - bid) / midpoint * Decimal(10_000),
        "observed",
        None,
        source,
    )


_UPSERT_OBSERVATION = """
    INSERT INTO strategy_quote_observations (
        universe_version, instrument_id, sample_bucket, observed_at,
        quote_at, bid, ask, last, spread_bps,
        observation_status, refusal_reason, source
    ) VALUES (
        %(universe_version)s, %(instrument_id)s, %(sample_bucket)s, %(observed_at)s,
        %(quote_at)s, %(bid)s, %(ask)s, %(last)s, %(spread_bps)s,
        %(observation_status)s, %(refusal_reason)s, %(source)s
    )
    ON CONFLICT (universe_version, instrument_id, sample_bucket) DO NOTHING
"""


def capture_active_universe_quotes(
    conn: psycopg.Connection[Any],
    provider: MarketDataProvider,
    *,
    observed_at: datetime | None = None,
) -> QuoteCaptureReport:
    """Fetch one quote batch and persist one bounded observation per resolved symbol.

    ``observed_at`` is a deterministic test hook. Production callers omit it:
    capture time is taken *after* the provider response, so a request lasting
    several seconds cannot make a newly returned quote appear to come from the
    future.
    """
    if observed_at is not None and observed_at.tzinfo is None:
        raise ValueError("observed_at must be timezone-aware")
    version, members = _active_quote_members(conn)
    resolved = [member for member in members if member.instrument_id is not None]
    failures = tuple(
        QuoteCaptureFailure(member.symbol, f"universe_resolution: {member.resolution_error}")
        for member in members
        if member.instrument_id is None
    )
    expected_ids = [int(member.instrument_id) for member in resolved if member.instrument_id is not None]
    quotes = provider.get_quotes(expected_ids)
    counts = Counter(quote.instrument_id for quote in quotes)
    duplicates = sorted(instrument_id for instrument_id, count in counts.items() if count > 1)
    extras = sorted(set(counts) - set(expected_ids))
    if duplicates:
        raise RuntimeError(f"provider returned duplicate quotes for {duplicates}")
    if extras:
        raise RuntimeError(f"provider returned out-of-scope quotes for {extras}")
    captured_at = datetime.now(tz=UTC) if observed_at is None else observed_at
    quote_by_id = {quote.instrument_id: quote for quote in quotes}
    observations = [
        _normalise_quote(
            universe_version=version,
            instrument_id=instrument_id,
            quote=quote_by_id.get(instrument_id),
            observed_at=captured_at,
        )
        for instrument_id in expected_ids
    ]
    rows_written = 0
    if observations:
        with conn.transaction(), conn.cursor() as cur:
            # The panel is capped at 50. Sum each statement's affected count
            # explicitly rather than depending on driver-specific executemany
            # rowcount aggregation; ON CONFLICT no-ops must report zero.
            for observation in observations:
                cur.execute(_UPSERT_OBSERVATION, asdict(observation))
                if cur.rowcount < 0:
                    raise RuntimeError("strategy quote INSERT did not report an affected-row count")
                rows_written += cur.rowcount
    status_counts = Counter(observation.observation_status for observation in observations)
    return QuoteCaptureReport(
        universe_version=version,
        expected=len(members),
        observed=status_counts["observed"],
        missing=status_counts["missing"],
        invalid=status_counts["invalid"],
        rows_written=rows_written,
        failures=failures,
    )


def _months_before(value: datetime, months: int) -> datetime:
    absolute = value.year * 12 + value.month - 1 - months
    year, month_index = divmod(absolute, 12)
    month = month_index + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return value.replace(year=year, month=month, day=day)


def retire_quote_observations(conn: psycopg.Connection[Any], *, as_of: datetime, dry_run: bool = True) -> int:
    """Count or delete samples older than the fixed 24-calendar-month horizon."""
    if as_of.tzinfo is None:
        raise ValueError("as_of must be timezone-aware")
    cutoff = _months_before(as_of.astimezone(UTC), RETENTION_MONTHS)
    if dry_run:
        row = conn.execute(
            "SELECT count(*) FROM strategy_quote_observations WHERE sample_bucket < %s", (cutoff,)
        ).fetchone()
        return int(row[0]) if row else 0
    with conn.transaction():
        deleted = conn.execute("DELETE FROM strategy_quote_observations WHERE sample_bucket < %s", (cutoff,)).rowcount
    return deleted


__all__ = [
    "MAX_PANEL_INSTRUMENTS",
    "RETENTION_MONTHS",
    "SAMPLE_MINUTES",
    "QuoteCaptureFailure",
    "QuoteCaptureReport",
    "QuoteObservation",
    "capture_active_universe_quotes",
    "retire_quote_observations",
    "sample_bucket",
]
