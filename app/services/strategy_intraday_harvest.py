"""Bounded prospective eToro intraday research harvester.

The provider has no date anchor, so retained history can only grow from repeated
recent-window fetches.  This service reads one predeclared active universe,
filters forming and non-RTH candles, removes overlap at the durable watermark,
records (but never fills) gaps, and delegates all bar writes/caps/retention
invariants to :mod:`strategy_observation_storage`.

Refs #2477. Schema: ``sql/298_strategy_intraday_harvest.sql``.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Final, cast
from zoneinfo import ZoneInfo

import psycopg

from app.providers.market_data import IntradayBar as ProviderBar
from app.providers.market_data import IntradayInterval, MarketDataProvider
from app.services.market_calendar import us_market_status
from app.services.strategy_observation_storage import (
    INTRADAY_TIERS,
    IntradayBar,
    Timeframe,
    store_intraday_bars,
)

MAX_REQUESTS_PER_RUN: Final = 12
MAX_PROVIDER_BARS: Final = 1_000
_OVERLAP_BARS: Final = 3
_NY: Final = ZoneInfo("America/New_York")
_INTERVALS: Final[dict[Timeframe, IntradayInterval]] = {
    "30m": "ThirtyMinutes",
    "5m": "FiveMinutes",
    "1m": "OneMinute",
}


@dataclass(frozen=True)
class HarvestMember:
    universe_version: str
    ordinal: int
    timeframe: Timeframe
    symbol: str
    instrument_id: int | None
    resolution_error: str | None = None


@dataclass(frozen=True)
class HarvestFailure:
    timeframe: Timeframe
    symbol: str
    reason: str


@dataclass(frozen=True)
class HarvestReport:
    universe_version: str
    selected: int
    fetched: int
    completed_rth: int
    written: int
    gaps_recorded: int
    failures: tuple[HarvestFailure, ...]


def _session_bounds(value: date) -> tuple[datetime, datetime] | None:
    status = us_market_status(value)
    if status == "closed":
        return None
    close = time(13) if status == "half_day" else time(16)
    return (
        datetime.combine(value, time(9, 30), tzinfo=_NY).astimezone(UTC),
        datetime.combine(value, close, tzinfo=_NY).astimezone(UTC),
    )


def _completed_rth_bars(
    bars: Sequence[ProviderBar],
    *,
    timeframe: Timeframe,
    observed_at: datetime,
) -> list[ProviderBar]:
    """Return unique completed NYSE-session bars, refusing conflicting duplicates."""
    if observed_at.tzinfo is None:
        raise ValueError("observed_at must be timezone-aware")
    duration = timedelta(minutes=INTRADAY_TIERS[timeframe].minutes_per_bar)
    by_time: dict[datetime, ProviderBar] = {}
    for bar in bars:
        if bar.timestamp.tzinfo is None:
            raise ValueError("provider returned a timezone-naive intraday timestamp")
        stamp = bar.timestamp.astimezone(UTC)
        if stamp + duration > observed_at.astimezone(UTC):
            continue
        bounds = _session_bounds(stamp.astimezone(_NY).date())
        if bounds is None or stamp < bounds[0] or stamp + duration > bounds[1]:
            continue
        prior = by_time.get(stamp)
        if prior is not None and prior != bar:
            raise ValueError(f"provider returned conflicting duplicate candle at {stamp.isoformat()}")
        by_time[stamp] = bar
    return [by_time[key] for key in sorted(by_time)]


def _fetch_count(*, timeframe: Timeframe, watermark: datetime | None, observed_at: datetime) -> int:
    if watermark is None:
        return MAX_PROVIDER_BARS
    elapsed = observed_at.astimezone(UTC) - watermark.astimezone(UTC)
    duration = timedelta(minutes=INTRADAY_TIERS[timeframe].minutes_per_bar)
    estimated = int(elapsed / duration) + _OVERLAP_BARS
    return min(MAX_PROVIDER_BARS, max(_OVERLAP_BARS, estimated))


def _gap_ranges(
    stamps: Sequence[datetime],
    *,
    timeframe: Timeframe,
    watermark: datetime | None,
) -> tuple[tuple[datetime, datetime], ...]:
    """Detect missing intervals inside an observed RTH path; never infer overnight gaps."""
    if not stamps:
        return ()
    duration = timedelta(minutes=INTRADAY_TIERS[timeframe].minutes_per_bar)
    ordered = sorted(stamp.astimezone(UTC) for stamp in stamps)
    points = ([watermark.astimezone(UTC)] if watermark is not None else []) + ordered
    gaps: list[tuple[datetime, datetime]] = []
    for left, right in zip(points, points[1:], strict=False):
        if left.astimezone(_NY).date() != right.astimezone(_NY).date():
            continue
        if right > left + duration:
            gaps.append((left + duration, right))
    return tuple(gaps)


def _active_members(conn: psycopg.Connection[Any]) -> tuple[str, list[HarvestMember], int]:
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
        SELECT member.ordinal, member.timeframe, member.symbol,
               array_agg(instrument.instrument_id ORDER BY instrument.instrument_id)
                   FILTER (WHERE instrument.instrument_id IS NOT NULL) AS instrument_ids
        FROM strategy_intraday_universe_members AS member
        LEFT JOIN instruments AS instrument
          ON instrument.symbol = member.symbol
         AND instrument.is_tradable = true
        WHERE member.universe_version = %(version)s
        GROUP BY member.ordinal, member.timeframe, member.symbol
        ORDER BY member.ordinal
        """,
        {"version": version},
    ).fetchall()
    cursor_row = conn.execute(
        "SELECT last_ordinal FROM strategy_intraday_harvest_cursors WHERE universe_version = %s",
        (version,),
    ).fetchone()
    cursor = int(cursor_row[0]) if cursor_row else 0
    members: list[HarvestMember] = []
    for ordinal, timeframe, symbol, instrument_ids in rows:
        ids = [int(value) for value in instrument_ids or []]
        members.append(
            HarvestMember(
                version,
                int(ordinal),
                cast(Timeframe, str(timeframe)),
                str(symbol),
                ids[0] if len(ids) == 1 else None,
                None if len(ids) == 1 else f"expected one tradable instrument, found {len(ids)}",
            )
        )
    if not members:
        raise RuntimeError(f"active intraday universe {version!r} has no members")
    return version, members, cursor


def _round_robin(members: Sequence[HarvestMember], *, cursor: int, limit: int) -> list[HarvestMember]:
    if limit <= 0:
        raise ValueError("request limit must be positive")
    start = next((index for index, member in enumerate(members) if member.ordinal > cursor), 0)
    width = min(limit, len(members))
    return [members[(start + offset) % len(members)] for offset in range(width)]


def _watermark(conn: psycopg.Connection[Any], member: HarvestMember) -> datetime | None:
    if member.instrument_id is None:
        raise RuntimeError(f"cannot read a watermark for unresolved member {member.symbol}")
    row = conn.execute(
        """
        SELECT last_bar_time
        FROM strategy_intraday_watermarks
        WHERE timeframe = %(timeframe)s AND instrument_id = %(instrument_id)s
        """,
        {"timeframe": member.timeframe, "instrument_id": member.instrument_id},
    ).fetchone()
    return row[0] if row else None


def _record_gaps(
    conn: psycopg.Connection[Any],
    *,
    member: HarvestMember,
    gaps: Sequence[tuple[datetime, datetime]],
    observed_at: datetime,
) -> int:
    if member.instrument_id is None:
        raise RuntimeError(f"cannot record gaps for unresolved member {member.symbol}")
    if not gaps:
        return 0
    with conn.transaction():
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO strategy_intraday_gaps (
                    universe_version, timeframe, instrument_id,
                    gap_start, gap_end, first_detected_at, last_detected_at
                ) VALUES (
                    %(version)s, %(timeframe)s, %(instrument_id)s,
                    %(gap_start)s, %(gap_end)s, %(observed_at)s, %(observed_at)s
                )
                ON CONFLICT (universe_version, timeframe, instrument_id, gap_start, gap_end)
                DO UPDATE SET last_detected_at = EXCLUDED.last_detected_at
                """,
                [
                    {
                        "version": member.universe_version,
                        "timeframe": member.timeframe,
                        "instrument_id": member.instrument_id,
                        "gap_start": gap[0],
                        "gap_end": gap[1],
                        "observed_at": observed_at,
                    }
                    for gap in gaps
                ],
            )
    return len(gaps)


def _advance_cursor(conn: psycopg.Connection[Any], member: HarvestMember) -> None:
    with conn.transaction():
        conn.execute(
            """
            INSERT INTO strategy_intraday_harvest_cursors (universe_version, last_ordinal, updated_at)
            VALUES (%(version)s, %(ordinal)s, now())
            ON CONFLICT (universe_version) DO UPDATE
               SET last_ordinal = EXCLUDED.last_ordinal,
                   updated_at = now()
            """,
            {"version": member.universe_version, "ordinal": member.ordinal},
        )


def run_intraday_harvest(
    conn: psycopg.Connection[Any],
    provider: MarketDataProvider,
    *,
    observed_at: datetime,
    max_requests: int = MAX_REQUESTS_PER_RUN,
) -> HarvestReport:
    """Fetch one bounded round-robin slice of the active research universe."""
    if observed_at.tzinfo is None:
        raise ValueError("observed_at must be timezone-aware")
    version, members, cursor = _active_members(conn)
    selected = _round_robin(members, cursor=cursor, limit=min(max_requests, MAX_REQUESTS_PER_RUN))
    fetched = completed = written = gaps_recorded = 0
    failures: list[HarvestFailure] = []
    for member in selected:
        if member.resolution_error is not None or member.instrument_id is None:
            failures.append(
                HarvestFailure(
                    member.timeframe,
                    member.symbol,
                    f"universe_resolution: {member.resolution_error or 'instrument id missing'}",
                )
            )
            _advance_cursor(conn, member)
            continue
        try:
            watermark = _watermark(conn, member)
            provider_bars = provider.get_intraday_candles(
                member.instrument_id,
                _INTERVALS[member.timeframe],
                _fetch_count(timeframe=member.timeframe, watermark=watermark, observed_at=observed_at),
            )
            fetched += len(provider_bars)
            rth = _completed_rth_bars(provider_bars, timeframe=member.timeframe, observed_at=observed_at)
            completed += len(rth)
            new = [bar for bar in rth if watermark is None or bar.timestamp.astimezone(UTC) > watermark]
            gaps = _gap_ranges([bar.timestamp for bar in new], timeframe=member.timeframe, watermark=watermark)
            rows = [
                IntradayBar(
                    timeframe=member.timeframe,
                    bar_time=bar.timestamp.astimezone(UTC),
                    instrument_id=member.instrument_id,
                    open=Decimal(bar.open),
                    high=Decimal(bar.high),
                    low=Decimal(bar.low),
                    close=Decimal(bar.close),
                    volume=None if bar.volume is None else Decimal(bar.volume),
                    source=f"etoro/{version}/nyse_rth",
                )
                for bar in new
            ]
            report = store_intraday_bars(conn, rows, observed_at=observed_at)
            written += report.rows_written
            gaps_recorded += _record_gaps(conn, member=member, gaps=gaps, observed_at=observed_at.astimezone(UTC))
        except Exception as exc:
            # Provider messages can contain request URLs, identifiers or
            # upstream response fragments. Persist the stable exception class
            # for diagnosis without copying those internals into job_runs.
            failures.append(HarvestFailure(member.timeframe, member.symbol, type(exc).__name__))
        finally:
            _advance_cursor(conn, member)
    return HarvestReport(
        universe_version=version,
        selected=len(selected),
        fetched=fetched,
        completed_rth=completed,
        written=written,
        gaps_recorded=gaps_recorded,
        failures=tuple(failures),
    )


__all__ = [
    "MAX_PROVIDER_BARS",
    "MAX_REQUESTS_PER_RUN",
    "HarvestFailure",
    "HarvestMember",
    "HarvestReport",
    "run_intraday_harvest",
]
