"""Bounded persistence for strategy observations and completed intraday bars.

Fired signals remain in ``strategy_signals`` because outcomes and future trade
ownership refer to their durable ``signal_id``. Routine negative decisions are
kept in monthly partitions for 90 days and represented durably by daily counts.
Intraday storage accepts only the three declared tiers and refuses a batch that
exceeds its instrument or completed-bars-per-day cap.

Refs #2437, #2448. Schema: ``sql/276_strategy_observation_storage.sql``.
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from types import MappingProxyType
from typing import Any, Final, Literal, cast

import psycopg
import psycopg.sql

from app.services.signal_ledger import LedgerRow, store_signals

Timeframe = Literal["30m", "5m", "1m"]

SIGNAL_DETAIL_RETENTION_DAYS: Final = 90


@dataclass(frozen=True)
class IntradayTier:
    timeframe: Timeframe
    minutes_per_bar: int
    max_instruments: int
    retention_days: int | None
    retention_months: int | None
    partition_granularity: Literal["month", "day"]

    def __post_init__(self) -> None:
        if (self.retention_days is None) == (self.retention_months is None):
            raise ValueError("an intraday tier needs exactly one retention unit")

    @property
    def max_bars_per_instrument_day(self) -> int:
        return 390 // self.minutes_per_bar


_TIERS = (
    IntradayTier(
        "30m",
        minutes_per_bar=30,
        max_instruments=1_000,
        retention_days=None,
        retention_months=24,
        partition_granularity="month",
    ),
    IntradayTier(
        "5m",
        minutes_per_bar=5,
        max_instruments=250,
        retention_days=None,
        retention_months=12,
        partition_granularity="month",
    ),
    IntradayTier(
        "1m",
        minutes_per_bar=1,
        max_instruments=50,
        retention_days=30,
        retention_months=None,
        partition_granularity="day",
    ),
)
INTRADAY_TIERS: Final[Mapping[Timeframe, IntradayTier]] = MappingProxyType({tier.timeframe: tier for tier in _TIERS})


@dataclass(frozen=True)
class SignalStorageReport:
    logical_rows: int
    fired_rows: int
    retained_observation_rows: int
    aggregate_rows: int
    input_payload_bytes: int


@dataclass(frozen=True)
class IntradayBar:
    timeframe: Timeframe
    bar_time: datetime
    instrument_id: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal | None
    source: str

    def __post_init__(self) -> None:
        if self.timeframe not in INTRADAY_TIERS:
            raise ValueError(f"unknown timeframe {self.timeframe!r}; must be one of {list(INTRADAY_TIERS)}")
        if self.bar_time.tzinfo is None:
            raise ValueError("bar_time must be timezone-aware")
        if self.instrument_id <= 0:
            raise ValueError("instrument_id must be positive")
        if min(self.open, self.high, self.low, self.close) <= 0:
            raise ValueError("OHLC prices must be positive")
        if self.high < max(self.open, self.close, self.low) or self.low > min(self.open, self.close, self.high):
            raise ValueError("OHLC prices are inconsistent")
        if self.volume is not None and self.volume < 0:
            raise ValueError("volume must be non-negative when present")
        if not self.source.strip():
            raise ValueError("source must be non-empty")


@dataclass(frozen=True)
class IntradayStorageReport:
    rows_written: int
    instruments: int
    partitions_touched: int
    input_payload_bytes: int


@dataclass(frozen=True)
class RetentionPlan:
    signal_partitions: tuple[str, ...]
    intraday_partitions: tuple[str, ...]
    intraday_gap_rows: int = 0

    @property
    def partitions(self) -> tuple[str, ...]:
        return self.signal_partitions + self.intraday_partitions


_INSERT_OBSERVATION = """
    INSERT INTO strategy_signal_observations (
        strategy_id, strategy_version, instrument_id, signal_bar_date,
        signal_kind, verdict, reason_code
    ) VALUES (
        %(strategy_id)s, %(strategy_version)s, %(instrument_id)s, %(signal_bar_date)s,
        %(signal_kind)s, %(verdict)s, %(reason_code)s
    )
"""

_INSERT_DAILY_COUNT = """
    INSERT INTO strategy_signal_daily_counts (
        strategy_id, strategy_version, signal_bar_date,
        signal_kind, verdict, reason_code, row_count
    ) VALUES (
        %(strategy_id)s, %(strategy_version)s, %(signal_bar_date)s,
        %(signal_kind)s, %(verdict)s, %(reason_code)s, %(row_count)s
    )
"""

_INSERT_INTRADAY_BAR = """
    INSERT INTO strategy_intraday_bars (
        timeframe, bar_time, instrument_id, open, high, low, close, volume, source
    ) VALUES (
        %(timeframe)s, %(bar_time)s, %(instrument_id)s,
        %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s, %(source)s
    )
"""

_READ_INTRADAY_WATERMARKS = """
    SELECT timeframe, instrument_id, last_bar_time
    FROM strategy_intraday_watermarks
    WHERE timeframe = ANY(%(timeframes)s)
      AND instrument_id = ANY(%(instrument_ids)s)
"""

_ADVANCE_INTRADAY_WATERMARK = """
    INSERT INTO strategy_intraday_watermarks (timeframe, instrument_id, last_bar_time, updated_at)
    VALUES (%(timeframe)s, %(instrument_id)s, %(last_bar_time)s, now())
    ON CONFLICT (timeframe, instrument_id) DO UPDATE
       SET last_bar_time = EXCLUDED.last_bar_time,
           updated_at = now()
     WHERE EXCLUDED.last_bar_time > strategy_intraday_watermarks.last_bar_time
"""

# These use PostgreSQL's two-int advisory-lock space as (2448, tier id).
# Strategy identities below use the separate one-bigint overload, so their
# hash can only over-serialise another identity; it cannot collide with a tier.
_TIER_LOCK_IDS: Final[Mapping[Timeframe, int]] = MappingProxyType({"30m": 30, "5m": 5, "1m": 1})

_FIND_CROSS_TABLE_SIGNAL_CONFLICT = """
    WITH incoming AS (
        SELECT *
        FROM unnest(
            %(strategy_ids)s::text[],
            %(strategy_versions)s::text[],
            %(instrument_ids)s::bigint[],
            %(signal_dates)s::date[],
            %(signal_kinds)s::text[],
            %(verdicts)s::text[]
        ) AS item(strategy_id, strategy_version, instrument_id, signal_bar_date, signal_kind, verdict)
    )
    SELECT item.strategy_id, item.strategy_version, item.instrument_id,
           item.signal_bar_date, item.signal_kind
    FROM incoming AS item
    JOIN strategy_signals AS durable
      USING (strategy_id, strategy_version, instrument_id, signal_bar_date, signal_kind)
    WHERE item.verdict <> 'fired'
    UNION ALL
    SELECT item.strategy_id, item.strategy_version, item.instrument_id,
           item.signal_bar_date, item.signal_kind
    FROM incoming AS item
    JOIN strategy_signal_observations AS retained
      USING (strategy_id, strategy_version, instrument_id, signal_bar_date, signal_kind)
    WHERE item.verdict = 'fired'
    LIMIT 1
"""


def _next_month(value: date) -> date:
    return date(value.year + (value.month == 12), 1 if value.month == 12 else value.month + 1, 1)


def _months_before(value: date, months: int) -> date:
    ordinal = value.year * 12 + value.month - 1 - months
    year, month_zero_based = divmod(ordinal, 12)
    month = month_zero_based + 1
    # Retention checks only complete month partitions, so the first of the
    # target month is the exact and leap-safe cutoff.
    return date(year, month, 1)


def _signal_partition_name(value: date) -> str:
    return f"strategy_signal_observations_y{value.year:04d}m{value.month:02d}"


def ensure_signal_partition(conn: psycopg.Connection[Any], value: date) -> str:
    """Create the one bounded monthly leaf required by ``value``."""
    start = value.replace(day=1)
    end = _next_month(start)
    name = _signal_partition_name(value)
    statement = psycopg.sql.SQL(
        "CREATE TABLE IF NOT EXISTS {} PARTITION OF strategy_signal_observations FOR VALUES FROM ({}) TO ({})"
    ).format(psycopg.sql.Identifier(name), psycopg.sql.Literal(start), psycopg.sql.Literal(end))
    conn.execute(statement)
    return name


def _intraday_partition_bounds(tier: IntradayTier, value: datetime) -> tuple[datetime, datetime, str]:
    utc_value = value.astimezone(UTC)
    if tier.partition_granularity == "day":
        start = utc_value.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
        suffix = f"y{start.year:04d}m{start.month:02d}d{start.day:02d}"
    else:
        start = utc_value.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        next_date = _next_month(start.date())
        end = datetime(next_date.year, next_date.month, 1, tzinfo=UTC)
        suffix = f"y{start.year:04d}m{start.month:02d}"
    return start, end, f"strategy_intraday_bars_{tier.timeframe}_{suffix}"


def _intraday_retention_floor(tier: IntradayTier, observed_date: date) -> date:
    if tier.retention_days is not None:
        return observed_date - timedelta(days=tier.retention_days)
    return _months_before(observed_date, cast(int, tier.retention_months))


def ensure_intraday_partition(conn: psycopg.Connection[Any], timeframe: Timeframe, value: datetime) -> str:
    tier = INTRADAY_TIERS[timeframe]
    start, end, name = _intraday_partition_bounds(tier, value)
    parent = f"strategy_intraday_bars_{timeframe}"
    statement = psycopg.sql.SQL("CREATE TABLE IF NOT EXISTS {} PARTITION OF {} FOR VALUES FROM ({}) TO ({})").format(
        psycopg.sql.Identifier(name),
        psycopg.sql.Identifier(parent),
        psycopg.sql.Literal(start),
        psycopg.sql.Literal(end),
    )
    conn.execute(statement)
    return name


def _ledger_payload_bytes(row: LedgerRow) -> int:
    fields = (
        row.strategy_id,
        row.strategy_version,
        str(row.instrument_id),
        row.signal_bar_date.isoformat(),
        row.signal_kind,
        row.verdict,
        row.not_evaluable_reason or "",
    )
    return sum(len(field.encode("utf-8")) for field in fields)


def _logical_signal_key(row: LedgerRow) -> tuple[str, str, int, date, str]:
    return (
        row.strategy_id,
        row.strategy_version,
        row.instrument_id,
        row.signal_bar_date,
        row.signal_kind,
    )


def store_strategy_observations(conn: psycopg.Connection[Any], rows: Sequence[LedgerRow]) -> SignalStorageReport:
    """Persist one complete census without durable negative-decision bloat.

    No conflict is ignored. The scan watermark makes a normal repeat a no-op;
    any collision here means recorded evidence drifted and must abort the same
    transaction as the watermark advance.
    """
    if not rows:
        return SignalStorageReport(0, 0, 0, 0, 0)

    logical_keys = [_logical_signal_key(row) for row in rows]
    duplicate_keys = [key for key, count in Counter(logical_keys).items() if count > 1]
    if duplicate_keys:
        raise ValueError(f"strategy observation batch contains duplicate logical signal key(s): {duplicate_keys[:3]}")

    fired = [row for row in rows if row.verdict == "fired"]
    observations = [row for row in rows if row.verdict != "fired"]
    counts: Counter[tuple[str, str, date, str, str, str]] = Counter(
        (
            row.strategy_id,
            row.strategy_version,
            row.signal_bar_date,
            row.signal_kind,
            row.verdict,
            row.not_evaluable_reason or "",
        )
        for row in rows
    )

    identities = sorted({(row.strategy_id, row.strategy_version) for row in rows})
    with conn.transaction():
        for strategy_id, strategy_version in identities:
            # One lock per immutable identity makes the cross-table check safe
            # for concurrent first writes without retaining one key row per
            # routine observation.
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 2448) # hashtextextended(%s, 2449))",
                (strategy_id, strategy_version),
            )
            watermark = conn.execute(
                """
                SELECT frontier_date
                FROM strategy_scan_watermark
                WHERE strategy_id = %s AND strategy_version = %s
                """,
                (strategy_id, strategy_version),
            ).fetchone()
            if watermark is not None:
                stale = [
                    row
                    for row in rows
                    if row.strategy_id == strategy_id
                    and row.strategy_version == strategy_version
                    and row.signal_bar_date < watermark[0]
                ]
                if stale:
                    raise ValueError(
                        f"strategy observation precedes terminal watermark {watermark[0]} for "
                        f"{strategy_id}/{strategy_version}: {_logical_signal_key(stale[0])}"
                    )

        conflict = conn.execute(
            _FIND_CROSS_TABLE_SIGNAL_CONFLICT,
            {
                "strategy_ids": [row.strategy_id for row in rows],
                "strategy_versions": [row.strategy_version for row in rows],
                "instrument_ids": [row.instrument_id for row in rows],
                "signal_dates": [row.signal_bar_date for row in rows],
                "signal_kinds": [row.signal_kind for row in rows],
                "verdicts": [row.verdict for row in rows],
            },
        ).fetchone()
        if conflict is not None:
            raise ValueError(f"strategy observation conflicts with a verdict in the other storage tier: {conflict}")

        for month in sorted({row.signal_bar_date.replace(day=1) for row in observations}):
            ensure_signal_partition(conn, month)
        fired_written = store_signals(cast(psycopg.Connection[tuple], conn), fired)
        with conn.cursor() as cur:
            if observations:
                cur.executemany(
                    _INSERT_OBSERVATION,
                    [
                        {
                            "strategy_id": row.strategy_id,
                            "strategy_version": row.strategy_version,
                            "instrument_id": row.instrument_id,
                            "signal_bar_date": row.signal_bar_date,
                            "signal_kind": row.signal_kind,
                            "verdict": row.verdict,
                            "reason_code": row.not_evaluable_reason or "",
                        }
                        for row in observations
                    ],
                )
                observation_written = cur.rowcount
                if observation_written < 0:
                    raise RuntimeError("strategy_signal_observations INSERT did not report a row count")
            else:
                observation_written = 0
            cur.executemany(
                _INSERT_DAILY_COUNT,
                [
                    {
                        "strategy_id": key[0],
                        "strategy_version": key[1],
                        "signal_bar_date": key[2],
                        "signal_kind": key[3],
                        "verdict": key[4],
                        "reason_code": key[5],
                        "row_count": count,
                    }
                    for key, count in counts.items()
                ],
            )
            aggregate_written = cur.rowcount
        if aggregate_written < 0:
            raise RuntimeError("strategy_signal_daily_counts INSERT did not report a row count")
        if fired_written + observation_written != len(rows):
            raise RuntimeError(
                f"strategy observation write stored {fired_written} fired + {observation_written} retained "
                f"against {len(rows)} logical rows"
            )
        return SignalStorageReport(
            logical_rows=len(rows),
            fired_rows=fired_written,
            retained_observation_rows=observation_written,
            aggregate_rows=aggregate_written,
            input_payload_bytes=sum(_ledger_payload_bytes(row) for row in rows),
        )


def _intraday_payload_bytes(row: IntradayBar) -> int:
    fields = (
        row.timeframe,
        row.bar_time.isoformat(),
        str(row.instrument_id),
        str(row.open),
        str(row.high),
        str(row.low),
        str(row.close),
        "" if row.volume is None else str(row.volume),
        row.source,
    )
    return sum(len(field.encode("utf-8")) for field in fields)


def store_intraday_bars(
    conn: psycopg.Connection[Any],
    rows: Sequence[IntradayBar],
    *,
    observed_at: datetime,
) -> IntradayStorageReport:
    """Store completed bars after applying tier caps and bounded backpressure."""
    if observed_at.tzinfo is None:
        raise ValueError("observed_at must be timezone-aware")
    if not rows:
        return IntradayStorageReport(0, 0, 0, 0)

    instruments_by_tier: dict[Timeframe, set[int]] = defaultdict(set)
    bars_by_instrument_day: Counter[tuple[Timeframe, int, date]] = Counter()
    keys: set[tuple[Timeframe, datetime, int]] = set()
    for row in rows:
        tier = INTRADAY_TIERS[row.timeframe]
        utc_time = row.bar_time.astimezone(UTC)
        if utc_time > observed_at.astimezone(UTC):
            raise ValueError(f"{row.timeframe} bar {utc_time} is still in the future at {observed_at}")
        retention_floor = _intraday_retention_floor(tier, observed_at.astimezone(UTC).date())
        if utc_time.date() < retention_floor:
            raise ValueError(
                f"{row.timeframe} bar {utc_time} is before retained horizon {retention_floor} at {observed_at}"
            )
        if utc_time.second or utc_time.microsecond or utc_time.minute % tier.minutes_per_bar:
            raise ValueError(f"{row.timeframe} bar_time {utc_time} is not aligned to a completed tier boundary")
        key = (row.timeframe, utc_time, row.instrument_id)
        if key in keys:
            raise ValueError(f"duplicate intraday bar in batch: {key}")
        keys.add(key)
        instruments_by_tier[row.timeframe].add(row.instrument_id)
        bars_by_instrument_day[(row.timeframe, row.instrument_id, utc_time.date())] += 1

    for timeframe, instruments in instruments_by_tier.items():
        tier = INTRADAY_TIERS[timeframe]
        if len(instruments) > tier.max_instruments:
            raise ValueError(
                f"{timeframe} batch contains {len(instruments)} instruments; cap is {tier.max_instruments}"
            )
    for (timeframe, instrument_id, bar_date), count in bars_by_instrument_day.items():
        maximum = INTRADAY_TIERS[timeframe].max_bars_per_instrument_day
        if count > maximum:
            raise ValueError(
                f"{timeframe}/{instrument_id}/{bar_date} contains {count} bars; regular-session cap is {maximum}"
            )

    ordered = sorted(rows, key=lambda item: (item.timeframe, item.instrument_id, item.bar_time))
    requested_days = sorted(bars_by_instrument_day)

    # This savepoint/transaction makes every application-level refusal below
    # atomic even if a caller catches it. A transaction advisory lock per tier
    # also closes the absent-watermark race without adding a per-bar btree.
    with conn.transaction():
        for timeframe in sorted(instruments_by_tier):
            conn.execute("SELECT pg_advisory_xact_lock(2448, %s)", (_TIER_LOCK_IDS[timeframe],))

        active_rows = conn.execute(
            """
            SELECT timeframe, instrument_id
            FROM strategy_intraday_watermarks
            WHERE timeframe = ANY(%(timeframes)s)
            """,
            {"timeframes": sorted(instruments_by_tier)},
        ).fetchall()
        active_by_tier: dict[Timeframe, set[int]] = defaultdict(set)
        for timeframe, instrument_id in active_rows:
            active_by_tier[cast(Timeframe, str(timeframe))].add(int(instrument_id))
        for timeframe, incoming in instruments_by_tier.items():
            resulting_width = len(active_by_tier[timeframe] | incoming)
            maximum = INTRADAY_TIERS[timeframe].max_instruments
            if resulting_width > maximum:
                raise ValueError(
                    f"{timeframe} retained universe would contain {resulting_width} instruments; cap is {maximum}"
                )

        stored_day_counts: dict[tuple[Timeframe, int, date], int] = {}
        for timeframe, instrument_id, bar_date in requested_days:
            row = conn.execute(
                """
                SELECT COUNT(*)
                FROM strategy_intraday_bars
                WHERE timeframe = %(timeframe)s
                  AND instrument_id = %(instrument_id)s
                  AND bar_time >= %(day_start)s
                  AND bar_time < %(day_end)s
                """,
                {
                    "timeframe": timeframe,
                    "instrument_id": instrument_id,
                    "day_start": datetime.combine(bar_date, datetime.min.time(), tzinfo=UTC),
                    "day_end": datetime.combine(bar_date + timedelta(days=1), datetime.min.time(), tzinfo=UTC),
                },
            ).fetchone()
            stored_day_counts[(timeframe, instrument_id, bar_date)] = int(row[0]) if row else 0
        for key, incoming_count in bars_by_instrument_day.items():
            maximum = INTRADAY_TIERS[key[0]].max_bars_per_instrument_day
            total = stored_day_counts[key] + incoming_count
            if total > maximum:
                raise ValueError(
                    f"{key[0]}/{key[1]}/{key[2]} would contain {total} bars; regular-session cap is {maximum}"
                )

        existing = {
            (cast(Timeframe, str(row[0])), int(row[1])): row[2]
            for row in conn.execute(
                _READ_INTRADAY_WATERMARKS,
                {
                    "timeframes": sorted(instruments_by_tier),
                    "instrument_ids": sorted({row.instrument_id for row in rows}),
                },
            ).fetchall()
        }
        last_by_key: dict[tuple[Timeframe, int], datetime] = {}
        for row in ordered:
            key = (row.timeframe, row.instrument_id)
            utc_time = row.bar_time.astimezone(UTC)
            prior = existing.get(key)
            if prior is not None and utc_time <= prior:
                raise ValueError(
                    f"{row.timeframe}/{row.instrument_id} bar {utc_time} is at or behind stored watermark {prior}"
                )
            last_by_key[key] = utc_time

        partitions = {ensure_intraday_partition(conn, row.timeframe, row.bar_time) for row in ordered}
        with conn.cursor() as cur:
            cur.executemany(
                _INSERT_INTRADAY_BAR,
                [
                    {
                        "timeframe": row.timeframe,
                        "bar_time": row.bar_time.astimezone(UTC),
                        "instrument_id": row.instrument_id,
                        "open": row.open,
                        "high": row.high,
                        "low": row.low,
                        "close": row.close,
                        "volume": row.volume,
                        "source": row.source,
                    }
                    for row in ordered
                ],
            )
            written = cur.rowcount
            cur.executemany(
                _ADVANCE_INTRADAY_WATERMARK,
                [
                    {"timeframe": key[0], "instrument_id": key[1], "last_bar_time": last_bar_time}
                    for key, last_bar_time in last_by_key.items()
                ],
            )
            watermarks_advanced = cur.rowcount
        if written < 0:
            raise RuntimeError("strategy_intraday_bars INSERT did not report a row count")
        if watermarks_advanced != len(last_by_key):
            raise RuntimeError(
                f"advanced {watermarks_advanced} intraday watermarks against {len(last_by_key)} instrument tiers"
            )
        return IntradayStorageReport(
            rows_written=written,
            instruments=len({row.instrument_id for row in rows}),
            partitions_touched=len(partitions),
            input_payload_bytes=sum(_intraday_payload_bytes(row) for row in rows),
        )


_SIGNAL_PARTITION_RE = re.compile(r"^strategy_signal_observations_y(\d{4})m(\d{2})$")
_INTRADAY_MONTH_RE = re.compile(r"^strategy_intraday_bars_(30m|5m)_y(\d{4})m(\d{2})$")
_INTRADAY_DAY_RE = re.compile(r"^strategy_intraday_bars_1m_y(\d{4})m(\d{2})d(\d{2})$")


def _partition_names(conn: psycopg.Connection[Any]) -> tuple[str, ...]:
    rows = conn.execute(
        """
        SELECT child.relname
        FROM pg_inherits i
        JOIN pg_class child ON child.oid = i.inhrelid
        JOIN pg_class parent ON parent.oid = i.inhparent
        WHERE parent.relname IN (
            'strategy_signal_observations',
            'strategy_intraday_bars_30m',
            'strategy_intraday_bars_5m',
            'strategy_intraday_bars_1m'
        )
        ORDER BY child.relname
        """
    ).fetchall()
    return tuple(str(row[0]) for row in rows)


def _intraday_retention_cutoffs(today: date) -> dict[Timeframe, date]:
    """Resolve each tier's day/month policy to one exclusive date cutoff."""
    return {
        timeframe: (
            today - timedelta(days=tier.retention_days)
            if tier.retention_days is not None
            else _months_before(today, cast(int, tier.retention_months))
        )
        for timeframe, tier in INTRADAY_TIERS.items()
    }


def retention_plan(conn: psycopg.Connection[Any], *, as_of: datetime) -> RetentionPlan:
    """Return only leaf relations whose entire bound is outside retention."""
    if as_of.tzinfo is None:
        raise ValueError("as_of must be timezone-aware")
    today = as_of.astimezone(UTC).date()
    signal_cutoff = today - timedelta(days=SIGNAL_DETAIL_RETENTION_DAYS)
    intraday_cutoffs = _intraday_retention_cutoffs(today)
    signal: list[str] = []
    intraday: list[str] = []
    for name in _partition_names(conn):
        if match := _SIGNAL_PARTITION_RE.fullmatch(name):
            end = _next_month(date(int(match[1]), int(match[2]), 1))
            if end <= signal_cutoff:
                signal.append(name)
            continue
        if match := _INTRADAY_MONTH_RE.fullmatch(name):
            timeframe = cast(Timeframe, match[1])
            end = _next_month(date(int(match[2]), int(match[3]), 1))
            if end <= intraday_cutoffs[timeframe]:
                intraday.append(name)
            continue
        if match := _INTRADAY_DAY_RE.fullmatch(name):
            end = date(int(match[1]), int(match[2]), int(match[3])) + timedelta(days=1)
            if end <= intraday_cutoffs["1m"]:
                intraday.append(name)
    gap_row = conn.execute(
        """
        SELECT count(*)
        FROM strategy_intraday_gaps
        WHERE (timeframe = '30m' AND gap_end <= %(cutoff_30m)s)
           OR (timeframe = '5m'  AND gap_end <= %(cutoff_5m)s)
           OR (timeframe = '1m'  AND gap_end <= %(cutoff_1m)s)
        """,
        {
            "cutoff_30m": datetime.combine(intraday_cutoffs["30m"], datetime.min.time(), tzinfo=UTC),
            "cutoff_5m": datetime.combine(intraday_cutoffs["5m"], datetime.min.time(), tzinfo=UTC),
            "cutoff_1m": datetime.combine(intraday_cutoffs["1m"], datetime.min.time(), tzinfo=UTC),
        },
    ).fetchone()
    return RetentionPlan(tuple(signal), tuple(intraday), int(gap_row[0]) if gap_row else 0)


def drop_expired_partitions(conn: psycopg.Connection[Any], *, as_of: datetime, dry_run: bool = True) -> RetentionPlan:
    """Drop expired detail leaves and release empty universe watermarks."""
    plan = retention_plan(conn, as_of=as_of)
    if dry_run:
        return plan
    with conn.transaction():
        for timeframe in sorted(INTRADAY_TIERS):
            conn.execute("SELECT pg_advisory_xact_lock(2448, %s)", (_TIER_LOCK_IDS[timeframe],))
        for name in plan.partitions:
            # ``name`` came from pg_class and must still match one of the closed
            # patterns in ``retention_plan``; Identifier handles quoting as a
            # second boundary. A caller can never supply a relation name.
            conn.execute(psycopg.sql.SQL("DROP TABLE {}").format(psycopg.sql.Identifier(name)))
        today = as_of.astimezone(UTC).date()
        intraday_cutoffs = _intraday_retention_cutoffs(today)
        deleted_gaps = conn.execute(
            """
            DELETE FROM strategy_intraday_gaps
            WHERE (timeframe = '30m' AND gap_end <= %(cutoff_30m)s)
               OR (timeframe = '5m'  AND gap_end <= %(cutoff_5m)s)
               OR (timeframe = '1m'  AND gap_end <= %(cutoff_1m)s)
            """,
            {
                "cutoff_30m": datetime.combine(intraday_cutoffs["30m"], datetime.min.time(), tzinfo=UTC),
                "cutoff_5m": datetime.combine(intraday_cutoffs["5m"], datetime.min.time(), tzinfo=UTC),
                "cutoff_1m": datetime.combine(intraday_cutoffs["1m"], datetime.min.time(), tzinfo=UTC),
            },
        ).rowcount
        if deleted_gaps != plan.intraday_gap_rows:
            raise RuntimeError(f"deleted {deleted_gaps} intraday gap rows against planned {plan.intraday_gap_rows}")
        # Watermarks are bounded control metadata, not observations. Releasing
        # one only after every retained bar for the tier/instrument has expired
        # permits deliberate universe rotation without weakening the cap.
        conn.execute(
            """
            DELETE FROM strategy_intraday_watermarks AS watermark
            WHERE NOT EXISTS (
                SELECT 1
                FROM strategy_intraday_bars AS bar
                WHERE bar.timeframe = watermark.timeframe
                  AND bar.instrument_id = watermark.instrument_id
            )
            """
        )
    return plan


__all__ = [
    "INTRADAY_TIERS",
    "SIGNAL_DETAIL_RETENTION_DAYS",
    "IntradayBar",
    "IntradayStorageReport",
    "IntradayTier",
    "RetentionPlan",
    "SignalStorageReport",
    "drop_expired_partitions",
    "ensure_intraday_partition",
    "ensure_signal_partition",
    "retention_plan",
    "store_intraday_bars",
    "store_strategy_observations",
]
