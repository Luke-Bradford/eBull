"""Freeze a bounded recent comparator snapshot from eToro ``price_daily``.

The live table is an execution-venue view and remains mutable.  Strategy
evidence needs an immutable input identity, so this module copies only a small,
declared ETF set through one fixed frontier and fingerprints the exact numeric
facts.  It never stores derived indicators and never resolves a comparator onto
``research_price_series.instrument_id``.

Spec: ``docs/proposals/ta/2026-08-10-recent-comparator-frontier.md``.
Refs #2482, parent #2469.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from math import log, sqrt
from statistics import median
from typing import Any

import psycopg

SNAPSHOT_ID = "etoro-comparators-2026-07-08-v1"
FROZEN_FRONTIER = date(2026, 7, 8)
VENDOR = f"etoro/{SNAPSHOT_ID}"
UPSTREAM_SOURCE = "etoro"
LICENCE = "eToro Builders Economy Terms 2026-02-17; personal/internal Licensed Content"
SOURCE_CONTRACT = "eToro Public API OneDay OHLCV via price_daily; portal verified 2026-08-10"
SOURCE_TERMS_URL = (
    "https://www.etoro.com/wp-content/uploads/2026/03/Master_eToro_Builders_Economy_Terms_17-Feb-2026-clean_R.pdf"
)
SESSION_CALENDAR = "eToro OneDay provider calendar; missing sessions remain missing"
SOURCE_TIMEZONE = "UTC fromDate truncated to provider session date"
OHLC_ADJUSTMENT_BASIS = "split_adjusted"
DIVIDEND_ADJUSTMENT_BASIS = "none"
ETORO_ETF_INSTRUMENT_TYPE_ID = 6

MIN_OVERLAP_SESSIONS = 500
MIN_RETURN_CORRELATION = 0.99
MIN_NORMALISED_LEVEL_RATIO = 0.995
MAX_NORMALISED_LEVEL_RATIO = 1.0
MAX_P99_RETURN_DIFFERENCE = 0.007

#: State Street's official 2025-11-20 notice declares these five 2:1 splits,
#: effective before the open on 2025-12-05. The legacy archive stops in 2024
#: and therefore remains on the old scale; eToro back-adjusts its earlier bars.
#: https://www.ssga.com/us/en/institutional/library-content/products/fund-docs/
#: etfs/us/information-schedules/select-sector-spdr-fund-share-splits-faq.pdf
POST_LEGACY_SPLIT_FACTOR: Mapping[str, Decimal] = {
    "XLB": Decimal("0.5"),
    "XLE": Decimal("0.5"),
    "XLK": Decimal("0.5"),
    "XLU": Decimal("0.5"),
    "XLY": Decimal("0.5"),
}

#: Broad market, size/growth, the complete current Select Sector SPDR set, and
#: three cross-asset regime series. DIA remains available only in the older
#: frozen source because eToro does not currently carry an exact DIA symbol.
COMPARATOR_SYMBOLS: tuple[str, ...] = (
    "SPY",
    "QQQ",
    "IWM",
    "VTI",
    "XLC",
    "XLY",
    "XLP",
    "XLE",
    "XLF",
    "XLV",
    "XLI",
    "XLB",
    "XLRE",
    "XLK",
    "XLU",
    "TLT",
    "GLD",
    "UUP",
)


@dataclass(frozen=True)
class ComparatorBar:
    bar_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal | None


@dataclass(frozen=True)
class ComparatorSourceSeries:
    symbol: str
    instrument_id: int
    bars: tuple[ComparatorBar, ...]
    sha256: str

    @property
    def first_bar(self) -> date:
        return self.bars[0].bar_date

    @property
    def last_bar(self) -> date:
        return self.bars[-1].bar_date


@dataclass(frozen=True)
class ComparatorSnapshot:
    members: tuple[ComparatorSourceSeries, ...]
    sha256: str

    @property
    def row_count(self) -> int:
        return sum(len(member.bars) for member in self.members)


@dataclass(frozen=True)
class OverlapMeasurement:
    symbol: str
    paired_sessions: int
    expected_split_factor: Decimal
    median_normalised_level_ratio: float
    return_correlation: float
    median_return_difference: float
    p99_absolute_return_difference: float


class ComparatorUnavailable(ValueError):
    """The frozen comparator cannot support an exact causal session set."""


def align_exact_sessions(
    symbol: str,
    required_dates: Sequence[date],
    closes: Mapping[date, Decimal],
) -> tuple[Decimal, ...]:
    """Return exact-session closes; never carry the last observation forward."""
    missing = [bar_date for bar_date in required_dates if bar_date not in closes]
    if missing:
        preview = ", ".join(item.isoformat() for item in missing[:3])
        raise ComparatorUnavailable(f"{symbol}: missing {len(missing)} required comparator sessions ({preview})")
    return tuple(closes[bar_date] for bar_date in required_dates)


def _correlation(left: Sequence[float], right: Sequence[float]) -> float:
    if len(left) != len(right) or len(left) < 2:
        raise ValueError("correlation requires equally sized sequences with at least two observations")
    left_mean = sum(left) / len(left)
    right_mean = sum(right) / len(right)
    covariance = sum((x - left_mean) * (y - right_mean) for x, y in zip(left, right, strict=True))
    left_variance = sum((x - left_mean) ** 2 for x in left)
    right_variance = sum((y - right_mean) ** 2 for y in right)
    if left_variance == 0 or right_variance == 0:
        raise ValueError("correlation is undefined for a constant return series")
    return covariance / sqrt(left_variance * right_variance)


def measure_overlap(
    symbol: str,
    recent: Mapping[date, Decimal],
    legacy: Mapping[date, Decimal],
) -> OverlapMeasurement:
    """Measure price-basis compatibility without pretending levels are equal."""
    dates = sorted(set(recent) & set(legacy))
    if len(dates) < MIN_OVERLAP_SESSIONS:
        raise ValueError(f"{symbol}: only {len(dates)} overlapping sessions; need {MIN_OVERLAP_SESSIONS}")
    factor = POST_LEGACY_SPLIT_FACTOR.get(symbol, Decimal("1"))
    ratios = [float(recent[item] / legacy[item] / factor) for item in dates]
    recent_returns = [log(float(recent[current] / recent[previous])) for previous, current in zip(dates, dates[1:])]
    legacy_returns = [log(float(legacy[current] / legacy[previous])) for previous, current in zip(dates, dates[1:])]
    differences = [left - right for left, right in zip(recent_returns, legacy_returns, strict=True)]
    p99_index = int(0.99 * (len(differences) - 1))
    measurement = OverlapMeasurement(
        symbol=symbol,
        paired_sessions=len(dates),
        expected_split_factor=factor,
        median_normalised_level_ratio=median(ratios),
        return_correlation=_correlation(recent_returns, legacy_returns),
        median_return_difference=median(differences),
        p99_absolute_return_difference=sorted(abs(item) for item in differences)[p99_index],
    )
    if not MIN_NORMALISED_LEVEL_RATIO <= measurement.median_normalised_level_ratio <= MAX_NORMALISED_LEVEL_RATIO:
        raise ValueError(
            f"{symbol}: normalised median level ratio {measurement.median_normalised_level_ratio:.6f} "
            f"is outside [{MIN_NORMALISED_LEVEL_RATIO}, {MAX_NORMALISED_LEVEL_RATIO}]"
        )
    if measurement.return_correlation < MIN_RETURN_CORRELATION:
        raise ValueError(
            f"{symbol}: return correlation {measurement.return_correlation:.6f} is below {MIN_RETURN_CORRELATION}"
        )
    if measurement.p99_absolute_return_difference > MAX_P99_RETURN_DIFFERENCE:
        raise ValueError(
            f"{symbol}: p99 absolute return difference {measurement.p99_absolute_return_difference:.6f} "
            f"exceeds {MAX_P99_RETURN_DIFFERENCE}"
        )
    return measurement


def _decimal_text(value: Decimal | None) -> str:
    """Canonical numeric meaning, independent of PostgreSQL NUMERIC scale."""
    if value is None:
        return ""
    if not value.is_finite():
        raise ValueError(f"non-finite decimal cannot be fingerprinted: {value}")
    normalised = value.normalize()
    if normalised == 0:
        return "0"
    return format(normalised, "f")


def _bar_bytes(bar: ComparatorBar) -> bytes:
    fields = (
        bar.bar_date.isoformat(),
        _decimal_text(bar.open),
        _decimal_text(bar.high),
        _decimal_text(bar.low),
        _decimal_text(bar.close),
        _decimal_text(bar.volume),
    )
    return ("|".join(fields) + "\n").encode()


def fingerprint_series(symbol: str, instrument_id: int, bars: Sequence[ComparatorBar]) -> str:
    digest = hashlib.sha256(f"{symbol}|{instrument_id}\n".encode())
    for bar in bars:
        digest.update(_bar_bytes(bar))
    return digest.hexdigest()


def build_snapshot(members: Iterable[tuple[str, int, Sequence[ComparatorBar]]]) -> ComparatorSnapshot:
    """Validate, order and fingerprint an exact comparator extraction."""
    built: list[ComparatorSourceSeries] = []
    seen: set[str] = set()
    for symbol, instrument_id, source_bars in members:
        if symbol in seen:
            raise ValueError(f"duplicate comparator symbol {symbol}")
        seen.add(symbol)
        if symbol not in COMPARATOR_SYMBOLS:
            raise ValueError(f"undeclared comparator symbol {symbol}")
        if instrument_id <= 0:
            raise ValueError(f"{symbol}: source instrument id must be positive")
        bars = tuple(source_bars)
        if not bars:
            raise ValueError(f"{symbol}: source has no bars")
        previous: date | None = None
        for bar in bars:
            if previous is not None and bar.bar_date <= previous:
                raise ValueError(f"{symbol}: bars are not strictly increasing at {bar.bar_date}")
            previous = bar.bar_date
            if bar.bar_date > FROZEN_FRONTIER:
                raise ValueError(f"{symbol}: bar {bar.bar_date} is beyond frozen frontier {FROZEN_FRONTIER}")
            prices = (bar.open, bar.high, bar.low, bar.close)
            if any(not value.is_finite() or value <= 0 for value in prices):
                raise ValueError(f"{symbol}: non-positive or non-finite OHLC at {bar.bar_date}")
            if bar.high < max(bar.open, bar.close) or bar.low > min(bar.open, bar.close) or bar.high < bar.low:
                raise ValueError(f"{symbol}: invalid OHLC envelope at {bar.bar_date}")
            if bar.volume is not None and (not bar.volume.is_finite() or bar.volume < 0):
                raise ValueError(f"{symbol}: negative or non-finite volume at {bar.bar_date}")
            if bar.volume is not None and bar.volume != bar.volume.to_integral_value():
                # research_price_daily.volume is BIGINT. Refuse before any
                # write rather than let PostgreSQL round a NUMERIC(20,4)
                # source and make the supposedly exact snapshot lossy.
                raise ValueError(f"{symbol}: fractional volume cannot be stored losslessly at {bar.bar_date}")
        if bars[-1].bar_date != FROZEN_FRONTIER:
            raise ValueError(f"{symbol}: latest bar is {bars[-1].bar_date}, expected {FROZEN_FRONTIER}")
        built.append(
            ComparatorSourceSeries(
                symbol=symbol,
                instrument_id=instrument_id,
                bars=bars,
                sha256=fingerprint_series(symbol, instrument_id, bars),
            )
        )
    if seen != set(COMPARATOR_SYMBOLS):
        missing = sorted(set(COMPARATOR_SYMBOLS) - seen)
        raise ValueError(f"comparator extraction is incomplete; missing {missing}")
    built.sort(key=lambda item: item.symbol)
    digest = hashlib.sha256()
    for member in built:
        digest.update(f"{member.symbol}|{member.instrument_id}|{member.sha256}|{len(member.bars)}\n".encode())
    return ComparatorSnapshot(members=tuple(built), sha256=digest.hexdigest())


def load_source_snapshot(conn: psycopg.Connection[Any]) -> ComparatorSnapshot:
    """Read the declared ETF set from the existing eToro execution table."""
    instruments = conn.execute(
        """
        SELECT instrument_id, symbol
        FROM instruments
        WHERE symbol = ANY(%(symbols)s)
          AND instrument_type_id = %(etf_type_id)s
          AND is_tradable = TRUE
        ORDER BY symbol, instrument_id
        """,
        {
            "symbols": list(COMPARATOR_SYMBOLS),
            "etf_type_id": ETORO_ETF_INSTRUMENT_TYPE_ID,
        },
    ).fetchall()
    by_symbol: dict[str, list[int]] = {}
    for instrument_id, symbol in instruments:
        by_symbol.setdefault(str(symbol), []).append(int(instrument_id))
    ambiguous = {symbol: ids for symbol, ids in by_symbol.items() if len(ids) != 1}
    missing = sorted(set(COMPARATOR_SYMBOLS) - set(by_symbol))
    if ambiguous or missing:
        raise RuntimeError(f"comparator source mapping is not exact; missing={missing}, ambiguous={ambiguous}")

    members: list[tuple[str, int, tuple[ComparatorBar, ...]]] = []
    for symbol in COMPARATOR_SYMBOLS:
        instrument_id = by_symbol[symbol][0]
        rows = conn.execute(
            """
            SELECT price_date, open, high, low, close, volume
            FROM price_daily
            WHERE instrument_id = %(instrument_id)s
              AND price_date <= %(frontier)s
            ORDER BY price_date
            """,
            {"instrument_id": instrument_id, "frontier": FROZEN_FRONTIER},
        ).fetchall()
        bars = tuple(
            ComparatorBar(
                bar_date=row[0],
                open=Decimal(row[1]),
                high=Decimal(row[2]),
                low=Decimal(row[3]),
                close=Decimal(row[4]),
                volume=None if row[5] is None else Decimal(row[5]),
            )
            for row in rows
        )
        members.append((symbol, instrument_id, bars))
    return build_snapshot(members)


def load_comparator_closes(
    conn: psycopg.Connection[Any],
    *,
    snapshot_id: str,
    symbol: str,
    through_date: date,
) -> dict[date, Decimal]:
    """Load only immutable closes known on or before ``through_date``."""
    if not snapshot_id.strip() or not symbol.strip():
        raise ValueError("snapshot_id and symbol must be non-empty")
    rows = conn.execute(
        """
        SELECT d.bar_date, d.close
        FROM research_price_daily d
        JOIN research_price_series s USING (series_id)
        WHERE s.comparator_snapshot_id = %(snapshot_id)s
          AND s.vendor_symbol = %(symbol)s
          AND s.instrument_id IS NULL
          AND s.resolution_method IS NULL
          AND d.bar_date <= %(through_date)s
        ORDER BY d.bar_date
        """,
        {"snapshot_id": snapshot_id, "symbol": symbol, "through_date": through_date},
    ).fetchall()
    if not rows:
        raise ComparatorUnavailable(f"{symbol}: snapshot {snapshot_id} has no comparator closes through {through_date}")
    return {row[0]: Decimal(row[1]) for row in rows}


def _snapshot_metadata(snapshot: ComparatorSnapshot) -> tuple[object, ...]:
    return (
        "etoro",
        "price_daily",
        SOURCE_CONTRACT,
        SOURCE_TERMS_URL,
        FROZEN_FRONTIER,
        snapshot.row_count,
        snapshot.sha256,
        SESSION_CALENDAR,
        SOURCE_TIMEZONE,
        OHLC_ADJUSTMENT_BASIS,
        DIVIDEND_ADJUSTMENT_BASIS,
    )


def store_snapshot(conn: psycopg.Connection[Any], snapshot: ComparatorSnapshot) -> int:
    """Write the snapshot once; any later source mutation is a refusal."""
    conn.execute(
        """
        INSERT INTO research_comparator_snapshots
            (snapshot_id, provider, source_relation, source_contract,
             source_terms_url, frozen_frontier, source_row_count, source_sha256,
             session_calendar, source_timezone, ohlc_adjustment_basis,
             dividend_adjustment_basis)
        VALUES (%(snapshot_id)s, %(provider)s, %(source_relation)s,
                %(source_contract)s, %(source_terms_url)s, %(frontier)s,
                %(row_count)s, %(sha256)s, %(calendar)s, %(timezone)s,
                %(ohlc_basis)s, %(dividend_basis)s)
        ON CONFLICT (snapshot_id) DO NOTHING
        """,
        {
            "snapshot_id": SNAPSHOT_ID,
            "provider": "etoro",
            "source_relation": "price_daily",
            "source_contract": SOURCE_CONTRACT,
            "source_terms_url": SOURCE_TERMS_URL,
            "frontier": FROZEN_FRONTIER,
            "row_count": snapshot.row_count,
            "sha256": snapshot.sha256,
            "calendar": SESSION_CALENDAR,
            "timezone": SOURCE_TIMEZONE,
            "ohlc_basis": OHLC_ADJUSTMENT_BASIS,
            "dividend_basis": DIVIDEND_ADJUSTMENT_BASIS,
        },
    )
    stored_meta = conn.execute(
        """
        SELECT provider, source_relation, source_contract, source_terms_url,
               frozen_frontier, source_row_count, source_sha256,
               session_calendar, source_timezone, ohlc_adjustment_basis,
               dividend_adjustment_basis
        FROM research_comparator_snapshots
        WHERE snapshot_id = %s
        """,
        (SNAPSHOT_ID,),
    ).fetchone()
    if stored_meta != _snapshot_metadata(snapshot):
        raise RuntimeError("stored comparator snapshot metadata/fingerprint differs; mint a new snapshot id")

    written = 0
    for member in snapshot.members:
        series_row = conn.execute(
            """
            INSERT INTO research_price_series
                (vendor, vendor_symbol, upstream_source, licence,
                 adjustment_basis, first_bar, last_bar, bar_count,
                 comparator_snapshot_id)
            VALUES (%(vendor)s, %(symbol)s, %(upstream)s, %(licence)s,
                    %(basis)s, %(first_bar)s, %(last_bar)s, %(bar_count)s,
                    %(snapshot_id)s)
            ON CONFLICT (vendor, vendor_symbol) DO NOTHING
            RETURNING series_id
            """,
            {
                "vendor": VENDOR,
                "symbol": member.symbol,
                "upstream": UPSTREAM_SOURCE,
                "licence": LICENCE,
                "basis": OHLC_ADJUSTMENT_BASIS,
                "first_bar": member.first_bar,
                "last_bar": member.last_bar,
                "bar_count": len(member.bars),
                "snapshot_id": SNAPSHOT_ID,
            },
        ).fetchone()
        if series_row is None:
            series_row = conn.execute(
                """
                SELECT series_id FROM research_price_series
                WHERE vendor = %s AND vendor_symbol = %s
                """,
                (VENDOR, member.symbol),
            ).fetchone()
        if series_row is None:
            raise RuntimeError(f"{member.symbol}: series identity did not resolve after insert")
        series_id = int(series_row[0])
        stored_series = conn.execute(
            """
            SELECT upstream_source, licence, adjustment_basis, instrument_id,
                   resolution_method, first_bar, last_bar, bar_count,
                   comparator_snapshot_id
            FROM research_price_series WHERE series_id = %s
            """,
            (series_id,),
        ).fetchone()
        expected_series = (
            UPSTREAM_SOURCE,
            LICENCE,
            OHLC_ADJUSTMENT_BASIS,
            None,
            None,
            member.first_bar,
            member.last_bar,
            len(member.bars),
            SNAPSHOT_ID,
        )
        if stored_series != expected_series:
            raise RuntimeError(f"{member.symbol}: stored series metadata differs from declared snapshot")

        with conn.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO research_price_daily
                    (series_id, bar_date, open, high, low, close, volume, adj_close)
                VALUES (%s,%s,%s,%s,%s,%s,%s,NULL)
                ON CONFLICT (series_id, bar_date) DO NOTHING
                """,
                [
                    (
                        series_id,
                        bar.bar_date,
                        bar.open,
                        bar.high,
                        bar.low,
                        bar.close,
                        None if bar.volume is None else int(bar.volume),
                    )
                    for bar in member.bars
                ],
            )
        stored_rows = conn.execute(
            """
            SELECT bar_date, open, high, low, close, volume
            FROM research_price_daily
            WHERE series_id = %s
            ORDER BY bar_date
            """,
            (series_id,),
        ).fetchall()
        stored_bars = tuple(
            ComparatorBar(
                bar_date=row[0],
                open=Decimal(row[1]),
                high=Decimal(row[2]),
                low=Decimal(row[3]),
                close=Decimal(row[4]),
                volume=None if row[5] is None else Decimal(row[5]),
            )
            for row in stored_rows
        )
        if fingerprint_series(member.symbol, member.instrument_id, stored_bars) != member.sha256:
            raise RuntimeError(f"{member.symbol}: stored bars differ from immutable source fingerprint")

        conn.execute(
            """
            INSERT INTO research_comparator_snapshot_members
                (snapshot_id, vendor_symbol, source_instrument_id, series_id,
                 source_row_count, source_sha256, first_bar, last_bar)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (snapshot_id, vendor_symbol) DO NOTHING
            """,
            (
                SNAPSHOT_ID,
                member.symbol,
                member.instrument_id,
                series_id,
                len(member.bars),
                member.sha256,
                member.first_bar,
                member.last_bar,
            ),
        )
        stored_member = conn.execute(
            """
            SELECT source_instrument_id, series_id, source_row_count,
                   source_sha256, first_bar, last_bar
            FROM research_comparator_snapshot_members
            WHERE snapshot_id = %s AND vendor_symbol = %s
            """,
            (SNAPSHOT_ID, member.symbol),
        ).fetchone()
        expected_member = (
            member.instrument_id,
            series_id,
            len(member.bars),
            member.sha256,
            member.first_bar,
            member.last_bar,
        )
        if stored_member != expected_member:
            raise RuntimeError(f"{member.symbol}: stored member provenance differs from declared snapshot")
        written += len(member.bars)
    return written


def verify_stored_snapshot(conn: psycopg.Connection[Any]) -> ComparatorSnapshot:
    """Reconstruct and validate the immutable snapshot from research tables."""
    members: list[tuple[str, int, tuple[ComparatorBar, ...]]] = []
    rows = conn.execute(
        """
        SELECT m.vendor_symbol, m.source_instrument_id, m.series_id,
               s.vendor, s.vendor_symbol, s.upstream_source, s.licence,
               s.adjustment_basis, s.instrument_id, s.resolution_method,
               s.first_bar, s.last_bar, s.bar_count
        FROM research_comparator_snapshot_members m
        JOIN research_price_series s ON s.series_id = m.series_id
        WHERE m.snapshot_id = %(snapshot_id)s
          AND s.comparator_snapshot_id = %(snapshot_id)s
          AND s.instrument_id IS NULL
          AND s.resolution_method IS NULL
        ORDER BY m.vendor_symbol
        """,
        {"snapshot_id": SNAPSHOT_ID},
    ).fetchall()
    for row in rows:
        symbol, instrument_id, series_id = str(row[0]), int(row[1]), int(row[2])
        stored_series_identity = tuple(row[3:10])
        expected_series_identity = (
            VENDOR,
            symbol,
            UPSTREAM_SOURCE,
            LICENCE,
            OHLC_ADJUSTMENT_BASIS,
            None,
            None,
        )
        if stored_series_identity != expected_series_identity:
            raise RuntimeError(f"{symbol}: stored series identity/provenance has drifted")
        bar_rows = conn.execute(
            """
            SELECT bar_date, open, high, low, close, volume
            FROM research_price_daily
            WHERE series_id = %s AND adj_close IS NULL
            ORDER BY bar_date
            """,
            (series_id,),
        ).fetchall()
        bars = tuple(
            ComparatorBar(
                bar_date=row[0],
                open=Decimal(row[1]),
                high=Decimal(row[2]),
                low=Decimal(row[3]),
                close=Decimal(row[4]),
                volume=None if row[5] is None else Decimal(row[5]),
            )
            for row in bar_rows
        )
        if not bars:
            raise RuntimeError(f"{symbol}: stored comparator series has no bars")
        if (row[10], row[11], row[12]) != (bars[0].bar_date, bars[-1].bar_date, len(bars)):
            raise RuntimeError(f"{symbol}: stored series census has drifted from its bars")
        members.append((symbol, instrument_id, bars))
    snapshot = build_snapshot(members)
    stored_meta = conn.execute(
        """
        SELECT provider, source_relation, source_contract, source_terms_url,
               frozen_frontier, source_row_count, source_sha256,
               session_calendar, source_timezone, ohlc_adjustment_basis,
               dividend_adjustment_basis
        FROM research_comparator_snapshots WHERE snapshot_id = %s
        """,
        (SNAPSHOT_ID,),
    ).fetchone()
    if stored_meta != _snapshot_metadata(snapshot):
        raise RuntimeError("stored snapshot metadata/fingerprint does not match its bars")
    stored_members = {
        str(row[0]): tuple(row[1:])
        for row in conn.execute(
            """
            SELECT vendor_symbol, source_instrument_id, source_row_count,
                   source_sha256, first_bar, last_bar
            FROM research_comparator_snapshot_members
            WHERE snapshot_id = %s
            ORDER BY vendor_symbol
            """,
            (SNAPSHOT_ID,),
        ).fetchall()
    }
    expected_members = {
        member.symbol: (
            member.instrument_id,
            len(member.bars),
            member.sha256,
            member.first_bar,
            member.last_bar,
        )
        for member in snapshot.members
    }
    if stored_members != expected_members:
        raise RuntimeError("stored comparator member census/fingerprint does not match its bars")
    return snapshot


__all__ = [
    "COMPARATOR_SYMBOLS",
    "FROZEN_FRONTIER",
    "SNAPSHOT_ID",
    "ComparatorBar",
    "ComparatorUnavailable",
    "ComparatorSnapshot",
    "OverlapMeasurement",
    "align_exact_sessions",
    "build_snapshot",
    "fingerprint_series",
    "load_comparator_closes",
    "load_source_snapshot",
    "measure_overlap",
    "store_snapshot",
    "verify_stored_snapshot",
]
