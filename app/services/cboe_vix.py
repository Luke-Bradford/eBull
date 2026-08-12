"""Bounded official Cboe VIX daily-close context (#2574).

The primary source is Cboe's public VIX history CSV, not a tradable eToro
instrument and not FRED's key-gated derivative.  Only 2021 onward is retained
in the existing research-series store.  Raw history is copyrighted Cboe data:
it is internal decision context with source attribution, never a UI download.
"""

from __future__ import annotations

import csv
import hashlib
import io
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
from typing import Any, Final
from zoneinfo import ZoneInfo

import httpx
import psycopg

from app.services.watermarks import get_watermark, set_watermark

VIX_HISTORY_URL: Final = "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv"
VIX_SOURCE_PAGE: Final = "https://www.cboe.com/tradable_products/vix/vix_historical_data"
RETENTION_START: Final = date(2021, 1, 1)
VENDOR: Final = "cboe"
VENDOR_SYMBOL: Final = "VIX"
WATERMARK_SOURCE: Final = "cboe.vix-history"
WATERMARK_KEY: Final = "VIX"
SOURCE_VERSION: Final = "cboe-vix-daily-close-v1"
LICENCE: Final = "copyrighted/cboe-internal-research-citation-required"
_EXPECTED_HEADER: Final = ("DATE", "OPEN", "HIGH", "LOW", "CLOSE")
_NEW_YORK: Final = ZoneInfo("America/New_York")


class VixSourceError(ValueError):
    """The source response cannot satisfy the frozen VIX contract."""


@dataclass(frozen=True)
class VixBar:
    bar_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal


@dataclass(frozen=True)
class VixRefreshReport:
    status: str
    retained_bars: int
    first_bar: date | None
    last_bar: date | None
    response_hash: str | None


def _positive_decimal(raw: str, *, field: str, row_number: int) -> Decimal:
    try:
        value = Decimal(raw.strip())
    except (InvalidOperation, ValueError) as exc:
        raise VixSourceError(f"row {row_number}: {field} is not decimal") from exc
    if not value.is_finite() or value <= 0:
        raise VixSourceError(f"row {row_number}: {field} must be finite and positive")
    return value


def parse_vix_history(text: str) -> tuple[VixBar, ...]:
    """Parse the exact Cboe schema and return the bounded ascending series."""
    reader = csv.DictReader(io.StringIO(text))
    if tuple(reader.fieldnames or ()) != _EXPECTED_HEADER:
        raise VixSourceError(f"unexpected header {reader.fieldnames!r}; expected {_EXPECTED_HEADER!r}")

    by_date: dict[date, VixBar] = {}
    for row_number, row in enumerate(reader, start=2):
        if None in row or any(row.get(field) is None for field in _EXPECTED_HEADER):
            raise VixSourceError(f"row {row_number}: ragged source row")
        try:
            bar_date = datetime.strptime(str(row["DATE"]).strip(), "%m/%d/%Y").date()
        except ValueError as exc:
            raise VixSourceError(f"row {row_number}: DATE is not MM/DD/YYYY") from exc
        # Cboe's 1990s history contains published OHLC anomalies (for example
        # 1992-02-11 has OPEN > HIGH).  Those rows are deliberately outside the
        # frozen 2021+ contract: do not let irrelevant legacy data widen or
        # prevent the bounded load.  Retained rows still receive every check.
        if bar_date < RETENTION_START:
            continue
        open_ = _positive_decimal(str(row["OPEN"]), field="OPEN", row_number=row_number)
        high = _positive_decimal(str(row["HIGH"]), field="HIGH", row_number=row_number)
        low = _positive_decimal(str(row["LOW"]), field="LOW", row_number=row_number)
        close = _positive_decimal(str(row["CLOSE"]), field="CLOSE", row_number=row_number)
        if high < max(open_, close) or low > min(open_, close) or low > high:
            raise VixSourceError(f"row {row_number}: OHLC envelope is impossible")
        if bar_date in by_date:
            raise VixSourceError(f"row {row_number}: duplicate DATE {bar_date.isoformat()}")
        by_date[bar_date] = VixBar(bar_date, open_, high, low, close)

    if not by_date:
        raise VixSourceError(f"source has no rows on or after {RETENTION_START.isoformat()}")
    return tuple(by_date[key] for key in sorted(by_date))


def resolve_vix_close_as_known(bars: tuple[VixBar, ...], *, decision_at: datetime) -> VixBar | None:
    """Latest close conservatively knowable at a decision timestamp.

    Historical source rows have a close date but no publication timestamp.
    Consequently a close from New York date D becomes usable only on a later
    New York date.  This forbids same-close lookahead even for an after-close
    decision and is conservative relative to the scheduled fetch.
    """
    if decision_at.tzinfo is None or decision_at.utcoffset() is None:
        raise ValueError("decision_at must be timezone-aware")
    decision_date = decision_at.astimezone(_NEW_YORK).date()
    eligible = [bar for bar in bars if bar.bar_date < decision_date]
    return eligible[-1] if eligible else None


def load_vix_close_as_known(conn: psycopg.Connection[Any], *, decision_at: datetime) -> VixBar | None:
    """Load the latest causally available retained VIX close."""
    if decision_at.tzinfo is None or decision_at.utcoffset() is None:
        raise ValueError("decision_at must be timezone-aware")
    decision_date = decision_at.astimezone(_NEW_YORK).date()
    row = conn.execute(
        """
        SELECT d.bar_date, d.open, d.high, d.low, d.close
        FROM research_price_series s
        JOIN research_price_daily d USING (series_id)
        WHERE s.vendor = %(vendor)s
          AND s.vendor_symbol = %(symbol)s
          AND d.bar_date < %(decision_date)s
        ORDER BY d.bar_date DESC
        LIMIT 1
        """,
        {"vendor": VENDOR, "symbol": VENDOR_SYMBOL, "decision_date": decision_date},
    ).fetchone()
    return None if row is None else VixBar(row[0], row[1], row[2], row[3], row[4])


def _last_modified(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
    except TypeError, ValueError, OverflowError:
        return None
    return parsed if parsed.tzinfo is not None else None


def refresh_cboe_vix(conn: psycopg.Connection[Any], *, client: httpx.Client) -> VixRefreshReport:
    """Fetch once, atomically reconcile the bounded series and watermark."""
    if not conn.autocommit:
        raise RuntimeError("refresh_cboe_vix requires an autocommit connection")

    prior = get_watermark(conn, WATERMARK_SOURCE, WATERMARK_KEY)
    headers: dict[str, str] = {}
    if prior is not None and prior.watermark_at is not None and prior.watermark:
        headers["If-Modified-Since"] = prior.watermark
    response = client.get(VIX_HISTORY_URL, headers=headers)
    if response.status_code == 304:
        if prior is None:
            raise VixSourceError("Cboe returned 304 without a prior watermark")
        with conn.transaction():
            set_watermark(
                conn,
                source=WATERMARK_SOURCE,
                key=WATERMARK_KEY,
                watermark=prior.watermark,
                watermark_at=prior.watermark_at,
                response_hash=prior.response_hash,
            )
        return VixRefreshReport("not_modified", 0, None, None, prior.response_hash)
    response.raise_for_status()

    body_hash = hashlib.sha256(response.content).hexdigest()
    bars = parse_vix_history(response.text)
    modified_literal = response.headers.get("Last-Modified") or bars[-1].bar_date.isoformat()
    modified_at = _last_modified(response.headers.get("Last-Modified"))

    with conn.transaction():
        series_row = conn.execute(
            """
            INSERT INTO research_price_series
                (vendor, vendor_symbol, upstream_source, licence, adjustment_basis,
                 first_bar, last_bar, bar_count, created_at, updated_at)
            VALUES (%(vendor)s, %(symbol)s, 'other', %(licence)s, 'unadjusted',
                    %(first)s, %(last)s, %(count)s, now(), now())
            ON CONFLICT (vendor, vendor_symbol) DO UPDATE
               SET upstream_source = EXCLUDED.upstream_source,
                   licence = EXCLUDED.licence,
                   adjustment_basis = EXCLUDED.adjustment_basis,
                   first_bar = EXCLUDED.first_bar,
                   last_bar = EXCLUDED.last_bar,
                   bar_count = EXCLUDED.bar_count,
                   updated_at = now()
            RETURNING series_id
            """,
            {
                "vendor": VENDOR,
                "symbol": VENDOR_SYMBOL,
                "licence": LICENCE,
                "first": bars[0].bar_date,
                "last": bars[-1].bar_date,
                "count": len(bars),
            },
        ).fetchone()
        if series_row is None:
            raise RuntimeError("Cboe VIX series upsert returned no series_id")
        series_id = int(series_row[0])
        dates = [bar.bar_date for bar in bars]
        conn.execute(
            """
            DELETE FROM research_price_daily
            WHERE series_id = %(series_id)s
              AND bar_date >= %(retention_start)s
              AND NOT (bar_date = ANY(%(dates)s))
            """,
            {"series_id": series_id, "retention_start": RETENTION_START, "dates": dates},
        )
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO research_price_daily
                    (series_id, bar_date, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, NULL)
                ON CONFLICT (series_id, bar_date) DO UPDATE
                   SET open = EXCLUDED.open, high = EXCLUDED.high,
                       low = EXCLUDED.low, close = EXCLUDED.close
                 WHERE (research_price_daily.open, research_price_daily.high,
                        research_price_daily.low, research_price_daily.close)
                       IS DISTINCT FROM
                       (EXCLUDED.open, EXCLUDED.high, EXCLUDED.low, EXCLUDED.close)
                """,
                [(series_id, bar.bar_date, bar.open, bar.high, bar.low, bar.close) for bar in bars],
            )
        set_watermark(
            conn,
            source=WATERMARK_SOURCE,
            key=WATERMARK_KEY,
            watermark=modified_literal,
            watermark_at=modified_at,
            response_hash=body_hash,
        )
    return VixRefreshReport("refreshed", len(bars), bars[0].bar_date, bars[-1].bar_date, body_hash)


__all__ = [
    "LICENCE",
    "RETENTION_START",
    "SOURCE_VERSION",
    "VIX_HISTORY_URL",
    "VIX_SOURCE_PAGE",
    "VixBar",
    "VixRefreshReport",
    "VixSourceError",
    "load_vix_close_as_known",
    "parse_vix_history",
    "refresh_cboe_vix",
    "resolve_vix_close_as_known",
]
