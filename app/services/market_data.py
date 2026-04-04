"""
Market data service.

Ingests daily OHLCV candles and current quotes for covered instruments,
computes rolling return and volatility features, and flags wide spreads.
"""

import logging
import math
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

import psycopg

from app.providers.market_data import MarketDataProvider, OHLCVBar, Quote

logger = logging.getLogger(__name__)

# Default spread threshold from trading-policy.md.
# An instrument is flagged if (ask - bid) / mid > this value.
DEFAULT_MAX_SPREAD_PCT = Decimal("1.0")  # 1%

# Lookback windows in calendar days for rolling return computation.
# These are approximate (trading days vary); exact day counts are resolved
# from the available price history.
_RETURN_WINDOWS: dict[str, int] = {
    "return_1w": 7,
    "return_1m": 30,
    "return_3m": 91,
    "return_6m": 182,
    "return_1y": 365,
}
_VOLATILITY_WINDOW_DAYS = 30


@dataclass(frozen=True)
class MarketRefreshSummary:
    symbols_refreshed: int
    candle_rows_upserted: int
    features_computed: int
    quotes_updated: int
    spread_flags_set: int


def refresh_market_data(
    provider: MarketDataProvider,
    conn: psycopg.Connection,  # type: ignore[type-arg]
    symbols: list[tuple[str, str]],  # [(symbol, instrument_id), ...]
    from_date: date,
    to_date: date,
    max_spread_pct: Decimal = DEFAULT_MAX_SPREAD_PCT,
) -> MarketRefreshSummary:
    """
    For each symbol: fetch candles, upsert to price_daily, compute features,
    fetch quote, upsert to quotes table with spread flag.

    symbols is a list of (symbol, instrument_id) tuples — instrument_id must
    already exist in the instruments table.

    Raw provider responses are persisted by the provider before being returned.
    """
    candle_rows_upserted = 0
    features_computed = 0
    quotes_updated = 0
    spread_flags_set = 0

    for symbol, instrument_id in symbols:
        # Candles
        bars = provider.get_daily_candles(symbol, from_date, to_date)
        if bars:
            upserted = _upsert_candles(conn, instrument_id, bars)
            candle_rows_upserted += upserted
            computed = _compute_and_store_features(conn, instrument_id)
            features_computed += computed

        # Quote
        quote = provider.get_quote(symbol)
        if quote is not None:
            flagged = _upsert_quote(conn, instrument_id, quote, max_spread_pct)
            quotes_updated += 1
            if flagged:
                spread_flags_set += 1

    return MarketRefreshSummary(
        symbols_refreshed=len(symbols),
        candle_rows_upserted=candle_rows_upserted,
        features_computed=features_computed,
        quotes_updated=quotes_updated,
        spread_flags_set=spread_flags_set,
    )


def _upsert_candles(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: str,
    bars: list[OHLCVBar],
) -> int:
    """
    Upsert OHLCV bars into price_daily. Idempotent — re-running with the same
    data produces no changes (ON CONFLICT DO UPDATE with WHERE clause).
    Returns the number of rows written (insert or update).
    """
    written = 0
    with conn.transaction():
        for bar in bars:
            result = conn.execute(
                """
                INSERT INTO price_daily (
                    instrument_id, price_date, open, high, low, close, volume
                )
                VALUES (
                    %(instrument_id)s, %(price_date)s,
                    %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s
                )
                ON CONFLICT (instrument_id, price_date) DO UPDATE SET
                    open   = EXCLUDED.open,
                    high   = EXCLUDED.high,
                    low    = EXCLUDED.low,
                    close  = EXCLUDED.close,
                    volume = EXCLUDED.volume
                WHERE (
                    price_daily.open   IS DISTINCT FROM EXCLUDED.open   OR
                    price_daily.high   IS DISTINCT FROM EXCLUDED.high   OR
                    price_daily.low    IS DISTINCT FROM EXCLUDED.low    OR
                    price_daily.close  IS DISTINCT FROM EXCLUDED.close  OR
                    price_daily.volume IS DISTINCT FROM EXCLUDED.volume
                )
                """,
                {
                    "instrument_id": instrument_id,
                    "price_date": bar.price_date,
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "volume": bar.volume,
                },
            )
            written += result.rowcount
    return written


def _compute_and_store_features(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: str,
) -> int:
    """
    Compute rolling returns and 30-day realised volatility for the most recent
    price_daily row of this instrument, then write back to the same row.

    Returns 1 if the most recent row was updated, 0 if no price data exists.

    Rolling return formula: (close_today / close_n_days_ago) - 1
    Volatility: annualised standard deviation of daily log returns over 30 days.
    """
    # Fetch enough history to compute all windows (up to 1y + buffer)
    rows = conn.execute(
        """
        SELECT price_date, close
        FROM price_daily
        WHERE instrument_id = %(instrument_id)s
          AND close IS NOT NULL
        ORDER BY price_date DESC
        LIMIT 400
        """,
        {"instrument_id": instrument_id},
    ).fetchall()

    if not rows:
        return 0

    # rows are newest-first; reverse to oldest-first for computation
    prices: list[tuple[date, Decimal]] = [(r[0], r[1]) for r in reversed(rows)]
    latest_date, latest_close = prices[-1]

    returns = _compute_rolling_returns(prices)
    volatility = _compute_volatility_30d(prices)

    conn.execute(
        """
        UPDATE price_daily SET
            return_1w     = %(return_1w)s,
            return_1m     = %(return_1m)s,
            return_3m     = %(return_3m)s,
            return_6m     = %(return_6m)s,
            return_1y     = %(return_1y)s,
            volatility_30d = %(volatility_30d)s
        WHERE instrument_id = %(instrument_id)s
          AND price_date = %(price_date)s
        """,
        {
            "instrument_id": instrument_id,
            "price_date": latest_date,
            "return_1w": returns.get("return_1w"),
            "return_1m": returns.get("return_1m"),
            "return_3m": returns.get("return_3m"),
            "return_6m": returns.get("return_6m"),
            "return_1y": returns.get("return_1y"),
            "volatility_30d": volatility,
        },
    )
    return 1


def _compute_rolling_returns(
    prices: list[tuple[date, Decimal]],
) -> dict[str, Decimal | None]:
    """
    Compute rolling returns for each window against the most recent close.

    prices must be sorted oldest-first. Returns a dict of column_name → return
    value (or None if insufficient history for that window).

    Return = (close_latest / close_at_window_start) - 1
    """
    if not prices:
        return {col: None for col in _RETURN_WINDOWS}

    latest_date, latest_close = prices[-1]
    results: dict[str, Decimal | None] = {}

    for col, days in _RETURN_WINDOWS.items():
        target_date = date.fromordinal(latest_date.toordinal() - days)
        # Find the closest available price on or before target_date
        anchor: Decimal | None = None
        for price_date, close in prices[:-1]:  # exclude the latest bar itself
            if price_date <= target_date:
                anchor = close  # keep iterating to find closest to target
            else:
                break
        if anchor is not None and anchor != 0:
            results[col] = (latest_close / anchor) - Decimal("1")
        else:
            results[col] = None

    return results


def _compute_volatility_30d(prices: list[tuple[date, Decimal]]) -> Decimal | None:
    """
    Compute 30-day annualised realised volatility from daily log returns.

    Uses the most recent 31 prices (30 daily returns).
    Returns None if fewer than 5 returns are available (too few to be meaningful).
    """
    if len(prices) < 2:
        return None

    recent = prices[-31:]  # up to 31 prices → up to 30 returns
    log_returns = []
    for i in range(1, len(recent)):
        prev = recent[i - 1][1]
        curr = recent[i][1]
        if prev > 0 and curr > 0:
            log_returns.append(math.log(float(curr) / float(prev)))

    if len(log_returns) < 5:
        return None

    n = len(log_returns)
    mean = sum(log_returns) / n
    variance = sum((r - mean) ** 2 for r in log_returns) / (n - 1)
    daily_std = math.sqrt(variance)
    annualised = daily_std * math.sqrt(252)  # trading days per year

    return Decimal(str(round(annualised, 6)))


def _upsert_quote(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: str,
    quote: Quote,
    max_spread_pct: Decimal,
) -> bool:
    """
    Upsert the current quote into the quotes table.
    Computes spread_pct and sets spread_flag if spread exceeds the threshold.
    Returns True if spread_flag was set (i.e. spread is wide).
    """
    mid = (quote.bid + quote.ask) / 2
    spread_pct = ((quote.ask - quote.bid) / mid * 100) if mid > 0 else None
    spread_flag = spread_pct is not None and spread_pct > max_spread_pct

    conn.execute(
        """
        INSERT INTO quotes (
            instrument_id, quoted_at, bid, ask, last, spread_pct, spread_flag
        )
        VALUES (
            %(instrument_id)s, %(quoted_at)s, %(bid)s, %(ask)s,
            %(last)s, %(spread_pct)s, %(spread_flag)s
        )
        ON CONFLICT (instrument_id) DO UPDATE SET
            quoted_at   = EXCLUDED.quoted_at,
            bid         = EXCLUDED.bid,
            ask         = EXCLUDED.ask,
            last        = EXCLUDED.last,
            spread_pct  = EXCLUDED.spread_pct,
            spread_flag = EXCLUDED.spread_flag
        """,
        {
            "instrument_id": instrument_id,
            "quoted_at": quote.timestamp,
            "bid": quote.bid,
            "ask": quote.ask,
            "last": quote.last,
            "spread_pct": spread_pct,
            "spread_flag": spread_flag,
        },
    )
    return spread_flag


def compute_spread_pct(bid: Decimal, ask: Decimal) -> Decimal | None:
    """Public helper for testing: compute spread % from bid/ask."""
    mid = (bid + ask) / 2
    if mid <= 0:
        return None
    return (ask - bid) / mid * 100
