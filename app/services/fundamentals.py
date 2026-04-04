"""
Fundamentals service.

Fetches and upserts normalised fundamentals snapshots from FMP.
The service layer owns identifier resolution and DB writes.
The provider is a pure HTTP client.
"""

import logging
from dataclasses import dataclass
from datetime import date

import psycopg

from app.providers.fundamentals import FundamentalsProvider, FundamentalsSnapshot

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FundamentalsRefreshSummary:
    symbols_attempted: int
    snapshots_upserted: int
    symbols_skipped: int  # no FMP coverage or identifier missing


def refresh_fundamentals(
    provider: FundamentalsProvider,
    conn: psycopg.Connection,  # type: ignore[type-arg]
    symbols: list[tuple[str, str]],  # [(symbol, instrument_id), ...]
) -> FundamentalsRefreshSummary:
    """
    For each symbol, fetch the latest fundamentals snapshot and upsert it.

    symbols is a list of (symbol, instrument_id) tuples. FMP uses the ticker
    symbol as its primary identifier, so no external_identifiers lookup is
    needed for FMP in v1. If the provider returns None for a symbol, that
    symbol is skipped and counted.
    """
    upserted = 0
    skipped = 0

    for symbol, instrument_id in symbols:
        try:
            snap = provider.get_latest_snapshot(symbol)
            if snap is None:
                logger.info("Fundamentals: no data from provider for %s, skipping", symbol)
                skipped += 1
                continue
            _upsert_snapshot(conn, instrument_id, snap)
            upserted += 1
        except Exception:
            logger.warning("Fundamentals: failed to refresh %s, skipping", symbol, exc_info=True)
            skipped += 1

    return FundamentalsRefreshSummary(
        symbols_attempted=len(symbols),
        snapshots_upserted=upserted,
        symbols_skipped=skipped,
    )


def refresh_fundamentals_history(
    provider: FundamentalsProvider,
    conn: psycopg.Connection,  # type: ignore[type-arg]
    symbols: list[tuple[str, str]],
    from_date: date,
    to_date: date,
    limit: int = 40,
) -> FundamentalsRefreshSummary:
    """
    Backfill historical fundamentals snapshots for each symbol.

    Each snapshot is upserted idempotently. Useful for initial population
    and for catching up after provider outages.
    """
    upserted = 0
    skipped = 0

    for symbol, instrument_id in symbols:
        try:
            snaps = provider.get_snapshot_history(symbol, from_date, to_date, limit=limit)
            if not snaps:
                logger.info("Fundamentals history: no data for %s in range, skipping", symbol)
                skipped += 1
                continue
            for snap in snaps:
                _upsert_snapshot(conn, instrument_id, snap)
                upserted += 1
        except Exception:
            logger.warning("Fundamentals history: failed to refresh %s, skipping", symbol, exc_info=True)
            skipped += 1

    return FundamentalsRefreshSummary(
        symbols_attempted=len(symbols),
        snapshots_upserted=upserted,
        symbols_skipped=skipped,
    )


def _upsert_snapshot(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: str,
    snap: FundamentalsSnapshot,
) -> None:
    """
    Upsert a single fundamentals snapshot into fundamentals_snapshot.
    Idempotent — keyed on (instrument_id, as_of_date).
    """
    conn.execute(
        """
        INSERT INTO fundamentals_snapshot (
            instrument_id, as_of_date,
            revenue_ttm, gross_margin, operating_margin,
            fcf, cash, debt, net_debt,
            shares_outstanding, book_value, eps
        )
        VALUES (
            %(instrument_id)s, %(as_of_date)s,
            %(revenue_ttm)s, %(gross_margin)s, %(operating_margin)s,
            %(fcf)s, %(cash)s, %(debt)s, %(net_debt)s,
            %(shares_outstanding)s, %(book_value)s, %(eps)s
        )
        ON CONFLICT (instrument_id, as_of_date) DO UPDATE SET
            revenue_ttm       = EXCLUDED.revenue_ttm,
            gross_margin      = EXCLUDED.gross_margin,
            operating_margin  = EXCLUDED.operating_margin,
            fcf               = EXCLUDED.fcf,
            cash              = EXCLUDED.cash,
            debt              = EXCLUDED.debt,
            net_debt          = EXCLUDED.net_debt,
            shares_outstanding = EXCLUDED.shares_outstanding,
            book_value        = EXCLUDED.book_value,
            eps               = EXCLUDED.eps
        WHERE (
            fundamentals_snapshot.revenue_ttm      IS DISTINCT FROM EXCLUDED.revenue_ttm      OR
            fundamentals_snapshot.gross_margin     IS DISTINCT FROM EXCLUDED.gross_margin     OR
            fundamentals_snapshot.operating_margin IS DISTINCT FROM EXCLUDED.operating_margin OR
            fundamentals_snapshot.fcf              IS DISTINCT FROM EXCLUDED.fcf              OR
            fundamentals_snapshot.cash             IS DISTINCT FROM EXCLUDED.cash             OR
            fundamentals_snapshot.debt             IS DISTINCT FROM EXCLUDED.debt             OR
            fundamentals_snapshot.net_debt         IS DISTINCT FROM EXCLUDED.net_debt         OR
            fundamentals_snapshot.shares_outstanding IS DISTINCT FROM EXCLUDED.shares_outstanding OR
            fundamentals_snapshot.book_value       IS DISTINCT FROM EXCLUDED.book_value       OR
            fundamentals_snapshot.eps              IS DISTINCT FROM EXCLUDED.eps
        )
        """,
        {
            "instrument_id": instrument_id,
            "as_of_date": snap.as_of_date,
            "revenue_ttm": snap.revenue_ttm,
            "gross_margin": snap.gross_margin,
            "operating_margin": snap.operating_margin,
            "fcf": snap.fcf,
            "cash": snap.cash,
            "debt": snap.debt,
            "net_debt": snap.net_debt,
            "shares_outstanding": snap.shares_outstanding,
            "book_value": snap.book_value,
            "eps": snap.eps,
        },
    )
