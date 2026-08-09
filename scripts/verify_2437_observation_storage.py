"""Size #2437 observation tiers and existing signal detail against the database.

Run:
    PYTHONPATH=. uv run python scripts/verify_2437_observation_storage.py

Read-only. Nothing is written. The output is an acceptance input for the storage
benchmark slice, not permission to create the proposed tables.
"""

from __future__ import annotations

from dataclasses import dataclass

import psycopg

from app.config import settings

TRADING_DAYS = 252
RTH_MINUTES = 390
REFERENCE_BYTES_PER_ROW = 143


@dataclass(frozen=True)
class Tier:
    name: str
    instruments: int
    minutes_per_bar: int
    retained_days: int

    @property
    def rows(self) -> int:
        bars_per_day = RTH_MINUTES // self.minutes_per_bar
        return self.instruments * bars_per_day * self.retained_days


TIERS = (
    Tier("30-minute context", instruments=1_000, minutes_per_bar=30, retained_days=TRADING_DAYS),
    Tier("5-minute setup", instruments=250, minutes_per_bar=5, retained_days=TRADING_DAYS),
    Tier("1-minute execution", instruments=50, minutes_per_bar=1, retained_days=30),
)


def projected_bytes(tier: Tier, *, bytes_per_row: int = REFERENCE_BYTES_PER_ROW) -> int:
    return tier.rows * bytes_per_row


def projected_annual_signal_rows(rows_per_scan: int, *, trading_days: int = TRADING_DAYS) -> int:
    """Annual detail rows if one completed daily scan has ``rows_per_scan`` rows."""
    if rows_per_scan < 0 or trading_days < 0:
        raise ValueError("signal rows and trading days must be non-negative")
    return rows_per_scan * trading_days


def projected_annual_signal_bytes(
    rows_per_scan: int,
    *,
    measured_rows: int,
    measured_total_bytes: int,
    trading_days: int = TRADING_DAYS,
) -> int:
    """Project heap+index growth from this database's measured signal row cost."""
    if measured_rows <= 0:
        raise ValueError("measured_rows must be positive")
    if measured_total_bytes < 0:
        raise ValueError("measured_total_bytes must be non-negative")
    return round(
        projected_annual_signal_rows(rows_per_scan, trading_days=trading_days) * measured_total_bytes / measured_rows
    )


def main() -> None:
    query = """
        SELECT pg_database_size(current_database()),
               (SELECT count(*) FROM instruments WHERE is_tradable),
               (SELECT count(*) FROM price_daily),
               pg_total_relation_size('price_daily'),
               (SELECT count(*) FROM research_price_daily),
               pg_total_relation_size('research_price_daily'),
               (SELECT count(*) FROM strategy_signals),
               pg_relation_size('strategy_signals'),
               pg_indexes_size('strategy_signals'),
               pg_total_relation_size('strategy_signals'),
               coalesce((
                   SELECT row_count
                   FROM job_runs
                   WHERE job_name = 'strategy_signal_scan'
                     AND status = 'success'
                     AND row_count IS NOT NULL
                   ORDER BY started_at DESC
                   LIMIT 1
               ), 0)
    """
    with psycopg.connect(settings.database_url) as conn:
        row = conn.execute(query).fetchone()
    if row is None:
        raise RuntimeError("database sizing query returned no row")

    (
        db_bytes,
        instruments,
        price_rows,
        price_bytes,
        research_rows,
        research_bytes,
        signal_rows,
        signal_heap_bytes,
        signal_index_bytes,
        signal_total_bytes,
        latest_scan_rows,
    ) = map(int, row)
    price_bpr = price_bytes / price_rows
    research_bpr = research_bytes / research_rows

    print(f"database: {db_bytes:,} bytes")
    print(f"tradable instruments: {instruments:,}")
    print(f"price_daily: {price_rows:,} rows, {price_bytes:,} bytes, {price_bpr:.1f} bytes/row")
    print(f"research_price_daily: {research_rows:,} rows, {research_bytes:,} bytes, {research_bpr:.1f} bytes/row")
    print(
        f"strategy_signals: {signal_rows:,} rows, {signal_total_bytes:,} bytes "
        f"(heap {signal_heap_bytes:,}; indexes {signal_index_bytes:,})"
    )
    if signal_rows and latest_scan_rows:
        annual_rows = projected_annual_signal_rows(latest_scan_rows)
        annual_bytes = projected_annual_signal_bytes(
            latest_scan_rows,
            measured_rows=signal_rows,
            measured_total_bytes=signal_total_bytes,
        )
        print(
            f"latest successful signal scan: {latest_scan_rows:,} rows; "
            f"daily-detail projection {annual_rows:,} rows / {annual_bytes / 1_000_000_000:.2f} GB per year"
        )
    else:
        print("latest successful signal scan: no measurable completed row count")
    print(f"\nprojections at conservative {REFERENCE_BYTES_PER_ROW} bytes/row:")
    total = 0
    for tier in TIERS:
        size = projected_bytes(tier)
        total += size
        print(f"  {tier.name:20} {tier.rows:>10,} rows  {size / 1_000_000:>8.1f} MB")
    print(f"  {'price tiers total':20} {'':>10}       {total / 1_000_000:>8.1f} MB")


if __name__ == "__main__":
    main()
