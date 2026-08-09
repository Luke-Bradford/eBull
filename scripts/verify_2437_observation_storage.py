"""Size #2437 observation tiers and verify #2448's bounded schema.

Run:
    PYTHONPATH=. uv run python scripts/verify_2437_observation_storage.py
    PYTHONPATH=. uv run python scripts/verify_2437_observation_storage.py --benchmark

The default census is read-only. ``--benchmark`` creates transaction-local
temporary tables, measures inserts/indexes/query plans, and rolls them back;
nothing persists after the connection closes.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from math import ceil
from time import perf_counter
from typing import Any, LiteralString, cast

import psycopg

from app.config import settings
from app.services.strategy_observation_storage import SIGNAL_DETAIL_RETENTION_DAYS

TRADING_DAYS = 252
RTH_MINUTES = 390
REFERENCE_BYTES_PER_ROW = 143
ANNUAL_OBSERVATION_GROWTH_BUDGET_BYTES = 1_500_000_000


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
    Tier("30-minute context", instruments=1_000, minutes_per_bar=30, retained_days=TRADING_DAYS * 2),
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


def measured_bytes_per_row(rows: int, total_bytes: int) -> float | None:
    """Return measured relation density, or ``None`` for an empty relation."""
    if rows < 0 or total_bytes < 0:
        raise ValueError("measured rows and bytes must be non-negative")
    return total_bytes / rows if rows else None


def _scalar(conn: psycopg.Connection[Any], query: str) -> int:
    row = conn.execute(cast(LiteralString, query)).fetchone()
    if row is None:
        raise RuntimeError("measurement query returned no row")
    return int(row[0])


def _benchmark_candidate_tables(conn: psycopg.Connection[Any], *, sample_rows: int = 200_000) -> None:
    """Measure the deployed row shapes without leaving a persistent relation."""
    conn.execute("CREATE TEMP TABLE bench_fired_signals (LIKE strategy_signals INCLUDING ALL)")
    started = perf_counter()
    conn.execute("INSERT INTO bench_fired_signals SELECT * FROM strategy_signals WHERE verdict = 'fired'")
    fired_insert_s = perf_counter() - started
    fired_rows = _scalar(conn, "SELECT count(*) FROM bench_fired_signals")
    fired_bytes = _scalar(conn, "SELECT pg_total_relation_size('bench_fired_signals')")

    conn.execute("CREATE TEMP TABLE bench_signal_detail (LIKE strategy_signal_observations INCLUDING ALL)")
    started = perf_counter()
    conn.execute(
        """
        INSERT INTO bench_signal_detail (
            strategy_id, strategy_version, instrument_id, signal_bar_date,
            signal_kind, verdict, reason_code, created_at
        )
        SELECT strategy_id, strategy_version, instrument_id, signal_bar_date,
               signal_kind, verdict, reason_code, created_at
        FROM strategy_signal_observations
        """
    )
    detail_insert_s = perf_counter() - started
    detail_rows = _scalar(conn, "SELECT count(*) FROM bench_signal_detail")
    detail_bytes = _scalar(conn, "SELECT pg_total_relation_size('bench_signal_detail')")

    conn.execute("CREATE TEMP TABLE bench_daily_counts (LIKE strategy_signal_daily_counts INCLUDING ALL)")
    started = perf_counter()
    conn.execute("INSERT INTO bench_daily_counts SELECT * FROM strategy_signal_daily_counts")
    count_insert_s = perf_counter() - started
    count_rows = _scalar(conn, "SELECT count(*) FROM bench_daily_counts")
    count_bytes = _scalar(conn, "SELECT pg_total_relation_size('bench_daily_counts')")

    conn.execute("CREATE TEMP TABLE bench_intraday (LIKE strategy_intraday_bars INCLUDING ALL)")
    started = perf_counter()
    conn.execute(
        """
        INSERT INTO bench_intraday (
            timeframe, bar_time, instrument_id, open, high, low, close, volume, source, captured_at
        )
        SELECT '5m',
               timestamptz '2024-01-01 00:00:00+00' + row_number() OVER () * interval '5 minutes',
               s.instrument_id, d.open, d.high, d.low, d.close, d.volume, 'research-sample', now()
        FROM research_price_daily d
        JOIN research_price_series s ON s.series_id = d.series_id
        WHERE s.instrument_id IS NOT NULL
          AND d.open > 0 AND d.high > 0 AND d.low > 0 AND d.close > 0
          AND d.high >= GREATEST(d.open, d.close, d.low)
          AND d.low <= LEAST(d.open, d.close, d.high)
        LIMIT %(sample_rows)s
        """,
        {"sample_rows": sample_rows},
    )
    intraday_insert_s = perf_counter() - started
    intraday_rows = _scalar(conn, "SELECT count(*) FROM bench_intraday")
    intraday_heap_bytes = _scalar(conn, "SELECT pg_relation_size('bench_intraday')")
    intraday_index_bytes = _scalar(conn, "SELECT pg_indexes_size('bench_intraday')")
    intraday_bytes = _scalar(conn, "SELECT pg_total_relation_size('bench_intraday')")

    intraday_bpr = measured_bytes_per_row(intraday_rows, intraday_bytes)
    print("\ntemporary-table benchmark (heap + indexes, rolled back):")
    print(
        f"  durable fired signals:  {fired_rows:,} rows / {fired_bytes:,} bytes "
        f"({measured_bytes_per_row(fired_rows, fired_bytes):.1f} bytes/row; {fired_insert_s:.3f}s)"
        if fired_rows
        else "  durable fired signals: no source rows"
    )
    print(
        f"  retained signal detail: {detail_rows:,} rows / {detail_bytes:,} bytes "
        f"({measured_bytes_per_row(detail_rows, detail_bytes):.1f} bytes/row; {detail_insert_s:.3f}s)"
        if detail_rows
        else "  retained signal detail: no source rows"
    )
    print(
        f"  durable daily counts:   {count_rows:,} rows / {count_bytes:,} bytes "
        f"({measured_bytes_per_row(count_rows, count_bytes):.1f} bytes/row; {count_insert_s:.3f}s)"
        if count_rows
        else "  durable daily counts: no source rows"
    )
    print(
        f"  intraday candidate:     {intraday_rows:,} rows / {intraday_bytes:,} bytes "
        f"(heap {intraday_heap_bytes:,}; indexes {intraday_index_bytes:,}; "
        f"{intraday_bpr:.1f} bytes/row; {intraday_insert_s:.3f}s)"
        if intraday_bpr is not None
        else "  intraday candidate: no sample rows"
    )
    if intraday_bpr is not None:
        # Round upward: a storage acceptance check must never gain headroom
        # from truncating the measured relation density.
        projected = sum(projected_bytes(tier, bytes_per_row=ceil(intraday_bpr)) for tier in TIERS)
        print(f"  capped intraday steady state: {sum(tier.rows for tier in TIERS):,} rows / {projected / 1e9:.3f} GB")
        if projected > ANNUAL_OBSERVATION_GROWTH_BUDGET_BYTES:
            raise RuntimeError(
                f"intraday caps project {projected:,} bytes, above "
                f"{ANNUAL_OBSERVATION_GROWTH_BUDGET_BYTES:,}-byte budget"
            )
    if fired_rows:
        fired_bpr = fired_bytes / fired_rows
        daily_fired = _scalar(
            conn,
            """
            SELECT COALESCE(MAX(row_count), 0)
            FROM (
                SELECT (created_at AT TIME ZONE 'UTC')::date, COUNT(*) AS row_count
                FROM strategy_signals
                WHERE verdict = 'fired'
                GROUP BY 1
            ) AS daily
            """,
        )
        annual_fired = ceil(daily_fired * TRADING_DAYS * fired_bpr)
        print(
            f"  durable fired-signal growth: {annual_fired / 1e9:.3f} GB/year "
            f"at measured peak {daily_fired:,} fired rows/day"
        )
        if annual_fired > ANNUAL_OBSERVATION_GROWTH_BUDGET_BYTES:
            raise RuntimeError(
                f"fired-signal growth projects {annual_fired:,} bytes/year, above "
                f"{ANNUAL_OBSERVATION_GROWTH_BUDGET_BYTES:,}-byte budget"
            )

    plan = conn.execute(
        """
        EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
        SELECT timeframe, bar_time, open, high, low, close, volume
        FROM bench_intraday
        WHERE instrument_id = 1
          AND timeframe = '5m'
          AND bar_time >= timestamptz '2024-01-01 00:00:00+00'
        ORDER BY bar_time DESC
        LIMIT 500
        """
    ).fetchone()
    if plan is not None:
        payload = plan[0][0]
        print(f"  representative intraday read: {payload['Execution Time']:.3f} ms")


def _verify_signal_parity_and_dependencies(conn: psycopg.Connection[Any]) -> None:
    parity_query = f"""
        WITH detail AS (
            SELECT strategy_id, strategy_version, signal_bar_date, signal_kind,
                   verdict, COALESCE(not_evaluable_reason, '') AS reason_code, COUNT(*) AS row_count
            FROM strategy_signals
            GROUP BY 1, 2, 3, 4, 5, 6
            UNION ALL
            SELECT strategy_id, strategy_version, signal_bar_date, signal_kind,
                   verdict, reason_code, COUNT(*) AS row_count
            FROM strategy_signal_observations
            WHERE signal_bar_date >= CURRENT_DATE - {SIGNAL_DETAIL_RETENTION_DAYS}
            GROUP BY 1, 2, 3, 4, 5, 6
        ), combined AS (
            SELECT strategy_id, strategy_version, signal_bar_date, signal_kind,
                   verdict, reason_code, SUM(row_count) AS row_count
            FROM detail
            GROUP BY 1, 2, 3, 4, 5, 6
        ), delta AS (
            (SELECT strategy_id, strategy_version, signal_bar_date, signal_kind,
                    verdict, reason_code, row_count
             FROM strategy_signal_daily_counts
             WHERE verdict = 'fired'
                OR signal_bar_date >= CURRENT_DATE - {SIGNAL_DETAIL_RETENTION_DAYS}
             EXCEPT ALL
             SELECT * FROM combined)
            UNION ALL
            (SELECT * FROM combined
             EXCEPT ALL
             SELECT strategy_id, strategy_version, signal_bar_date, signal_kind,
                    verdict, reason_code, row_count
             FROM strategy_signal_daily_counts
             WHERE verdict = 'fired'
                OR signal_bar_date >= CURRENT_DATE - {SIGNAL_DETAIL_RETENTION_DAYS})
        )
        SELECT COUNT(*) FROM delta
    """
    mismatches = _scalar(
        conn,
        parity_query,
    )
    dependencies = conn.execute(
        """
        SELECT conrelid::regclass::text, conname
        FROM pg_constraint
        WHERE confrelid = 'strategy_signals'::regclass
        ORDER BY 1, 2
        """
    ).fetchall()
    print(f"signal aggregate/detail parity mismatches (all fired + 90d routine): {mismatches}")
    print(f"relations protecting fired signal ids: {[(str(row[0]), str(row[1])) for row in dependencies]}")
    if mismatches:
        raise RuntimeError("strategy signal aggregate/detail census is not equal; retention must not run")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark", action="store_true", help="run rollback-only temporary-table benchmark")
    args = parser.parse_args()
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
        _verify_signal_parity_and_dependencies(conn)
        if args.benchmark:
            _benchmark_candidate_tables(conn)
            conn.rollback()
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
    price_bpr = measured_bytes_per_row(price_rows, price_bytes)
    research_bpr = measured_bytes_per_row(research_rows, research_bytes)
    price_bpr_text = f"{price_bpr:.1f}" if price_bpr is not None else "n/a"
    research_bpr_text = f"{research_bpr:.1f}" if research_bpr is not None else "n/a"

    print(f"database: {db_bytes:,} bytes")
    print(f"tradable instruments: {instruments:,}")
    print(f"price_daily: {price_rows:,} rows, {price_bytes:,} bytes, {price_bpr_text} bytes/row")
    print(f"research_price_daily: {research_rows:,} rows, {research_bytes:,} bytes, {research_bpr_text} bytes/row")
    print(
        f"strategy_signals: {signal_rows:,} rows, {signal_total_bytes:,} bytes "
        f"(heap {signal_heap_bytes:,}; indexes {signal_index_bytes:,})"
    )
    if signal_rows and latest_scan_rows and signal_rows == latest_scan_rows:
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
        print(
            f"latest successful signal scan: {latest_scan_rows:,} logical rows; "
            "durable relation is now fired-only, so its post-move physical size is not a daily-detail projection"
        )
    print(f"\nprojections at conservative {REFERENCE_BYTES_PER_ROW} bytes/row:")
    total = 0
    for tier in TIERS:
        size = projected_bytes(tier)
        total += size
        print(f"  {tier.name:20} {tier.rows:>10,} rows  {size / 1_000_000:>8.1f} MB")
    print(f"  {'price tiers total':20} {'':>10}       {total / 1_000_000:>8.1f} MB")


if __name__ == "__main__":
    main()
