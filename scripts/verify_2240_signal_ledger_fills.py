"""Full-population check of the phase-3c fill arithmetic (#2240).

Runs ``resolve_fills`` over EVERY instrument series in ``price_daily`` with a
signal on every bar, and compares each resolved fill against Postgres'
``lead(price_date)`` / ``lead(open)`` over the same rows.

⚠ The two derivations are independent, which is the point. The Python side
walks a ``BarSeries`` by index; the SQL side never sees an index. A shared
off-by-one would have to occur in both to go unnoticed.

⚠ NOTHING IS WRITTEN. This reads ``price_daily`` and prints. The signal rows
are resolved in memory and discarded.

    PYTHONPATH=. uv run python scripts/verify_2240_signal_ledger_fills.py

Gate on the exit code: 0 = every fill matched, 1 = mismatches (printed).
"""

from __future__ import annotations

import sys
from datetime import date
from decimal import Decimal

import psycopg

from app.config import settings
from app.services.indicator_series import BarSeries
from app.services.signal_ledger import resolve_fills
from app.services.strategy_registry import StrategyIdentity, StrategySignal
from app.services.technical_analysis import OHLCVRow

_IDENTITY = StrategyIdentity(
    strategy_id="S-VERIFY",
    params={},
    universe="survivor_only",
    cost_model_id="static-v1",
    source_hash="full-population-check",
)

_SQL = """
SELECT instrument_id, price_date, open, high, low, close, volume,
       lead(price_date) OVER w AS next_date,
       lead(open)       OVER w AS next_open
FROM price_daily
WINDOW w AS (PARTITION BY instrument_id ORDER BY price_date)
ORDER BY instrument_id, price_date
"""


def _check(
    instrument_id: int,
    dates: list[date],
    bars: list[OHLCVRow],
    expected: list[tuple[date | None, Decimal | None]],
) -> list[str]:
    series = BarSeries(dates=tuple(dates), rows=tuple(bars))
    rows = resolve_fills(
        [StrategySignal(verdict="fired", signal_index=i) for i in range(len(series))],
        series=series,
        identity=_IDENTITY,
        instrument_id=instrument_id,
    )
    problems: list[str] = []
    for i, row in enumerate(rows):
        next_date, next_open = expected[i]
        if next_date is None or next_open is None:
            # No t+1 in the table, or it has no open price: the writer must refuse.
            if row.verdict != "not_evaluable" or row.not_evaluable_reason != "no_fill_bar":
                problems.append(
                    f"{instrument_id} {dates[i]}: expected no_fill_bar, got {row.verdict}/{row.not_evaluable_reason}"
                )
            continue
        if row.fill_bar_date != next_date or row.fill_price != next_open:
            problems.append(
                f"{instrument_id} {dates[i]}: fill {row.fill_bar_date}/{row.fill_price} != lead {next_date}/{next_open}"
            )
    return problems


def main() -> int:
    bars_seen = 0
    instruments = 0
    refusals = 0
    problems: list[str] = []

    with psycopg.connect(settings.database_url) as conn, conn.cursor(name="pd_scan") as cur:
        cur.itersize = 50_000
        cur.execute(_SQL)
        current: int | None = None
        dates: list[date] = []
        bars: list[OHLCVRow] = []
        expected: list[tuple[date | None, Decimal | None]] = []
        for row in cur:
            instrument_id = row[0]
            if instrument_id != current:
                if current is not None:
                    problems.extend(_check(current, dates, bars, expected))
                    refusals += sum(1 for d, o in expected if d is None or o is None)
                    instruments += 1
                    if instruments % 1000 == 0:
                        print(f"  {instruments} instruments, {bars_seen} bars, {len(problems)} mismatches", flush=True)
                current, dates, bars, expected = instrument_id, [], [], []
            dates.append(row[1])
            bars.append({"open": row[2], "high": row[3], "low": row[4], "close": row[5], "volume": row[6]})
            expected.append((row[7], row[8]))
            bars_seen += 1
        if current is not None:
            problems.extend(_check(current, dates, bars, expected))
            refusals += sum(1 for d, o in expected if d is None or o is None)
            instruments += 1

    print(f"instruments   {instruments}")
    print(f"bars          {bars_seen}")
    print(f"no_fill_bar   {refusals}")
    print(f"mismatches    {len(problems)}")
    for problem in problems[:20]:
        print("  ", problem)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
