"""Verify the recent eToro snapshot against the independent legacy mirror.

The raw mirror is intentionally gitignored and must never be redistributed.
This command emits only aggregate compatibility evidence.
"""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import psycopg

from app.config import settings
from app.services.research_comparator_snapshot import (
    COMPARATOR_SYMBOLS,
    SNAPSHOT_ID,
    measure_overlap,
)

DEFAULT_MIRROR_ROOT = Path("var/research_corpus/mirrors/icyDenev_Intrader/Data/Day")


def _legacy_closes(path: Path) -> dict[date, Decimal]:
    closes: dict[date, Decimal] = {}
    with path.open(newline="") as handle:
        for row in csv.reader(handle):
            if len(row) < 5:
                continue
            try:
                bar_date = datetime.strptime(row[0], "%Y-%m-%d").date()
                close = Decimal(row[4])
            except InvalidOperation, ValueError:
                continue
            if close.is_finite() and close > 0:
                closes[bar_date] = close
    return closes


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mirror-root", type=Path, default=DEFAULT_MIRROR_ROOT)
    args = parser.parse_args(argv)

    with psycopg.connect(settings.database_url) as conn:
        for symbol in COMPARATOR_SYMBOLS:
            recent = dict(
                conn.execute(
                    """
                    SELECT d.bar_date, d.close
                    FROM research_price_daily d
                    JOIN research_price_series s USING (series_id)
                    WHERE s.comparator_snapshot_id = %s
                      AND s.vendor_symbol = %s
                    ORDER BY d.bar_date
                    """,
                    (SNAPSHOT_ID, symbol),
                ).fetchall()
            )
            legacy_path = args.mirror_root / f"{symbol}.csv"
            if not legacy_path.is_file():
                raise RuntimeError(f"{symbol}: legacy mirror is missing at {legacy_path}")
            measurement = measure_overlap(symbol, recent, _legacy_closes(legacy_path))
            print(
                f"{symbol:<5} n={measurement.paired_sessions:<4} "
                f"split={measurement.expected_split_factor} "
                f"level={measurement.median_normalised_level_ratio:.6f} "
                f"corr={measurement.return_correlation:.6f} "
                f"median_return_diff_bp={measurement.median_return_difference * 10_000:.4f} "
                f"p99_abs_return_diff_bp={measurement.p99_absolute_return_difference * 10_000:.3f}"
            )
    print(f"all {len(COMPARATOR_SYMBOLS)} comparator overlap checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
