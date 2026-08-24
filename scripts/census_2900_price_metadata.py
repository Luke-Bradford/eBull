"""Freeze outcome-blind Intrader identity and termination metadata for R6.

The report contains no OHLC, adjusted price, volume, or return value. It binds
the full database-side series census to the clean upstream mirror commit and
retains the Form-25 termination evidence already accepted by #2721.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from collections import Counter
from pathlib import Path
from typing import Any, Final

import psycopg
from psycopg.rows import dict_row

from app.config import settings
from app.services.universe_selection import SURVIVORSHIP_FREE_VENDOR

PARSER_VERSION: Final = "r6-price-metadata-census-v1"

_SQL: Final = """
SELECT series_id, vendor_symbol, instrument_id, first_bar, last_bar, bar_count,
       delisting_date, delisting_source, delisting_provision,
       delisting_filed_date
FROM research_price_series
WHERE vendor = %(vendor)s
ORDER BY vendor_symbol, series_id
"""


def _sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _git_output(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(root), *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def build_report(rows: list[dict[str, Any]], *, mirror_root: Path, mirror_commit: str) -> dict[str, Any]:
    symbols = [str(row["vendor_symbol"]) for row in rows]
    if len(symbols) != len(set(symbols)):
        raise RuntimeError("Intrader metadata contains duplicate vendor symbols")
    counts: Counter[str] = Counter(series=len(rows))
    records: list[dict[str, Any]] = []
    for row in rows:
        counts["with_bars" if row["bar_count"] is not None else "without_bars"] += 1
        counts["linked" if row["instrument_id"] is not None else "unlinked"] += 1
        source = row["delisting_source"]
        counts[f"delisting_source_{source or 'none'}"] += 1
        records.append(
            {
                "bar_count": row["bar_count"],
                "delisting_date": None if row["delisting_date"] is None else row["delisting_date"].isoformat(),
                "delisting_filed_date": (
                    None if row["delisting_filed_date"] is None else row["delisting_filed_date"].isoformat()
                ),
                "delisting_provision": row["delisting_provision"],
                "delisting_source": source,
                "first_bar": None if row["first_bar"] is None else row["first_bar"].isoformat(),
                "instrument_id": row["instrument_id"],
                "last_bar": None if row["last_bar"] is None else row["last_bar"].isoformat(),
                "series_id": row["series_id"],
                "symbol": row["vendor_symbol"],
            }
        )
    return {
        "counts": dict(sorted(counts.items())),
        "mirror_commit": mirror_commit,
        "mirror_root": str(mirror_root.resolve()),
        "parser_version": PARSER_VERSION,
        "query_sha256": hashlib.sha256(_SQL.encode()).hexdigest(),
        "records": records,
        "script_sha256": _sha256(Path(__file__)),
        "vendor": SURVIVORSHIP_FREE_VENDOR,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mirror-root", type=Path, required=True)
    args = parser.parse_args()
    commit = _git_output(args.mirror_root, "rev-parse", "HEAD")
    dirty = _git_output(args.mirror_root, "status", "--porcelain")
    if dirty:
        raise RuntimeError("Intrader mirror is dirty; point-in-time source identity refused")
    with psycopg.connect(settings.database_url) as conn:
        with conn.cursor(row_factory=dict_row) as cursor:
            cursor.execute(_SQL, {"vendor": SURVIVORSHIP_FREE_VENDOR})
            rows = cursor.fetchall()
    print(json.dumps(build_report(rows, mirror_root=args.mirror_root, mirror_commit=commit), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
