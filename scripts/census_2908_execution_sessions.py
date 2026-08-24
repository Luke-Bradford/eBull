"""Count #2908 execution-date availability without opening any price value."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import date
from pathlib import Path

from app.services.r6_pit_bundle import load_r6_pit_bundle


def _dates(path: Path) -> tuple[date, ...]:
    observed: list[date] = []
    with path.open(encoding="utf-8", errors="strict") as handle:
        for line in handle:
            raw = line.partition(",")[0].strip()
            try:
                observed.append(date.fromisoformat(raw))
            except ValueError:
                continue
    if observed != sorted(set(observed)):
        raise RuntimeError(f"price dates are duplicate or unordered: {path}")
    return tuple(observed)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--price-series-dir", type=Path, required=True)
    parser.add_argument("--window-end", type=date.fromisoformat, required=True)
    args = parser.parse_args()
    bundle = load_r6_pit_bundle(args.manifest, expected_manifest_sha256=args.manifest_sha256)
    cached: dict[str, tuple[date, ...]] = {}
    result: dict[str, object] = {}
    for formation in sorted({row.formation_close for row in bundle.records}):
        counts: Counter[str] = Counter()
        delays: Counter[int] = Counter()
        for row in bundle.records_at(formation):
            dates = cached.setdefault(row.symbol, _dates(args.price_series_dir / f"{row.symbol}.csv"))
            later = [value for value in dates if value > formation.date()]
            if not later:
                counts["no_post_formation_bar"] += 1
                continue
            delay = (later[0] - formation.date()).days
            delays[delay] += 1
            counts["has_post_formation_bar"] += 1
            if delay > 7:
                counts["post_formation_delay_over_7_days"] += 1
            if args.window_end in dates:
                counts["exact_window_end_bar"] += 1
            elif dates[-1] < args.window_end:
                counts["series_ends_before_window_end"] += 1
            else:
                counts["window_end_session_missing_inside_series"] += 1
        result[formation.isoformat()] = {
            "counts": dict(sorted(counts.items())),
            "first_post_formation_delay_calendar_days": {str(key): value for key, value in sorted(delays.items())},
        }
    print(json.dumps({"formations": result, "window_end": args.window_end.isoformat()}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
