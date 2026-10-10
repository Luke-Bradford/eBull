"""#3740 slice F: the planning read behind the dividend measurement spec's reference counts (disclosed exposure).

Spec: ``docs/research/2026-10-10-3740-slice-f-dividend-measurement.md`` §"Exposure before freeze". This is the
exact read run on 2026-10-10, kept so the exposure is reproducible. It is an UNVERIFIED read of the stage-B panel
artefact (no manifest check, no access-register entry): it parses every row but uses only ``M``, ``exclusion``,
``me``, ``name_key`` and ``series_id`` of formation 2023-09-30, then counts Intrader dividend rows for the top 1,000
over 2023-10-01..2024-08-31. No return, holding status or characteristic value enters any figure, and no EDGAR
extraction is run. The measurement itself re-reads the cohort through ``read_verified_artefact``.

Usage: ``PYTHONPATH=. uv run python -m scripts.probe_3740_slice_f_dividend_reference``
"""

from __future__ import annotations

import gzip
import json
from collections import Counter
from pathlib import Path

import psycopg

from app.config import settings

ARTEFACT = (
    Path.home()
    / "Library/Application Support/eBull/research/factor_panel_3609"
    / "2026-10-09-25577cff-stageB-1c5de22cccda4937b55eb7e16e505b7f"
)
FORMATION = "2023-09-30"
FIRST, LAST = "2023-10-01", "2024-08-31"


def main() -> None:
    admitted: list[tuple[float, int, int]] = []
    with gzip.open(ARTEFACT / "rows.jsonl.gz", "rt") as handle:
        for line in handle:
            row = json.loads(line)
            if row["M"] == FORMATION and row["exclusion"] is None:
                admitted.append((float(row["me"]["value"]), row["name_key"], row["series_id"]))
    admitted.sort(key=lambda t: (-t[0], t[1]))
    top = admitted[:1000]
    me = {series: value for value, _, series in top}
    total = sum(me.values())
    series_ids = list(me)
    print(f"admitted {len(admitted)}; top {len(top)}; ME of the 1,000th {top[-1][0]:.0f}")
    with psycopg.connect(settings.database_url) as conn:
        vendors = conn.execute(
            "select vendor, count(*) from research_price_series where series_id = any(%s) group by 1",
            (series_ids,),
        ).fetchall()
        events = conn.execute(
            "select series_id, bar_date, dividend, close from research_price_daily"
            " where series_id = any(%s) and dividend <> 0 and bar_date between %s and %s",
            (series_ids, FIRST, LAST),
        ).fetchall()
        last_bars = conn.execute(
            "select series_id, max(bar_date) from research_price_daily where series_id = any(%s) group by 1",
            (series_ids,),
        ).fetchall()
    payers = {series for series, *_ in events}
    print(f"vendors {vendors}")
    print(f"events {len(events)}; payers {len(payers)}; payer ME share {sum(me[s] for s in payers) / total:.4f}")
    per_payer = Counter(series for series, *_ in events)
    print(f"events per payer {sorted(Counter(per_payer.values()).items())}")
    print(f"negative amounts {sum(1 for e in events if e[2] < 0)}")
    yields = sorted(float(amount) / float(close) for _, _, amount, close in events if close)
    median, p99 = yields[len(yields) // 2], yields[int(len(yields) * 0.99)]
    print(f"yield median {median:.6f}; p99 {p99:.6f}; max {yields[-1]:.6f}")
    months = Counter(str(last)[:7] for _, last in last_bars)
    print(f"last-bar months {months.most_common(5)}")


if __name__ == "__main__":
    main()
