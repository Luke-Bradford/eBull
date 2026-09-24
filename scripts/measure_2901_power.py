"""#2901 power statement, from PRE-WINDOW published data only.

Spec: ``docs/proposals/ta/2026-09-24-2901-quality-arm.md`` ("Power"). Reads global-q's
``portf_gpa_monthly_2025.csv`` and refuses to look at any month on or after the first holding
month of the test window, so it cannot leak the window's GP/A premium.

Tracking-error proxy: value-weighted decile 10 minus the equal average of deciles 1-10, monthly,
annualised by sqrt(12) (IID approximation, stated). The annual excess whose expected t equals the
bar has 50% power; 80% power needs (t_bar + 0.8416) x TE / sqrt(years).

Usage::

    uv run python -m scripts.measure_2901_power --zip <prof_monthly_2025.zip> --sha256 <sha>
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import math
import statistics
import zipfile
from pathlib import Path
from typing import Final

MEMBER: Final = "prof_monthly_2025/portf_gpa_monthly_2025.csv"
FIRST: Final = (1967, 7)
FIRST_WINDOW_MONTH: Final = (2013, 7)  # first holding month of the 2013-06 formation
WINDOW_YEARS: Final = 134 / 12  # full months 2013-07 .. 2024-08 (spec: monthly statistics window)
T_BARS: Final = (3.0, 1.96)
Z_80: Final = 0.8416  # standard-normal quantile for 80% power, one-sided
TE_SENSITIVITY: Final = (0.06, 0.10)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--zip", type=Path, required=True)
    parser.add_argument("--sha256", required=True)
    args = parser.parse_args()
    if hashlib.sha256(args.zip.read_bytes()).hexdigest() != args.sha256:
        raise SystemExit("global-q archive digest moved")
    months: dict[tuple[int, int], dict[int, float]] = {}
    with zipfile.ZipFile(args.zip) as archive, archive.open(MEMBER) as raw:
        reader = csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8-sig"))
        if reader.fieldnames != ["year", "month", "rank_GPA", "nstocks", "ret_vw"]:
            raise SystemExit(f"unexpected header: {reader.fieldnames}")
        for row in reader:
            key = (int(row["year"]), int(row["month"]))
            if key >= FIRST_WINDOW_MONTH:
                continue  # never read the test window
            rank, value, count = int(row["rank_GPA"]), float(row["ret_vw"]) / 100.0, int(row["nstocks"])
            if not math.isfinite(value) or count <= 0:
                raise SystemExit(f"invalid row at {key} rank {rank}")
            if rank in months.setdefault(key, {}):
                raise SystemExit(f"duplicate rank {rank} at {key}")
            months[key][rank] = value
    keys = sorted(k for k in months if k >= FIRST)
    expected = (FIRST_WINDOW_MONTH[0] - FIRST[0]) * 12 + FIRST_WINDOW_MONTH[1] - FIRST[1]
    if len(keys) != expected or any(sorted(months[k]) != list(range(1, 11)) for k in keys):
        raise SystemExit(f"pre-window months are not {expected} complete, contiguous decile sets")
    active = [months[k][10] - statistics.fmean(months[k].values()) for k in keys]
    te = statistics.pstdev(active) * math.sqrt(12)
    print(f"months {keys[0]}..{keys[-1]} n={len(keys)}  tracking_error_annual={te:.4f}")
    for label, tracking in (("measured proxy", te), *((f"sensitivity {x:.0%}", x) for x in TE_SENSITIVITY)):
        for bar in T_BARS:
            half = bar * tracking / math.sqrt(WINDOW_YEARS)
            eighty = (bar + Z_80) * tracking / math.sqrt(WINDOW_YEARS)
            print(f"{label} TE={tracking:.4f} t>{bar}: annual excess for 50% power {half:.4f}, 80% power {eighty:.4f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
