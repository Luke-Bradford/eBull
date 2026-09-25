"""#2901 mirror date audit: bars dated on a day the NYSE was closed.

Spec: ``docs/proposals/ta/2026-09-25-2901-quality-declaration-spec.md`` ("Simulator", "Last close"). Reads only the
DATE column of every CSV under the survivorship-free mirror — never a price — and classifies each bar's day with
``market_calendar.us_market_status``. A closed weekday is a full NYSE closure. The declaration re-runs this and records
the output together with this file's sha256 and the mirror commit, both printed here.

Usage::

    uv run python -m scripts.measure_2901_offcalendar --root ~/Dev/eBull/var/research_corpus/mirrors/icyDenev_Intrader
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Final

from app.services.market_calendar import us_market_status

#: The simulator's window: the first formation month to ``WINDOW_END``.
WINDOW: Final = (date(2013, 6, 1), date(2024, 9, 27))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    root: Path = args.root.expanduser()

    closed: dict[date, bool] = {}
    bars = 0
    off = Counter[str]()
    in_window = 0
    files = files_with = files_starting = files_ending = 0
    for path in sorted(root.rglob("*.csv")):
        files += 1
        first: bool | None = None
        last = False
        had = False
        with path.open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                try:
                    day = date.fromisoformat(line[:10])
                except ValueError:
                    continue
                bars += 1
                is_closed = closed.get(day)
                if is_closed is None:
                    is_closed = closed[day] = us_market_status(day) == "closed"
                if first is None:
                    first = is_closed
                last = is_closed
                if is_closed:
                    had = True
                    off["weekend" if day.weekday() >= 5 else "closure"] += 1
                    if WINDOW[0] <= day <= WINDOW[1]:
                        in_window += 1
        files_with += had
        files_starting += bool(first)
        files_ending += last

    commit = subprocess.run(["git", "-C", str(root), "rev-parse", "HEAD"], capture_output=True, text=True, check=True)
    print(
        json.dumps(
            {
                "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                "mirror_commit": commit.stdout.strip(),
                "files": files,
                "bars": bars,
                "off_calendar": dict(sorted(off.items())),
                "files_with_off_calendar": files_with,
                "files_starting_off_calendar": files_starting,
                "files_ending_off_calendar": files_ending,
                "off_calendar_in_window": in_window,
                "window": [WINDOW[0].isoformat(), WINDOW[1].isoformat()],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
