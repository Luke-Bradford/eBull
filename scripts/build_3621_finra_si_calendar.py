"""Build ``docs/research/3621-finra-si-calendar.csv``: FINRA's designated short-interest settlement, due and
publication dates, 2021-05-28 to 2024-08-31.

FINRA Rule 4560 delegates the settlement calendar to FINRA, which publishes it per year on
``finra.org/filing-reporting/regulatory-filing-systems/short-interest`` ("Settlement Date | Due Date | Publication
Date"). The live page now shows only the current years, so the tables are read from Wayback snapshots of that page
(``SNAPSHOTS``). Each snapshot's "<year> Short Interest Reporting Dates" tables are parsed; a due or publication
month earlier than its settlement month rolls into the next year. A settlement date listed by two snapshots must
carry the same row in both, or the build refuses.

Usage: ``uv run python -m scripts.build_3621_finra_si_calendar [out.csv]``
"""

from __future__ import annotations

import html
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path

import httpx

PAGE = "https://www.finra.org/filing-reporting/regulatory-filing-systems/short-interest"
SNAPSHOTS = ("20211214074614", "20220811220216", "20230315011536", "20240320044607", "20241215000000")
FIRST = date(2021, 5, 28)
LAST = date(2024, 8, 31)
OUT = Path(__file__).resolve().parents[1] / "docs" / "research" / "3621-finra-si-calendar.csv"
_TABLE = re.compile(r"<h2[^>]*>[^<]*?(\d{4})\W+Short\W+Interest\W+Reporting\W+Dates(.*?)</table>", re.S)


class CalendarError(RuntimeError):
    pass


def parse_page(text: str) -> list[tuple[date, date, date]]:
    """(settlement, due, publication) rows from every year table on one snapshot."""
    out: list[tuple[date, date, date]] = []
    for table in _TABLE.finditer(text):
        year = int(table.group(1))
        for tr in re.findall(r"<tr>(.*?)</tr>", table.group(2), flags=re.S):
            cells = re.findall(r"<td[^>]*>(.*?)</td>", tr, flags=re.S)
            if len(cells) != 3:
                continue
            days: list[date] = []
            for cell in cells:
                words = re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", cell))).strip()
                found = re.match(r"([A-Z][a-z]+) (\d{1,2})", words)
                if found is None:
                    raise CalendarError(f"{year}: unparseable cell {words!r}")
                day = datetime.strptime(f"{found.group(1)} {found.group(2)} {year}", "%B %d %Y").date()
                if days and day < days[0]:
                    day = day.replace(year=year + 1)
                days.append(day)
            out.append((days[0], days[1], days[2]))
    return out


def build() -> list[tuple[date, date, date, list[str]]]:
    rows: dict[date, tuple[date, date, date]] = {}
    seen: dict[date, list[str]] = {}
    with httpx.Client(follow_redirects=True, timeout=60) as client:
        for snapshot in SNAPSHOTS:
            response = client.get(f"https://web.archive.org/web/{snapshot}/{PAGE}")
            response.raise_for_status()
            for row in parse_page(response.text):
                if rows.setdefault(row[0], row) != row:
                    raise CalendarError(f"{row[0]}: snapshots disagree ({rows[row[0]]} vs {row})")
                seen.setdefault(row[0], []).append(snapshot)
            time.sleep(2)
    return [(*rows[d], seen[d]) for d in sorted(rows) if FIRST <= d <= LAST]


def main(out: Path) -> None:
    lines = ["settlement_date,due_date,publication_date,wayback_snapshots"]
    lines += [f"{s},{d},{p},{' '.join(snaps)}" for s, d, p, snaps in build()]
    out.write_text("\n".join(lines) + "\n")
    print(f"{len(lines) - 1} rows -> {out}")


if __name__ == "__main__":
    main(Path(sys.argv[1]) if len(sys.argv) > 1 else OUT)
