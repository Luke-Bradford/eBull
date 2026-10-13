"""#3616 slice 3b: the Bank of England XUDLUSS download and the GOV.UK bank-holiday calendar, validated and
cross-checked (spec ``docs/specs/metrics/2026-10-13-3616-slice3b-tax-fx-boe.md``, "Facts established first").

Measurement only. Reads saved downloads, applies the parser contracts the spec freezes (Design 2), and prints the
publication pattern of the series and its agreement with the England-and-Wales bank-holiday calendar.

    curl -s -o XUDLUSS.csv -w "%{http_code} %{content_type}\\n" \\
      "https://www.bankofengland.co.uk/boeapps/database/_iadb-fromshowcolumns.asp?csv.x=yes\\
&Datefrom=01/Jan/1975&Dateto=13/Oct/2026&SeriesCodes=XUDLUSS&CSVF=TN&UsingCodes=Y&VPD=N&VFD=N"
    curl -s -o bank-holidays.json https://www.gov.uk/bank-holidays.json
    uv run python scripts/measure_3616_boe_xudluss.py XUDLUSS.csv --requested-from 1975-01-01 \\
      --requested-to 2026-10-13 --calendar bank-holidays.json --out <file.json>

The calendar measured for the spec is committed in ``docs/research/3616-slice3b-inputs/``. The XUDLUSS body is not
(this project's conservative handling rule while the Bank's reuse terms for the series are unresolved); its sha256 is
in the output. The output carries
counts and dates only, never a rate.

Neither source needs an account or key.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

SERIES = "XUDLUSS"
HEADER = f"DATE,{SERIES}"
MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
# The storage column is NUMERIC(12,6): at most 6 integer and 6 fractional digits (spec Design 1).
_ROW = re.compile(r"(\d{2}) (" + "|".join(MONTHS) + r") (\d{4}),(\d{1,6}(?:\.\d{1,6})?)")
CALENDAR_DIVISION = "england-and-wales"
MIN_HOLIDAYS_PER_YEAR = 8


class IncompleteFetch(ValueError):
    """The response does not meet the spec's parser contract."""


@dataclass(frozen=True)
class Observation:
    observation_date: date
    value: Decimal  # US dollars per pound sterling, as published


def parse_xudluss_csv(body: bytes, requested_from: date, requested_to: date) -> list[Observation]:
    """Parse a BoE CSV body under the spec's contract, or raise ``IncompleteFetch``.

    ASCII; CRLF line ends; the exact header; then ``DD Mon YYYY,<digits>[.<digits>]`` with English month tokens (no
    locale), at most 6 integer and 6 fractional digits, a value above zero, dates strictly increasing and inside the
    requested interval. A header-only body is a valid empty list.
    """
    try:
        text = body.decode("ascii")
    except UnicodeDecodeError as exc:
        raise IncompleteFetch("body is not ASCII") from exc
    if not text.endswith("\r\n"):
        raise IncompleteFetch("body does not end with CRLF")
    lines = text[:-2].split("\r\n")
    if lines[0] != HEADER:
        raise IncompleteFetch(f"header is not {HEADER}: {lines[0][:80]!r}")
    observations: list[Observation] = []
    for number, line in enumerate(lines[1:], start=2):
        match = _ROW.fullmatch(line)
        if match is None:
            raise IncompleteFetch(f"line {number}: does not match the row grammar: {line[:80]!r}")
        day_text, month_text, year_text, value_text = match.groups()
        try:
            day = date(int(year_text), MONTHS.index(month_text) + 1, int(day_text))
        except ValueError as exc:
            raise IncompleteFetch(f"line {number}: no such date {line[:11]!r}") from exc
        value = Decimal(value_text)
        if value <= 0:
            raise IncompleteFetch(f"line {number}: value {value_text!r} is not above zero")
        if not requested_from <= day <= requested_to:
            raise IncompleteFetch(f"line {number}: {day} outside {requested_from}..{requested_to}")
        if observations and day <= observations[-1].observation_date:
            raise IncompleteFetch(f"line {number}: {day} does not follow {observations[-1].observation_date}")
        observations.append(Observation(day, value))
    return observations


def parse_bank_holidays(body: bytes) -> dict[int, frozenset[date]]:
    """Parse GOV.UK ``bank-holidays.json`` into England-and-Wales holidays per year, or raise ``IncompleteFetch``.

    Every year from the first to the last listed must hold at least ``MIN_HOLIDAYS_PER_YEAR`` distinct dates (the 8
    recurring England-and-Wales bank and public holidays). That is a plausibility check only: it does not prove a year
    complete. The spec additionally checks agreement with the series over the received observation interval; neither
    check independently proves calendar completeness.
    """
    try:
        division = json.loads(body)[CALENDAR_DIVISION]
        dates = [date.fromisoformat(event["date"]) for event in division["events"]]
    except (ValueError, KeyError, TypeError) as exc:
        raise IncompleteFetch(f"calendar is not the documented shape: {exc}") from exc
    if division.get("division") != CALENDAR_DIVISION or not dates or len(set(dates)) != len(dates):
        raise IncompleteFetch("calendar division missing, empty or holding duplicate dates")
    by_year: dict[int, set[date]] = {}
    for day in dates:
        by_year.setdefault(day.year, set()).add(day)
    for year in range(min(by_year), max(by_year) + 1):
        if len(by_year.get(year, ())) < MIN_HOLIDAYS_PER_YEAR:
            raise IncompleteFetch(f"calendar year {year} holds {len(by_year.get(year, ()))} dates")
    return {year: frozenset(days) for year, days in by_year.items()}


def measure(observations: list[Observation]) -> dict[str, Any]:
    if len(observations) < 2:
        raise SystemExit(f"measurement needs at least 2 observations, got {len(observations)}")
    days = [o.observation_date for o in observations]
    pairs = list(zip(days, days[1:], strict=False))
    gaps = Counter((b - a).days for a, b in pairs)
    widest = max(gaps)
    # Move between consecutive observations, |ln(b/a)| in percent: the basis of the spec's plausibility bound.
    moves = sorted(
        (
            (abs(float((b.value / a.value).ln())) * 100, a.observation_date, b.observation_date)
            for a, b in zip(observations, observations[1:], strict=False)
        ),
        reverse=True,
    )
    return {
        "observations": len(observations),
        "first": days[0].isoformat(),
        "last": days[-1].isoformat(),
        "decimal_places": dict(sorted(Counter(-int(o.value.as_tuple().exponent) for o in observations).items())),
        "observed_weekdays": dict(sorted(Counter(d.strftime("%a") for d in days).items())),
        "gap_days_histogram": {str(k): gaps[k] for k in sorted(gaps)},
        "widest_gap_days": widest,
        "widest_gaps_latest_3": [[a.isoformat(), b.isoformat()] for a, b in pairs if (b - a).days == widest][-3:],
        "largest_moves_pct": [[round(m, 2), a.isoformat(), b.isoformat()] for m, a, b in moves[:3]],
        "largest_move_pct_since_2019": next(
            [round(m, 2), a.isoformat(), b.isoformat()] for m, a, b in moves if a >= date(2019, 1, 1)
        ),
    }


def cross_check(observations: list[Observation], holidays: dict[int, frozenset[date]]) -> dict[str, Any]:
    """Over every date from the calendar's first year to the last observation: does the series observe exactly the
    working days (Monday to Friday, not a bank holiday)? Both directions are reported, weekends included."""
    observed = {o.observation_date for o in observations}
    every_holiday = frozenset().union(*holidays.values())
    start, end = date(min(holidays), 1, 1), max(observed)
    if end.year > max(holidays):
        raise SystemExit(f"calendar ends {max(holidays)}, before the last observation {end}")
    working_without_observation, observed_non_working, weekday_holidays = [], [], 0
    day = start
    while day <= end:
        working = day.weekday() < 5 and day not in every_holiday
        weekday_holidays += day.weekday() < 5 and day in every_holiday
        if working and day not in observed:
            working_without_observation.append(day.isoformat())
        if not working and day in observed:
            observed_non_working.append(day.isoformat())
        day += timedelta(days=1)
    return {
        "calendar_years": [min(holidays), max(holidays)],
        "holidays_per_year": {str(y): len(d) for y, d in sorted(holidays.items())},
        "checked_interval": [start.isoformat(), end.isoformat()],
        "weekday_holidays_in_interval": weekday_holidays,
        "working_days_without_observation": working_without_observation,
        "observations_on_non_working_days": observed_non_working,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("csv", type=Path)
    parser.add_argument("--requested-from", type=date.fromisoformat, required=True)
    parser.add_argument("--requested-to", type=date.fromisoformat, required=True)
    parser.add_argument("--calendar", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    raw = args.csv.read_bytes()
    body = gzip.decompress(raw) if args.csv.suffix == ".gz" else raw
    calendar_body = args.calendar.read_bytes()
    observations = parse_xudluss_csv(body, args.requested_from, args.requested_to)
    result = {
        "measured_on": date.today().isoformat(),
        "series": SERIES,
        "requested": [args.requested_from.isoformat(), args.requested_to.isoformat()],
        "sha256": hashlib.sha256(body).hexdigest(),
        "bytes": len(body),
        **measure(observations),
        "calendar_sha256": hashlib.sha256(calendar_body).hexdigest(),
        "calendar_cross_check": cross_check(observations, parse_bank_holidays(calendar_body)),
    }
    args.out.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
