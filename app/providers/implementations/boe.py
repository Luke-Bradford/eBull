"""Bank of England XUDLUSS (US dollars per pound sterling, daily spot) — request URL and strict CSV parser.

#3616 slice 3b, spec ``docs/specs/metrics/2026-10-13-3616-slice3b-tax-fx-boe.md`` (Design 2, "Parser contracts").
The parser is the measurement script's contract (``scripts/measure_3616_boe_xudluss.py``), which accepted all 13,089
rows of the 1975-2026 download. No account or key is needed. The endpoint answers the default urllib and httpx
User-Agents with 403 (spec fact 6), so callers send ``USER_AGENT``.

⚠ The repository is public and the Bank's exchange-rate data is excluded from the Open Government Licence: never
commit a body or a rate. Tests build synthetic bodies.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Final

SERIES: Final = "XUDLUSS"
HEADER: Final = f"DATE,{SERIES}"
#: Fixed English month tokens, never the process locale.
MONTHS: Final = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
USER_AGENT: Final = "eBull/1.0 (+https://github.com/Luke-Bradford/eBull)"
_BASE: Final = "https://www.bankofengland.co.uk/boeapps/database/_iadb-fromshowcolumns.asp?csv.x=yes"
#: At most 6 integer and 6 fractional digits: the storage column is NUMERIC(12,6).
_ROW: Final = re.compile(r"(\d{2}) (" + "|".join(MONTHS) + r") (\d{4}),(\d{1,6}(?:\.\d{1,6})?)")


class BoeParseError(ValueError):
    """The body does not meet the parser contract."""


@dataclass(frozen=True)
class Observation:
    observation_date: date
    usd_per_gbp: Decimal


def _boe_date(day: date) -> str:
    return f"{day.day:02d}/{MONTHS[day.month - 1]}/{day.year}"


def xudluss_url(date_from: date, date_to: date) -> str:
    """The CSV request: ``VPD=N`` (no provisional data, spec fact 7) and ``VFD=N``."""
    return (
        f"{_BASE}&Datefrom={_boe_date(date_from)}&Dateto={_boe_date(date_to)}"
        f"&SeriesCodes={SERIES}&CSVF=TN&UsingCodes=Y&VPD=N&VFD=N"
    )


def parse_xudluss_csv(body: bytes, requested_from: date, requested_to: date) -> list[Observation]:
    """Parse a body under the contract, or raise ``BoeParseError``.

    ASCII; CRLF line ends; the exact header; then ``DD Mon YYYY,<digits>[.<digits>]`` with an English month token and a
    real date; a value above zero; dates strictly increasing inside the requested interval. Whitespace, a sign, an
    exponent, an underscore, another month spelling, LF-only line ends and HTML all refuse. A header-only body parses
    to an empty list; whether that is acceptable is the caller's decision.
    """
    try:
        text = body.decode("ascii")
    except UnicodeDecodeError as exc:
        raise BoeParseError("body is not ASCII") from exc
    if not text.endswith("\r\n"):
        raise BoeParseError("body does not end with CRLF")
    lines = text[:-2].split("\r\n")
    if lines[0] != HEADER:
        raise BoeParseError(f"header is not {HEADER}: {lines[0][:80]!r}")
    observations: list[Observation] = []
    for number, line in enumerate(lines[1:], start=2):
        match = _ROW.fullmatch(line)
        if match is None:
            raise BoeParseError(f"line {number}: does not match the row grammar: {line[:80]!r}")
        day_text, month_text, year_text, value_text = match.groups()
        try:
            day = date(int(year_text), MONTHS.index(month_text) + 1, int(day_text))
        except ValueError as exc:
            raise BoeParseError(f"line {number}: no such date {line[:11]!r}") from exc
        value = Decimal(value_text)
        if value <= 0:
            raise BoeParseError(f"line {number}: value is not above zero")
        if not requested_from <= day <= requested_to:
            raise BoeParseError(f"line {number}: {day} outside {requested_from}..{requested_to}")
        if observations and day <= observations[-1].observation_date:
            raise BoeParseError(f"line {number}: {day} does not follow {observations[-1].observation_date}")
        observations.append(Observation(day, value))
    return observations


__all__ = [
    "HEADER",
    "MONTHS",
    "SERIES",
    "USER_AGENT",
    "BoeParseError",
    "Observation",
    "parse_xudluss_csv",
    "xudluss_url",
]
