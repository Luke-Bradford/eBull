"""Read dividends off the stored eToro dividend-calendar captures (#3844).

The captures (``etoro_dividend_calendar_captures``, #3760) are raw HTML. Each calendar row carries a ticker, a
``data-ex-dividend-date`` attribute and five plain cells: sector, ex-date, pay date, annual dividend and periodic
dividend. Measured on the 8 captures of 2026-10-10..10-16 (422 distinct ticker/ex-date rows): a row first appears on
its ex-date, never before, and stays until its pay date; the page window is pay dates from today to about six weeks
ahead. A daily capture therefore sees every dividend the page lists, after the fact, from the first capture on.

Treatment, fixed here because the page states none:

* **Amount.** The periodic cell, per share in the listing's currency. eToro marks the page indicative.
* **Repeats.** The same ticker and ex-date seen in several captures resolves to the latest capture's amount.
* **Identity.** The ticker is matched to ``instruments.symbol`` case-insensitively, canonical rows only; a ticker
  matching more than one is dropped.
* **Unparsed rows.** A row without exactly the five cells, a date cell that disagrees with the attribute, or a
  non-numeric, non-finite or negative amount is skipped and counted, never guessed.
* **Covered days.** A capture's UTC date counts as covered only when every ``data-ex-dividend-date`` marker on it
  parsed (marker count = parsed rows, none unparsed). An empty page (stored before its job fails) or a drifted one
  therefore leaves a gap, and a consumer must treat a gap as "dividends unknown", never as "none paid".

Completeness against an independent dividend source is not measured; consumers must say so.
"""

from __future__ import annotations

import gzip
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date
from typing import Any, Final

import psycopg

Conn = psycopg.Connection[Any]

_ROW: Final = re.compile(r'<tr class="[^"]*" data-report-date="[^"]*">(.*?)</tr>', re.S)
_TICKER: Final = re.compile(r'wp-block-etoro-ed-company__ticker">\s*([^<\s]+)\s*<')
_EX_DATE: Final = re.compile(r'data-ex-dividend-date="(\d{4}-\d{2}-\d{2})"')
_PLAIN_CELL: Final = re.compile(r"<td[^>]*>([^<]*)</td>")


@dataclass(frozen=True)
class CalendarRow:
    ticker: str
    ex_date: date
    pay_date: date
    amount: float


@dataclass(frozen=True)
class ParsedPage:
    rows: tuple[CalendarRow, ...]
    unparsed: int


def parse_calendar(html: str) -> ParsedPage:
    rows: list[CalendarRow] = []
    unparsed = 0
    for body in _ROW.findall(html):
        ticker = _TICKER.search(body)
        ex = _EX_DATE.search(body)
        cells = [c.strip() for c in _PLAIN_CELL.findall(body)]
        try:
            if ticker is None or ex is None or len(cells) != 5 or cells[1] != ex.group(1):
                raise ValueError("row shape")
            amount = float(cells[4])
            # float() accepts "nan", "inf" and negatives; any of them would poison a basket's total silently.
            if not math.isfinite(amount) or amount < 0:
                raise ValueError("amount")
            rows.append(
                CalendarRow(
                    ticker=ticker.group(1).upper(),
                    ex_date=date.fromisoformat(cells[1]),
                    pay_date=date.fromisoformat(cells[2]),
                    amount=amount,
                )
            )
        except ValueError:
            unparsed += 1
    return ParsedPage(tuple(rows), unparsed)


def page_is_complete(page: ParsedPage, ex_date_markers: int) -> bool:
    """True when every ``data-ex-dividend-date`` marker the capture counted parsed into a row."""
    return ex_date_markers > 0 and page.unparsed == 0 and len(page.rows) == ex_date_markers


@dataclass(frozen=True)
class CalendarDividends:
    #: Instrument id -> (ex-date, periodic amount per share), ex-date ascending.
    by_instrument: Mapping[int, Sequence[tuple[date, float]]]
    #: UTC dates with at least one fully parsed 200 capture, ascending.
    captured_days: tuple[date, ...]
    unparsed_rows: int


_CAPTURES_SQL: Final = """
    SELECT capture_id, observed_at, ex_date_marker_count FROM etoro_dividend_calendar_captures
    WHERE http_status = 200 AND (observed_at AT TIME ZONE 'UTC')::date >= %s
    ORDER BY observed_at
"""

#: capture_id -> its parse. The table is append-only (trigger-enforced), so a parse never goes stale and each
#: payload is read and parsed once per process rather than on every request.
_PARSED: dict[int, ParsedPage] = {}


def load_calendar_dividends(conn: Conn, *, since: date) -> CalendarDividends:
    """Dividends from captures observed on or after ``since`` (UTC). A row appears on its ex-date, never before, so
    a capture before ``since`` cannot carry an ex-date after it."""
    captures = conn.execute(_CAPTURES_SQL, (since,)).fetchall()
    missing = [int(cid) for cid, _, _ in captures if int(cid) not in _PARSED]
    if missing:
        for cid, payload in conn.execute(
            "SELECT capture_id, payload_gzip FROM etoro_dividend_calendar_captures WHERE capture_id = ANY(%s)",
            (missing,),
        ).fetchall():
            _PARSED[int(cid)] = parse_calendar(gzip.decompress(payload).decode("utf-8"))
    latest: dict[tuple[str, date], float] = {}
    days: set[date] = set()
    unparsed = 0
    for cid, observed_at, markers in captures:
        page = _PARSED[int(cid)]
        unparsed += page.unparsed
        if page_is_complete(page, markers):
            days.add(observed_at.astimezone(UTC).date())
        for row in page.rows:
            latest[(row.ticker, row.ex_date)] = row.amount
    tickers = sorted({t for t, _ in latest})
    matches: dict[str, list[int]] = {}
    for iid, symbol in conn.execute(
        "SELECT instrument_id, symbol FROM instruments"
        " WHERE upper(symbol) = ANY(%s) AND canonical_instrument_id IS NULL",
        (tickers,),
    ).fetchall():
        matches.setdefault(str(symbol).upper(), []).append(int(iid))
    # A ticker matching two canonical instruments is ambiguous and dropped rather than credited to either.
    ids = {ticker: found[0] for ticker, found in matches.items() if len(found) == 1}
    by_instrument: dict[int, list[tuple[date, float]]] = {}
    for (ticker, ex_date), amount in sorted(latest.items(), key=lambda kv: kv[0][1]):
        if ticker in ids:
            by_instrument.setdefault(ids[ticker], []).append((ex_date, amount))
    return CalendarDividends(by_instrument, tuple(sorted(days)), unparsed)


__all__ = [
    "CalendarDividends",
    "CalendarRow",
    "ParsedPage",
    "load_calendar_dividends",
    "page_is_complete",
    "parse_calendar",
]
