"""Daily raw capture of eToro's public dividend calendar page (#3740 slice F).

The page is the only account-free forward candidate assessed for dividend ex-dates (EDGAR text cannot date
17.7% of a pilot's events; #3740 2026-10-10). eToro keeps no archive, so evidence starts on the first day we
capture. This module stores the response exactly as served and makes no parsing decisions: which rows
count, how dates and amounts resolve, and how the page compares with EDGAR belong to the later concordance
measurement.

The page is server-rendered (one ``data-ex-dividend-date`` attribute per row) and its HTML differs on every
fetch, so every capture is a new row. A non-200 response or a page with no calendar rows is stored and then
fails the job: a Cloudflare challenge would otherwise read as a successful capture of nothing. Two cases
store no row and fail the job through ``job_runs`` only: a transport error (no response exists) and a body
over ``MAX_PAYLOAD_BYTES``.
"""

from __future__ import annotations

import gzip
import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Final

import httpx
import psycopg
from psycopg.rows import dict_row

URL: Final = "https://www.etoro.com/investing/dividend-calendar/"
#: Measured 2026-10-10: Cloudflare answers httpx's default agent with 403 and this one with the page.
USER_AGENT: Final = "eBull/1.0 reference-data"
EX_DATE_MARKER: Final = b'data-ex-dividend-date="'
#: Session cookies say nothing about the page and are not kept.
_DROPPED_HEADERS: Final = frozenset({"set-cookie"})
#: Fixed by construction at about 17x the 954,392-byte page measured on 2026-10-10: room for the calendar to
#: grow, while an unrelated or runaway response is refused before it is held in memory or stored.
MAX_PAYLOAD_BYTES: Final = 16 * 1024 * 1024


class DividendCalendarCaptureError(RuntimeError):
    """The page was refused, held no calendar rows, or exceeded the size bound."""


@dataclass(frozen=True)
class CalendarResponse:
    url: str
    requested_at: datetime
    observed_at: datetime
    status: int
    headers: tuple[tuple[str, str], ...]
    body: bytes


@dataclass(frozen=True)
class CaptureReport:
    capture_id: int
    http_status: int
    ex_date_marker_count: int
    payload_bytes: int
    response_sha256: str


def fetch_dividend_calendar(
    *,
    url: str = URL,
    timeout: float = 60.0,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> CalendarResponse:
    """One GET, no redirects followed: a redirect means the source moved, and is stored as such."""
    requested_at = now()
    with httpx.stream(
        "GET", url, headers={"User-Agent": USER_AGENT}, timeout=timeout, follow_redirects=False
    ) as response:
        declared = response.headers.get("content-length")
        if declared is not None and declared.isdigit() and int(declared) > MAX_PAYLOAD_BYTES:
            raise DividendCalendarCaptureError(
                f"HTTP {response.status_code} declares {declared} bytes, over {MAX_PAYLOAD_BYTES}; not stored"
            )
        body = bytearray()
        for chunk in response.iter_bytes():
            body += chunk
            if len(body) > MAX_PAYLOAD_BYTES:
                raise DividendCalendarCaptureError(
                    f"HTTP {response.status_code} body exceeds {MAX_PAYLOAD_BYTES} bytes; not stored"
                )
        return CalendarResponse(
            url=url,
            requested_at=requested_at,
            observed_at=now(),
            status=response.status_code,
            headers=tuple(
                (name, value) for name, value in response.headers.multi_items() if name.lower() not in _DROPPED_HEADERS
            ),
            body=bytes(body),
        )


_INSERT: Final = """
    INSERT INTO etoro_dividend_calendar_captures
        (url, requested_at, observed_at, http_status, response_headers, response_sha256, payload_bytes,
         payload_gzip, ex_date_marker_count)
    VALUES (%(url)s, %(requested_at)s, %(observed_at)s, %(status)s, %(headers)s::jsonb, %(sha)s,
            %(payload_bytes)s, %(payload)s, %(markers)s)
    RETURNING capture_id
"""


def store_capture(conn: psycopg.Connection[Any], response: CalendarResponse) -> CaptureReport:
    """Store one capture, then raise if the page was refused or empty (the row stays as evidence)."""
    sha = hashlib.sha256(response.body).hexdigest()
    markers = response.body.count(EX_DATE_MARKER)
    with conn.transaction(), conn.cursor(row_factory=dict_row) as cursor:
        row = cursor.execute(
            _INSERT,
            {
                "url": response.url,
                "requested_at": response.requested_at,
                "observed_at": response.observed_at,
                "status": response.status,
                "headers": json.dumps([list(pair) for pair in response.headers]),
                "sha": sha,
                "payload_bytes": len(response.body),
                "payload": gzip.compress(response.body),
                "markers": markers,
            },
        ).fetchone()
    if row is None:
        raise RuntimeError("dividend calendar capture insert returned no id")
    report = CaptureReport(int(row["capture_id"]), response.status, markers, len(response.body), sha)
    if response.status != 200 or markers == 0:
        raise DividendCalendarCaptureError(
            f"capture {report.capture_id}: HTTP {response.status} with {markers} calendar rows; stored, not usable"
        )
    return report


__all__ = [
    "EX_DATE_MARKER",
    "MAX_PAYLOAD_BYTES",
    "URL",
    "USER_AGENT",
    "CalendarResponse",
    "CaptureReport",
    "DividendCalendarCaptureError",
    "fetch_dividend_calendar",
    "store_capture",
]
