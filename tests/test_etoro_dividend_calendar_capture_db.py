"""#3740 slice F — every capture is stored; a refused or empty page is stored and then fails the job."""

from __future__ import annotations

import gzip
import hashlib
from datetime import UTC, datetime

import psycopg
import pytest

from app.services.etoro_dividend_calendar_capture import (
    URL,
    CalendarResponse,
    DividendCalendarCaptureError,
    store_capture,
)

_PAGE = (
    b'<tr><td data-ex-dividend-date="2026-09-25">2026-09-25</td><td>2026-10-12</td></tr>'
    b'<tr><td data-ex-dividend-date="2026-10-09">2026-10-09</td><td>2026-10-13</td></tr>'
)


def _response(status: int, body: bytes) -> CalendarResponse:
    return CalendarResponse(
        url=URL,
        requested_at=datetime(2026, 10, 12, 12, 40, tzinfo=UTC),
        observed_at=datetime(2026, 10, 12, 12, 40, 2, tzinfo=UTC),
        status=status,
        headers=(("content-type", "text/html"), ("vary", "Accept-Encoding"), ("vary", "Cookie")),
        body=body,
    )


def test_store_then_refuse_then_append_only(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    conn.autocommit = True
    report = store_capture(conn, _response(200, _PAGE))
    assert (report.http_status, report.ex_date_marker_count, report.payload_bytes) == (200, 2, len(_PAGE))
    payload, headers, sha = conn.execute(
        "SELECT payload_gzip, response_headers, response_sha256 FROM etoro_dividend_calendar_captures "
        "WHERE capture_id = %s",
        (report.capture_id,),
    ).fetchone()  # type: ignore[misc]
    assert gzip.decompress(payload) == _PAGE
    assert sha == hashlib.sha256(_PAGE).hexdigest() == report.response_sha256
    # Repeated headers survive in served order.
    assert headers == [["content-type", "text/html"], ["vary", "Accept-Encoding"], ["vary", "Cookie"]]

    # The same bytes again are a second observation, not a duplicate.
    store_capture(conn, _response(200, _PAGE))
    with pytest.raises(DividendCalendarCaptureError, match="HTTP 403 with 0 calendar rows"):
        store_capture(conn, _response(403, b"Attention Required! | Cloudflare"))
    with pytest.raises(DividendCalendarCaptureError, match="HTTP 200 with 0 calendar rows"):
        store_capture(conn, _response(200, b"<html>challenge</html>"))
    assert conn.execute(
        "SELECT http_status, ex_date_marker_count FROM etoro_dividend_calendar_captures ORDER BY capture_id"
    ).fetchall() == [(200, 2), (200, 2), (403, 0), (200, 0)]

    # Append-only: the fixture empties tables under session_replication_role = replica, which skips the trigger.
    with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
        conn.execute("UPDATE etoro_dividend_calendar_captures SET http_status = 200")
    with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
        conn.execute("DELETE FROM etoro_dividend_calendar_captures")
