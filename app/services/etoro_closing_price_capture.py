"""Daily raw capture of eToro's official closing prices (#3740 slice F, sql/480).

``GET /api/v1/market-data/instruments/history/closing-price`` returns, for every instrument in one response,
the most recent official close and the previous trading day's, week's and month's. eToro serves no history
beyond those, so evidence starts on the first day we capture. The body is stored exactly as served with no
parsing decisions: which rows count and how a close maps to a session belong to slice F's measurement.

A non-200 response or a body without instrument rows is stored and then fails the job. A transport error or
a retryable status that exhausts its retries raises from the provider and stores nothing.
"""

from __future__ import annotations

import gzip
import hashlib
import json
from dataclasses import dataclass
from typing import Any, Final

import psycopg
from psycopg.rows import dict_row

from app.providers.implementations.etoro_perishables import RawBytesResponse


class ClosingPriceCaptureError(RuntimeError):
    """The response was refused or held no instrument rows."""


@dataclass(frozen=True)
class CaptureReport:
    capture_id: int
    http_status: int
    instrument_rows: int | None
    payload_bytes: int
    response_sha256: str


def count_instrument_rows(body: bytes) -> int | None:
    """Length of the top-level JSON array; None when the body is not one (liveness only, not validation)."""
    try:
        decoded = json.loads(body)
    except ValueError, RecursionError:  # a pathologically nested body must not escape before the row is stored
        return None
    return len(decoded) if isinstance(decoded, list) else None


_INSERT: Final = """
    INSERT INTO etoro_closing_price_captures
        (requested_at, observed_at, http_status, response_sha256, payload_bytes, payload_gzip, instrument_rows)
    VALUES (%(requested_at)s, %(observed_at)s, %(status)s, %(sha)s, %(payload_bytes)s, %(payload)s, %(rows)s)
    RETURNING capture_id
"""


def store_capture(conn: psycopg.Connection[Any], response: RawBytesResponse) -> CaptureReport:
    """Store one capture, then raise if it was refused or empty (the row stays as evidence)."""
    sha = hashlib.sha256(response.body).hexdigest()
    rows = count_instrument_rows(response.body)
    with conn.transaction(), conn.cursor(row_factory=dict_row) as cursor:
        row = cursor.execute(
            _INSERT,
            {
                "requested_at": response.requested_at,
                "observed_at": response.observed_at,
                "status": response.status,
                "sha": sha,
                "payload_bytes": len(response.body),
                "payload": gzip.compress(response.body),
                "rows": rows,
            },
        ).fetchone()
    if row is None:
        raise RuntimeError("closing price capture insert returned no id")
    report = CaptureReport(int(row["capture_id"]), response.status, rows, len(response.body), sha)
    if response.status != 200 or not rows:
        raise ClosingPriceCaptureError(
            f"capture {report.capture_id}: HTTP {response.status} with {rows} instrument rows; stored, not usable"
        )
    return report


__all__ = [
    "CaptureReport",
    "ClosingPriceCaptureError",
    "count_instrument_rows",
    "store_capture",
]
