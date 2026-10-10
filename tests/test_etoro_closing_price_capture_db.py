"""#3740 slice F — every closing-price capture is stored; a refused or empty response is stored, then fails."""

from __future__ import annotations

import gzip
import hashlib
from datetime import UTC, datetime

import psycopg
import pytest

from app.providers.implementations.etoro_perishables import RawBytesResponse
from app.services.etoro_closing_price_capture import ClosingPriceCaptureError, store_capture
from tests.fixtures.ebull_test_db import ebull_test_conn

__all__ = ["ebull_test_conn"]

_BODY = (
    b'[{"instrumentId":1001,"officialClosingPrice":336.64,"isMarketOpen":false,"closingPrices":'
    b'{"daily":{"price":340.42,"date":"2026-10-08 00:00:00Z"}}},'
    b'{"instrumentId":1002,"officialClosingPrice":347.86,"isMarketOpen":false,"closingPrices":{}}]'
)


def _response(status: int, body: bytes) -> RawBytesResponse:
    return RawBytesResponse(
        status=status,
        body=body,
        requested_at=datetime(2026, 10, 12, 23, 17, tzinfo=UTC),
        observed_at=datetime(2026, 10, 12, 23, 17, 3, tzinfo=UTC),
    )


def test_store_then_refuse_then_append_only(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    conn.autocommit = True
    report = store_capture(conn, _response(200, _BODY))
    assert (report.http_status, report.instrument_rows, report.payload_bytes) == (200, 2, len(_BODY))
    payload, sha = conn.execute(
        "SELECT payload_gzip, response_sha256 FROM etoro_closing_price_captures WHERE capture_id = %s",
        (report.capture_id,),
    ).fetchone()  # type: ignore[misc]
    assert gzip.decompress(payload) == _BODY
    assert sha == hashlib.sha256(_BODY).hexdigest() == report.response_sha256

    with pytest.raises(ClosingPriceCaptureError, match=r"^capture \d+: HTTP 401 with None instrument rows"):
        store_capture(conn, _response(401, b'{"errorCode":"Unauthorized"}'))
    with pytest.raises(ClosingPriceCaptureError, match=r"^capture \d+: HTTP 200 with 0 instrument rows"):
        store_capture(conn, _response(200, b"[]"))
    with pytest.raises(ClosingPriceCaptureError, match=r"^capture \d+: HTTP 200 with None instrument rows"):
        store_capture(conn, _response(200, b"[" * 100_000))
    assert conn.execute(
        "SELECT http_status, instrument_rows FROM etoro_closing_price_captures ORDER BY capture_id"
    ).fetchall() == [(200, 2), (401, None), (200, 0), (200, None)]

    # Each refused statement runs in an open transaction that is rolled back.
    conn.autocommit = False
    for statement in (
        "UPDATE etoro_closing_price_captures SET http_status = 200",
        "DELETE FROM etoro_closing_price_captures",
    ):
        with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
            conn.execute(statement)
        conn.rollback()
