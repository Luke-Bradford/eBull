"""Tests for /admin/business-summary-failures endpoints (#533).

Pins the failure-dashboard contract: histogram counts match
visible rows, quarantined filter excludes active failures, reset
clears the tracking columns without touching body/source_accession.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.business_summary_admin import router as failures_router
from app.db import get_conn


def _build_app(conn: MagicMock) -> FastAPI:
    app = FastAPI()
    app.include_router(failures_router)

    def _yield_conn():  # type: ignore[return]
        yield conn

    app.dependency_overrides[get_conn] = _yield_conn
    from app.api.auth import require_session_or_service_token

    app.dependency_overrides[require_session_or_service_token] = lambda: None
    return app


def test_list_returns_histogram_and_rows() -> None:
    from datetime import UTC, datetime

    last_parsed = datetime(2026, 4, 1, tzinfo=UTC)
    next_retry = datetime(2026, 4, 2, tzinfo=UTC)
    histogram_rows = [
        {"reason": "no_item_1_marker", "count": 167, "quarantined_count": 12},
        {"reason": "fetch_http_5xx", "count": 5, "quarantined_count": 0},
    ]
    detail_rows = [
        {
            "instrument_id": 7,
            "symbol": "ABC",
            "company_name": "ABC Co",
            "source_accession": "0001-26-1",
            "attempt_count": 4,
            "last_failure_reason": "no_item_1_marker",
            "last_parsed_at": last_parsed,
            "next_retry_at": next_retry,
        },
    ]
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    # 1st execute → histogram fetchall; 2nd execute → total fetchone; 3rd execute → detail fetchall.
    cur.fetchall.side_effect = [histogram_rows, detail_rows]
    cur.fetchone.return_value = {"total": 172}
    conn = MagicMock()
    conn.cursor.return_value = cur

    app = _build_app(conn)
    with TestClient(app) as client:
        resp = client.get("/admin/business-summary-failures")

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total_failing"] == 172
    assert body["histogram"][0] == {
        "reason": "no_item_1_marker",
        "count": 167,
        "quarantined_count": 12,
    }
    assert len(body["rows"]) == 1
    assert body["rows"][0]["symbol"] == "ABC"
    assert body["rows"][0]["is_quarantined"] is True


def test_reset_returns_404_when_no_row() -> None:
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.rowcount = 0
    conn = MagicMock()
    conn.cursor.return_value = cur

    app = _build_app(conn)
    with TestClient(app) as client:
        resp = client.post("/admin/business-summary-failures/999/reset")

    assert resp.status_code == 404


def test_reset_clears_failure_columns_and_requeues() -> None:
    """The reset SQL must (a) set next_retry_at to NOW() so the
    ingester picks the row up on its next run via the
    ``next_retry_at <= NOW()`` predicate (Codex review on #533),
    and (b) leave body + source_accession alone so any prior
    successful narrative survives. The WHERE clause must also
    require ``next_retry_at IS NOT NULL`` so a healthy row never
    counts as 'cleared'."""
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.rowcount = 1
    conn = MagicMock()
    conn.cursor.return_value = cur

    app = _build_app(conn)
    with TestClient(app) as client:
        resp = client.post("/admin/business-summary-failures/42/reset")

    assert resp.status_code == 200
    assert resp.json() == {"instrument_id": 42, "cleared": True}
    args, _ = cur.execute.call_args
    sql = args[0]
    assert "attempt_count       = 0" in sql
    assert "last_failure_reason = NULL" in sql
    assert "next_retry_at       = NOW()" in sql
    # The WHERE guard prevents resetting a healthy row.
    assert "next_retry_at IS NOT NULL" in sql
    # body / source_accession must not be touched.
    assert "body" not in sql
    assert "source_accession" not in sql
