"""Tests for GET /instruments/{symbol}/employees (#551).

Service-layer logic is exercised by the existing financial_facts_raw
ingest tests. This test pins the HTTP response shape, 404 on
unknown symbol, 404 on no fact on file — via mocked connection so
the test runs anywhere.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.instruments import router as instruments_router
from app.db import get_conn


def _build_app(conn: MagicMock) -> FastAPI:
    app = FastAPI()
    app.include_router(instruments_router)

    def _yield_conn():  # type: ignore[return]
        yield conn

    app.dependency_overrides[get_conn] = _yield_conn
    from app.api.auth import require_session_or_service_token

    app.dependency_overrides[require_session_or_service_token] = lambda: None
    return app


def _cursor_with(rows: list[dict[str, object] | None]) -> MagicMock:
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchone.side_effect = rows
    return cur


def test_employees_endpoint_returns_latest_fact() -> None:
    conn = MagicMock()
    conn.cursor.return_value = _cursor_with(
        [
            {"instrument_id": 1, "symbol": "AAPL"},
            {
                "period_end": date(2025, 9, 27),
                "val": Decimal("164000"),
                "accession_number": "0000320193-25-000079",
            },
        ]
    )
    app = _build_app(conn)
    with TestClient(app) as client:
        resp = client.get("/instruments/AAPL/employees")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body == {
        "symbol": "AAPL",
        "employees": 164000,
        "period_end_date": "2025-09-27",
        "source_accession": "0000320193-25-000079",
    }


def test_employees_endpoint_404_unknown_symbol() -> None:
    conn = MagicMock()
    conn.cursor.return_value = _cursor_with([None])
    app = _build_app(conn)
    with TestClient(app) as client:
        resp = client.get("/instruments/NOPE/employees")
    assert resp.status_code == 404


def test_employees_endpoint_404_when_no_fact() -> None:
    """Instrument exists but has no DEI EntityNumberOfEmployees row."""
    conn = MagicMock()
    conn.cursor.return_value = _cursor_with(
        [
            {"instrument_id": 1, "symbol": "BTC"},
            None,  # no fact on file
        ]
    )
    app = _build_app(conn)
    with TestClient(app) as client:
        resp = client.get("/instruments/BTC/employees")
    assert resp.status_code == 404
