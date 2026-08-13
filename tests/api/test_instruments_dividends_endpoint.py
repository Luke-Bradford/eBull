"""Tests for GET /instruments/{symbol}/dividends.

Service-layer logic is covered by ``tests/test_dividends_service.py``
against the real ebull_test DB. This test pins HTTP response shape,
404 on unknown symbol, and auth — all via a mocked connection so it
can run anywhere.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.instruments import router as instruments_router
from app.db import get_conn
from app.services.dividends import DividendPeriod, DividendSummary


def _build_app(conn: MagicMock) -> FastAPI:
    app = FastAPI()
    app.include_router(instruments_router)

    def _yield_conn():  # type: ignore[return]
        yield conn

    app.dependency_overrides[get_conn] = _yield_conn
    # Bypass auth for shape tests.
    from app.api.auth import require_session_or_service_token

    app.dependency_overrides[require_session_or_service_token] = lambda: None
    return app


def _cursor_with(rows: list[dict[str, object] | tuple[object, ...] | None]) -> MagicMock:
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchone.side_effect = rows
    return cur


def test_dividends_endpoint_returns_summary_and_history() -> None:
    summary = DividendSummary(
        has_dividend=True,
        ttm_dps=Decimal("2.0000"),
        ttm_dividends_paid=Decimal("200000000.0000"),
        ttm_yield_pct=Decimal("2.00"),
        latest_dps=Decimal("0.5000"),
        latest_dividend_at=date(2025, 12, 28),
        dividend_streak_q=20,
        dividend_currency="USD",
    )
    history = [
        DividendPeriod(
            period_end_date=date(2025, 12, 28),
            period_type="Q4",
            fiscal_year=2025,
            fiscal_quarter=4,
            dps_declared=Decimal("0.5000"),
            dividends_paid=Decimal("50000000.0000"),
            reported_currency="USD",
        ),
    ]

    conn = MagicMock()
    # Endpoint calls fetchone twice: symbol lookup, then ``_has_sec_cik``
    # SEC-CIK gate. The third call (``get_upcoming_dividends`` is fully
    # patched below) never reaches conn so the side_effect ends here.
    conn.cursor.return_value = _cursor_with(
        [{"instrument_id": 1, "symbol": "AAPL"}, (1,)],
    )
    app = _build_app(conn)

    with (
        patch("app.services.dividends.get_dividend_summary", return_value=summary),
        patch("app.services.dividends.get_dividend_history", return_value=history),
        patch("app.services.dividends.get_upcoming_dividends", return_value=[]),
        TestClient(app) as client,
    ):
        resp = client.get("/instruments/AAPL/dividends")

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["symbol"] == "AAPL"
    assert body["summary"]["has_dividend"] is True
    assert body["summary"]["ttm_yield_pct"] == "2.00"
    assert body["summary"]["dividend_streak_q"] == 20
    assert len(body["history"]) == 1
    assert body["history"][0]["period_type"] == "Q4"


def test_dividends_endpoint_unknown_symbol_404() -> None:
    conn = MagicMock()
    conn.cursor.return_value = _cursor_with([None])
    app = _build_app(conn)

    with TestClient(app) as client:
        resp = client.get("/instruments/__NOSUCHSYMBOL__/dividends")

    assert resp.status_code == 404


def test_dividends_endpoint_rejects_out_of_range_limit() -> None:
    conn = MagicMock()
    app = _build_app(conn)
    with TestClient(app) as client:
        resp = client.get("/instruments/AAPL/dividends", params={"limit": 0})
    assert resp.status_code == 422
    with TestClient(app) as client:
        resp = client.get("/instruments/AAPL/dividends", params={"limit": 401})
    assert resp.status_code == 422
