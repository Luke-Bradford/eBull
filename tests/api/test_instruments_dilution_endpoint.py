"""Tests for GET /instruments/{symbol}/dilution (#435)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.instruments import router as instruments_router
from app.db import get_conn
from app.services.dilution import DilutionSummary, ShareCountPeriod


def _build_app(conn: MagicMock) -> FastAPI:
    app = FastAPI()
    app.include_router(instruments_router)

    def _yield_conn():  # type: ignore[return]
        yield conn

    app.dependency_overrides[get_conn] = _yield_conn
    return app


def _cursor_with(rows: list[dict[str, object] | None]) -> MagicMock:
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchone.side_effect = rows
    return cur


def test_dilution_endpoint_returns_summary_and_history() -> None:
    summary = DilutionSummary(
        latest_shares=Decimal("15000000000"),
        latest_as_of=date(2025, 12, 31),
        yoy_shares=Decimal("15500000000"),
        net_dilution_pct_yoy=Decimal("-3.23"),
        ttm_shares_issued=Decimal("100000"),
        ttm_buyback_shares=Decimal("500000000"),
        ttm_net_share_change=Decimal("-499900000"),
        dilution_posture="buyback_heavy",
    )
    history = [
        ShareCountPeriod(
            period_end=date(2025, 12, 31),
            fiscal_year=2025,
            fiscal_period="FY",
            shares_outstanding=Decimal("15000000000"),
            shares_issued_new=None,
            buyback_shares=Decimal("500000000"),
        ),
    ]

    conn = MagicMock()
    conn.cursor.return_value = _cursor_with([{"instrument_id": 1, "symbol": "AAPL"}])
    app = _build_app(conn)

    with (
        patch("app.services.dilution.get_dilution_summary", return_value=summary),
        patch("app.services.dilution.get_share_count_history", return_value=history),
        TestClient(app) as client,
    ):
        resp = client.get("/instruments/AAPL/dilution")

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["symbol"] == "AAPL"
    assert body["summary"]["dilution_posture"] == "buyback_heavy"
    assert body["summary"]["net_dilution_pct_yoy"] == "-3.23"
    assert len(body["history"]) == 1
    assert body["history"][0]["fiscal_period"] == "FY"


def test_dilution_endpoint_unknown_symbol_404() -> None:
    conn = MagicMock()
    conn.cursor.return_value = _cursor_with([None])
    app = _build_app(conn)

    with TestClient(app) as client:
        resp = client.get("/instruments/__NOSUCH__/dilution")

    assert resp.status_code == 404


def test_dilution_endpoint_rejects_out_of_range_limit() -> None:
    conn = MagicMock()
    app = _build_app(conn)
    with TestClient(app) as client:
        resp = client.get("/instruments/AAPL/dilution", params={"limit": 0})
    assert resp.status_code == 422
    with TestClient(app) as client:
        resp = client.get("/instruments/AAPL/dilution", params={"limit": 201})
    assert resp.status_code == 422
