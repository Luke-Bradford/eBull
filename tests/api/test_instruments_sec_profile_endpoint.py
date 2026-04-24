"""Tests for GET /instruments/{symbol}/sec_profile (#427)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.instruments import router as instruments_router
from app.db import get_conn
from app.services.sec_entity_profile import SecEntityProfile


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


def _sample_profile(instrument_id: int = 1) -> SecEntityProfile:
    return SecEntityProfile(
        instrument_id=instrument_id,
        cik="0000320193",
        sic="3571",
        sic_description="Electronic Computers",
        owner_org="06 Technology",
        description="Designs consumer electronics.",
        website="https://apple.com",
        investor_website=None,
        ein="EIN",
        lei=None,
        state_of_incorporation="CA",
        state_of_incorporation_desc="California",
        fiscal_year_end="0930",
        category="Large accelerated filer",
        exchanges=["NASDAQ"],
        former_names=[{"name": "APPLE COMPUTER INC", "from": "1977-01-01", "to": "2007-01-01"}],
        has_insider_issuer=True,
        has_insider_owner=True,
    )


def test_sec_profile_endpoint_returns_profile() -> None:
    conn = MagicMock()
    conn.cursor.return_value = _cursor_with([{"instrument_id": 1, "symbol": "AAPL"}])
    app = _build_app(conn)

    with (
        patch("app.services.sec_entity_profile.get_entity_profile", return_value=_sample_profile()),
        TestClient(app) as client,
    ):
        resp = client.get("/instruments/AAPL/sec_profile")

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["symbol"] == "AAPL"
    assert body["cik"] == "0000320193"
    assert body["sic_description"] == "Electronic Computers"
    assert body["exchanges"] == ["NASDAQ"]
    assert body["has_insider_issuer"] is True
    assert len(body["former_names"]) == 1
    assert body["former_names"][0]["name"] == "APPLE COMPUTER INC"


def test_sec_profile_endpoint_unknown_symbol_404() -> None:
    conn = MagicMock()
    conn.cursor.return_value = _cursor_with([None])
    app = _build_app(conn)

    with TestClient(app) as client:
        resp = client.get("/instruments/__NOSUCH__/sec_profile")

    assert resp.status_code == 404
    assert resp.json()["detail"].startswith("Instrument")


def test_sec_profile_endpoint_no_profile_404() -> None:
    conn = MagicMock()
    conn.cursor.return_value = _cursor_with([{"instrument_id": 1, "symbol": "AAPL"}])
    app = _build_app(conn)

    with (
        patch("app.services.sec_entity_profile.get_entity_profile", return_value=None),
        TestClient(app) as client,
    ):
        resp = client.get("/instruments/AAPL/sec_profile")

    assert resp.status_code == 404
    assert resp.json()["detail"] == "no SEC profile on file for this instrument"


def test_sec_profile_endpoint_blank_symbol_rejected() -> None:
    conn = MagicMock()
    app = _build_app(conn)
    with TestClient(app) as client:
        resp = client.get("/instruments/ /sec_profile")
    # FastAPI path-param routing treats " " as a valid symbol, so the
    # 400 short-circuit inside the handler fires.
    assert resp.status_code == 400
