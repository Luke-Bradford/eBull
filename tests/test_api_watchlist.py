"""Tests for the watchlist API (Phase 3.2)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture(autouse=True)
def cleanup() -> Iterator[None]:
    yield
    from app.db import get_conn

    app.dependency_overrides.pop(get_conn, None)


def _install_conn(fetchone_returns: list, fetchall_returns: list | None = None) -> MagicMock:
    """Stub DB that drives fetchone calls in order and fetchall if supplied."""
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__.return_value = cur
    cur.__exit__.return_value = None
    cur.fetchone.side_effect = list(fetchone_returns)
    if fetchall_returns is not None:
        cur.fetchall.return_value = fetchall_returns
    cur.rowcount = 1
    conn.cursor.return_value = cur
    conn.commit = MagicMock()

    def _dep():
        yield conn

    from app.db import get_conn

    app.dependency_overrides[get_conn] = _dep
    return cur


def test_list_empty_watchlist(client: TestClient) -> None:
    with patch("app.api.watchlist.sole_operator_id", return_value=uuid4()):
        _install_conn([], fetchall_returns=[])
        resp = client.get("/watchlist")
    assert resp.status_code == 200
    body = resp.json()
    assert body["items"] == []
    assert body["total"] == 0


def test_list_returns_items_sorted_newest_first(client: TestClient) -> None:
    rows = [
        {
            "instrument_id": 42,
            "symbol": "AAPL",
            "company_name": "Apple Inc.",
            "exchange": "NMS",
            "currency": "USD",
            "sector": "Technology",
            "added_at": datetime(2026, 4, 15),
            "notes": "core holding",
        },
        {
            "instrument_id": 7,
            "symbol": "VOD.L",
            "company_name": "Vodafone",
            "exchange": "LSE",
            "currency": "GBP",
            "sector": "Telecom",
            "added_at": datetime(2026, 4, 10),
            "notes": None,
        },
    ]
    with patch("app.api.watchlist.sole_operator_id", return_value=uuid4()):
        _install_conn([], fetchall_returns=rows)
        resp = client.get("/watchlist")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total"] == 2
    assert body["items"][0]["symbol"] == "AAPL"
    assert body["items"][0]["notes"] == "core holding"
    assert body["items"][1]["notes"] is None


def test_add_happy_path(client: TestClient) -> None:
    # fetchone sequence: [instrument lookup, insert returning]
    fetchones = [
        {
            "instrument_id": 42,
            "symbol": "AAPL",
            "company_name": "Apple Inc.",
            "exchange": "NMS",
            "currency": "USD",
            "sector": "Technology",
        },
        {"added_at": datetime(2026, 4, 19), "notes": "watch for earnings"},
    ]
    with patch("app.api.watchlist.sole_operator_id", return_value=uuid4()):
        _install_conn(fetchones)
        resp = client.post(
            "/watchlist",
            json={"symbol": "aapl", "notes": "watch for earnings"},
        )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["symbol"] == "AAPL"
    assert body["notes"] == "watch for earnings"


def test_add_unknown_symbol_returns_404(client: TestClient) -> None:
    with patch("app.api.watchlist.sole_operator_id", return_value=uuid4()):
        _install_conn([None])
        resp = client.post("/watchlist", json={"symbol": "NOTREAL"})
    assert resp.status_code == 404


def test_add_blank_symbol_returns_400(client: TestClient) -> None:
    with patch("app.api.watchlist.sole_operator_id", return_value=uuid4()):
        # Pydantic min_length=1 rejects empty strings with 422 before handler runs,
        # but whitespace-only survives validation.
        _install_conn([])
        resp = client.post("/watchlist", json={"symbol": "   "})
    assert resp.status_code == 400


def test_delete_happy_path(client: TestClient) -> None:
    with patch("app.api.watchlist.sole_operator_id", return_value=uuid4()):
        cur = _install_conn([])
        cur.rowcount = 1
        resp = client.delete("/watchlist/AAPL")
    assert resp.status_code == 204


def test_delete_not_on_watchlist_returns_404(client: TestClient) -> None:
    with patch("app.api.watchlist.sole_operator_id", return_value=uuid4()):
        cur = _install_conn([])
        cur.rowcount = 0
        resp = client.delete("/watchlist/AAPL")
    assert resp.status_code == 404
