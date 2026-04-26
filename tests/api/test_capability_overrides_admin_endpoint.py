"""Tests for /admin/capability-overrides endpoint (#531).

Pins the diff contract: rows that match the migration-071 seed
default for their asset_class are excluded; rows that diverge
surface the seed-vs-current breakdown per capability.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.capability_overrides_admin import router as overrides_router
from app.db import get_conn


def _build_app(conn: MagicMock) -> FastAPI:
    app = FastAPI()
    app.include_router(overrides_router)

    def _yield_conn():  # type: ignore[return]
        yield conn

    app.dependency_overrides[get_conn] = _yield_conn
    from app.api.auth import require_session_or_service_token

    app.dependency_overrides[require_session_or_service_token] = lambda: None
    return app


def _make_cur(rows: list[dict[str, object]]) -> MagicMock:
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchall.return_value = rows
    return cur


_US_EQUITY_SEED: dict[str, list[str]] = {
    "filings": ["sec_edgar"],
    "fundamentals": ["sec_xbrl"],
    "dividends": ["sec_dividend_summary"],
    "insider": ["sec_form4"],
    "analyst": [],
    "ratings": [],
    "esg": [],
    "ownership": ["sec_13f", "sec_13d_13g"],
    "corporate_events": ["sec_8k_events"],
    "business_summary": ["sec_10k_item1"],
    "officers": [],
}


def test_seed_match_returns_empty_overrides() -> None:
    """An exchange whose capabilities exactly match the seed default
    must NOT appear in the overrides list."""
    cur = _make_cur(
        [
            {
                "exchange_id": "4",
                "name": "Nasdaq",
                "asset_class": "us_equity",
                "capabilities": dict(_US_EQUITY_SEED),
            },
        ]
    )
    conn = MagicMock()
    conn.cursor.return_value = cur

    app = _build_app(conn)
    with TestClient(app) as client:
        resp = client.get("/admin/capability-overrides")
    assert resp.status_code == 200
    body = resp.json()
    assert body["total_overrides"] == 0
    assert body["rows"] == []


def test_diverged_row_lists_only_changed_capabilities() -> None:
    """An exchange with one capability override surfaces with that
    capability's diff; clean capabilities are dropped from the row."""
    overridden = dict(_US_EQUITY_SEED)
    overridden["analyst"] = ["operator_custom"]
    cur = _make_cur(
        [
            {
                "exchange_id": "4",
                "name": "Nasdaq",
                "asset_class": "us_equity",
                "capabilities": overridden,
            },
        ]
    )
    conn = MagicMock()
    conn.cursor.return_value = cur

    app = _build_app(conn)
    with TestClient(app) as client:
        resp = client.get("/admin/capability-overrides")
    assert resp.status_code == 200
    body = resp.json()
    assert body["total_overrides"] == 1
    row = body["rows"][0]
    assert row["exchange_id"] == "4"
    assert len(row["diffs"]) == 1
    diff = row["diffs"][0]
    assert diff["capability"] == "analyst"
    assert diff["seed_providers"] == []
    assert diff["current_providers"] == ["operator_custom"]


def test_non_us_equity_seed_is_empty() -> None:
    """Non-us_equity rows have empty seed defaults; any populated
    capability counts as drift."""
    crypto_with_data: dict[str, list[str]] = {cap: [] for cap in _US_EQUITY_SEED}
    crypto_with_data["filings"] = ["coingecko"]
    cur = _make_cur(
        [
            {
                "exchange_id": "100",
                "name": "Crypto Exchange",
                "asset_class": "crypto",
                "capabilities": crypto_with_data,
            },
        ]
    )
    conn = MagicMock()
    conn.cursor.return_value = cur

    app = _build_app(conn)
    with TestClient(app) as client:
        resp = client.get("/admin/capability-overrides")
    assert resp.status_code == 200
    body = resp.json()
    assert body["total_overrides"] == 1
    diff = body["rows"][0]["diffs"][0]
    assert diff["capability"] == "filings"
    assert diff["seed_providers"] == []
    assert diff["current_providers"] == ["coingecko"]
