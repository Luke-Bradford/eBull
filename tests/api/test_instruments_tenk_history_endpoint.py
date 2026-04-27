"""GET /instruments/{symbol}/filings/10-k/history (#559)."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.instruments import router as instruments_router
from app.db import get_conn
from app.services.business_summary import TenKHistoryRow


def _build_app(conn: MagicMock) -> FastAPI:
    app = FastAPI()
    app.include_router(instruments_router)

    def _yield_conn():  # type: ignore[return]
        yield conn

    app.dependency_overrides[get_conn] = _yield_conn
    return app


def _cursor_returning_instrument() -> MagicMock:
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchone.return_value = {"instrument_id": 1, "symbol": "GME"}
    return cur


def _cursor_returning_nothing() -> MagicMock:
    """Cursor whose fetchone always returns None — simulates unknown symbol."""
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchone.return_value = None
    return cur


_FAKE_FILINGS: tuple[TenKHistoryRow, ...] = (
    TenKHistoryRow(
        accession_number="0001326380-26-000001",
        filing_date=date(2026, 3, 15),
        filing_type="10-K",
    ),
    TenKHistoryRow(
        accession_number="0001326380-25-000001",
        filing_date=date(2025, 3, 10),
        filing_type="10-K/A",
    ),
)


def test_tenk_history_returns_descending_filing_dates() -> None:
    """History endpoint returns filings in descending date order with correct shape."""
    conn = MagicMock()
    conn.cursor.return_value = _cursor_returning_instrument()
    app = _build_app(conn)

    with patch(
        "app.services.business_summary.list_10k_history",
        return_value=_FAKE_FILINGS,
    ):
        client = TestClient(app)
        r = client.get("/instruments/GME/filings/10-k/history")

    assert r.status_code == 200
    data = r.json()
    assert data["symbol"] == "GME"
    filings = data["filings"]
    assert len(filings) == 2

    # Shape check on first filing.
    f0 = filings[0]
    assert {"accession_number", "filing_date", "filing_type"} <= set(f0)
    assert f0["accession_number"] == "0001326380-26-000001"
    assert f0["filing_date"] == "2026-03-15"
    assert f0["filing_type"] == "10-K"

    # Ordering: newest date first.
    assert filings[0]["filing_date"] > filings[1]["filing_date"]


def test_tenk_history_404_for_unknown_symbol() -> None:
    """Endpoint returns 404 when the symbol is not in the instruments table."""
    conn = MagicMock()
    conn.cursor.return_value = _cursor_returning_nothing()
    app = _build_app(conn)

    # No patch on list_10k_history — the 404 should fire before it is called.
    client = TestClient(app)
    r = client.get("/instruments/XYZNOTREAL/filings/10-k/history")

    assert r.status_code == 404
