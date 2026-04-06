"""
Tests for app.api.filings — filings feed endpoint.

Test strategy:
  Mock DB via FastAPI dependency override.  The ``get_conn`` dependency is
  replaced with a mock connection.  Because the endpoint uses separate
  ``with conn.cursor() as cur:`` blocks for each query, the mock creates
  a fresh cursor per ``cursor()`` call, each pre-loaded with its own
  result set.

Structure:
  - TestListFilings — happy path, empty, pagination, filing_type filter,
                      nullable fields, instrument not found, validation
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime
from typing import Any
from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from app.db import get_conn
from app.main import app

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 4, 6, 12, 0, 0, tzinfo=UTC)


def _make_filing_row(
    filing_event_id: int = 1,
    instrument_id: int = 1,
    filing_date: date = date(2026, 3, 15),
    filing_type: str | None = "10-K",
    provider: str = "sec",
    source_url: str | None = "https://sec.gov/filing/1",
    primary_document_url: str | None = "https://sec.gov/doc/1",
    extracted_summary: str | None = "Annual report shows steady growth.",
    red_flag_score: float | None = 0.15,
    created_at: datetime = _NOW,
) -> dict[str, Any]:
    return {
        "filing_event_id": filing_event_id,
        "instrument_id": instrument_id,
        "filing_date": filing_date,
        "filing_type": filing_type,
        "provider": provider,
        "source_url": source_url,
        "primary_document_url": primary_document_url,
        "extracted_summary": extracted_summary,
        "red_flag_score": red_flag_score,
        "created_at": created_at,
    }


def _mock_conn(
    cursor_result_sets: list[list[dict[str, Any]]],
) -> tuple[MagicMock, list[MagicMock]]:
    """Build a mock psycopg.Connection with separate cursors per call."""
    conn = MagicMock()
    created_cursors: list[MagicMock] = []
    result_iter = iter(cursor_result_sets)

    def _make_cursor(*_a: Any, **_kw: Any) -> MagicMock:
        rows = next(result_iter, [])
        cur = MagicMock()
        cur.fetchone.return_value = rows[0] if rows else None
        cur.fetchall.return_value = rows
        cur.__enter__ = MagicMock(return_value=cur)
        cur.__exit__ = MagicMock(return_value=False)
        created_cursors.append(cur)
        return cur

    conn.cursor.side_effect = _make_cursor
    return conn, created_cursors


def _with_conn(
    cursor_result_sets: list[list[dict[str, Any]]],
) -> tuple[MagicMock, list[MagicMock]]:
    conn, cursors = _mock_conn(cursor_result_sets)

    def _override() -> Iterator[MagicMock]:
        yield conn

    app.dependency_overrides[get_conn] = _override
    return conn, cursors


def _cleanup() -> None:
    app.dependency_overrides[get_conn] = _fallback_conn


def _fallback_conn() -> Iterator[MagicMock]:
    conn, _ = _mock_conn([[], [], []])
    yield conn


app.dependency_overrides.setdefault(get_conn, _fallback_conn)

client = TestClient(app)


# ---------------------------------------------------------------------------
# TestListFilings
# ---------------------------------------------------------------------------


class TestListFilings:
    """GET /filings/{instrument_id} — filing events."""

    def teardown_method(self) -> None:
        _cleanup()

    def test_happy_path_returns_filing_items(self) -> None:
        row = _make_filing_row()
        # Cursor sequence: instrument lookup, COUNT, items
        _with_conn(
            [
                [{"symbol": "AAPL"}],
                [{"cnt": 1}],
                [row],
            ]
        )
        resp = client.get("/filings/1")

        assert resp.status_code == 200
        body = resp.json()
        assert body["instrument_id"] == 1
        assert body["symbol"] == "AAPL"
        assert body["total"] == 1
        assert body["offset"] == 0
        assert body["limit"] == 50
        assert len(body["items"]) == 1

        item = body["items"][0]
        assert item["filing_event_id"] == 1
        assert item["filing_date"] == "2026-03-15"
        assert item["filing_type"] == "10-K"
        assert item["provider"] == "sec"
        assert item["primary_document_url"] == "https://sec.gov/doc/1"
        assert item["extracted_summary"] == "Annual report shows steady growth."
        assert item["red_flag_score"] == 0.15

    def test_instrument_not_found_returns_404(self) -> None:
        _with_conn([[]])
        resp = client.get("/filings/999")

        assert resp.status_code == 404

    def test_no_filings_returns_empty_list(self) -> None:
        _with_conn(
            [
                [{"symbol": "AAPL"}],
                [{"cnt": 0}],
                [],
            ]
        )
        resp = client.get("/filings/1")

        assert resp.status_code == 200
        body = resp.json()
        assert body["items"] == []
        assert body["total"] == 0

    def test_pagination_offset_and_limit(self) -> None:
        row = _make_filing_row()
        _with_conn(
            [
                [{"symbol": "AAPL"}],
                [{"cnt": 50}],
                [row],
            ]
        )
        resp = client.get("/filings/1", params={"offset": 10, "limit": 25})

        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 50
        assert body["offset"] == 10
        assert body["limit"] == 25

    def test_items_query_receives_limit_and_offset(self) -> None:
        _, cursors = _with_conn(
            [
                [{"symbol": "AAPL"}],
                [{"cnt": 1}],
                [_make_filing_row()],
            ]
        )
        client.get("/filings/1", params={"offset": 5, "limit": 10})

        items_params = cursors[2].execute.call_args[0][1]
        assert items_params["limit"] == 10
        assert items_params["offset"] == 5

    def test_count_query_receives_no_limit_offset(self) -> None:
        _, cursors = _with_conn(
            [
                [{"symbol": "AAPL"}],
                [{"cnt": 0}],
                [],
            ]
        )
        resp = client.get("/filings/1", params={"offset": 5, "limit": 10})
        assert resp.status_code == 200

        count_params = cursors[1].execute.call_args[0][1]
        assert "limit" not in count_params
        assert "offset" not in count_params

    def test_filing_type_filter_passed_to_queries(self) -> None:
        _, cursors = _with_conn(
            [
                [{"symbol": "AAPL"}],
                [{"cnt": 1}],
                [_make_filing_row()],
            ]
        )
        resp = client.get("/filings/1", params={"filing_type": "10-K"})
        assert resp.status_code == 200

        count_sql: str = cursors[1].execute.call_args[0][0]
        assert "filing_type" in count_sql.lower()
        count_params = cursors[1].execute.call_args[0][1]
        assert count_params["filing_type"] == "10-K"

    def test_no_filing_type_filter_omits_clause(self) -> None:
        _, cursors = _with_conn(
            [
                [{"symbol": "AAPL"}],
                [{"cnt": 0}],
                [],
            ]
        )
        client.get("/filings/1")

        count_params = cursors[1].execute.call_args[0][1]
        assert "filing_type" not in count_params

    def test_nullable_fields_returned_as_null(self) -> None:
        row = _make_filing_row(
            filing_type=None,
            source_url=None,
            primary_document_url=None,
            extracted_summary=None,
            red_flag_score=None,
        )
        _with_conn(
            [
                [{"symbol": "AAPL"}],
                [{"cnt": 1}],
                [row],
            ]
        )
        resp = client.get("/filings/1")

        assert resp.status_code == 200
        item = resp.json()["items"][0]
        assert item["filing_type"] is None
        assert item["source_url"] is None
        assert item["primary_document_url"] is None
        assert item["extracted_summary"] is None
        assert item["red_flag_score"] is None

    def test_items_ordered_by_filing_date_desc(self) -> None:
        _, cursors = _with_conn(
            [
                [{"symbol": "AAPL"}],
                [{"cnt": 0}],
                [],
            ]
        )
        client.get("/filings/1")

        items_sql: str = cursors[2].execute.call_args[0][0]
        assert "order by filing_date desc" in items_sql.lower()

    def test_limit_capped_at_max(self) -> None:
        resp = client.get("/filings/1", params={"limit": 999})
        assert resp.status_code == 422

    def test_negative_offset_rejected(self) -> None:
        resp = client.get("/filings/1", params={"offset": -1})
        assert resp.status_code == 422

    def test_zero_limit_rejected(self) -> None:
        resp = client.get("/filings/1", params={"limit": 0})
        assert resp.status_code == 422

    def test_uses_separate_cursors_per_query(self) -> None:
        """Each query must use its own cursor (prevention log #77)."""
        _, cursors = _with_conn(
            [
                [{"symbol": "AAPL"}],
                [{"cnt": 0}],
                [],
            ]
        )
        client.get("/filings/1")

        assert len(cursors) == 3
