"""
Tests for app.api.news — news feed endpoint.

Test strategy:
  Mock DB via FastAPI dependency override.  The ``get_conn`` dependency is
  replaced with a mock connection.  Because the endpoint uses separate
  ``with conn.cursor() as cur:`` blocks for each query, the mock creates
  a fresh cursor per ``cursor()`` call, each pre-loaded with its own
  result set.

Structure:
  - TestListNews — happy path, empty, pagination, since filter,
                   nullable fields, instrument not found, validation
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from app.db import get_conn
from app.main import app

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 4, 6, 12, 0, 0, tzinfo=UTC)


def _make_news_row(
    news_event_id: int = 1,
    instrument_id: int = 1,
    event_time: datetime = _NOW,
    source: str | None = "reuters",
    headline: str = "Apple beats earnings",
    category: str | None = "earnings",
    sentiment_score: float | None = 0.75,
    importance_score: float | None = 0.90,
    snippet: str | None = "Apple reported Q1 earnings above expectations.",
    url: str | None = "https://example.com/news/1",
) -> dict[str, Any]:
    return {
        "news_event_id": news_event_id,
        "instrument_id": instrument_id,
        "event_time": event_time,
        "source": source,
        "headline": headline,
        "category": category,
        "sentiment_score": sentiment_score,
        "importance_score": importance_score,
        "snippet": snippet,
        "url": url,
    }


def _mock_conn(
    cursor_result_sets: list[list[dict[str, Any]]],
) -> tuple[MagicMock, list[MagicMock]]:
    """Build a mock psycopg.Connection with separate cursors per call.

    Returns ``(conn, cursors)`` where ``cursors`` accumulates every cursor
    created, allowing callers to inspect SQL and params per query.
    """
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
# TestListNews
# ---------------------------------------------------------------------------


class TestListNews:
    """GET /news/{instrument_id} — recent news events."""

    def teardown_method(self) -> None:
        _cleanup()

    def test_happy_path_returns_news_items(self) -> None:
        row = _make_news_row()
        # Cursor sequence: instrument lookup, COUNT, items
        _with_conn(
            [
                [{"symbol": "AAPL"}],
                [{"cnt": 1}],
                [row],
            ]
        )
        resp = client.get("/news/1")

        assert resp.status_code == 200
        body = resp.json()
        assert body["instrument_id"] == 1
        assert body["symbol"] == "AAPL"
        assert body["total"] == 1
        assert body["offset"] == 0
        assert body["limit"] == 50
        assert len(body["items"]) == 1

        item = body["items"][0]
        assert item["news_event_id"] == 1
        assert item["headline"] == "Apple beats earnings"
        assert item["sentiment_score"] == 0.75
        assert item["importance_score"] == 0.90
        assert item["snippet"] == "Apple reported Q1 earnings above expectations."
        assert item["url"] == "https://example.com/news/1"

    def test_instrument_not_found_returns_404(self) -> None:
        _with_conn([[]])  # instrument lookup returns no rows
        resp = client.get("/news/999")

        assert resp.status_code == 404

    def test_no_news_returns_empty_list(self) -> None:
        _with_conn(
            [
                [{"symbol": "AAPL"}],
                [{"cnt": 0}],
                [],
            ]
        )
        resp = client.get("/news/1")

        assert resp.status_code == 200
        body = resp.json()
        assert body["items"] == []
        assert body["total"] == 0

    def test_pagination_offset_and_limit(self) -> None:
        row = _make_news_row()
        _with_conn(
            [
                [{"symbol": "AAPL"}],
                [{"cnt": 100}],
                [row],
            ]
        )
        resp = client.get("/news/1", params={"offset": 10, "limit": 25})

        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 100
        assert body["offset"] == 10
        assert body["limit"] == 25

    def test_items_query_receives_limit_and_offset(self) -> None:
        _, cursors = _with_conn(
            [
                [{"symbol": "AAPL"}],
                [{"cnt": 1}],
                [_make_news_row()],
            ]
        )
        client.get("/news/1", params={"offset": 5, "limit": 10})

        # Third cursor is the items query.
        items_params = cursors[2].execute.call_args[0][1]
        assert items_params["limit"] == 10
        assert items_params["offset"] == 5

    def test_count_query_receives_no_limit_offset(self) -> None:
        """COUNT query params must not contain limit/offset keys."""
        _, cursors = _with_conn(
            [
                [{"symbol": "AAPL"}],
                [{"cnt": 0}],
                [],
            ]
        )
        resp = client.get("/news/1", params={"offset": 5, "limit": 10})
        assert resp.status_code == 200

        count_params = cursors[1].execute.call_args[0][1]
        assert "limit" not in count_params
        assert "offset" not in count_params

    def test_since_param_passed_to_queries(self) -> None:
        _, cursors = _with_conn(
            [
                [{"symbol": "AAPL"}],
                [{"cnt": 0}],
                [],
            ]
        )
        resp = client.get("/news/1", params={"since": "2026-03-01T00:00:00Z"})
        assert resp.status_code == 200

        count_params = cursors[1].execute.call_args[0][1]
        assert count_params["since"] == datetime(2026, 3, 1, tzinfo=UTC)

    def test_nullable_fields_returned_as_null(self) -> None:
        row = _make_news_row(
            source=None,
            category=None,
            sentiment_score=None,
            importance_score=None,
            snippet=None,
            url=None,
        )
        _with_conn(
            [
                [{"symbol": "AAPL"}],
                [{"cnt": 1}],
                [row],
            ]
        )
        resp = client.get("/news/1")

        assert resp.status_code == 200
        item = resp.json()["items"][0]
        assert item["source"] is None
        assert item["category"] is None
        assert item["sentiment_score"] is None
        assert item["importance_score"] is None
        assert item["snippet"] is None
        assert item["url"] is None

    def test_items_ordered_by_event_time_desc(self) -> None:
        _, cursors = _with_conn(
            [
                [{"symbol": "AAPL"}],
                [{"cnt": 0}],
                [],
            ]
        )
        client.get("/news/1")

        items_sql: str = cursors[2].execute.call_args[0][0]
        assert "order by event_time desc" in items_sql.lower()

    def test_limit_capped_at_max(self) -> None:
        resp = client.get("/news/1", params={"limit": 999})
        assert resp.status_code == 422

    def test_negative_offset_rejected(self) -> None:
        resp = client.get("/news/1", params={"offset": -1})
        assert resp.status_code == 422

    def test_zero_limit_rejected(self) -> None:
        resp = client.get("/news/1", params={"limit": 0})
        assert resp.status_code == 422

    def test_uses_separate_cursors_per_query(self) -> None:
        """Each query must use its own cursor (prevention log #77)."""
        conn, cursors = _with_conn(
            [
                [{"symbol": "AAPL"}],
                [{"cnt": 0}],
                [],
            ]
        )
        client.get("/news/1")

        assert len(cursors) == 3
