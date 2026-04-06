"""
Tests for app.api.recommendations — recommendation list and detail endpoints.

Test strategy:
  Mock DB via FastAPI dependency override (same pattern as test_api_instruments).

Structure:
  - TestListRecommendations — pagination, filters, HOLD dedup, empty state
  - TestGetRecommendation  — happy path, 404, nullable fields
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


def _make_rec_row(
    recommendation_id: int = 1,
    instrument_id: int = 1,
    symbol: str = "AAPL",
    company_name: str = "Apple Inc",
    action: str = "BUY",
    status: str = "proposed",
    rationale: str = "Strong fundamentals",
    score_id: int | None = 10,
    model_version: str | None = "v1-balanced",
    suggested_size_pct: float | None = 0.05,
    target_entry: float | None = 185.00,
    cash_balance_known: bool | None = True,
    created_at: datetime = _NOW,
    total_score: float | None = None,
) -> dict[str, Any]:
    """Build a dict matching the recommendations query shape."""
    return {
        "recommendation_id": recommendation_id,
        "instrument_id": instrument_id,
        "symbol": symbol,
        "company_name": company_name,
        "action": action,
        "status": status,
        "rationale": rationale,
        "score_id": score_id,
        "model_version": model_version,
        "suggested_size_pct": suggested_size_pct,
        "target_entry": target_entry,
        "cash_balance_known": cash_balance_known,
        "created_at": created_at,
        "total_score": total_score,
    }


def _mock_conn(cursor_results: list[list[dict[str, Any]]]) -> MagicMock:
    cur = MagicMock()
    result_iter = iter(cursor_results)

    def _on_execute(*_args: Any, **_kwargs: Any) -> None:
        rows = next(result_iter)
        cur.fetchone.return_value = rows[0] if rows else None
        cur.fetchall.return_value = rows

    cur.execute.side_effect = _on_execute
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)

    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn


def _with_conn(cursor_results: list[list[dict[str, Any]]]) -> MagicMock:
    conn = _mock_conn(cursor_results)

    def _override() -> Iterator[MagicMock]:
        yield conn

    app.dependency_overrides[get_conn] = _override
    return conn


def _cleanup() -> None:
    app.dependency_overrides[get_conn] = _fallback_conn


def _fallback_conn() -> Iterator[MagicMock]:
    yield _mock_conn([])


app.dependency_overrides.setdefault(get_conn, _fallback_conn)

client = TestClient(app)


# ---------------------------------------------------------------------------
# TestListRecommendations
# ---------------------------------------------------------------------------


class TestListRecommendations:
    """GET /recommendations — paginated list with HOLD dedup."""

    def teardown_method(self) -> None:
        _cleanup()

    def test_happy_path_returns_items(self) -> None:
        row = _make_rec_row()
        _with_conn([[{"cnt": 1}], [row]])

        resp = client.get("/recommendations")
        assert resp.status_code == 200
        body = resp.json()

        assert body["total"] == 1
        assert body["offset"] == 0
        assert body["limit"] == 50
        assert len(body["items"]) == 1

        item = body["items"][0]
        assert item["recommendation_id"] == 1
        assert item["symbol"] == "AAPL"
        assert item["action"] == "BUY"
        assert item["status"] == "proposed"
        assert item["rationale"] == "Strong fundamentals"
        assert item["score_id"] == 10
        assert item["suggested_size_pct"] == 0.05
        assert item["target_entry"] == 185.00
        assert item["cash_balance_known"] is True

    def test_empty_history_returns_empty_list(self) -> None:
        _with_conn([[{"cnt": 0}], []])

        resp = client.get("/recommendations")
        assert resp.status_code == 200
        body = resp.json()
        assert body["items"] == []
        assert body["total"] == 0

    def test_filter_by_action(self) -> None:
        row = _make_rec_row(action="EXIT")
        conn = _with_conn([[{"cnt": 1}], [row]])

        resp = client.get("/recommendations", params={"action": "EXIT"})
        assert resp.status_code == 200

        cur = conn.cursor.return_value
        count_params = cur.execute.call_args_list[0][0][1]
        assert count_params["action"] == "EXIT"

    def test_filter_by_status(self) -> None:
        row = _make_rec_row(status="executed")
        conn = _with_conn([[{"cnt": 1}], [row]])

        resp = client.get("/recommendations", params={"status": "executed"})
        assert resp.status_code == 200

        cur = conn.cursor.return_value
        count_params = cur.execute.call_args_list[0][0][1]
        assert count_params["status"] == "executed"

    def test_filter_by_instrument_id(self) -> None:
        row = _make_rec_row(instrument_id=42)
        conn = _with_conn([[{"cnt": 1}], [row]])

        resp = client.get("/recommendations", params={"instrument_id": 42})
        assert resp.status_code == 200

        cur = conn.cursor.return_value
        count_params = cur.execute.call_args_list[0][0][1]
        assert count_params["instrument_id"] == 42

    def test_pagination_offset_and_limit(self) -> None:
        row = _make_rec_row()
        conn = _with_conn([[{"cnt": 100}], [row]])

        resp = client.get("/recommendations", params={"offset": 10, "limit": 25})
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 100
        assert body["offset"] == 10
        assert body["limit"] == 25

        cur = conn.cursor.return_value
        items_params = cur.execute.call_args_list[1][0][1]
        assert items_params["offset"] == 10
        assert items_params["limit"] == 25

    def test_limit_capped_at_max(self) -> None:
        resp = client.get("/recommendations", params={"limit": 999})
        assert resp.status_code == 422

    def test_negative_offset_rejected(self) -> None:
        resp = client.get("/recommendations", params={"offset": -1})
        assert resp.status_code == 422

    def test_zero_limit_rejected(self) -> None:
        resp = client.get("/recommendations", params={"limit": 0})
        assert resp.status_code == 422

    def test_invalid_action_rejected(self) -> None:
        """Invalid action value rejected by Literal validation."""
        resp = client.get("/recommendations", params={"action": "NUKE"})
        assert resp.status_code == 422

    def test_invalid_status_rejected(self) -> None:
        """Invalid status value rejected by Literal validation."""
        resp = client.get("/recommendations", params={"status": "banana"})
        assert resp.status_code == 422

    def test_count_query_receives_only_filter_params(self) -> None:
        """COUNT query must not receive limit/offset keys."""
        conn = _with_conn([[{"cnt": 5}], []])
        client.get("/recommendations", params={"action": "BUY", "offset": 10, "limit": 25})

        cur = conn.cursor.return_value
        count_params = cur.execute.call_args_list[0][0][1]
        assert "limit" not in count_params
        assert "offset" not in count_params
        assert count_params["action"] == "BUY"

    def test_hold_dedup_cte_present_in_query(self) -> None:
        """The list query must use the HOLD dedup CTE."""
        conn = _with_conn([[{"cnt": 0}], []])
        client.get("/recommendations")

        cur = conn.cursor.return_value
        count_sql: str = cur.execute.call_args_list[0][0][0]
        assert "deduped" in count_sql.lower()
        assert "row_number" in count_sql.lower()
        assert "rn = 1" in count_sql

    def test_nullable_optional_fields(self) -> None:
        """score_id, model_version, suggested_size_pct, target_entry, cash_balance_known can be null."""
        row = _make_rec_row(
            score_id=None,
            model_version=None,
            suggested_size_pct=None,
            target_entry=None,
            cash_balance_known=None,
        )
        _with_conn([[{"cnt": 1}], [row]])

        resp = client.get("/recommendations")
        assert resp.status_code == 200
        item = resp.json()["items"][0]
        assert item["score_id"] is None
        assert item["model_version"] is None
        assert item["suggested_size_pct"] is None
        assert item["target_entry"] is None
        assert item["cash_balance_known"] is None

    def test_ordering_in_items_query(self) -> None:
        """Items query must ORDER BY created_at DESC, recommendation_id DESC."""
        conn = _with_conn([[{"cnt": 0}], []])
        client.get("/recommendations")

        cur = conn.cursor.return_value
        items_sql: str = cur.execute.call_args_list[1][0][0]
        assert "ORDER BY d.created_at DESC, d.recommendation_id DESC" in items_sql


# ---------------------------------------------------------------------------
# TestGetRecommendation
# ---------------------------------------------------------------------------


class TestGetRecommendation:
    """GET /recommendations/{recommendation_id} — single detail."""

    def teardown_method(self) -> None:
        _cleanup()

    def test_happy_path_with_score(self) -> None:
        row = _make_rec_row(total_score=0.72)
        _with_conn([[row]])

        resp = client.get("/recommendations/1")
        assert resp.status_code == 200
        body = resp.json()

        assert body["recommendation_id"] == 1
        assert body["symbol"] == "AAPL"
        assert body["action"] == "BUY"
        assert body["total_score"] == 0.72
        assert body["score_id"] == 10

    def test_not_found_returns_404(self) -> None:
        _with_conn([[]])

        resp = client.get("/recommendations/999")
        assert resp.status_code == 404
        assert "999" in resp.json()["detail"]

    def test_no_linked_score_returns_null_total_score(self) -> None:
        row = _make_rec_row(score_id=None, total_score=None)
        _with_conn([[row]])

        resp = client.get("/recommendations/1")
        assert resp.status_code == 200
        body = resp.json()
        assert body["score_id"] is None
        assert body["total_score"] is None

    def test_nullable_optional_fields(self) -> None:
        row = _make_rec_row(
            score_id=None,
            model_version=None,
            suggested_size_pct=None,
            target_entry=None,
            cash_balance_known=None,
            total_score=None,
        )
        _with_conn([[row]])

        resp = client.get("/recommendations/1")
        assert resp.status_code == 200
        body = resp.json()
        assert body["model_version"] is None
        assert body["suggested_size_pct"] is None
        assert body["target_entry"] is None
        assert body["cash_balance_known"] is None
