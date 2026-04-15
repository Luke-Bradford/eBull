"""
Tests for app.api.scores — rankings list and score history endpoints.

Test strategy:
  Mock DB via FastAPI dependency override, matching the pattern from
  test_api_instruments.  The ``get_conn`` dependency is replaced with
  a mock connection returning ``dict_row``-style dicts.

Structure:
  - TestListRankings       — happy path, empty run, filters, pagination, nullable fields
  - TestGetScoreHistory    — happy path, empty history, nullable fields
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


def _make_ranking_row(
    instrument_id: int = 1,
    symbol: str = "AAPL",
    company_name: str = "Apple Inc",
    sector: str | None = "Technology",
    coverage_tier: int | None = 1,
    rank: int | None = 1,
    rank_delta: int | None = -2,
    total_score: float | None = 0.82,
    raw_total: float | None = 0.87,
    quality_score: float | None = 0.90,
    value_score: float | None = 0.75,
    turnaround_score: float | None = 0.60,
    momentum_score: float | None = 0.70,
    sentiment_score: float | None = 0.50,
    confidence_score: float | None = 0.85,
    penalties_json: list[dict[str, object]] | None = None,
    explanation: str | None = "Strong quality + value",
    model_version: str = "v1.1-balanced",
    scored_at: datetime = _NOW,
) -> dict[str, Any]:
    """Build a dict matching the joined scores+instruments+coverage query shape."""
    return {
        "instrument_id": instrument_id,
        "symbol": symbol,
        "company_name": company_name,
        "sector": sector,
        "coverage_tier": coverage_tier,
        "rank": rank,
        "rank_delta": rank_delta,
        "total_score": total_score,
        "raw_total": raw_total,
        "quality_score": quality_score,
        "value_score": value_score,
        "turnaround_score": turnaround_score,
        "momentum_score": momentum_score,
        "sentiment_score": sentiment_score,
        "confidence_score": confidence_score,
        "penalties_json": penalties_json,
        "explanation": explanation,
        "model_version": model_version,
        "scored_at": scored_at,
    }


def _make_history_row(
    scored_at: datetime = _NOW,
    total_score: float | None = 0.82,
    raw_total: float | None = 0.87,
    quality_score: float | None = 0.90,
    value_score: float | None = 0.75,
    turnaround_score: float | None = 0.60,
    momentum_score: float | None = 0.70,
    sentiment_score: float | None = 0.50,
    confidence_score: float | None = 0.85,
    penalties_json: list[dict[str, object]] | None = None,
    explanation: str | None = "Strong quality + value",
    rank: int | None = 1,
    rank_delta: int | None = -2,
    model_version: str = "v1.1-balanced",
) -> dict[str, Any]:
    return {
        "scored_at": scored_at,
        "total_score": total_score,
        "raw_total": raw_total,
        "quality_score": quality_score,
        "value_score": value_score,
        "turnaround_score": turnaround_score,
        "momentum_score": momentum_score,
        "sentiment_score": sentiment_score,
        "confidence_score": confidence_score,
        "penalties_json": penalties_json,
        "explanation": explanation,
        "rank": rank,
        "rank_delta": rank_delta,
        "model_version": model_version,
    }


def _mock_conn(cursor_results: list[list[dict[str, Any]]]) -> MagicMock:
    """Build a mock psycopg.Connection.

    ``cursor_results`` is a list of result sets, one per ``cur.execute()`` call.
    Each ``fetchone()`` returns the first row (or None), and ``fetchall()``
    returns the full list.
    """
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
    """Set up a mock connection as the get_conn dependency override."""
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
# TestListRankings
# ---------------------------------------------------------------------------


class TestListRankings:
    """GET /rankings — latest scoring run with optional filters."""

    def teardown_method(self) -> None:
        _cleanup()

    def test_happy_path_returns_ranked_items(self) -> None:
        row = _make_ranking_row()
        # Query sequence: MAX(scored_at), COUNT, items
        _with_conn(
            [
                [{"latest": _NOW}],
                [{"cnt": 1}],
                [row],
            ]
        )
        resp = client.get("/rankings")

        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 1
        assert body["offset"] == 0
        assert body["limit"] == 50
        assert body["model_version"] == "v1.1-balanced"
        assert body["scored_at"] is not None
        assert len(body["items"]) == 1

        item = body["items"][0]
        assert item["instrument_id"] == 1
        assert item["symbol"] == "AAPL"
        assert item["rank"] == 1
        assert item["rank_delta"] == -2
        assert item["total_score"] == 0.82
        assert item["raw_total"] == 0.87
        assert item["quality_score"] == 0.90
        assert item["coverage_tier"] == 1

    def test_no_scoring_runs_returns_empty(self) -> None:
        """No run for this model_version → empty list, total=0, scored_at=null."""
        _with_conn([[{"latest": None}]])
        resp = client.get("/rankings")

        assert resp.status_code == 200
        body = resp.json()
        assert body["items"] == []
        assert body["total"] == 0
        assert body["scored_at"] is None

    def test_filter_by_sector(self) -> None:
        row = _make_ranking_row(sector="Technology")
        conn = _with_conn(
            [
                [{"latest": _NOW}],
                [{"cnt": 1}],
                [row],
            ]
        )
        resp = client.get("/rankings", params={"sector": "Technology"})

        assert resp.status_code == 200
        cur = conn.cursor.return_value
        # COUNT query is execute call index 1 (after MAX query)
        count_params = cur.execute.call_args_list[1][0][1]
        assert count_params["sector"] == "Technology"

    def test_filter_by_coverage_tier(self) -> None:
        row = _make_ranking_row(coverage_tier=1)
        conn = _with_conn(
            [
                [{"latest": _NOW}],
                [{"cnt": 1}],
                [row],
            ]
        )
        resp = client.get("/rankings", params={"coverage_tier": 1})

        assert resp.status_code == 200
        cur = conn.cursor.return_value
        count_params = cur.execute.call_args_list[1][0][1]
        assert count_params["coverage_tier"] == 1

    def test_filter_by_stance_adds_lateral_join(self) -> None:
        row = _make_ranking_row()
        conn = _with_conn(
            [
                [{"latest": _NOW}],
                [{"cnt": 1}],
                [row],
            ]
        )
        resp = client.get("/rankings", params={"stance": "buy"})

        assert resp.status_code == 200
        cur = conn.cursor.return_value
        # Both COUNT and items queries should contain the LATERAL join
        count_sql: str = cur.execute.call_args_list[1][0][0]
        assert "lateral" in count_sql.lower()
        assert "theses" in count_sql.lower()

        items_sql: str = cur.execute.call_args_list[2][0][0]
        assert "lateral" in items_sql.lower()

    def test_no_stance_filter_omits_lateral_join(self) -> None:
        conn = _with_conn(
            [
                [{"latest": _NOW}],
                [{"cnt": 0}],
                [],
            ]
        )
        client.get("/rankings")

        cur = conn.cursor.return_value
        count_sql: str = cur.execute.call_args_list[1][0][0]
        assert "lateral" not in count_sql.lower()
        assert "theses" not in count_sql.lower()

    def test_pagination_offset_and_limit(self) -> None:
        row = _make_ranking_row()
        conn = _with_conn(
            [
                [{"latest": _NOW}],
                [{"cnt": 100}],
                [row],
            ]
        )
        resp = client.get("/rankings", params={"offset": 10, "limit": 25})

        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 100
        assert body["offset"] == 10
        assert body["limit"] == 25

        cur = conn.cursor.return_value
        items_params = cur.execute.call_args_list[2][0][1]
        assert items_params["offset"] == 10
        assert items_params["limit"] == 25

    def test_count_query_receives_no_limit_offset(self) -> None:
        """COUNT query params must not contain limit/offset keys."""
        conn = _with_conn(
            [
                [{"latest": _NOW}],
                [{"cnt": 5}],
                [],
            ]
        )
        resp = client.get("/rankings", params={"sector": "Tech", "offset": 10, "limit": 25})
        assert resp.status_code == 200

        cur = conn.cursor.return_value
        count_params = cur.execute.call_args_list[1][0][1]
        assert "limit" not in count_params
        assert "offset" not in count_params
        assert count_params["sector"] == "Tech"

    def test_nullable_rank_and_scores_returned_as_null(self) -> None:
        """All nullable numeric fields can be None without crashing."""
        row = _make_ranking_row(
            rank=None,
            rank_delta=None,
            total_score=None,
            raw_total=None,
            quality_score=None,
            value_score=None,
            turnaround_score=None,
            momentum_score=None,
            sentiment_score=None,
            confidence_score=None,
            penalties_json=None,
            explanation=None,
        )
        _with_conn(
            [
                [{"latest": _NOW}],
                [{"cnt": 1}],
                [row],
            ]
        )
        resp = client.get("/rankings")

        assert resp.status_code == 200
        item = resp.json()["items"][0]
        assert item["rank"] is None
        assert item["rank_delta"] is None
        assert item["total_score"] is None
        assert item["raw_total"] is None
        assert item["quality_score"] is None
        assert item["penalties_json"] is None
        assert item["explanation"] is None

    def test_penalties_json_returned_as_array(self) -> None:
        penalties = [{"name": "stale_thesis", "deduction": 0.15, "reason": "thesis > 90 days"}]
        row = _make_ranking_row(penalties_json=penalties)
        _with_conn(
            [
                [{"latest": _NOW}],
                [{"cnt": 1}],
                [row],
            ]
        )
        resp = client.get("/rankings")

        assert resp.status_code == 200
        item = resp.json()["items"][0]
        assert len(item["penalties_json"]) == 1
        assert item["penalties_json"][0]["name"] == "stale_thesis"

    def test_limit_capped_at_max(self) -> None:
        resp = client.get("/rankings", params={"limit": 999})
        assert resp.status_code == 422

    def test_negative_offset_rejected(self) -> None:
        resp = client.get("/rankings", params={"offset": -1})
        assert resp.status_code == 422

    def test_zero_limit_rejected(self) -> None:
        resp = client.get("/rankings", params={"limit": 0})
        assert resp.status_code == 422

    def test_invalid_coverage_tier_rejected(self) -> None:
        resp = client.get("/rankings", params={"coverage_tier": 5})
        assert resp.status_code == 422

    def test_invalid_stance_rejected(self) -> None:
        resp = client.get("/rankings", params={"stance": "yolo"})
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# TestGetScoreHistory
# ---------------------------------------------------------------------------


class TestGetScoreHistory:
    """GET /rankings/history/{instrument_id} — score trend over time."""

    def teardown_method(self) -> None:
        _cleanup()

    def test_happy_path_returns_ordered_snapshots(self) -> None:
        earlier = datetime(2026, 4, 5, 12, 0, 0, tzinfo=UTC)
        rows = [
            _make_history_row(scored_at=_NOW, total_score=0.82, rank=1),
            _make_history_row(scored_at=earlier, total_score=0.78, rank=3),
        ]
        _with_conn([rows])
        resp = client.get("/rankings/history/1")

        assert resp.status_code == 200
        body = resp.json()
        assert body["instrument_id"] == 1
        assert len(body["items"]) == 2
        assert body["items"][0]["total_score"] == 0.82
        assert body["items"][0]["rank"] == 1
        assert body["items"][1]["total_score"] == 0.78
        assert body["items"][1]["rank"] == 3

    def test_no_scores_returns_empty_list(self) -> None:
        _with_conn([[]])
        resp = client.get("/rankings/history/999")

        assert resp.status_code == 200
        body = resp.json()
        assert body["instrument_id"] == 999
        assert body["items"] == []

    def test_nullable_fields_returned_as_null(self) -> None:
        row = _make_history_row(
            total_score=None,
            raw_total=None,
            quality_score=None,
            value_score=None,
            turnaround_score=None,
            momentum_score=None,
            sentiment_score=None,
            confidence_score=None,
            penalties_json=None,
            explanation=None,
            rank=None,
            rank_delta=None,
        )
        _with_conn([[row]])
        resp = client.get("/rankings/history/1")

        assert resp.status_code == 200
        item = resp.json()["items"][0]
        assert item["total_score"] is None
        assert item["rank"] is None
        assert item["rank_delta"] is None
        assert item["raw_total"] is None

    def test_custom_model_version(self) -> None:
        conn = _with_conn([[]])
        client.get("/rankings/history/1", params={"model_version": "v1-conservative"})

        cur = conn.cursor.return_value
        params = cur.execute.call_args_list[0][0][1]
        assert params["mv"] == "v1-conservative"

    def test_custom_limit(self) -> None:
        conn = _with_conn([[]])
        client.get("/rankings/history/1", params={"limit": 10})

        cur = conn.cursor.return_value
        params = cur.execute.call_args_list[0][0][1]
        assert params["limit"] == 10

    def test_limit_capped_at_max(self) -> None:
        resp = client.get("/rankings/history/1", params={"limit": 999})
        assert resp.status_code == 422

    def test_history_query_orders_by_scored_at_desc(self) -> None:
        conn = _with_conn([[]])
        client.get("/rankings/history/1")

        cur = conn.cursor.return_value
        sql: str = cur.execute.call_args_list[0][0][0]
        assert "order by s.scored_at desc" in sql.lower()
