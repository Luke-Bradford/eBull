"""
Tests for app.api.theses — latest thesis and thesis history endpoints.

Test strategy:
  Mock DB via FastAPI dependency override, matching the pattern from
  test_api_scores.  The ``get_conn`` dependency is replaced with
  a mock connection returning ``dict_row``-style dicts.

Structure:
  - TestGetLatestThesis   — happy path, 404, nullable fields
  - TestGetThesisHistory  — happy path, empty, pagination, instrument-not-found 404
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
_EARLIER = datetime(2026, 4, 5, 12, 0, 0, tzinfo=UTC)


def _make_thesis_row(
    thesis_id: int = 1,
    instrument_id: int = 100,
    thesis_version: int = 1,
    thesis_type: str = "compounder",
    stance: str = "buy",
    confidence_score: float | None = 0.85,
    buy_zone_low: float | None = 140.0,
    buy_zone_high: float | None = 155.0,
    base_value: float | None = 200.0,
    bull_value: float | None = 250.0,
    bear_value: float | None = 120.0,
    break_conditions_json: list[str] | None = None,
    memo_markdown: str = "Strong compounder thesis.",
    critic_json: dict[str, object] | None = None,
    created_at: datetime = _NOW,
) -> dict[str, Any]:
    """Build a dict matching the theses query shape."""
    return {
        "thesis_id": thesis_id,
        "instrument_id": instrument_id,
        "thesis_version": thesis_version,
        "thesis_type": thesis_type,
        "stance": stance,
        "confidence_score": confidence_score,
        "buy_zone_low": buy_zone_low,
        "buy_zone_high": buy_zone_high,
        "base_value": base_value,
        "bull_value": bull_value,
        "bear_value": bear_value,
        "break_conditions_json": break_conditions_json,
        "memo_markdown": memo_markdown,
        "critic_json": critic_json,
        "created_at": created_at,
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
# TestGetLatestThesis
# ---------------------------------------------------------------------------


class TestGetLatestThesis:
    """GET /theses/{instrument_id} — latest thesis for an instrument."""

    def teardown_method(self) -> None:
        _cleanup()

    def test_happy_path_returns_latest_thesis(self) -> None:
        row = _make_thesis_row(
            thesis_id=5,
            thesis_version=3,
            thesis_type="compounder",
            stance="buy",
            confidence_score=0.85,
            memo_markdown="Strong thesis.",
            critic_json={"overall": "sound"},
        )
        _with_conn([[row]])
        resp = client.get("/theses/100")

        assert resp.status_code == 200
        body = resp.json()
        assert body["thesis_id"] == 5
        assert body["instrument_id"] == 100
        assert body["thesis_version"] == 3
        assert body["thesis_type"] == "compounder"
        assert body["stance"] == "buy"
        assert body["confidence_score"] == 0.85
        assert body["memo_markdown"] == "Strong thesis."
        assert body["critic_json"] == {"overall": "sound"}
        assert body["created_at"] is not None

    def test_no_thesis_returns_404(self) -> None:
        _with_conn([[]])
        resp = client.get("/theses/999")

        assert resp.status_code == 404
        assert "No thesis found" in resp.json()["detail"]

    def test_nullable_numeric_fields_returned_as_null(self) -> None:
        """All nullable numeric fields can be None without crashing."""
        row = _make_thesis_row(
            confidence_score=None,
            buy_zone_low=None,
            buy_zone_high=None,
            base_value=None,
            bull_value=None,
            bear_value=None,
            break_conditions_json=None,
            critic_json=None,
        )
        _with_conn([[row]])
        resp = client.get("/theses/100")

        assert resp.status_code == 200
        body = resp.json()
        assert body["confidence_score"] is None
        assert body["buy_zone_low"] is None
        assert body["buy_zone_high"] is None
        assert body["base_value"] is None
        assert body["bull_value"] is None
        assert body["bear_value"] is None
        assert body["break_conditions_json"] is None
        assert body["critic_json"] is None

    def test_valuation_fields_returned_correctly(self) -> None:
        row = _make_thesis_row(
            buy_zone_low=140.50,
            buy_zone_high=155.25,
            base_value=200.0,
            bull_value=250.0,
            bear_value=120.0,
        )
        _with_conn([[row]])
        resp = client.get("/theses/100")

        assert resp.status_code == 200
        body = resp.json()
        assert body["buy_zone_low"] == 140.50
        assert body["buy_zone_high"] == 155.25
        assert body["base_value"] == 200.0
        assert body["bull_value"] == 250.0
        assert body["bear_value"] == 120.0

    def test_query_orders_by_created_at_desc_then_version_desc(self) -> None:
        conn = _with_conn([[_make_thesis_row()]])
        client.get("/theses/100")

        cur = conn.cursor.return_value
        sql: str = cur.execute.call_args_list[0][0][0]
        sql_lower = sql.lower()
        assert "order by" in sql_lower
        assert "created_at desc" in sql_lower
        assert "thesis_version desc" in sql_lower
        assert "limit 1" in sql_lower


# ---------------------------------------------------------------------------
# TestGetThesisHistory
# ---------------------------------------------------------------------------


class TestGetThesisHistory:
    """GET /theses/{instrument_id}/history — paginated thesis history."""

    def teardown_method(self) -> None:
        _cleanup()

    def test_happy_path_returns_ordered_theses(self) -> None:
        row_v2 = _make_thesis_row(thesis_id=2, thesis_version=2, created_at=_NOW)
        row_v1 = _make_thesis_row(thesis_id=1, thesis_version=1, created_at=_EARLIER)
        # Query sequence: EXISTS instrument, COUNT, data
        _with_conn(
            [
                [{"_": 1}],
                [{"cnt": 2}],
                [row_v2, row_v1],
            ]
        )
        resp = client.get("/theses/100/history")

        assert resp.status_code == 200
        body = resp.json()
        assert body["instrument_id"] == 100
        assert body["total"] == 2
        assert body["offset"] == 0
        assert body["limit"] == 50
        assert len(body["items"]) == 2
        assert body["items"][0]["thesis_version"] == 2
        assert body["items"][1]["thesis_version"] == 1

    def test_instrument_not_found_returns_404(self) -> None:
        """If the instrument doesn't exist, return 404."""
        _with_conn([[]])
        resp = client.get("/theses/999/history")

        assert resp.status_code == 404
        assert "Instrument 999 not found" in resp.json()["detail"]

    def test_instrument_exists_but_no_theses_returns_empty(self) -> None:
        """Instrument exists but has no thesis rows → 200 with empty items."""
        _with_conn(
            [
                [{"_": 1}],
                [{"cnt": 0}],
            ]
        )
        resp = client.get("/theses/100/history")

        assert resp.status_code == 200
        body = resp.json()
        assert body["instrument_id"] == 100
        assert body["items"] == []
        assert body["total"] == 0

    def test_pagination_offset_and_limit(self) -> None:
        row = _make_thesis_row()
        conn = _with_conn(
            [
                [{"_": 1}],
                [{"cnt": 20}],
                [row],
            ]
        )
        resp = client.get("/theses/100/history", params={"offset": 5, "limit": 10})

        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 20
        assert body["offset"] == 5
        assert body["limit"] == 10

        cur = conn.cursor.return_value
        data_params = cur.execute.call_args_list[2][0][1]
        assert data_params["offset"] == 5
        assert data_params["limit"] == 10

    def test_count_query_receives_no_limit_offset(self) -> None:
        """COUNT query params must not contain limit/offset keys."""
        conn = _with_conn(
            [
                [{"_": 1}],
                [{"cnt": 3}],
                [_make_thesis_row()],
            ]
        )
        client.get("/theses/100/history", params={"offset": 5, "limit": 10})

        cur = conn.cursor.return_value
        count_params = cur.execute.call_args_list[1][0][1]
        assert "limit" not in count_params
        assert "offset" not in count_params

    def test_limit_capped_at_max(self) -> None:
        resp = client.get("/theses/100/history", params={"limit": 999})
        assert resp.status_code == 422

    def test_negative_offset_rejected(self) -> None:
        resp = client.get("/theses/100/history", params={"offset": -1})
        assert resp.status_code == 422

    def test_zero_limit_rejected(self) -> None:
        resp = client.get("/theses/100/history", params={"limit": 0})
        assert resp.status_code == 422

    def test_nullable_fields_in_history_items(self) -> None:
        row = _make_thesis_row(
            confidence_score=None,
            buy_zone_low=None,
            buy_zone_high=None,
            base_value=None,
            bull_value=None,
            bear_value=None,
            break_conditions_json=None,
            critic_json=None,
        )
        _with_conn(
            [
                [{"_": 1}],
                [{"cnt": 1}],
                [row],
            ]
        )
        resp = client.get("/theses/100/history")

        assert resp.status_code == 200
        item = resp.json()["items"][0]
        assert item["confidence_score"] is None
        assert item["buy_zone_low"] is None
        assert item["critic_json"] is None

    def test_history_query_orders_by_created_at_desc_then_version_desc(self) -> None:
        conn = _with_conn(
            [
                [{"_": 1}],
                [{"cnt": 1}],
                [_make_thesis_row()],
            ]
        )
        client.get("/theses/100/history")

        cur = conn.cursor.return_value
        data_sql: str = cur.execute.call_args_list[2][0][0]
        sql_lower = data_sql.lower()
        assert "order by" in sql_lower
        assert "created_at desc" in sql_lower
        assert "thesis_version desc" in sql_lower
