"""
Tests for app.api.audit — execution audit trail endpoints.

Test strategy:
  Mock DB via FastAPI dependency override (same pattern as other API tests).

Structure:
  - TestListAudit  — pagination, filters, empty state, combined filters
  - TestGetAudit   — happy path with evidence_json, 404
  - TestFilterValidation — invalid pass_fail / stage returns 422
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


def _make_audit_row(
    decision_id: int = 1,
    decision_time: datetime = _NOW,
    instrument_id: int | None = 1,
    symbol: str | None = "AAPL",
    company_name: str | None = "Apple Inc",
    recommendation_id: int | None = 10,
    stage: str = "execution_guard",
    model_version: str | None = "v1-balanced",
    pass_fail: str = "PASS",
    explanation: str = "All rules passed",
    evidence_json: object | None = None,
) -> dict[str, Any]:
    """Build a dict matching the audit query shape."""
    return {
        "decision_id": decision_id,
        "decision_time": decision_time,
        "instrument_id": instrument_id,
        "symbol": symbol,
        "company_name": company_name,
        "recommendation_id": recommendation_id,
        "stage": stage,
        "model_version": model_version,
        "pass_fail": pass_fail,
        "explanation": explanation,
        "evidence_json": evidence_json,
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
    # Results must match query execution order: for list_audit, position 0 is
    # the COUNT query result and position 1 is the items query result.
    # For get_audit, position 0 is the single-row query result.
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
# TestListAudit
# ---------------------------------------------------------------------------


class TestListAudit:
    """GET /audit — paginated execution audit log."""

    def teardown_method(self) -> None:
        _cleanup()

    def test_happy_path_returns_items(self) -> None:
        row = _make_audit_row()
        _with_conn([[{"cnt": 1}], [row]])

        resp = client.get("/audit")
        assert resp.status_code == 200
        body = resp.json()

        assert body["total"] == 1
        assert body["offset"] == 0
        assert body["limit"] == 50
        assert len(body["items"]) == 1

        item = body["items"][0]
        assert item["decision_id"] == 1
        assert item["instrument_id"] == 1
        assert item["symbol"] == "AAPL"
        assert item["stage"] == "execution_guard"
        assert item["pass_fail"] == "PASS"
        assert item["explanation"] == "All rules passed"
        assert item["recommendation_id"] == 10
        assert item["model_version"] == "v1-balanced"

    def test_empty_state(self) -> None:
        _with_conn([[{"cnt": 0}], []])

        resp = client.get("/audit")
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 0
        assert body["items"] == []

    def test_pagination(self) -> None:
        _with_conn([[{"cnt": 100}], [_make_audit_row()]])

        resp = client.get("/audit?offset=10&limit=1")
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 100
        assert body["offset"] == 10
        assert body["limit"] == 1
        assert len(body["items"]) == 1

    def test_filter_by_instrument_id(self) -> None:
        _with_conn([[{"cnt": 1}], [_make_audit_row(instrument_id=42)]])

        resp = client.get("/audit?instrument_id=42")
        assert resp.status_code == 200
        assert len(resp.json()["items"]) == 1

    def test_filter_by_pass_fail(self) -> None:
        row = _make_audit_row(pass_fail="FAIL", explanation="Spread too wide")
        _with_conn([[{"cnt": 1}], [row]])

        resp = client.get("/audit?pass_fail=FAIL")
        assert resp.status_code == 200
        assert resp.json()["items"][0]["pass_fail"] == "FAIL"

    def test_filter_by_stage(self) -> None:
        row = _make_audit_row(stage="order_client")
        _with_conn([[{"cnt": 1}], [row]])

        resp = client.get("/audit?stage=order_client")
        assert resp.status_code == 200
        assert resp.json()["items"][0]["stage"] == "order_client"

    def test_filter_by_date_range(self) -> None:
        _with_conn([[{"cnt": 1}], [_make_audit_row()]])

        resp = client.get("/audit?date_from=2026-04-01T00:00:00Z&date_to=2026-04-07T00:00:00Z")
        assert resp.status_code == 200
        assert resp.json()["total"] == 1

    def test_combined_filters(self) -> None:
        row = _make_audit_row(pass_fail="FAIL", stage="execution_guard")
        _with_conn([[{"cnt": 1}], [row]])

        resp = client.get("/audit?pass_fail=FAIL&stage=execution_guard&instrument_id=1")
        assert resp.status_code == 200
        assert resp.json()["total"] == 1

    def test_nullable_instrument_fields(self) -> None:
        """Audit rows may have NULL instrument_id (future non-instrument audits)."""
        row = _make_audit_row(
            instrument_id=None,
            symbol=None,
            company_name=None,
            recommendation_id=None,
            model_version=None,
        )
        _with_conn([[{"cnt": 1}], [row]])

        resp = client.get("/audit")
        assert resp.status_code == 200
        item = resp.json()["items"][0]
        assert item["instrument_id"] is None
        assert item["symbol"] is None
        assert item["company_name"] is None
        assert item["recommendation_id"] is None
        assert item["model_version"] is None

    def test_list_omits_evidence_json(self) -> None:
        _with_conn([[{"cnt": 1}], [_make_audit_row()]])

        resp = client.get("/audit")
        assert resp.status_code == 200
        item = resp.json()["items"][0]
        assert "evidence_json" not in item


# ---------------------------------------------------------------------------
# TestGetAudit
# ---------------------------------------------------------------------------


class TestGetAudit:
    """GET /audit/{decision_id} — single audit row with evidence."""

    def teardown_method(self) -> None:
        _cleanup()

    def test_happy_path_with_evidence(self) -> None:
        evidence = [
            {"rule": "kill_switch", "passed": True, "detail": "inactive"},
            {"rule": "spread", "passed": False, "detail": "spread 3.2% > 2%"},
        ]
        row = _make_audit_row(evidence_json=evidence)
        _with_conn([[row]])

        resp = client.get("/audit/1")
        assert resp.status_code == 200
        body = resp.json()

        assert body["decision_id"] == 1
        assert body["pass_fail"] == "PASS"
        assert body["explanation"] == "All rules passed"
        assert len(body["evidence_json"]) == 2
        assert body["evidence_json"][0]["rule"] == "kill_switch"
        assert body["evidence_json"][1]["passed"] is False

    def test_evidence_json_dict_shape(self) -> None:
        """Order client writes evidence as a dict, not a list."""
        evidence = {"order_id": "abc-123", "raw_payload": {"status": "executed"}}
        row = _make_audit_row(
            stage="order_client",
            evidence_json=evidence,
        )
        _with_conn([[row]])

        resp = client.get("/audit/1")
        assert resp.status_code == 200
        body = resp.json()
        assert body["evidence_json"]["order_id"] == "abc-123"

    def test_evidence_json_null(self) -> None:
        row = _make_audit_row(evidence_json=None)
        _with_conn([[row]])

        resp = client.get("/audit/1")
        assert resp.status_code == 200
        assert resp.json()["evidence_json"] is None

    def test_not_found(self) -> None:
        _with_conn([[]])

        resp = client.get("/audit/999")
        assert resp.status_code == 404
        assert "999" in resp.json()["detail"]

    def test_nullable_fields_on_detail(self) -> None:
        row = _make_audit_row(
            instrument_id=None,
            symbol=None,
            company_name=None,
            recommendation_id=None,
            model_version=None,
            evidence_json=None,
        )
        _with_conn([[row]])

        resp = client.get("/audit/1")
        assert resp.status_code == 200
        body = resp.json()
        assert body["instrument_id"] is None
        assert body["recommendation_id"] is None
        assert body["evidence_json"] is None


# ---------------------------------------------------------------------------
# TestFilterValidation
# ---------------------------------------------------------------------------


class TestFilterValidation:
    """Invalid enum filter values should return 422."""

    def teardown_method(self) -> None:
        _cleanup()

    def test_invalid_pass_fail_returns_422(self) -> None:
        resp = client.get("/audit?pass_fail=NUKE")
        assert resp.status_code == 422

    def test_invalid_stage_returns_422(self) -> None:
        resp = client.get("/audit?stage=unknown_stage")
        assert resp.status_code == 422
