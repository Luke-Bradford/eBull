"""Tests for the alerts API (#315 Phase 3)."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services.operators import AmbiguousOperatorError, NoOperatorError


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture(autouse=True)
def cleanup() -> Iterator[None]:
    yield
    from app.db import get_conn

    app.dependency_overrides.pop(get_conn, None)


def _install_conn(
    fetchone_returns: Sequence[object] | None = None,
    fetchall_returns: Sequence[object] | None = None,
    rowcount: int = 1,
) -> MagicMock:
    """Stub DB whose cursor feeds fetchone/fetchall in the order supplied.

    Returns the MagicMock cursor so tests can assert on ``cur.execute.call_args_list``
    for SQL-shape pinning. Access the parent connection via ``cur._parent_conn`` to
    assert on commit for regression guards.

    Call ordering by endpoint:
      GET /alerts/guard-rejections — 2x fetchone, 1x fetchall:
        fetchone[0] → {"alerts_last_seen_decision_id": int | None}
        fetchone[1] → {"unseen_count": int}
        fetchall    → list[rejection-row dicts]
      POST /alerts/seen — no fetchone/fetchall; only cur.execute for the UPDATE.
      POST /alerts/dismiss-all — no fetchone/fetchall; only cur.execute for the UPDATE.

    Any test that doesn't supply the right number of fetchone entries will get a
    MagicMock back from the exhausted side_effect, which serialises to garbage.
    """
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__.return_value = cur
    cur.__exit__.return_value = None
    if fetchone_returns is not None:
        cur.fetchone.side_effect = list(fetchone_returns)
    if fetchall_returns is not None:
        cur.fetchall.return_value = fetchall_returns
    cur.rowcount = rowcount
    conn.cursor.return_value = cur
    conn.commit = MagicMock()
    cur._parent_conn = conn  # exposed so tests can assert on commit

    def _dep() -> Iterator[MagicMock]:
        yield conn

    from app.db import get_conn

    app.dependency_overrides[get_conn] = _dep
    return cur


def test_get_returns_503_when_no_operator(client: TestClient) -> None:
    with patch("app.api.alerts.sole_operator_id", side_effect=NoOperatorError()):
        _install_conn()
        resp = client.get("/alerts/guard-rejections")
    assert resp.status_code == 503


def test_get_returns_501_when_multiple_operators(client: TestClient) -> None:
    with patch(
        "app.api.alerts.sole_operator_id",
        side_effect=AmbiguousOperatorError(),
    ):
        _install_conn()
        resp = client.get("/alerts/guard-rejections")
    assert resp.status_code == 501


_OP_ID = UUID("00000000-0000-0000-0000-000000000001")


def _rejection_row(
    *,
    decision_id: int,
    decision_time: datetime | None = None,
    instrument_id: int | None = 42,
    symbol: str | None = "AAPL",
    action: str | None = "BUY",
    explanation: str = "FAIL — cash_available: need £200, have £50",
) -> dict[str, object]:
    return {
        "decision_id": decision_id,
        "decision_time": decision_time or datetime(2026, 4, 21, tzinfo=UTC),
        "instrument_id": instrument_id,
        "symbol": symbol,
        "action": action,
        "explanation": explanation,
    }


def test_get_empty_state(client: TestClient) -> None:
    with patch("app.api.alerts.sole_operator_id", return_value=_OP_ID):
        # fetchone #1 = operator's alerts_last_seen_decision_id (NULL)
        # fetchone #2 = unseen_count (0)
        _install_conn(
            fetchone_returns=[{"alerts_last_seen_decision_id": None}, {"unseen_count": 0}],
            fetchall_returns=[],
        )
        resp = client.get("/alerts/guard-rejections")
    assert resp.status_code == 200
    body = resp.json()
    assert body == {
        "alerts_last_seen_decision_id": None,
        "unseen_count": 0,
        "rejections": [],
    }


def test_get_returns_row_shape_and_unseen_count(client: TestClient) -> None:
    rows = [
        _rejection_row(decision_id=501, action="BUY"),
        _rejection_row(decision_id=500, action="ADD", symbol="MSFT", instrument_id=43),
    ]
    with patch("app.api.alerts.sole_operator_id", return_value=_OP_ID):
        _install_conn(
            fetchone_returns=[
                {"alerts_last_seen_decision_id": 499},
                {"unseen_count": 2},
            ],
            fetchall_returns=rows,
        )
        resp = client.get("/alerts/guard-rejections")
    assert resp.status_code == 200
    body = resp.json()
    assert body["alerts_last_seen_decision_id"] == 499
    assert body["unseen_count"] == 2
    assert len(body["rejections"]) == 2
    assert body["rejections"][0]["decision_id"] == 501
    assert body["rejections"][0]["symbol"] == "AAPL"
    assert body["rejections"][0]["action"] == "BUY"


def test_get_null_instrument_and_action_serialise(client: TestClient) -> None:
    rows = [
        _rejection_row(decision_id=1, instrument_id=None, symbol=None, action=None),
    ]
    with patch("app.api.alerts.sole_operator_id", return_value=_OP_ID):
        _install_conn(
            fetchone_returns=[
                {"alerts_last_seen_decision_id": None},
                {"unseen_count": 1},
            ],
            fetchall_returns=rows,
        )
        resp = client.get("/alerts/guard-rejections")
    assert resp.status_code == 200
    row = resp.json()["rejections"][0]
    assert row["instrument_id"] is None
    assert row["symbol"] is None
    assert row["action"] is None


def test_get_hold_action_round_trip(client: TestClient) -> None:
    rows = [_rejection_row(decision_id=7, action="HOLD")]
    with patch("app.api.alerts.sole_operator_id", return_value=_OP_ID):
        _install_conn(
            fetchone_returns=[
                {"alerts_last_seen_decision_id": None},
                {"unseen_count": 1},
            ],
            fetchall_returns=rows,
        )
        resp = client.get("/alerts/guard-rejections")
    assert resp.json()["rejections"][0]["action"] == "HOLD"


def test_get_sql_shape_pins_window_and_scope_and_cap(client: TestClient) -> None:
    """Pin SQL-shape invariants that the contract depends on:
    - 7-day window filter (INTERVAL '7 days')
    - pass_fail = 'FAIL' (uppercase) + stage = 'execution_guard'
    - ORDER BY decision_id DESC (NOT decision_time DESC)
    - LIMIT 500
    """
    with patch("app.api.alerts.sole_operator_id", return_value=_OP_ID):
        cur = _install_conn(
            fetchone_returns=[
                {"alerts_last_seen_decision_id": None},
                {"unseen_count": 0},
            ],
            fetchall_returns=[],
        )
        resp = client.get("/alerts/guard-rejections")
    assert resp.status_code == 200
    list_sql = next(c.args[0] for c in cur.execute.call_args_list if "FROM decision_audit da" in c.args[0])
    assert "pass_fail = 'FAIL'" in list_sql
    assert "stage = 'execution_guard'" in list_sql
    assert "INTERVAL '7 days'" in list_sql
    assert "ORDER BY da.decision_id DESC" in list_sql
    assert "LIMIT 500" in list_sql


def test_get_unseen_count_query_uses_strict_gt_on_decision_id(client: TestClient) -> None:
    """unseen_count query uses strict `decision_id > last_id` (race-safety)."""
    with patch("app.api.alerts.sole_operator_id", return_value=_OP_ID):
        cur = _install_conn(
            fetchone_returns=[
                {"alerts_last_seen_decision_id": 100},
                {"unseen_count": 3},
            ],
            fetchall_returns=[],
        )
        client.get("/alerts/guard-rejections")
    count_sql = next(c.args[0] for c in cur.execute.call_args_list if "SELECT COUNT(*)" in c.args[0])
    # Strict `>` ties are structurally impossible on a unique PK.
    assert "decision_id > %(last_id)s" in count_sql
    # Filter matches list query so counts and rows agree.
    assert "pass_fail = 'FAIL'" in count_sql
    assert "stage = 'execution_guard'" in count_sql
    assert "INTERVAL '7 days'" in count_sql
    # NULL last-seen path counts everything in window.
    assert "%(last_id)s IS NULL" in count_sql


def test_post_seen_rejects_missing_field(client: TestClient) -> None:
    with patch("app.api.alerts.sole_operator_id", return_value=_OP_ID):
        _install_conn()
        resp = client.post("/alerts/seen", json={})
    assert resp.status_code == 422


def test_post_seen_rejects_non_integer(client: TestClient) -> None:
    with patch("app.api.alerts.sole_operator_id", return_value=_OP_ID):
        _install_conn()
        resp = client.post("/alerts/seen", json={"seen_through_decision_id": "abc"})
    assert resp.status_code == 422


def test_post_seen_rejects_non_positive(client: TestClient) -> None:
    with patch("app.api.alerts.sole_operator_id", return_value=_OP_ID):
        _install_conn()
        resp = client.post("/alerts/seen", json={"seen_through_decision_id": 0})
    assert resp.status_code == 422


def test_post_seen_writes_update(client: TestClient) -> None:
    with patch("app.api.alerts.sole_operator_id", return_value=_OP_ID):
        cur = _install_conn(rowcount=1)
        resp = client.post("/alerts/seen", json={"seen_through_decision_id": 1234})
    assert resp.status_code == 204
    # One UPDATE call was issued with the posted value.
    calls = cur.execute.call_args_list
    assert any("UPDATE operators" in c.args[0] for c in calls)
    params = [c.args[1] for c in calls if "UPDATE operators" in c.args[0]][0]
    assert params["seen_through_decision_id"] == 1234
    assert params["op"] == _OP_ID
    # Regression guard: conn.commit() must fire or the UPDATE never persists.
    cur._parent_conn.commit.assert_called_once()


def test_post_seen_sql_shape_pins_greatest_and_least_and_scope(client: TestClient) -> None:
    """SQL must be: GREATEST(COALESCE(current, 0), LEAST(posted, MAX-in-window-or-0))
    with MAX subselect filtered to FAIL + execution_guard + 7-day window."""
    with patch("app.api.alerts.sole_operator_id", return_value=_OP_ID):
        cur = _install_conn(rowcount=1)
        resp = client.post("/alerts/seen", json={"seen_through_decision_id": 99999})
    assert resp.status_code == 204
    sql = next(c.args[0] for c in cur.execute.call_args_list if "UPDATE operators" in c.args[0])
    assert "GREATEST" in sql
    assert "LEAST" in sql
    assert "SELECT MAX(decision_id)" in sql
    assert "pass_fail = 'FAIL'" in sql
    assert "stage = 'execution_guard'" in sql
    assert "INTERVAL '7 days'" in sql


def test_post_seen_returns_503_when_no_operator(client: TestClient) -> None:
    with patch("app.api.alerts.sole_operator_id", side_effect=NoOperatorError()):
        _install_conn()
        resp = client.post("/alerts/seen", json={"seen_through_decision_id": 1})
    assert resp.status_code == 503


def test_post_seen_returns_501_when_multiple_operators(client: TestClient) -> None:
    with patch("app.api.alerts.sole_operator_id", side_effect=AmbiguousOperatorError()):
        _install_conn()
        resp = client.post("/alerts/seen", json={"seen_through_decision_id": 1})
    assert resp.status_code == 501
