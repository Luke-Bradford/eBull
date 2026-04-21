"""Tests for the alerts API (#315 Phase 3)."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch
from uuid import UUID

import psycopg
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
    # BIGINT cast avoids AmbiguousParameter when last_id is NULL on real psycopg.
    assert "decision_id > %(last_id)s::BIGINT" in count_sql
    # Filter matches list query so counts and rows agree.
    assert "pass_fail = 'FAIL'" in count_sql
    assert "stage = 'execution_guard'" in count_sql
    assert "INTERVAL '7 days'" in count_sql
    # NULL last-seen path counts everything in window.
    assert "%(last_id)s::BIGINT IS NULL" in count_sql


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


def test_post_dismiss_all_issues_update(client: TestClient) -> None:
    with patch("app.api.alerts.sole_operator_id", return_value=_OP_ID):
        cur = _install_conn(rowcount=1)
        resp = client.post("/alerts/dismiss-all")
    assert resp.status_code == 204
    calls = cur.execute.call_args_list
    assert any("UPDATE operators" in c.args[0] and "SELECT MAX(decision_id)" in c.args[0] for c in calls)
    cur._parent_conn.commit.assert_called_once()


def test_post_dismiss_all_filters_scope_to_guard_fails_in_window(client: TestClient) -> None:
    # Inspect the SQL shape — scope is execution_guard + FAIL + 7-day window.
    with patch("app.api.alerts.sole_operator_id", return_value=_OP_ID):
        cur = _install_conn(rowcount=1)
        resp = client.post("/alerts/dismiss-all")
    assert resp.status_code == 204
    update_sql = next(c.args[0] for c in cur.execute.call_args_list if "UPDATE operators" in c.args[0])
    assert "pass_fail = 'FAIL'" in update_sql
    assert "stage = 'execution_guard'" in update_sql
    assert "INTERVAL '7 days'" in update_sql
    assert "m.max_id IS NOT NULL" in update_sql
    assert "GREATEST" in update_sql
    assert "COALESCE" in update_sql


def test_post_dismiss_all_is_noop_on_zero_rowcount(client: TestClient) -> None:
    # rowcount=0 mimics the empty-window case (WHERE m.max_id IS NOT NULL excludes the row).
    with patch("app.api.alerts.sole_operator_id", return_value=_OP_ID):
        _install_conn(rowcount=0)
        resp = client.post("/alerts/dismiss-all")
    assert resp.status_code == 204  # No-op still returns 204.


def test_post_dismiss_all_returns_503_when_no_operator(client: TestClient) -> None:
    with patch("app.api.alerts.sole_operator_id", side_effect=NoOperatorError()):
        _install_conn()
        resp = client.post("/alerts/dismiss-all")
    assert resp.status_code == 503


def test_post_dismiss_all_returns_501_when_multiple_operators(client: TestClient) -> None:
    with patch("app.api.alerts.sole_operator_id", side_effect=AmbiguousOperatorError()):
        _install_conn()
        resp = client.post("/alerts/dismiss-all")
    assert resp.status_code == 501


# --- Integration tests (real ebull_test DB) ----------------------------------

from tests.fixtures.ebull_test_db import test_db_available  # noqa: E402,F401

_INT_OP_ID = UUID("11111111-1111-1111-1111-111111111111")


def _seed_operator(conn: psycopg.Connection[tuple]) -> None:
    """Insert a known operator row so sole_operator_id resolves and the
    UPDATE in /alerts/seen / /dismiss-all has a target row. `username`
    and `password_hash` are NOT NULL per sql/016_operators_sessions.sql;
    use dummy values — these rows are created for the alerts tests only
    and never authenticate."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO operators (operator_id, username, password_hash)
            VALUES (%s, 'alerts_test_op', 'x')
            ON CONFLICT DO NOTHING
            """,
            (_INT_OP_ID,),
        )
    conn.commit()


def _bind_test_client(conn: psycopg.Connection[tuple]) -> TestClient:
    """Bind TestClient's get_conn dep to the ebull_test connection + patch
    the operator resolver to return the seeded operator id. Returns a
    client whose requests run against ebull_test."""

    def _dep() -> Iterator[psycopg.Connection[tuple]]:
        yield conn

    from app.db import get_conn

    app.dependency_overrides[get_conn] = _dep
    return TestClient(app)


@pytest.mark.skipif("not test_db_available()")
def test_integration_get_orders_by_decision_id_under_clock_skew(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Row B gets a later decision_time but an earlier decision_id.
    GET must still put the row with the higher decision_id first."""
    _seed_operator(ebull_test_conn)

    with ebull_test_conn.cursor() as cur:
        # instruments.instrument_id is BIGINT PRIMARY KEY with no default
        # (sql/001_init.sql), so the caller supplies the id explicitly.
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, currency, is_tradable) "
            "VALUES (1, 'ZZZ', 'Test', 'USD', TRUE)"
        )
        iid = 1

        # Insert Row B first (gets lower decision_id) with the LATER decision_time.
        cur.execute(
            "INSERT INTO decision_audit "
            "(decision_time, instrument_id, stage, pass_fail, explanation) "
            "VALUES (now(), %s, 'execution_guard', 'FAIL', 'B-later-time') "
            "RETURNING decision_id",
            (iid,),
        )
        row_b = cur.fetchone()
        assert row_b is not None
        id_b = row_b[0]

        # Insert Row A second (gets HIGHER decision_id) with the EARLIER decision_time.
        cur.execute(
            "INSERT INTO decision_audit "
            "(decision_time, instrument_id, stage, pass_fail, explanation) "
            "VALUES (now() - INTERVAL '1 hour', %s, 'execution_guard', 'FAIL', 'A-earlier-time') "
            "RETURNING decision_id",
            (iid,),
        )
        row_a = cur.fetchone()
        assert row_a is not None
        id_a = row_a[0]
    ebull_test_conn.commit()

    assert id_a > id_b  # sanity check on the natural PK ordering

    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.get("/alerts/guard-rejections")
        assert resp.status_code == 200
        rejections = resp.json()["rejections"]
        assert rejections[0]["decision_id"] == id_a
        assert rejections[1]["decision_id"] == id_b
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_post_seen_clamps_to_in_window_max(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO decision_audit "
            "(decision_time, stage, pass_fail, explanation) "
            "VALUES (now(), 'execution_guard', 'FAIL', 'in-window') RETURNING decision_id"
        )
        max_in_window_row = cur.fetchone()
        assert max_in_window_row is not None
        max_in_window = max_in_window_row[0]
    ebull_test_conn.commit()

    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.post(
                "/alerts/seen",
                json={"seen_through_decision_id": max_in_window + 99999},
            )
        assert resp.status_code == 204

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alerts_last_seen_decision_id FROM operators WHERE operator_id = %s",
                (_INT_OP_ID,),
            )
            stored_row = cur.fetchone()
            assert stored_row is not None
            stored = stored_row[0]
        assert stored == max_in_window
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_dismiss_all_empty_window_stays_null(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    # No guard rows in window; cursor NULL; POST dismiss-all; cursor stays NULL.
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.post("/alerts/dismiss-all")
        assert resp.status_code == 204
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alerts_last_seen_decision_id FROM operators WHERE operator_id = %s",
                (_INT_OP_ID,),
            )
            dismiss_row = cur.fetchone()
            assert dismiss_row is not None
            assert dismiss_row[0] is None
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_non_guard_stage_excluded_from_list_and_dismiss(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO decision_audit "
            "(decision_time, stage, pass_fail, explanation) "
            "VALUES (now(), 'order_execution', 'FAIL', 'not a guard') RETURNING decision_id"
        )
        non_guard_row = cur.fetchone()
        assert non_guard_row is not None
        id_non_guard = non_guard_row[0]
    ebull_test_conn.commit()

    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.get("/alerts/guard-rejections")
        ids = [r["decision_id"] for r in resp.json()["rejections"]]
        assert id_non_guard not in ids

        # dismiss-all MAX subselect is NULL (no guard rows) → no-op.
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.post("/alerts/dismiss-all")
        assert resp.status_code == 204
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alerts_last_seen_decision_id FROM operators WHERE operator_id = %s",
                (_INT_OP_ID,),
            )
            non_guard_dismiss_row = cur.fetchone()
            assert non_guard_dismiss_row is not None
            assert non_guard_dismiss_row[0] is None
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)
