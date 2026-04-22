"""Tests for the alerts API (#315 Phase 3)."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from datetime import UTC, datetime
from decimal import Decimal
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


# --- #396 position-alert integration tests ----------------------------------

_PA_INSTRUMENT_ID_COUNTER = 1000  # module-scoped unique IDs to avoid PK clashes


def _seed_alert_instrument(conn: psycopg.Connection[tuple], *, symbol: str = "AAPL") -> int:
    """Insert one instrument row with a unique BIGINT PK; return the id.

    Isolated from the guard-rejection tests' ``iid = 1`` so a single
    ``ebull_test_conn`` fixture can host multiple instruments without PK
    clash after TRUNCATE resets (BIGSERIAL on other tables resets, but
    instruments uses caller-supplied PK).
    """
    global _PA_INSTRUMENT_ID_COUNTER
    _PA_INSTRUMENT_ID_COUNTER += 1
    iid = _PA_INSTRUMENT_ID_COUNTER
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, currency, is_tradable) "
            "VALUES (%s, %s, %s, 'USD', TRUE)",
            (iid, symbol, f"{symbol} Inc."),
        )
    conn.commit()
    return iid


def _seed_position_alert(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    alert_type: str = "sl_breach",
    opened_at_offset: str = "-1 hour",
    resolved_at_offset: str | None = None,
    detail: str = "breach",
    current_bid: Decimal | None = Decimal("100"),
) -> int:
    """Insert one position_alerts row with controlled offsets; return alert_id.

    ``opened_at_offset`` / ``resolved_at_offset`` are SQL interval
    literals (``'-1 hour'``, ``'-6 days'``). Whitespace / format is
    re-used verbatim in an f-string inside the INSERT — do not accept
    user input here, only test-controlled constants (prevention:
    f-string SQL composition for column / table identifiers).
    """
    resolved_sql = f"now() + INTERVAL '{resolved_at_offset}'" if resolved_at_offset else "NULL"
    sql = f"""
            INSERT INTO position_alerts
                (instrument_id, alert_type, opened_at, resolved_at, detail, current_bid)
            VALUES (
                %s, %s,
                now() + INTERVAL '{opened_at_offset}',
                {resolved_sql},
                %s, %s
            )
            RETURNING alert_id
            """
    with conn.cursor() as cur:
        cur.execute(sql, (instrument_id, alert_type, detail, current_bid))  # type: ignore[call-overload]
        row = cur.fetchone()
        assert row is not None
    conn.commit()
    return int(row[0])


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_get_empty_state(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.get("/alerts/position-alerts")
        assert resp.status_code == 200
        assert resp.json() == {
            "alerts_last_seen_position_alert_id": None,
            "unseen_count": 0,
            "alerts": [],
        }
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_get_includes_rows_within_window(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    iid = _seed_alert_instrument(ebull_test_conn)
    _seed_position_alert(ebull_test_conn, instrument_id=iid, opened_at_offset="-6 days")
    _seed_position_alert(
        ebull_test_conn,
        instrument_id=iid,
        opened_at_offset="-8 days",
        alert_type="tp_breach",
    )
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.get("/alerts/position-alerts")
        body = resp.json()
        assert len(body["alerts"]) == 1
        assert body["alerts"][0]["alert_type"] == "sl_breach"
        # Pins spec test 5: NULL cursor counts all in-window rows.
        assert body["unseen_count"] == 1
        assert body["alerts_last_seen_position_alert_id"] is None
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_get_caps_at_500(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    iid = _seed_alert_instrument(ebull_test_conn)
    with ebull_test_conn.cursor() as cur:
        for _ in range(510):
            cur.execute(
                "INSERT INTO position_alerts "
                "(instrument_id, alert_type, opened_at, resolved_at, detail) "
                "VALUES (%s, 'sl_breach', now() - INTERVAL '1 hour', now(), 'x')",
                (iid,),
            )
    ebull_test_conn.commit()

    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.get("/alerts/position-alerts")
        body = resp.json()
        assert len(body["alerts"]) == 500
        assert body["unseen_count"] == 510
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_get_unseen_count_respects_cursor(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    iid = _seed_alert_instrument(ebull_test_conn)
    a1 = _seed_position_alert(
        ebull_test_conn,
        instrument_id=iid,
        alert_type="sl_breach",
        resolved_at_offset="-30 min",
    )
    _seed_position_alert(
        ebull_test_conn,
        instrument_id=iid,
        alert_type="tp_breach",
        resolved_at_offset="-20 min",
    )
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "UPDATE operators SET alerts_last_seen_position_alert_id = %s WHERE operator_id = %s",
            (a1, _INT_OP_ID),
        )
    ebull_test_conn.commit()

    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.get("/alerts/position-alerts")
        body = resp.json()
        assert body["unseen_count"] == 1
        assert body["alerts_last_seen_position_alert_id"] == a1
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_get_includes_resolved_within_window(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    iid = _seed_alert_instrument(ebull_test_conn)
    _seed_position_alert(
        ebull_test_conn,
        instrument_id=iid,
        opened_at_offset="-6 days",
        resolved_at_offset="-2 hours",
    )
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.get("/alerts/position-alerts")
        body = resp.json()
        assert len(body["alerts"]) == 1
        assert body["alerts"][0]["resolved_at"] is not None
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_get_excludes_old_opened_even_if_unresolved(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    iid = _seed_alert_instrument(ebull_test_conn)
    _seed_position_alert(
        ebull_test_conn,
        instrument_id=iid,
        opened_at_offset="-9 days",
        resolved_at_offset=None,
    )
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.get("/alerts/position-alerts")
        body = resp.json()
        assert body["alerts"] == []
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_get_orders_by_alert_id_not_opened_at(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Later alert_id but earlier opened_at must rank higher."""
    _seed_operator(ebull_test_conn)
    iid = _seed_alert_instrument(ebull_test_conn)
    a1 = _seed_position_alert(
        ebull_test_conn,
        instrument_id=iid,
        alert_type="sl_breach",
        opened_at_offset="-10 min",
        resolved_at_offset="-5 min",
    )
    a2 = _seed_position_alert(
        ebull_test_conn,
        instrument_id=iid,
        alert_type="tp_breach",
        opened_at_offset="-1 hour",
        resolved_at_offset="-30 min",
    )
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.get("/alerts/position-alerts")
        body = resp.json()
        assert body["alerts"][0]["alert_id"] == a2
        assert body["alerts"][1]["alert_id"] == a1
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_seen_monotonic(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    iid = _seed_alert_instrument(ebull_test_conn)
    _seed_position_alert(
        ebull_test_conn,
        instrument_id=iid,
        opened_at_offset="-1 hour",
        resolved_at_offset=None,
    )
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "UPDATE operators SET alerts_last_seen_position_alert_id = 1000 WHERE operator_id = %s",
            (_INT_OP_ID,),
        )
    ebull_test_conn.commit()

    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.post(
                "/alerts/position-alerts/seen",
                json={"seen_through_position_alert_id": 500},
            )
        assert resp.status_code == 204

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alerts_last_seen_position_alert_id FROM operators WHERE operator_id = %s",
                (_INT_OP_ID,),
            )
            assert cur.fetchone() == (1000,)
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_seen_first_time(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    iid = _seed_alert_instrument(ebull_test_conn)
    a1 = _seed_position_alert(ebull_test_conn, instrument_id=iid, opened_at_offset="-1 hour")
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.post(
                "/alerts/position-alerts/seen",
                json={"seen_through_position_alert_id": a1},
            )
        assert resp.status_code == 204

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alerts_last_seen_position_alert_id FROM operators WHERE operator_id = %s",
                (_INT_OP_ID,),
            )
            assert cur.fetchone() == (a1,)
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_seen_missing_field_422(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.post("/alerts/position-alerts/seen", json={})
        assert resp.status_code == 422
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_seen_non_integer_422(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Spec case 12 — non-integer body field rejected."""
    _seed_operator(ebull_test_conn)
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.post(
                "/alerts/position-alerts/seen",
                json={"seen_through_position_alert_id": "abc"},
            )
        assert resp.status_code == 422
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_seen_non_positive_422(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.post(
                "/alerts/position-alerts/seen",
                json={"seen_through_position_alert_id": 0},
            )
        assert resp.status_code == 422
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_seen_clamped_to_in_window_max(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    iid = _seed_alert_instrument(ebull_test_conn)
    a1 = _seed_position_alert(ebull_test_conn, instrument_id=iid, opened_at_offset="-1 hour")
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.post(
                "/alerts/position-alerts/seen",
                json={"seen_through_position_alert_id": 99_999},
            )
        assert resp.status_code == 204

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alerts_last_seen_position_alert_id FROM operators WHERE operator_id = %s",
                (_INT_OP_ID,),
            )
            assert cur.fetchone() == (a1,)
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_seen_empty_window_noop(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.post(
                "/alerts/position-alerts/seen",
                json={"seen_through_position_alert_id": 500},
            )
        assert resp.status_code == 204

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alerts_last_seen_position_alert_id FROM operators WHERE operator_id = %s",
                (_INT_OP_ID,),
            )
            # Cursor stays NULL — no 0 written. Divergence from #394 /alerts/seen
            # (which does write 0 on the same edge; tracked separately).
            assert cur.fetchone() == (None,)
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_seen_race_strict_greater(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    iid = _seed_alert_instrument(ebull_test_conn)
    a_old = _seed_position_alert(
        ebull_test_conn,
        instrument_id=iid,
        alert_type="sl_breach",
        opened_at_offset="-1 hour",
        resolved_at_offset="-30 min",
    )
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            client.post(
                "/alerts/position-alerts/seen",
                json={"seen_through_position_alert_id": a_old},
            )
        # Row arrives AFTER the POST — larger alert_id, must remain unseen.
        _seed_position_alert(
            ebull_test_conn,
            instrument_id=iid,
            alert_type="tp_breach",
            opened_at_offset="-5 min",
        )
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.get("/alerts/position-alerts")
        body = resp.json()
        assert body["unseen_count"] == 1
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_dismiss_all_advances_to_max(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    iid = _seed_alert_instrument(ebull_test_conn)
    _seed_position_alert(
        ebull_test_conn,
        instrument_id=iid,
        alert_type="sl_breach",
        opened_at_offset="-3 hours",
        resolved_at_offset="-2 hours",
    )
    _seed_position_alert(
        ebull_test_conn,
        instrument_id=iid,
        alert_type="tp_breach",
        opened_at_offset="-2 hours",
        resolved_at_offset="-1 hour",
    )
    a3 = _seed_position_alert(
        ebull_test_conn,
        instrument_id=iid,
        alert_type="thesis_break",
        opened_at_offset="-30 min",
    )
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.post("/alerts/position-alerts/dismiss-all")
        assert resp.status_code == 204

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alerts_last_seen_position_alert_id FROM operators WHERE operator_id = %s",
                (_INT_OP_ID,),
            )
            assert cur.fetchone() == (a3,)
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_dismiss_all_monotonic(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    iid = _seed_alert_instrument(ebull_test_conn)
    _seed_position_alert(
        ebull_test_conn,
        instrument_id=iid,
        opened_at_offset="-1 hour",
        resolved_at_offset="-30 min",
    )
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "UPDATE operators SET alerts_last_seen_position_alert_id = 500 WHERE operator_id = %s",
            (_INT_OP_ID,),
        )
    ebull_test_conn.commit()

    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.post("/alerts/position-alerts/dismiss-all")
        assert resp.status_code == 204
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alerts_last_seen_position_alert_id FROM operators WHERE operator_id = %s",
                (_INT_OP_ID,),
            )
            assert cur.fetchone() == (500,)
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_dismiss_all_empty_window_null_cursor(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.post("/alerts/position-alerts/dismiss-all")
        assert resp.status_code == 204
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alerts_last_seen_position_alert_id FROM operators WHERE operator_id = %s",
                (_INT_OP_ID,),
            )
            assert cur.fetchone() == (None,)
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_dismiss_all_empty_window_existing_cursor(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "UPDATE operators SET alerts_last_seen_position_alert_id = 500 WHERE operator_id = %s",
            (_INT_OP_ID,),
        )
    ebull_test_conn.commit()

    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.post("/alerts/position-alerts/dismiss-all")
        assert resp.status_code == 204
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alerts_last_seen_position_alert_id FROM operators WHERE operator_id = %s",
                (_INT_OP_ID,),
            )
            assert cur.fetchone() == (500,)
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_dismiss_all_race_later_row_unseen(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    iid = _seed_alert_instrument(ebull_test_conn)
    _seed_position_alert(
        ebull_test_conn,
        instrument_id=iid,
        alert_type="sl_breach",
        opened_at_offset="-2 hours",
        resolved_at_offset="-1 hour",
    )
    _seed_position_alert(
        ebull_test_conn,
        instrument_id=iid,
        alert_type="tp_breach",
        opened_at_offset="-1 hour",
        resolved_at_offset="-30 min",
    )
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            client.post("/alerts/position-alerts/dismiss-all")
        _seed_position_alert(
            ebull_test_conn,
            instrument_id=iid,
            alert_type="thesis_break",
            opened_at_offset="-5 min",
        )
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.get("/alerts/position-alerts")
        body = resp.json()
        assert body["unseen_count"] == 1
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_cursor_isolated_from_guard_direction_1(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """POST /alerts/position-alerts/seen must not touch the guard cursor."""
    _seed_operator(ebull_test_conn)
    iid = _seed_alert_instrument(ebull_test_conn)
    a1 = _seed_position_alert(ebull_test_conn, instrument_id=iid, opened_at_offset="-1 hour")
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            client.post(
                "/alerts/position-alerts/seen",
                json={"seen_through_position_alert_id": a1},
            )
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alerts_last_seen_decision_id, alerts_last_seen_position_alert_id "
                "FROM operators WHERE operator_id = %s",
                (_INT_OP_ID,),
            )
            row = cur.fetchone()
            assert row is not None
            guard_cursor, pos_cursor = row
        assert guard_cursor is None
        assert pos_cursor == a1
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_cursor_isolated_from_guard_direction_2(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """POST /alerts/seen (guard) must not touch the position cursor."""
    _seed_operator(ebull_test_conn)
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO decision_audit "
            "(decision_time, stage, pass_fail, explanation) "
            "VALUES (now(), 'execution_guard', 'FAIL', 'guard-row') "
            "RETURNING decision_id"
        )
        row = cur.fetchone()
        assert row is not None
        did = row[0]
    ebull_test_conn.commit()

    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            client.post(
                "/alerts/seen",
                json={"seen_through_decision_id": did},
            )
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alerts_last_seen_decision_id, alerts_last_seen_position_alert_id "
                "FROM operators WHERE operator_id = %s",
                (_INT_OP_ID,),
            )
            row = cur.fetchone()
            assert row is not None
            guard_cursor, pos_cursor = row
        assert guard_cursor == did
        assert pos_cursor is None
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_no_operator_returns_503(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Do NOT seed an operator AND do NOT patch sole_operator_id — the
    real resolver must raise NoOperatorError and the API must map to 503.
    """
    client = _bind_test_client(ebull_test_conn)
    try:
        assert client.get("/alerts/position-alerts").status_code == 503
        assert (
            client.post(
                "/alerts/position-alerts/seen",
                json={"seen_through_position_alert_id": 1},
            ).status_code
            == 503
        )
        assert client.post("/alerts/position-alerts/dismiss-all").status_code == 503
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_multiple_operators_returns_501(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Seed two operators, do NOT patch sole_operator_id — real resolver
    raises AmbiguousOperatorError -> 501."""
    _seed_operator(ebull_test_conn)
    second_id = UUID("22222222-2222-2222-2222-222222222222")
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO operators (operator_id, username, password_hash) "
            "VALUES (%s, 'alerts_test_op2', 'x') ON CONFLICT DO NOTHING",
            (second_id,),
        )
    ebull_test_conn.commit()

    client = _bind_test_client(ebull_test_conn)
    try:
        assert client.get("/alerts/position-alerts").status_code == 501
        assert (
            client.post(
                "/alerts/position-alerts/seen",
                json={"seen_through_position_alert_id": 1},
            ).status_code
            == 501
        )
        assert client.post("/alerts/position-alerts/dismiss-all").status_code == 501
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_alert_type_round_trip_all_three(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    iid = _seed_alert_instrument(ebull_test_conn)
    offsets = {"sl_breach": "-3 hours", "tp_breach": "-2 hours", "thesis_break": "-1 hour"}
    for t, offset in offsets.items():
        _seed_position_alert(
            ebull_test_conn,
            instrument_id=iid,
            alert_type=t,
            opened_at_offset=offset,
        )

    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.get("/alerts/position-alerts")
        body = resp.json()
        types = {row["alert_type"] for row in body["alerts"]}
        assert types == {"sl_breach", "tp_breach", "thesis_break"}
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


# ---------------------------------------------------------------------
# #397: coverage-status-drops endpoints
# ---------------------------------------------------------------------


def _seed_coverage_status_event(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    old_status: str = "analysable",
    new_status: str = "insufficient",
    changed_at_offset: str = "-1 hour",
) -> int:
    """Insert one coverage_status_events row with controlled offset; return event_id.

    ``changed_at_offset`` is a SQL interval literal (``'-1 hour'``, ``'-8 days'``).
    F-string composition for interval literal only — test-controlled constants
    only, matches _seed_position_alert pattern in this file.
    """
    sql = f"""
            INSERT INTO coverage_status_events
                (instrument_id, old_status, new_status, changed_at)
            VALUES (
                %s, %s, %s,
                now() + INTERVAL '{changed_at_offset}'
            )
            RETURNING event_id
            """
    with conn.cursor() as cur:
        cur.execute(sql, (instrument_id, old_status, new_status))  # type: ignore[call-overload]
        row = cur.fetchone()
        assert row is not None
    conn.commit()
    return int(row[0])


class TestCoverageStatusDropsGet:
    def test_get_returns_503_when_no_operator(self, client: TestClient) -> None:
        with patch("app.api.alerts.sole_operator_id", side_effect=NoOperatorError()):
            _install_conn()
            resp = client.get("/alerts/coverage-status-drops")
        assert resp.status_code == 503

    def test_get_returns_501_when_multiple_operators(self, client: TestClient) -> None:
        with patch("app.api.alerts.sole_operator_id", side_effect=AmbiguousOperatorError()):
            _install_conn()
            resp = client.get("/alerts/coverage-status-drops")
        assert resp.status_code == 501

    def test_get_empty_state(self, client: TestClient) -> None:
        cur = _install_conn(
            fetchone_returns=[
                {"alerts_last_seen_coverage_event_id": None},
                {"unseen_count": 0},
            ],
            fetchall_returns=[],
        )
        with patch(
            "app.api.alerts.sole_operator_id",
            return_value=UUID("00000000-0000-0000-0000-000000000001"),
        ):
            resp = client.get("/alerts/coverage-status-drops")
        assert resp.status_code == 200
        assert resp.json() == {
            "alerts_last_seen_coverage_event_id": None,
            "unseen_count": 0,
            "drops": [],
        }
        # SQL shape pin: predicate references BOTH old_status = 'analysable'
        # AND new_status IS DISTINCT FROM 'analysable' on the list query; the
        # list is ordered event_id DESC and capped at 500.
        list_sql = cur.execute.call_args_list[-1][0][0]
        assert "old_status = 'analysable'" in list_sql
        assert "new_status IS DISTINCT FROM 'analysable'" in list_sql
        assert "ORDER BY e.event_id DESC" in list_sql
        assert "LIMIT 500" in list_sql


@pytest.mark.skipif("not test_db_available()")
class TestCoverageStatusDropsGetIntegration:
    def test_get_returns_drops_in_window(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        _seed_operator(ebull_test_conn)
        iid = _seed_alert_instrument(ebull_test_conn)
        _seed_coverage_status_event(
            ebull_test_conn, instrument_id=iid, changed_at_offset="-1 hour"
        )

        client = _bind_test_client(ebull_test_conn)
        try:
            with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
                resp = client.get("/alerts/coverage-status-drops")
            assert resp.status_code == 200
            body = resp.json()
            assert body["alerts_last_seen_coverage_event_id"] is None
            assert body["unseen_count"] == 1
            assert len(body["drops"]) == 1
            assert body["drops"][0]["old_status"] == "analysable"
            assert body["drops"][0]["new_status"] == "insufficient"
        finally:
            from app.db import get_conn

            app.dependency_overrides.pop(get_conn, None)

    def test_get_excludes_non_drops(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        """Promotions (insufficient -> analysable) + first audit (NULL -> terminal)
        must not appear on strip."""
        _seed_operator(ebull_test_conn)
        iid = _seed_alert_instrument(ebull_test_conn)
        _seed_coverage_status_event(
            ebull_test_conn,
            instrument_id=iid,
            old_status="insufficient",
            new_status="analysable",
        )
        # NULL -> 'analysable' first-audit — excluded (old_status IS NULL
        # does not match 'analysable' predicate).
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO coverage_status_events (instrument_id, old_status, new_status) "
                "VALUES (%s, NULL, 'analysable')",
                (iid,),
            )
        ebull_test_conn.commit()

        client = _bind_test_client(ebull_test_conn)
        try:
            with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
                resp = client.get("/alerts/coverage-status-drops")
            assert resp.json()["drops"] == []
            assert resp.json()["unseen_count"] == 0
        finally:
            from app.db import get_conn

            app.dependency_overrides.pop(get_conn, None)

    def test_get_excludes_rows_older_than_7_days(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        _seed_operator(ebull_test_conn)
        iid = _seed_alert_instrument(ebull_test_conn)
        _seed_coverage_status_event(
            ebull_test_conn, instrument_id=iid, changed_at_offset="-8 days"
        )

        client = _bind_test_client(ebull_test_conn)
        try:
            with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
                resp = client.get("/alerts/coverage-status-drops")
            assert resp.json()["drops"] == []
            assert resp.json()["unseen_count"] == 0
        finally:
            from app.db import get_conn

            app.dependency_overrides.pop(get_conn, None)

    def test_get_orders_by_event_id_desc(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        """Runtime check: BIGSERIAL PK ordering is the race-safe ordering.
        Seed events out of changed_at order — assert list comes back event_id
        DESC (most-recent-inserted first)."""
        _seed_operator(ebull_test_conn)
        iid = _seed_alert_instrument(ebull_test_conn)
        # e1 seeded first (lower event_id) but with a LATER changed_at offset
        # than e2 — if ordering were by changed_at, e1 would be first.
        e1 = _seed_coverage_status_event(
            ebull_test_conn, instrument_id=iid, changed_at_offset="-1 hour"
        )
        e2 = _seed_coverage_status_event(
            ebull_test_conn, instrument_id=iid, changed_at_offset="-2 hours"
        )
        client = _bind_test_client(ebull_test_conn)
        try:
            with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
                resp = client.get("/alerts/coverage-status-drops")
            body = resp.json()
            event_ids = [d["event_id"] for d in body["drops"]]
            # event_id DESC: e2 > e1 numerically, so e2 first.
            assert event_ids == [e2, e1]
        finally:
            from app.db import get_conn

            app.dependency_overrides.pop(get_conn, None)

    def test_get_caps_at_500(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        """Spec requires LIMIT 500 cap."""
        _seed_operator(ebull_test_conn)
        iid = _seed_alert_instrument(ebull_test_conn)
        # Bulk-insert 505 in-window drop events via a single SQL.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO coverage_status_events (instrument_id, old_status, new_status, changed_at)
                SELECT %s, 'analysable', 'insufficient', now() - (s || ' seconds')::interval
                FROM generate_series(1, 505) s
                """,
                (iid,),
            )
        ebull_test_conn.commit()

        client = _bind_test_client(ebull_test_conn)
        try:
            with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
                resp = client.get("/alerts/coverage-status-drops")
            body = resp.json()
            assert len(body["drops"]) == 500
            assert body["unseen_count"] == 505
        finally:
            from app.db import get_conn

            app.dependency_overrides.pop(get_conn, None)

    def test_get_respects_cursor_on_unseen_count(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        _seed_operator(ebull_test_conn)
        iid = _seed_alert_instrument(ebull_test_conn)
        e1 = _seed_coverage_status_event(
            ebull_test_conn, instrument_id=iid, changed_at_offset="-2 hours"
        )
        _seed_coverage_status_event(
            ebull_test_conn, instrument_id=iid, changed_at_offset="-1 hour"
        )

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "UPDATE operators SET alerts_last_seen_coverage_event_id = %s "
                "WHERE operator_id = %s",
                (e1, _INT_OP_ID),
            )
        ebull_test_conn.commit()

        client = _bind_test_client(ebull_test_conn)
        try:
            with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
                resp = client.get("/alerts/coverage-status-drops")
            body = resp.json()
            assert body["unseen_count"] == 1
            assert len(body["drops"]) == 2
        finally:
            from app.db import get_conn

            app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
class TestCoverageStatusDropsSeen:
    def test_seen_advances_cursor_monotonically(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        _seed_operator(ebull_test_conn)
        iid = _seed_alert_instrument(ebull_test_conn)
        e1 = _seed_coverage_status_event(
            ebull_test_conn, instrument_id=iid, changed_at_offset="-2 hours"
        )
        _seed_coverage_status_event(
            ebull_test_conn, instrument_id=iid, changed_at_offset="-1 hour"
        )

        client = _bind_test_client(ebull_test_conn)
        try:
            with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
                resp = client.post(
                    "/alerts/coverage-status-drops/seen",
                    json={"seen_through_event_id": e1},
                )
            assert resp.status_code == 204
            with ebull_test_conn.cursor() as cur:
                cur.execute(
                    "SELECT alerts_last_seen_coverage_event_id FROM operators "
                    "WHERE operator_id = %s",
                    (_INT_OP_ID,),
                )
                row = cur.fetchone()
            ebull_test_conn.commit()
            assert row is not None
            assert row[0] == e1

            # Second call with smaller value — cursor does NOT regress.
            with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
                resp = client.post(
                    "/alerts/coverage-status-drops/seen",
                    json={"seen_through_event_id": 1},
                )
            assert resp.status_code == 204
            with ebull_test_conn.cursor() as cur:
                cur.execute(
                    "SELECT alerts_last_seen_coverage_event_id FROM operators "
                    "WHERE operator_id = %s",
                    (_INT_OP_ID,),
                )
                row = cur.fetchone()
            ebull_test_conn.commit()
            assert row is not None
            assert row[0] == e1
        finally:
            from app.db import get_conn

            app.dependency_overrides.pop(get_conn, None)

    def test_seen_empty_window_is_noop_and_preserves_null(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        """No in-window drops + NULL cursor → /seen does NOT materialize a cursor.
        Mirrors position-alerts /seen behaviour (no #395 divergence)."""
        _seed_operator(ebull_test_conn)

        client = _bind_test_client(ebull_test_conn)
        try:
            with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
                resp = client.post(
                    "/alerts/coverage-status-drops/seen",
                    json={"seen_through_event_id": 99999},
                )
            assert resp.status_code == 204
            with ebull_test_conn.cursor() as cur:
                cur.execute(
                    "SELECT alerts_last_seen_coverage_event_id FROM operators "
                    "WHERE operator_id = %s",
                    (_INT_OP_ID,),
                )
                row = cur.fetchone()
            ebull_test_conn.commit()
            assert row is not None
            assert row[0] is None
        finally:
            from app.db import get_conn

            app.dependency_overrides.pop(get_conn, None)

    def test_seen_clamps_to_in_window_max(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        _seed_operator(ebull_test_conn)
        iid = _seed_alert_instrument(ebull_test_conn)
        e1 = _seed_coverage_status_event(
            ebull_test_conn, instrument_id=iid, changed_at_offset="-1 hour"
        )
        client = _bind_test_client(ebull_test_conn)
        try:
            with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
                resp = client.post(
                    "/alerts/coverage-status-drops/seen",
                    json={"seen_through_event_id": e1 + 999_999},
                )
            assert resp.status_code == 204
            with ebull_test_conn.cursor() as cur:
                cur.execute(
                    "SELECT alerts_last_seen_coverage_event_id FROM operators "
                    "WHERE operator_id = %s",
                    (_INT_OP_ID,),
                )
                row = cur.fetchone()
            ebull_test_conn.commit()
            assert row is not None
            assert row[0] == e1
        finally:
            from app.db import get_conn

            app.dependency_overrides.pop(get_conn, None)

    def test_seen_requires_positive_integer(self, client: TestClient) -> None:
        _install_conn()
        with patch(
            "app.api.alerts.sole_operator_id",
            return_value=UUID("00000000-0000-0000-0000-000000000001"),
        ):
            resp = client.post(
                "/alerts/coverage-status-drops/seen",
                json={"seen_through_event_id": 0},
            )
        assert resp.status_code == 422


@pytest.mark.skipif("not test_db_available()")
class TestCoverageStatusDropsDismissAll:
    def test_dismiss_all_advances_to_max(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        _seed_operator(ebull_test_conn)
        iid = _seed_alert_instrument(ebull_test_conn)
        _seed_coverage_status_event(
            ebull_test_conn, instrument_id=iid, changed_at_offset="-2 hours"
        )
        e2 = _seed_coverage_status_event(
            ebull_test_conn, instrument_id=iid, changed_at_offset="-1 hour"
        )

        client = _bind_test_client(ebull_test_conn)
        try:
            with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
                resp = client.post("/alerts/coverage-status-drops/dismiss-all")
            assert resp.status_code == 204
            with ebull_test_conn.cursor() as cur:
                cur.execute(
                    "SELECT alerts_last_seen_coverage_event_id FROM operators "
                    "WHERE operator_id = %s",
                    (_INT_OP_ID,),
                )
                row = cur.fetchone()
            ebull_test_conn.commit()
            assert row is not None
            assert row[0] == e2
        finally:
            from app.db import get_conn

            app.dependency_overrides.pop(get_conn, None)

    def test_dismiss_all_empty_window_preserves_null_cursor(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        _seed_operator(ebull_test_conn)
        client = _bind_test_client(ebull_test_conn)
        try:
            with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
                resp = client.post("/alerts/coverage-status-drops/dismiss-all")
            assert resp.status_code == 204
            with ebull_test_conn.cursor() as cur:
                cur.execute(
                    "SELECT alerts_last_seen_coverage_event_id FROM operators "
                    "WHERE operator_id = %s",
                    (_INT_OP_ID,),
                )
                row = cur.fetchone()
            ebull_test_conn.commit()
            assert row is not None
            assert row[0] is None
        finally:
            from app.db import get_conn

            app.dependency_overrides.pop(get_conn, None)

    def test_dismiss_all_does_not_regress_cursor(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        _seed_operator(ebull_test_conn)
        iid = _seed_alert_instrument(ebull_test_conn)
        e1 = _seed_coverage_status_event(
            ebull_test_conn, instrument_id=iid, changed_at_offset="-1 hour"
        )
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "UPDATE operators SET alerts_last_seen_coverage_event_id = %s "
                "WHERE operator_id = %s",
                (e1 + 999, _INT_OP_ID),
            )
        ebull_test_conn.commit()

        client = _bind_test_client(ebull_test_conn)
        try:
            with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
                resp = client.post("/alerts/coverage-status-drops/dismiss-all")
            assert resp.status_code == 204
            with ebull_test_conn.cursor() as cur:
                cur.execute(
                    "SELECT alerts_last_seen_coverage_event_id FROM operators "
                    "WHERE operator_id = %s",
                    (_INT_OP_ID,),
                )
                row = cur.fetchone()
            ebull_test_conn.commit()
            assert row is not None
            assert row[0] == e1 + 999  # GREATEST preserves larger existing cursor
        finally:
            from app.db import get_conn

            app.dependency_overrides.pop(get_conn, None)
