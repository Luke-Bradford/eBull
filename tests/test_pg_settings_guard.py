"""#1187 — PG max_locks_per_transaction boot guard unit tests.

Pure-logic unit tests using a fake ``psycopg.Connection``-shaped
``MagicMock``. No DB connection required.

Spec: ``docs/superpowers/specs/2026-05-17-pg-max-locks-per-tx-guard.md``.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.db.pg_settings import (
    API_FIXED_LONGLIVED_CONNS,
    CONNECTION_BUDGET_OVERRIDE_ENV,
    CONNECTION_BUDGET_RESERVE,
    JOBS_FIXED_LONGLIVED_CONNS,
    JOBS_STEADY_STATE_EXEC_CONNS,
    ORCHESTRATOR_GATE_CHECK_CONN,
    PG_LOCKS_FLOOR,
    PG_LOCKS_OVERRIDE_ENV,
    ConnectionBudgetExceeded,
    PgLocksFloorBreached,
    check_connection_budget,
    check_max_locks_per_transaction,
    enforce_connection_budget,
    enforce_max_locks_floor,
)
from app.db.pool import (
    AUDIT_POOL_MAX_SIZE,
    DB_POOL_MAX_SIZE,
    JOBS_POOL_MAX_SIZE,
)


def _fake_conn_returning(value: int) -> MagicMock:
    """Build a fake conn whose ``execute().fetchone()`` returns ``(str(value),)``."""
    conn = MagicMock()
    result = MagicMock()
    result.fetchone.return_value = (str(value),)
    conn.execute.return_value = result
    return conn


def _fake_conn_raising(exc: Exception) -> MagicMock:
    """Build a fake conn whose ``execute()`` raises ``exc``."""
    conn = MagicMock()
    conn.execute.side_effect = exc
    return conn


def test_check_returns_passes_when_above_floor() -> None:
    conn = _fake_conn_returning(PG_LOCKS_FLOOR)
    passes, value = check_max_locks_per_transaction(conn)
    assert passes is True
    assert value == PG_LOCKS_FLOOR


def test_check_returns_passes_when_well_above_floor() -> None:
    conn = _fake_conn_returning(PG_LOCKS_FLOOR * 4)
    passes, value = check_max_locks_per_transaction(conn)
    assert passes is True
    assert value == PG_LOCKS_FLOOR * 4


def test_check_returns_fail_when_below_floor() -> None:
    conn = _fake_conn_returning(64)
    passes, value = check_max_locks_per_transaction(conn)
    assert passes is False
    assert value == 64


def test_enforce_raises_when_below_floor_no_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(PG_LOCKS_OVERRIDE_ENV, raising=False)
    conn = _fake_conn_returning(64)
    with pytest.raises(PgLocksFloorBreached) as exc:
        enforce_max_locks_floor(conn)
    assert exc.value.value == 64
    assert exc.value.floor == PG_LOCKS_FLOOR
    assert PG_LOCKS_OVERRIDE_ENV in str(exc.value)


def test_enforce_passes_when_above_floor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(PG_LOCKS_OVERRIDE_ENV, raising=False)
    conn = _fake_conn_returning(PG_LOCKS_FLOOR)
    enforce_max_locks_floor(conn)  # no raise


def test_enforce_skips_when_env_override_set(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv(PG_LOCKS_OVERRIDE_ENV, "1")
    conn = _fake_conn_returning(64)
    with caplog.at_level("WARNING", logger="app.db.pg_settings"):
        enforce_max_locks_floor(conn)
    assert any(
        "running anyway because" in rec.message and PG_LOCKS_OVERRIDE_ENV in rec.message for rec in caplog.records
    )


def test_enforce_fail_open_on_show_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient SHOW failure must not block startup.

    ``check_*`` fail-opens with ``(True, 0)`` on exception; ``enforce_*``
    sees ``passes=True`` and returns without raising. The downstream
    OOM (if it materialises) surfaces naturally — the probe is
    informational, not safety-critical.
    """
    monkeypatch.delenv(PG_LOCKS_OVERRIDE_ENV, raising=False)
    conn = _fake_conn_raising(RuntimeError("transient SHOW failure"))
    enforce_max_locks_floor(conn)  # no raise


def test_breached_message_includes_actionable_alter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The raise message must name the exact ALTER SYSTEM command the
    operator should run + the override env var. Without this, the
    operator sees ``RuntimeError`` with no recovery path."""
    monkeypatch.delenv(PG_LOCKS_OVERRIDE_ENV, raising=False)
    conn = _fake_conn_returning(64)
    with pytest.raises(PgLocksFloorBreached) as exc:
        enforce_max_locks_floor(conn)
    msg = str(exc.value)
    assert "ALTER SYSTEM SET max_locks_per_transaction" in msg
    assert str(PG_LOCKS_FLOOR) in msg
    assert "restart Postgres" in msg
    assert PG_LOCKS_OVERRIDE_ENV in msg


# ---------------------------------------------------------------------------
# #1472 PR1 — connection-budget guard
# ---------------------------------------------------------------------------

# Expected dev-profile demand, derived from the same source constants the
# guard sums (not a hardcoded 23) so a deliberate pool-size change updates
# the expectation through the constants, never silently.
_EXPECTED_DEMAND = (
    DB_POOL_MAX_SIZE
    + AUDIT_POOL_MAX_SIZE
    + API_FIXED_LONGLIVED_CONNS
    + JOBS_POOL_MAX_SIZE
    + JOBS_FIXED_LONGLIVED_CONNS
    + JOBS_STEADY_STATE_EXEC_CONNS
    + ORCHESTRATOR_GATE_CHECK_CONN
    + CONNECTION_BUDGET_RESERVE
)

# A cluster config guaranteed OVER budget regardless of the current demand:
# usable sits a fixed margin below the derived demand. Deriving from
# _EXPECTED_DEMAND (not a literal max_connections=20) means a pool-size
# change re-tracks instead of silently turning "over budget" into "fits" —
# exactly what #1472 PR2b (demand 24→17) would have done to the old
# usable=17 fixtures (which then equal, not exceed, the new demand).
_OVER_BUDGET_SUPERUSER_RESERVED = 3
_OVER_BUDGET_USABLE = _EXPECTED_DEMAND - 5
_OVER_BUDGET_MAX_CONN = _OVER_BUDGET_USABLE + _OVER_BUDGET_SUPERUSER_RESERVED


def _fake_conn_budget(max_conn: int, reserved: int) -> MagicMock:
    """Fake conn answering the two SHOWs ``check_connection_budget`` issues:
    ``max_connections`` → ``max_conn``, ``superuser_reserved_connections``
    → ``reserved``. SQL-branching (not call-order) so the fake stays correct
    if the probe order ever changes."""
    conn = MagicMock()

    def _execute(sql: str, *args: object, **kwargs: object) -> MagicMock:
        result = MagicMock()
        if "superuser_reserved_connections" in sql:
            result.fetchone.return_value = (str(reserved),)
        else:
            result.fetchone.return_value = (str(max_conn),)
        return result

    conn.execute.side_effect = _execute
    return conn


def test_budget_passes_at_real_dev_config() -> None:
    """Acceptance: the guard PASSES at the real dev cluster
    (max_connections=30, superuser_reserved_connections=3 → 27 usable)
    with the shipped pool sizes. A regression that makes the real config
    refuse to boot trips here."""
    passes, demand, usable = check_connection_budget(_fake_conn_budget(30, 3), process="api")
    assert passes is True
    assert demand == _EXPECTED_DEMAND
    assert usable == 27


def test_budget_demand_tracks_pool_constants() -> None:
    """demand == both processes' pool maxes + fixed long-lived conns +
    reserve. Pins the model to its source constants."""
    _passes, demand, _usable = check_connection_budget(_fake_conn_budget(30, 3), process="jobs")
    assert demand == _EXPECTED_DEMAND


def test_budget_fails_when_usable_below_demand() -> None:
    passes, demand, usable = check_connection_budget(
        _fake_conn_budget(_OVER_BUDGET_MAX_CONN, _OVER_BUDGET_SUPERUSER_RESERVED), process="api"
    )
    assert passes is False
    assert usable == _OVER_BUDGET_USABLE
    assert demand > usable


def test_enforce_budget_passes_at_real_dev_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(CONNECTION_BUDGET_OVERRIDE_ENV, raising=False)
    enforce_connection_budget(_fake_conn_budget(30, 3), process="api")  # no raise


def test_enforce_budget_raises_when_over_budget_no_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(CONNECTION_BUDGET_OVERRIDE_ENV, raising=False)
    with pytest.raises(ConnectionBudgetExceeded) as exc:
        enforce_connection_budget(
            _fake_conn_budget(_OVER_BUDGET_MAX_CONN, _OVER_BUDGET_SUPERUSER_RESERVED), process="jobs"
        )
    assert exc.value.process == "jobs"
    assert exc.value.usable == _OVER_BUDGET_USABLE
    assert exc.value.demand == _EXPECTED_DEMAND


def test_enforce_budget_skips_when_env_override_set(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv(CONNECTION_BUDGET_OVERRIDE_ENV, "1")
    with caplog.at_level("WARNING", logger="app.db.pg_settings"):
        enforce_connection_budget(
            _fake_conn_budget(_OVER_BUDGET_MAX_CONN, _OVER_BUDGET_SUPERUSER_RESERVED), process="api"
        )  # over budget, but override set → no raise
    assert any(
        "running anyway because" in rec.message and CONNECTION_BUDGET_OVERRIDE_ENV in rec.message
        for rec in caplog.records
    )


def test_enforce_budget_fail_open_on_show_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """A transient SHOW failure must not block startup: ``check_*``
    fail-opens with ``(True, 0, 0)``; ``enforce_*`` sees ``passes=True``
    and returns without raising."""
    monkeypatch.delenv(CONNECTION_BUDGET_OVERRIDE_ENV, raising=False)
    enforce_connection_budget(_fake_conn_raising(RuntimeError("transient SHOW failure")), process="api")


def test_budget_exceeded_message_steers_to_shrink_not_raise(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The raise must steer the operator to SHRINK configured demand and
    name raising ``max_connections`` as diagnostic-only — never present
    raising the ceiling as the remediation (the #1472 anti-goal)."""
    monkeypatch.delenv(CONNECTION_BUDGET_OVERRIDE_ENV, raising=False)
    with pytest.raises(ConnectionBudgetExceeded) as exc:
        enforce_connection_budget(
            _fake_conn_budget(_OVER_BUDGET_MAX_CONN, _OVER_BUDGET_SUPERUSER_RESERVED), process="jobs"
        )
    msg = str(exc.value)
    assert "SHRINK" in msg
    assert "DIAGNOSTIC-ONLY" in msg
    assert CONNECTION_BUDGET_OVERRIDE_ENV in msg
    assert "jobs boot" in msg
