"""#1187 — PG max_locks_per_transaction boot guard unit tests.

Pure-logic unit tests using a fake ``psycopg.Connection``-shaped
``MagicMock``. No DB connection required.

Spec: ``docs/superpowers/specs/2026-05-17-pg-max-locks-per-tx-guard.md``.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.db.pg_settings import (
    PG_LOCKS_FLOOR,
    PG_LOCKS_OVERRIDE_ENV,
    PgLocksFloorBreached,
    check_max_locks_per_transaction,
    enforce_max_locks_floor,
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
