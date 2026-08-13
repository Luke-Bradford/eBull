"""#1455 — the dev-DB size tripwire's jobs-process exemption.

The size tripwire (``tests/conftest.py::_dev_db_size_tripwire``) fails the
session if ``ebull`` grows past tolerance, on the theory that a test opened
a raw ``psycopg.connect(settings.database_url)`` and wrote to the dev DB.
But when the operator's local jobs process is alive during a long full-suite
run, it legitimately writes tens of MB to ``ebull``. ``_jobs_process_running``
is the guard that distinguishes the two so the tripwire exempts (and warns
about) jobs-process growth rather than failing on it.

It reads ``pg_locks`` directly (no acquire-probe) so a passive tripwire never
perturbs the fence. These tests pin that read + the fail-safe error path; the
underlying ``-2`` corpse force-drop is covered by
``tests/test_dev_test_db_reaper.py``.
"""

from __future__ import annotations

import psycopg
import pytest

import tests.conftest as conftest


class _FakeCursor:
    def __init__(self, row: object) -> None:
        self._row = row

    def fetchone(self) -> object:
        return self._row


class _FakeConn:
    """Minimal stand-in for ``with psycopg.connect(...) as conn: conn.execute(...)``."""

    def __init__(self, row: object) -> None:
        self._row = row
        self.executed: tuple[str, tuple[object, ...]] | None = None

    def __enter__(self) -> _FakeConn:
        return self

    def __exit__(self, *_exc: object) -> bool:
        return False

    def execute(self, sql: str, params: tuple[object, ...]) -> _FakeCursor:
        self.executed = (sql, params)
        return _FakeCursor(self._row)


def test_jobs_process_running_true_when_lock_row_present(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeConn((1,))
    monkeypatch.setattr(psycopg, "connect", lambda *_a, **_k: fake)
    assert conftest._jobs_process_running() is True
    # Reads pg_locks for the advisory fence — never acquires it.
    assert fake.executed is not None
    sql, _params = fake.executed
    assert "pg_locks" in sql
    assert "advisory" in sql


def test_jobs_process_running_false_when_no_lock_row(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(psycopg, "connect", lambda *_a, **_k: _FakeConn(None))
    assert conftest._jobs_process_running() is False


def test_jobs_process_running_swallows_db_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """A read failure (dev DB down, etc.) must NOT mask a real leak — fall
    back to False so the tripwire runs its normal assertion."""

    def _boom(*_a: object, **_k: object) -> object:
        raise RuntimeError("dev DB unreachable")

    monkeypatch.setattr(psycopg, "connect", _boom)
    assert conftest._jobs_process_running() is False
