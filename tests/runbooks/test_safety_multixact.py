"""Acceptance tests for ``assert_no_multixact_wraparound``.

Uses a stub psycopg connection so we don't need a real PG with a
synthetic high-age multixact state. Pins the catalog-probe contract:

* When ``pg_database.datminmxid`` age exceeds 80% of
  ``autovacuum_multixact_freeze_max_age`` → refuse.
* When top-5 ``pg_class.relminmxid`` ages exceed threshold → refuse.
* Otherwise → pass (no raise).

Why: ``pg_resetwal``-damaged dev DBs (per ``project_1233_pr12_*``)
carry stale multixact state. Without this gate, a destructive runbook
mid-``--apply`` hits the wraparound and partially nukes the DB.

References:
* ``app/runbooks/safety.py:assert_no_multixact_wraparound``
* ``docs/proposals/etl/run-8-readiness-fixes.md`` §Item 2
* ``docs/specs/etl/retention-rubric.md`` §6.3 pre-wipe procedure
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest

from app.runbooks.safety import RunbookRefused, assert_no_multixact_wraparound


class _StubCursor:
    """Cursor stub holding a single canned response.

    Returns the response from both fetchone() and fetchall() so the
    same stub serves both shapes used in
    ``assert_no_multixact_wraparound``.
    """

    def __init__(self, response: Any) -> None:
        self._response = response

    def fetchone(self) -> Any:
        return self._response

    def fetchall(self) -> list[Any]:
        # fetchall expects a list-of-rows; if response is already a
        # list, return as-is; else wrap.
        if isinstance(self._response, list):
            return self._response
        return [self._response] if self._response is not None else []


class _StubConn:
    """Minimal psycopg.Connection stub for unit-testing safety probes.

    Each .execute() pops the next canned response from a queue. Tests
    set up the queue to mirror the exact sequence of cursors returned
    by ``assert_no_multixact_wraparound``:

    1. ``SHOW autovacuum_multixact_freeze_max_age`` → fetchone tuple.
    2. ``SELECT mxid_age(datminmxid) FROM pg_database WHERE ...`` →
       fetchone tuple.
    3. ``SELECT ... FROM pg_class ... ORDER BY age DESC LIMIT 5`` →
       fetchall list-of-tuples.
    """

    def __init__(self, responses: Sequence[Any]) -> None:
        self._queue = list(responses)

    def execute(self, _sql: str, _params: Any = None) -> _StubCursor:
        if not self._queue:
            raise AssertionError("StubConn ran out of canned responses")
        return _StubCursor(self._queue.pop(0))


def _freeze_max_age_row(value: int) -> tuple[str]:
    """``SHOW`` returns the value as a string."""
    return (str(value),)


def _db_age_row(age: int) -> tuple[int]:
    return (age,)


def _table_age_rows(rows: list[tuple[str, int]]) -> list[tuple[str, int]]:
    return rows


def test_passes_when_all_ages_under_threshold() -> None:
    """Healthy DB: pg_database age + table ages all under 80% threshold → no raise."""
    freeze_max_age = 200_000_000  # PG default
    safe_age = int(freeze_max_age * 0.5)
    conn = _StubConn(
        [
            _freeze_max_age_row(freeze_max_age),
            _db_age_row(safe_age),
            _table_age_rows([("public.foo", safe_age), ("public.bar", safe_age // 2)]),
        ]
    )
    assert_no_multixact_wraparound(conn)  # type: ignore[arg-type]


def test_refuses_when_database_age_exceeds_threshold() -> None:
    """pg_database.datminmxid age ≥ 80% × max_age → RunbookRefused."""
    freeze_max_age = 200_000_000
    breached_age = int(freeze_max_age * 0.9)
    conn = _StubConn(
        [
            _freeze_max_age_row(freeze_max_age),
            _db_age_row(breached_age),
        ]
    )
    with pytest.raises(RunbookRefused) as exc_info:
        assert_no_multixact_wraparound(conn)  # type: ignore[arg-type]
    assert "pg_database.datminmxid" in exc_info.value.msg
    assert "§6.3" in exc_info.value.msg


def test_refuses_when_table_age_exceeds_threshold() -> None:
    """pg_class.relminmxid for any top-5 table ≥ threshold → RunbookRefused."""
    freeze_max_age = 200_000_000
    safe_age = int(freeze_max_age * 0.3)
    breached_age = int(freeze_max_age * 0.9)
    conn = _StubConn(
        [
            _freeze_max_age_row(freeze_max_age),
            _db_age_row(safe_age),
            _table_age_rows(
                [
                    ("public.job_runtime_heartbeat", breached_age),
                    ("public.broker_credentials", safe_age),
                ]
            ),
        ]
    )
    with pytest.raises(RunbookRefused) as exc_info:
        assert_no_multixact_wraparound(conn)  # type: ignore[arg-type]
    assert "pg_class.relminmxid" in exc_info.value.msg
    assert "job_runtime_heartbeat" in exc_info.value.msg
    assert "§6.3" in exc_info.value.msg


def test_refuses_lists_multiple_breaching_tables() -> None:
    """Both top-5 tables over threshold: error message names both."""
    freeze_max_age = 200_000_000
    breached_age = int(freeze_max_age * 0.85)
    conn = _StubConn(
        [
            _freeze_max_age_row(freeze_max_age),
            _db_age_row(int(freeze_max_age * 0.5)),  # DB itself ok
            _table_age_rows(
                [
                    ("public.job_runtime_heartbeat", breached_age),
                    ("public.broker_credentials", breached_age),
                ]
            ),
        ]
    )
    with pytest.raises(RunbookRefused) as exc_info:
        assert_no_multixact_wraparound(conn)  # type: ignore[arg-type]
    msg = exc_info.value.msg
    assert "job_runtime_heartbeat" in msg
    assert "broker_credentials" in msg
