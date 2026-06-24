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

import psycopg
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

    Surface contract (#1331): this stub implements ONLY the connection
    surface ``assert_no_multixact_wraparound`` actually uses
    (``conn.execute(...)`` -> ``.fetchone()`` / ``.fetchall()``). It is
    deliberately NOT a full ``psycopg.Connection`` — the call sites pass
    it with ``# type: ignore[arg-type]``, an intentional stub
    substitution rather than a masked row-factory bug (cf.
    review-prevention-log "type: ignore[arg-type] masking a psycopg
    Connection row-factory mismatch": the stub returns tuple rows, so
    nothing is masked). The two ``test_stub_*`` guards below pin this
    surface so a future probe that drifts to an unstubbed surface (e.g.
    ``conn.cursor()``) fails loudly instead of silently passing, and so
    the stub can never claim a method ``psycopg`` does not have. Per
    settled-decisions "do not shape production APIs around test
    convenience", the probe signature stays ``psycopg.Connection`` — we
    pin the stub to that surface, not the reverse.

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


# ---------------------------------------------------------------------------
# Stub-surface guards (#1331): keep the stub honest against the real probe
# and the real ``psycopg`` ABC, so the four ``# type: ignore[arg-type]``
# substitutions above can never silently mask probe/API drift.
# ---------------------------------------------------------------------------


def test_probe_drifting_to_unstubbed_surface_fails_loud() -> None:
    """A probe reaching for a connection surface the stub does not
    implement (e.g. ``conn.cursor()``) must FAIL loudly, never silently
    pass.

    The current ``assert_no_multixact_wraparound`` uses only
    ``conn.execute(...)``. If a future revision drifts to
    ``conn.cursor().execute(...).fetchmany(...)``, the stub — which has
    no ``cursor`` attribute — raises ``AttributeError`` and the suite
    breaks, forcing the stub to be consciously extended (which in turn
    re-trips this guard). This is the #1331 acceptance criterion and a
    tripwire against a future too-permissive stub (e.g. a blanket
    ``__getattr__`` returning a mock) that would let drift pass green.
    """

    def _drifted_probe(conn: Any) -> None:
        cur = conn.cursor()  # surface the stub deliberately does not provide
        cur.execute("SELECT 1")
        cur.fetchmany(5)

    conn = _StubConn([(1,)])
    with pytest.raises(AttributeError):
        _drifted_probe(conn)


def test_stub_surface_is_subset_of_real_psycopg_abc() -> None:
    """Every public method the stubs implement must exist on the real
    ``psycopg`` class they stand in for.

    The stubs may implement FEWER methods than ``psycopg.Connection`` /
    ``psycopg.Cursor`` (they only need the probe's surface), but never a
    method ``psycopg`` lacks — otherwise the suite would pin the probe
    against a contract a live Postgres connection cannot honour, and the
    ``# type: ignore[arg-type]`` would be hiding a genuine API mismatch
    rather than a benign substitution.
    """
    conn_surface = {name for name in vars(_StubConn) if not name.startswith("_")}
    cursor_surface = {name for name in vars(_StubCursor) if not name.startswith("_")}

    conn_extra = conn_surface - set(dir(psycopg.Connection))
    cursor_extra = cursor_surface - set(dir(psycopg.Cursor))

    assert not conn_extra, f"_StubConn methods absent from psycopg.Connection: {conn_extra}"
    assert not cursor_extra, f"_StubCursor methods absent from psycopg.Cursor: {cursor_extra}"
