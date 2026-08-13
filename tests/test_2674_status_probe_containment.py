"""`/system/status` probe containment on a shared transactional connection (#2674).

The defect these pin: **a `try/except` around a DB call contains the exception,
not the transaction.** ``get_conn`` hands out a NON-autocommit pooled
connection, so a failed query leaves Postgres' transaction aborted; catching the
Python exception does not clear it, and every later statement on that connection
raises ``InFailedSqlTransaction``. The damage therefore lands on an unrelated
later probe, which is why the broken code reads as correct at the call site.

Measured on dev before the fix:

    caught: UndefinedTable
    subsequent query RAISES: InFailedSqlTransaction
    --- same failure inside conn.transaction() ---
    caught in savepoint: UndefinedTable
    after savepoint: OK

Kept pure (no Postgres) so these sit in the fast tier: the fakes below model the
one rule that matters — statements raise while ``aborted`` is set, and only the
savepoint rollback clears it.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.api.system import (
    _build_credential_health_summary,
    _jobs_process_down,
    _stalled_job_names,
)

_NOW = datetime(2026, 8, 13, 12, 0, 0, tzinfo=UTC)


class _FakeTransaction:
    def __init__(self, conn: FakeConn) -> None:
        self._conn = conn

    def __enter__(self) -> _FakeTransaction:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
        if exc_type is not None:
            self._conn.aborted = False  # the savepoint rollback
        return False


class FakeConn:
    """A connection that models Postgres' aborted-transaction rule.

    Any statement poisons the transaction on the way out; every later
    statement then fails with ``InFailedSqlTransaction`` until a savepoint
    rollback clears it. That is the whole behaviour under test.
    """

    def __init__(self) -> None:
        self.aborted = False

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction(self)

    def blow_up(self) -> None:
        if self.aborted:
            raise RuntimeError("InFailedSqlTransaction")
        self.aborted = True
        raise RuntimeError("relation does not exist")

    def next_statement_ok(self) -> bool:
        return not self.aborted


def test_stalled_job_probe_leaves_the_connection_usable(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn()
    monkeypatch.setattr(
        "app.services.job_liveness.find_stalled_jobs",
        lambda c, jobs, now: c.blow_up(),
    )

    assert _stalled_job_names(conn, _NOW) == set()  # type: ignore[arg-type]
    assert conn.next_statement_ok(), "a failed stall probe left the transaction aborted"


def test_jobs_process_probe_leaves_the_connection_usable(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn()
    monkeypatch.setattr(
        "app.api.system._build_jobs_process_health",
        lambda c, now: c.blow_up(),
    )

    assert _jobs_process_down(conn, _NOW) is False  # type: ignore[arg-type]
    assert conn.next_statement_ok(), "a failed heartbeat probe left the transaction aborted"


def test_credential_summary_reports_missing_instead_of_500ing(monkeypatch: pytest.MonkeyPatch) -> None:
    """The endpoint-level consequence, at the function it actually lands on.

    ``_build_credential_health_summary`` runs OUTSIDE ``get_system_status``'s
    503 guard, and its ``sole_operator_id`` lookup previously caught only the
    two operator-cardinality errors — so a DB-level fault there (the poisoned
    transaction, before the fix upstream; its own fault, after) escaped as an
    HTTP 500 on the page whose entire job is to stay readable when something
    is broken.
    """
    conn = FakeConn()
    monkeypatch.setattr("app.services.operators.sole_operator_id", lambda c: c.blow_up())

    summary = _build_credential_health_summary(conn)  # type: ignore[arg-type]

    assert summary.state == "missing"
    assert conn.next_statement_ok(), "a failed credential lookup left the transaction aborted"
