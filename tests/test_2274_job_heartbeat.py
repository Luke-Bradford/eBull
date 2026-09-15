"""#2274 — the ``job_runs`` heartbeat listener: install, chaining, cadence.

Pure-logic tier. The DB-backed half (real columns, real ``_tracked_job``
branches, the ``dev_reload`` ceiling) lives in
``tests/test_2274_job_heartbeat_db.py`` — a separate module ON PURPOSE,
because the ``db`` marker is applied per MODULE, so one DB test here would
evict every test in this file from the fast push gate.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, cast

import pytest

from app.db.background_write import set_background_pool
from app.services.job_heartbeat import JobRunHeartbeat, job_heartbeat
from app.services.sync_orchestrator.progress import (
    active_progress_callback,
    clear_active_progress,
    report_progress,
    set_active_progress,
)


class _StubConnection:
    """Minimal psycopg-shaped connection recording what was executed."""

    def __init__(self, fail: bool = False) -> None:
        self.autocommit = False
        self.statements: list[tuple[Any, Any]] = []
        self._fail = fail

    class _Info:
        # The seam checks this to decide whether to roll back before
        # returning the connection; IDLE means "nothing to clean up".
        transaction_status = __import__("psycopg").pq.TransactionStatus.IDLE

    info = _Info()

    @contextmanager
    def transaction(self) -> Iterator[None]:
        yield

    def execute(self, query: Any, params: Any = None) -> None:
        if self._fail:
            raise RuntimeError("heartbeat write failed")
        self.statements.append((query, params))

    def rollback(self) -> None:  # pragma: no cover - IDLE above means unused
        pass


@contextmanager
def _pool(conn: _StubConnection) -> Iterator[None]:
    class _Pool:
        @contextmanager
        def connection(self) -> Iterator[_StubConnection]:
            yield conn

    set_background_pool(cast(Any, _Pool()))
    try:
        yield
    finally:
        set_background_pool(None)


@pytest.fixture(autouse=True)
def _no_pool_leak() -> Iterator[None]:
    set_background_pool(None)
    yield
    set_background_pool(None)


def _updates(conn: _StubConnection) -> list[Any]:
    """Params of every job_runs UPDATE (the SET LOCAL carries none)."""
    return [params for _query, params in conn.statements if params is not None]


# ---------------------------------------------------------------------------
# Install gating
# ---------------------------------------------------------------------------


def test_no_run_id_installs_nothing() -> None:
    """``_tracked_job``'s start-failure path runs the body with no row."""
    conn = _StubConnection()
    with _pool(conn), job_heartbeat(0):
        assert active_progress_callback() is None
        report_progress(5, 10, force=True)
    assert conn.statements == []


def test_no_background_pool_installs_nothing() -> None:
    """⚠⚠ The OWNERSHIP predicate. ``_LIVE_JOB_SQL`` has no ownership column
    and ``dev_reload`` justifies that with "there is exactly one jobs
    process". A CLI/script/test run of a tracked job would break that premise
    and let an unrelated process defer the supervisor's reload. Pool
    registration happens only in ``app/jobs/__main__.py``, so it IS "am I the
    daemon"."""
    with job_heartbeat(42):
        assert active_progress_callback() is None


def test_install_fires_no_synthetic_tick() -> None:
    """``set_active_progress`` ticks ``(0, None)`` at install by design. Here
    that would stamp ``last_progress_at`` on the ~80 tracked jobs whose bodies
    never report — a fabricated progress claim, which is the one thing #2274
    forbids."""
    conn = _StubConnection()
    with _pool(conn), job_heartbeat(42):
        assert isinstance(active_progress_callback(), JobRunHeartbeat)
        assert conn.statements == []


def test_the_default_install_still_ticks() -> None:
    """The suppression is opt-in, so the orchestrator's own install
    (``adapters.py``) is unchanged."""
    seen: list[tuple[int, int | None]] = []
    token = set_active_progress(lambda done, total=None: seen.append((done, total)))
    clear_active_progress(token)
    assert seen == [(0, None)]


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------


def test_a_real_tick_writes_the_three_columns() -> None:
    conn = _StubConnection()
    with _pool(conn), job_heartbeat(42):
        report_progress(7, 99, force=True)
    assert _updates(conn) == [{"processed": 7, "target": 99, "run_id": 42}]


def test_an_unbounded_tick_stores_null_rather_than_retaining_a_total() -> None:
    """``sql/140``: "``target_count`` is nullable — NULL means unbounded". NULL
    is a value the producer ASSERTS, not a gap to back-fill, so no COALESCE."""
    conn = _StubConnection()
    hb = JobRunHeartbeat(42, inner=None)
    with _pool(conn):
        hb(3, 9)
        hb._last_write = 0.0  # bypass the cadence floor; it is tested separately
        hb(4, None)
    assert [p["target"] for p in _updates(conn)] == [9, None]


def test_the_cadence_floor_bounds_writes_that_force_past_report_progress() -> None:
    """⚠ ``report_progress``'s own throttle is bypassed by ``force=True``,
    which four of the covered tick sites use — so it bounds nothing and the
    writer keeps its own floor."""
    conn = _StubConnection()
    with _pool(conn), job_heartbeat(42):
        for i in range(1, 21):
            report_progress(i, 20, force=True)
    assert len(_updates(conn)) == 1


def test_the_floor_advances_on_a_FAILED_attempt() -> None:
    """Otherwise a sustained DB fault turns every tick into a retry."""
    conn = _StubConnection(fail=True)
    hb = JobRunHeartbeat(42, inner=None)
    with _pool(conn):
        hb(1, 20)
        assert hb._last_write != 0.0
        hb(2, 20)  # inside the floor — must not even attempt


def test_a_write_failure_never_reaches_the_body() -> None:
    conn = _StubConnection(fail=True)
    with _pool(conn), job_heartbeat(42):
        report_progress(1, 20, force=True)  # must not raise


# ---------------------------------------------------------------------------
# Chaining
# ---------------------------------------------------------------------------


def test_it_chains_to_the_callback_it_replaced() -> None:
    """The orchestrator installs OUTSIDE ``legacy_fn()`` and ``_tracked_job``
    opens INSIDE it, so an install that did not chain would blank the sync-run
    progress surface for the whole job."""
    conn = _StubConnection()
    seen: list[tuple[int, int | None]] = []
    token = set_active_progress(lambda done, total=None: seen.append((done, total)))
    try:
        with _pool(conn), job_heartbeat(42):
            report_progress(7, 99, force=True)
    finally:
        clear_active_progress(token)
    assert seen == [(0, None), (7, 99)]
    assert _updates(conn) == [{"processed": 7, "target": 99, "run_id": 42}]


def test_it_chains_PAST_a_heartbeat_rather_than_to_it() -> None:
    """⚠⚠ ``fundamentals_sync`` calls ``daily_financial_facts()`` directly, so
    a ``_tracked_job`` really does nest. Chaining child to parent would fill
    the PARENT's ``processed_count`` with the child's item counts — a
    fabricated measurement of a different unit of work."""
    conn = _StubConnection()
    seen: list[tuple[int, int | None]] = []
    token = set_active_progress(lambda done, total=None: seen.append((done, total)))
    try:
        with _pool(conn), job_heartbeat(10):  # parent
            with job_heartbeat(20):  # child
                report_progress(7, 99, force=True)
    finally:
        clear_active_progress(token)
    # Only the CHILD's row is written; the orchestrator still sees the tick.
    assert [p["run_id"] for p in _updates(conn)] == [20]
    assert seen == [(0, None), (7, 99)]


def test_a_raising_inner_callback_does_not_cost_the_heartbeat() -> None:
    conn = _StubConnection()

    def _boom(done: int, total: int | None = None) -> None:
        raise RuntimeError("orchestrator side failed")

    token = set_active_progress(_boom)
    try:
        with _pool(conn), job_heartbeat(42):
            report_progress(7, 99, force=True)
    finally:
        clear_active_progress(token)
    assert _updates(conn) == [{"processed": 7, "target": 99, "run_id": 42}]


# ---------------------------------------------------------------------------
# ContextVar lifetime
# ---------------------------------------------------------------------------


def test_the_body_exception_propagates_and_the_token_is_restored() -> None:
    conn = _StubConnection()
    with pytest.raises(RuntimeError, match="body"), _pool(conn), job_heartbeat(42):
        raise RuntimeError("body")
    assert active_progress_callback() is None


def test_nested_unwind_restores_the_parent_not_none() -> None:
    conn = _StubConnection()
    with _pool(conn), job_heartbeat(10):
        parent = active_progress_callback()
        with job_heartbeat(20):
            assert active_progress_callback() is not parent
        assert active_progress_callback() is parent
    assert active_progress_callback() is None
