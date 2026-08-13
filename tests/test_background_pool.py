"""#1472 PR4b — bounded self-healing background connection pool.

Two layers:
- Real test-DB: ``open_pool(autocommit=...)`` configures pooled conns;
  ``BackgroundConnectionPool`` round-trips a real write with real
  BEGIN/COMMIT semantics; application_name labelling.
- No-DB unit: the generation-safe hard-recreate state machine, driven by a
  fake underlying pool whose ``connection()`` is scripted to fail/succeed.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest
from psycopg_pool import PoolTimeout

from app.db.pool import BACKGROUND_POOL_MAX_SIZE, open_pool
from app.jobs.background_pool import (
    BACKGROUND_POOL_APPLICATION_NAME,
    BackgroundConnectionPool,
)
from tests.fixtures.ebull_test_db import test_database_url, test_db_available


@pytest.fixture
def settings_use_test_db(monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """Point ``settings.database_url`` at the per-worker test DB so the
    real-DB pool tests never touch the operator's dev DB."""
    from app.config import settings

    url = test_database_url()
    monkeypatch.setattr(settings, "database_url", url)
    yield url


# ── Real test-DB ──────────────────────────────────────────────────


@pytest.mark.skipif(not test_db_available(), reason="test DB unavailable")
def test_open_pool_autocommit_sets_conn_autocommit(settings_use_test_db: str) -> None:
    pool = open_pool("test_autocommit_pool", min_size=1, max_size=1, autocommit=True)
    try:
        with pool.connection() as conn:
            assert conn.autocommit is True
    finally:
        pool.close()


@pytest.mark.skipif(not test_db_available(), reason="test DB unavailable")
def test_open_pool_default_not_autocommit(settings_use_test_db: str) -> None:
    pool = open_pool("test_default_pool", min_size=1, max_size=1)
    try:
        with pool.connection() as conn:
            assert conn.autocommit is False
    finally:
        pool.close()


@pytest.mark.skipif(not test_db_available(), reason="test DB unavailable")
def test_open_pool_stamps_application_name(settings_use_test_db: str) -> None:
    pool = open_pool("test_appname_pool", min_size=1, max_size=1, application_name="ebull-test-appname")
    try:
        with pool.connection() as conn:
            row = conn.execute("SELECT current_setting('application_name')").fetchone()
            assert row is not None
            assert row[0] == "ebull-test-appname"
    finally:
        pool.close()


@pytest.mark.skipif(not test_db_available(), reason="test DB unavailable")
def test_background_pool_round_trips_a_real_write(settings_use_test_db: str) -> None:
    import psycopg

    bg = BackgroundConnectionPool(max_size=2)
    # A real (non-temp) table so durability is verifiable from a SEPARATE
    # connection — proving ``with conn.transaction()`` on the autocommit pooled
    # conn issued a real COMMIT, not a session-local SAVEPOINT (Codex ckpt-2).
    try:
        with bg.connection() as conn:
            assert conn.autocommit is True
            with conn.transaction():
                conn.execute("CREATE TABLE IF NOT EXISTS _pr4b_durability (n int)")
                conn.execute("DELETE FROM _pr4b_durability")
                conn.execute("INSERT INTO _pr4b_durability VALUES (42)")
        # Verify on an INDEPENDENT connection (different session).
        with psycopg.connect(settings_use_test_db, autocommit=True) as verify:
            row = verify.execute("SELECT n FROM _pr4b_durability").fetchone()
            assert row is not None and row[0] == 42
        # application_name visible in pg_stat_activity for this pool's conns.
        with bg.connection() as conn:
            row = conn.execute("SELECT current_setting('application_name')").fetchone()
            assert row is not None and row[0] == BACKGROUND_POOL_APPLICATION_NAME
        m = bg.metrics()
        assert m["checkouts"] >= 2
        assert m["hard_recreates"] == 0
        assert m["generation"] == 0
    finally:
        try:
            with psycopg.connect(settings_use_test_db, autocommit=True) as cleanup:
                cleanup.execute("DROP TABLE IF EXISTS _pr4b_durability")
        finally:
            bg.close()


@pytest.mark.skipif(not test_db_available(), reason="test DB unavailable")
def test_background_pool_close_is_idempotent_and_blocks_reuse(settings_use_test_db: str) -> None:
    bg = BackgroundConnectionPool(max_size=1)
    bg.close()
    bg.close()  # idempotent — no raise
    with pytest.raises(RuntimeError):
        with bg.connection():
            pass


def test_background_pool_default_max_size_is_budget_constant() -> None:
    # The default the pool builds with must be the budget-counted constant so
    # the two cannot drift (the boot guard sizes the budget off this value).
    import inspect

    sig = inspect.signature(BackgroundConnectionPool.__init__)
    assert sig.parameters["max_size"].default == BACKGROUND_POOL_MAX_SIZE


# ── No-DB unit: hard-recreate state machine ───────────────────────


class _FakePool:
    """Stand-in for the underlying ``ConnectionPool``. ``connection()`` fails
    (raises ``PoolTimeout`` at checkout) for the first ``fail_times`` calls,
    then yields a fake conn."""

    def __init__(self, *, fail_times: int = 0) -> None:
        self.fail_times = fail_times
        self.calls = 0
        self.closed = False

    @contextmanager
    def connection(self) -> Iterator[MagicMock]:
        self.calls += 1
        if self.calls <= self.fail_times:
            raise PoolTimeout("synthetic checkout failure")
        yield MagicMock(name="fake_conn")

    def close(self) -> None:
        self.closed = True


class _ScriptedBackgroundPool(BackgroundConnectionPool):
    """BackgroundConnectionPool whose ``_build_pool`` returns pre-scripted
    fakes instead of a real pool — drives the recreate state machine."""

    def __init__(self, fakes: list[_FakePool], **kw: object) -> None:
        self._fakes = list(fakes)
        self._build_calls = 0
        super().__init__(**kw)  # type: ignore[arg-type]

    def _build_pool(self):  # type: ignore[override]
        self._build_calls += 1
        return self._fakes.pop(0)


def _borrow_swallow(bg: BackgroundConnectionPool) -> None:
    """Borrow once, swallowing a checkout PoolTimeout (what a real caller's
    fail-open handler would do)."""
    try:
        with bg.connection():
            pass
    except PoolTimeout:
        pass


def test_hard_recreate_after_consecutive_checkout_failures() -> None:
    failing = _FakePool(fail_times=99)  # always fails
    healthy = _FakePool(fail_times=0)
    bg = _ScriptedBackgroundPool([failing, healthy], recreate_after=3)

    # 3 consecutive checkout failures → recreate on the 3rd.
    _borrow_swallow(bg)
    _borrow_swallow(bg)
    assert bg.metrics()["hard_recreates"] == 0  # not yet
    _borrow_swallow(bg)

    m = bg.metrics()
    assert m["hard_recreates"] == 1
    assert m["generation"] == 1
    assert m["pool_timeouts"] == 3
    assert failing.closed is True  # retired pool closed
    assert m["last_recreate_at"] is not None

    # Next borrow succeeds on the healthy pool → consecutive counter resets.
    with bg.connection() as conn:
        assert conn is not None
    assert bg.metrics()["checkouts"] == 1


def test_intermittent_failures_do_not_recreate() -> None:
    # Fail, succeed, fail, succeed... never 3 in a row → no recreate.
    p = _FakePool(fail_times=0)

    class _FlapPool(_FakePool):
        @contextmanager
        def connection(self) -> Iterator[MagicMock]:  # type: ignore[override]
            self.calls += 1
            if self.calls % 2 == 1:  # odd calls fail
                raise PoolTimeout("synthetic")
            yield MagicMock()

    flap = _FlapPool()
    bg = _ScriptedBackgroundPool([flap], recreate_after=3)
    for _ in range(6):
        _borrow_swallow(bg)
    assert bg.metrics()["hard_recreates"] == 0
    assert flap.closed is False
    del p


def test_recreate_build_failure_keeps_old_pool_and_retries() -> None:
    failing = _FakePool(fail_times=99)

    class _RaisingScripted(BackgroundConnectionPool):
        def __init__(self) -> None:
            self._first = failing
            self._build_calls = 0
            super().__init__(recreate_after=2)

        def _build_pool(self):  # type: ignore[override]
            self._build_calls += 1
            if self._build_calls == 1:
                return self._first  # initial pool
            raise PoolTimeout("synthetic: PG still down at recreate")

    bg = _RaisingScripted()
    # 2 failures → attempt recreate → build raises → keep old pool, counter at
    # threshold so the NEXT failure retries.
    _borrow_swallow(bg)
    _borrow_swallow(bg)
    m = bg.metrics()
    assert m["hard_recreates"] == 0  # build failed → no successful recreate
    assert m["generation"] == 0
    assert failing.closed is False  # old pool retained
    # One more failure retries the recreate immediately (counter held at threshold).
    _borrow_swallow(bg)
    assert bg._build_calls == 3  # initial + 2 recreate attempts


def test_stale_generation_failure_does_not_double_recreate() -> None:
    failing = _FakePool(fail_times=99)
    healthy = _FakePool(fail_times=0)
    bg = _ScriptedBackgroundPool([failing, healthy], recreate_after=1)

    _borrow_swallow(bg)  # 1 failure @ recreate_after=1 → recreate to gen 1
    assert bg.metrics()["generation"] == 1
    assert bg.metrics()["hard_recreates"] == 1

    # A failure reported against the STALE generation 0 must not recreate again.
    bg._on_checkout_failure(0, "pool_timeouts")
    m = bg.metrics()
    assert m["generation"] == 1  # unchanged
    assert m["hard_recreates"] == 1  # unchanged


def test_close_during_recreate_discards_new_pool() -> None:
    """If shutdown closes the wrapper while a hard-recreate is building the
    replacement pool, the freshly built pool must be discarded (closed), NOT
    swapped onto the closed wrapper (Codex PR4b-ckpt-2 HIGH)."""
    failing = _FakePool(fail_times=99)
    new_fake = _FakePool(fail_times=0)

    class _CloseDuringBuild(BackgroundConnectionPool):
        def __init__(self) -> None:
            self._n = 0
            super().__init__(recreate_after=1)

        def _build_pool(self):  # type: ignore[override]
            self._n += 1
            if self._n == 1:
                return failing  # initial pool
            # Simulate shutdown winning the race WHILE we build the replacement.
            self.close()
            return new_fake

    bg = _CloseDuringBuild()
    _borrow_swallow(bg)  # 1 failure @ recreate_after=1 → recreate; close() races in

    assert failing.closed is True  # old pool closed by close()
    assert new_fake.closed is True  # freshly built pool discarded, not swapped
    m = bg.metrics()
    assert m["hard_recreates"] == 0  # swap never happened
    assert m["generation"] == 0


def test_metrics_thread_safe_counters() -> None:
    healthy = _FakePool(fail_times=0)
    bg = _ScriptedBackgroundPool([healthy], recreate_after=3)

    def worker() -> None:
        for _ in range(50):
            with bg.connection():
                pass

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert bg.metrics()["checkouts"] == 200
