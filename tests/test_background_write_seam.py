"""#1472 PR4c — the background-write seam (app/db/background_write.py).

``background_write_connection()`` borrows from the registered jobs-process
``BackgroundConnectionPool`` when set, else falls back to a fresh raw autocommit
connection. The sync-orchestrator audit/progress writers route through it.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

from app.db.background_write import (
    background_write_connection,
    get_background_pool,
    set_background_pool,
)
from app.services.sync_orchestrator import executor
from tests.fixtures.ebull_test_db import test_database_url, test_db_available


@pytest.fixture(autouse=True)
def _clear_background_pool_global() -> Iterator[None]:
    """Always clear the process-global before AND after each test so a test
    that registers a pool can never leak it into another test (Codex
    PR4c-ckpt-1b). Belt-and-suspenders: clear on entry too."""
    set_background_pool(None)
    yield
    set_background_pool(None)


@pytest.fixture
def settings_use_test_db(monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    from app.config import settings

    url = test_database_url()
    monkeypatch.setattr(settings, "database_url", url)
    yield url


def test_set_get_background_pool_round_trip() -> None:
    assert get_background_pool() is None
    sentinel = MagicMock(name="pool")
    set_background_pool(sentinel)
    assert get_background_pool() is sentinel
    set_background_pool(None)
    assert get_background_pool() is None


def test_background_write_connection_borrows_from_pool_when_set() -> None:
    fake_conn = MagicMock(name="conn")
    borrowed: list[object] = []

    @contextmanager
    def fake_pool_connection() -> Iterator[MagicMock]:
        borrowed.append("checkout")
        yield fake_conn

    fake_pool = MagicMock(name="bg_pool")
    fake_pool.connection.side_effect = fake_pool_connection
    set_background_pool(fake_pool)

    with background_write_connection() as conn:
        assert conn is fake_conn
    assert borrowed == ["checkout"]  # borrowed from the registered pool


@pytest.mark.skipif(not test_db_available(), reason="test DB unavailable")
def test_background_write_connection_raw_fallback_when_unset(settings_use_test_db: str) -> None:
    # Global cleared (autouse) → raw fallback to the test DB, autocommit.
    with background_write_connection() as conn:
        assert conn.autocommit is True
        assert conn.execute("SELECT 1").fetchone() == (1,)


@pytest.mark.skipif(not test_db_available(), reason="test DB unavailable")
def test_executor_audit_writer_routes_through_seam(monkeypatch: pytest.MonkeyPatch, settings_use_test_db: str) -> None:
    """An executor audit writer must borrow via ``background_write_connection``
    (not a direct ``psycopg.connect``), so when the jobs process registers a
    pool the write lands on it."""
    calls: list[int] = []
    real = executor.background_write_connection

    @contextmanager
    def spy() -> Iterator[object]:
        calls.append(1)
        with real() as conn:  # delegate to the real seam (raw fallback → test DB)
            yield conn

    monkeypatch.setattr(executor, "background_write_connection", spy)
    # UPDATE matches 0 rows for a non-existent run/layer — still a real, clean
    # statement; we only assert the writer went through the seam.
    executor._record_layer_started(999_999_999, "no_such_layer")
    assert calls == [1]


@pytest.mark.skipif(not test_db_available(), reason="test DB unavailable")
def test_seam_falls_back_to_raw_when_registered_pool_is_closed(settings_use_test_db: str) -> None:
    """Shutdown race (Codex PR4c-ckpt-2): if the registered pool is already
    CLOSED when a late audit write borrows, the seam must degrade to a raw
    connection instead of raising — the write is never lost."""
    from app.jobs.background_pool import BackgroundConnectionPool

    bg = BackgroundConnectionPool(max_size=1)
    bg.close()  # closed BEFORE registration → every borrow raises BackgroundPoolClosed
    set_background_pool(bg)
    try:
        with background_write_connection() as conn:  # must NOT raise
            assert conn.execute("SELECT 1").fetchone() == (1,)
    finally:
        set_background_pool(None)


def test_all_executor_audit_writers_route_through_the_seam() -> None:
    """Pin the sweep: every executor audit/progress writer borrows via the seam,
    and none reverted to a raw ``autocommit=True`` connect (Codex PR4c-ckpt-2).
    A new audit writer added with a raw connect trips this guard."""
    from pathlib import Path

    src = Path(executor.__file__).read_text()
    # The 8 converted writers: _record_layer_started/result/failed/skipped,
    # _finalize_sync_run, _finalize_cancelled_sync_run, _fail_unfinished_layers,
    # and the _make_progress_callback closure.
    assert src.count("background_write_connection() as conn:") == 8
    # No audit-style raw autocommit connect remains (the PR4a gate-check conns
    # use a different binding — `as owned:` / a holder attribute — so this is
    # specific to the audit writers).
    assert "psycopg.connect(settings.database_url, autocommit=True) as conn:" not in src


@pytest.mark.skipif(not test_db_available(), reason="test DB unavailable")
def test_pooled_write_rollback_does_not_poison_next_borrow(settings_use_test_db: str) -> None:
    """A failed write inside ``with conn.transaction()`` on a pooled autocommit
    conn must roll back and return a clean conn to the pool — the next borrow
    must work (Codex PR4c-ckpt-1b)."""
    from app.jobs.background_pool import BackgroundConnectionPool

    bg = BackgroundConnectionPool(max_size=2)
    set_background_pool(bg)
    try:
        with pytest.raises(Exception):
            with background_write_connection() as conn:
                with conn.transaction():
                    conn.execute("SELECT * FROM _seam_no_such_table")
        # Next borrow on the same pool must be clean + usable.
        with background_write_connection() as conn:
            assert conn.execute("SELECT 1").fetchone() == (1,)
    finally:
        set_background_pool(None)
        bg.close()
