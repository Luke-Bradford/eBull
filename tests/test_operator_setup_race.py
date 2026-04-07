"""Real-DB race-safety test for first-run operator setup.

The advisory lock in ``perform_setup`` (sql/017 + operator_setup.py) is
the load-bearing piece that makes simultaneous /auth/setup requests safe
under READ COMMITTED. Mocks cannot exercise it -- we need a real
Postgres connection so the lock is actually taken.

Strategy:
  1. Open a "blocker" connection and take ``pg_advisory_xact_lock`` on
     the same key inside an open transaction.
  2. Spawn a worker thread that calls ``perform_setup`` with a fresh
     connection. The worker must block on the advisory lock.
  3. Assert the worker is still blocked after a short wait.
  4. Commit the blocker -- the worker now proceeds and succeeds.
  5. Run a second perform_setup against an already-populated table and
     assert it returns ALREADY_SETUP.

Skipped automatically if no Postgres URL is configured or the DB is
unreachable -- we never want this test to fail in a CI run that has no
Postgres at all, but we do want it to fail loudly if the lock is
removed.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator

import psycopg
import pytest

from app.config import settings
from app.services.operator_setup import (
    _BOOTSTRAP_LOCK_KEY,
    SetupOutcome,
    perform_setup,
    reset_token_slot_for_tests,
)


def _db_available() -> bool:
    try:
        with psycopg.connect(settings.database_url, connect_timeout=2) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _db_available(),
    reason="Postgres not reachable -- skipping real-DB race test",
)


@pytest.fixture
def clean_operators() -> Iterator[None]:
    """Wipe operators + sessions + audit so the test starts on an empty table.

    Uses TRUNCATE ... CASCADE so any FK-referenced rows are also cleared.
    """
    with psycopg.connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE operators, sessions, operator_audit RESTART IDENTITY CASCADE")
        conn.commit()
    reset_token_slot_for_tests()
    yield
    with psycopg.connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE operators, sessions, operator_audit RESTART IDENTITY CASCADE")
        conn.commit()
    reset_token_slot_for_tests()


def test_advisory_lock_serialises_concurrent_setup(clean_operators: None) -> None:
    """A held advisory lock must block perform_setup until released."""
    blocker = psycopg.connect(settings.database_url)
    blocker.autocommit = False
    with blocker.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(%s)", (_BOOTSTRAP_LOCK_KEY,))

    result: dict[str, object] = {}

    def worker() -> None:
        worker_conn = psycopg.connect(settings.database_url)
        try:
            outcome, success = perform_setup(
                worker_conn,
                username="alice",
                password="correct horse battery staple",
                submitted_token=None,
                request_host="127.0.0.1",
                user_agent="pytest",
                request_ip="127.0.0.1",
            )
            result["outcome"] = outcome
            result["success"] = success
        finally:
            worker_conn.close()

    t = threading.Thread(target=worker, daemon=True)
    t.start()

    # Worker should be blocked on the advisory lock.
    time.sleep(0.5)
    assert t.is_alive(), "perform_setup did not block on the advisory lock"
    assert "outcome" not in result

    # Release the blocker -- worker proceeds.
    blocker.commit()
    blocker.close()

    t.join(timeout=5.0)
    assert not t.is_alive(), "perform_setup did not finish after lock release"
    assert result["outcome"] is SetupOutcome.OK

    # Second call against the now-populated table must short-circuit.
    with psycopg.connect(settings.database_url) as conn:
        outcome, success = perform_setup(
            conn,
            username="bob",
            password="correct horse battery staple",
            submitted_token=None,
            request_host="127.0.0.1",
            user_agent="pytest",
            request_ip="127.0.0.1",
        )
    assert outcome is SetupOutcome.ALREADY_SETUP
    assert success is None
