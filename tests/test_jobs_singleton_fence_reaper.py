"""#1290 — singleton-fence stale-lock reaper.

Verifies :func:`app.jobs.__main__._reap_stale_singleton_fence_holder`:

- Returns ``None`` when no eligible holder exists in the current DB.
- Returns ``None`` when a holder exists but ``application_name`` does
  NOT match the singleton-fence marker (the filter must prevent
  third-party backends from being terminated).
- Terminates the matching holder + returns its PID when all criteria
  hold (application_name match + state='idle' + state_change older
  than the grace window).

Real backends; no mocks of pg_locks / pg_stat_activity. ``grace_seconds=0``
is used to admit just-gone-idle backends so the test does not have to
sleep through the production 5-minute grace.

Each test isolates its own lock key via a per-test random int —
:data:`app.jobs.locks.JOBS_PROCESS_LOCK_KEY` is held by the live
dev jobs daemon at all times in a dev environment, so reusing it
here would falsely reap that daemon's fence connection. The reaper's
behaviour is identical regardless of the bigint value.
"""

from __future__ import annotations

import os
import secrets
import threading
import time

import psycopg
import pytest

from app.jobs.__main__ import (
    SINGLETON_FENCE_APPLICATION_NAME,
    _acquire_singleton_fence,
    _fence_heartbeat_loop,
    _reap_stale_singleton_fence_holder,
)
from app.jobs.locks import JOBS_PROCESS_LOCK_KEY
from tests.fixtures.ebull_test_db import test_database_url
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable"),
]


# Random per-test bigint clear of any real jobs-process key. Generated
# once at module load so all tests in this file probe the same key but
# every pytest run picks a fresh value (cross-run pollution-proof).
_TEST_LOCK_KEY: int = 0x4000_0000_0000_0000 | (secrets.randbits(48))


def _open_holder(*, application_name: str | None) -> psycopg.Connection[tuple]:
    """Open a fresh autocommit connection and acquire the test lock on
    it. Returned connection holds the lock until closed; caller MUST
    close it to release the lock.
    """
    kwargs: dict[str, object] = {"autocommit": True}
    if application_name is not None:
        kwargs["application_name"] = application_name
    conn = psycopg.connect(test_database_url(), **kwargs)  # type: ignore[arg-type]
    row = conn.execute("SELECT pg_try_advisory_lock(%s)", (_TEST_LOCK_KEY,)).fetchone()
    assert row is not None and row[0], "test holder failed to acquire lock"
    return conn


def test_no_holder_returns_none() -> None:
    """No backend holds the test lock → reaper is a no-op."""
    pid = _reap_stale_singleton_fence_holder(
        test_database_url(),
        lock_key=_TEST_LOCK_KEY,
        grace_seconds=0,
    )
    assert pid is None


def test_holder_with_non_matching_app_name_is_not_reaped() -> None:
    """A holder whose application_name does NOT match the singleton-fence
    marker must NOT be terminated. The filter prevents the reaper from
    ever touching a third-party connection that happened to acquire
    this key (vanishingly unlikely for the real JOBS_PROCESS_LOCK_KEY
    bigint, but the safety net belongs at the SQL layer).
    """
    holder = _open_holder(application_name="some-other-tool")
    try:
        pid = _reap_stale_singleton_fence_holder(
            test_database_url(),
            lock_key=_TEST_LOCK_KEY,
            grace_seconds=0,
        )
        assert pid is None
        # Holder is still alive — a query through it succeeds.
        ok_row = holder.execute("SELECT 1").fetchone()
        assert ok_row == (1,)
    finally:
        holder.close()


def test_stale_idle_holder_with_matching_app_name_is_reaped() -> None:
    """A holder with application_name=SINGLETON_FENCE_APPLICATION_NAME
    and ``state='idle'`` past the grace window is terminated. Reaper
    returns the PID it killed.
    """
    holder = _open_holder(application_name=SINGLETON_FENCE_APPLICATION_NAME)
    try:
        holder_pid_row = holder.execute("SELECT pg_backend_pid()").fetchone()
        assert holder_pid_row is not None
        holder_pid = int(holder_pid_row[0])

        reaped = _reap_stale_singleton_fence_holder(
            test_database_url(),
            lock_key=_TEST_LOCK_KEY,
            grace_seconds=0,
        )
        assert reaped == holder_pid

        # Trying to use the holder connection after pg_terminate_backend
        # raises — the backend is gone, and psycopg detects the closed
        # socket on the next attempted statement. (psycopg3 maps the
        # underlying SSL/EOF to OperationalError.)
        with pytest.raises(Exception):  # noqa: BLE001
            holder.execute("SELECT 1").fetchone()
    finally:
        # Belt-and-braces: close the now-dead connection so its socket
        # is released. If close itself raises (already-dead), swallow.
        try:
            holder.close()
        except Exception:
            pass


def test_live_heartbeating_fence_is_not_reaped() -> None:
    """Codex 2 BLOCKING on #1290: a healthy jobs process's fence
    connection is idle most of the time. Without a liveness signal,
    a parallel jobs-process boot would terminate it after the grace
    window — two jobs processes running concurrently.

    The fix is :func:`_fence_heartbeat_loop`: every period_seconds
    it touches the fence connection so ``pg_stat_activity.state_change``
    advances. With ``period < grace`` the live fence never qualifies.

    This test runs the real heartbeat loop on a holder for >grace
    seconds and asserts the reaper finds NO candidate to reap.
    """
    holder = _open_holder(application_name=SINGLETON_FENCE_APPLICATION_NAME)
    holder_lock = threading.Lock()
    stop_event = threading.Event()
    heartbeat = threading.Thread(
        target=_fence_heartbeat_loop,
        args=(holder, holder_lock, stop_event),
        kwargs={"period_seconds": 0.1},
        name="test-fence-heartbeat",
        daemon=True,
    )
    heartbeat.start()
    try:
        # Let the heartbeat run for 2 grace windows so the test
        # would definitely catch any logic that fails to advance
        # state_change. grace_seconds=1 keeps the test under 3s.
        time.sleep(2.5)

        reaped = _reap_stale_singleton_fence_holder(
            test_database_url(),
            lock_key=_TEST_LOCK_KEY,
            grace_seconds=1,
        )
        assert reaped is None, (
            "live heartbeating fence was incorrectly reaped — the "
            "heartbeat is not advancing state_change. Without this "
            "guard, the reaper would terminate the live singleton "
            "fence connection and allow two jobs processes to run."
        )

        # And the holder is still alive (heartbeat keeps it working).
        with holder_lock:
            ok_row = holder.execute("SELECT 1").fetchone()
            assert ok_row == (1,)
    finally:
        stop_event.set()
        heartbeat.join(timeout=2.0)
        try:
            holder.close()
        except Exception:
            pass


def test_dead_fence_after_heartbeat_stops_is_reaped() -> None:
    """Complementary to the live case: when the heartbeat stops (the
    python process died), state_change stops advancing. After
    ``grace_seconds`` elapses, the reaper terminates the orphan
    backend. Together with the live-fence test this proves the
    state_change-based liveness signal works end-to-end.
    """
    holder = _open_holder(application_name=SINGLETON_FENCE_APPLICATION_NAME)
    # Snapshot PID BEFORE the heartbeat starts. Running a query on the
    # holder later (post-stop, pre-reap) would refresh state_change
    # and mask the failure mode this test is designed to catch.
    holder_pid_row = holder.execute("SELECT pg_backend_pid()").fetchone()
    assert holder_pid_row is not None
    holder_pid = int(holder_pid_row[0])

    holder_lock = threading.Lock()
    stop_event = threading.Event()
    heartbeat = threading.Thread(
        target=_fence_heartbeat_loop,
        args=(holder, holder_lock, stop_event),
        kwargs={"period_seconds": 0.1},
        name="test-fence-heartbeat-dies",
        daemon=True,
    )
    heartbeat.start()
    try:
        # Run heartbeat briefly, then kill it (simulating jobs-process
        # death). state_change is fresh at this moment.
        time.sleep(0.5)
        stop_event.set()
        heartbeat.join(timeout=2.0)

        # Confirm reaper does NOT yet reap (state_change too fresh).
        early = _reap_stale_singleton_fence_holder(
            test_database_url(),
            lock_key=_TEST_LOCK_KEY,
            grace_seconds=2,
        )
        assert early is None, "reaper fired before grace window — false positive"

        # Wait past the grace window — heartbeat is dead so
        # state_change cannot advance. NB: do NOT touch the holder
        # connection here; any query would refresh state_change and
        # break the staleness detection.
        time.sleep(2.5)

        reaped = _reap_stale_singleton_fence_holder(
            test_database_url(),
            lock_key=_TEST_LOCK_KEY,
            grace_seconds=2,
        )
        assert reaped == holder_pid
    finally:
        try:
            holder.close()
        except Exception:
            pass


def test_acquire_singleton_fence_pins_autocommit_and_holds_lock() -> None:
    """Codex 2 round 3 LOW on #1290: pin :func:`_acquire_singleton_fence`
    itself to ``autocommit=True`` and prove the session-scope advisory
    lock survives autocommit statement boundaries.

    Asserts:
      1. The returned fence connection reports ``autocommit=True``.
      2. While the fence is open, a sibling connection cannot acquire
         the same advisory key (proves the lock is held session-wide,
         not statement-wide — autocommit does NOT promote the lock to
         transaction-scope).
      3. Once the fence closes, the lock releases and a sibling can
         acquire it.

    Uses the real :data:`JOBS_PROCESS_LOCK_KEY` against the per-worker
    test DB — the dev jobs daemon holds the key in its own dev DB
    only, so this does not collide.
    """
    fence = _acquire_singleton_fence(test_database_url())
    try:
        assert fence.autocommit is True

        # Sibling cannot acquire — proves session-scoped hold survives
        # autocommit statement boundaries.
        sibling = psycopg.connect(test_database_url(), autocommit=True)
        try:
            row = sibling.execute("SELECT pg_try_advisory_lock(%s)", (JOBS_PROCESS_LOCK_KEY,)).fetchone()
            assert row is not None
            assert row[0] is False, (
                "sibling acquired lock while fence is open — autocommit "
                "may have promoted the lock to transaction-scope. "
                "Session-scope is critical: the lock MUST persist for "
                "the lifetime of the fence connection."
            )
        finally:
            sibling.close()
    finally:
        fence.close()

    # After fence close, lock releases — sibling can now acquire.
    sibling2 = psycopg.connect(test_database_url(), autocommit=True)
    try:
        row = sibling2.execute("SELECT pg_try_advisory_lock(%s)", (JOBS_PROCESS_LOCK_KEY,)).fetchone()
        assert row is not None
        assert row[0] is True, "lock not released after fence.close()"
        # Tidy up: release the lock so subsequent tests in this DB
        # are not affected. (Per-worker DB makes this defensive.)
        sibling2.execute("SELECT pg_advisory_unlock(%s)", (JOBS_PROCESS_LOCK_KEY,))
    finally:
        sibling2.close()


def test_reaper_excludes_other_databases() -> None:
    """The reaper's ``database = current_database()`` clause must scope
    the probe to the test DB only — a holder in the same PG cluster
    but a different DB on the exact same advisory key must NOT be
    reaped. Skip if no sibling DB exists to test against.
    """
    # The dev DB ``ebull`` (settings.database_url) is the natural
    # sibling in any developer's local cluster. Skip if test DB IS
    # the dev DB (single-database test runners) or if the dev DB is
    # unreachable.
    dev_db_url = os.environ.get("DATABASE_URL")
    if not dev_db_url or dev_db_url == test_database_url():
        pytest.skip("no separate dev DB available to test cross-DB isolation")

    try:
        sibling = psycopg.connect(
            dev_db_url,
            autocommit=True,
            application_name=SINGLETON_FENCE_APPLICATION_NAME,
            connect_timeout=2,
        )
    except Exception:
        pytest.skip("dev DB unreachable; skipping cross-DB isolation test")

    try:
        row = sibling.execute("SELECT pg_try_advisory_lock(%s)", (_TEST_LOCK_KEY,)).fetchone()
        if not (row and row[0]):
            pytest.skip("sibling DB already holds the test key; skipping")

        # Reaper probes test_database_url() only. The sibling holder
        # in dev DB must be invisible to it.
        reaped = _reap_stale_singleton_fence_holder(
            test_database_url(),
            lock_key=_TEST_LOCK_KEY,
            grace_seconds=0,
        )
        assert reaped is None
        ok_row = sibling.execute("SELECT 1").fetchone()
        assert ok_row == (1,)
    finally:
        try:
            sibling.execute("SELECT pg_advisory_unlock(%s)", (_TEST_LOCK_KEY,))
        except Exception:
            pass
        try:
            sibling.close()
        except Exception:
            pass
