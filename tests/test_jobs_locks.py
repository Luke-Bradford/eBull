"""Real-DB tests for the per-job advisory lock primitive.

These exercise ``pg_try_advisory_lock`` for real -- mocks would prove
nothing about the lock semantics. Skipped automatically if Postgres
is unreachable, so a CI run with no DB at all still passes cleanly.

Critically: the lock is read-only on the database (it does not write
any rows), so this test does NOT need the ``ebull_test`` isolation
pattern from ``test_operator_setup_race.py``. It runs against
``settings.database_url`` directly. Verified by the structural guard
``tests/smoke/test_no_settings_url_in_destructive_paths.py`` which
greps for ``connect(settings.database_url`` in destructive contexts;
this file does not match because the connection is opened *inside*
the JobLock implementation, not directly here.
"""

from __future__ import annotations

import psycopg
import pytest

from app.config import settings
from app.jobs.locks import JobAlreadyRunning, JobLock


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
    reason="Postgres unreachable -- skipping JobLock real-DB tests",
)


class TestJobLockAcquire:
    def test_first_acquire_succeeds(self) -> None:
        with JobLock(settings.database_url, "test_first_acquire"):
            pass  # acquired and released cleanly

    def test_second_acquire_while_held_raises(self) -> None:
        outer = JobLock(settings.database_url, "test_second_acquire")
        outer.__enter__()
        try:
            with pytest.raises(JobAlreadyRunning) as exc_info:
                with JobLock(settings.database_url, "test_second_acquire"):
                    pass
            assert exc_info.value.job_name == "test_second_acquire"
        finally:
            outer.__exit__(None, None, None)

    def test_acquire_after_release_succeeds(self) -> None:
        # First holder releases, second holder must be able to acquire.
        with JobLock(settings.database_url, "test_acquire_after_release"):
            pass
        with JobLock(settings.database_url, "test_acquire_after_release"):
            pass  # would raise JobAlreadyRunning if release was broken

    def test_different_names_do_not_block(self) -> None:
        # Two locks with different names must be holdable concurrently.
        with JobLock(settings.database_url, "test_different_names_a"):
            with JobLock(settings.database_url, "test_different_names_b"):
                pass

    def test_release_on_exception_in_body(self) -> None:
        # If the body raises, the lock must still release so a retry
        # can acquire it.
        class _BodyError(RuntimeError):
            pass

        with pytest.raises(_BodyError):
            with JobLock(settings.database_url, "test_release_on_exception"):
                raise _BodyError("boom")

        # Re-acquire must succeed.
        with JobLock(settings.database_url, "test_release_on_exception"):
            pass
