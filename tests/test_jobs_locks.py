"""Real-DB tests for the per-job advisory lock primitive.

These exercise ``pg_try_advisory_lock`` for real -- mocks would prove
nothing about the lock semantics. Skipped automatically if Postgres
is unreachable, so a CI run with no DB at all still passes cleanly.

Per #893, ``JobLock`` is constructed against the per-worker test DB
URL rather than the operator's dev DB. Advisory locks are scoped to
the connection's database, so two workers exercising the same lock
name in their own private DBs cannot interfere with each other.
"""

from __future__ import annotations

import pytest

from app.jobs.locks import JobAlreadyRunning, JobLock
from tests.fixtures.ebull_test_db import test_database_url, test_db_available

pytestmark = pytest.mark.skipif(
    not test_db_available(),
    reason="ebull_test DB unavailable -- skipping JobLock real-DB tests",
)


class TestJobLockAcquire:
    def test_first_acquire_succeeds(self) -> None:
        with JobLock(test_database_url(), "test_first_acquire"):
            pass  # acquired and released cleanly

    def test_second_acquire_while_held_raises(self) -> None:
        outer = JobLock(test_database_url(), "test_second_acquire")
        outer.__enter__()
        try:
            with pytest.raises(JobAlreadyRunning) as exc_info:
                with JobLock(test_database_url(), "test_second_acquire"):
                    pass
            assert exc_info.value.job_name == "test_second_acquire"
        finally:
            outer.__exit__(None, None, None)

    def test_acquire_after_release_succeeds(self) -> None:
        # First holder releases, second holder must be able to acquire.
        with JobLock(test_database_url(), "test_acquire_after_release"):
            pass
        with JobLock(test_database_url(), "test_acquire_after_release"):
            pass  # would raise JobAlreadyRunning if release was broken

    def test_different_names_do_not_block(self) -> None:
        # Two locks with different names must be holdable concurrently.
        with JobLock(test_database_url(), "test_different_names_a"):
            with JobLock(test_database_url(), "test_different_names_b"):
                pass

    def test_release_on_exception_in_body(self) -> None:
        # If the body raises, the lock must still release so a retry
        # can acquire it.
        class _BodyError(RuntimeError):
            pass

        with pytest.raises(_BodyError):
            with JobLock(test_database_url(), "test_release_on_exception"):
                raise _BodyError("boom")

        # Re-acquire must succeed.
        with JobLock(test_database_url(), "test_release_on_exception"):
            pass
