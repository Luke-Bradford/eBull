"""Tests for source-level JobLock semantics.

PR1a refactors JobLock from per-job-name to per-source. Verify:

* Two jobs sharing a source serialise (second JobLock raises
  ``JobAlreadyRunning``) **across thread / process boundaries**. The
  same-context same-source acquire is intentionally re-entrant post-
  #1184 (see ``tests/test_job_lock_reentrancy.py``); cross-context
  contention is the real serialisation invariant and is asserted
  here via ``threading.Thread`` (new threads start with empty
  ``_HELD_SOURCES``, so the inner acquire goes to the real Postgres
  advisory lock and collides).
* Two jobs in different sources run concurrently (both succeed).
* Unknown job_name raises KeyError (no silent fallback).
* test_only_per_name escape hatch keys on raw job_name (pre-PR1a
  behaviour) — opts out of #1184 re-entrancy.
"""

from __future__ import annotations

import queue
import threading

import pytest

from app.config import settings
from app.jobs.locks import JobAlreadyRunning, JobLock

# Postgres advisory locks are cluster-wide, not database-scoped. With pytest-xdist
# running tests in parallel workers against the same dev DB cluster, two tests
# acquiring the same source lock would contend across workers. Group these tests
# onto a single worker so the only contention is intra-test (which is what each
# test asserts).
pytestmark = pytest.mark.xdist_group(name="joblock_source_serial")


def _assert_cross_thread_serialises(outer_job: str, inner_job: str) -> None:
    """Hold ``outer_job`` on one thread, try ``inner_job`` on another,
    and assert the second acquire raises ``JobAlreadyRunning``.

    Cross-thread is the post-#1184 way to exercise same-source
    contention — same-context same-source is intentionally re-entrant
    and would silently bypass without raising.
    """
    outer_holding = threading.Event()
    inner_done = threading.Event()
    outer_errors: queue.Queue[BaseException] = queue.Queue()
    inner_result: queue.Queue[BaseException | str] = queue.Queue()

    def hold_outer() -> None:
        try:
            with JobLock(settings.database_url, outer_job):
                outer_holding.set()
                if not inner_done.wait(timeout=10.0):
                    raise TimeoutError("inner thread did not complete within 10s")
        except BaseException as exc:  # noqa: BLE001 — propagated to main
            outer_errors.put(exc)

    def try_inner() -> None:
        try:
            if not outer_holding.wait(timeout=10.0):
                raise TimeoutError("outer thread did not acquire within 10s")
            try:
                with JobLock(settings.database_url, inner_job):
                    inner_result.put("acquired unexpectedly")
            except JobAlreadyRunning as exc:
                inner_result.put(exc)
        finally:
            inner_done.set()

    t1 = threading.Thread(target=hold_outer, daemon=True)
    t2 = threading.Thread(target=try_inner, daemon=True)
    t1.start()
    t2.start()
    t1.join(timeout=15.0)
    t2.join(timeout=15.0)
    assert not t1.is_alive() and not t2.is_alive(), "test threads hung"

    if not outer_errors.empty():
        raise outer_errors.get()

    result = inner_result.get_nowait()
    assert isinstance(result, JobAlreadyRunning), f"expected JobAlreadyRunning from inner thread, got {result!r}"


class TestJobLockSourceLevel:
    """Source-level lock contention vs cross-source parallelism."""

    def test_same_source_serialises_cross_thread(self) -> None:
        """sec_form3_ingest + sec_8k_events_ingest share source=sec_rate.

        Cross-thread acquires MUST still serialise via the real
        Postgres advisory lock. Same-context re-entrancy is the
        intentional post-#1184 behaviour, covered by
        ``tests/test_job_lock_reentrancy.py``.
        """
        _assert_cross_thread_serialises("sec_form3_ingest", "sec_8k_events_ingest")

    def test_cross_source_runs_concurrently(self) -> None:
        """orchestrator_full_sync (db) + execute_approved_orders (etoro)
        are different sources and both must acquire successfully."""
        with JobLock(settings.database_url, "orchestrator_full_sync"):
            with JobLock(settings.database_url, "execute_approved_orders"):
                # Both held simultaneously — no exception means success.
                pass

    def test_db_source_serialises_cross_thread(self) -> None:
        """orchestrator_full_sync + retry_deferred_recommendations both source=db."""
        _assert_cross_thread_serialises("orchestrator_full_sync", "retry_deferred_recommendations")

    def test_etoro_source_serialises_cross_thread(self) -> None:
        """execute_approved_orders + etoro_lookups_refresh both source=etoro."""
        _assert_cross_thread_serialises("execute_approved_orders", "etoro_lookups_refresh")

    def test_sec_rate_vs_sec_bulk_download_run_parallel(self) -> None:
        """sec_rate and sec_bulk_download are disjoint rate buckets — no contention."""
        with JobLock(settings.database_url, "sec_form3_ingest"):  # sec_rate
            with JobLock(settings.database_url, "sec_bulk_download"):  # sec_bulk_download
                pass


class TestJobLockUnknownJobName:
    """Unknown job_name MUST raise KeyError, never silently fall back."""

    def test_unknown_raises_keyerror(self) -> None:
        with pytest.raises(KeyError, match="unknown job_name"):
            JobLock(settings.database_url, "completely_made_up_job_name_xyz")


class TestJobLockTestOnlyEscape:
    """test_only_per_name preserves pre-PR1a per-name semantics for fixtures."""

    def test_per_name_serialises_same_name(self) -> None:
        with JobLock.test_only_per_name(settings.database_url, "fake_test_job_a"):
            with pytest.raises(JobAlreadyRunning):
                with JobLock.test_only_per_name(settings.database_url, "fake_test_job_a"):
                    pytest.fail("same-name test_only lock should have raised")

    def test_per_name_different_names_run_parallel(self) -> None:
        with JobLock.test_only_per_name(settings.database_url, "fake_test_job_a"):
            with JobLock.test_only_per_name(settings.database_url, "fake_test_job_b"):
                pass

    def test_per_name_does_not_collide_with_real_source_lock(self) -> None:
        """test_only key is raw job_name; real lock is 'job_source:{source}'.

        These hash to different ints so a test fixture cannot accidentally
        block a real production source lock during pytest.
        """
        # Hold a real source-level lock.
        with JobLock(settings.database_url, "execute_approved_orders"):  # source=etoro → key 'job_source:etoro'
            # A test-only lock keyed on raw 'etoro' string would hash to
            # something different from 'job_source:etoro' — so this MUST succeed.
            with JobLock.test_only_per_name(settings.database_url, "etoro"):
                pass
