"""Tests for source-level JobLock semantics.

PR1a refactors JobLock from per-job-name to per-source. Verify:

* Two jobs sharing a source serialise (second JobLock raises JobAlreadyRunning).
* Two jobs in different sources run concurrently (both succeed).
* Unknown job_name raises KeyError (no silent fallback).
* test_only_per_name escape hatch keys on raw job_name (pre-PR1a behaviour).
"""

from __future__ import annotations

import pytest

from app.config import settings
from app.jobs.locks import JobAlreadyRunning, JobLock

# Postgres advisory locks are cluster-wide, not database-scoped. With pytest-xdist
# running tests in parallel workers against the same dev DB cluster, two tests
# acquiring the same source lock would contend across workers. Group these tests
# onto a single worker so the only contention is intra-test (which is what each
# test asserts).
pytestmark = pytest.mark.xdist_group(name="joblock_source_serial")


class TestJobLockSourceLevel:
    """Source-level lock contention vs cross-source parallelism."""

    def test_same_source_serialises(self) -> None:
        """sec_form3_ingest + sec_def14a_ingest share source=sec_rate.

        Holding the lock for one MUST block the other.
        """
        with JobLock(settings.database_url, "sec_form3_ingest"):
            with pytest.raises(JobAlreadyRunning):
                with JobLock(settings.database_url, "sec_def14a_ingest"):
                    pytest.fail("second sec_rate-source lock should have raised JobAlreadyRunning")

    def test_cross_source_runs_concurrently(self) -> None:
        """orchestrator_full_sync (db) + execute_approved_orders (etoro)
        are different sources and both must acquire successfully."""
        with JobLock(settings.database_url, "orchestrator_full_sync"):
            with JobLock(settings.database_url, "execute_approved_orders"):
                # Both held simultaneously — no exception means success.
                pass

    def test_db_source_serialises(self) -> None:
        """orchestrator_full_sync + retry_deferred_recommendations both source=db."""
        with JobLock(settings.database_url, "orchestrator_full_sync"):
            with pytest.raises(JobAlreadyRunning):
                with JobLock(settings.database_url, "retry_deferred_recommendations"):
                    pytest.fail("second db-source lock should have raised")

    def test_etoro_source_serialises(self) -> None:
        """execute_approved_orders + etoro_lookups_refresh both source=etoro."""
        with JobLock(settings.database_url, "execute_approved_orders"):
            with pytest.raises(JobAlreadyRunning):
                with JobLock(settings.database_url, "etoro_lookups_refresh"):
                    pytest.fail("second etoro-source lock should have raised")

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
