"""#1233 PR-D — operator-runbook helpers in ``app/jobs/locks.py``.

Covers :func:`probe_jobs_process_running` and
:func:`acquire_jobs_process_fence`. Both helpers wrap
``pg_try_advisory_lock(JOBS_PROCESS_LOCK_KEY)``; tests assert:

  * probe returns False when no holder + does not leave the lock held;
  * probe returns True when a holder on the SAME DB exists;
  * fence acquires + releases cleanly on the application DB;
  * fence raises ``JobAlreadyRunning`` when contended on the same DB;
  * the PER-DATABASE invariant — a holder on a sibling DB does NOT
    block an acquire on the application DB (regression gate for the
    documented limitation in :func:`acquire_jobs_process_fence`).

PG advisory locks are PER-DATABASE in PG 9.0+ (NOT cluster-wide).
Empirical test against the local PG 17 instance during PR-D bench
forced this design correction — see
:func:`acquire_jobs_process_fence` docstring + spec §17.

All tests use ``settings.database_url`` — advisory locks are
session-scoped and no rows are written.

Same ``xdist_group`` marker as ``tests/test_joblock_per_source.py``
so parallel xdist workers do not race on the singleton fence key.
"""

from __future__ import annotations

from urllib.parse import urlparse, urlunparse

import psycopg
import pytest

from app.config import settings
from app.jobs.locks import (
    JOBS_PROCESS_LOCK_KEY,
    JobAlreadyRunning,
    acquire_jobs_process_fence,
    probe_jobs_process_running,
)

pytestmark = pytest.mark.xdist_group(name="joblock_source_serial")


def _sibling_postgres_url() -> str:
    """Same cluster as ``settings.database_url``, but ``postgres`` DB.

    Used by :func:`test_per_database_isolation_regression_gate` to
    verify the per-database lockspace invariant.
    """
    parsed = urlparse(settings.database_url)
    return urlunparse(parsed._replace(path="/postgres"))


def test_probe_returns_false_when_no_holder() -> None:
    """Happy-path probe: no holder → returns False + releases cleanly."""
    assert probe_jobs_process_running(settings.database_url) is False
    # Lock must NOT remain held — second probe is also False.
    assert probe_jobs_process_running(settings.database_url) is False


def test_probe_returns_true_when_holder_present_on_same_db() -> None:
    """Probe sees a same-DB held fence as ``True``."""
    with psycopg.connect(settings.database_url, autocommit=True) as holder:
        row = holder.execute("SELECT pg_try_advisory_lock(%s)", (JOBS_PROCESS_LOCK_KEY,)).fetchone()
        assert row is not None and bool(row[0]) is True
        try:
            assert probe_jobs_process_running(settings.database_url) is True
        finally:
            holder.execute("SELECT pg_advisory_unlock(%s)", (JOBS_PROCESS_LOCK_KEY,))

    # After release the probe is False again.
    assert probe_jobs_process_running(settings.database_url) is False


def test_fence_acquires_and_releases_on_application_db() -> None:
    """Fence context-manager round-trip on the application DB."""
    with acquire_jobs_process_fence(settings.database_url):
        # Inside the context, probe on the SAME DB sees a holder.
        assert probe_jobs_process_running(settings.database_url) is True
    # Released on exit.
    assert probe_jobs_process_running(settings.database_url) is False


def test_fence_raises_when_already_held_on_same_db() -> None:
    """Second fence acquire on the same DB contends and raises."""
    with acquire_jobs_process_fence(settings.database_url):
        with pytest.raises(JobAlreadyRunning) as exc_info:
            with acquire_jobs_process_fence(settings.database_url):
                pytest.fail("inner fence acquire should have raised")
        assert exc_info.value.job_name == "jobs_process"

    # Released after outer exit.
    assert probe_jobs_process_running(settings.database_url) is False


def test_per_database_isolation_regression_gate() -> None:
    """Advisory locks are PER-DATABASE — regression gate for the
    documented limitation of :func:`acquire_jobs_process_fence`.

    A fence-equivalent acquire on the ``postgres`` admin DB does NOT
    block an acquire on the application DB. This is what forces the
    runbook to keep the jobs process stopped through the DROP/CREATE
    window: at the lock layer alone there is no cluster-wide mutex.

    If a future PG version (or extension) makes advisory locks
    cluster-wide, this test will FAIL — at which point
    :func:`acquire_jobs_process_fence` can be simplified to admit a
    cross-DB fence.
    """
    sibling_url = _sibling_postgres_url()
    # Acquire on the sibling DB.
    with psycopg.connect(sibling_url, autocommit=True) as sibling:
        row = sibling.execute("SELECT pg_try_advisory_lock(%s)", (JOBS_PROCESS_LOCK_KEY,)).fetchone()
        assert row is not None and bool(row[0]) is True
        try:
            # An acquire on the APPLICATION DB succeeds — locks are
            # per-database.
            assert probe_jobs_process_running(settings.database_url) is False
        finally:
            sibling.execute("SELECT pg_advisory_unlock(%s)", (JOBS_PROCESS_LOCK_KEY,))
