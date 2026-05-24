"""#1233 PR-D — unit tests for ``app/runbooks/safety.py``.

Covers all four primitives:

* ``assert_dev_env`` — env-var must be explicitly ``'dev'``.
* ``assert_dev_db`` — ``current_database()`` against allowlist.
* ``assert_jobs_process_stopped`` — wraps ``probe_jobs_process_running``.
* ``wait_for_jobs_process_started`` — inverse probe with timeout.

``assert_dev_env`` is pure-stdlib; tests use ``monkeypatch``. The
``assert_dev_db`` test uses a real conn against settings.database_url.
The jobs-process gates re-use the same ``JOBS_PROCESS_LOCK_KEY``
mechanics tested in ``test_jobs_process_probe_fence.py``; here we
focus on the runbook-shaped exception (``RunbookRefused`` raising
``SystemExit`` with code 2 + ``msg`` attribute).
"""

from __future__ import annotations

import time

import psycopg
import pytest

from app.config import settings
from app.jobs.locks import JOBS_PROCESS_LOCK_KEY
from app.runbooks.safety import (
    RunbookRefused,
    assert_dev_db,
    assert_dev_env,
    assert_jobs_process_stopped,
    wait_for_jobs_process_started,
)

pytestmark = pytest.mark.xdist_group(name="joblock_source_serial")


# ---------------------------------------------------------------------------
# assert_dev_env
# ---------------------------------------------------------------------------


def test_assert_dev_env_passes_when_dev(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EBULL_ENV", "dev")
    assert_dev_env()  # no raise


def test_assert_dev_env_refuses_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EBULL_ENV", raising=False)
    with pytest.raises(RunbookRefused) as exc:
        assert_dev_env()
    assert exc.value.code == 2
    assert "EBULL_ENV must be explicitly" in exc.value.msg


def test_assert_dev_env_refuses_when_prod(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EBULL_ENV", "prod")
    with pytest.raises(RunbookRefused):
        assert_dev_env()


def test_assert_dev_env_refuses_when_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EBULL_ENV", "")
    with pytest.raises(RunbookRefused):
        assert_dev_env()


# ---------------------------------------------------------------------------
# assert_dev_db
# ---------------------------------------------------------------------------


def test_assert_dev_db_passes_when_db_name_matches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Allowlist hit (the actual current DB name)."""
    with psycopg.connect(settings.database_url) as conn:
        row = conn.execute("SELECT current_database()").fetchone()
        assert row is not None
        actual = row[0]
    monkeypatch.setenv("EBULL_DEV_DB_NAMES", actual)
    with psycopg.connect(settings.database_url) as conn:
        assert_dev_db(conn)  # no raise


def test_assert_dev_db_refuses_when_not_in_allowlist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EBULL_DEV_DB_NAMES", "definitely_not_this_db")
    with psycopg.connect(settings.database_url) as conn:
        with pytest.raises(RunbookRefused) as exc:
            assert_dev_db(conn)
    assert exc.value.code == 2
    assert "not in dev allowlist" in exc.value.msg


def test_assert_dev_db_strips_whitespace_in_allowlist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """O12 fold — `"a, b"` must tokenise to `{"a", "b"}` not `{"a", " b"}`."""
    with psycopg.connect(settings.database_url) as conn:
        row = conn.execute("SELECT current_database()").fetchone()
        assert row is not None
        actual = row[0]
    monkeypatch.setenv("EBULL_DEV_DB_NAMES", f"other_name ,  {actual} , also_other")
    with psycopg.connect(settings.database_url) as conn:
        assert_dev_db(conn)  # whitespace stripped → match


# ---------------------------------------------------------------------------
# assert_jobs_process_stopped
# ---------------------------------------------------------------------------


def test_assert_jobs_process_stopped_passes_when_lock_free() -> None:
    assert_jobs_process_stopped(settings.database_url)  # no raise


def test_assert_jobs_process_stopped_refuses_when_lock_held() -> None:
    with psycopg.connect(settings.database_url, autocommit=True) as holder:
        row = holder.execute("SELECT pg_try_advisory_lock(%s)", (JOBS_PROCESS_LOCK_KEY,)).fetchone()
        assert row is not None and bool(row[0]) is True
        try:
            with pytest.raises(RunbookRefused) as exc:
                assert_jobs_process_stopped(settings.database_url)
            assert exc.value.code == 2
            assert "JOBS_PROCESS_LOCK_KEY held" in exc.value.msg
        finally:
            holder.execute("SELECT pg_advisory_unlock(%s)", (JOBS_PROCESS_LOCK_KEY,))


# ---------------------------------------------------------------------------
# wait_for_jobs_process_started
# ---------------------------------------------------------------------------


def test_wait_for_jobs_process_started_returns_immediately_when_already_held() -> None:
    """Happy path: lock already held → return immediately, no sleep."""
    with psycopg.connect(settings.database_url, autocommit=True) as holder:
        holder.execute("SELECT pg_try_advisory_lock(%s)", (JOBS_PROCESS_LOCK_KEY,))
        try:
            start = time.monotonic()
            wait_for_jobs_process_started(settings.database_url, timeout_sec=60, poll_sec=10)
            elapsed = time.monotonic() - start
            # Returns within the first poll without sleeping.
            assert elapsed < 2.0
        finally:
            holder.execute("SELECT pg_advisory_unlock(%s)", (JOBS_PROCESS_LOCK_KEY,))


def test_wait_for_jobs_process_started_times_out_when_lock_never_held() -> None:
    """Tight timeout (2s) + 1s poll → 2 polls then RunbookRefused."""
    with pytest.raises(RunbookRefused) as exc:
        wait_for_jobs_process_started(settings.database_url, timeout_sec=2, poll_sec=1)
    assert exc.value.code == 2
    assert "did not start within" in exc.value.msg
