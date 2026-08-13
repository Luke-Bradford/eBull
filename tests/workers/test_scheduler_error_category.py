import psycopg
import pytest

from tests.fixtures.ebull_test_db import test_database_url as _test_database_url


def _run_tracked(job_name: str, raise_exc: Exception | None = None, row_count: int | None = None) -> None:
    """Run ``_tracked_job`` against the per-worker test DB.

    ⚠ The redirect patches the attribute on the SHARED ``app.config.settings``
    object, not a module-local alias. ``_tracked_job`` does not open its own
    connection: every ``job_runs`` write goes through
    ``app.db.background_write.background_write_connection``, whose raw fallback
    reads ``settings.database_url`` from ITS own module namespace. Patching
    ``app.workers.scheduler.settings`` — which is what this helper did until
    #2629 — rebinds a name the write path no longer consults, so it fails
    OPEN: the tests read an empty test DB while the rows land in the
    operator's dev ``ebull``. Patching the object keeps every consumer in step
    however many modules the write path crosses, which is the pattern the six
    ``settings_use_test_db`` fixtures elsewhere in tests/ already use.
    """
    from unittest.mock import patch

    from app.config import settings
    from app.workers.scheduler import _tracked_job

    url = _test_database_url()
    with patch.object(settings, "database_url", url):
        if raise_exc is not None:
            with pytest.raises(type(raise_exc)):
                with _tracked_job(job_name) as _tracker:
                    raise raise_exc
        else:
            with _tracked_job(job_name) as tracker:
                if row_count is not None:
                    tracker.row_count = row_count


@pytest.mark.integration
def test_tracked_job_integrityerror_persists_db_constraint() -> None:
    # Drive a legacy scheduler job whose body raises IntegrityError
    # through _tracked_job and confirm job_runs.error_category ends up
    # as 'db_constraint'.
    unique_err = psycopg.errors.UniqueViolation("dup")
    _run_tracked("test_tracked_dbc", raise_exc=unique_err)

    with psycopg.connect(_test_database_url()) as conn:
        row = conn.execute(
            """
            SELECT status, error_category
            FROM job_runs
            WHERE job_name = 'test_tracked_dbc'
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()
    assert row is not None
    assert row[0] == "failure"
    assert row[1] == "db_constraint"


@pytest.mark.integration
def test_tracked_job_http_401_persists_auth_expired() -> None:
    import httpx

    resp = httpx.Response(401, text="unauthorized")
    err = httpx.HTTPStatusError("unauth", request=httpx.Request("GET", "https://x"), response=resp)
    _run_tracked("test_tracked_auth", raise_exc=err)

    with psycopg.connect(_test_database_url()) as conn:
        row = conn.execute(
            """
            SELECT status, error_category
            FROM job_runs
            WHERE job_name = 'test_tracked_auth'
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()
    assert row is not None
    assert row[0] == "failure"
    assert row[1] == "auth_expired"


@pytest.mark.integration
def test_tracked_job_runtime_error_persists_internal_error() -> None:
    _run_tracked("test_tracked_runtime", raise_exc=RuntimeError("surprise"))

    with psycopg.connect(_test_database_url()) as conn:
        row = conn.execute(
            """
            SELECT status, error_category
            FROM job_runs
            WHERE job_name = 'test_tracked_runtime'
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()
    assert row is not None
    assert row[0] == "failure"
    assert row[1] == "internal_error"


@pytest.mark.integration
def test_tracked_job_success_leaves_error_category_null() -> None:
    _run_tracked("test_tracked_ok", row_count=42)

    with psycopg.connect(_test_database_url()) as conn:
        row = conn.execute(
            """
            SELECT status, error_category
            FROM job_runs
            WHERE job_name = 'test_tracked_ok'
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()
    assert row is not None
    assert row[0] == "success"
    assert row[1] is None
