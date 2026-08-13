import psycopg
import pytest

from app.services.ops_monitor import record_job_finish, record_job_start
from app.services.sync_orchestrator.layer_types import FailureCategory
from tests.fixtures.ebull_test_db import test_database_url as _test_database_url


@pytest.mark.integration
def test_record_job_finish_persists_error_category() -> None:
    with psycopg.connect(_test_database_url()) as conn:
        run_id = record_job_start(conn, "test_job_cat")
        record_job_finish(
            conn,
            run_id,
            status="failure",
            error_msg="simulated",
            error_category=FailureCategory.DB_CONSTRAINT,
        )
        row = conn.execute(
            "SELECT status, error_category FROM job_runs WHERE run_id = %s",
            (run_id,),
        ).fetchone()
        assert row is not None
        assert row[0] == "failure"
        assert row[1] == "db_constraint"


@pytest.mark.integration
def test_record_job_finish_without_category_keeps_null() -> None:
    with psycopg.connect(_test_database_url()) as conn:
        run_id = record_job_start(conn, "test_job_nocat")
        record_job_finish(conn, run_id, status="failure", error_msg="oops")
        row = conn.execute(
            "SELECT error_category FROM job_runs WHERE run_id = %s",
            (run_id,),
        ).fetchone()
        assert row is not None
        assert row[0] is None


@pytest.mark.integration
def test_record_job_finish_success_also_accepts_none() -> None:
    # error_category stays optional on success paths too.
    with psycopg.connect(_test_database_url()) as conn:
        run_id = record_job_start(conn, "test_job_success")
        record_job_finish(conn, run_id, status="success", row_count=42)
        row = conn.execute(
            "SELECT status, error_category FROM job_runs WHERE run_id = %s",
            (run_id,),
        ).fetchone()
        assert row is not None
        assert row[0] == "success"
        assert row[1] is None
