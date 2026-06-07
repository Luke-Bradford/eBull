"""Job-level retry/backoff in ``record_job_finish`` (#1509 / T3 of #1508).

Pure tests pin the backoff curve + the transient classifier (delegated to
``REMEDIES``); DB-backed tests pin that ``record_job_finish`` stamps
``next_retry_at`` + ``attempt`` only for transient, non-exhausted failures.

Spec: ``docs/specs/ops/2026-06-07-job-retry-backoff.md``.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from app.services import ops_monitor
from app.services.ops_monitor import _backoff_seconds, record_job_finish, record_job_start
from app.services.sync_orchestrator.layer_types import FailureCategory

# --- pure: backoff curve + transient classifier -------------------------


def test_backoff_capped_exponential() -> None:
    cat = FailureCategory.INTERNAL_ERROR
    assert _backoff_seconds(1, cat) == 300  # 5m
    assert _backoff_seconds(2, cat) == 900  # 15m
    assert _backoff_seconds(3, cat) == 2700  # 45m
    assert _backoff_seconds(4, cat) == 3600  # 8100 → capped at 1h


def test_backoff_rate_limited_uses_longer_base() -> None:
    """RATE_LIMITED starts at 15m so a retry never lands inside a still-held
    rate window (#1484 caveat)."""
    cat = FailureCategory.RATE_LIMITED
    assert _backoff_seconds(1, cat) == 900  # 15m
    assert _backoff_seconds(2, cat) == 2700  # 45m
    assert _backoff_seconds(3, cat) == 3600  # 8100 → capped


@pytest.mark.parametrize(
    "category,expected",
    [
        (FailureCategory.RATE_LIMITED, True),
        (FailureCategory.SOURCE_DOWN, True),
        (FailureCategory.DATA_GAP, True),
        (FailureCategory.UPSTREAM_WAITING, True),
        (FailureCategory.INTERNAL_ERROR, True),
        (FailureCategory.AUTH_EXPIRED, False),
        (FailureCategory.SCHEMA_DRIFT, False),
        (FailureCategory.DB_CONSTRAINT, False),
        (FailureCategory.MASTER_KEY_MISSING, False),
        (None, False),
    ],
)
def test_is_transient_delegates_to_remedies(category: FailureCategory | None, expected: bool) -> None:
    assert ops_monitor._is_transient(category) is expected


# --- DB-backed: record_job_finish stamps the plan -----------------------


def _retry_state(conn: psycopg.Connection[tuple], run_id: int) -> tuple[str, datetime | None, int]:
    row = conn.execute(
        "SELECT status, next_retry_at, attempt FROM job_runs WHERE run_id = %s",
        (run_id,),
    ).fetchone()
    assert row is not None
    return row[0], row[1], row[2]


def test_transient_failure_schedules_backoff_retry(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    now = datetime(2026, 6, 7, 12, 0, tzinfo=UTC)
    run_id = record_job_start(ebull_test_conn, "retry_jobX")
    record_job_finish(
        ebull_test_conn,
        run_id,
        status="failure",
        error_msg="transient blip",
        error_category=FailureCategory.SOURCE_DOWN,
        now=now,
    )
    status, next_retry_at, attempt = _retry_state(ebull_test_conn, run_id)
    assert status == "failure"
    assert attempt == 1
    assert next_retry_at == now + timedelta(seconds=300)


def test_permanent_failure_never_retries(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """DB_CONSTRAINT (e.g. #1516's NUMERIC overflow) is permanent — no retry
    storm; it surfaces as Needs-attention immediately."""
    run_id = record_job_start(ebull_test_conn, "retry_jobP")
    record_job_finish(
        ebull_test_conn,
        run_id,
        status="failure",
        error_category=FailureCategory.DB_CONSTRAINT,
    )
    status, next_retry_at, attempt = _retry_state(ebull_test_conn, run_id)
    assert status == "failure"
    assert next_retry_at is None
    assert attempt == 1


def test_unknown_category_never_retries(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    run_id = record_job_start(ebull_test_conn, "retry_jobU")
    record_job_finish(ebull_test_conn, run_id, status="failure", error_category=None)
    _, next_retry_at, _ = _retry_state(ebull_test_conn, run_id)
    assert next_retry_at is None


def test_exhausted_attempts_stop_retrying(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    job = "retry_jobE"
    base = datetime(2026, 6, 7, 10, 0, tzinfo=UTC)
    for i in range(4):  # attempts 1-4 already failed
        ebull_test_conn.execute(
            "INSERT INTO job_runs (job_name, started_at, finished_at, status) VALUES (%s, %s, %s, 'failure')",
            (job, base + timedelta(minutes=i), base + timedelta(minutes=i)),
        )
    ebull_test_conn.commit()
    # Start the 5th attempt strictly after the 4 seeds so the streak query
    # (started_at < this.started_at) counts all of them deterministically.
    run_id = record_job_start(ebull_test_conn, job, now=base + timedelta(hours=1))
    record_job_finish(ebull_test_conn, run_id, status="failure", error_category=FailureCategory.SOURCE_DOWN)
    _, next_retry_at, attempt = _retry_state(ebull_test_conn, run_id)
    assert attempt == 5  # streak of 4 + 1
    assert next_retry_at is None  # attempt > _RETRY_MAX_ATTEMPTS


def test_success_clears_retry(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    run_id = record_job_start(ebull_test_conn, "retry_jobS")
    record_job_finish(ebull_test_conn, run_id, status="success", row_count=3)
    status, next_retry_at, attempt = _retry_state(ebull_test_conn, run_id)
    assert status == "success"
    assert next_retry_at is None
    assert attempt == 1


def test_success_breaks_streak_resets_attempt(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """A non-failure terminal breaks the streak: the next failure is attempt 1."""
    job = "retry_jobR"
    base = datetime(2026, 6, 7, 8, 0, tzinfo=UTC)
    for offset, status in ((0, "failure"), (1, "failure"), (2, "success")):
        ebull_test_conn.execute(
            "INSERT INTO job_runs (job_name, started_at, finished_at, status) VALUES (%s, %s, %s, %s)",
            (job, base + timedelta(minutes=offset), base + timedelta(minutes=offset), status),
        )
    ebull_test_conn.commit()
    now = datetime(2026, 6, 7, 8, 10, tzinfo=UTC)
    run_id = record_job_start(ebull_test_conn, job, now=now)  # after the seeds
    record_job_finish(ebull_test_conn, run_id, status="failure", error_category=FailureCategory.SOURCE_DOWN, now=now)
    _, next_retry_at, attempt = _retry_state(ebull_test_conn, run_id)
    assert attempt == 1
    assert next_retry_at == now + timedelta(seconds=300)
