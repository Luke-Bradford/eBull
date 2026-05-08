"""Tests for app.services.job_telemetry.

In-memory aggregator behaviour + DB flush against a synthetic
``job_runs`` row.
"""

from __future__ import annotations

import psycopg

from app.services.job_telemetry import JobTelemetryAggregator, flush_to_job_run


def test_record_error_groups_by_class() -> None:
    agg = JobTelemetryAggregator()
    agg.record_error(
        error_class="ConnectionTimeout",
        message="timed out connecting to sec.gov",
        subject="CIK 320193 / 0000320193-24-000001",
    )
    agg.record_error(
        error_class="ConnectionTimeout",
        message="timed out reading body from sec.gov",
        subject="CIK 320193 / 0000320193-24-000002",
    )
    agg.record_error(
        error_class="MissingCIK",
        message="instrument has no CIK in external_identifiers",
        subject="instrument 4517",
    )

    classes = agg.to_error_classes_jsonb()
    assert set(classes.keys()) == {"ConnectionTimeout", "MissingCIK"}
    assert classes["ConnectionTimeout"]["count"] == 2
    # Sample is the LATEST recorded message — operator wants the freshest example.
    assert "reading body" in classes["ConnectionTimeout"]["sample_message"]
    assert classes["ConnectionTimeout"]["last_subject"] == "CIK 320193 / 0000320193-24-000002"
    assert classes["MissingCIK"]["count"] == 1
    assert agg.rows_errored == 3


def test_record_skip_aggregates_counts() -> None:
    agg = JobTelemetryAggregator()
    agg.record_skip("unresolved_cusip", count=10)
    agg.record_skip("unresolved_cusip", count=5)
    agg.record_skip("rate_limited")

    skips = agg.to_skips_jsonb()
    assert skips == {"unresolved_cusip": 15, "rate_limited": 1}


def test_record_skip_zero_count_no_op() -> None:
    agg = JobTelemetryAggregator()
    agg.record_skip("unresolved_cusip", count=0)
    agg.record_skip("rate_limited", count=-3)
    assert agg.to_skips_jsonb() == {}


def test_sample_message_truncated_to_cap() -> None:
    agg = JobTelemetryAggregator()
    long_msg = "x" * 1000
    agg.record_error(error_class="LongError", message=long_msg, subject=None)
    classes = agg.to_error_classes_jsonb()
    # Cap is 500 chars; truncated stays within bound.
    assert len(classes["LongError"]["sample_message"]) <= 500


def _make_job_run(conn: psycopg.Connection[tuple], job_name: str) -> int:
    """Insert a synthetic running job_runs row and return its run_id."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO job_runs (job_name, started_at, status)
            VALUES (%s, now(), 'running')
            RETURNING run_id
            """,
            (job_name,),
        )
        row = cur.fetchone()
        assert row is not None
        return int(row[0])


def test_flush_to_job_run_writes_jsonb_columns(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    run_id = _make_job_run(ebull_test_conn, "test_job_telemetry")
    ebull_test_conn.commit()

    agg = JobTelemetryAggregator()
    agg.record_error(error_class="X", message="hi", subject="s1")
    agg.record_skip("reason_a", count=3)

    flush_to_job_run(ebull_test_conn, run_id=run_id, agg=agg)
    ebull_test_conn.commit()

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            SELECT rows_errored, error_classes, rows_skipped_by_reason
              FROM job_runs WHERE run_id = %s
            """,
            (run_id,),
        )
        row = cur.fetchone()
        assert row is not None
        rows_errored, error_classes, rows_skipped = row
        assert rows_errored == 1
        assert "X" in error_classes
        assert error_classes["X"]["count"] == 1
        assert rows_skipped == {"reason_a": 3}


def test_flush_to_job_run_idempotent_overwrite(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Calling flush twice replaces (last-writer-wins) per docstring contract."""
    run_id = _make_job_run(ebull_test_conn, "test_job_telemetry_replace")
    ebull_test_conn.commit()

    first = JobTelemetryAggregator()
    first.record_skip("reason_a")
    flush_to_job_run(ebull_test_conn, run_id=run_id, agg=first)
    ebull_test_conn.commit()

    second = JobTelemetryAggregator()
    second.record_skip("reason_b", count=2)
    flush_to_job_run(ebull_test_conn, run_id=run_id, agg=second)
    ebull_test_conn.commit()

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT rows_skipped_by_reason FROM job_runs WHERE run_id = %s",
            (run_id,),
        )
        row = cur.fetchone()
        assert row is not None
        # Second flush replaced first; reason_a should be gone.
        assert row[0] == {"reason_b": 2}
