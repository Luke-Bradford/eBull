"""Tests for app.services.job_telemetry.

In-memory aggregator behaviour + DB flush against a synthetic
``job_runs`` row. PR3 (#1071) extended the aggregator with the A3
operator-amendment progress producer API; tests below cover the new
``set_target`` / ``record_processed`` / ``record_warning`` /
``maybe_flush`` surface.
"""

from __future__ import annotations

from datetime import datetime

import psycopg
import pytest

from app.services.job_telemetry import (
    DEFAULT_FLUSH_INTERVAL_SECONDS,
    JobTelemetryAggregator,
    flush_to_job_run,
)


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


# ---------------------------------------------------------------------------
# Progress producer API (#1071, PR3 — A3 operator amendment)
# ---------------------------------------------------------------------------


def test_record_processed_bumps_counter_and_heartbeat() -> None:
    agg = JobTelemetryAggregator()
    assert agg.processed_count == 0
    assert agg.last_progress_at is None

    agg.record_processed()
    assert agg.processed_count == 1
    assert agg.last_progress_at is not None

    agg.record_processed(count=10)
    assert agg.processed_count == 11


def test_record_processed_zero_or_negative_no_op() -> None:
    agg = JobTelemetryAggregator()
    agg.record_processed(count=0)
    agg.record_processed(count=-5)
    assert agg.processed_count == 0
    assert agg.last_progress_at is None


def test_set_target_pins_denominator() -> None:
    agg = JobTelemetryAggregator()
    assert agg.target_count is None
    agg.set_target(1547)
    assert agg.target_count == 1547


def test_set_target_zero_allowed_negative_raises() -> None:
    agg = JobTelemetryAggregator()
    agg.set_target(0)
    assert agg.target_count == 0
    with pytest.raises(ValueError):
        agg.set_target(-1)


def test_record_warning_groups_by_class() -> None:
    agg = JobTelemetryAggregator()
    agg.record_warning(
        error_class="RateLimited",
        message="429 from sec.gov",
        subject="CIK 320193",
    )
    agg.record_warning(
        error_class="RateLimited",
        message="429 from sec.gov retry succeeded",
        subject="CIK 320194",
    )
    agg.record_warning(
        error_class="PartialParse",
        message="optional field missing",
        subject=None,
    )
    classes = agg.to_warning_classes_jsonb()
    assert set(classes.keys()) == {"RateLimited", "PartialParse"}
    assert classes["RateLimited"]["count"] == 2
    assert "retry succeeded" in classes["RateLimited"]["sample_message"]
    assert agg.warnings_count == 3


def test_warnings_independent_of_errors() -> None:
    """A warning is a recovered failure; it must NOT bump rows_errored."""
    agg = JobTelemetryAggregator()
    agg.record_warning(error_class="X", message="warn", subject=None)
    assert agg.rows_errored == 0
    assert agg.warnings_count == 1


def test_flush_writes_progress_columns(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    run_id = _make_job_run(ebull_test_conn, "test_job_progress")
    ebull_test_conn.commit()

    agg = JobTelemetryAggregator()
    agg.set_target(100)
    for _ in range(7):
        agg.record_processed()
    agg.record_warning(error_class="W", message="warn", subject="s")

    flush_to_job_run(ebull_test_conn, run_id=run_id, agg=agg)
    ebull_test_conn.commit()

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            SELECT processed_count, target_count, last_progress_at,
                   warnings_count, warning_classes
              FROM job_runs WHERE run_id = %s
            """,
            (run_id,),
        )
        row = cur.fetchone()
        assert row is not None
        processed, target, last_progress, warnings, warning_classes = row
        assert processed == 7
        assert target == 100
        assert isinstance(last_progress, datetime)
        # Producer wrote UTC; column is TIMESTAMPTZ so it round-trips aware.
        assert last_progress.tzinfo is not None
        assert warnings == 1
        assert "W" in warning_classes
        assert warning_classes["W"]["count"] == 1


def test_maybe_flush_first_call_always_flushes(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    run_id = _make_job_run(ebull_test_conn, "test_maybe_flush_first")
    ebull_test_conn.commit()

    agg = JobTelemetryAggregator()
    agg.record_processed(count=3)

    did_flush = agg.maybe_flush(ebull_test_conn, run_id=run_id)
    assert did_flush is True
    ebull_test_conn.commit()

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT processed_count FROM job_runs WHERE run_id = %s",
            (run_id,),
        )
        row = cur.fetchone()
        assert row is not None
        assert row[0] == 3


def test_maybe_flush_throttles_inside_interval(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Two maybe_flush calls inside the interval — only the first writes."""
    run_id = _make_job_run(ebull_test_conn, "test_maybe_flush_throttle")
    ebull_test_conn.commit()

    agg = JobTelemetryAggregator()
    agg.record_processed(count=1)
    assert agg.maybe_flush(ebull_test_conn, run_id=run_id) is True
    ebull_test_conn.commit()

    agg.record_processed(count=99)
    # second call inside DEFAULT_FLUSH_INTERVAL_SECONDS → no flush
    assert agg.maybe_flush(ebull_test_conn, run_id=run_id) is False
    ebull_test_conn.commit()

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT processed_count FROM job_runs WHERE run_id = %s",
            (run_id,),
        )
        row = cur.fetchone()
        assert row is not None
        # Still 1 — the second flush was throttled.
        assert row[0] == 1


def test_maybe_flush_short_interval_releases_immediately(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Short flush_interval lets the second tick land without sleeping."""
    run_id = _make_job_run(ebull_test_conn, "test_maybe_flush_short")
    ebull_test_conn.commit()

    agg = JobTelemetryAggregator()
    agg.record_processed(count=1)
    assert agg.maybe_flush(ebull_test_conn, run_id=run_id, flush_interval_seconds=0.0) is True
    ebull_test_conn.commit()

    agg.record_processed(count=2)
    # interval=0 → second call always flushes
    assert agg.maybe_flush(ebull_test_conn, run_id=run_id, flush_interval_seconds=0.0) is True
    ebull_test_conn.commit()

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT processed_count FROM job_runs WHERE run_id = %s",
            (run_id,),
        )
        row = cur.fetchone()
        assert row is not None
        assert row[0] == 3


def test_default_flush_interval_constant_is_present() -> None:
    """The wider codebase imports the constant; pin it so a future
    refactor that renames it surfaces here, not in production."""
    assert isinstance(DEFAULT_FLUSH_INTERVAL_SECONDS, float)
    assert DEFAULT_FLUSH_INTERVAL_SECONDS > 0
