"""Real-DB integration tests for #1140 Task C — precondition + final-data
row-count gates (spec at
docs/superpowers/specs/2026-05-13-precondition-final-data-gates.md).

Covers:

* ``_resolve_stage_rows`` source ordering (archive sum / __job__ / job_runs).
* ``record_archive_result_if_absent`` DO NOTHING semantics vs. the
  existing ``record_archive_result`` upsert.
* Timeline endpoint surface — `warning` per-stage + `has_warnings`
  per-run derivation.

End-to-end orchestrator coverage for strict-cap blocking / per-family
recovery lives in ``tests/test_bootstrap_orchestrator.py``
(``test_strict_cap_blocks_consumer_on_zero_rows`` /
``test_strict_cap_satisfied_by_one_of_two_providers``); this file
exercises the lower-level helpers + the API derivation in isolation.
"""

from __future__ import annotations

from collections.abc import Iterator

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg.types.json import Jsonb

from app.db import get_conn
from app.main import app
from app.services.bootstrap_orchestrator import (
    _resolve_stage_rows,
    _snapshot_job_runs_max_id,
)
from app.services.bootstrap_preconditions import (
    record_archive_result,
    record_archive_result_if_absent,
)

client = TestClient(app)


@pytest.fixture
def conn_override(
    ebull_test_conn: psycopg.Connection[tuple],
) -> Iterator[None]:
    def _yield_conn() -> Iterator[psycopg.Connection[tuple]]:
        yield ebull_test_conn

    app.dependency_overrides[get_conn] = _yield_conn
    try:
        yield
    finally:
        app.dependency_overrides.pop(get_conn, None)


def _wipe_bootstrap_state(conn: psycopg.Connection[tuple]) -> None:
    conn.execute("DELETE FROM bootstrap_archive_results")
    conn.execute("DELETE FROM bootstrap_stages")
    conn.execute("DELETE FROM bootstrap_runs")
    conn.execute("UPDATE bootstrap_state SET status='pending', last_run_id=NULL, last_completed_at=NULL WHERE id=1")


def _insert_run(conn: psycopg.Connection[tuple], *, run_status: str = "complete") -> int:
    row = conn.execute(
        """
        INSERT INTO bootstrap_runs (status, completed_at)
        VALUES (%s, CASE WHEN %s IN ('complete', 'partial_error', 'cancelled') THEN now() ELSE NULL END)
        RETURNING id
        """,
        (run_status, run_status),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _insert_stage(
    conn: psycopg.Connection[tuple],
    *,
    run_id: int,
    stage_key: str,
    stage_order: int,
    lane: str,
    job_name: str,
    status: str = "success",
    rows_processed: int | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO bootstrap_stages (
            bootstrap_run_id, stage_key, stage_order, lane, job_name,
            status, started_at, completed_at, last_error, rows_processed,
            processed_count, target_count
        ) VALUES (%s, %s, %s, %s, %s, %s, now(), now(), NULL, %s, 0, NULL)
        """,
        (run_id, stage_key, stage_order, lane, job_name, status, rows_processed),
    )


# ---------------------------------------------------------------------------
# _resolve_stage_rows source ordering
# ---------------------------------------------------------------------------


def test_resolver_archive_sum_wins_when_count_positive(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Per-archive rows present → return SUM(rows_written).

    Spec §5.2 test 1.
    """
    _wipe_bootstrap_state(ebull_test_conn)
    run_id = _insert_run(ebull_test_conn, run_status="running")
    _insert_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="sec_companyfacts_ingest",
        stage_order=9,
        lane="db",
        job_name="sec_companyfacts_ingest",
        status="running",
    )
    record_archive_result(
        ebull_test_conn,
        bootstrap_run_id=run_id,
        stage_key="sec_companyfacts_ingest",
        archive_name="__job__",
        rows_written=0,
    )
    record_archive_result(
        ebull_test_conn,
        bootstrap_run_id=run_id,
        stage_key="sec_companyfacts_ingest",
        archive_name="companyfacts.zip",
        rows_written=42,
    )
    ebull_test_conn.commit()

    resolved = _resolve_stage_rows(
        ebull_test_conn,
        bootstrap_run_id=run_id,
        stage_key="sec_companyfacts_ingest",
        job_name="sec_companyfacts_ingest",
        job_runs_id_before=0,
        job_runs_id_after=0,
    )
    assert resolved == 42


def test_resolver_archive_sum_zero_preserved(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Codex R1 BLOCKING §1 regression — archive_count > 0 AND
    SUM = 0 returns 0 (the C-stage ran every archive and produced
    zero rows; that's a real signal, not absence). Pre-revision
    drafts fell through to source 3 which would have returned None.

    Spec §5.2 test 2.
    """
    _wipe_bootstrap_state(ebull_test_conn)
    run_id = _insert_run(ebull_test_conn, run_status="running")
    _insert_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="sec_companyfacts_ingest",
        stage_order=9,
        lane="db",
        job_name="sec_companyfacts_ingest",
        status="running",
    )
    record_archive_result(
        ebull_test_conn,
        bootstrap_run_id=run_id,
        stage_key="sec_companyfacts_ingest",
        archive_name="__job__",
        rows_written=0,
    )
    record_archive_result(
        ebull_test_conn,
        bootstrap_run_id=run_id,
        stage_key="sec_companyfacts_ingest",
        archive_name="companyfacts.zip",
        rows_written=0,
    )
    ebull_test_conn.commit()

    resolved = _resolve_stage_rows(
        ebull_test_conn,
        bootstrap_run_id=run_id,
        stage_key="sec_companyfacts_ingest",
        job_name="sec_companyfacts_ingest",
        job_runs_id_before=0,
        job_runs_id_after=0,
    )
    assert resolved == 0


def test_resolver_uses_job_row_when_set_above_zero(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Codex R1 BLOCKING §2 regression — service-invoker shape: only
    the __job__ row exists, with operator-set rows_written > 0.
    Resolver returns that integer (source 2).

    Mirrors ``sec_submissions_files_walk`` overloading the provenance
    row with ``filings_upserted``.

    Spec §5.2 test 3.
    """
    _wipe_bootstrap_state(ebull_test_conn)
    run_id = _insert_run(ebull_test_conn, run_status="running")
    _insert_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="sec_submissions_files_walk",
        stage_order=13,
        lane="sec_rate",
        job_name="sec_submissions_files_walk",
        status="running",
    )
    record_archive_result(
        ebull_test_conn,
        bootstrap_run_id=run_id,
        stage_key="sec_submissions_files_walk",
        archive_name="__job__",
        rows_written=7,
    )
    ebull_test_conn.commit()

    resolved = _resolve_stage_rows(
        ebull_test_conn,
        bootstrap_run_id=run_id,
        stage_key="sec_submissions_files_walk",
        job_name="sec_submissions_files_walk",
        job_runs_id_before=0,
        job_runs_id_after=0,
    )
    assert resolved == 7


def test_resolver_job_runs_window_excludes_outside_ids(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Codex R2 BLOCKING §1 regression — the (before, after] window
    must exclude job_runs rows outside it.

    Spec §5.2 test 4.
    """
    _wipe_bootstrap_state(ebull_test_conn)
    run_id = _insert_run(ebull_test_conn, run_status="running")
    _insert_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="universe_sync",
        stage_order=1,
        lane="init",
        job_name="nightly_universe_sync",
        status="running",
    )

    # Three job_runs rows for the same job_name; only the middle one
    # is inside (before, after].
    job_name = "nightly_universe_sync"
    # Row 1 — before our window.
    ebull_test_conn.execute(
        "INSERT INTO job_runs (job_name, started_at, finished_at, status, row_count) "
        "VALUES (%s, now(), now(), 'success', 1)",
        (job_name,),
    )
    ebull_test_conn.commit()
    before = _snapshot_job_runs_max_id(ebull_test_conn, job_name=job_name)
    # Row 2 — inside the window.
    ebull_test_conn.execute(
        "INSERT INTO job_runs (job_name, started_at, finished_at, status, row_count) "
        "VALUES (%s, now(), now(), 'success', 2)",
        (job_name,),
    )
    ebull_test_conn.commit()
    after = _snapshot_job_runs_max_id(ebull_test_conn, job_name=job_name)
    # Row 3 — after our window (simulates a parallel scheduled fire
    # that landed after JobLock release).
    ebull_test_conn.execute(
        "INSERT INTO job_runs (job_name, started_at, finished_at, status, row_count) "
        "VALUES (%s, now(), now(), 'success', 3)",
        (job_name,),
    )
    ebull_test_conn.commit()

    resolved = _resolve_stage_rows(
        ebull_test_conn,
        bootstrap_run_id=run_id,
        stage_key="universe_sync",
        job_name=job_name,
        job_runs_id_before=before,
        job_runs_id_after=after,
    )
    assert resolved == 2


def test_resolver_falls_back_to_job_runs_or_none(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Source-3 happy path AND negative — no archive rows and no
    job_runs row in window → ``None``.

    Spec §5.2 test 5.
    """
    _wipe_bootstrap_state(ebull_test_conn)
    run_id = _insert_run(ebull_test_conn, run_status="running")
    _insert_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="universe_sync",
        stage_order=1,
        lane="init",
        job_name="nightly_universe_sync",
        status="running",
    )

    job_name = "nightly_universe_sync"
    before = _snapshot_job_runs_max_id(ebull_test_conn, job_name=job_name)
    ebull_test_conn.execute(
        "INSERT INTO job_runs (job_name, started_at, finished_at, status, row_count) "
        "VALUES (%s, now(), now(), 'success', 1500)",
        (job_name,),
    )
    ebull_test_conn.commit()
    after = _snapshot_job_runs_max_id(ebull_test_conn, job_name=job_name)

    resolved = _resolve_stage_rows(
        ebull_test_conn,
        bootstrap_run_id=run_id,
        stage_key="universe_sync",
        job_name=job_name,
        job_runs_id_before=before,
        job_runs_id_after=after,
    )
    assert resolved == 1500

    # Negative: empty window → None.
    resolved_empty = _resolve_stage_rows(
        ebull_test_conn,
        bootstrap_run_id=run_id,
        stage_key="universe_sync",
        job_name=job_name,
        job_runs_id_before=after,
        job_runs_id_after=after,
    )
    assert resolved_empty is None


# ---------------------------------------------------------------------------
# record_archive_result_if_absent vs record_archive_result
# ---------------------------------------------------------------------------


def test_orchestrator_job_row_preserves_invoker_value(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Codex R2 BLOCKING §2 regression — service invoker writes
    ``__job__`` with a real ``rows_written``; the orchestrator's
    subsequent ``record_archive_result_if_absent`` call (default 0)
    is DO NOTHING so the invoker's value survives.

    Control: the existing upsert helper still flips the value.

    Spec §5.2 test 6.
    """
    _wipe_bootstrap_state(ebull_test_conn)
    run_id = _insert_run(ebull_test_conn, run_status="running")
    _insert_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="sec_submissions_files_walk",
        stage_order=13,
        lane="sec_rate",
        job_name="sec_submissions_files_walk",
        status="running",
    )

    # Service invoker (existing upsert) writes the real count.
    record_archive_result(
        ebull_test_conn,
        bootstrap_run_id=run_id,
        stage_key="sec_submissions_files_walk",
        archive_name="__job__",
        rows_written=99,
    )
    ebull_test_conn.commit()

    # Orchestrator's auto-write (new DO NOTHING helper) with default
    # rows_written=0 — must NOT overwrite.
    record_archive_result_if_absent(
        ebull_test_conn,
        bootstrap_run_id=run_id,
        stage_key="sec_submissions_files_walk",
        archive_name="__job__",
        rows_written=0,
    )
    ebull_test_conn.commit()

    row = ebull_test_conn.execute(
        "SELECT rows_written FROM bootstrap_archive_results "
        "WHERE bootstrap_run_id = %s AND stage_key = %s AND archive_name = '__job__'",
        (run_id, "sec_submissions_files_walk"),
    ).fetchone()
    assert row is not None
    assert int(row[0]) == 99

    # Control: existing upsert helper does flip the value.
    record_archive_result(
        ebull_test_conn,
        bootstrap_run_id=run_id,
        stage_key="sec_submissions_files_walk",
        archive_name="__job__",
        rows_written=0,
    )
    ebull_test_conn.commit()
    row = ebull_test_conn.execute(
        "SELECT rows_written FROM bootstrap_archive_results "
        "WHERE bootstrap_run_id = %s AND stage_key = %s AND archive_name = '__job__'",
        (run_id, "sec_submissions_files_walk"),
    ).fetchone()
    assert row is not None
    assert int(row[0]) == 0


# ---------------------------------------------------------------------------
# Timeline endpoint — warning / has_warnings derivation
# ---------------------------------------------------------------------------


def test_timeline_has_warnings_derived_from_stage_rows(
    conn_override: None,
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """A ``complete`` run with a strict-cap provider stage at
    ``rows_processed=0`` surfaces a per-stage ``warning`` and
    ``has_warnings=True`` on the run payload. Control: a stage with
    ``rows_processed=42`` has ``warning=None``.

    Spec §5.2 test 10.
    """
    _wipe_bootstrap_state(ebull_test_conn)
    run_id = _insert_run(ebull_test_conn, run_status="complete")
    # Strict-cap provider with rows_processed=0 → warning.
    _insert_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="sec_companyfacts_ingest",
        stage_order=9,
        lane="db",
        job_name="sec_companyfacts_ingest",
        status="success",
        rows_processed=0,
    )
    # Control: same shape but rows_processed=42 → no warning.
    _insert_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="sec_insider_ingest_from_dataset",
        stage_order=11,
        lane="db",
        job_name="sec_insider_ingest_from_dataset",
        status="success",
        rows_processed=42,
    )
    # Control: non-strict cap provider with rows_processed=0 → no warning.
    _insert_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="universe_sync",
        stage_order=1,
        lane="init",
        job_name="nightly_universe_sync",
        status="success",
        rows_processed=0,
    )
    ebull_test_conn.commit()

    resp = client.get("/system/processes/bootstrap/timeline")
    assert resp.status_code == 200
    payload = resp.json()

    assert payload["run"]["has_warnings"] is True

    stages_by_key = {s["stage_key"]: s for s in payload["stages"]}
    assert stages_by_key["sec_companyfacts_ingest"]["warning"] is not None
    assert "fundamentals_raw_seeded" in stages_by_key["sec_companyfacts_ingest"]["warning"]
    assert "rows_processed=0" in stages_by_key["sec_companyfacts_ingest"]["warning"]
    assert stages_by_key["sec_insider_ingest_from_dataset"]["warning"] is None
    assert stages_by_key["universe_sync"]["warning"] is None


def test_timeline_no_warnings_when_all_strict_caps_pass(
    conn_override: None,
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Healthy run: every strict-cap provider has rows_processed >= 1
    → ``has_warnings=False`` and no per-stage ``warning``.
    """
    _wipe_bootstrap_state(ebull_test_conn)
    run_id = _insert_run(ebull_test_conn, run_status="complete")
    _insert_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="sec_companyfacts_ingest",
        stage_order=9,
        lane="db",
        job_name="sec_companyfacts_ingest",
        status="success",
        rows_processed=100,
    )
    ebull_test_conn.commit()

    resp = client.get("/system/processes/bootstrap/timeline")
    assert resp.status_code == 200
    payload = resp.json()

    assert payload["run"]["has_warnings"] is False
    stages_by_key = {s["stage_key"]: s for s in payload["stages"]}
    assert stages_by_key["sec_companyfacts_ingest"]["warning"] is None


# Silence the rows_skipped param in _insert_archive variants by not using it.
_ = Jsonb  # keep import for parity with the sibling timeline test file
