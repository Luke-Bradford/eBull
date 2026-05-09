"""Backend tests for ``GET /system/processes/bootstrap/timeline`` (#1080).

Issue #1080 (umbrella #1064) — admin control hub PR7.

Spec: docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md
      §"PR7 — Bootstrap timeline drawer + decommission BootstrapPanel".

DB-backed: the endpoint reads ``bootstrap_runs`` / ``bootstrap_stages`` /
``bootstrap_archive_results`` under one REPEATABLE READ snapshot.
Mocking the cursor would lose the snapshot guarantee.
"""

from __future__ import annotations

from collections.abc import Iterator

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg.types.json import Jsonb

from app.db import get_conn
from app.main import app

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
    """Reset bootstrap tables so each test starts from a known state."""
    conn.execute("DELETE FROM bootstrap_archive_results")
    conn.execute("DELETE FROM bootstrap_stages")
    conn.execute("DELETE FROM bootstrap_runs")
    conn.execute("UPDATE bootstrap_state SET status='pending', last_run_id=NULL, last_completed_at=NULL WHERE id=1")


def _insert_bootstrap_run(
    conn: psycopg.Connection[tuple],
    *,
    run_status: str = "running",
    completed: bool = False,
    cancel_requested: bool = False,
) -> int:
    """Insert one ``bootstrap_runs`` row and return its id."""
    row = conn.execute(
        """
        INSERT INTO bootstrap_runs (status, completed_at, cancel_requested_at)
        VALUES (
            %s,
            CASE WHEN %s THEN now() ELSE NULL END,
            CASE WHEN %s THEN now() ELSE NULL END
        )
        RETURNING id
        """,
        (run_status, completed, cancel_requested),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _insert_bootstrap_stage(
    conn: psycopg.Connection[tuple],
    *,
    run_id: int,
    stage_key: str,
    stage_order: int,
    lane: str,
    job_name: str,
    status: str,
    last_error: str | None = None,
    rows_processed: int | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO bootstrap_stages (
            bootstrap_run_id, stage_key, stage_order, lane, job_name,
            status, started_at, completed_at, last_error, rows_processed,
            processed_count, target_count
        ) VALUES (%s, %s, %s, %s, %s, %s,
                  CASE WHEN %s = 'pending' THEN NULL ELSE now() END,
                  CASE WHEN %s IN ('success', 'error', 'skipped', 'blocked') THEN now() ELSE NULL END,
                  %s, %s, 0, NULL)
        """,
        (
            run_id,
            stage_key,
            stage_order,
            lane,
            job_name,
            status,
            status,
            status,
            last_error,
            rows_processed,
        ),
    )


def _insert_archive(
    conn: psycopg.Connection[tuple],
    *,
    run_id: int,
    stage_key: str,
    archive_name: str,
    rows_written: int,
    rows_skipped: dict[str, int],
) -> None:
    conn.execute(
        """
        INSERT INTO bootstrap_archive_results (
            bootstrap_run_id, stage_key, archive_name, rows_written, rows_skipped
        ) VALUES (%s, %s, %s, %s, %s)
        """,
        (run_id, stage_key, archive_name, rows_written, Jsonb(rows_skipped)),
    )


# ---------------------------------------------------------------------------
# Restricted-endpoint contract
# ---------------------------------------------------------------------------


def test_timeline_404_for_orchestrator_full_sync(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    """Restricted endpoint: only ``bootstrap`` resolves; orchestrator → 404."""
    resp = client.get("/system/processes/orchestrator_full_sync/timeline")
    assert resp.status_code == 404


def test_timeline_404_for_ingest_sweep(conn_override: None, ebull_test_conn: psycopg.Connection[tuple]) -> None:
    resp = client.get("/system/processes/sec_form4_sweep/timeline")
    assert resp.status_code == 404


def test_timeline_404_for_unknown_process_id(conn_override: None, ebull_test_conn: psycopg.Connection[tuple]) -> None:
    resp = client.get("/system/processes/not_a_real_thing/timeline")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Empty-state contract: 200 + {run: null, stages: []}
# ---------------------------------------------------------------------------


def test_timeline_returns_empty_envelope_when_no_run(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    """No ``bootstrap_runs`` row → 200 with null run + empty stages.

    Spec §"PR7 ... empty run → ``{run: null, stages: []}`` (NOT 404)".
    """
    _wipe_bootstrap_state(ebull_test_conn)
    ebull_test_conn.commit()

    resp = client.get("/system/processes/bootstrap/timeline")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["run"] is None
    assert body["stages"] == []


def test_timeline_returns_empty_stages_when_run_exists_but_no_stages_seeded(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    """Codex plan-review #4: race between INSERT bootstrap_runs and stage materialisation."""
    _wipe_bootstrap_state(ebull_test_conn)
    run_id = _insert_bootstrap_run(ebull_test_conn, run_status="running")
    ebull_test_conn.commit()

    resp = client.get("/system/processes/bootstrap/timeline")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["run"] is not None
    assert body["run"]["run_id"] == run_id
    assert body["run"]["status"] == "running"
    assert body["stages"] == []


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_timeline_happy_path_groups_archives_per_stage(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    _wipe_bootstrap_state(ebull_test_conn)
    run_id = _insert_bootstrap_run(ebull_test_conn, run_status="running")
    # Use stage_keys present in the live catalogue so the spec-join branch runs.
    _insert_bootstrap_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="universe_sync",
        stage_order=1,
        lane="init",
        job_name="nightly_universe_sync",
        status="success",
        rows_processed=4520,
    )
    _insert_bootstrap_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="cik_refresh",
        stage_order=6,
        lane="sec_rate",
        job_name="daily_cik_refresh",
        status="running",
    )
    _insert_archive(
        ebull_test_conn,
        run_id=run_id,
        stage_key="universe_sync",
        archive_name="__job__",
        rows_written=0,
        rows_skipped={},
    )
    _insert_archive(
        ebull_test_conn,
        run_id=run_id,
        stage_key="cik_refresh",
        archive_name="cik_index_2026Q2.zip",
        rows_written=420,
        rows_skipped={"unresolved_cik": 12, "unresolved_cusip": 3},
    )
    ebull_test_conn.commit()

    resp = client.get("/system/processes/bootstrap/timeline")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["run"]["run_id"] == run_id
    assert body["run"]["status"] == "running"
    # stages ordered by stage_order ASC.
    stage_keys = [s["stage_key"] for s in body["stages"]]
    assert stage_keys == ["universe_sync", "cik_refresh"]
    universe = body["stages"][0]
    assert universe["lane"] == "init"
    assert universe["job_name"] == "nightly_universe_sync"
    assert universe["status"] == "success"
    assert universe["rows_processed"] == 4520
    assert universe["display_name"] == "Universe Sync"
    cik = body["stages"][1]
    assert cik["lane"] == "sec_rate"
    assert cik["status"] == "running"
    # Archive sublist round-trips per stage; rows_skipped JSONB shape preserved.
    assert len(universe["archives"]) == 1
    assert universe["archives"][0]["archive_name"] == "__job__"
    assert universe["archives"][0]["rows_skipped_by_reason"] == {}
    assert len(cik["archives"]) == 1
    assert cik["archives"][0]["rows_skipped_by_reason"] == {
        "unresolved_cik": 12,
        "unresolved_cusip": 3,
    }


def test_timeline_returns_latest_run_when_multiple_runs_exist(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    """``ORDER BY id DESC LIMIT 1`` — newest run wins."""
    _wipe_bootstrap_state(ebull_test_conn)
    older = _insert_bootstrap_run(ebull_test_conn, run_status="complete", completed=True)
    newer = _insert_bootstrap_run(ebull_test_conn, run_status="running")
    _insert_bootstrap_stage(
        ebull_test_conn,
        run_id=older,
        stage_key="universe_sync",
        stage_order=1,
        lane="init",
        job_name="nightly_universe_sync",
        status="success",
    )
    _insert_bootstrap_stage(
        ebull_test_conn,
        run_id=newer,
        stage_key="candle_refresh",
        stage_order=2,
        lane="etoro",
        job_name="daily_candle_refresh",
        status="running",
    )
    ebull_test_conn.commit()

    resp = client.get("/system/processes/bootstrap/timeline")
    body = resp.json()
    assert body["run"]["run_id"] == newer
    # Older-run stages MUST NOT leak into the response.
    stage_keys = {s["stage_key"] for s in body["stages"]}
    assert stage_keys == {"candle_refresh"}


# ---------------------------------------------------------------------------
# Status-enum drift coverage (Codex plan-review findings #2, #3, #5)
# ---------------------------------------------------------------------------


def test_timeline_cancelled_run_surfaces_cancel_requested_at(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    """Codex plan-review #2: cancelled latest run with cancel_requested_at set."""
    _wipe_bootstrap_state(ebull_test_conn)
    run_id = _insert_bootstrap_run(
        ebull_test_conn,
        run_status="cancelled",
        completed=True,
        cancel_requested=True,
    )
    ebull_test_conn.commit()

    resp = client.get("/system/processes/bootstrap/timeline")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["run"]["run_id"] == run_id
    assert body["run"]["status"] == "cancelled"
    assert body["run"]["cancel_requested_at"] is not None
    assert body["run"]["completed_at"] is not None


def test_timeline_partial_error_run_with_blocked_stage(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    """Codex plan-review #3 + #5: partial_error run + status='blocked' (sql/131)."""
    _wipe_bootstrap_state(ebull_test_conn)
    run_id = _insert_bootstrap_run(
        ebull_test_conn,
        run_status="partial_error",
        completed=True,
    )
    _insert_bootstrap_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="cik_refresh",
        stage_order=6,
        lane="sec_rate",
        job_name="daily_cik_refresh",
        status="error",
        last_error="SECRateLimited: 429",
    )
    _insert_bootstrap_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="sec_form3_ingest",
        stage_order=19,
        lane="sec_rate",
        job_name="sec_form3_ingest",
        status="blocked",
    )
    ebull_test_conn.commit()

    resp = client.get("/system/processes/bootstrap/timeline")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["run"]["status"] == "partial_error"
    statuses = {s["status"] for s in body["stages"]}
    assert statuses == {"error", "blocked"}
    error_stage = next(s for s in body["stages"] if s["status"] == "error")
    assert error_stage["last_error"] == "SECRateLimited: 429"


def test_timeline_legacy_lane_sec_renders_alongside_new_lanes(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    """Codex plan-review #5: legacy lane='sec' (sql/132 backwards-compat) round-trips."""
    _wipe_bootstrap_state(ebull_test_conn)
    run_id = _insert_bootstrap_run(ebull_test_conn, run_status="running")
    _insert_bootstrap_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="universe_sync",
        stage_order=1,
        lane="init",
        job_name="nightly_universe_sync",
        status="success",
    )
    # Legacy 17-stage row stored with lane='sec' before sql/132 lane refinement.
    _insert_bootstrap_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="legacy_archive_stage",
        stage_order=99,
        lane="sec",
        job_name="legacy_archive_stage",
        status="success",
    )
    ebull_test_conn.commit()

    resp = client.get("/system/processes/bootstrap/timeline")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    lanes = {s["lane"] for s in body["stages"]}
    assert {"init", "sec"} <= lanes


def test_timeline_unknown_stage_key_falls_back_to_db_columns(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    """Stage rows whose stage_key is not in the current catalogue must NOT 500.

    DB carries the historical truth — the catalogue is the deployable
    contract. Mirrors PR6's ``LAYERS`` defensive fallback.
    """
    _wipe_bootstrap_state(ebull_test_conn)
    run_id = _insert_bootstrap_run(ebull_test_conn, run_status="running")
    _insert_bootstrap_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="legacy_unknown_stage",
        stage_order=42,
        lane="sec",
        job_name="legacy_unknown_job",
        status="success",
    )
    ebull_test_conn.commit()

    resp = client.get("/system/processes/bootstrap/timeline")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert len(body["stages"]) == 1
    stage = body["stages"][0]
    assert stage["stage_key"] == "legacy_unknown_stage"
    # Unknown → fall back to DB stage_order + DB job_name; humanise the key.
    assert stage["stage_order"] == 42
    assert stage["job_name"] == "legacy_unknown_job"
    assert stage["display_name"] == "Legacy Unknown Stage"
