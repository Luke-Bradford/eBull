"""Backend tests for P5 live-timeline fields on the bootstrap timeline.

Issue #1409 — bootstrap stage progress UX (rate / ETA / heartbeat).
Plan: docs/superpowers/plans/2026-06-01-bootstrap-etl-redesign.md §"Phase 5".

Covers the fields added to ``BootstrapTimelineStageResponse``:

- ``last_progress_at`` (was dropped from the payload)
- ``rate`` — server-computed rows/sec = processed_count / (last_progress_at − started_at)
- ``eta_seconds`` — (target_count − processed_count) / rate
- ``heartbeat_age_seconds`` — now() − last_progress_at (server clock, no skew)
- ``is_stale`` — heartbeat_age > the bootstrap stale threshold (1800s)

Plus the §5.4 run-selection unification: the endpoint pins on
``bootstrap_state.last_run_id`` rather than ``ORDER BY id DESC``.

DB-backed (mirrors tests/test_bootstrap_timeline_endpoint.py): the
endpoint reads bootstrap tables under one REPEATABLE READ snapshot.
"""

from __future__ import annotations

from collections.abc import Iterator

import psycopg
import pytest
from fastapi.testclient import TestClient

from app.db import get_conn
from app.main import app

client = TestClient(app)


@pytest.fixture
def conn_override(ebull_test_conn: psycopg.Connection[tuple]) -> Iterator[None]:
    def _yield_conn() -> Iterator[psycopg.Connection[tuple]]:
        yield ebull_test_conn

    app.dependency_overrides[get_conn] = _yield_conn
    try:
        yield
    finally:
        app.dependency_overrides.pop(get_conn, None)


def _wipe(conn: psycopg.Connection[tuple]) -> None:
    conn.execute("DELETE FROM bootstrap_archive_results")
    conn.execute("DELETE FROM bootstrap_stages")
    conn.execute("DELETE FROM bootstrap_runs")
    conn.execute("UPDATE bootstrap_state SET status='pending', last_run_id=NULL, last_completed_at=NULL WHERE id=1")


def _insert_run(conn: psycopg.Connection[tuple], *, run_status: str = "running", pin_pointer: bool = True) -> int:
    """Insert a bootstrap_runs row; by default advance bootstrap_state.last_run_id.

    Mirrors production ``start_run`` (app/services/bootstrap_state.py:507),
    which sets ``bootstrap_state.last_run_id`` to the new run id. The §5.4
    endpoint selects the run via that pointer, so tests must set it.
    """
    row = conn.execute(
        "INSERT INTO bootstrap_runs (status) VALUES (%s) RETURNING id",
        (run_status,),
    ).fetchone()
    assert row is not None
    run_id = int(row[0])
    if pin_pointer:
        conn.execute("UPDATE bootstrap_state SET last_run_id=%s WHERE id=1", (run_id,))
    return run_id


def _insert_stage(
    conn: psycopg.Connection[tuple],
    *,
    run_id: int,
    stage_key: str,
    status: str = "running",
    stage_order: int = 10,
    lane: str = "db",
    job_name: str = "sec_13f_recent_sweep",
    started_offset_s: int | None = None,
    last_progress_offset_s: int | None = None,
    processed_count: int = 0,
    target_count: int | None = None,
) -> None:
    """Insert a stage row with explicit started_at / last_progress_at offsets.

    Offsets are seconds-ago relative to ``now()`` at INSERT time. Both
    derive from the same ``now()`` so ``last_progress_at − started_at``
    is exact (deterministic rate), while ``now() − last_progress_at`` at
    the endpoint's snapshot is approximately the offset (assert a band).
    """
    conn.execute(
        """
        INSERT INTO bootstrap_stages (
            bootstrap_run_id, stage_key, stage_order, lane, job_name, status,
            started_at, last_progress_at, processed_count, target_count
        ) VALUES (
            %(run_id)s, %(stage_key)s, %(stage_order)s, %(lane)s, %(job_name)s, %(status)s,
            CASE WHEN %(started_offset)s::double precision IS NULL THEN NULL
                 ELSE now() - make_interval(secs => %(started_offset)s::double precision) END,
            CASE WHEN %(lp_offset)s::double precision IS NULL THEN NULL
                 ELSE now() - make_interval(secs => %(lp_offset)s::double precision) END,
            %(processed)s, %(target)s
        )
        """,
        {
            "run_id": run_id,
            "stage_key": stage_key,
            "stage_order": stage_order,
            "lane": lane,
            "job_name": job_name,
            "status": status,
            "started_offset": started_offset_s,
            "lp_offset": last_progress_offset_s,
            "processed": processed_count,
            "target": target_count,
        },
    )


def _get(stage_key: str) -> dict:
    resp = client.get("/system/processes/bootstrap/timeline")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    stage = next(s for s in body["stages"] if s["stage_key"] == stage_key)
    return stage


# ---------------------------------------------------------------------------
# 5.1 — rate / eta / last_progress_at
# ---------------------------------------------------------------------------


def test_running_stage_with_target_computes_rate_and_eta(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    """processed=600 over a 60s window → 10 rows/s; eta = (9119−600)/10."""
    _wipe(ebull_test_conn)
    run_id = _insert_run(ebull_test_conn)
    _insert_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="sec_13f_recent_sweep",
        started_offset_s=100,
        last_progress_offset_s=40,
        processed_count=600,
        target_count=9119,
    )
    ebull_test_conn.commit()

    stage = _get("sec_13f_recent_sweep")
    assert stage["last_progress_at"] is not None
    assert stage["rate"] == pytest.approx(10.0, rel=1e-3)
    assert stage["eta_seconds"] == pytest.approx((9119 - 600) / 10.0, rel=1e-3)
    assert 38.0 < stage["heartbeat_age_seconds"] < 80.0
    assert stage["is_stale"] is False


def test_running_stage_without_target_has_rate_but_null_eta(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    """No target_count → rate-only; eta NULL (no fake 100%)."""
    _wipe(ebull_test_conn)
    run_id = _insert_run(ebull_test_conn)
    _insert_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="sec_13f_recent_sweep",
        started_offset_s=120,
        last_progress_offset_s=60,
        processed_count=300,
        target_count=None,
    )
    ebull_test_conn.commit()

    stage = _get("sec_13f_recent_sweep")
    assert stage["rate"] == pytest.approx(5.0, rel=1e-3)
    assert stage["eta_seconds"] is None


def test_zero_processed_has_null_rate_and_eta(conn_override: None, ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """processed=0 → no division, rate + eta both NULL (not 0, not div-by-zero)."""
    _wipe(ebull_test_conn)
    run_id = _insert_run(ebull_test_conn)
    _insert_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="sec_13f_recent_sweep",
        started_offset_s=30,
        last_progress_offset_s=30,
        processed_count=0,
        target_count=9119,
    )
    ebull_test_conn.commit()

    stage = _get("sec_13f_recent_sweep")
    assert stage["rate"] is None
    assert stage["eta_seconds"] is None


def test_processed_at_or_above_target_has_null_eta(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    """processed >= target → remaining 0; eta NULL (no negative ETA)."""
    _wipe(ebull_test_conn)
    run_id = _insert_run(ebull_test_conn)
    _insert_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="sec_13f_recent_sweep",
        started_offset_s=100,
        last_progress_offset_s=10,
        processed_count=9200,
        target_count=9119,
    )
    ebull_test_conn.commit()

    stage = _get("sec_13f_recent_sweep")
    assert stage["rate"] is not None
    assert stage["eta_seconds"] is None


def test_processed_without_heartbeat_has_null_rate(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    """Codex ckpt-2: a row with processed_count>0 but no last_progress_at
    (legacy / partially-instrumented) must NOT synthesise a rate from
    now()−started_at. The rate window is last_progress_at−started_at; no
    heartbeat → not measurable → rate + eta NULL.
    """
    _wipe(ebull_test_conn)
    run_id = _insert_run(ebull_test_conn)
    _insert_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="sec_13f_recent_sweep",
        started_offset_s=100,
        last_progress_offset_s=None,
        processed_count=500,
        target_count=9119,
    )
    ebull_test_conn.commit()

    stage = _get("sec_13f_recent_sweep")
    assert stage["rate"] is None
    assert stage["eta_seconds"] is None
    assert stage["heartbeat_age_seconds"] is None


# ---------------------------------------------------------------------------
# 5.3 — heartbeat age + stale flag
# ---------------------------------------------------------------------------


def test_running_stage_past_threshold_is_stale(conn_override: None, ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """last_progress 2000s ago > 1800s bootstrap threshold → is_stale True."""
    _wipe(ebull_test_conn)
    run_id = _insert_run(ebull_test_conn)
    _insert_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="sec_13f_recent_sweep",
        started_offset_s=2100,
        last_progress_offset_s=2000,
        processed_count=500,
        target_count=9119,
    )
    ebull_test_conn.commit()

    stage = _get("sec_13f_recent_sweep")
    assert stage["heartbeat_age_seconds"] > 1900.0
    assert stage["is_stale"] is True


def test_non_running_stage_never_stale(conn_override: None, ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """A succeeded stage with an old heartbeat is not flagged stale."""
    _wipe(ebull_test_conn)
    run_id = _insert_run(ebull_test_conn)
    _insert_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="sec_13f_recent_sweep",
        status="success",
        started_offset_s=5000,
        last_progress_offset_s=4000,
        processed_count=9119,
        target_count=9119,
    )
    ebull_test_conn.commit()

    stage = _get("sec_13f_recent_sweep")
    assert stage["is_stale"] is False


def test_stage_without_heartbeat_has_null_age(conn_override: None, ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """Pending stage (no last_progress_at) → null age, not stale."""
    _wipe(ebull_test_conn)
    run_id = _insert_run(ebull_test_conn)
    _insert_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="sec_13f_recent_sweep",
        status="pending",
        started_offset_s=None,
        last_progress_offset_s=None,
        processed_count=0,
        target_count=None,
    )
    ebull_test_conn.commit()

    stage = _get("sec_13f_recent_sweep")
    assert stage["heartbeat_age_seconds"] is None
    assert stage["is_stale"] is False
    assert stage["rate"] is None


# ---------------------------------------------------------------------------
# 5.4 — run selection pins on bootstrap_state.last_run_id
# ---------------------------------------------------------------------------


def test_timeline_pins_on_last_run_id_not_max_id(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    """last_run_id points at the OLDER run → endpoint returns it, not MAX(id)."""
    _wipe(ebull_test_conn)
    # Only one run may be 'running' at a time (bootstrap_runs_one_running_idx);
    # the older run is terminal, the newer is running. The pointer lagging on
    # the older run is the case §5.4 disambiguates.
    older = _insert_run(ebull_test_conn, run_status="partial_error", pin_pointer=False)
    newer = _insert_run(ebull_test_conn, run_status="running", pin_pointer=False)
    # Operator/audit pointer deliberately lags on the older run.
    ebull_test_conn.execute("UPDATE bootstrap_state SET last_run_id=%s WHERE id=1", (older,))
    _insert_stage(
        ebull_test_conn,
        run_id=older,
        stage_key="universe_sync",
        stage_order=1,
        lane="init",
        job_name="nightly_universe_sync",
        status="success",
    )
    _insert_stage(
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
    assert body["run"]["run_id"] == older
    assert {s["stage_key"] for s in body["stages"]} == {"universe_sync"}


def test_timeline_empty_when_pointer_null(conn_override: None, ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """last_run_id NULL → {run: null, stages: []} even if run rows exist."""
    _wipe(ebull_test_conn)
    _insert_run(ebull_test_conn, run_status="running", pin_pointer=False)
    ebull_test_conn.commit()

    resp = client.get("/system/processes/bootstrap/timeline")
    body = resp.json()
    assert body["run"] is None
    assert body["stages"] == []
