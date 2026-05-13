"""End-to-end bootstrap flow integration test.

Drives every public surface of the first-install bootstrap stack
together against ``ebull_test``:

  1. ``POST /system/bootstrap/run`` writes a ``manual_job`` queue row
     and seeds 24 ``bootstrap_stages``.
  2. The orchestrator (with stubbed invokers) runs Phase A then
     Phase B in parallel and finalises the run.
  3. ``GET /system/bootstrap/status`` returns the final shape.
  4. ``_bootstrap_complete`` returns ``(True, "")`` after success.

Spec: docs/superpowers/specs/2026-05-07-first-install-bootstrap.md.

Stubs every invoker the orchestrator dispatches with a deterministic
in-process fake — no real provider HTTP, no real DB writes beyond
``bootstrap_*`` tables. The test confirms the wiring of API + queue
+ orchestrator + scheduler gate end-to-end.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any

import psycopg
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services.bootstrap_orchestrator import (
    get_bootstrap_stage_specs,
    run_bootstrap_orchestrator,
)
from app.services.bootstrap_state import (
    read_latest_run_with_stages,
    read_state,
)
from app.workers.scheduler import _bootstrap_complete


def _reset_state(conn: psycopg.Connection[tuple]) -> None:
    conn.execute("UPDATE bootstrap_state SET status='pending', last_run_id=NULL, last_completed_at=NULL WHERE id = 1")
    conn.commit()


def _bind_settings_to_test_db(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.config import settings as app_settings
    from tests.fixtures.ebull_test_db import test_database_url

    monkeypatch.setattr(app_settings, "database_url", test_database_url())


def _override_get_conn(conn: psycopg.Connection[tuple]) -> None:
    from app.db import get_conn

    def _dep() -> Iterator[psycopg.Connection[tuple]]:
        yield conn

    app.dependency_overrides[get_conn] = _dep


def _record_job_runs_success(job_name: str, *, row_count: int = 1) -> None:
    """Mirror _tracked_job's job_runs write so the orchestrator's
    rows_processed resolver (#1140 Task C) finds a real row_count.
    Without this every fake-invoker stage would have rows_processed=NULL
    and the strict-gate caps (per-family ownership + fundamentals_raw_seeded)
    would block downstream consumers.
    """
    from app.config import settings as app_settings

    with psycopg.connect(app_settings.database_url) as conn:
        conn.execute(
            "INSERT INTO job_runs (job_name, started_at, finished_at, status, row_count) "
            "VALUES (%s, now(), now(), 'success', %s)",
            (job_name, row_count),
        )
        conn.commit()


def _patch_orchestrator_invokers(
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, list[str]]:
    """Replace every _INVOKERS entry the orchestrator might dispatch
    with a deterministic in-process fake. Returns a calls dict.
    """
    from app.jobs import runtime as runtime_module

    calls: dict[str, list[str]] = {"order": []}

    # PR1b-2 #1064 widened ``JobInvoker`` to ``(Mapping) -> None``;
    # bootstrap dispatch passes ``effective_params`` positionally.
    # Fakes accept-and-ignore so the test stub satisfies the runtime
    # contract.
    def _make_fake(name: str) -> Callable[..., None]:
        def _fake(_params: object = None) -> None:
            calls["order"].append(name)
            _record_job_runs_success(name)

        return _fake

    fake_invokers = {spec.job_name: _make_fake(spec.job_name) for spec in get_bootstrap_stage_specs()}
    monkeypatch.setattr(runtime_module, "_INVOKERS", fake_invokers)
    return calls


def test_bootstrap_end_to_end(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Drive the service-layer integration end-to-end against the
    test DB. The HTTP-layer integration is covered separately in
    ``test_api_bootstrap.py``; this test focuses on the
    service-layer + orchestrator + scheduler-gate wiring.
    """
    from app.services.bootstrap_state import start_run

    _reset_state(ebull_test_conn)
    _bind_settings_to_test_db(monkeypatch)
    calls = _patch_orchestrator_invokers(monkeypatch)

    # 1. _bootstrap_complete returns False before the run.
    met, reason = _bootstrap_complete(ebull_test_conn)
    assert met is False
    assert "first-install bootstrap not complete" in reason

    # 2. start_run seeds 24 stages atomically.
    run_id = start_run(
        ebull_test_conn,
        operator_id=None,
        stage_specs=get_bootstrap_stage_specs(),
    )
    ebull_test_conn.commit()

    snap = read_latest_run_with_stages(ebull_test_conn)
    assert snap is not None
    assert snap.run_id == run_id
    assert len(snap.stages) == 24
    assert all(s.status == "pending" for s in snap.stages)

    # 3. State is running, gate stays closed.
    state = read_state(ebull_test_conn)
    assert state.status == "running"
    met, _ = _bootstrap_complete(ebull_test_conn)
    assert met is False

    # 4. Orchestrator drives Phase A → B → C with the stubbed invokers.
    run_bootstrap_orchestrator()

    # 5. Every fake invoker fired once.
    assert len(calls["order"]) == 24

    # 6. State is complete; gate releases.
    state = read_state(ebull_test_conn)
    assert state.status == "complete"
    met, reason = _bootstrap_complete(ebull_test_conn)
    assert met is True
    assert reason == ""

    # 7. GET /status reflects the terminal shape (HTTP-layer smoke).
    _override_get_conn(ebull_test_conn)
    try:
        with TestClient(app) as client:
            resp = client.get("/system/bootstrap/status")
            assert resp.status_code == 200
            status_body: dict[str, Any] = resp.json()
            assert status_body["status"] == "complete"
            assert status_body["current_run_id"] == run_id
            assert all(stage["status"] == "success" for stage in status_body["stages"])
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


def test_bootstrap_partial_error_then_retry_failed(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mid-lane stage fails; retry-failed re-runs failed + downstream."""
    from app.jobs import runtime as runtime_module
    from app.services.bootstrap_state import (
        reset_failed_stages_for_retry,
        start_run,
    )

    _reset_state(ebull_test_conn)
    _bind_settings_to_test_db(monkeypatch)

    # First pass: one SEC-lane stage fails. Track which invokers fired.
    calls_pass1: dict[str, list[str]] = {"order": []}
    failing = {"sec_def14a_bootstrap"}

    def _make_pass1(name: str) -> Callable[..., None]:
        def _fake(_params: object = None) -> None:
            calls_pass1["order"].append(name)
            if name in failing:
                raise RuntimeError(f"forced {name} failure")
            _record_job_runs_success(name)

        return _fake

    pass1_invokers = {spec.job_name: _make_pass1(spec.job_name) for spec in get_bootstrap_stage_specs()}
    monkeypatch.setattr(runtime_module, "_INVOKERS", pass1_invokers)

    run_id = start_run(
        ebull_test_conn,
        operator_id=None,
        stage_specs=get_bootstrap_stage_specs(),
    )
    ebull_test_conn.commit()

    run_bootstrap_orchestrator()

    state = read_state(ebull_test_conn)
    assert state.status == "partial_error"

    # Second pass: replace invokers with a successful set + reset failed stages.
    calls_pass2: dict[str, list[str]] = {"order": []}

    def _make_pass2(name: str) -> Callable[..., None]:
        def _fake(_params: object = None) -> None:
            calls_pass2["order"].append(name)
            _record_job_runs_success(name)

        return _fake

    pass2_invokers = {spec.job_name: _make_pass2(spec.job_name) for spec in get_bootstrap_stage_specs()}
    monkeypatch.setattr(runtime_module, "_INVOKERS", pass2_invokers)

    helper_run_id, reset_count = reset_failed_stages_for_retry(ebull_test_conn)
    assert helper_run_id == run_id
    assert reset_count > 0
    ebull_test_conn.commit()

    run_bootstrap_orchestrator()

    # Pass 2 should have called only the failed stage + later-numbered SEC-lane peers.
    # The init + eToro stages stay 'success' from pass 1 and are skipped.
    # Cross-lane downstreams (e.g. fundamentals_sync on db lane) ALSO stay
    # 'success' from pass 1 — reset_failed_stages_for_retry only walks the
    # SAME lane as the failed stage, see bootstrap_state.reset_failed_stages_for_retry.
    assert "nightly_universe_sync" not in calls_pass2["order"]
    assert "daily_candle_refresh" not in calls_pass2["order"]
    assert "fundamentals_sync" not in calls_pass2["order"]
    # Failed stage and downstream peers in the SAME (sec_rate) lane re-fired.
    assert "sec_def14a_bootstrap" in calls_pass2["order"]
    assert "sec_business_summary_bootstrap" in calls_pass2["order"]

    state = read_state(ebull_test_conn)
    assert state.status == "complete"
