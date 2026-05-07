"""Tests for app.services.bootstrap_state.

Real-DB tests against the worker ``ebull_test`` database. The repo
helpers wrap the singleton ``bootstrap_state`` row plus
``bootstrap_runs`` and ``bootstrap_stages``; mocking psycopg cursors
for these checks would lose the FK / partial-unique guarantees the
tests are exercising.
"""

from __future__ import annotations

import psycopg
import pytest

from app.services.bootstrap_state import (
    BootstrapAlreadyRunning,
    StageSpec,
    finalize_run,
    force_mark_complete,
    mark_stage_error,
    mark_stage_running,
    mark_stage_success,
    read_latest_run_with_stages,
    read_state,
    reap_orphaned_running,
    reset_failed_stages_for_retry,
    start_run,
)


# Used by every test to leave the singleton row in a clean ``pending`` state.
# The truncate fixture wipes ``bootstrap_runs`` (and its FK-cascading
# ``bootstrap_stages``) but does not touch ``bootstrap_state``; tests are
# responsible for resetting the singleton when they leave it non-pending.
def _reset_state(conn: psycopg.Connection[tuple]) -> None:
    conn.execute(
        """
        UPDATE bootstrap_state
           SET status            = 'pending',
               last_run_id       = NULL,
               last_completed_at = NULL
         WHERE id = 1
        """
    )
    conn.commit()


_SPECS = (
    StageSpec(stage_key="universe_sync", stage_order=1, lane="init", job_name="nightly_universe_sync"),
    StageSpec(stage_key="candle_refresh", stage_order=2, lane="etoro", job_name="daily_candle_refresh"),
    StageSpec(stage_key="cusip_universe_backfill", stage_order=3, lane="sec", job_name="cusip_universe_backfill"),
)


def test_read_state_returns_seeded_singleton(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    _reset_state(ebull_test_conn)
    state = read_state(ebull_test_conn)
    assert state.status == "pending"
    assert state.last_run_id is None
    assert state.last_completed_at is None


def test_start_run_creates_run_and_stages(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    _reset_state(ebull_test_conn)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    ebull_test_conn.commit()

    assert run_id > 0

    state = read_state(ebull_test_conn)
    assert state.status == "running"
    assert state.last_run_id == run_id

    snap = read_latest_run_with_stages(ebull_test_conn)
    assert snap is not None
    assert snap.run_id == run_id
    assert snap.run_status == "running"
    assert len(snap.stages) == len(_SPECS)
    assert {s.stage_key for s in snap.stages} == {spec.stage_key for spec in _SPECS}
    for stage in snap.stages:
        assert stage.status == "pending"
        assert stage.attempt_count == 0


def test_start_run_rejects_concurrent_run(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    _reset_state(ebull_test_conn)
    first_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    ebull_test_conn.commit()

    with pytest.raises(BootstrapAlreadyRunning) as exc:
        start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    assert exc.value.run_id == first_id


def test_start_run_after_complete_creates_new_run(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    _reset_state(ebull_test_conn)
    first_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    ebull_test_conn.commit()

    for spec in _SPECS:
        mark_stage_running(ebull_test_conn, run_id=first_id, stage_key=spec.stage_key)
        mark_stage_success(ebull_test_conn, run_id=first_id, stage_key=spec.stage_key, rows_processed=1)
    ebull_test_conn.commit()

    terminal = finalize_run(ebull_test_conn, run_id=first_id)
    assert terminal == "complete"
    assert read_state(ebull_test_conn).status == "complete"

    second_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    ebull_test_conn.commit()
    assert second_id != first_id
    assert read_state(ebull_test_conn).status == "running"


def test_partial_unique_index_blocks_two_running_runs(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    _reset_state(ebull_test_conn)
    start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    ebull_test_conn.commit()

    with pytest.raises(psycopg.errors.UniqueViolation):
        ebull_test_conn.execute("INSERT INTO bootstrap_runs (status) VALUES ('running')")


def test_mark_stage_running_then_error_then_finalize_partial(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _reset_state(ebull_test_conn)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    ebull_test_conn.commit()

    mark_stage_running(ebull_test_conn, run_id=run_id, stage_key="universe_sync")
    mark_stage_success(ebull_test_conn, run_id=run_id, stage_key="universe_sync", rows_processed=1500)
    mark_stage_running(ebull_test_conn, run_id=run_id, stage_key="candle_refresh")
    mark_stage_error(
        ebull_test_conn,
        run_id=run_id,
        stage_key="candle_refresh",
        error_message="eToro 503",
    )
    mark_stage_running(ebull_test_conn, run_id=run_id, stage_key="cusip_universe_backfill")
    mark_stage_success(ebull_test_conn, run_id=run_id, stage_key="cusip_universe_backfill")
    ebull_test_conn.commit()

    terminal = finalize_run(ebull_test_conn, run_id=run_id)
    assert terminal == "partial_error"

    snap = read_latest_run_with_stages(ebull_test_conn)
    assert snap is not None
    by_key = {stage.stage_key: stage for stage in snap.stages}
    assert by_key["universe_sync"].status == "success"
    assert by_key["universe_sync"].rows_processed == 1500
    assert by_key["candle_refresh"].status == "error"
    assert by_key["candle_refresh"].last_error == "eToro 503"
    assert by_key["cusip_universe_backfill"].status == "success"

    state = read_state(ebull_test_conn)
    assert state.status == "partial_error"


def test_reset_failed_stages_resets_failed_and_downstream_in_lane(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _reset_state(ebull_test_conn)
    specs = (
        StageSpec(stage_key="a1", stage_order=1, lane="init", job_name="job_a"),
        StageSpec(stage_key="s1", stage_order=2, lane="sec", job_name="job_s1"),
        StageSpec(stage_key="s2", stage_order=3, lane="sec", job_name="job_s2"),
        StageSpec(stage_key="s3", stage_order=4, lane="sec", job_name="job_s3"),
        StageSpec(stage_key="e1", stage_order=5, lane="etoro", job_name="job_e1"),
    )
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=specs)
    ebull_test_conn.commit()

    for key in ("a1", "s1"):
        mark_stage_running(ebull_test_conn, run_id=run_id, stage_key=key)
        mark_stage_success(ebull_test_conn, run_id=run_id, stage_key=key)
    mark_stage_running(ebull_test_conn, run_id=run_id, stage_key="s2")
    mark_stage_error(ebull_test_conn, run_id=run_id, stage_key="s2", error_message="boom")
    mark_stage_running(ebull_test_conn, run_id=run_id, stage_key="s3")
    mark_stage_success(ebull_test_conn, run_id=run_id, stage_key="s3")
    mark_stage_running(ebull_test_conn, run_id=run_id, stage_key="e1")
    mark_stage_success(ebull_test_conn, run_id=run_id, stage_key="e1")
    ebull_test_conn.commit()

    finalize_run(ebull_test_conn, run_id=run_id)
    assert read_state(ebull_test_conn).status == "partial_error"

    reset_count = reset_failed_stages_for_retry(ebull_test_conn, run_id=run_id)
    # s2 (failed) + s3 (downstream same lane) = 2 stages reset.
    assert reset_count == 2
    ebull_test_conn.commit()

    snap = read_latest_run_with_stages(ebull_test_conn)
    assert snap is not None
    by_key = {stage.stage_key: stage for stage in snap.stages}
    assert by_key["a1"].status == "success"
    assert by_key["s1"].status == "success"
    assert by_key["s2"].status == "pending"
    assert by_key["s2"].last_error is None
    assert by_key["s3"].status == "pending"
    assert by_key["e1"].status == "success"  # other lane, untouched.

    state = read_state(ebull_test_conn)
    assert state.status == "running"


def test_reset_failed_stages_no_op_when_no_failures(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _reset_state(ebull_test_conn)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    ebull_test_conn.commit()

    for spec in _SPECS:
        mark_stage_running(ebull_test_conn, run_id=run_id, stage_key=spec.stage_key)
        mark_stage_success(ebull_test_conn, run_id=run_id, stage_key=spec.stage_key)
    finalize_run(ebull_test_conn, run_id=run_id)
    ebull_test_conn.commit()

    assert reset_failed_stages_for_retry(ebull_test_conn, run_id=run_id) == 0
    assert read_state(ebull_test_conn).status == "complete"


def test_force_mark_complete_flips_state(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    _reset_state(ebull_test_conn)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    ebull_test_conn.commit()
    mark_stage_running(ebull_test_conn, run_id=run_id, stage_key="universe_sync")
    mark_stage_error(
        ebull_test_conn,
        run_id=run_id,
        stage_key="universe_sync",
        error_message="forced fail",
    )
    finalize_run(ebull_test_conn, run_id=run_id)
    ebull_test_conn.commit()
    assert read_state(ebull_test_conn).status == "partial_error"

    force_mark_complete(ebull_test_conn)
    ebull_test_conn.commit()
    state = read_state(ebull_test_conn)
    assert state.status == "complete"
    assert state.last_completed_at is not None


def test_reap_orphaned_running_sweeps_running_and_pending_stages(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _reset_state(ebull_test_conn)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=_SPECS)
    ebull_test_conn.commit()

    mark_stage_running(ebull_test_conn, run_id=run_id, stage_key="universe_sync")
    # candle_refresh stays pending; cusip_universe_backfill stays pending.
    ebull_test_conn.commit()

    swept = reap_orphaned_running(ebull_test_conn)
    assert swept is True
    ebull_test_conn.commit()

    snap = read_latest_run_with_stages(ebull_test_conn)
    assert snap is not None
    by_key = {stage.stage_key: stage for stage in snap.stages}
    assert by_key["universe_sync"].status == "error"
    assert by_key["universe_sync"].last_error == "jobs process restarted mid-run"
    assert by_key["candle_refresh"].status == "error"
    assert by_key["candle_refresh"].last_error == "orchestrator did not dispatch before restart"
    assert by_key["cusip_universe_backfill"].status == "error"

    state = read_state(ebull_test_conn)
    assert state.status == "partial_error"


def test_reap_orphaned_running_no_op_when_not_running(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _reset_state(ebull_test_conn)
    assert reap_orphaned_running(ebull_test_conn) is False


def test_truncate_fixture_wipes_run_history(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """Sanity check that the truncate-cascade chain in the fixture
    wipes ``bootstrap_runs`` and (via FK CASCADE) ``bootstrap_stages``
    between tests, but leaves the singleton ``bootstrap_state`` row
    alone — see the comment block in tests/fixtures/ebull_test_db.py.
    """
    cnt_runs = ebull_test_conn.execute("SELECT COUNT(*) FROM bootstrap_runs").fetchone()
    cnt_stages = ebull_test_conn.execute("SELECT COUNT(*) FROM bootstrap_stages").fetchone()
    cnt_state = ebull_test_conn.execute("SELECT COUNT(*) FROM bootstrap_state").fetchone()
    assert cnt_runs is not None and cnt_runs[0] == 0
    assert cnt_stages is not None and cnt_stages[0] == 0
    assert cnt_state is not None and cnt_state[0] == 1
