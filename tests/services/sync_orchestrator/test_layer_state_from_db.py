import psycopg
import pytest

from app.services.sync_orchestrator.layer_failure_history import all_layer_histories
from app.services.sync_orchestrator.layer_state import (
    compute_layer_states_from_db,
)
from app.services.sync_orchestrator.layer_types import LayerState
from app.services.sync_orchestrator.registry import LAYERS
from tests.fixtures.ebull_test_db import test_database_url as _test_database_url


def _longest_path(layers) -> int:
    memo: dict[str, int] = {}

    def depth(name: str) -> int:
        if name in memo:
            return memo[name]
        deps = layers[name].dependencies
        d = 0 if not deps else 1 + max(depth(dep) for dep in deps)
        memo[name] = d
        return d

    return max((depth(n) for n in layers), default=0)


@pytest.mark.integration
def test_every_registered_layer_gets_a_state() -> None:
    with psycopg.connect(_test_database_url()) as conn:
        states = compute_layer_states_from_db(conn)
    assert set(states.keys()) == set(LAYERS.keys())
    for state in states.values():
        assert isinstance(state, LayerState)


def test_registry_depth_is_within_iteration_cap() -> None:
    # fixed-point iteration caps at 16; depth today is small. Update
    # MAX_STATE_ITERATIONS in layer_state.py if this ever exceeds the cap.
    assert _longest_path(LAYERS) <= 10


@pytest.mark.integration
def test_multi_hop_cascade_propagates_to_end_of_chain() -> None:
    # Seed sync_layer_progress with a failed universe row and confirm
    # every layer transitively downstream is CASCADE_WAITING.
    # universe → candles → scoring → recommendations.

    with psycopg.connect(_test_database_url()) as conn:
        # Clean slate
        conn.execute("DELETE FROM sync_layer_progress")
        conn.execute("DELETE FROM sync_runs")
        conn.execute("DELETE FROM layer_enabled")
        conn.commit()

        # Create a sync_runs row + a failed universe progress row with a
        # non-self-heal category so universe becomes ACTION_NEEDED, not
        # RETRYING.
        conn.execute(
            """
            INSERT INTO sync_runs (scope, trigger, started_at, layers_planned, status)
            VALUES ('full', 'manual', now(), 1, 'failed')
            RETURNING sync_run_id
            """
        )
        _row = conn.execute("SELECT MAX(sync_run_id) FROM sync_runs").fetchone()
        assert _row is not None
        sync_run_id = _row[0]
        conn.execute(
            """
            INSERT INTO sync_layer_progress
                (sync_run_id, layer_name, status, started_at, finished_at, error_category)
            VALUES
                (%s, 'universe', 'failed', now(), now(), 'db_constraint')
            """,
            (sync_run_id,),
        )
        conn.commit()

        states = compute_layer_states_from_db(conn)

    # universe itself is ACTION_NEEDED (db_constraint is self_heal=False).
    assert states["universe"] is LayerState.ACTION_NEEDED
    # candles depends on universe and has no own secrets, so it must cascade.
    assert states["candles"] is LayerState.CASCADE_WAITING, (
        f"candles should cascade from universe, got {states['candles']}"
    )
    # scoring depends on thesis + candles; thesis requires ANTHROPIC_API_KEY
    # so it may surface as SECRET_MISSING. scoring cascades from candles
    # either way.
    assert states["scoring"] is LayerState.CASCADE_WAITING, f"scoring should cascade, got {states['scoring']}"
    assert states["recommendations"] is LayerState.CASCADE_WAITING, (
        f"recommendations should cascade, got {states['recommendations']}"
    )


@pytest.mark.integration
def test_legacy_job_failure_category_surfaces_in_layer_state() -> None:
    # Drive a fake legacy failure into job_runs with an auth_expired
    # category, then confirm the state machine reflects it as
    # ACTION_NEEDED (auth_expired is self_heal=False).
    import psycopg as _psycopg

    from app.services.ops_monitor import record_job_finish, record_job_start
    from app.services.sync_orchestrator.layer_types import FailureCategory

    with _psycopg.connect(_test_database_url()) as conn:
        conn.execute("DELETE FROM sync_layer_progress")
        conn.execute("DELETE FROM sync_runs")
        conn.execute("DELETE FROM layer_enabled")
        conn.execute("DELETE FROM job_runs WHERE job_name = 'nightly_universe_sync'")
        conn.commit()

        run_id = record_job_start(conn, "nightly_universe_sync")
        record_job_finish(
            conn,
            run_id,
            status="failure",
            error_msg="credential rejected",
            error_category=FailureCategory.AUTH_EXPIRED,
        )

        # Seed a failed progress row with the category so the state
        # machine can pick it up via sync_layer_progress (the path
        # all_layer_histories uses). Chunk 5 will persist this row
        # automatically; for now we insert it manually.
        conn.execute(
            """
            INSERT INTO sync_runs (scope, trigger, started_at, layers_planned, status)
            VALUES ('full', 'scheduled', now(), 1, 'failed')
            """
        )
        _row = conn.execute("SELECT MAX(sync_run_id) FROM sync_runs").fetchone()
        assert _row is not None
        sync_run_id = _row[0]
        conn.execute(
            """
            INSERT INTO sync_layer_progress
              (sync_run_id, layer_name, status, started_at, finished_at, error_category)
            VALUES
              (%s, 'universe', 'failed', now(), now(), 'auth_expired')
            """,
            (sync_run_id,),
        )
        conn.commit()

        states = compute_layer_states_from_db(conn)

    assert states["universe"] is LayerState.ACTION_NEEDED, (
        f"auth_expired is not self-healing; expected ACTION_NEEDED, got {states['universe']}"
    )


@pytest.mark.integration
def test_dep_skipped_after_failure_resets_streak() -> None:
    # After a failed run, a later skipped run (with finished_at only —
    # the executor's _record_layer_skipped pattern) must be counted as
    # the latest row so the failure streak resets.
    with psycopg.connect(_test_database_url()) as conn:
        conn.execute("DELETE FROM sync_layer_progress")
        conn.execute("DELETE FROM sync_runs")
        conn.execute("DELETE FROM layer_enabled")
        conn.commit()

        conn.execute(
            """
            INSERT INTO sync_runs (scope, trigger, started_at, layers_planned, status)
            VALUES ('full', 'manual', now() - interval '10 minutes', 1, 'failed')
            """
        )
        _old_row = conn.execute("SELECT MAX(sync_run_id) FROM sync_runs").fetchone()
        assert _old_row is not None
        old_run = _old_row[0]
        conn.execute(
            """
            INSERT INTO sync_layer_progress
              (sync_run_id, layer_name, status, started_at, finished_at, error_category)
            VALUES
              (%s, 'news', 'failed', now() - interval '10 minutes', now() - interval '10 minutes', 'source_down')
            """,
            (old_run,),
        )

        conn.execute(
            """
            INSERT INTO sync_runs (scope, trigger, started_at, layers_planned, status)
            VALUES ('full', 'manual', now(), 1, 'complete')
            """
        )
        _new_row = conn.execute("SELECT MAX(sync_run_id) FROM sync_runs").fetchone()
        assert _new_row is not None
        new_run = _new_row[0]
        # Skipped row with ONLY finished_at set (matches
        # _record_layer_skipped in executor.py).
        conn.execute(
            """
            INSERT INTO sync_layer_progress
              (sync_run_id, layer_name, status, finished_at, skip_reason)
            VALUES
              (%s, 'news', 'skipped', now(), 'prereq_missing: test')
            """,
            (new_run,),
        )
        conn.commit()

        streaks, _categories = all_layer_histories(conn, ["news"])
    assert streaks["news"] == 0, "a later skip should break the failure streak"


@pytest.mark.integration
def test_dep_skipped_row_does_not_anchor_age() -> None:
    # A DEP_SKIPPED row (skip_reason not starting with prereq_missing:)
    # must NOT appear as the freshness anchor — the layer should still
    # look stale (age=inf) until it actually runs.
    with psycopg.connect(_test_database_url()) as conn:
        conn.execute("DELETE FROM sync_layer_progress")
        conn.execute("DELETE FROM sync_runs")
        conn.execute("DELETE FROM layer_enabled")
        conn.commit()

        conn.execute(
            """
            INSERT INTO sync_runs (scope, trigger, started_at, layers_planned, status)
            VALUES ('full', 'manual', now(), 1, 'failed')
            """
        )
        _row = conn.execute("SELECT MAX(sync_run_id) FROM sync_runs").fetchone()
        assert _row is not None
        sync_run_id = _row[0]
        conn.execute(
            """
            INSERT INTO sync_layer_progress
              (sync_run_id, layer_name, status, finished_at, skip_reason)
            VALUES
              (%s, 'candles', 'skipped', now(), 'dep failed: universe')
            """,
            (sync_run_id,),
        )
        conn.commit()

        states = compute_layer_states_from_db(conn)
    # candles has never actually run → age=inf → DEGRADED (no upstream
    # failure in scope for this test).
    assert states["candles"] is LayerState.DEGRADED, (
        f"DEP_SKIPPED must not anchor age; expected DEGRADED, got {states['candles']}"
    )
