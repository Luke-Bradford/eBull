import psycopg
import pytest

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
    # Seed sync_layer_progress with a failed cik_mapping row and
    # confirm every layer transitively downstream in the registry is
    # CASCADE_WAITING. cik_mapping → financial_facts →
    # financial_normalization → thesis → scoring → recommendations.

    with psycopg.connect(_test_database_url()) as conn:
        # Clean slate
        conn.execute("DELETE FROM sync_layer_progress")
        conn.execute("DELETE FROM sync_runs")
        conn.execute("DELETE FROM layer_enabled")
        conn.commit()

        # Create a sync_runs row + a failed cik_mapping progress row
        # with a non-self-heal category so cik_mapping becomes
        # ACTION_NEEDED, not RETRYING.
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
                (%s, 'cik_mapping', 'failed', now(), now(), 'db_constraint')
            """,
            (sync_run_id,),
        )
        conn.commit()

        states = compute_layer_states_from_db(conn)

    # cik_mapping itself is ACTION_NEEDED (db_constraint is self_heal=False).
    assert states["cik_mapping"] is LayerState.ACTION_NEEDED
    # Layers with no own secrets that are downstream of cik_mapping must
    # be CASCADE_WAITING.  financial_normalization has no secrets and its
    # only path to cik_mapping is via financial_facts.
    assert states["financial_facts"] is LayerState.CASCADE_WAITING, (
        f"financial_facts should cascade from cik_mapping, got {states['financial_facts']}"
    )
    assert states["financial_normalization"] is LayerState.CASCADE_WAITING, (
        f"financial_normalization should cascade from cik_mapping, got {states['financial_normalization']}"
    )
    # thesis requires ANTHROPIC_API_KEY. When absent it surfaces as
    # SECRET_MISSING (rule 3 beats rule 7), which is also a blocking
    # upstream state that propagates the cascade onward.  Accept either.
    assert states["thesis"] in {LayerState.CASCADE_WAITING, LayerState.SECRET_MISSING}, (
        f"thesis should be CASCADE_WAITING or SECRET_MISSING, got {states['thesis']}"
    )
    # scoring/recommendations sit downstream of thesis; thesis is either
    # CASCADE_WAITING or SECRET_MISSING — both propagate rule 7, so these
    # must be CASCADE_WAITING.
    assert states["scoring"] is LayerState.CASCADE_WAITING, f"scoring should cascade, got {states['scoring']}"
    assert states["recommendations"] is LayerState.CASCADE_WAITING, (
        f"recommendations should cascade, got {states['recommendations']}"
    )
