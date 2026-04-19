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
