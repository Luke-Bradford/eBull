"""Guard: /sync/layers (v1) must not gain or lose fields in this PR.

If a later refactor deliberately retires v1, delete this test in the
same PR that removes the endpoint.
"""

import pytest
from fastapi.testclient import TestClient

# Every test below uses the ``clean_client`` fixture, which spins up a
# real DB-backed FastAPI client. Per the #421 PREVENTION rule, those
# must be marked ``@pytest.mark.integration`` so unit-only CI passes
# can deselect them. Module-level pytestmark covers all current and
# future tests in this file uniformly.
pytestmark = pytest.mark.integration

EXPECTED_LAYER_KEYS = {
    "name",
    "display_name",
    "tier",
    "is_fresh",
    "freshness_detail",
    "last_success_at",
    "last_duration_seconds",
    "last_error_category",
    "consecutive_failures",
    "dependencies",
    "is_blocking",
}


def test_v1_top_level_shape(clean_client: TestClient) -> None:
    resp = clean_client.get("/sync/layers")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert set(body.keys()) == {"layers"}


def test_v1_layer_keys_unchanged(clean_client: TestClient) -> None:
    from app.services.sync_orchestrator.registry import LAYERS

    resp = clean_client.get("/sync/layers")
    layers = resp.json()["layers"]
    # Count matches the registry — a layer added or retired without
    # updating this test will fail loudly rather than silently.
    assert len(layers) == len(LAYERS)
    for layer in layers:
        assert set(layer.keys()) == EXPECTED_LAYER_KEYS, (
            f"{layer.get('name')} v1 shape drift: {set(layer.keys()) ^ EXPECTED_LAYER_KEYS}"
        )
