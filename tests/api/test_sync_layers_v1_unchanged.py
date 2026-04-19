"""Guard: /sync/layers (v1) must not gain or lose fields in this PR.

If a later refactor deliberately retires v1, delete this test in the
same PR that removes the endpoint.
"""

from fastapi.testclient import TestClient

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
