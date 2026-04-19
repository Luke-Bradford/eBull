"""Guard: /sync/layers (v1) must not gain or lose fields in this PR.

If a later refactor deliberately retires v1, delete this test in the
same PR that removes the endpoint.
"""

from fastapi.testclient import TestClient

from app.main import app

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


def test_v1_top_level_shape() -> None:
    with TestClient(app) as client:
        resp = client.get("/sync/layers")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert set(body.keys()) == {"layers"}


def test_v1_layer_keys_unchanged() -> None:
    with TestClient(app) as client:
        resp = client.get("/sync/layers")
    layers = resp.json()["layers"]
    assert len(layers) == 15
    for layer in layers:
        assert set(layer.keys()) == EXPECTED_LAYER_KEYS, (
            f"{layer.get('name')} v1 shape drift: {set(layer.keys()) ^ EXPECTED_LAYER_KEYS}"
        )
