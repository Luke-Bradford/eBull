from unittest.mock import patch

from fastapi.testclient import TestClient


def _make_empty_plan():
    from app.services.sync_orchestrator.types import ExecutionPlan

    return ExecutionPlan(layers_to_refresh=(), layers_skipped=(), estimated_duration=None)


def test_post_sync_behind_accepted(clean_client: TestClient) -> None:
    # Happy path: behind scope accepted, 202 returned. submit_sync gets
    # called with a SyncScope(kind='behind').
    from app.services.sync_orchestrator import SyncScope as _Scope

    with patch("app.api.sync.submit_sync") as submit:
        submit.return_value = (42, _make_empty_plan())
        resp = clean_client.post("/sync", json={"scope": "behind"})
    assert resp.status_code == 202, resp.text
    scope_arg: _Scope = submit.call_args.args[0]
    assert scope_arg.kind == "behind"


def test_post_sync_behind_with_all_healthy_returns_empty_plan(clean_client: TestClient) -> None:
    # scope=behind when every layer is HEALTHY → empty layers_to_refresh.
    from app.services.sync_orchestrator.layer_types import LayerState
    from app.services.sync_orchestrator.registry import LAYERS

    all_healthy = {n: LayerState.HEALTHY for n in LAYERS}
    with patch(
        "app.services.sync_orchestrator.planner.compute_layer_states_from_db",
        return_value=all_healthy,
    ):
        resp = clean_client.post("/sync", json={"scope": "behind"})
    assert resp.status_code == 202, resp.text
    assert resp.json()["plan"]["layers_to_refresh"] == []


def test_post_sync_behind_includes_non_healthy_upstream(clean_client: TestClient) -> None:
    # thesis is ACTION_NEEDED, all its upstreams (fundamentals,
    # financial_normalization, news) are healthy so only thesis fires.
    # Verify the candidate job set contains daily_thesis_refresh.
    from app.services.sync_orchestrator.layer_types import LayerState
    from app.services.sync_orchestrator.registry import LAYERS

    states = {n: LayerState.HEALTHY for n in LAYERS}
    states["thesis"] = LayerState.ACTION_NEEDED
    # Keep thesis's upstreams healthy so only thesis itself fires.
    with patch(
        "app.services.sync_orchestrator.planner.compute_layer_states_from_db",
        return_value=states,
    ):
        resp = clean_client.post("/sync", json={"scope": "behind"})
    body = resp.json()
    assert resp.status_code == 202, body
    planned = [lp["name"] for lp in body["plan"]["layers_to_refresh"]]
    assert "daily_thesis_refresh" in planned
