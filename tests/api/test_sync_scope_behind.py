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
    # scoring is ACTION_NEEDED, all its upstreams (candles, fundamentals)
    # are healthy so only scoring fires. Verify the candidate job set
    # contains morning_candidate_review (the job that emits scoring).
    from app.services.sync_orchestrator.layer_types import LayerState
    from app.services.sync_orchestrator.registry import LAYERS

    states = {n: LayerState.HEALTHY for n in LAYERS}
    states["scoring"] = LayerState.ACTION_NEEDED
    with patch(
        "app.services.sync_orchestrator.planner.compute_layer_states_from_db",
        return_value=states,
    ):
        resp = clean_client.post("/sync", json={"scope": "behind"})
    body = resp.json()
    assert resp.status_code == 202, body
    planned = [lp["name"] for lp in body["plan"]["layers_to_refresh"]]
    assert "morning_candidate_review" in planned


def test_post_sync_empty_body_defaults_to_behind(clean_client: TestClient) -> None:
    # Regression guard: posting {} (or omitting body) uses scope=behind,
    # not scope=full. Matches the frontend default in useSyncTrigger.
    from app.services.sync_orchestrator import SyncScope as _Scope

    with patch("app.api.sync.submit_sync") as submit:
        submit.return_value = (43, _make_empty_plan())
        resp = clean_client.post("/sync", json={})
    assert resp.status_code == 202, resp.text
    scope_arg: _Scope = submit.call_args.args[0]
    assert scope_arg.kind == "behind"


def test_post_sync_behind_skips_disabled_upstream(clean_client: TestClient) -> None:
    # A DEGRADED layer whose upstream is DISABLED must NOT pull the
    # disabled upstream into the plan — operator toggle wins over
    # cascade refresh.
    from app.services.sync_orchestrator.layer_types import LayerState
    from app.services.sync_orchestrator.registry import LAYERS

    states = {n: LayerState.HEALTHY for n in LAYERS}
    # universe is an upstream of candles.
    states["candles"] = LayerState.DEGRADED
    states["universe"] = LayerState.DISABLED

    with patch(
        "app.services.sync_orchestrator.planner.compute_layer_states_from_db",
        return_value=states,
    ):
        resp = clean_client.post("/sync", json={"scope": "behind"})
    body = resp.json()
    assert resp.status_code == 202, body
    planned = [lp["name"] for lp in body["plan"]["layers_to_refresh"]]
    assert "nightly_universe_sync" not in planned, body
    assert "daily_candle_refresh" in planned, body


def test_post_sync_behind_bypasses_legacy_freshness_filter(clean_client: TestClient) -> None:
    # Spec invariant: when the state machine says a layer is
    # ACTION_NEEDED or DEGRADED, scope=behind must plan it even if
    # the legacy is_fresh predicate returns True. The state machine
    # is authoritative under scope=behind.
    from app.services.sync_orchestrator.layer_types import LayerState
    from app.services.sync_orchestrator.registry import LAYERS

    states = {n: LayerState.HEALTHY for n in LAYERS}
    states["scoring"] = LayerState.ACTION_NEEDED

    # Patch is_fresh to always return True — if the freshness filter
    # leaked through, the plan would be empty. force=True on the
    # SyncScope.behind() classmethod should bypass the filter.
    with (
        patch(
            "app.services.sync_orchestrator.planner.compute_layer_states_from_db",
            return_value=states,
        ),
        patch(
            "app.services.sync_orchestrator.planner._all_emits_fresh",
            return_value=(True, "faked fresh"),
        ),
    ):
        resp = clean_client.post("/sync", json={"scope": "behind"})
    body = resp.json()
    assert resp.status_code == 202, body
    planned = [lp["name"] for lp in body["plan"]["layers_to_refresh"]]
    assert "morning_candidate_review" in planned, body
