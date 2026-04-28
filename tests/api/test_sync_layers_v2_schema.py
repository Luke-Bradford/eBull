import pytest
from fastapi.testclient import TestClient

# All tests use the ``clean_client`` fixture (real DB-backed). Per the
# #421 PREVENTION rule, mark the whole module integration so unit-only
# CI passes deselect cleanly.
pytestmark = pytest.mark.integration


def test_v2_endpoint_returns_expected_top_level_keys(clean_client: TestClient) -> None:
    resp = clean_client.get("/sync/layers/v2")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert set(body.keys()) == {
        "generated_at",
        "system_state",
        "system_summary",
        "action_needed",
        "degraded",
        "secret_missing",
        "healthy",
        "disabled",
        "cascade_groups",
        "layers",
    }


def test_v2_system_state_in_expected_set(clean_client: TestClient) -> None:
    resp = clean_client.get("/sync/layers/v2")
    assert resp.json()["system_state"] in {"ok", "catching_up", "needs_attention"}


def test_v2_healthy_entries_shape(clean_client: TestClient) -> None:
    resp = clean_client.get("/sync/layers/v2")
    for entry in resp.json()["healthy"]:
        assert set(entry.keys()) >= {"layer", "display_name", "last_updated"}


def test_v2_secret_missing_never_silently_dropped(clean_client: TestClient) -> None:
    # Regression guard: every layer the state machine classifies as
    # SECRET_MISSING must appear in the secret_missing bucket.
    # Phase 1.2 retired the only secret-declaring layers (news + thesis
    # moved to on-demand endpoints), so this test now fabricates a
    # SECRET_MISSING state on a regular layer to exercise the bucket
    # routing contract — the endpoint must NOT gate on secret_refs
    # being present in the LAYERS registry for the classification.
    from unittest.mock import patch

    from app.services.sync_orchestrator.layer_types import LayerState
    from app.services.sync_orchestrator.registry import LAYERS

    layer_names = list(LAYERS.keys())
    secret_layer = layer_names[0]
    action_layer = layer_names[1]

    fake = {n: LayerState.HEALTHY for n in LAYERS}
    fake[secret_layer] = LayerState.SECRET_MISSING
    fake[action_layer] = LayerState.ACTION_NEEDED

    with patch(
        "app.api.sync.compute_layer_states_from_db",
        return_value=fake,
    ):
        resp = clean_client.get("/sync/layers/v2")
    body = resp.json()
    secret_names = {s["layer"] for s in body["secret_missing"]}
    expected = {n for n, s in fake.items() if s is LayerState.SECRET_MISSING}
    assert secret_names >= expected, body


def test_v2_secret_missing_fallback_when_env_populated(clean_client: TestClient) -> None:
    # If compute_layer_states_from_db says SECRET_MISSING but os.environ
    # has every secret set by the time the endpoint loop runs, the
    # layer must still appear (with fallback display), not vanish.
    # Phase 1.2: no layer declares secret_refs; fake the state on a
    # regular layer to exercise the bucketing without env dependencies.
    from unittest.mock import patch

    from app.services.sync_orchestrator.layer_types import LayerState
    from app.services.sync_orchestrator.registry import LAYERS

    secret_layer = next(iter(LAYERS.keys()))
    fake = {n: LayerState.HEALTHY for n in LAYERS}
    fake[secret_layer] = LayerState.SECRET_MISSING

    with patch("app.api.sync.compute_layer_states_from_db", return_value=fake):
        resp = clean_client.get("/sync/layers/v2")
    body = resp.json()
    assert any(s["layer"] == secret_layer for s in body["secret_missing"]), body


def test_v2_cascade_groups_match_action_needed_downstream(clean_client: TestClient) -> None:
    # Pin the shared-cache invariant: cascade_groups[i].affected for
    # each action_needed root must match that root's
    # affected_downstream exactly, in order. Patches the state map so
    # the assertion exercises at least one ACTION_NEEDED + one
    # CASCADE_WAITING descendant — no live-DB precondition required.
    from unittest.mock import patch

    from app.services.sync_orchestrator.layer_types import LayerState
    from app.services.sync_orchestrator.registry import LAYERS

    # Pick a root with at least one downstream dependent in the registry.
    root = next(n for n, _ in LAYERS.items() if any(n in other.dependencies for other in LAYERS.values()))
    # Every layer transitively downstream of `root` becomes CASCADE_WAITING.
    fake = {n: LayerState.HEALTHY for n in LAYERS}
    fake[root] = LayerState.ACTION_NEEDED
    # Simple BFS in test to compute expected waiters.
    reverse: dict[str, list[str]] = {n: [] for n in LAYERS}
    for n, lay in LAYERS.items():
        for dep in lay.dependencies:
            reverse[dep].append(n)
    frontier = {root}
    visited = {root}
    while frontier:
        nxt = set()
        for parent in frontier:
            for child in reverse.get(parent, ()):
                if child in visited:
                    continue
                visited.add(child)
                fake[child] = LayerState.CASCADE_WAITING
                nxt.add(child)
        frontier = nxt

    with patch("app.api.sync.compute_layer_states_from_db", return_value=fake):
        resp = clean_client.get("/sync/layers/v2")
    body = resp.json()
    groups_by_root = {g["root"]: g["affected"] for g in body["cascade_groups"]}
    action_items = body["action_needed"]
    assert len(action_items) >= 1, body
    for item in action_items:
        assert groups_by_root.get(item["root_layer"]) == item["affected_downstream"], body


def test_v2_summary_never_contradicts_state(clean_client: TestClient) -> None:
    # Regression guard: catching_up system_state implies summary
    # names a non-healthy cohort (degraded/running/retrying/cascade).
    # needs_attention implies action_needed or secret_missing text.
    resp = clean_client.get("/sync/layers/v2")
    body = resp.json()
    state = body["system_state"]
    summary = body["system_summary"]
    if state == "catching_up":
        assert "All layers healthy" not in summary, body
    if state == "needs_attention":
        assert "All layers healthy" not in summary, body
    if state == "ok":
        assert summary == "All layers healthy", body


def test_v2_includes_canonical_layers_field(clean_client: TestClient) -> None:
    resp = clean_client.get("/sync/layers/v2")
    body = resp.json()
    assert "layers" in body
    assert isinstance(body["layers"], list)


def test_v2_layers_contains_every_registered_layer_once(clean_client: TestClient) -> None:
    from app.services.sync_orchestrator.registry import LAYERS

    resp = clean_client.get("/sync/layers/v2")
    body = resp.json()
    returned = [entry["layer"] for entry in body["layers"]]
    assert sorted(returned) == sorted(LAYERS.keys())
    assert len(returned) == len(set(returned)), "duplicate layer in v2.layers"


def test_v2_layer_entry_shape(clean_client: TestClient) -> None:
    resp = clean_client.get("/sync/layers/v2")
    for entry in resp.json()["layers"]:
        assert set(entry.keys()) == {
            "layer",
            "display_name",
            "state",
            "last_updated",
            "plain_language_sla",
        }
        from app.services.sync_orchestrator.layer_types import LayerState

        assert entry["state"] in {s.value for s in LayerState}


def test_v2_layer_entry_metadata_matches_registry(clean_client: TestClient) -> None:
    from app.services.sync_orchestrator.registry import LAYERS

    resp = clean_client.get("/sync/layers/v2")
    for entry in resp.json()["layers"]:
        layer = LAYERS[entry["layer"]]
        assert entry["display_name"] == layer.display_name
        assert entry["plain_language_sla"] == layer.plain_language_sla


def test_v2_layers_sorted_by_name(clean_client: TestClient) -> None:
    resp = clean_client.get("/sync/layers/v2")
    names = [entry["layer"] for entry in resp.json()["layers"]]
    assert names == sorted(names), "v2.layers must be sorted by layer name for determinism"


def test_v2_requires_auth() -> None:
    # /sync/layers/v2 must inherit the same require_session_or_service_token
    # dependency as /sync/layers. A no-auth TestClient using a bare app
    # without session setup should be rejected.
    #
    # require_session_or_service_token itself depends on get_conn (for session
    # lookups). We override get_conn with a lightweight mock so the dependency
    # graph can resolve far enough for auth to fire its 401 before crashing on
    # a missing db_pool — the goal is to prove the auth dependency is wired,
    # not to exercise the full DB stack.
    from unittest.mock import MagicMock

    from fastapi import FastAPI

    from app.api.sync import router as sync_router
    from app.db import get_conn

    def _mock_conn():  # type: ignore[return]
        yield MagicMock()

    bare = FastAPI()
    bare.include_router(sync_router)
    bare.dependency_overrides[get_conn] = _mock_conn
    with TestClient(bare) as client:
        resp = client.get("/sync/layers/v2")
    # 401 (no session) or 403 (no token) — both prove auth is applied.
    assert resp.status_code in {401, 403}, resp.text
