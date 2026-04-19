from fastapi.testclient import TestClient


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
    }


def test_v2_system_state_in_expected_set(clean_client: TestClient) -> None:
    resp = clean_client.get("/sync/layers/v2")
    assert resp.json()["system_state"] in {"ok", "catching_up", "needs_attention"}


def test_v2_healthy_entries_shape(clean_client: TestClient) -> None:
    resp = clean_client.get("/sync/layers/v2")
    for entry in resp.json()["healthy"]:
        assert set(entry.keys()) >= {"layer", "display_name", "last_updated"}


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
