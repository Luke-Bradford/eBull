from unittest.mock import patch

from fastapi.testclient import TestClient

from app.main import app


def test_health_ok_when_no_action_needed() -> None:
    from app.services.sync_orchestrator.layer_types import LayerState

    all_healthy = {"candles": LayerState.HEALTHY, "cik_mapping": LayerState.HEALTHY}
    with (
        patch(
            "app.main.compute_layer_states_from_db",
            return_value=all_healthy,
        ),
        TestClient(app) as client,
    ):
        resp = client.get("/health")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "ok"
    assert body["system_state"] == "ok"
    assert "env" in body
    assert "etoro_env" in body


def test_health_503_when_action_needed() -> None:
    from app.services.sync_orchestrator.layer_types import LayerState

    degraded_states = {
        "candles": LayerState.HEALTHY,
        "cik_mapping": LayerState.ACTION_NEEDED,
    }
    with (
        patch(
            "app.main.compute_layer_states_from_db",
            return_value=degraded_states,
        ),
        TestClient(app) as client,
    ):
        resp = client.get("/health")
    assert resp.status_code == 503, resp.text
    body = resp.json()
    assert body["system_state"] == "needs_attention"


def test_health_503_when_secret_missing() -> None:
    from app.services.sync_orchestrator.layer_types import LayerState

    states = {"news": LayerState.SECRET_MISSING}
    with (
        patch(
            "app.main.compute_layer_states_from_db",
            return_value=states,
        ),
        TestClient(app) as client,
    ):
        resp = client.get("/health")
    assert resp.status_code == 503, resp.text
    assert resp.json()["system_state"] == "needs_attention"


def test_health_503_when_pool_checkout_fails() -> None:
    # Pool exhaustion / DB down: request.app.state.db_pool.connection()
    # itself raises. /health must still return the JSON 503 shape,
    # not FastAPI's default 500 HTML.
    from contextlib import contextmanager

    class _BrokenPool:
        @contextmanager
        def connection(self):
            raise RuntimeError("pool exhausted")
            yield None  # unreachable

    with TestClient(app) as client:
        original = app.state.db_pool
        app.state.db_pool = _BrokenPool()
        try:
            resp = client.get("/health")
        finally:
            app.state.db_pool = original
    assert resp.status_code == 503, resp.text
    body = resp.json()
    assert body["system_state"] == "error"
    assert body["status"] == "error"


def test_health_falls_back_to_ok_on_db_error() -> None:
    # If compute_layer_states_from_db raises (e.g. transient DB query
    # failure while the pool is still up), /health must still respond
    # 503 with system_state="error". The TestClient runs the real
    # lifespan so app.state.db_pool exists; pool.connection() succeeds,
    # then the patched compute_layer_states_from_db raises inside the
    # `with` block. The assertion on .called pins that we reached the
    # state-machine path rather than an earlier exception masking it.
    with (
        patch(
            "app.main.compute_layer_states_from_db",
            side_effect=RuntimeError("db down"),
        ) as patched,
        TestClient(app) as client,
    ):
        resp = client.get("/health")
    # 503 so external monitoring sees the outage.
    assert resp.status_code == 503, resp.text
    body = resp.json()
    assert body["system_state"] == "error"
    # Guard: the test actually drove through the state-machine call,
    # not a pool-checkout exception that happened to look the same.
    assert patched.called, "compute_layer_states_from_db was not exercised"


def test_health_still_no_auth_required() -> None:
    # Regression guard: /health remains unauthenticated (liveness probe).
    # No session cookie, no service token — must still answer.
    # Patch both dependencies so the bare FastAPI app doesn't need
    # a real DB pool or layer state machine.
    from unittest.mock import MagicMock

    from app.services.sync_orchestrator.layer_types import LayerState

    mock_conn = MagicMock()
    all_healthy = {"candles": LayerState.HEALTHY}
    with (
        patch("app.main.get_conn", return_value=mock_conn),
        patch(
            "app.main.compute_layer_states_from_db",
            return_value=all_healthy,
        ),
    ):
        with TestClient(app) as client:
            resp = client.get("/health")
    # 200 or 503 is acceptable; 401/403 would mean auth leaked in.
    assert resp.status_code in {200, 503}
