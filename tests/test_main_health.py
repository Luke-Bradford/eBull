from unittest.mock import patch

from fastapi.testclient import TestClient

from app.main import app


def test_health_ok_when_no_action_needed() -> None:
    from app.services.sync_orchestrator.layer_types import LayerState

    all_healthy = {"candles": LayerState.HEALTHY, "cik_mapping": LayerState.HEALTHY}
    with patch(
        "app.main.compute_layer_states_from_db",
        return_value=all_healthy,
    ), TestClient(app) as client:
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
    with patch(
        "app.main.compute_layer_states_from_db",
        return_value=degraded_states,
    ), TestClient(app) as client:
        resp = client.get("/health")
    assert resp.status_code == 503, resp.text
    body = resp.json()
    assert body["system_state"] == "needs_attention"


def test_health_503_when_secret_missing() -> None:
    from app.services.sync_orchestrator.layer_types import LayerState

    states = {"news": LayerState.SECRET_MISSING}
    with patch(
        "app.main.compute_layer_states_from_db",
        return_value=states,
    ), TestClient(app) as client:
        resp = client.get("/health")
    assert resp.status_code == 503, resp.text
    assert resp.json()["system_state"] == "needs_attention"


def test_health_falls_back_to_ok_on_db_error() -> None:
    # If compute_layer_states_from_db raises (e.g. DB unreachable),
    # /health must still respond 200 with a degraded system_state so
    # orchestrator outages do not mask as a healthy system but also
    # do not cause liveness-probe pages to crash the app boot
    # smoke test.
    with patch(
        "app.main.compute_layer_states_from_db",
        side_effect=RuntimeError("db down"),
    ), TestClient(app) as client:
        resp = client.get("/health")
    # 503 so external monitoring sees the outage.
    assert resp.status_code == 503, resp.text
    body = resp.json()
    assert body["system_state"] == "error"


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
