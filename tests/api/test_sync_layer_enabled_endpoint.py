import pytest
from fastapi.testclient import TestClient


@pytest.mark.integration
def test_post_layer_enabled_happy_path(clean_client: TestClient) -> None:
    resp = clean_client.post("/sync/layers/candles/enabled", json={"enabled": False})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["layer"] == "candles"
    assert body["is_enabled"] is False
    assert body["warning"] is None
    assert body["display_name"] == "Daily Price Candles"

    # Re-enable and verify the round-trip (proves the write was committed).
    resp2 = clean_client.post("/sync/layers/candles/enabled", json={"enabled": True})
    assert resp2.status_code == 200
    assert resp2.json()["is_enabled"] is True


@pytest.mark.integration
def test_post_layer_enabled_fx_rates_disable_warning(clean_client: TestClient) -> None:
    try:
        resp = clean_client.post("/sync/layers/fx_rates/enabled", json={"enabled": False})
        assert resp.status_code == 200
        body = resp.json()
        assert body["is_enabled"] is False
        assert body["warning"] is not None
        assert "drift" in body["warning"].lower()
    finally:
        clean_client.post("/sync/layers/fx_rates/enabled", json={"enabled": True})


@pytest.mark.integration
def test_post_layer_enabled_portfolio_sync_disable_warning(clean_client: TestClient) -> None:
    try:
        resp = clean_client.post("/sync/layers/portfolio_sync/enabled", json={"enabled": False})
        assert resp.status_code == 200
        body = resp.json()
        assert body["warning"] is not None
        warning_lower = body["warning"].lower()
        assert "broker" in warning_lower or "portfolio" in warning_lower
    finally:
        clean_client.post("/sync/layers/portfolio_sync/enabled", json={"enabled": True})


def test_post_layer_enabled_unknown_layer_404(clean_client: TestClient) -> None:
    resp = clean_client.post("/sync/layers/not_a_real_layer/enabled", json={"enabled": False})
    assert resp.status_code == 404


@pytest.mark.integration
def test_post_layer_enabled_enable_surfaces_no_warning(clean_client: TestClient) -> None:
    resp = clean_client.post("/sync/layers/fx_rates/enabled", json={"enabled": True})
    assert resp.status_code == 200
    assert resp.json()["warning"] is None


def test_post_layer_enabled_requires_auth() -> None:
    # Bare TestClient without clean_client fixture — exercises the real
    # auth dependency. Must return 401 with no session/token.
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
        resp = client.post("/sync/layers/candles/enabled", json={"enabled": False})
    assert resp.status_code in {401, 403}, resp.text
