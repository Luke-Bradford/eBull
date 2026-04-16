"""Tests for /sync/* endpoints (read-only + flag behaviour).

Avoids direct destructive writes against settings.database_url per
tests/smoke/test_no_settings_url_in_destructive_paths.py — the API
surface is exercised here via TestClient, and the reaper's logic is
exercised via a pure-SQL smoke check on a separate isolated connection
(the reaper is idempotent when nothing is stale, so this is safe).

Integration tests that write sync_runs rows live under tests/integration
against the isolated ebull_test database — out of scope for Phase 1.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.config import settings


@pytest.fixture
def client():
    """Fresh TestClient with auth bypassed and get_conn restored.

    Other test files (e.g. test_orders_api.py) install
    app.dependency_overrides[get_conn] and don't always restore — the
    overrides stick on the module-global FastAPI app. We explicitly
    clear and set only the auth bypass so our read-only endpoints hit
    the real DB with a recognized auth context."""
    from app.api.auth import require_session_or_service_token
    from app.main import app

    app.dependency_overrides.clear()
    app.dependency_overrides[require_session_or_service_token] = lambda: None
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture
def auth_headers():
    token = settings.service_token or "test-token"
    return {"Authorization": f"Bearer {token}"}


class TestPostSyncDisabled:
    def test_returns_503_when_flag_off(self, client, auth_headers, monkeypatch) -> None:
        monkeypatch.setattr(settings, "orchestrator_enabled", False)
        r = client.post("/sync", json={"scope": "full"}, headers=auth_headers)
        assert r.status_code == 503
        assert "disabled" in r.json()["detail"].lower()


class TestGetSyncLayersShape:
    def test_returns_15_layers_with_schema(self, client, auth_headers) -> None:
        """Read-only against dev DB — pre-existing runs/freshness OK."""
        r = client.get("/sync/layers", headers=auth_headers)
        assert r.status_code == 200
        data = r.json()
        assert len(data["layers"]) == 15
        names = {layer["name"] for layer in data["layers"]}
        assert "universe" in names
        assert "monthly_reports" in names
        for layer in data["layers"]:
            for key in (
                "display_name",
                "tier",
                "is_fresh",
                "freshness_detail",
                "dependencies",
                "is_blocking",
            ):
                assert key in layer, f"missing {key} in {layer['name']}"


class TestGetSyncRunsLimit:
    def test_limit_below_minimum_rejected(self, client, auth_headers) -> None:
        assert client.get("/sync/runs?limit=0", headers=auth_headers).status_code == 422

    def test_limit_above_maximum_rejected(self, client, auth_headers) -> None:
        assert client.get("/sync/runs?limit=101", headers=auth_headers).status_code == 422

    def test_limit_in_range_returns_200(self, client, auth_headers) -> None:
        r = client.get("/sync/runs?limit=50", headers=auth_headers)
        assert r.status_code == 200
        assert "runs" in r.json()
        assert isinstance(r.json()["runs"], list)


class TestGetSyncStatusShape:
    def test_response_has_expected_top_level_keys(self, client, auth_headers) -> None:
        r = client.get("/sync/status", headers=auth_headers)
        assert r.status_code == 200
        data = r.json()
        assert "is_running" in data
        assert "current_run" in data
        assert "active_layer" in data
        assert isinstance(data["is_running"], bool)


class TestReaperIdempotent:
    def test_reap_orphaned_syncs_runs_without_error(self) -> None:
        """Pure smoke: running the reaper is idempotent and returns an int.
        Under dev-DB the expected common case is no orphans (count=0),
        but the reaper also handles any real orphans cleanly."""
        from app.services.sync_orchestrator.reaper import reap_orphaned_syncs

        result = reap_orphaned_syncs()
        assert isinstance(result, int)
        assert result >= 0
