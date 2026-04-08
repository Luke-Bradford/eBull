"""Tests for /auth/bootstrap-state and /auth/recover (#114 / ADR-0003)."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.auth_bootstrap import router
from app.db import get_conn


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_conn] = lambda: None  # type: ignore[misc]
    app.state.boot_state = "clean_install"
    app.state.needs_setup = True
    app.state.recovery_required = False
    return TestClient(app)


class TestBootstrapState:
    def test_returns_app_state(self, client: TestClient) -> None:
        resp = client.get("/auth/bootstrap-state")
        assert resp.status_code == 200
        body = resp.json()
        assert body == {
            "boot_state": "clean_install",
            "needs_setup": True,
            "recovery_required": False,
        }

    def test_no_store_header(self, client: TestClient) -> None:
        resp = client.get("/auth/bootstrap-state")
        assert resp.headers["cache-control"] == "no-store"


class TestRecoverInputValidation:
    def test_recover_called_outside_recovery_required_409(self, client: TestClient) -> None:
        """The state-machine guard fires before any phrase processing."""
        resp = client.post("/auth/recover", json={"phrase": "abandon " * 24})
        assert resp.status_code == 409
        assert resp.json()["detail"] == "recovery not required"

    def test_wrong_word_count_400(self, client: TestClient) -> None:
        client.app.state.recovery_required = True  # type: ignore[attr-defined]
        resp = client.post("/auth/recover", json={"phrase": "abandon abandon"})
        assert resp.status_code == 400
        assert resp.json()["detail"] == "recovery phrase invalid"

    def test_empty_phrase_422(self, client: TestClient) -> None:
        client.app.state.recovery_required = True  # type: ignore[attr-defined]
        # min_length=1 on the pydantic field -> 422 before reaching the
        # handler. Important: the handler never sees an empty body.
        resp = client.post("/auth/recover", json={"phrase": ""})
        assert resp.status_code == 422
