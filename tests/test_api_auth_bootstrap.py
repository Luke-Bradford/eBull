"""Tests for /auth/bootstrap-state and /auth/recover (#114 / ADR-0003)."""

from __future__ import annotations

import pytest
from fastapi import Depends, FastAPI
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
        # Submit a structurally valid 24-word phrase so the test
        # cleanly isolates the 409 (state-machine guard) from the
        # 400 (word-count guard) -- the 409 must fire first.
        phrase = " ".join(["abandon"] * 24)
        resp = client.post("/auth/recover", json={"phrase": phrase})
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


class TestRequireMasterKey:
    """Coverage for the structural require_master_key dependency (#118 round 9).

    Mounted on broker routes that need the cipher cache. Must 503
    on every not-loaded state EXCEPT clean_install (which is the
    legitimate entry point for the very first credential save).
    """

    def _app_with_route(self) -> tuple[FastAPI, TestClient]:
        from app.api.auth_bootstrap import require_master_key

        app = FastAPI()

        @app.get("/gated", dependencies=[Depends(require_master_key)])
        def _gated() -> dict[str, str]:
            return {"ok": "yes"}

        return app, TestClient(app)

    def test_recovery_required_503(self) -> None:
        app, c = self._app_with_route()
        app.state.recovery_required = True
        app.state.broker_key_loaded = False
        app.state.boot_state = "recovery_required"
        resp = c.get("/gated")
        assert resp.status_code == 503
        assert resp.json()["detail"] == "recovery required"

    def test_normal_state_with_key_loaded_passes(self) -> None:
        app, c = self._app_with_route()
        app.state.recovery_required = False
        app.state.broker_key_loaded = True
        app.state.boot_state = "normal"
        resp = c.get("/gated")
        assert resp.status_code == 200

    def test_clean_install_no_key_passes_for_lazy_gen(self) -> None:
        # The very first credential save lands here: clean_install,
        # no key loaded, no recovery required. Must NOT be 503'd by
        # the structural gate -- the create handler lazy-generates
        # the key.
        app, c = self._app_with_route()
        app.state.recovery_required = False
        app.state.broker_key_loaded = False
        app.state.boot_state = "clean_install"
        resp = c.get("/gated")
        assert resp.status_code == 200

    def test_unknown_not_loaded_state_503(self) -> None:
        # An env-override misconfig or internal bug that leaves
        # broker_key_loaded=False outside clean_install must 503,
        # not fall through to a 500 from the cipher cache.
        app, c = self._app_with_route()
        app.state.recovery_required = False
        app.state.broker_key_loaded = False
        app.state.boot_state = "normal"
        resp = c.get("/gated")
        assert resp.status_code == 503
        assert resp.json()["detail"] == "master key not loaded"
