"""Tests for /auth/bootstrap-state (#114 / ADR-0003, amended 2026-05-07).

Post-amendment coverage: the recovery_phrase ceremony, ``POST
/auth/recover`` endpoint, and ``recovery_required`` boot state are
removed. ``BootstrapStateResponse`` is now ``{boot_state, needs_setup}``
where needs_setup is operator-state only.
"""

from __future__ import annotations

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from app.api import auth_bootstrap
from app.api.auth_bootstrap import router
from app.db import get_conn


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_conn] = lambda: None  # type: ignore[misc]
    monkeypatch.setattr(auth_bootstrap, "operators_empty", lambda _conn: False)
    app.state.boot_state = "clean_install"
    return TestClient(app)


class TestBootstrapState:
    def test_returns_app_state(self, client: TestClient) -> None:
        resp = client.get("/auth/bootstrap-state")
        assert resp.status_code == 200
        body = resp.json()
        assert body == {
            "boot_state": "clean_install",
            "needs_setup": False,
        }

    def test_no_store_header(self, client: TestClient) -> None:
        resp = client.get("/auth/bootstrap-state")
        assert resp.headers["cache-control"] == "no-store"

    def test_needs_setup_true_when_operators_table_empty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Empty operators table → needs_setup True regardless of
        boot_state. needs_setup is operator-state only — the master-key
        boot state does not gate the wizard."""
        app = FastAPI()
        app.include_router(router)
        app.dependency_overrides[get_conn] = lambda: None  # type: ignore[misc]
        monkeypatch.setattr(auth_bootstrap, "operators_empty", lambda _conn: True)
        app.state.boot_state = "normal"
        c = TestClient(app)

        resp = c.get("/auth/bootstrap-state")
        assert resp.status_code == 200
        body = resp.json()
        assert body["needs_setup"] is True
        assert body["boot_state"] == "normal"
        # Field removed post-amendment 2026-05-07.
        assert "recovery_required" not in body

    def test_recover_endpoint_is_404(self, client: TestClient) -> None:
        """POST /auth/recover was removed in the 2026-05-07 amendment.
        The router no longer mounts it, so any caller hits 404/405.
        """
        resp = client.post("/auth/recover", json={"phrase": "x"})
        assert resp.status_code in (404, 405)


class TestRequireMasterKey:
    """Coverage for the structural require_master_key dependency.

    Post-amendment: only one failure mode remains —
    broker_key_loaded=False → 503 master key not loaded. The prior
    recovery_required branch is gone.
    """

    def _app_with_route(self) -> tuple[FastAPI, TestClient]:
        from app.api.auth_bootstrap import require_master_key

        app = FastAPI()

        @app.get("/gated", dependencies=[Depends(require_master_key)])
        def _gated() -> dict[str, str]:
            return {"ok": "yes"}

        return app, TestClient(app)

    def test_normal_state_with_key_loaded_passes(self) -> None:
        app, c = self._app_with_route()
        app.state.broker_key_loaded = True
        app.state.boot_state = "normal"
        resp = c.get("/gated")
        assert resp.status_code == 200

    def test_clean_install_no_key_503(self) -> None:
        # POST /broker-credentials does NOT mount this dependency
        # (the create handler self-gates so it can lazy-generate on
        # first save). Every other route mounted on this dependency
        # must 503 in clean_install state, not pass through and hit a
        # 500 from CredentialCryptoConfigError.
        app, c = self._app_with_route()
        app.state.broker_key_loaded = False
        app.state.boot_state = "clean_install"
        resp = c.get("/gated")
        assert resp.status_code == 503
        assert resp.json()["detail"] == "master key not loaded"

    def test_unknown_not_loaded_state_503(self) -> None:
        # An env-override misconfig or internal bug that leaves
        # broker_key_loaded=False outside clean_install must 503, not
        # fall through to a 500 from the cipher cache.
        app, c = self._app_with_route()
        app.state.broker_key_loaded = False
        app.state.boot_state = "normal"
        resp = c.get("/gated")
        assert resp.status_code == 503
        assert resp.json()["detail"] == "master key not loaded"
