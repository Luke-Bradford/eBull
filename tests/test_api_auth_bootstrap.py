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

    @pytest.mark.parametrize(
        "exc_factory",
        [
            pytest.param(
                lambda: __import__(
                    "app.security.recovery_phrase", fromlist=["RecoveryPhraseError"]
                ).RecoveryPhraseError("bad checksum"),
                id="RecoveryPhraseError",
            ),
            pytest.param(
                lambda: __import__(
                    "app.security.master_key", fromlist=["RecoveryVerificationError"]
                ).RecoveryVerificationError("phrase did not match"),
                id="RecoveryVerificationError",
            ),
            pytest.param(
                lambda: __import__(
                    "app.security.master_key", fromlist=["RecoveryNotApplicableError"]
                ).RecoveryNotApplicableError("no active credential"),
                id="RecoveryNotApplicableError",
            ),
        ],
    )
    def test_all_phrase_path_failures_return_generic_400(
        self,
        client: TestClient,
        monkeypatch: pytest.MonkeyPatch,
        exc_factory: object,
    ) -> None:
        """ADR-0003 §6: every failure mode reachable via /auth/recover
        in recovery_required state must return EXACTLY 400 with the
        same generic detail. A distinct status (e.g. 409 for
        RecoveryNotApplicableError) would let a caller fingerprint
        "wrong phrase" vs "no row to verify against" by status code
        alone (review feedback PR #118 round 18).

        Parametrized (round 19) so each exception class gets a fresh
        monkeypatch scope rather than stacking patches in a loop.
        """
        from app.api import auth_bootstrap

        client.app.state.recovery_required = True  # type: ignore[attr-defined]
        phrase = " ".join(["abandon"] * 24)
        exc = exc_factory()  # type: ignore[operator]

        def _raise(*_a: object, **_k: object) -> None:
            raise exc  # type: ignore[misc]

        monkeypatch.setattr(auth_bootstrap.master_key, "recover_from_phrase", _raise)
        resp = client.post("/auth/recover", json={"phrase": phrase})
        assert resp.status_code == 400
        assert resp.json()["detail"] == "recovery phrase invalid"


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

    def test_clean_install_no_key_503(self) -> None:
        # POST /broker-credentials does NOT mount this dependency
        # (the create handler self-gates so it can lazy-generate
        # on first save). Every other route mounted on this
        # dependency must 503 in clean_install state, not pass
        # through and hit a 500 from CredentialCryptoConfigError
        # (review feedback PR #118 round 10).
        app, c = self._app_with_route()
        app.state.recovery_required = False
        app.state.broker_key_loaded = False
        app.state.boot_state = "clean_install"
        resp = c.get("/gated")
        assert resp.status_code == 503
        assert resp.json()["detail"] == "master key not loaded"

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
