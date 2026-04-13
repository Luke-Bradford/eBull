"""
Tests for app.api.config — system config + kill switch endpoints (issue #56).

Mocks are applied at the service-function boundary (get_runtime_config,
update_runtime_config, get_kill_switch_status, activate_kill_switch,
deactivate_kill_switch).  The DB connection is dependency-overridden to a
MagicMock so the routes can resolve get_conn.

conftest.py installs a no-op require_auth override globally, so these tests
do not need to pass bearer tokens.  Real auth is exercised in test_api_auth.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from app.db import get_conn
from app.main import app
from app.services.runtime_config import RuntimeConfig, RuntimeConfigCorrupt

_NOW = datetime(2026, 4, 7, 12, 0, 0, tzinfo=UTC)


def _mock_conn() -> MagicMock:
    return MagicMock()


def _override_conn(conn: MagicMock) -> None:
    def _gen() -> Iterator[MagicMock]:
        yield conn

    app.dependency_overrides[get_conn] = _gen


def _clear_conn_override() -> None:
    app.dependency_overrides.pop(get_conn, None)


def _runtime(auto: bool = False, live: bool = False, currency: str = "USD") -> RuntimeConfig:
    return RuntimeConfig(
        enable_auto_trading=auto,
        enable_live_trading=live,
        display_currency=currency,
        updated_at=_NOW,
        updated_by="seed",
        reason="seed",
    )


client = TestClient(app)


# ---------------------------------------------------------------------------
# TestGetConfig
# ---------------------------------------------------------------------------


class TestGetConfig:
    def teardown_method(self) -> None:
        _clear_conn_override()

    def test_returns_runtime_kill_switch_and_env(self) -> None:
        _override_conn(_mock_conn())

        with (
            patch("app.api.config.get_runtime_config", return_value=_runtime(auto=True, live=False)),
            patch(
                "app.api.config.get_kill_switch_status",
                return_value={
                    "is_active": False,
                    "activated_at": None,
                    "activated_by": None,
                    "reason": None,
                },
            ),
        ):
            resp = client.get("/config")

        assert resp.status_code == 200
        body = resp.json()
        assert body["runtime"]["enable_auto_trading"] is True
        assert body["runtime"]["enable_live_trading"] is False
        assert body["runtime"]["display_currency"] == "USD"
        assert body["kill_switch"]["active"] is False
        assert "app_env" in body
        assert "etoro_env" in body

    def test_runtime_config_corrupt_returns_503(self) -> None:
        _override_conn(_mock_conn())

        with patch(
            "app.api.config.get_runtime_config",
            side_effect=RuntimeConfigCorrupt("singleton missing"),
        ):
            resp = client.get("/config")

        assert resp.status_code == 503
        assert "missing" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# TestPatchConfig
# ---------------------------------------------------------------------------


class TestPatchConfig:
    def teardown_method(self) -> None:
        _clear_conn_override()

    def test_partial_update_returns_new_runtime(self) -> None:
        _override_conn(_mock_conn())

        with patch(
            "app.api.config.update_runtime_config",
            return_value=_runtime(auto=True, live=False),
        ) as mock_update:
            resp = client.patch(
                "/config",
                json={"updated_by": "op", "reason": "enable auto", "enable_auto_trading": True},
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["enable_auto_trading"] is True

        # Service called with the partial-update kwargs
        mock_update.assert_called_once()
        kwargs = mock_update.call_args.kwargs
        assert kwargs["updated_by"] == "op"
        assert kwargs["reason"] == "enable auto"
        assert kwargs["enable_auto_trading"] is True
        assert kwargs["enable_live_trading"] is None

    def test_patch_display_currency_only_succeeds(self) -> None:
        _override_conn(_mock_conn())

        with patch(
            "app.api.config.update_runtime_config",
            return_value=_runtime(currency="GBP"),
        ) as mock_update:
            resp = client.patch(
                "/config",
                json={"updated_by": "op", "reason": "switch to GBP", "display_currency": "GBP"},
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["display_currency"] == "GBP"

        mock_update.assert_called_once()
        kwargs = mock_update.call_args.kwargs
        assert kwargs["display_currency"] == "GBP"
        assert kwargs["enable_auto_trading"] is None
        assert kwargs["enable_live_trading"] is None

    def test_empty_patch_returns_422(self) -> None:
        _override_conn(_mock_conn())
        resp = client.patch("/config", json={"updated_by": "op", "reason": "x"})
        assert resp.status_code == 422

    def test_missing_reason_returns_422(self) -> None:
        _override_conn(_mock_conn())
        resp = client.patch(
            "/config",
            json={"updated_by": "op", "reason": "", "enable_auto_trading": True},
        )
        assert resp.status_code == 422

    def test_missing_updated_by_returns_422(self) -> None:
        _override_conn(_mock_conn())
        resp = client.patch(
            "/config",
            json={"updated_by": "", "reason": "x", "enable_auto_trading": True},
        )
        assert resp.status_code == 422

    def test_enable_live_without_confirm_returns_422(self) -> None:
        _override_conn(_mock_conn())
        resp = client.patch(
            "/config",
            json={
                "updated_by": "op",
                "reason": "go live",
                "enable_live_trading": True,
            },
        )
        assert resp.status_code == 422
        assert "confirm_live_enable" in resp.text

    def test_enable_live_with_confirm_succeeds(self) -> None:
        _override_conn(_mock_conn())
        with patch(
            "app.api.config.update_runtime_config",
            return_value=_runtime(auto=True, live=True),
        ):
            resp = client.patch(
                "/config",
                json={
                    "updated_by": "op",
                    "reason": "go live",
                    "enable_live_trading": True,
                    "confirm_live_enable": True,
                },
            )
        assert resp.status_code == 200
        assert resp.json()["enable_live_trading"] is True

    def test_disable_live_does_not_require_confirm(self) -> None:
        _override_conn(_mock_conn())
        with patch(
            "app.api.config.update_runtime_config",
            return_value=_runtime(auto=True, live=False),
        ):
            resp = client.patch(
                "/config",
                json={
                    "updated_by": "op",
                    "reason": "stop live",
                    "enable_live_trading": False,
                },
            )
        assert resp.status_code == 200

    def test_corrupt_runtime_config_returns_503(self) -> None:
        _override_conn(_mock_conn())
        with patch(
            "app.api.config.update_runtime_config",
            side_effect=RuntimeConfigCorrupt("missing"),
        ):
            resp = client.patch(
                "/config",
                json={
                    "updated_by": "op",
                    "reason": "x",
                    "enable_auto_trading": True,
                },
            )
        assert resp.status_code == 503


# ---------------------------------------------------------------------------
# TestPostKillSwitch
# ---------------------------------------------------------------------------


class TestPostKillSwitch:
    def teardown_method(self) -> None:
        _clear_conn_override()

    def test_activate_calls_service(self) -> None:
        _override_conn(_mock_conn())
        with patch(
            "app.api.config.activate_kill_switch",
            return_value={
                "is_active": True,
                "activated_at": _NOW,
                "activated_by": "op",
                "reason": "halt",
            },
        ) as mock_activate:
            resp = client.post(
                "/config/kill-switch",
                json={"active": True, "reason": "halt", "activated_by": "op"},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["active"] is True
        assert body["activated_by"] == "op"
        assert body["reason"] == "halt"
        mock_activate.assert_called_once()

    def test_deactivate_calls_service(self) -> None:
        _override_conn(_mock_conn())
        with patch(
            "app.api.config.deactivate_kill_switch",
            return_value={
                "is_active": False,
                "activated_at": None,
                "activated_by": None,
                "reason": None,
            },
        ) as mock_deactivate:
            resp = client.post(
                "/config/kill-switch",
                json={"active": False, "reason": "resolved", "activated_by": "op"},
            )
        assert resp.status_code == 200
        assert resp.json()["active"] is False
        mock_deactivate.assert_called_once()

    def test_activate_without_reason_returns_422(self) -> None:
        _override_conn(_mock_conn())
        resp = client.post(
            "/config/kill-switch",
            json={"active": True, "reason": "", "activated_by": "op"},
        )
        assert resp.status_code == 422

    def test_missing_kill_switch_row_returns_503(self) -> None:
        _override_conn(_mock_conn())
        with patch(
            "app.api.config.activate_kill_switch",
            side_effect=RuntimeError("kill_switch row missing"),
        ):
            resp = client.post(
                "/config/kill-switch",
                json={"active": True, "reason": "halt", "activated_by": "op"},
            )
        assert resp.status_code == 503
