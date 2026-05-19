"""Tests for `GET /system/postgres-health` (#1208 Phase 4).

Strategy mirrors test_api_system.py: TestClient + patch the service
import boundary inside the route module. Endpoint shape, breach-flag
computation, per-metric isolation, and the auth gate are the things
under test.

Codex 1a/1b regressions:
- BLOCKING #1 — `test_metric_isolation_under_psycopg_error` proves
  the autocommit-conn shape prevents one failed probe from poisoning
  the others. Service is patched to raise inside one of seven
  metrics; response still 200; only the affected field/flag are
  null; `metric_errors` lists the failed probe.
- WARNING #4/#5 — `test_wal_breach_flag_keys_on_dir_not_since_checkpoint`
  proves the breach flag wires off `wal_dir_bytes`, not
  `wal_since_checkpoint_bytes`.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import psycopg
from fastapi.testclient import TestClient

from app.api.auth import require_session_or_service_token
from app.db import get_conn
from app.main import app
from app.services.postgres_health import (
    AutovacuumTableLag,
    PostgresHealthSnapshot,
)

client = TestClient(app)

_NOW = datetime(2026, 5, 19, 12, 0, 0, tzinfo=UTC)


def _snapshot(**overrides: object) -> PostgresHealthSnapshot:
    """Build a `PostgresHealthSnapshot` with safe defaults; tests
    override individual fields to exercise breach/null pathways."""
    defaults: dict[str, object] = {
        "db_size_bytes": 5 * 1024 * 1024 * 1024,  # 5 GB — below 10 GB
        "db_size_pretty": "5 GB",
        "db_size_warn_threshold_bytes": 10 * 1024 * 1024 * 1024,
        "db_size_breached_warn": False,
        "leaked_test_db_count": 0,
        "leaked_test_db_names": [],
        "wal_dir_bytes": 2 * 1024 * 1024 * 1024,  # 2 GB — below 4 GB
        "wal_dir_pretty": "2 GB",
        "wal_since_checkpoint_bytes": 1_000_000,
        "wal_warn_threshold_bytes": 4 * 1024 * 1024 * 1024,
        "wal_breached_warn": False,
        "last_checkpoint_at": _NOW,
        "autovacuum_top10": [
            AutovacuumTableLag(
                relname="financial_facts_raw_2024q3",
                last_autovacuum=_NOW,
                last_analyze=_NOW,
                n_dead_tup=2397,
                n_live_tup=1_125_216,
                dead_fraction=2397 / (2397 + 1_125_216),
            )
        ],
        "financial_facts_raw_default_rows": 1055,
        "financial_facts_raw_default_warn_threshold": 5000,
        "financial_facts_raw_default_breached_warn": False,
        "metric_errors": [],
        "collected_at": _NOW,
    }
    defaults.update(overrides)
    return PostgresHealthSnapshot(**defaults)  # type: ignore[arg-type]


def test_endpoint_returns_200_with_all_fields() -> None:
    with patch(
        "app.services.postgres_health.collect_postgres_health",
        return_value=_snapshot(),
    ):
        resp = client.get("/system/postgres-health")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    expected_keys = {
        "db_size_bytes",
        "db_size_pretty",
        "db_size_warn_threshold_bytes",
        "db_size_breached_warn",
        "leaked_test_db_count",
        "leaked_test_db_names",
        "wal_dir_bytes",
        "wal_dir_pretty",
        "wal_since_checkpoint_bytes",
        "wal_warn_threshold_bytes",
        "wal_breached_warn",
        "last_checkpoint_at",
        "autovacuum_top10",
        "financial_facts_raw_default_rows",
        "financial_facts_raw_default_warn_threshold",
        "financial_facts_raw_default_breached_warn",
        "metric_errors",
        "collected_at",
    }
    assert expected_keys.issubset(body.keys())
    assert body["metric_errors"] == []
    assert body["autovacuum_top10"][0]["relname"] == "financial_facts_raw_2024q3"


def test_db_size_breach_flag_above_threshold() -> None:
    with patch(
        "app.services.postgres_health.collect_postgres_health",
        return_value=_snapshot(
            db_size_bytes=50 * 1024 * 1024 * 1024,
            db_size_pretty="50 GB",
            db_size_breached_warn=True,
        ),
    ):
        resp = client.get("/system/postgres-health")
    assert resp.status_code == 200
    assert resp.json()["db_size_breached_warn"] is True


def test_default_partition_warn_flag_above_threshold() -> None:
    with patch(
        "app.services.postgres_health.collect_postgres_health",
        return_value=_snapshot(
            financial_facts_raw_default_rows=10_000,
            financial_facts_raw_default_breached_warn=True,
        ),
    ):
        resp = client.get("/system/postgres-health")
    assert resp.status_code == 200
    assert resp.json()["financial_facts_raw_default_breached_warn"] is True


def test_wal_breach_flag_keys_on_dir_not_since_checkpoint() -> None:
    """Codex 1b WARNING #4 regression: WAL breach flag MUST track
    `wal_dir_bytes`, not `wal_since_checkpoint_bytes`. Set
    since_checkpoint enormous + dir under threshold → breach is
    False. Set dir over threshold + since_checkpoint tiny → breach
    is True."""
    # Case A: since_checkpoint huge, dir under threshold → False
    with patch(
        "app.services.postgres_health.collect_postgres_health",
        return_value=_snapshot(
            wal_dir_bytes=1_000_000,  # tiny
            wal_dir_pretty="1 MB",
            wal_since_checkpoint_bytes=99_999_999_999,  # enormous
            wal_breached_warn=False,
        ),
    ):
        resp = client.get("/system/postgres-health")
    assert resp.json()["wal_breached_warn"] is False

    # Case B: dir over threshold, since_checkpoint tiny → True
    with patch(
        "app.services.postgres_health.collect_postgres_health",
        return_value=_snapshot(
            wal_dir_bytes=10 * 1024 * 1024 * 1024,
            wal_dir_pretty="10 GB",
            wal_since_checkpoint_bytes=42,
            wal_breached_warn=True,
        ),
    ):
        resp = client.get("/system/postgres-health")
    assert resp.json()["wal_breached_warn"] is True


def test_metric_isolation_returns_partial_payload() -> None:
    """Codex 1a BLOCKING #1 regression: when one metric probe fails,
    the response is still 200 with the failed metric's fields null +
    the breach flag null + `metric_errors` listing the probe name."""
    with patch(
        "app.services.postgres_health.collect_postgres_health",
        return_value=_snapshot(
            wal_dir_bytes=None,
            wal_dir_pretty=None,
            wal_breached_warn=None,
            metric_errors=["wal_dir: InsufficientPrivilege"],
        ),
    ):
        resp = client.get("/system/postgres-health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["wal_dir_bytes"] is None
    assert body["wal_breached_warn"] is None
    assert body["metric_errors"] == ["wal_dir: InsufficientPrivilege"]
    # Other metrics unaffected
    assert body["db_size_bytes"] is not None


def test_endpoint_returns_503_on_real_psycopg_connect_failure() -> None:
    """Codex 2 MED #2 regression: patch the underlying
    `psycopg.connect` not the service helper, so the test exercises
    the real production fail-closed path (the service's
    connection-open code raises → endpoint 503)."""
    with patch(
        "app.services.postgres_health.psycopg.connect",
        side_effect=psycopg.OperationalError("connection refused"),
    ):
        resp = client.get("/system/postgres-health")
    assert resp.status_code == 503


class TestAuthGate:
    """The conftest globally overrides `require_session_or_service_token`
    so most tests bypass auth. Reinstall it for this test class to
    prove the endpoint inherits the router-level auth dependency.
    Capture/restore pattern from test_api_system.py."""

    def setup_method(self) -> None:
        self._prior = app.dependency_overrides.get(require_session_or_service_token)
        app.dependency_overrides.pop(require_session_or_service_token, None)
        # The auth dep itself takes `conn: Depends(get_conn)`; FastAPI
        # evaluates it before reaching the auth function body, so the
        # conn dep must be overridden even though the unauth path
        # raises 401 before touching the DB.
        mock_conn = MagicMock()

        def _gen() -> Iterator[MagicMock]:
            yield mock_conn

        app.dependency_overrides[get_conn] = _gen

    def teardown_method(self) -> None:
        if self._prior is not None:
            app.dependency_overrides[require_session_or_service_token] = self._prior
        else:
            app.dependency_overrides.pop(require_session_or_service_token, None)
        app.dependency_overrides.pop(get_conn, None)

    def test_postgres_health_requires_auth(self) -> None:
        # Patch settings to force the fail-closed branch without
        # depending on env config.
        with patch("app.api.auth.settings") as mock_settings:
            mock_settings.service_token = None
            mock_settings.session_cookie_name = "ebull_session"
            mock_settings.session_idle_timeout_minutes = 60
            resp = client.get("/system/postgres-health")
        assert resp.status_code == 401
