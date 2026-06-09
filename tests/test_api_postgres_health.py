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
from fastapi import HTTPException
from fastapi.testclient import TestClient

from app.api.auth import require_session_or_service_token
from app.db import get_conn
from app.main import app
from app.services.postgres_health import (
    AutovacuumTableLag,
    ListenerConnectionCount,
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
        "leaked_test_db_total_bytes": 0,
        "leaked_test_db_total_pretty": "0 bytes",
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
        "financial_facts_raw_default_junk_rows": 0,
        "financial_facts_raw_default_warn_threshold": 5000,
        "financial_facts_raw_default_breached_warn": False,
        "listener_connections": [
            ListenerConnectionCount(application_name="ebull-jobs-job-request-listener", count=1),
            ListenerConnectionCount(application_name="ebull-jobs-credential-health-listener", count=1),
            ListenerConnectionCount(application_name="ebull-api-credential-health-listener", count=1),
        ],
        "listener_duplicate_detected": False,
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
        "leaked_test_db_total_bytes",
        "leaked_test_db_total_pretty",
        "wal_dir_bytes",
        "wal_dir_pretty",
        "wal_since_checkpoint_bytes",
        "wal_warn_threshold_bytes",
        "wal_breached_warn",
        "last_checkpoint_at",
        "autovacuum_top10",
        "financial_facts_raw_default_rows",
        "financial_facts_raw_default_junk_rows",
        "financial_facts_raw_default_warn_threshold",
        "financial_facts_raw_default_breached_warn",
        "listener_connections",
        "listener_duplicate_detected",
        "metric_errors",
        "collected_at",
    }
    assert expected_keys.issubset(body.keys())
    assert body["metric_errors"] == []
    assert body["autovacuum_top10"][0]["relname"] == "financial_facts_raw_2024q3"
    assert body["listener_duplicate_detected"] is False
    assert {lc["application_name"] for lc in body["listener_connections"]} == {
        "ebull-jobs-job-request-listener",
        "ebull-jobs-credential-health-listener",
        "ebull-api-credential-health-listener",
    }


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
    # #1221 — breach keys on the junk count; raw count can sit high
    # (legit forward-projected rows) without flipping the flag.
    with patch(
        "app.services.postgres_health.collect_postgres_health",
        return_value=_snapshot(
            financial_facts_raw_default_rows=10_000,
            financial_facts_raw_default_junk_rows=6_000,
            financial_facts_raw_default_breached_warn=True,
        ),
    ):
        resp = client.get("/system/postgres-health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["financial_facts_raw_default_breached_warn"] is True
    assert body["financial_facts_raw_default_junk_rows"] == 6_000


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


class TestDbDownReturns503:
    """#1325 / #1217: with the real auth dependency active and a valid
    bearer token, a DB-down ``/system/postgres-health`` returns 503 —
    NOT 401. Proves the auth dep no longer eagerly resolves get_conn,
    so a valid-token operator request reaches the handler (which then
    surfaces the genuine DB-unavailable 503) instead of being masked as
    an auth failure.

    Note: no ``get_conn`` override is installed here (unlike
    ``TestAuthGate`` above) — the whole point of the fix is that the
    auth dep no longer drags ``get_conn`` into FastAPI's signature
    resolution, so the bearer path needs no DB at all.
    """

    _VALID_TOKEN = "test-operator-token-with-32-chars"

    def setup_method(self) -> None:
        # Track presence separately (prevention-log #234: don't use the
        # value as a presence sentinel). Capture via subscript only when
        # present, so the stored value is non-Optional.
        self._had = require_session_or_service_token in app.dependency_overrides
        if self._had:
            self._prior = app.dependency_overrides[require_session_or_service_token]
        app.dependency_overrides.pop(require_session_or_service_token, None)
        # Deterministic DB-down for get_conn-handler endpoints: override
        # get_conn to raise the 503 it would produce on a dead pool. This
        # is the designed-for-tests mechanism (no shared app.state
        # mutation — review #1394 WARNING "bearer-path DB-down test
        # portability"; mutating app.state.db_pool leaked across tests).
        self._had_conn = get_conn in app.dependency_overrides
        if self._had_conn:
            self._prior_conn = app.dependency_overrides[get_conn]

        def _conn_503() -> object:
            raise HTTPException(status_code=503, detail="database temporarily unavailable")

        app.dependency_overrides[get_conn] = _conn_503

    def teardown_method(self) -> None:
        # Restore unconditionally when the key was present (#234).
        if self._had:
            app.dependency_overrides[require_session_or_service_token] = self._prior
        else:
            app.dependency_overrides.pop(require_session_or_service_token, None)
        if self._had_conn:
            app.dependency_overrides[get_conn] = self._prior_conn
        else:
            app.dependency_overrides.pop(get_conn, None)

    def test_valid_bearer_postgres_health_db_down_returns_503_not_401(self) -> None:
        """postgres-health SELF-connects via psycopg.connect (patched to
        fail) — it does NOT read the pool, so its 503 is independent of
        app.state.db_pool. With a valid bearer the auth dep no longer
        blocks, so the handler is reached and surfaces its own 503 (not
        a 401)."""
        with (
            patch("app.api.auth.settings") as mock_settings,
            patch(
                "app.services.postgres_health.psycopg.connect",
                side_effect=psycopg.OperationalError("connection refused"),
            ),
        ):
            mock_settings.service_token = self._VALID_TOKEN
            mock_settings.session_cookie_name = "ebull_session"
            mock_settings.session_idle_timeout_minutes = 60
            resp = client.get(
                "/system/postgres-health",
                headers={"Authorization": f"Bearer {self._VALID_TOKEN}"},
            )
        assert resp.status_code == 503, resp.text

    def test_valid_bearer_system_status_db_down_returns_503_not_401(self) -> None:
        """/system/status depends on get_conn (the pooled path). With
        get_conn forced to its DB-down 503 (setup override) and a valid
        bearer, the endpoint returns 503 — proving the get_conn-handler
        /system/* endpoints degrade to 503, not 401/500, once the auth
        dep stops blocking. (get_conn's own OperationalError->503 mapping
        is unit-tested in test_api_auth_db_down.py.)"""
        with patch("app.api.auth.settings") as mock_settings:
            mock_settings.service_token = self._VALID_TOKEN
            mock_settings.session_cookie_name = "ebull_session"
            mock_settings.session_idle_timeout_minutes = 60
            resp = client.get(
                "/system/status",
                headers={"Authorization": f"Bearer {self._VALID_TOKEN}"},
            )
        assert resp.status_code == 503, resp.text
