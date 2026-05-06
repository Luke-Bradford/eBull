"""Tests for the orchestrator credential-health + init-check gates (#977 / #974/C).

Spec: docs/superpowers/specs/2026-05-06-credential-health-precondition-design.md.

Coverage:
  * _credential_health_blocks: returns None when no requires_broker_credential
    emit; returns reason when operator health != VALID; returns None on VALID.
  * _layer_initialization_blocks: returns None when no requires_layer_initialized;
    returns reason when an init dep's INIT_CHECKS predicate is False; returns
    None when the predicate is True.
  * Registry tags: portfolio_sync has requires_layer_initialized=("universe",);
    universe / candles / portfolio_sync have requires_broker_credential=True;
    fundamentals / fx_rates / cost_models do NOT.
  * AUTH_EXPIRED suppression: consecutive_failures + all_layer_histories
    correctly filter rows whose error_category='auth_expired' and
    failed_at < suppress_auth_expired_before.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import psycopg
import psycopg.rows
import pytest

from app.security import secrets_crypto
from app.services.sync_orchestrator.executor import (
    _credential_health_blocks,
    _layer_initialization_blocks,
)
from app.services.sync_orchestrator.layer_failure_history import (
    all_layer_histories,
    consecutive_failures,
)
from app.services.sync_orchestrator.registry import INIT_CHECKS, LAYERS
from app.services.sync_orchestrator.types import LayerPlan
from tests.fixtures.ebull_test_db import (
    ebull_test_conn,  # noqa: F401
    test_database_url,
)


@pytest.fixture(autouse=True)
def _key() -> Iterator[None]:
    secrets_crypto.set_active_key(os.urandom(32))
    yield
    secrets_crypto._reset_for_tests()


@pytest.fixture(autouse=True)
def _patch_db_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force executor's psycopg.connect to use the test DB URL.

    The gate helpers open fresh autocommit connections via
    settings.database_url; without this patch they'd hit the dev DB.
    """
    monkeypatch.setattr("app.services.sync_orchestrator.executor.settings.database_url", test_database_url())


def _make_plan(name: str, emits: tuple[str, ...]) -> LayerPlan:
    """Build a minimal LayerPlan for gate tests."""
    return LayerPlan(
        name=name,
        emits=emits,
        reason="test",
        dependencies=LAYERS[emits[0]].dependencies,
        is_blocking=LAYERS[emits[0]].is_blocking,
        estimated_items=0,
    )


def _seed_operator_with_health(
    *,
    api_state: str,
    user_state: str,
) -> None:
    """Insert a single operator + both labels at the supplied health states."""
    op_id = uuid4()
    url = test_database_url()
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO operators (operator_id, username, password_hash) VALUES (%s, %s, %s)",
                (op_id, f"op-{op_id.hex[:8]}", "argon2:dummy"),
            )
            for label, state in (("api_key", api_state), ("user_key", user_state)):
                cur.execute(
                    """
                    INSERT INTO broker_credentials
                        (id, operator_id, provider, label, environment,
                         ciphertext, last_four, key_version, health_state)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (uuid4(), op_id, "etoro", label, "demo", b"\x00" * 32, "abcd", 1, state),
                )
        conn.commit()


# ---------------------------------------------------------------------------
# Registry tags — locked
# ---------------------------------------------------------------------------


class TestRegistryTags:
    def test_universe_requires_credential(self) -> None:
        assert LAYERS["universe"].requires_broker_credential is True
        assert LAYERS["universe"].requires_layer_initialized == ()

    def test_candles_requires_credential(self) -> None:
        assert LAYERS["candles"].requires_broker_credential is True

    def test_portfolio_sync_requires_credential_and_universe_init(self) -> None:
        assert LAYERS["portfolio_sync"].requires_broker_credential is True
        assert LAYERS["portfolio_sync"].requires_layer_initialized == ("universe",)

    def test_fundamentals_does_not_require_credential(self) -> None:
        # SEC XBRL — no eToro creds needed.
        assert LAYERS["fundamentals"].requires_broker_credential is False

    def test_fx_rates_does_not_require_credential(self) -> None:
        # Frankfurter — no eToro creds.
        assert LAYERS["fx_rates"].requires_broker_credential is False

    def test_cost_models_does_not_require_credential(self) -> None:
        # Re-seeded from existing DB data — no eToro creds.
        assert LAYERS["cost_models"].requires_broker_credential is False

    def test_init_checks_has_universe(self) -> None:
        assert "universe" in INIT_CHECKS
        # Must check `is_tradable = true` per spec.
        assert "is_tradable" in INIT_CHECKS["universe"]


# ---------------------------------------------------------------------------
# _credential_health_blocks
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestCredentialHealthBlocks:
    def test_no_credential_layer_passes(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        """Layers with requires_broker_credential=False are not gated."""
        del ebull_test_conn
        plan = _make_plan("fx_rates", ("fx_rates",))
        result = _credential_health_blocks(plan)
        assert result is None

    def test_no_operator_returns_skip_reason(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        """A credential-using layer with no operator is gated."""
        del ebull_test_conn  # leave fixture truncated; no operator seeded
        plan = _make_plan("universe", ("universe",))
        result = _credential_health_blocks(plan)
        assert result is not None
        assert "operator not configured" in result

    def test_rejected_health_returns_skip_reason(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        del ebull_test_conn
        _seed_operator_with_health(api_state="rejected", user_state="valid")
        plan = _make_plan("universe", ("universe",))
        result = _credential_health_blocks(plan)
        assert result is not None
        assert "rejected" in result

    def test_valid_health_passes(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        del ebull_test_conn
        _seed_operator_with_health(api_state="valid", user_state="valid")
        plan = _make_plan("universe", ("universe",))
        result = _credential_health_blocks(plan)
        assert result is None


# ---------------------------------------------------------------------------
# _layer_initialization_blocks
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestLayerInitializationBlocks:
    def test_no_init_dep_passes(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        """Layer without requires_layer_initialized is not gated."""
        del ebull_test_conn
        plan = _make_plan("universe", ("universe",))
        result = _layer_initialization_blocks(plan)
        assert result is None

    def test_uninitialized_universe_blocks_portfolio_sync(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        """portfolio_sync with empty instruments table → SKIP."""
        del ebull_test_conn  # truncated; instruments empty
        plan = _make_plan("portfolio_sync", ("portfolio_sync",))
        result = _layer_initialization_blocks(plan)
        assert result is not None
        assert "universe" in result and "not yet initialized" in result

    def test_initialized_universe_unblocks_portfolio_sync(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        del ebull_test_conn
        # Seed at least one tradable instrument.
        url = test_database_url()
        with psycopg.connect(url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO instruments
                        (instrument_id, symbol, company_name, is_tradable, currency, instrument_type)
                    VALUES (1, 'AAPL', 'Apple Inc.', TRUE, 'USD', 'stock')
                    """
                )
            conn.commit()

        plan = _make_plan("portfolio_sync", ("portfolio_sync",))
        result = _layer_initialization_blocks(plan)
        assert result is None


# ---------------------------------------------------------------------------
# AUTH_EXPIRED suppression in failure-history queries
# ---------------------------------------------------------------------------


def _seed_progress_row(
    conn: psycopg.Connection[Any],
    *,
    layer_name: str,
    status: str,
    error_category: str | None,
    started_at: datetime,
) -> int:
    """Insert a sync_layer_progress row for failure-history tests.

    sync_run_id is GENERATED ALWAYS so we let Postgres assign it and
    return the id. Each call creates a fresh sync_runs row.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sync_runs (scope, scope_detail, trigger, layers_planned, status)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING sync_run_id
            """,
            ("full", None, "manual", 1, "complete"),
        )
        row = cur.fetchone()
        assert row is not None
        sync_run_id = row[0]
        cur.execute(
            """
            INSERT INTO sync_layer_progress
                (sync_run_id, layer_name, status, error_category, started_at, finished_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (sync_run_id, layer_name, status, error_category, started_at, started_at + timedelta(seconds=5)),
        )
    conn.commit()
    return sync_run_id


@pytest.mark.integration
class TestAuthExpiredSuppression:
    def test_suppression_filters_old_auth_expired_failures(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        """A failed auth_expired row with started_at < suppress_before
        does NOT count toward the streak."""
        # Three auth_expired failures, all old.
        recovery_at = datetime(2026, 5, 6, 12, 0, 0, tzinfo=UTC)
        old = recovery_at - timedelta(hours=1)
        for i in range(3):
            _seed_progress_row(
                ebull_test_conn,
                layer_name="universe",
                status="failed",
                error_category="auth_expired",
                started_at=old + timedelta(minutes=i),
            )

        # Without suppression: streak = 3.
        assert consecutive_failures(ebull_test_conn, "universe", suppress_auth_expired_before=None) == 3
        # With suppression at recovery_at: streak = 0.
        assert (
            consecutive_failures(
                ebull_test_conn,
                "universe",
                suppress_auth_expired_before=recovery_at,
            )
            == 0
        )

    def test_suppression_preserves_new_failures_after_recovery(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        """Failures of any category that happen AFTER recovery still
        count toward the streak."""
        recovery_at = datetime(2026, 5, 6, 12, 0, 0, tzinfo=UTC)
        # Old auth_expired (suppressed).
        _seed_progress_row(
            ebull_test_conn,
            layer_name="universe",
            status="failed",
            error_category="auth_expired",
            started_at=recovery_at - timedelta(hours=1),
        )
        # New rate_limited AFTER recovery (visible).
        _seed_progress_row(
            ebull_test_conn,
            layer_name="universe",
            status="failed",
            error_category="rate_limited",
            started_at=recovery_at + timedelta(minutes=5),
        )

        result = consecutive_failures(
            ebull_test_conn,
            "universe",
            suppress_auth_expired_before=recovery_at,
        )
        assert result == 1  # only the rate_limited failure

    def test_suppression_does_not_filter_new_auth_expired(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        """auth_expired rows AFTER suppress_before remain visible —
        operator entered another bad key after recovery."""
        recovery_at = datetime(2026, 5, 6, 12, 0, 0, tzinfo=UTC)
        # New auth_expired (visible).
        _seed_progress_row(
            ebull_test_conn,
            layer_name="universe",
            status="failed",
            error_category="auth_expired",
            started_at=recovery_at + timedelta(minutes=5),
        )
        result = consecutive_failures(
            ebull_test_conn,
            "universe",
            suppress_auth_expired_before=recovery_at,
        )
        assert result == 1

    def test_all_layer_histories_applies_suppression(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        """Batched helper applies the same suppression as the single-layer one."""
        recovery_at = datetime(2026, 5, 6, 12, 0, 0, tzinfo=UTC)
        _seed_progress_row(
            ebull_test_conn,
            layer_name="universe",
            status="failed",
            error_category="auth_expired",
            started_at=recovery_at - timedelta(hours=1),
        )

        streaks_no_suppress, _ = all_layer_histories(ebull_test_conn, ["universe"])
        streaks_suppress, _ = all_layer_histories(
            ebull_test_conn,
            ["universe"],
            suppress_auth_expired_before=recovery_at,
        )
        assert streaks_no_suppress.get("universe", 0) == 1
        assert streaks_suppress.get("universe", 0) == 0
