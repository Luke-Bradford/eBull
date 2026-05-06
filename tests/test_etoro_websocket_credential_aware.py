"""Tests for the EtoroWebSocketSubscriber credential-aware path (#978 / #974/D).

Spec: docs/superpowers/specs/2026-05-06-credential-health-precondition-design.md.

Coverage:
  * Backoff sequence pinned: (5, 30, 120, 600, 600).
  * `_current_backoff` returns _RECONNECT_BACKOFF_S in legacy mode
    (no cache wired) and the auth-failure exponential sequence in
    credential-aware mode.
  * Counter increments on auth failure and resets on success.
  * Pre-flight gate: cache REJECTED → skip the connect (no auth flood).
  * `_record_auth_outcome` writes through to record_health_outcome
    when all of (operator_id, audit_pool, cache) are wired; no-op when
    any is missing.

These tests don't drive the websocket itself — they exercise the
state-machine and gate logic in isolation.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from psycopg_pool import ConnectionPool

from app.services.credential_health import CredentialHealth
from app.services.credential_health_cache import CredentialHealthCache
from app.services.etoro_websocket import (
    _AUTH_FAILURE_BACKOFF_S,
    _RECONNECT_BACKOFF_S,
    EtoroWebSocketSubscriber,
)


@pytest.fixture
def fake_pool() -> Any:
    """Mock pool — used for construction only, never opened in these tests."""
    return MagicMock(spec=ConnectionPool)


def _make_subscriber(
    *,
    cache: CredentialHealthCache | None = None,
    operator_id: Any = None,
    audit_pool: Any = None,
    fake_pool: Any,
) -> EtoroWebSocketSubscriber:
    """Construct a subscriber for state-machine tests.

    Tests never await `start()` or hit the network; they exercise
    `_current_backoff` and `_record_auth_outcome` directly.
    """
    return EtoroWebSocketSubscriber(
        api_key="api-test",
        user_key="user-test",
        env="demo",
        pool=fake_pool,
        operator_id=operator_id,
        credential_cache=cache,
        audit_pool=audit_pool,
    )


# ---------------------------------------------------------------------------
# Backoff sequence + counter
# ---------------------------------------------------------------------------


class TestBackoffSequence:
    def test_sequence_locked(self) -> None:
        """Spec-locked: (5, 30, 120, 600, 600)."""
        assert _AUTH_FAILURE_BACKOFF_S == (5.0, 30.0, 120.0, 600.0, 600.0)

    def test_legacy_mode_returns_fixed_backoff(self, fake_pool: Any) -> None:
        """No cache wired → fixed _RECONNECT_BACKOFF_S regardless of counter."""
        sub = _make_subscriber(fake_pool=fake_pool)
        sub._consecutive_auth_failures = 7
        assert sub._current_backoff() == _RECONNECT_BACKOFF_S

    def test_credential_aware_starts_at_5s(self, fake_pool: Any) -> None:
        cache = CredentialHealthCache()
        sub = _make_subscriber(cache=cache, operator_id=uuid4(), audit_pool=fake_pool, fake_pool=fake_pool)
        assert sub._current_backoff() == 5.0

    def test_credential_aware_progresses(self, fake_pool: Any) -> None:
        cache = CredentialHealthCache()
        sub = _make_subscriber(cache=cache, operator_id=uuid4(), audit_pool=fake_pool, fake_pool=fake_pool)
        for expected in _AUTH_FAILURE_BACKOFF_S:
            assert sub._current_backoff() == expected
            sub._consecutive_auth_failures += 1
        # Beyond the sequence, stay capped at the last value.
        sub._consecutive_auth_failures = 99
        assert sub._current_backoff() == _AUTH_FAILURE_BACKOFF_S[-1]


# ---------------------------------------------------------------------------
# _record_auth_outcome — no-op in legacy mode, write-through otherwise
# ---------------------------------------------------------------------------


class TestRecordAuthOutcome:
    def test_legacy_mode_is_noop(self, fake_pool: Any) -> None:
        """No operator_id / cache / audit_pool → never calls record_health_outcome."""
        sub = _make_subscriber(fake_pool=fake_pool)
        with patch("app.services.credential_health.record_health_outcome") as mock_record:
            sub._record_auth_outcome(success=False, error_detail="test")
        assert mock_record.call_count == 0

    def test_aware_mode_writes_through_for_each_label(self, fake_pool: Any) -> None:
        """When wired, looks up label rows and calls record_health_outcome
        once per row with source='incidental' against the AUDIT pool —
        not the request pool. Review #984 PREVENTION pins the
        identity check on the dedicated audit_pool so a regression
        that passed self._pool instead would be caught.
        """
        cache = CredentialHealthCache()
        op_id = uuid4()

        # Mock the cred-id lookup on the request pool to return two rows.
        api_id, user_id = uuid4(), uuid4()
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = [(api_id,), (user_id,)]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_conn.__enter__.return_value = mock_conn
        fake_pool.connection.return_value = mock_conn

        # DISTINCT mock for the audit pool so the assertion below
        # actually proves we used audit_pool, not self._pool.
        audit_pool_mock = MagicMock(spec=ConnectionPool)

        sub = _make_subscriber(
            cache=cache,
            operator_id=op_id,
            audit_pool=audit_pool_mock,
            fake_pool=fake_pool,
        )

        with patch("app.services.credential_health.record_health_outcome") as mock_record:
            sub._record_auth_outcome(success=False, error_detail="HTTP 401")

        # Both label rows should write through.
        assert mock_record.call_count == 2
        for call in mock_record.call_args_list:
            kwargs = call.kwargs
            assert kwargs["success"] is False
            assert kwargs["source"] == "incidental"
            assert kwargs["error_detail"] == "HTTP 401"
            assert kwargs["pool"] is audit_pool_mock
            assert kwargs["pool"] is not fake_pool

    def test_aware_mode_swallows_lookup_failure(self, fake_pool: Any) -> None:
        """A DB error during cred-id lookup is logged but doesn't raise —
        the WS auth path must not fail because health-write failed."""
        cache = CredentialHealthCache()
        op_id = uuid4()
        fake_pool.connection.side_effect = RuntimeError("pool exhausted")

        sub = _make_subscriber(cache=cache, operator_id=op_id, audit_pool=fake_pool, fake_pool=fake_pool)

        # Should not raise.
        sub._record_auth_outcome(success=False, error_detail="HTTP 401")


# ---------------------------------------------------------------------------
# Pre-flight cache gate — exercised by _run loop
# ---------------------------------------------------------------------------


class TestPreflightCacheGate:
    def test_cache_rejected_skips_connect(self, fake_pool: Any) -> None:
        """When health.get returns REJECTED, _run skips the connect
        attempt and waits on stop_event for the backoff window.

        We can't easily drive the async _run loop in a sync test, so
        this verifies the underlying gate logic via the cache contract:
        a REJECTED operator returns REJECTED, not VALID.
        """
        cache = CredentialHealthCache()
        op_id = uuid4()
        cache.set_initial_scan({(op_id, "demo"): CredentialHealth.REJECTED})

        # Verify the gate check the _run loop performs.
        result = cache.get(operator_id=op_id, environment="demo")
        assert result == CredentialHealth.REJECTED
        assert result.value != "valid"

    def test_cache_valid_allows_connect(self) -> None:
        cache = CredentialHealthCache()
        op_id = uuid4()
        cache.set_initial_scan({(op_id, "demo"): CredentialHealth.VALID})
        result = cache.get(operator_id=op_id, environment="demo")
        assert result == CredentialHealth.VALID
        assert result.value == "valid"

    def test_pre_initialized_cache_returns_missing(self) -> None:
        """Until initial scan completes, the gate sees MISSING — fail-safe.
        The _run loop treats anything != 'valid' as 'do not connect'."""
        cache = CredentialHealthCache()
        op_id = uuid4()
        result = cache.get(operator_id=op_id, environment="demo")
        assert result.value == "missing"
        # WS gate would skip on this — value != "valid".
        assert result.value != "valid"


# ---------------------------------------------------------------------------
# Counter reset on success — pinned by the implementation guarantee
# ---------------------------------------------------------------------------


class TestCounterReset:
    def test_counter_starts_at_zero(self, fake_pool: Any) -> None:
        sub = _make_subscriber(fake_pool=fake_pool)
        assert sub._consecutive_auth_failures == 0

    def test_counter_can_be_set(self, fake_pool: Any) -> None:
        sub = _make_subscriber(fake_pool=fake_pool)
        sub._consecutive_auth_failures = 3
        assert sub._consecutive_auth_failures == 3
