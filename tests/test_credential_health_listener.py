"""Tests for the credential-health LISTEN/NOTIFY listener (#976 / #974/B).

Spec: docs/superpowers/specs/2026-05-06-credential-health-precondition-design.md.

Coverage:
  * Cache fail-safe: pre-init reads return MISSING.
  * scan_all_operators: returns the right (operator, env) -> health map.
  * Listener startup: full scan populates the cache, sets initialized.
  * Listener startup retry: failing scans back off, final success
    flips initialized.
  * Listener notify path: a NOTIFY arriving on the channel triggers a
    re-read of the named operator's health and updates the cache.
  * Listener poll fallback: a dropped notify is recovered by the 5s
    re-scan within at most one cycle.
  * Stop event: listener returns cleanly when stop_event is set.
"""

from __future__ import annotations

import json
import os
import threading
import time
from collections.abc import Iterator
from typing import Any
from unittest.mock import patch
from uuid import UUID, uuid4

import psycopg
import psycopg.rows
import pytest
from psycopg_pool import ConnectionPool

from app.jobs.credential_health_listener import (
    INITIAL_SCAN_BACKOFF_S,
    NOTIFY_BLOCK_TIMEOUT_S,
    POLL_INTERVAL_S,
    listener_loop,
)
from app.security import secrets_crypto
from app.services.credential_health import (
    NOTIFY_CHANNEL,
    CredentialHealth,
)
from app.services.credential_health_cache import (
    CredentialHealthCache,
    scan_all_operators,
)
from tests.fixtures.ebull_test_db import (
    ebull_test_conn,  # noqa: F401
    test_database_url,
)


@pytest.fixture(autouse=True)
def _key() -> Iterator[None]:
    secrets_crypto.set_active_key(os.urandom(32))
    yield
    secrets_crypto._reset_for_tests()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _seed_pair_committed(
    *,
    api_state: str,
    user_state: str,
) -> UUID:
    """Insert an operator + both label rows. Uses its own connection so
    the row state survives across the test fixture's tx boundary."""
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
    return op_id


# ---------------------------------------------------------------------------
# Cache fail-safe pre-init
# ---------------------------------------------------------------------------


class TestCredentialHealthCachePreInit:
    def test_pre_init_returns_missing(self) -> None:
        """Until set_initial_scan completes, every read returns MISSING."""
        cache = CredentialHealthCache()
        assert cache.is_initialized() is False
        assert cache.get(operator_id=uuid4()) == CredentialHealth.MISSING

    def test_post_init_returns_missing_for_unknown_operator(self) -> None:
        """An operator absent from the cache map still returns MISSING."""
        cache = CredentialHealthCache()
        cache.set_initial_scan({})
        assert cache.is_initialized() is True
        assert cache.get(operator_id=uuid4()) == CredentialHealth.MISSING

    def test_post_init_returns_known_value(self) -> None:
        op_id = uuid4()
        cache = CredentialHealthCache()
        cache.set_initial_scan({(op_id, "demo"): CredentialHealth.VALID})
        assert cache.get(operator_id=op_id) == CredentialHealth.VALID

    def test_upsert_updates_single_operator(self) -> None:
        op_a = uuid4()
        op_b = uuid4()
        cache = CredentialHealthCache()
        cache.set_initial_scan({(op_a, "demo"): CredentialHealth.VALID, (op_b, "demo"): CredentialHealth.VALID})
        cache.upsert(operator_id=op_a, environment="demo", health=CredentialHealth.REJECTED)
        assert cache.get(operator_id=op_a) == CredentialHealth.REJECTED
        assert cache.get(operator_id=op_b) == CredentialHealth.VALID

    def test_replace_replaces_entire_map(self) -> None:
        op_a = uuid4()
        op_b = uuid4()
        cache = CredentialHealthCache()
        cache.set_initial_scan({(op_a, "demo"): CredentialHealth.VALID})
        cache.replace({(op_b, "demo"): CredentialHealth.UNTESTED})
        assert cache.get(operator_id=op_a) == CredentialHealth.MISSING
        assert cache.get(operator_id=op_b) == CredentialHealth.UNTESTED


# ---------------------------------------------------------------------------
# scan_all_operators (DB-backed)
# ---------------------------------------------------------------------------


def _open_test_pool() -> ConnectionPool[psycopg.Connection[Any]]:
    """Open a small pool against the test DB. Caller closes."""
    return ConnectionPool(test_database_url(), min_size=1, max_size=2, open=True)


@pytest.mark.integration
class TestScanAllOperators:
    def test_returns_aggregate_per_operator(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        del ebull_test_conn  # keep fixture alive for truncate; use own pool
        op_a = _seed_pair_committed(api_state="valid", user_state="valid")
        op_b = _seed_pair_committed(api_state="rejected", user_state="valid")

        pool = _open_test_pool()
        thread: threading.Thread | None = None
        try:
            result = scan_all_operators(pool)
        finally:
            pool.close()

        assert result.get((op_a, "demo")) == CredentialHealth.VALID
        assert result.get((op_b, "demo")) == CredentialHealth.REJECTED

    def test_excludes_revoked_only_operators(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        del ebull_test_conn
        # Seed an operator whose only rows are revoked.
        op_id = uuid4()
        url = test_database_url()
        with psycopg.connect(url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO operators (operator_id, username, password_hash) VALUES (%s, %s, %s)",
                    (op_id, f"op-{op_id.hex[:8]}", "argon2:dummy"),
                )
                cur.execute(
                    """
                    INSERT INTO broker_credentials
                        (id, operator_id, provider, label, environment,
                         ciphertext, last_four, key_version, health_state, revoked_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    """,
                    (uuid4(), op_id, "etoro", "api_key", "demo", b"\x00" * 32, "abcd", 1, "valid"),
                )
            conn.commit()

        pool = _open_test_pool()
        thread: threading.Thread | None = None
        try:
            result = scan_all_operators(pool)
        finally:
            pool.close()

        # Operator with no non-revoked rows is absent from the scan result.
        assert (op_id, "demo") not in result


# ---------------------------------------------------------------------------
# Listener startup + initial scan
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestListenerStartup:
    def test_initial_scan_populates_cache(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        del ebull_test_conn
        op_id = _seed_pair_committed(api_state="valid", user_state="valid")

        cache = CredentialHealthCache()
        stop_event = threading.Event()
        pool = _open_test_pool()
        thread: threading.Thread | None = None
        try:
            thread = threading.Thread(
                target=listener_loop,
                kwargs={
                    "cache": cache,
                    "pool": pool,
                    "stop_event": stop_event,
                },
                daemon=True,
            )
            thread.start()

            # Wait up to 3s for initial scan to complete.
            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline and not cache.is_initialized():
                time.sleep(0.05)

            assert cache.is_initialized()
            assert cache.get(operator_id=op_id) == CredentialHealth.VALID
        finally:
            stop_event.set()
            if thread is not None:
                thread.join(timeout=5.0)
            pool.close()

    def test_initial_scan_retries_on_failure(self) -> None:
        """A failing scan_all_operators backs off and retries until success."""
        cache = CredentialHealthCache()
        stop_event = threading.Event()
        # Replace the pool with a Mock that fails the first time, then
        # succeeds. We bypass the real DB to keep the test fast.
        attempt_count = [0]
        op_id = uuid4()

        def fake_scan(_pool: Any) -> dict[tuple[UUID, str], CredentialHealth]:
            attempt_count[0] += 1
            if attempt_count[0] < 2:
                raise RuntimeError("simulated scan failure")
            return {(op_id, "demo"): CredentialHealth.UNTESTED}

        # Tighten backoff for the test to keep wall-clock low.
        with patch.object(
            __import__("app.jobs.credential_health_listener", fromlist=["scan_all_operators"]),
            "scan_all_operators",
            side_effect=fake_scan,
        ):
            with patch(
                "app.jobs.credential_health_listener.INITIAL_SCAN_BACKOFF_S",
                (0.05, 0.1),
            ):
                # Provide a no-op listen-conn factory so the LISTEN
                # loop never opens a connection. Stop the event right
                # after init so we don't enter the LISTEN loop.
                def fake_factory() -> Any:
                    raise RuntimeError("LISTEN should not be reached in this test")

                thread = threading.Thread(
                    target=listener_loop,
                    kwargs={
                        "cache": cache,
                        "pool": None,  # type: ignore[arg-type]
                        "stop_event": stop_event,
                        "listen_conn_factory": fake_factory,
                    },
                    daemon=True,
                )
                thread.start()

                deadline = time.monotonic() + 3.0
                while time.monotonic() < deadline and not cache.is_initialized():
                    time.sleep(0.05)

                assert cache.is_initialized()
                assert attempt_count[0] >= 2
                # Stop before the LISTEN loop breaks.
                stop_event.set()
                thread.join(timeout=5.0)


# ---------------------------------------------------------------------------
# Listener constants — pinned values
# ---------------------------------------------------------------------------


class TestListenerConstants:
    def test_initial_scan_backoff_locked(self) -> None:
        """Spec-locked retry sequence."""
        assert INITIAL_SCAN_BACKOFF_S == (1.0, 2.0, 5.0, 10.0, 30.0)

    def test_poll_interval_locked(self) -> None:
        assert POLL_INTERVAL_S == 5.0

    def test_notify_block_timeout_lt_poll(self) -> None:
        """Notify block must be shorter than the poll interval so the
        poll fallback always gets a chance to run."""
        assert NOTIFY_BLOCK_TIMEOUT_S < POLL_INTERVAL_S


# ---------------------------------------------------------------------------
# Listener notify + poll-fallback paths (integration)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestListenerNotifyAndPoll:
    def test_notify_updates_cache(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        """Notify on the channel triggers a re-read and cache update."""
        del ebull_test_conn
        op_id = _seed_pair_committed(api_state="untested", user_state="untested")

        cache = CredentialHealthCache()
        stop_event = threading.Event()
        pool = _open_test_pool()
        thread: threading.Thread | None = None
        try:
            thread = threading.Thread(
                target=listener_loop,
                kwargs={"cache": cache, "pool": pool, "stop_event": stop_event},
                daemon=True,
            )
            thread.start()

            # Wait for initial scan.
            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline and not cache.is_initialized():
                time.sleep(0.05)
            assert cache.is_initialized()
            assert cache.get(operator_id=op_id) == CredentialHealth.UNTESTED

            # Mutate row state directly to VALID (bypass the helper to
            # isolate the notify path) and emit the channel notify.
            url = test_database_url()
            with psycopg.connect(url, autocommit=True) as sender:
                with sender.cursor() as cur:
                    cur.execute(
                        "UPDATE broker_credentials SET health_state = 'valid' WHERE operator_id = %s",
                        (op_id,),
                    )
                    payload = json.dumps(
                        {
                            "operator_id": str(op_id),
                            "provider": "etoro",
                            "environment": "demo",
                            "old_aggregate": "untested",
                            "new_aggregate": "valid",
                            "at": "2026-05-06T00:00:00Z",
                        }
                    )
                    cur.execute(
                        "SELECT pg_notify(%s, %s)",
                        (NOTIFY_CHANNEL, payload),
                    )

            # Wait up to 3s for the listener to observe the notify.
            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline:
                if cache.get(operator_id=op_id) == CredentialHealth.VALID:
                    break
                time.sleep(0.05)

            assert cache.get(operator_id=op_id) == CredentialHealth.VALID
        finally:
            stop_event.set()
            if thread is not None:
                thread.join(timeout=5.0)
            pool.close()

    def test_poll_fallback_recovers_dropped_notify(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        """Mutate row directly with NO notify; poll fallback re-scans
        within POLL_INTERVAL_S and updates the cache."""
        del ebull_test_conn
        op_id = _seed_pair_committed(api_state="valid", user_state="valid")

        cache = CredentialHealthCache()
        stop_event = threading.Event()
        pool = _open_test_pool()
        thread: threading.Thread | None = None
        try:
            # Tighten the poll interval to keep test fast.
            with patch("app.jobs.credential_health_listener.POLL_INTERVAL_S", 0.5):
                thread = threading.Thread(
                    target=listener_loop,
                    kwargs={"cache": cache, "pool": pool, "stop_event": stop_event},
                    daemon=True,
                )
                thread.start()

                deadline = time.monotonic() + 3.0
                while time.monotonic() < deadline and not cache.is_initialized():
                    time.sleep(0.05)
                assert cache.get(operator_id=op_id) == CredentialHealth.VALID

                # Mutate row WITHOUT emitting any notify.
                url = test_database_url()
                with psycopg.connect(url, autocommit=True) as sender:
                    with sender.cursor() as cur:
                        cur.execute(
                            "UPDATE broker_credentials SET health_state = 'rejected' WHERE operator_id = %s",
                            (op_id,),
                        )

                # Poll fallback should pick this up within ~0.5s.
                deadline = time.monotonic() + 3.0
                while time.monotonic() < deadline:
                    if cache.get(operator_id=op_id) == CredentialHealth.REJECTED:
                        break
                    time.sleep(0.05)

                assert cache.get(operator_id=op_id) == CredentialHealth.REJECTED
        finally:
            stop_event.set()
            if thread is not None:
                thread.join(timeout=5.0)
            pool.close()
