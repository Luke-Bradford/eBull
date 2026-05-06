"""LISTEN/NOTIFY thread for the credential-health cache (#976 / #974/B).

Owns one dedicated autocommit ``psycopg.Connection`` running
``LISTEN ebull_credential_health`` plus a 5s poll fallback so a
NOTIFY dropped during reconnection still surfaces within 5s.

Differences from ``app/jobs/listener.py`` (the job-request listener):

  * No durable event table — there is no equivalent of
    ``pending_job_requests`` for credential health (Codex r1.10 in
    spec). Subscribers MUST poll DB truth on startup; the channel is
    a wake-up only.
  * Startup full-scan with retry-with-backoff (1s, 2s, 5s, 10s, 30s
    cap) until the initial scan succeeds — until then the cache
    reports MISSING for every read so consumers fail-safe.
  * On notify: re-read DB truth for the operator carried in the
    payload (don't trust the payload's aggregate alone).
  * On 5s poll tick: full-table re-scan to recover dropped notifies
    and to detect operator deletions that wouldn't otherwise emit.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Callable
from typing import Any
from uuid import UUID

import psycopg
import psycopg.sql
from psycopg_pool import ConnectionPool

from app.config import settings
from app.services.credential_health import (
    NOTIFY_CHANNEL,
    get_operator_credential_health,
)
from app.services.credential_health_cache import (
    CredentialHealthCache,
    scan_all_operators,
)

logger = logging.getLogger(__name__)


# Initial-scan retry sequence. A clean restart should land in <2s; any
# longer means Postgres warm-up or pool not yet open. The 30s cap is
# generous for prolonged outages — consumers see MISSING the whole
# time, which means no creds-using work runs (fail-safe).
INITIAL_SCAN_BACKOFF_S: tuple[float, ...] = (1.0, 2.0, 5.0, 10.0, 30.0)

# Poll interval for the safety-net full re-scan. Independent of the
# NOTIFY-driven path so a dropped notify (network blip, reconnect
# window) surfaces within this interval.
POLL_INTERVAL_S: float = 5.0

# Notify-blocking timeout. The LISTEN loop polls notifies in this
# window then falls through to the poll-fallback re-scan. Short enough
# to keep the loop responsive to stop_event.
NOTIFY_BLOCK_TIMEOUT_S: float = 1.0


def listener_loop(
    *,
    cache: CredentialHealthCache,
    pool: ConnectionPool[psycopg.Connection[Any]],
    stop_event: threading.Event,
    listen_conn_factory: Callable[[], psycopg.Connection[Any]] | None = None,
) -> None:
    """Run startup scan + LISTEN/NOTIFY loop until ``stop_event`` is set.

    ``listen_conn_factory`` is overridable for tests — the default
    opens a fresh autocommit psycopg.Connection against
    ``settings.database_url``.
    """
    factory = listen_conn_factory or _default_listen_conn_factory

    # 1. Initial scan with retry-with-backoff. Until this succeeds the
    #    cache reports MISSING (fail-safe).
    if not _run_initial_scan(cache=cache, pool=pool, stop_event=stop_event):
        logger.info("credential_health_listener: stop requested before initial scan")
        return

    # 2. LISTEN + 5s poll loop. Re-enter on connection death; cache
    #    state is preserved across reconnects.
    while not stop_event.is_set():
        try:
            conn = factory()
        except Exception:
            logger.exception("credential_health_listener: failed to open LISTEN connection")
            if stop_event.wait(timeout=POLL_INTERVAL_S):
                return
            continue

        try:
            _run_listen_loop(conn=conn, cache=cache, pool=pool, stop_event=stop_event)
        except Exception:
            logger.exception("credential_health_listener: inner loop crashed; reconnecting")
        finally:
            try:
                conn.close()
            except Exception:
                logger.debug("listener close raised", exc_info=True)

        if stop_event.wait(timeout=1.0):
            return


def _default_listen_conn_factory() -> psycopg.Connection[Any]:
    """Open a fresh autocommit connection for LISTEN.

    autocommit is required so LISTEN takes effect immediately; the
    same connection is held for the life of the inner loop and closed
    on exit so a Postgres-side reset releases all queued notifies.
    """
    return psycopg.connect(settings.database_url, autocommit=True)


def _run_initial_scan(
    *,
    cache: CredentialHealthCache,
    pool: ConnectionPool[psycopg.Connection[Any]],
    stop_event: threading.Event,
) -> bool:
    """Run the initial full-table scan with retry-with-backoff.

    Returns True iff the scan eventually succeeded. Returns False if
    stop_event was set before any successful scan — caller should
    return without entering the LISTEN loop.

    Cache stays in pre-initialized state (every read returns MISSING)
    until this function returns True.
    """
    attempt = 0
    while not stop_event.is_set():
        try:
            populated = scan_all_operators(pool)
            cache.set_initial_scan(populated)
            return True
        except Exception:
            wait = INITIAL_SCAN_BACKOFF_S[min(attempt, len(INITIAL_SCAN_BACKOFF_S) - 1)]
            logger.exception(
                "credential_health_listener: initial scan failed (attempt %d); retrying in %ss",
                attempt + 1,
                wait,
            )
            attempt += 1
            if stop_event.wait(timeout=wait):
                return False
    return False


def _run_listen_loop(
    *,
    conn: psycopg.Connection[Any],
    cache: CredentialHealthCache,
    pool: ConnectionPool[psycopg.Connection[Any]],
    stop_event: threading.Event,
) -> None:
    """LISTEN + 5s poll inner loop. Returns when stop_event set or conn dies."""
    with conn.cursor() as cur:
        cur.execute(psycopg.sql.SQL("LISTEN {}").format(psycopg.sql.Identifier(NOTIFY_CHANNEL)))
    logger.info("credential_health_listener: LISTEN %s active", NOTIFY_CHANNEL)

    last_poll_at = 0.0
    while not stop_event.is_set():
        # NOTIFY-driven path. Block up to NOTIFY_BLOCK_TIMEOUT_S for
        # any notifies, drain everything that arrived, then fall
        # through to the poll fallback.
        for notify in conn.notifies(timeout=NOTIFY_BLOCK_TIMEOUT_S, stop_after=64):
            _handle_notify(notify=notify, cache=cache, pool=pool)

        # Poll fallback: once per POLL_INTERVAL_S, re-scan the full
        # table and replace the cache. Catches any dropped notify
        # within at most this interval.
        now = time.monotonic()
        if now - last_poll_at >= POLL_INTERVAL_S:
            last_poll_at = now
            try:
                populated = scan_all_operators(pool)
                cache.replace(populated)
            except Exception:
                logger.exception("credential_health_listener: poll-fallback scan failed; cache state preserved")


def _handle_notify(
    *,
    notify: Any,
    cache: CredentialHealthCache,
    pool: ConnectionPool[psycopg.Connection[Any]],
) -> None:
    """Re-read DB truth for the operator carried in the notify payload.

    Don't trust the payload's aggregate value alone — by the time the
    notify is delivered, additional transitions may have been recorded.
    The notify is a wake-up; the DB is the source of truth.
    """
    try:
        payload_obj = json.loads(notify.payload)
        operator_id = UUID(str(payload_obj["operator_id"]))
        # provider is in the payload but v1 only supports etoro.
        environment_hint = payload_obj.get("environment", "demo")
    except TypeError, ValueError, KeyError:
        logger.warning(
            "credential_health_listener: ignoring malformed notify payload: %r",
            notify.payload,
        )
        return

    # The notify payload only carries the (operator, provider) pair,
    # not environment. v1 hardcodes demo; when real arrives we'll need
    # to widen the channel payload. For now we rescan both env keys
    # known to be in use — currently just demo. The poll fallback
    # catches any drift.
    try:
        with pool.connection() as conn:
            health = get_operator_credential_health(
                conn,
                operator_id=operator_id,
                environment=environment_hint,
            )
    except Exception:
        logger.exception(
            "credential_health_listener: re-read for operator %s failed; cache entry preserved",
            operator_id,
        )
        return

    cache.upsert(
        operator_id=operator_id,
        environment=environment_hint,
        health=health,
    )
