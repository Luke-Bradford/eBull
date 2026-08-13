"""Thread-safe operator-credential-health cache (#976 / #974/B).

Per-process in-memory view of operator credential health. Populated
by the listener thread at ``app/jobs/credential_health_listener.py``
via:

  * Initial full-table scan at startup (with retry-with-backoff
    until success).
  * LISTEN/NOTIFY on the ``ebull_credential_health`` channel.
  * 5s poll fallback that re-scans the full table so a dropped
    notify is recovered within at most 5 seconds.

Consumers (orchestrator pre-flight gate at #977, WS subscriber at
#978, admin UI at #979) call ``get`` on the cache and act on the
returned ``CredentialHealth`` value. Until the initial scan
completes, ``get`` returns ``MISSING`` so credential-using work
fail-safes (no jobs run, WS stays disconnected) — see Codex
pre-push r2.8 in the spec.

The cache is process-local. Both the API process and the jobs
process create their own instance during lifespan / startup; they
do not share state. This is intentional: the only durable signal is
the DB, and notifies are wake-ups, not source-of-truth.
"""

from __future__ import annotations

import logging
import threading
from typing import Any
from uuid import UUID

import psycopg
import psycopg.rows
from psycopg_pool import ConnectionPool

from app.services.credential_health import (
    CredentialHealth,
    get_operator_credential_health,
)

logger = logging.getLogger(__name__)


class CredentialHealthCache:
    """Thread-safe operator → CredentialHealth map.

    Read API is non-blocking (single mutex around a dict lookup);
    write API is owned by the listener thread.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cache: dict[tuple[UUID, str], CredentialHealth] = {}
        # Until the listener completes its first full scan, the cache
        # cannot answer "what's this operator's health" honestly. We
        # report MISSING for all reads in this state (fail-safe — no
        # creds-using work runs). Set to True via set_initial_scan.
        self._initialized: bool = False

    # ------------------------------------------------------------------
    # Read API (consumers)
    # ------------------------------------------------------------------

    def get(
        self,
        *,
        operator_id: UUID,
        provider: str = "etoro",
        environment: str = "demo",
    ) -> CredentialHealth:
        """Return the cached operator-level health.

        Returns:
            CredentialHealth — REJECTED / MISSING / UNTESTED / VALID.

        Pre-initialization: returns MISSING regardless of the underlying
        DB state. This is the fail-safe contract — consumers (orchestrator,
        WS subscriber) treat MISSING as "do not run / connect" so a
        slow startup never opens a window where credential-using work
        fires against unknown state.
        """
        del provider  # provider is locked to 'etoro' in v1; key by env only
        with self._lock:
            if not self._initialized:
                return CredentialHealth.MISSING
            return self._cache.get((operator_id, environment), CredentialHealth.MISSING)

    def is_initialized(self) -> bool:
        """True once the listener has completed its first full scan."""
        with self._lock:
            return self._initialized

    def snapshot(self) -> dict[tuple[UUID, str], CredentialHealth]:
        """Return a copy of the current cache (debugging / metrics)."""
        with self._lock:
            return dict(self._cache)

    # ------------------------------------------------------------------
    # Write API (listener)
    # ------------------------------------------------------------------

    def set_initial_scan(self, populated: dict[tuple[UUID, str], CredentialHealth]) -> None:
        """Atomically install the initial scan result + flip initialized.

        Replaces the entire cache (initial state should be authoritative).
        Subsequent updates flow through ``upsert`` on per-operator notifies.
        """
        with self._lock:
            self._cache = dict(populated)
            self._initialized = True
        logger.info(
            "credential_health_cache: initialized with %d operator entries",
            len(populated),
        )

    def upsert(
        self,
        *,
        operator_id: UUID,
        environment: str,
        health: CredentialHealth,
    ) -> None:
        """Update one operator's cached health (called from notify handler).

        Caller is the listener thread; consumers see the new value on
        their next ``get`` call.
        """
        with self._lock:
            self._cache[(operator_id, environment)] = health

    def replace(self, populated: dict[tuple[UUID, str], CredentialHealth]) -> None:
        """Replace the entire cache with a fresh scan (poll-fallback path).

        Listener calls this on the 5s poll tick to recover any operator
        whose notify was dropped. Replaces ``_cache`` wholesale rather
        than diff-and-patch — simpler reasoning, no risk of stale entries.
        Initialized stays True.
        """
        with self._lock:
            self._cache = dict(populated)


# ---------------------------------------------------------------------------
# Full-scan helper — used by the listener for startup + poll fallback
# ---------------------------------------------------------------------------


def scan_all_operators(
    pool: ConnectionPool[psycopg.Connection[Any]],
) -> dict[tuple[UUID, str], CredentialHealth]:
    """Compute operator-level credential health for every (operator, env).

    Iterates distinct (operator_id, environment) pairs in
    broker_credentials filtered to non-revoked, then computes the
    aggregate per pair via get_operator_credential_health. The
    aggregate computation is cheap (small CTE + LEFT JOIN) and v1
    operator counts are 1; this scales fine for the foreseeable
    multi-operator case.
    """
    out: dict[tuple[UUID, str], CredentialHealth] = {}
    with pool.connection() as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT DISTINCT operator_id, environment
                  FROM broker_credentials
                 WHERE revoked_at IS NULL
                """
            )
            pairs = [(row["operator_id"], row["environment"]) for row in cur.fetchall()]

        for operator_id, environment in pairs:
            health = get_operator_credential_health(
                conn,
                operator_id=operator_id,
                environment=environment,
            )
            out[(operator_id, environment)] = health
    return out
