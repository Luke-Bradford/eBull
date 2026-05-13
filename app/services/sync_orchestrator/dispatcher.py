"""Sync orchestrator queue dispatcher (#719).

Two-sided helper for the durable trigger queue:

- ``publish_sync_request`` — API-side publisher. Inserts a row into
  ``pending_job_requests`` with ``request_kind='sync'`` and the scope
  serialised to JSON; emits ``pg_notify('ebull_job_request', request_id::text)``;
  returns the request_id. This is the only function the FastAPI process
  ever calls for async sync. Replaces the deleted ``submit_sync``.

- ``claim_pending_request`` / ``mark_request_*`` — jobs-process-side
  helpers used by the listener and dispatch wrapper. Defined here so
  the API and jobs process share a single source of truth for the
  schema column names + status transitions.

The queue lives in ``pending_job_requests`` (sql/084). NOTIFY is the
low-latency wakeup hint; the listener also runs a 5s poll fallback so
a notify dropped during reconnection still surfaces within 5s.
"""

from __future__ import annotations

from typing import Any, Literal

import psycopg
from psycopg.types.json import Jsonb

from app.config import settings
from app.services.sync_orchestrator.types import SyncScope, SyncTrigger

NOTIFY_CHANNEL = "ebull_job_request"


RequestStatus = Literal["pending", "claimed", "dispatched", "completed", "rejected"]


def _scope_to_json(scope: SyncScope) -> dict[str, Any]:
    """Serialise a SyncScope to a JSON-safe dict for the queue payload.

    Stable shape: callers on the jobs side reconstruct via
    ``_scope_from_json``. Keep this simple — the orchestrator's scope
    object has only three fields, all primitives.
    """
    return {"kind": scope.kind, "detail": scope.detail, "force": scope.force}


def scope_from_json(payload: dict[str, Any]) -> SyncScope:
    """Reconstruct a SyncScope from queue payload JSON.

    Public so the jobs-process listener can decode without re-importing
    private helpers. Strict — unknown ``kind`` raises ``ValueError``;
    the listener catches and rejects the request rather than dispatching
    a malformed scope.
    """
    kind = payload.get("kind")
    if kind not in {"full", "layer", "high_frequency", "job", "behind"}:
        raise ValueError(f"invalid scope kind: {kind!r}")
    return SyncScope(
        kind=kind,
        detail=payload.get("detail"),
        force=bool(payload.get("force", False)),
    )


def publish_sync_request(
    scope: SyncScope,
    *,
    trigger: SyncTrigger = "manual",
    requested_by: str | None = None,
) -> int:
    """Publish a sync request through the durable queue.

    INSERTs a ``request_kind='sync'`` row, emits NOTIFY, returns the
    new ``request_id``. Idempotent at the SQL level: every call is a
    fresh row, so a double-click produces two requests. The
    sync_runs partial unique index handles the actual deduplication
    of in-flight syncs further downstream.
    """
    payload = {
        "scope": _scope_to_json(scope),
        "trigger": trigger,
    }
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pending_job_requests
                    (request_kind, payload, requested_by)
                VALUES ('sync', %(payload)s, %(requested_by)s)
                RETURNING request_id
                """,
                {"payload": Jsonb(payload), "requested_by": requested_by},
            )
            row = cur.fetchone()
            if row is None:
                raise RuntimeError("INSERT...RETURNING produced no row")
            request_id = int(row[0])
            cur.execute(
                "SELECT pg_notify(%s, %s)",
                (NOTIFY_CHANNEL, str(request_id)),
            )
    return request_id


def publish_manual_job_request(
    job_name: str,
    *,
    requested_by: str | None = None,
    process_id: str | None = None,
    mode: Literal["iterate", "full_wash"] | None = None,
    payload: dict[str, Any] | None = None,
) -> int:
    """Publish a manual job request through the durable queue.

    Same shape as ``publish_sync_request`` but for ``request_kind='manual_job'``.
    Caller is responsible for validating ``job_name`` against the
    invoker registry before publishing — the queue stores arbitrary
    job names and the jobs-process listener rejects unknown ones.

    ``process_id`` and ``mode`` (sql/138, #1071) populate the full-wash
    fence columns. The trigger handler in ``app/api/processes.py`` sets
    both for every Iterate / Full-wash click; legacy callers that have
    not migrated leave them ``None`` so the columns stay nullable for
    pre-existing rows.

    ``payload`` (#1064 PR1b-2) carries the canonical
    ``{params, control}`` envelope into ``pending_job_requests.payload``.
    The listener extracts ``payload['params']`` for invoker dispatch and
    ``payload['control']['override_bootstrap_gate']`` for gate control.
    Legacy callers leave it ``None`` and the column writes ``NULL`` (no
    params, no control flags). Callers MUST validate the params dict
    before calling this helper — the listener trusts the envelope.

    UNIQUE partial index ``pending_job_requests_active_full_wash_idx``
    catches a concurrent INSERT racing past the trigger handler's
    fence-check as ``UniqueViolation`` — handler maps to 409.
    """
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        return publish_manual_job_request_with_conn(
            conn,
            job_name,
            requested_by=requested_by,
            process_id=process_id,
            mode=mode,
            payload=payload,
        )


def publish_manual_job_request_with_conn(
    conn: psycopg.Connection[Any],
    job_name: str,
    *,
    requested_by: str | None = None,
    process_id: str | None = None,
    mode: Literal["iterate", "full_wash"] | None = None,
    payload: dict[str, Any] | None = None,
) -> int:
    """Same as ``publish_manual_job_request`` but uses the caller's
    connection. Caller is responsible for the surrounding transaction
    and commit.

    Issue #1139: the bootstrap API needs the queue INSERT to live in
    the same transaction as ``start_run`` / ``reset_failed_stages_for_retry``
    so a publish failure rolls the state mutation back. The old
    autocommit-only helper splits the work across two connections,
    which strands the singleton at ``status='running'`` whenever the
    publish step fails after the state commit lands.

    NOTIFY semantics under a shared txn: PostgreSQL flushes the
    notification queue at commit boundary, so the listener observes
    the wakeup exactly when (and only when) the queue row is durable.
    Strictly safer than the autocommit shape, which could fire NOTIFY
    in a separate txn before the queue row's own commit returned to
    the client.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pending_job_requests
                (request_kind, job_name, payload, requested_by, process_id, mode)
            VALUES ('manual_job', %(job_name)s, %(payload)s, %(requested_by)s,
                    %(process_id)s, %(mode)s)
            RETURNING request_id
            """,
            {
                "job_name": job_name,
                "payload": Jsonb(payload) if payload is not None else None,
                "requested_by": requested_by,
                "process_id": process_id,
                "mode": mode,
            },
        )
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("INSERT...RETURNING produced no row")
        request_id = int(row[0])
        cur.execute(
            "SELECT pg_notify(%s, %s)",
            (NOTIFY_CHANNEL, str(request_id)),
        )
    return request_id


# ---------------------------------------------------------------------------
# Jobs-process-side claim helpers
# ---------------------------------------------------------------------------


def claim_request_by_id(
    conn: psycopg.Connection[Any],
    request_id: int,
    *,
    boot_id: str,
) -> dict[str, Any] | None:
    """Atomically claim a specific pending request by id.

    Used by the NOTIFY-driven path: the listener parses the request_id
    from the notify payload and tries to claim that row. Returns the
    claim record (request_id, request_kind, job_name, payload) on
    success, ``None`` if the row was already claimed by another path
    (boot-drain or the poll loop). Concurrent listeners cannot both
    claim — the WHERE status='pending' guard plus the atomic UPDATE
    is the gate.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pending_job_requests
            SET status = 'claimed',
                claimed_at = NOW(),
                claimed_by = %(boot_id)s
            WHERE request_id = %(request_id)s
              AND status = 'pending'
            RETURNING request_id, request_kind, job_name, payload, mode
            """,
            {"request_id": request_id, "boot_id": boot_id},
        )
        row = cur.fetchone()
        if row is None:
            return None
        return {
            "request_id": int(row[0]),
            "request_kind": str(row[1]),
            "job_name": row[2],
            "payload": row[3],
            "mode": row[4],
        }


def claim_oldest_pending(
    conn: psycopg.Connection[Any],
    *,
    boot_id: str,
    ttl_hours: int = 24,
) -> dict[str, Any] | None:
    """Atomically claim the oldest pending request inside the TTL window.

    Used by the poll-fallback loop and by the boot-drain. ``FOR UPDATE
    SKIP LOCKED`` lets concurrent listener restarts proceed without
    deadlocking — the second caller simply sees an empty result and
    retries on the next tick.

    Returns the same shape as ``claim_request_by_id`` or ``None`` when
    no pending row exists inside the TTL window.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pending_job_requests
            SET status = 'claimed',
                claimed_at = NOW(),
                claimed_by = %(boot_id)s
            WHERE request_id = (
                SELECT request_id
                FROM pending_job_requests
                WHERE status = 'pending'
                  AND requested_at > NOW() - make_interval(hours => %(ttl_hours)s)
                ORDER BY requested_at
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING request_id, request_kind, job_name, payload, mode
            """,
            {"boot_id": boot_id, "ttl_hours": ttl_hours},
        )
        row = cur.fetchone()
        if row is None:
            return None
        return {
            "request_id": int(row[0]),
            "request_kind": str(row[1]),
            "job_name": row[2],
            "payload": row[3],
            "mode": row[4],
        }


def reset_stale_in_flight(
    conn: psycopg.Connection[Any],
    *,
    current_boot_id: str,
    ttl_hours: int = 24,
) -> int:
    """Reset stale ``claimed`` / ``dispatched`` rows from prior boots back
    to ``pending`` so boot-drain replays them. Singleton-fence
    invariant guarantees the rows are not held by a live process.

    Skips rows whose linked job_runs / sync_runs already reached a
    terminal status — those completed before the crash and must not
    double-run.

    Returns the number of rows reset.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pending_job_requests pjr
            SET status = 'pending', claimed_at = NULL, claimed_by = NULL
            WHERE pjr.status IN ('claimed', 'dispatched')
              AND pjr.claimed_by IS DISTINCT FROM %(current_boot_id)s
              AND pjr.requested_at > NOW() - make_interval(hours => %(ttl_hours)s)
              AND NOT EXISTS (
                SELECT 1 FROM job_runs jr
                WHERE jr.linked_request_id = pjr.request_id
                  AND jr.status IN ('success', 'failure')
              )
              AND NOT EXISTS (
                SELECT 1 FROM sync_runs sr
                WHERE sr.linked_request_id = pjr.request_id
                  AND sr.status IN ('complete', 'failed', 'partial', 'cancelled')
              )
            """,
            {"current_boot_id": current_boot_id, "ttl_hours": ttl_hours},
        )
        return cur.rowcount


def mark_request_dispatched(
    conn: psycopg.Connection[Any],
    request_id: int,
) -> None:
    """Transition a claimed row to ``dispatched``.

    Called from inside the executor task AFTER the linked run row
    (job_runs / sync_runs) has been opened with
    ``linked_request_id`` populated. The contract is that no row
    reaches ``dispatched`` without a run row already in flight,
    so boot-recovery's ``NOT EXISTS`` clauses can use the run's
    terminal state as the replay-suppression signal.
    """
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE pending_job_requests SET status='dispatched' WHERE request_id=%s",
            (request_id,),
        )


def mark_request_completed(
    conn: psycopg.Connection[Any],
    request_id: int,
) -> None:
    """Transition a dispatched row to ``completed``."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE pending_job_requests SET status='completed' WHERE request_id=%s",
            (request_id,),
        )


def mark_request_rejected(
    conn: psycopg.Connection[Any],
    request_id: int,
    *,
    error_msg: str,
) -> None:
    """Transition a row to ``rejected`` with an error message.

    Used when the listener cannot dispatch (unknown job name, malformed
    payload, executor refused submit). The row stays visible at
    ``GET /jobs/requests?status=rejected`` so the operator sees the
    failure rather than guessing why nothing ran.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pending_job_requests
            SET status='rejected', error_msg=%(error_msg)s
            WHERE request_id=%(request_id)s
            """,
            {"request_id": request_id, "error_msg": error_msg[:1000]},
        )


__all__ = [
    "NOTIFY_CHANNEL",
    "RequestStatus",
    "claim_oldest_pending",
    "claim_request_by_id",
    "mark_request_completed",
    "mark_request_dispatched",
    "mark_request_rejected",
    "publish_manual_job_request",
    "publish_manual_job_request_with_conn",
    "publish_sync_request",
    "reset_stale_in_flight",
    "scope_from_json",
]
