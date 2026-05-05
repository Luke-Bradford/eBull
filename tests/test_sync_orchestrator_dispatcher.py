"""Unit coverage for ``app.services.sync_orchestrator.dispatcher`` (#719).

Drives every helper against a real Postgres so the SQL itself is
exercised: schema-mismatch bugs (column renamed, status enum drift)
fail loudly here rather than at first operator click. The helpers are
pure SQL with no provider I/O.

Per #893, ``settings.database_url`` is monkeypatched to the worker's
test DB so dispatcher helpers (which read the URL at call time) write
to per-worker isolation rather than the operator's dev DB.
"""

from __future__ import annotations

from collections.abc import Generator
from typing import Any

import psycopg
import pytest
from psycopg.types.json import Jsonb

from app.services.sync_orchestrator.dispatcher import (
    NOTIFY_CHANNEL,
    claim_oldest_pending,
    claim_request_by_id,
    mark_request_completed,
    mark_request_dispatched,
    mark_request_rejected,
    publish_manual_job_request,
    publish_sync_request,
    reset_stale_in_flight,
    scope_from_json,
)
from app.services.sync_orchestrator.types import SyncScope
from tests.fixtures.ebull_test_db import test_database_url, test_db_available

pytestmark = pytest.mark.skipif(
    not test_db_available(),
    reason="ebull_test DB unavailable; dispatcher tests require a real DB",
)


@pytest.fixture(autouse=True)
def _route_settings_to_test_db(monkeypatch: pytest.MonkeyPatch) -> None:
    """Point dispatcher helpers at the worker's test DB."""
    monkeypatch.setattr("app.config.settings.database_url", test_database_url())


@pytest.fixture()
def _dev_conn() -> Generator[psycopg.Connection[Any]]:
    """A short-lived autocommit connection scoped to one test.

    Name retained for diff-locality; pre-#893 this opened
    ``test_database_url()``. After migration it points at the
    worker's test DB.
    """
    conn = psycopg.connect(test_database_url(), autocommit=True)
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture()
def _cleanup_requests(_dev_conn: psycopg.Connection[Any]) -> Generator[list[int]]:
    """Track request_ids created in the test and delete them in teardown.

    Avoids polluting the dev DB across runs; the table is small and
    the deletes are scoped to the IDs the test wrote, so unrelated
    rows (from a real-life manual run) survive.
    """
    created: list[int] = []
    yield created
    if created:
        with _dev_conn.cursor() as cur:
            cur.execute(
                "DELETE FROM pending_job_requests WHERE request_id = ANY(%s)",
                (created,),
            )


def test_publish_sync_request_inserts_row_and_notifies(
    _dev_conn: psycopg.Connection[Any],
    _cleanup_requests: list[int],
) -> None:
    """A sync publish writes a `pending` row with request_kind='sync',
    serialises the scope into payload, and emits a NOTIFY whose payload
    is the new request_id.
    """
    # Open a side connection BEFORE publishing so its LISTEN catches
    # the NOTIFY. autocommit ensures the LISTEN takes effect immediately.
    listener = psycopg.connect(test_database_url(), autocommit=True)
    try:
        with listener.cursor() as cur:
            cur.execute(f"LISTEN {NOTIFY_CHANNEL}")

        request_id = publish_sync_request(
            SyncScope.behind(),
            trigger="boot_sweep",
            requested_by="test_publish_sync_request",
        )
        _cleanup_requests.append(request_id)

        # Pull the notify; psycopg buffers them on the connection and
        # exposes them via the notifies() iterator. With autocommit, a
        # short generator() call drains buffered events.
        listener.execute("SELECT 1")  # forces the wire flush
        notify_payloads = [n.payload for n in listener.notifies(timeout=2.0, stop_after=1)]
        assert notify_payloads == [str(request_id)]

        # Row is in the queue with the right shape.
        with _dev_conn.cursor() as cur:
            cur.execute(
                """
                SELECT request_kind, job_name, payload, status, requested_by
                FROM pending_job_requests
                WHERE request_id = %s
                """,
                (request_id,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == "sync"
        assert row[1] is None
        # payload is a dict round-tripped through JSONB.
        assert row[2]["scope"]["kind"] == "behind"
        assert row[2]["trigger"] == "boot_sweep"
        assert row[3] == "pending"
        assert row[4] == "test_publish_sync_request"
    finally:
        listener.close()


def test_publish_manual_job_request_inserts_manual_row(
    _dev_conn: psycopg.Connection[Any],
    _cleanup_requests: list[int],
) -> None:
    request_id = publish_manual_job_request(
        "fundamentals_sync",
        requested_by="test_publish_manual",
    )
    _cleanup_requests.append(request_id)

    with _dev_conn.cursor() as cur:
        cur.execute(
            "SELECT request_kind, job_name, status FROM pending_job_requests WHERE request_id=%s",
            (request_id,),
        )
        row = cur.fetchone()
    assert row == ("manual_job", "fundamentals_sync", "pending")


def test_claim_request_by_id_atomically_transitions(
    _dev_conn: psycopg.Connection[Any],
    _cleanup_requests: list[int],
) -> None:
    """Claiming a pending row atomically flips status and returns the
    payload. A second claim attempt returns None (already-claimed gate).
    """
    request_id = publish_manual_job_request("fundamentals_sync")
    _cleanup_requests.append(request_id)

    first = claim_request_by_id(_dev_conn, request_id, boot_id="boot-A")
    assert first is not None
    assert first["request_id"] == request_id
    assert first["request_kind"] == "manual_job"
    assert first["job_name"] == "fundamentals_sync"

    second = claim_request_by_id(_dev_conn, request_id, boot_id="boot-A")
    assert second is None  # already claimed


def test_claim_oldest_pending_skips_locked_rows(
    _dev_conn: psycopg.Connection[Any],
    _cleanup_requests: list[int],
) -> None:
    """Two concurrent claimers must not both grab the same row.
    Simulated by holding a row-level lock on conn A and calling
    claim_oldest_pending on conn B — B sees the row but SKIP LOCKED
    bypasses it and claims the next eligible row.

    The dev DB may carry pre-existing pending rows from prior runs; we
    don't care which row B claims, only that it does NOT claim the one
    A is holding.
    """
    rid_a = publish_manual_job_request("fundamentals_sync")
    _cleanup_requests.append(rid_a)
    publish_manual_job_request("fundamentals_sync")  # ensure at least one other claimable row exists
    max_row = _dev_conn.execute("SELECT MAX(request_id) FROM pending_job_requests").fetchone()
    assert max_row is not None
    _cleanup_requests.append(int(max_row[0]))

    # Hold rid_a in a transaction on conn A.
    conn_a = psycopg.connect(test_database_url())
    try:
        conn_a.execute(
            "SELECT request_id FROM pending_job_requests WHERE request_id=%s FOR UPDATE",
            (rid_a,),
        )
        # Conn B claims via the helper — must skip rid_a (held by A).
        result = claim_oldest_pending(_dev_conn, boot_id="boot-B")
        assert result is not None
        assert result["request_id"] != rid_a, (
            "claim_oldest_pending returned the row held by another connection — SKIP LOCKED isn't applied"
        )
    finally:
        conn_a.rollback()
        conn_a.close()


def test_reset_stale_in_flight_revives_orphaned_rows(
    _dev_conn: psycopg.Connection[Any],
    _cleanup_requests: list[int],
) -> None:
    """A row claimed by a prior boot id (still in 'claimed' state, no
    terminal job_runs) must be reset to 'pending' so boot-drain replays.
    """
    request_id = publish_manual_job_request("fundamentals_sync")
    _cleanup_requests.append(request_id)

    # Simulate a prior boot's claim.
    with _dev_conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pending_job_requests
            SET status='claimed', claimed_by='prior-boot-xyz', claimed_at=NOW()
            WHERE request_id=%s
            """,
            (request_id,),
        )

    rowcount = reset_stale_in_flight(_dev_conn, current_boot_id="this-boot")
    assert rowcount >= 1

    with _dev_conn.cursor() as cur:
        cur.execute("SELECT status FROM pending_job_requests WHERE request_id=%s", (request_id,))
        row = cur.fetchone()
    assert row is not None
    assert row[0] == "pending"


def test_reset_stale_in_flight_skips_completed_runs(
    _dev_conn: psycopg.Connection[Any],
    _cleanup_requests: list[int],
) -> None:
    """A row whose linked_request_id matches a terminal job_runs row
    must NOT be reset — the work already completed before the crash.
    """
    request_id = publish_manual_job_request("fundamentals_sync")
    _cleanup_requests.append(request_id)

    # Simulate prior boot's dispatched state + a completed job_runs.
    with _dev_conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pending_job_requests
            SET status='dispatched', claimed_by='prior-boot-xyz', claimed_at=NOW()
            WHERE request_id=%s
            """,
            (request_id,),
        )
        cur.execute(
            """
            INSERT INTO job_runs
                (job_name, started_at, finished_at, status, linked_request_id)
            VALUES
                ('fundamentals_sync', NOW(), NOW(), 'success', %s)
            RETURNING run_id
            """,
            (request_id,),
        )
        inserted = cur.fetchone()
    assert inserted is not None
    run_id = inserted[0]

    try:
        reset_stale_in_flight(_dev_conn, current_boot_id="this-boot")

        with _dev_conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM pending_job_requests WHERE request_id=%s",
                (request_id,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == "dispatched"  # NOT reset
    finally:
        with _dev_conn.cursor() as cur:
            cur.execute("DELETE FROM job_runs WHERE run_id=%s", (run_id,))


def test_mark_request_lifecycle_transitions(
    _dev_conn: psycopg.Connection[Any],
    _cleanup_requests: list[int],
) -> None:
    """dispatched / completed / rejected transitions write the right
    status (and error_msg, where applicable).
    """
    request_id = publish_manual_job_request("fundamentals_sync")
    _cleanup_requests.append(request_id)
    claim_request_by_id(_dev_conn, request_id, boot_id="boot-A")

    mark_request_dispatched(_dev_conn, request_id)
    with _dev_conn.cursor() as cur:
        cur.execute("SELECT status FROM pending_job_requests WHERE request_id=%s", (request_id,))
        row = cur.fetchone()
        assert row is not None and row[0] == "dispatched"

    mark_request_completed(_dev_conn, request_id)
    with _dev_conn.cursor() as cur:
        cur.execute("SELECT status FROM pending_job_requests WHERE request_id=%s", (request_id,))
        row = cur.fetchone()
        assert row is not None and row[0] == "completed"

    # A rejected request — fresh row.
    rejected_id = publish_manual_job_request("nope_unknown_job")
    _cleanup_requests.append(rejected_id)
    claim_request_by_id(_dev_conn, rejected_id, boot_id="boot-A")
    mark_request_rejected(_dev_conn, rejected_id, error_msg="unknown job name")
    with _dev_conn.cursor() as cur:
        cur.execute(
            "SELECT status, error_msg FROM pending_job_requests WHERE request_id=%s",
            (rejected_id,),
        )
        row = cur.fetchone()
    assert row is not None
    assert row[0] == "rejected"
    assert row[1] == "unknown job name"


def test_scope_from_json_round_trips_every_kind() -> None:
    """The scope serialiser used by publish_sync_request must round-trip
    through the JSONB payload with no information loss.
    """
    cases = [
        SyncScope.full(),
        SyncScope.high_frequency(),
        SyncScope.behind(),
        SyncScope.layer("fundamentals"),
        SyncScope.job("nightly_universe_sync", force=True),
    ]
    for original in cases:
        payload = {
            "kind": original.kind,
            "detail": original.detail,
            "force": original.force,
        }
        rebuilt = scope_from_json(payload)
        assert rebuilt == original, f"round-trip failed for {original}"


def test_scope_from_json_rejects_unknown_kind() -> None:
    """Malformed payloads must raise so the listener can mark the
    request rejected rather than dispatch a wrong scope.
    """
    with pytest.raises(ValueError):
        scope_from_json({"kind": "totally-not-a-kind", "detail": None, "force": False})


# Avoid an unused-import warning for Jsonb — we import it for parity with
# the dispatcher module and may need it in a future test that pre-seeds
# payload JSONB directly. Pin a no-op reference so ruff doesn't strip it.
_ = Jsonb
