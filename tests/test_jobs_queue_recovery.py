"""Boot-recovery branches of ``reset_stale_in_flight`` (#719).

The dispatcher unit tests cover the simple paths (claim, mark, scope
round-trip). This file pins the four recovery branches the spec
locked in:

  (a) `manual_job` row whose `linked_request_id` matches a terminal
      `job_runs` is NOT replayed.
  (b) `sync` row whose `linked_request_id` matches a terminal
      `sync_runs` is NOT replayed.
  (c) `sync` row whose linked sync_runs is still 'running' (mid-flight
      crash) IS replayed.
  (d) Any row whose `requested_at` is older than 24h is NOT replayed.
"""

from __future__ import annotations

from collections.abc import Generator
from typing import Any

import psycopg
import pytest

from app.config import settings
from app.services.sync_orchestrator.dispatcher import (
    publish_manual_job_request,
    publish_sync_request,
    reset_stale_in_flight,
)
from app.services.sync_orchestrator.types import SyncScope


def _db_reachable() -> bool:
    try:
        with psycopg.connect(settings.database_url, connect_timeout=2) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _db_reachable(),
    reason="dev Postgres not reachable; queue recovery tests require the real DB",
)


@pytest.fixture()
def _dev_conn() -> Generator[psycopg.Connection[Any]]:
    conn = psycopg.connect(settings.database_url, autocommit=True)
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture()
def _cleanup_requests(_dev_conn: psycopg.Connection[Any]) -> Generator[list[int]]:
    created: list[int] = []
    yield created
    if created:
        with _dev_conn.cursor() as cur:
            cur.execute(
                "DELETE FROM job_runs WHERE linked_request_id = ANY(%s)",
                (created,),
            )
            cur.execute(
                "DELETE FROM sync_runs WHERE linked_request_id = ANY(%s)",
                (created,),
            )
            cur.execute(
                "DELETE FROM pending_job_requests WHERE request_id = ANY(%s)",
                (created,),
            )


def _force_to_dispatched(conn: psycopg.Connection[Any], request_id: int, claimed_by: str = "prior-boot-xyz") -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pending_job_requests
            SET status='dispatched', claimed_by=%s, claimed_at=NOW()
            WHERE request_id=%s
            """,
            (claimed_by, request_id),
        )


def test_manual_job_with_terminal_run_not_replayed(
    _dev_conn: psycopg.Connection[Any],
    _cleanup_requests: list[int],
) -> None:
    """Branch (a): completed manual_job rows stay 'dispatched'."""
    request_id = publish_manual_job_request("fundamentals_sync")
    _cleanup_requests.append(request_id)
    _force_to_dispatched(_dev_conn, request_id)
    with _dev_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO job_runs (job_name, started_at, finished_at, status, linked_request_id)
            VALUES ('fundamentals_sync', NOW(), NOW(), 'success', %s)
            """,
            (request_id,),
        )

    reset_stale_in_flight(_dev_conn, current_boot_id="this-boot")

    with _dev_conn.cursor() as cur:
        cur.execute("SELECT status FROM pending_job_requests WHERE request_id=%s", (request_id,))
        row = cur.fetchone()
    assert row is not None
    assert row[0] == "dispatched"


def test_sync_with_terminal_sync_run_not_replayed(
    _dev_conn: psycopg.Connection[Any],
    _cleanup_requests: list[int],
) -> None:
    """Branch (b): completed sync rows stay 'dispatched'."""
    request_id = publish_sync_request(SyncScope.behind(), trigger="manual")
    _cleanup_requests.append(request_id)
    _force_to_dispatched(_dev_conn, request_id)
    with _dev_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sync_runs (scope, trigger, status, layers_planned, linked_request_id)
            VALUES ('behind', 'manual', 'complete', 0, %s)
            """,
            (request_id,),
        )

    reset_stale_in_flight(_dev_conn, current_boot_id="this-boot")

    with _dev_conn.cursor() as cur:
        cur.execute("SELECT status FROM pending_job_requests WHERE request_id=%s", (request_id,))
        row = cur.fetchone()
    assert row is not None
    assert row[0] == "dispatched"


def test_sync_with_running_sync_run_is_replayed(
    _dev_conn: psycopg.Connection[Any],
    _cleanup_requests: list[int],
) -> None:
    """Branch (c): a sync_runs row left 'running' by a prior crash means
    the queue request is replayed (NOT EXISTS clause requires terminal
    status; 'running' is non-terminal).
    """
    # Clear any stale running sync_runs left by other tests so the
    # partial unique index doesn't reject our INSERT below. The dev DB
    # is not test-isolated; we have to cooperate with siblings.
    with _dev_conn.cursor() as cur:
        cur.execute("UPDATE sync_runs SET status='cancelled' WHERE status='running'")

    request_id = publish_sync_request(SyncScope.behind(), trigger="manual")
    _cleanup_requests.append(request_id)
    _force_to_dispatched(_dev_conn, request_id)
    with _dev_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sync_runs (scope, trigger, status, layers_planned, linked_request_id)
            VALUES ('behind', 'manual', 'running', 0, %s)
            """,
            (request_id,),
        )

    reset_stale_in_flight(_dev_conn, current_boot_id="this-boot")

    with _dev_conn.cursor() as cur:
        cur.execute("SELECT status FROM pending_job_requests WHERE request_id=%s", (request_id,))
        row = cur.fetchone()
    assert row is not None
    assert row[0] == "pending"


def test_dispatched_with_no_run_row_is_replayed(
    _dev_conn: psycopg.Connection[Any],
    _cleanup_requests: list[int],
) -> None:
    """Regression for PR #719 review BLOCKING.

    A row stuck at ``dispatched`` with no `job_runs` / `sync_runs` row
    yet — possible only via a future bug since the wrapper now skips
    the dispatched transition — must still be replayed by
    ``reset_stale_in_flight``. Pin this branch so a future regression
    that re-introduces the broken ordering surfaces here.
    """
    request_id = publish_manual_job_request("fundamentals_sync")
    _cleanup_requests.append(request_id)
    _force_to_dispatched(_dev_conn, request_id)
    # Deliberately do NOT insert any job_runs row for this request_id.

    reset_stale_in_flight(_dev_conn, current_boot_id="this-boot")

    with _dev_conn.cursor() as cur:
        cur.execute("SELECT status FROM pending_job_requests WHERE request_id=%s", (request_id,))
        row = cur.fetchone()
    assert row is not None
    assert row[0] == "pending"


def test_old_request_outside_ttl_not_replayed(
    _dev_conn: psycopg.Connection[Any],
    _cleanup_requests: list[int],
) -> None:
    """Branch (d): a 25h-old row outside the 24h TTL stays as-is."""
    request_id = publish_manual_job_request("fundamentals_sync")
    _cleanup_requests.append(request_id)
    _force_to_dispatched(_dev_conn, request_id)
    # Backdate the row so it falls outside the TTL window.
    with _dev_conn.cursor() as cur:
        cur.execute(
            "UPDATE pending_job_requests SET requested_at = NOW() - interval '25 hours' WHERE request_id=%s",
            (request_id,),
        )

    reset_stale_in_flight(_dev_conn, current_boot_id="this-boot")

    with _dev_conn.cursor() as cur:
        cur.execute("SELECT status FROM pending_job_requests WHERE request_id=%s", (request_id,))
        row = cur.fetchone()
    assert row is not None
    assert row[0] == "dispatched"
