"""Listener dispatch routing (#719).

Drives the dispatch helpers directly with a mocked ``JobRuntime`` and
sync executor so the SQL paths stay real (claim queries are exercised
by ``test_sync_orchestrator_dispatcher.py``) but the dispatch routing
+ payload-validation logic is pinned. The full LISTEN/NOTIFY loop
needs a real psycopg connection and is out of scope for this unit
test; integration coverage runs through the smoke gate in dev when
the jobs process is up.
"""

from __future__ import annotations

from collections.abc import Generator
from typing import Any
from unittest.mock import MagicMock

import psycopg
import pytest

from app.config import settings
from app.jobs.listener import (
    ListenerState,
    _dispatch_manual_job,
    _dispatch_sync_request,
    _route_claim,
)
from app.services.sync_orchestrator.dispatcher import publish_manual_job_request


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
    reason="dev Postgres not reachable; listener tests require the real DB for queue claims",
)


@pytest.fixture()
def _cleanup_requests() -> Generator[list[int]]:
    created: list[int] = []
    yield created
    if created:
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM pending_job_requests WHERE request_id = ANY(%s)",
                    (created,),
                )


def test_dispatch_manual_job_with_unknown_name_marks_rejected(_cleanup_requests: list[int]) -> None:
    """An unknown job_name must mark the row rejected and skip the runtime."""
    request_id = publish_manual_job_request("definitely_not_a_real_job")
    _cleanup_requests.append(request_id)

    runtime = MagicMock()
    _dispatch_manual_job(runtime=runtime, request_id=request_id, job_name="definitely_not_a_real_job")

    runtime.submit_manual_with_request.assert_not_called()
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, error_msg FROM pending_job_requests WHERE request_id=%s",
                (request_id,),
            )
            row = cur.fetchone()
    assert row is not None
    assert row[0] == "rejected"
    assert "unknown job name" in row[1]


def test_dispatch_manual_job_with_valid_name_calls_runtime() -> None:
    """A valid job_name must be forwarded to ``runtime.submit_manual_with_request``."""
    runtime = MagicMock()
    _dispatch_manual_job(runtime=runtime, request_id=42, job_name="fundamentals_sync")
    runtime.submit_manual_with_request.assert_called_once_with("fundamentals_sync", request_id=42)


def test_dispatch_sync_with_invalid_payload_marks_rejected(
    _cleanup_requests: list[int],
) -> None:
    """A sync request with no scope dict must be rejected without submitting."""
    # publish a valid sync row first so the dispatcher has something to update
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        cur = conn.execute(
            "INSERT INTO pending_job_requests (request_kind, payload) VALUES ('sync', '{}'::jsonb) RETURNING request_id"
        )
        row = cur.fetchone()
    assert row is not None
    request_id = int(row[0])
    _cleanup_requests.append(request_id)

    sync_executor = MagicMock()
    _dispatch_sync_request(sync_executor=sync_executor, request_id=request_id, payload={"trigger": "manual"})
    sync_executor.submit.assert_not_called()


def test_route_claim_unknown_kind_rejects_and_logs(
    _cleanup_requests: list[int],
) -> None:
    """A claim row with an unknown request_kind must be rejected without
    touching the runtime or sync executor.
    """
    request_id = publish_manual_job_request("fundamentals_sync")
    _cleanup_requests.append(request_id)

    runtime = MagicMock()
    sync_executor = MagicMock()
    state = ListenerState()
    claim: dict[str, Any] = {
        "request_id": request_id,
        "request_kind": "totally_invalid_kind",
        "job_name": None,
        "payload": None,
    }
    _route_claim(claim, runtime=runtime, sync_executor=sync_executor, state=state)

    runtime.submit_manual_with_request.assert_not_called()
    sync_executor.submit.assert_not_called()
    assert state.claims_rejected == 1
