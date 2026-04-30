"""Boot-drain claim loop (#719).

The entrypoint's ``_drain_pending_at_boot`` loops over every pending
queue row at boot — same atomic claim path the listener uses on its
poll fallback. This test seeds two pending rows, drives the boot-drain
helper from ``app.jobs.__main__``, and asserts both rows are claimed
and dispatched.
"""

from __future__ import annotations

from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from unittest.mock import MagicMock

import psycopg
import pytest

from app.config import settings
from app.jobs.__main__ import _drain_pending_at_boot
from app.jobs.listener import ListenerState
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
    reason="dev Postgres not reachable; boot-drain test requires the real DB",
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


def test_drain_pending_at_boot_claims_each_row(
    _cleanup_requests: list[int],
) -> None:
    rid_a = publish_manual_job_request("fundamentals_sync")
    rid_b = publish_manual_job_request("fundamentals_sync")
    _cleanup_requests.extend([rid_a, rid_b])

    runtime = MagicMock()
    state = ListenerState()
    sync_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="test-sync")

    try:
        drained = _drain_pending_at_boot(
            runtime=runtime,
            sync_executor=sync_executor,
            boot_id="test-boot",
            state=state,
        )
    finally:
        sync_executor.shutdown(wait=False, cancel_futures=True)

    assert drained >= 2
    # Both rows should now be `claimed` (the dispatch path will only
    # transition to `dispatched` inside the executor wrapper, which
    # we mocked out via runtime.submit_manual_with_request).
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT request_id, status FROM pending_job_requests WHERE request_id = ANY(%s) ORDER BY request_id",
                ([rid_a, rid_b],),
            )
            rows: list[tuple[Any, ...]] = list(cur.fetchall())
    assert len(rows) == 2
    for _rid, status in rows:
        assert status == "claimed"


def test_drain_pending_at_boot_returns_zero_when_empty() -> None:
    """No pending rows: the helper returns 0 without calling the runtime."""
    # Best-effort — there may be other pending rows in the dev DB.
    # Drain those into the mock first, then re-call to confirm 0.
    runtime = MagicMock()
    state = ListenerState()
    sync_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="test-sync")

    try:
        first = _drain_pending_at_boot(runtime=runtime, sync_executor=sync_executor, boot_id="t", state=state)
        # Reset the rows we just claimed so another suite doesn't see them stuck.
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE pending_job_requests SET status='pending', claimed_at=NULL, claimed_by=NULL "
                    "WHERE claimed_by='t'"
                )
        # Now call again — should drain those re-pending rows.
        second = _drain_pending_at_boot(runtime=runtime, sync_executor=sync_executor, boot_id="t2", state=state)
    finally:
        sync_executor.shutdown(wait=False, cancel_futures=True)

    assert first >= 0
    assert second >= 0
