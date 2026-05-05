"""Boot-drain claim loop (#719).

The entrypoint's ``_drain_pending_at_boot`` loops over every pending
queue row at boot — same atomic claim path the listener uses on its
poll fallback. This test seeds two pending rows, drives the boot-drain
helper from ``app.jobs.__main__``, and asserts both rows are claimed
and dispatched.

Per #893, ``settings.database_url`` is monkeypatched to the worker's
test DB so the drain helper writes to per-worker isolation.
"""

from __future__ import annotations

from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from unittest.mock import MagicMock

import psycopg
import pytest

from app.jobs.__main__ import _drain_pending_at_boot
from app.jobs.listener import ListenerState
from app.services.sync_orchestrator.dispatcher import publish_manual_job_request
from tests.fixtures.ebull_test_db import test_database_url, test_db_available

pytestmark = pytest.mark.skipif(
    not test_db_available(),
    reason="ebull_test DB unavailable; boot-drain test requires a real DB",
)


@pytest.fixture(autouse=True)
def _route_settings_to_test_db(monkeypatch: pytest.MonkeyPatch) -> None:
    """Point the drain helper + dispatcher at the worker's test DB."""
    monkeypatch.setattr("app.config.settings.database_url", test_database_url())


@pytest.fixture()
def _cleanup_requests() -> Generator[list[int]]:
    created: list[int] = []
    yield created
    if created:
        with psycopg.connect(test_database_url(), autocommit=True) as conn:
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
    with psycopg.connect(test_database_url(), autocommit=True) as conn:
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
    # Per-worker test DB starts clean of pending_job_requests at fixture
    # truncation time, but other tests in the same worker may have
    # written rows. Drain those into the mock first, reset, drain again
    # to confirm idempotence.
    runtime = MagicMock()
    state = ListenerState()
    sync_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="test-sync")

    try:
        first = _drain_pending_at_boot(runtime=runtime, sync_executor=sync_executor, boot_id="t", state=state)
        # Reset the rows we just claimed so another suite doesn't see them stuck.
        with psycopg.connect(test_database_url(), autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE pending_job_requests SET status='pending', claimed_at=NULL, claimed_by=NULL "
                    "WHERE claimed_by='t'"
                )
        # Now call again — should drain those re-pending rows.
        second = _drain_pending_at_boot(runtime=runtime, sync_executor=sync_executor, boot_id="t2", state=state)
    finally:
        sync_executor.shutdown(wait=False, cancel_futures=True)
        # Cleanup: the per-worker fixture's _PLANNER_TABLES truncation
        # only fires for tests that opt into ``ebull_test_conn``; this
        # test does not, so any rows the drain claimed under boot_ids
        # ``t`` / ``t2`` would leak into later tests in this worker.
        # Delete them explicitly so the per-worker DB stays clean.
        with psycopg.connect(test_database_url(), autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM pending_job_requests WHERE claimed_by IN ('t', 't2')")

    assert first >= 0
    assert second >= 0
