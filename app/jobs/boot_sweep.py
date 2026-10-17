"""Boot-time freshness sweep for the jobs process (#719).

Pre-#719 this lived in ``app/main.py::_boot_freshness_sweep`` and ran
on every API restart. The API initiates no work in #719, so the sweep
relocates here — runs once per jobs-process boot, after the scheduler's
catch-up loop, before the listener thread starts. Any layer past its
freshness target gets a `scope='behind'` sync.

Best-effort: every exception is logged + swallowed. The sweep is
recovery on top of the regular schedule, not a critical path.

#3784: the single-sync slot can be held when the sweep arrives — boot
catch-up fires ``orchestrator_high_frequency_sync`` just before it, on a
job thread, so queueing the sweep through ``pending_job_requests`` would
hit the same slot. The sweep therefore waits for the slot itself:

* a holder whose scope is ``full`` or ``behind`` already syncs every stale
  layer, so the sweep is covered and stops;
* any other holder is waited out, retrying every ``RETRY_INTERVAL_S`` up to
  ``RETRY_DEADLINE_S``. There is no source rule for either; they are fixed
  by construction from the high-frequency sync's durations on the dev DB
  (6,909 runs, 2026-09-17..10-17: p50 5.1 s, p99 6.1 s, max 64.7 s;
  ``SELECT percentile_cont(array[0.5,0.99]) WITHIN GROUP (ORDER BY
  finished_at - started_at), max(finished_at - started_at) FROM sync_runs
  WHERE scope = 'high_frequency'``). The deadline is over 4x that maximum.

It runs on its own daemon thread (``app.jobs.__main__``), so waiting blocks
neither boot nor shutdown.
"""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Callable
from typing import Final

import psycopg

from app.config import settings
from app.services.sync_orchestrator.executor import run_sync
from app.services.sync_orchestrator.types import SyncAlreadyRunning, SyncScope

logger = logging.getLogger(__name__)

RETRY_INTERVAL_S: Final = 5.0
RETRY_DEADLINE_S: Final = 300.0

#: Holder scopes whose plan already covers every layer a ``behind`` sweep would sync.
_COVERING_SCOPES: Final = frozenset({"full", "behind"})


def _active_scope(sync_run_id: int | None) -> str | None:
    """The scope of the sync holding the slot, or None when it is unknown or has finished."""
    if sync_run_id is None:
        return None
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        row = conn.execute(
            "SELECT scope FROM sync_runs WHERE sync_run_id = %s AND status = 'running'", (sync_run_id,)
        ).fetchone()
    return None if row is None else str(row[0])


def run_boot_freshness_sweep(
    *,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
    active_scope: Callable[[int | None], str | None] = _active_scope,
) -> None:
    """Fire one ``scope='behind'`` sync if the operator hasn't opted out, waiting out a non-covering holder of the
    sync slot (module docstring).

    Gated by ``EBULL_SKIP_BOOT_SWEEP=1`` (mirrors the pre-#719 env
    var) so tests and CI can disable it. Runs synchronously on the
    caller's thread, which at boot is the sweep's own daemon thread
    (``app.jobs.__main__._run_boot_freshness_sweep_thread``, #1479).
    """
    if os.environ.get("EBULL_SKIP_BOOT_SWEEP") == "1":
        logger.info("boot freshness sweep skipped: EBULL_SKIP_BOOT_SWEEP=1")
        return
    deadline = monotonic() + RETRY_DEADLINE_S
    try:
        while True:
            try:
                run_sync(SyncScope.behind(), trigger="boot_sweep", linked_request_id=None)
                logger.info("boot freshness sweep dispatched (scope=behind)")
                return
            except SyncAlreadyRunning as exc:
                try:
                    holder = active_scope(exc.active_sync_run_id)
                except Exception:
                    # A failed lookup is an unknown holder: keep waiting, never drop the sweep on it.
                    logger.warning("boot freshness sweep: holder lookup failed", exc_info=True)
                    holder = None
                if holder in _COVERING_SCOPES:
                    logger.info("boot freshness sweep covered by running %s sync %s", holder, exc.active_sync_run_id)
                    return
                if monotonic() + RETRY_INTERVAL_S > deadline:
                    logger.warning(
                        "boot freshness sweep NOT dispatched: sync %s (scope=%s) still held the slot after %.0fs; "
                        "stale layers wait for the next scheduled full sync",
                        exc.active_sync_run_id,
                        holder,
                        RETRY_DEADLINE_S,
                    )
                    return
                logger.info(
                    "boot freshness sweep waiting: sync %s (scope=%s) holds the slot", exc.active_sync_run_id, holder
                )
                sleep(RETRY_INTERVAL_S)
    except Exception:
        logger.exception("boot freshness sweep raised — daily cron will retry")
