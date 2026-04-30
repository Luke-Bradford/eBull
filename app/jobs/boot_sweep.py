"""Boot-time freshness sweep for the jobs process (#719).

Pre-#719 this lived in ``app/main.py::_boot_freshness_sweep`` and ran
on every API restart. The API initiates no work in #719, so the sweep
relocates here — runs once per jobs-process boot, after the scheduler's
catch-up loop, before the listener thread starts. Any layer past its
freshness target gets a `scope='behind'` sync.

Best-effort: every exception is logged + swallowed. The sweep is
recovery on top of the regular schedule, not a critical path.
"""

from __future__ import annotations

import logging
import os

from app.services.sync_orchestrator.executor import run_sync
from app.services.sync_orchestrator.types import SyncAlreadyRunning, SyncScope

logger = logging.getLogger(__name__)


def run_boot_freshness_sweep() -> None:
    """Fire one ``scope='behind'`` sync if the operator hasn't opted out.

    Gated by ``EBULL_SKIP_BOOT_SWEEP=1`` (mirrors the pre-#719 env
    var) so tests and CI can disable it. Runs synchronously on the
    caller's thread — the entrypoint hosts the dedicated sync executor
    elsewhere; this helper is invoked once at boot from the main
    thread before the listener accepts NOTIFY traffic.
    """
    if os.environ.get("EBULL_SKIP_BOOT_SWEEP") == "1":
        logger.info("boot freshness sweep skipped: EBULL_SKIP_BOOT_SWEEP=1")
        return
    try:
        run_sync(SyncScope.behind(), trigger="boot_sweep", linked_request_id=None)
        logger.info("boot freshness sweep dispatched (scope=behind)")
    except SyncAlreadyRunning:
        logger.info("boot freshness sweep skipped: sync already running")
    except Exception:
        logger.exception("boot freshness sweep raised — daily cron will retry")
