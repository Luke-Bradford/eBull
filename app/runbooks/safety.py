"""Shared safety primitives for Stream A operator runbooks.

Three-tier defensive gate consumed by every runbook before any
destructive or HTTP action:

  1. ``assert_dev_env()`` — ``EBULL_ENV`` must be explicitly ``'dev'``.
     Unset or non-dev → ``RunbookRefused``.
  2. ``assert_dev_db(conn)`` — ``current_database()`` must be in the
     ``EBULL_DEV_DB_NAMES`` allowlist (default ``ebull_dev``).
  3. ``assert_jobs_process_stopped(database_url)`` — refuses if the
     jobs entrypoint holds ``JOBS_PROCESS_LOCK_KEY`` on the
     application DB.

Plus the inverse probe used after dispatching a bootstrap:

  * ``wait_for_jobs_process_started(database_url, timeout_sec)`` —
    blocks until the operator-started jobs process acquires the
    fence. Raises ``RunbookRefused`` on timeout.

Each helper fails CLOSED. There is no soft default that lets an
unconfigured environment slip through. ``RunbookRefused`` is a
``SystemExit`` subclass with exit code 2 (invalid input / refused
precondition) so runbooks can re-raise without wrapping.
"""

from __future__ import annotations

import os
import time

import psycopg

from app.jobs.locks import probe_jobs_process_running


class RunbookRefused(SystemExit):
    """Raised by guards; carries an exit code 2 + an operator-actionable
    message printed by the runbook ``main()``."""

    def __init__(self, msg: str) -> None:
        super().__init__(2)
        self.msg = f"REFUSE: {msg}"


def assert_dev_env() -> None:
    """``EBULL_ENV`` must be explicitly ``'dev'``.

    Fail-closed: no default. An unset env var is refused (would
    otherwise silently pass on PROD machines that don't set EBULL_ENV).
    Caught in PR-D Codex 1 BLOCKING fold.
    """
    if os.environ.get("EBULL_ENV") != "dev":
        raise RunbookRefused(
            "EBULL_ENV must be explicitly set to 'dev'. Unset or non-dev refused (Codex 1 BLOCKING fold)."
        )


def assert_dev_db(conn: psycopg.Connection[object]) -> None:
    """``current_database()`` must be in the dev allowlist.

    EBULL_ENV='dev' alone is insufficient — a mis-set DATABASE_URL
    pointing at prod would pass the env check while connecting to
    prod. This second gate compares the actual ``current_database()``
    against ``EBULL_DEV_DB_NAMES`` (comma-separated, whitespace-
    tolerant; default ``ebull_dev``). Caught in PR-D round-1
    Operator-lens IMPORTANT fold.
    """
    row = conn.execute("SELECT current_database()").fetchone()
    raw_name = row[0] if row is not None else ""  # type: ignore[unreachable]
    name = str(raw_name) if raw_name else ""
    raw = os.environ.get("EBULL_DEV_DB_NAMES", "ebull_dev")
    allowlist = {tok.strip() for tok in raw.split(",") if tok.strip()}
    if name not in allowlist:
        raise RunbookRefused(
            f"current_database()={name!r} not in dev allowlist {sorted(allowlist)} (set EBULL_DEV_DB_NAMES to extend)."
        )


def assert_jobs_process_stopped(database_url: str) -> None:
    """Refuse if the jobs entrypoint holds ``JOBS_PROCESS_LOCK_KEY``.

    Probe is side-effect-free (acquire-and-release on a short-lived
    autocommit conn). PG advisory locks are PER-DATABASE in PG 9.0+
    so ``database_url`` must point at the same DB the jobs process
    uses. Caught in PR-D Codex 1 IMPORTANT 4 + round-2 Operator B1
    fold + commit 1 empirical correction.

    NOTE on TOCTOU: this is a point-in-time probe. The jobs process
    could be started by the operator after the probe returns but
    before subsequent destructive steps run. Callers that bracket a
    ``DROP DATABASE`` SHOULD additionally hold the fence via
    ``app.jobs.locks.acquire_jobs_process_fence`` for as long as PG
    permits (the fence dies with the DB drop; operator-policy MUST
    keep the jobs service stopped throughout the destructive phase).
    """
    if probe_jobs_process_running(database_url):
        raise RunbookRefused(
            "jobs process appears to be running (JOBS_PROCESS_LOCK_KEY held). "
            "Stop the jobs process (e.g. systemctl stop ebull-jobs) before "
            "running this runbook."
        )


def wait_for_jobs_process_started(
    database_url: str,
    *,
    timeout_sec: int = 600,
    poll_sec: int = 10,
) -> None:
    """Block until the jobs process acquires ``JOBS_PROCESS_LOCK_KEY``.

    Inverse of ``assert_jobs_process_stopped``: used by
    ``stream_a_run_8_verify`` after ``/system/bootstrap/run`` dispatch
    — the runbook releases its own fence, asks the operator to start
    the jobs service, then polls until the operator has done so before
    beginning the 90-min bootstrap-status poll. Without this gate, a
    runbook that polls ``/bootstrap-status`` against a stationary
    ``status='queued'`` would burn 90 min waiting for an orchestrator
    that nobody started. Caught in PR-D round-2 Operator B2 fold.

    Prints a heartbeat message every 30s of elapsed time.

    Raises :class:`RunbookRefused` on timeout. Operator can re-run
    later with the captured ``run_id``.
    """
    started = time.monotonic()
    deadline = started + timeout_sec
    next_heartbeat = started + 30
    while time.monotonic() < deadline:
        if probe_jobs_process_running(database_url):
            return
        if time.monotonic() >= next_heartbeat:
            real_elapsed = int(time.monotonic() - started)
            print(
                f"WAITING for jobs process to start ({real_elapsed}s elapsed, timeout at {timeout_sec}s)...",
                flush=True,
            )
            next_heartbeat += 30
        time.sleep(poll_sec)
    raise RunbookRefused(
        f"jobs process did not start within {timeout_sec}s. "
        f"Bootstrap is queued but no orchestrator to drain it. "
        f"Start jobs process; check status at /system/bootstrap-status."
    )
