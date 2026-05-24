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
from urllib.parse import urlparse

import psycopg

from app.config import settings
from app.jobs.locks import probe_jobs_process_running

# Default dev-DB allowlist when EBULL_DEV_DB_NAMES is unset. Matches the
# post-connection default in ``assert_dev_db`` so pre + post checks agree.
_DEFAULT_DEV_DB_ALLOWLIST: frozenset[str] = frozenset({"ebull_dev"})

# Multixact-wraparound probe thresholds. PG raises WARNING at
# ``autovacuum_multixact_freeze_max_age`` and ERROR at 2^31. Our
# operator-actionable threshold = 80% of the autovacuum freeze max age.
_MULTIXACT_FREEZE_RATIO = 0.8


class RunbookRefused(SystemExit):
    """Raised by guards; carries an exit code 2 + an operator-actionable
    message printed by the runbook ``main()``."""

    def __init__(self, msg: str) -> None:
        super().__init__(2)
        self.msg = f"REFUSE: {msg}"


def _parse_db_name_from_url(database_url: str) -> str:
    """Extract the DB name from a postgres URL. Handles ``postgres://``,
    ``postgresql://``, ``postgresql+psycopg://`` schemes.

    Returns empty string for malformed URLs (caller checks).
    """
    parsed = urlparse(database_url)
    path = parsed.path or ""
    return path.lstrip("/")


def _dev_db_allowlist() -> frozenset[str]:
    """Read ``EBULL_DEV_DB_NAMES`` env (comma-separated, whitespace-
    tolerant). Default to ``{"ebull_dev"}`` to match
    :func:`assert_dev_db` post-connection check.
    """
    raw = os.environ.get("EBULL_DEV_DB_NAMES")
    if raw is None:
        return _DEFAULT_DEV_DB_ALLOWLIST
    parsed = {tok.strip() for tok in raw.split(",") if tok.strip()}
    return frozenset(parsed) if parsed else _DEFAULT_DEV_DB_ALLOWLIST


def assert_dev_db_name_in_url() -> None:
    """Pre-connection variant of :func:`assert_dev_db`.

    Parses the DB name out of ``settings.database_url`` and validates
    it against ``EBULL_DEV_DB_NAMES`` (default ``{"ebull_dev"}``). Fails
    BEFORE any psycopg connection attempt — operator gets an
    actionable error before the deep-stack ``OperationalError`` that
    would otherwise come from a wrong ``DATABASE_URL``.

    Belt-and-braces with :func:`assert_dev_db` post-connection. Codex 1
    diff re-pass caught: default match must equal post-check default
    (``{"ebull_dev"}``) — previously "skip with warning" would let
    ``DATABASE_URL=postgres://.../ebull`` pass pre-check and only fail
    after connection.
    """
    url = settings.database_url
    name = _parse_db_name_from_url(url)
    if not name:
        raise RunbookRefused(
            f"DATABASE_URL has no database name in path: {url!r}. Expected postgres://USER:PASS@HOST:PORT/DBNAME shape."
        )
    allowlist = _dev_db_allowlist()
    if name not in allowlist:
        raise RunbookRefused(
            f"DATABASE_URL points at database {name!r}, not in dev allowlist "
            f"{sorted(allowlist)}. Set EBULL_DEV_DB_NAMES env var to extend, "
            f"or point DATABASE_URL at a dev DB."
        )


def assert_dev_env() -> None:
    """``EBULL_ENV`` must be explicitly ``'dev'``.

    Fail-closed: no default. An unset env var is refused (would
    otherwise silently pass on PROD machines that don't set EBULL_ENV).
    Caught in PR-D Codex 1 BLOCKING fold.

    **Companion guard**: callers SHOULD also invoke
    :func:`assert_dev_db_name_in_url` immediately after this — they
    validate orthogonal failure modes (env var vs URL shape) and are
    intentionally separate so each can be unit-tested in isolation
    without leaking ``DATABASE_URL`` state into env-var tests.
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


def assert_no_multixact_wraparound(conn: psycopg.Connection[object]) -> None:
    """Catalog-level probe for multixact-wraparound proximity.

    Reads:

    * ``pg_database.datminmxid`` for the current DB — compares against
      ``autovacuum_multixact_freeze_max_age`` × ``_MULTIXACT_FREEZE_RATIO``
      (default 0.8). Approaching threshold → refuse.
    * Top-5 oldest ``pg_class.relminmxid`` in ``public`` schema —
      same threshold per table.

    Best-effort symptom probe against the historical victim tables
    (``job_runtime_heartbeat`` + ``broker_credentials``) supplements
    the catalog check as a non-fatal warning.

    Why: PR12 #1255 + the §6.3 pre-wipe procedure in
    ``docs/specs/etl/retention-rubric.md`` (formerly
    ``docs/superpowers/specs/2026-05-19-data-retention-rubric.md``)
    document the ``pg_resetwal``-damaged dev DB state with multixact
    wraparound on ``job_runtime_heartbeat`` + ``broker_credentials``.
    Without this probe, the operator runs ``--apply`` cold and hits
    the wraparound mid-DROP — partial nuke + manual recovery. Refusing
    with an actionable error before any destructive op is the gate.

    Raises :class:`RunbookRefused` if any tracked age exceeds
    threshold. Operator action: run the §6.3 pre-wipe procedure
    (``pg_resetwal --next-multixact ...`` + manual reclaim) BEFORE
    re-running this runbook.
    """
    cur = conn.execute("SHOW autovacuum_multixact_freeze_max_age")
    raw = cur.fetchone()
    if raw is None:
        # Unreachable on a live PG, but defensive
        return
    freeze_max_age = int(raw[0])  # type: ignore[arg-type]
    threshold = int(freeze_max_age * _MULTIXACT_FREEZE_RATIO)

    cur = conn.execute("SELECT mxid_age(datminmxid)::BIGINT FROM pg_database WHERE datname = current_database()")
    db_row = cur.fetchone()
    if db_row is not None:
        db_age = int(db_row[0])  # type: ignore[arg-type]
        if db_age >= threshold:
            raise RunbookRefused(
                f"pg_database.datminmxid age = {db_age} for current DB; "
                f"threshold = {threshold} (80% of "
                f"autovacuum_multixact_freeze_max_age = {freeze_max_age}). "
                f"Multixact wraparound proximity detected. Run the §6.3 "
                f"pre-wipe procedure (see docs/specs/etl/retention-rubric.md "
                f"+ project_1233_pr12_ownership_merge_writer.md) BEFORE "
                f"re-running this runbook."
            )

    cur = conn.execute(
        "SELECT n.nspname || '.' || c.relname AS qname, "
        "mxid_age(c.relminmxid)::BIGINT AS age "
        "FROM pg_class c "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'public' "
        "  AND c.relkind IN ('r', 'p') "
        "  AND c.relminmxid <> '0' "
        "ORDER BY age DESC LIMIT 5"
    )
    rows = cur.fetchall()
    breaches = [(qname, age) for qname, age in rows if int(age) >= threshold]
    if breaches:
        formatted = ", ".join(f"{name}=age{age}" for name, age in breaches)
        raise RunbookRefused(
            f"pg_class.relminmxid wraparound proximity (threshold={threshold}) "
            f"on tables: {formatted}. Run §6.3 pre-wipe procedure "
            f"(see docs/specs/etl/retention-rubric.md + "
            f"project_1233_pr12_ownership_merge_writer.md) BEFORE "
            f"re-running this runbook."
        )
