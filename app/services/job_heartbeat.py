"""The ``job_runs`` heartbeat producer (#2274).

``job_runs.processed_count`` / ``target_count`` / ``last_progress_at``
(``sql/140_per_run_progress_telemetry.sql``) have had two consumers in
production and, until this module, one producer covering one job:

* ``stale_detection`` rule 4 ``mid_flight_stuck`` — keyed on
  ``COALESCE(last_progress_at, started_at)``;
* ``app/jobs/dev_reload.py::live_job`` — defers an AUTOMATIC reload while
  a ``running`` row heartbeats inside that job's own threshold, instead
  of SIGKILLing it.

``_LIVE_JOB_SQL`` requires ``last_progress_at IS NOT NULL``, so that
second protection had never applied to anything but
``strategy_backtest_run``. Every other long job was still preempted by an
ordinary merge re-detaching the checkout — the incident ``dev_reload``'s
own docstring records (three consecutive backtest runs destroyed in an
hour) and the one ``docs/review-prevention-log.md:1353`` records on the
candle path (four consecutive sweeps, 100% of their work lost).

The tick mechanism was already wired: bodies call
``sync_orchestrator.progress.report_progress``. Only the LISTENER was
missing — ``set_active_progress`` has a single caller
(``sync_orchestrator/adapters.py``), so on an APScheduler fire the
ContextVar is unset and ``report_progress`` returns at its first line.
This module is that listener, installed by ``_tracked_job``, which is
where the ``job_runs`` row is owned.

Spec: ``docs/proposals/ops/2026-09-15-2274-job-heartbeat-writer.md``.
Ordering: ``docs/proposals/ops/2026-09-14-2274-tracked-job-heartbeat.md``
§3 — the wall-clock ceiling (``stale_detection`` rule 5, shipped
``7f8d2b99``) had to land FIRST, because rule 4 mutes the instant a
producer ticks and rule 5 measures the run's age and cannot be muted.
"""

from __future__ import annotations

import contextlib
import logging
import time
from collections.abc import Iterator

from psycopg import sql

from app.db.background_write import background_write_connection, get_background_pool
from app.services.sync_orchestrator.progress import (
    active_progress_callback,
    clear_active_progress,
    set_active_progress,
)
from app.services.sync_orchestrator.types import ProgressCallback

logger = logging.getLogger(__name__)

# Minimum wall-clock gap between two heartbeat WRITES, per run.
#
# ⚠ ``report_progress``'s own throttle (5 items or 10s) does NOT bound
# this: ``force=True`` bypasses it entirely, and four of our covered tick
# sites use it — in ``market_data.refresh_market_data`` and in
# ``fundamentals.refresh_financial_facts`` / ``fundamentals.execute_refresh``.
# A writer that inherited the caller's cadence would inherit an unbounded
# one. Re-derive the set with ``rg -n 'force=True' app/`` rather than
# trusting a line number here.
#
# 5s is the cadence ``docs/proposals/ui/admin-control-hub-rewrite.md``
# §A3 already documents for job telemetry, so this reconciles with the
# existing contract rather than inventing a second one.
#
# Consequence, stated rather than engineered around: ``processed_count``
# may lag the true count by up to this interval, including at run end.
# Both consumers read the TIMESTAMP, so that is immaterial to every
# verdict either of them produces.
_MIN_WRITE_INTERVAL_S: float = 5.0

# Bound on the heartbeat UPDATE itself.
#
# ⚠⚠ Load-bearing, not decoration. The callback runs SYNCHRONOUSLY inside
# the job body, so an UPDATE that blocks on a lock blocks the work it is
# reporting on. Neither ``background_write_connection`` nor the pool sets
# a statement timeout (Codex ckpt-1), so the writer sets its own —
# mirroring ``dev_reload.live_job``, which bounds its probe for the same
# reason.
#
# ⚠ ``SET LOCAL`` (not ``options=``) is correct HERE and does not
# contradict ``app/jobs/job_connection.py``'s verified note that a plain
# ``SET`` "IS reverted on ROLLBACK". That note is about job bodies, which
# are non-autocommit and roll back on close. This writer commits, and if
# its transaction ever rolled back the UPDATE would be gone too — so the
# timeout reverting with it costs nothing. ``options=`` is not available
# on a pooled connection in any case.
#
# ⚠ PUBLIC, and shared on purpose (#3111 slice 2). The rule generalises past
# this writer: any write that REPORTS on a job body, rather than doing its
# work, must not outlast it. ``sec_manifest_worker``'s telemetry flush is the
# second such writer and imports this rather than restating 3_000 — one bound,
# one place to change it.
#
# ⚠ A third copy already exists at ``sync_orchestrator/executor.py``
# (``_RUN_HEARTBEAT_STATEMENT_TIMEOUT_MS``), whose own comment says it mirrors
# this one. Pre-dates this constant going public and is left alone rather than
# folded in on an unrelated ticket.
REPORTING_WRITE_TIMEOUT_MS: int = 3_000

_SET_TIMEOUT_SQL = sql.SQL("SET LOCAL statement_timeout = {ms}").format(ms=sql.Literal(REPORTING_WRITE_TIMEOUT_MS))

_UPDATE_SQL = """
    UPDATE job_runs
       SET processed_count  = %(processed)s,
           target_count     = %(target)s,
           last_progress_at = now()
     WHERE run_id = %(run_id)s AND status = 'running'
"""


class JobRunHeartbeat:
    """``ProgressCallback`` that keeps one ``job_runs`` row's heartbeat fresh.

    Chains to ``inner`` (the callback that was active at install time) so
    installing this does not silence the sync-orchestrator's own progress
    surface — the orchestrator installs OUTSIDE ``legacy_fn()`` and
    ``_tracked_job`` opens INSIDE it.

    Each side is guarded independently: an orchestrator-side failure must
    not cost the heartbeat write, and a heartbeat failure must not cost
    the orchestrator tick. Neither may reach the job body —
    ``report_progress``'s contract is that "progress reporting must never
    abort the underlying work".
    """

    def __init__(self, run_id: int, inner: ProgressCallback | None) -> None:
        self._run_id = run_id
        self._inner = inner
        self._last_write = 0.0
        self._warned = False

    @property
    def inner(self) -> ProgressCallback | None:
        """The chained callback. Read by :func:`job_heartbeat` so a nested
        install can chain PAST this heartbeat rather than to it — see
        ``_resolve_inner``."""
        return self._inner

    def __call__(self, items_done: int, items_total: int | None = None) -> None:
        if self._inner is not None:
            try:
                self._inner(items_done, items_total)
            except Exception:
                logger.debug("chained progress callback raised", exc_info=True)

        now = time.monotonic()
        if self._last_write and now - self._last_write < _MIN_WRITE_INTERVAL_S:
            return
        # Advance the floor on ATTEMPT, not on success, so a sustained DB
        # fault cannot turn every tick into a retry — the same reasoning
        # ``report_progress`` applies to its own throttle state.
        self._last_write = now
        self._write(items_done, items_total)

    def _write(self, items_done: int, items_total: int | None) -> None:
        try:
            # autocommit=True is the seam's DEFAULT and it is deliberate.
            # ``checkpoint_progress`` asked for autocommit=False for a
            # single UPDATE and the pooled seam rolled the write back
            # before returning the connection — the bug 3f3c3517 fixed on
            # this same ticket. Under the default, ``conn.transaction()``
            # issues a real BEGIN/COMMIT, so the write lands and that
            # failure mode cannot be acquired.
            with background_write_connection() as conn:
                with conn.transaction():
                    # SET LOCAL reverts at COMMIT, so no pooled connection
                    # is left carrying a mutated timeout.
                    conn.execute(_SET_TIMEOUT_SQL)
                    conn.execute(
                        _UPDATE_SQL,
                        {
                            "processed": items_done,
                            "target": items_total,
                            "run_id": self._run_id,
                        },
                    )
        except Exception:
            # Deliberately NOT latching. ``_BacktestProgressWriter``
            # disables itself after one fault because it owns a
            # long-lived connection a fault may have poisoned; this one
            # borrows per tick, so a blip poisons nothing and latching
            # off would lose the signal for the remaining hours of
            # exactly the long run this exists to protect.
            if not self._warned:
                self._warned = True
                logger.warning("job heartbeat write failed for run_id=%d", self._run_id, exc_info=True)
            else:
                logger.debug("job heartbeat write failed for run_id=%d", self._run_id, exc_info=True)


def _resolve_inner() -> ProgressCallback | None:
    """The callback a new heartbeat should chain to.

    ⚠⚠ A heartbeat NEVER chains to another heartbeat. ``fundamentals_sync``
    calls ``daily_financial_facts()`` directly (``scheduler.py:5410``), so
    a ``_tracked_job`` genuinely nests inside another one. Chaining child
    to parent would fill the PARENT's ``processed_count`` with the child's
    item counts — a fabricated measurement of a different unit of work.
    Chaining past it leaves the parent's ``last_progress_at`` NULL, so
    rule 4 falls back to ``started_at`` for it: the status quo, not a
    regression.
    """
    active = active_progress_callback()
    while isinstance(active, JobRunHeartbeat):
        active = active.inner
    return active


@contextlib.contextmanager
def job_heartbeat(run_id: int) -> Iterator[None]:
    """Install a ``job_runs`` heartbeat for ``run_id`` for the duration of the body.

    A no-op — installing nothing at all — when either:

    * ``run_id <= 0``: ``_tracked_job``'s start-failure path runs the job
      body with no row to write to; or
    * no background pool is registered.

    ⚠⚠ The pool check is an OWNERSHIP predicate, and it is the
    predecessor spec's unanswered finding. ``_LIVE_JOB_SQL`` carries no
    ownership column, and ``dev_reload``'s docstring justifies that with
    "the singleton fence means there is exactly one jobs process, so a
    freshly-heartbeating ``running`` row is necessarily the current
    child's". True for ``strategy_backtest_run``, which only runs in the
    daemon. NOT true for a heartbeat in ``_tracked_job``, which a CLI
    invocation, a script or a test also enters — any of which could then
    defer the supervisor's reload from outside the child it supervises.
    ``set_background_pool`` is called from exactly one place
    (``app/jobs/__main__.py``), so pool presence IS "am I the daemon",
    and gating on it restores the premise the consumer already relies on.

    Never swallows the body's exception: the token is restored in a
    ``finally`` and the exception propagates unchanged.
    """
    if run_id <= 0 or get_background_pool() is None:
        yield
        return

    token = set_active_progress(JobRunHeartbeat(run_id, _resolve_inner()), initial_tick=False)
    try:
        yield
    finally:
        clear_active_progress(token)


__all__ = ["JobRunHeartbeat", "job_heartbeat"]
