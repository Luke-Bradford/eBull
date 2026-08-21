"""Dev-only auto-reload supervisor for the jobs daemon (#2144).

Runs as ``python -m app.jobs.dev_reload``. Watches ``app/**/*.py`` and
restarts the real jobs daemon (``python -m app.jobs``, spawned as a child
process) whenever a source file changes — so a merge to a scheduler /
ingest / parser / derivation module goes live without the operator
manually restarting the VS Code ``stack: jobs`` task. Mirrors what
uvicorn ``--reload`` already gives the API process.

Outside a dev-like ``app_env`` this module is a pass-through: it logs
once and runs ``app.jobs.__main__.main()`` IN-PROCESS, so pointing a
production launcher at it changes nothing — no watcher, no extra
process, no behavioural delta.

Why a hand-rolled supervisor and not ``watchfiles.run_process``
--------------------------------------------------------------
``run_process`` caps shutdown at ``sigint_timeout=5`` + ``sigkill_timeout=1``
— six seconds, then SIGKILL. The jobs daemon's graceful drain routinely
runs past two minutes (it waits for in-flight syncs/ingests to finish),
so ``run_process`` would hard-kill mid-job on every reload. #2144
explicitly requires the SIGTERM drain semantics stay intact, so we drive
the child's lifecycle ourselves and only escalate to SIGKILL after
``_DRAIN_TIMEOUT_S``. We still use watchfiles for the file watching —
it is already resolved in ``uv.lock`` as a ``uvicorn[standard]`` extra.

Singleton-fence ordering (load-bearing)
---------------------------------------
The daemon acquires a Postgres advisory lock as a singleton fence and
FATAL-exits at boot if it is already held (``app/jobs/__main__.py``
step 3). A restart must therefore be strictly sequential: the old child
is fully reaped — releasing the lock — BEFORE the replacement is
spawned. Never spawn-then-kill. After a SIGKILL escalation we also
settle for ``_POST_KILL_SETTLE_S`` so Postgres notices the dead session
and drops the lock, mirroring ``stack-restart.sh``'s ``kill_jobs_process``.

Deferring a reload past a live job (#2274)
------------------------------------------
An automatic reload used to preempt the child unconditionally. A job
that cannot drain inside ``_DRAIN_TIMEOUT_S`` — which a multi-hour
corpus run or backtest never can — was therefore SIGKILLed, and the
next boot's reaper wrote it off as ``orphaned: reaped at boot``. On
2026-08-21 that destroyed three consecutive ``strategy_backtest_run``
attempts inside one hour; the last died 8 seconds after its own
heartbeat, i.e. while healthy and 12,625 of 17,290 targets through.
The trigger each time was an ordinary merge re-detaching the checkout
at ``origin/main``, which is exactly how a merged change is *meant* to
reach this daemon — so the loop's own merge cadence made a long run
unfinishable.

So a reload now WAITS for a job that is provably alive instead of
killing it. "Provably alive" is a claim the job volunteers, not one we
infer: a ``job_runs`` row in ``status='running'`` whose
``last_progress_at`` is inside that job's own staleness threshold, as
already defined for the admin console by
``app/services/processes/stale_thresholds.py``. Deliberately NOT an age
cut — age is a proxy for liveness and would protect a wedged job
forever. A job that stops heartbeating goes stale and stops blocking
reloads on its own, which is why this needs no watchdog and cannot
false-fire on a legitimately-long run (#2274's own constraint).

This is a correctness fix as much as an availability one. Had a drain
mid-run actually succeeded, the surviving rows would carry a
``strategy_version`` derived from code that changed underneath the run.

Three properties are load-bearing:

* **Fail open.** Any probe failure — Postgres down, slow, schema drift —
  reloads as before. A supervisor must never be blockable by its own
  liveness check, or a DB blip freezes the daemon on stale code.
* **Ownership is free.** The singleton fence means there is exactly one
  jobs process, so a freshly-heartbeating ``running`` row is necessarily
  the current child's. No PID plumbing.
* **Only AUTOMATIC reloads defer.** An explicit stop (SIGTERM to the
  supervisor, operator stopping the task) still drains-then-kills
  immediately. Deferral is about not letting a file-watcher preempt
  hours of work; it is not a veto on the operator.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Iterable
from pathlib import Path
from typing import Final

from app.config import DEV_LIKE_ENVS, settings
from app.services.processes.stale_thresholds import (
    DEFAULT_THRESHOLD_S,
    get_threshold,
    overridden_process_ids,
)

logger = logging.getLogger(__name__)

# Repo root: app/jobs/dev_reload.py -> app/jobs -> app -> <repo root>.
_APP_DIR: Final[Path] = Path(__file__).resolve().parents[1]
_REPO_ROOT: Final[Path] = _APP_DIR.parent

# How long to let the child drain after SIGTERM before escalating to
# SIGKILL. The daemon's own shutdown waits on in-flight jobs; observed
# drains have exceeded two minutes, so three is the escalation floor
# rather than a target (a healthy idle daemon exits in well under a
# second).
_DRAIN_TIMEOUT_S: Final[float] = 180.0

# Bound on reaping the corpse after SIGKILL — a killed process is
# already gone; this only covers the wait() round-trip.
_KILL_REAP_TIMEOUT_S: Final[float] = 10.0

# How often to report progress while draining (#2666). A drain that runs
# the full _DRAIN_TIMEOUT_S is three minutes of silence between "SIGTERM
# -> pid N" and the respawn, and an observer polling PIDs across that
# window sees an unchanged PID and concludes the reload never fired. It
# did; it is mid-drain. Periodic INFO makes "still working" distinguish
# itself from "never started".
_DRAIN_PROGRESS_S: Final[float] = 15.0

# How much longer to wait when the drain budget expires but a job is
# still provably alive (#2274, Codex checkpoint 2 round 4). Also the
# re-probe cadence in that state, since each extension ends in another
# probe. Bounded by liveness rather than wall-clock: the extension stops
# the moment the heartbeat goes stale.
_DRAIN_EXTENSION_S: Final[float] = 60.0

# After a SIGKILL the fence connection is closed by the OS, not by the
# daemon, so Postgres only drops the advisory lock once it notices the
# dead session. Same one-second settle stack-restart.sh uses.
_POST_KILL_SETTLE_S: Final[float] = 1.0

# Watcher tick. watchfiles yields an empty set on timeout (with
# yield_on_timeout=True), which is how we poll for child death and for
# our own stop_event without a second thread.
_WATCH_TICK_MS: Final[int] = 1000

_CHILD_ARGV: Final[list[str]] = [sys.executable, "-m", "app.jobs"]

# How recent a job's heartbeat must be for it to count as alive and so
# block an automatic reload (#2274).
#
# NOT a fresh choice, and NOT a local constant: the repo already owns
# this rule in `app/services/processes/stale_thresholds.py`, which the
# admin console's mid_flight_stuck detection reads. Default 300s with
# per-job overrides to 1800s for slow-tick producers (SEC bulk seeds,
# the insider backfill) whose natural inter-tick gap exceeds five
# minutes. Hard-coding the 300s default here would have made those eight
# jobs "healthy" to the console and "stale" to the supervisor at the
# same moment — i.e. still SIGKILL-able while nothing reported them at
# risk (Codex checkpoint 2).
#
# The registry travels INTO the query as a jsonb map so the per-job cut
# is applied by Postgres, before any row limit. Filtering in Python
# after a `LIMIT` is unsafe in the direction that matters: 50 fresh
# rows that are stale by their own 300s cut would mask an older but
# still-live 1800s job and return "nothing to defer" — preempting
# exactly the run this change protects (Codex checkpoint 2, round 3).
#
# ⚠ The registry is keyed by admin `process_id` and this query matches on
# `job_runs.job_name`, which is NOT a bug: `process_id` is the
# `ScheduledJob.name` verbatim for everything that owns a job_runs row.
# The three keys that do not match (`bootstrap`, `sec_13f_sweep`,
# `nport_sweep`) are exactly the three that are not scheduled jobs and so
# can never produce one — the registry's own docstring says sweeps "have
# no own active_run". Translating them to `sec_13f_quarterly_sweep` /
# `sec_n_port_ingest` (Codex checkpoint 2, round 6) would add keys that
# match nothing either. Reproduce with:
#   python -c "from app.workers.scheduler import SCHEDULED_JOBS; \
#   from app.services.processes.stale_thresholds import overridden_process_ids; \
#   n={j.name for j in SCHEDULED_JOBS}; print({k: k in n for k in overridden_process_ids()})"
_THRESHOLD_OVERRIDES_JSON: Final[str] = json.dumps(
    {process_id: get_threshold(process_id) for process_id in sorted(overridden_process_ids())}
)

# How often to re-probe while a reload sits deferred. The watcher ticks
# at _WATCH_TICK_MS (1s); probing on every tick would open a connection
# per second for the hours a corpus run lasts.
_LIVE_JOB_PROBE_PERIOD_S: Final[float] = 15.0

# How often to restate a deferral. Same reasoning as _DRAIN_PROGRESS_S:
# a reload deferred for hours must not be silent, or an observer polling
# PIDs concludes the watcher is dead rather than waiting on purpose.
_DEFER_PROGRESS_S: Final[float] = 60.0

# Bound on the probe's own connection. The probe is advisory — a slow
# DB must degrade to "reload anyway", never stall the supervisor.
_LIVE_JOB_CONNECT_TIMEOUT_S: Final[int] = 5

# ``connect_timeout`` bounds only the CONNECT. Once connected, a query
# can wait indefinitely on a lock or a stalled server, and because the
# supervisor probes on its own thread that would stop it servicing
# watcher ticks entirely — a wedge, wearing fail-open's clothes (Codex
# checkpoint 2). Bounded with a libpq STARTUP option for the reason
# `app/jobs/job_connection.py` documents: `options=` is applied outside
# any transaction, so it cannot be undone by a ROLLBACK and opens no
# implicit transaction. The cancel surfaces as an exception, which the
# probe's own guard turns into "no live job".
_LIVE_JOB_STATEMENT_TIMEOUT_MS: Final[int] = 3_000

# Every row this returns is already live by its OWN threshold, so LIMIT 1
# is safe: it picks a blocker to name, it does not decide whether one
# exists. Freshest heartbeat first — the row that ticked most recently is
# the most likely to still be alive and the most useful to name.
_LIVE_JOB_SQL: Final[str] = """
    SELECT run_id,
           job_name,
           processed_count,
           target_count,
           EXTRACT(EPOCH FROM (now() - last_progress_at)) AS heartbeat_age_s
      FROM job_runs
     WHERE status = 'running'
       AND last_progress_at IS NOT NULL
       AND last_progress_at > now() - make_interval(
               secs => COALESCE((%(overrides)s::jsonb ->> job_name)::int, %(default_s)s)
           )
     ORDER BY last_progress_at DESC
     LIMIT 1
"""


def live_job() -> str | None:
    """Describe the in-flight job that should defer a reload, else ``None``.

    Returns a human-readable one-liner (job name, run id, progress,
    heartbeat age) so the deferral log names what it is waiting for —
    "reload deferred" with no subject is the kind of message an operator
    learns to ignore.

    ⚠ Never raises. Every failure path returns ``None``, which reloads
    as the pre-#2274 code did. The probe is an optimisation on top of
    correct-but-destructive behaviour, so a broken probe must degrade to
    the old behaviour rather than wedge the supervisor on stale code.
    Deliberately not ``app.jobs.job_connection.connect_job``: that helper
    exists to apply the per-job ``statement_timeout`` ContextVar set by
    ``_tracked_job``, and the supervisor is not a job body.
    """
    try:
        import psycopg

        with psycopg.connect(
            settings.database_url,
            connect_timeout=_LIVE_JOB_CONNECT_TIMEOUT_S,
            options=f"-c statement_timeout={_LIVE_JOB_STATEMENT_TIMEOUT_MS}",
            autocommit=True,
        ) as conn:
            row = conn.execute(
                _LIVE_JOB_SQL,
                {"overrides": _THRESHOLD_OVERRIDES_JSON, "default_s": DEFAULT_THRESHOLD_S},
            ).fetchone()
    except Exception:
        # Intentionally broad: a probe is not worth an exception class.
        logger.debug("jobs dev-reload: live-job probe failed; treating as no live job", exc_info=True)
        return None
    if row is None:
        return None
    run_id, job_name, processed, target, heartbeat_age_s = row
    progress = f"{processed}/{target}" if target is not None else str(processed)
    return f"{job_name} run {run_id} ({progress} done, heartbeat {float(heartbeat_age_s):.0f}s ago)"


def changes_are_stale(paths: Iterable[str], spawn_time: float) -> bool:
    """True when every changed path was last modified before ``spawn_time``.

    The rust watcher keeps collecting while we block on a drain, so the
    batch delivered right after a restart usually replays the very edits
    that *caused* it. The child we just spawned already read those files,
    so restarting again would be pure cost (another full drain).

    A path that no longer exists is NOT treated as stale (Codex pre-push
    review). A deletion has no mtime to compare, so suppressing on it
    would silently skip the reload for a deleted or renamed module — the
    daemon would keep running the old code until some unrelated edit
    happened to land. Deletions are rare next to edits, so forcing a
    reload costs at most one redundant restart while the suppression
    still does its job on the common edit path. Running stale code
    silently is the worse failure.

    An empty batch is NOT stale either: callers use empty batches as the
    watcher's idle tick, and they must not be mistaken for a suppressed
    reload.
    """
    paths = list(paths)
    if not paths:
        return False
    for raw in paths:
        try:
            if os.stat(raw).st_mtime > spawn_time:
                return False
        except OSError:
            return False
    return True


def running_commit() -> str:
    """The commit the child is about to run, or ``"unknown"`` (#2666).

    The supervisor's own output is then enough to answer "which commit is the
    running worker on", which is the property that was missing: PID polling
    across a slow drain cannot distinguish a reload in progress from one that
    never fired, and re-detaching the main checkout at ``origin/main`` is exactly
    how a merged service change is meant to reach this daemon.

    ⚠ ``GIT_*`` is scrubbed from the child environment.  A git hook exports
    ``GIT_DIR`` (and friends) into everything it runs, so this call would resolve
    against the HOOK's repository rather than ``_REPO_ROOT`` whenever the daemon
    -- or a test exercising it -- is reached from under one.  Same trap as #2658.

    Never raises: a missing git, a non-repo checkout or a slow disk degrades to
    ``"unknown"``.  A reload must not be blocked by its own log line.
    """
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=_REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=5.0,
            env=env,
            check=False,
        )
    except OSError, subprocess.SubprocessError:
        return "unknown"
    if result.returncode != 0:
        return "unknown"
    return result.stdout.strip() or "unknown"


def _spawn_child() -> tuple[subprocess.Popen[bytes], float]:
    """Spawn the daemon, returning it with the wall-time it was started.

    The timestamp is taken BEFORE ``Popen`` (review nitpick on PR #2156).
    Reading it afterwards would place it slightly late, so an edit landing
    in the fork/exec window would compare as older than the child and be
    suppressed — the daemon would then run code it never read. Sampling
    early biases the other way: such an edit reads as newer and forces one
    extra reload. Same principle as the deleted-path case — a redundant
    restart is always preferable to silently-stale code.
    """
    logger.info("jobs dev-reload: starting %s at commit %s", " ".join(_CHILD_ARGV), running_commit())
    spawn_time = time.time()
    return subprocess.Popen(_CHILD_ARGV, cwd=_REPO_ROOT), spawn_time


def _stop_child(
    proc: subprocess.Popen[bytes],
    *,
    extend_while_live: bool = False,
    stop_event: threading.Event | None = None,
) -> None:
    """SIGTERM the child and WAIT for it to die, escalating if it hangs.

    Returns only once the process has been reaped, so the caller is safe
    to spawn a replacement (see the singleton-fence note above).

    ``extend_while_live`` closes the gap between the supervisor's liveness
    probe and this SIGTERM (Codex checkpoint 2, round 4). The scheduler can
    start a long job in that window, and it would then be SIGKILLed at the
    drain budget — the very loss this change exists to prevent. With the
    flag set, expiry of the budget re-probes instead of escalating, and
    extends by ``_DRAIN_EXTENSION_S`` for as long as a job is provably
    alive. Bounded by liveness, not by wall-clock: a job that stops
    heartbeating goes stale and the escalation proceeds.

    It also fixes the original bug from the other side. A fixed 180s cap is
    what turns a graceful shutdown into data loss, because the drain a
    long job needs is longer than any constant that is safe to wait on
    blindly — the heartbeat is what makes waiting safe.

    Left OFF for the supervisor's own shutdown: an explicit stop drains
    and kills on the budget, because deferral must not become a veto on
    the operator stopping the stack.

    ⚠ That is not enough on its own, which is why ``stop_event`` is also
    threaded in (Codex checkpoint 2, round 5). A shutdown signal can
    arrive while an AUTOMATIC reload is already inside this function, and
    the handler that sets the event runs on the watcher's frame, not
    this one — so without the event here the extension loop keeps
    extending and the operator cannot stop the stack for as long as the
    job keeps heartbeating. Once the event is set, extensions stop and
    the escalation proceeds on the current budget.
    """
    if proc.poll() is not None:
        return
    logger.info("jobs dev-reload: SIGTERM -> pid %d, draining (max %.0fs)", proc.pid, _DRAIN_TIMEOUT_S)
    started = time.monotonic()
    try:
        proc.send_signal(signal.SIGTERM)
    except ProcessLookupError:
        proc.wait()
        return
    # Wait in _DRAIN_PROGRESS_S slices rather than one long block, so a slow
    # drain reports itself (#2666).  The deadline is computed once: slicing must
    # not extend the total budget, which is what a naive per-slice timeout would
    # do.  The ONLY thing that may move it is a proven-live job, below.
    deadline = started + _DRAIN_TIMEOUT_S
    drained = False
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            shutting_down = stop_event is not None and stop_event.is_set()
            if extend_while_live and shutting_down:
                logger.info(
                    "jobs dev-reload: shutdown requested mid-drain — no longer extending pid %d",
                    proc.pid,
                )
            if extend_while_live and not shutting_down:
                blocker = live_job()
                if blocker is not None:
                    logger.info(
                        "jobs dev-reload: pid %d past its %.0fs drain budget but %s is live — "
                        "extending %.0fs rather than escalating",
                        proc.pid,
                        _DRAIN_TIMEOUT_S,
                        blocker,
                        _DRAIN_EXTENSION_S,
                    )
                    deadline = time.monotonic() + _DRAIN_EXTENSION_S
                    continue
            break
        try:
            proc.wait(timeout=min(_DRAIN_PROGRESS_S, remaining))
            drained = True
            break
        except subprocess.TimeoutExpired:
            logger.info(
                "jobs dev-reload: pid %d still draining, %.0fs of %.0fs elapsed",
                proc.pid,
                time.monotonic() - started,
                _DRAIN_TIMEOUT_S,
            )
    if drained:
        logger.info("jobs dev-reload: drained cleanly in %.1fs", time.monotonic() - started)
        return
    logger.warning(
        "jobs dev-reload: pid %d still draining after %.0fs — escalating to SIGKILL",
        proc.pid,
        time.monotonic() - started,
    )
    proc.kill()
    try:
        proc.wait(timeout=_KILL_REAP_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        logger.error("jobs dev-reload: pid %d unreaped after SIGKILL", proc.pid)
    time.sleep(_POST_KILL_SETTLE_S)


def _supervise() -> int:
    # Imported lazily, INSIDE the dev-only path: watchfiles reaches us as a
    # `uvicorn[standard]` extra (and is declared in the dev dependency-group
    # for the direct import), so the non-dev pass-through in main() must not
    # depend on it being installed.
    from watchfiles import PythonFilter, watch

    stop_event = threading.Event()

    def _handler(signum: int, _frame: object) -> None:
        logger.info("jobs dev-reload: received signal %d, shutting down", signum)
        stop_event.set()

    for name in ("SIGTERM", "SIGINT", "SIGBREAK"):
        sig = getattr(signal, name, None)
        if sig is not None:
            try:
                signal.signal(sig, _handler)
            except ValueError, OSError:
                logger.debug("jobs dev-reload: signal %s not registrable on this platform", name)

    logger.info("jobs dev-reload: watching %s for *.py changes", _APP_DIR)
    child, spawn_time = _spawn_child()
    crash_reported = False

    # Changes seen but not yet applied, because a live job is holding the
    # reload off (#2274). A dict keeps insertion order for the "e.g."
    # message while de-duplicating: a deferral lasting hours would
    # otherwise re-accumulate the same paths on every save.
    pending: dict[str, None] = {}
    deferred_since: float | None = None
    last_probe = 0.0
    last_defer_log = 0.0

    try:
        for changes in watch(
            _APP_DIR,
            watch_filter=PythonFilter(),
            stop_event=stop_event,
            rust_timeout=_WATCH_TICK_MS,
            yield_on_timeout=True,
            raise_interrupt=False,
        ):
            if stop_event.is_set():
                break

            if changes:
                paths = [path for _change, path in changes]
                if changes_are_stale(paths, spawn_time):
                    logger.debug("jobs dev-reload: ignoring %d change(s) older than the running child", len(paths))
                    continue
                pending.update(dict.fromkeys(paths))
            else:
                # Idle tick — the only place we notice the child dying on
                # its own. A clean exit means somebody stopped the daemon
                # deliberately, so we stop too. A crash (bad import after a
                # merge, unhandled boot error) leaves the supervisor alive
                # so the next edit can bring it back without the operator
                # re-running the task.
                code = child.poll()
                if code is not None:
                    if code == 0:
                        logger.info("jobs dev-reload: child exited 0; supervisor exiting")
                        return 0
                    if not crash_reported:
                        # Once per crash, not once per tick.
                        crash_reported = True
                        logger.error(
                            "jobs dev-reload: child exited %d — staying up; next *.py change will respawn",
                            code,
                        )
                    if pending:
                        # The child died holding changes it never read, so
                        # the deferral's reason died with it — respawn now
                        # rather than wait for an unrelated edit. Observed
                        # for real: the jobs child took a bare SIGKILL
                        # (`child exited -9`) mid-backtest on 2026-08-21,
                        # which is exactly this path.
                        #
                        # Deliberately NOT probing live_job() first. A dead
                        # child's own `running` rows stay inside the
                        # staleness window for _LIVE_JOB_STALENESS_S, so a
                        # probe here would make a crash wait out a heartbeat
                        # that can never advance again.
                        logger.info(
                            "jobs dev-reload: child died with %d change(s) held — respawning on them",
                            len(pending),
                        )
                        child, spawn_time = _spawn_child()
                        crash_reported = False
                        pending.clear()
                        deferred_since = None
                    continue
                if not pending:
                    continue

            # Changes are waiting. Reload only if no job is provably alive.
            now = time.monotonic()
            if deferred_since is not None and now - last_probe < _LIVE_JOB_PROBE_PERIOD_S:
                continue
            last_probe = now
            blocker = live_job()
            if blocker is not None:
                if deferred_since is None:
                    deferred_since = now
                    last_defer_log = now
                    logger.info(
                        "jobs dev-reload: %d change(s) pending, e.g. %s — DEFERRING reload, live job: %s",
                        len(pending),
                        next(iter(pending)),
                        blocker,
                    )
                elif now - last_defer_log >= _DEFER_PROGRESS_S:
                    last_defer_log = now
                    logger.info(
                        "jobs dev-reload: reload still deferred after %.0fs, %d change(s) pending, live job: %s",
                        now - deferred_since,
                        len(pending),
                        blocker,
                    )
                continue

            if deferred_since is not None:
                logger.info(
                    "jobs dev-reload: no live job after %.0fs deferred — applying held reload",
                    now - deferred_since,
                )
            logger.info("jobs dev-reload: %d change(s), e.g. %s — reloading", len(pending), next(iter(pending)))
            # extend_while_live closes the probe->SIGTERM race: the scheduler
            # can start a long job in that window, and it must not be killed
            # at the drain budget just for starting a moment too late.
            _stop_child(child, extend_while_live=True, stop_event=stop_event)
            if stop_event.is_set():
                return 0
            child, spawn_time = _spawn_child()
            crash_reported = False
            pending.clear()
            deferred_since = None
    finally:
        # An explicit stop drains and kills regardless of any live job:
        # deferral withholds an AUTOMATIC reload, it does not veto the
        # operator stopping the stack.
        _stop_child(child)

    return 0


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if settings.app_env not in DEV_LIKE_ENVS:
        # Pass-through: identical to `python -m app.jobs`, no watcher and
        # no supervising process. Keeps this module safe to wire into any
        # launcher without branching per environment.
        from app.jobs.__main__ import serve

        logger.info(
            "jobs dev-reload: app_env=%s is not dev-like — auto-reload OFF, running daemon in-process",
            settings.app_env,
        )
        sys.exit(serve())
    sys.exit(_supervise())


if __name__ == "__main__":
    main()
