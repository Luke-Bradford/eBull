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
"""

from __future__ import annotations

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

# After a SIGKILL the fence connection is closed by the OS, not by the
# daemon, so Postgres only drops the advisory lock once it notices the
# dead session. Same one-second settle stack-restart.sh uses.
_POST_KILL_SETTLE_S: Final[float] = 1.0

# Watcher tick. watchfiles yields an empty set on timeout (with
# yield_on_timeout=True), which is how we poll for child death and for
# our own stop_event without a second thread.
_WATCH_TICK_MS: Final[int] = 1000

_CHILD_ARGV: Final[list[str]] = [sys.executable, "-m", "app.jobs"]


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
    logger.info("jobs dev-reload: starting %s", " ".join(_CHILD_ARGV))
    spawn_time = time.time()
    return subprocess.Popen(_CHILD_ARGV, cwd=_REPO_ROOT), spawn_time


def _stop_child(proc: subprocess.Popen[bytes]) -> None:
    """SIGTERM the child and WAIT for it to die, escalating if it hangs.

    Returns only once the process has been reaped, so the caller is safe
    to spawn a replacement (see the singleton-fence note above).
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
    try:
        proc.wait(timeout=_DRAIN_TIMEOUT_S)
        logger.info("jobs dev-reload: drained cleanly in %.1fs", time.monotonic() - started)
        return
    except subprocess.TimeoutExpired:
        logger.warning(
            "jobs dev-reload: pid %d still draining after %.0fs — escalating to SIGKILL",
            proc.pid,
            _DRAIN_TIMEOUT_S,
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

            if not changes:
                # Idle tick — the only place we notice the child dying on
                # its own. A clean exit means somebody stopped the daemon
                # deliberately, so we stop too. A crash (bad import after a
                # merge, unhandled boot error) leaves the supervisor alive
                # so the next edit can bring it back without the operator
                # re-running the task.
                code = child.poll()
                if code is None:
                    continue
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
                continue

            paths = [path for _change, path in changes]
            if changes_are_stale(paths, spawn_time):
                logger.debug("jobs dev-reload: ignoring %d change(s) older than the running child", len(paths))
                continue

            logger.info("jobs dev-reload: %d change(s), e.g. %s — reloading", len(paths), paths[0])
            _stop_child(child)
            if stop_event.is_set():
                return 0
            child, spawn_time = _spawn_child()
            crash_reported = False
    finally:
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
