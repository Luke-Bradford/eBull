"""Served-build identity + an on-demand thread dump for the API worker (#3119).

On 2026-09-16 the dev API served **stale code for ~10 hours** and then stopped
answering entirely. Neither half had a detector: nothing reported which commit
the worker had loaded, and ``py-spy`` needs root on macOS so no Python-level
stack was obtainable before ``SIGKILL`` destroyed the state.

This module supplies both, and deliberately supplies nothing else — the cause of
the wedge is unknown and #3119 forbids guessing at it in the fix.

Two surfaces, and the split between them is the point:

* a **sidecar JSON** on disk, because a wedged API cannot answer an HTTP request
  about its own wedge, because the prober needs the worker's pid before it may
  signal anything, and because publishing commit/pid/start-time on a public
  endpoint would widen exactly the fingerprint surface ``health_db``'s docstring
  narrowed in #240;
* ``SIGUSR1`` → ``faulthandler``, writing every thread's stack to a file.

⚠⚠ ``SIGUSR1``'s default disposition TERMINATES the process. A prober may only
signal the pid recorded in the sidecar, and only after confirming it is alive.
Signalling the reload parent — which ``pgrep uvicorn`` also matches — kills it.

Nothing here raises. A missing directory, a read-only disk or a platform without
``SIGUSR1`` degrades to a recorded ``false`` in the sidecar. An observability
feature must never be able to fail a boot.
"""

from __future__ import annotations

import faulthandler
import json
import logging
import os
import signal
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, Any

from app.security.master_key import resolve_data_dir
from app.system.git_identity import app_tree_hash, head_commit, is_dirty

logger = logging.getLogger(__name__)

SIDECAR_FILENAME = "api_served_build.json"
DUMP_FILENAME = "api_faulthandler.log"

# ``faulthandler`` keeps writing to whatever fd it was handed. CPython holds a
# strong reference to the file object, so this global is NOT about garbage
# collection (revision 1 of the spec claimed that; it is false) — it is about
# keeping the descriptor from being closed and recycled underneath the handler,
# which would send the next dump into an unrelated file.
_dump_handle: IO[bytes] | None = None

# Wall clock for the operator-facing timestamp, monotonic for the duration.
# Mixing them is how an NTP step turns an uptime into a negative number.
_started_at = datetime.now(UTC)
_started_monotonic = time.monotonic()


def uptime_s() -> float:
    """Seconds since this module was imported, on a monotonic clock."""
    return time.monotonic() - _started_monotonic


def started_at() -> datetime:
    """Wall-clock UTC instant this module was imported.

    ⚠ Import time, not process start time. Under ``uvicorn --reload`` the
    worker is spawned and imports immediately, so the two are within a second
    of each other, but they are not the same thing and the name says so.
    """
    return _started_at


def _register_faulthandler(dump_path: Path) -> bool:
    """Point ``SIGUSR1`` at a thread dump in ``dump_path``. Never raises.

    ⚠ POSIX only. ``faulthandler.register`` does not exist on Windows and
    neither does ``SIGUSR1``; ``.vscode/tasks.json`` carries a pwsh arm, so the
    absence is a supported configuration and not an error.

    ⚠ One registration writes to ONE fd — file *and* stderr is not available
    without an explicit tee. The file wins: stderr on this stack is a VS Code
    task buffer that does not survive the session that needs to read it.
    """
    global _dump_handle
    if not hasattr(faulthandler, "register") or not hasattr(signal, "SIGUSR1"):
        logger.info("served_build: SIGUSR1 thread dumps unavailable on this platform")
        return False
    try:
        dump_path.parent.mkdir(parents=True, exist_ok=True)
        handle = dump_path.open("ab", buffering=0)
        faulthandler.register(signal.SIGUSR1, file=handle, all_threads=True, chain=False)
    except Exception:
        logger.warning("served_build: could not register SIGUSR1 thread dump", exc_info=True)
        return False
    _dump_handle = handle
    return True


def build_record(*, data_dir: Path, faulthandler_ready: bool) -> dict[str, Any]:
    """The sidecar payload. Pure given its two arguments, so it is table-testable."""
    return {
        "pid": os.getpid(),
        "commit": head_commit(),
        "app_tree": app_tree_hash(),
        "dirty": is_dirty(),
        "started_at": _started_at.isoformat(),
        "executable": sys.executable,
        "dump_path": str(data_dir / DUMP_FILENAME),
        "faulthandler": faulthandler_ready,
    }


def _write_sidecar(path: Path, record: dict[str, Any]) -> None:
    """Atomically replace the sidecar. Never raises.

    Written via a temp file in the same directory + ``os.replace`` so a prober
    reading concurrently sees either the old record or the new one, never a
    half-written one.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(record, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(tmp_name, path)
    except Exception:
        Path(tmp_name).unlink(missing_ok=True)
        raise


def activate(data_dir: Path | None = None) -> dict[str, Any] | None:
    """Register the dump handler and publish the sidecar. Never raises.

    Returns the record written, or ``None`` if nothing could be published.
    Idempotent: a reload re-registers and rewrites, which is what a new worker
    should do.
    """
    try:
        resolved = data_dir if data_dir is not None else resolve_data_dir()
        ready = _register_faulthandler(resolved / DUMP_FILENAME)
        record = build_record(data_dir=resolved, faulthandler_ready=ready)
        _write_sidecar(resolved / SIDECAR_FILENAME, record)
    except Exception:
        logger.warning("served_build: could not publish served-build sidecar", exc_info=True)
        return None
    logger.info(
        "served_build: pid=%s app_tree=%s dirty=%s faulthandler=%s",
        record["pid"],
        record["app_tree"],
        record["dirty"],
        record["faulthandler"],
    )
    return record
