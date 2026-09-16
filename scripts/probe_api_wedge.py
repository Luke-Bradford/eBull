#!/usr/bin/env python
"""Probe the dev API for a wedge, and capture a thread dump if it is wedged (#3119).

One bounded invocation. No DB, no app import, no cadence — the caller decides
when to run it. Run it before trusting a dev-verify result: on 2026-09-16 the
API served stale code for ~10 hours with every deploy check looking clean.

What it reports
---------------
1. ``/health/live`` (async, dependency-free) and ``/health`` (sync, hits the DB),
   each under its own total deadline. The pair separates a blocked event loop
   from a starved sync path — see ``app/main.py::health_live``.
2. Freshness: the sidecar's ``app_tree`` against a live ``git rev-parse HEAD:app``.
   ⚠ Scoped to ``app/`` and not to HEAD, because ``--reload-dir app`` means a
   docs- or test-only commit moves HEAD and correctly triggers no reload.
   ⚠ Unknown is NEVER fresh: two missing values do not match.
3. On a hang, ``SIGUSR1`` to the worker, producing a thread dump.

⚠⚠ ``SIGUSR1``'s default disposition TERMINATES a process that did not register
a handler. This script therefore signals ONLY the pid the sidecar names, and
only after confirming it is alive and that the sidecar claims a handler was
registered. ``pgrep uvicorn`` is not an acceptable substitute — it also matches
the reload PARENT, which registers nothing and would simply die.

It never sends ``SIGTERM`` or ``SIGKILL``. ``SIGKILL`` is what destroyed the
evidence on 2026-09-16 and left the ticket with nothing to diagnose.

Exit status: 0 only when both probes answered AND the served tree is fresh.
Non-zero on a hang, on staleness, and on unknown.

Usage::

    PYTHONPATH=. uv run python scripts/probe_api_wedge.py
    PYTHONPATH=. uv run python scripts/probe_api_wedge.py --base-url http://127.0.0.1:8000
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.security.master_key import resolve_data_dir  # noqa: E402
from app.system.git_identity import app_tree_hash  # noqa: E402
from app.system.served_build import SIDECAR_FILENAME  # noqa: E402

DEFAULT_BASE_URL = "http://127.0.0.1:8000"

# ``/health/live`` touches nothing, so a second is already generous; ``/health``
# takes a pool connection whose own checkout bound is 15s (``app/db/pool.py``),
# so it is given room to exceed that and still be judged slow rather than hung.
LIVE_TIMEOUT_S = 5.0
HEALTH_TIMEOUT_S = 20.0

# ``ps`` reports whole seconds and the sidecar is written a moment after the
# process starts, so a healthy worker always reads marginally younger than its
# own record. Slack for that, and for nothing else — the check must still catch
# a recycled pid, which is younger by the whole life of the previous worker.
_AGE_TOLERANCE_S = 30.0

EXIT_OK = 0
EXIT_WEDGED = 1
EXIT_STALE = 2
EXIT_UNKNOWN = 3


@dataclass(frozen=True)
class ProbeResult:
    """Outcome of one HTTP probe. ``status`` is ``None`` when nothing came back."""

    path: str
    status: int | None
    elapsed_s: float
    error: str | None

    @property
    def answered(self) -> bool:
        """Any HTTP status counts. ``/health`` answering 503 is still alive."""
        return self.status is not None


class _NoRedirects(urllib.request.HTTPRedirectHandler):
    """Refuse redirects. A probe that follows one is measuring another URL."""

    def redirect_request(self, *args: object, **kwargs: object) -> None:
        return None


def _fetch(url: str, timeout_s: float) -> tuple[int | None, str | None]:
    """One GET. Returns ``(status, error)``; any HTTP status counts as an answer."""
    opener = urllib.request.build_opener(_NoRedirects)
    try:
        with opener.open(url, timeout=timeout_s) as response:  # noqa: S310
            return response.status, None
    except urllib.error.HTTPError as exc:
        # A 4xx/5xx is an ANSWER — the loop scheduled the handler and it
        # returned. Treating it as a failure would report a healthy-but-
        # degraded app as wedged, which is the opposite of this script's job.
        return exc.code, None
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"


def probe(base_url: str, path: str, timeout_s: float) -> ProbeResult:
    """GET ``path`` under a TOTAL deadline. Never raises.

    ⚠ ``urlopen(timeout=...)`` bounds each individual blocking socket
    operation, not the request as a whole — a peer that dribbles bytes, or a
    chain of slow stages, can overrun it by a multiple. This is a wedge
    detector, so an unbounded overrun is the one failure it cannot afford: it
    delays the thread dump, which is the evidence the whole exercise exists to
    capture. The fetch therefore runs on a daemon thread and the deadline is
    enforced by ``join``. A thread still running at the deadline is abandoned,
    which is correct for a short-lived CLI and is exactly the "no response"
    the caller needs to hear.
    """
    url = f"{base_url.rstrip('/')}{path}"
    outcome: list[tuple[int | None, str | None]] = []
    worker = threading.Thread(target=lambda: outcome.append(_fetch(url, timeout_s)), daemon=True)

    started = time.monotonic()
    worker.start()
    worker.join(timeout_s)
    elapsed = time.monotonic() - started

    if not outcome:
        return ProbeResult(path, None, elapsed, f"no response within {timeout_s:.1f}s")
    status, error = outcome[0]
    return ProbeResult(path, status, elapsed, error)


def read_sidecar(data_dir: Path) -> dict[str, object] | None:
    """Load the served-build sidecar, or ``None`` if absent/unreadable/not JSON."""
    try:
        return json.loads((data_dir / SIDECAR_FILENAME).read_text(encoding="utf-8"))
    except Exception:
        return None


def freshness(sidecar: dict[str, object] | None) -> tuple[str, str | None, str | None]:
    """Compare the served app-tree against the checkout's current one.

    Returns ``(verdict, served, ondisk)`` where verdict is ``fresh`` / ``STALE``
    / ``unknown``. ⚠ ``None == None`` is ``unknown``, never ``fresh`` — a git
    read that failed on both sides tells you nothing about staleness.
    """
    served = sidecar.get("app_tree") if sidecar else None
    ondisk = app_tree_hash()
    if not isinstance(served, str) or ondisk is None:
        return "unknown", served if isinstance(served, str) else None, ondisk
    return ("fresh" if served == ondisk else "STALE"), served, ondisk


def _pid_alive(pid: int) -> bool:
    """Whether ``pid`` exists. Signal 0 checks for existence without delivering."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Exists but is owned by another user — which is itself a reason not to
        # signal it, so the caller's guard below still declines.
        return True
    return True


def _parse_etime(raw: str) -> float | None:
    """Seconds from ``ps -o etime=`` (``[[dd-]hh:]mm:ss``), or ``None``.

    Elapsed time is used rather than a start timestamp deliberately: it is a
    DURATION, so it carries no timezone. ``ps -o lstart=`` is rendered in local
    time, and comparing that against a UTC field is a standing trap here.
    """
    raw = raw.strip()
    if not raw:
        return None
    days = 0
    if "-" in raw:
        day_part, _, raw = raw.partition("-")
        try:
            days = int(day_part)
        except ValueError:
            return None
    parts = raw.split(":")
    if not 1 <= len(parts) <= 3:
        return None
    try:
        values = [int(part) for part in parts]
    except ValueError:
        return None
    seconds = 0
    for value in values:
        seconds = seconds * 60 + value
    return days * 86400 + seconds


def _process_identity(pid: int) -> tuple[float | None, str | None]:
    """``(elapsed_seconds, executable)`` for ``pid``, either possibly ``None``.

    ⚠ ``pid`` is formatted from an ``int`` the caller has already type-checked.
    ``ps -p ""`` applies NO filter and prints the entire process table, so an
    empty value here would silently match everything.
    """
    try:
        result = subprocess.run(
            ["ps", "-p", str(pid), "-o", "etime=,comm="],
            capture_output=True,
            text=True,
            timeout=5.0,
            check=False,
        )
    except OSError, subprocess.SubprocessError:
        return None, None
    line = result.stdout.strip()
    if result.returncode != 0 or not line:
        return None, None
    etime, _, command = line.partition(" ")
    return _parse_etime(etime), command.strip() or None


def _identity_refusal(pid: int, sidecar: dict[str, object]) -> str | None:
    """Why ``pid`` must NOT be signalled, or ``None`` if it is the worker.

    ⚠⚠ PID reuse is the case this exists for. The sidecar outlives the process
    it describes, so a worker that exits and has its pid recycled leaves a
    record that still says ``faulthandler: true`` for a pid that is once again
    alive — and ``SIGUSR1`` to that unrelated process kills it. Liveness alone
    is not identity.

    Two independent checks, both derived from the sidecar's own record:

    * the executable must match the one the worker reported;
    * the process must have been running at least as long as the sidecar has
      existed. A recycled pid is necessarily younger than the record naming it.
    """
    elapsed_s, executable = _process_identity(pid)
    expected_executable = sidecar.get("executable")
    if isinstance(expected_executable, str) and executable is not None and executable != expected_executable:
        return f"pid {pid} runs {executable}, not the recorded {expected_executable} — pid reuse, NOT signalling"

    started_raw = sidecar.get("started_at")
    if not isinstance(started_raw, str) or elapsed_s is None:
        return f"cannot establish that pid {pid} is the recorded process — NOT signalling"
    try:
        started = datetime.fromisoformat(started_raw)
    except ValueError:
        return f"sidecar started_at is unparseable — cannot identify pid {pid}, NOT signalling"

    record_age_s = (datetime.now(UTC) - started).total_seconds()
    # ``ps`` truncates to whole seconds and the record is written a moment
    # after the process starts, so the process reads very slightly younger
    # than its record even in the healthy case.
    if elapsed_s + _AGE_TOLERANCE_S < record_age_s:
        return (
            f"pid {pid} has been running {elapsed_s:.0f}s but its record is "
            f"{record_age_s:.0f}s old — pid reuse, NOT signalling"
        )
    return None


def dump_threads(sidecar: dict[str, object] | None) -> str:
    """Send ``SIGUSR1`` to the registered worker. Returns a human-readable outcome.

    Declines unless the sidecar names a pid, claims ``faulthandler: true`` for
    it, and that pid is alive. Each refusal is a case where ``SIGUSR1`` would
    have killed the target instead of dumping it.
    """
    if not sidecar:
        return "no sidecar — cannot identify the worker, NOT signalling"
    pid = sidecar.get("pid")
    if not isinstance(pid, int):
        return "sidecar has no pid — NOT signalling"
    if not sidecar.get("faulthandler"):
        return f"sidecar reports no SIGUSR1 handler on pid {pid} — NOT signalling (it would die)"
    if not _pid_alive(pid):
        return f"pid {pid} is gone — stale sidecar, NOT signalling"
    if not hasattr(signal, "SIGUSR1"):
        return "SIGUSR1 unavailable on this platform"
    refusal = _identity_refusal(pid, sidecar)
    if refusal is not None:
        return refusal
    try:
        os.kill(pid, signal.SIGUSR1)
    except Exception as exc:
        return f"could not signal pid {pid}: {type(exc).__name__}: {exc}"
    return f"SIGUSR1 sent to pid {pid} — dump appended to {sidecar.get('dump_path')}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument(
        "--no-dump",
        action="store_true",
        help="report only; do not send SIGUSR1 even if a probe hangs",
    )
    args = parser.parse_args()

    live = probe(args.base_url, "/health/live", LIVE_TIMEOUT_S)
    health = probe(args.base_url, "/health", HEALTH_TIMEOUT_S)
    sidecar = read_sidecar(resolve_data_dir())
    verdict, served, ondisk = freshness(sidecar)

    for result in (live, health):
        state = f"HTTP {result.status}" if result.answered else f"NO RESPONSE ({result.error})"
        print(f"{result.path:<14} {state}  {result.elapsed_s:.3f}s")

    print(f"{'app_tree':<14} {verdict}  served={served} ondisk={ondisk}")
    if sidecar:
        worker = f"pid={sidecar.get('pid')} started_at={sidecar.get('started_at')} dirty={sidecar.get('dirty')}"
        print(f"{'worker':<14} {worker}")

    if not live.answered and not health.answered:
        print("reading        loop blocked, OR no live worker (the reload parent's backlog still accepts)")
    elif live.answered and not health.answered:
        print("reading        loop alive, sync path not completing")
    elif not live.answered and health.answered:
        print("reading        probes straddled a restart, or an intermittent stall — re-probe")

    # A 404 here is a stronger statement than the sidecar can make. This script
    # ships in the same commit as the route, so a worker that does not serve it
    # is running code older than this checkout — regardless of what any sidecar
    # says, and even when the git comparison came back ``unknown``.
    predates_instrumentation = live.status == 404
    if predates_instrumentation:
        print("reading        served build predates #3119 (no /health/live route) — older than this checkout")

    wedged = not (live.answered and health.answered)
    if wedged and not args.no_dump:
        print(f"{'dump':<14} {dump_threads(sidecar)}")

    if wedged:
        return EXIT_WEDGED
    if verdict == "STALE" or predates_instrumentation:
        return EXIT_STALE
    if verdict == "unknown":
        return EXIT_UNKNOWN
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
