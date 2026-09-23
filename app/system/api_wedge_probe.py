"""Probe the dev API for a wedge, and capture a thread dump if it is wedged (#3119).

Shared by the manual CLI (``scripts/probe_api_wedge.py``) and the periodic
``api_wedge_probe`` job. No DB access and no app-state import beyond the
sidecar/git readers, so it can run from any process.

What one observation reports
----------------------------
1. ``/health/live`` (async, dependency-free) and ``/health`` (sync, hits the DB),
   each under its own total deadline. The pair separates a blocked event loop
   from a starved sync path — see ``app/main.py::health_live``.
2. Freshness: the sidecar's ``app_tree`` against a live ``git rev-parse HEAD:app``.
   ⚠ Scoped to ``app/`` and not to HEAD, because ``--reload-dir app`` means a
   docs- or test-only commit moves HEAD and correctly triggers no reload.
   ⚠ Unknown is NEVER fresh: two missing values do not match.
3. On a hang, ``SIGUSR1`` to the worker, producing a thread dump.

⚠⚠ ``SIGUSR1``'s default disposition TERMINATES a process that did not register
a handler. ``dump_threads`` therefore signals ONLY the pid the sidecar names, and
only after confirming it is alive and that the sidecar claims a handler was
registered. ``pgrep uvicorn`` is not an acceptable substitute — it also matches
the reload PARENT, which registers nothing and would simply die.

Nothing here sends ``SIGTERM`` or ``SIGKILL``. ``SIGKILL`` is what destroyed the
evidence on 2026-09-16 and left the ticket with nothing to diagnose.
"""

from __future__ import annotations

import http.client
import json
import logging
import os
import signal
import socket
import subprocess
import threading
import time
import urllib.parse
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from app.config import DEV_LIKE_ENVS, settings
from app.security.master_key import resolve_data_dir
from app.system.git_identity import app_tree_hash
from app.system.served_build import SIDECAR_FILENAME

logger = logging.getLogger(__name__)

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


def _abort(conn: http.client.HTTPConnection) -> None:
    """Unblock a request thread stuck in a socket call. Never raises.

    ``shutdown`` (not just ``close``) is what wakes a thread already blocked in
    ``recv`` on the same socket.
    """
    sock = conn.sock
    if sock is not None:
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
    conn.close()


def probe(base_url: str, path: str, timeout_s: float) -> ProbeResult:
    """GET ``path`` under a TOTAL deadline. Never raises, never leaks the request.

    ⚠ A socket timeout bounds each individual blocking operation, not the
    request as a whole — a peer that dribbles bytes, or a chain of slow stages,
    can overrun it by a multiple. This is a wedge detector, so an unbounded
    overrun is the one failure it cannot afford: it delays the thread dump,
    which is the evidence the whole exercise exists to capture. The request
    therefore runs on a daemon thread and the deadline is enforced by ``join``.

    ⚠ At the deadline the socket is shut down rather than the thread abandoned.
    Abandoning was fine for a one-shot CLI, but the jobs process runs this every
    cadence period for days, and an abandoned thread holds its socket for as
    long as the peer keeps dribbling (Codex ckpt-2, #3119).

    ``http.client`` never follows redirects, so the probe measures only the URL
    it was given. Any HTTP status counts as an answer — ``/health`` answering
    503 is still alive; treating it as a failure would report a
    healthy-but-degraded app as wedged.
    """
    parts = urllib.parse.urlsplit(base_url)
    connection_cls = http.client.HTTPSConnection if parts.scheme == "https" else http.client.HTTPConnection
    conn = connection_cls(parts.hostname or "127.0.0.1", parts.port, timeout=timeout_s)
    target = f"{parts.path.rstrip('/')}{path}"
    outcome: list[tuple[int | None, str | None]] = []

    def request() -> None:
        try:
            conn.request("GET", target)
            response = conn.getresponse()
            response.read()
            outcome.append((response.status, None))
        except Exception as exc:
            outcome.append((None, f"{type(exc).__name__}: {exc}"))

    worker = threading.Thread(target=request, name="api-wedge-probe-request", daemon=True)
    started = time.monotonic()
    worker.start()
    worker.join(timeout_s)
    elapsed = time.monotonic() - started

    if not outcome:
        _abort(conn)
        worker.join(1.0)
        return ProbeResult(path, None, elapsed, f"no response within {timeout_s:.1f}s")
    conn.close()
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


@dataclass(frozen=True)
class Observation:
    """Everything one probe pass saw. No dump has been attempted yet."""

    live: ProbeResult
    health: ProbeResult
    sidecar: dict[str, object] | None
    verdict: str
    served: str | None
    ondisk: str | None

    @property
    def wedged(self) -> bool:
        return not (self.live.answered and self.health.answered)

    @property
    def predates_instrumentation(self) -> bool:
        """A 404 on ``/health/live`` means the served build is older than #3119.

        A stronger statement than the sidecar can make: the route ships in the
        same commit as this module, so a worker that does not serve it runs code
        older than this checkout, even when the git comparison came back
        ``unknown``.
        """
        return self.live.status == 404

    @property
    def exit_code(self) -> int:
        if self.wedged:
            return EXIT_WEDGED
        if self.verdict == "STALE" or self.predates_instrumentation:
            return EXIT_STALE
        if self.verdict == "unknown":
            return EXIT_UNKNOWN
        return EXIT_OK


def observe(base_url: str = DEFAULT_BASE_URL) -> Observation:
    """One bounded probe pass. Never raises, never signals."""
    live = probe(base_url, "/health/live", LIVE_TIMEOUT_S)
    health = probe(base_url, "/health", HEALTH_TIMEOUT_S)
    sidecar = read_sidecar(resolve_data_dir())
    verdict, served, ondisk = freshness(sidecar)
    return Observation(live, health, sidecar, verdict, served, ondisk)


StalePair = tuple[str | None, str | None]


def periodic_decision(obs: Observation, previous_stale: StalePair | None) -> tuple[str | None, StalePair | None]:
    """Decide one periodic run: ``(failure_reason, stale_pair_to_remember)``.

    ``failure_reason`` is ``None`` when the run is healthy. The caller dumps
    threads on a wedge before raising; this function only classifies.

    STALE fails only when the SAME ``(served, ondisk)`` pair was also seen by the
    previous run. A normal ``uvicorn --reload`` restart takes seconds, so a
    probe that lands mid-reload sees a transient mismatch; one full cadence
    period of the same mismatch is not a reload in progress. Fixed by
    construction (two consecutive observations) — there is no published bound
    for a reload's duration to cite.

    A wedge and ``unknown`` fail immediately: unknown is never fresh, and a hang
    is the event whose stacks must be captured before anyone restarts it.
    """
    if obs.wedged:
        return (
            f"API wedged: /health/live {_describe(obs.live)}, /health {_describe(obs.health)}",
            None,
        )
    if obs.verdict == "STALE" or obs.predates_instrumentation:
        pair: StalePair = (obs.served, obs.ondisk)
        if previous_stale == pair:
            return f"API serving stale code for >=2 consecutive probes: served={obs.served} ondisk={obs.ondisk}", pair
        return None, pair
    if obs.verdict == "unknown":
        return f"API freshness unknown: served={obs.served} ondisk={obs.ondisk}", None
    return None, None


def _describe(result: ProbeResult) -> str:
    if result.answered:
        return f"HTTP {result.status} in {result.elapsed_s:.1f}s"
    return f"NO RESPONSE ({result.error})"


# A wedge persists until someone restarts the API (the 2026-09-16 one served
# stale code for ~10h), so this bounds detection latency, not correctness. It
# also spaces the two STALE observations far beyond a uvicorn reload's seconds.
PERIODIC_INTERVAL_S = 900.0


def run_periodic_probe(stop_event: threading.Event, interval_s: float = PERIODIC_INTERVAL_S) -> None:
    """Jobs-process daemon thread: probe every ``interval_s`` until stopped.

    ⚠ Deliberately NOT a ``ScheduledJob`` (Codex ckpt-2, #3119). A scheduled
    fire first waits on its execution-lane permit — the general lane has ONE,
    held for hours by backfills — and then takes a Postgres ``JobLock`` and
    runs the DB prelude. Either can stall the detector before it probes, and a
    stalled Postgres is itself a plausible wedge cause. So this path touches no
    DB at all: the evidence is the thread dump the worker appends to its own
    ``dump_path``, and the alarm is an ERROR line in the jobs log.

    A hard no-op outside a dev-like ``app_env``: it probes the local dev API
    and signals that API's worker, neither of which exists anywhere else.
    Never raises — one failed pass must not kill the detector.
    """
    if settings.app_env not in DEV_LIKE_ENVS:
        logger.info("api wedge probe disabled: app_env=%s", settings.app_env)
        return
    previous_stale: StalePair | None = None
    while not stop_event.wait(interval_s):
        try:
            obs = observe()
            failure, previous_stale = periodic_decision(obs, previous_stale)
            if obs.wedged:
                logger.error("api wedge probe (#3119): %s", dump_threads(obs.sidecar))
            if failure is not None:
                logger.error("api wedge probe (#3119): %s", failure)
            else:
                logger.debug(
                    "api wedge probe: live=%s health=%s app_tree=%s", obs.live.status, obs.health.status, obs.verdict
                )
        except Exception:
            logger.exception("api wedge probe pass raised; continuing")
