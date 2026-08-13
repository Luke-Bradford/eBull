#!/usr/bin/env python3
"""Dev Postgres crash-loop / stuck-recovery detector (D1, #1449).

Runs OUTSIDE the app and Postgres — it talks only to the Docker daemon —
so it can alert even when BOTH are down. That is the exact blind spot that
let the dev PG OOM-recovery loop run silently for ~18h (RCA 2026-06-03):
the app could not even bind its port (lifespan blocks on PG), so nothing
in-process could notice, and ``restart: unless-stopped`` hid the loop.

D2 (#1447) stops the *infinite* loop (``restart: on-failure:5`` + a
healthcheck). D1 here adds a *proactive alert* so the operator learns in
minutes, regardless of the future root cause.

Two signals, both in the pure ``evaluate`` policy (unit-tested, no IO):

  A. **Crash-loop** — ``RestartCount`` rises by >= ``restart_threshold``
     within ``restart_window_s``. The container is being restarted faster
     than it can come up.
  B. **Stuck recovery** — cluster state is ``in crash recovery`` (or
     ``in archive recovery``) with a FROZEN "Latest checkpoint's REDO
     location" for > ``recovery_stall_s``. A redo pointer that never
     advances is the precise OOM-loop signature (each attempt replays the
     same span and dies before checkpointing).

On alert: a macOS notification (``osascript``), a JSON status file, and a
loud stderr line. Single-shot (``--once``, for cron/launchd/tests) or a
polling loop (default).

Wire it once (launchd) — see ``scripts/com.ebull.pg-crash-loop-detector.plist``
and ``docs/operator/runbooks/pg-crash-loop-detector.md``.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

# Cluster states (from ``pg_controldata``) that mean "replaying WAL, not yet
# accepting connections". A frozen redo LSN across these is the wedge.
_RECOVERY_STATES: frozenset[str] = frozenset({"in crash recovery", "in archive recovery"})

_DEFAULT_CONTAINER = "ebull-postgres"
_DEFAULT_PGDATA = "/var/lib/postgresql/data"
_DEFAULT_STATUS_FILE = Path.home() / ".cache" / "ebull" / "pg_crash_loop_status.json"


@dataclass(frozen=True)
class Sample:
    """One observation of the container + cluster at ``ts`` (epoch seconds).

    ``restart_count`` / ``cluster_state`` / ``redo_lsn`` are ``None`` when
    the corresponding probe could not be read (container gone, ``pg_exec``
    failed because PG is mid-restart, etc.) — a missing probe is never
    treated as a healthy reading.
    """

    ts: float
    restart_count: int | None
    cluster_state: str | None
    redo_lsn: str | None


def evaluate(
    samples: list[Sample],
    *,
    now: float,
    restart_threshold: int = 3,
    restart_window_s: float = 900.0,
    recovery_stall_s: float = 600.0,
) -> str | None:
    """Pure policy: return a human alert reason, or ``None`` if healthy.

    No IO — this is the unit-testable core. ``samples`` is the rolling
    history oldest→newest; ``now`` is epoch seconds.
    """
    if not samples:
        return None
    cur = samples[-1]

    # Signal A — crash-loop: RestartCount climbed past the threshold within
    # the window. Compare against the MIN in-window (the container may have
    # been recreated, resetting the counter, so a simple last-minus-first
    # could go negative; min is the robust baseline).
    if cur.restart_count is not None:
        in_window = [s.restart_count for s in samples if s.restart_count is not None and s.ts >= now - restart_window_s]
        if in_window:
            delta = cur.restart_count - min(in_window)
            if delta >= restart_threshold:
                return (
                    f"crash-loop: RestartCount rose +{delta} (to {cur.restart_count}) "
                    f"within {int(restart_window_s // 60)}min"
                )

    # Signal B — stuck recovery: in a recovery state with the redo LSN frozen
    # for longer than the stall budget. Walk back over the consecutive recent
    # samples that are ALL in recovery with the SAME redo LSN; if that span is
    # long enough, recovery is wedged (not merely slow-but-advancing).
    if cur.cluster_state in _RECOVERY_STATES and cur.redo_lsn is not None:
        oldest_frozen_ts = cur.ts
        for s in reversed(samples):
            if s.cluster_state in _RECOVERY_STATES and s.redo_lsn == cur.redo_lsn:
                oldest_frozen_ts = s.ts
            else:
                break
        frozen_for = cur.ts - oldest_frozen_ts
        if frozen_for >= recovery_stall_s:
            return (
                f"recovery stalled: state={cur.cluster_state!r}, redo frozen at "
                f"{cur.redo_lsn} for {int(frozen_for // 60)}min"
            )

    return None


# ──────────────────────────── IO shell ────────────────────────────────


def _run(cmd: list[str], timeout: float = 10.0) -> str | None:
    """Run ``cmd``, return stripped stdout, or ``None`` on any failure."""
    try:
        out = subprocess.run(  # noqa: S603 — fixed argv, no shell
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=True,
        )
    except subprocess.SubprocessError, OSError:  # py3.14 PEP 758: paren-free except tuple
        return None
    return out.stdout.strip()


def _inspect_restart_count(container: str) -> int | None:
    raw = _run(["docker", "inspect", "--format", "{{.RestartCount}}", container])
    if raw is None:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _pg_controldata(container: str, pgdata: str) -> tuple[str | None, str | None]:
    """Return (cluster_state, redo_lsn) from ``pg_controldata``.

    Reads the control file, which works even mid-recovery (the postmaster
    need not accept connections). ``(None, None)`` if the exec fails.
    """
    raw = _run(["docker", "exec", container, "pg_controldata", "-D", pgdata])
    if raw is None:
        return None, None
    state: str | None = None
    redo: str | None = None
    for line in raw.splitlines():
        if line.startswith("Database cluster state:"):
            state = line.split(":", 1)[1].strip()
        elif line.startswith("Latest checkpoint's REDO location:"):
            redo = line.split(":", 1)[1].strip()
    return state, redo


def sample_now(container: str, pgdata: str, *, now: float) -> Sample:
    restart_count = _inspect_restart_count(container)
    state, redo = _pg_controldata(container, pgdata)
    return Sample(ts=now, restart_count=restart_count, cluster_state=state, redo_lsn=redo)


def _notify(reason: str, *, container: str, status_file: Path) -> None:
    title = f"⚠️ {container} wedged"
    # macOS native notification — no third-party dependency. Best-effort.
    _run(
        [
            "osascript",
            "-e",
            f"display notification {json.dumps(reason)} with title {json.dumps(title)}",
        ],
        timeout=5.0,
    )
    try:
        status_file.parent.mkdir(parents=True, exist_ok=True)
        status_file.write_text(json.dumps({"ts": time.time(), "container": container, "reason": reason}))
    except OSError:
        pass
    print(f"[pg-crash-loop-detector] ALERT: {container}: {reason}", file=sys.stderr, flush=True)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--container", default=_DEFAULT_CONTAINER)
    p.add_argument("--pgdata", default=_DEFAULT_PGDATA)
    p.add_argument("--interval", type=float, default=60.0, help="poll seconds (loop mode)")
    p.add_argument("--restart-threshold", type=int, default=3)
    p.add_argument("--restart-window-s", type=float, default=900.0)
    p.add_argument("--recovery-stall-s", type=float, default=600.0)
    p.add_argument("--renotify-s", type=float, default=900.0, help="re-alert cadence while still wedged")
    p.add_argument("--status-file", type=Path, default=_DEFAULT_STATUS_FILE)
    p.add_argument("--once", action="store_true", help="single check then exit (cron/launchd/tests)")
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    history: list[Sample] = []
    # Keep enough history to span the longer of the two windows.
    retain_s = max(args.restart_window_s, args.recovery_stall_s) * 2
    last_alert_ts = 0.0

    def _tick() -> str | None:
        nonlocal last_alert_ts
        now = time.time()
        history.append(sample_now(args.container, args.pgdata, now=now))
        # Trim history outside the retention window.
        cutoff = now - retain_s
        del history[: max(0, len(history) - 1 - sum(1 for s in history if s.ts >= cutoff))]
        reason = evaluate(
            history,
            now=now,
            restart_threshold=args.restart_threshold,
            restart_window_s=args.restart_window_s,
            recovery_stall_s=args.recovery_stall_s,
        )
        if reason is not None and (now - last_alert_ts) >= args.renotify_s:
            _notify(reason, container=args.container, status_file=args.status_file)
            last_alert_ts = now
        return reason

    if args.once:
        # Single-shot needs >1 sample to judge a window; the caller (cron/
        # launchd) provides cadence. Persist/rehydrate is out of scope — the
        # loop mode is the primary runtime; --once is for tests + a quick
        # manual probe.
        return 2 if _tick() else 0

    print(
        f"[pg-crash-loop-detector] watching {args.container} every {args.interval:.0f}s "
        f"(restart>={args.restart_threshold}/{int(args.restart_window_s // 60)}min, "
        f"recovery-stall>{int(args.recovery_stall_s // 60)}min)",
        file=sys.stderr,
        flush=True,
    )
    while True:
        _tick()
        time.sleep(args.interval)


if __name__ == "__main__":
    raise SystemExit(main())
