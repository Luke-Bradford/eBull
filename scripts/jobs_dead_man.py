#!/usr/bin/env python3
"""External dead-man for the jobs daemon (#3614 item 3).

On 2026-09-27 the jobs child was SIGKILLed and stayed dark for ~14 h. launchd
still reported the supervisor ``running``, every in-process health check died
with the child, and the only evidence was a gap in ``job_runs``. This script
watches that gap from OUTSIDE the jobs process: launchd runs it every 5 min as
its own one-shot job, so neither a dead child nor a wedged supervisor can
silence it.

Liveness = ``max(job_runs.started_at)``. Several jobs start every 5 min, so a
healthy daemon never leaves a long gap. Default threshold 30 min, chosen by
construction as several multiples of that cadence and then checked against the
trailing 30 days (it would have fired on both real outages in that window and
nothing else). Reproduce the gap distribution with:

    WITH s AS (SELECT DISTINCT started_at FROM job_runs
               WHERE started_at > now() - interval '30 days'),
         g AS (SELECT started_at - lag(started_at) OVER (ORDER BY started_at) AS gap FROM s)
    SELECT max(gap),
           percentile_cont(0.999) WITHIN GROUP (ORDER BY extract(epoch FROM gap)),
           count(*) FILTER (WHERE gap > interval '30 minutes')
    FROM g;

When ``job_runs`` cannot be read (Postgres down, bad config), the newest start
time seen on an earlier run stands in as the evidence, so a DB outage pages on
the same clock as a dead daemon rather than on one failed connect.

On alert: a push through ``app.system.push_channel`` (ntfy, when configured),
a macOS notification, a JSON status file and a stderr line. It re-alerts every
``--renotify-s`` while still stale and sends one recovery notice. State lives
in the status file, which is what lets a one-shot run de-duplicate.

Each pass then runs the refusal-surface watch (``scripts/operator_alert_watch.py``:
kill-switch changes, held execution blocks, sandbox and loss-limit entry
refusals) on the same schedule, unless ``job_runs`` was unreadable. Its failure
is logged and never changes this script's verdict or exit code.

``--test-push`` sends one test notification and exits, to check the phone
subscription. Runbook: ``docs/operator/runbooks/jobs-dead-man.md``.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from app.system.push_channel import Priority, config_from_env, send_push

_DEFAULT_STATUS_FILE = Path.home() / ".cache" / "ebull" / "jobs_dead_man_status.json"
_STALE_AFTER_S = 1800.0
_RENOTIFY_S = 7200.0

Action = Literal["alert", "renotify", "recover"]


@dataclass(frozen=True)
class State:
    """Carried between one-shot runs in the status file."""

    last_seen_start: float | None = None  # newest max(started_at) ever read, epoch s
    first_unread_at: float | None = None  # start of the current unreadable streak
    alerting: bool = False
    last_notified_at: float | None = None
    reason: str | None = None


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, UTC).strftime("%Y-%m-%dT%H:%MZ")


def step(
    prior: State,
    *,
    observed_start: float | None,
    read_error: str | None,
    now: float,
    stale_after_s: float = _STALE_AFTER_S,
    renotify_s: float = _RENOTIFY_S,
) -> tuple[State, Action | None]:
    """Pure policy: fold one observation into the state and pick an action.

    ``observed_start`` is ``max(job_runs.started_at)`` this run (``None`` if the
    table is empty or unreadable); ``read_error`` names why it was unreadable
    and is only appended to the reason.
    """
    if observed_start is not None:
        last_seen: float | None = observed_start
        first_unread = None
    else:
        # Unreadable, or (never in practice) an empty table: no fresh evidence.
        last_seen = prior.last_seen_start
        first_unread = prior.first_unread_at if prior.first_unread_at is not None else now

    # The evidence clock: the newest start we know of, else the moment this
    # blind streak began — so a blind dead-man pages after the same threshold.
    evidence = last_seen if last_seen is not None else (first_unread if first_unread is not None else now)

    reason: str | None = None
    if now - evidence > stale_after_s:
        minutes = int((now - evidence) // 60)
        if last_seen is not None:
            reason = f"no job has started for {minutes} min (last start {_iso(last_seen)})"
        else:
            reason = f"no job start observed for {minutes} min"
        if read_error is not None:
            reason += f"; job_runs unreadable: {read_error}"

    state = replace(prior, last_seen_start=last_seen, first_unread_at=first_unread, reason=reason)
    if reason is None:
        if prior.alerting:
            return replace(state, alerting=False, last_notified_at=now), "recover"
        return state, None
    if not prior.alerting:
        return replace(state, alerting=True, last_notified_at=now), "alert"
    if prior.last_notified_at is None or now - prior.last_notified_at >= renotify_s:
        return replace(state, last_notified_at=now), "renotify"
    return state, None


# ──────────────────────────── IO shell ────────────────────────────────


def read_last_start() -> tuple[float | None, str | None]:
    """``(max(job_runs.started_at) as epoch s, None)`` or ``(None, error)``.

    The error is ``<phase>: <exception class>``, so a broken install or config
    is not mistaken for a database outage. Only the class name is used because
    the text is pushed off-machine and a driver message can carry a host, port
    or role name; the full exception goes to the local log.

    A row stamped more than 5 min in the future (clock skew) is not liveness
    evidence: counting it would hold the dead-man healthy until wall time
    caught up with the bad stamp.
    """
    phase = "import"
    try:
        import psycopg

        from app.config import Settings

        phase = "config"
        url = Settings().database_url
        phase = "db"
        with psycopg.connect(url, connect_timeout=5) as conn:
            # connect_timeout bounds the connect only; a read parked behind a lock
            # would hold this one-shot, and launchd starts no later pass meanwhile.
            conn.execute("SET statement_timeout = '10s'")
            row = conn.execute(
                "SELECT max(started_at) FROM job_runs WHERE started_at <= now() + interval '5 minutes'"
            ).fetchone()
    except Exception as exc:  # noqa: BLE001 — any failure to read IS the signal
        print(f"[jobs-dead-man] job_runs read failed ({phase}): {exc!r}", file=sys.stderr, flush=True)
        return None, f"{phase}: {type(exc).__name__}"
    if row is None or row[0] is None:
        return None, None
    return min(row[0].timestamp(), time.time()), None


def load_state(path: Path) -> State:
    try:
        raw = json.loads(path.read_text())
        return State(**{k: raw.get(k) for k in State.__dataclass_fields__})
    except OSError, ValueError, TypeError:
        return State()


def save_state(path: Path, state: State) -> bool:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({**asdict(state), "written_at": time.time()}))
    except OSError as exc:
        print(f"[jobs-dead-man] status file write failed: {exc!r}", file=sys.stderr, flush=True)
        return False
    return True


def settle(prior: State, state: State, *, delivered: bool) -> State:
    """The state to persist after a notification attempt.

    An undelivered alert, re-alert or recovery notice keeps the prior
    ``alerting`` / ``last_notified_at``, so the next run tries again instead of
    waiting out ``--renotify-s`` (or losing the recovery notice for good).
    """
    if delivered:
        return state
    return replace(state, alerting=prior.alerting, last_notified_at=prior.last_notified_at)


def _macos_notify(title: str, message: str) -> bool:
    try:
        proc = subprocess.run(  # noqa: S603 — fixed argv, no shell
            ["osascript", "-e", f"display notification {json.dumps(message)} with title {json.dumps(title)}"],
            capture_output=True,
            timeout=5.0,
            check=False,
        )
    except OSError, subprocess.SubprocessError:
        return False
    return proc.returncode == 0


def notify(action: Action, state: State, *, note: str = "") -> bool:
    """Send the notification; ``True`` when it reached its channel.

    With push configured the channel is ntfy, so delivery means a 2xx. Without
    it the macOS notification is the only channel, and its exit status decides.
    """
    priority: Priority
    if action == "recover":
        title, message, priority, tag = "eBull jobs recovered", "Job starts are flowing again.", 3, "white_check_mark"
    else:
        title = "eBull jobs daemon DARK" if action == "alert" else "eBull jobs daemon still DARK"
        message, priority, tag = state.reason or "", 5, "rotating_light"
    message += note
    pushed = send_push(title=title, message=message, priority=priority, tags=(tag,))
    shown = _macos_notify(title, message)
    print(f"[jobs-dead-man] {action.upper()}: {message} (push sent: {pushed})", file=sys.stderr, flush=True)
    return pushed or (config_from_env() is None and shown)


def run_alert_watch() -> None:
    """One refusal-surface pass (#3614 item 3, slice B), contained."""
    try:
        from scripts import operator_alert_watch

        undelivered = operator_alert_watch.run()
    except Exception as exc:  # noqa: BLE001 — the dead-man's own verdict must stand
        print(f"[jobs-dead-man] operator alert watch failed: {exc!r}", file=sys.stderr, flush=True)
        return
    if undelivered:
        print(f"[jobs-dead-man] operator alert watch: {undelivered} notice(s) undelivered", file=sys.stderr, flush=True)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--stale-after-s", type=float, default=_STALE_AFTER_S)
    p.add_argument("--renotify-s", type=float, default=_RENOTIFY_S)
    p.add_argument("--status-file", type=Path, default=_DEFAULT_STATUS_FILE)
    p.add_argument("--test-push", action="store_true", help="send one test notification and exit")
    return p


def main(argv: list[str] | None = None) -> int:
    """One check. Exit 0 healthy, 2 alerting, 3 alerting with the page undelivered.

    ``--test-push`` exits 0 when delivered, 1 when not.
    """
    args = _build_parser().parse_args(argv)
    if args.test_push:
        ok = send_push(title="eBull push test", message="The jobs dead-man can reach this device.", tags=("bell",))
        print(f"[jobs-dead-man] test push sent: {ok}", file=sys.stderr, flush=True)
        return 0 if ok else 1
    observed, error = read_last_start()
    prior = load_state(args.status_file)
    state, action = step(
        prior,
        observed_start=observed,
        read_error=error,
        now=time.time(),
        stale_after_s=args.stale_after_s,
        renotify_s=args.renotify_s,
    )
    # The exit code reports the decision, not the delivery bookkeeping that
    # settle() may roll back for the retry.
    dark = state.alerting
    delivered = True
    if action is not None:
        # Probe the status file before sending: if it cannot be written, the
        # de-duplication cannot work and every run will re-page, so say why.
        note = "" if save_state(args.status_file, prior) else " [dead-man status file unwritable: not de-duplicated]"
        delivered = notify(action, state, note=note)
        state = settle(prior, state, delivered=delivered)
    save_state(args.status_file, state)
    if error is None:
        run_alert_watch()
    if not dark:
        return 0
    return 2 if delivered else 3


if __name__ == "__main__":
    raise SystemExit(main())
