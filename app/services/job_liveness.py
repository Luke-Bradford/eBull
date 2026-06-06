"""Jobs liveness watchdog (#1500 / GAP-D, umbrella #1472).

Per-job **stall** detector. A scheduled job is *stalled* when it has
recorded ZERO ``job_runs`` rows over a window of ``K`` cadence cycles —
despite having fired at least once historically — and is not currently
running. This is the broad silent-failure guard the
``EVENT_JOB_MAX_INSTANCES`` listener (#1501) does not cover: that
listener only makes one *known* APScheduler suppression visible, whereas
a job can stop firing for any reason (wedge, crash-loop, mis-schedule).

Design notes (each pins a Codex ckpt-1 finding — see
``docs/proposals/ops/2026-06-06-jobs-liveness-watchdog.md``):

* **Any status counts as a fire.** A bootstrap/universal-gate block on a
  scheduled fire still writes a ``job_runs`` row with ``status='skipped'``
  (``record_job_skip``). So a gated job is NOT false-stalled and needs no
  bootstrap-state exclusion — a recorded row of *any* status proves the
  scheduler fired the job.
* **An active ``running`` row counts as alive.** A long/stuck run can
  self-skip later fires on the advisory ``JobLock`` without writing a
  row; counting the live row as alive prevents a false "stopped firing"
  verdict. A genuinely stuck run is a *different* failure mode, surfaced
  by :func:`fetch_active_runs` and terminalised by the #1474 reaper.
* **Never-run guard via lifetime rows.** A job with zero lifetime rows is
  not evaluated (no ``first_seen`` registry exists). Trade-off: a job
  broken from day one is not flagged until its first fire. v1 targets the
  regression of a previously-working job.
* **Self-tracked jobs are excluded by the caller.** ``orchestrator_*``
  write ``sync_runs`` not ``job_runs`` and are covered by layer-staleness.
* **Window ``W = K * P``** (no cap): "≥ K missed cadence cycles" exactly.
  A yearly job's window is ~3y, so it is only flagged on genuine
  multi-year silence.

Scheduler/process-wide death is OUT OF SCOPE — a watchdog that is itself
a scheduled job cannot report its own stall. That is owned by the
``job_runtime_heartbeat`` + ``supervisor.py`` path (#719).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import psycopg
import psycopg.rows

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from app.workers.scheduler import Cadence

# Default missed-cadence-cycle threshold before a silent job is flagged.
DEFAULT_MISSED_CYCLES = 3

# Stable anchor for deriving a cadence's intrinsic period. Any fixed
# timezone-aware instant works — the period is the gap between two
# consecutive fires, which does not depend on "now". Using a constant
# (not ``datetime.now``) keeps the function pure and deterministic.
_PERIOD_ANCHOR = datetime(2025, 1, 1, tzinfo=UTC)


@dataclass(frozen=True)
class StalledJob:
    """A scheduled job that has gone silent for ``window_seconds``."""

    job_name: str
    window_seconds: float
    last_fire_at: datetime | None


@dataclass(frozen=True)
class ActiveRun:
    """The oldest still-``running`` row for a job, with its age."""

    job_name: str
    started_at: datetime
    age_seconds: float


def cadence_period(cadence: Cadence) -> timedelta:
    """Representative period of ``cadence`` — the gap between two
    consecutive fires.

    Derived from the already-tested ``compute_next_run`` so the per-kind
    period math is not duplicated here. For variable-length cadences
    (monthly / yearly) this returns one representative period anchored at
    :data:`_PERIOD_ANCHOR` (e.g. ~365d for yearly). It is used ONLY to
    seed a generous lookback in :func:`window_start_for` — never as the
    silence window itself, which would be wrong for variable cadences
    (Codex ckpt-2): the real window is the span of the actual K most
    recent fire slots.
    """
    from app.workers.scheduler import compute_next_run

    first = compute_next_run(cadence, _PERIOD_ANCHOR)
    second = compute_next_run(cadence, first)
    return second - first


def window_start_for(cadence: Cadence, now: datetime, k: int) -> datetime:
    """Timestamp of the ``k``-th most recent expected fire at/just before
    ``now``.

    The window ``[window_start, now]`` then spans exactly the ``k`` most
    recent cadence slots, so "zero actual rows inside it" means "``k``
    consecutive missed fires" — exact for variable cadences
    (monthly / yearly) too, unlike a fixed ``period * k`` span which an
    old row could sit inside (Codex ckpt-2). ``cadence_period`` only
    seeds a generous lookback; the boundary itself comes from real
    ``compute_next_run`` slots.
    """
    from app.workers.scheduler import compute_next_run

    period = cadence_period(cadence)
    cursor = now - period * (k + 2)
    fires: list[datetime] = []
    for _ in range(10_000):  # defensive cap; real cadences yield ~k+2
        cursor = compute_next_run(cadence, cursor)
        if cursor > now:
            break
        fires.append(cursor)
    if len(fires) >= k:
        return fires[-k]
    if fires:
        return fires[0]
    # Degenerate fallback (no slot found in the lookback) — should not
    # happen for a real cadence; fall back to the representative span.
    return now - period * k


def find_stalled_jobs(
    conn: psycopg.Connection[Any],
    jobs: Iterable[tuple[str, Cadence]],
    now: datetime,
    *,
    k: int = DEFAULT_MISSED_CYCLES,
) -> list[StalledJob]:
    """Return the jobs that have recorded zero fires over ``K`` cadence
    cycles despite having fired before, and are not currently running.

    ``jobs`` is ``(job_name, cadence)`` pairs — the caller filters out
    self-tracked jobs (orchestrator_*). ``now`` must be timezone-aware.
    """
    if now.tzinfo is None:
        raise ValueError("find_stalled_jobs requires a timezone-aware 'now'")

    stalled: list[StalledJob] = []
    for job_name, cadence in jobs:
        window_start = window_start_for(cadence, now, k)
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT
                    count(*) FILTER (
                        WHERE started_at >= %(start)s AND started_at <= %(now)s
                    ) AS recent,
                    count(*) AS lifetime,
                    max(started_at) AS last_fire_at,
                    bool_or(status = 'running' AND finished_at IS NULL)
                        AS has_active_running
                FROM job_runs
                WHERE job_name = %(name)s
                """,
                {"name": job_name, "start": window_start, "now": now},
            )
            row = cur.fetchone()

        if row is None:
            continue
        recent = row["recent"] or 0
        lifetime = row["lifetime"] or 0
        has_active_running = bool(row["has_active_running"])

        # Stalled = used to fire (lifetime >= 1) AND nothing in the window
        # AND not currently running (a live run is "stuck", not "silent").
        if lifetime >= 1 and recent == 0 and not has_active_running:
            stalled.append(
                StalledJob(
                    job_name=job_name,
                    window_seconds=(now - window_start).total_seconds(),
                    last_fire_at=row["last_fire_at"],
                )
            )
    return stalled


def fetch_active_runs(
    conn: psycopg.Connection[Any],
    now: datetime,
) -> list[ActiveRun]:
    """Return the oldest still-``running`` row per job, with its age.

    Surfaces a wedged run even when newer ``skipped`` rows top the
    latest-row health path (``check_job_health`` is latest-row-based) —
    the Codex PR-visibility ckpt-1b nuance. Ordered oldest-age first.
    """
    if now.tzinfo is None:
        raise ValueError("fetch_active_runs requires a timezone-aware 'now'")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (job_name) job_name, started_at
            FROM job_runs
            WHERE status = 'running' AND finished_at IS NULL
            ORDER BY job_name, started_at ASC
            """
        )
        rows = cur.fetchall()

    runs: list[ActiveRun] = []
    for row in rows:
        started_at: datetime = row["started_at"]
        if started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=UTC)
        runs.append(
            ActiveRun(
                job_name=row["job_name"],
                # Store the tz-normalised value (not the raw row), so the
                # stored field and ``age_seconds`` stay consistent and
                # callers always get a tz-aware datetime (PR #1507 review).
                started_at=started_at,
                age_seconds=max(0.0, (now - started_at).total_seconds()),
            )
        )
    runs.sort(key=lambda r: r.age_seconds, reverse=True)
    return runs


def evaluate_liveness(
    conn: psycopg.Connection[Any],
    jobs: Sequence[tuple[str, Cadence]],
    now: datetime,
    *,
    k: int = DEFAULT_MISSED_CYCLES,
) -> tuple[list[StalledJob], list[ActiveRun]]:
    """Convenience: run both checks in one call (watchdog body + API)."""
    return find_stalled_jobs(conn, jobs, now, k=k), fetch_active_runs(conn, now)
