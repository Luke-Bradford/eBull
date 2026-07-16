"""
Ops monitor.

Responsibilities:
  - Check staleness of each data layer against expected refresh frequency.
  - Track job executions (start, finish, success/failure, row count).
  - Manage the kill switch (activate / deactivate).
  - Produce per-layer and per-job health checks consumed by /system/status.

Row-count spike detection moved to
``app.services.sync_orchestrator.row_count_spikes`` (#328 chunk 7); the
backward-compat shim that re-exported it from here was retired in #340.

Data layers monitored:
  - universe   — instruments.last_seen_at
  - prices     — price_daily.price_date
  - quotes     — quotes.quoted_at
  - fundamentals — financial_periods_raw.fetched_at (#2008)
  - filings    — filing_events.created_at
  - news       — news_events.created_at
  - theses     — theses.created_at
  - scores     — scores.scored_at

Each layer has an expected maximum age.  If the most recent row is older
than that threshold, the layer is flagged as stale.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Final, Literal

import psycopg
import psycopg.rows
from psycopg.types.json import Jsonb

# Back-compat re-export. Canonical home is
# ``app.services.processes.json_safe.to_jsonsafe_params`` (#1064 PR2).
# ``_jsonable_params`` is the legacy name used by ``app/jobs/runtime.py``
# and the two call sites in this module — kept as an alias so internal
# rewires can land in PR2 without touching every caller. New code MUST
# import ``to_jsonsafe_params`` directly.
from app.services.processes.json_safe import to_jsonsafe_params as _jsonable_params
from app.services.runtime_config import (
    BOOT_RECOVERY_CHANGED_BY,
    BOOT_RECOVERY_REASON,
    insert_runtime_config_audit_row,
    write_kill_switch_audit,
)

if TYPE_CHECKING:
    # layer_types sits at the bottom of the orchestrator import graph, but
    # the sync_orchestrator package __init__ re-exports executor/planner/
    # registry/adapters which all import back from ops_monitor, creating a
    # cycle if we import at module level. Guarding under TYPE_CHECKING breaks
    # the cycle; pyright still resolves the annotation correctly.
    from app.services.sync_orchestrator.layer_types import FailureCategory

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LayerName = Literal[
    "universe",
    "prices",
    "quotes",
    "fundamentals",
    "filings",
    "news",
    "theses",
    "scores",
]

ALL_LAYERS: list[LayerName] = [
    "universe",
    "prices",
    "quotes",
    "fundamentals",
    "filings",
    "news",
    "theses",
    "scores",
]

# Maximum acceptable age per data layer before it is considered stale.
# These thresholds represent normal operational expectations:
#   - universe syncs nightly → 2 days allows for a missed night
#   - prices refresh hourly → 4 hours gives comfortable margin
#   - quotes refresh hourly alongside prices → same window
#   - fundamentals refresh daily → 3 days allows for weekends
#   - filings refresh daily → 3 days allows for weekends
#   - news refresh daily → 3 days allows for weekends
#   - theses refresh daily for stale Tier 1 → 3 days
#   - scores run each morning → 2 days
_STALENESS_THRESHOLDS: dict[LayerName, timedelta] = {
    "universe": timedelta(days=2),
    "prices": timedelta(hours=4),
    "quotes": timedelta(hours=4),
    "fundamentals": timedelta(days=3),
    "filings": timedelta(days=3),
    "news": timedelta(days=3),
    "theses": timedelta(days=3),
    "scores": timedelta(days=2),
}

# Queries to find the most recent timestamp for each data layer.
# Each returns a single row with a nullable 'latest' column.
_LAYER_QUERIES: dict[LayerName, str] = {
    "universe": "SELECT MAX(last_seen_at) AS latest FROM instruments",
    "prices": "SELECT MAX(price_date)::timestamptz AS latest FROM price_daily",
    "quotes": "SELECT MAX(quoted_at) AS latest FROM quotes",
    # #2008: probe the ingest layer, not fundamentals_snapshot.as_of_date —
    # as_of is a fiscal period end (always days-to-weeks behind wall clock,
    # structurally >3d "stale"). financial_periods_raw.fetched_at advances
    # on every daily normalize of touched CIKs = honest pipeline liveness.
    "fundamentals": "SELECT MAX(fetched_at) AS latest FROM financial_periods_raw",
    "filings": "SELECT MAX(created_at) AS latest FROM filing_events",
    "news": "SELECT MAX(created_at) AS latest FROM news_events",
    "theses": "SELECT MAX(created_at) AS latest FROM theses",
    "scores": "SELECT MAX(scored_at) AS latest FROM scores",
}

JobStatus = Literal["running", "success", "failure", "skipped"]

LayerStatus = Literal["ok", "stale", "empty", "error"]

# Fixed marker used in LayerHealth.detail when a per-layer query raises.
# Exported as a module constant so test fixtures can reference the same
# string the production code emits — preventing the test/prod drift class
# called out in #86 round 3 review.
LAYER_QUERY_FAILED_DETAIL_TEMPLATE = "{layer}: query failed (see server logs)"

# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LayerHealth:
    layer: LayerName
    status: LayerStatus
    latest: datetime | None = None
    max_age: timedelta | None = None
    age: timedelta | None = None
    detail: str = ""


@dataclass(frozen=True)
class JobHealth:
    job_name: str
    last_status: JobStatus | None = None
    last_started_at: datetime | None = None
    last_finished_at: datetime | None = None
    detail: str = ""


# ---------------------------------------------------------------------------
# Staleness checks
# ---------------------------------------------------------------------------


def _utcnow() -> datetime:
    return datetime.now(tz=UTC)


# ---------------------------------------------------------------------------
# Job-level retry/backoff (#1509 / T3 of #1508)
# ---------------------------------------------------------------------------
#
# A transient failure (REMEDIES[category].self_heal == True) schedules a
# near-term retry on capped exponential backoff; a permanent one
# (auth/schema-drift/db-constraint/missing-key) leaves next_retry_at NULL and
# surfaces as Needs-attention immediately. Spec:
# docs/specs/ops/2026-06-07-job-retry-backoff.md
_RETRY_MAX_ATTEMPTS: int = 4
_RETRY_BASE_SECONDS: int = 300  # 5m
_RETRY_FACTOR: int = 3
_RETRY_CAP_SECONDS: int = 3600  # 1h
# RATE_LIMITED gets a longer first delay so a retry never lands back inside a
# still-held rate window (#1484 caveat).
_RETRY_BASE_RATE_LIMITED_SECONDS: int = 900  # 15m


def _backoff_seconds(attempt: int, category: FailureCategory | None) -> int:
    """Capped exponential backoff for retry ``attempt`` (1-based)."""
    from app.services.sync_orchestrator.layer_types import FailureCategory

    base = _RETRY_BASE_RATE_LIMITED_SECONDS if category == FailureCategory.RATE_LIMITED else _RETRY_BASE_SECONDS
    raw = base * (_RETRY_FACTOR ** (attempt - 1))
    return min(raw, _RETRY_CAP_SECONDS)


def _is_transient(category: FailureCategory | None) -> bool:
    """True when the failure category is auto-retriable (self-heal).

    Reuses ``REMEDIES`` — the single source of truth for transient-vs-
    permanent — instead of a parallel list. ``None``/unknown ⇒ permanent
    (never retry a failure we cannot classify). Uses ``.get`` so an
    unmapped category cannot ``KeyError``.
    """
    if category is None:
        return False
    from app.services.sync_orchestrator.layer_types import REMEDIES

    remedy = REMEDIES.get(category)
    return bool(remedy and remedy.self_heal)


def _retry_plan(
    conn: psycopg.Connection[Any],
    run_id: int,
    category: FailureCategory | None,
    now: datetime,
) -> tuple[int, datetime | None]:
    """Compute ``(attempt, next_retry_at)`` for a just-failed run.

    ``attempt`` = this run's position in the current consecutive-failure
    streak (1 = first natural fire), counted back from this run until the
    first non-failure terminal row. ``next_retry_at`` is set only when the
    failure is transient AND ``attempt <= _RETRY_MAX_ATTEMPTS``; otherwise
    ``None`` (permanent or exhausted → Needs-attention).
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT job_name, started_at FROM job_runs WHERE run_id = %(id)s",
            {"id": run_id},
        )
        this = cur.fetchone()
    if this is None:
        return 1, None

    # Prior terminal rows for this job, newest first. 'running' is excluded
    # (not terminal); success/skipped/cancelled break the streak. LIMIT is
    # _RETRY_MAX_ATTEMPTS because once the streak would push attempt past the
    # cap we stop retrying regardless, so deeper history is irrelevant.
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT status FROM job_runs
             WHERE job_name = %(job)s
               AND run_id   <> %(id)s
               AND status IN ('success', 'failure', 'skipped', 'cancelled')
               AND (started_at < %(ts)s
                    OR (started_at = %(ts)s AND run_id < %(id)s))
             ORDER BY started_at DESC, run_id DESC
             LIMIT %(cap)s
            """,
            {
                "job": this["job_name"],
                "id": run_id,
                "ts": this["started_at"],
                "cap": _RETRY_MAX_ATTEMPTS,
            },
        )
        prior_statuses = [row[0] for row in cur.fetchall()]

    streak = 0
    for status in prior_statuses:
        if status == "failure":
            streak += 1
        else:
            break
    attempt = streak + 1

    if not _is_transient(category) or attempt > _RETRY_MAX_ATTEMPTS:
        return attempt, None
    return attempt, now + timedelta(seconds=_backoff_seconds(attempt, category))


def check_layer_staleness(
    conn: psycopg.Connection[Any],
    layer: LayerName,
    *,
    now: datetime | None = None,
) -> LayerHealth:
    """Check whether a single data layer is stale, empty, or healthy."""
    now = now or _utcnow()
    threshold = _STALENESS_THRESHOLDS[layer]
    query = _LAYER_QUERIES[layer]

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        # All queries are compile-time string constants in _LAYER_QUERIES;
        # dict lookup widens the type to str, but the values are safe literals.
        cur.execute(query)  # type: ignore[arg-type]
        row = cur.fetchone()

    # MAX() on an empty table returns NULL → row["latest"] is None.
    if row is None or row["latest"] is None:
        return LayerHealth(
            layer=layer,
            status="empty",
            max_age=threshold,
            detail=f"{layer}: no data rows found",
        )

    latest: datetime = row["latest"]
    # Ensure timezone-aware comparison (date→timestamptz cast loses tz info
    # in some psycopg configurations).
    if latest.tzinfo is None:
        latest = latest.replace(tzinfo=UTC)

    age = now - latest
    if age > threshold:
        return LayerHealth(
            layer=layer,
            status="stale",
            latest=latest,
            max_age=threshold,
            age=age,
            detail=f"{layer}: age={age} exceeds threshold={threshold}",
        )

    return LayerHealth(
        layer=layer,
        status="ok",
        latest=latest,
        max_age=threshold,
        age=age,
    )


def check_all_layers(
    conn: psycopg.Connection[Any],
    *,
    now: datetime | None = None,
) -> list[LayerHealth]:
    """Check staleness for every monitored data layer.

    Each per-layer query is wrapped in a try/except so a single broken layer
    (e.g. table missing during a partial migration) yields a ``LayerHealth``
    with ``status="error"`` instead of bubbling out and 500-ing the entire
    operator visibility endpoint.

    Prevention-log #70: never let an infra-level fault degrade into a silent
    HTTP 200; here we surface it per-layer so the operator can see *which*
    layer failed, while the rest of the report still renders.
    """
    now = now or _utcnow()
    results: list[LayerHealth] = []
    for layer in ALL_LAYERS:
        try:
            results.append(check_layer_staleness(conn, layer, now=now))
        except Exception:
            # Full exception detail goes to the server-side log only.
            # The `detail` field is surfaced verbatim in the API response,
            # so it must be a fixed string — leaking driver error text,
            # SQL fragments, or table names to a bearer-token holder is
            # the same leak class as the 5xx HTTPException one fixed in
            # the API layer. The operator gets the layer name and a stable
            # marker; the full traceback is in the logs.
            logger.exception("check_all_layers: layer %s failed", layer)
            results.append(
                LayerHealth(
                    layer=layer,
                    status="error",
                    detail=LAYER_QUERY_FAILED_DETAIL_TEMPLATE.format(layer=layer),
                )
            )
    return results


# ---------------------------------------------------------------------------
# Job health tracking
# ---------------------------------------------------------------------------


def record_job_start(
    conn: psycopg.Connection[Any],
    job_name: str,
    *,
    now: datetime | None = None,
    params_snapshot: dict[str, Any] | None = None,
) -> int:
    """
    Record the start of a scheduled job.  Returns the run_id.

    The caller should later call record_job_finish() with this run_id.

    ``params_snapshot`` (#1064 PR1b-2) populates ``job_runs.params_snapshot``
    with the validated effective params dict. Three populate paths:

    * Manual queue: validated ``payload['params']`` from
      ``pending_job_requests``.
    * Scheduled fire: ``materialise_scheduled_params(job_name)`` after
      validation.
    * Bootstrap stage: the StageSpec params dict (PR1c lifts the
      bespoke wrappers; PR1b-2 still carries ``{}`` for those).

    ``None`` means the caller is on the legacy direct-invocation path
    (tests, prelude-fallback) where the column's column-default
    ``'{}'::jsonb`` is the right value — we omit the column from the
    INSERT and let Postgres apply the default.
    """
    now = now or _utcnow()
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        if params_snapshot is None:
            cur.execute(
                """
                INSERT INTO job_runs (job_name, started_at, status)
                VALUES (%(name)s, %(started)s, 'running')
                RETURNING run_id
                """,
                {"name": job_name, "started": now},
            )
        else:
            cur.execute(
                """
                INSERT INTO job_runs (job_name, started_at, status, params_snapshot)
                VALUES (%(name)s, %(started)s, 'running', %(params)s)
                RETURNING run_id
                """,
                {
                    "name": job_name,
                    "started": now,
                    "params": Jsonb(_jsonable_params(params_snapshot)),
                },
            )
        row = cur.fetchone()
    conn.commit()

    if row is None:
        raise RuntimeError("job_runs INSERT returned no row")
    return int(row["run_id"])


def record_job_finish(
    conn: psycopg.Connection[Any],
    run_id: int,
    *,
    status: Literal["success", "failure"],
    row_count: int | None = None,
    error_msg: str | None = None,
    error_category: FailureCategory | None = None,
    now: datetime | None = None,
) -> None:
    """Record the completion of a scheduled job.

    On the failure path (#1509), classifies the failure and — when
    transient and attempts are not exhausted — stamps ``next_retry_at`` +
    ``attempt`` so the ``jobs_retry_sweeper`` re-fires it before its next
    natural cadence slot. A non-failure terminal clears ``next_retry_at``
    and leaves ``attempt`` at its default.
    """
    now = now or _utcnow()
    if status == "failure":
        attempt, next_retry_at = _retry_plan(conn, run_id, error_category, now)
    else:
        attempt, next_retry_at = 1, None
    # #1689 — ``AND status = 'running'`` makes this a first-writer-wins finalize.
    # If a boot/orphan reaper (or any future out-of-band terminal writer) already
    # transitioned this row to a terminal status — and, for a transient category,
    # already stamped its OWN ``_retry_plan`` (#1688) — a late finalize by the
    # original worker must NOT clobber that terminal/retry. The guarded UPDATE
    # no-ops (rowcount 0); we log and return without a secondary write. Mirrors
    # ``sync_orchestrator.executor._finalize_sync_run``'s guarded UPDATE.
    cur = conn.execute(
        """
        UPDATE job_runs
        SET finished_at    = %(finished)s,
            status         = %(status)s,
            row_count      = %(row_count)s,
            error_msg      = %(error_msg)s,
            error_category = %(error_category)s,
            next_retry_at  = %(next_retry_at)s,
            attempt        = %(attempt)s
        WHERE run_id = %(run_id)s
          AND status = 'running'
        """,
        {
            "finished": now,
            "status": status,
            "row_count": row_count,
            "error_msg": error_msg,
            "error_category": error_category.value if error_category else None,
            "next_retry_at": next_retry_at,
            "attempt": attempt,
            "run_id": run_id,
        },
    )
    if cur.rowcount == 0:
        logger.info(
            "record_job_finish: run_id=%s already terminal (status != 'running'); "
            "finalize raced a reaper — skipping, terminal/retry plan preserved",
            run_id,
        )
    conn.commit()


def reap_orphaned_job_runs(
    conn: psycopg.Connection[Any],
    *,
    timeout: timedelta = timedelta(hours=1),
    reap_all: bool = False,
) -> int:
    """Transition stale ``job_runs`` rows stuck in ``status='running'``
    to ``status='failure'`` so the operator console stops showing them
    as "RUNNING / NO PROGRESS NNNNm" forever (#1474).

    A ``running`` row survives a jobs-process restart when the worker
    thread that owned it died without writing a terminal status (e.g. a
    double-dispatch orphan, ``kill -9``, OOM). ``job_runs`` carries no
    boot-id, so this mirrors ``reap_orphaned_syncs``: at boot the caller
    passes ``reap_all=True`` and EVERY ``running`` row is reaped, which
    is safe because this runs at boot **Step 4** — before boot-drain /
    ``runtime.start()`` / ``_catch_up`` dispatch any job — so no row can
    belong to the live process yet. The steady-state ``timeout``
    predicate exists for a future periodic watchdog; boot uses
    ``reap_all`` to avoid the timedelta-collapses-to-zero boundary bug
    the sync reaper documents.

    Takes a caller-owned connection (ops_monitor convention — the boot
    caller opens an autocommit conn, mirroring the other boot-recovery
    steps). Returns the count of rows reaped.
    """
    # Deferred import: layer_types sits under TYPE_CHECKING at module
    # level (the sync_orchestrator package re-exports modules that import
    # back from ops_monitor — a cycle). A local import here breaks the
    # cycle while keeping the typed constant (no magic 'internal_error'
    # string) at the one runtime site that needs it.
    from app.services.sync_orchestrator.layer_types import FailureCategory

    reaped = conn.execute(
        """
        UPDATE job_runs
        SET status = 'failure',
            finished_at = now(),
            error_msg = 'orphaned: reaped at boot (owning worker thread died without a terminal status)',
            error_category = %(category)s
        WHERE status = 'running'
          AND (%(reap_all)s OR started_at < now() - %(timeout)s::interval)
        RETURNING run_id
        """,
        {
            "category": FailureCategory.INTERNAL_ERROR.value,
            "reap_all": reap_all,
            "timeout": timeout,
        },
    ).fetchall()

    # Schedule an auto-retry for each reaped row (#1474 follow-up: a job
    # interrupted by a restart / dead worker is TRANSIENT by definition — it
    # never reached a terminal status, so re-firing it is the self-heal). The
    # raw UPDATE above bypassed record_job_finish, so without this the reaped row
    # carries no next_retry_at and the jobs_retry_sweeper never re-fires it — it
    # sits red on the admin console until the next natural cadence. Route each
    # reaped run through the SAME _retry_plan as a natural failure (INTERNAL_ERROR
    # is self_heal): it sets a near-term, attempt-capped next_retry_at, and the
    # sweeper re-enqueues it through the audited manual queue. Ineligible jobs
    # (sync-runs-tracked / manual-only) get their stray next_retry_at cleared by
    # the sweeper by design, so it is safe to stamp every reaped row here.
    now = _utcnow()
    for (run_id,) in reaped:
        attempt, next_retry_at = _retry_plan(conn, int(run_id), FailureCategory.INTERNAL_ERROR, now)
        # Always persist the computed attempt (so an exhausted streak shows its
        # real attempt count, not the default 1); next_retry_at is NULL once the
        # cap is exceeded → the sweeper won't re-fire, row reads Needs-attention.
        # NOTE: ineligible (sync-tracked / manual-only) reaped jobs also get
        # next_retry_at stamped here; the jobs_retry_sweeper clears those strays
        # on the next due sweep (it never dispatches them), so this is at worst a
        # brief "will retry" display before it self-corrects.
        conn.execute(
            "UPDATE job_runs SET attempt = %(a)s, next_retry_at = %(n)s WHERE run_id = %(id)s",
            {"a": attempt, "n": next_retry_at, "id": int(run_id)},
        )
    return len(reaped)


# #2052 — machine-checkable reason prefix for a scheduled fire skipped because
# its lane stayed busy (``_fire_scheduled_with_lane_retry`` exhausted its
# acquire-retry window, or no waiter slot was free). Delimiter included so a
# future ``lane_busyX`` reason can never be misclassified. Consumed by the
# writer (``app/jobs/runtime.py``) and by the ``expected_fire_at`` anchor
# exclusion in ``app/services/processes/scheduled_adapter.py`` — a lane-busy
# skip means "work was due, couldn't start", so unlike prereq/gate skips it
# must NOT reset the schedule-missed clock.
LANE_BUSY_SKIP_PREFIX: Final[str] = "lane_busy: "


def record_job_skip(
    conn: psycopg.Connection[Any],
    job_name: str,
    reason: str,
    *,
    now: datetime | None = None,
    params_snapshot: dict[str, Any] | None = None,
) -> int:
    """Record a skipped job run (prerequisite not met).

    Inserts a single ``job_runs`` row with ``status='skipped'``,
    ``started_at = finished_at = now``, and the skip reason in
    ``error_msg``.  Returns the ``run_id``.

    Callers open the connection with ``autocommit=True``.
    ``conn.transaction()`` wraps the INSERT in an explicit
    ``BEGIN``/``COMMIT`` pair (psycopg v3 does this when the
    connection is in autocommit mode).

    This is intentionally separate from the start/finish pair used by
    ``_tracked_job`` — a skipped job has no execution phase, so a
    single insert is the honest representation.

    ``params_snapshot`` (#1064 PR1b-2) — when supplied, the validated
    params dict is committed alongside the skip row so the operator
    audit trail reflects the effective inputs even when the run never
    executed. ``None`` lets the column default to ``'{}'``.
    """
    assert conn.autocommit, (
        "record_job_skip requires autocommit=True so conn.transaction() issues a real BEGIN/COMMIT, not a savepoint"
    )
    now = now or _utcnow()
    with conn.transaction():
        if params_snapshot is None:
            row = conn.execute(
                """
                INSERT INTO job_runs (job_name, started_at, finished_at, status, row_count, error_msg)
                VALUES (%(name)s, %(ts)s, %(ts)s, 'skipped', 0, %(reason)s)
                RETURNING run_id
                """,
                {"name": job_name, "ts": now, "reason": reason},
            ).fetchone()
        else:
            row = conn.execute(
                """
                INSERT INTO job_runs (
                    job_name, started_at, finished_at, status, row_count, error_msg, params_snapshot
                )
                VALUES (%(name)s, %(ts)s, %(ts)s, 'skipped', 0, %(reason)s, %(params)s)
                RETURNING run_id
                """,
                {
                    "name": job_name,
                    "ts": now,
                    "reason": reason,
                    "params": Jsonb(_jsonable_params(params_snapshot)),
                },
            ).fetchone()
        if row is None:
            raise RuntimeError("job_runs INSERT returned no row")
        run_id = int(row[0])
    return run_id


def check_job_health(
    conn: psycopg.Connection[Any],
    job_name: str,
) -> JobHealth:
    """Return the health status of the most recent run for a given job."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT status, started_at, finished_at, error_msg
            FROM job_runs
            WHERE job_name = %(name)s
            ORDER BY started_at DESC
            LIMIT 1
            """,
            {"name": job_name},
        )
        row = cur.fetchone()

    if row is None:
        return JobHealth(
            job_name=job_name,
            detail=f"{job_name}: no runs recorded",
        )

    status: JobStatus = row["status"]
    detail = ""

    if status == "skipped":
        detail = f"{job_name}: last run skipped"
        if row["error_msg"]:
            detail += f" — {row['error_msg']}"
    elif status == "failure":
        detail = f"{job_name}: last run failed"
        if row["error_msg"]:
            detail += f" — {row['error_msg']}"
    elif status == "running":
        # Self-healing guard: if a run has been 'running' for > 2 hours,
        # treat it as stuck (process likely crashed without recording finish).
        started_at: datetime = row["started_at"]
        if started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=UTC)
        age = _utcnow() - started_at
        if age > timedelta(hours=2):
            status = "failure"
            detail = f"{job_name}: stuck in 'running' since {row['started_at']} (>{age}); likely crashed"
        else:
            detail = f"{job_name}: run still in progress since {row['started_at']}"

    return JobHealth(
        job_name=job_name,
        last_status=status,
        last_started_at=row["started_at"],
        last_finished_at=row["finished_at"],
        detail=detail,
    )


def fetch_latest_successful_runs(
    conn: psycopg.Connection[Any],
    job_names: Sequence[str],
) -> dict[str, datetime]:
    """Return the ``started_at`` of the most recent successful run per job.

    Jobs with no successful run are omitted from the result dict.  The
    caller can distinguish "never run" (absent key) from "ran and
    succeeded" (present key with a timestamp).

    Used by the catch-up-on-boot logic in :class:`JobRuntime` to decide
    which jobs are overdue.
    """
    if not job_names:
        return {}
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (job_name)
                   job_name, started_at
            FROM job_runs
            WHERE job_name = ANY(%(names)s)
              AND status = 'success'
            ORDER BY job_name, started_at DESC
            """,
            {"names": list(job_names)},
        )
        rows = cur.fetchall()
    return {row["job_name"]: row["started_at"] for row in rows}


# ---------------------------------------------------------------------------
# Kill switch management
# ---------------------------------------------------------------------------


def activate_kill_switch(
    conn: psycopg.Connection[Any],
    reason: str,
    activated_by: str,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Activate the system-wide kill switch.

    Reads the prior state, writes the kill_switch UPDATE, and writes a
    runtime_config_audit row — all inside a single transaction so the
    audit `old_value` cannot race a concurrent toggle and the audit row
    cannot be skipped if the UPDATE succeeds.

    Raises RuntimeError if the kill_switch row is missing (configuration
    corruption) — the caller must not silently believe activation succeeded.
    Raises ValueError if `activated_by` or `reason` is empty — every
    transition must carry attribution, regardless of call site.
    """
    if not activated_by.strip():
        raise ValueError("activate_kill_switch: activated_by is required for attribution")
    if not reason.strip():
        raise ValueError("activate_kill_switch: reason is required when activating")
    now = now or _utcnow()
    with conn.transaction():
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute("SELECT is_active FROM kill_switch WHERE id = TRUE FOR UPDATE")
            prior = cur.fetchone()
        if prior is None:
            raise RuntimeError("kill_switch row missing — cannot activate; configuration corrupt")

        # RETURNING activated_at so the caller carries the DB-committed value
        # rather than the application-side `now`, eliminating any app/DB clock
        # skew in the response.
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                UPDATE kill_switch
                SET is_active = TRUE,
                    activated_at = %(at)s,
                    activated_by = %(by)s,
                    reason = %(reason)s
                WHERE id = TRUE
                RETURNING activated_at
                """,
                {"at": now, "by": activated_by, "reason": reason},
            )
            updated = cur.fetchone()
        if updated is None:  # pragma: no cover — SELECT FOR UPDATE proved row exists
            raise RuntimeError("kill_switch row missing — cannot activate; configuration corrupt")
        committed_at = updated["activated_at"]

        write_kill_switch_audit(
            conn,
            changed_by=activated_by,
            reason=reason,
            old_active=bool(prior["is_active"]),
            new_active=True,
            now=now,
        )
    logger.warning("Kill switch ACTIVATED by=%s reason=%s", activated_by, reason)
    # Return the values just committed inside the transaction so the caller
    # cannot race a concurrent toggle by re-reading after commit.
    return {
        "is_active": True,
        "activated_at": committed_at,
        "activated_by": activated_by,
        "reason": reason,
    }


def deactivate_kill_switch(
    conn: psycopg.Connection[Any],
    *,
    deactivated_by: str = "",
    reason: str = "",
    now: datetime | None = None,
) -> dict[str, Any]:
    """Deactivate the system-wide kill switch.

    Reads the prior state, writes the kill_switch UPDATE, and writes a
    runtime_config_audit row — all inside a single transaction.

    Raises RuntimeError if the kill_switch row is missing.
    Raises ValueError if `deactivated_by` is empty — every transition must
    carry attribution, regardless of call site.
    """
    if not deactivated_by.strip():
        raise ValueError("deactivate_kill_switch: deactivated_by is required for attribution")
    if not reason.strip():
        raise ValueError("deactivate_kill_switch: reason is required for attribution")
    now = now or _utcnow()
    with conn.transaction():
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute("SELECT is_active FROM kill_switch WHERE id = TRUE FOR UPDATE")
            prior = cur.fetchone()
        if prior is None:
            raise RuntimeError("kill_switch row missing — cannot deactivate; configuration corrupt")

        result = conn.execute(
            """
            UPDATE kill_switch
            SET is_active = FALSE,
                activated_at = NULL,
                activated_by = NULL,
                reason = NULL
            WHERE id = TRUE
            """,
        )
        if result.rowcount == 0:  # pragma: no cover
            raise RuntimeError("kill_switch row missing — cannot deactivate; configuration corrupt")

        write_kill_switch_audit(
            conn,
            changed_by=deactivated_by,
            reason=reason,
            old_active=bool(prior["is_active"]),
            new_active=False,
            now=now,
        )
    logger.info("Kill switch DEACTIVATED")
    return {
        "is_active": False,
        "activated_at": None,
        "activated_by": None,
        "reason": None,
    }


def get_kill_switch_status(
    conn: psycopg.Connection[Any],
) -> dict[str, Any]:
    """
    Return the current kill switch state.

    Returns a dict with keys: is_active, activated_at, activated_by, reason.
    If the row is missing (configuration corruption), returns
    is_active=True with a detail message — fail closed.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        # kill_switch has a single-row CHECK constraint (id = TRUE).
        cur.execute("SELECT is_active, activated_at, activated_by, reason FROM kill_switch")
        row = cur.fetchone()

    if row is None:
        # Missing row = configuration corruption → treat as active (fail closed).
        return {
            "is_active": True,
            "activated_at": None,
            "activated_by": None,
            "reason": "kill_switch row missing — configuration corrupt",
        }
    return dict(row)


def ensure_kill_switch_singleton(conn: psycopg.Connection[Any]) -> None:
    """Re-seed the kill_switch singleton row if it vanished (#1232).

    Migration ``sql/010_execution_guard.sql`` seeds the row via
    ``INSERT ... ON CONFLICT DO NOTHING`` — a one-time write. If the row
    is later lost (manual ``DELETE``, snapshot restore from pre-seed era,
    future bootstrap reset script), ``get_kill_switch_status`` fail-closes
    (returns ``is_active=True``), and the API ``deactivate`` path is
    structurally unable to recover because the UPDATE has no row to act
    on — see #1232 provenance from the 2026-05-19 T9-POST drive where
    one missing seed broke the operator-action retry path for hours.

    This boot-time guard inspects the singleton and re-seeds with the
    safe default (``is_active=FALSE``) on absence, writing one
    ``runtime_config_audit`` row with ``field='kill_switch'`` so the
    audit invariant ("every mutation writes one row") holds for boot
    recovery. WARNING log surfaces the recovery to the operator.

    Idempotent: no-op when exactly one row with ``id=TRUE`` exists.
    Fail-loud when a non-canonical row exists (``id != TRUE``; possible
    only under constraint corruption).

    Connection contract: caller MUST supply a conn in autocommit mode
    (mirrors ``ensure_runtime_config_singleton`` — see that helper's
    docstring for the SAVEPOINT-vs-COMMIT rationale). The helper opens
    its own real new transaction via ``conn.transaction()`` to keep the
    seed INSERT + the audit INSERT atomic.

    Race: ``ON CONFLICT DO NOTHING`` + ``RETURNING id`` suppresses our
    insert AND skips the audit row if another process re-seeded between
    our SELECT and our INSERT — no phantom audit rows.
    """
    if not conn.autocommit:
        raise RuntimeError(
            "ensure_kill_switch_singleton requires an autocommit "
            "connection — pass psycopg.connect(url, autocommit=True). "
            "The helper opens its own real BEGIN via conn.transaction(); "
            "a non-autocommit caller would degrade that into a SAVEPOINT."
        )

    with conn.cursor() as cur:
        cur.execute("SELECT id FROM kill_switch")
        rows = cur.fetchall()

    if len(rows) == 1 and rows[0][0] is True:
        return

    if len(rows) > 1 or (rows and rows[0][0] is not True):
        raise RuntimeError(f"kill_switch singleton constraint violated — rows={rows!r}")

    logger.warning(
        "kill_switch singleton vanished — re-seeding with safe default "
        "(is_active=FALSE). See docs/review-prevention-log.md section "
        "'Singleton-row migrations need a boot-time presence guard' + #1232."
    )

    with conn.transaction():
        inserted = conn.execute(
            """
            INSERT INTO kill_switch (id, is_active)
            VALUES (TRUE, FALSE)
            ON CONFLICT (id) DO NOTHING
            RETURNING id
            """
        ).fetchone()
        if inserted is None:
            return
        insert_runtime_config_audit_row(
            conn,
            changed_at=_utcnow(),
            changed_by=BOOT_RECOVERY_CHANGED_BY,
            reason=BOOT_RECOVERY_REASON,
            field="kill_switch",
            old_value=None,
            new_value="false",
        )
