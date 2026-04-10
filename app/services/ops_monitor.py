"""
Ops monitor.

Responsibilities:
  - Check staleness of each data layer against expected refresh frequency.
  - Track job executions (start, finish, success/failure, row count).
  - Detect row-count spikes (broken-source indicator).
  - Manage the kill switch (activate / deactivate).
  - Produce a unified system health report for the health endpoint.

Data layers monitored:
  - universe   — instruments.last_seen_at
  - prices     — price_daily.price_date
  - quotes     — quotes.quoted_at
  - fundamentals — fundamentals_snapshot.as_of_date
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
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

import psycopg
import psycopg.rows

from app.services.runtime_config import write_kill_switch_audit

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
    "fundamentals": "SELECT MAX(as_of_date)::timestamptz AS latest FROM fundamentals_snapshot",
    "filings": "SELECT MAX(created_at) AS latest FROM filing_events",
    "news": "SELECT MAX(created_at) AS latest FROM news_events",
    "theses": "SELECT MAX(created_at) AS latest FROM theses",
    "scores": "SELECT MAX(scored_at) AS latest FROM scores",
}

# Minimum expected row count ratio.  If a job run produces fewer than
# (previous_count * threshold), it is flagged as a potential broken source.
_SPIKE_RATIO_THRESHOLD: float = 0.5

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


@dataclass(frozen=True)
class SpikeResult:
    job_name: str
    flagged: bool
    current_count: int
    previous_count: int | None = None
    detail: str = ""


@dataclass
class SystemHealth:
    checked_at: datetime
    layers: list[LayerHealth] = field(default_factory=list)
    jobs: list[JobHealth] = field(default_factory=list)
    kill_switch_active: bool = False
    kill_switch_detail: str = ""


# ---------------------------------------------------------------------------
# Staleness checks
# ---------------------------------------------------------------------------


def _utcnow() -> datetime:
    return datetime.now(tz=UTC)


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
) -> int:
    """
    Record the start of a scheduled job.  Returns the run_id.

    The caller should later call record_job_finish() with this run_id.
    """
    now = now or _utcnow()
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            INSERT INTO job_runs (job_name, started_at, status)
            VALUES (%(name)s, %(started)s, 'running')
            RETURNING run_id
            """,
            {"name": job_name, "started": now},
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
    now: datetime | None = None,
) -> None:
    """Record the completion of a scheduled job."""
    now = now or _utcnow()
    conn.execute(
        """
        UPDATE job_runs
        SET finished_at = %(finished)s,
            status      = %(status)s,
            row_count   = %(row_count)s,
            error_msg   = %(error_msg)s
        WHERE run_id = %(run_id)s
        """,
        {
            "finished": now,
            "status": status,
            "row_count": row_count,
            "error_msg": error_msg,
            "run_id": run_id,
        },
    )
    conn.commit()


def record_job_skip(
    conn: psycopg.Connection[Any],
    job_name: str,
    reason: str,
    *,
    now: datetime | None = None,
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
    """
    assert conn.autocommit, (
        "record_job_skip requires autocommit=True so conn.transaction() issues a real BEGIN/COMMIT, not a savepoint"
    )
    now = now or _utcnow()
    with conn.transaction():
        row = conn.execute(
            """
            INSERT INTO job_runs (job_name, started_at, finished_at, status, row_count, error_msg)
            VALUES (%(name)s, %(ts)s, %(ts)s, 'skipped', 0, %(reason)s)
            RETURNING run_id
            """,
            {"name": job_name, "ts": now, "reason": reason},
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
# Row-count spike detection
# ---------------------------------------------------------------------------


def check_row_count_spike(
    conn: psycopg.Connection[Any],
    job_name: str,
    current_count: int,
    *,
    exclude_run_id: int | None = None,
) -> SpikeResult:
    """
    Compare current_count against the previous successful run's row_count.

    Flags when current_count < previous_count * _SPIKE_RATIO_THRESHOLD.
    This detects broken data sources that silently return fewer rows than
    expected.

    exclude_run_id: if provided, excludes this run from the comparison query
    so the current run does not compare against itself.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT row_count
            FROM job_runs
            WHERE job_name = %(name)s
              AND status = 'success'
              AND row_count IS NOT NULL
              AND (%(exclude_id)s IS NULL OR run_id != %(exclude_id)s)
            ORDER BY started_at DESC
            LIMIT 1
            """,
            {"name": job_name, "exclude_id": exclude_run_id},
        )
        row = cur.fetchone()

    if row is None or row["row_count"] is None:
        # No prior successful run with a row count — nothing to compare.
        return SpikeResult(
            job_name=job_name,
            flagged=False,
            current_count=current_count,
            detail=f"{job_name}: no prior row_count to compare",
        )

    previous_count: int = int(row["row_count"])
    if previous_count == 0:
        # Previous run also had zero rows — not a spike.
        return SpikeResult(
            job_name=job_name,
            flagged=False,
            current_count=current_count,
            previous_count=previous_count,
            detail=f"{job_name}: previous count was 0, skip comparison",
        )

    ratio = current_count / previous_count
    if ratio < _SPIKE_RATIO_THRESHOLD:
        return SpikeResult(
            job_name=job_name,
            flagged=True,
            current_count=current_count,
            previous_count=previous_count,
            detail=(
                f"{job_name}: row_count dropped from {previous_count} to "
                f"{current_count} (ratio={ratio:.2f} < threshold={_SPIKE_RATIO_THRESHOLD})"
            ),
        )

    return SpikeResult(
        job_name=job_name,
        flagged=False,
        current_count=current_count,
        previous_count=previous_count,
    )


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


# ---------------------------------------------------------------------------
# Unified system health
# ---------------------------------------------------------------------------


def get_system_health(
    conn: psycopg.Connection[Any],
    job_names: list[str] | None = None,
) -> SystemHealth:
    """
    Build a full system health report: layer staleness, job health, kill switch.

    job_names: list of job names to check health for.  If None, only layer
    staleness and kill switch are included.
    """
    now = _utcnow()

    layers = check_all_layers(conn, now=now)

    jobs: list[JobHealth] = []
    if job_names:
        jobs = [check_job_health(conn, name) for name in job_names]

    ks = get_kill_switch_status(conn)

    ks_detail = ""
    if ks["is_active"]:
        ks_detail = "kill switch active"
        if ks.get("reason"):
            ks_detail += f"; reason: {ks['reason']}"

    return SystemHealth(
        checked_at=now,
        layers=layers,
        jobs=jobs,
        kill_switch_active=bool(ks["is_active"]),
        kill_switch_detail=ks_detail,
    )
