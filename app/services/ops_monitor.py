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
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

import psycopg
import psycopg.rows

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

JobStatus = Literal["running", "success", "failure"]

LayerStatus = Literal["ok", "stale", "empty", "error"]

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
    """Check staleness for every monitored data layer."""
    now = now or _utcnow()
    return [check_layer_staleness(conn, layer, now=now) for layer in ALL_LAYERS]


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

    if status == "failure":
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
) -> None:
    """Activate the system-wide kill switch.

    Raises RuntimeError if the kill_switch row is missing (configuration
    corruption) — the caller must not silently believe activation succeeded.
    """
    now = now or _utcnow()
    result = conn.execute(
        """
        UPDATE kill_switch
        SET is_active = TRUE,
            activated_at = %(at)s,
            activated_by = %(by)s,
            reason = %(reason)s
        WHERE id = TRUE
        """,
        {"at": now, "by": activated_by, "reason": reason},
    )
    if result.rowcount == 0:
        raise RuntimeError("kill_switch row missing — cannot activate; configuration corrupt")
    conn.commit()
    logger.warning("Kill switch ACTIVATED by=%s reason=%s", activated_by, reason)


def deactivate_kill_switch(conn: psycopg.Connection[Any]) -> None:
    """Deactivate the system-wide kill switch.

    Raises RuntimeError if the kill_switch row is missing.
    """
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
    if result.rowcount == 0:
        raise RuntimeError("kill_switch row missing — cannot deactivate; configuration corrupt")
    conn.commit()
    logger.info("Kill switch DEACTIVATED")


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
