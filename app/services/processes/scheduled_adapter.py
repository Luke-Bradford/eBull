"""Scheduled-job → ProcessRow adapter for the admin control hub.

Issue #1071 (umbrella #1064).
Spec: ``docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md``
      §Adapter map / scheduled_job.

Translates the static ``SCHEDULED_JOBS`` registry plus the live
``job_runs`` + ``pending_job_requests`` tables into one ``ProcessRow``
per declared job. ``process_id`` is the job_name verbatim — a stable,
operator-readable identifier already used throughout the codebase.

Auto-hide-on-retry rule (spec §Auto-hide-on-retry rule): when the
latest terminal run of a job is ``failure`` AND a manual_job request is
currently in flight (``pending_job_requests`` row for the same job_name
in pending|claimed|dispatched), the row is rendered as ``running`` with
``last_n_errors`` empty so a retry that is about to re-fetch the failed
scope does not show stale red chips. The 'pending_retry' status (next
scheduled fire within the freshness window) requires watermark info and
is wired in PR4 — PR3 conservatively returns 'failed' for the
no-in-flight-retry case.

PR3 leaves ``ProcessRow.watermark = None``. PR4 wires per-cursor-kind
resolution.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
import psycopg.rows

from app.services.processes import (
    ActiveRunSummary,
    ErrorClassSummary,
    ProcessLane,
    ProcessRow,
    ProcessRunSummary,
    ProcessStatus,
    RunStatus,
    StaleReason,
)
from app.services.processes.stale_detection import (
    QUEUE_STUCK_THRESHOLD_S,
    WATERMARK_GAP_TOLERANCE_S,
)
from app.services.processes.stale_detection import (
    compute as compute_stale_reasons,
)
from app.services.processes.watermarks import (
    freshness_source_for,
    manifest_source_for,
    resolve_watermark,
)
from app.workers.scheduler import (
    SCHEDULED_JOBS,
    Cadence,
    ScheduledJob,
    compute_next_run,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lane assignment
# ---------------------------------------------------------------------------
#
# PR3 maps each scheduled job into one of the eight operator-facing
# lanes (spec §Process envelope). Unknown jobs default to ``ops``. The
# mapping lives here rather than on ``ScheduledJob`` so adding a lane
# field to that dataclass — touching every job declaration — can be
# deferred to PR8 + PR9 polish.

_LANE_BY_JOB: dict[str, ProcessLane] = {
    # Universe
    "nightly_universe_sync": "universe",
    "etoro_lookups_refresh": "universe",
    "exchanges_metadata_refresh": "universe",
    "cusip_universe_backfill": "universe",
    "cusip_extid_sweep": "universe",
    # Candles / market data
    "daily_candle_refresh": "candles",
    "fx_rates_refresh": "candles",
    # SEC ingest (filings + identifiers)
    "daily_cik_refresh": "sec",
    "daily_financial_facts": "sec",
    "sec_filing_documents_ingest": "sec",
    "sec_8k_events_ingest": "sec",
    "sec_dividend_calendar_ingest": "sec",
    # Ownership (insider / institutional / fund)
    "ownership_observations_sync": "ownership",
    "ownership_observations_backfill": "ownership",
    "sec_form3_ingest": "ownership",
    "sec_insider_transactions_ingest": "ownership",
    "sec_insider_transactions_backfill": "ownership",
    "sec_def14a_ingest": "ownership",
    "sec_def14a_bootstrap": "ownership",
    "sec_13f_filer_directory_sync": "ownership",
    "sec_13f_quarterly_sweep": "ownership",
    "sec_nport_filer_directory_sync": "ownership",
    "sec_n_port_ingest": "ownership",
    # Fundamentals
    "fundamentals_sync": "fundamentals",
    "sec_business_summary_ingest": "fundamentals",
    "sec_business_summary_bootstrap": "fundamentals",
    # Ops
    "monitor_positions": "ops",
    "attribution_summary": "ops",
    "weekly_report": "ops",
    "monthly_report": "ops",
    "raw_data_retention_sweep": "ops",
    "daily_tax_reconciliation": "ops",
    "daily_portfolio_sync": "ops",
    "retry_deferred_recommendations": "ops",
    "execute_approved_orders": "ops",
    "seed_cost_models": "ops",
    "morning_candidate_review": "ops",
    "daily_research_refresh": "ops",
    "orchestrator_full_sync": "ops",
    "orchestrator_high_frequency_sync": "ops",
}


def _lane_for(job_name: str) -> ProcessLane:
    return _LANE_BY_JOB.get(job_name, "ops")


# ---------------------------------------------------------------------------
# Cron rendering
# ---------------------------------------------------------------------------
#
# ``cadence_cron`` is informational ("show the operator the literal
# crontab line"). APScheduler runs in the jobs process so we cannot
# read its trigger from the API; rebuild the cron string from
# ``Cadence`` directly. Linux crontab weekday convention is 0=Sun..6=Sat
# while ``Cadence.weekday`` is 0=Mon..6=Sun (Python datetime), so the
# render shifts the field by one — Cadence Mon (0) → cron 1, Cadence
# Sun (6) → cron 0.


def _cron_for(cadence: Cadence) -> str:
    if cadence.kind == "every_n_minutes":
        return f"*/{cadence.interval_minutes} * * * *"
    if cadence.kind == "hourly":
        return f"{cadence.minute} * * * *"
    if cadence.kind == "daily":
        return f"{cadence.minute} {cadence.hour} * * *"
    if cadence.kind == "weekly":
        cron_dow = (cadence.weekday + 1) % 7
        return f"{cadence.minute} {cadence.hour} * * {cron_dow}"
    if cadence.kind == "monthly":
        return f"{cadence.minute} {cadence.hour} {cadence.day} * *"
    # Cadence.kind is a Literal — every value is enumerated above. The
    # fallthrough is a safety net for a future-added kind that forgets
    # to extend this function.
    raise ValueError(f"unsupported cadence kind: {cadence.kind!r}")


# ---------------------------------------------------------------------------
# Status mapping
# ---------------------------------------------------------------------------

_RUN_STATUS_TO_SUMMARY: dict[str, RunStatus] = {
    "success": "success",
    "failure": "failure",
    "skipped": "skipped",
    "cancelled": "cancelled",
}


def _status_for(
    *,
    has_running_row: bool,
    last_terminal_status: str | None,
    has_inflight_request: bool,
    kill_switch_active: bool,
    failed_scope_covered: bool,
) -> ProcessStatus:
    """Compute ProcessRow.status from the per-job inputs.

    Order matters:

    * ``disabled`` wins when kill_switch is on (the row is not actionable
      until it flips back).
    * ``running`` wins when a job_runs row is in flight, OR when the
      latest terminal was a failure AND a retry is in flight (auto-hide).
    * ``pending_retry`` when the latest terminal was failure, no retry
      is in flight, but the next scheduled fire provably covers the
      failed scope (freshness recheck or manifest retry within the
      next-fire window — spec §"Auto-hide-on-retry rule" / "Covered"
      check). Caller surfaces empty ``last_n_errors`` for this state.
    * ``failed`` wins when the latest terminal is failure with no retry
      in flight AND the failed scope is not covered.
    * ``ok`` for success; ``cancelled`` for cancelled; ``idle`` /
      ``pending_first_run`` when no terminal exists.
    """
    if kill_switch_active:
        return "disabled"
    if has_running_row:
        return "running"
    if last_terminal_status == "failure":
        if has_inflight_request:
            # Auto-hide: caller will set last_n_errors=(), status=running
            return "running"
        if failed_scope_covered:
            # Auto-hide: next scheduled fire will reattempt the failed
            # scope; caller surfaces last_n_errors=().
            return "pending_retry"
        return "failed"
    if last_terminal_status == "success":
        return "ok"
    if last_terminal_status == "cancelled":
        return "cancelled"
    if last_terminal_status == "skipped":
        # 'skipped' is a benign terminal — prerequisite not yet met. The
        # operator should see this as 'idle' so the row is not painted red.
        return "idle"
    # No history at all.
    return "pending_first_run"


# ---------------------------------------------------------------------------
# DB readers
# ---------------------------------------------------------------------------


def _kill_switch_active(conn: psycopg.Connection[Any]) -> bool:
    """Read the kill_switch singleton. Fail closed on missing row.

    Mirrors ``ops_monitor.get_kill_switch_status`` semantics so the
    adapter is honest under configuration corruption.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT is_active FROM kill_switch")
        row = cur.fetchone()
    if row is None:
        return True
    return bool(row[0])


def _read_running_run(conn: psycopg.Connection[Any], *, job_name: str) -> dict[str, Any] | None:
    """Latest in-flight job_runs row for the job, or None.

    Multiple ``running`` rows can theoretically coexist if a prior crash
    left a stranded entry — take the newest by ``started_at`` so the
    operator sees the live one rather than a ghost.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT run_id, started_at, processed_count, target_count,
                   last_progress_at, warnings_count, cancel_requested_at
              FROM job_runs
             WHERE job_name = %(name)s
               AND status   = 'running'
             ORDER BY started_at DESC
             LIMIT 1
            """,
            {"name": job_name},
        )
        return cur.fetchone()


def _read_latest_terminal_run(conn: psycopg.Connection[Any], *, job_name: str) -> dict[str, Any] | None:
    """Latest terminal job_runs row (success / failure / skipped / cancelled)."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT run_id, started_at, finished_at, status, row_count,
                   error_msg, error_classes, rows_skipped_by_reason,
                   rows_errored, cancelled_at
              FROM job_runs
             WHERE job_name = %(name)s
               AND status   IN ('success', 'failure', 'skipped', 'cancelled')
             ORDER BY started_at DESC
             LIMIT 1
            """,
            {"name": job_name},
        )
        return cur.fetchone()


def _has_inflight_manual_request(conn: psycopg.Connection[Any], *, job_name: str) -> bool:
    """True if a manual_job pending_job_requests row is live for this job."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
              FROM pending_job_requests
             WHERE request_kind = 'manual_job'
               AND job_name     = %(name)s
               AND status       IN ('pending', 'claimed', 'dispatched')
             LIMIT 1
            """,
            {"name": job_name},
        )
        return cur.fetchone() is not None


def _has_data_freshness_gap(
    conn: psycopg.Connection[Any],
    *,
    source: str,
    deadline: datetime,
) -> bool:
    """True when at least one ``data_freshness_index`` row for ``source``
    has ``expected_next_at IS NOT NULL`` AND ``expected_next_at <
    deadline``.

    Operator-amendment §A1.2 watermark_gap probe (PR8 / #1083). NULL
    ``expected_next_at`` rows are excluded — a filer with no historical
    filed_at has nothing to predict, so a NULL must NOT fire the gap
    rule (Codex pre-impl review WARNING).

    The query uses LIMIT 1 — adapters only need the boolean signal,
    not the count, and the partial index on
    ``(expected_next_at, source)`` (sql/120:115) makes the probe cheap.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
              FROM data_freshness_index
             WHERE source = %(source)s
               AND expected_next_at IS NOT NULL
               AND expected_next_at < %(deadline)s
             LIMIT 1
            """,
            {"source": source, "deadline": deadline},
        )
        return cur.fetchone() is not None


def _has_dispatched_queue_age(
    conn: psycopg.Connection[Any],
    *,
    process_id: str,
    deadline: datetime,
) -> bool:
    """True when at least one ``pending_job_requests`` row for
    ``process_id`` has ``status='dispatched'`` AND worker pickup older
    than ``deadline``.

    Operator-amendment §A1.3 queue_stuck probe (PR8 / #1083). The
    timestamp used is ``COALESCE(claimed_at, requested_at)`` —
    ``claimed_at`` is set on ``pending → claimed`` (sql/084:24,
    dispatcher.py:183) and a row in ``status='dispatched'`` will
    almost always have it populated, but a buggy NULL would otherwise
    silently skip the stuck row. Falling back to ``requested_at`` is
    conservative (older or equal to claimed_at) so the rule is still
    correct.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
              FROM pending_job_requests
             WHERE process_id = %(pid)s
               AND status     = 'dispatched'
               AND COALESCE(claimed_at, requested_at) < %(deadline)s
             LIMIT 1
            """,
            {"pid": process_id, "deadline": deadline},
        )
        return cur.fetchone() is not None


def _has_pending_full_wash_fence(conn: psycopg.Connection[Any], *, process_id: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
              FROM pending_job_requests
             WHERE process_id = %(pid)s
               AND mode       = 'full_wash'
               AND status     IN ('pending', 'claimed', 'dispatched')
             LIMIT 1
            """,
            {"pid": process_id},
        )
        return cur.fetchone() is not None


def _freshness_failure_counts(
    conn: psycopg.Connection[Any],
    *,
    source: str,
    deadline: datetime,
) -> tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE state = 'error') AS total,
                COUNT(*) FILTER (
                  WHERE state = 'error'
                    AND (next_recheck_at IS NULL OR next_recheck_at > %(deadline)s)
                ) AS uncovered
              FROM data_freshness_index
             WHERE source = %(source)s
            """,
            {"source": source, "deadline": deadline},
        )
        row = cur.fetchone()
    if row is None:
        return (0, 0)
    return (int(row[0]), int(row[1]))


def _manifest_failure_counts(
    conn: psycopg.Connection[Any],
    *,
    source: str,
    deadline: datetime,
) -> tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE ingest_status = 'failed') AS total,
                COUNT(*) FILTER (
                  WHERE ingest_status = 'failed'
                    AND (next_retry_at IS NULL OR next_retry_at > %(deadline)s)
                ) AS uncovered
              FROM sec_filing_manifest
             WHERE source = %(source)s
            """,
            {"source": source, "deadline": deadline},
        )
        row = cur.fetchone()
    if row is None:
        return (0, 0)
    return (int(row[0]), int(row[1]))


def _is_failed_scope_covered(
    conn: psycopg.Connection[Any],
    *,
    process_id: str,
    next_fire_at: datetime | None,
    kill_switch_active: bool,
) -> bool:
    """True when the next scheduled fire provably reattempts the failed scope.

    Spec §"Auto-hide-on-retry rule" / "Covered" check (post-W2).

    Codex pre-push round 1 BLOCKING: the check must prove EVERY
    failed row has retry coverage; an existential probe was wrong.

    Codex pre-push round 2 BLOCKING: for jobs with BOTH a
    ``freshness_source`` and a ``manifest_source`` (e.g.
    ``sec_filing_documents_ingest`` post-WARNING fix), the check must
    consider ALL applicable sources together. A short-circuit on the
    first covered source could auto-hide errors while another source's
    failed rows remain uncovered.

    Final shape: enumerate every applicable source. Any uncovered
    failure on any applicable source → False. At least one source
    must contribute a positive ``total`` (otherwise nothing is
    actionable and there is no scope to cover).

    Kill-switch + no-cadence both short-circuit to False so a paused
    or one-shot job never enters auto-hide.
    """
    if kill_switch_active or next_fire_at is None:
        return False
    freshness_source = freshness_source_for(process_id)
    manifest_source = manifest_source_for(process_id)
    if freshness_source is None and manifest_source is None:
        return False
    cumulative_total = 0
    if freshness_source is not None:
        total, uncovered = _freshness_failure_counts(conn, source=freshness_source, deadline=next_fire_at)
        if uncovered > 0:
            return False
        cumulative_total += total
    if manifest_source is not None:
        total, uncovered = _manifest_failure_counts(conn, source=manifest_source, deadline=next_fire_at)
        if uncovered > 0:
            return False
        cumulative_total += total
    return cumulative_total > 0


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def _build_active_run(active_row: dict[str, Any]) -> ActiveRunSummary:
    target = active_row["target_count"]
    processed = int(active_row["processed_count"]) if active_row["processed_count"] is not None else 0
    last_progress_at = active_row.get("last_progress_at")
    return ActiveRunSummary(
        run_id=int(active_row["run_id"]),
        started_at=active_row["started_at"],
        rows_processed_so_far=processed if processed > 0 else None,
        progress_units_done=processed if target is not None else None,
        progress_units_total=int(target) if target is not None else None,
        last_progress_at=last_progress_at,
        is_cancelling=active_row["cancel_requested_at"] is not None,
    )


def _build_last_run(terminal_row: dict[str, Any]) -> ProcessRunSummary:
    started_at: datetime = terminal_row["started_at"]
    finished_at: datetime | None = terminal_row["finished_at"]
    duration = (finished_at - started_at).total_seconds() if finished_at else 0.0
    summary_status = _RUN_STATUS_TO_SUMMARY.get(terminal_row["status"], "skipped")
    skips = terminal_row.get("rows_skipped_by_reason") or {}
    return ProcessRunSummary(
        run_id=int(terminal_row["run_id"]),
        started_at=started_at,
        finished_at=finished_at if finished_at is not None else started_at,
        duration_seconds=duration,
        rows_processed=terminal_row.get("row_count"),
        rows_skipped_by_reason={k: int(v) for k, v in skips.items()},
        rows_errored=int(terminal_row.get("rows_errored") or 0),
        status=summary_status,
        cancelled_by_operator_id=None,  # PR4 wires the join on process_stop_requests
    )


def _build_error_summaries(
    error_classes: dict[str, Any] | None,
) -> tuple[ErrorClassSummary, ...]:
    """Translate the ``job_runs.error_classes`` JSONB into envelope shape.

    JSONB shape (sql/137 header): ``{"<error_class>": {"count": N,
    "sample_message": "...", "last_subject": "...",
    "last_seen_at": "ISO-8601"}}``. Keys with malformed values are
    skipped — the producer-side aggregator pins the shape, but defensive
    parsing prevents a single corrupt row from breaking the whole row.
    """
    if not error_classes:
        return ()
    summaries: list[ErrorClassSummary] = []
    for error_class, payload in error_classes.items():
        if not isinstance(payload, dict):
            continue
        last_seen_raw = payload.get("last_seen_at")
        if not isinstance(last_seen_raw, str):
            continue
        try:
            last_seen_at = datetime.fromisoformat(last_seen_raw)
        except ValueError:
            continue
        # Producer writes UTC ISO strings (datetime.now(UTC).isoformat());
        # coerce naive parses to UTC so downstream comparisons are safe
        # (prevention-log "Naive datetime in TIMESTAMPTZ query params" #80).
        if last_seen_at.tzinfo is None:
            last_seen_at = last_seen_at.replace(tzinfo=UTC)
        summaries.append(
            ErrorClassSummary(
                error_class=str(error_class),
                count=int(payload.get("count", 0)),
                last_seen_at=last_seen_at,
                sample_message=str(payload.get("sample_message", ""))[:500],
                sample_subject=(str(payload["last_subject"]) if payload.get("last_subject") else None),
            )
        )
    return tuple(summaries)


def _build_row(
    job: ScheduledJob,
    *,
    conn: psycopg.Connection[Any],
    active_row: dict[str, Any] | None,
    terminal_row: dict[str, Any] | None,
    has_inflight_request: bool,
    fence_held: bool,
    kill_switch_active: bool,
) -> ProcessRow:
    # next_fire_at: always compute against ``now`` rather than the last
    # successful run — the operator wants "when will this fire next?",
    # which APScheduler answers identically regardless of the prior
    # outcome. ``compute_next_run`` is pure cadence math so it is safe to
    # call from the API process (no APScheduler consult). PR4 needs it
    # before status computation so the covered-check can compare against
    # the next-fire deadline.
    next_fire_at: datetime | None = compute_next_run(job.cadence, datetime.now(UTC))

    last_terminal_status = terminal_row["status"] if terminal_row is not None else None
    failed_scope_covered = False
    if last_terminal_status == "failure" and not has_inflight_request:
        # Only probe the per-source covered check when it could affect
        # the status (last terminal failed AND no explicit retry is
        # already in flight).
        failed_scope_covered = _is_failed_scope_covered(
            conn,
            process_id=job.name,
            next_fire_at=next_fire_at,
            kill_switch_active=kill_switch_active,
        )

    process_status = _status_for(
        has_running_row=active_row is not None,
        last_terminal_status=last_terminal_status,
        has_inflight_request=has_inflight_request,
        kill_switch_active=kill_switch_active,
        failed_scope_covered=failed_scope_covered,
    )

    last_run = _build_last_run(terminal_row) if terminal_row is not None else None
    active_run = _build_active_run(active_row) if active_row is not None else None

    # Auto-hide-on-retry: hide error chips when status is running (retry
    # in flight) or pending_retry (next fire covers failed scope). Only
    # surface grouped errors in the actionable ``failed`` state.
    last_n_errors: tuple[ErrorClassSummary, ...] = ()
    if process_status == "failed" and terminal_row is not None:
        last_n_errors = _build_error_summaries(terminal_row.get("error_classes"))

    # Stale-reason probes — operator-amendment §A1 four-case model
    # (PR8 / #1083). Each probe is a LIMIT 1 read; only run them when
    # the rule's mechanism gate could fire to keep the snapshot read
    # cheap.
    now = datetime.now(UTC)
    freshness_source = freshness_source_for(job.name)
    has_data_freshness_gap = (
        freshness_source is not None
        and process_status != "running"
        and _has_data_freshness_gap(
            conn,
            source=freshness_source,
            deadline=now - timedelta(seconds=WATERMARK_GAP_TOLERANCE_S),
        )
    )
    has_dispatched_queue_age = _has_dispatched_queue_age(
        conn,
        process_id=job.name,
        deadline=now - timedelta(seconds=QUEUE_STUCK_THRESHOLD_S),
    )
    stale_reasons: tuple[StaleReason, ...] = compute_stale_reasons(
        mechanism="scheduled_job",
        status=process_status,
        next_fire_at=next_fire_at,
        has_data_freshness_gap=has_data_freshness_gap,
        has_dispatched_queue_age=has_dispatched_queue_age,
        last_progress_at=active_run.last_progress_at if active_run is not None else None,
        active_run_started_at=active_run.started_at if active_run is not None else None,
        process_id=job.name,
        now=now,
    )

    # PR4 watermark — surface the resume cursor on the FE tooltip.
    # Returns None for jobs without a registered source (heartbeat,
    # monitor_positions, …); the FE renders that as "no resume cursor".
    watermark = resolve_watermark(conn, process_id=job.name, mechanism="scheduled_job")

    can_cancel = (
        active_run is not None and active_run.run_id is not None and process_status == "running"
        # short-runners (heartbeat etc.) are not worth cooperative-cancel;
        # spec marks them can_cancel=False but PR3 doesn't know which jobs
        # cooperate — leave True universally and let PR8 trim the list
        # once the per-job checkpoint catalogue is wired.
    )

    return ProcessRow(
        process_id=job.name,
        display_name=job.name,
        lane=_lane_for(job.name),
        mechanism="scheduled_job",
        status=process_status,
        last_run=last_run,
        active_run=active_run,
        cadence_human=job.cadence.label,
        cadence_cron=_cron_for(job.cadence),
        next_fire_at=next_fire_at,
        watermark=watermark,
        can_iterate=(
            not kill_switch_active and not has_inflight_request and not fence_held and process_status != "running"
        ),
        can_full_wash=(not kill_switch_active and not fence_held and process_status != "running"),
        can_cancel=can_cancel,
        last_n_errors=last_n_errors,
        stale_reasons=stale_reasons,
    )


# ---------------------------------------------------------------------------
# Public entrypoints
# ---------------------------------------------------------------------------


def list_rows(conn: psycopg.Connection[Any]) -> list[ProcessRow]:
    """Return one ProcessRow per declared scheduled job.

    Caller MUST be inside a ``snapshot_read(conn)`` block. The kill_switch
    read is shared across rows — capture it once at the top so a switch
    flip mid-loop cannot produce a row-by-row inconsistency.
    """
    kill_switch_active = _kill_switch_active(conn)
    rows: list[ProcessRow] = []
    for job in SCHEDULED_JOBS:
        active_row = _read_running_run(conn, job_name=job.name)
        terminal_row = _read_latest_terminal_run(conn, job_name=job.name)
        has_inflight_request = _has_inflight_manual_request(conn, job_name=job.name)
        fence_held = _has_pending_full_wash_fence(conn, process_id=job.name)
        rows.append(
            _build_row(
                job,
                conn=conn,
                active_row=active_row,
                terminal_row=terminal_row,
                has_inflight_request=has_inflight_request,
                fence_held=fence_held,
                kill_switch_active=kill_switch_active,
            )
        )
    return rows


def get_row(conn: psycopg.Connection[Any], *, process_id: str) -> ProcessRow | None:
    """Return a single ProcessRow for ``process_id`` (= job_name)."""
    job = next((j for j in SCHEDULED_JOBS if j.name == process_id), None)
    if job is None:
        return None
    return _build_row(
        job,
        conn=conn,
        active_row=_read_running_run(conn, job_name=job.name),
        terminal_row=_read_latest_terminal_run(conn, job_name=job.name),
        has_inflight_request=_has_inflight_manual_request(conn, job_name=job.name),
        fence_held=_has_pending_full_wash_fence(conn, process_id=job.name),
        kill_switch_active=_kill_switch_active(conn),
    )


def list_runs(conn: psycopg.Connection[Any], *, process_id: str, days: int) -> list[ProcessRunSummary]:
    """Return last-N-days terminal runs for the History tab."""
    if days <= 0:
        raise ValueError("days must be positive")
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT run_id, started_at, finished_at, status, row_count,
                   rows_skipped_by_reason, rows_errored, cancelled_at
              FROM job_runs
             WHERE job_name   = %(name)s
               AND status     IN ('success', 'failure', 'skipped', 'cancelled')
               AND started_at >= now() - (%(days)s::int * INTERVAL '1 day')
             ORDER BY started_at DESC
            """,
            {"name": process_id, "days": days},
        )
        rows = cur.fetchall()
    return [_build_last_run(row) for row in rows]


def list_run_errors(conn: psycopg.Connection[Any], *, process_id: str, run_id: int) -> tuple[ErrorClassSummary, ...]:
    """Return the grouped error_classes for a specific run."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT error_classes
              FROM job_runs
             WHERE run_id   = %(run_id)s
               AND job_name = %(name)s
            """,
            {"run_id": run_id, "name": process_id},
        )
        row = cur.fetchone()
    if row is None:
        return ()
    return _build_error_summaries(row.get("error_classes"))


__all__ = ["get_row", "list_run_errors", "list_rows", "list_runs"]
