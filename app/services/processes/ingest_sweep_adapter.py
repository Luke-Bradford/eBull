"""Ingest-sweep → ProcessRow adapter.

Issue #1078 (umbrella #1064) — admin control hub PR6.
Spec: ``docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md``
      §"Adapter map" / ingest_sweep + §PR6.

Aggregates per-source state from ``sec_filing_manifest`` +
``data_freshness_index`` + per-source ingest logs
(``institutional_holdings_ingest_log``, ``n_port_ingest_log``) and emits
one ``ProcessRow`` per logical sweep. Surface is operator-facing
SOURCE-LEVEL state: how many subjects are awaiting next poll, how many
manifest rows are stuck failed, what error classes dominate. The
underlying scheduled cron job (e.g. ``sec_filing_documents_ingest``)
continues to surface as its own ``mechanism="scheduled_job"`` row —
that's the row the operator triggers / cancels. Ingest-sweep rows are
READ-ONLY in v1 (``can_iterate`` / ``can_full_wash`` / ``can_cancel``
all False); operator triggers via the underlying scheduled_job.

Status derivation (post-Codex round 1 H2 fix — sweeps don't carry
their own ``next_fire_at``, so ``pending_retry`` is NEVER emitted by
ingest_sweep rows; the underlying scheduled_job row is the retry
surface that holds the covered-check):

* ``running`` when the underlying scheduled_job has a ``manual_job``
  request in flight OR a ``job_runs.status='running'`` row.
* ``failed`` when freshness rows in ``state='error'`` exist OR (manifest
  sweeps) ``sec_filing_manifest.ingest_status='failed'`` exist.
* ``ok`` otherwise.

Failure-mode invariant (spec §Failure-mode invariants): adapter
exceptions BUBBLE UP — the cross-adapter snapshot loop catches and
flips ``partial=True``. Per-row exceptions are NOT swallowed.

Out of v1: ``cusip_universe_sweep`` / ``cik_refresh_sweep`` — those
sources have no ``sec_filing_manifest`` rows + no per-source ingest
log to aggregate, so the existing scheduled_job rows already carry the
operator-visible state. Documented inline; follow-up under #1064 if
reviewer pushes back.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import psycopg
import psycopg.rows
import psycopg.sql

from app.services.processes import (
    ErrorClassSummary,
    ProcessLane,
    ProcessRow,
    ProcessRunSummary,
    ProcessStatus,
    RunStatus,
)
from app.services.processes.watermarks import resolve_watermark

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Sweep registry
# ---------------------------------------------------------------------------


_LogShape = (
    None  # freshness-only: errors come from data_freshness_index.state_reason
    | str  # per-source ingest log table name
)


@dataclass(frozen=True, slots=True)
class _SweepSpec:
    """One logical sweep row.

    ``manifest_source`` and ``freshness_source`` mirror the source
    enums on ``sec_filing_manifest`` / ``data_freshness_index``.
    ``log_table`` is the per-source ingest log table when one exists
    (NPORT, 13F-HR); freshness-only sweeps leave it None and read errors
    from ``data_freshness_index.state_reason``.

    ``underlying_job`` is the scheduled_job that drives the sweep —
    used to derive ``running`` status (the sweep is "running" when its
    cron job is actively draining).
    """

    process_id: str
    display_name: str
    lane: ProcessLane
    manifest_source: str | None
    freshness_source: str | None
    log_table: str | None
    underlying_job: str
    cadence_human: str


_SWEEPS: tuple[_SweepSpec, ...] = (
    _SweepSpec(
        process_id="sec_form3_sweep",
        display_name="Form 3 (initial insider) sweep",
        lane="ownership",
        manifest_source=None,
        freshness_source="sec_form3",
        log_table=None,
        underlying_job="sec_form3_ingest",
        cadence_human="freshness-driven",
    ),
    _SweepSpec(
        process_id="sec_form4_sweep",
        display_name="Form 4 (insider transactions) sweep",
        lane="ownership",
        manifest_source="sec_form4",
        freshness_source="sec_form4",
        log_table=None,
        # The Form 4 manifest worker drains rows; the freshness scheduler
        # discovers new accessions. The manifest worker is the dominant
        # signal for "running"; the freshness scheduler also writes to
        # the same source via ``daily_cik_refresh`` indirectly.
        underlying_job="sec_filing_documents_ingest",
        cadence_human="manifest-driven",
    ),
    _SweepSpec(
        process_id="sec_def14a_sweep",
        display_name="DEF 14A sweep",
        lane="ownership",
        manifest_source=None,
        freshness_source="sec_def14a",
        log_table=None,
        underlying_job="sec_def14a_ingest",
        cadence_human="freshness-driven",
    ),
    _SweepSpec(
        process_id="sec_8k_sweep",
        display_name="8-K sweep",
        lane="sec",
        manifest_source=None,
        freshness_source="sec_8k",
        log_table=None,
        underlying_job="sec_8k_events_ingest",
        cadence_human="freshness-driven",
    ),
    _SweepSpec(
        process_id="sec_13f_sweep",
        display_name="13F holdings sweep",
        lane="ownership",
        manifest_source="sec_13f_hr",
        freshness_source=None,
        log_table="institutional_holdings_ingest_log",
        underlying_job="sec_13f_quarterly_sweep",
        cadence_human="quarterly",
    ),
    _SweepSpec(
        process_id="nport_sweep",
        display_name="N-PORT (fund holdings) sweep",
        lane="ownership",
        manifest_source="sec_n_port",
        freshness_source=None,
        log_table="n_port_ingest_log",
        underlying_job="sec_n_port_ingest",
        cadence_human="monthly",
    ),
)

_SWEEP_BY_ID: dict[str, _SweepSpec] = {s.process_id: s for s in _SWEEPS}


def sweep_process_ids() -> tuple[str, ...]:
    """Public registry — `_resolve_mechanism` consults this to route ids."""
    return tuple(_SWEEP_BY_ID.keys())


def is_sweep(process_id: str) -> bool:
    return process_id in _SWEEP_BY_ID


# ---------------------------------------------------------------------------
# DB readers
# ---------------------------------------------------------------------------


def _kill_switch_active(conn: psycopg.Connection[Any]) -> bool:
    """Mirror ``scheduled_adapter._kill_switch_active``."""
    with conn.cursor() as cur:
        cur.execute("SELECT is_active FROM kill_switch")
        row = cur.fetchone()
    if row is None:
        return True
    return bool(row[0])


def _underlying_job_running(conn: psycopg.Connection[Any], *, job_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
              FROM job_runs
             WHERE job_name = %(name)s
               AND status   = 'running'
             LIMIT 1
            """,
            {"name": job_name},
        )
        return cur.fetchone() is not None


def _underlying_job_pending(conn: psycopg.Connection[Any], *, job_name: str) -> bool:
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


def _freshness_error_summaries(
    conn: psycopg.Connection[Any],
    *,
    source: str,
    limit: int = 10,
) -> tuple[ErrorClassSummary, ...]:
    """Group ``data_freshness_index`` error rows by ``state_reason``.

    Codex pre-impl review H3: freshness-only sweeps must read errors
    from ``data_freshness_index.state_reason`` — NOT
    ``sec_filing_manifest.error`` — because freshness scheduler poll
    failures (rate-limit, 404 on submissions.json, parse) only ever
    land on the freshness row; the manifest table doesn't see them.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT COALESCE(state_reason, 'unknown') AS error_class,
                   COUNT(*)                          AS count,
                   MAX(updated_at)                   AS last_seen_at,
                   MAX(state_reason)                 AS sample_message,
                   MAX(subject_id)                   AS sample_subject
              FROM data_freshness_index
             WHERE source = %(source)s
               AND state  = 'error'
             GROUP BY COALESCE(state_reason, 'unknown')
             ORDER BY COUNT(*) DESC, MAX(updated_at) DESC
             LIMIT %(limit)s
            """,
            {"source": source, "limit": limit},
        )
        rows = cur.fetchall()
    return _coerce_error_rows(rows, default_subject_prefix="subject")


def _manifest_error_summaries(
    conn: psycopg.Connection[Any],
    *,
    source: str,
    limit: int = 10,
) -> tuple[ErrorClassSummary, ...]:
    """Group ``sec_filing_manifest`` failed rows by error text first line.

    Manifest rows carry a freeform ``error`` text; group by the first
    line (cap at 80 chars) so identical parser exceptions cluster
    together. Empty error text falls into ``"unknown"``.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            WITH grouped AS (
                SELECT
                    COALESCE(NULLIF(LEFT(SPLIT_PART(error, E'\n', 1), 80), ''), 'unknown')
                                                AS error_class,
                    accession_number, error, last_attempted_at
                  FROM sec_filing_manifest
                 WHERE source         = %(source)s
                   AND ingest_status  = 'failed'
            )
            SELECT error_class,
                   COUNT(*)                  AS count,
                   MAX(last_attempted_at)    AS last_seen_at,
                   MAX(error)                AS sample_message,
                   MAX(accession_number)     AS sample_subject
              FROM grouped
             GROUP BY error_class
             ORDER BY COUNT(*) DESC, MAX(last_attempted_at) DESC NULLS LAST
             LIMIT %(limit)s
            """,
            {"source": source, "limit": limit},
        )
        rows = cur.fetchall()
    return _coerce_error_rows(rows, default_subject_prefix="accession")


def _log_error_summaries(
    conn: psycopg.Connection[Any],
    *,
    log_table: str,
    limit: int = 10,
) -> tuple[ErrorClassSummary, ...]:
    """Group per-source ingest log failed rows by error first line.

    ``log_table`` is one of {institutional_holdings_ingest_log,
    n_port_ingest_log}. Both share the shape
    ``(accession_number PK, filer_cik, status, error, fetched_at)``
    so the query is uniform. Identifier-injection guard: ``log_table``
    is selected from the static ``_SWEEPS`` registry, not user input.
    """
    if log_table not in {"institutional_holdings_ingest_log", "n_port_ingest_log"}:
        # Defence-in-depth: refuse anything not in the allow-list.
        # Static registry only holds these two; this guard is for the
        # impossible-but-safety case of registry mutation at runtime.
        raise ValueError(f"unsupported log table: {log_table!r}")
    query = psycopg.sql.SQL(
        """
        WITH grouped AS (
            SELECT
                COALESCE(NULLIF(LEFT(SPLIT_PART(error, E'\n', 1), 80), ''), 'unknown')
                                            AS error_class,
                accession_number, filer_cik, error, fetched_at
              FROM {table}
             WHERE status = 'failed'
        )
        SELECT error_class,
               COUNT(*)                  AS count,
               MAX(fetched_at)           AS last_seen_at,
               MAX(error)                AS sample_message,
               MAX(accession_number)     AS sample_subject
          FROM grouped
         GROUP BY error_class
         ORDER BY COUNT(*) DESC, MAX(fetched_at) DESC
         LIMIT %(limit)s
        """
    ).format(table=psycopg.sql.Identifier(log_table))
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(query, {"limit": limit})
        rows = cur.fetchall()
    return _coerce_error_rows(rows, default_subject_prefix="accession")


def _coerce_error_rows(
    rows: list[dict[str, Any]],
    *,
    default_subject_prefix: str,
) -> tuple[ErrorClassSummary, ...]:
    summaries: list[ErrorClassSummary] = []
    for row in rows:
        last_seen_at = row.get("last_seen_at")
        if last_seen_at is None:
            continue
        if isinstance(last_seen_at, datetime) and last_seen_at.tzinfo is None:
            # Coerce naive to UTC; producers store TIMESTAMPTZ but
            # defensive (prevention-log #278).
            last_seen_at = last_seen_at.replace(tzinfo=UTC)
        sample_subject = row.get("sample_subject")
        sample_subject_str = f"{default_subject_prefix} {sample_subject}" if sample_subject is not None else None
        summaries.append(
            ErrorClassSummary(
                error_class=str(row["error_class"]),
                count=int(row["count"]),
                last_seen_at=last_seen_at,
                sample_message=str(row.get("sample_message") or "")[:500],
                sample_subject=sample_subject_str,
            )
        )
    return tuple(summaries)


def _has_freshness_errors(conn: psycopg.Connection[Any], *, source: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM data_freshness_index WHERE source=%s AND state='error' LIMIT 1",
            (source,),
        )
        return cur.fetchone() is not None


def _has_manifest_failures(conn: psycopg.Connection[Any], *, source: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM sec_filing_manifest
             WHERE source=%s AND ingest_status='failed' LIMIT 1
            """,
            (source,),
        )
        return cur.fetchone() is not None


def _last_terminal_log_run(conn: psycopg.Connection[Any], *, log_table: str) -> dict[str, Any] | None:
    if log_table not in {"institutional_holdings_ingest_log", "n_port_ingest_log"}:
        raise ValueError(f"unsupported log table: {log_table!r}")
    query = psycopg.sql.SQL(
        """
        SELECT accession_number, status, fetched_at,
               holdings_inserted, holdings_skipped, error
          FROM {table}
         ORDER BY fetched_at DESC
         LIMIT 1
        """
    ).format(table=psycopg.sql.Identifier(log_table))
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(query)
        return cur.fetchone()


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def _status_for(
    *,
    has_running_underlying: bool,
    has_failures: bool,
    kill_switch_active: bool,
) -> ProcessStatus:
    if kill_switch_active:
        return "disabled"
    if has_running_underlying:
        return "running"
    if has_failures:
        return "failed"
    return "ok"


_LOG_STATUS_TO_RUN: dict[str, RunStatus] = {
    "success": "success",
    "partial": "partial",
    "failed": "failure",
}


def _build_last_run_from_log(row: dict[str, Any]) -> ProcessRunSummary:
    """Construct a ``ProcessRunSummary`` from a per-source ingest log row.

    ``fetched_at`` is the ingester's atomic write timestamp; the log
    rows do not carry a started_at / duration. Set finished_at =
    fetched_at and duration = 0.0 — the operator-visible value is
    "newest accession we saw" not a running stopwatch.
    """
    fetched_at = row["fetched_at"]
    if fetched_at is None:
        fetched_at = datetime.now(UTC)
    elif fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=UTC)
    summary_status: RunStatus = _LOG_STATUS_TO_RUN.get(row.get("status") or "", "success")
    rows_processed = int(row.get("holdings_inserted") or 0) if row.get("holdings_inserted") is not None else None
    skips: dict[str, int] = {}
    skipped_total = row.get("holdings_skipped")
    if skipped_total is not None and int(skipped_total) > 0:
        skips["unknown"] = int(skipped_total)
    return ProcessRunSummary(
        run_id=0,  # ingest log has no integer run_id; surface 0 as sentinel.
        started_at=fetched_at,
        finished_at=fetched_at,
        duration_seconds=0.0,
        rows_processed=rows_processed,
        rows_skipped_by_reason=skips,
        rows_errored=0,
        status=summary_status,
        cancelled_by_operator_id=None,
    )


def _build_row(
    spec: _SweepSpec,
    *,
    conn: psycopg.Connection[Any],
    kill_switch_active: bool,
) -> ProcessRow:
    has_running_underlying = _underlying_job_running(conn, job_name=spec.underlying_job)
    has_pending_underlying = _underlying_job_pending(conn, job_name=spec.underlying_job)
    has_failures = False
    if spec.freshness_source is not None and _has_freshness_errors(conn, source=spec.freshness_source):
        has_failures = True
    if spec.manifest_source is not None and _has_manifest_failures(conn, source=spec.manifest_source):
        has_failures = True

    status = _status_for(
        has_running_underlying=(has_running_underlying or has_pending_underlying),
        has_failures=has_failures,
        kill_switch_active=kill_switch_active,
    )

    # last_n_errors only when status='failed'. Auto-hide-on-retry when
    # status='running' (retry in flight) follows the same shape as the
    # scheduled adapter — empty errors when not actionable.
    last_n_errors: tuple[ErrorClassSummary, ...] = ()
    if status == "failed":
        if spec.log_table is not None:
            # Per-source log is the dominant error surface for 13F /
            # NPORT (parser failures land here). Falls back to manifest
            # if log is empty (e.g. fresh install).
            log_errors = _log_error_summaries(conn, log_table=spec.log_table)
            if log_errors:
                last_n_errors = log_errors
            elif spec.manifest_source is not None:
                last_n_errors = _manifest_error_summaries(conn, source=spec.manifest_source)
        elif spec.manifest_source is not None and spec.freshness_source is not None:
            # Manifest + freshness sweep (e.g. Form 4): manifest is the
            # parser-error surface; freshness is the scheduler-error
            # surface. Manifest dominates because parser failures are
            # what the operator triages most often.
            manifest_errors = _manifest_error_summaries(conn, source=spec.manifest_source)
            if manifest_errors:
                last_n_errors = manifest_errors
            else:
                last_n_errors = _freshness_error_summaries(conn, source=spec.freshness_source)
        elif spec.manifest_source is not None:
            last_n_errors = _manifest_error_summaries(conn, source=spec.manifest_source)
        elif spec.freshness_source is not None:
            last_n_errors = _freshness_error_summaries(conn, source=spec.freshness_source)

    last_run: ProcessRunSummary | None = None
    if spec.log_table is not None:
        log_row = _last_terminal_log_run(conn, log_table=spec.log_table)
        if log_row is not None:
            last_run = _build_last_run_from_log(log_row)

    watermark = resolve_watermark(conn, process_id=spec.process_id, mechanism="ingest_sweep")

    return ProcessRow(
        process_id=spec.process_id,
        display_name=spec.display_name,
        lane=spec.lane,
        mechanism="ingest_sweep",
        status=status,
        last_run=last_run,
        active_run=None,  # in-flight state is on the underlying scheduled_job row
        cadence_human=spec.cadence_human,
        cadence_cron=None,
        next_fire_at=None,  # sweeps don't carry their own cron — see underlying_job
        watermark=watermark,
        # Sweeps are READ-ONLY in v1; operator triggers via the underlying
        # scheduled_job (Codex pre-impl plan review §"Triggering"). PR6
        # spec defers source-level iterate / full-wash to a v2 ticket.
        can_iterate=False,
        can_full_wash=False,
        can_cancel=False,
        last_n_errors=last_n_errors,
    )


# ---------------------------------------------------------------------------
# Public entrypoints
# ---------------------------------------------------------------------------


def list_rows(conn: psycopg.Connection[Any]) -> list[ProcessRow]:
    """Return one ProcessRow per registered sweep.

    Caller MUST be inside a ``snapshot_read(conn)`` block. Capture the
    kill-switch read once at the top so a switch flip mid-loop cannot
    produce a row-by-row inconsistency. Per-sweep exceptions BUBBLE UP
    so ``_gather_snapshot`` flips ``partial=True`` for the whole
    mechanism (spec §Failure-mode invariants).
    """
    kill_switch_active = _kill_switch_active(conn)
    rows: list[ProcessRow] = []
    for spec in _SWEEPS:
        rows.append(_build_row(spec, conn=conn, kill_switch_active=kill_switch_active))
    return rows


def get_row(conn: psycopg.Connection[Any], *, process_id: str) -> ProcessRow | None:
    spec = _SWEEP_BY_ID.get(process_id)
    if spec is None:
        return None
    kill_switch_active = _kill_switch_active(conn)
    return _build_row(spec, conn=conn, kill_switch_active=kill_switch_active)


def list_runs(conn: psycopg.Connection[Any], *, process_id: str, days: int) -> list[ProcessRunSummary]:
    """Return last-N-days terminal runs from the per-source ingest log.

    Sweeps without a log table return [] — the operator history lives
    on the underlying scheduled_job row's History tab.
    """
    if days <= 0:
        raise ValueError("days must be positive")
    spec = _SWEEP_BY_ID.get(process_id)
    if spec is None or spec.log_table is None:
        return []
    if spec.log_table not in {"institutional_holdings_ingest_log", "n_port_ingest_log"}:
        raise ValueError(f"unsupported log table: {spec.log_table!r}")
    query = psycopg.sql.SQL(
        """
        SELECT accession_number, status, fetched_at,
               holdings_inserted, holdings_skipped, error
          FROM {table}
         WHERE fetched_at >= now() - (%(days)s::int * INTERVAL '1 day')
         ORDER BY fetched_at DESC
         LIMIT 50
        """
    ).format(table=psycopg.sql.Identifier(spec.log_table))
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(query, {"days": days})
        rows = cur.fetchall()
    return [_build_last_run_from_log(r) for r in rows]


def list_run_errors(conn: psycopg.Connection[Any], *, process_id: str, run_id: int) -> tuple[ErrorClassSummary, ...]:
    """Per-run error grouping is not meaningful for sweeps (no run_id).

    Sweeps aggregate across ALL accessions for a source; the per-run
    drilldown lives on the underlying scheduled_job row. Return empty.
    """
    return ()


__all__ = [
    "get_row",
    "is_sweep",
    "list_run_errors",
    "list_rows",
    "list_runs",
    "sweep_process_ids",
]
