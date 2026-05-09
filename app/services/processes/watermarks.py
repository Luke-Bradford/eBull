"""Per-process watermark resolver for the admin control hub.

Issue #1073 (umbrella #1064).
Spec: ``docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md``
      §"Watermark + resume contract" / §PR4.

PR3 left ``ProcessRow.watermark = None`` on every row. PR4 adds a
per-mechanism resolver that reads the relevant existing source table
(``data_freshness_index``, ``sec_filing_manifest``,
``external_data_watermarks``, ``bootstrap_stages``,
``n_port_ingest_log``, ``price_daily``, ``pending_job_requests``) and
produces a ``ProcessWatermark`` for the FE tooltip + the trigger
handler's full-wash reset.

The resolver is purely read-only: it does not mutate state. The
full-wash reset SQL lives in ``app/api/processes.py::_apply_full_wash_reset``
(same module that already holds the per-mechanism precondition checks)
so the reset and the durable fence INSERT happen inside the same
advisory-lock-held transaction. This module exposes the per-job source
identifiers (``freshness_source_for``, ``manifest_source_for``,
``atom_etag_target_for``) so the trigger handler + the auto-hide
covered check both read from a single source of truth.

Spec gaps that PR4 explicitly leaves at ``watermark=None`` (with
follow-up tickets in #1064's umbrella tracking):

- ``sync_runs.layer_state_at_finish`` does NOT exist as a column
  (sql/033 + sql/041 + sql/086 + sql/139 each only ALTER scope/status/
  cancel columns). ``orchestrator_full_sync`` therefore returns None
  until that column lands.
- ``instrument_market_data_refresh`` does NOT exist as a table. The
  candle refresh resolver substitutes a global ``MAX(price_date) FROM
  price_daily`` summary so the operator still sees a meaningful
  cursor; per-instrument fan-out is deferred.
- Some scheduled jobs have no watermark source (``heartbeat``,
  ``monitor_positions``, ``weekly_report``, …). The resolver returns
  None for those — full-wash on a no-watermark job is just "rerun"
  with no reset step.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import psycopg
import psycopg.rows

from app.services.processes import ProcessMechanism, ProcessWatermark
from app.services.watermarks import get_watermark

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Per-job source registry
# ---------------------------------------------------------------------------
#
# A single source-of-truth mapping that drives:
#   * the watermark resolver (FE tooltip + per-row ``ProcessRow.watermark``)
#   * the full-wash reset SQL (trigger handler)
#   * the auto-hide covered check (scheduled adapter status)
#
# The registry is keyed by ``process_id`` (= job_name for scheduled
# jobs, "bootstrap" for the bootstrap orchestrator). Each entry pins
# one of:
#   * ``freshness_source`` — ``data_freshness_index.source`` value
#                            (filed_at cursor; full-wash resets to NULL)
#   * ``manifest_source`` — ``sec_filing_manifest.source`` value
#                           (accession cursor; full-wash resets to pending)
#   * ``atom_etag``       — (source, key, display) for ``external_data_watermarks``
#                           (full-wash deletes the ETag row)
#   * ``custom``          — special-cased mechanisms (bootstrap stages,
#                           candle MAX, NPORT log, universe epoch).
#
# Jobs absent from this map have no watermark surface; full-wash on
# them is a no-reset rerun.


@dataclass(frozen=True, slots=True)
class _AtomEtagTarget:
    source: str
    key: str
    display: str


@dataclass(frozen=True, slots=True)
class _JobSpec:
    """One entry in the per-job source registry.

    Exactly one of ``freshness_source`` / ``manifest_source`` /
    ``atom_etag`` / ``custom`` is non-None. ``display`` is the
    operator-facing label that appears in the watermark tooltip
    ("Form 4", "DEF 14A", "company_tickers.json"); the resolver
    falls back to ``process_id`` if absent.
    """

    display: str
    freshness_source: str | None = None
    manifest_source: str | None = None
    atom_etag: _AtomEtagTarget | None = None
    custom: str | None = None


_JOB_REGISTRY: dict[str, _JobSpec] = {
    # Universe (epoch cursor — custom resolver)
    "nightly_universe_sync": _JobSpec(display="universe sweep", custom="universe_epoch"),
    # Candles (instrument_offset cursor — custom resolver against price_daily)
    "daily_candle_refresh": _JobSpec(display="daily candles", custom="candle_offset"),
    # SEC submissions ingest (filed_at via data_freshness_index)
    "sec_form3_ingest": _JobSpec(display="Form 3", freshness_source="sec_form3"),
    "sec_insider_transactions_ingest": _JobSpec(display="Form 4", freshness_source="sec_form4"),
    "sec_def14a_ingest": _JobSpec(display="DEF 14A", freshness_source="sec_def14a"),
    "sec_8k_events_ingest": _JobSpec(display="8-K", freshness_source="sec_8k"),
    "daily_financial_facts": _JobSpec(display="XBRL facts", freshness_source="sec_xbrl_facts"),
    "fundamentals_sync": _JobSpec(display="XBRL facts", freshness_source="sec_xbrl_facts"),
    "sec_business_summary_ingest": _JobSpec(display="XBRL facts", freshness_source="sec_xbrl_facts"),
    # NPORT (accession cursor via n_port_ingest_log; full-wash uses
    # freshness_source='sec_n_port' so the freshness scheduler reseeds
    # NPORT subjects from scratch).
    "sec_n_port_ingest": _JobSpec(
        display="N-PORT",
        custom="n_port_accession",
        freshness_source="sec_n_port",
    ),
    # Manifest-driven workers (accession cursor via sec_filing_manifest).
    # Full-wash also resets the freshness scheduler so the post-reset
    # poller can rediscover missing accessions, not just re-parse known
    # ones (Codex pre-push WARNING).
    "sec_filing_documents_ingest": _JobSpec(
        display="Form 4 manifest",
        manifest_source="sec_form4",
        freshness_source="sec_form4",
    ),
    # Atom ETag (external_data_watermarks)
    "daily_cik_refresh": _JobSpec(
        display="company_tickers.json",
        atom_etag=_AtomEtagTarget(source="sec.tickers", key="global", display="company_tickers.json"),
    ),
    # NOTE: orchestrator_full_sync intentionally absent. Spec §"Adapter
    # map" predicates its watermark on ``sync_runs.layer_state_at_finish``
    # which does not exist as a column in this repo. Leaving unresolved
    # per spec guidance; follow-up tracked under #1064.
}


# ---------------------------------------------------------------------------
# Bootstrap — stage_index cursor
# ---------------------------------------------------------------------------


def _resolve_bootstrap_stage_index(
    conn: psycopg.Connection[Any],
) -> ProcessWatermark | None:
    """Bootstrap watermark: max(stage_order) of last ``success`` per lane.

    Spec §"Adapter map" — Bootstrap stages → ``stage_index``.

    The cursor_value is a stable lane→stage_order rendering for the
    operator: ``"etoro:2,sec:14"``. ``human`` is the rendered tooltip
    text. ``last_advanced_at`` is the most recent completed_at across
    the success rows so a stalled run still shows the right "last
    advanced" timestamp on the tooltip.

    Returns None when there is no bootstrap_runs row yet (fresh install
    pre-first-trigger) — surfacing an empty cursor would mislead the
    operator into thinking the next iterate has work to resume from.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("SELECT MAX(id) AS run_id FROM bootstrap_runs")
        run = cur.fetchone()
    if run is None or run["run_id"] is None:
        return None
    run_id = int(run["run_id"])

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT lane,
                   MAX(stage_order)  AS max_order,
                   MAX(completed_at) AS last_completed_at
              FROM bootstrap_stages
             WHERE bootstrap_run_id = %(run_id)s
               AND status           = 'success'
             GROUP BY lane
             ORDER BY lane
            """,
            {"run_id": run_id},
        )
        rows = cur.fetchall()
    if not rows:
        # Run exists but no stage has reached 'success' yet — nothing
        # to resume from. Iterate would re-pick at stage_order=1 which
        # is exactly what an empty watermark conveys; surface None so
        # the FE tooltip says "no progress yet" rather than "0 of N".
        return None

    cursor_parts: list[str] = []
    last_advanced_at: datetime | None = None
    for row in rows:
        lane = str(row["lane"])
        max_order = int(row["max_order"])
        cursor_parts.append(f"{lane}:{max_order}")
        completed_at = row["last_completed_at"]
        if completed_at is not None and (last_advanced_at is None or completed_at > last_advanced_at):
            last_advanced_at = completed_at
    cursor_value = ",".join(cursor_parts)
    if last_advanced_at is None:
        # Defence-in-depth: a 'success' row without completed_at would
        # be a producer bug (mark_stage_success sets completed_at).
        # Falling back to "now" would lie about the cursor age, so omit
        # the watermark entirely.
        return None
    human = f"Resume after stages [{cursor_value}] (last advanced {last_advanced_at.isoformat()})"
    return ProcessWatermark(
        cursor_kind="stage_index",
        cursor_value=cursor_value,
        human=human,
        last_advanced_at=last_advanced_at,
    )


# ---------------------------------------------------------------------------
# Per-cursor-kind resolvers
# ---------------------------------------------------------------------------


def _resolve_universe_sync_epoch(
    conn: psycopg.Connection[Any],
) -> ProcessWatermark | None:
    """Universe sync watermark: latest completed manual/scheduled run id.

    Spec §"Adapter map" — Universe sync uses ``pending_job_requests``
    last successful run row id as a coarse-grained ``epoch`` cursor.
    The universe sync is fully idempotent at the row level (UPSERT on
    ``instruments``); the cursor is informational only — operator sees
    "latest universe pull was epoch=12345 at 2026-05-08T03:00Z" rather
    than a fine-grained subject cursor.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT request_id, requested_at
              FROM pending_job_requests
             WHERE job_name = 'nightly_universe_sync'
               AND status   = 'completed'
             ORDER BY request_id DESC
             LIMIT 1
            """
        )
        row = cur.fetchone()
    if row is None:
        return None
    request_id = int(row["request_id"])
    requested_at = row["requested_at"]
    if requested_at is None:
        return None
    cursor_value = str(request_id)
    human = f"Resume after universe epoch #{request_id} (last advanced {requested_at.isoformat()})"
    return ProcessWatermark(
        cursor_kind="epoch",
        cursor_value=cursor_value,
        human=human,
        last_advanced_at=requested_at,
    )


def _resolve_candle_offset(
    conn: psycopg.Connection[Any],
) -> ProcessWatermark | None:
    """Candle refresh watermark: global MAX(price_date) FROM price_daily.

    Spec lists ``instrument_market_data_refresh.last_synced_at`` as the
    source, but that table does not exist in this repo — the daily
    candle refresh writes directly to ``price_daily``. The operator
    surface still wants a single "resume from" cursor; use the global
    MAX so a stale sweep is visible at a glance. Per-instrument cursor
    fan-out (which would let the resolver report "12 of 1547
    instruments awaiting next poll") is deferred to a follow-up.

    Cursor is the ISO date string. ``last_advanced_at`` is the
    ``price_date`` coerced to a UTC midnight so the ProcessWatermark
    contract (TIMESTAMPTZ field) holds — ``price_daily`` does not
    carry a writer-side updated_at.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("SELECT MAX(price_date) AS max_date FROM price_daily")
        row = cur.fetchone()
    if row is None or row["max_date"] is None:
        return None
    max_date = row["max_date"]
    cursor_value = max_date.isoformat()
    last_advanced_at = datetime(max_date.year, max_date.month, max_date.day, tzinfo=UTC)
    human = f"Resume from candles after {cursor_value}"
    return ProcessWatermark(
        cursor_kind="instrument_offset",
        cursor_value=cursor_value,
        human=human,
        last_advanced_at=last_advanced_at,
    )


def _resolve_n_port_accession(
    conn: psycopg.Connection[Any],
) -> ProcessWatermark | None:
    """NPORT watermark: max accession + max fetched_at across SUCCESS rows.

    Spec §"Adapter map" — N-PORT uses ``n_port_ingest_log`` last
    processed accession as ``accession`` cursor.

    Accession numbers do not sort chronologically across CIKs (the
    middle YY field bites you across year boundaries; the last sequence
    is filer-local). Codex pre-push WARNING — order by ``fetched_at``
    DESC and return the matching accession instead of MAX().
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT accession_number, fetched_at
              FROM n_port_ingest_log
             WHERE status = 'success'
             ORDER BY fetched_at DESC, accession_number DESC
             LIMIT 1
            """
        )
        row = cur.fetchone()
    if row is None or row["accession_number"] is None:
        return None
    cursor_value = str(row["accession_number"])
    last_advanced_at = row["fetched_at"]
    if last_advanced_at is None:
        return None
    human = f"Resume after accession {cursor_value} (last advanced {last_advanced_at.isoformat()})"
    return ProcessWatermark(
        cursor_kind="accession",
        cursor_value=cursor_value,
        human=human,
        last_advanced_at=last_advanced_at,
    )


def _resolve_filed_at(
    conn: psycopg.Connection[Any],
    *,
    source: str,
    display: str,
) -> ProcessWatermark | None:
    """Resolve ``filed_at`` cursor via ``data_freshness_index``.

    ``last_known_filed_at`` is the per-subject pointer to "newest
    accession seen for this (subject, source) in steady-state". The
    operator-facing watermark is the global MAX across all subjects
    of that source.

    ``human`` includes the count of subjects whose
    ``state IN ('expected_filing_overdue', 'unknown')`` — the
    "awaiting next poll" cohort the operator sees in the freshness
    sweeper. Mirrors spec §"Watermark sources" tooltip example.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT MAX(last_known_filed_at) AS max_filed_at,
                   COUNT(*)                  AS subjects_total,
                   COUNT(*) FILTER (
                     WHERE state IN ('expected_filing_overdue', 'unknown')
                   )                         AS subjects_awaiting
              FROM data_freshness_index
             WHERE source = %(source)s
            """,
            {"source": source},
        )
        row = cur.fetchone()
    if row is None or row["max_filed_at"] is None:
        return None
    max_filed_at = row["max_filed_at"]
    subjects_total = int(row["subjects_total"])
    subjects_awaiting = int(row["subjects_awaiting"])
    cursor_value = max_filed_at.isoformat()
    human = (
        f"Resume from {display} filings filed after {cursor_value}"
        f" ({subjects_awaiting} of {subjects_total} subjects awaiting next poll)"
    )
    return ProcessWatermark(
        cursor_kind="filed_at",
        cursor_value=cursor_value,
        human=human,
        last_advanced_at=max_filed_at,
    )


def _resolve_manifest_accession(
    conn: psycopg.Connection[Any],
    *,
    source: str,
    display: str,
) -> ProcessWatermark | None:
    """Manifest worker watermark: max accession + retry telemetry.

    Spec §"Adapter map" — SEC manifest worker uses
    ``sec_filing_manifest.next_retry_at`` + ``last_attempted_at`` as
    ``accession`` cursor. The operator-visible value is "newest
    accession we have on file for this source"; the suffix surfaces
    the count of pending+failed accessions awaiting drain.

    Codex pre-push WARNING: accession lexicographic order is not
    global filing order across CIKs. Order by ``filed_at`` DESC and
    return the matching accession. The pending-count is computed in
    a separate aggregate query so the LIMIT 1 ordering doesn't
    interfere with the COUNT.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT accession_number, filed_at
              FROM sec_filing_manifest
             WHERE source = %(source)s
             ORDER BY filed_at DESC, accession_number DESC
             LIMIT 1
            """,
            {"source": source},
        )
        row = cur.fetchone()
    if row is None or row["accession_number"] is None:
        return None
    max_acc = str(row["accession_number"])
    max_filed_at = row["filed_at"]
    if max_filed_at is None:
        return None
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT COUNT(*) FILTER (WHERE ingest_status IN ('pending', 'failed')) AS pending_count
              FROM sec_filing_manifest
             WHERE source = %(source)s
            """,
            {"source": source},
        )
        agg = cur.fetchone()
    pending = 0 if agg is None else int(agg["pending_count"])
    cursor_value = max_acc
    suffix = "" if pending == 0 else f" ({pending} accessions awaiting drain)"
    human = f"Resume after {display} accession {max_acc} (filed {max_filed_at.isoformat()}){suffix}"
    return ProcessWatermark(
        cursor_kind="accession",
        cursor_value=cursor_value,
        human=human,
        last_advanced_at=max_filed_at,
    )


def _resolve_atom_etag(
    conn: psycopg.Connection[Any],
    *,
    target: _AtomEtagTarget,
) -> ProcessWatermark | None:
    """Atom ETag watermark from ``external_data_watermarks``.

    The provider-native ETag string is opaque (``Last-Modified``
    header value, response_hash, accession). Surfacing it verbatim is
    fine — operator only needs to know "we're at the same provider
    state we saw at <fetched_at>".
    """
    wm = get_watermark(conn, target.source, target.key)
    if wm is None:
        return None
    last_advanced_at = wm.watermark_at or wm.fetched_at
    cursor_value = wm.watermark
    human = f"Provider {target.display} unchanged since {last_advanced_at.isoformat()}"
    return ProcessWatermark(
        cursor_kind="atom_etag",
        cursor_value=cursor_value,
        human=human,
        last_advanced_at=last_advanced_at,
    )


_CUSTOM_RESOLVERS: dict[str, Callable[[psycopg.Connection[Any]], ProcessWatermark | None]] = {
    "universe_epoch": _resolve_universe_sync_epoch,
    "candle_offset": _resolve_candle_offset,
    "n_port_accession": _resolve_n_port_accession,
}


# ---------------------------------------------------------------------------
# Public API — watermark
# ---------------------------------------------------------------------------


def resolve_watermark(
    conn: psycopg.Connection[Any],
    *,
    process_id: str,
    mechanism: ProcessMechanism,
) -> ProcessWatermark | None:
    """Return the per-process watermark, or None if no source applies.

    Caller is the adapter (bootstrap/scheduled/ingest_sweep) inside
    ``snapshot_read(conn)`` so the watermark read sees the same
    REPEATABLE READ snapshot as the rest of the row's data. Callers
    upstream of the adapter (e.g. the trigger handler) MUST NOT rely
    on this for atomicity — the trigger handler holds the per-process
    advisory lock and reads/resets watermark state inside its own
    transaction.

    A None return is honest: it means "no watermark applies" (e.g.
    heartbeat job) OR "the source has no rows yet" (e.g. fresh install
    pre-first-poll). The adapter renders None as "no resume cursor"
    in the FE tooltip; the trigger handler treats None as "full-wash
    has nothing to reset" and goes straight to the queue INSERT.
    """
    if mechanism == "bootstrap":
        return _safely(_resolve_bootstrap_stage_index, conn, process_id)
    if mechanism == "ingest_sweep":
        # PR6 wires per-source ingest sweep rows; PR4 has nothing to
        # surface here.
        return None
    spec = _JOB_REGISTRY.get(process_id)
    if spec is None:
        return None
    if spec.custom is not None:
        custom = _CUSTOM_RESOLVERS.get(spec.custom)
        if custom is None:
            return None
        return _safely(custom, conn, process_id)
    # Priority: manifest > freshness > atom_etag. A job may carry both
    # ``manifest_source`` AND ``freshness_source`` (the manifest worker
    # for SEC Form 4 also resets the freshness scheduler on full-wash);
    # the operator-facing cursor for that job is the manifest accession,
    # not the freshness max(filed_at). Freshness-only jobs (Form 3,
    # DEF 14A, …) are unaffected because they leave ``manifest_source``
    # unset.
    if spec.manifest_source is not None:
        m_source = spec.manifest_source
        m_display = spec.display
        return _safely(
            lambda c: _resolve_manifest_accession(c, source=m_source, display=m_display),
            conn,
            process_id,
        )
    if spec.freshness_source is not None:
        source = spec.freshness_source
        display = spec.display
        return _safely(lambda c: _resolve_filed_at(c, source=source, display=display), conn, process_id)
    if spec.atom_etag is not None:
        target = spec.atom_etag
        return _safely(lambda c: _resolve_atom_etag(c, target=target), conn, process_id)
    return None


def _safely(
    fn: Callable[[psycopg.Connection[Any]], ProcessWatermark | None],
    conn: psycopg.Connection[Any],
    process_id: str,
) -> ProcessWatermark | None:
    """Invoke a resolver and swallow any exception as ``watermark=None``.

    Per spec §"Failure-mode invariants": a per-row resolver failure
    must not 500 the snapshot. The adapter's outer try/except in
    ``app/api/processes.py::_gather_snapshot`` is the cross-row safety
    net; this is the per-row guard so one resolver bug doesn't blank
    out an entire mechanism.
    """
    try:
        return fn(conn)
    except Exception:
        logger.exception(
            "watermark resolver raised for process_id=%r; surfacing watermark=None",
            process_id,
        )
        return None


# ---------------------------------------------------------------------------
# Public API — sources for full-wash + covered-check
# ---------------------------------------------------------------------------


def freshness_source_for(process_id: str) -> str | None:
    """Return the ``data_freshness_index.source`` value for a job, or None.

    Used by:
      * ``app/api/processes.py::_apply_full_wash_reset`` to issue the
        ``UPDATE data_freshness_index SET last_known_filed_at = NULL,
        state = 'unknown' WHERE source = ?`` reset.
      * ``scheduled_adapter._is_failed_scope_covered`` to query whether
        a freshness recheck is scheduled within the next-fire window.
    """
    spec = _JOB_REGISTRY.get(process_id)
    return None if spec is None else spec.freshness_source


def manifest_source_for(process_id: str) -> str | None:
    """Return the ``sec_filing_manifest.source`` value for a job, or None.

    Used by:
      * ``_apply_full_wash_reset`` to issue the ``UPDATE
        sec_filing_manifest SET ingest_status='pending',
        last_attempted_at=NULL, next_retry_at=NULL WHERE source = ?``
        reset.
      * ``_is_failed_scope_covered`` to query whether failed manifest
        rows have a ``next_retry_at`` within the next-fire window.
    """
    spec = _JOB_REGISTRY.get(process_id)
    return None if spec is None else spec.manifest_source


def atom_etag_target_for(process_id: str) -> tuple[str, str] | None:
    """Return ``(source, key)`` for ``external_data_watermarks``, or None.

    Used by ``_apply_full_wash_reset`` to issue the ``DELETE FROM
    external_data_watermarks WHERE source = ? AND key = ?`` reset.
    """
    spec = _JOB_REGISTRY.get(process_id)
    if spec is None or spec.atom_etag is None:
        return None
    return (spec.atom_etag.source, spec.atom_etag.key)


__all__ = [
    "atom_etag_target_for",
    "freshness_source_for",
    "manifest_source_for",
    "resolve_watermark",
]
