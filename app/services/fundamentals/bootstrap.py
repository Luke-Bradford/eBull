"""Stream A PR-C2 T1.2 (#1233): bootstrap-only fundamentals derivation entrypoint.

Replaces the steady-state ``fundamentals_sync`` job's HTTP-heavy
Phase 2 (SEC change-driven planner + executor) with a pure-derivation
path that operates exclusively on data already populated by upstream
bootstrap stages:

- ``financial_facts_raw``  — populated by S9 ``sec_companyfacts_ingest``
  (already fan-out per share-class sibling at ingest time —
  ``sec_companyfacts_ingest.py:224`` loops every matched_instrument).
- ``sec_filing_manifest`` + ``filing_events`` — populated by S8 + S14
  + S15 chain.
- ``instrument_sec_profile`` — populated by S8.

The 4-cap gate at ``_STAGE_REQUIRES_CAPS["fundamentals_sync"]``
(strengthened in PR-C1 — ``bulk_archives_ready`` +
``cik_mapping_ready`` + ``submissions_processed`` +
``fundamentals_raw_seeded``) guarantees all upstream data is in
place before this entrypoint fires. Without the gate, the audit
would misclassify mid-bootstrap and re-introduce HTTP backfill
(Codex v3 finding #8).

**Pure-DB by construction (no HTTP guard, no enforcement).** Both
``audit_all_instruments`` and ``normalize_financial_periods`` are
pure-SQL — verified by code reading at PR-C2 review time. There is
no runtime guard that would log ``forbidden_http_in_bootstrap`` or
similar; future refactors that add transitive HTTP calls would NOT
be caught by an active sentinel. The pure-DB invariant is a code-
review contract, not a runtime contract.

**Phase 3 (``review_coverage``) deferred to first post-bootstrap
scheduled cron.** The steady-state ``fundamentals_sync`` job runs
four phases: 0 CIK refresh → 1 XBRL pull + normalize → 2
``audit_all_instruments`` → 3 ``review_coverage`` (Tier 1 cap
enforcement; promotion / demotion writes). PR-C2's bootstrap
entrypoint runs ONLY phases 2 + 1 (audit + normalize) — phase 3
``review_coverage`` fires on the first scheduled ``fundamentals_sync``
window after bootstrap completes (cadence 02:30 UTC). Operator-
visible consequence: admin UI may show stale tier assignments for
up to one cadence window post-bootstrap. Acceptable because tier
review is not load-bearing for the audit + normalize correctness
this entrypoint guarantees.

**Lane disjointness is operationally sufficient but not absolute.**
Steady-state ``fundamentals_sync`` registers on the ``db`` source-
lock; bootstrap ``fundamentals_sync_bootstrap`` registers on the
``db_fundamentals_raw`` source-lock. JobLock is source-keyed, so
the two paths do not block each other at the lock layer. Scheduled
``fundamentals_sync`` fires are also gated by ``_all_of(_bootstrap_complete,
_has_any_coverage)`` (``app/workers/scheduler.py:639``), so under
normal flow they CANNOT fire concurrently. Manual-override
invocations (``mark_request_completed`` bypass on the manual-queue
path) could in theory race; the data-layer writes
(``coverage.filings_status``, ``financial_periods``,
``ownership_treasury_*``) are last-write-wins UPSERTs so a race is
benign for correctness, only telemetry-confusing. Codex 2 pre-push
flagged this overstatement; documenting it here keeps the invariant
honest.

Spec: docs/proposals/etl/stream-a-run-8-fixes.md v2.3 §1 T1.2 + §13.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import psycopg

logger = logging.getLogger(__name__)


@dataclass
class FundamentalsSyncBootstrapResult:
    """Per-invocation telemetry for the bootstrap-only entrypoint."""

    audit_analysable: int = 0
    audit_insufficient: int = 0
    audit_fpi: int = 0
    audit_no_primary_sec_cik: int = 0
    audit_total_updated: int = 0
    # Data-integrity signal from ``coverage.audit_all_instruments``: count
    # of tradable instruments whose ``filings_status`` is NULL after the
    # bulk UPDATE (Chunk B regression detector). Reviewer IMPORTANT —
    # pre-fix this was silently dropped from the result mapping, which
    # would have masked the very anomaly the audit was designed to
    # surface.
    audit_null_anomalies: int = 0
    normalize_instruments_processed: int = 0
    normalize_periods_raw_upserted: int = 0
    normalize_periods_canonical_upserted: int = 0


def fundamentals_sync_bootstrap(conn: psycopg.Connection[Any]) -> FundamentalsSyncBootstrapResult:
    """Derivation-only bootstrap entrypoint for S25 ``fundamentals_sync``.

    Two-phase pure-DB pipeline:

    1. ``audit_all_instruments(conn)`` — re-classifies every tradable
       instrument's ``coverage.filings_status`` from
       ``financial_facts_raw`` aggregates. Wrapped in
       ``conn.transaction()`` internally; a mid-flight failure rolls
       back the whole audit (no partial-state contamination).

    2. ``normalize_financial_periods(conn)`` — full normalization
       pipeline: ``financial_facts_raw`` → ``financial_periods_raw`` →
       canonical ``financial_periods`` + treasury observation write-
       through. Iterates every instrument with rows in
       ``financial_facts_raw`` (per-instrument, share-class siblings
       already fanned out at S9 ingest time —
       ``sec_companyfacts_ingest.py:224``).

    Returns telemetry for ``bootstrap_archive_results.rows_skipped``.
    """
    # Local imports break the module-load cycle (fundamentals.__init__
    # imports a lot at module-load time; deferring keeps the bootstrap
    # entrypoint cheap to import from test fixtures).
    from app.services.coverage import audit_all_instruments
    from app.services.fundamentals import normalize_financial_periods

    logger.info("fundamentals_sync_bootstrap: starting derivation-only run")

    audit_summary = audit_all_instruments(conn)
    logger.info(
        "fundamentals_sync_bootstrap audit: analysable=%d insufficient=%d fpi=%d "
        "no_primary_sec_cik=%d updated=%d null_anomalies=%d",
        audit_summary.analysable,
        audit_summary.insufficient,
        audit_summary.fpi,
        audit_summary.no_primary_sec_cik,
        audit_summary.total_updated,
        audit_summary.null_anomalies,
    )
    if audit_summary.null_anomalies > 0:
        logger.warning(
            "fundamentals_sync_bootstrap: %d null_anomalies — tradable instruments with "
            "filings_status=NULL post-UPDATE. Investigate before next thesis/scoring cycle "
            "(coverage.py:_count_null_anomalies + Chunk B regression check).",
            audit_summary.null_anomalies,
        )

    normalize_summary = normalize_financial_periods(conn)
    logger.info(
        "fundamentals_sync_bootstrap normalize: instruments=%d raw_periods=%d canonical_periods=%d",
        normalize_summary.instruments_processed,
        normalize_summary.periods_raw_upserted,
        normalize_summary.periods_canonical_upserted,
    )

    return FundamentalsSyncBootstrapResult(
        audit_analysable=audit_summary.analysable,
        audit_insufficient=audit_summary.insufficient,
        audit_fpi=audit_summary.fpi,
        audit_no_primary_sec_cik=audit_summary.no_primary_sec_cik,
        audit_total_updated=audit_summary.total_updated,
        audit_null_anomalies=audit_summary.null_anomalies,
        normalize_instruments_processed=normalize_summary.instruments_processed,
        normalize_periods_raw_upserted=normalize_summary.periods_raw_upserted,
        normalize_periods_canonical_upserted=normalize_summary.periods_canonical_upserted,
    )


JOB_FUNDAMENTALS_SYNC_BOOTSTRAP = "fundamentals_sync_bootstrap"
"""Job name registered in ``_INVOKERS`` (``app/jobs/runtime.py``) +
``_BOOTSTRAP_STAGE_SPECS`` (``app/services/bootstrap_orchestrator.py``).
Distinct from the steady-state ``fundamentals_sync`` so the two paths
register on disjoint lanes (steady-state ``db`` vs bootstrap
``db_fundamentals_raw``) and the lane-source registry cross-check at
``app/jobs/sources.py:_build_job_name_to_source`` is satisfied.
"""


def fundamentals_sync_bootstrap_invoker(_params: Mapping[str, Any] | None = None) -> None:
    """Zero-arg job invoker for ``_INVOKERS`` registration.

    Opens its own connection from ``settings.database_url`` (mirroring
    sibling bootstrap-stage invokers in ``app/services/sec_bulk_orchestrator_jobs.py``)
    so the orchestrator's executor can dispatch without threading a
    per-stage connection through.
    """
    from app.config import settings

    with psycopg.connect(settings.database_url) as conn:
        result = fundamentals_sync_bootstrap(conn)
        conn.commit()

    # Record per-run telemetry on the bootstrap_runs context if one
    # exists. The audit-record path is best-effort — failure here MUST
    # NOT propagate to the orchestrator (would mark the stage 'error'
    # and a retry would re-run the whole derivation pipeline, doubling
    # the cost on every transient hiccup). Pattern parallels
    # sec_submissions_files_walk_job's __job__ audit row (#1038).
    _record_bootstrap_audit_row(result)


def _record_bootstrap_audit_row(result: FundamentalsSyncBootstrapResult) -> None:
    """Best-effort write of the per-run audit row.

    Architect WARNING + DE WARNING + Codex 2 fold (PR-C2 pre-push):
    the ENTIRE function body is wrapped in a single try/except so
    transient PG errors on EITHER (a) the second connection open OR
    (b) the SELECT-for-run-id OR (c) the audit-row write itself
    surface as a WARNING log instead of propagating as a stage error.
    The derivation has already committed by the time we get here;
    failure of this audit-row write MUST NOT mark the stage 'error'
    (would re-dispatch the whole derivation on retry, doubling the
    cost for a telemetry hiccup).

    Reviewer IMPORTANT fold: `run_id is None` no longer silently
    skips — emits an info log so the absence is visible.
    """
    from app.config import settings
    from app.services.bootstrap_preconditions import record_archive_result

    try:
        with psycopg.connect(settings.database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM bootstrap_runs WHERE status='running' ORDER BY id DESC LIMIT 1",
                )
                row = cur.fetchone()
                run_id = int(row[0]) if row else None
            if run_id is None:
                logger.info("fundamentals_sync_bootstrap: no running bootstrap_runs row; audit-row write skipped")
                return
            record_archive_result(
                conn,
                bootstrap_run_id=run_id,
                stage_key="fundamentals_sync",
                archive_name="__job__",
                rows_written=result.normalize_periods_canonical_upserted,
                rows_skipped={
                    "audit_analysable": result.audit_analysable,
                    "audit_insufficient": result.audit_insufficient,
                    "audit_fpi": result.audit_fpi,
                    "audit_no_primary_sec_cik": result.audit_no_primary_sec_cik,
                    "audit_null_anomalies": result.audit_null_anomalies,
                    "normalize_instruments_processed": result.normalize_instruments_processed,
                    "normalize_periods_raw_upserted": result.normalize_periods_raw_upserted,
                },
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not block stage
        logger.warning(
            "fundamentals_sync_bootstrap: failed to record __job__ audit row (derivation already committed): %s",
            exc,
        )
