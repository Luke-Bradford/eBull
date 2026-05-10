"""Coverage admin endpoints (#268 Chunk H).

Read-only surface for operator visibility into the filings coverage
pipeline. Two endpoints:

- ``GET /coverage/summary`` — counts by ``filings_status`` across all
  tradable instruments + audit freshness metadata. Powers the
  ``AdminPage`` "Filings coverage" card.
- ``GET /coverage/insufficient`` — drill-down list of instruments
  whose ``filings_status`` is currently ``insufficient`` or
  ``structurally_young``. Each row carries symbol, primary SEC CIK,
  backfill attempt count, last reason, and earliest SEC filing date.
  Powers the ``/admin/coverage/insufficient`` route.

Auth: both endpoints require operator auth via
``require_session_or_service_token``, mounted on the router so
individual handlers cannot accidentally be exposed without it. Status
counts + drill-down rows reveal data-pipeline gaps, so they must
not be public.

No write actions — Chunk H is intentionally read-only. Operator-
driven fixes (manual re-enqueue, status override) are deferred.
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.api.auth import require_session_or_service_token
from app.db import get_conn

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/coverage",
    tags=["coverage"],
    dependencies=[Depends(require_session_or_service_token)],
)


# ---------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------


class CoverageSummaryResponse(BaseModel):
    """Per-status counts across all tradable instruments."""

    checked_at: datetime
    analysable: int
    insufficient: int
    fpi: int
    no_primary_sec_cik: int
    structurally_young: int
    unknown: int
    # Null rows exist only pre-first-audit. Surfaced so ops can spot
    # a stalled audit job without drilling into the table.
    null_rows: int
    total_tradable: int


class InsufficientRow(BaseModel):
    """One drill-down row for the /coverage/insufficient listing."""

    instrument_id: int
    symbol: str
    company_name: str | None
    cik: str | None
    filings_status: str
    filings_backfill_attempts: int
    filings_backfill_last_at: datetime | None
    filings_backfill_reason: str | None
    earliest_sec_filing_date: date | None


class InsufficientListResponse(BaseModel):
    checked_at: datetime
    rows: list[InsufficientRow]


# #1067 — CIK coverage audit.


class CikGapRowResponse(BaseModel):
    """One unmapped instrument in the CIK gap detail."""

    instrument_id: int
    symbol: str
    company_name: str | None
    category: str  # "suffix_variant" | "other"


class CikCoverageGapResponse(BaseModel):
    """Aggregate counters + capped sample for the CIK gap report.

    Cohort is the us_equity tradable producer cohort (matches
    ``daily_cik_refresh``'s scope). ``unmapped_suffix_variants``
    rows are operational-duplicate variants (``.RTH``, ``.US`` etc)
    that legitimately lack their own CIK row; ``unmapped_other`` is
    the real gap signal — typically ETFs, funds, merger CVRs, or
    a genuine missing mapping.
    """

    checked_at: datetime
    cohort_total: int
    mapped: int
    unmapped: int
    unmapped_suffix_variants: int
    unmapped_other: int
    sample: list[CikGapRowResponse]


# ---------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------


@router.get("/summary", response_model=CoverageSummaryResponse)
def get_coverage_summary(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> CoverageSummaryResponse:
    """Counts by ``filings_status`` across all tradable instruments.

    Join via ``is_tradable = TRUE`` so non-tradable rows (delisted,
    disabled) don't inflate the null-row count. ``null_rows``
    captures tradable instruments whose coverage row exists but
    whose ``filings_status`` is NULL — a pre-audit placeholder that
    should only persist until the first ``fundamentals_sync``
    run. Any non-zero ``null_rows`` in steady state is an ops
    signal that the audit job is wedged.
    """
    with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE c.filings_status = 'analysable'),
                COUNT(*) FILTER (WHERE c.filings_status = 'insufficient'),
                COUNT(*) FILTER (WHERE c.filings_status = 'fpi'),
                COUNT(*) FILTER (WHERE c.filings_status = 'no_primary_sec_cik'),
                COUNT(*) FILTER (WHERE c.filings_status = 'structurally_young'),
                COUNT(*) FILTER (WHERE c.filings_status = 'unknown'),
                COUNT(*) FILTER (WHERE c.filings_status IS NULL),
                COUNT(*)
            FROM instruments i
            LEFT JOIN coverage c ON c.instrument_id = i.instrument_id
            WHERE i.is_tradable = TRUE
            """
        )
        row = cur.fetchone()
    # LEFT JOIN on a populated table with a populated coverage table
    # always returns one aggregate row, but guard against the degenerate
    # empty-instruments case (fresh deploy) to avoid a type error.
    if row is None:
        return CoverageSummaryResponse(
            checked_at=datetime.now(tz=UTC),
            analysable=0,
            insufficient=0,
            fpi=0,
            no_primary_sec_cik=0,
            structurally_young=0,
            unknown=0,
            null_rows=0,
            total_tradable=0,
        )

    return CoverageSummaryResponse(
        checked_at=datetime.now(tz=UTC),
        analysable=int(row[0]),
        insufficient=int(row[1]),
        fpi=int(row[2]),
        no_primary_sec_cik=int(row[3]),
        structurally_young=int(row[4]),
        unknown=int(row[5]),
        null_rows=int(row[6]),
        total_tradable=int(row[7]),
    )


@router.get("/insufficient", response_model=InsufficientListResponse)
def get_coverage_insufficient(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> InsufficientListResponse:
    """Drill-down list for non-terminal coverage states.

    Includes ``insufficient`` AND ``structurally_young`` — both are
    actionable from an ops perspective (insufficient may indicate a
    failed backfill; structurally_young flips to analysable once the
    issuer ages past 18 months). Excludes ``analysable`` / ``fpi`` /
    ``no_primary_sec_cik`` (terminal non-actionable) and NULL
    (surfaced via ``summary.null_rows`` instead).

    Ordering: highest-attempts first, then earliest ``last_at`` so
    the operator sees the most-painful stuck rows first. Ties broken
    by symbol.

    Per-row ``earliest_sec_filing_date`` is the MIN ``filing_date``
    in ``filing_events`` for the instrument (provider='sec'). A
    NULL result indicates zero SEC filings for the instrument
    — typically a CIK-mapping issue or a very-young issuer.
    """
    with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
        cur.execute(
            """
            SELECT
                i.instrument_id,
                i.symbol,
                i.company_name,
                ei.identifier_value AS cik,
                c.filings_status,
                c.filings_backfill_attempts,
                c.filings_backfill_last_at,
                c.filings_backfill_reason,
                (
                    SELECT MIN(fe.filing_date)
                    FROM filing_events fe
                    WHERE fe.instrument_id = i.instrument_id
                      AND fe.provider = 'sec'
                ) AS earliest_sec_filing_date
            FROM instruments i
            JOIN coverage c ON c.instrument_id = i.instrument_id
            LEFT JOIN external_identifiers ei
                ON ei.instrument_id = i.instrument_id
               AND ei.provider = 'sec'
               AND ei.identifier_type = 'cik'
               AND ei.is_primary = TRUE
            WHERE i.is_tradable = TRUE
              AND c.filings_status IN ('insufficient', 'structurally_young')
            ORDER BY
                c.filings_backfill_attempts DESC,
                c.filings_backfill_last_at ASC NULLS LAST,
                i.symbol
            """
        )
        rows = cur.fetchall()

    out: list[InsufficientRow] = []
    for r in rows:
        # filing_date is SQL DATE → Python date. Preserved as date
        # in the response so frontend formats it as a calendar date
        # (no timezone coercion, no midnight-UTC drift).
        # Explicit ``type(...) is date`` — ``isinstance(x, date)``
        # also matches ``datetime`` instances since datetime subclasses
        # date. psycopg3 returns a plain date for SQL DATE columns, so
        # this check is belt-and-braces + clearer intent.
        raw_earliest = r[8]
        earliest: date | None = raw_earliest if type(raw_earliest) is date else None
        out.append(
            InsufficientRow(
                instrument_id=int(r[0]),
                symbol=str(r[1]),
                company_name=str(r[2]) if r[2] is not None else None,
                cik=str(r[3]) if r[3] is not None else None,
                filings_status=str(r[4]),
                filings_backfill_attempts=int(r[5]) if r[5] is not None else 0,
                filings_backfill_last_at=r[6],
                filings_backfill_reason=str(r[7]) if r[7] is not None else None,
                earliest_sec_filing_date=earliest,
            )
        )

    return InsufficientListResponse(checked_at=datetime.now(tz=UTC), rows=out)


@router.get("/cik-gap", response_model=CikCoverageGapResponse)
def get_cik_gap(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> CikCoverageGapResponse:
    """#1067 — CIK coverage audit for the us_equity tradable cohort.

    Operator-visible gap report. Cohort matches ``daily_cik_refresh``
    so the ``mapped`` + ``unmapped`` split correlates with the
    bridge's hit / miss rate. Unmapped rows are bucketed:

    * ``suffix_variants`` — symbol contains ``.`` (operational
      duplicates like ``AAPL.RTH``). Pre-#819 these would render
      empty pages; post-#819 the canonical-redirect mechanism
      ensures they don't need their own CIK.
    * ``other`` — ETFs, funds, merger CVRs, and any genuine gap
      worth operator triage.

    See ``docs/wiki/runbooks/runbook-diagnosing-missing-cik.md`` for
    the runbook that interprets this report.
    """
    from app.services.cik_coverage_audit import compute_cik_gap_report

    report = compute_cik_gap_report(conn)  # type: ignore[arg-type]
    return CikCoverageGapResponse(
        checked_at=datetime.now(tz=UTC),
        cohort_total=report.cohort_total,
        mapped=report.mapped,
        unmapped=report.unmapped,
        unmapped_suffix_variants=report.unmapped_suffix_variants,
        unmapped_other=report.unmapped_other,
        sample=[
            CikGapRowResponse(
                instrument_id=r.instrument_id,
                symbol=r.symbol,
                company_name=r.company_name,
                category=r.category,
            )
            for r in report.sample
        ],
    )


# #935 §5 — manifest-parser audit.


class ManifestParserSourceRowResponse(BaseModel):
    """One row per ``ManifestSource`` in the parser audit."""

    source: str
    has_registered_parser: bool
    rows_pending: int
    rows_fetched: int
    rows_parsed: int
    rows_failed: int
    rows_tombstoned: int
    stuck_no_parser: int


class ManifestParserAuditResponse(BaseModel):
    """Operator-visible report of manifest sources without a parser.

    Pre-#935 §5 the worker debug-skipped rows whose ``source`` had no
    registered parser — silent on every tick. This endpoint joins
    ``sec_filing_manifest`` against the API process's parser registry
    so the operator sees the stuck-row count per source.
    ``stuck_no_parser`` is the actionable number: rows the worker
    would silently skip until a parser lands.

    Process-boundary caveat (Codex pre-push round 1): the parser
    registry is module-global and populated at module import time.
    The API process reads its OWN registry. Today nothing registers
    parsers in either API or worker process (pre-#873), so the
    audit's ``has_registered_parser=False`` is correct everywhere
    and the stuck-row count surfaces the entire manifest. Once #873
    lands parsers in the worker process, the API's registry will
    DIVERGE — the audit becomes misleading until the worker
    publishes its registry into a DB table the API can read.
    Track this as a follow-up before #873 ships.
    """

    checked_at: datetime
    sources: list[ManifestParserSourceRowResponse]
    total_stuck_no_parser: int


@router.get("/manifest-parsers", response_model=ManifestParserAuditResponse)
def get_manifest_parser_audit(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> ManifestParserAuditResponse:
    """#935 §5 — surface no-parser manifest rows.

    For every ``ManifestSource``, returns whether a parser is
    registered and the per-status row count in
    ``sec_filing_manifest``. ``stuck_no_parser`` (pending + fetched
    on a source with no parser) is the actionable count — operator
    must register the parser or tombstone the rows.
    """
    from app.jobs.sec_manifest_worker import registered_parser_sources
    from app.services.manifest_parser_audit import compute_manifest_parser_audit

    registered = frozenset(str(s) for s in registered_parser_sources())
    report = compute_manifest_parser_audit(conn, registered_sources=registered)
    return ManifestParserAuditResponse(
        checked_at=datetime.now(tz=UTC),
        sources=[
            ManifestParserSourceRowResponse(
                source=r.source,
                has_registered_parser=r.has_registered_parser,
                rows_pending=r.rows_pending,
                rows_fetched=r.rows_fetched,
                rows_parsed=r.rows_parsed,
                rows_failed=r.rows_failed,
                rows_tombstoned=r.rows_tombstoned,
                stuck_no_parser=r.stuck_no_parser,
            )
            for r in report.sources
        ],
        total_stuck_no_parser=report.total_stuck_no_parser,
    )
