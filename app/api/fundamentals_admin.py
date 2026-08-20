"""Targeted SEC-fundamentals force-refresh endpoint (#677 Part A).

``POST /admin/fundamentals/refresh`` lets an operator re-extract
companyfacts for a named set of symbols *now*, bypassing the
``plan_refresh`` watermark gate. The daily job only refreshes CIKs whose
top-accession changed, so a ``TRACKED_CONCEPTS`` / allowlist extension
does not backfill existing instruments until SEC ships them a fresh
filing. This endpoint closes that gap.

The resolve -> fetch -> normalize core is shared with the CLI
(``scripts/force_refresh_fundamentals.py``) via
``app.services.fundamentals.force_refresh.run_force_refresh`` — one
reviewed implementation, no drift. Data treatment is unchanged from the
daily path (settled "Fundamentals provider posture", #532); only the
trigger is new.

Connection model: the handler is a sync ``def`` (FastAPI runs it in the
threadpool — no event-loop block) and opens a dedicated ``connect_job()``
connection for the multi-second SEC fetch rather than the pooled
``get_conn`` dependency, so a slow refresh never starves the request
pool. ``run_force_refresh`` owns commit discipline and closes the
connection via the ``with`` block.

Auth: ``require_session_or_service_token`` (matches ``/jobs/{name}/run``).

Spec: docs/specs/api/2026-06-28-fundamentals-force-refresh.md.
Part B (``expected_filings`` poll watchlist) is a deferred follow-up.
"""

from __future__ import annotations

import logging

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.api.auth import require_session_or_service_token
from app.db import get_conn
from app.jobs.job_connection import connect_job
from app.services.fair_value_band import METHOD_VERSION as FAIR_VALUE_BAND_METHOD_VERSION
from app.services.fundamentals.force_refresh import run_force_refresh

logger = logging.getLogger(__name__)

# Work-bounding safety cap on distinct symbols, NOT a latency guarantee
# — companyfacts latency + normalization volume vary. SEC's 10 req/s
# shared throttle means ~50 issuers finish in the single-digit-seconds
# range typically.
MAX_SYMBOLS = 50

router = APIRouter(
    prefix="/admin/fundamentals",
    tags=["admin", "fundamentals"],
    dependencies=[Depends(require_session_or_service_token)],
)


class ForceRefreshRequest(BaseModel):
    symbols: list[str] = Field(..., description="Ticker symbols to force-refresh (case-insensitive).")


class SymbolResult(BaseModel):
    symbol: str
    resolved: bool
    instrument_id: int | None
    cik: str | None


class ForceRefreshResponse(BaseModel):
    requested: int  # distinct, normalized symbols
    resolved: int
    facts_upserted: int
    facts_skipped: int
    symbols_failed: int  # fetch/parse failures among resolved (see spec §4)
    periods_canonical_upserted: int
    results: list[SymbolResult]  # resolved entries first, then unresolved


def _distinct_symbols(raw: list[str]) -> list[str]:
    """Upper-case, strip, drop empties, dedup preserving first-seen order."""
    out: list[str] = []
    seen: set[str] = set()
    for s in raw:
        sym = s.strip().upper()
        if not sym or sym in seen:
            continue
        seen.add(sym)
        out.append(sym)
    return out


@router.post("/refresh", response_model=ForceRefreshResponse)
def force_refresh_fundamentals(req: ForceRefreshRequest) -> ForceRefreshResponse:
    """Force a companyfacts re-fetch + re-normalization for ``symbols``.

    Unresolved symbols (unknown instrument or no primary SEC CIK) are
    reported per-symbol with ``resolved=false`` — not a whole-call 404,
    since partial resolution is normal.
    """
    symbols = _distinct_symbols(req.symbols)
    if not symbols:
        raise HTTPException(status_code=400, detail="symbols must contain at least one non-empty symbol")
    if len(symbols) > MAX_SYMBOLS:
        raise HTTPException(status_code=400, detail=f"at most {MAX_SYMBOLS} symbols per call")

    with connect_job() as conn:
        result = run_force_refresh(conn, symbols)

    results = [
        SymbolResult(symbol=r.symbol.upper(), resolved=True, instrument_id=r.instrument_id, cik=r.cik)
        for r in result.resolved
    ] + [SymbolResult(symbol=m, resolved=False, instrument_id=None, cik=None) for m in result.missing]

    logger.info(
        "force_refresh_fundamentals: requested=%d resolved=%d facts_upserted=%d failed=%d canonical=%d",
        len(symbols),
        len(result.resolved),
        result.facts.facts_upserted,
        result.facts.symbols_failed,
        result.periods.periods_canonical_upserted,
    )

    return ForceRefreshResponse(
        requested=len(symbols),
        resolved=len(result.resolved),
        facts_upserted=result.facts.facts_upserted,
        facts_skipped=result.facts.facts_skipped,
        symbols_failed=result.facts.symbols_failed,
        periods_canonical_upserted=result.periods.periods_canonical_upserted,
        results=results,
    )


# ---------------------------------------------------------------------
# Fair-value band reason-bucket rollup (#2009, Task A9)
# ---------------------------------------------------------------------
#
# The band is an orchestrator DAG layer, not an admin ProcessRow, so it
# does not auto-surface a health tile. This read lets the operator
# distinguish an expected dev-stale distribution (mostly ``stale_price``
# when dev market-data is weeks behind) from a real regression (a spike
# in ``thin_cohort`` / ``no_multiple`` / ``currency_mismatch``) without
# reading thousands of ``fair_value_band_current`` rows. ``reason`` and
# ``quality_status`` are the two axes the operator triages on.


class FairValueBandRollupBucket(BaseModel):
    """One (reason, quality_status) count. ``quality_status`` is NULL for
    every statused-absent reason (only ``reason='ok'`` carries a tier)."""

    reason: str
    quality_status: str | None
    count: int


class FairValueBandRollupResponse(BaseModel):
    method_version: str
    total: int
    buckets: list[FairValueBandRollupBucket]


@router.get("/fair-value-band/rollup", response_model=FairValueBandRollupResponse)
def fair_value_band_rollup(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> FairValueBandRollupResponse:
    """Count ``fair_value_band_current`` rows grouped by ``reason`` then
    ``quality_status`` for the live method version.

    Scoped to the current ``fvb_v2`` method version so a stale prior-version
    row (left behind by a ``method_version`` bump before the operator runs
    the ``fair_value_band_refresh`` recompute) never inflates the counts.
    """
    with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
        cur.execute(
            """
            SELECT reason, quality_status, count(*)
            FROM fair_value_band_current
            WHERE method_version = %(mv)s
            GROUP BY reason, quality_status
            ORDER BY reason, quality_status NULLS FIRST
            """,
            {"mv": FAIR_VALUE_BAND_METHOD_VERSION},
        )
        rows = cur.fetchall()

    buckets = [FairValueBandRollupBucket(reason=str(r[0]), quality_status=r[1], count=int(r[2])) for r in rows]
    return FairValueBandRollupResponse(
        method_version=FAIR_VALUE_BAND_METHOD_VERSION,
        total=sum(b.count for b in buckets),
        buckets=buckets,
    )
