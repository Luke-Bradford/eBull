"""Fund-metadata read endpoints (#1171, T9 in the plan).

Three routes:

- ``GET /instruments/{symbol}/fund-metadata`` — current row from
  ``fund_metadata_current`` (most recent observation per the source-
  priority chain).
- ``GET /instruments/{symbol}/fund-metadata/history`` — currently-valid
  observation timeline from ``fund_metadata_observations`` with
  optional ``since`` filter.
- ``GET /coverage/fund-metadata`` — per-source coverage audit
  (observation count + resolver-miss directory entries).

Auth: all routes require operator auth via
``require_session_or_service_token``.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from app.api.auth import require_session_or_service_token
from app.db import get_conn

logger = logging.getLogger(__name__)

router = APIRouter(
    tags=["fund-metadata"],
    dependencies=[Depends(require_session_or_service_token)],
)


class FundMetadataResponse(BaseModel):
    instrument_id: int
    symbol: str
    class_id: str
    series_id: str | None
    document_type: str
    period_end: date
    filed_at: datetime
    parser_version: str
    amendment_flag: bool
    trust_cik: str
    trust_name: str | None
    series_name: str | None
    class_name: str | None
    trading_symbol: str | None
    exchange: str | None
    inception_date: date | None
    shareholder_report_type: str | None
    expense_ratio_pct: Decimal | None
    expenses_paid_amt: Decimal | None
    net_assets_amt: Decimal | None
    advisory_fees_paid_amt: Decimal | None
    portfolio_turnover_pct: Decimal | None
    holdings_count: int | None
    returns_pct: dict[str, Any] | None
    benchmark_returns_pct: dict[str, Any] | None
    sector_allocation: dict[str, Any] | None
    region_allocation: dict[str, Any] | None
    credit_quality_allocation: dict[str, Any] | None
    growth_curve: list[dict[str, Any]] | None
    material_chng_date: date | None
    material_chng_notice: str | None
    contact_phone: str | None
    contact_website: str | None
    contact_email: str | None
    prospectus_phone: str | None
    prospectus_website: str | None
    prospectus_email: str | None
    refreshed_at: datetime


class FundMetadataObservation(BaseModel):
    source_accession: str
    document_type: str
    period_end: date
    filed_at: datetime
    parser_version: str
    amendment_flag: bool
    class_id: str
    series_id: str | None
    expense_ratio_pct: Decimal | None
    net_assets_amt: Decimal | None


class FundMetadataCoverageResponse(BaseModel):
    total_observations_current: int
    total_instruments_with_current: int
    directory_class_count: int
    directory_with_external_id: int
    directory_pending_external_id: int


def _resolve_instrument_id(conn: psycopg.Connection[Any], symbol: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT instrument_id FROM instruments WHERE symbol = %s", (symbol,))
        row = cur.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Unknown symbol: {symbol}")
    return int(row[0])


@router.get("/instruments/{symbol}/fund-metadata", response_model=FundMetadataResponse)
def get_fund_metadata(
    symbol: str,
    conn: psycopg.Connection[Any] = Depends(get_conn),
) -> FundMetadataResponse:
    """Return the current fund-metadata row for ``symbol``.

    404 if symbol is not a known instrument. 404 if instrument has no
    fund-metadata row (a non-fund instrument or a fund whose N-CSR
    hasn't been ingested yet).
    """
    try:
        instrument_id = _resolve_instrument_id(conn, symbol)
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT * FROM fund_metadata_current WHERE instrument_id = %s",
                (instrument_id,),
            )
            row = cur.fetchone()
    except HTTPException:
        raise
    except Exception:  # noqa: BLE001 — surface infra failure via fixed-phrase 503 (prevention-log #86)
        logger.exception("get_fund_metadata DB error symbol=%s", symbol)
        raise HTTPException(status_code=503, detail="fund_metadata read failed") from None

    if row is None:
        raise HTTPException(status_code=404, detail=f"No fund metadata for symbol: {symbol}")

    return FundMetadataResponse(instrument_id=instrument_id, symbol=symbol, **row)


@router.get(
    "/instruments/{symbol}/fund-metadata/history",
    response_model=list[FundMetadataObservation],
)
def get_fund_metadata_history(
    symbol: str,
    since: date | None = Query(None, description="Filter observations with period_end >= since"),
    conn: psycopg.Connection[Any] = Depends(get_conn),
) -> list[FundMetadataObservation]:
    """Return the currently-valid observation timeline for ``symbol``."""
    try:
        instrument_id = _resolve_instrument_id(conn, symbol)
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            if since is not None:
                cur.execute(
                    """
                    SELECT source_accession, document_type, period_end, filed_at,
                           parser_version, amendment_flag, class_id, series_id,
                           expense_ratio_pct, net_assets_amt
                    FROM fund_metadata_observations
                    WHERE instrument_id = %s
                      AND known_to IS NULL
                      AND period_end >= %s
                    ORDER BY period_end DESC, filed_at DESC
                    """,
                    (instrument_id, since),
                )
            else:
                cur.execute(
                    """
                    SELECT source_accession, document_type, period_end, filed_at,
                           parser_version, amendment_flag, class_id, series_id,
                           expense_ratio_pct, net_assets_amt
                    FROM fund_metadata_observations
                    WHERE instrument_id = %s
                      AND known_to IS NULL
                    ORDER BY period_end DESC, filed_at DESC
                    """,
                    (instrument_id,),
                )
            rows = cur.fetchall()
    except HTTPException:
        raise
    except Exception:  # noqa: BLE001
        logger.exception("get_fund_metadata_history DB error symbol=%s", symbol)
        raise HTTPException(status_code=503, detail="fund_metadata history read failed") from None

    return [FundMetadataObservation(**r) for r in rows]


@router.get("/coverage/fund-metadata", response_model=FundMetadataCoverageResponse)
def get_fund_metadata_coverage(
    conn: psycopg.Connection[Any] = Depends(get_conn),
) -> FundMetadataCoverageResponse:
    """Operator audit: per-source coverage + resolver-miss state."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM fund_metadata_current")
            row = cur.fetchone()
            current_count = int(row[0]) if row else 0

            cur.execute("SELECT COUNT(*) FROM fund_metadata_observations WHERE known_to IS NULL")
            row = cur.fetchone()
            observation_count = int(row[0]) if row else 0

            cur.execute("SELECT COUNT(*) FROM cik_refresh_mf_directory")
            row = cur.fetchone()
            directory_count = int(row[0]) if row else 0

            cur.execute(
                """
                SELECT COUNT(*)
                FROM cik_refresh_mf_directory mf
                WHERE EXISTS (
                    SELECT 1 FROM external_identifiers ei
                    WHERE ei.provider='sec' AND ei.identifier_type='class_id'
                      AND ei.identifier_value = mf.class_id
                )
                """
            )
            row = cur.fetchone()
            with_ext_id = int(row[0]) if row else 0

        return FundMetadataCoverageResponse(
            total_observations_current=observation_count,
            total_instruments_with_current=current_count,
            directory_class_count=directory_count,
            directory_with_external_id=with_ext_id,
            directory_pending_external_id=directory_count - with_ext_id,
        )
    except Exception:  # noqa: BLE001
        logger.exception("get_fund_metadata_coverage DB error")
        raise HTTPException(status_code=503, detail="fund_metadata coverage read failed") from None
