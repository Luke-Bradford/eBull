"""Financial facts service — fetch XBRL facts and store in financial_facts_raw.

Orchestrates:
  1. Start an ingestion run (audit trail)
  2. For each instrument, call provider.extract_facts() to get XBRL facts
  3. Upsert facts into financial_facts_raw
  4. Finish the ingestion run with summary counts
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

import psycopg

from app.providers.fundamentals import XbrlFact
from app.services.sync_orchestrator.progress import report_progress

if TYPE_CHECKING:
    from app.providers.implementations.sec_fundamentals import SecFundamentalsProvider

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FactsRefreshSummary:
    symbols_attempted: int
    facts_upserted: int
    facts_skipped: int
    symbols_failed: int


def start_ingestion_run(
    conn: psycopg.Connection[tuple],
    *,
    source: str,
    endpoint: str | None = None,
    instrument_count: int | None = None,
) -> int:
    """Insert a new data_ingestion_runs row with status='running'. Returns the run ID."""
    cur = conn.execute(
        """
        INSERT INTO data_ingestion_runs (source, endpoint, instrument_count)
        VALUES (%(source)s, %(endpoint)s, %(instrument_count)s)
        RETURNING ingestion_run_id
        """,
        {"source": source, "endpoint": endpoint, "instrument_count": instrument_count},
    )
    row = cur.fetchone()
    assert row is not None
    return row[0]


def finish_ingestion_run(
    conn: psycopg.Connection[tuple],
    *,
    run_id: int,
    status: str,
    rows_upserted: int = 0,
    rows_skipped: int = 0,
    error: str | None = None,
) -> None:
    """Update an ingestion run with final status and counts."""
    conn.execute(
        """
        UPDATE data_ingestion_runs
        SET finished_at = NOW(),
            status = %(status)s,
            rows_upserted = %(rows_upserted)s,
            rows_skipped = %(rows_skipped)s,
            error = %(error)s
        WHERE ingestion_run_id = %(run_id)s
        """,
        {
            "run_id": run_id,
            "status": status,
            "rows_upserted": rows_upserted,
            "rows_skipped": rows_skipped,
            "error": error,
        },
    )


def upsert_facts_for_instrument(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    facts: Sequence[XbrlFact],
    ingestion_run_id: int,
) -> tuple[int, int]:
    """Upsert XBRL facts into financial_facts_raw.

    Returns (upserted_count, skipped_count).
    Uses ON CONFLICT DO UPDATE so restatements overwrite prior values.
    """
    if not facts:
        return 0, 0

    upserted = 0
    skipped = 0
    for fact in facts:
        cur = conn.execute(
            """
            INSERT INTO financial_facts_raw (
                instrument_id, taxonomy, concept, unit,
                period_start, period_end, val, frame,
                accession_number, form_type, filed_date,
                fiscal_year, fiscal_period, decimals,
                ingestion_run_id
            ) VALUES (
                %(instrument_id)s, %(taxonomy)s, %(concept)s, %(unit)s,
                %(period_start)s, %(period_end)s, %(val)s, %(frame)s,
                %(accession_number)s, %(form_type)s, %(filed_date)s,
                %(fiscal_year)s, %(fiscal_period)s, %(decimals)s,
                %(ingestion_run_id)s
            )
            ON CONFLICT (
                instrument_id, concept, unit,
                COALESCE(period_start, '0001-01-01'::date),
                period_end, accession_number
            )
            DO UPDATE SET
                val = EXCLUDED.val,
                frame = EXCLUDED.frame,
                form_type = EXCLUDED.form_type,
                filed_date = EXCLUDED.filed_date,
                fiscal_year = EXCLUDED.fiscal_year,
                fiscal_period = EXCLUDED.fiscal_period,
                decimals = EXCLUDED.decimals,
                ingestion_run_id = EXCLUDED.ingestion_run_id,
                fetched_at = NOW()
            WHERE financial_facts_raw.val IS DISTINCT FROM EXCLUDED.val
               OR financial_facts_raw.frame IS DISTINCT FROM EXCLUDED.frame
            """,
            {
                "instrument_id": instrument_id,
                "taxonomy": fact.taxonomy,
                "concept": fact.concept,
                "unit": fact.unit,
                "period_start": fact.period_start,
                "period_end": fact.period_end,
                "val": fact.val,
                "frame": fact.frame,
                "accession_number": fact.accession_number,
                "form_type": fact.form_type,
                "filed_date": fact.filed_date,
                "fiscal_year": fact.fiscal_year,
                "fiscal_period": fact.fiscal_period,
                "decimals": fact.decimals,
                "ingestion_run_id": ingestion_run_id,
            },
        )
        if cur.rowcount > 0:
            upserted += 1
        else:
            skipped += 1

    return upserted, skipped


def refresh_financial_facts(
    provider: SecFundamentalsProvider,
    conn: psycopg.Connection[tuple],
    symbols: Sequence[tuple[str, int, str]],
) -> FactsRefreshSummary:
    """Fetch and store XBRL facts for all given symbols.

    Parameters
    ----------
    symbols:
        List of (symbol, instrument_id, cik) tuples.
    """
    run_id = start_ingestion_run(
        conn,
        source="sec_edgar",
        endpoint="/api/xbrl/companyfacts",
        instrument_count=len(symbols),
    )

    total_upserted = 0
    total_skipped = 0
    failed = 0
    total = len(symbols)

    for idx, (symbol, instrument_id, cik) in enumerate(symbols, start=1):
        try:
            with conn.transaction():
                facts = provider.extract_facts(symbol, cik)
                if not facts:
                    logger.info("No XBRL facts for %s (CIK %s)", symbol, cik)
                    continue
                upserted, skipped = upsert_facts_for_instrument(
                    conn,
                    instrument_id=instrument_id,
                    facts=facts,
                    ingestion_run_id=run_id,
                )
                total_upserted += upserted
                total_skipped += skipped
                logger.info(
                    "SEC facts for %s: %d upserted, %d skipped",
                    symbol,
                    upserted,
                    skipped,
                )
        except Exception:
            failed += 1
            logger.exception("Failed to refresh SEC facts for %s", symbol)
        report_progress(idx, total)

    report_progress(total, total, force=True)

    status = "success" if failed == 0 else ("partial" if total_upserted > 0 else "failed")
    finish_ingestion_run(
        conn,
        run_id=run_id,
        status=status,
        rows_upserted=total_upserted,
        rows_skipped=total_skipped,
        error=f"{failed} symbols failed" if failed > 0 else None,
    )

    return FactsRefreshSummary(
        symbols_attempted=len(symbols),
        facts_upserted=total_upserted,
        facts_skipped=total_skipped,
        symbols_failed=failed,
    )
