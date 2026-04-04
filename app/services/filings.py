"""
Filings service.

Ingests filing metadata from SEC EDGAR and Companies House.
The service layer owns:
  - instrument_id → provider-native identifier resolution (via external_identifiers)
  - provider selection
  - DB upserts

Providers are pure HTTP clients and do not touch the database.

Skip behaviour:
  - missing SEC CIK → skip SEC for that instrument, record reason in logs
  - missing Companies House company_number → skip CH for that instrument, record reason
  - provider HTTP error → skip that instrument for that provider, log warning
  - do not fail the whole batch for one missing identifier
"""

import json
import logging
from dataclasses import dataclass
from datetime import date

import psycopg

from app.providers.filings import FilingSearchResult, FilingsProvider

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FilingsRefreshSummary:
    instruments_attempted: int
    filings_upserted: int
    instruments_skipped: int  # identifier missing or provider error


def refresh_filings(
    provider: FilingsProvider,
    provider_name: str,
    identifier_type: str,
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_ids: list[str],
    start_date: date | None = None,
    end_date: date | None = None,
    filing_types: list[str] | None = None,
) -> FilingsRefreshSummary:
    """
    For each instrument_id, resolve the provider-native identifier from
    external_identifiers, fetch filing metadata, and upsert to filing_events.

    provider_name: e.g. 'sec', 'companies_house' — used for identifier lookup
        and for the provider column in filing_events.
    identifier_type: e.g. 'cik', 'company_number'.
    """
    upserted = 0
    skipped = 0

    for instrument_id in instrument_ids:
        identifier_value = _resolve_identifier(conn, instrument_id, provider_name, identifier_type)
        if identifier_value is None:
            logger.info(
                "Filings: no %s/%s for instrument_id=%s, skipping",
                provider_name,
                identifier_type,
                instrument_id,
            )
            skipped += 1
            continue

        try:
            results = provider.list_filings_by_identifier(
                identifier_type=identifier_type,
                identifier_value=identifier_value,
                start_date=start_date,
                end_date=end_date,
                filing_types=filing_types,
            )
            with conn.transaction():
                for result in results:
                    _upsert_filing(conn, instrument_id, provider_name, result)
            # Count only after the transaction commits successfully
            upserted += len(results)
        except Exception:
            logger.warning(
                "Filings: failed to refresh instrument_id=%s via %s, skipping",
                instrument_id,
                provider_name,
                exc_info=True,
            )
            skipped += 1

    return FilingsRefreshSummary(
        instruments_attempted=len(instrument_ids),
        filings_upserted=upserted,
        instruments_skipped=skipped,
    )


def upsert_cik_mapping(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    mapping: dict[str, str],
    instrument_symbols: list[tuple[str, str]],  # [(symbol, instrument_id), ...]
) -> int:
    """
    Upsert SEC CIK mappings into external_identifiers.

    mapping: {TICKER: zero-padded-CIK} as returned by SecFilingsProvider.build_cik_mapping()
    instrument_symbols: list of (symbol, instrument_id) for instruments to update.

    Returns the number of rows upserted.
    """
    upserted = 0
    with conn.transaction():
        for symbol, instrument_id in instrument_symbols:
            cik = mapping.get(symbol.upper())
            if not cik:
                logger.debug("CIK mapping: no CIK found for symbol %s", symbol)
                continue
            conn.execute(
                """
                INSERT INTO external_identifiers (
                    instrument_id, provider, identifier_type, identifier_value,
                    is_primary, last_verified_at
                )
                VALUES (
                    %(instrument_id)s, 'sec', 'cik', %(cik)s,
                    TRUE, NOW()
                )
                ON CONFLICT (provider, identifier_type, identifier_value) DO UPDATE SET
                    instrument_id    = EXCLUDED.instrument_id,
                    last_verified_at = NOW()
                """,
                {"instrument_id": instrument_id, "cik": cik},
            )
            upserted += 1
    return upserted


def _resolve_identifier(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: str,
    provider: str,
    identifier_type: str,
) -> str | None:
    """Look up the primary external identifier for an instrument from the DB."""
    row = conn.execute(
        """
        SELECT identifier_value
        FROM external_identifiers
        WHERE instrument_id = %(instrument_id)s
          AND provider = %(provider)s
          AND identifier_type = %(identifier_type)s
          AND is_primary = TRUE
        LIMIT 1
        """,
        {
            "instrument_id": instrument_id,
            "provider": provider,
            "identifier_type": identifier_type,
        },
    ).fetchone()
    return row[0] if row else None


def _upsert_filing(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: str,
    provider_name: str,
    result: FilingSearchResult,
) -> None:
    """
    Upsert a single filing into filing_events.
    Idempotent — keyed on (provider, provider_filing_id).
    """
    conn.execute(
        """
        INSERT INTO filing_events (
            instrument_id, filing_date, filing_type,
            provider, provider_filing_id, source_url, primary_document_url,
            raw_payload_json
        )
        VALUES (
            %(instrument_id)s, %(filing_date)s, %(filing_type)s,
            %(provider)s, %(provider_filing_id)s, %(source_url)s, %(primary_document_url)s,
            %(raw_payload_json)s
        )
        ON CONFLICT (provider, provider_filing_id) DO UPDATE SET
            filing_date          = EXCLUDED.filing_date,
            filing_type          = EXCLUDED.filing_type,
            source_url           = EXCLUDED.source_url,
            primary_document_url = EXCLUDED.primary_document_url
        """,
        {
            "instrument_id": instrument_id,
            "filing_date": result.filed_at.date(),
            "filing_type": result.filing_type,
            "provider": provider_name,
            "provider_filing_id": result.provider_filing_id,
            "source_url": result.primary_document_url,
            "primary_document_url": result.primary_document_url,
            # Serialise the normalised metadata fields as the auditable payload.
            # Full document text is out of scope for v1; disk persistence of the
            # raw provider response is handled by _persist_raw in each provider.
            "raw_payload_json": json.dumps(
                {
                    "provider_filing_id": result.provider_filing_id,
                    "symbol": result.symbol,
                    "filed_at": result.filed_at.isoformat(),
                    "filing_type": result.filing_type,
                    "period_of_report": result.period_of_report.isoformat() if result.period_of_report else None,
                    "primary_document_url": result.primary_document_url,
                }
            ),
        },
    )
