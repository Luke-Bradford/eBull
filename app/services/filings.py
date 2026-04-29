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

from app.providers.filings import FilingEvent, FilingSearchResult, FilingsProvider

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FilingsRefreshSummary:
    instruments_attempted: int
    filings_upserted: int
    instruments_skipped: int  # identifier missing or provider error


def _bulk_resolve_identifiers(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_ids: list[str],
    provider_name: str,
    identifier_type: str,
) -> dict[str, str]:
    """Single SELECT to map every instrument_id with a primary identifier
    of (provider_name, identifier_type) to its identifier_value.

    Was: per-row ``_resolve_identifier`` in the refresh loop, which
    issued one SELECT per instrument and emitted an INFO log line
    for every miss. With a 12k-row universe and ~7k instruments
    lacking SEC CIKs that produced ~7k DB roundtrips and ~7k log
    lines per refresh tick — enough log spam to make the dev
    terminal unusable (operator report 2026-04-29). The bulk
    resolver replaces both with one query and one summary line at
    the end of ``refresh_filings``.
    """
    if not instrument_ids:
        return {}
    # Cast the parameter list to int so `instrument_id = ANY(%s)`
    # compares against the int4 column without any per-row cast that
    # would defeat the primary-key index (PR #679 review). The
    # caller-supplied list may contain str-typed ids — coerce here
    # rather than at every call site.
    int_ids: list[int] = []
    for i in instrument_ids:
        try:
            int_ids.append(int(i))
        except TypeError, ValueError:
            # Caller passed a non-numeric id; skip rather than abort
            # the whole refresh. Logged at DEBUG so a typo surfaces
            # under verbose logging without inflating the aggregate
            # skip count.
            logger.debug("Skipping non-numeric instrument_id %r in bulk resolve", i)
    if not int_ids:
        return {}
    rows = conn.execute(
        """
        SELECT instrument_id, identifier_value
        FROM external_identifiers
        WHERE provider = %(provider)s
          AND identifier_type = %(identifier_type)s
          AND is_primary = TRUE
          AND instrument_id = ANY(%(ids)s)
        """,
        {
            "provider": provider_name,
            "identifier_type": identifier_type,
            "ids": int_ids,
        },
    ).fetchall()
    # Caller signature accepts str ids — return str-keyed map so the
    # dict-iteration in `refresh_filings` keeps the same key type the
    # original loop used (loop body passes `instrument_id` straight
    # to `_upsert_filing`, which has historically tolerated either
    # int or str).
    return {str(row[0]): row[1] for row in rows}


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
    """For each instrument_id with a primary ``(provider_name,
    identifier_type)`` identifier, fetch filing metadata and upsert
    into ``filing_events``.

    Identifier resolution is bulk-fetched in one SELECT before the
    loop runs (#669). Instruments lacking the identifier are dropped
    silently from the iteration — they're a known property of the
    universe (crypto, FX, non-US equities have no SEC CIK), not a
    transient miss worth logging per-row. A single summary INFO at
    the end records the aggregate skip count so the observability
    signal ("how many of the cohort were eligible") survives.

    provider_name: e.g. 'sec', 'companies_house'.
    identifier_type: e.g. 'cik', 'company_number'.
    """
    if not instrument_ids:
        return FilingsRefreshSummary(
            instruments_attempted=0,
            filings_upserted=0,
            instruments_skipped=0,
        )

    resolved = _bulk_resolve_identifiers(conn, instrument_ids, provider_name, identifier_type)
    skipped_no_identifier = len(instrument_ids) - len(resolved)
    upserted = 0
    skipped_provider_error = 0

    for instrument_id, identifier_value in resolved.items():
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
            skipped_provider_error += 1

    if skipped_no_identifier > 0:
        logger.info(
            "Filings: %d/%d instruments skipped (missing %s/%s identifier — expected for non-%s issuers)",
            skipped_no_identifier,
            len(instrument_ids),
            provider_name,
            identifier_type,
            provider_name.upper(),
        )

    return FilingsRefreshSummary(
        instruments_attempted=len(instrument_ids),
        filings_upserted=upserted,
        instruments_skipped=skipped_no_identifier + skipped_provider_error,
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
            # external_identifiers has TWO uniqueness constraints:
            #   (a) uq_external_identifiers_provider_value — UNIQUE(provider,
            #       identifier_type, identifier_value)
            #   (b) uq_external_identifiers_primary — partial UNIQUE
            #       (instrument_id, provider, identifier_type) WHERE is_primary=TRUE
            # ON CONFLICT can only target one. If an instrument already has a
            # primary sec/cik row with a DIFFERENT value (e.g. the SEC ticker
            # map changed), the INSERT below would violate (b) before (a)
            # ever matched. Demote any mismatching primary row first so the
            # upsert's own conflict target handles the rest.
            conn.execute(
                """
                UPDATE external_identifiers
                SET is_primary = FALSE
                WHERE instrument_id     = %(instrument_id)s
                  AND provider          = 'sec'
                  AND identifier_type   = 'cik'
                  AND is_primary        = TRUE
                  AND identifier_value != %(cik)s
                """,
                {"instrument_id": instrument_id, "cik": cik},
            )
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
                    is_primary       = TRUE,
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


def _upsert_filing_event(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: str | int,
    provider_name: str,
    event: FilingEvent,
) -> None:
    """Upsert a ``FilingEvent`` (from ``provider.get_filing``) into
    ``filing_events``.

    Mirrors ``_upsert_filing``'s idempotent ON CONFLICT semantics and
    payload shape but accepts the richer ``FilingEvent`` variant
    returned by ``FilingsProvider.get_filing``. Chunk E's 8-K gap
    fill path uses this when re-fetching a single accession (#268).
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
            "filing_date": event.filed_at.date(),
            "filing_type": event.filing_type,
            "provider": provider_name,
            "provider_filing_id": event.provider_filing_id,
            "source_url": event.primary_document_url,
            "primary_document_url": event.primary_document_url,
            "raw_payload_json": json.dumps(
                {
                    "provider_filing_id": event.provider_filing_id,
                    "symbol": event.symbol,
                    "filed_at": event.filed_at.isoformat(),
                    "filing_type": event.filing_type,
                    "period_of_report": event.period_of_report.isoformat() if event.period_of_report else None,
                    "primary_document_url": event.primary_document_url,
                }
            ),
        },
    )


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
