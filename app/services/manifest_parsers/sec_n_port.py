"""SEC NPORT-P manifest-worker parser adapter (#873).

NPORT-P is filed monthly by registered investment companies (mutual
funds, ETFs, closed-end funds) detailing their portfolio holdings.
The bulk path is the monthly per-CIK ingest in
``app/services/n_port_ingest.py``; the quarterly dataset job
(``sec_nport_dataset_ingest``) covers historical backfill. This thin
manifest adapter handles atom-discovered freshness one accession at
a time, mirroring ``app/services/manifest_parsers/sec_13f_hr.py``.

ParseOutcome contract:

  * ``status='parsed'`` + ``raw_status='stored'`` — primary_doc.xml
    persisted; ``sec_fund_series`` upserted; per-holding rows
    filtered (equity-common only, Long only, NS units only, non-zero
    shares, resolved CUSIP) and written via ``record_fund_observation``;
    ``ownership_funds_current`` refreshed per touched instrument; one
    ``n_port_ingest_log`` row with status='success' / 'partial'.
  * ``status='tombstoned'`` — primary_doc.xml fetch failed,
    ``NPortMissingSeriesError`` raised (filing lacks series id), or
    parse otherwise raised deterministically. ``n_port_ingest_log``
    records 'failed' so dashboard counts converge.
  * ``status='failed'`` — transient error (fetch raise, store_raw
    error, psycopg ``OperationalError`` on upsert). Worker schedules
    1h backoff retry.

Subject identity (sec-edgar §3.6): NPORT-P is fund-trust-scoped —
manifest row carries ``subject_type='institutional_filer'``
(filer-scoped reuse; the trust CIK is the filer) OR
``subject_type='fund_series'`` depending on discovery path. The
adapter only requires ``row.cik``; the parser extracts the series
id from the XML payload.

#1131 discrimination: transient psycopg ``OperationalError`` on the
observation upsert retries via 1h backoff; deterministic constraint
violations write the audit-log row with status='failed' and
tombstone the manifest. See
``app/services/manifest_parsers/_classify.py``.

Raw-payload invariant (#938): registered with
``requires_raw_payload=True``; the parser stores the body BEFORE
parsing so a downstream re-wash can reparse without re-fetching SEC.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import psycopg

from app.config import settings
from app.providers.implementations.sec_edgar import SecFilingsProvider
from app.services.manifest_parsers._classify import (
    format_upsert_error,
    is_transient_upsert_error,
)
from app.services.n_port_ingest import (
    _PARSER_VERSION_NPORT,
    NPortMissingSeriesError,
    NPortParseError,
    _archive_file_url,
    _record_ingest_attempt,
    _resolve_cusip_to_instrument_id,
    parse_n_port_payload,
)
from app.services.ownership_observations import (
    record_fund_observation,
    refresh_funds_current,
    upsert_sec_fund_series,
)
from app.services.raw_filings import store_raw

logger = logging.getLogger(__name__)

_FAILED_RETRY_DELAY = timedelta(hours=1)


def _failed_outcome(error: str, raw_status: Any = None) -> Any:
    """Build a ``failed`` ParseOutcome with a 1h backoff applied."""
    from app.jobs.sec_manifest_worker import ParseOutcome

    return ParseOutcome(
        status="failed",
        parser_version=_PARSER_VERSION_NPORT,
        raw_status=raw_status,
        error=error,
        next_retry_at=datetime.now(tz=UTC) + _FAILED_RETRY_DELAY,
    )


def _parse_n_port(
    conn: psycopg.Connection[Any],
    row: Any,  # ManifestRow — forward-ref to avoid circular import
) -> Any:  # ParseOutcome — forward-ref
    """Manifest-worker parser for one NPORT-P / NPORT-P/A accession."""
    from app.jobs.sec_manifest_worker import ParseOutcome

    accession = row.accession_number
    filer_cik = (row.cik or "").strip()

    if not filer_cik:
        logger.warning(
            "n_port manifest parser: accession=%s has no filer cik; tombstoning",
            accession,
        )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_NPORT,
            error="missing filer cik",
        )

    # NPORT-P primary doc lives at the accession's primary_doc.xml.
    primary_url = _archive_file_url(filer_cik, accession, "primary_doc.xml")

    try:
        with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider:
            primary_xml = provider.fetch_document_text(primary_url)
    except Exception as exc:  # noqa: BLE001 — transient fetch errors retry via backoff
        logger.warning(
            "n_port manifest parser: fetch raised accession=%s url=%s: %s",
            accession,
            primary_url,
            exc,
        )
        return _failed_outcome(f"fetch error: {exc}")

    if not primary_xml:
        # 404 / empty — mirror legacy 'failed' accounting in the
        # ingest log; manifest itself tombstones so the worker stops
        # re-fetching a persistently-404 doc.
        try:
            with conn.transaction():
                _record_ingest_attempt(
                    conn,
                    filer_cik=filer_cik,
                    accession_number=accession,
                    fund_series_id=None,
                    period_of_report=row.filed_at.date() if row.filed_at else None,
                    status="failed",
                    holdings_inserted=0,
                    holdings_skipped=0,
                    error="primary_doc.xml fetch failed",
                )
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "n_port manifest parser: ingest-log INSERT failed accession=%s",
                accession,
            )
            return _failed_outcome(f"log error: {exc}")
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_NPORT,
            error="primary_doc.xml fetch failed",
        )

    try:
        with conn.transaction():
            store_raw(
                conn,
                accession_number=accession,
                document_kind="nport_xml",
                payload=primary_xml,
                parser_version=_PARSER_VERSION_NPORT,
                source_url=primary_url,
            )
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "n_port manifest parser: store_raw failed accession=%s",
            accession,
        )
        return _failed_outcome(f"store_raw error: {exc}")

    # Parse-phase: NPortMissingSeriesError + NPortParseError are
    # deterministic — the filing's XML shape itself is the problem.
    # Tombstone with log entry. Unexpected exceptions also caught so a
    # parser-crash doesn't leak past the worker's generic handler with
    # the raw row diverging from the manifest's view.
    try:
        parsed = parse_n_port_payload(primary_xml)
    except NPortMissingSeriesError as exc:
        log_error = f"missing series: {exc}"
        try:
            with conn.transaction():
                _record_ingest_attempt(
                    conn,
                    filer_cik=filer_cik,
                    accession_number=accession,
                    fund_series_id=None,
                    period_of_report=None,
                    status="failed",
                    holdings_inserted=0,
                    holdings_skipped=0,
                    error=log_error,
                )
        except Exception:  # noqa: BLE001
            logger.exception(
                "n_port manifest parser: ingest-log INSERT failed after missing-series accession=%s",
                accession,
            )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_NPORT,
            raw_status="stored",
            error=log_error,
        )
    except Exception as exc:  # noqa: BLE001
        is_schema = isinstance(exc, NPortParseError)
        kind = "parse failed" if is_schema else "parse failed (unexpected)"
        logger.exception(
            "n_port manifest parser: parse raised accession=%s (unexpected=%s)",
            accession,
            not is_schema,
        )
        try:
            with conn.transaction():
                _record_ingest_attempt(
                    conn,
                    filer_cik=filer_cik,
                    accession_number=accession,
                    fund_series_id=None,
                    period_of_report=None,
                    status="failed",
                    holdings_inserted=0,
                    holdings_skipped=0,
                    error=f"{kind}: {exc}",
                )
        except Exception:  # noqa: BLE001
            logger.exception(
                "n_port manifest parser: ingest-log INSERT failed after parse error accession=%s",
                accession,
            )
        return _failed_outcome(f"{kind}: {exc}", raw_status="stored")

    # Upsert phase. Mirror the legacy filter ladder from
    # ``n_port_ingest._ingest_single_accession``. Wrap the entire
    # batch in one savepoint so a mid-batch DB error rolls back
    # partial state, and apply #1131 discrimination on the upsert
    # exception class.
    inserted = 0
    skipped_no_cusip = 0
    skipped_non_equity = 0
    skipped_short = 0
    skipped_non_share_units = 0
    skipped_zero_shares = 0
    touched_instruments: set[int] = set()
    run_id = uuid4()

    if parsed.filed_at is not None:
        filed_at = parsed.filed_at
    elif row.filed_at is not None:
        filed_at = row.filed_at
    else:
        filed_at = datetime(parsed.period_end.year, parsed.period_end.month, parsed.period_end.day, tzinfo=UTC)

    try:
        with conn.transaction():
            upsert_sec_fund_series(
                conn,
                fund_series_id=parsed.series_id,
                fund_series_name=parsed.series_name,
                fund_filer_cik=parsed.filer_cik,
                last_seen_period_end=parsed.period_end,
            )

            for holding in parsed.holdings:
                # Apply the equity-common-Long write-side guards as drop
                # filters BEFORE the helper's value-error raise so the
                # operator-visible counters stay accurate. Same order as
                # legacy ingester: non-equity → short → units → zero-shares
                # → no-cusip (most specific first).
                if holding.asset_category != "EC":
                    skipped_non_equity += 1
                    continue
                if holding.payoff_profile != "Long":
                    skipped_short += 1
                    continue
                if holding.units != "NS":
                    skipped_non_share_units += 1
                    continue
                if holding.shares is None or holding.shares <= 0:
                    skipped_zero_shares += 1
                    continue
                if not holding.cusip or len(holding.cusip) != 9:
                    skipped_no_cusip += 1
                    continue
                instrument_id = _resolve_cusip_to_instrument_id(conn, holding.cusip)
                if instrument_id is None:
                    skipped_no_cusip += 1
                    continue

                try:
                    record_fund_observation(
                        conn,
                        instrument_id=instrument_id,
                        fund_series_id=parsed.series_id,
                        fund_series_name=parsed.series_name,
                        fund_filer_cik=parsed.filer_cik,
                        source_document_id=accession,
                        source_accession=accession,
                        source_field=None,
                        source_url=primary_url,
                        filed_at=filed_at,
                        period_start=None,
                        period_end=parsed.period_end,
                        ingest_run_id=run_id,
                        shares=holding.shares,
                        market_value_usd=holding.value_usd,
                        payoff_profile=holding.payoff_profile,
                        asset_category=holding.asset_category,
                    )
                    inserted += 1
                    touched_instruments.add(instrument_id)
                except ValueError:
                    # Helper-side guard rejected — log + count as
                    # non-equity (closest existing bucket). The loop
                    # guards above mirror the helper; reaching this
                    # branch signals a parser-helper contract drift.
                    logger.exception(
                        "n_port manifest parser: helper-side guard rejected holding "
                        "cik=%s accession=%s cusip=%s (parser-helper contract drift)",
                        filer_cik,
                        accession,
                        holding.cusip,
                    )
                    skipped_non_equity += 1

            for unique_instrument_id in touched_instruments:
                refresh_funds_current(conn, instrument_id=unique_instrument_id)

            total_skipped = (
                skipped_no_cusip + skipped_non_equity + skipped_short + skipped_non_share_units + skipped_zero_shares
            )
            if not parsed.holdings:
                # Legal-empty NPORT-P — fund holding only cash mid-quarter.
                status = "success"
                log_error: str | None = None
            elif inserted == 0 and total_skipped > 0:
                # Every parsed holding filtered out.
                status = "partial"
                log_error = (
                    f"all holdings filtered (non-equity={skipped_non_equity}, short={skipped_short}, "
                    f"non-share-units={skipped_non_share_units}, zero-shares={skipped_zero_shares}, "
                    f"no-cusip={skipped_no_cusip})"
                )
            elif total_skipped > 0:
                status = "partial"
                log_error = (
                    f"non-equity={skipped_non_equity}, short={skipped_short}, "
                    f"non-share-units={skipped_non_share_units}, zero-shares={skipped_zero_shares}, "
                    f"no-cusip={skipped_no_cusip}"
                )
            else:
                status = "success"
                log_error = None

            _record_ingest_attempt(
                conn,
                filer_cik=filer_cik,
                accession_number=accession,
                fund_series_id=parsed.series_id,
                period_of_report=parsed.period_end,
                status=status,
                holdings_inserted=inserted,
                holdings_skipped=total_skipped,
                error=log_error,
            )
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "n_port manifest parser: upsert/observation batch failed accession=%s",
            accession,
        )
        if is_transient_upsert_error(exc):
            return _failed_outcome(format_upsert_error(exc), raw_status="stored")
        # Deterministic — write ingest-log 'failed' in a fresh savepoint
        # then tombstone the manifest.
        try:
            with conn.transaction():
                _record_ingest_attempt(
                    conn,
                    filer_cik=filer_cik,
                    accession_number=accession,
                    fund_series_id=parsed.series_id,
                    period_of_report=parsed.period_end,
                    status="failed",
                    holdings_inserted=0,
                    holdings_skipped=0,
                    error=format_upsert_error(exc),
                )
        except Exception:  # noqa: BLE001
            logger.exception(
                "n_port manifest parser: ingest-log INSERT failed after upsert error accession=%s",
                accession,
            )
            return _failed_outcome(
                f"upsert+log error: {type(exc).__name__}: {exc}",
                raw_status="stored",
            )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_NPORT,
            raw_status="stored",
            error=format_upsert_error(exc),
        )

    return ParseOutcome(
        status="parsed",
        parser_version=_PARSER_VERSION_NPORT,
        raw_status="stored",
    )


def register() -> None:
    """Register the NPORT-P parser with the manifest worker.

    Idempotent — ``register_parser`` is last-write-wins. The bulk
    monthly / quarterly paths continue to handle historical drains
    in parallel; the manifest path drives atom-discovered freshness
    one accession at a time.
    """
    from app.jobs.sec_manifest_worker import register_parser

    register_parser("sec_n_port", _parse_n_port, requires_raw_payload=True)
