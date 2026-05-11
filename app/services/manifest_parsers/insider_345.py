"""Form 3 / Form 4 manifest-worker parser adapter (#873).

Wraps the existing ``parse_form_3_xml`` / ``parse_form_4_xml`` + the
matching upsert paths into the manifest-worker ``ParserFn`` contract.
One callable is registered against each source — Form 3 and Form 4
share the EDGAR ownership XML namespace but persist into different
tables (``insider_initial_holdings`` vs ``insider_transactions``)
and have separate parser_version watermarks, so they are kept as two
sibling callables (not merged into one dispatch).

Form 5 (annual statement of changes in beneficial ownership) is
NOT covered by this PR — the legacy ingester does not parse Form 5
and adding it requires a Form 5 parser + upsert path. Form 5
manifest rows continue to skip with ``no parser`` until the
legacy support lands.

ParseOutcome contract:

  * ``status='parsed'`` + ``raw_status='stored'`` — XML persisted in
    ``filing_raw_documents``; ``insider_filings`` + per-form child
    tables upserted; observations write-through + per-instrument
    ``ownership_insiders_current`` refresh fan out across share-class
    siblings.
  * ``status='tombstoned'`` — fetch returned non-200/empty body OR
    the parser returned ``None`` (malformed XML, missing required
    fields). Matches legacy semantics so dashboard counts converge.
  * ``status='failed'`` — transient error (fetch raise, store_raw
    error, upsert error). Worker schedules a 1h backoff retry.

Raw-payload invariant (#938): registered with
``requires_raw_payload=True``. ``store_raw`` runs in its own
savepoint BEFORE parse + upsert so the invariant holds whether
downstream succeeds or raises.

URL canonicalisation: legacy ``_canonical_form_4_url`` strips the
SEC XSL-rendering prefix so the fetch returns raw XML (not
XSL-transformed HTML). Applied here so the manifest's
``primary_document_url`` — which may be the XSL-rendered URL from
Atom discovery — gets normalised before the fetch.

Share-class fan-out: legacy ``upsert_filing`` /
``upsert_form_3_filing`` ALREADY fan out across siblings via
``siblings_for_issuer_cik`` internally — the manifest parser does
NOT need to repeat the loop.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg

from app.config import settings
from app.providers.implementations.sec_edgar import SecFilingsProvider
from app.services.insider_form3_ingest import (
    _FORM3_PARSER_VERSION,
    _write_form_3_tombstone,
    upsert_form_3_filing,
)
from app.services.insider_transactions import (
    _PARSER_VERSION_FORM4,
    _canonical_form_4_url,
    _write_tombstone,
    parse_form_3_xml,
    parse_form_4_xml,
    upsert_filing,
)
from app.services.raw_filings import store_raw

logger = logging.getLogger(__name__)

_FAILED_RETRY_DELAY = timedelta(hours=1)


_FORM3_PARSER_VERSION_STR = f"form3-v{_FORM3_PARSER_VERSION}"


def _failed_outcome(error: str, *, parser_version: str, raw_status: Any = None) -> Any:
    """Build a ``failed`` ParseOutcome with a 1h backoff applied.

    Pattern mirrors the other per-source adapters — see eight_k.py
    for the rationale on duplicating the literal instead of importing
    the worker's private ``_backoff_for(0)``."""
    from app.jobs.sec_manifest_worker import ParseOutcome

    return ParseOutcome(
        status="failed",
        parser_version=parser_version,
        raw_status=raw_status,
        error=error,
        next_retry_at=datetime.now(tz=UTC) + _FAILED_RETRY_DELAY,
    )


def _parse_form4(
    conn: psycopg.Connection[Any],
    row: Any,  # ManifestRow
) -> Any:  # ParseOutcome
    """Manifest-worker parser for one Form 4 / 4/A accession."""
    from app.jobs.sec_manifest_worker import ParseOutcome

    accession = row.accession_number
    instrument_id = row.instrument_id
    url = row.primary_document_url

    if instrument_id is None:
        logger.warning(
            "form4 manifest parser: accession=%s has no instrument_id; tombstoning",
            accession,
        )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_FORM4,
            error="missing instrument_id",
        )
    if not url:
        logger.warning(
            "form4 manifest parser: accession=%s has no primary_document_url; tombstoning",
            accession,
        )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_FORM4,
            error="missing primary_document_url",
        )

    canonical_url = _canonical_form_4_url(url)

    try:
        with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider:
            xml = provider.fetch_document_text(canonical_url)
    except Exception as exc:  # noqa: BLE001 — transient retries via 1h backoff
        logger.warning(
            "form4 manifest parser: fetch raised accession=%s url=%s: %s",
            accession,
            canonical_url,
            exc,
        )
        return _failed_outcome(f"fetch error: {exc}", parser_version=_PARSER_VERSION_FORM4)

    if not xml:
        # Empty / non-200. Legacy tombstones the row in insider_filings;
        # mirror so dashboard counts match. Savepoint isolates the
        # tombstone write from the outer worker tx.
        try:
            with conn.transaction():
                _write_tombstone(
                    conn,
                    instrument_id=instrument_id,
                    accession_number=accession,
                    primary_document_url=canonical_url,
                )
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "form4 manifest parser: tombstone INSERT failed accession=%s",
                accession,
            )
            return _failed_outcome(f"tombstone error: {exc}", parser_version=_PARSER_VERSION_FORM4)
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_FORM4,
            error="empty or non-200 fetch",
        )

    try:
        with conn.transaction():
            store_raw(
                conn,
                accession_number=accession,
                document_kind="form4_xml",
                payload=xml,
                parser_version=_PARSER_VERSION_FORM4,
                source_url=canonical_url,
            )
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "form4 manifest parser: store_raw failed accession=%s",
            accession,
        )
        return _failed_outcome(f"store_raw error: {exc}", parser_version=_PARSER_VERSION_FORM4)

    # Parse-phase exceptions AFTER store_raw must return
    # raw_status='stored' so the manifest matches filing_raw_documents
    # state. One broad-except block writes the same tombstone path
    # regardless of exception class — see review-prevention-log.md
    # entry "Manifest parser parse-failure branch must write ingest-log
    # on EVERY exception class" (PR #1129).
    try:
        parsed = parse_form_4_xml(xml)
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "form4 manifest parser: parse raised accession=%s",
            accession,
        )
        try:
            with conn.transaction():
                _write_tombstone(
                    conn,
                    instrument_id=instrument_id,
                    accession_number=accession,
                    primary_document_url=canonical_url,
                )
        except Exception:  # noqa: BLE001
            logger.exception(
                "form4 manifest parser: tombstone INSERT failed after parse error accession=%s",
                accession,
            )
        return _failed_outcome(f"parse error: {exc}", parser_version=_PARSER_VERSION_FORM4, raw_status="stored")

    if parsed is None:
        try:
            with conn.transaction():
                _write_tombstone(
                    conn,
                    instrument_id=instrument_id,
                    accession_number=accession,
                    primary_document_url=canonical_url,
                )
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "form4 manifest parser: tombstone INSERT failed accession=%s",
                accession,
            )
            return _failed_outcome(f"tombstone error: {exc}", parser_version=_PARSER_VERSION_FORM4, raw_status="stored")
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_FORM4,
            raw_status="stored",
            error="parser returned None (malformed XML or missing required fields)",
        )

    # upsert_filing handles siblings fan-out + observation
    # write-through + refresh_insiders_current internally. Single
    # savepoint covers every child write so a mid-batch failure rolls
    # back atomically across share-class siblings.
    try:
        with conn.transaction():
            upsert_filing(
                conn,
                instrument_id=instrument_id,
                accession_number=accession,
                primary_document_url=canonical_url,
                parsed=parsed,
            )
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "form4 manifest parser: upsert failed accession=%s",
            accession,
        )
        return _failed_outcome(f"upsert error: {exc}", parser_version=_PARSER_VERSION_FORM4, raw_status="stored")

    return ParseOutcome(
        status="parsed",
        parser_version=_PARSER_VERSION_FORM4,
        raw_status="stored",
    )


def _parse_form3(
    conn: psycopg.Connection[Any],
    row: Any,  # ManifestRow
) -> Any:  # ParseOutcome
    """Manifest-worker parser for one Form 3 / 3/A accession.

    Same shape as ``_parse_form4`` but routes through
    ``parse_form_3_xml`` + ``upsert_form_3_filing``. Form 3 is the
    insider's initial-statement filing — first-time-named officers,
    directors, 10%+ owners — whose holdings populate the
    ``insider_initial_holdings`` table that backstops the cumulative
    insider-cohort view when no transactions exist yet.
    """
    from app.jobs.sec_manifest_worker import ParseOutcome

    accession = row.accession_number
    instrument_id = row.instrument_id
    url = row.primary_document_url

    if instrument_id is None:
        logger.warning(
            "form3 manifest parser: accession=%s has no instrument_id; tombstoning",
            accession,
        )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_FORM3_PARSER_VERSION_STR,
            error="missing instrument_id",
        )
    if not url:
        logger.warning(
            "form3 manifest parser: accession=%s has no primary_document_url; tombstoning",
            accession,
        )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_FORM3_PARSER_VERSION_STR,
            error="missing primary_document_url",
        )

    canonical_url = _canonical_form_4_url(url)

    try:
        with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider:
            xml = provider.fetch_document_text(canonical_url)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "form3 manifest parser: fetch raised accession=%s url=%s: %s",
            accession,
            canonical_url,
            exc,
        )
        return _failed_outcome(f"fetch error: {exc}", parser_version=_FORM3_PARSER_VERSION_STR)

    if not xml:
        try:
            with conn.transaction():
                _write_form_3_tombstone(
                    conn,
                    instrument_id=instrument_id,
                    accession_number=accession,
                    primary_document_url=canonical_url,
                )
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "form3 manifest parser: tombstone INSERT failed accession=%s",
                accession,
            )
            return _failed_outcome(f"tombstone error: {exc}", parser_version=_FORM3_PARSER_VERSION_STR)
        return ParseOutcome(
            status="tombstoned",
            parser_version=_FORM3_PARSER_VERSION_STR,
            error="empty or non-200 fetch",
        )

    try:
        with conn.transaction():
            store_raw(
                conn,
                accession_number=accession,
                document_kind="form3_xml",
                payload=xml,
                parser_version=_FORM3_PARSER_VERSION_STR,
                source_url=canonical_url,
            )
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "form3 manifest parser: store_raw failed accession=%s",
            accession,
        )
        return _failed_outcome(f"store_raw error: {exc}", parser_version=_FORM3_PARSER_VERSION_STR)

    try:
        parsed = parse_form_3_xml(xml)
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "form3 manifest parser: parse raised accession=%s",
            accession,
        )
        try:
            with conn.transaction():
                _write_form_3_tombstone(
                    conn,
                    instrument_id=instrument_id,
                    accession_number=accession,
                    primary_document_url=canonical_url,
                )
        except Exception:  # noqa: BLE001
            logger.exception(
                "form3 manifest parser: tombstone INSERT failed after parse error accession=%s",
                accession,
            )
        return _failed_outcome(f"parse error: {exc}", parser_version=_FORM3_PARSER_VERSION_STR, raw_status="stored")

    if parsed is None:
        try:
            with conn.transaction():
                _write_form_3_tombstone(
                    conn,
                    instrument_id=instrument_id,
                    accession_number=accession,
                    primary_document_url=canonical_url,
                )
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "form3 manifest parser: tombstone INSERT failed accession=%s",
                accession,
            )
            return _failed_outcome(
                f"tombstone error: {exc}", parser_version=_FORM3_PARSER_VERSION_STR, raw_status="stored"
            )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_FORM3_PARSER_VERSION_STR,
            raw_status="stored",
            error="parser returned None (malformed XML or missing required fields)",
        )

    try:
        with conn.transaction():
            upsert_form_3_filing(
                conn,
                instrument_id=instrument_id,
                accession_number=accession,
                primary_document_url=canonical_url,
                parsed=parsed,
            )
    except Exception as exc:  # noqa: BLE001
        # Legacy Form 3 ingester (insider_form3_ingest.py:685) tombstones
        # on upsert failure so a deterministic constraint violation
        # doesn't loop the scheduler refetching the same dead XML on
        # every tick. Mirror that policy here: write the tombstone in a
        # fresh savepoint, then transition the manifest row to
        # ``tombstoned`` (not ``failed``) so the worker doesn't schedule
        # a 1h retry on a deterministic bug. A parser-version bump will
        # re-pick the accession via the existing manifest rewash path.
        logger.exception(
            "form3 manifest parser: upsert failed accession=%s; tombstoning per legacy parity",
            accession,
        )
        try:
            with conn.transaction():
                _write_form_3_tombstone(
                    conn,
                    instrument_id=instrument_id,
                    accession_number=accession,
                    primary_document_url=canonical_url,
                )
        except Exception:  # noqa: BLE001 — tombstone failure shouldn't mask upsert failure
            logger.exception(
                "form3 manifest parser: tombstone INSERT failed after upsert error accession=%s",
                accession,
            )
            return _failed_outcome(
                f"upsert+tombstone error: {exc}",
                parser_version=_FORM3_PARSER_VERSION_STR,
                raw_status="stored",
            )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_FORM3_PARSER_VERSION_STR,
            raw_status="stored",
            error=f"upsert failed: {exc}",
        )

    return ParseOutcome(
        status="parsed",
        parser_version=_FORM3_PARSER_VERSION_STR,
        raw_status="stored",
    )


def register() -> None:
    """Register Form 3 + Form 4 parsers with the manifest worker.

    Form 5 is intentionally NOT registered — the legacy ingester
    has no Form 5 parser; Form 5 manifest rows continue to skip
    until upstream support lands.
    """
    from app.jobs.sec_manifest_worker import register_parser

    register_parser("sec_form4", _parse_form4, requires_raw_payload=True)
    register_parser("sec_form3", _parse_form3, requires_raw_payload=True)
