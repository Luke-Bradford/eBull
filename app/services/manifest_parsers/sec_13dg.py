"""SEC SC 13D / 13G manifest-worker parser adapter (#873).

Wraps the existing ``parse_primary_doc`` + ``blockholders`` helpers
into the manifest-worker ``ParserFn`` contract. Same callable is
registered against BOTH ``sec_13d`` and ``sec_13g`` since one XML
parser handles both schemas — the parsed ``submission_type`` field
disambiguates downstream.

ParseOutcome contract (see ``sec_manifest_worker.ParserSpec``):

  * ``status='parsed'`` + ``raw_status='stored'`` — primary_doc.xml
    persisted; ``blockholder_filings`` rows upserted (one per
    reporting person on the cover page); ``blockholder_filings_ingest_log``
    records ``success`` or ``partial``; if issuer CUSIP resolved,
    ``ownership_blockholders_observations`` + ``_current`` refresh.
  * ``status='tombstoned'`` — fetch returned non-200/empty body. The
    legacy ``ingest_filer_blockholders`` returns status='failed' for
    this case; manifest path treats persistently-404 primary docs as
    tombstones so the row doesn't spin retry forever.
  * ``status='failed'`` — transient error (fetch raise, store_raw
    error, upsert error). Worker schedules a 1h backoff retry.

Raw-payload invariant (#938): registered with
``requires_raw_payload=True``. ``store_raw`` runs in a savepoint
BEFORE parse so the invariant holds whether parsing succeeds or
raises.

Subject identity: 13D/G manifest rows have ``subject_type='blockholder_filer'``
+ ``subject_id=filer_cik`` (per spec §I10 + sec_filing_manifest CHECK).
``instrument_id`` is NULL on the manifest row — issuer linkage is
resolved at parse-time via CUSIP→instrument lookup in
``external_identifiers``. CUSIP-unresolved accessions still write
``blockholder_filings`` rows (with NULL instrument_id) — the audit
trail is preserved even when the rollup join is gated by #740
backfill coverage.

URL construction: the canonical ``primary_doc.xml`` URL is built
from ``cik + accession_number`` via ``_archive_file_url`` rather
than read from ``row.primary_document_url``. The manifest's URL
may be the filing-index page from Atom discovery or a sibling
attachment — only the canonical archive URL guarantees XML.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET  # noqa: S405 — only ET.ParseError caught; no untrusted parse.
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import psycopg

from app.config import settings
from app.providers.implementations.sec_13dg import (
    BlockholderFiling,
    parse_primary_doc,
)
from app.providers.implementations.sec_edgar import SecFilingsProvider
from app.services.blockholders import (
    _PARSER_VERSION_13DG,
    AccessionRef,
    _archive_file_url,
    _record_13dg_observation_for_filing,
    _record_ingest_attempt,
    _resolve_cusip_to_instrument_id,
    _upsert_filer,
    _upsert_filing_row,
)
from app.services.manifest_parsers._classify import (
    format_upsert_error,
    is_transient_upsert_error,
)
from app.services.ownership_observations import refresh_blockholders_current
from app.services.raw_filings import store_raw

logger = logging.getLogger(__name__)

_FAILED_RETRY_DELAY = timedelta(hours=1)


def _failed_outcome(error: str, raw_status: Any = None) -> Any:
    """Build a ``failed`` ParseOutcome with a 1h backoff applied."""
    from app.jobs.sec_manifest_worker import ParseOutcome

    return ParseOutcome(
        status="failed",
        parser_version=_PARSER_VERSION_13DG,
        raw_status=raw_status,
        error=error,
        next_retry_at=datetime.now(tz=UTC) + _FAILED_RETRY_DELAY,
    )


def _parse_13dg(
    conn: psycopg.Connection[Any],
    row: Any,  # ManifestRow — forward-ref to avoid circular import
) -> Any:  # ParseOutcome — forward-ref
    """Manifest-worker parser for one SC 13D / 13G accession."""
    from app.jobs.sec_manifest_worker import ParseOutcome

    accession = row.accession_number
    filer_cik = (row.cik or "").strip()

    if not filer_cik:
        logger.warning(
            "13D/G manifest parser: accession=%s has no filer cik; tombstoning",
            accession,
        )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_13DG,
            error="missing filer cik",
        )

    # Build canonical primary_doc.xml URL — manifest's
    # primary_document_url may be the filing-index page or a sibling
    # attachment. The legacy ingester builds this URL directly from
    # cik+accession; mirror that so the manifest path doesn't fetch
    # an HTML wrapper and immediately tombstone-on-parse.
    primary_url = _archive_file_url(filer_cik, accession, "primary_doc.xml")

    try:
        with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider:
            primary_xml = provider.fetch_document_text(primary_url)
    except Exception as exc:  # noqa: BLE001 — transient fetch errors retry via backoff
        logger.warning(
            "13D/G manifest parser: fetch raised accession=%s url=%s: %s",
            accession,
            primary_url,
            exc,
        )
        return _failed_outcome(f"fetch error: {exc}")

    if not primary_xml:
        # Empty / non-200. Log the attempt with status='failed' to
        # match legacy accounting; manifest itself tombstones so we
        # don't spin retry on a persistently-404 doc.
        try:
            with conn.transaction():
                _record_ingest_attempt(
                    conn,
                    filer_cik=filer_cik,
                    accession_number=accession,
                    submission_type=None,
                    status="failed",
                    error="primary_doc.xml fetch returned empty or non-200",
                )
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "13D/G manifest parser: ingest-log INSERT failed accession=%s",
                accession,
            )
            return _failed_outcome(f"log error: {exc}")
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_13DG,
            error="empty or non-200 fetch",
        )

    # store_raw in a savepoint so the worker's outer tx stays clean
    # on partial failure. Raw body persisted BEFORE parse so #938
    # invariant holds even when downstream raises.
    try:
        with conn.transaction():
            store_raw(
                conn,
                accession_number=accession,
                document_kind="primary_doc_13dg",
                payload=primary_xml,
                parser_version=_PARSER_VERSION_13DG,
                source_url=primary_url,
            )
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "13D/G manifest parser: store_raw failed accession=%s",
            accession,
        )
        return _failed_outcome(f"store_raw error: {exc}")

    # Parse-phase errors AFTER store_raw must return
    # raw_status='stored' so the manifest matches filing_raw_documents.
    # ET.ParseError covers malformed XML; ValueError covers schema
    # errors (missing field, unknown submissionType); broader Exception
    # covers unexpected raises (e.g. AttributeError in a tag walker).
    # Every parse-failure branch writes an ingest-log row so the audit
    # trail is consistent regardless of which exception type fires
    # (#1129 review WARNING + PREVENTION).
    try:
        filing: BlockholderFiling = parse_primary_doc(primary_xml)
    except Exception as exc:  # noqa: BLE001 — broad catch + audit-log write
        # Tag the error string by exception class so operators reading
        # blockholder_filings_ingest_log can distinguish expected
        # schema/parse failures from unexpected parser crashes.
        is_unexpected = not isinstance(exc, (ValueError, ET.ParseError))
        kind = "parse error (unexpected)" if is_unexpected else "parse error"
        logger.exception(
            "13D/G manifest parser: parse raised accession=%s (unexpected=%s)",
            accession,
            is_unexpected,
        )
        try:
            with conn.transaction():
                _record_ingest_attempt(
                    conn,
                    filer_cik=filer_cik,
                    accession_number=accession,
                    submission_type=None,
                    status="failed",
                    error=f"{kind}: {exc}",
                )
        except Exception:  # noqa: BLE001 — log failure shouldn't mask parse failure
            logger.exception(
                "13D/G manifest parser: ingest-log INSERT failed after parse error accession=%s",
                accession,
            )
        return _failed_outcome(f"{kind}: {exc}", raw_status="stored")

    # Resolve the filer's canonical name — pure-Python, no DB.
    filer_name = next(
        (p.name for p in filing.reporting_persons if p.cik == filing.primary_filer_cik),
        filing.reporting_persons[0].name if filing.reporting_persons else f"CIK {filing.primary_filer_cik}",
    )

    ref = AccessionRef(
        accession_number=accession,
        filing_type=filing.submission_type,
        filed_at=row.filed_at,
    )

    # CUSIP lookup is a DB SELECT — keep inside the same try that
    # returns ``_failed_outcome(raw_status='stored')`` so a DB error
    # doesn't escape to the worker's generic exception handler and
    # lose the manifest's view of the stored raw row (Codex pre-push
    # finding). Skipping the observation write-through is conditional
    # on instrument_id resolving non-NULL — the upsert path still
    # writes ``blockholder_filings`` rows for auditability.
    inserted = 0
    try:
        with conn.transaction():
            instrument_id = _resolve_cusip_to_instrument_id(conn, filing.issuer_cusip)
            skipped_no_cusip = 0 if instrument_id is not None else len(filing.reporting_persons)
            filer_id = _upsert_filer(conn, cik=filing.primary_filer_cik, name=filer_name)
            for person in filing.reporting_persons:
                if _upsert_filing_row(
                    conn,
                    filer_id=filer_id,
                    accession_number=accession,
                    submission_type=filing.submission_type,
                    status=filing.status,
                    instrument_id=instrument_id,
                    issuer_cik=filing.issuer_cik,
                    issuer_cusip=filing.issuer_cusip,
                    securities_class_title=filing.securities_class_title,
                    date_of_event=filing.date_of_event,
                    filed_at=filing.filed_at or ref.filed_at,
                    person=person,
                ):
                    inserted += 1

            if instrument_id is not None:
                _record_13dg_observation_for_filing(
                    conn,
                    instrument_id=instrument_id,
                    accession_number=accession,
                    primary_document_url=primary_url,
                    filing=filing,
                    filer_name=filer_name,
                    ref=ref,
                    run_id=uuid4(),  # one-shot per accession; no batch run_id concept on manifest path
                )
                refresh_blockholders_current(conn, instrument_id=instrument_id)

            log_status = "success" if instrument_id is not None else "partial"
            log_error = (
                None
                if instrument_id is not None
                else f"issuer CUSIP {filing.issuer_cusip!r} unresolved (gated by #740 backfill)"
            )
            _record_ingest_attempt(
                conn,
                filer_cik=filer_cik,
                accession_number=accession,
                submission_type=filing.submission_type,
                status=log_status,
                rows_inserted=inserted,
                rows_skipped=skipped_no_cusip,
                error=log_error,
            )
    except Exception as exc:  # noqa: BLE001
        # #1131 transient-vs-deterministic discrimination — see
        # ``_classify.is_transient_upsert_error`` for the policy.
        # Transient (OperationalError) gets a 1h retry; deterministic
        # constraint violations tombstone the manifest so a permanently
        # broken accession stops re-fetching from SEC every tick.
        # Tombstone for 13D/G means writing the ingest-log attempt with
        # status='failed' (mirrors the empty-body branch above) +
        # returning manifest ``tombstoned``.
        logger.exception(
            "13D/G manifest parser: upsert/observation batch failed accession=%s",
            accession,
        )
        if is_transient_upsert_error(exc):
            return _failed_outcome(format_upsert_error(exc), raw_status="stored")
        try:
            with conn.transaction():
                _record_ingest_attempt(
                    conn,
                    filer_cik=filer_cik,
                    accession_number=accession,
                    submission_type=None,
                    status="failed",
                    error=format_upsert_error(exc),
                )
        except Exception:  # noqa: BLE001 — ingest-log failure shouldn't mask upsert failure
            logger.exception(
                "13D/G manifest parser: ingest-log INSERT failed after upsert error accession=%s",
                accession,
            )
            return _failed_outcome(
                f"upsert+log error: {type(exc).__name__}: {exc}",
                raw_status="stored",
            )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_13DG,
            raw_status="stored",
            error=format_upsert_error(exc),
        )

    return ParseOutcome(
        status="parsed",
        parser_version=_PARSER_VERSION_13DG,
        raw_status="stored",
    )


def register() -> None:
    """Register the 13D/G parser under BOTH manifest sources.

    Idempotent — last-write-wins. One callable handles both schemas
    (parse_primary_doc dispatches on submissionType inside the XML)
    so the manifest dispatcher need only know the source key.
    """
    from app.jobs.sec_manifest_worker import register_parser

    register_parser("sec_13d", _parse_13dg, requires_raw_payload=True)
    register_parser("sec_13g", _parse_13dg, requires_raw_payload=True)
