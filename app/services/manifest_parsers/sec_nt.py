"""Manifest-worker parser for SEC Form 12b-25 (NT 10-K / NT 10-Q).

Issue #1015 item 1. Upgrades NT 10-K / NT 10-Q from metadata-only to PARSE+RAW
under the new manifest source ``sec_nt``. Mirrors ``eight_k.py``: fetch the
primary document, persist it as ``nt_body`` in ``filing_raw_documents`` (the
#938 'parsed implies raw stored' invariant), then run the pure extractor
(``app.services.nt_notices.parse_nt_notice``) and upsert ``nt_filing_notices``.

All field logic lives in ``nt_notices`` (single-chokepoint discipline); this
module only orchestrates fetch / store / transition.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg

from app.config import settings
from app.providers.implementations.sec_edgar import SecFilingsProvider
from app.services.nt_notices import (
    PARSER_VERSION as _NT_PARSER_VERSION,
)
from app.services.nt_notices import (
    parse_nt_notice,
    upsert_nt_notice,
)
from app.services.raw_filings import store_raw

logger = logging.getLogger(__name__)

_FAILED_RETRY_DELAY = timedelta(hours=1)

# ParseOutcome.parser_version and store_raw.parser_version are TEXT; the
# nt_filing_notices.parser_version column is INT (set by upsert_nt_notice).
_PARSER_VERSION_NT = str(_NT_PARSER_VERSION)

# Subject form derived from the manifest form code (authoritative — see spec).
_FORM_TO_LATE_FORM: dict[str, str] = {
    "NT 10-K": "10-K",
    "NT 10-Q": "10-Q",
}


def _failed_outcome(error: str, raw_status: Any = None) -> Any:
    """Build a ``failed`` ParseOutcome with a 1h backoff (mirrors eight_k)."""
    from app.jobs.sec_manifest_worker import ParseOutcome

    return ParseOutcome(
        status="failed",
        parser_version=_PARSER_VERSION_NT,
        raw_status=raw_status,
        error=error,
        next_retry_at=datetime.now(tz=UTC) + _FAILED_RETRY_DELAY,
    )


def _parse_nt(
    conn: psycopg.Connection[Any],
    row: Any,  # ManifestRow — forward-ref to avoid circular import
) -> Any:  # ParseOutcome — forward-ref
    """Parser for one NT 10-K / NT 10-Q accession."""
    from app.jobs.sec_manifest_worker import ParseOutcome

    accession = row.accession_number
    url = row.primary_document_url
    instrument_id = row.instrument_id

    late_form = _FORM_TO_LATE_FORM.get(row.form.strip())
    if late_form is None:
        # The manifest should only route NT 10-K / NT 10-Q here
        # (_FORM_TO_SOURCE). Anything else is an upstream misroute.
        logger.warning(
            "sec_nt parser: accession=%s unexpected form=%r; tombstoning",
            accession,
            row.form,
        )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_NT,
            error=f"unexpected form {row.form!r}",
        )
    if not url:
        logger.warning(
            "sec_nt parser: accession=%s has no primary_document_url; tombstoning",
            accession,
        )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_NT,
            error="missing primary_document_url",
        )
    if instrument_id is None:
        logger.warning(
            "sec_nt parser: accession=%s has no instrument_id; tombstoning",
            accession,
        )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_NT,
            error="missing instrument_id",
        )

    try:
        with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider:
            body = provider.fetch_document_text(url)
    except Exception as exc:  # noqa: BLE001 — transient fetch errors retry via worker backoff
        logger.warning(
            "sec_nt parser: fetch raised accession=%s url=%s: %s",
            accession,
            url,
            exc,
        )
        return _failed_outcome(f"fetch error: {exc}")

    if not body:
        # Non-200 or empty body — tombstone (no raw to store).
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_NT,
            error="empty or non-200 fetch",
        )

    # Persist raw BEFORE parse so the #938 invariant holds even if the parse /
    # upsert later raises and the worker retries. Savepoint protects the
    # worker's outer transaction.
    try:
        with conn.transaction():
            store_raw(
                conn,
                accession_number=accession,
                document_kind="nt_body",
                payload=body,
                parser_version=str(_PARSER_VERSION_NT),
                source_url=url,
            )
    except Exception as exc:  # noqa: BLE001
        logger.exception("sec_nt parser: store_raw failed accession=%s", accession)
        return _failed_outcome(f"store_raw error: {exc}")

    # Raw is now stored — every subsequent outcome MUST carry
    # raw_status='stored' so the manifest's view matches the raw table.
    try:
        notice = parse_nt_notice(body, late_form)
    except Exception as exc:  # noqa: BLE001
        logger.exception("sec_nt parser: parse raised accession=%s", accession)
        return _failed_outcome(f"parse error: {exc}", raw_status="stored")

    if notice is None:
        # Not a recognizable Form 12b-25 (no 12b-25 marker) — tombstone, but
        # raw IS stored.
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_NT,
            raw_status="stored",
            error="not a recognizable Form 12b-25",
        )

    try:
        with conn.transaction():
            upsert_nt_notice(
                conn,
                instrument_id=instrument_id,
                accession_number=accession,
                notice=notice,
            )
    except Exception as exc:  # noqa: BLE001
        logger.exception("sec_nt parser: upsert failed accession=%s", accession)
        return _failed_outcome(f"upsert error: {exc}", raw_status="stored")

    return ParseOutcome(
        status="parsed",
        parser_version=_PARSER_VERSION_NT,
        raw_status="stored",
    )


def _nt_fetch_url(conn: Any, row: Any) -> str | None:  # conn unused; row: ManifestRow
    """#1591 prefetch hook — the single primary-document URL the NT parser
    GETs, returned only when the parser would actually fetch it (mirrors
    ``_parse_nt``'s pre-fetch tombstone gates). Both gates are row-local so
    ``conn`` is unused. ``nt_body`` is small and retained, but the parser
    always re-fetches (no reuse gate), so an over-broad ``None`` is safe.
    """
    if row.form.strip() not in _FORM_TO_LATE_FORM:
        return None
    url = row.primary_document_url
    if not url or row.instrument_id is None:
        return None
    return url


def register() -> None:
    """Register the NT parser with the manifest worker (idempotent)."""
    from app.jobs.sec_manifest_worker import register_parser

    register_parser("sec_nt", _parse_nt, requires_raw_payload=True, fetch_url=_nt_fetch_url)
