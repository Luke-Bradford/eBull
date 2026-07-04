"""Manifest-worker parser for SEC PRE 14A / PRER14A (Issue #1892, #1015 item 3).

Upgrades PRE 14A / PRER14A from metadata-only to PARSE+RAW under the new
manifest source ``sec_pre14a``. Mirrors ``sec_nt.py``: fetch the primary
document, persist it as ``pre14a_body`` in ``filing_raw_documents`` (the #938
'parsed implies raw stored' invariant), then run the pure extractor
(``app.services.pre14a_proposals.parse_pre14a_proposals``) and upsert
``pre14a_proposal_signals``.

Does NOT touch ``sec_def14a`` — #1320's ownership-pipeline concern (PRE 14A
drafts never counted for ownership) is fully preserved; this is a wholly
separate source/table for a different (proposal-signal) purpose.

All field logic lives in ``pre14a_proposals`` (single-chokepoint discipline);
this module only orchestrates fetch / store / transition.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg

from app.config import settings
from app.providers.implementations.sec_edgar import SecFilingsProvider
from app.services.pre14a_proposals import (
    PARSER_VERSION as _PRE14A_PARSER_VERSION,
)
from app.services.pre14a_proposals import (
    parse_pre14a_proposals,
    upsert_pre14a_proposal_signal,
)
from app.services.raw_filings import store_raw

logger = logging.getLogger(__name__)

_FAILED_RETRY_DELAY = timedelta(hours=1)

_PARSER_VERSION_PRE14A = str(_PRE14A_PARSER_VERSION)

_ROUTED_FORMS = frozenset({"PRE 14A", "PRER14A"})


def _failed_outcome(error: str, raw_status: Any = None) -> Any:
    """Build a ``failed`` ParseOutcome with a 1h backoff (mirrors sec_nt)."""
    from app.jobs.sec_manifest_worker import ParseOutcome

    return ParseOutcome(
        status="failed",
        parser_version=_PARSER_VERSION_PRE14A,
        raw_status=raw_status,
        error=error,
        next_retry_at=datetime.now(tz=UTC) + _FAILED_RETRY_DELAY,
    )


def _parse_pre14a(
    conn: psycopg.Connection[Any],
    row: Any,  # ManifestRow — forward-ref to avoid circular import
) -> Any:  # ParseOutcome — forward-ref
    """Parser for one PRE 14A / PRER14A accession."""
    from app.jobs.sec_manifest_worker import ParseOutcome

    accession = row.accession_number
    url = row.primary_document_url
    instrument_id = row.instrument_id

    if row.form.strip() not in _ROUTED_FORMS:
        # The manifest should only route PRE 14A / PRER14A here
        # (_FORM_TO_SOURCE). Anything else is an upstream misroute.
        logger.warning(
            "sec_pre14a parser: accession=%s unexpected form=%r; tombstoning",
            accession,
            row.form,
        )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_PRE14A,
            error=f"unexpected form {row.form!r}",
        )
    if not url:
        logger.warning(
            "sec_pre14a parser: accession=%s has no primary_document_url; tombstoning",
            accession,
        )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_PRE14A,
            error="missing primary_document_url",
        )
    if instrument_id is None:
        logger.warning(
            "sec_pre14a parser: accession=%s has no instrument_id; tombstoning",
            accession,
        )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_PRE14A,
            error="missing instrument_id",
        )

    try:
        with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider:
            body = provider.fetch_document_text(url)
    except Exception as exc:  # noqa: BLE001 — transient fetch errors retry via worker backoff
        logger.warning(
            "sec_pre14a parser: fetch raised accession=%s url=%s: %s",
            accession,
            url,
            exc,
        )
        return _failed_outcome(f"fetch error: {exc}")

    if not body:
        # Non-200 or empty body — tombstone (no raw to store).
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_PRE14A,
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
                document_kind="pre14a_body",
                payload=body,
                parser_version=str(_PARSER_VERSION_PRE14A),
                source_url=url,
            )
    except Exception as exc:  # noqa: BLE001
        logger.exception("sec_pre14a parser: store_raw failed accession=%s", accession)
        return _failed_outcome(f"store_raw error: {exc}")

    # Raw is now stored — every subsequent outcome MUST carry
    # raw_status='stored' so the manifest's view matches the raw table.
    try:
        signal = parse_pre14a_proposals(body)
    except Exception as exc:  # noqa: BLE001
        logger.exception("sec_pre14a parser: parse raised accession=%s", accession)
        return _failed_outcome(f"parse error: {exc}", raw_status="stored")

    if signal is None:
        # No recognizable Rule 14a-4(a)(3) numbered proposals list — tombstone,
        # but raw IS stored.
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_PRE14A,
            raw_status="stored",
            error="no recognizable numbered proposals list",
        )

    try:
        with conn.transaction():
            upsert_pre14a_proposal_signal(
                conn,
                instrument_id=instrument_id,
                accession_number=accession,
                signal=signal,
            )
    except Exception as exc:  # noqa: BLE001
        logger.exception("sec_pre14a parser: upsert failed accession=%s", accession)
        return _failed_outcome(f"upsert error: {exc}", raw_status="stored")

    return ParseOutcome(
        status="parsed",
        parser_version=_PARSER_VERSION_PRE14A,
        raw_status="stored",
    )


def _pre14a_fetch_url(conn: Any, row: Any) -> str | None:  # conn unused; row: ManifestRow
    """#1591 prefetch hook — the single primary-document URL the PRE 14A
    parser GETs, returned only when the parser would actually fetch it
    (mirrors ``_parse_pre14a``'s pre-fetch tombstone gates). Both gates are
    row-local so ``conn`` is unused.
    """
    if row.form.strip() not in _ROUTED_FORMS:
        return None
    url = row.primary_document_url
    if not url or row.instrument_id is None:
        return None
    return url


def register() -> None:
    """Register the PRE 14A parser with the manifest worker (idempotent)."""
    from app.jobs.sec_manifest_worker import register_parser

    register_parser("sec_pre14a", _parse_pre14a, requires_raw_payload=True, fetch_url=_pre14a_fetch_url)
