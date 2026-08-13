"""Manifest-worker parser for SEC tender / going-private schedules.

Issue #1982 (child of #1015 item 4). Upgrades SC TO-T / SC TO-I / SC 14D9 /
SC 13E3 (+ /A) from metadata-only to PARSE+RAW under the new manifest source
``sec_tender``. Mirrors ``sec_424b.py``: fetch the primary document, persist
it as ``tender_body`` in ``filing_raw_documents`` (born-compacted — swept
kind, sha only), fetch the EDGAR SGML header (``<acc>.hdr.sgml``, same archive
directory as the primary document — verified live 2026-07-05), then run the
pure extractor (``app.services.tender_offers``) and upsert
``tender_offer_events``.

The header fetch is the load-bearing step: ``sec_filing_manifest`` is keyed on
accession alone, so a dual-party accession (subject + offeror both in
universe) has ONE manifest row whose ``instrument_id`` is arbitrary between
the parties (last-discovery-wins). The parser derives roles for BOTH parties
from the header's SUBJECT-COMPANY / FILED-BY CIK blocks and writes one typed
row per matched instrument — independent of which party owns the manifest row.

All field logic lives in ``tender_offers`` (single-chokepoint discipline);
this module only orchestrates fetch / store / transition.
"""

from __future__ import annotations

import logging
import posixpath
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import psycopg

from app.config import settings
from app.providers.implementations.sec_edgar import SecFilingsProvider
from app.services.raw_filings import store_raw
from app.services.tender_offers import (
    IN_SCOPE_FORMS,
    delete_tender_offer_events,
    map_ciks_to_instruments,
    parse_tender_offer,
    resolve_party_roles,
    upsert_tender_offer_events,
)
from app.services.tender_offers import (
    PARSER_VERSION as _TENDER_PARSER_VERSION,
)

logger = logging.getLogger(__name__)

_FAILED_RETRY_DELAY = timedelta(hours=1)

# ParseOutcome.parser_version and store_raw.parser_version are TEXT; the
# tender_offer_events.parser_version column is INT (set by the upsert).
_PARSER_VERSION_TENDER = str(_TENDER_PARSER_VERSION)


def _failed_outcome(error: str, raw_status: Any = None) -> Any:
    """Build a ``failed`` ParseOutcome with a 1h backoff (mirrors sec_424b)."""
    from app.jobs.sec_manifest_worker import ParseOutcome

    return ParseOutcome(
        status="failed",
        parser_version=_PARSER_VERSION_TENDER,
        raw_status=raw_status,
        error=error,
        next_retry_at=datetime.now(tz=UTC) + _FAILED_RETRY_DELAY,
    )


def _tombstone_after_store(conn: psycopg.Connection[Any], accession: str, error: str) -> Any:
    """Tombstone a row whose raw is already stored (#938 ``raw_status='stored'``),
    first reconciling away any typed rows a PRIOR parse wrote for the accession.
    The API join is on ``(accession, instrument_id)``, so a later parse that
    resolves nothing (unusable header / no in-universe party) must not leave
    stale rows rendering. A cleanup failure fails(retry) rather than
    tombstoning over stale data."""
    from app.jobs.sec_manifest_worker import ParseOutcome

    try:
        with conn.transaction():
            delete_tender_offer_events(conn, accession)
    except Exception as exc:  # noqa: BLE001
        logger.exception("sec_tender parser: tombstone cleanup failed accession=%s", accession)
        return _failed_outcome(f"tombstone cleanup error: {exc}", raw_status="stored")
    return ParseOutcome(
        status="tombstoned",
        parser_version=_PARSER_VERSION_TENDER,
        raw_status="stored",
        error=error,
    )


def _header_url(primary_document_url: str, accession: str) -> str:
    """``<acc>.hdr.sgml`` URL in the primary document's archive directory.

    EDGAR disseminates the SGML header alongside the filing's documents
    (``.../Archives/edgar/data/<cik>/<acc-nodash>/<acc-dashed>.hdr.sgml``) —
    verified live on 0001193125-26-280246 (200, 1.6 KB). All 1,753 in-scope
    ``filing_events`` URLs live under an instrument-owned CIK directory
    (full-population check in the spec), so deriving from the manifest URL
    never routes through a filing-agent CIK (#1233 n/a).
    """
    scheme, netloc, path, _query, _fragment = urlsplit(primary_document_url)
    directory = posixpath.dirname(path)
    return urlunsplit((scheme, netloc, f"{directory}/{accession}.hdr.sgml", "", ""))


def _parse_tender(
    conn: psycopg.Connection[Any],
    row: Any,  # ManifestRow — forward-ref to avoid circular import
) -> Any:  # ParseOutcome — forward-ref
    """Parser for one tender-schedule accession."""
    from app.jobs.sec_manifest_worker import ParseOutcome

    accession = row.accession_number
    url = row.primary_document_url

    form = row.form.strip()
    if form not in IN_SCOPE_FORMS:
        # _FORM_TO_SOURCE routes only the eight in-scope forms here. Anything
        # else (a PREM14C, or a legacy/manual seed) is an upstream misroute.
        logger.warning(
            "sec_tender parser: accession=%s unexpected form=%r; tombstoning",
            accession,
            row.form,
        )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_TENDER,
            error=f"unexpected form {row.form!r}",
        )
    if not url:
        logger.warning(
            "sec_tender parser: accession=%s has no primary_document_url; tombstoning",
            accession,
        )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_TENDER,
            error="missing primary_document_url",
        )
    # No instrument_id gate: roles come from the header CIK blocks, not from
    # the manifest row's (arbitrary-between-parties) instrument_id.

    try:
        with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider:
            body = provider.fetch_document_text(url)
            header = provider.fetch_document_text(_header_url(url, accession)) if body else None
    except Exception as exc:  # noqa: BLE001 — transient fetch errors retry via worker backoff
        logger.warning(
            "sec_tender parser: fetch raised accession=%s url=%s: %s",
            accession,
            url,
            exc,
        )
        return _failed_outcome(f"fetch error: {exc}")

    if not body:
        # Non-200 or empty body — tombstone (no raw to store).
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_TENDER,
            error="empty or non-200 fetch",
        )

    # Persist raw BEFORE parse so the #938 invariant holds even if the parse /
    # upsert later raises and the worker retries. ``tender_body`` is a swept
    # kind → store_raw born-compacts (sha only, no bytes). The header is
    # parsed in-memory and never stored raw (1-2 KB, rehydratable; its durable
    # facts land as subject_cik / offeror_names columns). Savepoint protects
    # the worker's outer transaction.
    try:
        with conn.transaction():
            store_raw(
                conn,
                accession_number=accession,
                document_kind="tender_body",
                payload=body,
                parser_version=_PARSER_VERSION_TENDER,
                source_url=url,
            )
    except Exception as exc:  # noqa: BLE001
        logger.exception("sec_tender parser: store_raw failed accession=%s", accession)
        return _failed_outcome(f"store_raw error: {exc}")

    # Raw is now stored — every subsequent outcome MUST carry
    # raw_status='stored' so the manifest's view matches the raw table (#938).
    if not header:
        # The header is required for attribution; without it no role row can
        # be derived. Empty/non-200 header on a live accession is not
        # transient — tombstone rather than spin retries.
        return _tombstone_after_store(conn, accession, "empty or non-200 header fetch")

    try:
        parse = parse_tender_offer(body, header, form)
    except Exception as exc:  # noqa: BLE001
        logger.exception("sec_tender parser: parse raised accession=%s", accession)
        return _failed_outcome(f"parse error: {exc}", raw_status="stored")

    if parse is None:
        # Unusable header blocks / body not a recognizable schedule.
        return _tombstone_after_store(conn, accession, "unusable header or unrecognizable schedule")

    roles = resolve_party_roles(parse)
    cik_to_instruments = map_ciks_to_instruments(conn, list(roles))
    # One typed row per matched instrument. A share-class-sibling CIK maps to
    # multiple instruments and EACH gets a row (the tender applies to every
    # sibling class). An instrument reachable from BOTH a subject and an
    # offeror CIK (pathological CIK-history overlap) prefers 'subject',
    # mirroring the header-block collapse rule; the (accession, instrument_id)
    # PK admits only one row per instrument regardless.
    instrument_role: dict[int, str] = {}
    for cik, instrument_ids in cik_to_instruments.items():
        role = roles[cik]
        for iid in instrument_ids:
            if role == "subject" or iid not in instrument_role:
                instrument_role[iid] = role
    instrument_roles = sorted(instrument_role.items())
    if not instrument_roles:
        # Event concerns nothing in universe (identifiers churned after
        # seeding) — tombstone, cleaning any rows a prior parse wrote.
        return _tombstone_after_store(conn, accession, "no header party maps to an in-universe instrument")

    try:
        with conn.transaction():
            upsert_tender_offer_events(
                conn,
                accession_number=accession,
                parse=parse,
                instrument_roles=instrument_roles,
            )
    except Exception as exc:  # noqa: BLE001
        logger.exception("sec_tender parser: upsert failed accession=%s", accession)
        return _failed_outcome(f"upsert error: {exc}", raw_status="stored")

    return ParseOutcome(
        status="parsed",
        parser_version=_PARSER_VERSION_TENDER,
        raw_status="stored",
    )


def _tender_fetch_url(conn: Any, row: Any) -> str | None:  # row: ManifestRow
    """#1591 prefetch hook — the primary-document URL, returned only when the
    parser would actually fetch it (mirrors ``_parse_tender``'s pre-fetch
    tombstone gates). The header fetch is NOT prefetched (tiny, sequential
    after the body). ``tender_body`` is born-compacted (swept kind), so the
    parser always re-fetches on re-drain — no reuse gate.
    """
    if row.form.strip() not in IN_SCOPE_FORMS:
        return None
    return row.primary_document_url or None


def register() -> None:
    """Register the tender parser with the manifest worker (idempotent)."""
    from app.jobs.sec_manifest_worker import register_parser

    register_parser("sec_tender", _parse_tender, requires_raw_payload=True, fetch_url=_tender_fetch_url)
