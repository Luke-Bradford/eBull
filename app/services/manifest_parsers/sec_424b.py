"""Manifest-worker parser for SEC 424B prospectuses (tier-1 subtypes).

Issue #1816 (child of #1015 item 2). Upgrades 424B1/B3/B4/B5/B7 from
metadata-only to PARSE+RAW under the new manifest source ``sec_424b``. Mirrors
``sec_nt.py``: fetch the primary document, persist it as ``prospectus_body`` in
``filing_raw_documents`` (the #938 'parsed implies raw stored' invariant —
``prospectus_body`` is in ``SWEPT_DOCUMENT_KINDS``, so the row is born-compacted:
sha256 recorded, bytes never stored; prospectus bodies run 100 KB-12 MB), then
run the pure extractor (``app.services.prospectus_offerings``) on the in-memory
body and upsert ``prospectus_offerings``.

424B2 is volume-gated (#1975): routed here, but tombstoned WITHOUT fetch when
the filer's lifetime B2 count exceeds ``_424B2_VOLUME_CAP`` — in our population
every filer above the cap is a bank/ETN/credit-vehicle structured-note factory
(JPM 30k … PRU 106; full-population scan 2026-07-05). The cap is a fetch-cost
bound, NOT a classification — equity-vs-debt comes only from the parsed Item
501(b)(3) cover. 424B8 is NOT routed here (late-filing duplicate of another
424(b) paragraph). An unexpected form reaching this parser is an upstream
misroute → tombstone.

All field logic lives in ``prospectus_offerings`` (single-chokepoint
discipline); this module only orchestrates fetch / store / transition.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg

from app.config import settings
from app.providers.implementations.sec_edgar import SecFilingsProvider
from app.services.prospectus_offerings import (
    IN_SCOPE_SUBTYPES,
    parse_prospectus_offering,
    upsert_prospectus_offering,
)
from app.services.prospectus_offerings import (
    PARSER_VERSION as _424B_PARSER_VERSION,
)
from app.services.raw_filings import store_raw

logger = logging.getLogger(__name__)

_FAILED_RETRY_DELAY = timedelta(hours=1)

# #1975: max lifetime 424B2 filings per instrument before the parser stops
# fetching that filer's B2 bodies. Full-population scan (149,555 B2 rows / 739
# instruments, 2026-07-05): all 21 filers above 100 are banks/ETNs/credit
# vehicles; the ≤100 tail is 718 instruments / ~4,252 filings. Evaluated
# against the live ``filing_events`` horizon at parse time (self-updating —
# a new note factory crossing the cap self-excludes; no allowlist to rot).
# Deliberately non-idempotent across rebuilds: a fetch-cost policy, not a
# filing-time fact.
_424B2_VOLUME_CAP = 100

_424B2_CAP_ERROR = "424B2 volume cap: high-volume structured-note filer"

# ParseOutcome.parser_version and store_raw.parser_version are TEXT; the
# prospectus_offerings.parser_version column is INT (set by the upsert).
_PARSER_VERSION_424B = str(_424B_PARSER_VERSION)


def _failed_outcome(error: str, raw_status: Any = None) -> Any:
    """Build a ``failed`` ParseOutcome with a 1h backoff (mirrors sec_nt)."""
    from app.jobs.sec_manifest_worker import ParseOutcome

    return ParseOutcome(
        status="failed",
        parser_version=_PARSER_VERSION_424B,
        raw_status=raw_status,
        error=error,
        next_retry_at=datetime.now(tz=UTC) + _FAILED_RETRY_DELAY,
    )


def _424b2_within_volume_cap(conn: psycopg.Connection[Any], instrument_id: int) -> bool:
    """#1975 pre-fetch gate predicate: does this filer's lifetime 424B2 count
    sit within ``_424B2_VOLUME_CAP``? Exact-match ``'424B2'`` (the canonical
    EDGAR form string stored by discovery; no ``/A`` variants exist in
    ``filing_events`` — full-pop check 2026-07-05).
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM filing_events WHERE filing_type = '424B2' AND instrument_id = %s",
            (instrument_id,),
        )
        row = cur.fetchone()
    count = int(row[0]) if row is not None else 0
    return count <= _424B2_VOLUME_CAP


def _parse_424b(
    conn: psycopg.Connection[Any],
    row: Any,  # ManifestRow — forward-ref to avoid circular import
) -> Any:  # ParseOutcome — forward-ref
    """Parser for one tier-1 424B accession."""
    from app.jobs.sec_manifest_worker import ParseOutcome

    accession = row.accession_number
    url = row.primary_document_url
    instrument_id = row.instrument_id

    subtype = row.form.strip()
    if subtype not in IN_SCOPE_SUBTYPES:
        # _FORM_TO_SOURCE routes only the tier-1 subtypes here. Anything else
        # (a B2/B8, or a legacy/manual seed) is an upstream misroute.
        logger.warning(
            "sec_424b parser: accession=%s unexpected form=%r; tombstoning",
            accession,
            row.form,
        )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_424B,
            error=f"unexpected form {row.form!r}",
        )
    if not url:
        logger.warning(
            "sec_424b parser: accession=%s has no primary_document_url; tombstoning",
            accession,
        )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_424B,
            error="missing primary_document_url",
        )
    if instrument_id is None:
        logger.warning(
            "sec_424b parser: accession=%s has no instrument_id; tombstoning",
            accession,
        )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_424B,
            error="missing instrument_id",
        )

    if subtype == "424B2" and not _424b2_within_volume_cap(conn, instrument_id):
        # #1975 volume gate — one COUNT query, no SEC request. Same placement
        # class as the 13D/G retention + DEF 14A latest-N pre-fetch gates.
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_424B,
            error=_424B2_CAP_ERROR,
        )

    try:
        with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider:
            body = provider.fetch_document_text(url)
    except Exception as exc:  # noqa: BLE001 — transient fetch errors retry via worker backoff
        logger.warning(
            "sec_424b parser: fetch raised accession=%s url=%s: %s",
            accession,
            url,
            exc,
        )
        return _failed_outcome(f"fetch error: {exc}")

    if not body:
        # Non-200 or empty body — tombstone (no raw to store).
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_424B,
            error="empty or non-200 fetch",
        )

    # Persist raw BEFORE parse so the #938 invariant holds even if the parse /
    # upsert later raises and the worker retries. ``prospectus_body`` is a
    # swept kind → store_raw born-compacts (sha only, no bytes). Savepoint
    # protects the worker's outer transaction.
    try:
        with conn.transaction():
            store_raw(
                conn,
                accession_number=accession,
                document_kind="prospectus_body",
                payload=body,
                parser_version=_PARSER_VERSION_424B,
                source_url=url,
            )
    except Exception as exc:  # noqa: BLE001
        logger.exception("sec_424b parser: store_raw failed accession=%s", accession)
        return _failed_outcome(f"store_raw error: {exc}")

    # Raw is now stored — every subsequent outcome MUST carry
    # raw_status='stored' so the manifest's view matches the raw table (#938).
    try:
        offering = parse_prospectus_offering(body, subtype)
    except Exception as exc:  # noqa: BLE001
        logger.exception("sec_424b parser: parse raised accession=%s", accession)
        return _failed_outcome(f"parse error: {exc}", raw_status="stored")

    if offering is None:
        # Not a recognizable prospectus body — tombstone, but raw IS stored.
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_424B,
            raw_status="stored",
            error="not a recognizable prospectus",
        )

    try:
        with conn.transaction():
            upsert_prospectus_offering(
                conn,
                instrument_id=instrument_id,
                accession_number=accession,
                offering=offering,
            )
    except Exception as exc:  # noqa: BLE001
        logger.exception("sec_424b parser: upsert failed accession=%s", accession)
        return _failed_outcome(f"upsert error: {exc}", raw_status="stored")

    return ParseOutcome(
        status="parsed",
        parser_version=_PARSER_VERSION_424B,
        raw_status="stored",
    )


def _424b_fetch_url(conn: Any, row: Any) -> str | None:  # row: ManifestRow
    """#1591 prefetch hook — the single primary-document URL the 424B parser
    GETs, returned only when the parser would actually fetch it (mirrors
    ``_parse_424b``'s pre-fetch tombstone gates, including the #1975 B2 volume
    gate — without parity the prefetcher would fetch bodies the parser then
    refuses to parse). ``prospectus_body`` is born-compacted (swept kind), so
    the parser always re-fetches on re-drain — no reuse gate.
    """
    subtype = row.form.strip()
    if subtype not in IN_SCOPE_SUBTYPES:
        return None
    url = row.primary_document_url
    if not url or row.instrument_id is None:
        return None
    if subtype == "424B2" and not _424b2_within_volume_cap(conn, row.instrument_id):
        return None
    return url


def register() -> None:
    """Register the 424B parser with the manifest worker (idempotent)."""
    from app.jobs.sec_manifest_worker import register_parser

    register_parser("sec_424b", _parse_424b, requires_raw_payload=True, fetch_url=_424b_fetch_url)
