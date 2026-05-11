"""DEF 14A manifest-worker parser adapter (#873).

Wraps the existing pure-function parser
``parse_beneficial_ownership_table`` + table-writer helpers from
``app.services.def14a_ingest`` so the generic manifest worker can
drive DEF 14A ingest one accession at a time.

Pre-#873 the legacy ``ingest_def14a`` job scanned ``filing_events``
for DEF 14A accessions whose ``def14a_ingest_log`` row was missing
and processed them in batches. That path still works (no breakage
in this PR) but the manifest worker is the future-facing single-
writer pattern from the #869 spec. The manifest parser writes a
``def14a_ingest_log`` row on every outcome so the legacy discovery
filter (which excludes accessions already in the log) skips
manifest-handled accessions — no duplicate fetches during cutover.

ParseOutcome contract (see ``sec_manifest_worker.ParserSpec``):

  * ``status='parsed'`` + ``raw_status='stored'`` — success path.
    Raw HTML persisted in ``filing_raw_documents``; one
    ``def14a_beneficial_holdings`` row per (sibling, holder); one
    ``def14a_ingest_log`` row with ``status='success'``; one
    ``ownership_def14a_observations`` row per non-ESOP holder; one
    ``ownership_esop_observations`` row per ESOP plan.
  * ``status='tombstoned'`` — fetch returned non-200/empty body
    OR parser identified no beneficial-ownership table (notice-only
    proxy / unrecognisable layout). Matches the legacy ``partial``
    bucket so dashboard counts converge.
  * ``status='failed'`` — transient error (fetch raise, store_raw
    error, upsert error). Worker schedules a 1h backoff retry per
    ``_FAILED_RETRY_DELAY``.

Raw-payload invariant (#938): registered with
``requires_raw_payload=True`` so the worker refuses to mark a row
``parsed`` when ``raw_status='absent'``. ``store_raw`` runs in a
savepoint BEFORE parse + upsert so the invariant holds whether
parsing succeeds or raises.

Share-class fan-out: DEF 14A is an issuer-level filing. The parser
resolves the issuer CIK from ``instrument_sec_profile`` then fans
out across every share-class sibling via
``siblings_for_issuer_cik`` — same pattern the legacy ingester uses
post-#1117 PR-B. Each sibling gets its own
``def14a_beneficial_holdings`` row + observation row so per-
instrument reads return identical figures across the share-class
panel.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET  # noqa: S405 — only ET.ParseError caught; no untrusted parse.
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg

from app.config import settings
from app.providers.implementations.sec_def14a import (
    Def14ABeneficialOwnershipTable,
    parse_beneficial_ownership_table,
)
from app.providers.implementations.sec_edgar import SecFilingsProvider
from app.services.def14a_ingest import (
    _CIK_MISSING_SENTINEL,
    _PARSER_VERSION_DEF14A,
    _record_def14a_observations_for_filing,
    _record_esop_observations_for_filing,
    _record_ingest_attempt,
    _resolve_issuer_cik,
    _upsert_holding,
)
from app.services.ownership_observations import (
    refresh_def14a_current,
    refresh_esop_current,
)
from app.services.raw_filings import store_raw
from app.services.sec_identity import siblings_for_issuer_cik

logger = logging.getLogger(__name__)

# Explicit 1h backoff matches the worker's ``_backoff_for(0)`` value.
# Duplicated as a literal — see eight_k.py for the rationale (importing
# the private worker symbol couples to internal layout).
_FAILED_RETRY_DELAY = timedelta(hours=1)


def _failed_outcome(error: str, raw_status: Any = None) -> Any:
    """Build a ``failed`` ParseOutcome with a 1h backoff applied.

    The worker only computes backoff for parser-raised exceptions;
    a parser that RETURNS ``ParseOutcome(status='failed')`` without
    ``next_retry_at`` would get immediately retried, hammering SEC
    on every tick. Mirror the eight_k.py pattern."""
    from app.jobs.sec_manifest_worker import ParseOutcome

    return ParseOutcome(
        status="failed",
        parser_version=_PARSER_VERSION_DEF14A,
        raw_status=raw_status,
        error=error,
        next_retry_at=datetime.now(tz=UTC) + _FAILED_RETRY_DELAY,
    )


def _resolve_siblings(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    issuer_cik: str,
) -> list[int]:
    """Resolve share-class siblings for fan-out.

    Returns ``[instrument_id]`` when CIK is the sentinel OR the
    siblings query returns empty (defensive fallback so a
    ``instrument_sec_profile`` gap doesn't drop the primary write).
    """
    if issuer_cik == _CIK_MISSING_SENTINEL:
        return [instrument_id]
    siblings = siblings_for_issuer_cik(conn, issuer_cik)
    return siblings if siblings else [instrument_id]


def _parse_def14a(
    conn: psycopg.Connection[Any],
    row: Any,  # ManifestRow — forward-ref to avoid circular import
) -> Any:  # ParseOutcome — forward-ref
    """Manifest-worker parser for one DEF 14A accession.

    Steps:

    1. Validate URL + instrument_id (tombstone on missing —
       upstream discovery should never enqueue these).
    2. Fetch the primary document HTML.
    3. ``store_raw`` in a savepoint (satisfies #938 invariant).
    4. Parse via ``parse_beneficial_ownership_table``.
    5. On parse exception: return failed with raw_status=stored so
       the manifest's view matches ground truth.
    6. On no rows (notice-only / unrecognisable layout): write
       ingest-log tombstone + return tombstoned.
    7. On rows: resolve siblings, upsert holdings + observations
       per sibling, write success log row, return parsed.
    """
    from app.jobs.sec_manifest_worker import ParseOutcome

    accession = row.accession_number
    url = row.primary_document_url
    instrument_id = row.instrument_id

    if not url:
        logger.warning(
            "def14a manifest parser: accession=%s has no primary_document_url; tombstoning",
            accession,
        )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_DEF14A,
            error="missing primary_document_url",
        )
    # Codex pre-push P1: ``sec_manifest._FORM_TO_SOURCE`` routes
    # ``PRE 14A`` (preliminary proxy) into ``sec_def14a`` but the
    # legacy ingester's ``_DEF14A_FORM_TYPES`` excludes it. Accepting
    # PRE 14A here would create a divergence between the manifest
    # path and the legacy path during cutover — PRE rows are pre-
    # finalisation drafts whose ownership figures the operator
    # never historically counted. Tombstone here so dual-path
    # accounting stays consistent. Whether PRE 14A should
    # eventually land is a policy decision deferred to a follow-up.
    if (row.form or "").strip().upper() == "PRE 14A":
        logger.info(
            "def14a manifest parser: accession=%s is PRE 14A (preliminary); tombstoning to match legacy filter",
            accession,
        )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_DEF14A,
            error="PRE 14A preliminary proxy — deferred (policy TBD)",
        )
    if instrument_id is None:
        logger.warning(
            "def14a manifest parser: accession=%s has no instrument_id; tombstoning",
            accession,
        )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_DEF14A,
            error="missing instrument_id",
        )

    try:
        with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider:
            body = provider.fetch_document_text(url)
    except Exception as exc:  # noqa: BLE001 — transient fetch errors retry via worker backoff
        logger.warning(
            "def14a manifest parser: fetch raised accession=%s url=%s: %s",
            accession,
            url,
            exc,
        )
        return _failed_outcome(f"fetch error: {exc}")

    # Resolve issuer CIK once; threaded through every outcome path so
    # the ingest-log write doesn't re-issue the lookup (same shape as
    # the legacy ingester's _AccessionOutcome.issuer_cik).
    issuer_cik = _resolve_issuer_cik(conn, instrument_id=instrument_id) or _CIK_MISSING_SENTINEL

    if not body:
        # Non-200 or empty body. Legacy path returned status='failed'
        # for this case; manifest path treats it as a tombstone so the
        # row doesn't get retried forever on a persistently-404 doc.
        # Write the ingest-log row with status='failed' to match legacy
        # accounting; manifest itself records tombstoned.
        try:
            with conn.transaction():
                _record_ingest_attempt(
                    conn,
                    accession_number=accession,
                    issuer_cik=issuer_cik,
                    status="failed",
                    error="primary doc fetch returned empty or non-200",
                )
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "def14a manifest parser: ingest-log INSERT failed accession=%s",
                accession,
            )
            return _failed_outcome(f"log error: {exc}")
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_DEF14A,
            error="empty or non-200 fetch",
        )

    # store_raw in a savepoint so a partial write doesn't abort the
    # worker's outer transaction. The body lands in
    # filing_raw_documents BEFORE parse so a downstream re-wash can
    # reparse without re-fetching from SEC.
    try:
        with conn.transaction():
            store_raw(
                conn,
                accession_number=accession,
                document_kind="def14a_body",
                payload=body,
                parser_version=_PARSER_VERSION_DEF14A,
                source_url=url,
            )
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "def14a manifest parser: store_raw failed accession=%s",
            accession,
        )
        return _failed_outcome(f"store_raw error: {exc}")

    # Parse-phase exceptions must return raw_status='stored' because
    # store_raw already committed inside its savepoint — the manifest
    # MUST reflect that state or it permanently diverges from
    # filing_raw_documents (the "preserves stored raw_status" rule
    # from the 8-K Codex round 2 BLOCKING). ET.ParseError + ValueError
    # are what parse_beneficial_ownership_table raises on malformed
    # input; broader Exception handler covers unexpected raises (e.g.
    # AttributeError in a tag walker on truly junk HTML).
    try:
        parsed: Def14ABeneficialOwnershipTable = parse_beneficial_ownership_table(body)
    except (ValueError, ET.ParseError) as exc:
        logger.exception(
            "def14a manifest parser: parse raised accession=%s",
            accession,
        )
        return _failed_outcome(f"parse error: {exc}", raw_status="stored")
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "def14a manifest parser: parse raised (unexpected) accession=%s",
            accession,
        )
        return _failed_outcome(f"parse error (unexpected): {exc}", raw_status="stored")

    if not parsed.rows:
        # Notice-only proxy / unrecognisable layout. Legacy path
        # returned status='partial'; manifest path records
        # tombstoned. Write the log row with status='partial' to
        # mirror legacy accounting so /coverage/def14a counts a
        # consistent figure.
        try:
            with conn.transaction():
                _record_ingest_attempt(
                    conn,
                    accession_number=accession,
                    issuer_cik=issuer_cik,
                    status="partial",
                    error=f"no beneficial-ownership table identified (best_score={parsed.raw_table_score})",
                )
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "def14a manifest parser: ingest-log INSERT failed accession=%s",
                accession,
            )
            return _failed_outcome(f"log error: {exc}", raw_status="stored")
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_DEF14A,
            raw_status="stored",
            error=f"no beneficial-ownership table identified (best_score={parsed.raw_table_score})",
        )

    # Success path: fan out across share-class siblings. Wrap
    # siblings resolution + entire write batch + ingest-log row in
    # ONE try so any DB error from sibling lookup OR upsert returns
    # ``_failed_outcome(raw_status='stored')`` — store_raw already
    # ran inside its own committed savepoint, so the manifest MUST
    # reflect stored raw or it permanently diverges from
    # filing_raw_documents (Codex pre-push BLOCKING). The savepoint
    # also rolls back partial-sibling state on mid-batch failure so
    # share-class rollups stay consistent.
    inserted = 0
    updated = 0

    try:
        with conn.transaction():
            siblings = _resolve_siblings(conn, instrument_id=instrument_id, issuer_cik=issuer_cik)
            for sibling_iid in siblings:
                for holder in parsed.rows:
                    outcome = _upsert_holding(
                        conn,
                        accession_number=accession,
                        issuer_cik=issuer_cik,
                        instrument_id=sibling_iid,
                        as_of_date=parsed.as_of_date,
                        holder=holder,
                    )
                    if outcome == "inserted":
                        inserted += 1
                    else:
                        updated += 1
                _record_def14a_observations_for_filing(
                    conn,
                    instrument_id=sibling_iid,
                    accession_number=accession,
                    as_of_date=parsed.as_of_date,
                    holders=parsed.rows,
                )
                refresh_def14a_current(conn, instrument_id=sibling_iid)
                esop_rows_written = _record_esop_observations_for_filing(
                    conn,
                    instrument_id=sibling_iid,
                    accession_number=accession,
                    as_of_date=parsed.as_of_date,
                    holders=parsed.rows,
                )
                if esop_rows_written > 0:
                    refresh_esop_current(conn, instrument_id=sibling_iid)
            _record_ingest_attempt(
                conn,
                accession_number=accession,
                issuer_cik=issuer_cik,
                status="success",
                rows_inserted=inserted,
                rows_skipped=0,
                error=None,
            )
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "def14a manifest parser: upsert/observation batch failed accession=%s",
            accession,
        )
        return _failed_outcome(f"upsert error: {exc}", raw_status="stored")

    return ParseOutcome(
        status="parsed",
        parser_version=_PARSER_VERSION_DEF14A,
        raw_status="stored",
    )


def register() -> None:
    """Register the DEF 14A parser with the manifest worker.

    Idempotent — ``register_parser`` is last-write-wins. Called once
    from ``register_all_parsers`` at package import; re-callable
    from tests after a registry wipe.
    """
    from app.jobs.sec_manifest_worker import register_parser

    register_parser("sec_def14a", _parse_def14a, requires_raw_payload=True)
