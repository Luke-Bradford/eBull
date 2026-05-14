"""8-K manifest-worker parser adapter (#873).

Wraps the existing pure-function parser ``parse_8k_filing`` +
table-writer ``upsert_8k_filing`` from ``app.services.eight_k_events``
so the generic manifest worker can drive 8-K ingest one accession at
a time.

Pre-#873 the legacy ``ingest_8k_events`` job scanned
``filing_events`` for rows lacking an ``eight_k_filings`` row and
processed them in batches. That path still works (no breakage in
this PR) but the manifest worker is the future-facing single-writer
pattern from the #869 spec. As the manifest worker drains its
backlog, the legacy job becomes redundant and can be retired in a
follow-up.

ParseOutcome contract reminder (see
``sec_manifest_worker.ParserSpec``):

  * ``status='parsed'`` + ``raw_status='stored'`` — success path.
    The raw HTML body is persisted in ``filing_raw_documents`` so a
    future re-wash can reparse without re-fetching from SEC.
  * ``status='tombstoned'`` — fetch returned non-200 / empty body
    or the parser couldn't extract anything (no items, no header
    fields). Matches the legacy job's tombstone semantics so the
    operator sees the same accession counted under
    ``rows_tombstoned`` regardless of which path landed it.
  * ``status='failed'`` — transient error (HTTP 5xx, DB exception
    inside the upsert). Worker schedules a 1h backoff retry per
    ``_backoff_for``.

Raw-payload invariant (#938): registered with
``requires_raw_payload=True`` so the worker refuses to mark a row
``parsed`` when ``raw_status='absent'``. The parser stores the raw
HTML via ``raw_filings.store_raw`` before returning the parsed
outcome, so the invariant holds by construction.

Share-class fan-out: ``eight_k_filings`` is PK=accession (entity-
level table per sec-edgar §3.6 / data-engineer §11). The per-instrument
read bridge runs through ``filing_events`` (sql/144). The parser
writes ONE ``eight_k_filings`` row anchored on ``row.instrument_id``
from the manifest; downstream readers fan out via the bridge.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg

from app.config import settings
from app.providers.implementations.sec_edgar import SecFilingsProvider
from app.services.dividend_calendar import (
    _PARSER_VERSION_DIVIDEND,
    parse_dividend_announcement,
    upsert_dividend_event,
)
from app.services.eight_k_events import (
    _PARSER_VERSION,
    _load_item_labels,
    _write_tombstone,
    parse_8k_filing,
    upsert_8k_filing,
)
from app.services.manifest_parsers._classify import (
    format_upsert_error,
    is_transient_upsert_error,
)
from app.services.manifest_parsers._siblings import (
    CIK_MISSING_SENTINEL as _CIK_MISSING_SENTINEL,
)
from app.services.manifest_parsers._siblings import (
    resolve_siblings as _resolve_siblings,
)
from app.services.raw_filings import store_raw

logger = logging.getLogger(__name__)

# Explicit 1h backoff matches the worker's ``_backoff_for(0)`` return
# value (Codex pre-push round 2 WARNING: importing a private symbol
# from another module is brittle — if the worker renames or changes
# the backoff signature, the parser breaks silently at runtime).
# Duplicated here as a literal so the parser's contract with the
# manifest worker is the PUBLIC ``ParseOutcome`` shape only.
_FAILED_RETRY_DELAY = timedelta(hours=1)

# Composite parser version stamped on ``sec_filing_manifest.parser_version``
# only. Mirrors the sec_13f_hr.py:88-89 pattern: encoding two sub-
# versions into one string lets a bump to either trigger a manifest
# rewash via parser_version mismatch (operator-driven via
# ``POST /jobs/sec_rebuild/run {"source": "sec_8k"}``).
#
# IMPORTANT: this composite ONLY drives the manifest column. The typed
# ``eight_k_filings.parser_version`` (written by ``upsert_8k_filing``
# / ``_write_tombstone`` in eight_k_events.py) intentionally stays at
# the bare ``_PARSER_VERSION`` integer — that column is provenance
# for the typed-table writer and is not consumed by rewash logic.
# Likewise ``store_raw(... parser_version=str(_PARSER_VERSION))``
# below stays bare — that column is raw-document provenance, not a
# rewash signal; tying it to dividend regex versions would invalidate
# the raw HTML cache on every regex bump for no integrity benefit
# (#1158 Codex pre-spec round 2 HIGH).
_PARSER_VERSION_EIGHT_K = f"8k:{_PARSER_VERSION}+dividend:{_PARSER_VERSION_DIVIDEND}"


def _failed_outcome(error: str, raw_status: Any = None) -> Any:
    """Build a ``failed`` ParseOutcome with a 1h backoff applied.

    Codex pre-push round 1: the worker only computes backoff for
    parser-raised exceptions; a parser that RETURNS
    ``ParseOutcome(status='failed')`` without ``next_retry_at`` would
    get immediately retried, hammering SEC on every tick. Set it
    here so every failed-return path is on the 1h cadence — matches
    the worker's internal default at sec_manifest_worker.py:126.
    """
    from app.jobs.sec_manifest_worker import ParseOutcome

    return ParseOutcome(
        status="failed",
        parser_version=_PARSER_VERSION_EIGHT_K,
        raw_status=raw_status,
        error=error,
        next_retry_at=datetime.now(tz=UTC) + _FAILED_RETRY_DELAY,
    )


def _maybe_extract_dividend(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    issuer_cik: str | None,
    accession: str,
    html: str,
) -> None:
    """Best-effort dividend_events extraction from a parsed 8-K body.

    NOT gated on ``parsed.items``: ``parse_8k_filing`` extracts items
    from the HTML body itself, and there is real divergence between
    submissions-declared items and HTML-extracted items. The pure
    function ``parse_dividend_announcement`` already gates on its
    internal ``_DIVIDEND_CONTEXT_RE`` (requires ``$N.NN per share``
    in proximity to the word ``dividend``); false positives on
    non-dividend bodies are extremely unlikely. Net effect: equal-or-
    better coverage than the legacy
    ``sec_dividend_calendar_ingest`` cron, since amendments (8-K/A)
    and items-array-misclassified filings now get extraction too.
    (#1158 Codex pre-spec round 1 BLOCKING.)

    Fans out to share-class siblings via ``_resolve_siblings``
    (#1102 / #1117). The legacy cron iterated ``filing_events``
    rows so siblings naturally got their own row each; the manifest
    row carries one anchor instrument_id and must fan out
    explicitly. Fail-closed union (``set(siblings) | {instrument_id}``)
    preserves the canonical sibling on stale ``external_identifiers``
    state.

    Idempotent: ``upsert_dividend_event``'s
    ``ON CONFLICT (instrument_id, source_accession) DO UPDATE`` makes
    re-runs harmless (last_parsed_at bumps, other columns rewrite
    to identical values when regex output is unchanged). Re-running
    after ``_PARSER_VERSION_DIVIDEND`` bump rewrites partial rows
    with the new regex output.
    """
    announcement = parse_dividend_announcement(html)
    if announcement is None:
        # Non-dividend body. Item 8.01 covers buybacks / litigation /
        # JVs; some dividend announcements live under other items
        # (7.01 has been observed). The regex gate in
        # parse_dividend_announcement handles all cases. No tombstone:
        # the manifest row's ``parsed`` status caps re-fetch cadence
        # to operator-triggered rewash, equivalent to the legacy
        # cron's 7-day TTL but operator-controlled.
        return
    siblings = _resolve_siblings(
        conn,
        instrument_id=instrument_id,
        issuer_cik=(issuer_cik or "").strip() or _CIK_MISSING_SENTINEL,
    )
    for sibling_iid in siblings:
        upsert_dividend_event(
            conn,
            instrument_id=sibling_iid,
            source_accession=accession,
            announcement=announcement,
        )


def _parse_eight_k(
    conn: psycopg.Connection[Any],
    row: Any,  # ManifestRow — forward-ref to avoid circular import
) -> Any:  # ParseOutcome — forward-ref
    """Manifest-worker parser for one 8-K accession.

    Steps:

    1. Fetch the primary document HTML from
       ``row.primary_document_url``.
    2. Persist the raw HTML in ``filing_raw_documents`` so the
       worker's #938 invariant is satisfied AND a re-wash can
       reparse without re-fetching.
    3. Parse via ``parse_8k_filing``.
    4. On parse miss: write the entity-level tombstone in
       ``eight_k_filings`` AND return ``tombstoned`` so the manifest
       row reflects the same state.
    5. On parse success: upsert ``eight_k_filings`` + items +
       exhibits via ``upsert_8k_filing``.
    """
    # Lazy import the worker contract types so this module can be
    # registered at package import time without a circular import.
    from app.jobs.sec_manifest_worker import ParseOutcome

    accession = row.accession_number
    url = row.primary_document_url
    instrument_id = row.instrument_id

    if not url:
        # The manifest row should always have a primary_document_url
        # (Atom / daily-index / per-CIK poll all populate it). A
        # missing URL is an upstream bug — log + tombstone so we don't
        # spin retry on an irrecoverable row.
        logger.warning(
            "eight_k manifest parser: accession=%s has no primary_document_url; tombstoning",
            accession,
        )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_EIGHT_K,
            error="missing primary_document_url",
        )
    if instrument_id is None:
        logger.warning(
            "eight_k manifest parser: accession=%s has no instrument_id; tombstoning",
            accession,
        )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_EIGHT_K,
            error="missing instrument_id",
        )

    try:
        with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider:
            html = provider.fetch_document_text(url)
    except Exception as exc:  # noqa: BLE001 — transient fetch errors retry via worker backoff
        logger.warning(
            "eight_k manifest parser: fetch raised accession=%s url=%s: %s",
            accession,
            url,
            exc,
        )
        return _failed_outcome(f"fetch error: {exc}")

    if not html:
        # Non-200 or empty body — legacy path tombstoned this; mirror
        # the semantics so dashboard counts match across both writers.
        # Savepoint protects the worker's outer transaction (Codex
        # pre-push round 2): a tombstone INSERT failure shouldn't
        # leave the tx aborted before transition_status fires.
        try:
            with conn.transaction():
                _write_tombstone(
                    conn,
                    instrument_id=instrument_id,
                    accession_number=accession,
                    primary_document_url=url,
                    document_type="8-K",
                )
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "eight_k manifest parser: tombstone INSERT failed accession=%s",
                accession,
            )
            return _failed_outcome(f"tombstone error: {exc}")
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_EIGHT_K,
            error="empty or non-200 fetch",
        )

    # Persist the raw HTML BEFORE the parse + upsert so the #938
    # invariant ('parsed implies raw stored') is satisfied even if
    # the upsert later raises and the worker retries. Wrap in a
    # savepoint so a partial write doesn't leave the worker's outer
    # transaction in an aborted state (Codex pre-push round 1 —
    # without the savepoint a store_raw error would block the
    # subsequent transition_status call).
    try:
        with conn.transaction():
            store_raw(
                conn,
                accession_number=accession,
                document_kind="primary_doc",
                payload=html,
                parser_version=str(_PARSER_VERSION),
                source_url=url,
            )
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "eight_k manifest parser: store_raw failed accession=%s",
            accession,
        )
        return _failed_outcome(f"store_raw error: {exc}")

    # Codex pre-push round 2 BLOCKING: ``_load_item_labels`` and
    # ``parse_8k_filing`` are pure DB read + pure-Python parse, but
    # the raw body is already in ``filing_raw_documents`` by this
    # point. An unhandled exception here would propagate to the
    # worker, which would record ``raw_status='absent'`` on the
    # manifest row even though raw IS stored — leaving a permanent
    # split between the manifest's view and the raw table's view.
    # Catch + return ``_failed_outcome`` with ``raw_status='stored'``
    # so the manifest reflects ground truth and the worker retries
    # the parse on the next tick without re-fetching from SEC.
    try:
        labels = _load_item_labels(conn)
        # ``known_items`` is an OPTIONAL hint from filing_events used
        # by the legacy bulk path to cross-check the parser's item
        # extraction. The manifest row carries no items, so pass
        # empty — the parser extracts items from the HTML body
        # unaided.
        parsed = parse_8k_filing(html, known_items=(), item_labels=labels)
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "eight_k manifest parser: labels-load or parse raised accession=%s",
            accession,
        )
        return _failed_outcome(f"parse error: {exc}", raw_status="stored")
    if parsed is None:
        try:
            with conn.transaction():
                _write_tombstone(
                    conn,
                    instrument_id=instrument_id,
                    accession_number=accession,
                    primary_document_url=url,
                    document_type="8-K",
                )
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "eight_k manifest parser: tombstone INSERT failed accession=%s",
                accession,
            )
            return _failed_outcome(f"tombstone error: {exc}", raw_status="stored")
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_EIGHT_K,
            raw_status="stored",
            error="parser returned no header fields or items",
        )

    # Savepoint protects the worker's outer transaction from a
    # partial upsert leaving aborted state. ``upsert_8k_filing``
    # already wraps its child DELETE+INSERT in an inner
    # ``conn.transaction()``; the outer savepoint here covers the
    # parent INSERT too.
    try:
        with conn.transaction():
            upsert_8k_filing(
                conn,
                instrument_id=instrument_id,
                accession_number=accession,
                primary_document_url=url,
                parsed=parsed,
                item_labels=labels,
            )
    except Exception as exc:  # noqa: BLE001
        # #1131 transient-vs-deterministic discrimination. A psycopg
        # OperationalError (SerializationFailure / DeadlockDetected /
        # connection drop) is worth retrying with a 1h backoff. A
        # deterministic constraint violation (IntegrityError /
        # DataError / etc.) won't self-fix on retry — refetching the
        # same dead XML every hour wastes SEC fair-use budget. Tombstone
        # the row instead so the worker stops re-fetching; a parser
        # bump can re-pick the accession via the manifest rewash path.
        logger.exception(
            "eight_k manifest parser: upsert failed accession=%s",
            accession,
        )
        if is_transient_upsert_error(exc):
            return _failed_outcome(format_upsert_error(exc), raw_status="stored")
        try:
            with conn.transaction():
                _write_tombstone(
                    conn,
                    instrument_id=instrument_id,
                    accession_number=accession,
                    primary_document_url=url,
                    document_type="8-K",
                )
        except Exception:  # noqa: BLE001 — tombstone failure shouldn't mask upsert failure
            logger.exception(
                "eight_k manifest parser: tombstone INSERT failed after upsert error accession=%s",
                accession,
            )
            return _failed_outcome(
                f"upsert+tombstone error: {type(exc).__name__}: {exc}",
                raw_status="stored",
            )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_EIGHT_K,
            raw_status="stored",
            error=format_upsert_error(exc),
        )

    # Dividend_events extraction (#1158). Runs on every parsed 8-K
    # body; ``parse_dividend_announcement`` has its own internal
    # gate. Wrapped in a savepoint so a deterministic dividend-side
    # failure (IntegrityError, etc.) doesn't abort the worker's
    # outer transaction or block the subsequent ``transition_status``
    # write. Transient OperationalError → ``_failed_outcome`` (the
    # whole 8-K row retries on the next tick — upsert_8k_filing is
    # idempotent so the redo is safe). Deterministic exceptions →
    # log + drop the dividend write but PRESERVE the parsed
    # outcome: the 8-K filing/items/exhibits rows already landed
    # successfully and tombstoning the manifest row would lose
    # operator-visible 8-K data over a dividend-extraction bug.
    try:
        with conn.transaction():
            _maybe_extract_dividend(
                conn,
                instrument_id=instrument_id,
                issuer_cik=row.cik,
                accession=accession,
                html=html,
            )
    except Exception as exc:  # noqa: BLE001
        if is_transient_upsert_error(exc):
            logger.warning(
                "eight_k manifest parser: dividend extraction transient failure accession=%s: %s",
                accession,
                exc,
            )
            return _failed_outcome(format_upsert_error(exc), raw_status="stored")
        logger.exception(
            "eight_k manifest parser: dividend extraction failed accession=%s",
            accession,
        )

    return ParseOutcome(
        status="parsed",
        parser_version=_PARSER_VERSION_EIGHT_K,
        raw_status="stored",
    )


def register() -> None:
    """Register the 8-K parser with the manifest worker.

    Idempotent: ``register_parser`` is last-write-wins, so calling
    this twice with the same callable is a no-op. Called once from
    ``app.services.manifest_parsers.register_all_parsers`` at package
    import time, and re-callable from tests after a registry wipe.
    """
    from app.jobs.sec_manifest_worker import register_parser

    register_parser("sec_8k", _parse_eight_k, requires_raw_payload=True)
