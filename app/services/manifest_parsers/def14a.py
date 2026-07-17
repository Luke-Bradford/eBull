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
    apply_exec_comp_best_effort,
    def14a_within_cap,
)
from app.services.manifest_parsers._classify import (
    format_upsert_error,
    is_transient_upsert_error,
)
from app.services.ownership_observations import (
    refresh_def14a_current,
    refresh_esop_current,
)
from app.services.raw_filings import (
    acquire_filing_accession_write_lock,
    store_raw,
    stored_body,
)
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

    # #1233 PR5 §4.2 — latest-N-primary-proxies-per-filer pre-fetch
    # gate. Refuses pre-rank-2 DEF 14A accessions BEFORE the SEC HTTP
    # call so no rate-limit budget is burned. Supplemental form
    # variants (DEFA14A / DEFR14A / DEFM14A) and CIK-missing
    # accessions pass the helper and proceed via their existing
    # tombstone paths. Out-of-corpus rows (no filing_events source)
    # refuse — safe default for manifest rows whose source-of-truth
    # is missing.
    if not def14a_within_cap(
        conn,
        accession_number=accession,
        instrument_id=instrument_id,
    ):
        logger.debug(
            "def14a manifest parser: accession=%s exceeds latest-N primary cap; tombstoning",
            accession,
        )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_DEF14A,
            error="latest-N primary cap",
        )

    # Resolve issuer CIK once; threaded through every outcome path (the
    # empty-body ingest-log write + the parse/upsert below) so neither
    # re-issues the lookup. Hoisted above the fetch branch so the #1591
    # reuse path resolves it too (same shape as the legacy ingester's
    # _AccessionOutcome.issuer_cik).
    issuer_cik = _resolve_issuer_cik(conn, instrument_id=instrument_id) or _CIK_MISSING_SENTINEL

    # #1591 — reuse the stored body on a re-drain (parser-version bump →
    # sec_rebuild resets the row to pending) instead of re-downloading.
    # def14a_body is retained (avg ~725KB) and SEC filings are immutable
    # per accession, so a present body is always safe to re-parse. Fetch +
    # store only on a miss (first ingest); reuse skips both so fetched_at
    # is not churned and the rate-limited SEC budget is spared.
    body = stored_body(conn, accession_number=accession, document_kind="def14a_body")
    if body is None:
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
        #
        # #2086 — Item 402(c) exec comp runs HERE too, before the
        # tombstone return. Reg S-K Items 402 and 403 are independent
        # items of Schedule 14A: a proxy whose beneficial-ownership
        # table defeats the detector (GME: best_score 0-9 across all
        # proxies) can still carry a perfectly standard Summary
        # Compensation Table. Comp stays a savepoint-isolated
        # best-effort augment that never changes this ParseOutcome —
        # the row still tombstones for Item 403 accounting.
        try:
            comp_siblings = _resolve_siblings(conn, instrument_id=instrument_id, issuer_cik=issuer_cik)
        except Exception:  # noqa: BLE001 — best-effort: fall back to the primary
            logger.exception(
                "def14a manifest parser: sibling resolve failed pre-comp accession=%s",
                accession,
            )
            comp_siblings = [instrument_id]
        apply_exec_comp_best_effort(
            conn,
            accession_number=accession,
            issuer_cik=issuer_cik,
            body=body,
            instrument_ids=comp_siblings,
        )
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
            # #817 — serialise this accession's def14a_beneficial_holdings
            # writes against a concurrent rewash DELETE+INSERT (same lock key).
            # First statement in the txn, after the body fetch above (no SEC
            # fetch inside this block).
            # #1735: the manifest worker now commits per row (``_dispatch_rows``
            # commits the implicit read-tx before the loop), so this
            # ``with conn.transaction()`` is a TOP-LEVEL txn and this xact lock
            # releases at the accession's row boundary, same as the rewash +
            # legacy per-accession callers. Mutual exclusion preserved; the
            # pre-#1735 batch-txn rewash-vs-live deadlock no longer arises.
            acquire_filing_accession_write_lock(conn, accession)
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
        # #1131 transient-vs-deterministic discrimination — see
        # ``_classify.is_transient_upsert_error``. Transient
        # (OperationalError) keeps the 1h-retry contract; deterministic
        # constraint violations tombstone the manifest so a permanently
        # broken accession stops re-fetching from SEC every tick. For
        # DEF 14A "tombstone" means writing the ingest-log row with
        # status='failed' (mirrors the empty-body + no-rows branches
        # above) + returning manifest ``tombstoned``.
        logger.exception(
            "def14a manifest parser: upsert/observation batch failed accession=%s",
            accession,
        )
        if is_transient_upsert_error(exc):
            return _failed_outcome(format_upsert_error(exc), raw_status="stored")
        try:
            with conn.transaction():
                _record_ingest_attempt(
                    conn,
                    accession_number=accession,
                    issuer_cik=issuer_cik,
                    status="failed",
                    error=format_upsert_error(exc),
                )
        except Exception:  # noqa: BLE001 — ingest-log failure shouldn't mask upsert failure
            logger.exception(
                "def14a manifest parser: ingest-log INSERT failed after upsert error accession=%s",
                accession,
            )
            return _failed_outcome(
                f"upsert+log error: {type(exc).__name__}: {exc}",
                raw_status="stored",
            )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_DEF14A,
            raw_status="stored",
            error=format_upsert_error(exc),
        )

    # Item 402(c) exec-comp augment (#1945) — runs AFTER the holdings txn has
    # committed, in its OWN savepoint (inside apply_exec_comp_best_effort), so
    # a comp parse/upsert failure rolls back comp only; the committed holdings
    # write + this accession's fairness tick are unaffected. ``siblings`` is
    # bound here because the holdings try-block succeeded (a failure would have
    # returned above). Comp is a best-effort augment — its outcome never
    # changes the ParseOutcome (still ``parsed``).
    apply_exec_comp_best_effort(
        conn,
        accession_number=accession,
        issuer_cik=issuer_cik,
        body=body,
        instrument_ids=siblings,
    )

    return ParseOutcome(
        status="parsed",
        parser_version=_PARSER_VERSION_DEF14A,
        raw_status="stored",
    )


def _def14a_fetch_url(conn: psycopg.Connection[Any], row: Any) -> str | None:  # row: ManifestRow
    """#1700 Phase 2 prefetch hook — the SINGLE proxy-doc URL the DEF 14A
    parser GETs, returned ONLY when the parser would actually fetch it.
    Mirrors EVERY pre-fetch gate in :func:`_parse_def14a` (in order):
      * missing ``primary_document_url`` → tombstone pre-fetch.
      * ``form == 'PRE 14A'`` (preliminary) → tombstone pre-fetch.
      * missing ``instrument_id`` → tombstone pre-fetch.
      * past the latest-N-per-filer cap (``def14a_within_cap``) → tombstone
        pre-fetch. This gate needs a DB read (ranks proxies via
        ``filing_events``) — hence the ``conn`` in the #1700 hook contract.
        Calls the SAME helper the parser calls (one rank source of truth).
      * #1591 — body already stored → the parser REUSES it (no fetch), so
        skip the prefetch (prevention-log #1956). Checked LAST, after the
        cheap row-local gates, so a gated-out row never pays the DB read.
    An over-broad ``None`` is always safe (serial fallback reaches the
    identical tombstone).
    """
    url = row.primary_document_url
    if not url:
        return None
    if (row.form or "").strip().upper() == "PRE 14A":
        return None
    if row.instrument_id is None:
        return None
    if not def14a_within_cap(conn, accession_number=row.accession_number, instrument_id=row.instrument_id):
        return None
    if stored_body(conn, accession_number=row.accession_number, document_kind="def14a_body") is not None:
        return None
    return url


def register() -> None:
    """Register the DEF 14A parser with the manifest worker.

    Idempotent — ``register_parser`` is last-write-wins. Called once
    from ``register_all_parsers`` at package import; re-callable
    from tests after a registry wipe.
    """
    from app.jobs.sec_manifest_worker import register_parser

    register_parser("sec_def14a", _parse_def14a, requires_raw_payload=True, fetch_url=_def14a_fetch_url)
