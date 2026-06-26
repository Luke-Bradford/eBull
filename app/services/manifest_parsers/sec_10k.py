"""10-K manifest-worker parser adapter (#1151).

Wraps the existing pure-function parser
``extract_business_section`` / ``extract_business_sections`` +
table-writers ``upsert_business_summary`` /
``upsert_business_sections`` from ``app.services.business_summary``
so the generic manifest worker can drive 10-K Item 1 ingest one
accession at a time.

Pre-#1151 the legacy bulk path
``business_summary.ingest_business_summaries`` scanned
``filing_events`` for the newest 10-K per instrument and processed
them in batches with its own backoff/quarantine machinery. That
path still works (no breakage in this PR) but the manifest worker
is the future-facing single-writer pattern from the #869 spec. As
the manifest worker drains its backlog, the legacy job becomes
redundant and can be retired in a follow-up.

ParseOutcome contract:

  * ``status='parsed'`` + ``raw_status='stored'`` — success path.
    Raw HTML in ``filing_raw_documents``; one
    ``instrument_business_summary`` row per share-class sibling;
    one ``instrument_business_summary_sections`` row per subsection
    per sibling. Also returned on the "newer accession is already
    present" suppression branch — the DB state is already
    correct, so the manifest's drain succeeds without a body write.
  * ``status='tombstoned'`` — fetch returned non-200/empty body,
    or parser couldn't extract Item 1 and (for 10-K/A) the prior
    plain 10-K fallback also missed.
  * ``status='failed'`` — transient error (fetch raised, store_raw
    error, deterministic-vs-transient discrimination on upsert).
    Worker schedules a 1h backoff retry per ``_FAILED_RETRY_DELAY``.

Raw-payload invariant (#938): registered with
``requires_raw_payload=True`` so the worker refuses to mark a row
``parsed`` when ``raw_status='absent'``. ``store_raw`` runs in a
savepoint BEFORE parse + upsert so the invariant holds whether
parsing succeeds or raises.

Share-class fan-out: 10-K Item 1 is an issuer-level narrative. The
parser resolves the share-class siblings from
``row.cik`` (via ``siblings_for_issuer_cik``) and writes the body +
sections per sibling so GOOG and GOOGL both render the same
narrative on per-instrument reads.

Option C filed_at gate (#1151): ``upsert_business_summary``'s
conditional ``ON CONFLICT`` accepts only filings whose
``(filed_at, source_accession)`` tuple is greater-or-equal to the
incumbent's. The manifest worker drains ``filed_at ASC`` (oldest
first); without the gate the operator would briefly see the 2018
Item 1 narrative mid-drain before the 2024 update fires. The gate
returns ``'suppressed'`` for stale arrivals; the adapter treats
that as a successful drain (no body write needed).
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, time, timedelta
from typing import Any

import psycopg

from app.config import settings
from app.providers.concurrent_fetch import fetch_document_texts
from app.providers.implementations.sec_edgar import SecFilingsProvider, archive_dir_url
from app.services.business_summary import (
    _MIN_BODY_LEN,
    _find_prior_plain_10k,
    extract_business_section,
    extract_business_sections,
    upsert_business_sections,
    upsert_business_summary,
)
from app.services.dimensional_facts import discover_xbrl_files, extract_dimensional_facts
from app.services.dimensional_facts_store import replace_accession_rows
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


# 10-K parser version. Independent of the parser version recorded by
# the legacy bulk ingester so a parser-version bump in either path
# can rewash without colliding with the other. Mirror the convention
# in def14a.py (``_PARSER_VERSION_DEF14A``) + insider_345.py
# (``_PARSER_VERSION_FORM4``).
# v2 (#554): dimensional XBRL step added — bump drives the
# sec_rebuild backfill across all 10-K accessions.
_PARSER_VERSION_10K = "10k-v2"

# Explicit 1h backoff. Duplicated from the worker's internal
# ``_backoff_for(0)`` value — see eight_k.py for the rationale
# (importing the private worker symbol couples to internal layout).
_FAILED_RETRY_DELAY = timedelta(hours=1)


def _failed_outcome(error: str, raw_status: Any = None) -> Any:
    """Build a ``failed`` ParseOutcome with a 1h backoff applied.

    Setting ``next_retry_at`` here so the worker doesn't immediately
    retry on the next tick — mirrors the pattern in eight_k.py and
    def14a.py."""
    from app.jobs.sec_manifest_worker import ParseOutcome

    return ParseOutcome(
        status="failed",
        parser_version=_PARSER_VERSION_10K,
        raw_status=raw_status,
        error=error,
        next_retry_at=datetime.now(tz=UTC) + _FAILED_RETRY_DELAY,
    )


def _fetch_html(
    url: str,
) -> str | None:
    """Fetch a primary document over the rate-limited SEC client.

    Returns the body on success, ``None`` on empty body (provider's
    own 404/410/empty signal). Raises on transport-level failures so
    the adapter can map them to a ``_failed_outcome``."""
    with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider:
        return provider.fetch_document_text(url)


class _DimensionalFetchError(Exception):
    """Transport-level failure in the dimensional-facts step — maps to
    a ``failed`` outcome so the whole manifest row retries (idempotent:
    the Item 1 upsert is filed_at-gate-suppressed on the retry and the
    dimensional write is delete-then-insert)."""


def _xbrl_index_locator(
    *,
    accession: str,
    issuer_cik: str | None,
    primary_document_url: str,
) -> tuple[str, str, str | None] | None:
    """Shared (index_url, archive_base, discovery_primary_name) for an accession's
    XBRL, or ``None`` when the issuer CIK is missing / non-numeric (no usable
    archive path → no dimensional facts).

    SINGLE source of the index + base URLs so the serial
    :func:`_fetch_dimensional_facts` and the #1730 prefetch chain
    :func:`_sec10k_xbrl_prefetch` can NEVER drift — a one-character mismatch
    silently misses the prefetch cache. ``archive_dir_url`` is the same builder
    :meth:`SecFilingsProvider.fetch_filing_index` routes through, so the chain's
    cached index URL byte-matches the serial path's ``fetch_filing_index`` lookup.

    ``discovery_primary_name`` is the basename passed to ``discover_xbrl_files``
    for its size-preference rule: full-submission ``.txt`` names carry no useful
    stem, so pass ``None`` and let discovery fall back to its size rules."""
    if issuer_cik is None or not issuer_cik.strip().isdigit():
        return None
    raw_id = accession.replace("-", "")
    base = archive_dir_url(raw_id, int(issuer_cik))
    primary_basename = primary_document_url.rsplit("/", 1)[-1]
    primary_name = None if primary_basename.lower().endswith(".txt") else primary_basename
    return base + "index.json", base, primary_name


def _fetch_dimensional_facts(
    *,
    accession: str,
    issuer_cik: str | None,
    primary_document_url: str,
) -> list[Any]:
    """Fetch + extract dimensional XBRL facts for THIS accession (#554).

    Always targets ``accession``'s own archive — never the Item-1
    fallback's ``chosen_accession`` (a 10-K/A without Item 1 borrows
    the prior 10-K's narrative, but its XBRL, when present, is its
    own; spec §D1).

    Pure I/O + parse; no DB. Returns ``[]`` on the structural no-XBRL
    path (pre-mandate filings, 404'd index, empty instance). Raises
    :class:`_DimensionalFetchError` on transport failures and lets
    parse errors (``ValueError`` etc.) propagate for the caller's
    degrade-to-parsed handling.

    The instance is parse-and-drop — NOT retained in
    ``filing_raw_documents`` (raw-payload scope narrowing #470: every
    extracted field lands in SQL).
    """
    # Archive base + index URL + discovery primary-name from (cik, accession) —
    # via the SHARED locator so the serial path here and the #1730 prefetch chain
    # (``_sec10k_xbrl_prefetch``) resolve byte-identical URLs (a mismatch silently
    # misses the prefetch cache). NEVER from the primary URL's directory: legacy
    # manifest rows carry the full-submission ``.txt`` URL (``…/data/{cik}/{accn}.txt``),
    # whose dirname is the CIK root, so every artifact fetch would 404 (caught live
    # on GME/HD/JPM in the dev backfill).
    locator = _xbrl_index_locator(accession=accession, issuer_cik=issuer_cik, primary_document_url=primary_document_url)
    if locator is None:
        logger.warning(
            "sec_10k manifest parser: accession=%s has no usable issuer cik; skipping dimensional facts",
            accession,
        )
        return []
    _index_url, base, primary_name = locator

    with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider:
        try:
            raw_index = provider.fetch_filing_index(accession, issuer_cik=issuer_cik)
        except Exception as exc:  # noqa: BLE001 — transport; retry via worker backoff
            raise _DimensionalFetchError(f"dimensional index fetch error: {exc}") from exc
        if raw_index is None:
            return []

        refs = discover_xbrl_files(raw_index, primary_document_name=primary_name)
        if refs is None:
            return []

        def _fetch(name: str) -> bytes | None:
            try:
                text = provider.fetch_document_text(base + name)
            except Exception as exc:  # noqa: BLE001 — transport; retry via worker backoff
                raise _DimensionalFetchError(f"dimensional fetch error {name}: {exc}") from exc
            # SEC EDGAR serves ASCII-clean XML; the provider decodes to
            # str, so re-encode for lxml (which refuses str inputs that
            # carry an encoding declaration).
            return text.encode("utf-8") if text else None

        instance = _fetch(refs.instance_name)
        if instance is None:
            return []
        label = _fetch(refs.label_name) if refs.label_name else None
        if refs.definition_name == refs.label_name:
            definition = label  # xsd fallback serves both — one fetch
        else:
            definition = _fetch(refs.definition_name) if refs.definition_name else None

    return extract_dimensional_facts(instance, label, definition, accession=accession)


def _parse_sec_10k(
    conn: psycopg.Connection[Any],
    row: Any,  # ManifestRow — forward-ref to avoid circular import
) -> Any:  # ParseOutcome — forward-ref
    """Manifest-worker parser for one 10-K (or 10-K/A) accession.

    Steps:

    1. Validate URL + instrument_id (tombstone on missing).
    2. Fetch primary doc. Exception → failed. Empty → tombstone.
    3. ``store_raw`` in a savepoint (#938 invariant).
    4. Parse Item 1 via ``extract_business_section``.
    5. If body None / too short AND form is 10-K/A: fall back to the
       prior plain 10-K (mirrors legacy #534 path). Otherwise tombstone.
    6. Extract sections from the chosen HTML (best-effort; sections=()
       on extractor failure).
    7. Resolve share-class siblings (#1117).
    8. Inside ONE batched savepoint: upsert per sibling under the
       conditional filed_at gate (Option C). A sections upsert failure
       in a nested savepoint logs + degrades to blob-only. Deterministic
       upsert error → tombstone with the savepoint rolling back partial
       fan-out state.
    Dimensional XBRL facts (#554) run as step 2.5 — after store_raw,
    BEFORE the narrative parse — so an Item-1 tombstone cannot
    suppress segments (spec §D1 independence in BOTH directions; the
    MSFT FY2025 dev backfill caught the original post-narrative
    ordering): fetch THIS accession's instance (+ lab/def linkbases)
    through the throttled provider, extract, delete-then-insert per
    sibling in a separate savepoint. Degrades to
    continue-without-segments on parse/write bugs; transport errors
    retry the whole row.
    """
    from app.jobs.sec_manifest_worker import ParseOutcome

    accession = row.accession_number
    url = row.primary_document_url
    instrument_id = row.instrument_id
    form = (row.form or "").strip().upper()
    filed_at = row.filed_at
    issuer_cik = row.cik or _CIK_MISSING_SENTINEL

    if not url:
        logger.warning(
            "sec_10k manifest parser: accession=%s has no primary_document_url; tombstoning",
            accession,
        )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_10K,
            error="missing primary_document_url",
        )
    if instrument_id is None:
        logger.warning(
            "sec_10k manifest parser: accession=%s has no instrument_id; tombstoning",
            accession,
        )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_10K,
            error="missing instrument_id",
        )

    # 1. Fetch primary doc.
    try:
        html = _fetch_html(url)
    except Exception as exc:  # noqa: BLE001 — transient fetch errors retry via worker backoff
        logger.warning(
            "sec_10k manifest parser: fetch raised accession=%s url=%s: %s",
            accession,
            url,
            exc,
        )
        return _failed_outcome(f"fetch error: {exc}")

    if not html:
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_10K,
            error="empty or non-200 fetch",
        )

    # 2. Store raw BEFORE parse so the #938 invariant holds even when
    # the parse later raises. Savepoint isolates a partial write from
    # the worker's outer transaction.
    try:
        with conn.transaction():
            store_raw(
                conn,
                accession_number=accession,
                document_kind="primary_doc",
                payload=html,
                parser_version=_PARSER_VERSION_10K,
                source_url=url,
            )
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "sec_10k manifest parser: store_raw failed accession=%s",
            accession,
        )
        return _failed_outcome(f"store_raw error: {exc}")

    # 2.5. Dimensional XBRL facts (#554, spec §D1 step 2). Runs BEFORE
    # the narrative parse so an Item-1 tombstone (e.g. a filer whose
    # 10-K defeats the Item 1 extractor — MSFT FY2025 in the dev
    # backfill) cannot suppress segments: the spec invariant is that
    # narrative and dimensional failures are independent in BOTH
    # directions. Failure semantics: transport error → failed (whole
    # row retries; idempotent — Item 1 is filed_at-gate-suppressed,
    # dimensional write is delete-then-insert; a narrative may become
    # visible before segments on the retry, intended); structural
    # no-XBRL → zero rows; parse/write error → WARN +
    # continue-without-segments (a segment bug must NOT regress the
    # narrative), transient DB error excepted (→ failed).
    if filed_at is None:
        logger.warning(
            "sec_10k manifest parser: accession=%s has no filed_at; skipping dimensional facts",
            accession,
        )
    else:
        try:
            dimensional = _fetch_dimensional_facts(
                accession=accession,
                issuer_cik=None if issuer_cik == _CIK_MISSING_SENTINEL else issuer_cik,
                primary_document_url=url,
            )
        except _DimensionalFetchError as exc:
            return _failed_outcome(str(exc), raw_status="stored")
        except Exception:  # noqa: BLE001 — degrade: narrative pipeline continues
            logger.warning(
                "sec_10k manifest parser: dimensional extraction failed accession=%s (continuing without segments)",
                accession,
                exc_info=True,
            )
            dimensional = None

        if dimensional is not None:
            try:
                with conn.transaction():
                    for sibling_iid in _resolve_siblings(conn, instrument_id=instrument_id, issuer_cik=issuer_cik):
                        replace_accession_rows(
                            conn,
                            instrument_id=sibling_iid,
                            source_accession=accession,
                            form_type=form,
                            filed_at=filed_at,
                            parser_version=_PARSER_VERSION_10K,
                            facts=dimensional,
                        )
            except Exception as exc:  # noqa: BLE001
                if is_transient_upsert_error(exc):
                    return _failed_outcome(format_upsert_error(exc), raw_status="stored")
                logger.warning(
                    "sec_10k manifest parser: dimensional write failed accession=%s (continuing without segments)",
                    accession,
                    exc_info=True,
                )

    # 3. Parse Item 1. The bare-call-after-committed-savepoint rule
    # (PR #1126) — wrap the next expression that can raise so an
    # unhandled exception cannot leave the worker's outer tx aborted
    # before transition_status runs.
    try:
        body = extract_business_section(html)
    except Exception as exc:  # noqa: BLE001 — see PR #1129 pinned rule
        logger.exception(
            "sec_10k manifest parser: parse raised accession=%s",
            accession,
        )
        return _failed_outcome(f"parse error: {exc}", raw_status="stored")

    chosen_accession = accession
    chosen_filed_at = filed_at
    chosen_html = html

    # 4. 10-K/A fallback path (mirrors legacy #534).
    if body is None or len(body) < _MIN_BODY_LEN:
        if form != "10-K/A":
            return ParseOutcome(
                status="tombstoned",
                parser_version=_PARSER_VERSION_10K,
                raw_status="stored",
                error="no Item 1 marker (plain 10-K)",
            )

        prior = _find_prior_plain_10k(
            conn,
            instrument_id=instrument_id,
            before_accession=accession,
        )
        if prior is None:
            return ParseOutcome(
                status="tombstoned",
                parser_version=_PARSER_VERSION_10K,
                raw_status="stored",
                error="10-K/A missing Item 1 and no prior plain 10-K",
            )

        fallback_acc, fallback_url, fallback_filing_date = prior
        logger.info(
            "sec_10k manifest parser: 10-K/A accession=%s -> prior plain 10-K accession=%s",
            accession,
            fallback_acc,
        )

        try:
            fallback_html = _fetch_html(fallback_url)
        except Exception as exc:  # noqa: BLE001 — original raw is stored already; retry as failed.
            logger.warning(
                "sec_10k manifest parser: 10-K/A fallback fetch raised accession=%s url=%s: %s",
                fallback_acc,
                fallback_url,
                exc,
            )
            return _failed_outcome(f"fallback fetch error: {exc}", raw_status="stored")

        if not fallback_html:
            return ParseOutcome(
                status="tombstoned",
                parser_version=_PARSER_VERSION_10K,
                raw_status="stored",
                error="10-K/A fallback returned empty body",
            )

        # Store fallback raw under fallback_acc so audit/rewash can
        # find it under the accession the parent row will point at.
        try:
            with conn.transaction():
                store_raw(
                    conn,
                    accession_number=fallback_acc,
                    document_kind="primary_doc",
                    payload=fallback_html,
                    parser_version=_PARSER_VERSION_10K,
                    source_url=fallback_url,
                )
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "sec_10k manifest parser: fallback store_raw failed accession=%s",
                fallback_acc,
            )
            return _failed_outcome(f"fallback store_raw error: {exc}", raw_status="stored")

        try:
            fallback_body = extract_business_section(fallback_html)
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "sec_10k manifest parser: 10-K/A fallback parse raised accession=%s",
                fallback_acc,
            )
            return _failed_outcome(f"fallback parse error: {exc}", raw_status="stored")

        if fallback_body is None or len(fallback_body) < _MIN_BODY_LEN:
            return ParseOutcome(
                status="tombstoned",
                parser_version=_PARSER_VERSION_10K,
                raw_status="stored",
                error="10-K/A fallback also missed Item 1",
            )

        body = fallback_body
        chosen_accession = fallback_acc
        chosen_filed_at = (
            datetime.combine(fallback_filing_date, time.min, tzinfo=UTC) if fallback_filing_date is not None else None
        )
        chosen_html = fallback_html

    assert body is not None  # narrowed by the fallback branch logic above

    # 5. Extract sections once from the chosen HTML. Sections are a
    # best-effort enrichment — a parser bug here must NOT escape after
    # store_raw committed (otherwise raw_status='stored' preservation
    # is violated per 8-K Codex round 2 BLOCKING).
    try:
        sections = extract_business_sections(chosen_html)
    except Exception:  # noqa: BLE001
        logger.warning(
            "sec_10k manifest parser: sections extractor raised accession=%s "
            "(blob will still write; sections degrade to empty)",
            chosen_accession,
            exc_info=True,
        )
        sections = ()

    # 6. Fan out across share-class siblings (#1117). ONE batched
    # savepoint wraps sibling resolution + the whole write batch so a
    # mid-batch failure unwinds partial state cleanly. Mirrors
    # def14a._parse_def14a.
    try:
        with conn.transaction():
            siblings = _resolve_siblings(conn, instrument_id=instrument_id, issuer_cik=issuer_cik)
            for sibling_iid in siblings:
                outcome = upsert_business_summary(
                    conn,
                    instrument_id=sibling_iid,
                    body=body,
                    source_accession=chosen_accession,
                    filed_at=chosen_filed_at,
                )
                if outcome == "suppressed":
                    logger.debug(
                        "sec_10k manifest parser: filed_at gate suppressed sibling=%s accession=%s (incumbent newer)",
                        sibling_iid,
                        chosen_accession,
                    )
                    continue
                if not sections:
                    continue
                # Nested savepoint absorbs a sections failure so the
                # parent blob write survives. Matches legacy "sections
                # are best-effort; blob-only fallback acceptable"
                # semantics (business_summary.py:1774).
                try:
                    with conn.transaction():
                        upsert_business_sections(
                            conn,
                            instrument_id=sibling_iid,
                            source_accession=chosen_accession,
                            sections=sections,
                        )
                except Exception:  # noqa: BLE001
                    logger.warning(
                        "sec_10k manifest parser: sections upsert failed sibling=%s accession=%s "
                        "(blob already stored; rendering degrades to blob-only)",
                        sibling_iid,
                        chosen_accession,
                        exc_info=True,
                    )
    except Exception as exc:  # noqa: BLE001
        # #1131 transient-vs-deterministic discrimination — a transient
        # OperationalError can self-resolve on the next tick; everything
        # else won't and should tombstone so the worker stops re-fetching.
        # The outer savepoint already unwound any partial sibling writes
        # by the time we reach this branch.
        logger.exception(
            "sec_10k manifest parser: fan-out batch failed accession=%s",
            chosen_accession,
        )
        if is_transient_upsert_error(exc):
            return _failed_outcome(format_upsert_error(exc), raw_status="stored")
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_10K,
            raw_status="stored",
            error=format_upsert_error(exc),
        )

    return ParseOutcome(
        status="parsed",
        parser_version=_PARSER_VERSION_10K,
        raw_status="stored",
    )


def _sec10k_fetch_url(conn: Any, row: Any) -> str | None:  # conn unused; row: ManifestRow
    """#1591 Part 2 prefetch hook — the SINGLE primary-document URL the 10-K
    parser GETs (the business-summary HTML), returned ONLY when the parser
    would actually fetch it. Mirrors :func:`_parse_sec_10k`'s pre-fetch
    tombstone gates (missing ``primary_document_url`` / missing
    ``instrument_id``); both are row-local so ``conn`` is unused (part of the
    shared #1700 hook contract).

    Scope is the PRIMARY doc only. The 10-K also fetches XBRL linkbase
    artifacts (``index.json`` + instance + label/def) via a SEPARATE provider
    in :func:`_fetch_dimensional_facts` after an independent index discovery —
    the pass-2 ``expand_urls`` (keyed off the primary HTML body) can't reach
    them. As of #1730 those are prefetched by the independent
    :func:`_sec10k_xbrl_prefetch` ``prefetch_chain`` hook, so on a re-drain BOTH
    the primary doc AND the XBRL chain overlap across rows. An over-broad
    ``None`` is always safe (serial fallback reaches the identical
    fetch/tombstone).

    ``primary_doc`` is born-compacted (SWEPT #1617) so there is no stored body
    to mirror a #1591-PR1 reuse gate against — the 10-K always (re-)fetches /
    rehydrates from source.
    """
    url = row.primary_document_url
    if not url or row.instrument_id is None:
        return None
    return url


def _sec10k_xbrl_prefetch(rows: list[Any], provider: Any) -> dict[str, str]:  # rows: list[ManifestRow]
    """#1730 ``prefetch_chain`` hook — concurrently prefetch the 10-K XBRL chain
    (archive ``index.json`` + instance/label/def linkbases) for a batch of rows so
    a re-drain overlaps them across rows instead of fetching ~4 docs SERIALLY per
    row inside dispatch (:func:`_fetch_dimensional_facts`). Parse-and-drop (#470 —
    artifacts never stored), so this is pure concurrency, NOT reuse.

    Mirrors the parser's pre-XBRL gates (return nothing for a row the parser would
    skip before the dimensional step): ``primary_document_url`` + ``instrument_id``
    present (``_parse_sec_10k`` step-1 tombstone), ``filed_at`` present (step-2.5
    guard), ``cik`` present + numeric (``_xbrl_index_locator`` returns None
    otherwise). The primary-fetch / ``store_raw`` outcomes are NOT
    row-local-knowable, so a row the serial parser later tombstones (primary 404)
    or fails (transient / store_raw) wastes its prefetched XBRL — a rate-budget cost
    only (parse-and-drop; the serial parser still gates), accepted (rare on the
    last-3-annual horizon).

    Two concurrent rounds against the shared ≤10 req/s throttle:
      1. ``index.json`` for every gated row.
      2. instance/label/def for every successfully-discovered index.

    Returns ``{url: body}`` (index bodies + artifact bodies) for the tick cache.
    The serial :func:`_fetch_dimensional_facts` then does 0 HTTP:
    ``fetch_filing_index`` hits the cached index (#1730 cache-aware), each artifact
    hits via ``fetch_document_text``, and roles (instance vs label vs def) are
    re-discovered from the cached index — no role info crosses this hook, so a flat
    URL→body cache is sufficient. The locator is SHARED with the serial path so the
    URLs cannot drift."""
    # accession -> (index_url, base, primary_name) for rows past the cheap gates.
    index_url_to_loc: dict[str, tuple[str, str | None]] = {}
    for row in rows:
        if not row.primary_document_url or row.instrument_id is None or row.filed_at is None:
            continue
        loc = _xbrl_index_locator(
            accession=row.accession_number,
            issuer_cik=row.cik,
            primary_document_url=row.primary_document_url,
        )
        if loc is None:
            continue
        index_url, base, primary_name = loc
        index_url_to_loc[index_url] = (base, primary_name)
    if not index_url_to_loc:
        return {}

    cache: dict[str, str] = {}
    artifact_urls: set[str] = set()
    index_bodies = fetch_document_texts(provider, set(index_url_to_loc))
    for index_url, body in index_bodies.items():
        if body is None:
            continue
        cache[index_url] = body
        base, primary_name = index_url_to_loc[index_url]
        try:
            raw_index = json.loads(body)
        except json.JSONDecodeError:
            continue
        if not isinstance(raw_index, dict):
            continue
        refs = discover_xbrl_files(raw_index, primary_document_name=primary_name)
        if refs is None:
            continue
        # A set auto-dedups the def==label single-fetch case the serial path also
        # collapses (sec_10k.py: ``if refs.definition_name == refs.label_name``).
        for name in (refs.instance_name, refs.label_name, refs.definition_name):
            if name:
                artifact_urls.add(base + name)

    if artifact_urls:
        artifact_bodies = fetch_document_texts(provider, artifact_urls)
        cache.update({url: body for url, body in artifact_bodies.items() if body is not None})
    return cache


def register() -> None:
    """Register the 10-K parser with the manifest worker.

    Idempotent — ``register_parser`` is last-write-wins, so calling
    this twice with the same callable is a no-op. Called once from
    ``app.services.manifest_parsers.register_all_parsers`` at package
    import time, and re-callable from tests after a registry wipe.
    """
    from app.jobs.sec_manifest_worker import register_parser

    register_parser(
        "sec_10k",
        _parse_sec_10k,
        requires_raw_payload=True,
        fetch_url=_sec10k_fetch_url,
        prefetch_chain=_sec10k_xbrl_prefetch,
    )
