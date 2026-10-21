"""Raw-payload retention sweep + SHA-256 reproducibility guard (#1014).

Spec: ``docs/specs/etl/2026-06-10-raw-payload-retention-sweep.md``.

``filing_raw_documents`` ``primary_doc`` payloads (10-K / 8-K primary
documents, 16 GB raw on dev) are write-only: no rewash parser is
registered for the kind (pinned by
``tests/test_raw_payload_retention.py``), and the manifest re-parse
path re-fetches from EDGAR unconditionally. The sweep nulls the
payload of PARSED 10-K / 8-K rows after recording a SHA-256 of the
bytes being destroyed, and flips the manifest ``raw_status`` to
``'compacted'`` (the value sql/118 reserved for exactly this).

Destruction is opt-in: ``SWEPT_MANIFEST_SOURCES`` is a drop-list —
anything not listed defaults to keep-always. ``def14a_body`` is
consciously excluded (it HAS a stored-body rewash consumer).

Connection ownership (prevention-log §"orchestrator-of-N autocommit"):
the sweep OWNS its connection — opens ``autocommit=True`` and wraps
each batch in ``with conn.transaction()`` (a real top-level
BEGIN/COMMIT per batch). Mirror of
:func:`app.services.filing_events_cleanup.cleanup_skip_tier_filing_events`.

Hash semantics: SHA-256 of the UTF-8 encoding of the TEXT payload as
stored. Server-side ``encode(sha256(convert_to(payload, 'UTF8')), 'hex')``
equals Python ``hashlib.sha256(text.encode('utf-8')).hexdigest()``
(verified on dev PG 17, 2026-06-10) — so the sweep never ships
multi-MB payloads to the client, and the re-fetch verifier hashes the
decoded text, immune to transfer-encoding/charset variance. The text is
the provider's canonical body: the SEC provider strips the edge's
injected script (#3899, ``strip_sec_edge_script``), which otherwise
breaks every match.
"""

from __future__ import annotations

import hashlib
import logging
import time
from collections import Counter
from collections.abc import Callable, Collection
from dataclasses import dataclass, field
from typing import Any, Literal

import psycopg
import psycopg.errors
import psycopg.rows

from app.config import settings
from app.services.raw_filings import SWEPT_DOCUMENT_KINDS, DocumentKind
from app.services.sec_manifest import ManifestSource, is_filing_index_url

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 100
"""Bounds per-tx detoast cost (10-K avg ~3.5 MB -> ~350 MB worst-case
detoast per batch for the server-side hash) and the WAL burst."""

# ``SWEPT_DOCUMENT_KINDS`` is canonical in ``raw_filings`` (#1615) — the
# same set ``store_raw`` born-compacts at ingest, so this retroactive
# sweep and the source-side born-compaction can never diverge. Re-export
# kept so existing importers (and the retention-classification test)
# resolve it here too.
__all__ = [
    "SWEPT_DOCUMENT_KINDS",
    "VERIFIED_SWEEP_TARGETS",
    "rehydrate_raw_document",
    "sweep_raw_payloads",
    "sweep_verified",
    "verified_body",
]

# Manifest sources whose parsed accessions are sweep-eligible. Keyed on
# ``sec_filing_manifest.source`` (CHECK-constrained enum, collapses
# amendments: sec_10k covers 10-K + 10-K/A) rather than the free-text
# ``form`` column. Opt-in destruction: a new source (sec_10q, ...)
# defaults to keep-always until explicitly added here. 13F-HR
# primary_doc rows (~33 MB) are excluded — not worth touching, and
# ``raw_status`` is accession-scoped while a 13F stores two kinds.
SWEPT_MANIFEST_SOURCES: frozenset[Literal["sec_10k", "sec_8k"]] = frozenset({"sec_10k", "sec_8k"})


class RawPayloadIntegrityError(RuntimeError):
    """Re-fetched bytes do not hash-match the recorded payload_sha256.

    SEC silently changed the document under the same accession — the
    operator must adjudicate; never auto-overwrite."""


@dataclass(frozen=True)
class RawPayloadSweepSummary:
    """Outcome of one sweep run."""

    rows_swept: int
    batches: int
    bytes_reclaimed: int
    by_source: dict[str, int] = field(default_factory=dict)
    dry_run: bool = False


# Per-batch statement. Chained data-modifying CTEs, one snapshot, one
# transaction:
#   batch   — pick candidates; lock ONLY the raw rows (FOR UPDATE OF r
#             SKIP LOCKED). The manifest is not locked: a concurrent
#             sec_rebuild flipping parsed->pending is benign — the
#             worker re-parse fetches from primary_document_url
#             unconditionally and store_raw re-populates the payload.
#   swept   — null the payload; re-checks ``payload IS NOT NULL`` under
#             the row lock so a store_raw that won the race is never
#             clobbered with a hash-of-nothing.
#   flagged — manifest stored->compacted; 'compacted' rows (re-swept
#             after a store_raw without parser transition) are already
#             correct, hence the raw_status='stored' guard.
# RETURNING evaluates the NEW row, so the OLD byte_count comes from the
# batch CTE, never from the UPDATE.
# Eligibility requires raw_status IN ('stored','compacted'): a split
# row (parsed + raw_status='absent' + payload present) is already an
# invariant violation — excluded, surfaces as a dry-run discrepancy.
_SWEEP_BATCH_SQL = """
WITH batch AS (
    SELECT r.accession_number, r.byte_count, m.source
    FROM filing_raw_documents r
    JOIN sec_filing_manifest m ON m.accession_number = r.accession_number
    WHERE r.document_kind = %(kind)s
      AND r.payload IS NOT NULL
      AND m.source = ANY(%(sources)s)
      AND m.ingest_status = 'parsed'
      AND m.raw_status IN ('stored', 'compacted')
    ORDER BY r.accession_number
    LIMIT %(batch)s
    FOR UPDATE OF r SKIP LOCKED
),
swept AS (
    UPDATE filing_raw_documents r
    SET payload_sha256   = encode(sha256(convert_to(r.payload, 'UTF8')), 'hex'),
        payload_swept_at = NOW(),
        payload          = NULL
    FROM batch b
    WHERE r.accession_number = b.accession_number
      AND r.document_kind = %(kind)s
      AND r.payload IS NOT NULL
    RETURNING r.accession_number
),
flagged AS (
    UPDATE sec_filing_manifest m
    SET raw_status = 'compacted'
    FROM swept s
    WHERE m.accession_number = s.accession_number
      AND m.raw_status = 'stored'
    RETURNING m.accession_number
)
SELECT b.accession_number, b.source, b.byte_count
FROM batch b
JOIN swept s ON s.accession_number = b.accession_number
"""

_DRY_RUN_SQL = """
SELECT m.source, COUNT(*) AS rows, COALESCE(SUM(r.byte_count), 0) AS bytes
FROM filing_raw_documents r
JOIN sec_filing_manifest m ON m.accession_number = r.accession_number
WHERE r.document_kind = %(kind)s
  AND r.payload IS NOT NULL
  AND m.source = ANY(%(sources)s)
  AND m.ingest_status = 'parsed'
  AND m.raw_status IN ('stored', 'compacted')
GROUP BY m.source
"""


def sweep_raw_payloads(
    *,
    database_url: str | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    dry_run: bool = True,
) -> RawPayloadSweepSummary:
    """Null the payload of sweep-eligible ``filing_raw_documents`` rows
    in bounded batches, recording a SHA-256 per row first.

    ``dry_run`` defaults TRUE at this entrypoint too (Codex ckpt-2):
    a destructive service must be opt-in even for REPL / test callers,
    not just at the manual-trigger layer.

    Idempotent: the ``payload IS NOT NULL`` predicate means a drained
    DB sweeps 0. Terminates: the candidate set strictly shrinks per
    batch (a swept row cannot re-match).
    """
    if batch_size < 1:
        raise ValueError(f"batch_size must be >= 1, got {batch_size}")

    url = database_url or settings.database_url
    sources = sorted(SWEPT_MANIFEST_SOURCES)

    tally: Counter[str] = Counter()
    rows_swept = 0
    bytes_reclaimed = 0
    batches = 0

    with psycopg.connect(url, autocommit=True) as conn:
        if dry_run:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                for kind in sorted(SWEPT_DOCUMENT_KINDS):
                    cur.execute(_DRY_RUN_SQL, {"kind": kind, "sources": sources})
                    for row in cur.fetchall():
                        tally[str(row["source"])] += int(row["rows"])
                        rows_swept += int(row["rows"])
                        bytes_reclaimed += int(row["bytes"])
            logger.info(
                "raw_payload_retention_sweep dry_run: eligible=%d bytes=%d by_source=%s",
                rows_swept,
                bytes_reclaimed,
                dict(tally),
            )
            return RawPayloadSweepSummary(
                rows_swept=rows_swept,
                batches=0,
                bytes_reclaimed=bytes_reclaimed,
                by_source=dict(tally),
                dry_run=True,
            )

        for kind in sorted(SWEPT_DOCUMENT_KINDS):
            while True:
                with conn.transaction(), conn.cursor() as cur:
                    cur.execute(
                        _SWEEP_BATCH_SQL,
                        {"kind": kind, "sources": sources, "batch": batch_size},
                    )
                    swept = cur.fetchall()
                if not swept:
                    break
                batches += 1
                rows_swept += len(swept)
                for _accession, source, byte_count in swept:
                    tally[str(source)] += 1
                    bytes_reclaimed += int(byte_count)

    logger.info(
        "raw_payload_retention_sweep: rows_swept=%d batches=%d bytes_reclaimed=%d by_source=%s",
        rows_swept,
        batches,
        bytes_reclaimed,
        dict(tally),
    )
    return RawPayloadSweepSummary(
        rows_swept=rows_swept,
        batches=batches,
        bytes_reclaimed=bytes_reclaimed,
        by_source=dict(tally),
    )


@dataclass(frozen=True)
class RehydrateOutcome:
    accession_number: str
    document_kind: DocumentKind
    status: Literal["already_present", "restored"]


def rehydrate_raw_document(
    conn: psycopg.Connection[Any],
    *,
    accession_number: str,
    document_kind: DocumentKind,
    fetch_text: Callable[[str], str],
) -> RehydrateOutcome:
    """Restore a swept payload from source, verifying byte-identity.

    ``fetch_text`` is injected (production callers pass the
    SEC-rate-limited provider fetch) so this module owns no HTTP.

    Raises :class:`RawPayloadIntegrityError` when the re-fetched bytes
    do not hash-match the recorded ``payload_sha256`` — SEC silently
    changed the document; the operator must adjudicate. Never
    auto-overwrites on mismatch.

    Caller owns the transaction (service-no-commit rule); all I/O
    happens BEFORE the writes.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT payload, payload_sha256, source_url
            FROM filing_raw_documents
            WHERE accession_number = %s AND document_kind = %s
            """,
            (accession_number, document_kind),
        )
        row = cur.fetchone()
    if row is None:
        raise ValueError(f"no filing_raw_documents row for accession={accession_number!r} kind={document_kind!r}")
    if row["payload"] is not None:
        return RehydrateOutcome(
            accession_number=accession_number,
            document_kind=document_kind,
            status="already_present",
        )
    expected_sha = row["payload_sha256"]
    if expected_sha is None:
        # chk_swept_rows_carry_hash forbids this shape at the DB layer;
        # reaching here means the constraint was dropped or bypassed.
        raise RuntimeError(f"swept row accession={accession_number!r} kind={document_kind!r} carries no payload_sha256")
    source_url = row["source_url"]
    if not source_url:
        raise RuntimeError(f"swept row accession={accession_number!r} kind={document_kind!r} has no source_url")

    # I/O outside the write — fetch + hash before touching any row.
    text = fetch_text(str(source_url))
    actual_sha = hashlib.sha256(text.encode("utf-8")).hexdigest()
    if actual_sha != expected_sha:
        logger.warning(
            "rehydrate hash mismatch accession=%s kind=%s expected=%s actual=%s url=%s",
            accession_number,
            document_kind,
            expected_sha,
            actual_sha,
            source_url,
        )
        raise RawPayloadIntegrityError(
            f"re-fetched payload for accession={accession_number!r} kind={document_kind!r} "
            f"does not match recorded sha256 (expected {expected_sha}, got {actual_sha}); "
            "SEC may have silently changed the document — operator adjudication required"
        )

    # Hash matches — restore. payload_sha256 stays (still true);
    # payload_swept_at clears (row is live again); fetched_at is NOT
    # refreshed (preserves the original "when did SEC publish this?"
    # timestamp, mirroring rewash's _bump_parser_version rationale).
    # The WHERE re-pins payload_sha256 to the hash this fetch was
    # verified against (Codex ckpt-2): a concurrent store_raw +
    # re-sweep between our SELECT and this UPDATE can install a NEWER
    # hash — writing our (stale-verified) bytes under it would pair
    # bytes with a hash they were never checked against. rowcount==0
    # then means "row moved on" — report already_present, don't write.
    result = conn.execute(
        """
        UPDATE filing_raw_documents
        SET payload = %s, payload_swept_at = NULL
        WHERE accession_number = %s AND document_kind = %s
          AND payload IS NULL AND payload_sha256 = %s
        """,
        (text, accession_number, document_kind, expected_sha),
    )
    if result.rowcount == 0:
        # Lost a race with store_raw / another rehydrate / a re-sweep
        # under a newer hash — the row has moved on; nothing written.
        return RehydrateOutcome(
            accession_number=accession_number,
            document_kind=document_kind,
            status="already_present",
        )
    conn.execute(
        """
        UPDATE sec_filing_manifest
        SET raw_status = 'stored'
        WHERE accession_number = %s AND raw_status = 'compacted'
        """,
        (accession_number,),
    )
    return RehydrateOutcome(
        accession_number=accession_number,
        document_kind=document_kind,
        status="restored",
    )


# ---------------------------------------------------------------------------
# #3899 — verified bodies and the verified sweep
# Spec: docs/specs/etl/2026-10-21-3899-def14a-pre14a-body-sweep.md (D0, D2).
# ---------------------------------------------------------------------------

FetchText = Callable[[str], str | None]
"""``SecFilingsProvider.fetch_document_text``'s shape: the canonical body on 2xx
(edge script stripped), ``None`` on 404 / 410, raise on any other failure."""

UnavailableReason = Literal["no_row", "no_source_url", "index_url", "no_hash", "fetch_error", "not_found", "mismatch"]


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class VerifiedBody:
    """``present`` (live payload), ``fetched`` (swept row re-fetched and hash-matched)
    or ``unavailable`` (``reason`` says why). ``sha256`` is the hash of exactly
    ``text``, so anything derived from ``text`` can be keyed on it."""

    status: Literal["present", "fetched", "unavailable"]
    text: str | None = None
    sha256: str | None = None
    reason: UnavailableReason | None = None


def _unavailable(reason: UnavailableReason) -> VerifiedBody:
    return VerifiedBody(status="unavailable", reason=reason)


def verified_body(
    conn: psycopg.Connection[Any],
    *,
    accession_number: str,
    document_kind: DocumentKind,
    fetch_text: FetchText,
) -> VerifiedBody:
    """The body of one raw row, without depending on it being retained.

    A live payload is returned as stored. A swept row is re-fetched from its
    ``source_url`` and returned only when the fetch hash-matches
    ``payload_sha256`` — the bytes the row was hashed from. A body fetched from a
    filing-index URL (#3590) is never a document, so it is ``unavailable``.

    Never writes: restoring the payload would only be swept again. Any exception
    from ``fetch_text`` is ``fetch_error``, so the caller decides whether to retry.
    """
    row = conn.execute(
        """
        SELECT payload, payload_sha256, source_url
        FROM filing_raw_documents
        WHERE accession_number = %s AND document_kind = %s
        """,
        (accession_number, document_kind),
    ).fetchone()
    if row is None:
        return _unavailable("no_row")
    payload, expected_sha, source_url = row
    if is_filing_index_url(source_url):
        return _unavailable("index_url")
    if payload is not None:
        return VerifiedBody(status="present", text=payload, sha256=_sha256(payload))
    if not source_url:
        return _unavailable("no_source_url")
    if not expected_sha:
        return _unavailable("no_hash")
    try:
        text = fetch_text(str(source_url))
    except Exception:  # noqa: BLE001 — any fetch failure is "unavailable now", never a parse input
        logger.warning(
            "verified_body: fetch failed accession=%s kind=%s", accession_number, document_kind, exc_info=True
        )
        return _unavailable("fetch_error")
    if not text:
        return _unavailable("not_found")
    actual_sha = _sha256(text)
    if actual_sha != expected_sha:
        logger.warning(
            "verified_body: hash mismatch accession=%s kind=%s expected=%s actual=%s",
            accession_number,
            document_kind,
            expected_sha,
            actual_sha,
        )
        return _unavailable("mismatch")
    return VerifiedBody(status="fetched", text=text, sha256=actual_sha)


# The verified arm sweeps exactly these (source, kind) pairs — never the cross-product
# of sources and SWEPT_DOCUMENT_KINDS the unverified #1014 arm walks, so a verified run
# cannot touch the sec_10k / sec_8k backlog.
VERIFIED_SWEEP_TARGETS: dict[ManifestSource, DocumentKind] = {
    "sec_def14a": "def14a_body",
    "sec_pre14a": "pre14a_body",
}

# Manifest states a verified sweep may select. ``tombstoned`` is selected only when a
# caller names it: the tombstoned cohort waits for the operator (spec §4 "Authority").
VERIFIED_SWEEP_STATUSES: frozenset[str] = frozenset({"parsed", "tombstoned"})
DEFAULT_VERIFIED_SWEEP_STATUSES: tuple[str, ...] = ("parsed",)

VerifiedSweepOutcome = Literal["swept", "mismatch", "fetch_error", "not_found", "race"]

_VERIFIED_PAGE_SIZE = 50

# Candidates in keyset order. The stored digest is computed server-side so a mismatch
# is decided before any write (spec D2). Filing-index URLs are dropped in Python with
# the production predicate.
_VERIFIED_CANDIDATES_SQL = """
SELECT r.accession_number, r.document_kind, m.ingest_status, r.source_url, r.byte_count,
       encode(sha256(convert_to(r.payload, 'UTF8')), 'hex') AS digest
FROM filing_raw_documents r
JOIN sec_filing_manifest m ON m.accession_number = r.accession_number
WHERE (m.source, r.document_kind) IN (SELECT * FROM unnest(%(sources)s::text[], %(kinds)s::text[]))
  AND r.payload IS NOT NULL
  AND m.ingest_status = ANY(%(statuses)s)
  AND m.raw_status IN ('stored', 'compacted')
  AND r.accession_number > %(after)s
ORDER BY r.accession_number
LIMIT %(limit)s
"""

_VERIFIED_DRY_RUN_SQL = """
SELECT r.document_kind, m.ingest_status, r.source_url, r.byte_count
FROM filing_raw_documents r
JOIN sec_filing_manifest m ON m.accession_number = r.accession_number
WHERE (m.source, r.document_kind) IN (SELECT * FROM unnest(%(sources)s::text[], %(kinds)s::text[]))
  AND r.payload IS NOT NULL
  AND m.ingest_status = ANY(%(statuses)s)
  AND m.raw_status IN ('stored', 'compacted')
"""

# Compare-and-null for one row: null only if the payload is still exactly the bytes the
# fetch matched, under the URL that was fetched. Same CTE shape as _SWEEP_BATCH_SQL.
_VERIFIED_SWEEP_ROW_SQL = """
WITH swept AS (
    UPDATE filing_raw_documents
    SET payload_sha256 = %(sha)s, payload_swept_at = NOW(), payload = NULL
    WHERE accession_number = %(acc)s
      AND document_kind = %(kind)s
      AND payload IS NOT NULL
      AND source_url = %(url)s
      AND encode(sha256(convert_to(payload, 'UTF8')), 'hex') = %(sha)s
    RETURNING accession_number
),
flagged AS (
    UPDATE sec_filing_manifest m
    SET raw_status = 'compacted'
    FROM swept s
    WHERE m.accession_number = s.accession_number
      AND m.raw_status = 'stored'
    RETURNING m.accession_number
)
SELECT accession_number FROM swept
"""


@dataclass(frozen=True)
class VerifiedSweepSummary:
    """One verified-sweep pass. ``outcomes`` counts every attempted row; ``unswept``
    lists ``(accession, kind, outcome)`` for every attempted row left live.
    ``complete`` means the pass reached the end of the candidates; otherwise resume
    with ``after=last_accession``. A dry run fills only ``eligible_*``."""

    dry_run: bool
    statuses: tuple[str, ...]
    attempted: int = 0
    bytes_reclaimed: int = 0
    outcomes: dict[str, int] = field(default_factory=dict)
    unswept: list[tuple[str, str, str]] = field(default_factory=list)
    last_accession: str | None = None
    complete: bool = False
    eligible_rows: dict[str, int] = field(default_factory=dict)
    eligible_bytes: dict[str, int] = field(default_factory=dict)


def _validate_statuses(statuses: Collection[str]) -> tuple[str, ...]:
    chosen = tuple(sorted(set(statuses)))
    if not chosen or not set(chosen) <= VERIFIED_SWEEP_STATUSES:
        raise ValueError(f"statuses must be a non-empty subset of {sorted(VERIFIED_SWEEP_STATUSES)}, got {statuses!r}")
    return chosen


def sweep_verified(
    *,
    fetch_text: FetchText,
    database_url: str | None = None,
    statuses: Collection[str] = DEFAULT_VERIFIED_SWEEP_STATUSES,
    max_rows: int = 5000,
    max_seconds: float = 1800.0,
    after: str = "",
    dry_run: bool = True,
    _monotonic: Callable[[], float] = time.monotonic,
) -> VerifiedSweepSummary:
    """Null each eligible ``VERIFIED_SWEEP_TARGETS`` payload only after re-fetching it
    from EDGAR and matching its SHA-256 to the stored bytes (spec D2).

    One call is one keyset pass from ``after``. A pass stops when ``max_rows`` rows have
    been attempted or ``max_seconds`` has elapsed, checked before each row, so the
    overrun is one row's work. A failed row is passed over and never re-attempted
    within the pass. ``dry_run`` defaults TRUE, as for ``sweep_raw_payloads``.

    The manifest is not locked (the #1014 concurrency contract, ``_SWEEP_BATCH_SQL``):
    a concurrent ``sec_rebuild`` flipping a row to pending is benign, because the
    re-parse re-fetches the body just proven identical to EDGAR's.
    """
    chosen = _validate_statuses(statuses)
    if max_rows < 1:
        raise ValueError(f"max_rows must be >= 1, got {max_rows}")
    if max_seconds <= 0:
        raise ValueError(f"max_seconds must be > 0, got {max_seconds}")

    url = database_url or settings.database_url
    sources = list(VERIFIED_SWEEP_TARGETS)
    kinds = [VERIFIED_SWEEP_TARGETS[s] for s in sources]
    params: dict[str, Any] = {"sources": sources, "kinds": kinds, "statuses": list(chosen)}

    with psycopg.connect(url, autocommit=True) as conn:
        if dry_run:
            rows_by: Counter[str] = Counter()
            bytes_by: Counter[str] = Counter()
            for kind, status, source_url, byte_count in conn.execute(_VERIFIED_DRY_RUN_SQL, params).fetchall():
                if is_filing_index_url(source_url):
                    continue
                key = f"{kind}/{status}"
                rows_by[key] += 1
                bytes_by[key] += int(byte_count)
            summary = VerifiedSweepSummary(
                dry_run=True, statuses=chosen, eligible_rows=dict(rows_by), eligible_bytes=dict(bytes_by)
            )
            logger.info("raw_payload_verified_sweep dry_run: %s", summary)
            return summary

        started = _monotonic()
        outcomes: Counter[str] = Counter()
        unswept: list[tuple[str, str, str]] = []
        attempted = 0
        bytes_reclaimed = 0
        cursor = after
        complete = False
        while True:
            page = conn.execute(
                _VERIFIED_CANDIDATES_SQL, {**params, "after": cursor, "limit": _VERIFIED_PAGE_SIZE}
            ).fetchall()
            if not page:
                complete = True
                break
            stopped = False
            for acc, kind, _status, source_url, byte_count, digest in page:
                if attempted >= max_rows or _monotonic() - started >= max_seconds:
                    stopped = True
                    break
                cursor = acc
                if is_filing_index_url(source_url):
                    continue
                attempted += 1
                outcome = _sweep_one_verified(
                    conn, accession=acc, kind=kind, source_url=source_url, digest=digest, fetch_text=fetch_text
                )
                outcomes[outcome] += 1
                if outcome == "swept":
                    bytes_reclaimed += int(byte_count)
                else:
                    unswept.append((acc, kind, outcome))
            if stopped:
                break

    summary = VerifiedSweepSummary(
        dry_run=False,
        statuses=chosen,
        attempted=attempted,
        bytes_reclaimed=bytes_reclaimed,
        outcomes=dict(outcomes),
        unswept=unswept,
        last_accession=cursor or None,
        complete=complete,
    )
    logger.info(
        "raw_payload_verified_sweep: attempted=%d outcomes=%s bytes_reclaimed=%d complete=%s last=%s",
        attempted,
        dict(outcomes),
        bytes_reclaimed,
        complete,
        summary.last_accession,
    )
    return summary


def _sweep_one_verified(
    conn: psycopg.Connection[Any],
    *,
    accession: str,
    kind: str,
    source_url: str,
    digest: str,
    fetch_text: FetchText,
) -> VerifiedSweepOutcome:
    try:
        text = fetch_text(source_url)
    except Exception:  # noqa: BLE001 — a failed fetch leaves the row live; listed, retried next pass
        logger.warning("verified sweep: fetch failed accession=%s url=%s", accession, source_url, exc_info=True)
        return "fetch_error"
    if not text:
        return "not_found"
    sha = _sha256(text)
    if sha != digest:
        logger.warning(
            "verified sweep: mismatch accession=%s kind=%s stored=%s fetched=%s", accession, kind, digest, sha
        )
        return "mismatch"
    try:
        with conn.transaction():
            conn.execute("SET LOCAL statement_timeout = '60s'")
            swept = conn.execute(
                _VERIFIED_SWEEP_ROW_SQL, {"sha": sha, "acc": accession, "kind": kind, "url": source_url}
            ).fetchall()
    except psycopg.errors.QueryCanceled:
        return "race"
    return "swept" if swept else "race"
