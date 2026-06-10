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
decoded text, immune to transfer-encoding/charset variance.
"""

from __future__ import annotations

import hashlib
import logging
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal

import psycopg
import psycopg.rows

from app.config import settings
from app.services.raw_filings import DocumentKind

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 100
"""Bounds per-tx detoast cost (10-K avg ~3.5 MB -> ~350 MB worst-case
detoast per batch for the server-side hash) and the WAL burst."""

# The document kinds the sweep may destroy payloads for. MUST stay
# disjoint from the rewash registry (structural test) — a registered
# rewash parser reads stored bodies, which a sweep would null.
SWEPT_DOCUMENT_KINDS: frozenset[DocumentKind] = frozenset({"primary_doc"})

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
