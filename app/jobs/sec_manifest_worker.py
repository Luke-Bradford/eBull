"""Manifest-driven SEC ingest worker (#869).

Issue #869 / spec §"#868 — manifest-driven worker"
(``docs/superpowers/specs/2026-05-04-etl-coverage-model.md``).

The worker scans ``sec_filing_manifest`` for rows in:

  - ``ingest_status='pending'``      (fresh discovery; awaiting fetch)
  - ``ingest_status='failed'`` AND
    ``next_retry_at <= NOW()``        (retry after backoff)

For each row, dispatches to a per-source parser callable. The parser
returns a ``ParseOutcome`` describing what happened; the worker
transitions the manifest row's state based on that outcome
(parsed / tombstoned / failed).

Parser registry: pluggable. Each per-form parser is registered via
``register_parser(source, callable)``. The legacy
``app/services/{def14a,form4,institutional_holdings,blockholder_filings}_ingest.py``
modules are NOT auto-wired here; rewiring them to feed off the
manifest is the scope of #873 (write-through observations + retire
periodic sync). Until then, the worker is a thin dispatcher whose
shape lets the rest of the ETL chain (#870 per-CIK polling, #871
first-install drain, #872 targeted rebuild) push pending rows
through to ``parsed`` end-to-end without touching the legacy
ingester batch-limit logic.

Rate budget: bounded externally — the worker passes through the SEC
10 req/s token bucket via the parser callables it dispatches to. The
worker itself imposes a per-tick row limit (``max_rows``) to keep
batch sizes predictable.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

import psycopg

from app.services.sec_manifest import (
    IngestStatus,
    ManifestRow,
    ManifestSource,
    iter_pending,
    iter_retryable,
    transition_status,
)

logger = logging.getLogger(__name__)


ParseStatus = Literal["parsed", "tombstoned", "failed"]


@dataclass(frozen=True)
class ParseOutcome:
    """Result of one parser invocation against a manifest row.

    The worker uses the ``status`` to drive the manifest state
    transition; ``parser_version``, ``raw_status``, ``error``, and
    ``next_retry_at`` are forwarded into ``transition_status``."""

    status: ParseStatus
    parser_version: str | None = None
    raw_status: Literal["absent", "stored", "compacted"] | None = None
    error: str | None = None
    next_retry_at: datetime | None = None


ParserFn = Callable[[psycopg.Connection[Any], ManifestRow], ParseOutcome]
"""Per-source parser contract. Receives the manifest row + a DB
connection; returns a ParseOutcome. The parser is responsible for
fetching + parsing + persisting typed-table rows. The worker handles
the manifest state transition based on the outcome."""


@dataclass(frozen=True)
class ParserSpec:
    """Registry entry for one ManifestSource.

    ``requires_raw_payload`` enforces the audit invariant from #938:
    payload-backed parsers (Form 4, 13F-HR, 13D/G, NPORT-P, DEF 14A)
    cannot transition a row to ``parsed`` while ``raw_status='absent'``.
    The worker turns such an outcome into a ``failed`` transition with
    a descriptive error rather than silently retaining unauditable
    rows. Synthesised / non-payload parsers leave the flag at False
    (default) and are unaffected.
    """

    fn: ParserFn
    requires_raw_payload: bool = False


_PARSERS: dict[ManifestSource, ParserSpec] = {}


def register_parser(
    source: ManifestSource,
    parser: ParserFn,
    *,
    requires_raw_payload: bool = False,
) -> None:
    """Register a parser callable for one ManifestSource.

    Idempotent on re-registration (last-write-wins). The legacy
    ingest services will register their callables in #873 when the
    write-through wiring lands; until then, ``run_manifest_worker``
    skips rows whose source has no registered parser (logs a debug
    line per skipped row).

    ``requires_raw_payload=True`` opts the source into the #938 audit
    invariant: a ``parsed`` outcome with ``raw_status not in
    ('stored', 'compacted')`` is rejected and the row is transitioned
    to ``failed`` instead. Use for every parser that pulls upstream
    body bytes (Form 4 XML, 13F infotable, 13D/G primary doc, DEF 14A
    HTML, NPORT-P XML). Leave at the default for synthesised /
    non-payload sources."""
    _PARSERS[source] = ParserSpec(fn=parser, requires_raw_payload=requires_raw_payload)


def _backoff_for(attempt_count: int) -> timedelta:
    """Exponential backoff for ``failed`` rows.

    Doubles per attempt, capped at 24h. We don't track attempt_count
    on the manifest yet (would need a column); for the initial cut
    we use a flat 1h backoff. ``next_retry_at`` is recomputed on each
    failure regardless."""
    return timedelta(hours=1)


@dataclass(frozen=True)
class WorkerStats:
    """Per-tick summary for observability."""

    rows_processed: int
    parsed: int
    tombstoned: int
    failed: int
    skipped_no_parser: int
    raw_payload_violations: int = 0


def run_manifest_worker(
    conn: psycopg.Connection[Any],
    *,
    source: ManifestSource | None = None,
    max_rows: int = 100,
    now: datetime | None = None,
) -> WorkerStats:
    """One worker tick: drain pending + retryable manifest rows.

    Strategy:
      1. iter_pending(source, max_rows) — newest backlog by filed_at
      2. iter_retryable(source, max_rows - pending_count) — failed
         rows past their retry window
      3. For each row, look up registered parser by source. Skip with
         a debug log if no parser is registered.
      4. Invoke parser. On exception, mark row failed with the
         exception text + 1h backoff.
      5. Otherwise transition_status from the ParseOutcome.

    ``source=None`` drains across all sources up to ``max_rows`` total.
    Per-source filtering (``source='sec_form4'``) is the operator-
    triggered path used by the targeted-rebuild job (#872).

    Returns a WorkerStats summary for logs / job_runs persistence.
    """
    if now is None:
        now = datetime.now(tz=UTC)

    rows: list[ManifestRow] = []
    rows.extend(iter_pending(conn, source=source, limit=max_rows))
    if len(rows) < max_rows:
        rows.extend(iter_retryable(conn, source=source, limit=max_rows - len(rows)))

    parsed = 0
    tombstoned = 0
    failed = 0
    skipped = 0
    raw_violations = 0

    for row in rows:
        spec = _PARSERS.get(row.source)
        if spec is None:
            logger.debug(
                "manifest worker: no parser registered for source=%s; skipping accession=%s",
                row.source,
                row.accession_number,
            )
            skipped += 1
            continue

        try:
            outcome = spec.fn(conn, row)
        except Exception as exc:  # parser-internal failure — fail loudly
            logger.exception(
                "manifest worker: parser raised for source=%s accession=%s",
                row.source,
                row.accession_number,
            )
            transition_status(
                conn,
                row.accession_number,
                ingest_status="failed",
                error=f"{type(exc).__name__}: {exc}"[:500],
                next_retry_at=now + _backoff_for(0),
            )
            failed += 1
            continue

        # #938 audit invariant: payload-backed parsers cannot transition
        # to ``parsed`` while the row's effective raw_status is
        # ``absent``. Convert to a ``failed`` transition with a
        # descriptive error so the row remains visible to the operator
        # + retry path. Silent ``parsed + absent`` would leave an
        # unauditable row in the manifest forever.
        #
        # Effective raw_status falls back to the row's existing value
        # when the parser doesn't restamp (``outcome.raw_status is
        # None``). This matches ``transition_status`` semantics — a
        # ``parsed`` transition with ``raw_status=None`` preserves the
        # row's existing column — so a rebuild/retry flow where raw
        # evidence already exists on disk doesn't get misclassified
        # as a violation. (Codex pre-push catch.)
        effective_raw_status = outcome.raw_status or row.raw_status
        if (
            outcome.status == "parsed"
            and spec.requires_raw_payload
            and effective_raw_status not in ("stored", "compacted")
        ):
            logger.error(
                "manifest worker: source=%s accession=%s parser returned parsed but "
                "effective raw_status=%r — payload-backed parsers must persist evidence; "
                "transitioning to failed for retry",
                row.source,
                row.accession_number,
                effective_raw_status,
            )
            transition_status(
                conn,
                row.accession_number,
                ingest_status="failed",
                error=(
                    "raw payload missing: parser returned parsed without storing "
                    f"the upstream body (effective raw_status={effective_raw_status!r}). "
                    "Payload-backed parsers must persist evidence (#938)."
                ),
                next_retry_at=now + _backoff_for(0),
            )
            failed += 1
            raw_violations += 1
            continue

        target_status: IngestStatus = outcome.status
        transition_status(
            conn,
            row.accession_number,
            ingest_status=target_status,
            parser_version=outcome.parser_version,
            raw_status=outcome.raw_status,
            error=outcome.error,
            next_retry_at=outcome.next_retry_at if outcome.status == "failed" else None,
        )
        if outcome.status == "parsed":
            parsed += 1
        elif outcome.status == "tombstoned":
            tombstoned += 1
        else:
            failed += 1

    return WorkerStats(
        rows_processed=len(rows),
        parsed=parsed,
        tombstoned=tombstoned,
        failed=failed,
        skipped_no_parser=skipped,
        raw_payload_violations=raw_violations,
    )


def clear_registered_parsers() -> None:
    """Test helper — wipe the parser registry between cases. NOT for
    production code paths. The registry is module-global so tests that
    register fakes leak into subsequent tests without this hook."""
    _PARSERS.clear()
