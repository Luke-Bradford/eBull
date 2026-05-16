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

import itertools
import logging
from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

import psycopg

from app.services.sec_manifest import (
    IngestStatus,
    ManifestRow,
    ManifestSource,
    iter_pending,
    iter_pending_topup,
    iter_retryable,
    iter_retryable_topup,
    transition_status,
)

logger = logging.getLogger(__name__)


# Tick counter for Phase A `lead` rotation (#1179). Module-global
# because the production scheduled tick wrapper passes
# ``tick_id=None`` and the worker must advance by exactly +1 per
# call regardless of scheduler cadence (avoids the
# ``gcd(tick_step, n) > 1`` regime that would visit only a subset
# of lead offsets). Tests inject ``tick_id`` explicitly so the
# counter is irrelevant under test.
_TICK_COUNTER = itertools.count(0)


def compute_quotas(
    sources: Sequence[ManifestSource],
    max_rows: int,
    tick_id: int,
) -> dict[ManifestSource, int]:
    """Per-source quota with tick-rotated lead (#1179).

    Returns a ``{source: slot_count}`` mapping such that
    ``sum(quotas.values()) == max_rows`` for non-empty ``sources``.
    Rotation: ``lead = tick_id % len(sources)``; the first
    ``max_rows mod n`` sources at rotated index get ``base + 1``
    slots, the rest get ``base = max_rows // n``. Every source
    receives a Phase A slot within ``n - remainder + 1`` consecutive
    ticks regardless of scheduler cadence (independent of
    ``gcd(tick_step, n)``).
    """
    n = len(sources)
    if n == 0:
        return {}
    base, remainder = divmod(max_rows, n)
    lead = tick_id % n
    return {s: base + (1 if (i - lead) % n < remainder else 0) for i, s in enumerate(sources)}


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
    # Per-source breakdown of ``skipped_no_parser``. Operators
    # reading this from job_runs / a future status endpoint can see
    # exactly which manifest sources are dropping work because no
    # parser is registered (#940). Empty when ``skipped_no_parser=0``.
    skipped_no_parser_by_source: dict[ManifestSource, int] = field(default_factory=dict)
    # Per-source breakdown of dispatched rows (#1179). Bumped once
    # per row reaching the parser dispatch entry point (i.e. rows
    # that exit via parsed / tombstoned / failed /
    # raw_payload_violations). Sum equals
    # ``rows_processed - skipped_no_parser``.
    processed_by_source: dict[ManifestSource, int] = field(default_factory=dict)


def run_manifest_worker(
    conn: psycopg.Connection[Any],
    *,
    source: ManifestSource | None = None,
    max_rows: int = 100,
    now: datetime | None = None,
    tick_id: int | None = None,
) -> WorkerStats:
    """One worker tick: drain pending + retryable manifest rows.

    Two paths:

    - ``source is None`` (scheduled tick): per-source Phase A slice
      via :func:`compute_quotas` + Phase B residual top-up. This is
      the fairness path (#1179) that prevents the globally-oldest
      source from starving every other source. ``tick_id`` rotates
      Phase A's lead window by +1 per tick — defaults to a
      process-local :data:`_TICK_COUNTER` for production callers;
      tests inject explicitly.
    - ``source is not None`` (per-source rebuild): unchanged shape;
      drains ``max_rows`` from one source (pending then retryable).

    Returns a :class:`WorkerStats` summary including a
    ``processed_by_source`` per-source breakdown.
    """
    # Normalise ``now`` BEFORE branching so the dispatch helper
    # always has a tz-aware UTC value for parser-exception +
    # raw-payload-violation backoff math (``now + _backoff_for(0)``).
    if now is None:
        now = datetime.now(tz=UTC)

    if source is not None:
        # Per-source rebuild path — unchanged shape.
        rows: list[ManifestRow] = list(iter_pending(conn, source=source, limit=max_rows))
        if len(rows) < max_rows:
            rows.extend(iter_retryable(conn, source=source, limit=max_rows - len(rows)))
        return _dispatch_rows(conn, rows, now=now)

    # Fairness path (#1179) — Phase A per-source slice + Phase B
    # top-up across the global oldest tail.
    sources = sorted(registered_parser_sources())
    n = len(sources)
    if n == 0:
        return WorkerStats(
            rows_processed=0,
            parsed=0,
            tombstoned=0,
            failed=0,
            skipped_no_parser=0,
        )

    if tick_id is None:
        tick_id = next(_TICK_COUNTER)
    quotas = compute_quotas(sources, max_rows, tick_id)

    rows = []

    # Phase A — per-source quota slice (pending first, retryable
    # within the same per-source budget).
    for s in sources:
        q = quotas[s]
        if q == 0:
            continue
        per_source: list[ManifestRow] = list(iter_pending(conn, source=s, limit=q))
        if len(per_source) < q:
            per_source.extend(iter_retryable(conn, source=s, limit=q - len(per_source)))
        rows.extend(per_source)

    # Phase B — top-up pending, then retryable, both scoped to
    # registered sources, excluding Phase A picks.
    seen: set[str] = {r.accession_number for r in rows}
    remaining = max_rows - len(rows)
    if remaining > 0:
        topup_pending = list(
            iter_pending_topup(
                conn,
                sources=sources,
                exclude_accessions=sorted(seen),
                limit=remaining,
            )
        )
        rows.extend(topup_pending)
        seen.update(r.accession_number for r in topup_pending)
        remaining = max_rows - len(rows)
    if remaining > 0:
        topup_retryable = list(
            iter_retryable_topup(
                conn,
                sources=sources,
                exclude_accessions=sorted(seen),
                limit=remaining,
            )
        )
        rows.extend(topup_retryable)

    return _dispatch_rows(conn, rows, now=now)


def _dispatch_rows(
    conn: psycopg.Connection[Any],
    rows: list[ManifestRow],
    *,
    now: datetime,
) -> WorkerStats:
    """Per-row dispatch loop shared by both worker paths.

    For each row: skip if no parser registered, else invoke parser
    and translate :class:`ParseOutcome` into a ``transition_status``
    call. Parser-internal exceptions → ``failed`` + 1h backoff.
    #938 raw-payload audit invariant fires here (payload-backed
    parsers cannot transition ``parsed + raw_status='absent'``).

    Returns a :class:`WorkerStats` summary; the caller has already
    decided WHICH rows to dispatch (fairness allocation or per-source
    rebuild).
    """
    parsed = 0
    tombstoned = 0
    failed = 0
    skipped = 0
    raw_violations = 0
    skipped_by_source: dict[ManifestSource, int] = defaultdict(int)
    processed_by_source: dict[ManifestSource, int] = defaultdict(int)

    for row in rows:
        spec = _PARSERS.get(row.source)
        if spec is None:
            logger.debug(
                "manifest worker: no parser registered for source=%s; skipping accession=%s",
                row.source,
                row.accession_number,
            )
            skipped += 1
            skipped_by_source[row.source] += 1
            continue

        # #1179: bump processed-by-source ONCE per dispatched row,
        # BEFORE parser invocation. Every code path below exits via
        # parsed / tombstoned / failed / raw-payload-violation — the
        # counter must not double-count for the raw-violation path
        # (which writes both ``failed += 1`` and ``raw_violations += 1``).
        processed_by_source[row.source] += 1

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

    # #940: surface no-parser drops at WARNING level with per-source
    # breakdown. Per-row debug logs above let operators dig in if
    # needed; the once-per-tick summary is the loud signal that real
    # work is being silently dropped because no parser is registered.
    if skipped:
        logger.warning(
            "manifest worker: skipped %d row(s) with no registered parser; per-source counts: %s",
            skipped,
            dict(sorted(skipped_by_source.items())),
        )

    return WorkerStats(
        rows_processed=len(rows),
        parsed=parsed,
        tombstoned=tombstoned,
        failed=failed,
        skipped_no_parser=skipped,
        raw_payload_violations=raw_violations,
        skipped_no_parser_by_source=dict(skipped_by_source),
        processed_by_source=dict(processed_by_source),
    )


def clear_registered_parsers() -> None:
    """Test helper — wipe the parser registry between cases. NOT for
    production code paths. The registry is module-global so tests that
    register fakes leak into subsequent tests without this hook."""
    _PARSERS.clear()


def registered_parser_sources() -> frozenset[ManifestSource]:
    """Return the set of ``ManifestSource`` values that have a parser
    registered with the worker right now.

    #935 §5: the audit endpoint at ``/coverage/manifest-parsers``
    reads this to flag manifest rows whose source has no parser and
    would therefore be silently debug-skipped on every worker tick.
    Returning a ``frozenset`` keeps the registry read-only at the
    caller boundary.
    """
    return frozenset(_PARSERS.keys())
