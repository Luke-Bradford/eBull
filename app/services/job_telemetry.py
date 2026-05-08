"""Per-item error + skip-reason telemetry for the admin Processes table.

Issue #1065 (umbrella #1064).
Spec: ``docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md``
      §Schema migrations / sql/137 + §Error display rules.

Existing scheduled jobs and the bootstrap orchestrator collapse all
mid-run failures into a single ``job_runs.error_msg`` truncated string.
The Processes drill-in renders error_class-grouped summaries for
operator triage ("12 ConnectionTimeout × CIK 320193 etc.") which
requires structured per-item data.

This module provides three primitives:

* ``record_per_item_error`` — incremental aggregation into an in-memory
  ``ErrorAggregator`` keyed by error_class. Producers (parsers, fetchers)
  call this once per failed item.
* ``record_skip`` — incremental aggregation into a skip-reason dict,
  matching ``rows_skipped_by_reason`` JSONB shape.
* ``flush_to_job_run`` — write the aggregated state back to the
  active ``job_runs`` row at run completion.

The aggregator lives per-job-run-instance; producers pass it through
their call stack. No global singleton (avoids cross-run leakage).

Producer-side example:

.. code-block:: python

    aggregator = JobTelemetryAggregator()
    for accession in accessions:
        try:
            ingest(accession)
        except ConnectionTimeout as exc:
            aggregator.record_error(
                error_class="ConnectionTimeout",
                message=str(exc),
                subject=f"CIK {cik} / {accession}",
            )
        except UnresolvedCusip:
            aggregator.record_skip("unresolved_cusip")

    flush_to_job_run(conn, run_id=run_id, agg=aggregator)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

logger = logging.getLogger(__name__)


# Cap per-class sample message to prevent JSONB row bloat. Operators
# need a representative example, not the full stack — full traces stay
# in the structured logger trail.
_MAX_SAMPLE_MESSAGE_LEN = 500


@dataclass(slots=True)
class _ErrorClassState:
    count: int = 0
    sample_message: str = ""
    last_subject: str | None = None
    last_seen_at: datetime | None = None


@dataclass(slots=True)
class JobTelemetryAggregator:
    """Per-job-run aggregator for grouped errors + skip reasons.

    Not thread-safe; producers that fan out into threads must
    serialise their writes (or own one aggregator per worker thread
    and merge before flush).
    """

    _errors: dict[str, _ErrorClassState] = field(default_factory=dict)
    _skips: dict[str, int] = field(default_factory=dict)
    _errored: int = 0

    def record_error(
        self,
        *,
        error_class: str,
        message: str,
        subject: str | None,
    ) -> None:
        """Record a single failed item.

        ``error_class`` is the operator-facing grouping key
        (e.g. ``"ConnectionTimeout"``, ``"MissingCIK"``,
        ``"Form4ParseError"``). Avoid full exception class paths — keep
        it short and intelligible.

        ``message`` is truncated to a fixed cap; the JSONB row keeps
        ONE sample per class to bound size.

        ``subject`` is the offending entity (CIK / accession /
        instrument symbol). Operators need at least one breadcrumb to
        reproduce.
        """
        state = self._errors.setdefault(error_class, _ErrorClassState())
        state.count += 1
        state.sample_message = message[:_MAX_SAMPLE_MESSAGE_LEN]
        state.last_subject = subject
        state.last_seen_at = datetime.now(UTC)
        self._errored += 1

    def record_skip(self, reason: str, count: int = 1) -> None:
        """Increment the skip-by-reason counter.

        ``reason`` is the operator-facing grouping key (e.g.
        ``"unresolved_cusip"``, ``"rate_limited"``,
        ``"pre_universe_skip"``). Adapters without per-reason
        granularity emit ``"unknown"``.
        """
        if count <= 0:
            return
        self._skips[reason] = self._skips.get(reason, 0) + count

    @property
    def rows_errored(self) -> int:
        return self._errored

    def to_error_classes_jsonb(self) -> dict[str, dict[str, Any]]:
        """Render the aggregated error state into JSONB-ready shape.

        Matches the schema documented in sql/137 header:
        ``{"<error_class>": {"count": N, "sample_message": "...",
                              "last_subject": "...",
                              "last_seen_at": "ISO-8601"}}``
        """
        result: dict[str, dict[str, Any]] = {}
        for error_class, state in self._errors.items():
            result[error_class] = {
                "count": state.count,
                "sample_message": state.sample_message,
                "last_subject": state.last_subject,
                "last_seen_at": (state.last_seen_at.isoformat() if state.last_seen_at is not None else None),
            }
        return result

    def to_skips_jsonb(self) -> dict[str, int]:
        """Render the aggregated skip state into JSONB-ready shape.

        Returns the dict copy directly — JSONB column expects
        ``{"<reason>": <count>, ...}``.
        """
        return dict(self._skips)


def flush_to_job_run(
    conn: psycopg.Connection[Any],
    *,
    run_id: int,
    agg: JobTelemetryAggregator,
) -> None:
    """Write the aggregator's state into the ``job_runs`` row.

    Caller is responsible for committing the surrounding transaction.
    Uses ``COALESCE``-style override semantics: this UPDATE replaces
    whatever was previously written (idempotent if called more than
    once, last-writer-wins).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE job_runs
               SET rows_errored = %s,
                   error_classes = %s,
                   rows_skipped_by_reason = %s
             WHERE run_id = %s
            """,
            (
                agg.rows_errored,
                Jsonb(agg.to_error_classes_jsonb()),
                Jsonb(agg.to_skips_jsonb()),
                run_id,
            ),
        )


__all__ = [
    "JobTelemetryAggregator",
    "flush_to_job_run",
]
