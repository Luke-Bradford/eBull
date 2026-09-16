"""Per-item error + skip-reason + progress telemetry for the admin Processes table.

Issue #1065 (umbrella #1064) — PR1 added the error + skip aggregator.
Issue #1071 (umbrella #1064) — PR3 extends the aggregator with the A3
operator-amendment progress producer API: ``set_target``,
``record_processed``, ``record_warning``, ``maybe_flush``. These wire
the sql/140 columns (``processed_count``, ``target_count``,
``last_progress_at``, ``warnings_count``, ``warning_classes``) added in
PR2.

Spec: ``docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md``
      §Schema migrations / sql/137 + sql/140 + §Error display rules +
      §Operator-amendment round 1 / A3.

Existing scheduled jobs and the bootstrap orchestrator collapse all
mid-run failures into a single ``job_runs.error_msg`` truncated string.
The Processes drill-in renders error_class-grouped summaries for
operator triage ("12 ConnectionTimeout × CIK 320193 etc.") which
requires structured per-item data.

This module provides:

* ``record_per_item_error`` / ``record_error`` — incremental aggregation
  into an in-memory ``ErrorAggregator`` keyed by error_class. Producers
  (parsers, fetchers) call this once per failed item.
* ``record_skip`` — incremental aggregation into a skip-reason dict,
  matching ``rows_skipped_by_reason`` JSONB shape.
* ``record_processed`` — bumps the live progress ticker (sql/140
  ``processed_count`` + ``last_progress_at``).
* ``set_target`` — pins the bounded denominator (sql/140
  ``target_count``); leave unset for unbounded sweeps so the FE renders
  ``Processed: N`` only.
* ``record_warning`` — non-fatal per-item issue (rate-limited retry
  successful, partial parse fallback) aggregated into the sql/140
  ``warning_classes`` JSONB.
* ``maybe_flush`` / ``flush_to_job_run`` — write the aggregated state
  back to the active ``job_runs`` row mid-flight (cooperative tick) or
  at run completion.

The aggregator lives per-job-run-instance; producers pass it through
their call stack. No global singleton (avoids cross-run leakage).

Producer-side example:

.. code-block:: python

    agg = JobTelemetryAggregator()
    agg.set_target(len(accessions))
    for accession in accessions:
        try:
            ingest(accession)
            agg.record_processed()
        except RateLimited as exc:
            agg.record_warning(
                error_class="RateLimited",
                message=str(exc),
                subject=f"CIK {cik}",
            )
            backoff_and_retry()
        except ConnectionTimeout as exc:
            agg.record_error(
                error_class="ConnectionTimeout",
                message=str(exc),
                subject=f"CIK {cik} / {accession}",
            )
        except UnresolvedCusip:
            agg.record_skip("unresolved_cusip")
        agg.maybe_flush(conn, run_id=run_id)  # flush every 5s

    flush_to_job_run(conn, run_id=run_id, agg=agg)
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


def _sanitise_sample(message: str) -> str:
    """Truncate to the cap and strip NUL, which PostgreSQL text cannot hold.

    ⚠ The NUL strip is not defensive decoration. A producer's ``message`` is
    routinely an exception's ``str()``, and an exception raised over raw
    upstream bytes (a truncated XML body, a mis-decoded filing) can carry
    ``\\x00``.

    ⚠ **POSTGRES rejects it, not the driver** — measured, because the
    driver-side message for plain text ("A string literal cannot contain NUL")
    is not what this path hits. The JSON encoder escapes the byte happily and
    the SERVER refuses the cast::

        psycopg.errors.UntranslatableCharacter: unsupported Unicode escape sequence
        DETAIL:  \\u0000 cannot be converted to text.

    That fails the WHOLE flush — every class, every skip reason — over one bad
    sample. The aggregate is worth more than the byte.
    """
    return message[:_MAX_SAMPLE_MESSAGE_LEN].replace("\x00", "")


@dataclass(slots=True)
class _ErrorClassState:
    count: int = 0
    sample_message: str = ""
    last_subject: str | None = None
    last_seen_at: datetime | None = None


# Default cooperative-flush interval — producers calling ``maybe_flush``
# on every item write at most once per ``DEFAULT_FLUSH_INTERVAL_SECONDS``
# so a tight loop does not hammer the DB. 5s is the sweet spot: well
# under the 30s `useProcesses` poll cadence so the FE sees fresh ticks,
# well over typical ingest item latency so we don't UPDATE per-item.
DEFAULT_FLUSH_INTERVAL_SECONDS = 5.0


@dataclass(slots=True)
class JobTelemetryAggregator:
    """Per-job-run aggregator for grouped errors + skip reasons + progress.

    Not thread-safe; producers that fan out into threads must
    serialise their writes (or own one aggregator per worker thread
    and merge before flush).

    Progress fields (sql/140) are independent of error fields (sql/137)
    so a producer that only cares about the live ticker can call
    ``record_processed`` without ever touching the error API, and
    vice versa.
    """

    _errors: dict[str, _ErrorClassState] = field(default_factory=dict)
    _warnings: dict[str, _ErrorClassState] = field(default_factory=dict)
    _skips: dict[str, int] = field(default_factory=dict)
    _errored: int = 0
    _warned: int = 0
    _processed: int = 0
    _target: int | None = None
    _last_progress_at: datetime | None = None
    _last_flush_at: datetime | None = None

    # ------------------------------------------------------------------
    # Producer API — errors + skips (sql/137)
    # ------------------------------------------------------------------

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
        state.sample_message = _sanitise_sample(message)
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

    # ------------------------------------------------------------------
    # Producer API — progress + warnings (sql/140)
    # ------------------------------------------------------------------

    def set_target(self, target: int) -> None:
        """Pin the bounded denominator for the live progress ticker.

        Leave unset for unbounded sweeps (e.g. SEC drain "anything
        since T?"); the FE then renders ``Processed: N`` without a
        percentage.

        ``target`` of zero is allowed — represents "nothing to do" and
        the FE renders ``0/0 (–)``. Negative targets raise.
        """
        if target < 0:
            raise ValueError(f"target must be non-negative, got {target}")
        self._target = int(target)

    def record_processed(self, count: int = 1) -> None:
        """Bump the live progress ticker.

        Bumps both ``processed_count`` and ``last_progress_at`` (the
        producer's heartbeat — PR8 stale-detection reads this field to
        flag a row whose ``last_progress_at < now() - threshold``).
        """
        if count <= 0:
            return
        self._processed += int(count)
        self._last_progress_at = datetime.now(UTC)

    def record_warning(
        self,
        *,
        error_class: str,
        message: str,
        subject: str | None,
    ) -> None:
        """Record a non-fatal per-item warning.

        Distinct from ``record_error``: a warning means the producer
        recovered (rate-limit retry succeeded, partial-parse fallback
        used). Aggregates onto sql/140 ``warning_classes`` JSONB with
        the same shape ``record_error`` uses for ``error_classes``.
        """
        state = self._warnings.setdefault(error_class, _ErrorClassState())
        state.count += 1
        state.sample_message = _sanitise_sample(message)
        state.last_subject = subject
        state.last_seen_at = datetime.now(UTC)
        self._warned += 1

    # ------------------------------------------------------------------
    # Read-only views
    # ------------------------------------------------------------------

    @property
    def has_processed_state(self) -> bool:
        """True once ``record_processed`` has actually bumped the ticker.

        Read by :func:`flush_to_job_run` to decide whether this producer owns
        ``processed_count`` / ``last_progress_at`` — see its docstring for why
        the three progress columns are guarded INDEPENDENTLY rather than by one
        combined flag."""
        return self._processed > 0

    @property
    def has_target_state(self) -> bool:
        """True once ``set_target`` has been called.

        ⚠ Deliberately NOT ``bool(self._target)``: ``set_target(0)`` is a
        meaningful denominator ("nothing to do", rendered ``0/0``), so the guard
        has to be "was it set", not truthiness."""
        return self._target is not None

    @property
    def rows_errored(self) -> int:
        return self._errored

    @property
    def warnings_count(self) -> int:
        return self._warned

    @property
    def processed_count(self) -> int:
        return self._processed

    @property
    def target_count(self) -> int | None:
        return self._target

    @property
    def last_progress_at(self) -> datetime | None:
        return self._last_progress_at

    def to_error_classes_jsonb(self) -> dict[str, dict[str, Any]]:
        """Render the aggregated error state into JSONB-ready shape.

        Matches the schema documented in sql/137 header:
        ``{"<error_class>": {"count": N, "sample_message": "...",
                              "last_subject": "...",
                              "last_seen_at": "ISO-8601"}}``
        """
        return _state_dict_to_jsonb(self._errors)

    def to_warning_classes_jsonb(self) -> dict[str, dict[str, Any]]:
        """Render the aggregated warning state into JSONB-ready shape.

        Same shape as ``to_error_classes_jsonb`` so adapters can reuse
        the parser.
        """
        return _state_dict_to_jsonb(self._warnings)

    def to_skips_jsonb(self) -> dict[str, int]:
        """Render the aggregated skip state into JSONB-ready shape.

        Returns the dict copy directly — JSONB column expects
        ``{"<reason>": <count>, ...}``.
        """
        return dict(self._skips)

    # ------------------------------------------------------------------
    # Cooperative flush
    # ------------------------------------------------------------------

    def maybe_flush(
        self,
        conn: psycopg.Connection[Any],
        *,
        run_id: int,
        flush_interval_seconds: float = DEFAULT_FLUSH_INTERVAL_SECONDS,
    ) -> bool:
        """Flush to the ``job_runs`` row at most every N seconds.

        Returns True if a flush actually occurred. Producer pattern:

        .. code-block:: python

            for item in items:
                ingest(item)
                agg.record_processed()
                agg.maybe_flush(conn, run_id=run_id)

        First call always flushes (``_last_flush_at`` is None) so the
        operator sees motion immediately on row open.
        """
        now = datetime.now(UTC)
        if self._last_flush_at is not None:
            elapsed = (now - self._last_flush_at).total_seconds()
            if elapsed < flush_interval_seconds:
                return False
        flush_to_job_run(conn, run_id=run_id, agg=self)
        self._last_flush_at = now
        return True


def _state_dict_to_jsonb(
    states: dict[str, _ErrorClassState],
) -> dict[str, dict[str, Any]]:
    """Shared shape converter for error_classes + warning_classes JSONB."""
    result: dict[str, dict[str, Any]] = {}
    for key, state in states.items():
        result[key] = {
            "count": state.count,
            "sample_message": state.sample_message,
            "last_subject": state.last_subject,
            "last_seen_at": (state.last_seen_at.isoformat() if state.last_seen_at is not None else None),
        }
    return result


def flush_to_job_run(
    conn: psycopg.Connection[Any],
    *,
    run_id: int,
    agg: JobTelemetryAggregator,
) -> None:
    """Write the aggregator's state into the ``job_runs`` row.

    Caller is responsible for committing the surrounding transaction.
    The sql/137 error/skip fields and the sql/140 warning fields are
    replaced wholesale — last-writer-wins, idempotent on re-flush. One
    producer per run is the contract; this is not a partial-writer API.

    ⚠⚠ THE THREE PROGRESS COLUMNS ARE NOT THIS WRITER'S UNLESS IT HAS THE
    STATE (#3111 slice 2). ``processed_count``, ``target_count`` and
    ``last_progress_at`` are also written by
    :class:`app.services.job_heartbeat.JobRunHeartbeat`, which
    ``_tracked_job`` installs for every tracked job that has a run id and a
    background pool. Writing them unconditionally meant an ERROR-ONLY
    producer — which is what the first real wiring of this aggregator is —
    flushed ``processed_count=0, target_count=NULL, last_progress_at=NULL``
    straight over a live liveness stamp that #2274 shipped and that
    stale-detection reads.

    No production producer existed when this module was written, so the trap
    never fired; it would have fired on the FIRST wiring and on every later
    one. Each progress column is therefore written only when this aggregator
    carries THAT piece of state, via ``COALESCE(%s, <column>)`` with NULL
    meaning "not mine, leave it".

    ⚠ Guarded per column, not by one combined flag: a target-only producer
    must not zero ``processed_count``, and a processed-only one must not NULL
    the denominator another writer set. ``last_progress_at`` follows
    ``processed_count`` because ``record_processed`` is what moves both.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE job_runs
               SET rows_errored           = %s,
                   error_classes          = %s,
                   rows_skipped_by_reason = %s,
                   processed_count        = COALESCE(%s, processed_count),
                   target_count           = COALESCE(%s, target_count),
                   last_progress_at       = COALESCE(%s, last_progress_at),
                   warnings_count         = %s,
                   warning_classes        = %s
             WHERE run_id = %s
            """,
            (
                agg.rows_errored,
                Jsonb(agg.to_error_classes_jsonb()),
                Jsonb(agg.to_skips_jsonb()),
                agg.processed_count if agg.has_processed_state else None,
                agg.target_count if agg.has_target_state else None,
                agg.last_progress_at if agg.has_processed_state else None,
                agg.warnings_count,
                Jsonb(agg.to_warning_classes_jsonb()),
                run_id,
            ),
        )


__all__ = [
    "DEFAULT_FLUSH_INTERVAL_SECONDS",
    "JobTelemetryAggregator",
    "flush_to_job_run",
]
