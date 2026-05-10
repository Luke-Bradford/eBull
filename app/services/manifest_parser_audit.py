"""Manifest-parser audit (#935 §5).

Surfaces ``sec_filing_manifest`` rows whose ``source`` has no
registered parser. Pre-#935 §5 the worker debug-skipped these on
every tick — no operator-visible signal, so an entire source could
go un-ingested forever and the only trace was a per-row debug log
that no human reads.

This service is the persistent counterpart to the per-tick
``WorkerStats.skipped_no_parser_by_source`` map. It joins the live
manifest table against a caller-supplied parser registry to answer:

  * Which ``ManifestSource`` values have a parser registered in the
    caller's process?
  * For each source: how many manifest rows are pending /
    retryable / parsed / failed / tombstoned?
  * If the source has NO parser, how many rows are stuck?

Process-boundary caveat (Codex pre-push round 1): the parser
registry (``sec_manifest_worker._PARSERS``) is module-global, so a
caller in process X sees only what X has registered. When the API
process queries this audit, it reads its OWN registry, which may
diverge from the worker's. Pre-#873 nothing registers parsers in
either process so the question is moot. Once #873 lands the worker-
side registration, the audit's ``has_registered_parser`` field is
honest about which process's view it represents — see the
``ManifestParserSourceRow`` docstring. A future PR should publish
the worker's registry into a DB table so the API can read truth
rather than its own (possibly empty) cache.

The output is the input to ``GET /coverage/manifest-parsers`` and
to the data-engineer playbook for diagnosing a stuck ingest lane.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, get_args

import psycopg

from app.services.sec_manifest import IngestStatus, ManifestSource

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ManifestParserSourceRow:
    """One row per manifest source in the audit report.

    ``has_registered_parser`` is computed against the
    ``registered_sources`` argument the caller passed to
    :func:`compute_manifest_parser_audit`. The caller is expected
    to populate that set from its own process's
    ``sec_manifest_worker._PARSERS`` registry. Cross-process
    correctness depends on registration happening at module-import
    time (so both API and worker processes see the same registry);
    see module docstring for the process-boundary caveat.
    """

    source: str
    has_registered_parser: bool
    rows_pending: int
    rows_fetched: int
    rows_parsed: int
    rows_failed: int
    rows_tombstoned: int

    @property
    def stuck_no_parser(self) -> int:
        """Rows the worker would silently debug-skip every tick.

        Sums ``pending + fetched + failed`` for a source with no
        parser. Codex pre-push round 1 caught the original logic:
        ``run_manifest_worker`` calls ``iter_retryable`` which
        includes ``failed`` rows past their backoff window, so
        excluding ``failed`` from the stuck count would under-report
        the worker's actual no-parser drop rate. ``parsed`` and
        ``tombstoned`` are terminal — they don't contribute.
        """
        if self.has_registered_parser:
            return 0
        return self.rows_pending + self.rows_fetched + self.rows_failed


@dataclass(frozen=True)
class ManifestParserAuditReport:
    """Aggregate manifest-parser audit."""

    sources: list[ManifestParserSourceRow]

    @property
    def total_stuck_no_parser(self) -> int:
        return sum(r.stuck_no_parser for r in self.sources)


def compute_manifest_parser_audit(
    conn: psycopg.Connection[Any],
    *,
    registered_sources: frozenset[str],
) -> ManifestParserAuditReport:
    """Build the audit report.

    ``registered_sources`` is the set of source names the worker has
    a parser registered for right now (read from
    ``registered_parser_sources()`` at the API boundary so the
    service is testable without importing the worker's module
    state).

    Caller owns ``conn`` — read-only, no commit.
    """
    # Group by (source, ingest_status) so a single scan covers every
    # source. LEFT JOIN against the full ManifestSource Literal so a
    # source with zero manifest rows still appears (it's a "wired but
    # unused" lane, distinct from "wired and accumulating").
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT source, ingest_status, COUNT(*)
              FROM sec_filing_manifest
             GROUP BY source, ingest_status
            """,
        )
        rows = cur.fetchall()

    # Initialise every known source to zero counts. ``ManifestSource``
    # is a ``Literal`` — pull the runtime values so a future addition
    # to the type automatically shows up in the audit without code
    # changes here.
    known_sources: tuple[str, ...] = get_args(ManifestSource)
    known_statuses: tuple[str, ...] = get_args(IngestStatus)
    by_source: dict[str, dict[str, int]] = {s: {st: 0 for st in known_statuses} for s in known_sources}
    for source, status, count in rows:
        # Defensive: a manifest row whose source isn't in the Literal
        # (would only happen via direct DB edit) still surfaces as an
        # unknown bucket so the operator sees it.
        if source not in by_source:
            by_source[source] = {st: 0 for st in known_statuses}
        # Defensive (Codex pre-push round 2): the CHECK constraint on
        # ``sec_filing_manifest.ingest_status`` rules out unknown
        # values today, but a constraint relaxation or a direct DB
        # edit could still produce one. Skip the unknown status
        # rather than KeyError on the inner dict — the operator gets
        # the rest of the report instead of a 500.
        if status not in by_source[source]:
            logger.warning(
                "manifest_parser_audit: unknown ingest_status=%r on source=%s; skipping",
                status,
                source,
            )
            continue
        by_source[source][status] = int(count)

    out: list[ManifestParserSourceRow] = []
    for source in sorted(by_source.keys()):
        counts = by_source[source]
        out.append(
            ManifestParserSourceRow(
                source=source,
                has_registered_parser=source in registered_sources,
                rows_pending=counts.get("pending", 0),
                rows_fetched=counts.get("fetched", 0),
                rows_parsed=counts.get("parsed", 0),
                rows_failed=counts.get("failed", 0),
                rows_tombstoned=counts.get("tombstoned", 0),
            )
        )

    report = ManifestParserAuditReport(sources=out)
    logger.info(
        "manifest_parser_audit: sources=%d total_stuck_no_parser=%d",
        len(out),
        report.total_stuck_no_parser,
    )
    return report
